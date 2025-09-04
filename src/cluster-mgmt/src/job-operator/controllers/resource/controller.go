/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resource

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/controllers/resource/lock"
	"cerebras.com/job-operator/controllers/resource/namespace"
)

const (
	resourceReloadEvent = "_ResourceReloadEvent_"
)

var (
	triggerResourceReload = event.GenericEvent{Object: &corev1.ConfigMap{ObjectMeta: v1.ObjectMeta{Name: resourceReloadEvent}}}
	triggerReinitialize   = event.GenericEvent{Object: &corev1.ConfigMap{ObjectMeta: v1.ObjectMeta{Name: common.ConfigEventName}}}
	triggerNsrScale       = event.GenericEvent{Object: &corev1.ConfigMap{ObjectMeta: v1.ObjectMeta{Name: namespace.NSRecycleEvent}}}
)

type Controller struct {
	client   client.Client
	recorder record.EventRecorder
	log      logrus.FieldLogger

	// channel/cfg to detect resource update
	NewCfg  *common.ClusterConfig
	CfgChan chan common.ConfigUpdateEvent
	sync.Mutex
	isInitialized      bool
	shouldReconcileNSR bool
	isLeader           func() bool

	internalReconcileEventCh chan event.GenericEvent

	cfgMgr    *common.ClusterConfigMgr
	Cfg       *common.ClusterConfig
	resources *resource.Collection

	lockReconciler *lock.Reconciler
	nsReconciler   *namespace.Reconciler
}

func NewController(cfgMgr *common.ClusterConfigMgr) *Controller {
	c := &Controller{
		CfgChan:            make(chan common.ConfigUpdateEvent, 30),
		cfgMgr:             cfgMgr,
		log:                logrus.StandardLogger().WithField("component", "resource-controller"),
		shouldReconcileNSR: common.Namespace == common.SystemNamespace,
	}
	c.startConfigUpdateRoutine()
	return c
}

func newController(resources *resource.Collection) *Controller {
	return &Controller{
		CfgChan:            make(chan common.ConfigUpdateEvent, 30),
		log:                logrus.StandardLogger().WithField("component", "resource-controller"),
		Cfg:                &common.ClusterConfig{Resources: resources},
		shouldReconcileNSR: common.Namespace == common.SystemNamespace,
	}
}

// routine to receive new config
func (c *Controller) startConfigUpdateRoutine() {
	logger := c.log.WithField("method", "startConfigUpdateRoutine")
	go func() {
		for newCfg := range c.CfgChan {
			if newCfg.Reason == common.ConfigUpdateNamespace {
				continue // ignore NS updates since NS reconciler updates shared c.collection
			}
			logger.Info("new config detected in resource controller")
			c.Lock()
			c.NewCfg = newCfg.Cfg
			c.Unlock()
			c.internalReconcileEventCh <- triggerReinitialize
		}
	}()
}

func (c *Controller) reloadConfig() bool {
	c.Lock()
	defer c.Unlock()
	if c.NewCfg != nil {
		c.Cfg = c.NewCfg
		c.NewCfg = nil
		return true
	}
	return false
}

type request ctrl.Request

func (r request) isCfgEvent() bool {
	return r.Name == common.ConfigEventName
}

func (r request) isResourceReloadEvent() bool {
	return r.Name == resourceReloadEvent
}

func (r request) isNsEvent() bool {
	return strings.HasPrefix(r.Name, namespace.NSREvent) ||
		strings.HasPrefix(r.Name, namespace.NSEvent) ||
		r.Name == namespace.NSRecycleEvent
}

func (r request) isLockEvent() bool {
	return !r.isCfgEvent() && !r.isNsEvent() && !r.isResourceReloadEvent()
}

func (c *Controller) Reconcile(ctx context.Context, origReq ctrl.Request) (ctrl.Result, error) {
	req := request(origReq)
	ctx = context.WithValue(ctx, "reconcile", uuid.NewString())
	cfgInitialized, err := c.initialize(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !cfgInitialized {
		// wait for c.Cfg initialization
		return common.ConfigInitRequeue, nil
	}

	if req.isNsEvent() && c.shouldReconcileNSR {
		return c.nsReconciler.Reconcile(ctx, ctrl.Request(req))
	} else if req.isLockEvent() && (c.nsReconciler == nil || c.nsReconciler.IsResourceInitialized()) {
		// trigger lock grants with initialized resources after NSR has reconciled at least once
		return c.lockReconciler.Reconcile(ctx, origReq)
	} else if req.isResourceReloadEvent() || req.isCfgEvent() {
		// trigger update resources after reload
		// config update case resources are reinitialized already
		if req.isResourceReloadEvent() {
			c.resources.ResetAllocations()
			c.lockReconciler.UpdateResources(c.resources)
		}
		// trigger lock reconcile after resource reload/config update with a empty request
		return c.lockReconciler.Reconcile(ctx, ctrl.Request{})
	}
	return ctrl.Result{}, nil
}

func (c *Controller) initialize(ctx context.Context) (bool, error) {
	log.FromContext(ctx).Info("checking reconciler initialization")
	if c.reloadConfig() {
		c.log.WithField("cluster-health", c.Cfg.PrettyHealthInfo("", true)).
			Info("new config loaded in resource controller")
		c.isInitialized = false
	}
	if c.isInitialized {
		return true, nil
	}
	if c.Cfg == nil {
		return false, nil
	}

	c.log.Info("resource controller initialization start")
	c.resources = c.Cfg.Resources.DeepUnallocatedCopy()
	c.lockReconciler = lock.NewLockReconciler(c.client, c.recorder, c.Cfg, c.resources)
	if _, err := c.lockReconciler.Initialize(ctx); err != nil {
		c.log.Error(err, "lockReconciler initialization failed")
		return false, err
	}
	if c.shouldReconcileNSR {
		c.nsReconciler = namespace.NewNamespaceReconciler(
			c.client,
			c.resources,
			namespace.NewResourceStatus(c.resources),
			c.onNsChange,
		)
		_, err := c.nsReconciler.Reconcile(ctx, ctrl.Request{})
		if err != nil {
			c.log.Error(err, "nsReconciler controller Reconcile failed")
			return false, err
		}
	}
	c.isInitialized = true
	return true, nil
}

func (c *Controller) onNsChange(changes common.NSChange) {
	for _, id := range changes.Unassigned() {
		rtype, rname := resource.MustRIDToTypeName(id)
		if rtype == resource.NodegroupType {
			c.resources.UpdateGroupProp(rname, common.NamespaceKey, "")
		} else {
			c.resources.UpdateProp(id, common.NamespaceKey, "")
		}
	}
	for id, ns := range changes.Assigned() {
		rtype, rname := resource.MustRIDToTypeName(id)
		if rtype == resource.NodegroupType {
			c.resources.UpdateGroupProp(rname, common.NamespaceKey, ns)
		} else {
			c.resources.UpdateProp(id, common.NamespaceKey, ns)
		}
	}

	if c.cfgMgr != nil {
		c.cfgMgr.UpdateNsAssignment(changes)
	}

	c.internalReconcileEventCh <- triggerResourceReload // trigger grant cycle after NS update
}

// SetupWithManager sets up Controller to manage Locks, watch System + NamespaceReservation events, and watch
// the cluster-cfg map to trigger config refreshes
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	c.client = mgr.GetClient()
	c.recorder = mgr.GetEventRecorderFor("resource-controller")

	b := ctrl.NewControllerManagedBy(mgr).Named("resource-controller")
	b.For(
		&rlv1.ResourceLock{},
		builder.WithPredicates(
			common.CurrentNamespacePredicate(),
			predicate.Funcs{
				// watch all create/delete events
				// watch update events for pending/granted locks for priority change/parent reservation status change
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldLock, ok1 := e.ObjectOld.(*rlv1.ResourceLock)
					newLock, ok2 := e.ObjectNew.(*rlv1.ResourceLock)
					if !ok1 || !ok2 {
						// Not a ResourceLock, ignore
						return false
					} else if oldLock.HadFailed() || newLock.HadFailed() {
						// Do not process error locks
						return false
					}
					return true
				},
			},
		))

	prefixedEventMapper := func(prefix string) func(ctx context.Context, o client.Object) []reconcile.Request {
		return func(ctx context.Context, o client.Object) []reconcile.Request {
			return []reconcile.Request{
				{NamespacedName: types.NamespacedName{
					Name: prefix + o.GetName(),
				}},
			}
		}
	}

	b.Watches(
		&namespacev1.NamespaceReservation{},
		handler.EnqueueRequestsFromMapFunc(prefixedEventMapper(namespace.NSREvent)),
	)

	b.Watches(
		&corev1.Namespace{},
		handler.EnqueueRequestsFromMapFunc(prefixedEventMapper(namespace.NSEvent)),
		builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				// for orphaned user NS
				_, ok := e.Object.GetLabels()[namespacev1.UserNSLabel]
				return ok
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				_, ok := e.ObjectOld.GetLabels()[namespacev1.UserNSLabel]
				if !ok {
					return false
				}
				// watch for Deletion updates (DeletionTimestamp != nil) since envtest env doesn't support NS delete
				return e.ObjectNew.GetDeletionTimestamp() != e.ObjectOld.GetDeletionTimestamp()
			},
			GenericFunc: func(e event.GenericEvent) bool { return false },
		}),
	)

	// event channel for triggering:
	// 1/ reconcile locks after nsReconciler has modified ns in shared collection
	// 2/ re-init reconcilers after config updates from cfgMgr
	// 3/ time-based tick to trigger namespace scale down routines
	c.internalReconcileEventCh = make(chan event.GenericEvent, 1024)
	b.WatchesRawSource(&source.Channel{Source: c.internalReconcileEventCh}, &handler.EnqueueRequestForObject{})

	if common.NamespaceScaleDownCheckInterval > 0 {
		if err := mgr.Add(scaleDownTriggerRunnable{c: c, tickInterval: common.NamespaceScaleDownCheckInterval}); err != nil {
			c.log.Fatalf("failed to add scaleDownTriggerRunnable to manager: %s", err.Error())
		}
	}

	return b.Complete(c)
}

// for test purpose only
func (c *Controller) GetResource(name string) (resource.Resource, bool) {
	return c.lockReconciler.GetResource(name)
}

// for test purpose only
func (c *Controller) GetCollection() *resource.Collection {
	return c.lockReconciler.GetCollection()
}

// for test purpose only
func (c *Controller) GetLockReconciler() *lock.Reconciler {
	return c.lockReconciler
}

type scaleDownTriggerRunnable struct {
	c *Controller

	tickInterval time.Duration
}

func (scaleDownTriggerRunnable) NeedLeaderElection() bool { return true }

func (t scaleDownTriggerRunnable) Start(ctx context.Context) error {
	t.c.log.Infof("starting namespace scale down trigger routine check interval: %s", t.tickInterval.String())
	defer t.c.log.Info("stopping namespace scale down trigger routine")

	tick := time.NewTicker(t.tickInterval)
	for {
		select {
		case _ = <-tick.C:
			t.c.internalReconcileEventCh <- triggerNsrScale
		case _ = <-ctx.Done():
			return ctx.Err()
		}
	}
}

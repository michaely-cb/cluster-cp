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

package namespace

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/controllers/cluster"
)

const (
	NSREvent       = "_NSREvent_"
	NSEvent        = "_NSEvent_"
	NSRecycleEvent = "_NSRecycleEvent_"

	// deprecated label, remove it if seen
	lastNamespaceLabel       = "lastAppliedNamespace"
	originalReplicaCountAnno = "cerebras/original-replica-count"
	systemCountAnno          = "cerebras/system-count"
)

var (
	DisableSystemNsAutoAssign bool // set manually for test purpose
	UserNsObserved            bool // set when user-namespaces are observed
)

// nsChange to populate changes to resource controller
type nsChange struct {
	// nsr name
	name string
	// RID -> updated NS. `RID:"foo"` means assigned to ns "foo" `RID-:""` means unassigned.
	// A resource can be unassigned and assigned in the same update reconcile but cannot be assigned to multiple NS
	resChanges map[string]string
	// nsr latest props
	nsrProps map[string]string
}

func (c nsChange) Unassigned() []string {
	var rv []string
	for k := range c.resChanges {
		klen := len(k)
		if klen != 0 && k[klen-1] == '-' {
			rv = append(rv, k[0:klen-1])
		}
	}
	return rv
}

func (c nsChange) Assigned() map[string]string {
	rv := map[string]string{}
	for k, v := range c.resChanges {
		klen := len(k)
		if klen != 0 && k[klen-1] != '-' {
			rv[k] = v
		}
	}
	return rv
}

func (c nsChange) GetProps() (string, map[string]string) {
	return c.name, c.nsrProps
}

func (c nsChange) IsEmpty() bool {
	return len(c.resChanges) == 0
}

func (c nsChange) addUnassigned(ids []string) {
	for _, id := range ids {
		c.resChanges[id+"-"] = ""
	}
}

func (c nsChange) addAssigned(ids []string, ns string) {
	if ns == "" {
		panic("cannot assign to empty namespace")
	}
	for _, id := range ids {
		if v, ok := c.resChanges[id]; !ok || v == ns {
			c.resChanges[id] = ns
		} else {
			panic(fmt.Sprintf("assigned resource to 2 namespaces in same reconcile, resource=%s ns=%s,%s", id, v, ns))
		}
	}
}

type reconcileCtx struct {
	context.Context

	log logr.Logger

	modifiedRID nsChange
}

type Reconciler struct {
	client client.Client // cached client for NSR lookups

	resourceStatus ResourceStatus

	initialized bool
	nsState

	notifyNsrChangeCallback func(common.NSChange)
}

func NewNamespaceReconciler(
	client client.Client,
	c *resource.Collection,
	resourceStatus ResourceStatus,
	callback func(common.NSChange)) *Reconciler {

	return &Reconciler{
		client:                  client,
		resourceStatus:          resourceStatus,
		nsState:                 nsState{collection: newCollectionForNsMgmt(c)},
		notifyNsrChangeCallback: callback,
	}
}

//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=namespacereservations,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=namespacereservations/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=namespacereservations/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=list;delete;deletecollection

/*
Reconcile updates the Status field of NamespaceReservations (NSR) based on their requests or
implied requests from legacy System.metadata.labels.namespace updates. The reconciliation rules are:

Overall:
 1. Resources can be explicitly assigned to a namespace, as indicated by an entry in NSR.status.
 2. Resources that exist but are not in any NSR.status are implicitly assigned to the SystemNamespace if there are no user namespaces
 3. Changes to the NSR.status are reconciled by granting or rejecting the request in NSR.spec

NSR Reconcile Rules:
 1. NSR.spec describes a selector for assigning a set of Systems, Nodegroups, and Nodes.
 2. On update reconcile, if the spec.requestUid != status.requestHistory[0].requestUid:
    A. Calculate the resources added/released by the user params
    B. If it can be released without affecting running jobs, and there are enough free resources for requested params, do
    assign + release. If no such assignment exists, then the request failed and the requestHistory is updated
    accordingly.

NS Reconcile Rules:
 1. NS deletes cascade to delete the NSR with the same name as that NS

ConfigUpdate Rules:
 1. ConfigUpdates may cause resource deletions, forcing de-allocations and out-of-sync NSRs
 2. New resources will be added to the system namespace automatically if there are no user namespaces
*/
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqType, resourceName := splitRequest(req.Name)
	logger := log.FromContext(ctx).WithName("nsReconciler").
		WithValues("type", reqType, "resource", resourceName, "reconcile", ctx.Value("reconcile"))
	rctx := reconcileCtx{Context: ctx, log: logger, modifiedRID: nsChange{name: resourceName, resChanges: make(map[string]string)}}

	if err := r.initialize(rctx); err != nil {
		rctx.log.Error(err, "failed to initialize")
		return ctrl.Result{}, err
	}

	switch reqType {
	case NSREvent:
		var nsrWasScaledDown bool
		{
			nsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: resourceName}}
			if err := r.client.Get(ctx, client.ObjectKey{Name: resourceName}, nsr); err != nil {
				if apierrors.IsNotFound(err) {
					rctx.log.Info("session deleted", "session", resourceName)
					if err = r.cascadeNSRDelete(rctx, resourceName); err != nil {
						return ctrl.Result{}, err
					}
					r.removeNSR(rctx, resourceName)
				} else {
					return ctrl.Result{}, err
				}
			} else {
				nsrWasScaledDown = !nsr.Status.HasResources() && nsr.Status.GrantedRequest.RequestUid != ""
				if nsr.Spec.RequestUid != "" && nsr.Spec.RequestUid != nsr.Status.GetLastAttemptedRequest().Request.RequestUid {
					rctx.log.Info("reconcile NSR",
						"spec", common.FmtLogJson(nsr.Spec),
						"lastGranted", common.FmtLogJson(nsr.Status.GrantedRequest),
						"haveSystems", nsr.Status.Systems,
						"groups", nsr.Status.Nodegroups,
						"nodes", nsr.Status.Nodes)

					// In rel-2.4, coordinator and management nodes separation was introduced.
					// From the nsr request point of view, old cluster servers only set the "ManagementNodeNames" field,
					// while new cluster servers set both the "ManagementNodeNames" and "CoordinatorNodeNames" field with
					// same values. From the request processing point of view, the job operator only processes the
					// "CoordinatorNodeNames" field internally.
					// TODO: In rel-2.5, we deprecate the "ManagementNodeNames" field.
					if len(nsr.Spec.RequestParams.CoordinatorNodeNames) == 0 {
						nsr.Spec.RequestParams.CoordinatorNodeNames = append(nsr.Spec.RequestParams.CoordinatorNodeNames, nsr.Spec.RequestParams.ManagementNodeNames...)
					}

					mode := nsr.Spec.RequestParams.UpdateMode
					switch mode {
					case namespacev1.ModeNotSet, namespacev1.SetResourceSpecMode, namespacev1.SetResourceSpecDryRunMode:
						if err = r.reconcileNSRResourceSpecMode(rctx, nsr); err != nil {
							return ctrl.Result{}, err
						}
					case namespacev1.DebugRemoveMode, namespacev1.DebugRemoveAllMode, namespacev1.DebugAppendMode, namespacev1.DebugSetMode:
						if err = r.reconcileNSRDebugMode(rctx, nsr); err != nil {
							return ctrl.Result{}, err
						}
					default:
						nsr.Status.SetFailedRequest(nsr.Spec.ReservationRequest, fmt.Sprintf(
							"unrecognized session update request mode '%s'. "+
								"job-operator may be older than cluster-server", mode))
						if err = r.client.Status().Update(ctx, nsr); err != nil {
							return ctrl.Result{}, err
						}
					}

					rctx.log.Info("nsr updated", "requestStatus", common.FmtLogJson(nsr.Status.GetLastAttemptedRequest()))
					r.nsState.reservations[nsr.Name] = nsr
				}
			}

			r.syncAndNotifyNsrChange(rctx, nsr)
			if nsr.Name != common.SystemNamespace {
				UserNsObserved = true
			}

			clusterSysCount, err := r.syncSystemLabels(rctx, nsr.Name, nsr.Status.Systems)
			if err != nil {
				return ctrl.Result{}, err
			}
			if err := r.ensureNSExists(rctx, nsr.Name); err != nil {
				return ctrl.Result{}, err
			}
			if nsrWasScaledDown && nsr.Status.HasResources() {
				if err := r.scaleUpNS(rctx, nsr); err != nil {
					return ctrl.Result{}, err
				}
			} else if _, ok := nsr.Annotations[NSRScaleDownNowAnnotation]; ok && !nsr.Status.HasResources() {
				// for testing
				if err := r.scaleDownNS(rctx, nsr); err != nil {
					return ctrl.Result{}, err
				}
			} else {
				if err := r.annotateDeploy(rctx, nsr, clusterSysCount); err != nil {
					return ctrl.Result{}, err
				}
			}

			return ctrl.Result{}, r.updateUnassignedResourcesCM(rctx)
		}
	case NSEvent:
		{
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: resourceName}}
			nsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: resourceName}}
			if err := r.client.Get(ctx, client.ObjectKey{Name: resourceName}, ns); err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			} else {
				if ns.DeletionTimestamp == nil {
					// cleanup orphan namespace
					if err := r.client.Get(ctx, client.ObjectKey{Name: resourceName}, nsr); err != nil && apierrors.IsNotFound(err) &&
						ns.CreationTimestamp.Add(SessionIdleThresholdForCleanup).Before(time.Now()) {
						rctx.log.Info("session not exist for orphaned NS", "namespace", resourceName)
						return ctrl.Result{}, r.cascadeNSRDelete(rctx, resourceName)
					}
					rctx.log.Info("event triggered for active namespace, ignore", "namespace", resourceName)
					return ctrl.Result{}, nil
				}
			}
			// ns deletes trigger nsr deletes
			rctx.log.Info("namespace deleted, cascade delete to NSR if exists", "namespace", resourceName)
			if err := r.client.Delete(rctx, nsr); err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{}, nil
		}
	case NSRecycleEvent:
		{
			// recycle idle empty NSRs
			downs, cleans := r.findNSRecycleCandidates()
			for _, candidate := range cleans {
				if err := r.cleanupNS(rctx, candidate); err != nil {
					return ctrl.Result{}, err
				}
			}
			for _, candidate := range downs {
				if err := r.scaleDownNS(rctx, candidate); err != nil {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{}, nil
		}
	default:
		return ctrl.Result{}, nil
	}
}

func splitRequest(name string) (string, string) {
	if strings.HasPrefix(name, NSREvent) {
		return NSREvent, name[len(NSREvent):]
	}
	if strings.HasPrefix(name, NSEvent) {
		return NSEvent, name[len(NSEvent):]
	}
	if strings.HasPrefix(name, NSRecycleEvent) {
		return NSRecycleEvent, NSRecycleEvent
	}
	return common.ConfigEventName, common.ConfigEventName
}

// indicates at least one session observed and ready for lock scheduler to reconcile
func (r *Reconciler) IsResourceInitialized() bool {
	return r.initialized && len(r.nsState.reservations) > 0
}

// initialize reloads the cluster configuration if there is a new one available or on first reconcile.
func (r *Reconciler) initialize(ctx reconcileCtx) (err error) {
	if r.initialized {
		return nil
	}

	ctx.log.Info("initializing")
	// current assignment state is reflected in NSR.status
	nsrList := namespacev1.NamespaceReservationList{}
	if err = r.client.List(ctx, &nsrList); err != nil {
		return err
	}
	r.nsState.reservations = make(map[string]*namespacev1.NamespaceReservation)

	// initialize each NSR, updating the spec or status if needed
	UserNsObserved = false
	for i := range nsrList.Items {
		nsr := &nsrList.Items[i]

		if nsr.Name != common.SystemNamespace {
			ctx.log.WithValues("name", nsr.Name).Info("found user namespace, skip auto-assign system namespace")
			UserNsObserved = true
		}

		if setInitialResourceParams := r.nsState.addInitialNSR(nsr); setInitialResourceParams != nil {
			updatedNsr := nsr.DeepCopy()
			updatedNsr.Spec.RequestUid = operatorRequestId()
			updatedNsr.Spec.RequestParams = *setInitialResourceParams
			// xxx: this will clobber any user request that has not been processed yet
			if err = r.client.Update(ctx, updatedNsr); err != nil {
				ctx.log.WithValues("nsr", nsr.Name, "err", err.Error()).Info("warning: failed to update NSR spec on init")
				return err
			}
			ctx.log.WithValues("nsr", nsr.Name, "spec", *setInitialResourceParams).Info("updated nsr.spec to remove deleted resources")
			r.nsState.reservations[updatedNsr.Name] = updatedNsr
		} else {
			// check if resource estimates changed (e.g. crd compile/execute memory changed on job-operator redeploy)
			rspec := estimateResourceSpec(r.collection, nsr.Status, nsr.GetWorkloadType())
			if rspec != nsr.Status.CurrentResourceSpec {
				updatedNsr := nsr.DeepCopy()
				updatedNsr.Status.CurrentResourceSpec = rspec

				if err = r.client.Status().Update(ctx, updatedNsr); err != nil {
					ctx.log.WithValues("nsr", nsr.Name, "err", err.Error()).Info("warning: failed to update NSR status on init")
					return err
				}
				ctx.log.WithValues("nsr", nsr.Name).Info("updated NSR on init due to CurrentResourceSpec change")
				r.nsState.reservations[updatedNsr.Name] = updatedNsr
			}
		}
	}

	// config resChanges can add / remove free resources
	if err = r.updateUnassignedResourcesCM(ctx); err != nil {
		return err
	}

	// if the cluster has system namespace only, then we auto-assign resources to the system namespace
	if !DisableSystemNsAutoAssign && !UserNsObserved {
		if err = r.assignAllResourcesToSystemNs(ctx); err != nil {
			return err
		}
	}
	r.initialized = true
	return nil
}

// syncSystemLabels unlabels systems which are no longer owned by the NSR and labels systems owned by the NSR.
func (r *Reconciler) syncSystemLabels(ctx reconcileCtx, ns string, systems []string) (int, error) {
	shouldLabel := common.MapBool(systems)
	systemList := &systemv1.SystemList{}
	if err := r.client.List(ctx, systemList); err != nil {
		return 0, err
	}
	systemMap := map[string]systemv1.System{}
	for _, item := range systemList.Items {
		systemMap[item.GetName()] = item
	}
	if cluster.DisableLabeler {
		return len(systemList.Items), nil
	}

	ctx.log.V(1).WithValues("namespace", ns, "systems", systems).Info("sync system labels")
	rmNSPatch := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"labels":{"namespace":null,"`+lastNamespaceLabel+`":null}}}`))
	for sysName, sys := range systemMap {
		if !shouldLabel[sysName] && sys.Labels[common.NamespaceKey] == ns {
			sys.ResourceVersion = ""
			if err := r.client.Patch(ctx, &sys, rmNSPatch); err != nil {
				ctx.log.Info("failed to unlabel patch", "system", sysName, "err", err)
				if !apierrors.IsNotFound(err) {
					return 0, err
				}
			}
			ctx.log.Info("unlabeled system", "system", sysName, "prevNamespace", ns)
		} else if shouldLabel[sysName] && sys.Labels[common.NamespaceKey] == ns {
			ctx.log.V(1).Info("skip label due to previously labeled by self", "namespace", ns, "system", sysName)
			delete(shouldLabel, sysName)
		}
	}

	addNSPatch := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"labels":{"namespace":"`+ns+`"}}}`))
	for sysName := range shouldLabel {
		sys := &systemv1.System{ObjectMeta: metav1.ObjectMeta{Name: sysName}}
		if err := r.client.Patch(ctx, sys, addNSPatch); err != nil {
			ctx.log.Info("failed to label patch", "system", sysName, "err", err)
			if !apierrors.IsNotFound(err) {
				return 0, err
			}
		}
		ctx.log.Info("labeled system", "system", sys.Name, "namespace", ns)
	}
	return len(systemList.Items), nil
}

func (r *Reconciler) syncAndNotifyNsrChange(rctx reconcileCtx, nsr *namespacev1.NamespaceReservation) {
	defer func() {
		if r.notifyNsrChangeCallback != nil {
			r.notifyNsrChangeCallback(rctx.modifiedRID)
		}
	}()
	rctx.modifiedRID.nsrProps = nsr.Labels
	if rctx.modifiedRID.IsEmpty() {
		rctx.log.V(1).Info("reconcile ended with no assignment resChanges")
		return
	}
	rctx.log.Info("reconcile ended with resource assignment resChanges", "changed", rctx.modifiedRID)

	for _, rid := range rctx.modifiedRID.Unassigned() {
		r.nsState.setNS(rid, "")
	}
	for rid, ns := range rctx.modifiedRID.Assigned() {
		r.nsState.setNS(rid, ns)
	}

	sessionName := nsr.Name

	r.updateResourceMetrics(sessionName, rctx.modifiedRID, nsr)
}

func (r *Reconciler) updateResourceMetrics(sessionName string, modifiedRID nsChange, nsr *namespacev1.NamespaceReservation) {
	// Clean up all previous metrics
	common.RemoveSessionResourceMetric(sessionName)

	// Record remove metrics for what was unassigned
	for _, rid := range modifiedRID.Unassigned() {
		resourceType, resourceName := resource.MustRIDToTypeName(rid)
		metricResourceType := MapToMetricResourceType(rid, r.collection, resourceType)
		common.RecordResourceMovement(metricResourceType, resourceName, "remove", sessionName)
	}
	// Record add metrics for ALL currently assigned resources
	for _, systemName := range nsr.Status.Systems {
		common.RecordResourceMovement(resource.SystemType, systemName, "add", sessionName)
	}
	for _, nodeName := range nsr.Status.Nodes {
		rid := resource.NodeType + "/" + nodeName
		if res, ok := r.collection.Get(rid); ok {
			nodeType := GetNodeMetricType(res) // Helper to determine node type
			common.RecordResourceMovement(nodeType, nodeName, "add", sessionName)
		}
	}
	for _, nodegroupName := range nsr.Status.Nodegroups {
		common.RecordResourceMovement(resource.NodegroupType, nodegroupName, "add", sessionName)
	}

	// Update session resource count metrics
	r.updateSessionResourceCounts(nsr)
}

// updateSessionResourceCounts updates all resource count metrics for a given session.
func (r *Reconciler) updateSessionResourceCounts(nsr *namespacev1.NamespaceReservation) {
	sessionName := nsr.Name

	systemCounts := float64(len(nsr.Status.Systems))
	nodegroupCounts := float64(len(nsr.Status.Nodegroups))

	// Update system and nodegroup counts
	common.UpdateSessionResourceCount(resource.SystemType, sessionName, systemCounts)
	common.UpdateSessionResourceCount(resource.NodegroupType, sessionName, nodegroupCounts)

	nodeCounts := map[string]int{}

	for _, nodeName := range nsr.Status.Nodes {
		rid := resource.NodeType + "/" + nodeName
		if res, ok := r.collection.Get(rid); ok {
			if res.IsCrdNode() || res.IsMgmtNode() {
				nodeCounts[resource.RoleCrdPropKey]++
			} else if res.IsAxNode() {
				nodeCounts[resource.RoleAxPropKey]++
			} else if res.IsIxNode() {
				nodeCounts[resource.RoleIxPropKey]++
			} else if res.IsBrNode() {
				nodeCounts[resource.RoleBRPropKey]++
			}
		}
	}
	// Update all node type counts
	for nodeType, count := range nodeCounts {
		common.UpdateSessionResourceCount(nodeType, sessionName, float64(count))
	}
}

func (r *Reconciler) removeNSR(rctx reconcileCtx, ns string) {
	delete(r.reservations, ns)
	rctx.modifiedRID.addUnassigned(
		r.collection.FilterByProps(map[string][]string{common.NamespaceKey: {ns}}, nil).ListRids(),
	)
}

// ensureNSExists creates Namespace if not exist else updates it if the labels do not exist
func (r *Reconciler) ensureNSExists(rctx reconcileCtx, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}

	isSystemNs := name == common.SystemNamespace
	label := namespacev1.UserNSLabel
	if isSystemNs {
		label = namespacev1.SystemNSLabel
	}

	if err := r.client.Get(rctx, client.ObjectKeyFromObject(ns), ns); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		if isSystemNs {
			return nil // don't create system namespace since this process should be running in the system namespace
		}
		rctx.log.Info("create namespace")
		ns.Labels = map[string]string{label: ""}
		return r.client.Create(rctx, ns)
	} else if _, ok := ns.Labels[label]; !ok {
		rctx.log.Info("update namespace labels")
		ns.Labels = common.EnsureMap(ns.Labels)
		ns.Labels[label] = ""
		return r.client.Update(rctx, ns)
	}
	return nil
}

// cascadeNSRDelete deletes namespace + cluster-scoped resources associated with the session
func (r *Reconciler) cascadeNSRDelete(rctx reconcileCtx, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := r.client.Get(rctx, client.ObjectKeyFromObject(ns), ns); err == nil {
		if _, ok := ns.Labels[namespacev1.UserNSLabel]; ok {
			if ns.DeletionTimestamp == nil {
				if delErr := r.client.Delete(rctx, ns); delErr != nil {
					return delErr
				}
				rctx.log.Info("deleted namespace", "namespace", name)
			} else {
				rctx.log.Info("namespace is already being deleted, skip deletion", "namespace", name)
			}
		} else {
			rctx.log.Info("no user label on Namespace, skip cascade delete namespace", "namespace", name)
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	// best effort clean up global resources if exist
	listOpts := client.ListOptions{LabelSelector: labels.SelectorFromSet(labels.Set{namespacev1.NSLabel: name})}
	cascadeDeleteOpts := client.DeleteAllOfOptions{
		ListOptions: listOpts,
	}

	// Delete ceph persistent volumes if exist otherwise if NS is recreated, volumes will exist in Released state
	// note: ceph static volume types do not support Delete reclaim policy otherwise this wouldn't be needed
	if err := r.client.DeleteAllOf(rctx, &corev1.PersistentVolume{}, &cascadeDeleteOpts); err != nil {
		rctx.log.Error(err, "delete persistent volumes failed", "namespace", name)
	} else {
		rctx.log.Info("cascade delete persistent volumes", "namespace", name)
	}

	return nil
}

func (r *Reconciler) assignAllResourcesToSystemNs(ctx reconcileCtx) error {
	hasUnassigned := false
	for _, rs := range r.collection.List() {
		if rs.InUnassignedPool() {
			hasUnassigned = true
			break
		}
	}
	if !hasUnassigned { // avoid unnecessary nsr updates when all resources are assigned
		return nil
	}

	ctx.log.Info("found unassigned resources, assigning to system namespace")
	nsrSpec := namespacev1.NamespaceReservationSpec{
		ReservationRequest: r.newSystemNamespaceSelectAllRequest(),
	}
	sysNsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: common.SystemNamespace}}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(sysNsr), sysNsr); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		sysNsr.Spec = nsrSpec
		ctx.log.WithValues("requestUid", nsrSpec.RequestUid).Info("creating system nsr")
		return r.client.Create(ctx, sysNsr)
	} else {
		sysNsr.Spec = nsrSpec
		ctx.log.WithValues("requestUid", nsrSpec.RequestUid).Info("updating system nsr")
		return r.client.Update(ctx, sysNsr)
	}
}

// newSystemNamespaceSelectAllRequest returns a request that selects all resources in the cluster
func (r *Reconciler) newSystemNamespaceSelectAllRequest() namespacev1.ReservationRequest {
	params := namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode}

	for _, rs := range r.collection.List() {
		switch rs.Type() {
		case resource.SystemType:
			params.SystemNames = append(params.SystemNames, rs.Name())
		case resource.NodegroupType:
			params.NodegroupNames = append(params.NodegroupNames, rs.Name())
		case resource.NodeType:
			if _, ok := rs.GetProp(resource.RoleCrdPropKey); ok {
				params.CoordinatorNodeNames = append(params.CoordinatorNodeNames, rs.Name())
			} else if _, ok = rs.GetProp(resource.RoleAxPropKey); ok {
				params.ActivationNodeNames = append(params.ActivationNodeNames, rs.Name())
			} else if _, ok = rs.GetProp(resource.RoleIxPropKey); ok {
				params.InferenceDriverNodeNames = append(params.InferenceDriverNodeNames, rs.Name())
			}
		}
	}

	return namespacev1.ReservationRequest{
		RequestParams: params,
		RequestUid:    operatorRequestId(),
	}
}

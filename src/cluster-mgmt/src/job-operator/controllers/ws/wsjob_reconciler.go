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

package ws

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/go-logr/logr"
	batchV1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/utils/lru"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	commonapisv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonctrlv1 "github.com/kubeflow/common/pkg/controller.v1/common"
	"github.com/kubeflow/common/pkg/controller.v1/control"
	"github.com/kubeflow/common/pkg/controller.v1/expectation"
	commonutil "github.com/kubeflow/common/pkg/util"

	volcanoclient "volcano.sh/apis/pkg/client/clientset/versioned"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

// WSJobReconciler reconciles a WSJob object
type WSJobReconciler struct {
	*WSJobController
}

const controllerName = "wsjob-controller"

const (
	CoordinatorNodeNameAnnotationKey = "k8s.cerebras.com/wsjob-coordinator-node-name"

	SystemNameAnnotationKey = "k8s.cerebras.com/wsjob-system-names"

	// NodeGroupAnnotationKey points to cached name of elected node groups if nodeGroupScheduling is enabled
	NodeGroupAnnotationKey = "k8s.cerebras.com/wsjob-nodegroup-names"

	// MemxSecondaryNicAnnotationKey indicates whether secondary nics were disabled, partially used,
	// fully used, or not used at all.
	MemxSecondaryNicAnnotationKey = "cerebras/wsjob-memx-secondary-nics"
)

type lockCondState int

const (
	lockCondStateNew lockCondState = iota
	lockCondStateAwait
	lockCondStateGranted
)

func NewJobOperatorReconciler(
	clusterConfig *wscommon.ClusterConfig,
	mgr manager.Manager,
	enableGangScheduling bool) *WSJobReconciler {

	cfg := mgr.GetConfig()
	cfg.QPS = float32(wscommon.KubeApiQPS)
	cfg.Burst = wscommon.KubeApiQPS
	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)
	volcanoClientSet := volcanoclient.NewForConfigOrDie(cfg)
	sharedInformers := informers.NewSharedInformerFactory(kubeClientSet, 0)
	priorityClassInformer := sharedInformers.Scheduling().V1beta1().PriorityClasses()

	wsController := &WSJobController{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Recorder:  &eventRecorderInterceptor{wrapped: mgr.GetEventRecorderFor(controllerName)},
		ApiReader: mgr.GetAPIReader(),
		Log:       mgr.GetLogger(),

		Cfg: clusterConfig,
		// assume low frequency of config change
		CfgChan: make(chan wscommon.ConfigUpdateEvent, 10),

		// 8x systems or 16 - whichever is greater. Assumes 1 active train job + 7 active compile jobs per system
		// which is an overestimate for most cases. Precision isn't critical here since this is an optimization.
		JobResourceCache: lru.New(int(math.Max(float64(8*len(clusterConfig.Systems)), 16.0))),

		// Similar overestimates like above.
		WorkflowCache: lru.New(int(math.Max(float64(8*len(clusterConfig.Systems)), 16.0))),

		WorkflowEventChan: make(chan event.GenericEvent, 1024),

		MetricsQuerier: wscommon.NewPromMetricsQuerier(""),
	}

	wsController.JobController = commonctrlv1.JobController{
		Config: commonctrlv1.JobControllerConfiguration{
			EnableGangScheduling:   enableGangScheduling,
			EnableMultus:           !wscommon.DisableMultus,
			KubeApiQPS:             wscommon.KubeApiQPS,
			PendingPodGracePeriod:  wscommon.PendingPodGracePeriod,
			LockCleanupGracePeriod: wscommon.LockCleanupGracePeriod,
			ClientLeaseDuration:    wscommon.ClientLeaseDuration,
		},
		Controller:                  wsController,
		Expectations:                expectation.NewControllerExpectations(),
		WorkQueue:                   &wscommon.DummyWorkQueue{},
		Recorder:                    wsController.Recorder,
		KubeClientSet:               kubeClientSet,
		VolcanoClientSet:            volcanoClientSet,
		PriorityClassLister:         priorityClassInformer.Lister(),
		PriorityClassInformerSynced: priorityClassInformer.Informer().HasSynced,
		PodControl:                  control.RealPodControl{KubeClient: kubeClientSet, Recorder: wsController.Recorder},
		ServiceControl: WsjobServiceControl{
			RealServiceControl: control.RealServiceControl{KubeClient: kubeClientSet, Recorder: wsController.Recorder},
			client:             mgr.GetClient(),
		},
	}

	wsReconciler := &WSJobReconciler{
		WSJobController: wsController,
	}
	wsReconciler.startConfigUpdateRoutine()
	return wsReconciler
}

// routine to receive new config
func (r *WSJobReconciler) startConfigUpdateRoutine() {
	go func() {
		for newCfg := range r.CfgChan {
			r.Log.Info("new config detected in job controller")
			r.Lock()
			r.NewCfg = newCfg.Cfg
			r.Unlock()
			r.cfgInitialized = true
		}
	}()
}

func (r *WSJobReconciler) checkConfigReload() {
	if r.NewCfg != nil {
		r.Log.Info("new config loaded in job controller",
			"cluster-health", r.NewCfg.PrettyHealthInfo("", false))
		r.Lock()
		r.Cfg = r.NewCfg
		r.NewCfg = nil
		r.Unlock()
	}
}

func (r *WSJobReconciler) IsInferenceJobSharingEnabledForSession(wsjob *wsapisv1.WSJob) bool {
	jobSharingEnabledForSession := r.Cfg.GetNsrProperty(wsjob.Namespace, wscommon.InferenceJobSharingSessionLabelKey)
	return !wscommon.DisableInferenceJobSharing && wsjob.IsInference() && jobSharingEnabledForSession == strconv.FormatBool(true)
}

// TODO: remove once SwDriver is scheduled on IX/SX
func (r *WSJobReconciler) IsSwDriverMutualExclusionEnabledForSession(wsjob *wsapisv1.WSJob) bool {
	return r.Cfg.GetNsrProperty(wsjob.Namespace, wscommon.SwDriverMutualExclusionSessionLabelKey) == strconv.FormatBool(true)
}

func (r *WSJobReconciler) IsInferenceJobSharingEnabledForReplica(wsjob *wsapisv1.WSJob, rt commonapisv1.ReplicaType) bool {
	return r.IsInferenceJobSharingEnabledForSession(wsjob) &&
		(rt == wsapisv1.WSReplicaTypeKVStorageServer ||
			rt == wsapisv1.WSReplicaTypeActivation ||
			rt == wsapisv1.WSReplicaTypeChief)
}

//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=wsjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=wsjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=wsjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=resourcelocks,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods/log,verbs=get
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;delete;update
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch;update
//+kubebuilder:rbac:groups="",resources=nodes/status,verbs=get;list;patch
//+kubebuilder:rbac:groups="",resources=endpoints,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="k8s.cni.cncf.io",resources=network-attachment-definitions,verbs=get;list

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *WSJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	logger := r.Log.WithValues("component", "wsJobReconciler", wsapisv1.Singular, req.NamespacedName)
	ctx = log.IntoContext(ctx, logger)
	r.checkConfigReload()
	if !r.cfgInitialized {
		// defer job processing until initial config update
		return wscommon.ConfigInitRequeue, nil
	}
	wsjob := &wsapisv1.WSJob{}
	needReconcile := false

	defer func() {
		if err == nil {
			return
		}
		var criticalErr *CriticalError
		var requeueErr *commonapisv1.RequeueAfterTTL
		switch {
		case errors.As(err, &criticalErr):
			// Handle CriticalError
			err = r.failJob(wsjob, criticalErr.message, &wsjob.Status)
		case errors.As(err, &requeueErr):
			// Handle RequeueAfterTTL
			res = ctrl.Result{RequeueAfter: requeueErr.TTL}
			err = nil // Clear the error after assigning requeue result
		default:
			// Fallback for unknown error types
			logger.Error(err, "Job reconciling returned an error, retrying on the next loop")
		}
	}()

	err = r.Get(ctx, req.NamespacedName, wsjob)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "unable to fetch WSJob")
		return res, err
	}

	logger.Info("start reconciling", "rv", wsjob.ResourceVersion)

	if wsjob.CreationTimestamp.IsZero() || wsjob.DeletionTimestamp != nil || commonutil.IsEnded(wsjob.Status) {
		wscommon.JobMetricRemoved(req.Namespace, req.Name)
		if wsjob.CreationTimestamp.IsZero() || wsjob.DeletionTimestamp != nil {
			logger.Info("job already deleted or in deletion, cleanup resources if necessary")
			// job deleted, populate ns/name fields for cleanup flow below
			wsjob.Namespace = req.Namespace
			wsjob.Name = req.Name
			wscommon.PersistentJobMetricRemoved(req.Namespace, req.Name)
		} else {
			logger.Info("job ended, cleanup resources if necessary")
			// need reconcile to clean up pods/svc
			needReconcile = true
		}
		if err = r.releaseResources(ctx, wsjob); err != nil {
			logger.Error(err, "wsjob releaseResources failed")
			return res, err
		}
		if err = r.cleanupInfraSetup(ctx, wsjob); err != nil {
			logger.Error(err, "wsjob cleanupInfraSetup failed")
			return res, err
		}
		if err = r.cleanupIngress(ctx, wsjob.Namespace, wsjob.Name); err != nil {
			logger.Error(err, "wsjob cleanupIngress failed")
			return res, err
		}
		if err = r.cleanupLease(ctx, wsjob.Namespace, wsjob.Name); err != nil {
			logger.Error(err, "wsjob cleanupLease failed")
			return res, err
		}
	} else if wsjob.CancelRequested() {
		logger.Info("job cancel requested, start canceling job")
		err = r.CancelJob(
			wsjob,
			wsjob.Annotations[wsapisv1.CancelJobContextKey],
			wsjob.Annotations[wsapisv1.CancelWithStatusKey],
			&wsjob.Status)
		return res, err
	} else {
		jobKey, err := commonctrlv1.KeyFunc(wsjob)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("couldn't get jobKey for WSJob object %#v: %v", wsjob, err))
		}

		// Set default priorities to wsjob
		r.Scheme.Default(wsjob)
		wscommon.SemanticVersionMetricAdded(wsjob.Namespace, wsjob.Name,
			wsjob.GetAppClientSemver(), wsjob.GetClusterServerSemver(), wscommon.SemanticVersion)

		proceed, err := r.validate(wsjob)
		if !proceed || err != nil {
			return res, err
		}

		if err = r.createLease(ctx, req, wsjob); err != nil {
			logger.Error(err, "lease creation failed")
			return res, err
		}

		proceed, err = r.reconcileWorkflow(ctx, wsjob)
		if !proceed || err != nil {
			return res, err
		}

		resources, proceed, err := r.reconcileResources(ctx, wsjob)
		if !proceed || err != nil {
			return res, err
		}

		proceed, err = r.reconcileCfg(ctx, wsjob, resources)
		if !proceed || err != nil {
			return res, err
		}

		proceed, err = r.reconcileInfraSetup(ctx, wsjob)
		if !proceed || err != nil {
			logger.Error(err, "infra setup failed")
			return res, err
		}

		if err = r.createIngress(ctx, req, wsjob); err != nil {
			logger.Error(err, "ingress creation failed")
			return res, err
		}

		replicaTypes := wscommon.Keys(wsjob.Spec.WSReplicaSpecs)
		// no rush to reconcile if none of the expectations met, wait till pod/svc of a replica type fully created/deleted
		needReconcile = wscommon.SatisfiedExpectations(r.Expectations, jobKey, replicaTypes)
	}

	if !needReconcile {
		logger.Info("reconcile cancelled, WSJob does not need to do reconcile")
		return res, nil
	}
	// Use kubeflow common to reconcile the job related pod and service
	err = r.ReconcileJobs(wsjob, wsjob.Spec.WSReplicaSpecs, wsjob.Status, &wsjob.Spec.RunPolicy)
	return res, err
}

func (r *WSJobReconciler) validate(wsjob *wsapisv1.WSJob) (bool, error) {
	msg := ""
	defer func() {
		if msg != "" {
			r.Log.Info(msg)
		}
	}()
	// validate if job priority is a valid value
	if wsjob.Spec.Priority != nil {
		if *wsjob.Spec.Priority < wsapisv1.MinJobPriorityValue || *wsjob.Spec.Priority > wsapisv1.MaxJobPriorityValue {
			msg = fmt.Sprintf("Job priority value should be between %d and %d",
				wsapisv1.MinJobPriorityValue, wsapisv1.MaxJobPriorityValue)
			return false, &CriticalError{msg}
		}
	}
	// error out early if not enough resource in session
	if wsapisv1.IsMultiBox {
		if r.Cfg.GetSessionSystemCount(wsjob.Namespace) < int(wsjob.Spec.NumWafers) {
			msg = fmt.Sprintf(
				"invalid job, job requested %d system(s) but session '%s' has only %d system(s) assigned",
				wsjob.Spec.NumWafers, wsjob.Namespace, r.Cfg.GetSessionSystemCount(wsjob.Namespace))
			return false, &CriticalError{msg}
		} else if r.Cfg.GetSessionNodeCount(wsjob.Namespace) == 0 {
			msg = fmt.Sprintf(
				"invalid job, job session '%s' doesn't have any node resource assigned", wsjob.Namespace)
			return false, &CriticalError{msg}
		}
	}
	// error out early if from session marked as shared
	if r.Cfg.GetNsrProperty(wsjob.Namespace, namespacev1.IsRedundantModeKey) == "true" {
		msg = fmt.Sprintf(
			"invalid job, job session %s is in redundant mode and can only be accessible by jobs "+
				"from permitted sessions", wsjob.Namespace)
		return false, &CriticalError{msg}
	}
	// validate BR specs
	if wsjob.Spec.NumWafers <= 1 && wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce] != nil {
		msg = "invalid job, no BR should be specified for job with <= 1 csx request"
		return false, &CriticalError{msg}
	}
	if wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce] != nil &&
		*wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce].Replicas < 0 {
		msg = "invalid job, BR replicas should not be negative"
		return false, &CriticalError{msg}
	}
	if !commonutil.HasCondition(wsjob.Status, commonapisv1.JobQueueing) && !wsjob.IsCompile() &&
		wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce] != nil &&
		*wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce].Replicas > 1 {
		msg = "invalid job, don't specify BR replica to larger than 1, it will be auto computed"
		return false, &CriticalError{msg}
	}
	// check if each container has positive resources where specified
	for replicaType, spec := range wsjob.Spec.WSReplicaSpecs {
		for _, c := range spec.Template.Spec.Containers {
			err := r.validateResourceValue(replicaType, c.Resources.Limits, "limit")
			if err != nil {
				return false, err
			}
			err = r.validateResourceValue(replicaType, c.Resources.Requests, "request")
			if err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (r *WSJobReconciler) validateResourceValue(replicaType commonapisv1.ReplicaType,
	resources corev1.ResourceList, limOrRq string) error {
	for k, v := range resources {
		if v.Value() < 0 {
			failMsg := fmt.Sprintf("invalid job, replica type %s has negative resource %s %s (%d)",
				replicaType, limOrRq, k, v.Value())
			return &CriticalError{failMsg}
		}
	}
	return nil
}

func (r *WSJobReconciler) wsjobLogger(wsjob *wsapisv1.WSJob) logr.Logger {
	return r.Log.WithValues(
		"component", "wsJobReconciler",
		wsapisv1.Singular, client.ObjectKeyFromObject(wsjob).String())
}

// SetupWithManager sets up the controller with the Manager.
func (r *WSJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := controller.New(r.ControllerName(), mgr, controller.Options{
		Reconciler: r,
	})

	if err != nil {
		return err
	}

	// using onOwnerCreateFunc is easier to set defaults
	if err = c.Watch(source.Kind(mgr.GetCache(), &wsapisv1.WSJob{}), &handler.EnqueueRequestForObject{},
		wscommon.CurrentNamespacePredicate(),
		predicate.Funcs{
			CreateFunc: r.onOwnerCreateFunc(),
		},
	); err != nil {
		return err
	}

	// inject watching for job related pod
	if err = c.Watch(source.Kind(mgr.GetCache(), &corev1.Pod{}), handler.EnqueueRequestForOwner(
		mgr.GetScheme(), mgr.GetRESTMapper(),
		&wsapisv1.WSJob{}, handler.OnlyControllerOwner(),
	),
		wscommon.CurrentNamespacePredicate(),
		predicate.Funcs{
			CreateFunc: wscommon.OnDependentCreateFunc(r.Expectations),
			UpdateFunc: wscommon.OnDependentUpdateFunc(&r.JobController),
			DeleteFunc: wscommon.OnDependentDeleteFunc(r.Expectations),
		}); err != nil {
		return err
	}

	// inject watching for job related service
	if err = c.Watch(source.Kind(mgr.GetCache(), &corev1.Service{}), handler.EnqueueRequestForOwner(
		mgr.GetScheme(), mgr.GetRESTMapper(),
		&wsapisv1.WSJob{}, handler.OnlyControllerOwner(),
	),
		wscommon.CurrentNamespacePredicate(),
		predicate.Funcs{
			CreateFunc: wscommon.OnDependentCreateFunc(r.Expectations),
			UpdateFunc: wscommon.OnDependentUpdateFunc(&r.JobController),
			DeleteFunc: wscommon.OnDependentDeleteFunc(r.Expectations),
		}); err != nil {
		return err
	}

	if err = c.Watch(
		source.Kind(mgr.GetCache(), &rlv1.ResourceLock{}),
		handler.EnqueueRequestForOwner(
			mgr.GetScheme(), mgr.GetRESTMapper(),
			&wsapisv1.WSJob{}, handler.OnlyControllerOwner(),
		),
		wscommon.CurrentNamespacePredicate(),
		predicate.Funcs{
			CreateFunc: func(_ event.CreateEvent) bool { return false },
			UpdateFunc: func(e event.UpdateEvent) bool {
				// only process updates on lock gets granted or not
				state := e.ObjectNew.(*rlv1.ResourceLock).Status.State
				return state != rlv1.LockPending
			},
			DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		}); err != nil {
		return err
	}

	if err = c.Watch(
		source.Kind(mgr.GetCache(), &batchV1.Job{}),
		handler.EnqueueRequestForOwner(
			mgr.GetScheme(), mgr.GetRESTMapper(),
			&wsapisv1.WSJob{}, handler.OnlyControllerOwner(),
		),
		wscommon.CurrentNamespacePredicate(),
		predicate.Funcs{
			CreateFunc: func(_ event.CreateEvent) bool { return false },
			UpdateFunc: func(e event.UpdateEvent) bool {
				job := e.ObjectNew.(*batchV1.Job)
				return job.Status.Failed > 0 || job.Status.CompletionTime != nil
			},
			DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		}); err != nil {
		return err
	}

	return nil
}

func (r *WSJobReconciler) GetReconcilerName() string {
	return "WSJobReconciler"
}

// onOwnerCreateFunc modify creation condition.
func (r *WSJobReconciler) onOwnerCreateFunc() func(event.CreateEvent) bool {
	return func(e event.CreateEvent) bool {
		wsJob, ok := e.Object.(*wsapisv1.WSJob)
		if !ok {
			return true
		}

		r.Scheme.Default(wsJob)

		// could be new job creation or job-operator restart
		wscommon.CreatedJobsCounterInc(wsJob.Namespace, wsapisv1.Kind)
		if err := commonutil.UpdateJobConditions(
			&wsJob.Status, commonapisv1.JobCreated, "WSJobCreated", "job created"); err != nil {
			log.Log.Error(err, "append job condition error")
			return false
		}
		return true
	}
}

func (r *WSJobReconciler) markCfgUpdateDone(ctx context.Context,
	wsjob *wsapisv1.WSJob, clusterDetailsCM *corev1.ConfigMap) (bool, error) {
	// idempotent check
	if _, ok := wsjob.Annotations[wsapisv1.CfgUpdatedAnno]; ok {
		return true, nil
	}
	// skip if CM not updated yet
	// this assumes cluster details is updated only after BR update done
	if clusterDetailsCM.Data[wsapisv1.ClusterDetailsConfigUpdatedSignal] != wsapisv1.UpdatedSignalVal {
		return true, nil
	}
	r.Log.Info("start marking cfg update done", "job", wsjob.NamespacedName())
	job := wsjob.DeepCopy()
	job.Annotations[wsapisv1.CfgUpdatedAnno] = ""
	if err := r.Update(ctx, job); err != nil {
		return false, err
	}
	r.Log.Info("marking cfg update done success", "job", wsjob.NamespacedName())
	return false, nil
}

func (r *WSJobReconciler) reconcileCfg(ctx context.Context, wsjob *wsapisv1.WSJob, res jobResources) (bool, error) {
	clusterDetailsCM, proceed, err := r.reconcileClusterDetailsConfig(ctx, wsjob, res)
	if !proceed || err != nil {
		return proceed, err
	}

	proceed, err = r.updateWsjobByClusterDetails(ctx, wsjob, clusterDetailsCM)
	if !proceed || err != nil {
		return proceed, err
	}

	proceed, err = r.reconcileBrConfig(ctx, wsjob, res)
	if !proceed || err != nil {
		return proceed, err
	}

	proceed, err = r.updateWsJobByBrConfig(ctx, wsjob, res)
	if !proceed || err != nil {
		return proceed, err
	}

	return r.markCfgUpdateDone(ctx, wsjob, clusterDetailsCM)
}

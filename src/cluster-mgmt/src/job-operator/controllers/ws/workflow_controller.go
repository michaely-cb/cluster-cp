package ws

import (
	"context"
	"fmt"
	"strings"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/lru"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wsv1 "cerebras.com/job-operator/client/clientset/versioned/typed/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonutil "github.com/kubeflow/common/pkg/util"
)

const (
	ReleaseRequestPrefix  = "_release_"
	CancelRequestPrefix   = "_cancel_"
	PriorityRequestPrefix = "_priority_"
)

type WorkflowReconciler struct {
	client        client.Client
	wsV1Interface wsv1.WsV1Interface
	eventChan     chan event.GenericEvent
	workflowCache *lru.Cache
}

func NewWorkflowReconciler(eventChan chan event.GenericEvent, workflowCache *lru.Cache) *WorkflowReconciler {
	return &WorkflowReconciler{
		eventChan:     eventChan,
		workflowCache: workflowCache,
	}
}

//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=wsjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=wsjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=wsjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=resourcelocks,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=resourcelocks/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=resourcelocks/finalizers,verbs=update

// Reconcile accepts requests to reconcile on a workflow ID in a namespace.
// 1. update active job priorities to be in-sync
// 2. cancel jobs if workflow lease expired here
func (r *WorkflowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("workflowReconciler").
		WithValues("request", req, "reconcile", ctx.Value("reconcile"))
	ctx = log.IntoContext(ctx, logger)
	logger.Info("begin reconcile")
	defer func() { logger.V(1).Info("end reconcile") }()

	workflowId := request(req).getWorkflowId()
	jobsInWorkflow, err := getWorkflowJobs(ctx, r.client, req.Namespace, workflowId)
	if err != nil {
		return ctrl.Result{}, err
	}

	if request(req).isCancelRequest() {
		return r.cancelJobs(ctx, req, jobsInWorkflow)
	} else if request(req).isReleaseRequest() {
		return r.releaseReservations(ctx, req, jobsInWorkflow)
	} else if request(req).isPriorityRequest() {
		return r.updatePriority(ctx, req, jobsInWorkflow)
	}
	return ctrl.Result{}, nil
}

func (r *WorkflowReconciler) cancelJobs(ctx context.Context, req ctrl.Request,
	jobs *wsapisv1.WSJobList) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("CancelJobs")
	logger.Info("Workflow lease expiration detected")

	workflowId := request(req).getWorkflowId()
	lease := &coordv1.Lease{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: workflowId}, lease)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	for _, job := range jobs.Items {
		logger.Info("Cancel job/reservation in workflow", "job", job.Name)
		if err = cancelJob(ctx, logger, r.client,
			job.Namespace, job.Name,
			leaseExpireAnnotations(time.Second*time.Duration(*lease.Spec.LeaseDurationSeconds)),
			true); err != nil {
			logger.Error(err, "Failed to cancel job/reservation in workflow", "job", job.Name)
			return ctrl.Result{}, err
		}
	}
	// cleanup workflow lease
	return ctrl.Result{}, r.client.Delete(ctx, lease)
}

func (r *WorkflowReconciler) releaseReservations(ctx context.Context, req ctrl.Request,
	jobs *wsapisv1.WSJobList) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("CancelJobs")
	logger.Info("Workflow lease release detected")

	workflowId := request(req).getWorkflowId()
	lease := &coordv1.Lease{}
	err := r.client.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: workflowId}, lease)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	for _, job := range jobs.Items {
		logger.Info("Release job reservation in workflow", "job", job.Name)
		if err = releaseJob(ctx, logger, r.client, &job); err != nil {
			logger.Error(err, "Failed to release job reservation in workflow", "job", job.Name)
			return ctrl.Result{}, err
		}
	}
	// clear workflow lease release status to reduce duplicate events
	return ctrl.Result{}, r.client.Patch(ctx, lease,
		client.RawPatch(types.MergePatchType,
			wscommon.GetAnnotationPatch(map[string]string{
				commonv1.ResourceReservationStatus: "",
			})))
}

func (r *WorkflowReconciler) updatePriority(ctx context.Context, req ctrl.Request,
	jobs *wsapisv1.WSJobList) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("UpdatePriority")

	workflowId := request(req).getWorkflowId()
	e, ok := r.workflowCache.Get(req.Namespace + "/" + workflowId)
	if !ok {
		// We always expect the cache be set before being able to reconcile on a request.
		return ctrl.Result{},
			fmt.Errorf("received priority update request %s but cache not populated", workflowId)
	}
	priority := e.(*int)

	// patch workflow jobs priorities
	for _, j := range jobs.Items {
		if !commonutil.IsEnded(j.Status) && j.GetPriority() != *priority {
			logger.Info("Received a request to update job priority in workflow",
				"workflow", workflowId, "job", j.Name,
				"new-priority", *priority, "old-priority", j.GetPriority())
			err := wscommon.PatchWsjobPriority(ctx, r.wsV1Interface, j.Namespace, j.Name, *priority)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	return ctrl.Result{}, nil
}

func (r *WorkflowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.client = mgr.GetClient()
	wsV1Client, err := wsv1.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	r.wsV1Interface = wsV1Client
	b, err := controller.New("workflow-controller", mgr, controller.Options{
		Reconciler: r,
	})
	if err != nil {
		return err
	}
	return b.Watch(&source.Channel{Source: r.eventChan}, &handler.EnqueueRequestForObject{})
}

type request ctrl.Request

func (r request) isReleaseRequest() bool {
	return strings.HasPrefix(r.Name, ReleaseRequestPrefix)
}

func (r request) isCancelRequest() bool {
	return strings.HasPrefix(r.Name, CancelRequestPrefix)
}

func (r request) isPriorityRequest() bool {
	return strings.HasPrefix(r.Name, PriorityRequestPrefix)
}

func (r request) getWorkflowId() string {
	if strings.HasPrefix(r.Name, ReleaseRequestPrefix) {
		return r.Name[len(ReleaseRequestPrefix):]
	}
	if strings.HasPrefix(r.Name, CancelRequestPrefix) {
		return r.Name[len(CancelRequestPrefix):]
	}
	if strings.HasPrefix(r.Name, PriorityRequestPrefix) {
		return r.Name[len(PriorityRequestPrefix):]
	}
	return ""
}

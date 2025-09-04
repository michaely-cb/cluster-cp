package ws

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonutil "github.com/kubeflow/common/pkg/util"
)

type LeaseReconciler struct {
	log      logr.Logger
	client   client.Client
	recorder record.EventRecorder
	Disabled bool

	workflowEventChan chan event.GenericEvent
}

func NewLeaseReconciler(eventChan chan event.GenericEvent, disable bool) *LeaseReconciler {
	return &LeaseReconciler{workflowEventChan: eventChan, Disabled: disable}
}

// operator start time
var operatorStartTime = time.Now()
var startGracePeriod = time.Minute

func init() {
	duration, err := strconv.Atoi(os.Getenv("LEASE_OPERATOR_STARTUP_GRACE_PERIOD_MINUTES"))
	if err == nil && duration > 0 {
		startGracePeriod = time.Duration(duration) * time.Minute
	}
}

//+kubebuilder:rbac:groups="coordination.k8s.io",resources=leases,verbs=get;list;watch;create;update;patch;delete

// Reconcile processes create and update events of wsjob-owned leases. Delete events will be ignored.
// The reconciler assumes an existing wsjob and its corresponding lease.
// The cancel event CM will get created to trigger the cancel process, if the expiration of the lease is detected.
// If the lease is not expired, then the request will get re-evaluated right after the expected expiration time
func (r *LeaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// skip if override for test purpose
	if r.Disabled {
		return reconcile.Result{}, nil
	}
	logger := log.FromContext(ctx,
		"cycle", time.Now().UnixMilli(),
		"reconcileTrigger", req.NamespacedName.String())
	ctx = log.IntoContext(ctx, logger)
	lease := &coordv1.Lease{}
	err := r.client.Get(ctx, req.NamespacedName, lease)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// job lease cancel case, keep for legacy purpose
	// todo: remove at rel-2.7
	if _, ok := lease.Annotations[wsapisv1.CancelWithStatusKey]; ok {
		annotations := map[string]string{
			wsapisv1.CancelReasonKey:     lease.Annotations[wsapisv1.CancelReasonKey],
			wsapisv1.CancelWithStatusKey: lease.Annotations[wsapisv1.CancelWithStatusKey],
			wsapisv1.CancelJobContextKey: lease.Annotations[wsapisv1.CancelJobContextKey],
		}
		if annotations[wsapisv1.CancelReasonKey] == "" {
			annotations[wsapisv1.CancelReasonKey] = lease.Labels[wsapisv1.CancelReasonKey]
		}
		logger.Info("Lease cancel detected", "reason", annotations[wsapisv1.CancelReasonKey])
		return ctrl.Result{}, cancelJob(ctx, r.log, r.client, req.Namespace, req.Name, annotations, false)
	}

	// workflow reservation release case
	if lease.Annotations[commonv1.ResourceReservationStatus] == commonv1.ResourceReleasedValue &&
		lease.Labels[wsapisv1.WorkflowIdLabelKey] == lease.Name {
		r.workflowEventChan <- event.GenericEvent{
			Object: &corev1.Event{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ReleaseRequestPrefix + lease.Name,
					Namespace: lease.Namespace,
				},
			},
		}
	}

	// lease expiration case
	leaseDuration := time.Second * time.Duration(*lease.Spec.LeaseDurationSeconds)
	leaseExpirationTime := lease.Spec.RenewTime.Time.Add(leaseDuration)
	if startGracePeriod > 0 && lease.Spec.RenewTime.Time.Before(operatorStartTime) {
		// if operator restarts, give grace period in case k8s CP down causing renew failure
		// operator up means k8s CP is up
		logger.Info("Lease renewed before operator start time, give grace period",
			"lease", len(lease.Spec.String()),
			"operator-start-time", operatorStartTime.String())
		leaseExpirationTime = operatorStartTime.Add(startGracePeriod)
	}
	if leaseExpirationTime.Before(time.Now()) {
		logger.Info("Lease expiration detected",
			"lease", lease.Spec.String(),
			"labels", lease.Labels,
			"deadline", leaseExpirationTime.String())
		// workflow lease expired case
		// trigger workflow reconcile on job canceling
		if lease.Labels[wsapisv1.WorkflowIdLabelKey] == lease.Name {
			r.workflowEventChan <- event.GenericEvent{
				Object: &corev1.Event{
					ObjectMeta: metav1.ObjectMeta{
						Name:      CancelRequestPrefix + lease.Name,
						Namespace: lease.Namespace,
					},
				},
			}
			return ctrl.Result{}, nil
		}
		// job lease expired case
		return ctrl.Result{}, cancelJob(ctx, r.log, r.client, req.Namespace, req.Name,
			leaseExpireAnnotations(leaseDuration), false)
	}
	// keep requeue if still within deadline, adding one more second to avoid jittering
	requeueAfter := leaseExpirationTime.Add(time.Second).Sub(time.Now())
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func releaseJob(ctx context.Context, logger logr.Logger, c client.Client, wsjob *wsapisv1.WSJob) error {
	if !wsjob.IsReserved() {
		return nil
	}
	logger.Info("start annotating job to release reservation",
		"job", wsjob.NamespacedName())
	err := c.Patch(ctx, wsjob,
		client.RawPatch(types.MergePatchType,
			wscommon.GetAnnotationPatch(map[string]string{
				commonv1.ResourceReservationStatus: commonv1.ResourceReleasedValue,
			})))
	if err != nil {
		logger.Error(err, "failed to annotate wsjob to release", "wsjob", wsjob.Name)
	}
	return err
}

// annotate job to request cancel
// workflow lease expiration can force reservation release
func cancelJob(ctx context.Context, logger logr.Logger, c client.Client,
	namespace, name string, annotations map[string]string, releaseReservation bool) error {
	wsjob := &wsapisv1.WSJob{}
	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, wsjob)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// release reserving lock if already ended, triggered only by workflow lease expire
	if wsjob.IsReserved() && releaseReservation {
		logger.Info("force release lock for reserved job if still exists",
			"job", wsjob.NamespacedName())
		// clear redundant cancel context if job ended
		if commonutil.IsEnded(wsjob.Status) {
			annotations = make(map[string]string)
		}
		annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReleasedValue
	} else if commonutil.IsEnded(wsjob.Status) {
		return nil
	}

	logger.Info("start annotating job to cancel/release",
		"job", wsjob.NamespacedName(),
		"anno", annotations)
	err = c.Patch(ctx, wsjob,
		client.RawPatch(types.MergePatchType, wscommon.GetAnnotationPatch(annotations)))
	if err != nil {
		logger.Error(err, "failed to annotate wsjob to cancel", "wsjob", wsjob.Name)
	}
	return err
}

func leaseExpireAnnotations(leaseDuration time.Duration) map[string]string {
	return map[string]string{
		wsapisv1.CancelReasonKey:     wsapisv1.ExpiredLeaseCancelReason,
		wsapisv1.CancelWithStatusKey: wsapisv1.CancelWithStatusFailed,
		wsapisv1.CancelJobContextKey: fmt.Sprintf(
			"job failed due to client disconnected from cluster server for more than %s, "+
				"likely due to client side abrupt exit, please check client logs for more details",
			leaseDuration.String()),
	}
}

func (r *LeaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.log = mgr.GetLogger()
	r.client = mgr.GetClient()
	r.recorder = mgr.GetEventRecorderFor("lease-reconciler")
	ctr, err := controller.New("lease-reconciler", mgr, controller.Options{
		Reconciler: r,
	})
	if err != nil {
		return err
	}

	if err = ctr.Watch(
		source.Kind(mgr.GetCache(), &coordv1.Lease{}),
		&handler.EnqueueRequestForObject{},
		wscommon.CurrentNamespacePredicate(),
		predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				lease, _ := e.Object.(*coordv1.Lease)
				_, isClientLease := lease.Labels[commonv1.JobNameLabel]
				_, inWorkFlow := lease.Labels[wsapisv1.WorkflowIdLabelKey]
				return isClientLease || inWorkFlow
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				lease, _ := e.ObjectNew.(*coordv1.Lease)
				_, isClientLease := lease.Labels[commonv1.JobNameLabel]
				_, inWorkFlow := lease.Labels[wsapisv1.WorkflowIdLabelKey]
				return isClientLease || inWorkFlow
			},
			DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		}); err != nil {
		return err
	}
	return nil
}

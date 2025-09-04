package health

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
)

type SystemReconciler struct {
	reconciler
}

func NewSystemReconciler(r reconciler) *SystemReconciler {
	return &SystemReconciler{
		reconciler: r,
	}
}

func (s *SystemReconciler) SetupWithManager(mgr ctrl.Manager) error {
	s.recorder = mgr.GetEventRecorderFor("SystemReconciler")
	ctr, err := controller.New("SystemReconciler", mgr, controller.Options{
		Reconciler:              s,
		MaxConcurrentReconciles: 100,
	})
	if err != nil {
		return err
	}
	if err := ctr.Watch(source.Kind(mgr.GetCache(), &systemv1.System{}), &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}
	return nil
}

//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=systems,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=systems/status,verbs=get;update;patch

func (s *SystemReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	// skip if override for test purpose
	if s.Disabled {
		return reconcile.Result{}, nil
	}
	log := s.log.WithValues("request", request)
	currentSystem := &systemv1.System{}
	err := s.client.Get(ctx, request.NamespacedName, currentSystem)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithValues("system", request.Name).Info("system not found")
			return reconcile.Result{}, nil
		}
		log.WithValues("system", request.Name).Error(err, "could not get system")
		return reconcile.Result{}, err
	}
	specUpdated, statusUpdated, err := s.getUpdates(log, currentSystem)
	if err != nil {
		log.Error(err, "could not get system updates")
		return reconcile.Result{}, err
	}
	if statusUpdated != nil {
		if err := s.client.Status().Patch(ctx, statusUpdated, client.MergeFrom(currentSystem)); err != nil {
			log.Error(err, "could not patch system status")
			return reconcile.Result{}, err
		}
	}
	if specUpdated != nil {
		if err := s.client.Patch(ctx, specUpdated, client.MergeFrom(currentSystem)); err != nil {
			log.Error(err, "could not patch system")
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{RequeueAfter: syncInterval}, nil
}

func (s *SystemReconciler) getUpdates(log logr.Logger, currentSystem *systemv1.System) (*systemv1.System,
	*systemv1.System, error) {
	_, updatedConditions, err := getUpdatedConditions(s.AlertCache, currentSystem,
		currentSystem.Status.Conditions, log, s.recorder, s.client, currentSystem.Name)
	if err != nil {
		return nil, nil, err
	}

	var specUpdated, statusUpdated *systemv1.System
	version := s.MetricCache.GetSystemVersion(currentSystem.Name)
	if version != "" && currentSystem.Labels[wsapisv1.SystemVersionKey] != version {
		specUpdated = currentSystem.DeepCopy()
		specUpdated.Labels = common.EnsureMap(specUpdated.Labels)
		specUpdated.Labels[wsapisv1.SystemVersionKey] = version
		log.Info("system version updated",
			"system", currentSystem.Name,
			"old", currentSystem.Labels[wsapisv1.SystemVersionKey],
			"new", version)
	}
	statusUpdated = currentSystem.DeepCopy()
	statusUpdated.Status.Conditions = updatedConditions
	if equality.Semantic.DeepEqual(statusUpdated.Status, currentSystem.Status) {
		statusUpdated = nil
	}
	return specUpdated, statusUpdated, nil
}

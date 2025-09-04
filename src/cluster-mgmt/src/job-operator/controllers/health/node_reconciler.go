package health

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

type NodeReconciler struct {
	reconciler
}

func NewNodeReconciler(r reconciler) *NodeReconciler {
	return &NodeReconciler{
		reconciler: r,
	}
}

func (n *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	n.recorder = mgr.GetEventRecorderFor("NodeReconciler")
	ctr, err := controller.New("NodeReconciler", mgr, controller.Options{
		Reconciler:              n,
		MaxConcurrentReconciles: 500,
	})
	if err != nil {
		return err
	}
	if err := ctr.Watch(source.Kind(mgr.GetCache(), &corev1.Node{}), &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}
	return nil
}

func (n *NodeReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	// skip if override for test purpose
	if n.Disabled {
		return reconcile.Result{}, nil
	}
	log := n.log.WithValues("request", request)
	currentNode := &corev1.Node{}
	err := n.client.Get(ctx, request.NamespacedName, currentNode)
	if err != nil {
		log.Error(err, "could not get node")
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	updatedNode, err := n.getUpdates(log, currentNode)
	if err != nil {
		log.Error(err, "could not get node updates")
		return reconcile.Result{}, err
	}
	if updatedNode != nil {
		if err = n.client.Status().Patch(ctx, updatedNode, client.MergeFrom(currentNode)); err != nil {
			log.Error(err, "could not patch node")
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{RequeueAfter: syncInterval}, nil
}

func (n *NodeReconciler) getUpdates(log logr.Logger, currentNode *corev1.Node) (*corev1.Node, error) {
	alertFilters := []string{currentNode.Name}
	if !wsapisv1.IsV2Network && wscommon.EnableSwitchAlertDetection && currentNode.Labels[wscommon.GroupLabelKey] != "" {
		alertFilters = append(alertFilters, currentNode.Labels[wscommon.GroupLabelKey])
	}
	_, updatedConditions, err := getUpdatedConditions(n.AlertCache, currentNode,
		currentNode.Status.Conditions, log, n.recorder, n.client, alertFilters...)
	if err != nil {
		return nil, err
	}

	updatedNode := currentNode.DeepCopy()
	updatedNode.Status.Conditions = updatedConditions
	if equality.Semantic.DeepEqual(updatedNode, currentNode) {
		return nil, nil
	}
	return updatedNode, nil
}

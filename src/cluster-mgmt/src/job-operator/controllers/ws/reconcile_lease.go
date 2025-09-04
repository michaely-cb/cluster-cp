package ws

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	coordv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

// createLease if not exist.
// We assume there should be no legitimate reason for lease delete events.
func (r *WSJobReconciler) createLease(ctx context.Context, req ctrl.Request, wsjob *wsapisv1.WSJob) error {
	if wsjob.Labels == nil {
		return nil
	}
	if _, ok := wsjob.Labels[wsapisv1.ClientLeaseDisabledLabelKey]; ok {
		return nil
	}
	lease := &coordv1.Lease{}
	err := r.Client.Get(ctx, req.NamespacedName, lease)
	if err == nil {
		return nil
	}

	now := &metav1.MicroTime{Time: time.Now()}
	leaseDurationSeconds := int32(wscommon.ClientLeaseDuration.Seconds())
	holderIdentity := fmt.Sprintf("%s/%s", wsjob.Namespace, wsjob.Name)
	labels := r.GenLabels(wsjob.GetName())
	labels[wsapisv1.WorkflowIdLabelKey] = wsjob.GetWorkflowId()
	lease = &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: req.Namespace,
			Name:      req.Name,
			Labels:    labels,
		},
		Spec: coordv1.LeaseSpec{
			AcquireTime:          now,
			RenewTime:            now,
			HolderIdentity:       &holderIdentity,
			LeaseDurationSeconds: &leaseDurationSeconds,
		},
	}
	err = controllerutil.SetControllerReference(wsjob, lease, r.Scheme)
	if err != nil {
		return fmt.Errorf("set controller reference error: %w", err)
	}
	err = r.Client.Create(ctx, lease)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("lease creation error: %w", err)
	}
	logrus.Infof("lease has been created for %s (%s)", req.Name, req.NamespacedName)
	return nil
}

// cleanupLease only runs when the job is ended
func (r *WSJobReconciler) cleanupLease(ctx context.Context, namespace, name string) error {
	lease := &coordv1.Lease{}
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, lease)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get lease: %w", err)
	}
	err = r.KubeClientSet.CoordinationV1().Leases(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to cleanup lease: %w", err)
	}
	// seeing this message the first time for a job indicates that beyond this point in time
	// no more activities will be seen from this job
	r.Log.Info("wsjob cleanupLease succeeded", "job", name)
	return nil
}

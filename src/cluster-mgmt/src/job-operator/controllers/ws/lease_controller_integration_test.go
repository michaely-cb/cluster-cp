//go:build integration && lease_controller

package ws

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

func TestController_Scenarios(t *testing.T) {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	eventCh := make(chan event.GenericEvent, 1024)

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	reconciler := NewLeaseReconciler(eventCh, false)
	err := reconciler.SetupWithManager(mgr)
	require.NoError(t, err)

	t.Run("lease_expiration", func(t *testing.T) {
		jobName := "wsjob-1"
		job := newWsExecuteJob(wscommon.Namespace, jobName, 1)
		require.NoError(t, k8sClient.Create(ctx, job))
		defer func() { require.NoError(t, k8sClient.Delete(context.Background(), job)) }()

		// Creates a lease and asserts validity
		leaseDurationSeconds := int32(10)
		require.NoError(t, k8sClient.Create(ctx, newLeaseBuilder(jobName, "", leaseDurationSeconds)))
		assertValidLease(t, jobName)
		assertWsJobAnnotatedNotCanceled(t, job.Namespace, jobName)
		assertWsJobLabeledNotCanceled(t, job.Namespace, jobName)

		// Sleeps for less than lease duration
		time.Sleep(time.Second * time.Duration(1))
		assertValidLease(t, jobName)
		assertWsJobAnnotatedNotCanceled(t, job.Namespace, jobName)
		assertWsJobLabeledNotCanceled(t, job.Namespace, jobName)

		// Sleeps for longer than lease duration
		time.Sleep(time.Second * time.Duration(leaseDurationSeconds))
		assertExpiredLease(t, jobName)
		assertWsJobAnnotatedExpired(t, job.Namespace, jobName)
		assertWsJobLabeledExpired(t, job.Namespace, jobName)
	})

	t.Run("lease_renewal", func(t *testing.T) {
		jobName := "wsjob-2"
		job := newWsExecuteJob(wscommon.Namespace, jobName, 1)
		require.NoError(t, k8sClient.Create(ctx, job))
		defer func() { require.NoError(t, k8sClient.Delete(context.Background(), job)) }()

		// Creates a lease and asserts validity
		leaseDurationSeconds := int32(5)
		require.NoError(t, k8sClient.Create(ctx, newLeaseBuilder(jobName, "", leaseDurationSeconds)))

		sleepIntervalSeconds := int32(1)
		sleepTotalSeconds := int32(0)

		// By the time this loop finishes, we have slept for longer than the lease duration time
		// We expect the lease to remain valid since the lease was extended.
		for sleepTotalSeconds < leaseDurationSeconds {
			// Sleeps for less than lease duration and update
			time.Sleep(time.Second * time.Duration(sleepIntervalSeconds))
			l := assertValidLease(t, jobName)
			assertWsJobAnnotatedNotCanceled(t, job.Namespace, jobName)
			assertWsJobLabeledNotCanceled(t, job.Namespace, jobName)

			patch := client.MergeFrom(l.DeepCopy())
			l.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
			require.NoError(t, k8sClient.Patch(ctx, l, patch))

			sleepTotalSeconds += sleepIntervalSeconds
		}
	})

	t.Run("lease_canceled_by_client", func(t *testing.T) {
		jobName := "wsjob-3"
		job := newWsExecuteJob(wscommon.Namespace, jobName, 1)
		require.NoError(t, k8sClient.Create(ctx, job))
		defer func() { require.NoError(t, k8sClient.Delete(context.Background(), job)) }()

		// Creates a lease and asserts validity
		leaseDurationSeconds := int32(5)
		require.NoError(t, k8sClient.Create(ctx, newLeaseBuilder(jobName, "", leaseDurationSeconds)))
		l := assertValidLease(t, jobName)
		assertWsJobAnnotatedNotCanceled(t, job.Namespace, jobName)
		assertWsJobLabeledNotCanceled(t, job.Namespace, jobName)

		// Cancels the lease by annotating
		wscommon.AnnotateCanceledLease(l,
			wsapisv1.CancelWithStatusCancelled,
			wsapisv1.CanceledByClientReason,
			"canceled by client")
		require.NoError(t, k8sClient.Update(ctx, l))

		// Asserts canceled lease and canceled job
		assertWsJobLabeledCanceled(t, job.Namespace, jobName)
		assertWsJobAnnotatedCanceled(t, job.Namespace, jobName)
	})
}

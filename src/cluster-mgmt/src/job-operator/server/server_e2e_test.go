//go:build e2e

package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wscommon "cerebras.com/job-operator/common"
)

const (
	timeout  = 60 * time.Second
	interval = 1 * time.Second
)

func TestServerE2E(t *testing.T) {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(true, t)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()
	k8sClient := mgr.GetClient()
	clientSet, err := kubernetes.NewForConfig(mgr.GetConfig())
	require.NoError(t, err)

	t.Run("test endpointslice pod ip updates", func(t *testing.T) {
		jobOperatorPod := jobOperatorPodReady(t, clientSet, ctx, time.Now())
		originalIp := jobOperatorPod.Status.PodIP

		assertEndpointReconciledEventually(t, originalIp, k8sClient, ctx)

		require.NoError(t, k8sClient.Delete(ctx, jobOperatorPod))

		jobOperatorPod = jobOperatorPodReady(t, clientSet, ctx, time.Now())
		newIp := jobOperatorPod.Status.PodIP
		assert.NotEqual(t, originalIp, newIp)

		assertEndpointReconciledEventually(t, newIp, k8sClient, ctx)
	})
}

func jobOperatorPodReady(t *testing.T, clientSet *kubernetes.Clientset, ctx context.Context, afterTime time.Time) *corev1.Pod {
	jobOperatorPod := &corev1.Pod{}
	require.Eventually(t, func() bool {
		pods, err := clientSet.CoreV1().Pods(wscommon.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "control-plane=controller-manager",
		})
		assert.NoError(t, err)

		lease, err := clientSet.CoordinationV1().Leases(wscommon.Namespace).Get(ctx, wscommon.LeaseName, metav1.GetOptions{})
		assert.NoError(t, err)

		// Ensure that the lease has been updated at least once before checking its status
		if !lease.Spec.RenewTime.Time.After(afterTime) {
			return false
		}

		holder := lease.Spec.HolderIdentity
		podName := strings.Split(*holder, "_")[0]

		for _, pod := range pods.Items {
			if pod.Name == podName {
				jobOperatorPod = &pod
				break
			}
		}

		for _, cond := range jobOperatorPod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				return true
			}
		}
		return false
	}, timeout, interval)
	return jobOperatorPod
}

func assertEndpointReconciledEventually(t *testing.T, expectedIp string, k8sClient client.Client, ctx context.Context) {
	require.Eventually(t, func() bool {
		endpointSlice := &discoveryv1.EndpointSlice{}
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{
			Name:      wscommon.GRPCServerEndpointSliceName,
			Namespace: wscommon.Namespace,
		}, endpointSlice))

		if len(endpointSlice.Endpoints) != 1 || len(endpointSlice.Endpoints[0].Addresses) != 1 {
			return false
		}

		return expectedIp == endpointSlice.Endpoints[0].Addresses[0]
	}, timeout, interval)
}

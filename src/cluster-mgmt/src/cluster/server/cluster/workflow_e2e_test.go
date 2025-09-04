//go:build e2e

// Copyright 2025 Cerebras Systems, Inc.

package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	rlCliv1 "cerebras.com/job-operator/client-resourcelock/clientset/versioned/typed/resourcelock/v1"
	"cerebras.com/job-operator/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	"cerebras.com/cluster/server/pkg/wsclient"
)

func TestWorkflow(t *testing.T) {
	mgr, testEnv, ctx, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()
	assertClusterServerIngressSetup(t, common.Namespace)
	defer assertClusterServerIngressCleanup(t, common.Namespace)

	rlCli, _ := rlCliv1.NewForConfig(mgr.GetConfig())
	lockCli = rlCli.ResourceLocks(common.Namespace)
	wsjobClient := getWsJobClient(t, mgr)
	ctx, cleanup, _, clusterClient := setup(false, wsjobClient.Clientset, wsjobClient, nil, false)
	defer cleanup()

	for i, testcase := range []struct {
		name        string
		autoReserve bool
	}{
		{
			name:        "test workflow default reserve/release",
			autoReserve: true,
		},
		{
			name: "test job level reserve/release",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			wRes, err := clusterClient.InitWorkflow(ctx, &pb.InitWorkflowRequest{
				ResourceReserve:              testcase.autoReserve,
				LeaseDurationSecondsOverride: 300,
			})
			require.NoError(t, err)
			workflowID := wRes.WorkflowId

			jobName := fmt.Sprintf("wf-reserve-%d", i)
			specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
			for i := range specs {
				specs[i] = wsclient.ServiceSpec{
					Image:    common.DefaultImage,
					Command:  []string{"sleep", "300"},
					Replicas: 1,
				}
			}
			jobSpec := wsclient.JobSpec{
				Namespace:      common.Namespace,
				JobName:        jobName,
				WsJobMode:      wsclient.ExecuteJobMode,
				CleanPodPolicy: commonv1.CleanPodPolicyRunning,
				NumWafers:      2,
				Labels: map[string]string{
					wsapisv1.WorkflowIdLabelKey:          workflowID,
					wsapisv1.ClientLeaseDisabledLabelKey: "",
					wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				},
			}
			applyVolumesAndMounts(&jobSpec, true)
			job, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
			require.NoError(t, err)
			_, err = wsjobClient.WsV1Client.Create(ctx, job, metav1.CreateOptions{})
			require.NoError(t, err)

			// wait till job running
			assertWsJobRunningEventually(t, clusterClient, jobName)

			if !testcase.autoReserve {
				// call reserving
				_, err = clusterClient.ReserveJobResources(ctx, &pb.ReserveJobResourcesRequest{
					JobId: job.Name,
				})
				require.NoError(t, err)
			}

			// expect reservation created
			require.Eventually(t, func() bool {
				lock := &rlv1.ResourceLock{}
				err = k8sClient.Get(ctx, client.ObjectKey{Namespace: common.SystemNamespace,
					Name: common.ReservingLockName(jobName)}, lock)
				return err == nil
			}, 30*time.Second, 1*time.Second)

			// cancel job to mock job done
			_, err = clusterClient.CancelJobV2(ctx, &pb.CancelJobRequest{JobId: jobName,
				JobStatus: pb.JobStatus_JOB_STATUS_CANCELLED})
			require.NoError(t, err)

			// query reservation status
			res, err := clusterClient.GetReservationStatus(ctx, &pb.GetReservationStatusRequest{
				JobId: job.Name,
			})
			require.NoError(t, err)
			require.Equal(t, int32(2), res.SystemCount)

			// call releasing
			if !testcase.autoReserve {
				_, err = clusterClient.ReleaseJobResources(ctx, &pb.ReleaseJobResourcesRequest{
					JobId: job.Name,
				})
				require.NoError(t, err)
			} else {
				_, err = clusterClient.ReleaseWorkflowResources(ctx, &pb.ReleaseWorkflowResourcesRequest{
					WorkflowId: workflowID,
				})
				require.NoError(t, err)
			}

			// expect reservation released
			require.Eventually(t, func() bool {
				lock := &rlv1.ResourceLock{}
				err := k8sClient.Get(ctx, client.ObjectKey{Namespace: common.SystemNamespace,
					Name: common.ReservingLockName(jobName)}, lock)
				return err != nil && k8serrors.IsNotFound(err)
			}, 30*time.Second, 1*time.Second)
			require.NoError(t, wsjobClient.Delete(common.Namespace, job.Name))
		})
	}
}

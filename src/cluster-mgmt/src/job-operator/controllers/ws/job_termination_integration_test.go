//go:build integration && job_termination

package ws

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonctrlv1 "github.com/kubeflow/common/pkg/controller.v1/common"
	commonutil "github.com/kubeflow/common/pkg/util"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

func (t *TestSuite) TestPodFailureHandling() {
	ctx := context.Background()

	t.createNsIfNotExists(wscommon.DefaultNamespace)
	t.assignEverythingToNS(wscommon.DefaultNamespace, systemCount)

	require.Eventually(t.T(), func() bool {
		t.T().Log(t.CfgMgr.Cfg.GetNsSysNameMap())
		return len(t.CfgMgr.Cfg.GetAssignedSystemsList(wscommon.DefaultNamespace)) == systemCount
	}, eventuallyTimeout, eventuallyTick)
	t.Run("test_pending_pod_failure_at_init_container_waiting", func() {
		job := newWsExecuteJob(wscommon.Namespace, "init-container-waiting-wsjob", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)
		podList := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 10)

		for i := range podList.Items {
			pod := podList.Items[i]
			pod.Status.Phase = corev1.PodPending
			pod.Status.StartTime = &metav1.Time{
				Time: time.Now().Add(-commonctrlv1.GetGracePeriodByJobSize(wscommon.PendingPodGracePeriod,
					10) + 10*time.Second),
			}
			if pod.ObjectMeta.Labels[wsReplicaTypeLabel] == string(wscommon.RoleCoordinator) {
				// A failed mount on the pod would have lead to this waiting state for init containers.
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{
						Name: wsapisv1.DefaultInitContainerName,
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "PodInitializing",
							},
						},
					},
				}
			} else {
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{
						Name: wsapisv1.DefaultInitContainerName,
						State: corev1.ContainerState{
							Running: &corev1.ContainerStateRunning{
								StartedAt: metav1.Now(),
							},
						},
					},
				}
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}
		time.Sleep(time.Second)
		assertWsJobScheduledEventually(t.T(), job.Namespace, job.Name)
		assertCoordinatorPodFailure(t.T(), job)
	})

	for _, testcase := range []struct {
		kubeApiQps int
	}{
		{20},
		{1000},
	} {
		t.Run(
			fmt.Sprintf(
				"test pending pod requeue event does not block other pod/svc creation with qps %d",
				testcase.kubeApiQps),
			func() {
				t.JobReconciler.JobController.Config.KubeApiQPS = testcase.kubeApiQps
				defer func() {
					t.JobReconciler.JobController.Config.KubeApiQPS = wscommon.KubeApiQPS
				}()
				job := newWsExecuteJob(wscommon.Namespace, "init-event-non-blocking", 1)
				require.NoError(t.T(), k8sClient.Create(ctx, job))
				defer assertWsjobCleanedUp(t.T(), job)

				podList := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 10)
				// set first pods init pending,
				for i := range podList.Items {
					pod := podList.Items[i]
					if i == 0 {
						pod.Status.Phase = corev1.PodPending
						pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
							{
								Name: wsapisv1.DefaultInitContainerName,
								State: corev1.ContainerState{
									Waiting: &corev1.ContainerStateWaiting{},
								},
							},
						}
						require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
					} else {
						// delete the rest of the pods to simulate initial creation state
						require.NoError(t.T(), k8sClient.Delete(ctx, &pod))
					}
				}
				// asserts the init event will not block other resources creation
				assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 10)
			})
	}

	t.Run("test_pending_pod_failure_at_init_container_running_after_deadline", func() {
		commonctrlv1.MediumJobPodCount = 2
		commonctrlv1.LargeJobPodCount = 5
		defer func() {
			commonctrlv1.MediumJobPodCount = 2500
			commonctrlv1.LargeJobPodCount = 5000
		}()
		job := newWsExecuteJob(wscommon.Namespace, "init-container-running-wsjob", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		podList := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 10)
		// fake the init status
		job = assertWsJobScheduledEventually(t.T(), job.Namespace, job.Name)
		job.Status.Conditions = append(job.Status.Conditions, commonv1.JobCondition{
			Type:   commonv1.JobInitializing,
			Status: corev1.ConditionTrue,
			LastTransitionTime: metav1.Time{
				Time: time.Now().Add(-commonctrlv1.GetGracePeriodByJobSize(wscommon.PendingPodGracePeriod,
					10) + 10*time.Second),
			},
			LastUpdateTime: metav1.Now(),
		})
		require.NoError(t.T(), k8sClient.Status().Update(ctx, job))
		podList = assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 10)
		// set one pod main container running + rest of pods init container running
		for i := range podList.Items {
			pod := podList.Items[i]
			pod.Status.StartTime = &metav1.Time{
				Time: time.Now(),
			}
			if pod.Name == "init-container-running-wsjob-coordinator-0" {
				pod.Status.Phase = corev1.PodRunning
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: wsapisv1.DefaultContainerName,
						State: corev1.ContainerState{
							Running: &corev1.ContainerStateRunning{},
						},
					},
				}
			} else {
				pod.Status.Phase = corev1.PodPending
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{
						Name: wsapisv1.DefaultInitContainerName,
						State: corev1.ContainerState{
							Running: &corev1.ContainerStateRunning{},
						},
					},
				}
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}
		job = assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
		assert.Contains(t.T(), job.Status.Conditions[len(job.Status.Conditions)-1].Message,
			"failed due to stuck at init phase")
		assert.Contains(t.T(), job.Status.Conditions[len(job.Status.Conditions)-1].Message,
			"logs of one init pod")
		assert.Equal(t.T(), "init-container-running-wsjob-worker-0",
			job.Status.ReplicaStatuses[wsapisv1.WSReplicaTypeWorker].InitPods[0])
	})

	t.Run("test_pending_pod_failure_at_main_container_stage_due_to_unrecoverable_errors", func() {
		job := newWsExecuteJob(wscommon.Namespace, "main-container-invalid-image-wsjob", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)
		podList := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 10)

		for i := range podList.Items {
			pod := podList.Items[i]
			assert.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(&pod), &pod))
			pod.Status.Phase = corev1.PodPending
			// Pod start time is less irrelevant to this test case, since
			// there is no grace period for the unrecoverable errors.
			pod.Status.StartTime = &metav1.Time{
				Time: time.Now(),
			}
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
				{
					Name: wsapisv1.DefaultInitContainerName,
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							FinishedAt: metav1.Now(),
						},
					},
				},
			}
			if pod.ObjectMeta.Labels[wsReplicaTypeLabel] == string(wscommon.RoleCoordinator) {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: wsapisv1.DefaultContainerName,
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "InvalidImageName",
							},
						},
					},
				}
			} else {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: wsapisv1.DefaultContainerName,
						State: corev1.ContainerState{
							Running: &corev1.ContainerStateRunning{
								StartedAt: metav1.Now(),
							},
						},
					},
				}
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}
		assertCoordinatorPodFailure(t.T(), job)
	})

	t.Run("test_pending_pod_failure_at_main_container_stage_due_to_transient_errors", func() {
		job := newWsExecuteJob(wscommon.Namespace, "main-container-image-backoff-wsjob", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)
		podList := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 10)

		for i := range podList.Items {
			pod := podList.Items[i]
			pod.Status.Phase = corev1.PodPending
			pod.Status.StartTime = &metav1.Time{
				Time: time.Now().Add(-15 * time.Minute),
			}
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
				{
					Name: wsapisv1.DefaultInitContainerName,
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							FinishedAt: metav1.NewTime(time.Now().Add(-commonctrlv1.GetGracePeriodByJobSize(
								wscommon.PendingPodGracePeriod, 10) + 10*time.Second)),
						},
					},
				},
			}
			if pod.ObjectMeta.Labels[wsReplicaTypeLabel] == string(wscommon.RoleCoordinator) {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: wsapisv1.DefaultContainerName,
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "ImagePullBackOff",
							},
						},
					},
				}
			} else {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: wsapisv1.DefaultContainerName,
						State: corev1.ContainerState{
							Running: &corev1.ContainerStateRunning{
								StartedAt: metav1.Now(),
							},
						},
					},
				}
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}
		time.Sleep(time.Second)
		assertWsJobScheduledEventually(t.T(), job.Namespace, job.Name)
		assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
		assertCoordinatorPodFailure(t.T(), job)
	})

	t.Run("test_running_pod_failure_at_main_container_stage_with_multi_containers", func() {
		job := newWsExecuteJob(wscommon.Namespace, "streamer-nonzero-exit-wsjob", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)
		podList := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 10)

		defaultContainerStatus := corev1.ContainerStatus{
			Name: wsapisv1.DefaultContainerName,
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.Now(),
				},
			},
		}
		runningStreamerStatus := corev1.ContainerStatus{
			Name: wsapisv1.UserSidecarContainerName,
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.Now(),
				},
			},
		}
		terminatedStreamerStatus := corev1.ContainerStatus{
			Name: wsapisv1.UserSidecarContainerName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1,
				},
			},
		}
		for i := range podList.Items {
			pod := podList.Items[i]
			pod.Status.Phase = corev1.PodRunning
			pod.Status.StartTime = &metav1.Time{
				Time: time.Now().Add(-15 * time.Minute),
			}
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
				{
					Name: wsapisv1.DefaultInitContainerName,
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							FinishedAt: metav1.NewTime(time.Now().Add(-commonctrlv1.GetGracePeriodByJobSize(
								wscommon.PendingPodGracePeriod, 10) + 10*time.Second)),
						},
					},
				},
			}
			if pod.ObjectMeta.Labels[wsReplicaTypeLabel] == string(wscommon.RoleWorker) {
				if pod.ObjectMeta.Labels[wsReplicaIndexLabel] == "0" {
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{defaultContainerStatus, terminatedStreamerStatus}
				} else {
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{defaultContainerStatus, runningStreamerStatus}
				}
			} else {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{defaultContainerStatus}
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}
		assertWorkerPodFailre(t.T(), job)
		assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
	})

	t.Run("job_stays_alive_during_pod_failure_grace_period", func() {
		wsJob := newWsExecuteJob(wscommon.Namespace, "failing-grace-period", 1)
		wsJob.Annotations = wscommon.EnsureMap(wsJob.Annotations)
		wsJob.Annotations[wscommon.FailingJobGracePeriodLabel] = "10"

		// Create and start job
		require.NoError(t.T(), k8sClient.Create(ctx, wsJob))
		defer assertWsjobCleanedUp(t.T(), wsJob)

		// Get pods and fail one
		pods := assertPodCountEventually(t.T(), wsJob.Namespace, wsJob.Name, "", 10)

		// Fail one of the pods
		pod := pods.Items[0]
		expectedPodName := pod.Name
		pod.Status.Phase = corev1.PodFailed
		pod.Status.Message = "I've been a bad pod"
		require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))

		// Verify job is not failed immediately (wait upto a second before the grace period expires)
		require.Never(t.T(), func() bool {
			require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(wsJob), wsJob))
			return commonutil.IsEnded(wsJob.Status)
		}, wscommon.FailingJobGracePeriod-1*time.Second, 100*time.Millisecond)
		require.True(t.T(), commonutil.IsFailing(wsJob.Status))
		require.Contains(t.T(), wsJob.Status.Conditions[len(wsJob.Status.Conditions)-1].Message,
			"job failing with pod failure detected, starting grace period")
		require.Contains(t.T(), wsJob.Status.Conditions[len(wsJob.Status.Conditions)-1].Message, expectedPodName)

		// Verify job fails after grace period and contains error pod
		wsJob = assertWsJobFailEventually(t.T(), wsJob.Namespace, wsJob.Name)
		require.Contains(t.T(), wsJob.Status.Conditions[len(wsJob.Status.Conditions)-1].Message, expectedPodName)
	})

	t.Run("pod_eviction_case", func() {
		wsJob := newWsExecuteJob(wscommon.Namespace, "pod-eviction", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, wsJob))
		defer assertWsjobCleanedUp(t.T(), wsJob)
		pods := assertPodCountEventually(t.T(), wsJob.Namespace, wsJob.Name, "", 10)

		event0 := &corev1.Event{
			Type:              "Warning",
			Reason:            "Evicted",
			Message:           "The node had condition: [DiskPressure]. ",
			ReportingInstance: "node-x",
			InvolvedObject: corev1.ObjectReference{
				Name:      pods.Items[0].Name,
				Namespace: pods.Items[0].Namespace,
			},
			LastTimestamp: metav1.Now(),
			ObjectMeta: metav1.ObjectMeta{
				Name:      pods.Items[0].Name + "-event",
				Namespace: pods.Items[0].Namespace,
			},
		}
		require.NoError(t.T(), k8sClient.Create(ctx, event0))
		defer func() {
			require.NoError(t.T(), k8sClient.Delete(ctx, event0))
		}()
		pods.Items[0].Status.Phase = corev1.PodRunning
		require.NoError(t.T(), k8sClient.Status().Update(ctx, &pods.Items[0]))
		assertWsJobRunningEventually(t.T(), wsJob.Namespace, wsJob.Name)

		cancelJobEventually(t.T(), wsJob, wsapisv1.CancelWithStatusFailed)
		wsJob = assertWsJobFailEventually(t.T(), wsJob.Namespace, wsJob.Name)
		require.Contains(t.T(), wsJob.Status.Conditions[len(wsJob.Status.Conditions)-1].Message,
			fmt.Sprintf("job terminated by client with status: Failed, "+
				"cluster side failure detected: %s evicted by node-x: "+
				"The node had condition: [DiskPressure]", pods.Items[0].Name))
	})

	t.Run("test_per_container_grace_period_two_containers_timeout", func() {
		/*
		 * Scenario
		 *
		 * - single compile-job → exactly ONE pod (coordinator).
		 * - pod has TWO regular containers:
		 *     - container[0] pulled and started successfully.
		 *     - container[1] is still **Waiting** with ImagePullBackOff.
		 * *Per-container* grace period is 5 min (or the cluster-scaled value).
		 *
		 * We pre-date pod/containers so that:
		 *   now-2*grace-5s  <-- container[0] StartedAt
		 *   now-2*grace-5s + grace  == start of container[1] window
		 *   now               > start + grace      → container[1] timed-out
		 *
		 * Result: controller marks the job Failed and the failure message
		 *         references container[1] (“extra”) as the culprit.
		 */

		// 1.  Build the job – compile job with TWO containers on the coordinator

		job := newWsCompileJob(wscommon.Namespace, "per-container-timeout", 1)

		coordSpec := job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator]
		// duplicate the default container to get a 2-container pod
		extra := coordSpec.Template.Spec.Containers[0].DeepCopy()
		extra.Name = "extra"
		coordSpec.Template.Spec.Containers = append(coordSpec.Template.Spec.Containers, *extra)

		// (optional) strip resource req/limits so env-test scheduler never complains
		clearResourceLimits(coordSpec)

		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		// we know compile-job ⇒ exactly one pod
		pods := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 1)
		job = assertWsJobScheduledEventually(t.T(), job.Namespace, job.Name)

		// 2.  Work-out the grace period the controller will apply
		gp := commonctrlv1.GetGracePeriodByJobSize(wscommon.PendingPodGracePeriod, 1)

		// For ImagePullBackOff, timeout starts from lastContainerStartTime
		// So we need container[0] to start far enough in the past that
		// container[1]'s ImagePullBackOff timeout (from container[0]'s start) expires
		container0Started := time.Now().Add(-gp - 5*time.Second) // container[1] will timeout from this time

		// Tell the job it is still in the Initialising phase (needed so the
		// controller keeps checking the pod in Pending)
		job.Status.Conditions = append(job.Status.Conditions, commonv1.JobCondition{
			Type:               commonv1.JobInitializing,
			Status:             corev1.ConditionTrue,
			LastUpdateTime:     metav1.Now(),
			LastTransitionTime: metav1.Time{Time: container0Started},
		})
		require.NoError(t.T(), k8sClient.Status().Update(ctx, job))

		// 3.  Fake pod status so that container[0] succeeded & container[1] waits

		pod := pods.Items[0]

		pod.Status.Phase = corev1.PodPending
		pod.Status.StartTime = &metav1.Time{Time: container0Started}
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
			Name: wsapisv1.DefaultInitContainerName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					FinishedAt: metav1.Time{Time: container0Started}, // init finished long ago
				},
			},
		}}

		pod.Status.ContainerStatuses = []corev1.ContainerStatus{
			{
				Name: coordSpec.Template.Spec.Containers[0].Name,
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{
						StartedAt: metav1.Time{Time: container0Started}, // This becomes lastContainerStartTime
					},
				},
				Ready: true,
			},
			{
				Name: extra.Name,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: "ImagePullBackOff", // Uses lastContainerStartTime for timeout
					},
				},
			},
		}

		require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))

		// 4.  Expect job to FAIL due to container[1] timeout
		job = assertWsJobFailEventually(t.T(), job.Namespace, job.Name)

		coordStatus, ok := job.Status.ReplicaStatuses[wsapisv1.WSReplicaTypeCoordinator]
		t.T().Logf("Coordinator status: %+v", coordStatus.FailedPodStatuses)
		for i, failedPod := range coordStatus.FailedPodStatuses {
			t.T().Logf("FailedPodStatuses[%d]: Name=%s, Container=%s, Message=%s", i, failedPod.Name, failedPod.Container, failedPod.Message)
		}
		require.True(t.T(), ok, "coordinator status must exist")
		require.Len(t.T(), coordStatus.FailedPodStatuses, 1,
			"exactly one pod should be marked failed")

		failed := coordStatus.FailedPodStatuses[0]
		assert.Equal(t.T(), pod.Name, failed.Name,
			"failed entry must point at coordinator-0 pod")
		assert.Contains(t.T(), failed.Message, extra.Name,
			"failed entry must point at *extra* (2nd) container")
		assert.Contains(t.T(), strings.ToLower(failed.Message), "timeout",
			"failure reason must mention timeout")
	})

	t.Run("test_per_container_grace_period_coord_container3_timeout", func() {
		/*
		 * Scenario
		 *
		 * - TWO pods in total
		 *     • coordinator-0  → 3 containers  (default, extra-1, extra-2)
		 *     • worker-0       → 2 containers  (default, sidecar)
		 * - All containers succeed **except** coordinator's extra-2
		 *   which stays “Waiting → ImagePullBackOff”.
		 * - We pre-date the Initialising condition so that only
		 *   container[2] is beyond its per-container deadline.
		 * Expectation: controller marks job Failed; only one
		 *   FailedPodStatus entry that points at coordinator-0 / extra-2.
		 */

		// 1. Build the job … coordinator + *one* worker

		job := newWsCompileJob(wscommon.Namespace, "coord-container3-timeout", 1)

		// add a single worker replica-spec
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker] =
			wsjobContainerSpec(wscommon.Namespace, wsapisv1.WSReplicaTypeWorker, 1)
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker].Replicas = int32Ptr(1)

		//  coordinator → 3 containers
		coord := job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator]
		c1 := coord.Template.Spec.Containers[0].DeepCopy()
		c1.Name = "extra-1"
		c2 := coord.Template.Spec.Containers[0].DeepCopy()
		c2.Name = "extra-2" // this one will time-out
		coord.Template.Spec.Containers = append(coord.Template.Spec.Containers, *c1, *c2)

		//  worker → 2 containers
		worker := job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker]
		if len(worker.Template.Spec.Containers) == 1 {
			wSide := worker.Template.Spec.Containers[0].DeepCopy()
			wSide.Name = "sidecar"
			worker.Template.Spec.Containers = append(worker.Template.Spec.Containers, *wSide)
		}

		// make scheduler happy in env-test
		clearResourceLimits(coord)
		clearResourceLimits(worker)

		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		// 2. Wait for the two pods & mark job Initializing
		assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 2)
		job = assertWsJobScheduledEventually(t.T(), job.Namespace, job.Name)

		grace := commonctrlv1.GetGracePeriodByJobSize(wscommon.PendingPodGracePeriod, 2)

		// extra-2 (ImagePullBackOff) times out from latest container start time
		// We need extra-1 to start far enough in the past that extra-2 times out
		startTS := time.Now().Add(-grace - 10*time.Second) // Base time - extra-1 will start from this time

		job.Status.Conditions = append(job.Status.Conditions, commonv1.JobCondition{
			Type:               commonv1.JobInitializing,
			Status:             corev1.ConditionTrue,
			LastUpdateTime:     metav1.Now(),
			LastTransitionTime: metav1.Time{Time: startTS},
		})
		require.NoError(t.T(), k8sClient.Status().Update(ctx, job))

		// 3. Fake pod statuses

		podList := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 2)
		for i := range podList.Items {
			p := &podList.Items[i]
			p.Status.Phase = corev1.PodPending
			p.Status.StartTime = &metav1.Time{Time: startTS}
			p.Status.InitContainerStatuses = []corev1.ContainerStatus{{
				Name: wsapisv1.DefaultInitContainerName,
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						FinishedAt: metav1.Time{Time: startTS},
					},
				},
			}}

			switch p.Labels[wsReplicaTypeLabel] {
			case wsapisv1.WSReplicaTypeCoordinator.Lower():
				// container 0 and 1 succeed; container 2 waits
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name:  coord.Template.Spec.Containers[0].Name,
						Ready: true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Time{Time: startTS}}},
					},
					{
						Name:  "extra-1",
						Ready: true,
						// This becomes lastContainerStartTime, so extra-2 times out from startTS
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Time{Time: startTS}}},
					},
					{
						Name: "extra-2", // Will timeout because lastContainerStartTime is old enough
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"},
						},
					},
				}
			default: // worker
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name:  worker.Template.Spec.Containers[0].Name,
						Ready: true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
					},
					{
						Name:  "sidecar",
						Ready: true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
					},
				}
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, p))
		}

		// 4. Expect job → Failed and ONLY coord extra-2 recorded

		job = assertWsJobFailEventually(t.T(), job.Namespace, job.Name)

		coordStatus, ok := job.Status.ReplicaStatuses[wsapisv1.WSReplicaTypeCoordinator]
		require.True(t.T(), ok, "coordinator status missing")
		require.Len(t.T(), coordStatus.FailedPodStatuses, 1,
			"exactly one failed-pod entry expected")

		fp := coordStatus.FailedPodStatuses[0]
		assert.Contains(t.T(), fp.Message, "extra-2", "wrong failed container recorded")
		assert.Equal(t.T(), "coord-container3-timeout-coordinator-0", fp.Name, "wrong pod recorded")
		assert.Contains(t.T(), strings.ToLower(fp.Message), "timeout", "failure reason should mention timeout")

		// worker replica type must show **no** failure
		wStatus := job.Status.ReplicaStatuses[wsapisv1.WSReplicaTypeWorker]
		require.NotNil(t.T(), wStatus)
		assert.Empty(t.T(), wStatus.FailedPodStatuses, "worker should not have failures")
	})

}

func (t *TestSuite) TestCancelLogic() {
	ctx := context.Background()
	for _, testcase := range []struct {
		name            string
		cancelStatus    string
		expectCondition commonv1.JobConditionType
	}{
		{"cancel with cancel status", wsapisv1.CancelWithStatusCancelled, commonv1.JobCancelled},
		{"cancel with failed status", wsapisv1.CancelWithStatusFailed, commonv1.JobFailed},
		{"cancel with succeeded status", wsapisv1.CancelWithStatusSucceeded, commonv1.JobSucceeded},
	} {
		t.Run(testcase.name, func() {
			jobName := strings.ReplaceAll(testcase.name, " ", "-")
			wsjob := newWsExecuteJob(wscommon.Namespace, jobName, 1)
			require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
			expectedPodCount := countNonBrPods(wsjob)
			assertPodCountEventually(t.T(), wsjob.Namespace, wsjob.Name, "", expectedPodCount)

			cancelJobEventually(t.T(), wsjob, testcase.cancelStatus)
			assertPodCountEventually(t.T(), wsjob.Namespace, jobName, "", 0)
			newWsjob := &wsapisv1.WSJob{
				ObjectMeta: ctrl.ObjectMeta{
					Name:      jobName,
					Namespace: wscommon.DefaultNamespace,
				},
			}
			require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(newWsjob), newWsjob))
			assert.Equal(t.T(), newWsjob.Status.Conditions[len(newWsjob.Status.Conditions)-1].Type, testcase.expectCondition)
			assertPodCountEventually(t.T(), newWsjob.Namespace, jobName, "", 0)
			assertLockCountEventually(t.T(), 0)
		})
	}

	t.Run("cancel_a_succeeded_job", func() {
		jobName := "wsjob-cancel-succeeded-job"
		wsjob := newWsCompileJob(wscommon.Namespace, jobName, 1)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		pods := assertPodCountEventually(t.T(), wsjob.Namespace, jobName, "", 1) // wait for pod scheduling
		// set pod as succeeded to succeed the job
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}
		assertLockCountEventually(t.T(), 0)
		newWsjob := &wsapisv1.WSJob{
			ObjectMeta: ctrl.ObjectMeta{
				Name:      jobName,
				Namespace: wscommon.DefaultNamespace,
				Labels:    map[string]string{},
			},
		}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(newWsjob), newWsjob))
		assert.Equal(t.T(), newWsjob.Status.Conditions[len(newWsjob.Status.Conditions)-1].Type, commonv1.JobSucceeded)

		newWsjob.Annotations = make(map[string]string)
		newWsjob.Annotations[wsapisv1.CancelWithStatusKey] = wsapisv1.CancelWithStatusCancelled
		require.NoError(t.T(), k8sClient.Update(ctx, newWsjob))

		// The job should still be succeeded after the cancel.
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(wsjob), wsjob))
		assert.Equal(t.T(), newWsjob.Status.Conditions[len(wsjob.Status.Conditions)-1].Type, commonv1.JobSucceeded)
		assertLockCountEventually(t.T(), 0)
	})

	t.LeaseReconciler.Disabled = false
	defer func() {
		t.LeaseReconciler.Disabled = true
	}()
	for _, testcase := range []struct {
		name             string
		jobCancelContext string
		newJobStatus     string
		leaseDisabled    bool
		gracePeriod      time.Duration
	}{
		{
			name:             "canceled-wsjob-with-lease",
			jobCancelContext: "user canceled the job as canceled",
			newJobStatus:     wsapisv1.CancelWithStatusCancelled,
			leaseDisabled:    false,
		},
		{
			name:             "succeeded-wsjob-with-lease",
			jobCancelContext: "user canceled the job as succeeded",
			newJobStatus:     wsapisv1.CancelWithStatusSucceeded,
			leaseDisabled:    false,
		},
		{
			name:             "failed-wsjob-with-lease",
			jobCancelContext: "user canceled the job as failed",
			newJobStatus:     wsapisv1.CancelWithStatusFailed,
			leaseDisabled:    false,
		},
		{
			name:             "wsjob-with-expired-lease",
			jobCancelContext: "lease expired",
			newJobStatus:     "",
			leaseDisabled:    false,
		},
		{
			name:             "wsjob-with-expired-lease-before-start",
			jobCancelContext: "lease expired but before operator restarts",
			gracePeriod:      5 * time.Second,
			newJobStatus:     "",
			leaseDisabled:    false,
		},
		{
			name:             "wsjob-without-lease-enabled",
			jobCancelContext: "",
			newJobStatus:     "",
			leaseDisabled:    true,
		},
	} {
		t.Run(testcase.jobCancelContext, func() {
			startGracePeriod = testcase.gracePeriod
			previous := operatorStartTime
			defer func() {
				startGracePeriod = time.Minute
				operatorStartTime = previous
			}()
			job := newWsExecuteJob(wscommon.Namespace, testcase.name, 1)
			if testcase.leaseDisabled {
				job.Labels[wsapisv1.ClientLeaseDisabledLabelKey] = ""
			}
			require.NoError(t.T(), k8sClient.Create(ctx, job))
			defer assertWsjobCleanedUp(t.T(), job)

			if testcase.leaseDisabled {
				assertWsjobLeaseEventually(t.T(), job.Namespace, job.Name, false, "")
				assertWsJobAnnotatedNotCanceled(t.T(), job.Namespace, job.Name)
				assertWsJobLabeledNotCanceled(t.T(), job.Namespace, job.Name)
				return
			}
			// Lease creation on job creation
			lease := assertWsjobLeaseEventually(t.T(), job.Namespace, job.Name, true, "")

			if testcase.newJobStatus != "" {
				wscommon.AnnotateCanceledLease(
					lease,
					testcase.newJobStatus,
					wsapisv1.CanceledByClientReason,
					testcase.jobCancelContext)
				require.NoError(t.T(), k8sClient.Update(ctx, lease))

				if testcase.newJobStatus == wsapisv1.CancelWithStatusCancelled {
					assertWsJobAnnotatedCanceled(t.T(), job.Namespace, job.Name)
					assertWsJobLabeledCanceled(t.T(), job.Namespace, job.Name)
					assertWsJobCancelledEventually(t.T(), job.Namespace, job.Name)
				} else if testcase.newJobStatus == wsapisv1.CancelWithStatusSucceeded {
					assertWsJobAnnotatedSucceeded(t.T(), job.Namespace, job.Name)
					assertWsJobLabeledSucceeded(t.T(), job.Namespace, job.Name)
					assertWsJobSuccessEventually(t.T(), job.Namespace, job.Name)
				} else {
					assertWsJobAnnotatedFailed(t.T(), job.Namespace, job.Name)
					assertWsJobLabeledFailed(t.T(), job.Namespace, job.Name)
					assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
				}
			} else {
				leaseDuration := wscommon.ClientLeaseDuration
				patch := client.MergeFrom(lease.DeepCopy())
				if startGracePeriod > 0 {
					operatorStartTime = time.Now()
					duration := int32(startGracePeriod)
					lease.Spec.LeaseDurationSeconds = &duration
					lease.Spec.RenewTime = &metav1.MicroTime{Time: operatorStartTime.Add(-leaseDuration).Add(-startGracePeriod)}
					require.NoError(t.T(), k8sClient.Patch(ctx, lease, patch))
					time.Sleep(3 * time.Second)
					assertWsJobScheduled(t.T(), job.Namespace, job.Name)
				} else {
					lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now().Add(-leaseDuration).Add(5 * time.Second)}
					require.NoError(t.T(), k8sClient.Patch(ctx, lease, patch))
				}
				assertWsJobAnnotatedExpired(t.T(), job.Namespace, job.Name)
				assertWsJobLabeledExpired(t.T(), job.Namespace, job.Name)
				assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
			}
		})
	}
}

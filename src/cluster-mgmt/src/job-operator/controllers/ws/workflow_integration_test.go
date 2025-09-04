//go:build integration && workflow

package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

func (t *TestSuite) TestJobPriority() {
	ctx := context.Background()

	eventuallyReconciled := func(_t *testing.T, nsn types.NamespacedName) {
		require.Eventually(_t, func() bool {
			_, err := t.JobReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: nsn})
			return err == nil
		}, eventuallyTimeout, eventuallyTick)
	}

	// If source channel has an event, then redirect to the destination channel
	channelEventuallySilent := func(_t *testing.T, srcChannel, destChannel chan event.GenericEvent) {
		timeoutCtx, cancel := context.WithTimeout(ctx, eventuallyTimeout)
		defer cancel()

		silencePeriod := 3 * time.Second
		silenceTimer := time.NewTimer(silencePeriod)
		defer silenceTimer.Stop()

		done := make(chan struct{})

		go func() {
			defer close(done)
			for {
				select {
				case e := <-srcChannel:
					// Event received, reset the silence timer
					if !silenceTimer.Stop() {
						<-silenceTimer.C
					}
					silenceTimer.Reset(silencePeriod)
					nsn := fmt.Sprintf("%s/%s", e.Object.GetNamespace(), e.Object.GetName())
					_t.Logf("event: %s", nsn)
					destChannel <- e
				case <-silenceTimer.C:
					// Timer expired, channel has been silent for the specified period
					return
				case <-timeoutCtx.Done():
					// Context cancelled or deadline exceeded
					return
				}
			}
		}()

		select {
		case <-done:
			if err := timeoutCtx.Err(); err == nil {
				require.True(_t, true, "Channel was silent for 3 seconds")
			} else {
				_t.Error("Context cancelled or deadline exceeded before the channel became silent")
			}
		}
	}

	t.Run("priority_setting_for_pending_job_without_workflow", func() {
		preJob := newWsExecuteJob(wscommon.Namespace, "pre-job", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, preJob))
		defer assertWsjobCleanedUp(t.T(), preJob)
		preLock0 := assertLockExists(t.T(), preJob.Namespace, preJob.Name)
		assertLocked(t.T(), preLock0)

		// Create a pending job without workflow
		pendingJob0 := newWsExecuteJob(wscommon.Namespace, "pending-job-0", systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, pendingJob0))
		defer assertWsjobCleanedUp(t.T(), pendingJob0)
		pendingLock0 := assertLockExists(t.T(), pendingJob0.Namespace, pendingJob0.Name)
		assertNeverGranted(t.T(), pendingLock0)
		updateJobPriorityEventually(t.T(), pendingJob0, 10)
		assertPriorityForLocks(t.T(), 10, pendingLock0)
	})

	t.Run("priority_setting_for_new_job", func() {
		workflowId := "test-foobar-1"
		// Create a scheduled job
		job0 := newWsExecuteJob(wscommon.Namespace, "job-0", 1)
		job0.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job0.SetPriority(30)
		require.NoError(t.T(), k8sClient.Create(ctx, job0))
		defer assertWsjobCleanedUp(t.T(), job0)
		lock0 := assertLockExists(t.T(), job0.Namespace, job0.Name)
		assertLocked(t.T(), lock0)

		// Create a completed job with the same workflow id
		job1 := newWsCompileJob(wscommon.Namespace, "job-1", 1)
		job1.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job1.SetPriority(20)
		require.NoError(t.T(), k8sClient.Create(ctx, job1))
		defer assertWsjobCleanedUp(t.T(), job1)
		lock1 := assertLockExists(t.T(), job1.Namespace, job1.Name)
		assertLocked(t.T(), lock1)
		assert.Equal(t.T(), 30, lock1.GetPriority())

		pods := assertPodCountEventually(t.T(), job1.Namespace, job1.Name, "", 1)
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}

		// A job launched after another scheduled job but ended before that scheduled job
		// Subsequent jobs should inherit priority of the scheduled job
		require.NoError(t.T(), k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(job1), job1))
		assert.Equal(t.T(), 30, job1.GetPriority())
		updateJobPriorityEventually(t.T(), job0, 10)

		// Create a pending job with the same workflow id
		job2 := newWsExecuteJob(wscommon.Namespace, "job-2", systemCount)
		job2.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job2.SetPriority(20)
		require.NoError(t.T(), k8sClient.Create(ctx, job2))
		defer assertWsjobCleanedUp(t.T(), job2)

		lock2 := assertLockExists(t.T(), job2.Namespace, job2.Name)
		assertPending(t.T(), lock2)
		assert.Equal(t.T(), 10, lock2.GetPriority())
		require.NoError(t.T(), k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(job2), job2))
		assert.Equal(t.T(), 10, job2.GetPriority())

		// Update the priority of the pending job
		updateJobPriorityEventually(t.T(), job2, 5)
		assertPriorityForJobs(t.T(), 5, job0, job2)
		assertPriorityForLocks(t.T(), 5, lock0, lock2)

		// Create another pending job with the same workflow id
		// This new job should see (job0:p10:scheduled, job2:p5:pending) and both have the same priority
		job3 := newWsExecuteJob(wscommon.Namespace, "job-3", systemCount)
		job3.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job3.SetPriority(20)
		require.NoError(t.T(), k8sClient.Create(ctx, job3))
		defer assertWsjobCleanedUp(t.T(), job3)

		lock3 := assertLockExists(t.T(), job3.Namespace, job3.Name)
		assertPending(t.T(), lock3)
		assertPriorityForJob(t.T(), job3, 5)
		assertPriorityForLock(t.T(), lock3, 5)

		updateJobPriorityEventually(t.T(), job3, 70)
		assertPriorityForJobs(t.T(), 70, job0, job2, job3)
		assertPriorityForLocks(t.T(), 70, lock0, lock2, lock3)

		// Create a new job that should use the default priority value
		job4 := newWsExecuteJob(wscommon.Namespace, "job-4", systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, job4))
		defer assertWsjobCleanedUp(t.T(), job4)
		lock4 := assertLockExists(t.T(), job4.Namespace, job4.Name)
		assertPending(t.T(), lock4)
		assertPriorityForJob(t.T(), job4, wsapisv1.DefaultJobPriorityValue)
		assertPriorityForLock(t.T(), lock4, wsapisv1.DefaultJobPriorityValue)
	})

	t.Run("update_priority_on_completed,_running_and_pending_jobs", func() {
		workflowId := "test-foobar-2"

		// Create a completed job
		job0 := newWsCompileJob(wscommon.Namespace, "job-00", 1)
		job0.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job0.SetPriority(10)
		require.NoError(t.T(), k8sClient.Create(ctx, job0))
		defer assertWsjobCleanedUp(t.T(), job0)

		lock0 := assertLockExists(t.T(), job0.Namespace, job0.Name)
		assertLocked(t.T(), lock0)
		assertPriorityForLock(t.T(), lock0, 10)
		pods := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, "", 1)
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}

		// Create a running job
		job1 := newWsExecuteJob(wscommon.Namespace, "job-11", 1)
		job1.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job1.SetPriority(20)
		require.NoError(t.T(), k8sClient.Create(ctx, job1))
		defer assertWsjobCleanedUp(t.T(), job1)

		lock1 := assertLockExists(t.T(), job1.Namespace, job1.Name)
		assertLocked(t.T(), lock1)
		assertPriorityForLock(t.T(), lock1, 10)
		pods = getJobPods(t.T(), job1.Namespace, job1.Name, "")
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}

		// Update priority of the running job so that it's different from the completed job
		updateJobPriorityEventually(t.T(), job1, 60)
		assertPriorityForLock(t.T(), lock1, 60)
		assertPriorityForJob(t.T(), job0, 10) // unchanged

		// Create a pending job
		job2 := newWsExecuteJob(wscommon.Namespace, "job-22", systemCount)
		job2.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job2.SetPriority(30)
		require.NoError(t.T(), k8sClient.Create(ctx, job2))
		defer assertWsjobCleanedUp(t.T(), job2)

		lock2 := assertLockExists(t.T(), job2.Namespace, job2.Name)
		assertPending(t.T(), lock2)
		assertPriorityForLock(t.T(), lock2, 60)

		// Create another pending job
		job3 := newWsExecuteJob(wscommon.Namespace, "job-33", systemCount)
		job3.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job3.SetPriority(30)
		require.NoError(t.T(), k8sClient.Create(ctx, job3))
		defer assertWsjobCleanedUp(t.T(), job3)

		lock3 := assertLockExists(t.T(), job3.Namespace, job3.Name)
		assertPending(t.T(), lock3)
		assertPriorityForLock(t.T(), lock3, 60)

		// Update priority of the running job and the two pending jobs should inherit the priority
		updateJobPriorityEventually(t.T(), job1, 5)
		assertPriorityForJob(t.T(), job1, 5)
		assertPriorityForLock(t.T(), lock1, 5)

		// Check priority of the pending job
		// Should be synced with the running job
		assertPriorityForLocks(t.T(), 5, lock2, lock3)
		assertPriorityForJobs(t.T(), 5, job2, job3)

		// Update priority of the complete job and the running/pending jobs should have no change
		updateJobPriorityEventually(t.T(), job0, 50)
		assertPriorityForJobs(t.T(), 5, job1, job2, job3)
		assertPriorityForLocks(t.T(), 5, lock1, lock2, lock3)

		// Update priority of one pending job and the other pending job should inherit the update
		// The running job should also inherit the update for consistency.
		updateJobPriorityEventually(t.T(), job2, 100)
		assertPriorityForJobs(t.T(), 100, job1, job2, job3)
		assertPriorityForLocks(t.T(), 100, lock1, lock2, lock3)

		updateJobPriorityEventually(t.T(), job3, 75)
		assertPriorityForJobs(t.T(), 75, job1, job2, job3)
		assertPriorityForLocks(t.T(), 75, lock1, lock2, lock3)

		// Finish the running job and see a pending job to take over
		cancelJobEventually(t.T(), job1, wsapisv1.CancelWithStatusCancelled)

		// lock1 cleanup is deferred until all pods from job1 is cleaned up
		assertLockCountEventually(t.T(), 2) // left with lock2 and lock3
		assertLocked(t.T(), lock2)

		cancelJobEventually(t.T(), job2, wsapisv1.CancelWithStatusCancelled)
		assertLocked(t.T(), lock3)
	})

	t.Run("cache_rebuild", func() {
		workflowId := "test-foobar-3"
		nsWorkflowId := fmt.Sprintf("%s/%s", wscommon.Namespace, workflowId)

		// Create a completed job
		job0 := newWsCompileJob(wscommon.Namespace, "job-000", 1)
		job0.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job0.SetPriority(10)
		require.NoError(t.T(), k8sClient.Create(ctx, job0))

		lock0 := assertLockExists(t.T(), job0.Namespace, job0.Name)
		assertLocked(t.T(), lock0)
		assertPriorityForLock(t.T(), lock0, 10)

		// Cache got loaded
		e, ok := t.JobReconciler.WorkflowCache.Get(nsWorkflowId)
		assert.True(t.T(), ok)
		priority := e.(*int)
		assert.Equal(t.T(), 10, *priority)

		pods := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, "", 1)
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}

		// Clean up cache to simulate operator restart
		// New job should inherit from completed job
		t.JobReconciler.WorkflowCache.Remove(nsWorkflowId)

		// Create a scheduled job
		job1 := newWsExecuteJob(wscommon.Namespace, "job-111", 1)
		job1.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job1.SetPriority(20)
		require.NoError(t.T(), k8sClient.Create(ctx, job1))
		defer assertWsjobCleanedUp(t.T(), job1)
		lock1 := assertLockExists(t.T(), job1.Namespace, job1.Name)
		assertLocked(t.T(), lock1)

		// Cache got loaded
		e, ok = t.JobReconciler.WorkflowCache.Get(nsWorkflowId)
		assert.True(t.T(), ok)
		priority = e.(*int)
		assert.Equal(t.T(), 10, *priority)

		// Clean up cache to simulate operator restart
		t.JobReconciler.WorkflowCache.Remove(nsWorkflowId)
		eventuallyReconciled(t.T(), job1.NamespacedName())
		e, ok = t.JobReconciler.WorkflowCache.Get(nsWorkflowId)
		assert.True(t.T(), ok)
		priority = e.(*int)
		assert.Equal(t.T(), 10, *priority)

		// Create a pending job with the same workflow id
		// Pending job should re-populate cache
		// Deleting job0 to test inheritance should respect the scheduled job
		assertWsjobCleanedUp(t.T(), job0)
		job2 := newWsExecuteJob(wscommon.Namespace, "job-222", systemCount)
		job2.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
		job2.SetPriority(20)
		require.NoError(t.T(), k8sClient.Create(ctx, job2))
		defer assertWsjobCleanedUp(t.T(), job2)

		lock2 := assertLockExists(t.T(), job2.Namespace, job2.Name)
		assertPending(t.T(), lock2)

		// Cache got lazy loaded
		eventuallyReconciled(t.T(), job2.NamespacedName())
		e, ok = t.JobReconciler.WorkflowCache.Get(nsWorkflowId)
		assert.True(t.T(), ok)
		priority = e.(*int)
		assert.Equal(t.T(), 10, *priority)
		assert.Equal(t.T(), 10, lock2.GetPriority())
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(job2), job2))
		assert.Equal(t.T(), 10, job2.GetPriority())

		// Clean up cache to simulate operator restart on different stages during priority propagation
		realEventChan := t.JobReconciler.WorkflowEventChan
		fakeEventChan := make(chan event.GenericEvent, 1024)

		stageJobPriorities := func(priority int, jobs []*wsapisv1.WSJob) {
			for _, j := range jobs {
				require.NoError(t.T(),
					wscommon.PatchWsjobPriority(ctx, wsV1Interface, j.Namespace, j.Name, priority))
			}
		}

		stageLockPriorities := func(priority int, locks []*rlv1.ResourceLock) {
			for _, l := range locks {
				patch := map[string]map[string]*int{
					"spec": {
						"priority": &priority,
					},
				}
				reqBody, _ := json.Marshal(patch)
				_, err := rlV1Interface.ResourceLocks(l.Namespace).Patch(
					ctx, l.Name, types.MergePatchType, reqBody, metav1.PatchOptions{})
				require.NoError(t.T(), err)
			}
		}

		testPriorityDriftOnRestart := func(
			priorityAhead int, jobsAhead []*wsapisv1.WSJob, locksAhead []*rlv1.ResourceLock,
			priorityBehind int, jobsBehind []*wsapisv1.WSJob, locksBehind []*rlv1.ResourceLock,
			expectedEventualPriority int) {

			// disconnect the channel before staging drifts
			t.JobReconciler.WorkflowEventChan = fakeEventChan

			stageJobPriorities(priorityAhead, jobsAhead)
			stageLockPriorities(priorityAhead, locksAhead)
			stageJobPriorities(priorityBehind, jobsBehind)
			stageLockPriorities(priorityBehind, locksBehind)

			// remove cache and reconnect the channel afterward
			t.JobReconciler.WorkflowCache.Remove(nsWorkflowId)
			t.JobReconciler.WorkflowEventChan = realEventChan

			eventuallyReconciled(t.T(), job1.NamespacedName())
			eventuallyReconciled(t.T(), job2.NamespacedName())

			// cache value should be repopulated
			e, ok = t.JobReconciler.WorkflowCache.Get(nsWorkflowId)
			assert.True(t.T(), ok)
			priority = e.(*int)
			assert.Equal(t.T(), expectedEventualPriority, *priority)
			// all jobs and locks should eventually have their priorities synchronized
			assertPriorityForJobs(t.T(), expectedEventualPriority, append(jobsAhead, jobsBehind...)...)
			assertPriorityForLocks(t.T(), expectedEventualPriority, append(locksAhead, locksBehind...)...)
		}

		testMultiplePriorityDriftsOnRestart := func(
			priorityDrift1 int, job1 *wsapisv1.WSJob,
			priorityDrift2 int, job2 *wsapisv1.WSJob,
			lockPriority int, locks []*rlv1.ResourceLock) {

			// disconnect the channel before staging drifts
			t.JobReconciler.WorkflowEventChan = fakeEventChan

			stageJobPriorities(priorityDrift1, []*wsapisv1.WSJob{job1})
			stageJobPriorities(priorityDrift2, []*wsapisv1.WSJob{job2})
			stageLockPriorities(lockPriority, locks)

			// remove cache and reconnect the channel afterward
			// t.JobReconciler -> fakeEventChan -> realEventChan -> t.WorkflowReconciler
			t.JobReconciler.WorkflowCache.Remove(nsWorkflowId)

			eventuallyReconciled(t.T(), job1.NamespacedName())
			eventuallyReconciled(t.T(), job2.NamespacedName())

			channelEventuallySilent(t.T(), fakeEventChan, realEventChan)
			require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(job1), job1))
			require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(job2), job2))
			require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(lock1), lock1))
			require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(lock2), lock2))
			assert.Equal(t.T(), job1.GetPriority(), lock1.GetPriority())
			assert.Equal(t.T(), job2.GetPriority(), lock2.GetPriority())
			assert.Equal(t.T(), job1.GetPriority(), job2.GetPriority())
			// either drift could win
			assert.True(t.T(), job1.GetPriority() == priorityDrift1 || job1.GetPriority() == priorityDrift2)
		}

		testPriorityDriftOnRestart(
			50, []*wsapisv1.WSJob{job1}, []*rlv1.ResourceLock{},
			10, []*wsapisv1.WSJob{job2}, []*rlv1.ResourceLock{lock1, lock2},
			50,
		)
		testPriorityDriftOnRestart(
			50, []*wsapisv1.WSJob{job1, job2}, []*rlv1.ResourceLock{},
			10, []*wsapisv1.WSJob{}, []*rlv1.ResourceLock{lock1, lock2},
			50,
		)
		testPriorityDriftOnRestart(
			50, []*wsapisv1.WSJob{job1, job2}, []*rlv1.ResourceLock{lock1},
			10, []*wsapisv1.WSJob{}, []*rlv1.ResourceLock{lock2},
			50,
		)
		testMultiplePriorityDriftsOnRestart(
			10, job1,
			20, job2,
			30, []*rlv1.ResourceLock{lock1, lock2},
		)

		// reset the workflow event channel
		t.JobReconciler.WorkflowEventChan = realEventChan
	})
}

// test on workflow reservation/expiration
func (t *TestSuite) TestWorkflow() {
	t.LeaseReconciler.Disabled = false
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		t.LeaseReconciler.Disabled = true
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()
	ctx := context.Background()
	deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
	cfg := wscommon.NewTestClusterConfigSchema(systemCount, 1, brCount, systemCount, 0, 1, memoryXPerGroup)
	t.NoError(wscommon.CreateK8sNodes(cfg, t.JobReconciler.Client))
	t.NoError(t.CfgMgr.Initialize(cfg))

	// create workflow/lease with auto reserving
	workflow := "restart"
	lease := newLeaseBuilder(workflow, workflow, int32(300))
	lease.Annotations = wscommon.EnsureMap(lease.Annotations)
	lease.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReservedValue
	require.NoError(t.T(), k8sClient.Create(ctx, lease))

	// check auto reserve behavior on first compile job
	comJob := newWsCompileJob(wscommon.Namespace, "compile-wf-job", 1)
	comJob.Labels[wsapisv1.WorkflowIdLabelKey] = workflow
	require.NoError(t.T(), k8sClient.Create(t.Ctx, comJob))
	defer assertWsjobCleanedUp(t.T(), comJob)
	comLock := assertLockExists(t.T(), comJob.Namespace, comJob.Name)
	assertLocked(t.T(), comLock)
	compileReserve := assertLockExists(t.T(), comJob.Namespace, wscommon.ReservingLockName(comJob.Name))
	require.True(t.T(), compileReserve.IsSubsidiaryReservation())
	cancelJobEventually(t.T(), comJob, wsapisv1.CancelWithStatusCancelled)
	comLock = assertLockExists(t.T(), comJob.Namespace, wscommon.ReservingLockName(comJob.Name))

	// second compile won't reserve and won't schedule till execute job granted
	prefetchCompile := newWsCompileJob(wscommon.Namespace, "prefetch-wf-job", 1)
	prefetchCompile.Labels[wsapisv1.WorkflowIdLabelKey] = workflow
	require.NoError(t.T(), k8sClient.Create(t.Ctx, prefetchCompile))
	defer assertWsjobCleanedUp(t.T(), prefetchCompile)
	prefetchLock := assertLockExists(t.T(), prefetchCompile.Namespace, prefetchCompile.Name)
	require.Empty(t.T(), prefetchLock.GetSubsidiaryReservationName())
	assertNeverGranted(t.T(), prefetchLock)
	assertLockNotExists(t.T(), prefetchLock.Namespace, wscommon.ReservingLockName(prefetchLock.Name))

	// check auto reserve behavior on first execute job
	parentJob := newWsExecuteJob(wscommon.Namespace, "parent-job", 5)
	parentJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker].Replicas = wscommon.Pointer(int32(5))
	parentJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker].Template.Spec.Containers =
		parentJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker].Template.Spec.Containers[:1]
	parentJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker].Template.Spec.Containers[0].Resources =
		corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: *apiresource.NewQuantity((wscommon.DefaultNodeMem)<<20, apiresource.BinarySI),
			},
		}
	parentJob.Labels[wsapisv1.WorkflowIdLabelKey] = workflow
	parentJob.Annotations[wsapisv1.SchedulerHintAllowSys] = "system-1,system-2,system-3,system-4,system-5"
	require.NoError(t.T(), k8sClient.Create(t.Ctx, parentJob))
	defer assertWsjobCleanedUp(t.T(), parentJob)
	pLock := assertLockExists(t.T(), parentJob.Namespace, parentJob.Name)
	assertLocked(t.T(), pLock)
	require.Equal(t.T(), compileReserve.Name, pLock.GetSubsidiaryReservationName())
	pSystems := map[string]bool{}
	for _, s := range pLock.Status.SystemGrants {
		pSystems[s.Name] = true
	}
	// prefetch granted after first execute granted
	assertLocked(t.T(), prefetchLock)
	wfLock := assertLockExists(t.T(), parentJob.Namespace, wscommon.ReservingLockName(parentJob.Name))
	// information check if regular NG type set at creation time
	require.Equal(t.T(), []string{wsapisv1.PopNgTypeRegular}, wfLock.GetPopulatedNgTypes())

	// another job not in workflow granted to validate no double booking
	otherJob0 := newWsExecuteJob(wscommon.Namespace, "other-0", 3)
	require.NoError(t.T(), k8sClient.Create(t.Ctx, otherJob0))
	defer assertWsjobCleanedUp(t.T(), otherJob0)
	oLock0 := assertLockExists(t.T(), otherJob0.Namespace, otherJob0.Name)
	assertLocked(t.T(), oLock0)

	// cancel parent job to enable reserving
	cancelJobEventually(t.T(), parentJob, wsapisv1.CancelWithStatusCancelled)
	assertLockNotExists(t.T(), parentJob.Namespace, parentJob.Name)
	// update granted system to unhealthy
	s := &systemv1.System{}
	require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: pLock.Status.SystemGrants[0].Name}, s))
	s.Spec.Unschedulable = true
	require.NoError(t.T(), k8sClient.Update(ctx, s))
	require.Eventually(t.T(), func() bool {
		return len(t.ResourceController.Cfg.UnhealthySystems) == 1
	}, eventuallyTimeout, eventuallyTick)
	// update 2 NG to have no worker
	var ng1, ng2 string
	for _, g := range pLock.Status.NodeGroupGrants {
		if g.Name == "ng-secondaries" {
			ng1 = g.NodeGroups[0]
			ng2 = g.NodeGroups[1]
			break
		}
	}
	require.NoError(t.T(), patchNode(t, fmt.Sprintf("%s-worker-0", ng1),
		[]corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeErrorAlertType,
			Status: corev1.ConditionTrue,
		}}))
	require.NoError(t.T(), patchNode(t, fmt.Sprintf("%s-worker-0", ng2),
		[]corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeErrorAlertType,
			Status: corev1.ConditionTrue,
		}}))
	// update one AX to error
	var ax1 string
	for _, g := range pLock.Status.GroupResourceGrants {
		if strings.Contains(g.Name, "Activation") {
			ax1 = g.ResourceGrants[0].Resources[0].Name
		}
	}
	require.NotEmpty(t.T(), ax1)
	require.NoError(t.T(), patchNode(t, ax1,
		[]corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeErrorAlertType,
			Status: corev1.ConditionTrue,
		}}))

	// trigger query on reservation status and expect in sync with cluster status
	require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{
		Namespace: pLock.Namespace,
		Name:      wscommon.ReservingLockName(pLock.Name),
	}, pLock))
	pLock.QueryReservation()
	require.NoError(t.T(), k8sClient.Update(ctx, pLock))
	require.Eventually(t.T(), func() bool {
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(pLock), pLock))
		sys, msg, _ := pLock.GetReservedSystems()
		t.T().Log("reserved-sys-count", sys, "msg", msg)
		// 4 systems + 3 NGs, so return 3
		return sys == 3 &&
			strings.Contains(msg, "requested 5 system[name:system-1,system-2,system-3,system-4,system-5, "+
				"namespace:job-operator] but only 4 available") &&
			strings.Contains(msg, "requested 5 nodegroups in total but only 3 match request") &&
			strings.Contains(msg, "requested 5 AX node(s) but only 4 available")
	}, eventuallyTimeout, eventuallyTick)

	// update back to healthy after query
	require.NoError(t.T(), patchNode(t, fmt.Sprintf("%s-worker-0", ng1), nil))
	require.NoError(t.T(), patchNode(t, fmt.Sprintf("%s-worker-0", ng2), nil))
	require.NoError(t.T(), patchNode(t, fmt.Sprintf(ax1), nil))
	s.Spec.Unschedulable = false
	require.NoError(t.T(), k8sClient.Update(ctx, s))
	require.Eventually(t.T(), func() bool {
		return len(t.ResourceController.Cfg.UnhealthySystems) == 0
	}, eventuallyTimeout, eventuallyTick)

	// another job not in workflow but still pending to validate reserving enabled
	cancelJobEventually(t.T(), otherJob0, wsapisv1.CancelWithStatusCancelled)
	otherJob := newWsExecuteJob(wscommon.Namespace, "other-1", 4)
	require.NoError(t.T(), k8sClient.Create(t.Ctx, otherJob))
	defer assertWsjobCleanedUp(t.T(), otherJob)
	oLock := assertLockExists(t.T(), otherJob.Namespace, otherJob.Name)
	assertNeverGranted(t.T(), oLock)

	// workflow child-1
	childJob := newWsExecuteJob(wscommon.Namespace, "child-1-success", 4)
	childJob.Labels[wsapisv1.WorkflowIdLabelKey] = workflow
	require.NoError(t.T(), k8sClient.Create(t.Ctx, childJob))
	defer assertWsjobCleanedUp(t.T(), childJob)
	cLock := assertLockExists(t.T(), childJob.Namespace, childJob.Name)
	require.Empty(t.T(), cLock.GetSubsidiaryReservationName())
	// inherited after previous job terminated
	assertLocked(t.T(), cLock)
	// reservation disabled after child inherited
	assertLocked(t.T(), oLock)
	// affinity check on systems
	for _, s := range cLock.Status.SystemGrants {
		require.True(t.T(), pSystems[s.Name])
	}
	// release child 1
	cancelJobEventually(t.T(), childJob, wsapisv1.CancelWithStatusCancelled)

	// next child job after first child failed
	failedChildJob := newWsExecuteJob(wscommon.Namespace, "child-2-fail", 5)
	failedChildJob.Labels[wsapisv1.WorkflowIdLabelKey] = workflow
	require.NoError(t.T(), k8sClient.Create(t.Ctx, failedChildJob))
	defer assertWsjobCleanedUp(t.T(), failedChildJob)
	// 2nd child job failed due to requesting more system than reserved
	// no queueing for restart job
	assertLockNotExists(t.T(), failedChildJob.Namespace, failedChildJob.Name)
	assertLockNotExists(t.T(), cLock.Namespace, cLock.Name)
	assertLocked(t.T(), oLock)

	// 3rd child
	succeedChildJob := newWsExecuteJob(wscommon.Namespace, "child-3-success", 4)
	succeedChildJob.Labels[wsapisv1.WorkflowIdLabelKey] = workflow
	require.NoError(t.T(), k8sClient.Create(t.Ctx, succeedChildJob))
	defer assertWsjobCleanedUp(t.T(), succeedChildJob)
	// 3rd inherited successfully after 1st child release
	sLock := assertLockExists(t.T(), succeedChildJob.Namespace, succeedChildJob.Name)
	assertLocked(t.T(), sLock)
	assertLockNotExists(t.T(), cLock.Namespace, cLock.Name)
	assertLocked(t.T(), oLock)
	// release child-3 res
	cancelJobEventually(t.T(), succeedChildJob, wsapisv1.CancelWithStatusCancelled)

	// mock with 5s left to expire
	patch := client.MergeFrom(lease.DeepCopy())
	lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
	lease.Spec.LeaseDurationSeconds = int32Ptr(5)
	require.NoError(t.T(), k8sClient.Patch(ctx, lease, patch))
	// assert lock still exist within duration
	time.Sleep(1 * time.Second)
	assertLockExists(t.T(), parentJob.Namespace, wscommon.ReservingLockName(parentJob.Name))
	// sleeps for longer than lease duration
	time.Sleep(5 * time.Second)
	// assert lock released for reserved lock after lease expire
	assertLockNotExists(t.T(), wfLock.Namespace, wfLock.Name)
	assertWsJobAnnotatedNotCanceled(t.T(), oLock.Namespace, oLock.Name)
	assertLeaseDeleted(t.T(), workflow)
}

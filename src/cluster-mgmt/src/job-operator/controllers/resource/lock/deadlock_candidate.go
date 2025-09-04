package lock

import (
	"context"
	"fmt"
	"time"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

// deadlockCheck detects if there was a prioritized deadlock.
// It happens because of legacy job flows of eval-compile flow.
// In such case, execute job is running and trigger a compile job and
// blocks till compile ends which leads to cyclic dependency when compile doesn't have resource to run.
// The key missing point is it didn't reserve the compile resource.
//
// After detected, we would fail prioritized deadlock candidates in the order from least priority to highest
// until the highest prioritized lock is no longer a deadlock,
// i.e. either can schedule immediately or can wait in queue for other non-prioritized job to release resources.
// The queuing is allowed with a max grace period and in case it exceeds, it will be failed as a best effort.
//
// TODO: This can be removed at rel-2.7 as all train-eval flow starts reservation at 2.5
func (r *Reconciler) deadlockCheck(ctx context.Context, requeueInterval *commonv1.RequeueAfterTTL) (bool, error) {
	// technically can apply to both prioritized compile+execute
	// but currently only prioritized compile have non-reserved child jobs
	q := r.getQueue(rlv1.PrioritizedCompileQueueName)
	if len(q.data) == 0 {
		return true, nil
	}

	logger, endLog := r.debugLogger(ctx, "deadlockCheck")
	defer endLog()
	unallocatedResources := r.resources.UnallocatedCopy()
	var firstPrioritizedLock *lock
	var locksUnaffected []*lock
	var rejectedLockName string
	var rejectedParentLockName string
	affectedWorkflowId := ""

	// Iterate all prioritized-compile locks and allocate the parents' resources in a collection
	// If at any point the first lock in queue could not be allocated on top of that collection,
	// it means we encounter a deadlock and the first lock in queue could not be scheduled unless
	// we reject some lock(s) of lower priority. In case where a lock did get rejected in this
	// routine, Framework will cancel the parent execute job of that rejected lock.
	for _, l := range q.data {
		// job with reserved resources or non-workflow job won't cause deadlock
		if l.GrantDeadline == nil || r.getCachedLock(l.ParentLockNsName) == nil {
			continue
		}
		if time.Now().After(*l.GrantDeadline) {
			msgf := "Prioritized-compile %s was stuck in pending state for too long and could not be granted. %s"
			args := []any{l.Obj.NamespacedName(), prioritizedCompileSupportMsg}
			msg := fmt.Sprintf(msgf, args...)
			logger.Error(nil, msg)
			return false, r.setLockErrorInK8s(ctx, l.Obj, msgf, args...)
		} else {
			interval := l.GrantDeadline.Sub(time.Now())
			if requeueInterval == nil || requeueInterval.TTL > interval {
				requeueInterval = &commonv1.RequeueAfterTTL{TTL: interval}
			}
		}
		if firstPrioritizedLock == nil {
			firstPrioritizedLock = l
		}
		if affectedWorkflowId == "" {
			alloc := r.getCachedLock(l.ParentLockNsName).getAllocations()
			unallocatedResources.Allocate(nil, alloc)

			if _, err := r.checkGrantable(firstPrioritizedLock, unallocatedResources); err != nil {
				affectedWorkflowId = fmt.Sprintf("%s/%s", l.Obj.Namespace, l.WorkflowId)
				rejectedParentLockName = l.ParentLockNsName
				rejectedLockName = l.Name
				break
			} else {
				locksUnaffected = append(locksUnaffected, l)
			}
		}
	}

	if affectedWorkflowId != "" {
		mostBehindLock := q.data[len(q.data)-1]
		// Fail the least prioritized-compile lock in the queue if deadlock is detected
		// The failed lock should trigger the corresponding job to fail and in return delete the failed lock
		// The reconciling due to failed lock deletion triggers a further deadlock detection on other candidates
		if len(q.data) == 1 {
			logger.Info("Prioritized-compile failed",
				"affected-workflow-id", affectedWorkflowId,
				"prioritized-compile-rejected", mostBehindLock.Obj.NamespacedName())
			return false, r.setLockErrorInK8s(
				ctx,
				mostBehindLock.Obj,
				"Prioritized-compile job %s could not be scheduled due to resource contention "+
					"from execute job %s from the same workflow. %s",
				rejectedLockName, rejectedParentLockName, prioritizedCompileSupportMsg)
		} else {
			var compileJobsUnaffected []string
			var workflowIdsUnaffected []string
			for _, l := range locksUnaffected {
				compileJobsUnaffected = append(compileJobsUnaffected, l.Name)
				workflowIdsUnaffected = append(workflowIdsUnaffected, l.WorkflowId)
			}
			logger.Info("Prioritized-compile failed",
				"prioritized-workflow-ids", workflowIdsUnaffected,
				"prioritized-compile-ids", compileJobsUnaffected,
				"affected-workflow-id", mostBehindLock.Obj.Labels[wsapisv1.WorkflowIdLabelKey],
				"prioritized-compile-rejected", mostBehindLock.Obj.NamespacedName())
			return false, r.setLockErrorInK8s(
				ctx,
				mostBehindLock.Obj,
				"Prioritized-compile job %s could not be scheduled due to resource contention "+
					"and lost to prioritized-compile job(s) %v of higher priority from being scheduled. %s",
				rejectedLockName, compileJobsUnaffected, prioritizedCompileSupportMsg)
		}
	}
	return true, nil
}

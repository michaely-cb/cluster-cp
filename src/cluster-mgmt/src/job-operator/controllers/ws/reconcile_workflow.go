package ws

import (
	"context"
	"fmt"
	"sort"
	"strings"

	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonutil "github.com/kubeflow/common/pkg/util"
)

// 1. reconcile workflow reservation
// 2. reconcile workflow priority
func (r *WSJobReconciler) reconcileWorkflow(ctx context.Context, wsjob *wsapisv1.WSJob) (bool, error) {
	jobsInWorkflow, err := getWorkflowJobs(ctx, r.Client, wsjob.Namespace, wsjob.GetWorkflowId())
	if err != nil {
		return false, err
	}
	// reconcile reservation for new child job spec or parent lock release
	err = r.reconcileWorkflowReservation(ctx, wsjob, jobsInWorkflow)
	if err != nil {
		return false, err
	}
	// reconcile priority for all existing running/scheduled jobs in workflow
	err = r.reconcileWorkflowPriority(ctx, wsjob, jobsInWorkflow)
	if err != nil {
		return false, err
	}
	return true, nil
}

// key aspects for workflow reservations
// 1. create reservation after job annotated with reserve
// 2. reservation lock can be enabled/disabled with child lock status
// e.g. if child lock exists/inherited, then reserving lock set as disabled inside of lock reconciler
// if child lock cleaned after job ended, then reserving lock will be enabled inside of lock reconciler
// 3. cleanup reservation after job annotated with release, this is part of `releaseResources()` flow
// 4. workflow lease expiration will trigger all jobs to be cancelled including the reserving lock
func (r *WSJobReconciler) reconcileWorkflowReservation(ctx context.Context,
	wsjob *wsapisv1.WSJob, jobsInWorkflow *wsapisv1.WSJobList) error {
	if wsjob.GetWorkflowId() == "" {
		return nil
	}
	// set workflow meta if not set yet
	if !wsjob.HasWorkflowMeta() {
		isReservationFlow, parentJobName, subsidiary := r.getWorkflowMetadata(ctx, wsjob, jobsInWorkflow)
		if parentJobName != "" || isReservationFlow {
			r.Log.Info("workflow metadata retrieved",
				"workflow-id", wsjob.GetWorkflowId(),
				"current-job", wsjob.Name,
				"parent-job", parentJobName,
				"is-reservation-flow", isReservationFlow)
			wsjob.SetWorkflowMeta(isReservationFlow, parentJobName, subsidiary)
			return r.Client.Update(ctx, wsjob)
		}
	}

	// skip reservation creation/update not a active reservation flow
	if !wsjob.InReservingWorkflow() {
		// release reservation if requested
		if wsjob.UnReserved() {
			err := r.deleteLockIfExist(ctx, wsjob.Namespace, wscommon.ReservingLockName(wsjob.Name))
			if err != nil {
				return err
			}
		}
		return nil
	}

	// defer reservation check if not created/granted yet
	lock, err := r.getLock(ctx, wsjob.NamespacedName())
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	} else if !lock.IsGranted() {
		return nil
	}

	// create reservingLock lock if current job needs to be reserved
	_, err = r.createOrGetReservingLock(ctx, lock, wsjob)
	if err != nil {
		return err
	}
	return nil
}

func (r *WSJobReconciler) createOrGetReservingLock(ctx context.Context,
	lock *rlv1.ResourceLock, wsjob *wsapisv1.WSJob) (*rlv1.ResourceLock, error) {
	reserveLockName := wscommon.ReservingLockName(lock.Name)
	if lock.HasReservation() {
		reserveLockName = lock.ParentLockName()
	}
	reservationLock := &rlv1.ResourceLock{
		ObjectMeta: metav1.ObjectMeta{
			Name:        reserveLockName,
			Namespace:   lock.Namespace,
			Annotations: lock.Annotations,
			Labels:      lock.Labels,
		},
		Spec: rlv1.ResourceLockSpec{
			QueueName: rlv1.ReservationQueueName,
		},
	}
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(reservationLock), reservationLock); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		} else if wsjob.IsReserved() {
			r.Log.Info("create reserving lock from child job",
				"child", lock.Name,
				"reservingLock", reservationLock.Name,
				"workflow", wsjob.GetWorkflowId())
			// set system/crd request+affinity
			for _, rq := range lock.Spec.ResourceRequests {
				req := *rq.DeepCopy()
				if req.Name == rlv1.SystemsRequestName {
					req.Annotations = wscommon.EnsureMap(req.Annotations)
					for _, sys := range lock.Status.SystemGrants {
						req.Annotations[wsapisv1.AffinityPrefix+sys.Name] = ""
					}
				}
				reservationLock.Spec.ResourceRequests = append(reservationLock.Spec.ResourceRequests, req)
			}
			// set group request
			for _, rq := range lock.Spec.GroupResourceRequests {
				req := *rq.DeepCopy()
				reservationLock.Spec.GroupResourceRequests = append(reservationLock.Spec.GroupResourceRequests, req)
			}
			// set pop NG group type
			for _, g := range lock.Status.NodeGroupGrants {
				if strings.HasPrefix(g.Name, rlv1.PrimaryNgPrefix) && len(g.NodeGroups) > 0 {
					popType := r.Cfg.GroupMap[g.NodeGroups[0]].Properties[wsapisv1.MemxMemClassProp]
					reservationLock.SetPopulatedNgTypes([]string{popType})
					break
				}
			}
			// skip br to simplify reservation
			reservationLock.SetSkipBr()
			// set child job to current child job
			// this is a bit counter-intuitive as reservation api is called after parent job created
			// ideally in future, if reservation call is called at first, then it's more clear
			reservationLock.SetChildLock(lock.Name)
			// compile lock can only be reserved after execute granted to avoid deadlock
			if wsjob.IsCompile() {
				reservationLock.SetAsSubsidiaryReservation()
			}
			// set owner ref to job to simplify cleanup in case of job deleted
			if err = controllerutil.SetControllerReference(wsjob, reservationLock, r.Scheme); err != nil {
				r.Log.Error(err, "set reservationLock owner reference error")
				return nil, err
			}
			if err = r.Client.Create(ctx, reservationLock); err != nil && !apierrors.IsAlreadyExists(err) {
				return nil, err
			}
		} else {
			// should not happen
			r.Log.Error(nil, "reserving lock not found while child job has reservation on it",
				"child", lock.Name, "reservingLock", reservationLock.Name)
		}
	}
	return reservationLock, nil
}

// reconcileWorkflowPriority checks current cached priority and compare with the expected priority
// if not consistent, trigger reconcile event to ensure reconcile across jobs in workflow.
// The expected priority derives from:
// 1. if job/lock priority diff, return job's priority
// 2. if first case not met, return any active job's priority
// 3. last created job's priority if first two cases not met
//
// For concurrent updates, there could be more than 1 drift.
// In that case, there's determined order, and we will pick the first drifted priority seen.
// This is a known limitation without workflow CRD/heartbeat.
// Todo: optimize detection by workflow lease/crd after client side fully adopts workflow heartbeat
func (r *WSJobReconciler) reconcileWorkflowPriority(ctx context.Context, wsjob *wsapisv1.WSJob,
	jobsInWorkflow *wsapisv1.WSJobList) error {
	nsWorkflowId := fmt.Sprintf("%s/%s", wsjob.Namespace, wsjob.GetWorkflowId())
	var desiredPriority *int
	var jobLock *rlv1.ResourceLock
	var err error
	defer func() {
		// always update current lock priority after workflow level job reconcile
		if jobLock != nil && desiredPriority != nil && jobLock.GetPriority() != *desiredPriority {
			r.Log.Info("priority update on current job lock",
				"lock-name", jobLock.Name,
				"workflow-id", nsWorkflowId,
				"target-priority", *desiredPriority,
				"old-priority", jobLock.GetPriority())
			jobLock.SetPriority(*desiredPriority)
			err = r.Client.Update(ctx, jobLock)
		}
	}()

	// no workflow id case, get current lock for possible priority update
	if jobsInWorkflow == nil {
		jobsInWorkflow = &wsapisv1.WSJobList{
			Items: []wsapisv1.WSJob{*wsjob},
		}
	}
	for i := len(jobsInWorkflow.Items) - 1; i >= 0; i-- {
		j := jobsInWorkflow.Items[i]
		// If no priority found yet and this job has ended,
		// use its priority as a fallback (last resort priority)
		if desiredPriority == nil && commonutil.IsEnded(j.Status) {
			desiredPriority = j.GetPriorityPointer()
		}
		lock, err := r.getLock(ctx, j.NamespacedName())
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if j.Name == wsjob.Name {
			jobLock = lock
		}
		// Always update desiredPriority to this job's priority
		// This overwrites any previous assignments.
		// The loop goes backwards (newest to oldest), so we prefer newer jobs
		desiredPriority = j.GetPriorityPointer()

		// If there's a priority mismatch between job and lock,
		// this indicates drift - break early to handle it
		// The current desiredPriority will be used to fix the drift
		if j.GetPriority() != lock.GetPriority() {
			break
		}
	}
	if wsjob.GetWorkflowId() == "" || desiredPriority == nil {
		return nil
	}
	var cachedPriority *int
	if e, ok := r.WorkflowCache.Get(nsWorkflowId); ok {
		cachedPriority = e.(*int)
	}
	// trigger reconcile if cache diff or job diff
	if cachedPriority == nil || *desiredPriority != *cachedPriority || *desiredPriority != wsjob.GetPriority() {
		r.WorkflowCache.Add(nsWorkflowId, desiredPriority)
		if cachedPriority == nil {
			// Operator restart could happen when a new job was submitted but its lock has not been created yet.
			// In that case, we should send the priority request down the channel so that the priority sync can
			// happen after the lock is created.

			// propagate desired state if it's due to cache rebuild
			r.Log.Info("rebuild workflow priority cache, triggering workflow priority reconcile",
				"target-priority", *desiredPriority,
				"current-job-priority", wsjob.GetPriority(),
			)
		} else {
			// propagate desired state if it's due to drift
			r.Log.Info("workflow priority drift detected, triggering workflow priority reconcile",
				"workflow-id", nsWorkflowId,
				"target-priority", *desiredPriority,
				"current-job-priority", wsjob.GetPriority(),
				"cached-priority", *cachedPriority)
		}
		r.WorkflowEventChan <- event.GenericEvent{
			Object: &corev1.Event{
				ObjectMeta: metav1.ObjectMeta{
					Name:      PriorityRequestPrefix + wsjob.GetWorkflowId(),
					Namespace: wsjob.Namespace,
				},
			},
		}
	}
	return err
}

// return all jobs within workflow, sorted by creation time
func getWorkflowJobs(ctx context.Context, c client.Client, ns, workflowId string) (*wsapisv1.WSJobList, error) {
	if workflowId == "" {
		return nil, nil
	}
	jobsInWorkflow := &wsapisv1.WSJobList{}
	nsOpt := client.InNamespace(ns)
	labelSelector := client.MatchingLabels{wsapisv1.WorkflowIdLabelKey: workflowId}
	err := c.List(ctx, jobsInWorkflow, nsOpt, labelSelector)
	if err != nil {
		return nil, err
	}
	sort.Slice(jobsInWorkflow.Items, func(i, j int) bool {
		if jobsInWorkflow.Items[i].CreationTimestamp.Equal(&jobsInWorkflow.Items[j].CreationTimestamp) {
			return jobsInWorkflow.Items[i].Name < jobsInWorkflow.Items[j].Name
		}
		return jobsInWorkflow.Items[i].CreationTimestamp.Before(&jobsInWorkflow.Items[j].CreationTimestamp)
	})
	return jobsInWorkflow, nil
}

// parent job is defined as one of the following cases
// 1. reservation flow: job of same type is reserved for child job to inherit
// 2. non-reservation flow: execute job still running while new compile job running concurrently
// one key assumption is no concurrent execute jobs in same workflow
// check both cases while reservation flow takes priority
//
// getWorkflowMetadata returns whether whether it's reservation flow, parent/reserving lock name
func (r *WSJobReconciler) getWorkflowMetadata(ctx context.Context, currentJob *wsapisv1.WSJob,
	jobsInWorkflow *wsapisv1.WSJobList) (bool, string, string) {
	if jobsInWorkflow == nil || len(jobsInWorkflow.Items) == 0 {
		return false, "", ""
	}
	var firstCompileJob string
	var firstExecuteJob string
	// for reservation flow
	for i := len(jobsInWorkflow.Items) - 1; i >= 0; i-- {
		j := jobsInWorkflow.Items[i]
		if j.GetJobType() == wsapisv1.JobTypeExecute {
			firstExecuteJob = j.Name
		} else {
			firstCompileJob = j.Name
		}
		if j.NamespacedName().String() != currentJob.NamespacedName().String() &&
			j.IsReserved() && j.GetJobType() == currentJob.GetJobType() &&
			// extra check to work for envtest case
			!j.CreationTimestamp.After(currentJob.CreationTimestamp.Time) {
			r.Log.Info("parent job found with reservation status",
				"parent-job", j.Name,
				"child-job", currentJob.Name)
			return true, wscommon.ReservingLockName(j.Name), ""
		}
		// check workflow default reservation
		if i == 0 && (currentJob.Name == firstCompileJob || currentJob.Name == firstExecuteJob) {
			lease := &coordv1.Lease{}
			if err := r.Get(ctx, client.ObjectKey{
				Name:      currentJob.GetWorkflowId(),
				Namespace: currentJob.Namespace}, lease); err != nil && !apierrors.IsNotFound(err) {
				// can be retried next cycle, skip not found for other tests compatible
				return false, "", ""
			}
			if lease.Annotations[commonv1.ResourceReservationStatus] == commonv1.ResourceReservedValue {
				subsidiary := ""
				if currentJob.Name == firstExecuteJob && firstCompileJob != "" {
					subsidiary = wscommon.ReservingLockName(firstCompileJob)
					r.Log.Info("workflow enabled auto reservation for first execute job with subsidiary compile",
						"current-job", currentJob.Name,
						"subsidiary", subsidiary)
				} else {
					r.Log.Info("workflow enabled auto reservation",
						"current-job", currentJob.Name,
						"firstCompileJob", firstCompileJob,
						"firstExecuteJob", firstExecuteJob)
				}
				return true, "", subsidiary
			}
		}
	}

	// for non-reservation flow, todo: remove at rel-2.7 as train-eval flow has taken reservation since 2.5
	for i := len(jobsInWorkflow.Items) - 1; i >= 0; i-- {
		j := jobsInWorkflow.Items[i]
		if j.NamespacedName().String() == currentJob.NamespacedName().String() {
			continue
		}
		if commonutil.IsActive(j.Status) &&
			j.GetJobType() == wsapisv1.JobTypeExecute &&
			currentJob.GetJobType() == wsapisv1.JobTypeCompile &&
			// extra check to work for envtest case
			!j.CreationTimestamp.After(currentJob.CreationTimestamp.Time) {
			r.Log.Info("parent job found with running status",
				"parent-job", j.Name,
				"child-job", currentJob.Name)
			return false, j.Name, ""
		}
	}
	return false, "", ""
}

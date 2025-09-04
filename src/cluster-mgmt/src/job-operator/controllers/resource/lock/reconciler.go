/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lock

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

const (
	EventSchedulingFailed        = "SchedulingFailed"
	EventSchedulingSucceeded     = "SchedulingSucceeded"
	prioritizedCompileSupportMsg = "Please update session with more coordinator nodes to allow more compiles and retry."
)

type Reconciler struct {
	client   client.Client
	recorder record.EventRecorder

	Cfg       *common.ClusterConfig
	resources *resource.Collection

	// initialized is true if Reconciler has populated pending and grants which are a cache of the locks in the system
	initialized bool
	sync.Mutex
	newResources *resource.Collection

	// namespacedName -> lock
	locks map[string]*lock
	// namespace -> lock names list/set
	namespaceToLockMap map[string]map[string]bool
	// workflow -> lock names list/set
	workflowToLockMap map[string]map[string]bool

	queues []*queue
}

// Reconciler returns a new Reconciler with 2 queues where the Execute queue is processed first. Since the
// cluster will have a limited number of systems, systems will act as a bottleneck for scheduling. Having a
// separate queue for compile jobs allows compiles to execute with whatever cpu/mem resources are left over
// after scheduling systems.
func NewLockReconciler(client client.Client, recorder record.EventRecorder, cfg *common.ClusterConfig, resources *resource.Collection) *Reconciler {
	r := &Reconciler{
		client:    client,
		recorder:  recorder,
		Cfg:       cfg,
		resources: resources,
	}
	return r
}

// for test only
func newLockReconciler(resources *resource.Collection, cfg *common.ClusterConfig) *Reconciler {
	return &Reconciler{
		Cfg:       cfg,
		resources: resources.Copy(),
	}
}

func (r *Reconciler) UpdateResources(res *resource.Collection) {
	r.Lock()
	defer r.Unlock()
	r.newResources = res
	logrus.Infof("new resource detected in lock controller")
}

func (r *Reconciler) reloadResources() bool {
	r.Lock()
	defer r.Unlock()
	if r.newResources != nil {
		r.resources = r.newResources
		r.newResources = nil
		return true
	}
	return false
}

//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=resourcelocks,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=resourcelocks/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=jobs.cerebras.com,resources=resourcelocks/finalizers,verbs=update

// Reconcile is only triggered for create/delete of locks. rlv1.ResourceLockSpec is treated as immutable after create
// (though this is not enforced by a webhook: we assume no one edits locks manually).
// Logic:
//
//	 if new lock:
//		if not valid then reject else add to pending and do grantPending
//	 else:
//		remove lock from datastructures. if lock was in granted list, do grantPending
//
// Note that one opportunity to optimize is in that we redundantly calculate reservations for each lock grant cycle. We
// could cache reservations between cache cycles. However, this is low-priority since reconciles are ~1ms at a 1k
// system cluster.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("lockReconciler").
		WithValues("reconcileTrigger", req, "reconcileID", ctx.Value("reconcile"))
	logger.Info("begin reconcile", "reconciler", r.logRepr())
	defer func() { logger.V(1).Info("end reconcile", "reconciler", r.logRepr()) }()

	// resource reload will trigger initialization process by relist locks to get reservation
	relisted, err := r.Initialize(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	// skip individual lock check if already listed all or it's triggered by a resource/config update
	if !relisted && req.Namespace != "" {
		k8sLock, err := r.getK8sLock(ctx, req)
		if err != nil {
			return ctrl.Result{}, err
		}
		if exists(k8sLock) {
			if proceed, err := r.addLockToCache(ctx, k8sLock); err != nil || !proceed {
				return ctrl.Result{}, err
			}
		} else {
			logger.Info("lock release")
			r.removeLockFromCache(req)
		}
	}

	// deadlock check
	var requeueInterval *commonv1.RequeueAfterTTL
	proceed, err := r.deadlockCheck(ctx, requeueInterval)
	if err != nil || !proceed {
		return ctrl.Result{}, err
	}
	// trigger requeue after granting cycle to re-check by deadline
	defer func() {
		if err == nil {
			err = requeueInterval
		}
	}()

	// lock granting cycle starts
	err = r.grantPending(ctx)
	return ctrl.Result{}, err
}

func (r *Reconciler) getK8sLock(ctx context.Context, req ctrl.Request) (*rlv1.ResourceLock, error) {
	rl := &rlv1.ResourceLock{}
	if err := r.client.Get(ctx, req.NamespacedName, rl); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return rl, nil
}

func (r *Reconciler) getQueue(name string) *queue {
	for _, q := range r.queues {
		if q.name == name {
			return q
		}
	}
	return nil
}

func (r *Reconciler) Initialize(ctx context.Context) (bool, error) {
	logger, logEnd := r.debugLogger(ctx, "initialize")
	defer logEnd()
	if r.reloadResources() {
		logger.Info("new resources loaded in lock controller")
		r.initialized = false
	}
	if r.initialized {
		return false, nil
	}

	logger.Info("lock controller initialization start")
	locksByQueue := map[string][]*lock{}
	k8Locks := rlv1.ResourceLockList{}
	nsOpt := client.InNamespace("")
	if !common.EnableClusterMode || common.Namespace != common.SystemNamespace {
		nsOpt = client.InNamespace(common.Namespace)
	}
	if err := r.client.List(ctx, &k8Locks, nsOpt); err != nil {
		logger.Error(err, "k8s.listLocks failed during initialize")
		return false, err
	}
	// sort to ensure parent lock traversed first
	sort.Slice(k8Locks.Items, func(i, j int) bool {
		if k8Locks.Items[i].CreationTimestamp.Equal(&k8Locks.Items[j].CreationTimestamp) {
			return k8Locks.Items[i].Name < k8Locks.Items[j].Name
		}
		return k8Locks.Items[i].CreationTimestamp.Before(&k8Locks.Items[j].CreationTimestamp)
	})

	locks := map[string]*lock{}
	namespaceToLocks := map[string]map[string]bool{}
	workflowToLocks := map[string]map[string]bool{}

	for _, k8Lock := range k8Locks.Items {
		if prevLock, ok := r.locks[k8Lock.NamespacedName()]; ok && prevLock.IsGranted() && k8Lock.Status.State == rlv1.LockPending {
			return false, fmt.Errorf(
				"initialize found known granted lock %s in pending state. Retrying initialize after cache update",
				k8Lock.NamespacedName(),
			)
		}

		l, err := r.validateNewLock(ctx, &k8Lock, locks)
		if err != nil {
			return false, err
		} else if l == nil {
			continue
		}

		locks[l.Name] = l
		workflowToLocks[l.WorkflowId] = common.EnsureMap(workflowToLocks[l.WorkflowId])
		workflowToLocks[l.WorkflowId][l.Name] = true
		namespaceToLocks[l.Obj.Namespace] = common.EnsureMap(namespaceToLocks[l.Obj.Namespace])
		namespaceToLocks[l.Obj.Namespace][l.Name] = true
		if !l.IsGranted() {
			logger.Info("queue pending lock", "lock", l.Name, "queue", l.QueueName)
			locksByQueue[l.QueueName] = append(locksByQueue[l.QueueName], l)
		} else {
			logger.Info("initialize resources allocation from granted lock",
				"internal-lock", l.Name,
				"granted-systems", k8Lock.Status.SystemGrants,
				"granted-ngs", k8Lock.Status.NodeGroupGrants)
			r.resources.Allocate(l.Obj, l.getAllocations())
		}
	}

	var queues []*queue
	for _, name := range rlv1.OrderedQueues {
		queues = append(queues, newQueue(name, locksByQueue[name]))
	}

	r.queues = queues
	r.locks = locks
	r.namespaceToLockMap = namespaceToLocks
	r.workflowToLockMap = workflowToLocks
	r.initialized = true
	return true, nil
}

// addLockToCache validates the lock's request can be granted with some set of resources which the server controls.
// We assume that basic field validation has already been done via the CRD's validation. If invalid the lock will be
// updated to a fail state. True is returned if the reconciler should perform a grant cycle.
func (r *Reconciler) addLockToCache(ctx context.Context, rl *rlv1.ResourceLock) (bool, error) {
	newLock, err := r.validateNewLock(ctx, rl, nil)
	if err != nil || newLock == nil {
		return false, err
	}
	if !newLock.IsGranted() {
		if cachedLock := r.getCachedLock(rl.NamespacedName()); cachedLock != nil {
			// refresh lock for any update, e.g. priority/annotation update
			r.getQueue(newLock.QueueName).Requeue(newLock)
		} else {
			r.getQueue(newLock.QueueName).Add(newLock)
		}
	}
	// update cache
	r.locks[newLock.Name] = newLock
	r.namespaceToLockMap[newLock.Obj.Namespace] = common.EnsureMap(r.namespaceToLockMap[newLock.Obj.Namespace])
	r.namespaceToLockMap[newLock.Obj.Namespace][newLock.Name] = true
	r.workflowToLockMap[newLock.WorkflowId] = common.EnsureMap(r.workflowToLockMap[newLock.WorkflowId])
	r.workflowToLockMap[newLock.WorkflowId][newLock.Name] = true
	return true, nil
}

func (r *Reconciler) removeLockFromCache(req ctrl.Request) {
	namespacedName := req.NamespacedName.String()
	if l, ok := r.locks[namespacedName]; ok {
		delete(r.locks, namespacedName)
		delete(r.namespaceToLockMap[req.Namespace], namespacedName)
		delete(r.workflowToLockMap[l.WorkflowId], namespacedName)

		if !l.IsGranted() {
			r.getQueue(l.QueueName).Remove(l)
		} else {
			r.resources.LogRemainingMemoryForAxNodes(fmt.Sprintf("Before deallocation for %s", l.Name),
				l.getResIdsAllocated(), l.isInferenceJobSharingEnabled())
			r.resources.Deallocate(l.Name, l.getAllocations())
			r.resources.LogRemainingMemoryForAxNodes(fmt.Sprintf("After deallocation for %s", l.Name),
				l.getResIdsAllocated(), l.isInferenceJobSharingEnabled())
		}
	}
}

func (r *Reconciler) getCachedLock(namespacedName string) *lock {
	return r.locks[namespacedName]
}

// grantPending iterates over the queues and grants locks in FIFO order.
// To preserve FIFO ordering and allow for maximal resource utilization,
// locks in the same queue will reserve requested resources regardless if the request could be satisfied.
//
// However, queues do not share resource reservations except locks in reservation queue.
// i.e. locks in reservation queue will always be guaranteed to have reservation respected in other queues
// but other queues like execute queue's reservations will not be respected in compile queue.
// This is to avoid starvation, e.g. compile queue can still consume CPU/MEM resources from the coordinator node
// in the event that pending jobs in the execute queue already reserved 100% of the coordinator node's resources.
func (r *Reconciler) grantPending(ctx context.Context) error {
	// actual cluster resources tracking real granted usage
	clusterRes := r.resources
	// cycle copy of cluster resources will track reservation across queues within grantPending cycle
	cycleCopy := r.resources.Copy()
	// queue copy of cluster resources will track reservation within queue only
	var queueCopy *resource.Collection
	for _, q := range r.queues {
		if len(q.data) == 0 {
			continue
		}
		queueCopy = cycleCopy.Copy()
		if err := r.grantQueue(ctx, q, queueCopy, cycleCopy, clusterRes); err != nil {
			return err
		}
	}
	return nil
}

// try grant locks in one queue in one loop
func (r *Reconciler) grantQueue(ctx context.Context, q *queue,
	queueCopy, cycleCopy, clusterResource *resource.Collection) error {
	logger, endLog := r.debugLogger(ctx, "grantQueue")
	defer endLog()

	// iterate through pending grants and grant according to desired state
	// copy the queue to maintain traversal order since lock can be removed during iteration
	for _, l := range q.entryCopy() {
		if _, err := r.grantLock(ctx, logger, queueCopy, cycleCopy, clusterResource, q, l); err != nil {
			return err
		}
	}
	return nil
}

// try grant specified lock in specified queue
func (r *Reconciler) grantLock(ctx context.Context, logger logr.Logger,
	queueCopy, cycleCopy, clusterResource *resource.Collection, q *queue, l *lock) (bool, error) {
	var skipReserving bool
	var result grantResult
	var subsidiary *lock
	// resource allocation update in last step to keep code path clean
	defer func() {
		// skip reservation if failed fast or didn't go through schedule flow
		if skipReserving {
			l.clearAllocations()
			return
		}
		r.resources.LogRemainingMemoryForAxNodes(fmt.Sprintf("Before allocation for %s", l.Name),
			l.getResIdsAllocated(), l.isInferenceJobSharingEnabled())
		r.updateAllocation(queueCopy, cycleCopy, clusterResource, q, l)
		// allocate subsidiary to ensure atomic reservation
		if result.ok() && subsidiary != nil {
			r.updateAllocation(queueCopy, cycleCopy, clusterResource, q, subsidiary)
		}
		r.resources.LogRemainingMemoryForAxNodes(fmt.Sprintf("After allocation for %s", l.Name),
			l.getResIdsAllocated(), l.isInferenceJobSharingEnabled())
	}()

	// lock may have reservation
	subsidiary, resourceCandidates, proceed := r.reservationCheck(logger, queueCopy, l)
	// wait till reservation ready if exists
	if !proceed {
		skipReserving = true
		return false, nil
	}

	// try schedule candidate lock
	result = r.findCandidates(resourceCandidates, l, l.ReservingOnly())

	// reserving only lock will skip lock grant/fail update
	if l.ReservingOnly() {
		return result.ok(), r.evaluateReservation(ctx, logger, result, l)
	} else if !result.ok() {
		// grant failed
		// fail fast instead of queueing if workflow child job can't schedule with reservation
		proceed, err := r.failFastIfNecessary(ctx, logger, result, l)
		if err != nil || !proceed {
			skipReserving = true
			return result.ok(), err
		}
		// put back in queue
		logger.Info("unable to grant in this reconciling cycle, put back to queue",
			"candidateLock", l.Name, "queue", l.QueueName, "details", result.String())
		return result.ok(), nil
	} else if l.Obj.GetAssignedRedundancy() != nil {
		// grant success
		// redundancy constraint check before granting
		sessionGrantedLocks, err := r.checkRedundancyConstraint(logger, l)
		if err != nil {
			logger.Info("redundancy disallowed due to exceeding local capacity, put back to queue",
				"candidateLock", l.Name, "queue", l.QueueName,
				"sessionGrantedLocks", sessionGrantedLocks, "error", err)
			l.granted = false
			return result.ok(), nil
		} else if sessionGrantedLocks != nil {
			logger.Info("redundancy allowed",
				"candidateLock", l.Name, "queue", l.QueueName,
				"sessionGrantedLocks", sessionGrantedLocks)
		}
	}

	// grant success, update lock status
	if err := r.setLockGrantedInK8s(ctx, l); err != nil {
		skipReserving = true
		return result.ok(), err
	}
	l.EmitInfJobSharingMemLimitMetrics()

	logger.Info("lock grant success",
		"name", l.Name,
		"queue", l.QueueName,
		"requests", l.ResourceRequests,
		"grants", l.ResourceGrants,
		"group-requests", l.GroupRequests,
		"group-grants", l.GroupGrants,
		"chf-requests", l.ChfGroupRequests,
		"chf-grants", l.ChfGroupGrants,
		"act-requests", l.ActGroupRequests,
		"act-grants", l.ActGroupGrants,
		"wgt-cmd-requests", l.WgtCmdGroupRequests,
		"wgt-cmd-grants", l.WgtCmdGroupGrants,
		"upper-bounds", l.GroupGrantUpperBounds,
		"br-request", l.BrGroupRequest,
		"br-grant", l.BrGroupGrant,
		"br-tree-policy", l.BrLogicalTreePolicy,
		"br-logical-tree", l.BrLogicalTree.String(),
		"kvss-requests", l.KvssGroupRequests,
		"kvss-grants", l.KvssGroupGrants)

	return result.ok(), nil
}

func (r *Reconciler) updateAllocation(queueCopy, cycleCopy, clusterResource *resource.Collection, q *queue, l *lock) {
	// always reserve fully/partial granted resource to ensure FIFO scheduling in current cycle
	queueCopy.AllocateForced(l.Obj, l.Obj.GetSessionFilter(), l.getAllocations(),
		l.RequireFullAllocation(), common.EnableClusterMode)
	// update on cycle/cluster resource if fully allocated
	if l.RequireFullAllocation() {
		if cycleCopy != nil && cycleCopy != queueCopy {
			cycleCopy.Allocate(l.Obj, l.getAllocations())
		}
		// only track fully granted resources for actual cluster resource usage for session side check
		// e.g. resource can be removed from session if it's not actually in use
		if !l.ReservingOnly() {
			clusterResource.Allocate(l.Obj, l.getAllocations())
			// dequeue from pending locks
			q.Remove(l)
		}
	}
}

// job can inherit reserved resources when conditions met
func (r *Reconciler) reservationCheck(logger logr.Logger, c *resource.Collection, l *lock) (*lock, *resource.Collection, bool) {
	resourceCandidates := c
	// if current job is in reserving only mode, proceed to schedule phase if enabled
	if l.ReservingOnly() {
		if !r.reservingIsEnabled(logger, l) {
			return nil, nil, false
		}
		logger.Info(
			"start reserving resources for child jobs",
			"reservingLock", l.Name,
			"childLock", l.ChildLockNsName)
		return nil, resourceCandidates, true
	}

	// if current job try to inherit reservation, proceed to schedule phase if parent reserving lock enabled
	parentLock := r.getCachedLock(l.ParentLockNsName)
	if parentLock != nil && parentLock.ReservingOnly() {
		// if reservation not enabled yet, current job should queue till it ready
		// this will help execute job to have system affinity with parent job
		// this also avoid compiles from same workflow oversubscribe on compile resources
		if !r.reservingIsEnabled(logger, parentLock) {
			logger.Info(
				"job reservation disabled, defer inheriting till it's ready",
				"queue", l.QueueName,
				"currentLock", l.Name,
				"reservingLock", l.ParentLockNsName,
				"childLock", parentLock.ChildLockNsName)
			return nil, nil, false
		}
		resourceCandidates = resourceCandidates.Copy()
		resourceCandidates.Deallocate(parentLock.Name, parentLock.getAllocations())
		// inherit affinity on parent job systems
		for k, v := range parentLock.ResourceRequests[rlv1.SystemsRequestName].GetAffinities() {
			l.ResourceRequests[rlv1.SystemsRequestName].AddAffinity(k, v, false)
		}
		logger.Info(
			"job reservation enabled, start inheriting",
			"queue", l.QueueName,
			"currentLock", l.Name,
			"reservingLock", l.ParentLockNsName,
			"childLock", parentLock.ChildLockNsName)
		return nil, resourceCandidates, true
	}

	// if current lock has subsidiary dependency, proceed to schedule if subsidiary grantable to ensure atomic reservation
	if subsidiary := r.getCachedLock(l.SubsidiaryLockNsName); subsidiary != nil {
		logger.Info("reserving subsidiary compile lock with first execute together",
			"subsidiary", subsidiary.Name, "first-execute", l.Name)
		resourceCandidates = resourceCandidates.Copy()
		// enforce the grant to allow the subsidiary lock to proceed to scheduling
		// otherwise it will skip as current first execute job not granted yet
		l.granted = true
		defer func() { l.granted = false }()
		granted, _ := r.grantLock(context.Background(), logger,
			resourceCandidates, nil, nil, nil, subsidiary)
		if !granted {
			logger.Info("failed to reserve subsidiary compile lock",
				"subsidiary", subsidiary.Name, "first-execute", l.Name)
			return nil, nil, false
		}
		logger.Info("reserving subsidiary compile lock success, "+
			"proceed to schedule first execute job",
			"subsidiary", subsidiary.Name, "first-execute", l.Name)
		return subsidiary, resourceCandidates, true
	}
	return nil, resourceCandidates, true
}

// fail fast for restart flow to keep system utilization high
func (r *Reconciler) failFastIfNecessary(ctx context.Context, logger logr.Logger,
	result grantResult, l *lock) (bool, error) {
	parentLock := r.getCachedLock(l.ParentLockNsName)
	// fail lock if child job with reserved resources failed to schedule
	if !result.ok() && parentLock != nil && parentLock.ReservingOnly() {
		msg := "child job failed to schedule with reservation, " +
			"likely due to reserved resources became unhealthy, " +
			"skip queueing and fail fast to allow client retry. "
		logger.Info(msg,
			"queue", l.QueueName,
			"currentLock", l.Name,
			"reservingLock", l.ParentLockNsName,
			"details", result.String())
		if err := r.setLockErrorInK8s(ctx, l.Obj, result.String()); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// check reserving status and update system count if necessary
func (r *Reconciler) evaluateReservation(ctx context.Context, logger logr.Logger,
	result grantResult, l *lock) error {
	rl := l.Obj
	message := fmt.Sprintf("system-grant: %d, ng-grant-shortage: %d, ax-shortage: %d, details: %s",
		result.systemGrantCount(), result.groupGrantShortage, result.axGrantShortage, result.String())
	logger.Info("reserving lock evaluated",
		"reservingLock", l.Name,
		"status-queried", rl.ReservationQueried(),
		"fully-granted", result.ok(),
		"message", message)
	if rl.ReservationQueried() {
		sysCount := result.systemRequestCount()
		sysGrantShortage := result.systemGrantShortage()
		ngGrantShortage := result.groupGrantShortage
		axGrantShortage := result.axGrantShortage
		// best effort to get blocking resource shortage to get higher chance of retry success
		grant := sysCount - common.MaxInArray([]int{sysGrantShortage, ngGrantShortage, axGrantShortage})
		rl.SetReservedSystems(grant, message)
		if err := r.client.Update(ctx, rl); err != nil {
			logger.Error(err, "failed to update lock reserved system count")
			return err
		}
	}
	return nil
}

// findCandidates attempts to find resources that match the lock's ResourceRequests and GroupRequests.
// If grant is found, the exact match will be returned.
// If not, the candidate resources considered for the grant will be returned.
func (r *Reconciler) findCandidates(c *resource.Collection, l *lock, grantableCheckOnly bool) grantResult {
	result := grantResult{name: l.Obj.NamespacedName()}
	// schedule systems and a coordinator
	result.resourceGrants, result.resourceErrors = c.FindUngroupedScheduling(l.ResourceRequests)

	// schedule AXs and BRs based on system affinity
	r.scheduleForSystemAffinity(c, l, &result)

	// schedule primary and secondary nodegroups
	r.scheduleNodegroups(c, l, &result)

	// attempt allocating optional requests
	if !grantableCheckOnly && result.ok() {
		r.tryAllocateOptionalRequests(c, l, &result)
	}

	l.Allocations = resource.GatherAllocations(result.resourceGrants, result.mergeGroupGrants())
	// assign grant result
	l.granted = false
	if result.ok() && !grantableCheckOnly {
		l.granted = true
		l.ResourceGrants = result.resourceGrants
		l.BrGroupGrant = result.brGroupGrant
		l.ChfGroupGrants = result.chfGroupGrants
		l.ActGroupGrants = result.actGroupGrants
		l.WgtCmdGroupGrants = result.wgtCmdGroupGrants
		l.KvssGroupGrants = result.kvssGroupGrants
		l.SWDriverGroupGrants = result.swDriverGroupGrants
		l.GroupGrants = result.groupGrants
		l.GroupGrantUpperBounds = result.upperBounds
	}

	// return any matched groups to do FIFO reservation
	return result
}

func (r *Reconciler) setLockGrantedInK8s(ctx context.Context, l *lock) error {
	logger, endLog := r.debugLogger(ctx, "setLockGrantedInK8s")
	defer endLog()
	rl := l.toK8s()
	logger.Info("update lock status start", "lock", rl.Name, "status", rl.Status)
	if err := r.client.Status().Update(ctx, rl); err != nil {
		// Could fail because the lock was deleted or updated in the intervening time, or k8s is unreachable/broken.
		// In all these cases, a re-reconcile should allow forward progress.
		logger.Error(err, "failed to update lock status, put back in queue and retry")
		return err
	}
	logger.Info("lock status update success", "lock", rl.Name)
	l.Obj = rl
	r.recorder.Event(rl, corev1.EventTypeNormal, EventSchedulingSucceeded, "granted lock "+l.Name)
	return nil
}

func (r *Reconciler) setLockErrorInK8s(ctx context.Context, rl *rlv1.ResourceLock, msgf string, args ...interface{}) error {
	logger, endLog := r.debugLogger(ctx, "setLockErrorInK8s")
	defer endLog()

	msg := fmt.Sprintf(msgf, args...)
	logger.Info("reject lock request", "reason", EventSchedulingFailed, "message", msg)
	r.recorder.Event(rl, corev1.EventTypeWarning, EventSchedulingFailed, msg)

	rl.Status.State = rlv1.LockError
	rl.Status.Message = rlv1.LockStatusMessage(msg)
	if err := r.client.Status().Update(ctx, rl); err != nil {
		logger.Error(err, "failed to patch lock status.state=Error")
		return err
	}
	return nil
}

func (r *Reconciler) debugLogger(ctx context.Context, method string) (logr.Logger, func()) {
	logger := log.FromContext(ctx, "method", method)
	if logger.Enabled() {
		logger.V(1).Info("begin " + method)
		return logger, func() { logger.V(1).Info("end "+method, "reconciler", r.logRepr()) }
	}
	return logger, func() {}
}

// validateNewLock checks if the lock's requests are valid (have unique names, etc). It also checks if the lock can be
// satisfied by the system using all the capacity of the system. If the lock is valid, it is returned as a cacheEntry.
func (r *Reconciler) validateNewLock(ctx context.Context, rl *rlv1.ResourceLock,
	existingLocks map[string]*lock) (*lock, error) {
	logger, endLog := r.debugLogger(ctx, "validateNewLock")
	defer endLog()
	var err error
	var l *lock

	if rl.Status.State == rlv1.LockError {
		return nil, nil
	}

	l, err = NewLockFromK8s(rl)
	if err != nil {
		logger.Info("NewLockFromK8s error", "lock", rl.NamespacedName(), "err", err.Error())
		return nil, r.setLockErrorInK8s(ctx, rl, err.Error())
	}

	if !slices.Contains(rlv1.OrderedQueues, l.QueueName) { // this should not happen because of API validation, but just in case...
		return nil, r.setLockErrorInK8s(
			ctx, rl, "requested queue '%s' was not found. Options: %v", l.QueueName, rlv1.OrderedQueues)
	}

	if rl.ParentLockName() != "" {
		if existingLocks == nil {
			existingLocks = r.locks
		}
		l.ParentLockNsName = rl.Namespace + "/" + rl.ParentLockName()
		if parentLock := existingLocks[l.ParentLockNsName]; parentLock != nil {
			l.ParentLockCreateTime = parentLock.CreateTime
		} else if rl.IsPending() && rl.HasReservation() {
			// parent should exist if pending lock has reservation
			logger.Error(nil, "Reserving lock not found",
				"currentLock", l.Name,
				"reserving lock", rl.ParentLockName(),
				"existing locks", common.Keys(existingLocks))
			return nil, r.setLockErrorInK8s(
				ctx, rl,
				"Reserving lock %s not found for job %s",
				rl.ParentLockName(),
				rl.Name)
		}
	} else if rl.ChildLockName() != "" {
		l.ChildLockNsName = rl.Namespace + "/" + rl.ChildLockName()
	} else if rl.GetSubsidiaryReservationName() != "" {
		l.SubsidiaryLockNsName = rl.Namespace + "/" + rl.GetSubsidiaryReservationName()
	}

	if rl.Status.State == rlv1.LockPending {
		copyGroupReq := func(gr map[string]resource.GroupRequest) map[string]resource.GroupRequest {
			copied := map[string]resource.GroupRequest{}
			for k, v := range gr {
				copied[k] = v.Copy()
			}
			return copied
		}
		originalGroupReq := l.GroupRequests
		grantable := false
		mostProgress := NoProgress
		var finalError error

		// todo: push down to node group schedule side to keep scheduling logic in same place
		for _, option := range l.GetAllowedPopGroupOptions() {
			// make copy of original group request before redistribute
			l.GroupRequests = copyGroupReq(originalGroupReq)
			l.PopGroupTypes = option.popNgTypes
			l.PopGroupCount = option.popNgCount

			// Redistributing weight and worker tasks is only relevant in a multi-pop-group grant in V2 networking.
			if wsapisv1.IsV2Network && l.PopGroupCount > 1 {
				l.redistributeTasks()
			}

			result := grantResult{name: rl.NamespacedName()}
			if result, err = r.checkGrantable(l, nil); err != nil {
				logger.V(1).Info("checkGrantable error with attempted popNG type",
					"attempted-popNG-type", option, "lock", rl.NamespacedName(), "err", err.Error())
				// memoize the best progress made
				if result.groupErrorType > mostProgress {
					mostProgress = result.groupErrorType
					finalError = err
				} else if result.groupErrorType == mostProgress &&
					mostProgress != InsufficientPopGroups && mostProgress != InsufficientTotalGroups {
					// if "Insufficient*Groups" was the best progress, the first error is usually more valuable of showing
					// todo: populate the decision on not enough regular pop groups or have more fine grained progress
					mostProgress = result.groupErrorType
					finalError = err
				}
			} else {
				grantable = true
				logger.Info("checkGrantable succeeded",
					"lock", rl.NamespacedName(),
					"pop-group-types", l.PopGroupTypes,
					"pop-group-count", l.PopGroupCount)
				break
			}
		}

		if !grantable {
			// reserving lock should never error out as it can be partially reserved
			if l.ReservingOnly() {
				return l, nil
			}
			return nil, r.setLockErrorInK8s(ctx, rl, finalError.Error())
		}
	}
	return l, nil
}

func (r *Reconciler) checkGrantable(l *lock, c *resource.Collection) (grantResult, error) {
	resourceCandidates := c
	if c == nil {
		resourceCandidates = r.resources.UnallocatedCopy()
	}
	// for lock with shared sessions, disallow job exceeding local session's capacity
	// also for simplification to check grantable in one pass, lock won't fail due to unhealthy resource
	// ignore health-prop/system-port-subresource to avoid failing at grantable check as long as capacity fits
	skipHealthFilter := false
	if sessions := l.Obj.GetAssignedRedundancy(); len(sessions) > 0 {
		skipHealthFilter = true
		resourceCandidates = resourceCandidates.Filter(resource.NewSessionFilter(l.Obj.Namespace))
		resourceCandidates.AddIgnorePropKey(resource.HealthProp)
		resourceCandidates.AddIgnoreSubresKey(resource.SystemPortSubresource)
		defer resourceCandidates.ClearIgnoreKeys()
	}
	result := r.findCandidates(resourceCandidates, l, true)
	if !result.ok() {
		healthRelatedError := ""
		clusterHealthDetails := ""
		if r.Cfg != nil && r.Cfg.HasUnhealthyResources() && !skipHealthFilter {
			healthRelatedError = " possibly due to unhealthy resources"
			clusterHealthDetails = r.Cfg.PrettyHealthInfo(l.Obj.Namespace, result.brGroupError != nil)
		}
		return result, fmt.Errorf("cluster lacks requested capacity%s: %s %s",
			healthRelatedError, result.String(), clusterHealthDetails)
	}
	return result, nil
}

// redundancy constraint check, e.g.
// for 4 system session(1 system down) with enough NG/CRD/AX... + 2 system redundancy with enough NG/CRD/AX...
// job-a of 4 box coming in and takes all 3 local healthy systems + 1 redundancy system
// job-b of 1 box coming in, 2 cases on whether it can be scheduled
// 1. if local system still down, job-b should be queued waiting for job-a to finish
// this avoids excessive usage on redundancy exceeding local session capacity
// otherwise all redundancy can be used by one small session unfairly
// 2. if local system comes back healthy, job-b will be allowed to schedule
// this ensures high system utilization by temporarily exceeding local session capacity
// idea is that if all usage are local, there's no side effect on redundancy contention and it's wasteful to not use it
// https://cerebras.atlassian.net/wiki/spaces/runtime/pages/4131881055/Redundancy+pool+usage+guidance
func (r *Reconciler) checkRedundancyConstraint(logger logr.Logger, l *lock) ([]string, error) {
	var redundancyUsed bool
	for rid := range l.getAllocations() {
		rs, _ := r.resources.Get(rid)
		if slices.Contains(l.Obj.GetAssignedRedundancy(), rs.GetNamespace()) {
			redundancyUsed = true
			break
		}
	}
	// allow scheduling for case 2
	if !redundancyUsed {
		return nil, nil
	}
	// capacity check for case 1
	var sessionLocks []*lock
	var sessionLocksNames []string
	for name := range r.namespaceToLockMap[l.Obj.Namespace] {
		ll := r.getCachedLock(name)
		// allocate all granted locks + effective reserving locks
		if ll.IsGranted() || r.reservingIsEnabled(logger, ll) {
			// copy to avoid contaminating original grant results
			grantLock := ll.Copy()
			// skip br to speed up as it's not part of session
			grantLock.SkipBr = true
			sessionLocks = append(sessionLocks, grantLock)
			sessionLocksNames = append(sessionLocksNames, grantLock.Name)
		}
	}
	// skip capacity check if current lock is the only one since already checked at grantable admission stage
	if len(sessionLocksNames) == 1 && sessionLocksNames[0] == l.Name {
		return nil, nil
	}
	if err := r.batchGrantableCheck(sessionLocks, r.resources.UnallocatedCopy()); err != nil {
		return sessionLocksNames, err
	}
	return sessionLocksNames, nil
}

// capacity check on whether all given locks can fit in specified resources
func (r *Reconciler) batchGrantableCheck(locks []*lock, c *resource.Collection) error {
	for _, l := range locks {
		_, err := r.checkGrantable(l, c)
		if err != nil {
			return fmt.Errorf("check grantable failed for %s: %s", l.Name, err)
		}
		c.Allocate(l.Obj, l.getAllocations())
	}
	return nil
}

// reserving will dynamically switch between enabled/disabled status as workflow jobs brought up/down
// enabled when original child lock has completed and new child job not granted, disabled otherwise
func (r *Reconciler) reservingIsEnabled(logger logr.Logger, reservingLock *lock) bool {
	if !reservingLock.ReservingOnly() {
		return false
	}
	newGrantedFound := false
	for name := range r.workflowToLockMap[reservingLock.WorkflowId] {
		if name == reservingLock.Name {
			continue
		}
		if ll := r.getCachedLock(name); ll != nil {
			if ll.IsGranted() &&
				(ll.ParentLockNsName == reservingLock.Name || reservingLock.ChildLockNsName == ll.Name) {
				logger.Info(
					"reservation disabled as inherited by child job",
					"reservingLock", reservingLock.Name,
					"childLock", ll.Name)
				return false
			}
			if ll.ReservingOnly() || ll.IsGranted() {
				newGrantedFound = true
			}
		}
	}
	// subsidiary reserving lock requires separate reserving/granted lock to enable
	if reservingLock.Obj.IsSubsidiaryReservation() && !newGrantedFound {
		logger.Info(
			"reservation disabled as compile can only be reserved when execute granted, skip scheduling",
			"reservingLock", reservingLock.Name,
			"workflow-locks", common.Keys(r.workflowToLockMap[reservingLock.WorkflowId]))
		return false
	}
	return true
}

// createTime returns an overridden the CreationTimestamp. For use in tests only.
func createTime(l *rlv1.ResourceLock) time.Time {
	if override, ok := l.Labels["createTime"]; ok {
		t, err := strconv.ParseInt(override, 10, 64)
		if err == nil {
			return time.UnixMilli(t)
		}
	}
	return l.CreationTimestamp.Time
}

// logRepr returns a map of the reconciler's state for logging purposes.
func (r *Reconciler) logRepr() map[string]interface{} {
	var granted []string
	for _, l := range r.locks {
		if l.IsGranted() {
			granted = append(granted, l.Name)
		}
	}
	var pendingByQ []string
	for _, q := range r.queues {
		pendingByQ = append(pendingByQ, q.String())
	}
	return map[string]interface{}{
		"granted": granted,
		"pending": pendingByQ,
	}
}

func (r *Reconciler) hasPendingLocks() bool {
	for _, q := range r.queues {
		if len(q.data) > 0 {
			return true
		}
	}
	return false
}

// for test purpose only
func (r *Reconciler) RemoveResource(name string) {
	r.resources.Remove(name)
}

// for test purpose only
func (r *Reconciler) GetResource(name string) (resource.Resource, bool) {
	return r.resources.Get(name)
}

// for test purpose only
func (r *Reconciler) GetCollection() *resource.Collection {
	return r.resources
}

func exists[V any](v *V) bool {
	return v != nil
}

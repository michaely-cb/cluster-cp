//go:build integration && redundancy_sessions

package ws

import (
	"context"
	"fmt"
	"strings"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

func (t *TestSuite) TestRedundancySessions() {
	ctx := context.Background()
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	wsapisv1.SkipIngressReconcile = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
		wsapisv1.SkipIngressReconcile = false
	}()
	deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
	// 2 stamps of systems/AX: (0,2,4,6), (1,3,5,7)
	cfg := wscommon.NewTestClusterConfigSchema(systemCount, 3,
		brCount, systemCount, 1, 1, memoryXPerGroup).SplitNStamps(2, 4)

	t.NoError(wscommon.CreateK8sNodes(cfg, t.JobReconciler.Client))
	t.NoError(t.CfgMgr.Initialize(cfg))
	t.unassignEverythingFromNS(wscommon.Namespace)

	// setup shared session + 2 user sessions
	redundantNs := "redundant-session"
	sharedSystem := 2
	// assign systems of 0,2 in same stamp
	t.createNsIfNotExists(redundantNs)
	t.updateSessionLabel(redundantNs, namespacev1.IsRedundantModeKey, "true")
	t.assignNS(redundantNs, namespacev1.SetResourceSpecMode, nil, sharedSystem, 1)
	require.Eventually(t.T(), func() bool {
		return len(t.CfgMgr.Cfg.GetAssignedSystemsList(redundantNs)) == sharedSystem
	}, eventuallyTimeout, eventuallyTick)
	// assign systems of 1,3,5 in same stamp
	userNs0 := "user-session-0"
	userSystem := 3
	t.createNsIfNotExists(userNs0)
	t.updateSessionLabel(userNs0, namespacev1.AssignedRedundancyKey, redundantNs)
	t.assignNS(userNs0, namespacev1.SetResourceSpecMode, nil, userSystem, 2)
	require.Eventually(t.T(), func() bool {
		t.T().Log(t.CfgMgr.Cfg.GetNsSysNameMap())
		return len(t.CfgMgr.Cfg.GetAssignedSystemsList(userNs0)) == userSystem
	}, eventuallyTimeout, eventuallyTick)
	userNs1 := "user-session-1"
	user1System := 3
	// assign rest of systems in 2 stamps (4,6) (7)
	t.createNsIfNotExists(userNs1)
	t.updateSessionLabel(userNs1, namespacev1.AssignedRedundancyKey, redundantNs)
	t.assignNS(userNs1, namespacev1.SetResourceSpecMode, nil, user1System, 1)
	require.Eventually(t.T(), func() bool {
		t.T().Log(t.CfgMgr.Cfg.GetNsSysNameMap())
		return len(t.CfgMgr.Cfg.GetAssignedSystemsList(userNs1)) == user1System
	}, eventuallyTimeout, eventuallyTick)
	defer func() {
		assertLockCountEventually(t.T(), 0)
		t.unassignEverythingFromNS(userNs0)
		t.unassignEverythingFromNS(userNs1)
		t.unassignEverythingFromNS(redundantNs)
		t.assignEverythingToNS(wscommon.Namespace, systemCount)
		require.Eventually(t.T(), func() bool {
			t.T().Log(t.CfgMgr.Cfg.GetNsSysNameMap())
			return len(t.CfgMgr.Cfg.GetAssignedSystemsList(wscommon.Namespace)) == systemCount
		}, eventuallyTimeout, eventuallyTick)
		require.NoError(t.T(), k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: ctrl.ObjectMeta{Name: userNs0}}))
		require.NoError(t.T(), k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: ctrl.ObjectMeta{Name: userNs1}}))
		require.NoError(t.T(), k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: ctrl.ObjectMeta{Name: redundantNs}}))
	}()

	// take down 1 nodegroup from session-0
	nodeName := fmt.Sprintf("%s-worker-0", t.CfgMgr.Cfg.GetAssignedGroupsList(userNs0)[0])
	require.NoError(t.T(), patchNode(t, nodeName,
		[]corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeErrorAlertType,
			Status: corev1.ConditionTrue,
		}}))
	// take down 2 systems from session-1 (one with port error)
	s4 := &systemv1.System{}
	require.NoError(t.T(), k8sClient.Get(ctx,
		client.ObjectKey{Name: t.CfgMgr.Cfg.GetAssignedSystemsList(userNs1)[0]}, s4))
	s4.Status.Conditions = []corev1.NodeCondition{
		{
			Type:               wscommon.ClusterMgmtConditionPrefix + wscommon.SystemPortAlertType + "_n00",
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			LastHeartbeatTime:  metav1.Now(),
		}}
	require.NoError(t.T(), k8sClient.Status().Update(ctx, s4))
	s6 := &systemv1.System{}
	require.NoError(t.T(), k8sClient.Get(ctx,
		client.ObjectKey{Name: t.CfgMgr.Cfg.GetAssignedSystemsList(userNs1)[1]}, s6))
	s6.Spec.Unschedulable = true
	require.NoError(t.T(), k8sClient.Update(ctx, s6))
	require.Eventually(t.T(), func() bool {
		return len(t.ResourceController.Cfg.UnhealthySystems) == 2 && len(t.ResourceController.Cfg.UnhealthyNodes) == 1
	}, eventuallyTimeout, eventuallyTick)
	// mock system version check
	getVer := wscommon.GetSystemVersions
	versionMap := map[string][]string{
		"rel-2.4": {"system-3"},
		"master":  {"system-5"},
	}
	wscommon.GetSystemVersions = func(ctx context.Context, metricsQuerier wscommon.MetricsQuerier,
		systemNames []string, parse, buildId bool) (map[string]string, map[string][]string) {
		return nil, versionMap
	}
	defer func() {
		wscommon.GetSystemVersions = getVer
		s4.Status.Conditions = nil
		require.NoError(t.T(), k8sClient.Status().Update(ctx, s4))
		s6.Spec.Unschedulable = false
		require.NoError(t.T(), k8sClient.Update(ctx, s6))
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 0 && len(t.ResourceController.Cfg.UnhealthyNodes) == 0
		}, eventuallyTimeout, eventuallyTick)
	}()

	// helper function
	validateLocalAffinity := func(lock *rlv1.ResourceLock) {
		var assignedSystems []string
		jobStamps := map[string]bool{}
		sessionStamps := map[string]bool{}
		for _, sys := range lock.Status.SystemGrants {
			assignedSystems = append(assignedSystems, sys.Name)
			jobStamps[t.CfgMgr.Cfg.SystemMap[sys.Name].GetStamp()] = true
		}
		// expect always pick local systems except when unhealthy
		for _, sys := range t.CfgMgr.Cfg.GetAssignedSystemsList(lock.Namespace) {
			sessionStamps[t.CfgMgr.Cfg.SystemMap[sys].GetStamp()] = true
			if t.CfgMgr.Cfg.UnhealthySystems[sys] != nil {
				require.NotContains(t.T(), assignedSystems, sys)
			} else {
				require.Contains(t.T(), assignedSystems, sys)
			}
		}
		var assignedGroups []string
		for _, g := range lock.Status.NodeGroupGrants {
			for _, gg := range g.NodeGroups {
				assignedGroups = append(assignedGroups, gg)
			}
		}
		// expect always pick local groups except when unhealthy
		for _, grp := range t.CfgMgr.Cfg.GetAssignedGroupsList(lock.Namespace) {
			if t.CfgMgr.Cfg.UnhealthyNodes[grp+"-worker-0"] != nil {
				require.NotContains(t.T(), assignedGroups, grp)
			} else {
				require.Contains(t.T(), assignedGroups, grp)
			}
		}
		assignedAx := map[string]bool{}
		for _, group := range lock.Status.GroupResourceGrants {
			if strings.Contains(group.Name, rlv1.ActivationRequestName) {
				for _, r := range group.ResourceGrants {
					for _, rr := range r.Resources {
						assignedAx[rr.Name] = true
					}
				}
			}
		}
		// expect always pick local ax if stamp affinity can be met locally
		if len(sessionStamps) == len(jobStamps) {
			for _, no := range t.CfgMgr.Cfg.GetAssignedNodesMap(lock.Namespace) {
				if no.HasRole(wscommon.RoleActivation) {
					require.Contains(t.T(), assignedAx, no.Name)
				}
			}
		}
		t.T().Log(lock.Name, jobStamps,
			assignedSystems, assignedGroups, wscommon.SortedKeys(assignedAx))
		t.T().Log(lock.Namespace,
			t.CfgMgr.Cfg.GetAssignedSystemsList(lock.Namespace),
			t.CfgMgr.Cfg.GetAssignedGroupsList(lock.Namespace),
			t.CfgMgr.Cfg.GetAssignedNodesList(lock.Namespace))
	}

	// user job from first user session reserved in workflow
	job := newWsExecuteJob(userNs0, "user-ns0-job-0", uint32(userSystem))
	job.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReservedValue
	job.Labels[wsapisv1.WorkflowIdLabelKey] = "workflow"
	require.NoError(t.T(), k8sClient.Create(ctx, job))
	defer assertWsjobCleanedUp(t.T(), job)
	lock := assertLockExists(t.T(), job.Namespace, job.Name)
	assertLocked(t.T(), lock)
	validateLocalAffinity(lock)
	assertLockExists(t.T(), job.Namespace, wscommon.ReservingLockName(job.Name))
	cancelJobEventually(t.T(), job, wsapisv1.CancelWithStatusFailed)
	job = assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
	require.True(t.T(), job.IsReserved())
	defer func() {
		job.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReleasedValue
		require.NoError(t.T(), k8sClient.Update(ctx, job))
		assertLockNotExists(t.T(), job.Namespace, wscommon.ReservingLockName(job.Name))
	}()

	// takes down 2 systems and valid restart job should be able to schedule with reserved redundancy
	s1 := &systemv1.System{}
	require.NoError(t.T(), k8sClient.Get(ctx,
		client.ObjectKey{Name: t.CfgMgr.Cfg.GetAssignedSystemsList(userNs0)[0]}, s1))
	s1.Spec.Unschedulable = true
	require.NoError(t.T(), k8sClient.Update(ctx, s1))
	s3 := &systemv1.System{}
	require.NoError(t.T(), k8sClient.Get(ctx,
		client.ObjectKey{Name: t.CfgMgr.Cfg.GetAssignedSystemsList(userNs0)[1]}, s3))
	s3.Spec.Unschedulable = true
	require.NoError(t.T(), k8sClient.Update(ctx, s3))
	require.Eventually(t.T(), func() bool {
		return len(t.ResourceController.Cfg.UnhealthySystems) == 4
	}, eventuallyTimeout, eventuallyTick)
	defer func() {
		s1.Spec.Unschedulable = false
		require.NoError(t.T(), k8sClient.Update(ctx, s1))
		s3.Spec.Unschedulable = false
		require.NoError(t.T(), k8sClient.Update(ctx, s3))
	}()
	jobChild := newWsExecuteJob(userNs0, "user-ns0-job-0-child", uint32(userSystem))
	jobChild.Labels[wsapisv1.WorkflowIdLabelKey] = "workflow"
	require.NoError(t.T(), k8sClient.Create(ctx, jobChild))
	defer assertWsjobCleanedUp(t.T(), jobChild)
	lock = assertLockExists(t.T(), jobChild.Namespace, jobChild.Name)
	assertLocked(t.T(), lock)
	validateLocalAffinity(lock)

	// another user job from first session queued as constrained by local session capacity
	queueJob := newWsExecuteJob(userNs0, "user-ns0-job-1", 1)
	require.NoError(t.T(), k8sClient.Create(ctx, queueJob))
	defer assertWsjobCleanedUp(t.T(), queueJob)
	lock = assertLockExists(t.T(), queueJob.Namespace, queueJob.Name)
	assertNeverGranted(t.T(), lock)

	// one local system comes back, job should be scheduled as temporary exceeding capacity is allowed
	s3.Spec.Unschedulable = false
	require.NoError(t.T(), k8sClient.Update(ctx, s3))
	require.NoError(t.T(), patchNode(t, nodeName, nil))
	require.Eventually(t.T(), func() bool {
		return len(t.ResourceController.Cfg.UnhealthySystems) == 3 && len(t.ResourceController.Cfg.UnhealthyNodes) == 0
	}, eventuallyTimeout, eventuallyTick)
	lock = assertLockExists(t.T(), queueJob.Namespace, queueJob.Name)
	assertLocked(t.T(), lock)
	assert.Equal(t.T(), s3.Name, lock.Status.SystemGrants[0].Name)

	// another user job from first session queued as exceeding local capacity
	queueJob1 := newWsExecuteJob(userNs0, "user-ns0-job-2", 2)
	require.NoError(t.T(), k8sClient.Create(ctx, queueJob1))
	defer assertWsjobCleanedUp(t.T(), queueJob1)
	lock = assertLockExists(t.T(), queueJob1.Namespace, queueJob1.Name)
	assertNeverGranted(t.T(), lock)
	// cancel child job+queue job but new job still blocked due to workflow reservation caused exceeding capacity
	cancelJobEventually(t.T(), jobChild, wsapisv1.CancelWithStatusSucceeded)
	cancelJobEventually(t.T(), queueJob, wsapisv1.CancelWithStatusSucceeded)
	assertNeverGranted(t.T(), lock)

	// second session's user job
	job1Ns1 := newWsExecuteJob(userNs1, "user-ns1-job-1", uint32(user1System))
	require.NoError(t.T(), k8sClient.Create(ctx, job1Ns1))
	defer assertWsjobCleanedUp(t.T(), job1Ns1)
	lock = assertLockExists(t.T(), job1Ns1.Namespace, job1Ns1.Name)
	// queued as it requires 2 redundant systems while 1 is reserved by user session-0's workflow job
	assertNeverGranted(t.T(), lock)
	// should be scheduled after one local system come back
	// this validates only 1 redundant system is reserved by workflow job while non-workflow job doesn't reserve redundancy
	s6.Spec.Unschedulable = false
	require.NoError(t.T(), k8sClient.Update(ctx, s6))
	require.Eventually(t.T(), func() bool {
		return len(t.ResourceController.Cfg.UnhealthySystems) == 2 && len(t.ResourceController.Cfg.UnhealthyNodes) == 0
	}, eventuallyTimeout, eventuallyTick)
	assertLocked(t.T(), lock)
	validateLocalAffinity(lock)

	// shrink session and validates check grantable works at local session level without redundancy
	t.assignNS(userNs0, namespacev1.SetResourceSpecMode, nil, 1, 1)
	require.Eventually(t.T(), func() bool {
		t.T().Log(t.CfgMgr.Cfg.GetNsSysNameMap())
		return len(t.CfgMgr.Cfg.GetAssignedSystemsList(userNs0)) == 1
	}, eventuallyTimeout, eventuallyTick)
	queueJob1 = assertWsJobFailEventually(t.T(), queueJob1.Namespace, queueJob1.Name)
	cond := getConditions(t.T(), queueJob1.Namespace, queueJob1.Name)[commonv1.JobFailed]
	assert.Contains(t.T(), cond.Message,
		"job requested 2 system(s) but session 'user-session-0' has only 1 system(s) assigned")

	// sanity check on shared session
	jobRedundantNs := newWsExecuteJob(redundantNs, "redundant-ns-job-0", 1)
	require.NoError(t.T(), k8sClient.Create(ctx, jobRedundantNs))
	defer assertWsjobCleanedUp(t.T(), jobRedundantNs)
	jobRedundantNs = assertWsJobFailEventually(t.T(), jobRedundantNs.Namespace, jobRedundantNs.Name)
	cond = getConditions(t.T(), jobRedundantNs.Namespace, jobRedundantNs.Name)[commonv1.JobFailed]
	assert.Contains(t.T(), cond.Message,
		"invalid job, job session redundant-session "+
			"is in redundant mode and can only be accessible by jobs from permitted sessions")
}

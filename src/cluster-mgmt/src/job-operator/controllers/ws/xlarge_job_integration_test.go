//go:build integration && xlarge_jobs

package ws

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
)

func (t *TestSuite) TestXLargeJobScheduling() {
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	t.createNsIfNotExists(wscommon.DefaultNamespace)
	t.assignEverythingToNS(wscommon.DefaultNamespace, systemCount)

	setResourceRequirements := func(job *wsapisv1.WSJob, memRequest, memUpperBound, memMaxLimit int) {
		for rt, spec := range job.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeWeight {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(memRequest)<<20, resource.BinarySI),
				}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(memRequest)<<20, resource.BinarySI),
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(memUpperBound << 20),
					wsapisv1.PodMemoryMaxLimitKey:   strconv.Itoa(memMaxLimit << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeCommand {
				// unset command resource requirements so calculation can be made easier
				spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
			}
		}
	}

	t.Run("multiple_populated_nodegroups_scheduling", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 1, 12, 4, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		// There are in total 4 systems, 4 populated nodegroups and 0 depop nodegroup in this suite
		sysList := wscommon.SortedKeys(t.CfgMgr.Cfg.SystemMap)
		groupList := wscommon.SortedKeys(t.CfgMgr.Cfg.GroupMap)
		assert.Equal(t.T(), 4, len(sysList))
		assert.Equal(t.T(), 4, len(groupList))

		job0 := newWsExecuteJob(wscommon.Namespace, "multi-primary-ngs-job-0", 4)
		job0.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		// set limit overhead to 25%, 4.5g*0.25>1g
		overheadMB := int(wscommon.DefaultNodeMem * 0.25)
		job0.Annotations[wsapisv1.RtNodeMaxLimitReservedMemGbKey] = "1"
		job0.Annotations[wsapisv1.RtNodeMaxLimitReservedMemRatioKey] = "0.25"
		job0.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(47))
		setResourceRequirements(job0, wscommon.DefaultNodeMem/8, wscommon.DefaultNodeMem/4, wscommon.DefaultNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		lock := assertLockExists(t.T(), job0.Namespace, job0.Name)
		assertLocked(t.T(), lock)

		expectedReplicas := map[string]map[string]int{
			"ng-primary-0": {
				rlv1.WeightRequestName:  23,
				rlv1.CommandRequestName: 1,
				rlv1.WorkerRequestName:  2,
			},
			"ng-primary-1": {
				// In the event WGT replicas are not balanced among primary nodegroups,
				// the latter primary nodegroup(s) should have the remainder WGT replica(s)
				rlv1.WeightRequestName: 24,
				// Redistribute workers to reduce the total number of nodegroups required
				rlv1.WorkerRequestName: 2,
			},
			rlv1.SecondaryNgName: {
				rlv1.WorkerRequestName: 2,
			},
		}
		// Each GG should have a set of WSE-specific workers
		expectedGGCount := map[string]int{
			"ng-primary-0":       1,
			"ng-primary-1":       1,
			rlv1.SecondaryNgName: 2,
		}
		expectedSetMems := map[int]int{
			// 22 WGT from ng-primary-0 and 24 WGT from ng-primary-1 are scheduled two-by-two on nodes
			(wscommon.DefaultNodeMem - overheadMB) / 2: 46,
			// 1 remaining WGT from ng-primary-0 should utilize all memory available
			wscommon.DefaultNodeMem - overheadMB: 1,
		}
		actualReplicas := map[string]map[string]int{}
		actualGGCount := map[string]int{}
		actualSetMems := map[int]int{}
		ngContainsLargeHeteroMemWgt := ""
		for _, gg := range lock.Status.GroupResourceGrants {
			// ACT, CHF, BR are in their own group grants, which are irrelevant to this particular test
			if !strings.HasPrefix(gg.Name, "ng-") {
				continue
			}
			actualGGCount[gg.Name] += 1
			actualReplicas[gg.Name] = wscommon.EnsureMap(actualReplicas[gg.Name])
			for _, rg := range gg.ResourceGrants {
				actualReplicas[gg.Name][rg.Name] = len(rg.Resources)
				if rg.Name == rlv1.WeightRequestName {
					for _, res := range rg.Resources {
						if res.Subresources[wsresource.MemSubresource] == wscommon.DefaultNodeMem-overheadMB {
							ngContainsLargeHeteroMemWgt = gg.Name
							break
						}
					}
				}
			}
		}
		assert.Equal(t.T(), expectedReplicas, actualReplicas)
		assert.Equal(t.T(), expectedGGCount, actualGGCount)
		assert.Equal(t.T(), "ng-primary-0", ngContainsLargeHeteroMemWgt)

		wgts := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, rlv1.WeightRequestName, 47)
		for _, wgt := range wgts.Items {
			assert.Equal(t.T(), wscommon.DataNetAttachDef, wgt.Annotations[wscommon.NetworkAttachmentKey])
			for _, env := range wgt.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
					assert.Equal(t.T(), fmt.Sprintf("%sMi", env.Value), wgt.Spec.Containers[0].Resources.Limits.Memory().String())
					val, _ := strconv.Atoi(env.Value)
					actualSetMems[val] += 1
				}
			}
		}
		assert.Equal(t.T(), expectedSetMems, actualSetMems)
		// do not cancel job0 and see later jobs starve

		job1 := newWsExecuteJob(wscommon.Namespace, "single-primary-ngs-job", 1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		lock = assertLockExists(t.T(), job1.Namespace, job1.Name)
		// pending due to all nodegroups are in use
		assertPending(t.T(), lock)
		assertWsjobCleanedUp(t.T(), job1)

		job2 := newWsExecuteJob(wscommon.Namespace, "multi-primary-ngs-job-1", 1)
		job2.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		lock = assertLockExists(t.T(), job2.Namespace, job2.Name)
		// pending due to all nodegroups are in use
		assertPending(t.T(), lock)

		// cancel job0 and assert job1, job2 granted
		assertWsjobCleanedUp(t.T(), job0)
		job2 = assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)
		// do not cancel job2 and see later jobs starve

		job3 := newWsExecuteJob(wscommon.Namespace, "multi-primary-ngs-job-2", 5)
		job3.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))
		job3 = assertWsJobFailEventually(t.T(), job3.Namespace, job3.Name)
		lastCondition := job3.Status.Conditions[len(job3.Status.Conditions)-1]
		assert.Contains(t.T(), lastCondition.Message, "requested 5 system(s) but session 'job-operator' has only 4 system(s) assigned")

		job4 := newWsExecuteJob(wscommon.Namespace, "multi-primary-ngs-job-3", 3)
		job4.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		job4.Annotations[wsapisv1.SchedulerHintAllowNG] = "group1,group2"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job4))
		job4 = assertWsJobFailEventually(t.T(), job4.Namespace, job4.Name)
		lastCondition = job4.Status.Conditions[len(job4.Status.Conditions)-1]
		assert.Contains(t.T(), lastCondition.Message, "requested 3 systems 3 nodegroups in total but only specified 2 hint node group(s)")

		ngsInUseStr := job2.Annotations[NodeGroupAnnotationKey]
		ngsInUse := strings.Split(ngsInUseStr, ",")
		var ngsNotInUse []string
		for _, ngName := range groupList {
			if !slices.Contains(ngsInUse, ngName) {
				ngsNotInUse = append(ngsNotInUse, ngName)
			}
		}
		ngsNotInUseStr := strings.Join(ngsNotInUse, ",")

		job5 := newWsExecuteJob(wscommon.Namespace, "multi-primary-ngs-job-4", 1)
		job5.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		job5.Annotations[wsapisv1.SchedulerHintAllowNG] = ngsInUseStr
		require.NoError(t.T(), k8sClient.Create(context.Background(), job5))
		lock = assertLockExists(t.T(), job5.Namespace, job5.Name)
		// pending due to the targeted nodegroups currently in use
		// note that there are sufficient free nodegroups to use if we didn't specify the scheduler hint
		assertPending(t.T(), lock)

		// create a job targeting the free nodegroups, but failed due to a bad node
		badNode := fmt.Sprintf("%s-memory-0", ngsNotInUse[0])
		require.NoError(t.T(), patchNode(t, badNode, []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))
		job6 := newWsExecuteJob(wscommon.Namespace, "multi-primary-ngs-job-5", 1)
		job6.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		job6.Annotations[wsapisv1.SchedulerHintAllowNG] = ngsNotInUseStr
		// Every WGT pod takes all resources on each node
		job6.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(24))
		setResourceRequirements(job6, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job6))
		assertWsJobFailEventually(t.T(), job6.Namespace, job6.Name)

		// bad node recovered and the same job should succeed now
		require.NoError(t.T(), patchNode(t, badNode, []corev1.NodeCondition{}))
		job7 := newWsExecuteJob(wscommon.Namespace, "multi-primary-ngs-job-6", 1)
		job7.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		job7.Annotations[wsapisv1.SchedulerHintAllowNG] = ngsNotInUseStr
		job7.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(24))
		setResourceRequirements(job7, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job7))
		lock = assertLockExists(t.T(), job7.Namespace, job7.Name)
		assertLocked(t.T(), lock)
		assertWsJobScheduledEventually(t.T(), job7.Namespace, job7.Name)

		// cancel job2 and assert job5 granted - using the same nodegroups
		assertWsjobCleanedUp(t.T(), job2)
		job5 = assertWsJobScheduledEventually(t.T(), job5.Namespace, job5.Name)

		defer assertWsjobCleanedUp(t.T(), job5)
		defer assertWsjobCleanedUp(t.T(), job7)
	})

	t.Run("memory_class_annotation-driven_scheduling", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 3, 12, 4, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

		// group[0-1]: xlarge
		// group2: large
		// group3: regular
		// no depop nodegroup
		for _, g := range v2Cfg.Groups {
			g.Properties[wsapisv1.MemxPopNodegroupProp] = "true"
			if g.Name == "group0" || g.Name == "group1" {
				g.Properties[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeXLarge
			} else if g.Name == "group2" {
				g.Properties[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeLarge
			}
		}
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		// granted: 1 system, 2 xlarge nodegroups
		job0 := newWsExecuteJob(wscommon.Namespace, "happy-500b-xlarge-job", 1)
		job0.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		job0.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = wsapisv1.PopNgTypeXLarge
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		lock := assertLockExists(t.T(), job0.Namespace, job0.Name)
		assertLocked(t.T(), lock)
		assertNgCountsAcquired(t.T(), job0, 2, 0)
		assertWsJobScheduledEventually(t.T(), job0.Namespace, job0.Name)

		// pending: requires 2 systems and 1 xlarge nodegroup
		// all xlarge nodegroups are in use at this point
		job1 := newWsExecuteJob(wscommon.Namespace, "happy-250b-xlarge-job", 2)
		job1.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = wsapisv1.PopNgTypeXLarge
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		lock = assertLockExists(t.T(), job1.Namespace, job1.Name)
		assertPending(t.T(), lock)

		// granted: 1 system, 1 regular nodegroup
		job2 := newWsExecuteJob(wscommon.Namespace, "happy-non-xlarge-job-1", 1)
		job2.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = strings.Join(
			[]string{wsapisv1.PopNgTypeRegular, wsapisv1.PopNgTypeLarge}, ",")
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		lock = assertLockExists(t.T(), job2.Namespace, job2.Name)
		assertLocked(t.T(), lock)
		assertNgCountsAcquired(t.T(), job2, 1, 0)
		assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)

		// pending: 1 system, 1 regular or large nodegroup
		// cannot be granted given 2 systems in use and 2 systems being queued by job1
		job3 := newWsExecuteJob(wscommon.Namespace, "happy-non-xlarge-job-2", 1)
		job3.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = strings.Join(
			[]string{wsapisv1.PopNgTypeRegular, wsapisv1.PopNgTypeLarge}, ",")
		job3.Labels[wsapisv1.WorkflowIdLabelKey] = "going-to-expedite"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))
		lock = assertLockExists(t.T(), job3.Namespace, job3.Name)
		assertPending(t.T(), lock)

		// nothing gets granted
		job4 := newWsExecuteJob(wscommon.Namespace, "oversized-non-xlarge-job-2", 1)
		job4.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		job4.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = wsapisv1.PopNgTypeLarge
		// only large nodegroups are considered
		require.NoError(t.T(), k8sClient.Create(context.Background(), job4))
		job4 = assertWsJobFailEventually(t.T(), job4.Namespace, job4.Name)
		lastCondition := job4.Status.Conditions[len(job4.Status.Conditions)-1]
		assert.Contains(t.T(), lastCondition.Message, "requested 2 populated nodegroups of type [large] but only 1 available")

		// updating the job priority of job3 to be ahead of job1 to see the queued systems was indeed the cause
		// job3 granted: 1 system and 1 large nodegroup
		updateJobPriorityEventually(t.T(), job3, 50)
		assertWsJobScheduledEventually(t.T(), job3.Namespace, job3.Name)
		lock = assertLockExists(t.T(), job3.Namespace, job3.Name)
		assertNgCountsAcquired(t.T(), job3, 1, 0)
		assertWsJobQueuedEventually(t.T(), job1.Namespace, job1.Name) // job1 still in queue

		// job0 released: 1 system 2 xlarge nodegroup
		// job1 granted: 2 systems and 1 xlarge nodegroup
		assertWsjobCleanedUp(t.T(), job0)
		assertWsJobScheduledEventually(t.T(), job1.Namespace, job1.Name)
		lock = assertLockExists(t.T(), job1.Namespace, job1.Name)
		assertNgCountsAcquired(t.T(), job1, 2, 0)

		job5 := newWsExecuteJob(wscommon.Namespace, "full-size-non-xlarge-job", 4)
		job5.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = strings.Join(
			[]string{wsapisv1.PopNgTypeRegular, wsapisv1.PopNgTypeLarge}, ",")
		require.NoError(t.T(), k8sClient.Create(context.Background(), job5))
		lock = assertLockExists(t.T(), job5.Namespace, job5.Name)
		assertPending(t.T(), lock)

		job6 := newWsExecuteJob(wscommon.Namespace, "full-size-xlarge-job", 4)
		job6.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = wsapisv1.PopNgTypeXLarge
		require.NoError(t.T(), k8sClient.Create(context.Background(), job6))
		lock = assertLockExists(t.T(), job6.Namespace, job6.Name)
		assertPending(t.T(), lock)
		defer assertWsjobCleanedUp(t.T(), job6)

		assertWsjobCleanedUp(t.T(), job1)
		assertWsjobCleanedUp(t.T(), job2)
		assertWsjobCleanedUp(t.T(), job3)
		// using large and xlarge nodegroups as depop
		assertWsJobScheduledEventually(t.T(), job5.Namespace, job5.Name)
		lock = assertLockExists(t.T(), job5.Namespace, job5.Name)
		assertNgCountsAcquired(t.T(), job5, 4, 0)
		assert.Equal(t.T(), 4, lock.GetSystemsAcquired())

		assertWsjobCleanedUp(t.T(), job5)
		// using regular and large nodegroups as depop
		assertWsJobScheduledEventually(t.T(), job6.Namespace, job6.Name)
		lock = assertLockExists(t.T(), job6.Namespace, job6.Name)
		assertNgCountsAcquired(t.T(), job6, 4, 0)
		assert.Equal(t.T(), 4, lock.GetSystemsAcquired())
	})

	t.Run("memory_class_auto_incremental_scheduling", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 3, 12, 4, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

		// group[0-1]: xlarge
		// group2: large
		// group3: regular
		// no depop nodegroup
		for _, g := range v2Cfg.Groups {
			g.Properties[wsapisv1.MemxPopNodegroupProp] = "true"
			if g.Name == "group0" || g.Name == "group1" {
				g.Properties[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeXLarge
			} else if g.Name == "group2" {
				g.Properties[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeLarge
			}
		}
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		// granted: 1 system, 1 regular nodegroup
		job0 := newWsExecuteJob(wscommon.Namespace, "regular-job-0", 1)
		// two weights sit on one node, requiring 1 regular nodegroup
		job0.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(24))
		setResourceRequirements(job0, wscommon.DefaultNodeMem/2, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		assertWsJobScheduledEventually(t.T(), job0.Namespace, job0.Name)
		assertNgNamesAcquired(t.T(), job0, []string{"group3"})

		// granted: 1 system, 1 large nodegroup (would prefer regular nodegroup but large nodegroup is also fine)
		job1 := newWsExecuteJob(wscommon.Namespace, "regular-job-1", 1)
		// two weights sit on one node, requiring 1 regular nodegroup
		job1.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(24))
		setResourceRequirements(job1, wscommon.DefaultNodeMem/2, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		assertWsJobScheduledEventually(t.T(), job1.Namespace, job1.Name)
		assertNgNamesAcquired(t.T(), job1, []string{"group2"})

		// pending: waiting on 1 regular/large nodegroup to free up
		// since the non-xlarge nodegroup could fit the job, xlarge nodegroup won't be considered
		job2 := newWsExecuteJob(wscommon.Namespace, "regular-job-2", 1)
		// two weights sit on one node, requiring 1 regular nodegroup
		job2.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(24))
		setResourceRequirements(job2, wscommon.DefaultNodeMem/2, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		assertWsJobQueuedEventually(t.T(), job2.Namespace, job2.Name)

		// pending: waiting on 1 large nodegroup to free up
		// since the large nodegroup could fit the job, xlarge nodegroup won't be considered
		job3 := newWsExecuteJob(wscommon.Namespace, "large-job-1", 1)
		// two weights sit on one node, requiring 1 large nodegroup
		job3.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(24))
		setResourceRequirements(job3, wscommon.LargeNodeMem/2, wscommon.LargeNodeMem, wscommon.LargeNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))
		assertWsJobQueuedEventually(t.T(), job3.Namespace, job3.Name)

		// freeing up the regular nodegroup gets job2 granted
		assertWsjobCleanedUp(t.T(), job0)
		assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)
		assertNgNamesAcquired(t.T(), job2, []string{"group3"})

		// freeing up the large nodegroup gets job3 granted
		assertWsjobCleanedUp(t.T(), job1)
		assertWsJobScheduledEventually(t.T(), job3.Namespace, job3.Name)
		assertNgNamesAcquired(t.T(), job3, []string{"group2"})

		// granted: 1 system, 1 xlarge nodegroup (smaller nodegroups won't fit)
		job4 := newWsExecuteJob(wscommon.Namespace, "xlarge-job-1", 1)
		// two weights sit on one node, requiring 1 xlarge nodegroup
		job4.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(24))
		setResourceRequirements(job4, wscommon.XLargeNodeMem/2, wscommon.XLargeNodeMem, wscommon.XLargeNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job4))
		assertWsJobScheduledEventually(t.T(), job4.Namespace, job4.Name)

		// pending: 1 xlarge nodegroup gets granted, but no more free nodegroup to use for the secondary nodegroup request
		job5 := newWsExecuteJob(wscommon.Namespace, "xlarge-job-2", 2)
		// two weights sit on one node, requiring 1 xlarge nodegroup
		job5.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(24))
		setResourceRequirements(job5, wscommon.XLargeNodeMem/2, wscommon.XLargeNodeMem, wscommon.XLargeNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job5))
		assertWsJobQueuedEventually(t.T(), job5.Namespace, job5.Name)

		// freeing up the large nodegroup gets job5 granted
		// large nodegroup gets used to fulfill the secondary nodegroup request
		assertWsjobCleanedUp(t.T(), job3)
		assertWsJobScheduledEventually(t.T(), job5.Namespace, job5.Name)

		// pending: requires 2 xlarge nodegroups but both are in use
		job6 := newWsExecuteJob(wscommon.Namespace, "super-xlarge-job-1", 1)
		// two weights sit on one node, requiring 2 xlarge nodegroups
		job6.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(48))
		setResourceRequirements(job6, wscommon.XLargeNodeMem/2, wscommon.XLargeNodeMem, wscommon.XLargeNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job6))
		assertWsJobQueuedEventually(t.T(), job6.Namespace, job6.Name)

		// freeing up 1 xlarge nodegroup and 1 large nodegroup won't help
		assertWsjobCleanedUp(t.T(), job5)
		assertWsJobQueuedEventually(t.T(), job6.Namespace, job6.Name)

		// freeing up 1 regular nodegroup also won't help
		assertWsjobCleanedUp(t.T(), job2)
		assertWsJobQueuedEventually(t.T(), job6.Namespace, job6.Name)

		// freeing up the second xlarge nodegroup gets the super xlarge job granted
		assertWsjobCleanedUp(t.T(), job4)
		assertWsJobScheduledEventually(t.T(), job6.Namespace, job6.Name)
		assertNgNamesAcquired(t.T(), job6, []string{"group0", "group1"})

		// failed since even 2 xlarge nodegroups could not fit all the weights
		job7 := newWsExecuteJob(wscommon.Namespace, "too-demanding-xlarge-job-1", 1)
		// two weights sit on one node, requiring 3 xlarge nodegroups
		job7.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(50))
		setResourceRequirements(job7, wscommon.XLargeNodeMem/2, wscommon.XLargeNodeMem, wscommon.XLargeNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job7))
		assertWsJobFailEventually(t.T(), job7.Namespace, job7.Name)

		// pending: largest possible job for this cluster
		job8 := newWsExecuteJob(wscommon.Namespace, "super-xlarge-job-2", 4)
		// two weights sit on one node, requiring 2 xlarge nodegroups
		// in additional, it requires two nodegroups to fulfill the secondary nodegroup requests
		job8.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(48))
		setResourceRequirements(job8, wscommon.XLargeNodeMem/2, wscommon.XLargeNodeMem, wscommon.XLargeNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job8))
		assertWsJobQueuedEventually(t.T(), job8.Namespace, job8.Name)

		// freeing up the running super xlarge job gets the job8 to grant
		assertWsjobCleanedUp(t.T(), job6)
		assertWsJobScheduledEventually(t.T(), job8.Namespace, job8.Name)
		assertNgNamesAcquired(t.T(), job8, []string{"group0", "group1", "group2", "group3"})

		// annotation (if specified) takes precedence over auto incremental granting
		// we'd only consider regular and large nodegroups here given the annotation specified
		// but since nodegroup count was not specified, we'd only attempt to acquire 1 nodegroup to fulfill the request
		job9 := newWsExecuteJob(wscommon.Namespace, "regular-job-3", 1)
		// 1 large nodegroup could at most fit 48 weights
		// having 49 weights require a second pop nodegroup
		job9.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = strings.Join(
			[]string{wsapisv1.PopNgTypeRegular, wsapisv1.PopNgTypeLarge}, ",")
		job9.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(49))
		setResourceRequirements(job9, wscommon.DefaultNodeMem/2, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job9))
		assertWsJobFailEventually(t.T(), job9.Namespace, job9.Name)
		defer assertWsjobCleanedUp(t.T(), job9)

		// annotation (if specified) takes precedence over auto incremental granting
		// same spec as job9 except an annotation explicitly requesting two pop groups
		job10 := newWsExecuteJob(wscommon.Namespace, "regular-job-4", 1)
		// 1 large nodegroup could at most fit 48 weights
		// having 49 weights require a second pop nodegroup
		job10.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = strings.Join(
			[]string{wsapisv1.PopNgTypeRegular, wsapisv1.PopNgTypeLarge}, ",")
		job10.Annotations[wsapisv1.WsJobPopulatedNodegroupCount] = "2"
		job10.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(49))
		setResourceRequirements(job10, wscommon.DefaultNodeMem/2, wscommon.DefaultNodeMem, wscommon.DefaultNodeMem)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job10))
		assertWsJobQueuedEventually(t.T(), job10.Namespace, job10.Name)

		// freeing up super xlarge job gets job10 granted
		assertWsjobCleanedUp(t.T(), job8)
		assertWsJobScheduledEventually(t.T(), job10.Namespace, job10.Name)
		assertNgNamesAcquired(t.T(), job10, []string{"group2", "group3"})
		assertWsjobCleanedUp(t.T(), job10)
	})
}

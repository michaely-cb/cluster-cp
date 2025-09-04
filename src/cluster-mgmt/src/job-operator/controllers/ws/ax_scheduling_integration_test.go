//go:build integration && ax_scheduling

package ws

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"strings"

	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
)

func (t *TestSuite) TestClusterHealthOnAxScheduling() {
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	assertAxNsAware := func() {
		for _, node := range t.CfgMgr.Cfg.NodeMap {
			if strings.HasPrefix(node.Name, "ax-") {
				_, ok := node.Properties[wsresource.RoleAxPropKey]
				require.True(t.T(), ok)
				val, ok := node.Properties[wscommon.NamespaceKey]
				require.True(t.T(), ok)
				require.Equal(t.T(), wscommon.Namespace, val)
			}
		}
	}

	nic0SrName := fmt.Sprintf("%s/nic0", wsresource.NodeNicSubresource)
	nic1SrName := fmt.Sprintf("%s/nic1", wsresource.NodeNicSubresource)

	t.Run("ax_failure_handling", func() {
		deleteNodes(t.T())
		// ax-0, nic-0 -> all ports in domain 0, nic-1 -> all ports in domain 1
		// ax-1, nic-0 -> all ports in domain 2, nic-1 -> all ports in domain 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(3, 1, 12, 2, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertAxNsAware()

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		// mark 1 ax node down first, we will be scheduling all ACTs and CHFs on 1 remaining ax node
		require.NoError(t.T(), patchNode(t, "ax-1", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))

		// with one nic down, the job should continue to run but the lock will only grant the healthy nic
		require.NoError(t.T(), patchNode(t, "ax-0", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
			Status: corev1.ConditionTrue,
		}}))

		numSystems := 1
		job0 := newWsExecuteJob(wscommon.Namespace, "ax-one-nic-down", uint32(numSystems))
		for rt, spec := range job0.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/32)<<20, resource.BinarySI),
				}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/32)<<20, resource.BinarySI),
				}
			}
		}
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))

		lock := assertLockExists(t.T(), job0.Namespace, job0.Name)
		assertLocked(t.T(), lock)
		assert.Equal(t.T(), 3, len(lock.Status.GroupResourceGrants))
		for _, gg := range lock.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, rlv1.ActivationRequestName) {
				continue
			}
			for _, rg := range gg.ResourceGrants {
				_, ok := rg.Resources[0].Subresources[nic1SrName]
				assert.True(t.T(), ok)
			}
			break
		}
		chfs := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, rlv1.ChiefRequestName, numSystems)
		for _, chf := range chfs.Items {
			assert.NotEmpty(t.T(), chf.Annotations[wscommon.NetworkAttachmentKey])
		}
		acts := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, rlv1.ActivationRequestName, len(defaultActToCsxDomains)*numSystems)
		for _, act := range acts.Items {
			assert.NotEmpty(t.T(), act.Annotations[wscommon.NetworkAttachmentKey])

			// assert mem granting without upper bound and max limit
			gotExpectedSetMem := false
			expectedSetMem := wscommon.DefaultNodeMem / 32
			for _, env := range act.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
					if env.Value == strconv.Itoa(expectedSetMem) {
						gotExpectedSetMem = true
					}
				}
			}
			assert.True(t.T(), gotExpectedSetMem)
			assert.Equal(t.T(), fmt.Sprintf("%dMi", expectedSetMem), act.Spec.Containers[0].Resources.Limits.Memory().String())
		}

		// all 4 pods using ax-0 nic-1, which only has affinity to ports in domain 3, where one pod can benefit for affinity
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 3, numMultiHopTraffic)
		assert.Equal(t.T(), 1, numOneHopTraffic)

		require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKeyFromObject(job0), job0))
		assert.Equal(t.T(), fmt.Sprintf(AxSpineLoadMessage, 3, 4, 1), job0.Annotations[wscommon.ActV2SpineLoad])
		assertWsjobCleanedUp(t.T(), job0)

		// with both nics down, the job should fail
		require.NoError(t.T(), patchNode(t, "ax-0", []corev1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
				Status: corev1.ConditionTrue,
			},
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic1",
				Status: corev1.ConditionTrue,
			},
		}))

		job1 := newWsExecuteJob(wscommon.Namespace, "ax-all-nics-down", 1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))

		assertWsJobFailEventually(t.T(), job1.Namespace, job1.Name)
		assertWsjobCleanedUp(t.T(), job1)

		// with an instance error, the job should fail
		require.NoError(t.T(), patchNode(t, "ax-0", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))

		job2 := newWsExecuteJob(wscommon.Namespace, "ax-instance-down", 1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))

		assertWsJobFailEventually(t.T(), job2.Namespace, job2.Name)
		assertWsjobCleanedUp(t.T(), job2)

		// after recovering, the job should succeed and nics should be load balanced
		require.NoError(t.T(), patchNode(t, "ax-0", []corev1.NodeCondition{}))
		require.NoError(t.T(), patchNode(t, "ax-1", []corev1.NodeCondition{}))

		job3 := newWsExecuteJob(wscommon.Namespace, "ax-2-box-happy-case", 2)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))

		lock = assertLockExists(t.T(), job3.Namespace, job3.Name)
		assertLocked(t.T(), lock)
		assert.Equal(t.T(), 5, len(lock.Status.GroupResourceGrants))
		for _, gg := range lock.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, rlv1.ActivationRequestName) {
				continue
			}
			nic0Load, nic1Load := 0, 0
			for _, rg := range gg.ResourceGrants {
				if _, ok := rg.Resources[0].Subresources[nic1SrName]; ok {
					nic1Load += 1
				} else if _, ok = rg.Resources[0].Subresources[nic0SrName]; ok {
					nic0Load += 1
				}
			}
			assert.Equal(t.T(), nic0Load, nic1Load)
			break
		}

		// zero spine load given full affinity coverage
		numOneHopTraffic, numMultiHopTraffic = wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 0, numMultiHopTraffic)
		assert.Equal(t.T(), 8, numOneHopTraffic)
		assertWsjobCleanedUp(t.T(), job3)

		// 2 parallel 1-box jobs
		job4 := newWsExecuteJob(wscommon.Namespace, "ax-1-box-happy-case-a", 1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job4))
		lock = assertLockExists(t.T(), job4.Namespace, job4.Name)
		assertLocked(t.T(), lock)
		// half spine load given half affinity coverage
		numOneHopTraffic, numMultiHopTraffic = wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 2, numMultiHopTraffic)
		assert.Equal(t.T(), 2, numOneHopTraffic)

		job5 := newWsExecuteJob(wscommon.Namespace, "ax-1-box-happy-case-b", 1)
		job5.Annotations[wsapisv1.SchedulerHintAllowNG] = "group1,group2" // ax scheduling should ignore scheduler hints
		require.NoError(t.T(), k8sClient.Create(context.Background(), job5))
		defer assertWsjobCleanedUp(t.T(), job5)
		lock = assertLockExists(t.T(), job5.Namespace, job5.Name)
		assertLocked(t.T(), lock)
		// half spine load given half affinity coverage
		numOneHopTraffic, numMultiHopTraffic = wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 2, numMultiHopTraffic)
		assert.Equal(t.T(), 2, numOneHopTraffic)

		job6 := newWsExecuteJob(wscommon.Namespace, "ax-1-box-pending", 1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job6))
		defer assertWsjobCleanedUp(t.T(), job6)
		lock = assertLockExists(t.T(), job6.Namespace, job6.Name)
		assertPending(t.T(), lock)
		assertWsjobCleanedUp(t.T(), job4)
		require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKeyFromObject(lock), lock))
		assertLocked(t.T(), lock)
	})

	// The ACT/CHF requests are hydrated, split into stamps, then corresponding grants merged in the end.
	// Activation lock requests uses annotations to encode more required information. The annotations are used
	// to rebuild the mapping from request to pod index.
	// It's crucial to ensure resource (systems, acts, chiefs) ordering of the lock, the pods and the cluster details config match e2e.
	t.Run("ax_multi-stamp_scheduling", func() {
		deleteNodes(t.T())
		// 2 systems/ax per stamp:
		// ax-0, nic-0 -> system-{0,4}-cs-port-{0,1,2}, nic-1 -> system-{0,4}-cs-port-{6,7,8}
		// ax-4, nic-0 -> system-{0,4}-cs-port-{3,4,5}, nic-1 -> system-{0,4}-cs-port-{9,10,11}
		// corresponding systems in stamp: system-0, system-4
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 30, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(4, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertAxNsAware()

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{0}, {1}, {2}, {3}, {0}, {1}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		// should pick ax-0 and ax-4 from the first stamp, and ax-1 from the second stamp
		jobName := "cross-stamp-job"
		crossStampJob := newWsExecuteJob(wscommon.Namespace, jobName, 3)
		// set limit overhead to 25%, 4.5g*0.25>1g
		overheadMB := int(wscommon.DefaultNodeMem * 0.25)
		crossStampJob.Annotations[wsapisv1.RtNodeMaxLimitReservedMemGbKey] = "1"
		crossStampJob.Annotations[wsapisv1.RtNodeMaxLimitReservedMemRatioKey] = "0.25"
		for rt, spec := range crossStampJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/32)<<20, resource.BinarySI),
				}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/32)<<20, resource.BinarySI),
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 16) << 20),
					wsapisv1.PodMemoryMaxLimitKey:   strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeChief {
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/3)<<20, resource.BinarySI),
				}
			}
		}
		require.NoError(t.T(), k8sClient.Create(context.Background(), crossStampJob))
		defer assertWsjobCleanedUp(t.T(), crossStampJob)
		lock := assertLockExists(t.T(), crossStampJob.Namespace, crossStampJob.Name)
		assertLocked(t.T(), lock)

		expectedSystemsOrdered := []string{"system-1", "system-0", "system-4"}
		assertSystemsOrdered(t, expectedSystemsOrdered, lock)

		type scheduleResult struct {
			nodeName       string
			nicName        string
			systemName     string
			csPort         string
			preferredPorts string
		}
		expectActScheduleResult := map[int]scheduleResult{}
		for _, gg := range lock.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, rlv1.ActivationRequestName) {
				continue
			}
			for _, rg := range gg.ResourceGrants {
				podIdx, _ := strconv.Atoi(rg.Name)
				sysIdx, _ := strconv.Atoi(rg.Annotations[rlv1.SystemIdAnnotKey])
				nodeName := rg.Resources[0].Name
				nicName := ""
				for k := range rg.Resources[0].Subresources {
					if !strings.HasPrefix(k, wsresource.NodeNicSubresource) {
						continue
					}
					nicName = k
					break
				}
				expectActScheduleResult[podIdx] = scheduleResult{
					nodeName:       nodeName,
					nicName:        nicName,
					systemName:     expectedSystemsOrdered[sysIdx],
					csPort:         rg.Annotations[rlv1.TargetCsPortAnnotKey],
					preferredPorts: rg.Annotations[rlv1.PreferredCsPortsAnnotKey],
				}
			}
		}

		acts := assertPodCountEventually(t.T(), crossStampJob.Namespace, crossStampJob.Name, rlv1.ActivationRequestName, len(defaultActToCsxDomains)*3)
		assert.Equal(t.T(), len(expectActScheduleResult), len(acts.Items))

		for _, pod := range acts.Items {
			podName := pod.Name
			tokens := strings.Split(podName, "-")
			podIdxStr := tokens[len(tokens)-1]
			podIdx, _ := strconv.Atoi(podIdxStr)
			expectedResult := expectActScheduleResult[podIdx]

			// assert node scheduling
			assert.Equal(t.T(), expectedResult.nodeName, pod.Spec.NodeSelector["kubernetes.io/hostname"])
			assert.Equal(t.T(), 1, len(pod.Spec.Containers))
			_, ok := pod.Spec.NodeSelector["k8s.cerebras.com/node-role-activation"]
			assert.True(t.T(), ok)

			// assert nic to nad mapping
			expectedNad := wscommon.Ternary(expectedResult.nicName == nic0SrName, wscommon.DataNetAttachDef, wscommon.SecondNadLegacy)
			assert.Equal(t.T(), expectedNad, pod.Annotations[wscommon.NetworkAttachmentKey])

			// assert set port env var being set
			foundSetPortEnvVar := false
			foundPreferredPortsEnvVar := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeCsPortEnvVarName {
					assert.Equal(t.T(), expectedResult.csPort, env.Value)
					foundSetPortEnvVar = true
				} else if env.Name == wsapisv1.RuntimeCsPortAffinityEnvVarName {
					assert.Equal(t.T(), expectedResult.preferredPorts, env.Value)
					foundPreferredPortsEnvVar = true
				}
			}
			assert.True(t.T(), foundSetPortEnvVar)
			assert.True(t.T(), foundPreferredPortsEnvVar)

			// assert mem max limit granting
			gotExpectedSetMem := false
			expectedSetMem := (wscommon.DefaultNodeMem - overheadMB - wscommon.DefaultNodeMem/3) / len(defaultActToCsxDomains)
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
					if env.Value == strconv.Itoa(expectedSetMem) {
						gotExpectedSetMem = true
					}
				}
			}
			assert.True(t.T(), gotExpectedSetMem)
			assert.Equal(t.T(), fmt.Sprintf("%dMi", expectedSetMem), pod.Spec.Containers[0].Resources.Limits.Memory().String())
		}

		chfs := assertPodCountEventually(t.T(), crossStampJob.Namespace, crossStampJob.Name, rlv1.ChiefRequestName, 3)
		assert.Equal(t.T(), 3, len(chfs.Items))

		for _, pod := range chfs.Items {
			assert.Equal(t.T(), fmt.Sprintf("%dMi", wscommon.DefaultNodeMem/3), pod.Spec.Containers[0].Resources.Requests.Memory().String())
		}

		clusterDetailConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, jobName),
				Namespace: wscommon.Namespace,
			},
		}
		require.Eventually(t.T(), func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(clusterDetailConfigMap), clusterDetailConfigMap)
			return err == nil
		}, eventuallyTimeout, eventuallyTick)
		assert.Equal(t.T(), wsapisv1.ConfigMapTypeClusterDetails, clusterDetailConfigMap.Labels[wsapisv1.ConfigMapType])

		cdConfig := &commonpb.ClusterDetails{}
		options := protojson.UnmarshalOptions{
			AllowPartial:   true,
			DiscardUnknown: true,
		}
		data, err := wscommon.Gunzip(clusterDetailConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName])
		require.NoError(t.T(), err)
		err = options.Unmarshal(data, cdConfig)
		require.NoError(t.T(), err)

		// verify BR metrics emitted
		assert.True(t.T(),
			wscommon.PodNetworkTopologyForTesting(crossStampJob.Namespace, crossStampJob.Name,
				"broadcastreduce-0", "br-0", "nic0,nic2", "leaf-0-0", "",
				"system-1,system-0,system-4", "n00", "leaf-1-0,leaf-0-0", "0", "", "0", false))

		// assert ACT -> WSE relations
		for _, v := range cdConfig.Tasks {
			if v.TaskType == commonpb.ClusterDetails_TaskInfo_WSE {
				for i, task := range v.TaskMap {
					assert.Equal(t.T(), i, int(task.TaskId.WseId))
					assert.Equal(t.T(), expectedSystemsOrdered[i], task.AddressBook.TaskNodeName)
				}
			} else if v.TaskType == commonpb.ClusterDetails_TaskInfo_ACT {
				for i, task := range v.TaskMap {
					assert.Equal(t.T(), expectActScheduleResult[i].systemName, expectedSystemsOrdered[int(task.TaskId.WseId)])
				}
			}
		}
	})

	t.Run("ax_stamp-less_scheduling", func() {
		wsapisv1.AxCountPerSystemDiscoveryEnabled = true
		defer func() { wsapisv1.AxCountPerSystemDiscoveryEnabled = false }()
		v2Cfg := wscommon.NewTestClusterConfigSchema(3, 1, 12, 6, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(0, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertAxNsAware()

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{0}, {1}, {2}, {3}, {0}, {1}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		jobName := "stamp-less-job"
		job := newWsExecuteJob(wscommon.Namespace, jobName, 3)
		for rt, spec := range job.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/32)<<20, resource.BinarySI),
				}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/32)<<20, resource.BinarySI),
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 16) << 20),
				}
			}
		}
		require.NoError(t.T(), k8sClient.Create(context.Background(), job))
		defer assertWsjobCleanedUp(t.T(), job)
		lock := assertLockExists(t.T(), job.Namespace, job.Name)
		assertLocked(t.T(), lock)

		assertSystemsOrdered(t, []string{"system-0", "system-1", "system-2"}, lock)
		assertNodesScheduled(t, map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-2": true,
			"ax-3": true,
			"ax-4": true,
			"ax-5": true,
		}, lock, rlv1.ActivationRequestName)
		expectActPods := len(defaultActToCsxDomains) * 3

		acts := assertPodCountEventually(t.T(), job.Namespace, job.Name, rlv1.ActivationRequestName, len(defaultActToCsxDomains)*3)
		assert.Equal(t.T(), expectActPods, len(acts.Items))

		for _, pod := range acts.Items {
			// assert upper bound mem granting
			gotExpectedSetMem := false
			expectedSetMem := wscommon.DefaultNodeMem / 16
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
					if env.Value == strconv.Itoa(expectedSetMem) {
						gotExpectedSetMem = true
					}
				}
			}
			assert.True(t.T(), gotExpectedSetMem)
			assert.Equal(t.T(), fmt.Sprintf("%dMi", expectedSetMem), pod.Spec.Containers[0].Resources.Limits.Memory().String())
		}
	})

	t.Run("ax_enforced_stamp_scheduling", func() {
		wsapisv1.AxCountPerSystemDiscoveryEnabled = true
		wsapisv1.StampCountOverride = 2
		defer func() {
			wsapisv1.AxCountPerSystemDiscoveryEnabled = false
			wsapisv1.StampCountOverride = 0
		}()

		v2Cfg := wscommon.NewTestClusterConfigSchema(3, 1, 12, 6, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(0, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertAxNsAware()

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{0}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		jobName := "enforced-stamps-job"
		job := newWsExecuteJob(wscommon.Namespace, jobName, 3)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job))
		defer assertWsjobCleanedUp(t.T(), job)
		lock := assertLockExists(t.T(), job.Namespace, job.Name)
		assertLocked(t.T(), lock)

		assertSystemsOrdered(t, []string{"system-0", "system-1", "system-2"}, lock)

		// pick 2 AX from the first stamp and 1 AX from the other stamp
		expectedAxNodes := map[string]bool{
			"ax-0": true,
			"ax-2": true,
			"ax-1": true,
		}
		assertNodesScheduled(t, expectedAxNodes, lock, rlv1.ActivationRequestName)
	})

	t.Run("parallel_jobs_from_different_namespaces", func() {
		deleteNodes(t.T())
		// 2 systems/ax per stamp:
		// ax-0, nic-0 -> system-{0,4}-cs-port-{0,1,2}, nic-1 -> system-{0,4}-cs-port-{6,7,8}
		// ax-4, nic-0 -> system-{0,4}-cs-port-{3,4,5}, nic-1 -> system-{0,4}-cs-port-{9,10,11}
		// corresponding systems in stamp: system-0, system-4
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 2, 30, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(4, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertAxNsAware()

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{0}, {1}, {2}, {3}, {0}, {1}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		systemNsr := t.createNsIfNotExists(wscommon.Namespace)
		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(systemNsr), systemNsr))
		// releasing the following resources from the system nsr:
		// - system-6, system-3, system-7
		// - ax-6, ax-3, ax-7
		systemNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(5),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, systemNsr))
		t.ensureNsUpdateSuccess(systemNsr)
		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(systemNsr), systemNsr))

		// Test resource removal metrics for system namespace
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(wscommon.DefaultNamespace, wsresource.SystemType, "system-6", "remove"))
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(wscommon.DefaultNamespace, wsresource.SystemType, "system-3", "remove"))
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(wscommon.DefaultNamespace, wsresource.RoleAxPropKey, "ax-6", "remove"))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(wscommon.DefaultNamespace, wsresource.SystemType, 5))

		userNsrName := "user-ns-ax"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()
		userNsJob := newWsExecuteJob(userNsrName, "user-ns-failed-job", 1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob))
		assertWsJobFailEventually(t.T(), userNsJob.Namespace, userNsJob.Name)
		assertWsjobCleanedUp(t.T(), userNsJob)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		// picking up the released resources to the user nsr
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(3),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		// Test resource addition metrics for user namespace
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.SystemType, "system-6", "add"))
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.SystemType, "system-3", "add"))
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.RoleAxPropKey, "ax-6", "add"))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(userNsrName, wsresource.SystemType, 3))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(userNsrName, wsresource.RoleAxPropKey, 3))

		userNsJob = newWsExecuteJob(userNsrName, "user-ns-succeeded-job", 3)
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob))
		lock := assertLockExists(t.T(), userNsJob.Namespace, userNsJob.Name)
		assertLocked(t.T(), lock)

		// Only system available in the user namespace
		assertSystemsOrdered(t, []string{
			"system-6",
			"system-3",
			"system-7"}, lock)
		// Only ax node available in the user namespace
		assertNodesScheduled(t, map[string]bool{
			"ax-6": true,
			"ax-3": true,
			"ax-7": true}, lock, rlv1.ActivationRequestName)

		systemNsJob := newWsExecuteJob(wscommon.Namespace, "system-ns-job", 5)
		require.NoError(t.T(), k8sClient.Create(context.Background(), systemNsJob))
		defer assertWsjobCleanedUp(t.T(), systemNsJob)
		lock = assertLockExists(t.T(), systemNsJob.Namespace, systemNsJob.Name)
		assertLocked(t.T(), lock)
		assertSystemsOrdered(t, []string{
			"system-2",
			"system-0",
			"system-4",
			"system-1",
			"system-5"}, lock)
		assertNodesScheduled(t, map[string]bool{
			"ax-2": true,
			"ax-0": true,
			"ax-4": true,
			"ax-1": true,
			"ax-5": true,
		}, lock, rlv1.ActivationRequestName)

		assertWsjobCleanedUp(t.T(), userNsJob)
		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		// scale down the user nsr
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(0),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(systemNsr), systemNsr))

		// Test resource removal metrics for user namespace
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.SystemType, "system-6", "remove"))
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.RoleAxPropKey, "ax-6", "remove"))
		assert.False(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.RoleAxPropKey, "ax-6", "add"))

		// system ns picks up all resources
		systemNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(8),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, systemNsr))
		t.ensureNsUpdateSuccess(systemNsr)

		// Test resource addition metrics for system namespace
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(wscommon.DefaultNamespace, wsresource.SystemType, "system-6", "add"))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(wscommon.DefaultNamespace, wsresource.SystemType, 8))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(wscommon.DefaultNamespace, wsresource.RoleAxPropKey, 8))
		assert.False(t.T(), wscommon.SessionHasResourceMovementForTesting(wscommon.DefaultNamespace, wsresource.SystemType, "system-6", "remove"))

	})

	t.Run("cross-job_target_port_load_balancing", func() {
		deleteNodes(t.T())
		// 32 systems/ax per stamp
		v2Cfg := wscommon.NewTestClusterConfigSchema(32, 2, 30, 32, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 12)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertAxNsAware()

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{3}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		assertAct0Port := func(job *wsapisv1.WSJob, exportedPort int) {
			acts := assertPodCountEventually(t.T(), job.Namespace, job.Name, rlv1.ActivationRequestName, 1)
			gotExpectedPort := false
			for _, env := range acts.Items[0].Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeCsPortEnvVarName {
					if env.Value == strconv.Itoa(exportedPort) {
						gotExpectedPort = true
					}
				}
			}
			assert.True(t.T(), gotExpectedPort)
		}

		// jobs alternate between least used ports in domain 3 across jobs
		// reason why we didn't end up using port 11 is just related to how the test constructs the network topology
		job0 := newInfExecuteJob(wscommon.Namespace, "ax-target-domain-3-job-1", 1, 0, 0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		defer assertWsjobCleanedUp(t.T(), job0)
		assertWsJobScheduledEventually(t.T(), job0.Namespace, job0.Name)
		assertAct0Port(job0, 10)

		job1 := newInfExecuteJob(wscommon.Namespace, "ax-target-domain-3-job-2", 1, 0, 0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		defer assertWsjobCleanedUp(t.T(), job1)
		assertWsJobScheduledEventually(t.T(), job1.Namespace, job1.Name)
		assertAct0Port(job1, 9)

		job2 := newInfExecuteJob(wscommon.Namespace, "ax-target-domain-3-job-3", 1, 0, 0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		defer assertWsjobCleanedUp(t.T(), job2)
		assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)
		assertAct0Port(job2, 10)

		job3 := newInfExecuteJob(wscommon.Namespace, "ax-target-domain-3-job-4", 1, 0, 0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))
		defer assertWsjobCleanedUp(t.T(), job3)
		assertWsJobScheduledEventually(t.T(), job3.Namespace, job3.Name)
		assertAct0Port(job3, 9)
	})

	t.Run("local_act-0_for_inference", func() {
		deleteNodes(t.T())
		// 32 systems/ax per stamp:
		// ax-0, nic-0 -> system-{0-31}-cs-port-0, nic-1 -> system-{0-31}-cs-port-1
		// ax-1, nic-0 -> system-{0-31}-cs-port-10, nic-1 -> system-{0-31}-cs-port-11
		v2Cfg := wscommon.NewTestClusterConfigSchema(32, 1, 72, 32, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 12)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertAxNsAware()

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{3}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		assertAct0Local := func(job *wsapisv1.WSJob, shouldBeLocal bool) string {
			var pods *corev1.PodList
			require.Eventually(t.T(), func() bool {
				pods = getJobPods(t.T(), job.Namespace, job.Name, rlv1.ActivationRequestName)
				return len(pods.Items) > 0
			}, eventuallyTimeout, eventuallyTick)

			usePort := ""
			preferredPorts := ""
			nodeName := ""
			for _, pod := range pods.Items {
				if pod.Labels["replica-index"] != "0" {
					continue
				}
				for _, env := range pod.Spec.Containers[0].Env {
					if env.Name == wsapisv1.RuntimeCsPortEnvVarName {
						usePort = env.Value
					} else if env.Name == wsapisv1.RuntimeCsPortAffinityEnvVarName {
						preferredPorts = env.Value
					}
				}
				nodeName = pod.Spec.NodeSelector["kubernetes.io/hostname"]
			}
			assert.Equal(t.T(), shouldBeLocal, slices.Contains(strings.Split(preferredPorts, ","), usePort))
			return nodeName
		}

		defaultActToCsxDomains = [][]int{{2}}
		job0 := newInfExecuteJob(wscommon.Namespace, "assert-act0-local-inference-job", 3, 0, 0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		assertWsJobScheduledEventually(t.T(), job0.Namespace, job0.Name)
		assertAct0Local(job0, true)
		assertWsjobCleanedUp(t.T(), job0)

		// non inference job does not get special handling over biased ax node selection yet
		job1 := newWsExecuteJob(wscommon.Namespace, "assert-act0-nonlocal-ws-job", 1)
		job1.Labels[wsapisv1.JobModeLabelKey] = wsapisv1.JobModeWeightStreaming
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		assertWsJobScheduledEventually(t.T(), job1.Namespace, job1.Name)
		assertAct0Local(job1, false)
		assertWsjobCleanedUp(t.T(), job1)
	})

	t.Run("cross_stamp_ax_more_ax_nodes_and_fragmented_stamps_handling", func() {
		deleteNodes(t.T())
		// 4 systems/ax per stamp
		// corresponding systems in stamp: system-0, system-2, system-4, system-6
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 2, 30, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(2, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertAxNsAware()

		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		wscommon.DisableSecondaryDataNic = false
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = true
		}()

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{0}, {1}, {2}, {3}, {0}, {1}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		systemNsr := t.createNsIfNotExists(wscommon.Namespace)
		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(systemNsr), systemNsr))
		systemNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(0),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, systemNsr))
		t.ensureNsUpdateSuccess(systemNsr)

		setResourceRequirements := func(wsReplicaSpecs map[commonv1.ReplicaType]*commonv1.ReplicaSpec) {
			for rt := range wsReplicaSpecs {
				if rt == wsapisv1.WSReplicaTypeActivation {
					wsReplicaSpecs[rt].Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
						corev1.ResourceMemory: *resource.NewQuantity((wscommon.DefaultNodeMem/32)<<20, resource.BinarySI),
						corev1.ResourceCPU:    defaultActCpu,
					}
					wsReplicaSpecs[rt].Template.Annotations = map[string]string{
						wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 16) << 20),
					}
				} else if rt == wsapisv1.WSReplicaTypeWeight {
					wsReplicaSpecs[rt].Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
						corev1.ResourceMemory: actOnlyWgtMem,
						corev1.ResourceCPU:    actOnlyWgtCpu,
					}
					wsReplicaSpecs[rt].Template.Annotations = map[string]string{
						wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 7) << 20),
						wsapisv1.PodMemoryMaxLimitKey:   strconv.Itoa(wscommon.DefaultNodeMem << 20),
					}
					wsReplicaSpecs[rt].Replicas = wscommon.Pointer(int32(4))
				} else if rt == wsapisv1.WSReplicaTypeCommand {
					wsReplicaSpecs[rt].Template.Annotations = map[string]string{
						wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 7) << 20),
						wsapisv1.PodMemoryMaxLimitKey:   strconv.Itoa(wscommon.DefaultNodeMem << 20),
					}
					wsReplicaSpecs[rt].Replicas = wscommon.Pointer(int32(1))
				}
			}
		}

		userNsrName := "user-ns-ax-1"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()
		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1", "group2"},
				// 2 systems in one stamp, 1 system in another stamp
				// 3 ax nodes in each stamp
				SystemNames:         []string{"system-0", "system-2", "system-1"},
				ActivationNodeNames: []string{"ax-0", "ax-2", "ax-4", "ax-1", "ax-3", "ax-5"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.SystemType, "system-0", "add"))
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.RoleCrdPropKey, "0-coord-0", "add"))
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.RoleAxPropKey, "ax-0", "add"))
		assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(userNsrName, wsresource.NodegroupType, "group0", "add"))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(userNsrName, wsresource.SystemType, 3))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(userNsrName, wsresource.RoleCrdPropKey, 1))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(userNsrName, wsresource.RoleAxPropKey, 6))
		assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(userNsrName, wsresource.NodegroupType, 3))

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		userNsJob1 := newWsExecuteJob(userNsrName, "user-ns-more-ax-job", 3)
		userNsJob1.Annotations[wsapisv1.UseAxOnlyForRuntimeKey] = strconv.FormatBool(true)
		userNsJob1.Annotations[wsapisv1.WsJobActivationNodeCount] = "5"
		setResourceRequirements(userNsJob1.Spec.WSReplicaSpecs)
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob1))
		lock := assertLockExists(t.T(), userNsJob1.Namespace, userNsJob1.Name)
		assertLocked(t.T(), lock)

		assertSystemsOrdered(t, []string{
			"system-1",
			"system-0",
			"system-2"}, lock)
		assertNodesScheduled(t, map[string]bool{
			"ax-1": true,
			"ax-0": true,
			"ax-2": true,
		}, lock, rlv1.ActivationRequestName)
		assertNodesScheduled(t, map[string]bool{
			// prefer ax-3 and ax-4 first given no usage
			"ax-3": true,
			"ax-4": true,
			// ax-0 and ax-1 are better choice, after scheduling 2 wgt pods to ax-3 and ax-4,
			"ax-0": true,
			"ax-1": true,
		}, lock, rlv1.WgtCmdGroupPrefix)
		assertWsjobCleanedUp(t.T(), userNsJob1)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode: namespacev1.DebugSetMode,
				// 2 systems in first stamp, 1 system in second stamp
				// 1 ax node in first stamp, 3 ax nodes in second stamp
				// redistributing a fraction of runtime pods from first stamp to second stamp
				SystemNames:         []string{"system-0", "system-2", "system-1"},
				ActivationNodeNames: []string{"ax-0", "ax-1", "ax-3", "ax-5"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		userNsJob2 := newWsExecuteJob(userNsrName, "user-ns-redistributed-more-ax-job-1", 3)
		userNsJob2.Annotations[wsapisv1.UseAxOnlyForRuntimeKey] = strconv.FormatBool(true)
		userNsJob2.Annotations[wsapisv1.WsJobActivationNodeCount] = "4"
		setResourceRequirements(userNsJob2.Spec.WSReplicaSpecs)
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob2))
		lock = assertLockExists(t.T(), userNsJob2.Namespace, userNsJob2.Name)
		assertLocked(t.T(), lock)

		assertSystemsOrdered(t, []string{
			"system-1",
			"system-0",
			"system-2"}, lock)
		assertNodesScheduled(t, map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-3": true,
		}, lock, rlv1.ChiefRequestName)
		assertNodesScheduled(t, map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-3": true,
		}, lock, rlv1.ActivationRequestName)
		assertNodesScheduled(t, map[string]bool{
			"ax-5": true,
			"ax-0": true,
			"ax-1": true,
			"ax-3": true,
		}, lock, rlv1.WgtCmdGroupPrefix)
		assertWsjobCleanedUp(t.T(), userNsJob2)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode: namespacev1.DebugSetMode,
				// 1 system in first stamp and 1 system in second stamp
				// all 4 ax nodes in second stamp
				// redistributing all runtime pods from first stamp to second stamp
				SystemNames:         []string{"system-0", "system-1"},
				ActivationNodeNames: []string{"ax-1", "ax-3", "ax-5", "ax-7"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		userNsJob3 := newWsExecuteJob(userNsrName, "user-ns-redistributed-more-ax-job-2", 2)
		userNsJob3.Annotations[wsapisv1.UseAxOnlyForRuntimeKey] = strconv.FormatBool(true)
		userNsJob3.Annotations[wsapisv1.WsJobActivationNodeCount] = "4"
		setResourceRequirements(userNsJob3.Spec.WSReplicaSpecs)
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob3))
		lock = assertLockExists(t.T(), userNsJob3.Namespace, userNsJob3.Name)
		assertLocked(t.T(), lock)

		assertSystemsOrdered(t, []string{
			"system-0",
			"system-1"}, lock)
		assertNodesScheduled(t, map[string]bool{
			"ax-1": true,
			"ax-3": true,
		}, lock, rlv1.ActivationRequestName)
		assertNodesScheduled(t, map[string]bool{
			"ax-5": true,
			"ax-7": true,
			"ax-1": true,
			"ax-3": true,
		}, lock, rlv1.WgtCmdGroupPrefix)
		assertWsjobCleanedUp(t.T(), userNsJob3)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode: namespacev1.DebugSetMode,
				// 2 systems in first stamp and 4 ax nodes in second stamp
				// redistributing all runtime pods from first stamp to second stamp
				SystemNames:         []string{"system-1", "system-3"},
				ActivationNodeNames: []string{"ax-0", "ax-2", "ax-4", "ax-6"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		userNsJob4 := newWsExecuteJob(userNsrName, "user-ns-redistributed-more-ax-job-3", 2)
		userNsJob4.Annotations[wsapisv1.UseAxOnlyForRuntimeKey] = strconv.FormatBool(true)
		userNsJob4.Annotations[wsapisv1.WsJobActivationNodeCount] = "4"
		setResourceRequirements(userNsJob4.Spec.WSReplicaSpecs)
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob4))
		lock = assertLockExists(t.T(), userNsJob4.Namespace, userNsJob4.Name)
		assertLocked(t.T(), lock)

		assertSystemsOrdered(t, []string{
			"system-1",
			"system-3"}, lock)
		assertNodesScheduled(t, map[string]bool{
			"ax-0": true,
			"ax-2": true,
		}, lock, rlv1.ActivationRequestName)
		assertNodesScheduled(t, map[string]bool{
			"ax-4": true,
			"ax-6": true,
			"ax-0": true,
			"ax-2": true,
		}, lock, rlv1.WgtCmdGroupPrefix)
		assertWsjobCleanedUp(t.T(), userNsJob4)

		defaultActToCsxDomains = [][]int{{3}}
		makeClusterDetailsCm := func(job *wsapisv1.WSJob, withActDomainsBinary bool) *corev1.ConfigMap {
			makeActTaskMap := func() ([]*commonpb.ClusterDetails_TaskInfo_TaskMap, []string) {
				var taskMaps []*commonpb.ClusterDetails_TaskInfo_TaskMap
				var wseIds []int32
				var actToCsxDomains []string
				// each act pod talks to all "numSys" systems
				for i := 0; i < int(job.Spec.NumWafers); i++ {
					wseIds = append(wseIds, int32(i))
				}
				for i := 0; i < int(job.Spec.NumWafers); i++ {
					for j := 0; j < len(defaultActToCsxDomains); j++ {
						taskMap := &commonpb.ClusterDetails_TaskInfo_TaskMap{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{TaskId: int32(i*len(defaultActToCsxDomains) + j), WseIds: wseIds},
							AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
								WseDomains: []int32{int32(defaultActToCsxDomains[j][0])},
							},
						}
						taskMaps = append(taskMaps, taskMap)
						actToCsxDomains = append(actToCsxDomains, wscommon.SerializeWioTaskMap(taskMap))
					}
				}
				return taskMaps, actToCsxDomains
			}

			actTaskMap, actToCsxDomains := makeActTaskMap()
			clusterDetails := &commonpb.ClusterDetails{
				Tasks: []*commonpb.ClusterDetails_TaskInfo{
					{
						TaskType: commonpb.ClusterDetails_TaskInfo_CRD,
						TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
							{
								TaskId:      &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{TaskId: 0, WseIds: []int32{-1}},
								AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{},
							},
						},
					},
					{
						TaskType: commonpb.ClusterDetails_TaskInfo_ACT,
						TaskMap:  actTaskMap,
					},
					{
						TaskType: commonpb.ClusterDetails_TaskInfo_WSE,
						TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
							{TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{TaskId: 0, WseIds: []int32{0}}},
							{TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{TaskId: 1, WseIds: []int32{1}}},
							{TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{TaskId: 2, WseIds: []int32{2}}},
						},
					},
				},
			}

			options := protojson.MarshalOptions{EmitUnpopulated: true}
			serializedClusterDetails, _ := options.Marshal(clusterDetails)
			compressedCd, _ := wscommon.Gzip(serializedClusterDetails)
			compressedDomains, _ := wscommon.Gzip([]byte(strings.Join(actToCsxDomains, ",")))

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job.Name),
					Namespace: job.Namespace,
					Labels: map[string]string{
						"job-name":             job.Name,
						wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeClusterDetails,
					},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion:         "jobs.cerebras.com/v1",
						BlockOwnerDeletion: wscommon.Pointer(true),
						Controller:         wscommon.Pointer(true),
						Kind:               "WSJob",
						Name:               job.Name,
						UID:                job.ObjectMeta.UID,
					}},
				},
				Data: map[string]string{
					wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.InitSignalVal,
				},
				BinaryData: map[string][]byte{
					wsapisv1.ClusterDetailsConfigCompressedFileName: compressedCd,
				},
			}
			if withActDomainsBinary {
				cm.BinaryData[wsapisv1.ClusterDetailsActToCsxDomains] = compressedDomains
			}
			return cm
		}

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1", "group2"},
				// 2 systems in one stamp, 1 system in another stamp
				// all 3 ax nodes in the first stamp
				SystemNames:         []string{"system-0", "system-2", "system-1"},
				ActivationNodeNames: []string{"ax-0", "ax-2", "ax-4"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		userNsJob5 := newInfExecuteJob(userNsrName, "user-ns-redistributed-parallel-experts-job-1", 3, 0, 0)
		userNsJob5.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob5))
		lock = assertLockNotExists(t.T(), userNsJob5.Namespace, userNsJob5.Name) // cluster details readiness blocks lock grant
		t.T().Log(lock)

		err := k8sClient.Create(t.Ctx, makeClusterDetailsCm(userNsJob5, true))
		require.NoError(t.T(), err)
		assertWsJobScheduledEventually(t.T(), userNsJob5.Namespace, userNsJob5.Name)
		require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKeyFromObject(userNsJob5), userNsJob5))
		assert.Equal(t.T(), fmt.Sprintf(AxSpineLoadMessage, 7, 9, 3), userNsJob5.Annotations[wscommon.ActV2SpineLoad])
		assertWsjobCleanedUp(t.T(), userNsJob5)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1", "group2"},
				// all 3 systems in the first stamp
				// 2 ax nodes in the one stamp, 1 ax node in another stamp
				SystemNames:         []string{"system-0", "system-2", "system-4"},
				ActivationNodeNames: []string{"ax-0", "ax-2", "ax-1"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		userNsJob6 := newInfExecuteJob(userNsrName, "user-ns-redistributed-parallel-experts-job-2", 3, 0, 0)
		userNsJob6.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob6))
		require.NoError(t.T(), k8sClient.Create(t.Ctx, makeClusterDetailsCm(userNsJob6, true)))
		assertWsJobScheduledEventually(t.T(), userNsJob6.Namespace, userNsJob6.Name)
		require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKeyFromObject(userNsJob6), userNsJob6))
		assert.Equal(t.T(), fmt.Sprintf(AxSpineLoadMessage, 6, 9, 3), userNsJob6.Annotations[wscommon.ActV2SpineLoad])
		assertWsjobCleanedUp(t.T(), userNsJob6)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1", "group2"},
				// all 3 systems and 3 ax nodes are in the same stamp
				// 2 ax nodes in the one stamp, 1 ax node in another stamp
				SystemNames:         []string{"system-0", "system-2", "system-4"},
				ActivationNodeNames: []string{"ax-0", "ax-2", "ax-6"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		userNsJob7 := newInfExecuteJob(userNsrName, "user-ns-redistributed-parallel-experts-job-3", 3, 0, 0)
		userNsJob7.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob7))
		require.NoError(t.T(), k8sClient.Create(t.Ctx, makeClusterDetailsCm(userNsJob7, true)))
		assertWsJobScheduledEventually(t.T(), userNsJob7.Namespace, userNsJob7.Name)
		require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKeyFromObject(userNsJob7), userNsJob7))
		assert.Equal(t.T(), fmt.Sprintf(AxSpineLoadMessage, 3, 9, 3), userNsJob7.Annotations[wscommon.ActV2SpineLoad])
		assertWsjobCleanedUp(t.T(), userNsJob7)

		// backward compatible test
		// server generated cluster details without the act domain binary is still functional with non-moe topology
		userNsJob8 := newInfExecuteJob(userNsrName, "user-ns-redistributed-parallel-experts-job-4", 3, 0, 0)
		userNsJob8.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), userNsJob8))
		require.NoError(t.T(), k8sClient.Create(t.Ctx, makeClusterDetailsCm(userNsJob8, false)))
		assertWsJobScheduledEventually(t.T(), userNsJob8.Namespace, userNsJob8.Name)
		require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKeyFromObject(userNsJob8), userNsJob8))
		assert.Equal(t.T(), fmt.Sprintf(AxSpineLoadMessage, 1, 3, 3), userNsJob8.Annotations[wscommon.ActV2SpineLoad])
		assertWsjobCleanedUp(t.T(), userNsJob8)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		// scale down the user nsr
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(0),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(systemNsr), systemNsr))
		// system ns picks up all resources
		systemNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(8),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, systemNsr))
		t.ensureNsUpdateSuccess(systemNsr)
	})

	t.Run("inference_scheduling_kvss_job_sharing_with_unhealthy_ax_node", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(10, 1, 0, 10, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 10)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		// Using 12 kvss replicas, evenly distributed across all domains, this would ensure each healthy ax gets at least one pod
		testDataFileName := "testdata/10_sys_12_kvss_cluster_details_req.json"

		tempDefaultActMem := defaultActMem
		defaultActMem = *resource.NewQuantity(1<<20, resource.BinarySI)
		defer func() {
			defaultActMem = tempDefaultActMem
		}()

		numSys, numKvss := 10, 10

		// Mark ax-0 and ax-1 unhealthy
		require.NoError(t.T(), patchNode(t, "ax-0", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))
		require.NoError(t.T(), patchNode(t, "ax-1", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))

		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-kvss-job-sharing-unhealthy-ax-node", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))

		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		// No pod should land on ax-0 or ax-1
		assertNodesScheduled(t, map[string]bool{
			"ax-2": true,
			"ax-3": true,
			"ax-4": true,
			"ax-5": true,
			"ax-6": true,
			"ax-7": true,
			"ax-8": true,
			"ax-9": true,
		}, lock, rlv1.KVStorageServerRequestName)

		assertPodCountEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name, "kvstorageserver", numKvss)

		// clean up job and retry with the same setup
		assertWsjobCleanedUp(t.T(), inferenceJob)

		// recover the ax nodes to healthy state, the new job should have pods landing on them
		require.NoError(t.T(), patchNode(t, "ax-0", []corev1.NodeCondition{}))
		require.NoError(t.T(), patchNode(t, "ax-1", []corev1.NodeCondition{}))

		inferenceJob2 := newInfExecuteJob(wscommon.Namespace, "inference-kvss-job-sharing-healthy-ax-node", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob2.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		createActKvssConfigMap(t.T(), inferenceJob2, testDataFileName)
		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock2)
		assertInferenceJobSharing(t.T(), lock2, true)

		// Pods should land on all ax nodes
		assertNodesScheduled(t, map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-2": true,
			"ax-3": true,
			"ax-4": true,
			"ax-5": true,
			"ax-6": true,
			"ax-7": true,
			"ax-8": true,
			"ax-9": true,
		}, lock2, rlv1.KVStorageServerRequestName)

		assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, "kvstorageserver", numKvss)
	})
}

func (t *TestSuite) TestV2NetworkingClusterMisc() {
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	assertNodeStamps := func(nodeRole string, expectedStamps map[string]int) {
		actualStamps := map[string]int{}
		for _, node := range t.CfgMgr.Cfg.NodeMap {
			if _, ok := node.Properties[nodeRole]; ok {
				actualStamps[node.Properties[wsresource.StampPropertyKey]]++
			}
		}
		assert.Equal(t.T(), expectedStamps, actualStamps)
	}

	assertSwitchPreferredCsPorts := func(switchName string, expectedPreferredCsPorts string) {
		actualPreferredCsPorts := ""
		for _, g := range t.CfgMgr.Cfg.V2Groups {
			if g.Name == switchName {
				actualPreferredCsPorts = g.Properties[wsresource.PreferredCsPortsPropKey]
				break
			}
		}
		assert.Equal(t.T(), expectedPreferredCsPorts, actualPreferredCsPorts)
	}

	assertSystemStamps := func(expectedStamps map[string]int) {
		actualStamps := map[string]int{}
		for _, system := range t.CfgMgr.Cfg.SystemMap {
			actualStamps[system.Properties[wsresource.StampPropertyKey]]++
		}
		assert.Equal(t.T(), expectedStamps, actualStamps)
	}

	getLmrCoefficients := func(testDataFileName string, taskType commonpb.ClusterDetails_TaskInfo_TaskType) []int {
		clusterDetailsData, err := ioutil.ReadFile(testDataFileName)
		require.NoError(t.T(), err)

		clusterDetails := &commonpb.ClusterDetails{}
		options := protojson.UnmarshalOptions{
			AllowPartial:   true,
			DiscardUnknown: true,
		}

		err = options.Unmarshal(clusterDetailsData, clusterDetails)
		require.NoError(t.T(), err)

		var coefficients []int
		var linearMemoryRange *commonpb.LinearMemoryRange

		for _, taskInfo := range clusterDetails.Tasks {
			if taskInfo.TaskType == taskType {
				if linearMemoryRange == nil && taskInfo.ResourceInfo != nil {
					linearMemoryRange = taskInfo.ResourceInfo.LinearMemoryRange
				}

				for _, taskMap := range taskInfo.TaskMap {
					var coef int64
					if taskMap.LinearMemoryRangeOverride == nil || taskMap.LinearMemoryRangeOverride.Coefficient == nil {
						coef = *linearMemoryRange.Coefficient
					} else {
						coef = *taskMap.LinearMemoryRangeOverride.Coefficient
					}
					coefficients = append(coefficients, int(coef))
				}
			}
		}
		return coefficients
	}

	assertMemoryGrantedBasedOnRatio := func(pods *corev1.PodList, coef []int, b, x int) {
		expectedSetMem := map[int]int{}
		actualSetMem := map[int]int{}
		expectedSetMemBytes := map[int]int{}
		actualSetMemBytes := map[int]int{}

		for _, k := range coef {
			memoryBytes := k*x + b
			roundedMemoryMiB := int(math.Ceil(float64(memoryBytes) / 1024 / 1024))
			expectedSetMem[roundedMemoryMiB] += 1
			expectedSetMemBytes[roundedMemoryMiB<<20] += 1
		}

		for _, pod := range pods.Items {
			gotSetMem, gotSetMemBytes := false, false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
					setMem, err := strconv.Atoi(env.Value)
					gotSetMem = true
					assert.True(t.T(), err == nil)
					actualSetMem[setMem] += 1
				}

				if env.Name == wsapisv1.RuntimeMemoryBytesEnvVarName {
					setMemBytes, err := strconv.Atoi(env.Value)
					gotSetMemBytes = true
					assert.True(t.T(), err == nil)
					actualSetMemBytes[setMemBytes] += 1
				}
			}
			assert.True(t.T(), gotSetMem)
			assert.True(t.T(), gotSetMemBytes)
		}

		assert.Equal(t.T(), expectedSetMem, actualSetMem)
		assert.Equal(t.T(), expectedSetMemBytes, actualSetMemBytes)
	}

	assertEnvVarSetForAllPods := func(pods *corev1.PodList, envVarName string) {
		for _, pod := range pods.Items {
			gotEnvVar := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == envVarName && env.Value != "" {
					gotEnvVar = true
				}
			}
			assert.True(t.T(), gotEnvVar, fmt.Sprintf("env variable %s not set", envVarName))
		}
	}

	assertPortUsageEvenlyDistributed := func(resourceGrants []rlv1.ResourceGrant, allowedMaxDifference int, testFileName string, useRequestLmr bool) int {
		systemDomainPortUsage := map[string]map[int]map[int]int{}
		maxDiff := 0

		// In general, we want to look at the annotated value, which contains normalized usage
		// When useRequestLmr is true, we use the raw value in the memory requests
		podIoRequests := make([]int, 0)
		if useRequestLmr {
			podIoRequests = getLmrCoefficients(testFileName, commonpb.ClusterDetails_TaskInfo_KVSS)
		}

		for _, grant := range resourceGrants {
			annotations := grant.Annotations

			systemId, hasSystemId := annotations[rlv1.SystemIdAnnotKey]
			csPortStr, hasCsPort := annotations[rlv1.TargetCsPortAnnotKey]
			ioUsageStr, hasIoUsage := annotations[rlv1.WioBandwidthAnnotKey]
			assert.True(t.T(), hasSystemId, "ResourceGrant %s missing system-id annotation", grant.Name)
			assert.True(t.T(), hasCsPort, "ResourceGrant %s missing cs-port annotation", grant.Name)
			assert.True(t.T(), hasIoUsage, "ResourceGrant %s missing io-usage annotation", grant.Name)

			csPort, _ := strconv.Atoi(csPortStr)
			ioUsage, _ := strconv.Atoi(ioUsageStr)
			podIdx, _ := strconv.Atoi(grant.Name)

			if podIdx < len(podIoRequests) {
				ioUsage = podIoRequests[podIdx]
			}

			domain := csPort / 3

			systemDomainPortUsage[systemId] = wscommon.EnsureMap(systemDomainPortUsage[systemId])
			systemDomainPortUsage[systemId][domain] = wscommon.EnsureMap(systemDomainPortUsage[systemId][domain])
			systemDomainPortUsage[systemId][domain][csPort] += ioUsage
		}

		for systemId, domainPortUsage := range systemDomainPortUsage {
			for domain, portUsage := range domainPortUsage {
				if len(portUsage) == 0 {
					continue
				}

				minUsage := math.MaxInt
				maxUsage := 0

				for _, usage := range portUsage {
					if usage < minUsage {
						minUsage = usage
					}
					if usage > maxUsage {
						maxUsage = usage
					}
				}

				// The difference depends on the data condition on domain assignment and usage in cluster details
				difference := maxUsage - minUsage
				if difference > maxDiff {
					maxDiff = difference
				}
				assert.True(t.T(), difference <= allowedMaxDifference,
					"System %s Domain %d io-usage distribution not even: difference is %d (max: %d, min: %d), port usage: %v",
					systemId, domain, difference, maxUsage, minUsage, portUsage)
			}
		}
		return maxDiff
	}

	assertGlobalPortUsageEvenlyDistributed := func(resourceGrants []rlv1.ResourceGrant, allowedMaxDifference int) {
		portUsage := map[string]int{}

		for _, grant := range resourceGrants {
			annotations := grant.Annotations

			csPortStr, hasCsPort := annotations[rlv1.TargetCsPortAnnotKey]
			assert.True(t.T(), hasCsPort, "ResourceGrant %s missing cs-port annotation", grant.Name)
			portUsage[csPortStr]++
		}

		minUsage := math.MaxInt
		maxUsage := 0
		for _, usage := range portUsage {
			if usage < minUsage {
				minUsage = usage
			}
			if usage > maxUsage {
				maxUsage = usage
			}
		}
		maxDiff := maxUsage - minUsage
		assert.True(t.T(), maxDiff <= allowedMaxDifference,
			"usage difference: %d across ports larger than allowedMaxDifference: %d", maxDiff, allowedMaxDifference)
	}

	assertAxNodesScheduledAndBalanced := func(expectedNodes map[string]bool, lock *rlv1.ResourceLock, requestName string) {
		actualAxNodes := map[string]bool{}
		axNodeCount := map[string]int{}
		for _, gg := range lock.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, requestName) {
				continue
			}
			for _, rg := range gg.ResourceGrants {
				for _, res := range rg.Resources {
					actualAxNodes[res.Name] = true
					axNodeCount[res.Name]++
				}
			}
		}

		// Validate that the number of kvss per ax node would differ at most by 1 across all granted ax nodes
		maxPodCountPerAx := -1
		minPodCountPerAx := -1
		for _, v := range axNodeCount {
			if maxPodCountPerAx == -1 {
				maxPodCountPerAx = v
				minPodCountPerAx = v
			} else {
				if v > maxPodCountPerAx {
					maxPodCountPerAx = v
				}

				if v < minPodCountPerAx {
					minPodCountPerAx = v
				}
			}
		}

		assert.True(t.T(), minPodCountPerAx > 0)
		assert.True(t.T(), maxPodCountPerAx > 0)
		if lock.Annotations[wsapisv1.InferenceJobSharingAnnotKey] != strconv.FormatBool(true) {
			assert.True(t.T(), maxPodCountPerAx-minPodCountPerAx <= 1)
		} else {
			t.T().Log(fmt.Sprintf("max pod count: %v, min pod count: %v, diff: %v", maxPodCountPerAx, minPodCountPerAx, maxPodCountPerAx-minPodCountPerAx))
		}
		assert.Equal(t.T(), expectedNodes, actualAxNodes)
	}

	assertSubResource := func(nodeName, subResName string, subResValue int) {
		swDriverNode1, ok := t.ResourceController.GetCollection().Get(nodeName)
		assert.True(t.T(), ok)
		allocated := swDriverNode1.GetAllocatedSubresources()
		for k, v := range allocated {
			if k == subResName {
				assert.Equal(t.T(), subResValue, v)
				return
			}
		}
		assert.Fail(t.T(), fmt.Sprintf("Subresource not found: %s", subResName))
	}

	getTotalMemoryLimitForPods := func(pods *corev1.PodList) int {
		memorySum := 0
		for _, pod := range pods.Items {
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
					setMem, _ := strconv.Atoi(env.Value)
					memorySum += setMem
				}
			}
		}
		return memorySum
	}

	t.Run("checkV2Network - one stamp", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(3, 1, 12, 2, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertSystemStamps(map[string]int{"0": 3})
		assertNodeStamps(wsresource.RoleAxPropKey, map[string]int{"0": 2})
		assertNodeStamps(wsresource.RoleBRPropKey, map[string]int{"0": 12})
		assertNodeStamps(wsresource.RoleMemoryPropKey, map[string]int{})
		assertNodeStamps(wsresource.RoleWorkerPropKey, map[string]int{})
		assertSwitchPreferredCsPorts("leaf-0-0", "0,1,2")
		assertSwitchPreferredCsPorts("leaf-0-1", "3,4,5")
		assertSwitchPreferredCsPorts("leaf-0-2", "6,7,8")
		assertSwitchPreferredCsPorts("leaf-0-3", "9,10,11")
	})

	t.Run("checkV2Network_multiple_stamps", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 30, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(4, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertSystemStamps(map[string]int{"0": 2, "1": 2, "2": 2, "3": 2})
		assertNodeStamps(wsresource.RoleAxPropKey, map[string]int{"0": 2, "1": 2, "2": 2, "3": 2})
		assertNodeStamps(wsresource.RoleBRPropKey, map[string]int{"0": 8, "1": 8, "2": 7, "3": 7})
		assertSwitchPreferredCsPorts("leaf-0-0", "0,1,2")
		assertSwitchPreferredCsPorts("leaf-1-1", "3,4,5")
		assertSwitchPreferredCsPorts("leaf-2-2", "6,7,8")
		assertSwitchPreferredCsPorts("leaf-3-3", "9,10,11")
	})

	t.Run("checkV2Network_no_stamp", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(3, 1, 12, 6, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(0, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertSystemStamps(map[string]int{"": 3})
		assertNodeStamps(wsresource.RoleAxPropKey, map[string]int{"": 6})
		assertNodeStamps(wsresource.RoleBRPropKey, map[string]int{"": 12})
		// ax-csx ratio discovery disabled by default
		assert.Equal(t.T(), 1, wsapisv1.AxCountPerSystem)
	})

	t.Run("checkV2Network_ax-csx_ratio_discovery_enabled_explicitly", func() {
		wsapisv1.AxCountPerSystemDiscoveryEnabled = true
		defer func() { wsapisv1.AxCountPerSystemDiscoveryEnabled = false }()
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(3, 1, 12, 6, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertSystemStamps(map[string]int{"0": 3})
		assertNodeStamps(wsresource.RoleAxPropKey, map[string]int{"0": 6})
		assertNodeStamps(wsresource.RoleBRPropKey, map[string]int{"0": 12})
		assert.Equal(t.T(), 2, wsapisv1.AxCountPerSystem)
	})

	t.Run("checkV2Network_enforced_2_stamps", func() {
		wsapisv1.StampCountOverride = 2
		defer func() { wsapisv1.StampCountOverride = -1 }()
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 29, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(4, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertSystemStamps(map[string]int{"0": 4, "1": 4})
		assertNodeStamps(wsresource.RoleAxPropKey, map[string]int{"0": 4, "1": 4})
		assertNodeStamps(wsresource.RoleBRPropKey, map[string]int{"0": 15, "1": 14})
	})

	t.Run("checkV2Network_enforced_5_stamps", func() {
		wsapisv1.StampCountOverride = 5
		defer func() { wsapisv1.StampCountOverride = -1 }()
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 29, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(4, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assertSystemStamps(map[string]int{"0": 2, "1": 2, "2": 2, "3": 1, "4": 1})
		assertNodeStamps(wsresource.RoleAxPropKey, map[string]int{"0": 2, "1": 2, "2": 2, "3": 1, "4": 1})
		assertNodeStamps(wsresource.RoleBRPropKey, map[string]int{"0": 6, "1": 6, "2": 6, "3": 6, "4": 5})
	})

	t.Run("csx/ax_node_reload", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 2, 30, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(4, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))

		v2CfgWithoutSys := v2Cfg
		v2CfgWithoutSys.Systems = []*systemv1.SystemSpec{}
		t.NoError(t.CfgMgr.Initialize(v2CfgWithoutSys))
		assert.Equal(t.T(), 0, wsapisv1.AxCountPerSystem)
		assert.Equal(t.T(), false, wsapisv1.UseAxScheduling)

		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		assert.Equal(t.T(), 1, wsapisv1.AxCountPerSystem)
		assert.Equal(t.T(), true, wsapisv1.UseAxScheduling)
	})

	t.Run("scheduling_ax-only-for-runtime_should_succeed", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 1, 12, 4, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 4)

		trainJob := newWsExecuteJob(wscommon.Namespace, "ax-only-for-runtime", 4)
		trainJob.Annotations[wsapisv1.UseAxOnlyForRuntimeKey] = wsapisv1.UseAxOnlyForRuntimeValue
		for rt, spec := range trainJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeWeight {
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: actOnlyWgtMem,
					corev1.ResourceCPU:    actOnlyWgtCpu,
				}
			}
		}
		require.NoError(t.T(), k8sClient.Create(context.Background(), trainJob))
		defer assertWsjobCleanedUp(t.T(), trainJob)

		assertPodCountEventually(t.T(), trainJob.Namespace, trainJob.Name, string(wsapisv1.WSReplicaTypeCoordinator), 1)

		lock := assertLockExists(t.T(), trainJob.Namespace, trainJob.Name)
		assert.True(t.T(), lock.Status.State == rlv1.LockGranted)
		assert.Equal(t.T(), 8, len(lock.Status.GroupResourceGrants))
		assert.Equal(t.T(), 2, len(lock.Spec.ResourceRequests))

		foundWgt := false
		foundCmd := false
		for _, grant := range lock.Status.GroupResourceGrants {
			if strings.HasPrefix(grant.Name, rlv1.WgtCmdGroupPrefix) {
				assert.Equal(t.T(), 2, len(grant.ResourceGrants))
				for _, resourceGrant := range grant.ResourceGrants {
					if strings.HasPrefix(resourceGrant.Name, string(wsapisv1.WSReplicaTypeWeight)) {
						foundWgt = true
					} else if resourceGrant.Name == string(wsapisv1.WSReplicaTypeCommand) {
						foundCmd = true
					}
					assert.True(t.T(), strings.HasPrefix(resourceGrant.Name, string(wsapisv1.WSReplicaTypeWeight)) ||
						resourceGrant.Name == string(wsapisv1.WSReplicaTypeCommand),
						"ResourceGrant should be either starting with %s or %s (seen %s)",
						string(wsapisv1.WSReplicaTypeWeight),
						string(wsapisv1.WSReplicaTypeCommand), resourceGrant.Name)
				}
			}
		}
		assert.True(t.T(), foundWgt && foundCmd, "weight/command group request should be granted")
	})

	t.Run("scheduling_ax-only-for-runtime_with_upper_bound_+_limit_should_succeed", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 12, 2, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 2)

		trainJob := newWsExecuteJob(wscommon.Namespace, "ax-only-for-runtime-upper-bound-limit", 2)
		// set limit overhead to 1gb, 4.5g*0.1<1g
		overheadMB := 1 << 10
		trainJob.Annotations[wsapisv1.RtNodeMaxLimitReservedMemGbKey] = "1"
		trainJob.Annotations[wsapisv1.RtNodeMaxLimitReservedMemRatioKey] = "0.1"
		for rt, spec := range trainJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity((wscommon.DefaultNodeMem/32)<<20, resource.BinarySI),
					corev1.ResourceCPU:    defaultActCpu,
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 16) << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeWeight {
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: actOnlyWgtMem,
					corev1.ResourceCPU:    actOnlyWgtCpu,
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 7) << 20),
					wsapisv1.PodMemoryMaxLimitKey:   strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(4))
			} else if rt == wsapisv1.WSReplicaTypeCommand {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 7) << 20),
					wsapisv1.PodMemoryMaxLimitKey:   strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(1))
			}
		}

		trainJob.Annotations[wsapisv1.UseAxOnlyForRuntimeKey] = wsapisv1.UseAxOnlyForRuntimeValue
		require.NoError(t.T(), k8sClient.Create(context.Background(), trainJob))
		defer assertWsjobCleanedUp(t.T(), trainJob)

		// First AX node:
		//   4 act with wscommon.DefaultNodeMem/16
		//   2 wgt with wscommon.DefaultNodeMem/7
		//   1 cmd with wscommon.DefaultNodeMem/7
		// Second AX node:
		//   4 act with wscommon.DefaultNodeMem/16
		//   2 wgt with wscommon.DefaultNodeMem/7
		perPodExtra1 := (wscommon.DefaultNodeMem - overheadMB - 4*int(wscommon.DefaultNodeMem/16) - 3*int(wscommon.DefaultNodeMem/7)) / 7
		perPodExtra2 := (wscommon.DefaultNodeMem - overheadMB - 4*int(wscommon.DefaultNodeMem/16) - 2*int(wscommon.DefaultNodeMem/7)) / 6

		pods := assertPodCountEventually(t.T(), trainJob.Namespace, "", "activation", 8)
		for _, pod := range pods.Items {
			requests := pod.Spec.Containers[0].Resources.Requests
			perPodExtra := perPodExtra1
			if pod.Spec.NodeSelector["kubernetes.io/hostname"] == "ax-1" {
				perPodExtra = perPodExtra2
			}
			assert.Equal(t.T(), wscommon.DefaultNodeMem/16+perPodExtra, int(requests.Memory().Value()>>20))
			foundMemEnvVar := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
					assert.Equal(t.T(), strconv.Itoa(wscommon.DefaultNodeMem/16+perPodExtra), env.Value)
					foundMemEnvVar = true
				}
			}
			assert.True(t.T(), foundMemEnvVar)
		}

		pods = assertPodCountEventually(t.T(), trainJob.Namespace, "", "weight", 4)
		for _, pod := range pods.Items {
			_, ok := pod.Spec.NodeSelector["k8s.cerebras.com/node-role-activation"]
			assert.True(t.T(), ok)

			assert.Equal(t.T(), len(pod.Spec.Containers[0].Resources.Limits), 0)
			requests := pod.Spec.Containers[0].Resources.Requests
			perPodExtra := perPodExtra1
			if pod.Spec.NodeSelector["kubernetes.io/hostname"] == "ax-1" {
				perPodExtra = perPodExtra2
			}
			assert.Equal(t.T(), wscommon.DefaultNodeMem/7+perPodExtra, int(requests.Memory().Value()>>20))
		}

		pods = assertPodCountEventually(t.T(), trainJob.Namespace, "", "command", 1)
		pod := pods.Items[0]
		_, ok := pod.Spec.NodeSelector["k8s.cerebras.com/node-role-activation"]
		assert.True(t.T(), ok)
		assert.Equal(t.T(), len(pod.Spec.Containers[0].Resources.Limits), 0)
		requests := pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), wscommon.DefaultNodeMem/7+perPodExtra1, int(requests.Memory().Value()>>20))

		lock := assertLockExists(t.T(), trainJob.Namespace, trainJob.Name)
		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 3)
		for _, ub := range lock.Status.GroupResourceGrantUpperBounds {
			if ub.Name == string(wsapisv1.WSReplicaTypeActivation) {
				assert.Equal(t.T(), wscommon.DefaultNodeMem/16, ub.Subresources[wsresource.MemSubresource])
			} else if ub.Name == string(wsapisv1.WSReplicaTypeWeight) {
				assert.Equal(t.T(), wscommon.DefaultNodeMem/7, ub.Subresources[wsresource.MemSubresource])
			} else if ub.Name == string(wsapisv1.WSReplicaTypeCommand) {
				assert.Equal(t.T(), wscommon.DefaultNodeMem/7, ub.Subresources[wsresource.MemSubresource])
			}
		}
	})

	t.Run("scheduling_ax-only-for-runtime_with_upper_bound_should_succeed", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(1, 1, 0, 1, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 1)

		// 4 act pods, 1 wgt, 1 cmd.
		podExtraMemMi := (wscommon.DefaultNodeMem - 4*(defaultActMem.Value()>>20) - (actOnlySmallWgtMem.Value() >> 20) - (defaultCmdMem.Value() >> 20)) / 6
		expectedActMemMi := (defaultActMem.Value() >> 20) + podExtraMemMi
		expectedWgtMemMi := (actOnlySmallWgtMem.Value() >> 20) + podExtraMemMi
		expectedCmdMemMi := (defaultCmdMem.Value() >> 20) + podExtraMemMi

		trainJob := newWsExecuteJob(wscommon.Namespace, "ax-only-for-runtime-upper-bound", 1)
		for rt, spec := range trainJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeWeight {
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: actOnlySmallWgtMem,
					corev1.ResourceCPU:    actOnlySmallWgtCpu,
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(1))
			} else if rt == wsapisv1.WSReplicaTypeCommand {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(1))
			}
		}

		trainJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		trainJob.Annotations[wsapisv1.UseAxOnlyForRuntimeKey] = wsapisv1.UseAxOnlyForRuntimeValue
		require.NoError(t.T(), k8sClient.Create(context.Background(), trainJob))
		defer assertWsjobCleanedUp(t.T(), trainJob)

		pods := assertPodCountEventually(t.T(), trainJob.Namespace, "", "activation", 4)
		pod := pods.Items[0]
		requests := pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), expectedActMemMi<<20, requests.Memory().Value())
		foundMemEnvVar := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
				assert.Equal(t.T(), strconv.Itoa(int(expectedActMemMi)), env.Value)
				foundMemEnvVar = true
			}
		}
		assert.True(t.T(), foundMemEnvVar)

		// Should not set limit if only request was originally specified
		pods = assertPodCountEventually(t.T(), trainJob.Namespace, "", "weight", 1)
		pod = pods.Items[0]
		assert.Equal(t.T(), len(pod.Spec.Containers[0].Resources.Limits), 0)
		requests = pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), expectedWgtMemMi<<20, requests.Memory().Value())

		pods = assertPodCountEventually(t.T(), trainJob.Namespace, "", "command", 1)
		pod = pods.Items[0]
		assert.Equal(t.T(), len(pod.Spec.Containers[0].Resources.Limits), 0)
		requests = pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), expectedCmdMemMi<<20, requests.Memory().Value())

		lock := assertLockExists(t.T(), trainJob.Namespace, trainJob.Name)
		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 3)
		for _, ub := range lock.Status.GroupResourceGrantUpperBounds {
			if ub.Name == rlv1.ActivationRequestName {
				assert.Equal(t.T(), expectedActMemMi, int64(ub.Subresources[wsresource.MemSubresource]))
			} else if ub.Name == rlv1.WeightRequestName {
				assert.Equal(t.T(), expectedWgtMemMi, int64(ub.Subresources[wsresource.MemSubresource]))
			} else if ub.Name == rlv1.CommandRequestName {
				assert.Equal(t.T(), expectedCmdMemMi, int64(ub.Subresources[wsresource.MemSubresource]))
			}
		}
	})

	t.Run("v2network_kvss_linear_memory_override_without_job_sharing", func() {
		ctx := context.Background()
		deleteNodes(t.T())
		// 2 systems, 4 AX and 6 KVSS pods
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 0, 4, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		require.NoError(t.T(), wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		require.NoError(t.T(), t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 2)

		job := newInfExecuteJob(wscommon.Namespace, "kvss-lmr-non-job-sharing", 2, 6, 0)
		job.Annotations[wsapisv1.SemanticClusterServerVersion] =
			wsapisv1.SoftwareFeatureSemverMap[wsapisv1.LinearMemoryRangeSupport].String()
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		// inject the override JSON
		createActKvssConfigMap(t.T(), job, "testdata/2_sys_6_kvss_linear_memory_override.json")
		// assert lock exists and is locked
		lock := assertLockExists(t.T(), job.Namespace, job.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, false)
		// wait until exactly 6 KVSS pods appear
		pods := assertPodCountEventually(
			t.T(),
			job.Namespace,
			job.Name,
			rlv1.KVStorageServerRequestName,
			6,
		)

		distinct := map[resource.Quantity]bool{}
		for _, p := range pods.Items {
			if p.Labels["replica-type"] != strings.ToLower(rlv1.KVStorageServerRequestName) {
				continue
			}
			// collect the request
			mi := p.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]
			distinct[mi] = true

			// Ensure WS_RT_SET_MEM_MIB env matches the memory request
			memMi := (&mi).Value() >> 20
			found := false
			for _, env := range p.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName &&
					env.Value == strconv.Itoa(int(memMi)) {
					found = true
					break
				}
			}
			require.True(t.T(), found,
				"pod %s: expected %s=%d, envs=%+v",
				p.Name, wsapisv1.RuntimeMemoryEnvVarName, memMi, p.Spec.Containers[0].Env)
		}

		// Each pod should have a distinct memory override
		require.Equal(t.T(), 6, len(distinct),
			"expected per-replica KVSS memory to differ under override, got %v", distinct)
	})

	t.Run("v2network_kvss_linear_memory_override_job_sharing", func() {
		ctx := context.Background()
		deleteNodes(t.T())

		// 1 system, 2 KVSS pods (job‐sharing)
		v2Cfg := wscommon.NewTestClusterConfigSchema(1, 1, 0, 2, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		require.NoError(t.T(), wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		require.NoError(t.T(), t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 1)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		// create a WSJob with 1 system and 2 KVSS replicas
		job := newInfExecuteJob(wscommon.Namespace, "kvss-lmr-job-sharing", 1, 2, 0)
		job.Annotations[wsapisv1.SemanticClusterServerVersion] =
			wsapisv1.SoftwareFeatureSemverMap[wsapisv1.LinearMemoryRangeSupport].String()
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		// inject the override JSON that carves 32Gi into two distinct chunks
		createActKvssConfigMap(t.T(), job, "testdata/1_sys_2_kvss_job_sharing_memory_override.json")

		// wait for the lock and pods
		lock := assertLockExists(t.T(), job.Namespace, job.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)
		pods := assertPodCountEventually(
			t.T(),
			job.Namespace,
			job.Name,
			rlv1.KVStorageServerRequestName,
			2,
		)

		pod := pods.Items[0]
		gotExpectedSetLMRX := false
		for _, env := range pod.Spec.Containers[0].Env {
			// We don't have enough memory to scale upperbound to 2
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(1) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)

		// ensure we got exactly 2 distinct memory requests
		distinct := map[resource.Quantity]bool{}
		for _, p := range pods.Items {
			if p.Labels["replica-type"] != strings.ToLower(rlv1.KVStorageServerRequestName) {
				continue
			}
			mi := p.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]
			distinct[mi] = true
		}
		require.Equal(t.T(), 2, len(distinct),
			"expected 2 distinct KVSS memory slices under override, got %v", distinct)
	})

	t.Run("inference_scheduling_kvss_without_job_sharing,_allocate_to_upperbound", func() {
		deleteNodes(t.T())
		// Each AX talks to 2 domains:
		// AX 0, 2, 4, 6 => domain 0, 1
		// AX 1, 3, 5, 7 => domain 2, 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 8)

		numKvss := 8
		podExtraMemMi := (wscommon.DefaultNodeMem - 4*(defaultActMem.Value()>>20) - defaultKvssMem.Value()>>20) / 5
		expectedKvssMemMi := (defaultKvssMem.Value() >> 20) + podExtraMemMi

		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-kvss-no-job-sharing", 8, uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// 8 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob, "testdata/8_sys_8_kvss_cluster_details_req.json")
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, false)

		// All AXs scheduled
		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-2": true,
			"ax-3": true,
			"ax-4": true,
			"ax-5": true,
			"ax-6": true,
			"ax-7": true,
		}, lock, rlv1.KVStorageServerRequestName)

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)
		for _, ub := range lock.Status.GroupResourceGrantUpperBounds {
			if ub.Name == rlv1.KVStorageServerRequestName {
				assert.Equal(t.T(), expectedKvssMemMi, int64(ub.Subresources[wsresource.MemSubresource]))
			}
		}

		// 32 act, 8 kvss, all placed with affinity
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 40, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		pod := pods.Items[0]
		requests := pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), expectedKvssMemMi<<20, requests.Memory().Value())
		gotExpectedSetMem := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Value == strconv.Itoa(int(expectedKvssMemMi)) {
				gotExpectedSetMem = true
			}
		}
		assert.True(t.T(), gotExpectedSetMem)
	})

	t.Run("inference_scheduling_kvss_without_job_sharing,_cross_stamp", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(2, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))

		systemNsr := t.createNsIfNotExists(wscommon.Namespace)
		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(systemNsr), systemNsr))
		systemNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(0),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, systemNsr))
		t.ensureNsUpdateSuccess(systemNsr)

		userNsrName := "user-ns-ax-kvss-no-job-sharing-cross-stamp"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1", "group2"},
				// 3 systems in stamp 0
				// 2 ax nodes in stamp 0, 1 ax node in stamp 1
				SystemNames:         []string{"system-0", "system-2", "system-4"},
				ActivationNodeNames: []string{"ax-0", "ax-4", "ax-3"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{0}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		numSys, numKvss := 3, 9
		inferenceJob := newInfExecuteJob(userNsrName, "inference-kvss-no-job-sharing-cross-stamp", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// 9 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob, "testdata/3_sys_9_kvss_cluster_details_req.json")
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, false)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-3": true,
			"ax-4": true,
		}, lock, rlv1.KVStorageServerRequestName)

		assertSystemsOrdered(t, []string{
			"system-0",
			"system-2",
			"system-4"}, lock)

		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 4, numOneHopTraffic)
		assert.Equal(t.T(), 8, numMultiHopTraffic)

		assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", 9)
	})

	t.Run("inference_scheduling_kvss_job_sharing_&_linear_memory_granting_bounded_by_node_resource", func() {
		deleteNodes(t.T())
		// Each AX talks to 2 domains:
		// AX 0, 2, 4, 6 => domain 0, 1
		// AX 1, 3, 5, 7 => domain 2, 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 8)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		numSys, numKvss := 4, 10
		aggrRemainingNodeMemory := float64(wscommon.DefaultNodeMem * 8)
		sysRatio := float64(numSys) / 8
		aggrActMemoryRequested := float64(len(defaultActToCsxDomains)) * float64(numSys) * float64((defaultActMem.Value() >> 20))

		// 16 act pods, 10 kvss pods. 4/8 systems requested
		expectedKvssMemMi := int64((aggrRemainingNodeMemory*sysRatio - aggrActMemoryRequested) / float64(numKvss))

		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-kvss-job-sharing", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// 10 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob, "testdata/4_sys_10_kvss_cluster_details_req.json")
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		// 7 kvss -> domain 0: AX 0, 2, 4, 6
		// 1 kvss -> domain 1: AX 6
		// 1 kvss -> domain 2: AX 1
		// 1 kvss -> domain 3: AX 3
		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-2": true,
			"ax-4": true,
			"ax-6": true,
			"ax-1": true,
			"ax-3": true,
		}, lock, rlv1.KVStorageServerRequestName)

		// 16 act, 10 kvss, all placed with affinity
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 26, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		pod := pods.Items[0]
		requests := pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), expectedKvssMemMi<<20, requests.Memory().Value())
		gotExpectedSetMem := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Value == strconv.Itoa(int(expectedKvssMemMi)) {
				gotExpectedSetMem = true
			}
		}
		assert.True(t.T(), gotExpectedSetMem)

		// Schedule second job
		numKvssJob2 := 20
		inferenceJobNameJob2 := "inference-kvss-job-sharing2"

		// AX-7 would have the lowest remaining memory among all nodes, and becomes the bottleneck, it has 2 ACT pods at 562Mi from job1,
		// and 3 ACT pods at 562Mi, 4 KVSS pod at 281Mi (base) from job 2.
		// Uniform extra per pod is: (4500 - 5*562 - 4*281) / 4 = 141
		expectedKvssExtraMemMi := (wscommon.DefaultNodeMem - (defaultKvssMem.Value()>>20)*4 - (defaultActMem.Value()>>20)*5) / 4
		expectedKvssMemMi2 := defaultKvssMem.Value()>>20 + expectedKvssExtraMemMi

		inferenceJob2 := newInfExecuteJob(wscommon.Namespace, inferenceJobNameJob2, uint32(numSys), uint32(numKvssJob2), 0)

		// Test no upperbound scenario for kvss in job2
		for rt, spec := range inferenceJob2.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
				}
			}
		}

		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		// 20 kvss pods, all requesting domain 0
		createActKvssConfigMap(t.T(), inferenceJob2, "testdata/4_sys_20_kvss_all_domain_0_cluster_details_req.json")

		lock = assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock)

		// kvss distributed to less optimal AX nodes due to not enough resources on AX 0, 2, 4, 6
		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-2": true,
			"ax-3": true,
			"ax-4": true,
			"ax-5": true,
			"ax-6": true,
			"ax-7": true,
		}, lock, rlv1.KVStorageServerRequestName)
		numOneHopTraffic, numMultiHopTraffic = wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assertInferenceJobSharing(t.T(), lock, true)

		// 16 act, 20 kvss, 16 placed on less optimal nodes, incurring spine traffic
		assert.Equal(t.T(), 20, numOneHopTraffic)
		assert.Equal(t.T(), 16, numMultiHopTraffic)

		// 20 + 10 kvss pods total in the 2 jobs
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, "", "kvstorageserver", numKvss+numKvssJob2)
		pods = assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJobNameJob2, "kvstorageserver", numKvssJob2)
		pod = pods.Items[0]
		requests = pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), expectedKvssMemMi2<<20, requests.Memory().Value())
		gotExpectedSetMem = false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Value == strconv.Itoa(int(expectedKvssMemMi2)) {
				gotExpectedSetMem = true
			}
		}
		assert.True(t.T(), gotExpectedSetMem)
	})

	t.Run("inference_scheduling_kvss_with_job_sharing,_cross_stamp", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(2, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))

		systemNsr := t.createNsIfNotExists(wscommon.Namespace)
		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(systemNsr), systemNsr))
		systemNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.ModeNotSet,
				SystemCount: wscommon.Pointer(0),
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, systemNsr))
		t.ensureNsUpdateSuccess(systemNsr)

		userNsrName := "user-ns-ax-kvss-job-sharing-cross-stamp"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1", "group2"},
				// 3 systems in stamp 0
				// 2 ax nodes in stamp 0, 1 ax node in stamp 1
				SystemNames:         []string{"system-0", "system-2", "system-4"},
				ActivationNodeNames: []string{"ax-0", "ax-4", "ax-3"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		// Turn on session level job sharing
		t.updateSessionLabel(userNsrName, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(userNsrName, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		_defaultActToCsxDomains := defaultActToCsxDomains
		defaultActToCsxDomains = [][]int{{0}}
		defer func() {
			defaultActToCsxDomains = _defaultActToCsxDomains
		}()

		numSys, numKvss := 3, 9
		inferenceJob := newInfExecuteJob(userNsrName, "inference-kvss-job-sharing-cross-stamp", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// 9 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob, "testdata/3_sys_9_kvss_cluster_details_req.json")
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-3": true,
			"ax-4": true,
		}, lock, rlv1.KVStorageServerRequestName)

		assertSystemsOrdered(t, []string{
			"system-0",
			"system-2",
			"system-4"}, lock)

		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 5, numOneHopTraffic)
		assert.Equal(t.T(), 7, numMultiHopTraffic)

		assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", 9)
	})

	t.Run("inference_scheduling_kvss_job_sharing_&_linear_memory_granting_bounded_by_LMR_upperbound", func() {
		deleteNodes(t.T())
		// Each AX talks to 2 domains:
		// AX 0 => domain 0, 1
		// AX 1 => domain 2, 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 0, 2, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 2)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()
		testDataFileName := "testdata/1_sys_5_kvss_linear_mem_override_no_max_x.json"

		// We don't have enough space to go to token upperbound for request 1, the best we can do is 2
		expectedLMRX := 2

		// 1_sys_5_kvss_linear_mem_override_no_max_x.json
		// 5 kvss pods, with coefficient set to 100, 100, 300, 300, 200 MiB, intercept at 100MiB
		memoryRatio := getLmrCoefficients(testDataFileName, commonpb.ClusterDetails_TaskInfo_KVSS)
		baseIntercept := 100 << 20

		tempDefaultActMem := defaultActMem
		defaultActMem = *resource.NewQuantity(1<<20, resource.BinarySI)

		defer func() {
			defaultActMem = tempDefaultActMem
		}()

		numSys, numKvss := 1, 5
		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-kvss-job-sharing-lmr", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// 5 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		for _, rg := range lock.Status.GroupResourceGrants {
			if strings.HasPrefix(rg.Name, rlv1.KVStorageServerRequestName) {
				assertPortUsageEvenlyDistributed(rg.ResourceGrants, 2, "", false)
			}
		}

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
		}, lock, rlv1.KVStorageServerRequestName)

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)

		// 4 act, 5 kvss, all placed with affinity
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 9, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		pod := pods.Items[0]
		gotExpectedSetLMRX := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(expectedLMRX) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)
		assertMemoryGrantedBasedOnRatio(pods, memoryRatio, baseIntercept, expectedLMRX)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)

		inferenceJob2 := newInfExecuteJob(wscommon.Namespace, "inference-kvss-job-sharing-lmr2", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob2.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		// 5 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob2, "testdata/1_sys_5_kvss_linear_mem_override_with_max_x.json")
		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock2)
		assertInferenceJobSharing(t.T(), lock2, true)

		for _, rg := range lock2.Status.GroupResourceGrants {
			if strings.HasPrefix(rg.Name, rlv1.KVStorageServerRequestName) {
				assertPortUsageEvenlyDistributed(rg.ResourceGrants, 2, "", false)
			}
		}

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
		}, lock2, rlv1.KVStorageServerRequestName)

		require.Len(t.T(), lock2.Status.GroupResourceGrantUpperBounds, 2)

		// 4 act, 5 kvss, all placed with affinity
		numOneHopTraffic, numMultiHopTraffic = wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock2)
		assert.Equal(t.T(), 9, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods2 := assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, "kvstorageserver", numKvss)

		// We have more available resources during job2, thus can go up to 4(the MaxX), but we take MaxX - 1 for conservativity
		expectedLMRX = 3
		gotExpectedSetLMRX = false
		for _, env := range pods2.Items[0].Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(expectedLMRX) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)

		assertMemoryGrantedBasedOnRatio(pods2, memoryRatio, baseIntercept, expectedLMRX)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)
	})

	t.Run("inference_scheduling_kvss_job_sharing_&_linear_memory_lowerbound_granting", func() {
		deleteNodes(t.T())
		// Each AX talks to 2 domains:
		// AX 0 => domain 0, 1
		// AX 1 => domain 2, 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 0, 2, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 2)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()
		testDataFileName := "testdata/1_sys_5_kvss_linear_mem_override_no_max_x.json"

		// We don't have enough space to go to token upperbound, the best we can do is MinX (1)
		// In this test we would like to have KVSS replicas having the base granting only, for that purpose we
		// give ACT replicas very high upperbonud that's equivalent to the max limit of AX nodes.
		// As a result, KVSS replicas don't have enough space to go to token upperbound, the best we can do is MinX (1)
		expectedLMRX := 1

		// 1_sys_5_kvss_linear_mem_override_no_max_x.json
		// 5 kvss pods, with coefficient set to 100, 100, 300, 300, 200 MiB, intercept at 100MiB
		memoryRatio := getLmrCoefficients(testDataFileName, commonpb.ClusterDetails_TaskInfo_KVSS)
		baseIntercept := 100 << 20

		numSys, numKvss := 1, 5
		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-kvss-job-sharing-lmr-lowerbound", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					// Setting a high upperbound for ACT, make sure that ACT upperbound granting do not exceed per job limit
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// 5 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		for _, rg := range lock.Status.GroupResourceGrants {
			if strings.HasPrefix(rg.Name, rlv1.KVStorageServerRequestName) {
				assertPortUsageEvenlyDistributed(rg.ResourceGrants, 2, "", false)
			}
		}

		// Total memory granted for job - 4 * act base - linear kvss base for 5 pods, then divided evenly by the 4 act pods
		expectedActUbGranting := wscommon.DefaultNodeMem * numSys
		for _, coef := range memoryRatio {
			expectedActUbGranting -= (coef*1 + baseIntercept) >> 20
		}
		expectedActUbGranting /= 4
		for _, ub := range lock.Status.GroupResourceGrantUpperBounds {
			if ub.Name == rlv1.ActivationRequestName {
				assert.Equal(t.T(), expectedActUbGranting, ub.Subresources[wsresource.MemSubresource])
			}
		}

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
		}, lock, rlv1.KVStorageServerRequestName)

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)

		// 4 act, 5 kvss, all placed with affinity
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 9, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		pod := pods.Items[0]
		gotExpectedSetLMRX := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(expectedLMRX) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)
		assertMemoryGrantedBasedOnRatio(pods, memoryRatio, baseIntercept, expectedLMRX)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)
	})

	t.Run("inference_scheduling_kvss_job_sharing_&_linear_memory_grant_intercept_only_when_min_=_max_=_0", func() {
		deleteNodes(t.T())
		// Each AX talks to 2 domains:
		// AX 0 => domain 0, 1
		// AX 1 => domain 2, 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 0, 2, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 2)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		testDataFileName := "testdata/1_sys_5_kvss_linear_mem_override_0_min_max.json"
		// MinX = MaxX = 0, all pods should get intercept only
		expectedLMRX := 0

		// 1_sys_5_kvss_linear_mem_override_0_min_max.json
		// 5 kvss pods, with coefficient set to 100, 100, 300, 300, 200 MiB, intercept at 100MiB
		memoryRatio := getLmrCoefficients(testDataFileName, commonpb.ClusterDetails_TaskInfo_KVSS)
		baseIntercept := 100 << 20

		numSys, numKvss := 1, 5
		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-kvss-job-sharing-lmr-zero-minmax", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					// Setting a high upperbound for ACT, make sure that ACT upperbound granting do not exceed per job limit
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// 5 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		for _, rg := range lock.Status.GroupResourceGrants {
			if strings.HasPrefix(rg.Name, rlv1.KVStorageServerRequestName) {
				assertPortUsageEvenlyDistributed(rg.ResourceGrants, 2, "", false)
			}
		}

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
		}, lock, rlv1.KVStorageServerRequestName)

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)

		// 4 act, 5 kvss, all placed with affinity
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 9, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		pod := pods.Items[0]
		gotExpectedSetLMRX := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(expectedLMRX) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)
		assertMemoryGrantedBasedOnRatio(pods, memoryRatio, baseIntercept, expectedLMRX)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)
	})

	t.Run("lock reconciler - sharing-enabled inference job should not consider AX nodes with non-sharing job running", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 8)

		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-act-without-job-sharing", 4, 0, 0)

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, false)

		// non-sharing job targeting 4 systems, selecting 4 AX nodes only
		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-3": true,
			"ax-5": true,
		}, lock, rlv1.ActivationRequestName)
		assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "activation", 16)

		inferenceJob2 := newInfExecuteJob(wscommon.Namespace, "inference-act-with-job-sharing", 4, 0, 0)

		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock2)
		assertInferenceJobSharing(t.T(), lock2, true)

		// sharing-enabled job should not select AX nodes that's running non-sharing jobs
		// thus selecting only 4 nodes, where in the normal situation it should select all the AX nodes
		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-2": true,
			"ax-4": true,
			"ax-6": true,
			"ax-7": true,
		}, lock2, rlv1.ActivationRequestName)
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, "", "activation", 32)
	})

	// TODO: remove once SwDriver is scheduled on IX/SX
	t.Run("inference_scheduling_job_sharing_swdriver_mutual_exclusion", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 8)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")

		// Turn on swdriver mutual exclusion feature
		// Without this feature, the 2 swdriver in this test's setup would land on the same AX
		t.updateSessionLabel(wscommon.Namespace, wscommon.SwDriverMutualExclusionSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
			t.updateSessionLabel(wscommon.Namespace, wscommon.SwDriverMutualExclusionSessionLabelKey, "false")
		}()

		getSwDriverNode := func(lock *rlv1.ResourceLock) string {
			for _, gg := range lock.Status.GroupResourceGrants {
				if strings.HasPrefix(gg.Name, rlv1.ActivationRequestName) {
					for _, g := range gg.ResourceGrants {
						if g.Name != "0" {
							continue
						}
						for _, res := range g.Resources {
							if res.Type == rlv1.NodeResourceType {
								return res.Name
							}
						}
					}
				}
			}
			return ""
		}

		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-act-swdriver-mutual-excl", 4, 0, 0)

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)

		swDriverNodeName1 := getSwDriverNode(lock)
		assert.True(t.T(), swDriverNodeName1 != "")
		assertSubResource(fmt.Sprintf("%s/%s", rlv1.NodeResourceType, swDriverNodeName1), wsresource.Act0SubResource, 1)

		inferenceJob2 := newInfExecuteJob(wscommon.Namespace, "inference-act-swdriver-mutual-excl2", 4, 0, 0)

		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock2)

		swDriverNodeName2 := getSwDriverNode(lock2)
		assert.True(t.T(), swDriverNodeName2 != "")
		assert.True(t.T(), swDriverNodeName1 != swDriverNodeName2)
		assertSubResource(fmt.Sprintf("%s/%s", rlv1.NodeResourceType, swDriverNodeName2), wsresource.Act0SubResource, 1)

		// For previous lock, swdriver landed on ax-0
		// Need to make sure that other ACT pods are still capable of using ax-0 for this job
		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-2": true,
			"ax-3": true,
			"ax-4": true,
			"ax-5": true,
			"ax-6": true,
			"ax-7": true,
		}, lock2, rlv1.ActivationRequestName)
	})

	t.Run("inference_scheduling_kvss_job_sharing_non_whole_mib_rounding", func() {
		deleteNodes(t.T())
		// Each AX talks to 2 domains:
		// AX 0 => domain 0, 1
		// AX 1 => domain 2, 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 0, 2, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 2)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		testDataFileName := "testdata/1_sys_5_kvss_linear_mem_override_byte_granularity.json"

		// 1_sys_5_kvss_linear_mem_override_byte_granularity.json
		// 5 kvss pods, with coefficient set to 6144, 4032, 10752, 16896, 16896, intercept at 10 bytes
		// there no upperbound on max_X, in this case our expected X upperbound would be 85963
		memoryRatio := getLmrCoefficients(testDataFileName, commonpb.ClusterDetails_TaskInfo_KVSS)
		baseIntercept := 10
		expectedLMRX := 85963

		tempDefaultActMem := defaultActMem
		defaultActMem = *resource.NewQuantity(1<<20, resource.BinarySI)

		defer func() {
			defaultActMem = tempDefaultActMem
		}()

		numSys, numKvss := 1, 5
		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-kvss-job-sharing-rounding", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// 5 kvss pods
		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
		}, lock, rlv1.KVStorageServerRequestName)

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)

		// 4 act, 5 kvss, all placed with affinity
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 9, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		pod := pods.Items[0]
		gotExpectedSetLMRX := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(expectedLMRX) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)
		assertMemoryGrantedBasedOnRatio(pods, memoryRatio, baseIntercept, expectedLMRX)
		// verify ACT metrics emitted
		assert.True(t.T(),
			wscommon.PodNetworkTopologyForTesting(inferenceJob.Namespace, inferenceJob.Name,
				"activation-0", "ax-0", "nic0", "leaf-0-0", "0,1,2",
				"system-0", "n00", "leaf-0-0", "", "", "", true))
		// verify KVSS metrics emitted
		assert.True(t.T(),
			wscommon.PodNetworkTopologyForTesting(inferenceJob.Namespace, inferenceJob.Name,
				"kvstorageserver-0", "ax-0", "nic0", "leaf-0-0", "0,1,2",
				"system-0", "n00", "leaf-0-0", "", "", "", true))
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)
	})

	t.Run("inference_scheduling_kvss_job_sharing_uneven_replica-system_ratio", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 8)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		testDataFileName := "testdata/6_sys_79_kvss_job_sharing.json"
		tempDefaultActMem := defaultActMem
		defaultActMem = *resource.NewQuantity(1<<20, resource.BinarySI)

		defer func() {
			defaultActMem = tempDefaultActMem
		}()

		numSys, numKvss := 6, 78
		inferenceJob := newInfExecuteJob(wscommon.Namespace, "job-uneven-replica-system-ratio", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)
		for _, rg := range lock.Status.GroupResourceGrants {
			if strings.HasPrefix(rg.Name, rlv1.KVStorageServerRequestName) {
				assertPortUsageEvenlyDistributed(rg.ResourceGrants, 2, "", false)
			}
		}

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)

		// 24 act + 79 kvss = 103 traffic counts
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 103, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)
	})

	t.Run("inference_scheduling_kvss_job_sharing_io_usage_balance_across_ports", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 8)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		testDataFileName := "testdata/2_sys_25_kvss_job_sharing_io_usage_balance.json"
		tempDefaultActMem := defaultActMem
		defaultActMem = *resource.NewQuantity(1<<20, resource.BinarySI)

		defer func() {
			defaultActMem = tempDefaultActMem
		}()

		numSys, numKvss := 2, 25
		inferenceJob := newInfExecuteJob(wscommon.Namespace, "job-io-usage-balance", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		for _, rg := range lock.Status.GroupResourceGrants {
			if strings.HasPrefix(rg.Name, rlv1.KVStorageServerRequestName) {
				// The allowedMaxDifference for each system/domain here is 2:
				// sys0/domain0: [5, 4(3 + 1), 3(2 + 1)] - diff: 2
				// sys0/domain1: [7, 7(6 + 1), 5(4 + 1)] - diff: 2
				// sys0/domain2: [3, 2, 2(1 + 1)] - diff: 1
				// sys0/domain3: [7(6 + 1), 6, 6] - diff: 1
				// sys1/domain0: [0, 0, 0] - diff: 0
				// sys1/domain1: [0, 0, 0] - diff: 0
				// sys1/domain2: [101(89 + 11), 101(34 + 67), 99(56 + 43)] - diff: 2
				// sys1/domain3: [2, 0, 0] - diff: 2
				assertPortUsageEvenlyDistributed(rg.ResourceGrants, 2, "", false)
			}
		}

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)

		// 8 act + 25 kvss = 103 traffic counts
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 33, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)
	})

	portLBComparisonTestCases := []struct {
		name                       string
		expectedPortLoadDifference int
		disableWioBandwidthLB      bool
	}{
		// For all thest cases, usage is [1, 2, 1, 3, 3], either passed in by override or specified in LMR
		// Expected placement: [4 (1 + 3), 5 (2 + 3), 1], load diff = 4
		{"round-robin", 4, true},
		// Expected placement: [4(3 + 1), 3, 3 (2 + 1)], load diff = 1
		{"greedy-normalized-lmr", 1, false},
	}

	for _, tc := range portLBComparisonTestCases {
		t.Run("inference_scheduling_load_balancing_across_ports_"+tc.name, func() {
			deleteNodes(t.T())
			v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
				SplitNStamps(1, 4)
			t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
			t.NoError(t.CfgMgr.Initialize(v2Cfg))
			t.assignEverythingToNS(wscommon.Namespace, 8)

			// Turn on session level job sharing
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
			defer func() {
				t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
			}()

			if tc.disableWioBandwidthLB {
				wscommon.DisableWioBandwidthLB = true
			}
			defer func() {
				wscommon.DisableWioBandwidthLB = false
			}()

			testDataFileName := "testdata/1_sys_5_kvss_linear_mem_override.json"
			tempDefaultActMem := defaultActMem
			defaultActMem = *resource.NewQuantity(1<<20, resource.BinarySI)

			defer func() {
				defaultActMem = tempDefaultActMem
			}()

			numSys, numKvss := 1, 5
			inferenceJob := newInfExecuteJob(wscommon.Namespace, "job-io-usage-balance-"+tc.name, uint32(numSys), uint32(numKvss), 0)
			for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
				if rt == wsapisv1.WSReplicaTypeActivation {
					spec.Template.Annotations = map[string]string{
						wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(int(defaultActMem.Value())),
						// Setting a high max limit, this should be ignored when job sharing is enabled
						wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
					}
				} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
					spec.Template.Annotations = map[string]string{
						// Setting a low max limit, this should be ignored when job sharing is enabled
						wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
					}
				}
			}

			inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
			require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
			defer assertWsjobCleanedUp(t.T(), inferenceJob)

			createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
			lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
			assertLocked(t.T(), lock)
			getLmrCoefficients(testDataFileName, commonpb.ClusterDetails_TaskInfo_KVSS)

			for _, rg := range lock.Status.GroupResourceGrants {
				if strings.HasPrefix(rg.Name, rlv1.KVStorageServerRequestName) {
					usageDiff := assertPortUsageEvenlyDistributed(rg.ResourceGrants, tc.expectedPortLoadDifference, testDataFileName, tc.disableWioBandwidthLB)
					assert.Equal(t.T(), tc.expectedPortLoadDifference, usageDiff)
				}
			}
		})
	}

	t.Run("inference_scheduling_kvss_act_linear_memory_range", func() {
		deleteNodes(t.T())
		// Each AX talks to 2 domains:
		// AX 0 => domain 0, 1
		// AX 1 => domain 2, 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(8, 1, 0, 8, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 5)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		useConfigMapForAct = true
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
			useConfigMapForAct = false
		}()

		testDataFileName := "testdata/5_sys_5_kvss_5_act.json"

		tempDefaultActMem := defaultActMem
		defaultActMem = *resource.NewQuantity(1<<20, resource.BinarySI)

		defer func() {
			defaultActMem = tempDefaultActMem
		}()

		numSys, numKvss := 5, 5
		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-scheduling-kvss-act-lmr", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-2": true,
			"ax-3": true,
			"ax-4": true,
		}, lock, rlv1.KVStorageServerRequestName)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-1": true,
			"ax-2": true,
			"ax-3": true,
			"ax-4": true,
		}, lock, rlv1.ActivationRequestName)

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)

		for _, rg := range lock.Status.GroupResourceGrants {
			if strings.HasPrefix(rg.Name, rlv1.ActivationRequestName) {
				// If considerGlobalPortAllocation is set to false, multiple pods will land on port 0, this assertion will fail
				// else pods will land on different ports in the domain, which is the expected behaviour for ACT
				assertGlobalPortUsageEvenlyDistributed(rg.ResourceGrants, 0)
			}
		}

		// 5 act, 5 kvss, all placed with affinity
		numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(t.CfgMgr.Cfg, lock)
		assert.Equal(t.T(), 10, numOneHopTraffic)
		assert.Equal(t.T(), 0, numMultiHopTraffic)

		// validate kvss LMR
		memoryRatio := getLmrCoefficients(testDataFileName, commonpb.ClusterDetails_TaskInfo_KVSS)
		baseIntercept := 10
		expectedLMRX := 1
		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "kvstorageserver", numKvss)
		pod := pods.Items[0]
		gotExpectedSetLMRX := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(expectedLMRX) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)
		assertMemoryGrantedBasedOnRatio(pods, memoryRatio, baseIntercept, expectedLMRX)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)

		// validate act LMR
		memoryRatio = getLmrCoefficients(testDataFileName, commonpb.ClusterDetails_TaskInfo_ACT)
		baseIntercept = 104857600
		expectedLMRX = 1
		pods = assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "activation", numKvss)
		pod = pods.Items[0]
		gotExpectedSetLMRX = false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(expectedLMRX) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)
		assertMemoryGrantedBasedOnRatio(pods, memoryRatio, baseIntercept, expectedLMRX)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)
	})

	t.Run("inference_scheduling_job_sharing_fair_share", func() {
		deleteNodes(t.T())
		// Each AX talks to 2 domains:
		// AX 0, 2 => domain 0, 1
		// AX 1 => domain 2, 3
		v2Cfg := wscommon.NewTestClusterConfigSchema(3, 1, 0, 3, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 3)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		useConfigMapForAct = true
		defer func() {
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
			useConfigMapForAct = false
		}()

		testDataFileName := "testdata/2_sys_2_kvss_2_act_uneven_dist.json"

		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-scheduling-job-sharing-fair-share", 2, 2, 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-1": true,
		}, lock, rlv1.KVStorageServerRequestName)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-1": true,
		}, lock, rlv1.ActivationRequestName)
		kvssPods := assertPodCountEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name, "kvstorageserver", 2)
		actPods := assertPodCountEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name, "activation", 2)
		totalMem := getTotalMemoryLimitForPods(kvssPods) + getTotalMemoryLimitForPods(actPods)

		// Uneven distribution, act/kvss all targeting domain2 resulting in all pods on ax-1.
		// Although this job requested 2 systems, it should using less than DefaultNodeMem of a single node.
		assert.True(t.T(), totalMem < wscommon.DefaultNodeMem, "total memory allocated should be less than fairshare amount")

		testDataFileName2 := "testdata/1_sys_2_kvss_1_act.json"

		inferenceJob2 := newInfExecuteJob(wscommon.Namespace, "inference-scheduling-job-sharing-fair-share2", 1, 2, 0)
		for rt, spec := range inferenceJob2.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			} else if rt == wsapisv1.WSReplicaTypeKVStorageServer {
				spec.Template.Annotations = map[string]string{
					// Setting a low max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(1 << 20),
				}
			}
		}

		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		createActKvssConfigMap(t.T(), inferenceJob2, testDataFileName2)
		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock2)
		assertInferenceJobSharing(t.T(), lock2, true)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
			"ax-2": true,
		}, lock2, rlv1.KVStorageServerRequestName)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
		}, lock2, rlv1.ActivationRequestName)
		kvssPods2 := assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, "kvstorageserver", 2)
		actPods2 := assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, "activation", 1)
		totalMem2 := getTotalMemoryLimitForPods(kvssPods2) + getTotalMemoryLimitForPods(actPods2)

		// Previous job using less than DefaultNodeMem of 1 node, resulting in excess memory for this job.
		// But due to fairshare, it shouldn't use more than DefaultNodeMem of 1 node as it requests for only 1 system.
		assert.True(t.T(), totalMem2 <= wscommon.DefaultNodeMem, "total memory allocated should be less than fairshare amount")
	})

	t.Run("inference_scheduling_act_only_linear_memory_range", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(1, 1, 0, 1, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		t.assignEverythingToNS(wscommon.Namespace, 1)

		// Turn on session level job sharing
		t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
		useConfigMapForAct = true
		defer func() {
			useConfigMapForAct = false
			t.updateSessionLabel(wscommon.Namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		testDataFileName := "testdata/1_sys_1_act_lmr.json"

		tempDefaultActMem := defaultActMem
		defaultActMem = *resource.NewQuantity(1<<20, resource.BinarySI)

		defer func() {
			defaultActMem = tempDefaultActMem
		}()

		numSys, numKvss := 1, 0
		inferenceJob := newInfExecuteJob(wscommon.Namespace, "inference-scheduling-act-only-lmr", uint32(numSys), uint32(numKvss), 0)
		for rt, spec := range inferenceJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Annotations = map[string]string{
					// Setting a high max limit, this should be ignored when job sharing is enabled
					wsapisv1.PodMemoryMaxLimitKey: strconv.Itoa(wscommon.DefaultNodeMem * 100_000 << 20),
				}
			}
		}

		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		createActKvssConfigMap(t.T(), inferenceJob, testDataFileName)
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)
		assertInferenceJobSharing(t.T(), lock, true)

		assertAxNodesScheduledAndBalanced(map[string]bool{
			"ax-0": true,
		}, lock, rlv1.ActivationRequestName)

		// validate act LMR
		memoryRatio := getLmrCoefficients(testDataFileName, commonpb.ClusterDetails_TaskInfo_ACT)
		baseIntercept := 104857600
		expectedLMRX := 86
		pods := assertPodCountEventually(t.T(), inferenceJob.Namespace, "", "activation", 1)
		pod := pods.Items[0]
		gotExpectedSetLMRX := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeLinearMemRangeXEnvVarName && env.Value == strconv.Itoa(expectedLMRX) {
				gotExpectedSetLMRX = true
			}
		}
		assert.True(t.T(), gotExpectedSetLMRX)
		assertMemoryGrantedBasedOnRatio(pods, memoryRatio, baseIntercept, expectedLMRX)
		assertEnvVarSetForAllPods(pods, wsapisv1.RuntimeCsPortEnvVarName)
	})
}

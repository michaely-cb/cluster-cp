//go:build integration && nvmf_scheduling

package ws

import (
	"context"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
)

func (t *TestSuite) TestNvmfJobScheduling() {
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	t.createNsIfNotExists(wscommon.DefaultNamespace)
	t.assignEverythingToNS(wscommon.DefaultNamespace, systemCount)

	assertAggrAllocatedDiskRanges := func(subresourceRanges wsresource.SubresourceRanges) {
		res, _ := t.ResourceController.GetResource("node/0-coord-0")
		assert.True(t.T(), res.GetAllocatedSubresourceRanges().Equals(subresourceRanges))
	}

	t.Run("nvmf_disk_space_granting", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 1, 12, 4, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

		// Configure coordinator and some nodegroups to be nvmf enabled
		var coordNodes []*wscommon.Node
		nvmfEnabledNodegroups := []string{"group0", "group2"}
		for _, n := range v2Cfg.Nodes {
			if n.Role == wscommon.RoleCoordinator {
				coordNodes = append(coordNodes, n)
				n.Properties[wsresource.NvmfDiskBytesPropertyKey] = "150"
			} else if n.Role == wscommon.RoleMemory && slices.Contains(nvmfEnabledNodegroups, n.GetGroup()) {
				n.Properties[wsresource.NvmfInitiatorPropKey] = ""
			}
		}
		slices.SortFunc(coordNodes, func(a, b *wscommon.Node) bool {
			return a.Name < b.Name
		})
		for i, n := range coordNodes {
			n.Properties[wsresource.NvmfTargetIdPropKey] = strconv.Itoa(i + 1)
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

		job0 := newWsExecuteJob(wscommon.Namespace, "nvmf-enabled-job-0", 1)
		job0.Annotations[wsapisv1.SchedulerHintAllowNG] = "group0,group2"
		job0.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] = "36"
		job0.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] = "72"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		lock := assertLockExists(t.T(), job0.Namespace, job0.Name)
		assertLocked(t.T(), lock)
		expectedUsableRanges := "/dev/datanvme101:0-72,/dev/datanvme102:0-72,/dev/datanvme103:0-72,/dev/datanvme101:72-144"
		for _, rg := range lock.Status.ResourceGrants {
			if rg.Name != rlv1.CoordinatorRequestName {
				continue
			}
			res := rg.Resources[0]
			assert.Equal(t.T(), rlv1.Res{
				Type: rlv1.NodeResourceType,
				Name: "0-coord-0",
				Subresources: map[string]int{
					wsresource.CPUSubresource: wscommon.DefaultNodeMem / 16,
					wsresource.MemSubresource: wscommon.DefaultNodeMem / 16,
					wsresource.PodSubresource: 1,
				},
				SubresourceRanges: map[string][]rlv1.Range{
					wsresource.NvmfDiskResName(1): {{Start: 0, End: 144}},
					wsresource.NvmfDiskResName(2): {{Start: 0, End: 72}},
					wsresource.NvmfDiskResName(3): {{Start: 0, End: 72}},
				},
			}, res)
			assert.Equal(t.T(), map[string]string{
				rlv1.NvmfTargetIdAnnotKey:                       "1",
				wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "36",
				wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "72",
				wsapisv1.NvmfUsableRangesEnvVarName:             expectedUsableRanges,
				wscommon.NamespaceKey:                           wscommon.Namespace,
			}, rg.Annotations)
		}
		assertAggrAllocatedDiskRanges(wsresource.SubresourceRanges{
			wsresource.NvmfDiskResName(1): {{Start: 0, End: 144}},
			wsresource.NvmfDiskResName(2): {{Start: 0, End: 72}},
			wsresource.NvmfDiskResName(3): {{Start: 0, End: 72}},
		})

		// assert pod env vars
		coords := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, rlv1.CoordinatorRequestName, 1)
		weights := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, rlv1.WeightRequestName, defaultWgtPerJob)
		for _, pod := range append(coords.Items, weights.Items...) {
			nvmfEnvVarFound := false
			for _, v := range pod.Spec.Containers[0].Env {
				if v.Name != wsapisv1.NvmfUsableRangesEnvVarName {
					continue
				}
				nvmfEnvVarFound = true
				if pod.Labels["replica-type"] == strings.ToLower(rlv1.WeightRequestName) {
					ranges := strings.Split(expectedUsableRanges, ",")
					podIdx, _ := strconv.Atoi(pod.Labels["replica-index"])
					assert.Equal(t.T(), ranges[podIdx], v.Value)
				} else {
					assert.Equal(t.T(), expectedUsableRanges, v.Value)
				}
				break
			}
			assert.True(t.T(), nvmfEnvVarFound)

			blockDeviceVolumesFound := 0
			for _, v := range pod.Spec.Volumes {
				if strings.HasPrefix(v.Name, wsapisv1.NvmfVolumePrefix) {
					blockDeviceVolumesFound += 1
				}
			}
			if pod.Labels["replica-type"] == strings.ToLower(rlv1.WeightRequestName) {
				assert.Equal(t.T(), 1, blockDeviceVolumesFound)
			} else {
				assert.Equal(t.T(), wsresource.NvmfDiskCountPerCoordNode, blockDeviceVolumesFound)
			}

			blockDeviceMountsFound := 0
			for _, v := range pod.Spec.Containers[0].VolumeMounts {
				if strings.HasPrefix(v.Name, wsapisv1.NvmfVolumePrefix) {
					blockDeviceMountsFound += 1
				}
			}
			if pod.Labels["replica-type"] == strings.ToLower(rlv1.WeightRequestName) {
				assert.Equal(t.T(), 1, blockDeviceMountsFound)
			} else {
				assert.Equal(t.T(), wsresource.NvmfDiskCountPerCoordNode, blockDeviceMountsFound)
			}
		}

		job1 := newWsExecuteJob(wscommon.Namespace, "nvmf-enabled-job-1", 1)
		job1.Annotations[wsapisv1.SchedulerHintAllowNG] = "group0,group2"
		job1.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] = "12"
		job1.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] = "24"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		expectedUsableRanges = "/dev/datanvme102:72-96,/dev/datanvme103:72-96,/dev/datanvme102:96-120,/dev/datanvme103:96-120"
		lock = assertLockExists(t.T(), job1.Namespace, job1.Name)
		assertLocked(t.T(), lock)
		for _, rg := range lock.Status.ResourceGrants {
			if rg.Name != rlv1.CoordinatorRequestName {
				continue
			}
			res := rg.Resources[0]
			assert.Equal(t.T(), rlv1.Res{
				Type: rlv1.NodeResourceType,
				Name: "0-coord-0",
				Subresources: map[string]int{
					wsresource.CPUSubresource: wscommon.DefaultNodeMem / 16,
					wsresource.MemSubresource: wscommon.DefaultNodeMem / 16,
					wsresource.PodSubresource: 1,
				},
				SubresourceRanges: map[string][]rlv1.Range{
					// 2x grant
					wsresource.NvmfDiskResName(2): {{Start: 72, End: 120}},
					wsresource.NvmfDiskResName(3): {{Start: 72, End: 120}},
				},
			}, res)
			assert.Equal(t.T(), map[string]string{
				rlv1.NvmfTargetIdAnnotKey:                       "1",
				wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "12",
				wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "24",
				wsapisv1.NvmfUsableRangesEnvVarName:             expectedUsableRanges,
				wscommon.NamespaceKey:                           wscommon.Namespace,
			}, rg.Annotations)
		}

		events := &corev1.EventList{}
		require.NoError(t.T(), k8sReader.List(context.Background(), events,
			client.MatchingFieldsSelector{
				Selector: fields.AndSelectors(
					fields.ParseSelectorOrDie("involvedObject.kind=WSJob"),
					fields.OneTermEqualSelector("involvedObject.name", job1.Name),
					fields.OneTermEqualSelector("type", "Warning"),
				),
			}))
		for _, e := range events.Items {
			t.T().Log(e.Message)
		}
		require.NotEmpty(t.T(), events.Items)
		assert.Contains(t.T(), events.Items[0].Message, "2 out of 3 NVMe disks were used")
		job1 = assertWsJobScheduledEventually(t.T(), job1.Namespace, job1.Name)
		assert.Equal(t.T(), expectedUsableRanges, job1.Annotations[wscommon.NvmfGrants])
		assert.Equal(t.T(), "2 grant(s) on /dev/datanvme102, 2 grant(s) on /dev/datanvme103", job1.Annotations[wscommon.NvmfGrantNotes])

		assertAggrAllocatedDiskRanges(wsresource.SubresourceRanges{
			wsresource.NvmfDiskResName(1): {{Start: 0, End: 144}},
			wsresource.NvmfDiskResName(2): {{Start: 0, End: 120}},
			wsresource.NvmfDiskResName(3): {{Start: 0, End: 120}},
		})

		assertWsjobCleanedUp(t.T(), job0)
		assertAggrAllocatedDiskRanges(wsresource.SubresourceRanges{
			wsresource.NvmfDiskResName(2): {{Start: 72, End: 120}},
			wsresource.NvmfDiskResName(3): {{Start: 72, End: 120}},
		})

		job2 := newWsExecuteJob(wscommon.Namespace, "nvmf-disabled-job-0", 1)
		job2.Annotations[wsapisv1.SchedulerHintAllowNG] = "group0,group2"
		// not granted - no sufficient nvmf disk space available
		job2.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] = "100"
		job2.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] = "200"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		expectedUsableRanges = ""
		lock = assertLockExists(t.T(), job2.Namespace, job2.Name)
		assertLocked(t.T(), lock)
		for _, rg := range lock.Status.ResourceGrants {
			if rg.Name != rlv1.CoordinatorRequestName {
				continue
			}
			res := rg.Resources[0]
			assert.Equal(t.T(), rlv1.Res{
				Type: rlv1.NodeResourceType,
				Name: "0-coord-0",
				Subresources: map[string]int{
					wsresource.CPUSubresource: wscommon.DefaultNodeMem / 16,
					wsresource.MemSubresource: wscommon.DefaultNodeMem / 16,
					wsresource.PodSubresource: 1,
				},
				// no subresource ranges grant given impossible
			}, res)
			assert.Equal(t.T(), map[string]string{
				wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "100",
				wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "200",
				wscommon.NamespaceKey:                           wscommon.Namespace,
			}, rg.Annotations)
		}
		job2 = assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)
		assert.Equal(t.T(), expectedUsableRanges, job2.Annotations[wscommon.NvmfGrants])

		assertWsjobCleanedUp(t.T(), job2)
		job3 := newWsExecuteJob(wscommon.Namespace, "nvmf-enabled-job-2", 1)
		job3.Annotations[wsapisv1.SchedulerHintAllowNG] = "group0,group2"
		job3.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] = "5"
		job3.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] = "10"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))
		assertWsJobScheduledEventually(t.T(), job3.Namespace, job3.Name)
		// no intention to reduce fragmentation
		assertAggrAllocatedDiskRanges(wsresource.SubresourceRanges{
			wsresource.NvmfDiskResName(1): {{Start: 0, End: 20}},
			wsresource.NvmfDiskResName(2): {{Start: 0, End: 10}, {Start: 72, End: 120}},
			wsresource.NvmfDiskResName(3): {{Start: 0, End: 10}, {Start: 72, End: 120}},
		})

		assertWsjobCleanedUp(t.T(), job1)
		job4 := newWsExecuteJob(wscommon.Namespace, "nvmf-enabled-job-3", 1)
		job4.Annotations[wsapisv1.SchedulerHintAllowNG] = "group0,group2"
		job4.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] = "20"
		job4.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] = "40"
		require.NoError(t.T(), k8sClient.Create(context.Background(), job4))
		assertWsJobScheduledEventually(t.T(), job4.Namespace, job4.Name)
		// ranges grow after allocated ranges
		assertAggrAllocatedDiskRanges(wsresource.SubresourceRanges{
			wsresource.NvmfDiskResName(1): {{Start: 0, End: 60}},
			wsresource.NvmfDiskResName(2): {{Start: 0, End: 90}},
			wsresource.NvmfDiskResName(3): {{Start: 0, End: 50}},
		})

		// mark mgmt node down and still expect resource deallocation to happen properly
		require.NoError(t.T(), patchNode(t, "0-coord-0", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))

		assertWsjobCleanedUp(t.T(), job3)
		assertWsjobCleanedUp(t.T(), job4)
		assertAggrAllocatedDiskRanges(nil)

		// recover mgmt node
		require.NoError(t.T(), patchNode(t, "0-coord-0", nil))
	})
}

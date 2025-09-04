//go:build integration && resource_controller

package resource

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource/helpers"
	"cerebras.com/job-operator/controllers/cluster"
	"cerebras.com/job-operator/controllers/resource/lock"
	"cerebras.com/job-operator/controllers/resource/namespace"
)

var k8sClient client.Client

const (
	eventuallyTimeout = 10 * time.Second
	eventuallyTick    = 1 * time.Second

	systemCount = 8
	brNum       = 30
)

var assertGranted = func(t *testing.T, l *rlv1.ResourceLock) {
	require.Eventually(t, func() bool {
		var grantedLock = &rlv1.ResourceLock{}
		require.NoError(t, k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(l), grantedLock))
		return grantedLock.Status.State == rlv1.LockGranted
	}, eventuallyTimeout, eventuallyTick)
}

var assertPending = func(t *testing.T, lock *rlv1.ResourceLock) {
	require.Eventually(t, func() bool {
		var l = &rlv1.ResourceLock{}
		err := k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(lock), l)
		if err != nil { // assume not created yet
			return false
		}
		return rlv1.LockPending == l.Status.State
	}, eventuallyTimeout, eventuallyTick)
}

var assertFailed = func(t *testing.T, lock *rlv1.ResourceLock) {
	require.Eventually(t, func() bool {
		var l = &rlv1.ResourceLock{}
		err := k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(lock), l)
		if err != nil { // assume not created yet
			return false
		}
		return rlv1.LockError == l.Status.State
	}, eventuallyTimeout, eventuallyTick)
}

func TestReconcileLocks(t *testing.T) {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	collection := helpers.NewDefaultClusterBuilder(systemCount, brNum).
		WithNvmfCoordinatorNodesEnabled(1, 200).
		WithNvmfEnabledPopulatedNodegroups(1, wsapisv1.PopNgTypeRegular).
		Build()
	controller := newController(collection)
	err := controller.SetupWithManager(mgr)
	require.NoError(t, err)

	t.Run("release resources after delete", func(t *testing.T) {
		lock0 := lock.NewLockBuilder("test-lock-0").
			WithSystemRequest(1).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, lock0))

		assertGranted(t, lock0)
		require.NoError(t, k8sClient.Delete(ctx, lock0))

		// re-create should re-grant
		lock1 := lock.NewLockBuilder("test-lock-1").WithSystemRequest(1).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, lock1))
		assertGranted(t, lock1)
		require.NoError(t, k8sClient.Delete(ctx, lock1))
	})

	t.Run("fifo order of grants", func(t *testing.T) {
		var createTime = time.Now()
		incrementCreateTime := func() {
			createTime = createTime.Add(2 * time.Second)
		}

		csLock0 := lock.NewLockBuilder("lock-0").WithSystemRequest(systemCount / 2).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock0))

		incrementCreateTime()
		csLock1 := lock.NewLockBuilder("lock-1").WithSystemRequest(systemCount).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock1))

		incrementCreateTime()
		csLock2 := lock.NewLockBuilder("lock-2").WithSystemRequest(systemCount / 2).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock2))

		assertGranted(t, csLock0)
		assertPending(t, csLock1)
		assertPending(t, csLock2)

		// continue test with first type of lock
		require.NoError(t, k8sClient.Delete(ctx, csLock0))
		assertGranted(t, csLock1)
		assertPending(t, csLock2)

		require.NoError(t, k8sClient.Delete(ctx, csLock1))
		assertGranted(t, csLock2)

		require.NoError(t, k8sClient.DeleteAllOf(ctx, &rlv1.ResourceLock{}, client.InNamespace(wscommon.Namespace)))
		require.Eventually(t, func() bool {
			list := &rlv1.ResourceLockList{}
			return k8sClient.List(ctx, list) == nil && len(list.Items) == 0
		}, eventuallyTimeout, eventuallyTick)
	})

	t.Run("lock reservations", func(t *testing.T) {
		compile0 := lock.NewCompileLockBuilder("c0", 1, 1).
			WithCreateTime(1).
			WithQueue(rlv1.ReservationQueueName).
			WithReservationStatus(commonv1.ResourceReservedValue).
			SetIsSubsidiary().
			WithWorkflowId("reservation").
			BuildPending()
		require.NoError(t, k8sClient.Create(ctx, compile0))

		compile1 := lock.NewCompileLockBuilder("c1", 1, 2).
			WithCreateTime(2).
			BuildPending()
		require.NoError(t, k8sClient.Create(ctx, compile1))

		compile2 := lock.NewCompileLockBuilder("c2", 1, 2).
			WithCreateTime(3).
			WithWorkflowId("reservation").
			SetSubsidiaryLock("c0").
			BuildPending()
		require.NoError(t, k8sClient.Create(ctx, compile2))

		assertPending(t, compile0)
		// compile1 granted validates compile0 is not reserving
		assertGranted(t, compile1)
		// compile2 pending validates compile2+compile0 is scheduled together
		assertPending(t, compile2)
		require.NoError(t, k8sClient.Delete(ctx, compile0))
		require.NoError(t, k8sClient.Delete(ctx, compile1))
		require.NoError(t, k8sClient.Delete(ctx, compile2))

		csLock0 := lock.NewLockBuilder("lock-0").WithSystemRequest(systemCount/2 + 1).
			WithWorkflowId("reservation").
			WithCreateTime(4).
			WithQueue(rlv1.ReservationQueueName).
			WithReservationStatus(commonv1.ResourceReservedValue).
			BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock0))

		csLock1 := lock.NewLockBuilder("lock-1").WithSystemRequest(systemCount / 2).
			WithCreateTime(5).
			BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock1))

		csLock2 := lock.NewLockBuilder("lock-2").WithSystemRequest(systemCount / 2).
			WithWorkflowId("reservation").
			WithCreateTime(6).
			WithParentLock("lock-0").
			BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock2))

		// reservation successfully and cleared after job inherited
		assertGranted(t, csLock2)
		assertGranted(t, csLock1)
		assertPending(t, csLock0)

		// reserving lock never grant
		require.NoError(t, k8sClient.Delete(ctx, csLock2))
		require.NoError(t, k8sClient.Delete(ctx, csLock1))
		time.Sleep(time.Second)
		assertPending(t, csLock0)
		require.NoError(t, k8sClient.Delete(ctx, csLock0))
	})

	t.Run("resource error triggered reload will not cause scheduling behavior change ", func(t *testing.T) {
		csLock0 := lock.NewLockBuilder("lock-0").WithSystemRequest(systemCount).
			WithCoordinatorRequest(1, 1, map[string]string{wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey: "10"}).
			WithPrimaryNodeGroupWithNumWeights(4, "group:nodegroup8").BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock0))
		assertGranted(t, csLock0)
		// trigger reload
		res := collection.Copy()
		res.Remove("system/system-0")
		controller.NewCfg = &wscommon.ClusterConfig{
			Resources: res,
		}

		// should fail since 1 system is down
		csLockFail := lock.NewLockBuilder("lock-fail").WithSystemRequest(systemCount).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLockFail))
		assertFailed(t, csLockFail)
		assert.Nil(t, controller.NewCfg)

		// should be pending as all resources are reserved
		csLock1 := lock.NewLockBuilder("lock-1").WithSystemRequest(systemCount - 1).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock1))
		assertPending(t, csLock1)
		assert.Nil(t, controller.NewCfg)

		// trigger reload
		controller.NewCfg = &wscommon.ClusterConfig{
			Resources: collection,
		}
		// should release+grant successfully
		require.NoError(t, k8sClient.Delete(ctx, csLock0))
		assertGranted(t, csLock1)

		require.NoError(t, k8sClient.DeleteAllOf(ctx, &rlv1.ResourceLock{}, client.InNamespace(wscommon.Namespace)))
		require.Eventually(t, func() bool {
			list := &rlv1.ResourceLockList{}
			return k8sClient.List(ctx, list) == nil && len(list.Items) == 0
		}, eventuallyTimeout, eventuallyTick)
	})

	t.Run("system recovery should trigger lock grant", func(t *testing.T) {
		res := collection.Copy()
		res.Remove("system/system-0")

		controller.NewCfg = &wscommon.ClusterConfig{
			Resources: res,
		}
		cfg := cfgFromCollection(collection)
		require.NoError(t, createClusterCfgCM(mgr.GetClient(), &cfg))

		csLock0 := lock.NewLockBuilder("lock-0").WithSystemRequest(1).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock0))
		assertGranted(t, csLock0)

		csLock1 := lock.NewLockBuilder("lock-1").WithSystemRequest(systemCount - 1).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock1))
		assertPending(t, csLock1)

		// trigger reload
		controller.NewCfg = &wscommon.ClusterConfig{Resources: collection.Copy()}
		controller.internalReconcileEventCh <- triggerReinitialize

		assertGranted(t, csLock1)
		require.NoError(t, k8sClient.DeleteAllOf(ctx, &rlv1.ResourceLock{}, client.InNamespace(wscommon.Namespace)))
		require.Eventually(t, func() bool {
			list := &rlv1.ResourceLockList{}
			return k8sClient.List(ctx, list) == nil && len(list.Items) == 0
		}, eventuallyTimeout, eventuallyTick)
	})

	t.Run("invalid request errors on reconcile", func(t *testing.T) {
		for _, testcase := range []struct {
			lock          *rlv1.ResourceLock
			partialErrMsg string
		}{
			{
				lock:          lock.NewLockBuilder("test-too-many-systems").WithSystemRequest(systemCount + 1).BuildPending(),
				partialErrMsg: fmt.Sprintf("requested %d system but only %d available", systemCount+1, systemCount),
			},
			{
				lock: lock.NewLockBuilder("test-too-little-mem").
					WithSystemRequest(systemCount).
					WithCoordinatorRequest(1, helpers.DefaultMgtNodeMem+1, nil).BuildPending(),
				partialErrMsg: fmt.Sprintf("mem:%dMi", helpers.DefaultMgtNodeMem+1),
			},
			{
				lock: lock.NewLockBuilder("test-too-many-memx-nodes").WithSystemRequest(1).
					WithGroupRequest("ng-primary-0", 1).
					WithNodeRequest("weight", helpers.DefaultMemNodePerSys+1).Role("memory").
					CpuMem(helpers.DefaultMemNodeCpu, helpers.DefaultMemNodeMem).BuildGroupRequest().
					BuildPending(),
				partialErrMsg: "group request ng-primary-0: 0 of 9 groups scheduled but required 1 " +
					"(largest candidate group failed with 10 nodes[role:memory]{cpu:8m, mem:32Mi} " +
					"could only schedule 10 of 11 weight{cpu:8m, mem:32Mi})",
			},
			{
				lock: lock.NewLockBuilder("test-group-upper-bounds-violation").WithSystemRequest(1).
					WithGroupRequest("ng-0", 1).WithNodeRequest("node", 2).
					CpuMem(helpers.DefaultMemNodeCpu/2, helpers.DefaultMemNodeMem/2).BuildGroupRequest().
					WithUpperBoundsRequest("node", helpers.DefaultMemNodeMem/4).
					BuildPending(),
				partialErrMsg: "invalid groupedRequestUpperBounds: node subresource 'memory' upper bound",
			},
		} {
			t.Run(testcase.lock.Name, func(t *testing.T) {
				controller.NewCfg = &wscommon.ClusterConfig{
					Resources: collection.Copy(),
				}
				require.NoError(t, k8sClient.Create(ctx, testcase.lock))

				var l = &rlv1.ResourceLock{}
				require.Eventually(t, func() bool {
					err := k8sClient.Get(ctx, client.ObjectKeyFromObject(testcase.lock), l)
					return err == nil && l.Status.State == rlv1.LockError
				}, eventuallyTimeout, eventuallyTick)
				assert.Contains(t, l.Status.Message, testcase.partialErrMsg)

				require.NoError(t, k8sClient.Delete(ctx, testcase.lock))
			})
		}
	})

	t.Run("invalid request errors on create", func(t *testing.T) {
		lockUnknownType := lock.NewLockBuilder("test-invalid-resource-type").WithSystemRequest(1).BuildPending()
		lockUnknownType.Spec.ResourceRequests[0].Type = "invalid"
		assert.Error(t, k8sClient.Create(ctx, lockUnknownType))
	})

	t.Run("failure handling partial system/br ports", func(t *testing.T) {
		wsapisv1.EnablePartialSystemMode = true
		defer func() {
			wsapisv1.EnablePartialSystemMode = false
		}()
		validate := func(t *testing.T, gg []rlv1.GroupResourceGrant, skipGroup int) {
			brGranted := false
			for _, group := range gg {
				if group.Name == rlv1.BrRequestName {
					brGranted = true
					allocatedNodes := map[string]bool{}
					require.Equal(t, 24, len(group.ResourceGrants))
					t.Log(group.ResourceGrants)
					for i := 0; i < 24; i++ {
						allocatedNodes[group.ResourceGrants[i].Resources[0].Name] = true
						// expect skip group match
						brId, _ := strconv.Atoi(group.ResourceGrants[i].Name)
						require.NotEqual(t, common.CSVersionSpec.Group(brId), skipGroup)
						if !wsapisv1.IsV2Network {
							brNodeId, _ := strconv.Atoi(strings.Split(
								group.ResourceGrants[i].Resources[0].Name, "-")[1])
							brPort := brId % 12
							if i < 16 {
								// expect assigned br node has port match for bottom layer
								require.Equal(t, brPort, brNodeId%12)
							} else {
								// top layer is 1:2 mode packed
								// e.g. node-24 connects to 2 ports: 3 port-0 + 3 port-1
								brNodePortsLow := (brNodeId % 12) * 2
								brNodePortsHigh := brNodePortsLow + 1
								require.True(t, brPort >= brNodePortsLow && brId%12 <= brNodePortsHigh)
							}
						}
					}
					if !wsapisv1.IsV2Network {
						// 16 + 6 nodes on top due to 1:2 mode
						require.Equal(t, 22, len(allocatedNodes))
					} else {
						// less nodes depending on affinity
						t.Log(len(allocatedNodes))
						require.True(t, len(allocatedNodes) < 22)
					}
				}
			}
			require.True(t, brGranted)
		}
		defer func() { wsapisv1.IsV2Network = false }()
		skipGroup := 1
		for i := 1; i <= 2; i++ {
			if i == 2 {
				wsapisv1.IsV2Network = true
				skipGroup = 0
			}
			postName := fmt.Sprintf("-v%d", i)
			res := helpers.NewCluster(systemCount, brNum)
			resCollection := res.Copy()

			csLock1 := lock.NewLockBuilder("partial-system" + postName).
				EnablePartialMode().WithSystemRequest(systemCount).BuildPending()
			cfg := cfgFromCollection(res)
			res.UpdateSubresource("system/system-0", resource.SystemPortSubresource, 11)
			cfg.SystemMap["system-0"].Ports[1].HasError = true
			controller.NewCfg = &wscommon.ClusterConfig{
				Resources: res,
				SystemMap: cfg.SystemMap,
			}
			require.NoError(t, k8sClient.Create(ctx, csLock1))
			assertGranted(t, csLock1)
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock1), csLock1))
			validate(t, csLock1.Status.GroupResourceGrants, 1)
			require.NoError(t, k8sClient.Delete(ctx, csLock1))

			csLock2 := lock.NewLockBuilder("br-node-down" + postName).
				EnablePartialMode().WithSystemRequest(systemCount).BuildPending()
			res = resCollection.Copy()
			res.Remove("node/br-1")
			res.Remove("node/br-7")
			cfg.SystemMap["system-0"].Ports[1].HasError = false
			controller.NewCfg = &wscommon.ClusterConfig{
				Resources: res,
			}
			controller.internalReconcileEventCh <- triggerReinitialize
			require.NoError(t, k8sClient.Create(ctx, csLock2))
			assertGranted(t, csLock2)
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock2), csLock2))
			validate(t, csLock2.Status.GroupResourceGrants, skipGroup)
			require.NoError(t, k8sClient.Delete(ctx, csLock2))

			csLock3 := lock.NewLockBuilder("br-nic-down" + postName).
				EnablePartialMode().WithSystemRequest(systemCount).BuildPending()
			res = resCollection.Copy()
			res.UpdateSubresource("node/br-1", "cs-port/nic-0", 0)
			res.UpdateSubresource("node/br-1", "cs-port/nic-1", 0)
			controller.NewCfg = &wscommon.ClusterConfig{
				Resources: res,
			}
			require.NoError(t, k8sClient.Create(ctx, csLock3))
			assertGranted(t, csLock3)
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock3), csLock3))
			validate(t, csLock3.Status.GroupResourceGrants, skipGroup)
			require.NoError(t, k8sClient.Delete(ctx, csLock3))

			csLock4 := lock.NewLockBuilder("multiple-br-failure" + postName).
				EnablePartialMode().WithSystemRequest(systemCount).BuildPending()
			res.Remove("node/br-2")
			controller.NewCfg = &wscommon.ClusterConfig{
				Resources: res,
			}
			require.NoError(t, k8sClient.Create(ctx, csLock4))
			if wsapisv1.IsV2Network {
				// v2 has a much larger fault tolerance on BR nodes/ports
				assertGranted(t, csLock4)
			} else {
				assertFailed(t, csLock4)
			}

			// system are scheduled first so system errors will dominate
			res.UpdateSubresource("system/system-0", resource.SystemPortSubresource, 10)
			cfg.SystemMap["system-0"].Ports[0].HasError = true
			cfg.SystemMap["system-0"].Ports[1].HasError = true
			controller.NewCfg = &wscommon.ClusterConfig{
				Resources: res,
				SystemMap: cfg.SystemMap,
			}
			csLock5 := lock.NewLockBuilder("multiple-port-failure" + postName).
				EnablePartialMode().WithSystemRequest(systemCount).BuildPending()
			require.NoError(t, k8sClient.Create(ctx, csLock5))
			assertFailed(t, csLock5)
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock5), csLock5))
			require.Contains(t, csLock5.Status.Message, "unable to schedule systems due to multiple port group failures: system-0{ports=n00,n01}")

			// trigger reload
			controller.NewCfg = &wscommon.ClusterConfig{Resources: collection}
			controller.internalReconcileEventCh <- triggerReinitialize

			require.NoError(t, k8sClient.DeleteAllOf(ctx, &rlv1.ResourceLock{}, client.InNamespace(wscommon.Namespace)))
			require.Eventually(t, func() bool {
				list := &rlv1.ResourceLockList{}
				return k8sClient.List(ctx, list) == nil && len(list.Items) == 0
			}, eventuallyTimeout, eventuallyTick)
		}
	})

	t.Run("V2 BR schedule affinity", func(t *testing.T) {
		wsapisv1.IsV2Network = true
		defer func() {
			wsapisv1.IsV2Network = false
		}()
		res := helpers.NewCluster(systemCount, brNum)
		cfg := &wscommon.ClusterConfig{
			Resources: res,
		}
		controller.NewCfg = cfg

		csLock1 := lock.NewLockBuilder("v2-br-2-system-a").
			WithSystemRequest(2).WithPrimaryNodeGroup().WithSecondaryNodeGroup(1).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock1))
		assertGranted(t, csLock1)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock1), csLock1))
		for _, group := range csLock1.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				t.Logf("%+v", group.ResourceGrants)
				require.Equal(t, 12, len(group.ResourceGrants))
				for i := 0; i < 12; i++ {
					brId := group.ResourceGrants[i].Resources[0].Id()
					br, exists := res.Get(brId)
					require.True(t, exists)
					for _, sys := range csLock1.Status.SystemGrants {
						// expect assigned br has group affinity on assign sys port
						_, ok := br.GetProp(systemv1.GetSysPortAffinityKey(sys.Name, i%12))
						require.True(t, ok)
					}
				}
			}
		}

		csLock2 := lock.NewLockBuilder("v2-br-2-system-b").WithSystemRequest(2).
			WithPrimaryNodeGroup().WithSecondaryNodeGroup(1).EnableBalanced().BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock2))
		assertGranted(t, csLock2)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock2), csLock2))
		for _, group := range csLock2.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				t.Logf("%+v", group.ResourceGrants)
			}
		}

		require.NoError(t, k8sClient.Delete(ctx, csLock1))
		csLock3 := lock.NewLockBuilder("v2-br-4-system").
			WithPrimaryNodeGroup().WithSecondaryNodeGroup(3).
			WithSystemRequest(4).EnableBalanced().BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock3))
		assertGranted(t, csLock3)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock3), csLock3))
		for _, group := range csLock3.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				t.Logf("%+v", group.ResourceGrants)
			}
		}

		l2, _ := lock.NewLockFromK8s(csLock2)
		l3, _ := lock.NewLockFromK8s(csLock3)
		allocated := resource.GatherAllocations(nil, map[string][]resource.GroupGrant{"": {l2.BrGroupGrant, l3.BrGroupGrant}})
		for n, r := range allocated {
			for k, v := range r.Subresources {
				if strings.Contains(k, resource.NodeNicSubresource) {
					// checking error of over booking possibly caused by duplicated deallocating of BR
					assert.True(t, v <= 6*resource.DefaultNicBW, "%s allocated %d %s, expecting <= %d", n, v, k, 6*resource.DefaultNicBW)
				}
			}
		}
		require.NoError(t, k8sClient.Delete(ctx, csLock2))
		require.NoError(t, k8sClient.Delete(ctx, csLock3))

		csLock4 := lock.NewLockBuilder("v2-br-6-system").WithSystemRequest(6).EnableBalanced().BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock4))
		assertGranted(t, csLock4)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock4), csLock4))
		// allocated br nodes
		allocatedNodes := map[string]bool{}
		for _, group := range csLock4.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				t.Logf("%+v", group.ResourceGrants)
				require.Equal(t, 36, len(group.ResourceGrants))
				for i := 0; i < 36; i++ {
					brId := group.ResourceGrants[i].Resources[0].Id()
					br, exists := res.Get(brId)
					require.True(t, exists)
					if i < 12 {
						allocatedNodes[brId] = true
						for _, sys := range csLock4.Status.SystemGrants[:4] {
							// expect bottom layer has affinity on direct connected systems
							_, ok := br.GetProp(systemv1.GetSysPortAffinityKey(sys.Name, i%12))
							require.True(t, ok)
						}
					} else if i < 24 {
						allocatedNodes[brId] = true
						for _, sys := range csLock4.Status.SystemGrants[4:] {
							// expect bottom layer has affinity on direct connected systems
							_, ok := br.GetProp(systemv1.GetSysPortAffinityKey(sys.Name, i%12))
							require.True(t, ok)
						}
					} else {
						// assert different layers won't share same node
						require.False(t, allocatedNodes[brId])
					}
				}
			}
		}
		require.NoError(t, k8sClient.Delete(ctx, csLock4))

		require.NoError(t, k8sClient.DeleteAllOf(ctx, &rlv1.ResourceLock{}, client.InNamespace(wscommon.Namespace)))
		require.Eventually(t, func() bool {
			list := &rlv1.ResourceLockList{}
			return k8sClient.List(ctx, list) == nil && len(list.Items) == 0
		}, eventuallyTimeout, eventuallyTick)
	})

	t.Run("V2 BR policy pick", func(t *testing.T) {
		wsapisv1.IsV2Network = true
		defer func() {
			wsapisv1.IsV2Network = false
		}()
		res := helpers.NewClusterBuilder(1).WithV2Network().
			WithSystems(5, "g0").
			WithSystems(3, "g1").
			WithBR(30).Build()
		cfg := &wscommon.ClusterConfig{
			Resources: res,
		}
		controller.NewCfg = cfg

		// port policy picked because 5+3 will generate 2*1:3 mode which won't fit in 30 sx nodes
		// port policy: 1:4 (same-stamp) + 1:4 (cross-stamp) + 1:2 (cross-stamp), requires 5 + 5 + 3 = 13ports
		// spine policy: 1:4 (same-stamp) + 1:3 (cross-stamp) + 1:3 (same-stamp), requires 5 + 4 + 4 = 13ports
		csLock1 := lock.NewLockBuilder("v2-br-8-system-policy-port").
			WithSystemRequest(8).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock1))
		assertGranted(t, csLock1)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock1), csLock1))
		for _, group := range csLock1.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				require.Equal(t, wsapisv1.BrTreePolicyPort, group.Annotations[rlv1.BrLogicalTreePolicy])
			}
		}
		systemGrants := wscommon.Map(func(grant rlv1.SystemGrant) string {
			return grant.Name
		}, csLock1.Status.SystemGrants)
		require.Equal(t, []string{"system-5", "system-6", "system-7", "system-0", "system-1", "system-2", "system-3", "system-4"}, systemGrants)
		require.NoError(t, k8sClient.Delete(ctx, csLock1))

		res = helpers.NewClusterBuilder(1).WithV2Network().
			WithSystems(3, "g0").
			WithSystems(5, "g1").
			WithBR(36).Build()
		cfg = &wscommon.ClusterConfig{
			Resources: res,
		}
		controller.NewCfg = cfg
		// spine picked since spine uses the same 13ports but less cross traffic
		csLock2 := lock.NewLockBuilder("v2-br-8-system-policy-spine").
			WithSystemRequest(8).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock2))
		assertGranted(t, csLock2)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock2), csLock2))
		for _, group := range csLock2.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				require.Equal(t, wsapisv1.BrTreePolicySpine, group.Annotations[rlv1.BrLogicalTreePolicy])
			}
		}
		require.NoError(t, k8sClient.Delete(ctx, csLock2))

		res = helpers.NewClusterBuilder(1).WithV2Network().
			WithSystems(10, "g0").
			WithSystems(2, "g1").
			WithBR(48).Build()
		cfg = &wscommon.ClusterConfig{
			Resources: res,
		}
		controller.NewCfg = cfg
		// port picked since spine uses more ports and redundant port is 0 by default
		// port policy: 2* 1:4 (same-stamp) + 1:4 (cross-stamp) + 1:3 (cross-stamp), requires 19 ports
		// spine policy: 2* 1:4 (same-stamp) + 1:2 (same-stamp) + 1:2 (same-stamp) + 1:4 (cross-stamp), requires 21 ports
		csLock3 := lock.NewLockBuilder("v2-br-12-system-policy-port").
			WithSystemRequest(12).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock3))
		assertGranted(t, csLock3)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock3), csLock3))
		for _, group := range csLock3.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				require.Equal(t, wsapisv1.BrTreePolicyPort, group.Annotations[rlv1.BrLogicalTreePolicy])
			}
		}
		require.NoError(t, k8sClient.Delete(ctx, csLock3))

		res = helpers.NewClusterBuilder(1).WithV2Network().
			WithSystems(10, "g0").
			WithSystems(2, "g1").
			WithBR(48).Build()
		cfg = &wscommon.ClusterConfig{
			Resources: res,
		}
		controller.NewCfg = cfg
		wsapisv1.RedundantBrPortBwPerSystem = 2.0
		defer func() {
			wsapisv1.RedundantBrPortBwPerSystem = 0
		}()
		// spine picked since redundant port is enough, 19 +2 = 21
		csLock4 := lock.NewLockBuilder("v2-br-12-system-policy-spine").
			WithSystemRequest(12).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock4))
		assertGranted(t, csLock4)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock4), csLock4))
		for _, group := range csLock4.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				require.Equal(t, wsapisv1.BrTreePolicySpine, group.Annotations[rlv1.BrLogicalTreePolicy])
			}
		}
		require.NoError(t, k8sClient.Delete(ctx, csLock4))
	})

	t.Run("Multiple systems with skip BR", func(t *testing.T) {
		res := helpers.NewCluster(systemCount, brNum)
		cfg := &wscommon.ClusterConfig{
			Resources: res,
		}
		controller.NewCfg = cfg

		csLock1 := lock.NewLockBuilder("skip-br-with-2-systems").
			WithSystemRequest(2).WithPrimaryNodeGroup().WithSecondaryNodeGroup(1).WithSkipBR(true).BuildPending()
		require.NoError(t, k8sClient.Create(ctx, csLock1))
		assertGranted(t, csLock1)
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(csLock1), csLock1))
		hasBR := false
		for _, group := range csLock1.Status.GroupResourceGrants {
			if group.Name == rlv1.BrRequestName {
				hasBR = true
			}
		}
		assert.False(t, hasBR, "No BR nodes should be granted")
	})
}

func TestReconcileSharedSession(t *testing.T) {
	cluster.DisableLabeler = true
	namespace.DisableSystemNsAutoAssign = true
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
		cluster.DisableLabeler = false
		namespace.DisableSystemNsAutoAssign = false
	}()

	redundantNs := "redundant"
	userNs := "user"
	wscommon.Namespace = redundantNs
	collection := helpers.NewDefaultClusterBuilder(systemCount, brNum)
	wscommon.Namespace = userNs
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: wscommon.Namespace,
		},
	}
	require.NoError(t, k8sClient.Create(ctx, ns))
	defer func() {
		require.NoError(t, k8sClient.Delete(ctx, ns))
		wscommon.Namespace = wscommon.DefaultNamespace
	}()
	collection = collection.
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeRegular).
		WithSystem("").
		WithCoordinator(1)
	controller := newController(collection.Build())
	err := controller.SetupWithManager(mgr)
	require.NoError(t, err)

	l := lock.NewLockBuilder("lock-with-redundancy").
		WithSystemRequest(1).
		WithCoordinatorRequest(1, 1, nil).
		WithPrimaryNodeGroup().
		WithAssignedRedundancy([]string{redundantNs}).
		BuildPendingForNS(userNs)
	require.NoError(t, k8sClient.Create(ctx, l))
	defer func() {
		require.NoError(t, k8sClient.Delete(ctx, l))
	}()

	// validate local session affinity
	// shared session gets system0-7,nodegroup0-7,coordinator-0
	// without local affinity, it will prefer system0/nodegroup0/coordinator0
	assertGranted(t, l)
	require.NoError(t, k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(l), l))
	require.Equal(t, l.Status.SystemGrants[0].Name, "system-8")
	require.Equal(t, l.Status.NodeGroupGrants[0].NodeGroups[0], "nodegroup8")
	for _, res := range l.Status.ResourceGrants {
		if res.Name == rlv1.CoordinatorRequestName {
			require.Equal(t, res.Resources[0].Name, "coordinator-1")
		}
	}
}

func createClusterCfgCM(c client.Client, cfg *wscommon.ClusterConfig) error {
	cfgBytes, _ := yaml.Marshal(cfg.ClusterSchema)
	err := c.Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: wscommon.ClusterConfigMapName, Namespace: wscommon.SystemNamespace},
		Data:       map[string]string{wscommon.ClusterConfigFile: string(cfgBytes)},
	})
	return err
}

func cfgFromCollection(collection *resource.Collection) wscommon.ClusterConfig {
	cfg := wscommon.ClusterConfig{
		SystemMap: map[string]*systemv1.SystemSpec{},
	}
	for _, sysResource := range collection.List() {
		sys := &systemv1.SystemSpec{
			Name: sysResource.Name(),
			Type: string(common.CSVersionSpec.Version),
		}
		sys.InitPorts()
		cfg.SystemMap[sysResource.Name()] = sys
	}
	return cfg
}

//go:build integration && ix_scheduling

package ws

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource_config"
)

// TestClusterHealthOnIxScheduling validates IX node health monitoring and failure handling
// Tests node-level conditions, NIC-specific failures, and recovery scenarios
func (t *TestSuite) TestClusterHealthOnIxScheduling() {
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	// Basic IX scheduling with healthy nodes - verifies single SWDriver job schedules successfully
	// on healthy IX nodes with proper domain assignment
	t.Run("ix_scheduling_single_swdriver_healthy_nodes", func() {
		deleteNodes(t.T())

		// Enable IX scheduling
		wsapisv1.UseIxScheduling = true
		defer func() {
			wsapisv1.UseIxScheduling = false
		}()

		// Create cluster with 2 IX nodes
		v2Cfg := wscommon.NewTestClusterConfigSchema(1, 1, 12, 1, 2, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

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

		userNsrName := "user-ns-ix-health-test"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:               namespacev1.DebugSetMode,
				ManagementNodeNames:      []string{"0-coord-0"},
				NodegroupNames:           []string{"group0"},
				SystemNames:              []string{"system-0"},
				ActivationNodeNames:      []string{"ax-0"},
				InferenceDriverNodeNames: []string{"ix-0", "ix-1"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		// Test 1: Both IX nodes healthy - job should succeed
		job1 := newInfExecuteJob(userNsrName, "swdriver-healthy", 1, 0, 1)
		job1.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		defer assertWsjobCleanedUp(t.T(), job1)

		createSwDriverConfigMap(t.T(), job1, "testdata/1_sys_1_swdriver_domain_3.json")

		// Should succeed using healthy IX nodes
		assertWsJobScheduledEventually(t.T(), job1.Namespace, job1.Name)
		lock1 := assertLockExists(t.T(), job1.Namespace, job1.Name)
		assertLocked(t.T(), lock1)

		// Should use one of the healthy IX nodes
		assertNodesScheduled(t, map[string]bool{
			"ix-0": true, // or ix-1, either is fine
		}, lock1, rlv1.SWDriverRequestName)

		assertPodCountEventually(t.T(), job1.Namespace, job1.Name, rlv1.SWDriverRequestName, 1)
		assertWsjobCleanedUp(t.T(), job1)

		// Test 2: Mark one IX node unhealthy with node-level condition
		require.NoError(t.T(), patchNode(t, "ix-1", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))

		job2 := newInfExecuteJob(userNsrName, "swdriver-one-ix-down", 1, 0, 1)
		job2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		defer assertWsjobCleanedUp(t.T(), job2)

		createSwDriverConfigMap(t.T(), job2, "testdata/1_sys_1_swdriver_domain_3.json")

		// Should still succeed using the healthy IX node (ix-0)
		assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)
		lock2 := assertLockExists(t.T(), job2.Namespace, job2.Name)
		assertLocked(t.T(), lock2)

		// Should use the healthy IX node only
		assertNodesScheduled(t, map[string]bool{
			"ix-0": true,
		}, lock2, rlv1.SWDriverRequestName)

		assertPodCountEventually(t.T(), job2.Namespace, job2.Name, rlv1.SWDriverRequestName, 1)
		assertWsjobCleanedUp(t.T(), job2)

		// Test 3: Mark both IX nodes unhealthy - job should fail
		require.NoError(t.T(), patchNode(t, "ix-0", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))

		job3 := newInfExecuteJob(userNsrName, "swdriver-all-ix-down", 1, 0, 1)
		job3.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))
		defer assertWsjobCleanedUp(t.T(), job3)

		createSwDriverConfigMap(t.T(), job3, "testdata/1_sys_1_swdriver_domain_3.json")

		// Should fail since no healthy IX nodes available
		assertWsJobFailEventually(t.T(), job3.Namespace, job3.Name)
		assertWsjobCleanedUp(t.T(), job3)

		// Test 4: Recovery - mark nodes healthy again
		require.NoError(t.T(), patchNode(t, "ix-0", []corev1.NodeCondition{}))
		require.NoError(t.T(), patchNode(t, "ix-1", []corev1.NodeCondition{}))

		job4 := newInfExecuteJob(userNsrName, "swdriver-recovery", 1, 0, 1)
		job4.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job4))
		defer assertWsjobCleanedUp(t.T(), job4)

		createSwDriverConfigMap(t.T(), job4, "testdata/1_sys_1_swdriver_domain_3.json")

		// Should succeed with recovered nodes
		assertWsJobScheduledEventually(t.T(), job4.Namespace, job4.Name)
		lock4 := assertLockExists(t.T(), job4.Namespace, job4.Name)
		assertLocked(t.T(), lock4)

		assertPodCountEventually(t.T(), job4.Namespace, job4.Name, rlv1.SWDriverRequestName, 1)
	})

	t.Run("ix_swdriver_cross_job_port_load_balancing", func() {
		deleteNodes(t.T())
		// Enable IX scheduling
		wsapisv1.UseIxScheduling = true
		resource_config.MaxSwDriverPodsPerIxNode = 1
		defer func() {
			resource_config.MaxSwDriverPodsPerIxNode = 1
			wsapisv1.UseIxScheduling = false
		}()

		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 1, 0, 4, 4, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 12)

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

		userNsrName := "user-ns-ix-port-load-balancing"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:               namespacev1.DebugSetMode,
				ManagementNodeNames:      []string{"0-coord-0"},
				NodegroupNames:           []string{"group0"},
				SystemNames:              []string{"system-0", "system-1", "system-2", "system-3"},
				ActivationNodeNames:      []string{"ax-0", "ax-1", "ax-2", "ax-3"},
				InferenceDriverNodeNames: []string{"ix-0", "ix-1", "ix-2", "ix-3"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		assertSwDriverPort := func(job *wsapisv1.WSJob, expectedPort int) {
			swDrivers := assertPodCountEventually(t.T(), job.Namespace, job.Name, rlv1.SWDriverRequestName, 1)

			actualPort := -1
			var err error
			for _, env := range swDrivers.Items[0].Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeCsPortEnvVarName {
					actualPort, err = strconv.Atoi(env.Value)
					require.NoError(t.T(), err)
				}
			}
			assert.Equal(t.T(), expectedPort, actualPort)
		}

		// jobs alternate between least used ports in domain 3 across jobs
		// SWDriver jobs targeting same domain should get different ports
		job0 := newInfExecuteJob(userNsrName, "ix-target-domain-3-job-0", 1, 0, 1)
		job0.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		defer assertWsjobCleanedUp(t.T(), job0)

		createSwDriverConfigMap(t.T(), job0, "testdata/1_sys_1_swdriver_domain_3.json")
		assertWsJobScheduledEventually(t.T(), job0.Namespace, job0.Name)

		assertLocked(t.T(), assertLockExists(t.T(), job0.Namespace, job0.Name))
		assertSwDriverPort(job0, 10)

		job1 := newInfExecuteJob(userNsrName, "ix-target-domain-3-job-1", 1, 0, 1)
		job1.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		defer assertWsjobCleanedUp(t.T(), job1)
		createSwDriverConfigMap(t.T(), job1, "testdata/1_sys_1_swdriver_domain_3.json")
		assertWsJobScheduledEventually(t.T(), job1.Namespace, job1.Name)
		assertLocked(t.T(), assertLockExists(t.T(), job1.Namespace, job1.Name))

		assertSwDriverPort(job1, 9)

		job2 := newInfExecuteJob(userNsrName, "ix-target-domain-3-job-2", 1, 0, 1)
		job2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		defer assertWsjobCleanedUp(t.T(), job2)
		createSwDriverConfigMap(t.T(), job2, "testdata/1_sys_1_swdriver_domain_3.json")
		assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)
		assertLocked(t.T(), assertLockExists(t.T(), job2.Namespace, job2.Name))

		assertSwDriverPort(job2, 9)

		job3 := newInfExecuteJob(userNsrName, "ix-target-domain-3-job-3", 1, 0, 1)
		job3.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))
		defer assertWsjobCleanedUp(t.T(), job3)
		createSwDriverConfigMap(t.T(), job3, "testdata/1_sys_1_swdriver_domain_3.json")
		assertWsJobScheduledEventually(t.T(), job3.Namespace, job3.Name)
		assertLocked(t.T(), assertLockExists(t.T(), job3.Namespace, job3.Name))

		assertSwDriverPort(job3, 10)

	})

	// NIC-level failure handling - tests granular NIC health monitoring
	// and IX scheduling with partial NIC failures across multiple scenarios
	t.Run("ix_scheduling_swdriver_single_nic_failure", func() {
		deleteNodes(t.T())

		// Enable IX scheduling
		wsapisv1.UseIxScheduling = true
		defer func() {
			wsapisv1.UseIxScheduling = false
		}()

		// Create cluster with 2 IX nodes
		v2Cfg := wscommon.NewTestClusterConfigSchema(1, 1, 12, 1, 2, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

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

		userNsrName := "user-ns-ix-nic-failure-test"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:               namespacev1.DebugSetMode,
				ManagementNodeNames:      []string{"0-coord-0"},
				NodegroupNames:           []string{"group0"},
				SystemNames:              []string{"system-0"},
				ActivationNodeNames:      []string{"ax-0"},
				InferenceDriverNodeNames: []string{"ix-0", "ix-1"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		nic0SrName := fmt.Sprintf("%s/nic0", wsresource.NodeNicSubresource)
		nic1SrName := fmt.Sprintf("%s/nic1", wsresource.NodeNicSubresource)

		// Test 1: Both IX nodes healthy - job should succeed normally
		job1 := newInfExecuteJob(userNsrName, "swdriver-both-nics-healthy", 1, 0, 1)
		job1.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		defer assertWsjobCleanedUp(t.T(), job1)

		createSwDriverConfigMap(t.T(), job1, "testdata/1_sys_1_swdriver_domain_3.json")

		assertWsJobScheduledEventually(t.T(), job1.Namespace, job1.Name)
		lock1 := assertLockExists(t.T(), job1.Namespace, job1.Name)
		assertLocked(t.T(), lock1)

		// Should use one of the healthy IX nodes with all NICs available
		foundIxNode := ""
		for _, gg := range lock1.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, rlv1.SWDriverRequestName) {
				continue
			}
			for _, rg := range gg.ResourceGrants {
				for _, res := range rg.Resources {
					if strings.HasPrefix(res.Name, "ix-") {
						foundIxNode = res.Name
						// Should have access to both NICs (or at least one, depending on domain 3 requirements)
						hasNics := len(res.Subresources) > 0
						for nicName := range res.Subresources {
							if strings.Contains(nicName, "nic") {
								hasNics = true
								break
							}
						}
						assert.True(t.T(), hasNics, "IX node should have NIC resources available")
					}
				}
			}
		}
		assert.True(t.T(), foundIxNode == "ix-0" || foundIxNode == "ix-1", "Should use either ix-0 or ix-1")

		assertPodCountEventually(t.T(), job1.Namespace, job1.Name, rlv1.SWDriverRequestName, 1)
		assertWsjobCleanedUp(t.T(), job1)

		// Test 2: Mark one NIC down on ix-0 (nic0 failure)
		require.NoError(t.T(), patchNode(t, "ix-0", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
			Status: corev1.ConditionTrue,
		}}))

		job2 := newInfExecuteJob(userNsrName, "swdriver-single-nic-down", 1, 0, 1)
		job2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		defer assertWsjobCleanedUp(t.T(), job2)

		createSwDriverConfigMap(t.T(), job2, "testdata/1_sys_1_swdriver_domain_3.json")

		// Should still succeed but avoid the failed NIC
		assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)
		lock2 := assertLockExists(t.T(), job2.Namespace, job2.Name)
		assertLocked(t.T(), lock2)

		// Verify the job uses either:
		// 1. ix-0 with only the healthy NIC (nic1), or
		// 2. ix-1 with both NICs healthy
		// The scheduler should prefer ix-1 (fully healthy) over ix-0 (partially degraded)
		foundHealthyIxNode := false
		for _, gg := range lock2.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, rlv1.SWDriverRequestName) {
				continue
			}
			for _, rg := range gg.ResourceGrants {
				for _, res := range rg.Resources {
					if res.Name == "ix-0" {
						// If ix-0 is used, it should only have nic1 (healthy NIC)
						_, hasNic0 := res.Subresources[nic0SrName]
						_, hasNic1 := res.Subresources[nic1SrName]
						assert.False(t.T(), hasNic0, "ix-0 should not use failed nic0")
						if hasNic1 {
							foundHealthyIxNode = true
						}
					} else if res.Name == "ix-1" {
						// ix-1 should have both NICs available
						_, hasNic0 := res.Subresources[nic0SrName]
						_, hasNic1 := res.Subresources[nic1SrName]
						if hasNic0 || hasNic1 {
							foundHealthyIxNode = true
						}
						assert.True(t.T(), hasNic0 || hasNic1, "ix-1 should have at least one NIC")
					}
				}
			}
		}
		assert.True(t.T(), foundHealthyIxNode, "Should find a healthy IX node with available NICs")

		assertPodCountEventually(t.T(), job2.Namespace, job2.Name, rlv1.SWDriverRequestName, 1)
		assertWsjobCleanedUp(t.T(), job2)

		// Test 3: Mark both NICs down on ix-0, one NIC down on ix-1
		require.NoError(t.T(), patchNode(t, "ix-0", []corev1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
				Status: corev1.ConditionTrue,
			},
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic1",
				Status: corev1.ConditionTrue,
			},
		}))
		require.NoError(t.T(), patchNode(t, "ix-1", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
			Status: corev1.ConditionTrue,
		}}))

		job3 := newInfExecuteJob(userNsrName, "swdriver-partial-nic-failures", 1, 0, 1)
		job3.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job3))
		defer assertWsjobCleanedUp(t.T(), job3)

		createSwDriverConfigMap(t.T(), job3, "testdata/1_sys_1_swdriver_domain_3.json")

		// Should use ix-1 with its remaining healthy NIC (nic1)
		assertWsJobScheduledEventually(t.T(), job3.Namespace, job3.Name)
		lock3 := assertLockExists(t.T(), job3.Namespace, job3.Name)
		assertLocked(t.T(), lock3)

		// Should prefer ix-1 since it still has one healthy NIC
		foundIx1WithNic1 := false
		for _, gg := range lock3.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, rlv1.SWDriverRequestName) {
				continue
			}
			for _, rg := range gg.ResourceGrants {
				for _, res := range rg.Resources {
					if res.Name == "ix-1" {
						_, hasNic1 := res.Subresources[nic1SrName]
						if hasNic1 {
							foundIx1WithNic1 = true
						}
						// Should not use nic0 on ix-1 (it's failed)
						_, hasNic0 := res.Subresources[nic0SrName]
						assert.False(t.T(), hasNic0, "ix-1 should not use failed nic0")
					}
					// ix-0 should not be used at all (both NICs failed)
					assert.NotEqual(t.T(), "ix-0", res.Name, "ix-0 should not be used (all NICs failed)")
				}
			}
		}
		assert.True(t.T(), foundIx1WithNic1, "Should use ix-1 with its healthy nic1")

		assertPodCountEventually(t.T(), job3.Namespace, job3.Name, rlv1.SWDriverRequestName, 1)
		assertWsjobCleanedUp(t.T(), job3)

		// Test 4: Recovery - restore all NICs to healthy
		require.NoError(t.T(), patchNode(t, "ix-0", []corev1.NodeCondition{}))
		require.NoError(t.T(), patchNode(t, "ix-1", []corev1.NodeCondition{}))

		job4 := newInfExecuteJob(userNsrName, "swdriver-nics-recovered", 1, 0, 1)
		job4.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job4))
		defer assertWsjobCleanedUp(t.T(), job4)

		createSwDriverConfigMap(t.T(), job4, "testdata/1_sys_1_swdriver_domain_3.json")

		// Should succeed with all nodes and NICs healthy again
		assertWsJobScheduledEventually(t.T(), job4.Namespace, job4.Name)
		lock4 := assertLockExists(t.T(), job4.Namespace, job4.Name)
		assertLocked(t.T(), lock4)

		// Should use either IX node with full NIC availability restored
		foundHealthyIxNodeRecovery := false
		for _, gg := range lock4.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, rlv1.SWDriverRequestName) {
				continue
			}
			for _, rg := range gg.ResourceGrants {
				for _, res := range rg.Resources {
					if strings.HasPrefix(res.Name, "ix-") {
						// Should have NIC resources available again
						hasNics := false
						for nicName := range res.Subresources {
							if strings.Contains(nicName, "nic") {
								hasNics = true
								break
							}
						}
						if hasNics {
							foundHealthyIxNodeRecovery = true
						}
					}
				}
			}
		}
		assert.True(t.T(), foundHealthyIxNodeRecovery, "Should find healthy IX nodes with NICs after recovery")

		assertPodCountEventually(t.T(), job4.Namespace, job4.Name, rlv1.SWDriverRequestName, 1)
	})
}

// TestIXScheduling covers core IX scheduling functionality and resource sharing
// Tests basic scheduling, job sharing, resource contention, and pod limits
func (t *TestSuite) TestIXScheduling() {
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	// Helper function to get domain from SWDriver config map
	getDomainFromSwDriverConfigMap := func(job *wsapisv1.WSJob) int {
		configMapName := fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job.Name)
		configMap := &corev1.ConfigMap{}
		err := k8sClient.Get(context.Background(), client.ObjectKey{
			Name:      configMapName,
			Namespace: job.Namespace,
		}, configMap)
		require.NoError(t.T(), err)

		// Decompress and parse the cluster details
		compressedData := configMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName]
		data, err := wscommon.Gunzip(compressedData)
		require.NoError(t.T(), err)

		clusterDetails := &commonpb.ClusterDetails{}
		options := protojson.UnmarshalOptions{
			AllowPartial:   true,
			DiscardUnknown: true,
		}
		err = options.Unmarshal(data, clusterDetails)
		require.NoError(t.T(), err)

		// Find SWDriver task and extract domain
		for _, taskInfo := range clusterDetails.Tasks {
			if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_SWD {
				for _, taskMap := range taskInfo.TaskMap {
					if len(taskMap.AddressBook.WseDomains) > 0 {
						return int(taskMap.AddressBook.WseDomains[0])
					}
				}
			}
		}

		require.Fail(t.T(), "Could not find SWDriver domain in config map")
		return -1
	}

	// Helper function to get domain port range
	getDomainPortRange := func(domain, totalPorts, numDomains int) (int, int) {
		portsPerDomain := totalPorts / numDomains
		startPort := domain * portsPerDomain
		endPort := startPort + portsPerDomain - 1
		return startPort, endPort
	}

	// Helper function to assert IX NIC is on specified domain switch
	assertIxNicOnDomainSwitch := func(lock *rlv1.ResourceLock, requestName string, expectedDomain int, expectedNicName string) {
		for _, gg := range lock.Status.GroupResourceGrants {
			if !strings.HasPrefix(gg.Name, requestName) {
				continue
			}
			for _, rg := range gg.ResourceGrants {
				for _, res := range rg.Resources {
					if strings.HasPrefix(res.Name, "ix-") {
						// Check that the IX node has a NIC assigned to the expected domain switch
						foundExpectedDomainNic := false
						for nicName := range res.Subresources {
							if strings.Contains(nicName, expectedNicName) {
								foundExpectedDomainNic = true
								break
							}
						}
						assert.True(t.T(), foundExpectedDomainNic,
							"IX node %s should have NIC assigned to domain %d switch (leaf-0-%d)",
							res.Name, expectedDomain, expectedDomain)
					}
				}
			}
		}
	}

	// Single SWDriver job scheduling - validates basic IX node assignment
	// and proper domain/port configuration for inference workloads
	t.Run("ix_scheduling_single_swdriver_job", func() {
		deleteNodes(t.T())
		// Enable IX scheduling

		wsapisv1.UseIxScheduling = true
		defer func() {
			wsapisv1.UseIxScheduling = false
		}()

		// Create cluster with IX nodes - 2 systems, 2 AX nodes, 2 IX nodes
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 12, 2, 2, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

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

		userNsrName := "user-ns-ix-swdriver-basic"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:               namespacev1.DebugSetMode,
				ManagementNodeNames:      []string{"0-coord-0"},
				NodegroupNames:           []string{"group0", "group1"},
				SystemNames:              []string{"system-0", "system-1"},
				ActivationNodeNames:      []string{"ax-0", "ax-1"},
				InferenceDriverNodeNames: []string{"ix-0", "ix-1"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus like other inference tests
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		// Create inference job with SWDriver
		inferenceJob := newInfExecuteJob(userNsrName, "swdriver-basic-test", 1, 0, 1)
		inferenceJob.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
		defer assertWsjobCleanedUp(t.T(), inferenceJob)

		// Set cluster details for SWDriver - domain 3 of first system
		createSwDriverConfigMap(t.T(), inferenceJob, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name)

		// Verify lock is granted
		lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
		assertLocked(t.T(), lock)

		// Verify SWDriver gets IX node grant using the pattern from other inference tests
		assertNodesScheduled(t, map[string]bool{
			"ix-0": true,
		}, lock, rlv1.SWDriverRequestName)

		// Verify SWDriver pod gets created and scheduled correctly
		swDriverPods := assertPodCountEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name, rlv1.SWDriverRequestName, 1)
		swDriverPod := swDriverPods.Items[0]

		// Verify IX node selector
		val, exists := swDriverPod.Spec.NodeSelector[fmt.Sprintf(RoleLabelKeyF, common.RoleInferenceDriver)]
		t.T().Logf("SWDriver pod node selector value: %s", val)
		assert.True(t.T(), exists, "SWDriver pod should have IX node selector")

		// Verify domain and port assignment
		// Then in the test, replace the hardcoded assertion:
		foundPortEnv := false
		for _, env := range swDriverPod.Spec.Containers[0].Env {
			t.T().Logf("SWDriver port environment variable: %s=%s", env.Name, env.Value)
			if env.Name == wsapisv1.RuntimeCsPortEnvVarName {
				port, err := strconv.Atoi(env.Value)
				assert.NoError(t.T(), err)

				// Get the actual domain from the config map
				actualDomain := getDomainFromSwDriverConfigMap(inferenceJob)
				startPort, endPort := getDomainPortRange(actualDomain, 12, 4)

				assert.True(t.T(), port >= startPort && port <= endPort,
					"SWDriver should be assigned port in domain %d (%d-%d), got: %d",
					actualDomain, startPort, endPort, port)
				foundPortEnv = true
			}
		}
		assert.True(t.T(), foundPortEnv, "Should find port environment variable")
		assertIxNicOnDomainSwitch(lock, rlv1.SWDriverRequestName, 3, "nic1")

		// Verify system assignment matches expected order
		assertSystemsOrdered(t, []string{"system-0"}, lock)
	})

	// Job sharing with limited IX resources - tests multiple jobs sharing IX nodes
	// when IX node count is insufficient for exclusive allocation
	t.Run("ix_scheduling_swdriver_job_sharing", func() {
		deleteNodes(t.T())

		// Enable IX scheduling
		wsapisv1.UseIxScheduling = true
		resource_config.MaxSwDriverPodsPerIxNode = 2
		defer func() {
			wsapisv1.UseIxScheduling = false
			resource_config.MaxSwDriverPodsPerIxNode = 1
		}()
		// Modified cluster config: 2 systems, 2 AX nodes, but only 1 IX node to force IX node sharing
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 12, 2, 2, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

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

		userNsrName := "user-ns-ix-swdriver-job-sharing"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1"},
				// Assign both systems so each job gets its own system
				SystemNames:              []string{"system-0", "system-1"},
				ActivationNodeNames:      []string{"ax-0", "ax-1"},
				InferenceDriverNodeNames: []string{"ix-0"}, // Only 1 IX node to force sharing
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus like other inference tests
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		// Create first SWDriver inference job - 1 system, 1 SWDriver
		inferenceJob1 := newInfExecuteJob(userNsrName, "swdriver-job-sharing-1", 1, 0, 1)
		inferenceJob1.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob1))
		defer assertWsjobCleanedUp(t.T(), inferenceJob1)

		// Create cluster details ConfigMap for first job - targets system-0
		createSwDriverConfigMap(t.T(), inferenceJob1, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify first job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)

		// Verify lock is granted with job sharing enabled
		lock1 := assertLockExists(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)
		assertLocked(t.T(), lock1)

		// First job should use the only IX node and get system-0
		assertNodesScheduled(
			t,
			map[string]bool{
				"ix-0": true,
			}, lock1, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-0"}, lock1)

		// Create second SWDriver inference job - different system
		inferenceJob2 := newInfExecuteJob(userNsrName, "swdriver-job-sharing-2", 1, 0, 1)
		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		// Create cluster details ConfigMap for second job - also targets 1 system (will get system-1)
		createSwDriverConfigMap(t.T(), inferenceJob2, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify second job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)

		// Verify lock is granted with job sharing enabled
		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock2)

		// Second job should share the same IX node but get a different system
		assertNodesScheduled(t, map[string]bool{
			"ix-0": true,
		}, lock2, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-1"}, lock2) // Different system!

		// Verify SWDriver pods get created for both jobs
		assertPodCountEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name, rlv1.SWDriverRequestName, 1)
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, rlv1.SWDriverRequestName, 1)

		// Verify both jobs are running concurrently with IX node sharing
		allSwDriverPods := assertPodCountEventually(t.T(), userNsrName, "", rlv1.SWDriverRequestName, 2)
		assert.Equal(t.T(), 2, len(allSwDriverPods.Items))

		// Verify both pods have IX node selectors and are scheduled on the same IX node
		ixNodeUsage := make(map[string]int)

		// Create a mapping from job name to WSJob for domain lookup
		jobMap := map[string]*wsapisv1.WSJob{
			inferenceJob1.Name: inferenceJob1,
			inferenceJob2.Name: inferenceJob2,
		}
		for _, pod := range allSwDriverPods.Items {
			_, exists := pod.Spec.NodeSelector[fmt.Sprintf(RoleLabelKeyF, common.RoleInferenceDriver)]
			assert.True(t.T(), exists, "SWDriver pod should have IX node selector")

			nodeName := pod.Spec.NodeSelector["kubernetes.io/hostname"]
			if strings.HasPrefix(nodeName, "ix-") {
				ixNodeUsage[nodeName]++
			}

			// Minimal assertion: memory limit must be set
			memLimit := pod.Spec.Containers[0].Resources.Limits.Memory()
			t.T().Logf("SWDriver pod %s memory limit: %v", pod.Name, memLimit.String())
			assert.NotNil(t.T(), memLimit, "SWDriver pod should have a memory limit specified")
			assert.True(t.T(), memLimit.Value() > 0, "SWDriver pod should have a non-zero memory limit")

			// Minimal assertion: CPU limit must be set
			cpuLimit := pod.Spec.Containers[0].Resources.Limits.Cpu()
			t.T().Logf("SWDriver pod %s CPU limit: %v", pod.Name, cpuLimit.String())
			assert.NotNil(t.T(), cpuLimit, "SWDriver pod should have a CPU limit specified")
			assert.True(t.T(), cpuLimit.MilliValue() > 0, "SWDriver pod should have a non-zero CPU limit")

			// Extract job name from pod to get the correct WSJob
			jobName := pod.Labels["job-name"]
			correspondingJob, exists := jobMap[jobName]
			if !exists {
				t.T().Fatalf("Could not find WSJob for pod %s with job name %s", pod.Name, jobName)
			}

			// Verify domain assignment (should be domain 3 from the config)
			foundPortEnv := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeCsPortEnvVarName {
					port, err := strconv.Atoi(env.Value)
					assert.NoError(t.T(), err)

					// Get the actual domain from the config map using the correct job
					actualDomain := getDomainFromSwDriverConfigMap(correspondingJob)
					startPort, endPort := getDomainPortRange(actualDomain, 12, 4)

					assert.True(t.T(), port >= startPort && port <= endPort,
						"SWDriver should be assigned port in domain %d (%d-%d), got: %d",
						actualDomain, startPort, endPort, port)
					foundPortEnv = true
				}
			}
			assert.True(t.T(), foundPortEnv, "Should find port environment variable")
		}

		t.T().Logf("IX node usage with job sharing: %+v", ixNodeUsage)

		// Assert that both pods are sharing the single IX node
		assert.Equal(t.T(), 1, len(ixNodeUsage), "Should use exactly 1 IX node")
		assert.Equal(t.T(), 2, ixNodeUsage["ix-0"], "Both SWDriver pods should share ix-0")

		// Verify both jobs continue to run successfully
		assertPodCountEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name, rlv1.SWDriverRequestName, 1)
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, rlv1.SWDriverRequestName, 1)

		// Add IX NIC domain switch assertions for both jobs
		assertIxNicOnDomainSwitch(lock1, rlv1.SWDriverRequestName, 3, "nic1")
		assertIxNicOnDomainSwitch(lock2, rlv1.SWDriverRequestName, 3, "nic1")

	})

	// Resource contention without job sharing - verifies exclusive IX node allocation
	// and proper queueing when job sharing is disabled with limited resources
	t.Run("ix_scheduling_swdriver_exclusive_allocation", func() {
		deleteNodes(t.T())

		// Enable IX scheduling
		wsapisv1.UseIxScheduling = true
		defer func() {
			wsapisv1.UseIxScheduling = false
		}()
		// Modified cluster config: 2 systems, 2 AX nodes, but only 1 IX node
		// Without job sharing, the second job should fail to schedule
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 12, 2, 1, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

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

		userNsrName := "user-ns-ix-swdriver-no-job-sharing"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1"},
				// Assign both systems so each job tries to get its own system
				SystemNames:              []string{"system-0", "system-1"},
				ActivationNodeNames:      []string{"ax-0", "ax-1"},
				InferenceDriverNodeNames: []string{"ix-0"}, // Only 1 IX node
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		// Explicitly ensure job sharing is disabled (should be default, but make it explicit)
		t.updateSessionLabel(userNsrName, wscommon.InferenceJobSharingSessionLabelKey, "false")
		defer func() {
			t.updateSessionLabel(userNsrName, wscommon.InferenceJobSharingSessionLabelKey, "false")
		}()

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus like other inference tests
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		// Create first SWDriver inference job - 1 system, 1 SWDriver
		inferenceJob1 := newInfExecuteJob(userNsrName, "swdriver-no-job-sharing-1", 1, 0, 1)
		inferenceJob1.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob1))
		defer assertWsjobCleanedUp(t.T(), inferenceJob1)

		// Create cluster details ConfigMap for first job
		createSwDriverConfigMap(t.T(), inferenceJob1, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify first job gets scheduled successfully
		assertWsJobScheduledEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)

		// Verify lock is granted with job sharing disabled
		lock1 := assertLockExists(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)
		assertLocked(t.T(), lock1)

		// First job should use the only IX node and get system-0
		assertNodesScheduled(t, map[string]bool{
			"ix-0": true,
		}, lock1, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-0"}, lock1)

		// Verify first job's SWDriver pod is created and running
		assertPodCountEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name, rlv1.SWDriverRequestName, 1)

		// Create second SWDriver inference job - should fail to schedule
		inferenceJob2 := newInfExecuteJob(userNsrName, "swdriver-no-job-sharing-2", 1, 0, 1)
		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		// Create cluster details ConfigMap for second job
		createSwDriverConfigMap(t.T(), inferenceJob2, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify second job exists but cannot get a lock (should remain pending)
		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)

		// The second job should remain pending because:
		// 1. Without job sharing, each job needs exclusive access to IX nodes
		// 2. The single IX node (ix-0) is already allocated to the first job
		// 3. There are no more IX nodes available for the second job
		assertNeverGranted(t.T(), lock2)

		// Verify the second job does not get any IX node grants
		// (since the only IX node is exclusively used by the first job)
		actualIxNodes := map[string]bool{}
		for _, gg := range lock2.Status.GroupResourceGrants {
			if strings.HasPrefix(gg.Name, rlv1.SWDriverRequestName) {
				for _, rg := range gg.ResourceGrants {
					for _, res := range rg.Resources {
						actualIxNodes[res.Name] = true
					}
				}
			}
		}
		assert.Equal(t.T(), 0, len(actualIxNodes), "Second job should not get any IX nodes without job sharing")

		// Verify only the first job has SWDriver pods running
		// Second job should have no SWDriver pods because it cannot schedule
		assertPodCountEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name, rlv1.SWDriverRequestName, 1)

		// Verify that the total number of SWDriver pods in the namespace is still just 1
		// (only from the first job)
		allSwDriverPods := assertPodCountEventually(t.T(), userNsrName, "", rlv1.SWDriverRequestName, 1)
		assert.Equal(t.T(), 1, len(allSwDriverPods.Items), "Should only have 1 SWDriver pod from the first job")

		swDriverPod := allSwDriverPods.Items[0]
		// Minimal assertion: memory limit should NOT be set
		memLimit := swDriverPod.Spec.Containers[0].Resources.Limits.Memory()
		t.T().Logf("SWDriver pod %s memory limit: %v", swDriverPod.Name, memLimit.String())
		assert.True(t.T(), memLimit == nil || memLimit.Value() == 0, "SWDriver pod should NOT have a memory limit specified")
		// Verify the single SWDriver pod belongs to the first job
		assert.Contains(t.T(), swDriverPod.Name, "swdriver-no-job-sharing-1",
			"The only SWDriver pod should belong to the first job")

		assertIxNicOnDomainSwitch(lock1, rlv1.SWDriverRequestName, 3, "nic1")

		// Verify IX node usage - only one node should be used by the first job
		ixNodeUsage := make(map[string]int)
		nodeName := swDriverPod.Spec.NodeSelector["kubernetes.io/hostname"]
		if strings.HasPrefix(nodeName, "ix-") {
			ixNodeUsage[nodeName]++
		}

		t.T().Logf("IX node usage without job sharing: %+v", ixNodeUsage)
		assert.Equal(t.T(), 1, len(ixNodeUsage), "Should use exactly 1 IX node")
		assert.Equal(t.T(), 1, ixNodeUsage["ix-0"], "Only the first job should use ix-0")

		// Clean up first job and verify second job can then schedule
		assertWsjobCleanedUp(t.T(), inferenceJob1)

		// After first job is cleaned up, the second job should be able to get the lock
		require.Eventually(t.T(), func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(lock2), lock2)
			if err != nil {
				return false
			}
			return lock2.Status.State == rlv1.LockGranted
		}, eventuallyTimeout, eventuallyTick)

		// Now the second job should be able to use the IX node
		assertLocked(t.T(), lock2)
		assertNodesScheduled(t, map[string]bool{
			"ix-0": true,
		}, lock2, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-0"}, lock2) // Gets the other system

		// Verify second job's SWDriver pod is now created
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, rlv1.SWDriverRequestName, 1)
		assertIxNicOnDomainSwitch(lock2, rlv1.SWDriverRequestName, 3, "nic1")

	})

	// Pod limit enforcement on IX nodes - validates IX node capacity limits
	// prevent over-scheduling when multiple jobs attempt to share resources
	t.Run("ix_scheduling_swdriver_pod_limit_enforcement", func() {
		deleteNodes(t.T())

		// Enable IX scheduling
		wsapisv1.UseIxScheduling = true
		resource_config.MaxSwDriverPodsPerIxNode = 2
		defer func() {
			wsapisv1.UseIxScheduling = false
			resource_config.MaxSwDriverPodsPerIxNode = 1
		}()
		// Modified cluster config: 2 systems, 2 AX nodes, but only 1 IX node to force IX node sharing
		v2Cfg := wscommon.NewTestClusterConfigSchema(2, 1, 12, 2, 1, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)

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

		userNsrName := "user-ns-ix-swdriver-pod-limit"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1"},
				// Need 3 systems for 3 jobs, but only 1 IX node
				SystemNames:              []string{"system-0", "system-1"},
				ActivationNodeNames:      []string{"ax-0", "ax-1"},
				InferenceDriverNodeNames: []string{"ix-0"}, // Only 1 IX node with 2-pod limit
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus like other inference tests
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		// Create first SWDriver inference job
		inferenceJob1 := newInfExecuteJob(userNsrName, "swdriver-pod-limit-1", 1, 0, 1)
		inferenceJob1.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob1))
		defer assertWsjobCleanedUp(t.T(), inferenceJob1)
		createSwDriverConfigMap(t.T(), inferenceJob1, "testdata/1_sys_1_swdriver_domain_3.json")

		// Create second SWDriver inference job
		inferenceJob2 := newInfExecuteJob(userNsrName, "swdriver-pod-limit-2", 1, 0, 1)
		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)
		createSwDriverConfigMap(t.T(), inferenceJob2, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify first two jobs get scheduled (should consume the 2-pod limit)
		assertWsJobScheduledEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)
		assertWsJobScheduledEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)

		lock1 := assertLockExists(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)
		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock1)
		assertLocked(t.T(), lock2)

		// Verify both jobs are using the same IX node
		assertNodesScheduled(t, map[string]bool{"ix-0": true}, lock1, rlv1.SWDriverRequestName)
		assertNodesScheduled(t, map[string]bool{"ix-0": true}, lock2, rlv1.SWDriverRequestName)

		// Verify 2 SWDriver pods are running (at the limit)
		assertPodCountEventually(t.T(), userNsrName, "", rlv1.SWDriverRequestName, 2)

		// Create third SWDriver inference job - should fail due to 2-pod limit on IX node
		inferenceJob3 := newInfExecuteJob(userNsrName, "swdriver-pod-limit-3", 1, 0, 1)
		inferenceJob3.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob3))
		defer assertWsjobCleanedUp(t.T(), inferenceJob3)
		createSwDriverConfigMap(t.T(), inferenceJob3, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify third job gets a lock but remains pending (cannot schedule due to pod limit)
		lock3 := assertLockExists(t.T(), inferenceJob3.Namespace, inferenceJob3.Name)
		assertNeverGranted(t.T(), lock3)

		// Verify no additional SWDriver pods are created (still only 2)
		time.Sleep(5 * time.Second) // Give it time to potentially create a pod
		allSwDriverPods := assertPodCountEventually(t.T(), userNsrName, "", rlv1.SWDriverRequestName, 2)
		assert.Equal(t.T(), 2, len(allSwDriverPods.Items), "Should still only have 2 SWDriver pods due to IX node limit")

		// Verify the third job has no IX node grants (no resources available)
		actualIxNodes := map[string]bool{}
		for _, gg := range lock3.Status.GroupResourceGrants {
			if strings.HasPrefix(gg.Name, rlv1.SWDriverRequestName) {
				for _, rg := range gg.ResourceGrants {
					for _, res := range rg.Resources {
						actualIxNodes[res.Name] = true
					}
				}
			}
		}
		assert.Equal(t.T(), 0, len(actualIxNodes), "Third job should not get any IX nodes due to 2-pod limit")

		t.T().Logf("Successfully verified that IX node enforces 2-pod limit across multiple jobs")
	})

	t.Run("ix_scheduling_swdriver_job_sharing_packing", func() {

		deleteNodes(t.T())

		// Enable IX scheduling
		wsapisv1.UseIxScheduling = true
		resource_config.MaxSwDriverPodsPerIxNode = 2
		defer func() {
			wsapisv1.UseIxScheduling = false
			resource_config.MaxSwDriverPodsPerIxNode = 1
		}()
		// Modified cluster config: 2 systems, 2 AX nodes and 2 IX nodes
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 1, 12, 4, 2, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 12)

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

		userNsrName := "user-ns-ix-swdriver-job-sharing-packing"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1"},
				// Assign both systems so each job gets its own system
				SystemNames:              []string{"system-0", "system-1", "system-2", "system-3"},
				ActivationNodeNames:      []string{"ax-0", "ax-1", "ax-2", "ax-3"},
				InferenceDriverNodeNames: []string{"ix-0", "ix-1"}, // 2 IX nodes to test breadth first packing
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus like other inference tests
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		// Create first SWDriver inference job - 1 system, 1 SWDriver
		inferenceJob1 := newInfExecuteJob(userNsrName, "swdriver-job-sharing-1", 1, 0, 1)
		inferenceJob1.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob1))
		defer assertWsjobCleanedUp(t.T(), inferenceJob1)

		// Create cluster details ConfigMap for first job - targets system-0
		createSwDriverConfigMap(t.T(), inferenceJob1, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify first job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)

		// Verify lock is granted with job sharing enabled
		lock1 := assertLockExists(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)
		assertLocked(t.T(), lock1)

		// First job should use the only IX node and get system-0
		assertNodesScheduled(
			t,
			map[string]bool{
				"ix-0": true,
			}, lock1, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-0"}, lock1)

		// Create second SWDriver inference job - different system
		inferenceJob2 := newInfExecuteJob(userNsrName, "swdriver-job-sharing-2", 1, 0, 1)
		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		// Create cluster details ConfigMap for second job - also targets 1 system (will get system-1)
		createSwDriverConfigMap(t.T(), inferenceJob2, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify second job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)

		// Verify lock is granted with job sharing enabled
		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock2)

		// Second job should share the same IX node but get a different system
		assertNodesScheduled(t, map[string]bool{
			"ix-1": true,
		}, lock2, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-1"}, lock2) // Different system!

		// Verify SWDriver pods get created for both jobs
		assertPodCountEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name, rlv1.SWDriverRequestName, 1)
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, rlv1.SWDriverRequestName, 1)

		// Verify both jobs are running concurrently with IX node sharing
		allSwDriverPods := assertPodCountEventually(t.T(), userNsrName, "", rlv1.SWDriverRequestName, 2)
		assert.Equal(t.T(), 2, len(allSwDriverPods.Items))

		// Verify both pods have IX node selectors and are scheduled on the same IX node
		ixNodeUsage := make(map[string]int)

		// Create a mapping from job name to WSJob for domain lookup
		jobMap := map[string]*wsapisv1.WSJob{
			inferenceJob1.Name: inferenceJob1,
			inferenceJob2.Name: inferenceJob2,
		}
		for _, pod := range allSwDriverPods.Items {
			_, exists := pod.Spec.NodeSelector[fmt.Sprintf(RoleLabelKeyF, common.RoleInferenceDriver)]
			assert.True(t.T(), exists, "SWDriver pod should have IX node selector")

			nodeName := pod.Spec.NodeSelector["kubernetes.io/hostname"]
			if strings.HasPrefix(nodeName, "ix-") {
				ixNodeUsage[nodeName]++
			}

			// Extract job name from pod to get the correct WSJob
			jobName := pod.Labels["job-name"]
			correspondingJob, exists := jobMap[jobName]
			if !exists {
				t.T().Fatalf("Could not find WSJob for pod %s with job name %s", pod.Name, jobName)
			}

			// Verify domain assignment (should be domain 3 from the config)
			foundPortEnv := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeCsPortEnvVarName {
					port, err := strconv.Atoi(env.Value)
					assert.NoError(t.T(), err)

					// Get the actual domain from the config map using the correct job
					actualDomain := getDomainFromSwDriverConfigMap(correspondingJob)
					startPort, endPort := getDomainPortRange(actualDomain, 12, 4)

					assert.True(t.T(), port >= startPort && port <= endPort,
						"SWDriver should be assigned port in domain %d (%d-%d), got: %d",
						actualDomain, startPort, endPort, port)
					foundPortEnv = true
				}
			}
			assert.True(t.T(), foundPortEnv, "Should find port environment variable")
		}

		t.T().Logf("IX node usage with job sharing: %+v", ixNodeUsage)

		// Assert that both pods are sharing the single IX node
		assert.Equal(t.T(), 2, len(ixNodeUsage), "Should be using 2 IX nodes")

		// Verify both jobs continue to run successfully
		assertPodCountEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name, rlv1.SWDriverRequestName, 1)
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, rlv1.SWDriverRequestName, 1)

		// Create third SWDriver inference job - 1 system, 1 SWDriver
		inferenceJob3 := newInfExecuteJob(userNsrName, "swdriver-job-sharing-3", 1, 0, 1)
		inferenceJob3.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob3))
		defer assertWsjobCleanedUp(t.T(), inferenceJob3)

		// Create cluster details ConfigMap for first job - targets system-0
		createSwDriverConfigMap(t.T(), inferenceJob3, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify first job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob3.Namespace, inferenceJob3.Name)

		// Verify lock is granted with job sharing enabled
		lock3 := assertLockExists(t.T(), inferenceJob3.Namespace, inferenceJob3.Name)
		assertLocked(t.T(), lock3)

		// First job should use the only IX node and get system-0
		assertNodesScheduled(
			t,
			map[string]bool{
				"ix-0": true,
			}, lock3, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-2"}, lock3)

		// Create third SWDriver inference job - 1 system, 1 SWDriver
		inferenceJob4 := newInfExecuteJob(userNsrName, "swdriver-job-sharing-4", 1, 0, 1)
		inferenceJob4.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob4))
		defer assertWsjobCleanedUp(t.T(), inferenceJob4)

		// Create cluster details ConfigMap for first job - targets system-0
		createSwDriverConfigMap(t.T(), inferenceJob4, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify first job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob4.Namespace, inferenceJob4.Name)

		// Verify lock is granted with job sharing enabled
		lock4 := assertLockExists(t.T(), inferenceJob4.Namespace, inferenceJob4.Name)
		assertLocked(t.T(), lock3)

		// First job should use the only IX node and get system-0
		assertNodesScheduled(
			t,
			map[string]bool{
				"ix-1": true,
			}, lock4, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-3"}, lock4)

	})

	// 12 switches per stamp, 4 IX
	// 1 job with 1 SWD on ix-0
	// ix 1 is unhealthy, so it should not be used
	// ix-2 is unhealthy, so it should not be used
	// 1 job with 1 SWD on ix-3
	t.Run("ix_scheduling_swdriver_job_sharing_pack_unhealthy", func() {

		deleteNodes(t.T())

		// Enable IX scheduling
		wsapisv1.UseIxScheduling = true
		resource_config.MaxSwDriverPodsPerIxNode = 2
		defer func() {
			wsapisv1.UseIxScheduling = false
			resource_config.MaxSwDriverPodsPerIxNode = 1
		}()
		// Modified cluster config: 2 systems, 2 AX nodes and 4 IX nodes
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 1, 12, 4, 4, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 12)

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

		userNsrName := "user-ns-ix-swdriver-job-sharing-packing-unhealthy"
		userNsr := t.createNsIfNotExists(userNsrName)
		defer func() {
			t.unassignEverythingFromNS(userNsrName)
		}()

		assert.NoError(t.T(), k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(userNsr), userNsr))
		userNsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:          namespacev1.DebugSetMode,
				ManagementNodeNames: []string{"0-coord-0"},
				NodegroupNames:      []string{"group0", "group1"},
				// Assign both systems so each job gets its own system
				SystemNames:              []string{"system-0", "system-1", "system-2", "system-3"},
				ActivationNodeNames:      []string{"ax-0", "ax-1", "ax-2", "ax-3"},
				InferenceDriverNodeNames: []string{"ix-0", "ix-1", "ix-2", "ix-3"}, // 4 IX nodes to test breadth first packing
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, userNsr))
		t.ensureNsUpdateSuccess(userNsr)

		assertClusterServerIngressSetup(t.T(), userNsrName)
		defer assertClusterServerIngressCleanup(t.T(), userNsrName)

		// Enable Multus like other inference tests
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		// Mark ax-0 and ax-1 unhealthy
		require.NoError(t.T(), patchNode(t, "ix-1", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))
		require.NoError(t.T(), patchNode(t, "ix-2", []corev1.NodeCondition{{
			Type:   wscommon.ClusterMgmtConditionPrefix + wscommon.NodeSwitchPortAlertType,
			Status: corev1.ConditionTrue,
		}}))

		// Create first SWDriver inference job - 1 system, 1 SWDriver
		inferenceJob1 := newInfExecuteJob(userNsrName, "swdriver-job-sharing-1", 1, 0, 1)
		inferenceJob1.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob1))
		defer assertWsjobCleanedUp(t.T(), inferenceJob1)

		// Create cluster details ConfigMap for first job - targets system-0
		createSwDriverConfigMap(t.T(), inferenceJob1, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify first job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)

		// Verify lock is granted with job sharing enabled
		lock1 := assertLockExists(t.T(), inferenceJob1.Namespace, inferenceJob1.Name)
		assertLocked(t.T(), lock1)

		// First job should use the only IX node and get system-0
		assertNodesScheduled(
			t,
			map[string]bool{
				"ix-0": true,
			}, lock1, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-0"}, lock1)

		// Create second SWDriver inference job - different system
		inferenceJob2 := newInfExecuteJob(userNsrName, "swdriver-job-sharing-2", 1, 0, 1)
		inferenceJob2.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob2))
		defer assertWsjobCleanedUp(t.T(), inferenceJob2)

		// Create cluster details ConfigMap for second job - also targets 1 system (will get system-1)
		createSwDriverConfigMap(t.T(), inferenceJob2, "testdata/1_sys_1_swdriver_domain_3.json")

		// Verify second job gets scheduled
		assertWsJobScheduledEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)

		// Verify lock is granted with job sharing enabled
		lock2 := assertLockExists(t.T(), inferenceJob2.Namespace, inferenceJob2.Name)
		assertLocked(t.T(), lock2)

		// Second job should share the same IX node but get a different system
		assertNodesScheduled(t, map[string]bool{
			"ix-3": true,
		}, lock2, rlv1.SWDriverRequestName)
		assertSystemsOrdered(t, []string{"system-1"}, lock2) // Different system!

		// Verify SWDriver pods get created for both jobs
		assertPodCountEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name, rlv1.SWDriverRequestName, 1)
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, rlv1.SWDriverRequestName, 1)

		// Verify both jobs are running concurrently with IX node sharing
		allSwDriverPods := assertPodCountEventually(t.T(), userNsrName, "", rlv1.SWDriverRequestName, 2)
		assert.Equal(t.T(), 2, len(allSwDriverPods.Items))

		// Verify both pods have IX node selectors and are scheduled on the same IX node
		ixNodeUsage := make(map[string]int)

		// Create a mapping from job name to WSJob for domain lookup
		jobMap := map[string]*wsapisv1.WSJob{
			inferenceJob1.Name: inferenceJob1,
			inferenceJob2.Name: inferenceJob2,
		}
		portUsage := make(map[int]int) // Track port usage
		for _, pod := range allSwDriverPods.Items {
			_, exists := pod.Spec.NodeSelector[fmt.Sprintf(RoleLabelKeyF, common.RoleInferenceDriver)]
			assert.True(t.T(), exists, "SWDriver pod should have IX node selector")

			nodeName := pod.Spec.NodeSelector["kubernetes.io/hostname"]
			if strings.HasPrefix(nodeName, "ix-") {
				ixNodeUsage[nodeName]++
			}

			// Extract job name from pod to get the correct WSJob
			jobName := pod.Labels["job-name"]
			correspondingJob, exists := jobMap[jobName]
			if !exists {
				t.T().Fatalf("Could not find WSJob for pod %s with job name %s", pod.Name, jobName)
			}

			// Verify domain assignment (should be domain 3 from the config)
			foundPortEnv := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeCsPortEnvVarName {
					port, err := strconv.Atoi(env.Value)
					assert.NoError(t.T(), err)

					portUsage[port]++

					// Get the actual domain from the config map using the correct job
					actualDomain := getDomainFromSwDriverConfigMap(correspondingJob)
					startPort, endPort := getDomainPortRange(actualDomain, 12, 4)

					assert.True(t.T(), port >= startPort && port <= endPort,
						"SWDriver should be assigned port in domain %d (%d-%d), got: %d",
						actualDomain, startPort, endPort, port)
					foundPortEnv = true
				}
			}
			assert.True(t.T(), foundPortEnv, "Should find port environment variable")
		}

		t.T().Logf("IX node usage with job sharing: %+v", ixNodeUsage)
		t.T().Logf("CS port usage: %+v", portUsage)

		// Assert that both pods are sharing the single IX node
		assert.Equal(t.T(), 2, len(ixNodeUsage), "Should be using 2 IX nodes")

		// Verify both jobs continue to run successfully
		assertPodCountEventually(t.T(), inferenceJob1.Namespace, inferenceJob1.Name, rlv1.SWDriverRequestName, 1)
		assertPodCountEventually(t.T(), inferenceJob2.Namespace, inferenceJob2.Name, rlv1.SWDriverRequestName, 1)

	})

}

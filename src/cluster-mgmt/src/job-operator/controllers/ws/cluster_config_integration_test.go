//go:build integration && cluster_cfg

package ws

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/controllers/cluster"
)

type ClusterConfigTestSuite struct {
	TestSuite // Embed the base suite
}

// BeforeTest prepares the test environment by ensuring a clean node state before each test.
func (t *ClusterConfigTestSuite) BeforeTest(suiteName, testName string) {
	logrus.Infof("ClusterConfig BeforeTest suite/test %s/%s", suiteName, testName)
	// This is necessary because TestSuite is embedded and the base suite's BeforeTest won't run automatically
	t.TestSuite.BeforeTest(suiteName, testName)

}

// AftterTest cleans up the test environment after each test.
func (t *ClusterConfigTestSuite) AfterTest(suiteName, testName string) {
	logrus.Infof("ClusterConfig AfterTest suite/test %s/%s", suiteName, testName)
	// This is necessary because TestSuite is embedded and the base suite's AfterTest won't run automatically
	t.TestSuite.AfterTest(suiteName, testName)
	// Ensure clean node state
	deleteNodes(t.T())
	// Wait for deletion to complete
	require.Eventually(t.T(), func() bool {
		nodes := &corev1.NodeList{}
		k8sClient.List(context.Background(), nodes)
		return len(nodes.Items) == 0
	}, eventuallyTimeout, eventuallyTick)
}

func TestClusterConfigSuite(t *testing.T) {
	suite.Run(t, new(ClusterConfigTestSuite))
}

func (t *ClusterConfigTestSuite) TestClusterConfigIntegration() {
	ctx := context.Background()
	t.Run("test_coordinator_assigns_to_worker_node_in_dev_mode", func() {
		wscommon.IsDev = true
		defer func() {
			wscommon.IsDev = false
		}()
		job1 := newWsCompileJob(wscommon.Namespace, "dev-mode-crd", 1)
		job1.Annotations[wsapisv1.SchedulerHintAllowSys] = t.JobReconciler.Cfg.Systems[0].Name
		require.NoError(t.T(), k8sClient.Create(ctx, job1))
		defer assertWsjobCleanedUp(t.T(), job1)
		crd := assertPodCountEventually(t.T(), job1.Namespace, job1.Name, "", 1).Items[0]
		// crd will be assigned to worker node
		assert.Contains(t.T(), crd.Spec.NodeSelector[fmt.Sprintf(RoleLabelKeyF, wscommon.RoleWorker)], "")
		wscommon.IsDev = false
	})

	t.Run("test_single_coordinator_mode_assigns_to_mgmt_node", func() {
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()
		job := newWsCompileJob(wscommon.Namespace, "default-mode-crd", 1)
		job.Annotations[wsapisv1.SchedulerHintAllowSys] = t.JobReconciler.Cfg.Systems[0].Name
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)
		crd := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 1).Items[0]
		// crd will be assigned to coord node irrespective of the node group assignment by scheduler hint
		coordNodes := t.JobReconciler.Cfg.GetCoordNodes()
		require.Len(t.T(), coordNodes, 1)
		assert.Contains(t.T(), crd.Spec.NodeSelector[corev1.LabelHostname], coordNodes[0].Name)
		t.Assert().Equal(crd.Annotations[wscommon.NetworkAttachmentKey], wscommon.DataNetAttachDef)
		crd.Status = corev1.PodStatus{
			HostIP: coordNodes[0].HostIP,
		}
		require.NoError(t.T(), k8sClient.Status().Update(ctx, &crd))
	})

	t.Run("test_multi-coordinator_load_balance", func() {
		// create config to k8s to trigger reload with multi crd mode
		wscommon.EnableMultiCoordinator = true
		//t.NoError(t.CfgMgr.Initialize(t.DefaultCfgSchema))
		updateClusterConfig(t.T(), t.CfgMgr, t.DefaultCfgSchema)
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			wscommon.EnableMultiCoordinator = false
			//t.NoError(t.CfgMgr.Initialize(t.DefaultCfgSchema))
			updateClusterConfig(t.T(), t.CfgMgr, t.DefaultCfgSchema)
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		job1 := newWsCompileJob(wscommon.Namespace, "multi-crd-train-1", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job1))
		crd1 := assertPodCountEventually(t.T(), job1.Namespace, job1.Name, "", 1).Items[0]
		defer assertWsjobCleanedUp(t.T(), job1)

		job2 := newWsCompileJob(wscommon.Namespace, "multi-crd-train-2", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job2))
		crd2 := assertPodCountEventually(t.T(), job2.Namespace, job2.Name, "", 1).Items[0]
		defer assertWsjobCleanedUp(t.T(), job2)

		no1 := crd1.Spec.NodeSelector[corev1.LabelHostname]
		no2 := crd2.Spec.NodeSelector[corev1.LabelHostname]
		t.Assert().Equal(crd1.Annotations[wscommon.NetworkAttachmentKey], wscommon.DataNetAttachDef)
		t.Assert().Equal(crd2.Annotations[wscommon.NetworkAttachmentKey], wscommon.DataNetAttachDef)

		coordNodes := t.JobReconciler.Cfg.GetCoordNodes()
		if no1 == coordNodes[0].Name {
			assert.NotEqual(t.T(), coordNodes[0].Name, no2)
		} else if no2 == coordNodes[0].Name {
			assert.NotEqual(t.T(), coordNodes[0].Name, no1)
		}
	})

	t.Run("node_available_resources_metrics", func() {
		targetNode := t.JobReconciler.Cfg.Nodes[0]
		node0cpu := testutil.ToFloat64(wscommon.NodeAvailableResources.WithLabelValues(targetNode.Name, "cpu", "core"))
		node0mem := testutil.ToFloat64(wscommon.NodeAvailableResources.WithLabelValues(targetNode.Name, "memory", "bytes"))
		node, _ := t.JobReconciler.Cfg.Resources.Get("node/" + targetNode.Name)
		assert.Equal(t.T(), float64(node.Subresources()[wsresource.CPUSubresource])/1000.0, node0cpu)
		assert.Equal(t.T(), float64(node.Subresources()[wsresource.MemSubresource]<<20), node0mem)
	})

	t.Run("test_overhead_change_watched", func() {
		cfg := wscommon.NewTestClusterConfigSchema(1, 1, 0, 0, 0, 0, 0)
		updateClusterConfig(t.T(), t.CfgMgr, cfg)
		wscommon.DisableMultus = false
		t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		defer func() {
			updateClusterConfig(t.T(), t.CfgMgr, t.DefaultCfgSchema)
			wscommon.DisableMultus = true
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
		}()

		job1 := newWsCompileJob(wscommon.Namespace, "compile-job-1", 0)
		job1.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].
			Template.Spec.Containers[0].Resources.Limits[wsresource.MemSubresource] =
			*apiresource.NewQuantity((wscommon.DefaultNodeMem/2)<<20, apiresource.BinarySI)
		require.NoError(t.T(), k8sClient.Create(ctx, job1))
		defer assertWsjobCleanedUp(t.T(), job1)
		lock1 := assertLockExists(t.T(), job1.Namespace, job1.Name)
		assert.True(t.T(), lock1.Status.State == rlv1.LockGranted)

		overheadPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "overhead-pod",
				Namespace: wscommon.Namespace,
			},
			Spec: corev1.PodSpec{
				NodeName: "0-coord-0",
				Containers: []corev1.Container{
					{
						Name:  "overhead",
						Image: "overhead",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: *apiresource.NewQuantity((wscommon.DefaultNodeMem/4)<<20, apiresource.BinarySI),
							},
						},
					},
				},
			},
		}
		require.NoError(t.T(), k8sClient.Create(ctx, overheadPod))
		overheadPod.Status.Phase = corev1.PodRunning
		require.NoError(t.T(), k8sClient.Status().Update(ctx, overheadPod))
		time.Sleep(5 * time.Second)

		// queueing due to capacity can't fit two jobs together
		job2 := newWsCompileJob(wscommon.Namespace, "compile-job-2", 0)
		job2.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].
			Template.Spec.Containers[0].Resources.Limits[wsresource.MemSubresource] =
			*apiresource.NewQuantity((wscommon.DefaultNodeMem/2)<<20, apiresource.BinarySI)
		require.NoError(t.T(), k8sClient.Create(ctx, job2))
		defer assertWsjobCleanedUp(t.T(), job2)
		lock2 := assertLockExists(t.T(), job2.Namespace, job2.Name)
		assertPending(t.T(), lock2)

		// expect job scheduled after overhead removed
		overheadPod.Status.Phase = corev1.PodSucceeded
		require.NoError(t.T(), k8sClient.Status().Update(ctx, overheadPod))
		assertWsJobScheduledEventually(t.T(), job2.Namespace, job2.Name)
	})
}

func (t *ClusterConfigTestSuite) TestV2Cluster_BrAffinity() {
	wsapisv1.IsV2Network = true
	defer func() {
		wsapisv1.IsV2Network = false
	}()
	deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
	v2Cfg := wscommon.NewTestClusterConfigSchema(systemCount, 1, brCount, systemCount, 0, workersPerGroup, memoryXPerGroup).
		SplitNStamps(1, 4)
	// add one missing port affinity
	for _, v2 := range v2Cfg.V2Groups {
		for k := range v2.Properties {
			if k == "system-0-0" {
				delete(v2.Properties, k)
				break
			}
		}
	}
	t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
	t.NoError(t.CfgMgr.Initialize(v2Cfg))

	jobName := "v2-br-affinity"
	wsjob := newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
	require.NoError(t.T(), k8sClient.Create(t.Ctx, wsjob))
	lock := assertLockExists(t.T(), wsjob.Namespace, wsjob.Name)
	assertLocked(t.T(), lock)
	assertPodCountEventually(t.T(), wsjob.Namespace, jobName,
		rlv1.BrRequestName, 36)
	brGrant, _ := t.JobReconciler.JobResourceCache.Get(wsjob.NamespacedName())
	for i := 0; i < 36; i++ {
		grantNics := brGrant.(jobResources).getAssignedNicsForReplica(wsapisv1.WSReplicaTypeBroadcastReduce, i)
		if i < 24 {
			assert.Equal(t.T(), len(grantNics), 5)
		} else {
			assert.Equal(t.T(), len(grantNics), 3)
		}
		nicBW := map[string]int{}
		for _, nic := range grantNics {
			nicBW[nic]++
			assert.Contains(t.T(), []string{"nic0", "nic1", "nic2", "nic3", "nic4", "nic5"}, nic)
		}
		for _, nic := range grantNics {
			assert.True(t.T(), nicBW[grantNics[0]] <= nicBW[nic], grantNics)
		}
	}

	// Check a sample exported info metrics.
	grantN := brGrant.(jobResources).replicaToNodeMap[string(wsapisv1.WSReplicaTypeBroadcastReduce)][20]
	assert.True(t.T(), wscommon.JobHasNicForTesting(wsjob.Namespace, wsjob.Name,
		wsjob.Name+"-broadcastreduce-20",
		grantN, "nic1", t.JobReconciler.Cfg.NodeMap[grantN].NICs[1].Addr))

	for _, group := range lock.Status.GroupResourceGrants {
		if group.Name == rlv1.BrRequestName {
			require.Equal(t.T(), wsapisv1.BrTreePolicySpine, group.Annotations[rlv1.BrLogicalTreePolicy])
		}
	}
	require.NoError(t.T(), k8sClient.Get(t.Ctx,
		client.ObjectKey{Name: jobName, Namespace: wsjob.Namespace}, wsjob))
	msg, ok := wsjob.Annotations[wscommon.BrV2SpineLoad]
	assert.True(t.T(), ok)
	assert.Contains(t.T(), msg, "1 times from BR to systems")
	assert.Contains(t.T(), msg, "times from BR to BR")
	assert.NotContains(t.T(), msg, "0 times from BR to BR")
	assertWsjobCleanedUp(t.T(), wsjob)
}

func (t *ClusterConfigTestSuite) TestV2Cluster_Pickup_SX_nodes() {
	wsapisv1.IsV2Network = true
	defer func() {
		wsapisv1.IsV2Network = false
	}()
	deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
	// 12sx for first job + 6sx for second job + 1 sx version mismatch
	v2Cfg := wscommon.NewTestClusterConfigSchema(systemCount, 1, 19, systemCount, 0, workersPerGroup, memoryXPerGroup).
		SplitNStamps(1, 6)
	t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
	t.NoError(t.CfgMgr.Initialize(v2Cfg))

	jobName := "v2-pickup-sx-pre-job"
	wsjob := newWsExecuteJob(wscommon.Namespace, jobName, 2)
	defer assertWsjobCleanedUp(t.T(), wsjob)
	wsjob.Annotations["cerebras/prefer-idle-sx-node"] = "true"
	require.NoError(t.T(), k8sClient.Create(t.Ctx, wsjob))
	lock := assertLockExists(t.T(), wsjob.Namespace, wsjob.Name)
	assert.True(t.T(), lock.Status.State == rlv1.LockGranted)
	grantNodes := map[string]bool{}
	for _, group := range lock.Status.GroupResourceGrants {
		if group.Name == rlv1.BrRequestName {
			for _, n := range group.ResourceGrants {
				grantNodes[n.Resources[0].Name] = true
			}
		}
	}
	// no sharing expected due to prefer idle anno
	assert.Equal(t.T(), 12, len(grantNodes), grantNodes)

	// update one node version to be different
	filterOutNode := ""
	for _, n := range t.CfgMgr.Cfg.Nodes {
		if n.Role == wscommon.RoleBroadcastReduce {
			if grantNodes[n.Name] {
				continue
			}
			node := &corev1.Node{}
			require.NoError(t.T(), k8sClient.Get(context.TODO(), client.ObjectKey{Name: n.Name}, node))
			node.Labels[wsapisv1.PlatformVersionKey] = "v1"
			require.NoError(t.T(), k8sClient.Update(context.TODO(), node))
			require.Eventually(t.T(), func() bool {
				_, ok := t.ResourceController.Cfg.NodeMap[n.Name].Properties[wsresource.PlatformVersionResKey("v1")]
				return ok
			}, eventuallyTimeout, eventuallyTick)
			filterOutNode = n.Name
			break
		}
	}
	jobName = "v2-pickup-sx"
	wsjob1 := newWsExecuteJob(wscommon.Namespace, jobName, 2)
	// avoid v1 version
	wsjob1.Annotations["cerebras/anti-affinity-platform-version-v1"] = ""
	wsjob1.Annotations["cerebras/job-allocate-affinity-wsjob-name"] = "v2-pickup-sx"
	wsjob1.Annotations["cerebras/prefer-idle-sx-node"] = "true"
	require.NoError(t.T(), k8sClient.Create(t.Ctx, wsjob1))
	lock = assertLockExists(t.T(), wsjob1.Namespace, wsjob1.Name)
	assert.True(t.T(), lock.Status.State == rlv1.LockGranted)
	grantNodes = map[string]bool{}
	for _, group := range lock.Status.GroupResourceGrants {
		if group.Name == rlv1.BrRequestName {
			for _, n := range group.ResourceGrants {
				grantNodes[n.Resources[0].Name] = true
			}
		}
	}
	// sharing preferred due to job level affinity
	assert.Equal(t.T(), 6, len(grantNodes), grantNodes)
	// filter out node as anti-affinity
	assert.False(t.T(), grantNodes[filterOutNode], "filter", filterOutNode, "nodes", grantNodes)
	assertWsjobCleanedUp(t.T(), wsjob1)
}

func (t *ClusterConfigTestSuite) TestNonStandardCluster_SingleNode() {
	deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
	singleNodeCfg := wscommon.NewClusterConfigBuilder("test-single").
		WithAnyNode().
		WithGroup(0, 0).
		BuildClusterSchema()
	t.NoError(wscommon.CreateK8sNodes(singleNodeCfg, t.JobReconciler.Client))
	t.NoError(t.CfgMgr.Initialize(singleNodeCfg))

	t.Run("test_single_node_cluster_schedules_train_job", func() {
		// create a single node train job
		job := newWsExecuteJob(wscommon.Namespace, "single-node-job", 1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job))
		assertPodCountEventually(t.T(), job.Namespace, job.Name, "", countNonBrPods(job))
		assert.Equal(t.T(), 1, len(t.JobReconciler.Cfg.Nodes))
		assertWsjobCleanedUp(t.T(), job)
	})
}

func (t *ClusterConfigTestSuite) TestNonStandardCluster_TwoNode() {
	deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
	twoNodeCfg := wscommon.NewClusterConfigBuilder("test-double").
		WithCoordNode().
		WithBR(1).
		WithGroup(0, 0).
		BuildClusterSchema()
	t.NoError(wscommon.CreateK8sNodes(twoNodeCfg, t.JobReconciler.Client))
	t.NoError(t.CfgMgr.Initialize(twoNodeCfg))

	t.Run("test_not_exist_k8s_node_filtered_from_config", func() {
		node := wscommon.CreateK8sNode(twoNodeCfg.Nodes[1], nil, nil)
		require.NoError(t.T(), k8sClient.Delete(context.Background(), node))
		updateClusterConfig(t.T(), t.CfgMgr, twoNodeCfg)
		assert.Len(t.T(), t.CfgMgr.Cfg.Nodes, 1)
	})
}

func (t *ClusterConfigTestSuite) TestNonStandardCluster_Depop() {
	// create a depop cluster
	deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
	depopClusterCfg := wscommon.NewClusterConfigBuilder("depop").
		WithCoordNode().
		WithGroup(wsapisv1.MinMemxPerPopNodegroup, 2).
		WithGroup(wsapisv1.MinMemxPerPopNodegroup*2, 2).
		// depop
		WithGroup(1, 2).
		// depop
		WithGroup(wsapisv1.MinMemxPerPopNodegroup-1, 2).
		WithBR(24).
		BuildClusterSchema()
	t.NoError(wscommon.CreateK8sNodes(depopClusterCfg, k8sClient))
	t.NoError(t.CfgMgr.Initialize(depopClusterCfg))

	// Wait for resources to actually be assigned before creating jobs
	assertNSRReady(t.T(), t.Ctx, wscommon.Namespace, 4) // 4 groups in depop cluster

	t.Run("scheduling_sdk_job_should_succeed", func() {
		// Now jobs should consistently succeed
		job0 := newSdkExecuteJob("sdk-train-0")
		defer assertWsjobCleanedUp(t.T(), job0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))

		assertPodCountEventually(t.T(), job0.Namespace, job0.Name, "", 1)
		assertPodCountEventually(t.T(), job0.Namespace, job0.Name, rlv1.WorkerRequestName, 1)

		lock := assertLockExists(t.T(), job0.Namespace, job0.Name)
		assert.True(t.T(), lock.Status.State == rlv1.LockGranted)

		assert.Equal(t.T(), 1, len(lock.Status.ResourceGrants))
		assert.Equal(t.T(), 1, len(lock.Status.GroupResourceGrants))
		// Should pick the smallest nodegroup, which is the smallest depop group in this case
		assert.Equal(t.T(), "group2", lock.Status.GroupResourceGrants[0].GroupingValue)
		assertNgCountsAcquired(t.T(), job0, 0, 1)

		job1 := newSdkExecuteJob("sdk-train-1")
		defer assertWsjobCleanedUp(t.T(), job1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		assertPodCountEventually(t.T(), job1.Namespace, job1.Name, rlv1.WorkerRequestName, 1)
		lock = assertLockExists(t.T(), job1.Namespace, job1.Name)
		assert.True(t.T(), lock.Status.State == rlv1.LockGranted)
		// Should choose the left-over depop nodegroup
		assert.Equal(t.T(), "group3", lock.Status.GroupResourceGrants[0].GroupingValue)
		assertNgCountsAcquired(t.T(), job1, 0, 1)

		job2 := newSdkExecuteJob("sdk-train-2")
		defer assertWsjobCleanedUp(t.T(), job2)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		assertPodCountEventually(t.T(), job2.Namespace, job2.Name, rlv1.WorkerRequestName, 1)
		lock = assertLockExists(t.T(), job2.Namespace, job2.Name)
		assert.True(t.T(), lock.Status.State == rlv1.LockGranted)
		// Should choose the smaller populated nodegroup
		assert.Equal(t.T(), "group0", lock.Status.GroupResourceGrants[0].GroupingValue)
		assertNgCountsAcquired(t.T(), job2, 1, 0)
	})

	t.Run("test_memxPopulated_e2e", func() {
		require.Equal(t.T(), "false", t.CfgMgr.Cfg.Groups[2].Properties[wsapisv1.MemxPopNodegroupProp])
		require.Equal(t.T(), "false", t.CfgMgr.Cfg.Groups[3].Properties[wsapisv1.MemxPopNodegroupProp])

		// finally, create a job with weight replicas and assert that the memxPopulated property is set
		job := newWsExecuteJob(wscommon.Namespace, "memx-populated", 2)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job))
		defer assertWsjobCleanedUp(t.T(), job)
		assertPodCountEventually(t.T(), job.Namespace, job.Name, "", countNonBrPods(job)+12)

		// get the lock and assert that the memxPopulated property is set
		lock := assertLockExists(t.T(), job.Namespace, job.Name)
		checkedWeightGroupProp := false
		for _, grr := range lock.Spec.GroupResourceRequests {
			if strings.HasPrefix(grr.Name, rlv1.PrimaryNgPrefix) {
				for _, r := range grr.ResourceRequests {
					if r.Name == rlv1.WeightRequestName {
						assert.Equal(t.T(), []string{"true"}, r.Properties[wsapisv1.MemxPopNodegroupProp])
						checkedWeightGroupProp = true
						break
					}
				}
			}
		}
		assert.True(t.T(), checkedWeightGroupProp)
	})
}

func (t *ClusterConfigTestSuite) TestNonStandardCluster_DepopOnly() {
	// create a depop cluster similar to dropflow clusters - one depop group only
	deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
	depopClusterCfg := wscommon.NewClusterConfigBuilder("depop").
		WithCoordNode().
		// depop
		WithGroup(wsapisv1.MinMemxPerPopNodegroup-1, 2).
		WithBR(24).
		BuildClusterSchema()
	t.NoError(wscommon.CreateK8sNodes(depopClusterCfg, k8sClient))
	t.NoError(t.CfgMgr.Initialize(depopClusterCfg))

	// when only one depop group is present, we treat this as a pop group during resource initialization
	require.Equal(t.T(), 1, len(t.CfgMgr.Cfg.Groups))
	require.Equal(t.T(), "true", t.CfgMgr.Cfg.Groups[0].Properties[wsapisv1.MemxPopNodegroupProp])

	t.Run("test_dropflow_cluster_schedules_train_job", func() {
		// create a train job
		job := newWsExecuteJob(wscommon.Namespace, "dropflow-job", 1)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job))
		assertPodCountEventually(t.T(), job.Namespace, job.Name, "", countNonBrPods(job))
		assertWsjobCleanedUp(t.T(), job)
	})
}

func (t *ClusterConfigTestSuite) TestNonStandardCluster_Depop_WithSecondaryNicEnabled() {
	cluster.DisableLabeler = false
	defer func() {
		cluster.DisableLabeler = true
	}()
	for _, testcase := range []struct {
		name                    string
		numSystem               int
		actPerSys               int
		includeNodeWithoutNic   bool
		includeNodeWithErrorNic bool
		wsjobAnnotation         string
	}{
		{
			name:                    "fully-using-secondary-nics",
			numSystem:               2,
			actPerSys:               29,
			includeNodeWithoutNic:   false,
			includeNodeWithErrorNic: false,
			wsjobAnnotation:         "fully-used",
		},
		{
			name:                    "missing-secondary-nics",
			numSystem:               2,
			actPerSys:               29,
			includeNodeWithoutNic:   true,
			includeNodeWithErrorNic: false,
			wsjobAnnotation:         "degraded",
		},
		{
			name:                    "secondary-nics-error",
			numSystem:               2,
			actPerSys:               29,
			includeNodeWithoutNic:   false,
			includeNodeWithErrorNic: true,
			wsjobAnnotation:         "degraded",
		},
		{
			name:                    "secondary-nics-error-but-too-few-acts",
			numSystem:               2,
			actPerSys:               1,
			includeNodeWithoutNic:   false,
			includeNodeWithErrorNic: true,
			wsjobAnnotation:         "none-used",
		},
		{
			name:                    "not-using-depop-nodegroup",
			numSystem:               1,
			actPerSys:               29,
			includeNodeWithoutNic:   true,
			includeNodeWithErrorNic: false,
			wsjobAnnotation:         "none-used",
		},
	} {
		// create a depop cluster
		deleteNodes(t.T()) // delete existing nodes prior to recreate since cfg Build() inspects nodes
		depopClusterCfg := wscommon.NewClusterConfigBuilder("depop").
			WithCoordNode().
			WithGroup(wsapisv1.MinMemxPerPopNodegroup, 2).
			// depop
			WithGroup(wsapisv1.MinMemxPerPopNodegroup-1, 2).
			WithBR(6).
			BuildClusterSchema()

		degradedNode := ""
		// simulate one node in the depop nodegroup to only have one nic
		shouldDegrade := testcase.includeNodeWithoutNic || testcase.includeNodeWithErrorNic
		if shouldDegrade {
			for i, no := range depopClusterCfg.Nodes {
				if no.GetGroup() != depopClusterCfg.Groups[1].Name || no.Role != wscommon.RoleMemory {
					continue
				}
				degradedNode = no.Name
				if testcase.includeNodeWithoutNic {
					depopClusterCfg.Nodes[i].NICs = []*wscommon.NetworkInterface{no.NICs[0]}
					// if wsjob annotation should be "none-used" in the end, we should remove the
					// secondary NIC on all memoryX nodes in the depop nodegroup.
					if testcase.wsjobAnnotation != "none-used" {
						break
					}
				}
			}
		}
		t.NoError(wscommon.CreateK8sNodes(depopClusterCfg, k8sClient))
		t.NoError(t.CfgMgr.Initialize(depopClusterCfg))

		t.createNsIfNotExists(wscommon.DefaultNamespace)
		t.assignEverythingToNS(wscommon.DefaultNamespace, 2)

		t.Run(testcase.name, func() {
			wscommon.DisableMultus = false
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			wscommon.DisableSecondaryDataNic = false
			defer func() {
				wscommon.DisableMultus = true
				t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
				wscommon.DisableSecondaryDataNic = true
			}()
			globalDefaultActPerSys := defaultActPerSys
			defaultActPerSys = testcase.actPerSys
			defer func() {
				defaultActPerSys = globalDefaultActPerSys
			}()

			if testcase.includeNodeWithErrorNic {
				// wait until the node labeler updates the node, then update the node conditions
				require.Eventually(t.T(), func() bool {
					no := &corev1.Node{}
					err := k8sClient.Get(context.Background(), client.ObjectKey{Name: degradedNode}, no)
					t.T().Log(fmt.Sprintf("%+v", no))
					require.NoError(t.T(), err)
					return no.Labels[wscommon.NamespaceLabelKey] == wscommon.Namespace
				}, eventuallyTimeout, eventuallyTick)

				no := &corev1.Node{}
				require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKey{Name: degradedNode}, no))
				no.Status.Conditions = []corev1.NodeCondition{
					{
						Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic2",
						Status: corev1.ConditionTrue,
					},
				}
				require.NoError(t.T(), k8sClient.Status().Update(context.Background(), no))
				require.Eventually(t.T(), func() bool {
					return len(t.CfgMgr.Cfg.UnhealthyNodes) == 0 &&
						len(t.CfgMgr.Cfg.NodeMap[degradedNode].GetErrorNICs()) == 1
				}, eventuallyTimeout, eventuallyTick)
			}

			numSystems := uint32(testcase.numSystem)
			job := newWsExecuteJob(wscommon.Namespace, testcase.name, numSystems)
			require.NoError(t.T(), k8sClient.Create(context.Background(), job))
			defer assertWsjobCleanedUp(t.T(), job)

			if shouldDegrade && testcase.numSystem > 1 && testcase.wsjobAnnotation != "none-used" {
				assertEventEventually(t.T(), job.Namespace, job.Name, wscommon.DegradedSchedulingJobEventReason)
			}

			acts := assertPodCountEventually(t.T(), job.Namespace, job.Name, rlv1.ActivationRequestName, defaultActPerSys*int(numSystems))
			actsOnPopGroup := 0
			actsOnDepopGroup := 0
			type NadCount struct {
				Primary   int
				Secondary int
			}
			nodeNadMap := map[string]NadCount{}
			for _, act := range acts.Items {
				val, ok := act.Annotations[wscommon.NetworkAttachmentKey]
				assert.True(t.T(), ok)

				nodeName := act.Spec.NodeSelector[corev1.LabelHostname]
				ng := t.CfgMgr.Cfg.NodeMap[nodeName].GetGroup()
				if g, ok := t.CfgMgr.Cfg.Resources.GetGroup(ng); ok && g.IsPopulated() {
					actsOnPopGroup += 1
					assert.Equal(t.T(), wscommon.DataNetAttachDef, val)
				} else {
					actsOnDepopGroup += 1
					nodeName = act.Spec.NodeSelector[corev1.LabelHostname]
					nadCount := nodeNadMap[nodeName]
					if val == wscommon.DataNetAttachDef {
						nadCount.Primary += 1
					} else if val == wscommon.SecondNadLegacy {
						nadCount.Secondary += 1
					}
					nodeNadMap[nodeName] = nadCount

					if shouldDegrade && nodeName == degradedNode {
						assert.Equal(t.T(), wscommon.DataNetAttachDef, val)
					}
				}
			}

			assert.Equal(t.T(), defaultActPerSys, actsOnPopGroup)
			if testcase.numSystem > 1 {
				assert.Equal(t.T(), defaultActPerSys, actsOnDepopGroup)
			}

			for no, val := range nodeNadMap {
				if shouldDegrade && (no == degradedNode || testcase.wsjobAnnotation == "none-used") {
					assert.Equal(t.T(), 0, val.Secondary)
					continue
				}
				diff := math.Abs(float64(val.Primary - val.Secondary))
				assert.True(t.T(), diff <= 1)
			}
			assertWsJobAnnotatedEventually(
				MemxSecondaryNicAnnotationKey,
				testcase.wsjobAnnotation,
			)(t.T(), wscommon.Namespace, job.Name)
		})
	}
}

func (t *ClusterConfigTestSuite) TestManagementCoordinatorSeparation() {
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	assertNodeLabels := func(nodeName string, expectedRoleLabels []string) {
		n := &corev1.Node{}
		require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKey{Name: nodeName}, n))

		var actualRoleLabels []string
		for labelKey := range n.Labels {
			if strings.HasPrefix(labelKey, "k8s.cerebras.com/node-role") {
				actualRoleLabels = append(actualRoleLabels, labelKey)
				assert.Contains(t.T(), expectedRoleLabels, labelKey)
			}
		}
		assert.Equal(t.T(), len(expectedRoleLabels), len(actualRoleLabels))
	}

	t.Run("mgmt-coord_separation_in_action", func() {
		cluster.DisableLabeler = false
		originalInterval := wscommon.OverheadSyncInterval
		wscommon.OverheadSyncInterval = 500 * time.Hour // disable overhead sync between tests
		defer func() {
			cluster.DisableLabeler = true
			wscommon.OverheadSyncInterval = originalInterval
		}()
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(systemCount, 6, 30, systemCount, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		for _, n := range v2Cfg.Nodes {
			if n.Role == wscommon.RoleCoordinator {
				// dedicated management nodes: 0-coord-0, 0-coord-1, 0-coord-2, 0-coord-3
				// dedicated coordinator nodes: 0-coord-4, 0-coord-5
				// separation not enforced at the beginning but will be enforced eventually towards end of tests
				n.Role = wscommon.RoleManagement
				if string(n.Name[len(n.Name)-1]) < "4" {
					n.Properties[wscommon.StorageTypeKey] = wscommon.CephStorageType
				}
			}
		}
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))

		assert.False(t.T(), wsapisv1.ManagementSeparationEnforced)

		t.createNsIfNotExists(wscommon.DefaultNamespace)
		t.unassignEverythingFromNS(wscommon.DefaultNamespace)

		ns1 := "ns1"
		ns2 := "ns2"
		nsr1 := t.createNsIfNotExists(ns1)
		nsr1.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:           namespacev1.DebugSetMode,
				SystemNames:          []string{"system-0", "system-1"},
				CoordinatorNodeNames: []string{"0-coord-0", "0-coord-1", "0-coord-4"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, nsr1))
		t.ensureNsUpdateSuccess(nsr1)

		nsr2 := t.createNsIfNotExists(ns2)
		nsr2.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:           namespacev1.DebugSetMode,
				SystemNames:          []string{"system-2", "system-3"},
				CoordinatorNodeNames: []string{"0-coord-2", "0-coord-3", "0-coord-5"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, nsr2))
		t.ensureNsUpdateSuccess(nsr2)

		// removing all management nodes from ns1
		nsr1.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:           namespacev1.DebugSetMode,
				SystemNames:          []string{"system-0", "system-1"},
				NodegroupNames:       []string{"group0", "group1"},
				CoordinatorNodeNames: []string{"0-coord-4"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, nsr1))
		t.ensureNsUpdateSuccess(nsr1)
		// ns2 still owns some management nodes

		assertNodeLabels("0-coord-0", []string{"k8s.cerebras.com/node-role-management", "k8s.cerebras.com/node-role-coordinator"})
		assertNodeLabels("0-coord-4", []string{"k8s.cerebras.com/node-role-management", "k8s.cerebras.com/node-role-coordinator"})
		assertNodeLabels("group0-memory-0", []string{"k8s.cerebras.com/node-role-memory"})
		assertNodeLabels("group0-worker-0", []string{"k8s.cerebras.com/node-role-worker"})
		assertNodeLabels("group1-memory-0", []string{"k8s.cerebras.com/node-role-memory"})
		assertNodeLabels("group1-worker-0", []string{"k8s.cerebras.com/node-role-worker"})
		assertNodeLabels("group2-memory-0", []string{"k8s.cerebras.com/node-role-memory"})
		assertNodeLabels("group2-worker-0", []string{"k8s.cerebras.com/node-role-worker"})

		// adding more management nodes to ns2 is not forbidden
		nsr2.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:           namespacev1.DebugAppendMode,
				CoordinatorNodeNames: []string{"0-coord-0"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, nsr2))
		t.ensureNsUpdateSuccess(nsr2)
		assert.False(t.T(), wsapisv1.ManagementSeparationEnforced)

		// removing all management nodes from ns2
		nsr2.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:           namespacev1.DebugSetMode,
				SystemNames:          []string{"system-2", "system-3"},
				CoordinatorNodeNames: []string{"0-coord-5"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, nsr2))
		t.ensureNsUpdateSuccess(nsr2)

		// simulating operator redeployment with separation enforced through cluster pkg properties
		wsapisv1.ManagementSeparationEnforced = true
		defer func() { wsapisv1.ManagementSeparationEnforced = false }()
		t.NoError(t.CfgMgr.Initialize(v2Cfg))
		require.Eventually(t.T(), func() bool {
			return t.ResourceController.NewCfg == nil &&
				t.ResourceController.Cfg.ResourceVersion == t.ConfigReconciler.CfgMgr.Cfg.ResourceVersion
		}, eventuallyTimeout, eventuallyTick)

		assertNodeLabels("0-coord-0", []string{"k8s.cerebras.com/node-role-management"})
		assertNodeLabels("0-coord-4", []string{"k8s.cerebras.com/node-role-coordinator"})
		assertNodeLabels("group0-memory-0", []string{"k8s.cerebras.com/node-role-memory"})
		assertNodeLabels("group0-worker-0", []string{"k8s.cerebras.com/node-role-worker"})
		assertNodeLabels("group1-memory-0", []string{"k8s.cerebras.com/node-role-memory"})
		assertNodeLabels("group1-worker-0", []string{"k8s.cerebras.com/node-role-worker"})
		assertNodeLabels("group2-memory-0", []string{"k8s.cerebras.com/node-role-memory"})
		assertNodeLabels("group2-worker-0", []string{"k8s.cerebras.com/node-role-worker"})

		// adding more management nodes to ns2 is forbidden
		nsr2.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:           namespacev1.DebugAppendMode,
				CoordinatorNodeNames: []string{"0-coord-0"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, nsr2))
		t.ensureNsUpdateFailed(nsr2)
		t.Require().Contains(nsr2.Status.GetLastAttemptedRequest().Message, "use a dedicated coordinator node instead")
		assert.True(t.T(), wsapisv1.ManagementSeparationEnforced)

		// additional session mgmt operations should not change the enforcement decision
		t.unassignEverythingFromNS(ns1)
		t.unassignEverythingFromNS(ns2)
		assert.True(t.T(), wsapisv1.ManagementSeparationEnforced)
		t.assignEverythingToNS(wscommon.DefaultNamespace, systemCount)
		assert.True(t.T(), wsapisv1.ManagementSeparationEnforced)
	})
}

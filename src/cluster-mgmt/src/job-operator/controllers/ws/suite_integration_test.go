//go:build integration

package ws

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	resourcelockv1 "cerebras.com/job-operator/client-resourcelock/clientset/versioned/typed/resourcelock/v1"
	wsv1 "cerebras.com/job-operator/client/clientset/versioned/typed/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/controllers/cluster"
	"cerebras.com/job-operator/controllers/health"
	rl "cerebras.com/job-operator/controllers/resource"
	"cerebras.com/job-operator/controllers/resource/namespace"
)

func (t *TestSuite) SetupSuite() {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(t.UseExistingCluster, t.T())
	t.Manager = mgr
	k8sClient = mgr.GetClient()
	k8sReader = mgr.GetAPIReader()
	wsV1Interface, _ = wsv1.NewForConfig(mgr.GetConfig())
	rlV1Interface, _ = resourcelockv1.NewForConfig(mgr.GetConfig())
	t.TestEnv = testEnv
	t.Cancel = cancel
	t.Ctx = ctx
	t.DefaultCfgSchema = wscommon.NewTestClusterConfigSchema(systemCount, 1, brCount, systemCount, 0, workersPerGroup, memoryXPerGroup)
	require.NoError(t.T(), wscommon.CreateK8sNodes(t.DefaultCfgSchema, k8sClient))
	t.CfgMgr = wscommon.NewTestClusterConfigMgr(mgr, systemCount, brCount, systemCount, workersPerGroup, memoryXPerGroup)

	// Reduce wait intervals for testing
	wscommon.OverheadSyncInterval = 3 * time.Second
	wscommon.FailingJobGracePeriod = 2 * time.Second
	cluster.DisableLabeler = true
	t.NoError(t.Manager.Add(t.CfgMgr))

	nodeLabeler := cluster.NewNodeLabeler(mgr.GetClient(), mgr.GetLogger())
	t.CfgMgr.AddWatcher(nodeLabeler.GetCfgCh())
	t.NoError(t.Manager.Add(nodeLabeler))

	systemUpdater := cluster.NewSystemUpdater(mgr.GetClient(), mgr.GetLogger())
	t.CfgMgr.AddWatcher(systemUpdater.GetCfgCh())
	t.NoError(t.Manager.Add(systemUpdater))

	resourcesController := rl.NewController(t.CfgMgr)
	t.NoError(resourcesController.SetupWithManager(mgr))
	t.CfgMgr.AddWatcher(resourcesController.CfgChan)
	t.ResourceController = resourcesController

	jobReconciler := NewJobOperatorReconciler(t.CfgMgr.Cfg, mgr, false)
	t.CfgMgr.AddWatcher(jobReconciler.CfgChan)
	t.NoError(jobReconciler.SetupWithManager(mgr))
	t.JobReconciler = jobReconciler

	workflowReconciler := NewWorkflowReconciler(jobReconciler.WorkflowEventChan, jobReconciler.WorkflowCache)
	t.NoError(workflowReconciler.SetupWithManager(mgr))
	t.WorkflowReconciler = workflowReconciler

	leaseReconciler := NewLeaseReconciler(jobReconciler.WorkflowEventChan, true)
	require.NoError(t.T(), leaseReconciler.SetupWithManager(mgr))
	t.LeaseReconciler = leaseReconciler

	configReconciler, err := cluster.NewConfigReconciler(t.CfgMgr, mgr)
	require.NoError(t.T(), err)
	t.ConfigReconciler = configReconciler

	err, nR, sR := health.NewHealthReconciler(mgr, true)
	require.NoError(t.T(), err)
	t.NodeReconciler = nR
	t.SystemReconciler = sR

	t.KubeConfigFile = CreateKubeconfigFileForRestConfig(testEnv.Config)
	logrus.Infof("Envtest kubeconfig path: %s", t.KubeConfigFile)

	updateClusterConfig(t.T(), t.CfgMgr, t.CfgMgr.Cfg.ClusterSchema)
}

func (t *TestSuite) BeforeTest(suiteName, testName string) {
	logrus.Infof("BeforeTest suite/test %s/%s", suiteName, testName)
	wscommon.EnableMultiCoordinator = false
	wscommon.Namespace = wscommon.DefaultNamespace
	wscommon.DisableMultus = true
	wsapisv1.DisableMultusForBR = true
	cluster.DisableLabeler = true
	// Reset memory tier variables
	memxRegularMemBytes = wscommon.DefaultNodeMem << 20
	memxLargeMemBytes = wscommon.LargeNodeMem << 20
	memxXLargeMemBytes = wscommon.XLargeNodeMem << 20
	memxSecondaryMemBytes = wscommon.DefaultNodeMem << 20
	axMemoryBytes = wscommon.DefaultNodeMem << 20

	wsapisv1.CrdExecuteMinMemoryBytes = int(defaultCrdMem.Value())
	wsapisv1.CrdCompileMinMemoryBytes = int(defaultCrdMem.Value())
	namespace.InitMemConstants()
	t.JobReconciler.JobController.Config.EnableMultus = false

	nodes := &v1.NodeList{}
	t.Require().NoError(k8sClient.List(context.Background(), nodes))
	if len(nodes.Items) != len(t.DefaultCfgSchema.Nodes) {
		logrus.Infof("Number of k8s nodes does not match cluster config nodes, re-creating nodes")
		for _, node := range nodes.Items {
			t.Require().NoError(k8sClient.Delete(context.Background(), &node))
		}
		t.Require().NoError(wscommon.CreateK8sNodes(t.DefaultCfgSchema, k8sClient))
	}
	// rebuild to ensure no update affects no test since there are pointers in schema
	t.DefaultCfgSchema = wscommon.NewTestClusterConfigSchema(systemCount, 1, brCount, systemCount, 0, workersPerGroup, memoryXPerGroup)
	t.NoError(t.CfgMgr.Initialize(t.DefaultCfgSchema))
	t.Require().Eventually(func() bool {
		sys := systemv1.SystemList{}
		t.NoError(k8sClient.List(t.Ctx, &sys))
		return len(sys.Items) == len(t.DefaultCfgSchema.Systems)
	}, eventuallyTimeout, eventuallyTick)
	assertClusterServerIngressSetup(t.T(), wscommon.Namespace)
}

func (t *TestSuite) AfterTest(suiteName, testName string) {
	logrus.Infof("AfterTest suite/test %s/%s", suiteName, testName)
	assertClusterServerIngressCleanup(t.T(), wscommon.Namespace)
}

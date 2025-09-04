//go:build e2e

package ws

import (
	"github.com/sirupsen/logrus"

	resourcelockv1 "cerebras.com/job-operator/client-resourcelock/clientset/versioned/typed/resourcelock/v1"
	wsv1 "cerebras.com/job-operator/client/clientset/versioned/typed/ws/v1"
	"cerebras.com/job-operator/common"
)

func (t *TestSuite) SetupSuite() {
	t.UseExistingCluster = true
	mgr, testEnv, _, cancel := common.EnvSetup(t.UseExistingCluster, t.T())
	t.Manager = mgr
	k8sClient = mgr.GetClient()
	k8sReader = mgr.GetAPIReader()
	wsV1Interface, _ = wsv1.NewForConfig(mgr.GetConfig())
	rlV1Interface, _ = resourcelockv1.NewForConfig(mgr.GetConfig())
	t.TestEnv = testEnv
	t.Cancel = cancel

	t.KubeConfigFile = CreateKubeconfigFileForRestConfig(testEnv.Config)
	logrus.Infof("Envtest kubeconfig path: %s", t.KubeConfigFile)
}

func (t *TestSuite) BeforeTest(suiteName, testName string) {
	logrus.Info("start test", "suite", suiteName, "test", testName)
	assertClusterServerIngressSetup(t.T(), common.Namespace)
}

func (t *TestSuite) AfterTest(suiteName, testName string) {
	logrus.Info("end test", "suite", suiteName, "test", testName)
	assertClusterServerIngressCleanup(t.T(), common.Namespace)
}

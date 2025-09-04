//go:build integration && cluster_controller

package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wscommon "cerebras.com/job-operator/common"
)

var cfgChan chan wscommon.ConfigUpdateEvent
var latestCfg *wscommon.ClusterConfig
var cr *ConfigReconciler
var err error

const (
	timeout  = 5 * time.Second
	interval = 1 * time.Second
)

func TestConfigReconciler(t *testing.T) {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()
	cfgMgr := wscommon.NewTestClusterConfigMgr(mgr, 2, 1, 0, 1, 5)
	sysUpdater := NewSystemUpdater(mgr.GetClient(), mgr.GetLogger())
	cfgMgr.AddWatcher(sysUpdater.GetCfgCh())
	require.NoError(t, mgr.Add(sysUpdater))

	cfgChan = make(chan wscommon.ConfigUpdateEvent, 10)
	go func() {
		for {
			select {
			case c := <-cfgChan:
				latestCfg = c.Cfg
			case <-ctx.Done():
				return
			}
		}
	}()
	cfgMgr.AddWatcher(cfgChan)

	cr, err = NewConfigReconciler(cfgMgr, mgr)
	require.NoError(t, err)

	// create cm to trigger system crd initCfgMgr
	cmYaml, err := yaml.Marshal(cfgMgr.Cfg.ClusterSchema)
	require.NoError(t, err)
	cm := v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      wscommon.DefaultClusterConfigMapName,
		Namespace: wscommon.SystemNamespace,
	}}
	cm.Data = map[string]string{wscommon.ClusterConfigFile: string(cmYaml)}
	require.NoError(t, mgr.GetClient().Create(ctx, &cm))
	sysList := &systemv1.SystemList{}
	require.Eventually(t, func() bool {
		require.NoError(t, mgr.GetClient().List(ctx, sysList))
		return len(sysList.Items) == 2
	}, timeout, interval)

	t.Run("reconcile_on_node_unschedulable", func(t *testing.T) {
		cr.initCfgMgr = false
		coordNodes := cfgMgr.Cfg.GetCoordNodes()
		require.Len(t, coordNodes, 1)

		n := &v1.Node{}
		require.NoError(t, mgr.GetClient().Get(ctx, client.ObjectKey{Name: coordNodes[0].Name}, n))
		n.Spec.Unschedulable = true
		require.NoError(t, mgr.GetClient().Update(ctx, n))

		time.Sleep(time.Millisecond * 500)
		assertUnhealthyNodeRecovered(t) // should not update until cfg initialized

		// trigger cfg init
		cm.Annotations = map[string]string{"test": "triggerUpdate0"}
		require.NoError(t, mgr.GetClient().Update(ctx, &cm))

		assertUnhealthyNodeDetected(t, 0)

		n.Spec.Unschedulable = false
		require.NoError(t, mgr.GetClient().Update(ctx, n))
		assertUnhealthyNodeRecovered(t)
	})

	t.Run("reconcile_on_node_condition", func(t *testing.T) {
		coordNodes := cfgMgr.Cfg.GetCoordNodes()
		require.Len(t, coordNodes, 1)
		n := &v1.Node{}
		require.NoError(t, mgr.GetClient().Get(ctx, client.ObjectKey{Name: coordNodes[0].Name}, n))
		n.Status.Conditions = []v1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeError",
				Status: v1.ConditionTrue,
			},
		}
		require.NoError(t, mgr.GetClient().Status().Update(ctx, n))
		assertUnhealthyNodeDetected(t, 0)

		n.Status.Conditions = nil
		require.NoError(t, mgr.GetClient().Status().Update(ctx, n))
		assertUnhealthyNodeRecovered(t)
	})

	t.Run("reconcile_on_node_NIC_condition", func(t *testing.T) {
		n := &v1.Node{}
		require.NoError(t, mgr.GetClient().Get(ctx, client.ObjectKey{Name: "br-0"}, n))
		n.Status.Conditions = []v1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
				Status: v1.ConditionTrue,
			},
		}
		require.NoError(t, mgr.GetClient().Status().Update(ctx, n))
		assertUnhealthyNodeDetected(t, 1)

		n.Status.Conditions = []v1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
				Status: v1.ConditionTrue,
			},
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic1",
				Status: v1.ConditionTrue,
			},
		}
		require.NoError(t, mgr.GetClient().Status().Update(ctx, n))
		assertUnhealthyNodeDetected(t, 2)

		n.Status.Conditions = []v1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
				Status: v1.ConditionFalse,
			},
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic1",
				Status: v1.ConditionFalse,
			},
		}
		require.NoError(t, mgr.GetClient().Status().Update(ctx, n))
		assertUnhealthyNodeRecovered(t)
	})

	t.Run("reconcile_on_system_unschedulable", func(t *testing.T) {
		cr.initCfgMgr = false
		s := &systemv1.System{}
		require.NoError(t, mgr.GetClient().Get(ctx, client.ObjectKey{Name: cfgMgr.Cfg.Systems[0].Name}, s))
		s.Spec.Unschedulable = true
		require.NoError(t, mgr.GetClient().Update(ctx, s))

		time.Sleep(500 * time.Millisecond)
		// state should not have changed to error until cfg init
		assertUnhealthySystemRecovered(t)

		// trigger cfg with updated crd and expect schedule status kept
		cfgMgr.Cfg.ClusterSchema.Systems[0].CmAddr = "updated"
		cmYaml, err = yaml.Marshal(cfgMgr.Cfg.ClusterSchema)
		require.NoError(t, err)
		cm = v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name:      wscommon.DefaultClusterConfigMapName,
			Namespace: wscommon.SystemNamespace,
		}}
		cm.Data = map[string]string{wscommon.ClusterConfigFile: string(cmYaml)}
		require.NoError(t, mgr.GetClient().Update(ctx, &cm))
		assertUnhealthySystemDetected(t)
		require.NoError(t, mgr.GetClient().Get(ctx, client.ObjectKey{Name: cfgMgr.Cfg.Systems[0].Name}, s))
		require.True(t, s.Spec.Unschedulable)
		require.Equal(t, "updated", s.Spec.CmAddr)

		s.Spec.Unschedulable = false
		require.NoError(t, mgr.GetClient().Update(ctx, s))
		assertUnhealthySystemRecovered(t)
	})
}

func assertUnhealthyNodeDetected(t *testing.T, expectErrorNICs int) {
	require.Eventually(t, func() bool {
		for _, n := range latestCfg.UnhealthyNodes { //cr.CfgMgr.Cfg.UnhealthyNodes {
			return len(n.GetErrorNICs()) == expectErrorNICs
		}
		return false
	}, timeout, interval)
	require.Equal(t, 1, len(latestCfg.UnhealthyNodes))
}

func assertUnhealthyNodeRecovered(t *testing.T) {
	require.Eventually(t, func() bool {
		return len(latestCfg.UnhealthyNodes) == 0
	}, timeout, interval)
	require.Equal(t, 0, len(latestCfg.UnhealthyNodes))
}

func assertUnhealthySystemDetected(t *testing.T) {
	require.Eventually(t, func() bool {
		return len(latestCfg.UnhealthySystems) == 1
	}, timeout, interval)
	require.Equal(t, 1, len(latestCfg.UnhealthySystems))
}

func assertUnhealthySystemRecovered(t *testing.T) {
	require.Eventually(t, func() bool {
		return len(latestCfg.UnhealthySystems) == 0
	}, timeout, interval)
	require.Equal(t, 0, len(latestCfg.UnhealthySystems))
}

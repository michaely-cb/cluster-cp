package common

import (
	"context"
	"strings"
	"sync"
	"time"

	netclient "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/typed/k8s.cni.cncf.io/v1"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource_config"
)

type NSChange interface {
	// Unassigned returns list of resource ids unassigned from a namespace, e.g. system/systemf1, nodegroup/ng0, node/x
	Unassigned() []string

	// Assigned returns a map of resource ids to the namespace they were assigned to, e.g. system/systemf1:namespacex
	Assigned() map[string]string

	// Check if there's resource changes
	IsEmpty() bool

	// Get nsr name + props
	GetProps() (string, map[string]string)
}

type ConfigUpdateReason string

const (
	ConfigUpdateReload          = ConfigUpdateReason("ConfigReload")
	ConfigUpdateHealth          = ConfigUpdateReason("HealthUpdate")
	ConfigUpdateResourceVersion = ConfigUpdateReason("ResourceVersionUpdate")
	ConfigUpdateCapacity        = ConfigUpdateReason("CapacityUpdate")
	ConfigUpdateNamespace       = ConfigUpdateReason("NamespaceUpdate")
)

var OverheadSyncInterval = 5 * time.Minute

type ConfigUpdateEvent struct {
	Reason ConfigUpdateReason
	Cfg    *ClusterConfig
}

// ClusterConfigMgr creates/recreates cluster config and synchronizes updates to cluster config. It also owns channels
// propagating config updates (CM, health, NS assign).
// The synchronization model works as follows:
//  1. All mutations to ClusterCfg should go through the ClusterConfigMgr's mutex locked Update* methods
//  2. Update* methods will make a shallow copy of the overall config and a deep copy of modified indices
//  3. If an Update* method actually changes the indices, it will swap the Mgr.Cfg reference and propagate the updated
//     Cfg to Watchers
//
// This method is not perfect since the Node/System/Nodegroup pointers within the object are not copied meaning that
// inconsistent states could be viewed (e.g. a node is in the Unhealthy index but its ports are being modified to not
// be in healthy state by an Update* call in Mgr).
type ClusterConfigMgr struct {
	client client.Client
	reader client.Reader
	config *rest.Config
	log    logrus.FieldLogger

	mu sync.Mutex

	watchers []chan ConfigUpdateEvent

	nodeCapacity resource_config.NodeCapacity
	Cfg          *ClusterConfig
}

func InitClusterConfigMgr(
	reader client.Reader,
	cli client.Client,
	config *rest.Config,
) (*ClusterConfigMgr, error) {
	m := &ClusterConfigMgr{
		reader: reader,
		client: cli,
		config: config,
		log:    logrus.WithField("component", "ClusterConfigMgr"),
	}

	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: SystemNamespace, Name: ClusterConfigMapName}
	if err := m.reader.Get(context.Background(), key, cm); err != nil {
		if !errors.IsNotFound(err) || !IsDev {
			logrus.WithField("error", err).Warn("Error fetching cluster configmap")
			return nil, err
		}
		logrus.Infof("Cluster config not found, assuming dev mode")
	}
	schema, err := NewClusterSchemaFromCM(cm)
	if err != nil {
		return nil, err
	}

	if err := m.Initialize(schema); err != nil {
		return nil, err
	}
	return m, nil
}

// periodically sync overhead in case of adhoc deployment
func (m *ClusterConfigMgr) Start(ctx context.Context) error {
	for {
		select {
		case <-time.After(OverheadSyncInterval):
			if m.Cfg == nil {
				continue
			}
			capacity, err := m.syncNodeCapacity(m.Cfg, true)
			if err == nil && !m.nodeCapacity.Equals(capacity) {
				m.log.Info("node capacity change watched, reload cfg")
				m.UpdateNodeCapacity(m.Cfg, capacity)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *ClusterConfigMgr) AddWatcher(ch chan ConfigUpdateEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch != nil {
		m.watchers = append(m.watchers, ch)
	}
}

// Initialize creates a new ClusterConfig by passed in schema and adjust based on cluster setup.
// Has side effect of updating global configuration variables, e.g. IsV2Network...
func (m *ClusterConfigMgr) Initialize(schema ClusterSchema) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// check operator running mode, i.e. global/non-global mode
	m.checkOperatorMode()

	// construct the cluster config object
	cfg := NewClusterConfig(schema)
	// check cluster physical/virtual network
	if err := m.checkClusterNetwork(cfg); err != nil {
		return err
	}
	// sync cluster health/capacity
	if err := m.syncClusterStatus(cfg); err != nil {
		return nil
	}

	// init nsr/resources
	if err := m.initNsrAssign(cfg); err != nil {
		return err
	}

	// adjust node roles/props based on cluster setup
	cfg.AdjustNodesByClusterSetup()
	m.reloadResourceMgmt(cfg)

	m.Cfg = cfg
	Log.WithField("cfg", cfg.String()).Debug("Reloaded cluster config")
	m.notify(ConfigUpdateEvent{Cfg: cfg, Reason: ConfigUpdateReload})
	return nil
}

func (m *ClusterConfigMgr) notify(event ConfigUpdateEvent) {
	m.log.WithFields(logrus.Fields{
		"rv":           event.Cfg.ResourceVersion,
		"reason":       event.Reason,
		"watcherCount": len(m.watchers),
	}).Info("notify config watchers")
	// Todo: do a deepcopy of config or build a new config to avoid race conditions
	// not critical since resources will be copied in lock controller
	for _, ch := range m.watchers {
		ch <- event
	}
}

// Notify force notify watchers of config. Useful for testing when config_reconciler doesn't trigger initial notify
func (m *ClusterConfigMgr) Notify() {
	m.notify(ConfigUpdateEvent{Cfg: m.Cfg, Reason: ConfigEventName})
}

func (m *ClusterConfigMgr) checkOperatorMode() {
	// detect non-cluster mode deployments
	if !EnableClusterMode {
		dps := appv1.DeploymentList{}
		cmLabels := client.MatchingLabels{"control-plane": "controller-manager"}
		if err := m.reader.List(context.Background(), &dps, cmLabels); err != nil {
			logrus.Warnf("Unable to list deploys")
		}
		clusterModeDisabledNS := map[string]bool{}
		for _, dp := range dps.Items {
			disableClusterMode, _ := m.IsClusterModeDisabled(dp.Namespace)
			if disableClusterMode {
				logrus.Infof("non cluster mode ns found: %s", dp.Namespace)
				clusterModeDisabledNS[dp.Namespace] = true
			}
		}
		ClusterModeDisabledNamespaceMap = clusterModeDisabledNS
	}
}

// syncClusterStatus sync system/node health+capacity
func (m *ClusterConfigMgr) syncClusterStatus(cfg *ClusterConfig) error {
	// sync system health
	sysList := &systemv1.SystemList{}
	if err := m.reader.List(context.Background(), sysList); err != nil && !errors.IsNotFound(err) {
		logrus.Warn("Error listing systems")
		return err
	}
	for _, s := range sysList.Items {
		m.updateSystemVersion(&s, cfg)
		m.updateSystemHealth(&s, cfg)
	}

	// sync node capacity + health
	k8sNodeMap := map[string]*corev1.Node{}
	nodeList := &corev1.NodeList{}
	if err := m.reader.List(context.Background(), nodeList); err != nil && !errors.IsNotFound(err) {
		logrus.Warn("Error listing k8s nodes")
		return err
	}
	for i, n := range nodeList.Items {
		k8sNodeMap[n.Name] = &nodeList.Items[i]
	}
	cfg.K8sNodeMap = k8sNodeMap

	// filter cluster nodes
	for i := range cfg.Nodes {
		node := cfg.Nodes[i]
		if k8Node, ok := k8sNodeMap[node.Name]; ok {
			for _, addr := range k8Node.Status.Addresses {
				if addr.Type == corev1.NodeInternalIP {
					node.HostIP = addr.Address
					break
				}
			}
			m.updateNodeVersion(k8Node, cfg)
			m.updateNodeHealth(k8Node, cfg)
		} else if !IsDev {
			delete(cfg.NodeMap, node.Name)
			logrus.Warnf("node %s exists in cluster config but not in k8s, filtering", node.Name)
		}
	}
	cfg.Nodes = ValuesBySortedKey(cfg.NodeMap)
	capacity, err := m.syncNodeCapacity(cfg, false)
	if err == nil {
		m.nodeCapacity = capacity
		return nil
	}
	return err
}

// sync node capacity based on CP pods + reserved overhead
func (m *ClusterConfigMgr) syncNodeCapacity(cfg *ClusterConfig, informerOnly bool) (resource_config.NodeCapacity, error) {
	b := resource_config.NewNodeCapacityBuilder()
	if m.config == nil {
		m.log.Warnf("Rest cfg not set in ClusterConfigMgr, cannot determine node overhead, using default overhead mode")
	} else {
		if err := b.UpdateOverheadPods(m.client, m.config, informerOnly); err != nil {
			m.log.Errorf("could not get update with overhead pods, err: %s", err)
			return nil, err
		}
	}
	for _, node := range cfg.K8sNodeMap {
		b.UpdateCapacityK8s(cfg.K8sNodeMap[node.Name])
	}
	nodeCapacity := b.Build()

	if Namespace == SystemNamespace {
		// record node available resources
		for _, node := range cfg.K8sNodeMap {
			cpu := nodeCapacity.Cpu(node.Name)
			mem := nodeCapacity.Mem(node.Name)
			NodeAvailableResources.DeletePartialMatch(map[string]string{"node": node.Name})
			NodeAvailableResources.WithLabelValues(node.Name, "cpu", "core").Set(float64(cpu.MilliValue()) / 1000.0)
			NodeAvailableResources.WithLabelValues(node.Name, "memory", "bytes").Set(float64(mem.Value()))
		}
	}
	return nodeCapacity, nil
}

// checkClusterNetwork check physical/virtual network, i.e. v2+multus
func (m *ClusterConfigMgr) checkClusterNetwork(cfg *ClusterConfig) error {
	// check physical network
	cfg.CheckV2Network()
	checkInferenceCluster(cfg)

	// check virtual network
	return m.checkMultusSetup()
}

// checkInferenceCluster check if V2-network cluster is inference cluster
func checkInferenceCluster(cfg *ClusterConfig) {
	wsapisv1.IsInferenceCluster = false
	if wsapisv1.IsV2Network && DisableNodeGroupScheduling && !IsDev {
		wsapisv1.IsInferenceCluster = true
		logrus.Info("V2 inference-only cluster detected.")
	}
}

func (m *ClusterConfigMgr) checkMultusSetup() error {
	if DisableMultus || m.config == nil {
		DisableSecondaryDataNic = true
		m.log.Info("DisableMultus is set to true or mgr.config is nil, skipping NAD check")
		return nil
	}

	// checking primary nad
	netClient := netclient.NewForConfigOrDie(m.config).NetworkAttachmentDefinitions(SystemNamespace)
	_, ready, err := m.checkNadDeployed(netClient, GetNadByIndex(0))
	if err != nil {
		return err
	} else if !ready {
		DisableMultus = true
		DisableSecondaryDataNic = true
		m.log.Info("Primary NAD not installed, skipping NAD check")
		return nil
	}

	// checking secondary nad
	if DisableSecondaryDataNic {
		m.log.Infof("DisableSecondaryDataNic is set to true, skipping secondary NAD check")
		return nil
	}
	// backwards compatible check
	nad, ready, err := m.checkNadDeployed(netClient, SecondNad, SecondNadLegacy)
	if err != nil {
		return err
	} else if !ready {
		DisableSecondaryDataNic = true
	} else {
		SecondNetAttachDef = nad
	}

	// checking additional nad
	if !wsapisv1.DisableMultusForBR {
		_, ready, err = m.checkNadDeployed(netClient, GetNadByIndex(2))
		if err != nil {
			return err
		} else if !ready {
			// disable br multus if third NAD not detected
			wsapisv1.DisableMultusForBR = true
		}
	}
	m.log.Infof("Primary NAD enabled: %t, secondary NAD enabled: %t, br NAD enabled: %t",
		!DisableMultus, !DisableSecondaryDataNic, !wsapisv1.DisableMultusForBR)
	return nil
}

// check nad list and return first found nad
func (m *ClusterConfigMgr) checkNadDeployed(netClient netclient.NetworkAttachmentDefinitionInterface,
	nadResName ...string) (string, bool, error) {
	for _, nad := range nadResName {
		nsname := strings.Split(nad, "/")
		name := nsname[len(nsname)-1]
		_, nadErr := netClient.
			Get(context.Background(), name, metav1.GetOptions{})
		if nadErr != nil {
			if !errors.IsNotFound(nadErr) {
				return "", false, nadErr
			}
			m.log.WithField("NAD", nad).Info("NAD not installed.")
		} else {
			m.log.WithField("NAD", nad).Info("NAD installed.")
			return nad, true, nil
		}
	}
	return "", false, nil
}

func (m *ClusterConfigMgr) IsClusterModeDisabled(namespace string) (bool, error) {
	cm := &corev1.ConfigMap{}
	if err := m.reader.Get(context.Background(), types.NamespacedName{
		Namespace: namespace,
		Name:      namespace + "-cluster-env"}, cm); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		m.log.Errorf("could not get cluster env for namespace: %s, err: %s", namespace, err)
		return false, err
	}
	return strings.ToLower(cm.Data[DisableClusterModeKey]) == "true", nil
}

// init nsr/resources
func (m *ClusterConfigMgr) initNsrAssign(cfg *ClusterConfig) error {
	// get systems to initialize user NS assignment
	nsrList := &namespacev1.NamespaceReservationList{}
	if err := m.reader.List(context.Background(), nsrList); err != nil {
		return err
	}
	cfg.InitializeNsAssignment(*nsrList)
	return nil
}

// generate resources based on cluster config
// todo: optimize by moving resources to upper level and keep cluster config the lowest level
// and ingest cfg for resource init instead of keep resources in config
// todo: instead of reloading all, update specific resources only if possible
func (m *ClusterConfigMgr) reloadResourceMgmt(cfg *ClusterConfig) {
	var iNodes []resource_config.INodeCfg
	var iSystems []resource_config.ISystemCfg
	nodeGroupProps := make(map[string]map[string]string)

	for _, node := range cfg.Nodes {
		iNodes = append(iNodes, node)
	}
	for _, system := range cfg.Systems {
		system.InitPorts()
		iSystems = append(iSystems, system)
	}
	for _, g := range cfg.Groups {
		nodeGroupProps[g.Name] = g.Properties
	}

	cfg.Resources = resource_config.InitializeResourceManagement(
		iNodes,
		nodeGroupProps,
		iSystems,
		cfg.systemToNsMap,
		m.nodeCapacity,
	)
}

func (m *ClusterConfigMgr) UpdateNodeCapacity(cfg *ClusterConfig, capacity resource_config.NodeCapacity) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeCapacity = capacity
	m.reloadResourceMgmt(cfg)
	m.notify(ConfigUpdateEvent{Cfg: cfg, Reason: ConfigUpdateCapacity})
}

// update node props based on cluster monitoring from k8s node
func (m *ClusterConfigMgr) UpdateNode(n *corev1.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var cfg *ClusterConfig
	var reason ConfigUpdateReason
	if c := m.updateNodeVersion(n, nil); c != nil {
		cfg = c
		reason = ConfigUpdateResourceVersion
	}
	if c := m.updateNodeHealth(n, cfg); c != nil {
		cfg = c
		reason = ConfigUpdateHealth
	}
	if cfg != nil {
		m.reloadResourceMgmt(cfg)
		m.notify(ConfigUpdateEvent{Cfg: cfg, Reason: reason})
	}
}

// update system props based on cluster monitoring from k8s system
func (m *ClusterConfigMgr) UpdateSystem(s *systemv1.System) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var cfg *ClusterConfig
	var reason ConfigUpdateReason
	if c := m.updateSystemVersion(s, nil); c != nil {
		cfg = c
		reason = ConfigUpdateResourceVersion
	}
	if c := m.updateSystemHealth(s, cfg); c != nil {
		cfg = c
		reason = ConfigUpdateHealth
	}
	if cfg != nil {
		m.reloadResourceMgmt(cfg)
		m.notify(ConfigUpdateEvent{Cfg: cfg, Reason: reason})
	}
}

func (m *ClusterConfigMgr) UpdateSystemHealth(s *systemv1.System) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cfg := m.updateSystemHealth(s, nil); cfg != nil {
		m.reloadResourceMgmt(cfg)
		m.notify(ConfigUpdateEvent{Cfg: cfg, Reason: ConfigUpdateHealth})
	}
}

func (m *ClusterConfigMgr) UpdateNsAssignment(nsChange NSChange) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if newCfg := m.updateNsAssignment(nsChange); newCfg != nil {
		m.reloadResourceMgmt(newCfg)
		m.notify(ConfigUpdateEvent{Cfg: newCfg, Reason: ConfigUpdateNamespace})
	}
}

// UpdateSystemHealth updates system health status and return new config if status changed
// newCfg: optional param, if passed in, will update status in newCfg instead of m.Cfg
func (m *ClusterConfigMgr) updateSystemHealth(s *systemv1.System, newCfg *ClusterConfig) *ClusterConfig {
	needReload := false
	cfg := newCfg
	if cfg == nil {
		cfg = m.Cfg.Copy()
	}
	system, ok := cfg.SystemMap[s.Name]
	if !ok {
		m.log.Warnf("system %s isn't in current cluster config, ignore till config updated", s.Name)
		return nil
	}

	healthy, errPorts, _ := GetSystemHealth(s)
	systemLevelError := false
	if !healthy && len(errPorts) == 0 {
		systemLevelError = true
		if len(system.GetErrorPorts()) != 0 {
			m.log.Warnf("reload needed, system %s error escalated from port level to system level", system.Name)
			needReload = true
		}
	}
	for i := range system.Ports {
		port := &system.Ports[i]
		if systemLevelError {
			port.HasError = false
			continue
		}
		// reload if NIC status change
		if errPorts[port.Name] != port.HasError {
			m.log.WithFields(logrus.Fields{
				"system":             system.Name,
				"port":               port.Name,
				"previousErrorState": port.HasError,
				"currentErrorState":  errPorts[port.Name],
			}).Warn(
				"reload needed, error status changed",
			)
			needReload = true
			port.HasError = errPorts[port.Name]
		}
	}

	// if detected unhealthy first time
	if _, ok := cfg.UnhealthySystems[system.Name]; !ok && !healthy {
		m.log.Warnf("reload needed, unhealthy system %s detected", system.Name)
		cfg.UnhealthySystems[system.Name] = system
		system.HasError = true
		if len(errPorts) > 0 {
			logrus.Warnf("Ports error detected: %v", Keys(errPorts))
		}
		needReload = true
	} else if ok && healthy {
		// if recovered back to healthy
		m.log.Infof("reload needed, system %s recovered to be healthy", system.Name)
		delete(cfg.UnhealthySystems, system.Name)
		system.HasError = false
		needReload = true
	}

	if needReload {
		m.Cfg = cfg
		return m.Cfg
	}
	return nil
}

func (m *ClusterConfigMgr) updateSystemVersion(s *systemv1.System, newCfg *ClusterConfig) *ClusterConfig {
	cfg := newCfg
	if cfg == nil {
		cfg = m.Cfg.Copy()
	}
	system := cfg.SystemMap[s.Name]
	if system == nil {
		m.log.Warnf("system %s isn't in current cluster config, ignore till config reloaded", s.Name)
		return nil
	}
	updated := false
	// support single system to have multiple versions to be compatible
	if v := s.Labels[wsapisv1.SystemVersionKey]; v != "" {
		if system.Properties[resource.SystemVersionPropertyKey] != v {
			system.Properties = EnsureMap(system.Properties)
			system.Properties[resource.SystemVersionPropertyKey] = v
			updated = true
		}
	}
	if updated {
		m.Cfg = cfg
		return m.Cfg
	}
	return nil
}

// UpdateSystemAssign updates system -> namespace assignment if label changes
// Assumes that resources are first unassigned to a namespace (resourceId -> "") before they are re-assigned (resourceId -> "newNs")
func (m *ClusterConfigMgr) updateNsAssignment(nsChange NSChange) *ClusterConfig {
	cfgCopy := m.Cfg
	if !nsChange.IsEmpty() {
		cfgCopy = m.Cfg.Copy()
	}
	name, props := nsChange.GetProps()
	if !maps.Equal(cfgCopy.nsProperties[name], props) {
		cfgCopy.nsProperties[name] = props
		m.log.Infof("Updated NsProperties for %s: %v", name, cfgCopy.nsProperties[name])
	}
	if nsChange.IsEmpty() {
		return nil
	}

	// remove from existing
	for _, rid := range nsChange.Unassigned() {
		rtype, rname, err := resource.RIDToTypeName(rid)
		if err != nil {
			logrus.Warnf("invalid resource id: %s", rid)
			continue
		}
		if rtype == resource.SystemType {
			oldNs := cfgCopy.systemToNsMap[rname]
			delete(cfgCopy.systemToNsMap, rname)
			delete(cfgCopy.nsAssignedSystems[oldNs], rname)
		} else if rtype == resource.NodegroupType {
			ng := m.Cfg.GroupMap[rname]
			if ng == nil {
				logrus.Warnf("%s was unassigned from namespace but no longer exists, ignore", rid)
				continue
			}
			oldNs := ng.Properties[NamespaceKey]
			delete(cfgCopy.nsAssignedGroups[oldNs], rname)
			delete(ng.Properties, NamespaceKey)
		} else if rtype == resource.NodeType {
			node := m.Cfg.NodeMap[rname]
			if node == nil {
				logrus.Warnf("%s was unassigned from namespace but no longer exists, ignore", rid)
				continue
			}
			oldNs := node.Properties[NamespaceKey]
			delete(cfgCopy.nsAssignedNodes[oldNs], rname)
			delete(node.Properties, NamespaceKey)
		}
	}

	// add to new
	for rid, newNs := range nsChange.Assigned() {
		id := strings.Split(rid, "/")
		if len(id) != 2 {
			logrus.Warnf("invalid resource id: %s", rid)
			continue
		}
		rtype, rname := id[0], id[1]
		if rtype == resource.SystemType {
			cfgCopy.systemToNsMap[rname] = newNs
			cfgCopy.nsAssignedSystems[newNs] = EnsureMap(cfgCopy.nsAssignedSystems[newNs])
			cfgCopy.nsAssignedSystems[newNs][rname] = cfgCopy.SystemMap[rname]
		} else if rtype == resource.NodegroupType {
			ng := m.Cfg.GroupMap[rname]
			if ng == nil {
				logrus.Warnf("%s was assigned to namespace/%s but no longer exists, ignore", rid, newNs)
				continue
			}
			ng.Properties[NamespaceKey] = newNs
			cfgCopy.nsAssignedGroups[newNs] = EnsureMap(cfgCopy.nsAssignedGroups[newNs])
			cfgCopy.nsAssignedGroups[newNs][rname] = ng
		} else if rtype == resource.NodeType {
			node := m.Cfg.NodeMap[rname]
			if node == nil {
				logrus.Warnf("%s was assigned to namespace/%s but no longer exists, ignore", rid, newNs)
				continue
			}
			node.Properties[NamespaceKey] = newNs
			cfgCopy.nsAssignedNodes[newNs] = EnsureMap(cfgCopy.nsAssignedNodes[newNs])
			cfgCopy.nsAssignedNodes[newNs][rname] = node
		}
	}

	m.log.Infof("Updated systemToNsMap: %v", cfgCopy.systemToNsMap)
	m.log.Infof("Updated NsAssignedSystemNames: %v", cfgCopy.GetNsSysNameMap())
	m.log.Infof("Updated NsAssignedGroups: %v", mapValuesToKeyList(cfgCopy.nsAssignedGroups))
	m.log.Infof("Updated NsAssignedNodes: %v", mapValuesToKeyList(cfgCopy.nsAssignedNodes))
	m.Cfg = cfgCopy

	return m.Cfg
}

func (m *ClusterConfigMgr) updateNodeVersion(node *corev1.Node, newCfg *ClusterConfig) *ClusterConfig {
	cfg := newCfg
	if cfg == nil {
		cfg = m.Cfg.Copy()
	}
	cfgNode := cfg.NodeMap[node.Name]
	if cfgNode == nil {
		m.log.Warnf("node %s isn't in current cluster config, ignore till config reloaded", node.Name)
		return nil
	}
	updated := false
	// support single node to have multiple versions to be compatible
	if v := node.Labels[wsapisv1.PlatformVersionKey]; v != "" {
		for _, version := range ParseLabelVersions(v) {
			if _, ok := cfgNode.Properties[resource.PlatformVersionResKey(version)]; !ok {
				cfgNode.Properties[resource.PlatformVersionResKey(version)] = ""
				updated = true
			}
		}
	}
	if updated {
		m.Cfg = cfg
		return m.Cfg
	}
	return nil
}

// UpdateNodeHealth updates node health status and return new config if status changed
// newCfg: optional param, if passed in, will update status in newCfg instead of m.Cfg
func (m *ClusterConfigMgr) updateNodeHealth(node *corev1.Node, newCfg *ClusterConfig) *ClusterConfig {
	needReload := false
	cfg := newCfg
	if cfg == nil {
		cfg = m.Cfg.Copy()
	}
	cfgNode := cfg.NodeMap[node.Name]
	if cfgNode == nil {
		m.log.Warnf("node %s isn't in current cluster config, ignore till config reloaded", node.Name)
		return nil
	}

	healthy, errNICs, _ := GetNodeHealth(node, cfgNode)
	// Only BR, AX and IX support NIC level failure handling.
	// MemoryX does not need NIC health information for scheduling, but will do NIC assignment outside lock reconciler.
	if cfgNode.IsRailOptimized() || cfgNode.HasRole(RoleMemory) {
		nodeLevelError := false
		if !healthy && len(errNICs) == 0 {
			nodeLevelError = true
			if len(cfgNode.GetErrorNICs()) != 0 {
				logrus.Warnf("reload needed, node %s error escalated from NIC level to node level", node.Name)
				needReload = true
			}
		}
		for i := range cfgNode.NICs {
			nic := cfgNode.NICs[i]
			if nodeLevelError {
				nic.HasError = false
				continue
			}
			// we only reload if NIC status change for BR and AX due to changes need
			// the scheduler to be aware
			if errNICs[nic.Name] != nic.HasError {
				m.log.WithFields(logrus.Fields{
					"node":               cfgNode.Name,
					"nic":                nic.Name,
					"previousErrorState": nic.HasError,
					"currentErrorState":  errNICs[nic.Name],
				}).Warn(
					"nic error status changed",
				)
				nic.HasError = errNICs[nic.Name]
				if cfgNode.IsRailOptimized() {
					needReload = true
				}
			}
		}
	}

	// if detected unhealthy first time
	if _, ok := cfg.UnhealthyNodes[node.Name]; !ok && !healthy {
		if len(errNICs) > 0 {
			// keep node for link level failure
			// we will remove error NICs if it's node level failure
			m.log.Warnf("NICs error detected: %v", SortedKeys(errNICs))
		}
		m.log.Warnf("reload needed, unhealthy node %s detected", node.Name)
		cfg.UnhealthyNodes[node.Name] = cfgNode
		cfgNode.HasError = true
		needReload = true
	} else if ok && healthy {
		// if recovered back to healthy
		m.log.Infof("reload needed, node %s recovered to be healthy", node.Name)
		delete(cfg.UnhealthyNodes, node.Name)
		cfgNode.HasError = false
		needReload = true
	}

	if needReload {
		m.Cfg = cfg
		return m.Cfg
	}
	return nil
}

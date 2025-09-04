package common

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	zaplogfmt "github.com/sykesm/zap-logfmt"
	uzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource_config"
	// +kubebuilder:scaffold:imports
)

// Defaults are set to reflect a typical kind node
const (
	DefaultNodeCpu = 4500  // mcore
	DefaultNodeMem = 4500  // Mi
	LargeNodeMem   = 9000  // Mi
	XLargeNodeMem  = 18000 // Mi
)

type ClusterConfigK8sNodeParam struct {
	DefaultNodeCpu int64 // mcore
	DefaultNodeMem int64 // Mi
	LargeNodeMem   int64 // Mi
	XLargeNodeMem  int64 // Mi
}

func defaultK8sNodeParam() *ClusterConfigK8sNodeParam {
	return &ClusterConfigK8sNodeParam{
		DefaultNodeCpu: DefaultNodeCpu,
		DefaultNodeMem: DefaultNodeMem,
		LargeNodeMem:   LargeNodeMem,
		XLargeNodeMem:  XLargeNodeMem,
	}
}

var (
	scheme = runtime.NewScheme()

	// DefaultImage Loaded in before test start in Makefile: avoid hitting docker.io rate limit
	DefaultImage = wsapisv1.KubectlImage
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rlv1.AddToScheme(scheme))
	utilruntime.Must(wsapisv1.AddToScheme(scheme))
	utilruntime.Must(systemv1.AddToScheme(scheme))
	utilruntime.Must(namespacev1.AddToScheme(scheme))
}

func InitLogging() {
	configLog := uzap.NewProductionEncoderConfig()
	configLog.EncodeTime = func(ts time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(ts.UTC().Format(time.RFC3339Nano))
	}
	opts := zap.Options{
		Development: true,
		Encoder:     zaplogfmt.NewEncoder(configLog),
		Level:       zapcore.DebugLevel,
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
}

func EnvSetup(useExisting bool, t *testing.T) (ctrl.Manager, *envtest.Environment, context.Context, context.CancelFunc) {
	InitLogging()

	log.Log.Info("bootstrapping test environment")
	ParseEnvVar()
	resource_config.NodeReservedCpuDefault = *apiresource.NewQuantity(0, apiresource.DecimalSI)
	resource_config.NodeReservedMemDefault = *apiresource.NewQuantity(0, apiresource.BinarySI)
	testEnv := &envtest.Environment{
		ControlPlaneStartTimeout: 60 * time.Second,
		UseExistingCluster:       &useExisting,
		AttachControlPlaneOutput: false,
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
			filepath.Join("..", "..", "..", "..", "job-operator", "config", "crd", "bases"),
		},
	}
	if useExisting {
		// point to the config in case of context switch in CI env
		filePath := filepath.Join(os.Getenv("GITTOP"), "src", "cluster_mgmt", "src", "job-operator", ".kindconfig")
		config, _ := clientcmd.BuildConfigFromFlags("", filePath)
		testEnv.Config = config
	}

	var err error
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// +kubebuilder:scaffold:scheme
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err = mgr.Start(ctx)
		require.NoError(t, err)
	}()

	k8sClient := mgr.GetClient()
	require.NotNil(t, k8sClient)
	k8sReader := mgr.GetAPIReader()
	require.NotNil(t, k8sReader)

	_ = k8sClient.Create(ctx, &v1.Namespace{ObjectMeta: ctrl.ObjectMeta{Name: DefaultNamespace}})
	_ = k8sClient.DeleteAllOf(ctx, &wsapisv1.WSJob{}, client.InNamespace(DefaultNamespace))
	return mgr, testEnv, ctx, cancel
}

type clusterConfigBuilder struct {
	name       string
	systems    []*systemv1.SystemSpec
	nodes      []*Node
	nodeGroups []*NodeGroup
	v2Groups   []*NodeGroup
	numStamps  int
}

// NewClusterConfigBuilder creates a builder.
func NewClusterConfigBuilder(name string) *clusterConfigBuilder {
	return &clusterConfigBuilder{
		name: name,
	}
}

func (b *clusterConfigBuilder) BuildClusterSchema() ClusterSchema {
	c := ClusterSchema{
		Name:       b.name,
		Systems:    b.systems,
		Nodes:      b.nodes,
		Groups:     b.nodeGroups,
		V2Groups:   b.v2Groups,
		Properties: map[string]string{},
	}
	if wsapisv1.IsV2Network {
		c.Properties["topology"] = "v2"
	}
	return c
}

func (b *clusterConfigBuilder) BuildClusterConfig() *ClusterConfig {
	cfg := NewClusterConfig(b.BuildClusterSchema())
	cfg.BuildNodeSysGroupIndices()
	return cfg
}

func (b *clusterConfigBuilder) WithCoordNode() *clusterConfigBuilder {
	return b.withCoordNode(len(b.systems), len(b.nodes))
}

func (b *clusterConfigBuilder) WithCoordNodes(count int) *clusterConfigBuilder {
	for i := 0; i < count; i++ {
		b.withCoordNode(i, i)
	}
	return b
}

func (b *clusterConfigBuilder) withCoordNode(group, index int) *clusterConfigBuilder {
	name := fmt.Sprintf("%d-coord-%d", len(b.systems), len(b.nodes))
	addr := fmt.Sprintf("127.%d.0.%d", group, index)
	b.nodes = append(b.nodes, &Node{
		Name:   name,
		Role:   RoleCoordinator,
		HostIP: addr,
		Properties: map[string]string{
			"group": fmt.Sprintf("group%d", group),
		},
		NICs: []*NetworkInterface{
			{Name: "enp0", Addr: addr},
		},
	})
	return b
}

func (b *clusterConfigBuilder) WithAnyNode() *clusterConfigBuilder {
	name := fmt.Sprintf("any-%d", len(b.nodes))
	b.nodes = append(b.nodes, &Node{
		Name:   name,
		Role:   RoleAny,
		HostIP: "any-IP-" + name,
		Properties: map[string]string{
			"group": fmt.Sprintf("group%d", len(b.systems)),
		},
	})
	return b
}

func (b *clusterConfigBuilder) WithBR(count int) *clusterConfigBuilder {
	if wsapisv1.IsV2Network {
		// use 6 groups to simulate cases one leaf has multiple ports for one system
		for i := 0; i < 6; i++ {
			group := fmt.Sprintf("leaf-%d", i)
			props := map[string]string{}
			for j := 0; j < 2; j++ {
				port := i*2 + j
				for s := 0; s < len(b.systems); s++ {
					systemName := fmt.Sprintf("system-%d", s)
					props[systemv1.GetSysPortAffinityKey(systemName, port)] = ""
				}
			}
			b.v2Groups = append(b.v2Groups, &NodeGroup{
				Name:       group,
				Properties: props,
			})
		}
	}

	portCount := CSVersionSpec.NumPorts
	for i := 0; i < count; i++ {
		port := i % portCount
		var nics []*NetworkInterface
		for j := 0; j < 6; j++ {
			csPort := port
			// if count is not dividable by 12, it means top level BR will be in half modes
			// e.g. for 8CS2, it will have 30 BR nodes, BR24-BR29 will be connected with 2 CS ports separately
			if i >= count/portCount*portCount {
				csPort = (i-count/portCount*portCount)*2 + j/3
			}
			gbps := 100
			if wsapisv1.IsV2Network {
				// mock with 3 nics with 100, 200, 300 gbps
				if j >= 3 {
					break
				}
				csPort = -1
				gbps = (j + 1) * 100
			}
			nic := &NetworkInterface{
				Addr:   fmt.Sprintf("127.0.%d.%d", i, j),
				Name:   fmt.Sprintf("nic%d", j),
				CsPort: &csPort,
				BwGbps: &gbps,
			}
			nics = append(nics, nic)
		}
		props := map[string]string{resource.RoleBRPropKey: ""}
		b.nodes = append(b.nodes, &Node{
			Name:       fmt.Sprintf("br-%d", i),
			HostIP:     fmt.Sprintf("br-%d", i),
			Role:       RoleBroadcastReduce,
			Properties: props,
			NICs:       nics,
		})
	}
	return b
}

func (b *clusterConfigBuilder) WithAXInGroup(group string, count int) *clusterConfigBuilder {
	// CS4 has 4 NICs, CS2/CS3 have 2 NICs
	numNicsOnAx := 2
	if CSVersionSpec.Version == systemv1.CSVersion4 {
		numNicsOnAx = 4
	}
	startingIdx := -1
	for i := len(b.nodes) - 1; i >= 0; i-- {
		if strings.HasPrefix(b.nodes[i].Name, "ax") {
			tokens := strings.Split(b.nodes[i].Name, "-")
			idxStr := tokens[len(tokens)-1]
			startingIdx, _ = strconv.Atoi(idxStr)
			break
		}
	}
	for i := startingIdx + 1; i < startingIdx+1+count; i++ {
		var nics []*NetworkInterface
		for j := 0; j < numNicsOnAx; j++ {
			nics = append(nics, &NetworkInterface{
				Addr: fmt.Sprintf("127.1.%d.%d", i, j),
				Name: fmt.Sprintf("nic%d", j),
			})
		}
		node := &Node{
			Name:   fmt.Sprintf("ax-%d", i),
			HostIP: fmt.Sprintf("ax-%d", i),
			Role:   RoleActivation,
			Properties: map[string]string{
				resource.RoleAxPropKey:    "",
				resource.StampPropertyKey: group,
				NamespaceKey:              DefaultNamespace,
			},
			NICs: nics,
		}
		b.nodes = append(b.nodes, node)
	}
	return b
}

func (b *clusterConfigBuilder) WithIXInGroup(group string, count int) *clusterConfigBuilder {
	startingIdx := -1
	for i := len(b.nodes) - 1; i >= 0; i-- {
		if strings.HasPrefix(b.nodes[i].Name, "ix") {
			tokens := strings.Split(b.nodes[i].Name, "-")
			idxStr := tokens[len(tokens)-1]
			startingIdx, _ = strconv.Atoi(idxStr)
			break
		}
	}
	for i := startingIdx + 1; i < startingIdx+1+count; i++ {
		var nics []*NetworkInterface
		for j := 0; j < 2; j++ {
			nics = append(nics, &NetworkInterface{
				Addr: fmt.Sprintf("127.3.%d.%d", i, j),
				Name: fmt.Sprintf("nic%d", j),
			})
		}

		node := &Node{
			Name:   fmt.Sprintf("ix-%d", i),
			HostIP: fmt.Sprintf("ix-%d", i),
			Role:   RoleInferenceDriver,
			Properties: map[string]string{
				resource.RoleIxPropKey:    "",
				resource.StampPropertyKey: group,
				NamespaceKey:              DefaultNamespace,
			},
			NICs: nics,
		}
		b.nodes = append(b.nodes, node)
	}
	return b
}

func (b *clusterConfigBuilder) WithNumStamps(count int) *clusterConfigBuilder {
	b.numStamps = count
	return b
}

func (b *clusterConfigBuilder) WithGroup(memxCount, workerCount int) *clusterConfigBuilder {
	id := len(b.systems)
	sysName := fmt.Sprintf("system-%d", id)
	b.systems = append(b.systems, &systemv1.SystemSpec{
		Name:   sysName,
		Type:   string(CSVersionSpec.Version),
		CmAddr: fmt.Sprintf("192.168.0.%d:9000", id),
	})

	group := fmt.Sprintf("group%d", id)
	defaultNodeProps := func() map[string]string {
		return map[string]string{
			resource.GroupPropertyKey: group,
		}
	}

	nicCount := 2
	b.nodeGroups = append(b.nodeGroups, &NodeGroup{
		Name:       group,
		Properties: map[string]string{resource.GroupPropertyKey: group},
	})
	for j := 0; j < workerCount; j++ {
		var nics []*NetworkInterface
		for k := 1; k <= nicCount; k++ {
			nics = append(nics, &NetworkInterface{
				Addr: fmt.Sprintf("127.%d.%d.%d", len(b.nodeGroups)+2, j, k),
				Name: fmt.Sprintf("nic%d", k),
			})
		}
		b.nodes = append(b.nodes, &Node{
			Name:       fmt.Sprintf("%s-worker-%d", group, j),
			Role:       RoleWorker,
			HostIP:     fmt.Sprintf("%s-worker-%d-IP", group, j),
			NICs:       nics,
			Properties: defaultNodeProps(),
		})
	}
	for j := 0; j < memxCount; j++ {
		var nics []*NetworkInterface
		for k := 1; k <= nicCount; k++ {
			nics = append(nics, &NetworkInterface{
				Addr: fmt.Sprintf("127.%d.%d.%d", len(b.nodeGroups)+3, j, k),
				Name: fmt.Sprintf("nic%d", k),
			})
		}
		b.nodes = append(b.nodes, &Node{
			Name:       fmt.Sprintf("%s-memory-%d", group, j),
			Role:       RoleMemory,
			HostIP:     fmt.Sprintf("%s-memory-%d-IP", group, j),
			NICs:       nics,
			Properties: defaultNodeProps(),
		})
	}
	return b
}

func (b *clusterConfigBuilder) WithSysMemxWorkerGroups(groups, memxPerGroup, workersPerGroup int) *clusterConfigBuilder {
	for i := 0; i < groups; i++ {
		b.WithGroup(memxPerGroup, workersPerGroup)
	}
	return b
}

func NewClusterConfigMgr(mgr manager.Manager) *ClusterConfigMgr {
	return &ClusterConfigMgr{
		reader: mgr.GetAPIReader(),
		client: mgr.GetClient(),
		config: mgr.GetConfig(),
		log:    logrus.WithField("component", "ClusterConfigMgr"),
	}
}

func NewClusterConfigMgrFromSchema(mgr manager.Manager, schema ClusterSchema) *ClusterConfigMgr {
	m := &ClusterConfigMgr{
		reader: mgr.GetAPIReader(),
		client: mgr.GetClient(),
		config: mgr.GetConfig(),
		log:    logrus.WithField("component", "ClusterConfigMgr"),
	}

	// create k8s nodes so resource overhead info is calculated
	if err := CreateK8sNodes(schema, mgr.GetClient()); err != nil {
		panic("failed to create k8s nodes: " + err.Error())
	}

	err := m.Initialize(schema)
	if err != nil {
		panic("failed to reload config: " + err.Error())
	}
	return m
}

func NewTestClusterConfigSchema(systemCount, coordCount, brCount, axCount, ixCount, workersPerGroup, memorysPerGroup int) ClusterSchema {
	builder := NewClusterConfigBuilder("test").WithCoordNodes(coordCount)
	if wsapisv1.IsV2Network && wsapisv1.UseAxScheduling && axCount > 0 {
		// at most 4 populated nodegroup for v2 network - test parallel jobs when possible
		numPopGroups := Ternary(systemCount > 4, 4, systemCount)
		builder = builder.WithSysMemxWorkerGroups(numPopGroups, memorysPerGroup, workersPerGroup)
		if systemCount > numPopGroups {
			// depop nodegroups in v2 network do not contain any memx nodes
			builder = builder.WithSysMemxWorkerGroups(systemCount-numPopGroups, 0, workersPerGroup)
		}
	} else {
		// all populated nodegroups for v1 network
		builder = builder.WithSysMemxWorkerGroups(systemCount, memorysPerGroup, workersPerGroup)
	}

	builder = builder.WithBR(brCount).WithAXInGroup("", axCount).WithIXInGroup("", ixCount)

	return builder.BuildClusterSchema()
}

// SplitNStamps recreates v2 nodegroups and re-assign system-port affinities in the following manner:
// 1. Split system pools into N stamps
// 2. Create N*k switches where each stamp has k switches
// 3. For each system in every stamp, splits the port affinity k-way and update v2group/switch properties
// 4. For each AX node in every stamp, round-robin v2group/switch assignment on node nics
// Example outcome (2 systems/ax per stamp):
// ax-0, nic-0 -> system-{0,4}-cs-port-{0,1,2}, nic-1 -> system-{0,4}-cs-port-{6,7,8}
// ax-4, nic-0 -> system-{0,4}-cs-port-{3,4,5}, nic-1 -> system-{0,4}-cs-port-{9,10,11}
func (cs ClusterSchema) SplitNStamps(stampCount, switchesPerStamp int) ClusterSchema {

	portCount := CSVersionSpec.NumPorts

	if stampCount == 0 {
		// no stamp - only fake systems were added
		for _, v2Group := range cs.V2Groups {
			// removing sys port affinities
			v2Group.Properties = nil
		}
		return cs
	}

	systemsInStamps := map[string][]*systemv1.SystemSpec{}
	nextAssignPortInStamp := map[string]int{}
	for i := range cs.Systems {
		cs.Systems[i].Properties = EnsureMap(cs.Systems[i].Properties)
		groupVal := strconv.Itoa(i % stampCount)
		systemsInStamps[groupVal] = append(systemsInStamps[groupVal], cs.Systems[i])
		nextAssignPortInStamp[groupVal] = 0
	}

	// POR has twelve switches per stamp
	// This value represents how many switches a system should split its ports to,
	// for example, 2 means a split of 6-6, 3 means 4-4-4, 4 means 3-3-3-3, etc
	switchesRequired := switchesPerStamp * stampCount

	// Rebuild switches and sys-port affinities
	cs.V2Groups = []*NodeGroup{}
	v2GroupMap := map[string]*NodeGroup{}
	v2StampMap := map[string]map[string]*NodeGroup{}
	for i := 0; i < switchesRequired; i++ {
		groupVal := strconv.Itoa(i / switchesPerStamp)
		switchName := fmt.Sprintf("leaf-%s-%d", groupVal, i%switchesPerStamp)
		if _, ok := v2GroupMap[switchName]; !ok {
			v2GroupMap[switchName] = &NodeGroup{
				Name:       switchName,
				Properties: map[string]string{},
			}
		}

		if _, ok := v2StampMap[groupVal]; !ok {
			v2StampMap[groupVal] = map[string]*NodeGroup{}
		}
		v2StampMap[groupVal][switchName] = v2GroupMap[switchName]

		j := nextAssignPortInStamp[groupVal]
		for _, sys := range systemsInStamps[groupVal] {
			for j = nextAssignPortInStamp[groupVal]; j < nextAssignPortInStamp[groupVal]+portCount/switchesPerStamp; j++ {
				affinityKey := fmt.Sprintf("%s-%d", sys.Name, j)
				v2GroupMap[switchName].Properties[affinityKey] = ""
			}
		}
		nextAssignPortInStamp[groupVal] = j
	}

	for _, k := range SortedKeys(v2GroupMap) {
		cs.V2Groups = append(cs.V2Groups, v2GroupMap[k])
	}

	// Add sys-port affinities to node or node nic level
	var axNodes []*Node
	var ixNodes []*Node
	var sxNodes []*Node
	axNodeCountInStamp := map[string]int{}
	ixNodeCountInStamp := map[string]int{}
	sxNodeCountInStamp := map[string]int{}
	for i := range cs.Nodes {
		cs.Nodes[i].Properties = EnsureMap(cs.Nodes[i].Properties)
		if _, ok := cs.Nodes[i].Properties[resource.RoleAxPropKey]; ok {
			groupVal := strconv.Itoa(len(axNodes) % stampCount)
			switchesInStamp := SortedKeys(v2StampMap[groupVal])
			for idx := 0; idx < len(cs.Nodes[i].NICs); idx++ {
				cs.Nodes[i].NICs[idx].V2Group = switchesInStamp[(axNodeCountInStamp[groupVal]*len(cs.Nodes[i].NICs)+idx)%len(switchesInStamp)]
			}
			axNodes = append(axNodes, cs.Nodes[i])
			axNodeCountInStamp[groupVal]++
		} else if _, ok := cs.Nodes[i].Properties[resource.RoleIxPropKey]; ok {
			groupVal := strconv.Itoa(len(ixNodes) % stampCount)
			switchesInStamp := SortedKeys(v2StampMap[groupVal])

			// For IX nodes, ensure at least one NIC is affined to domain 3
			// Domain 3 switches are those that contain port affinities for ports 9, 10, 11
			// For CS4 logic, infer totalPorts and numDomains from definied variables
			// For CS2/CS3 logic, we assume 12 total ports and 4 domains
			domain3Switches, otherSwitches := partitionSwitchesByDomain(switchesInStamp, v2GroupMap, 3, 12, 4)

			// Assign first NIC to other domain, second NIC to domain 3 switch
			if len(domain3Switches) > 0 {
				if len(otherSwitches) > 0 {
					cs.Nodes[i].NICs[0].V2Group = otherSwitches[ixNodeCountInStamp[groupVal]%len(otherSwitches)]
					cs.Nodes[i].NICs[1].V2Group = domain3Switches[ixNodeCountInStamp[groupVal]%len(domain3Switches)]
				} else {
					// If no other domain switches, use different domain 3 switches
					cs.Nodes[i].NICs[0].V2Group = domain3Switches[ixNodeCountInStamp[groupVal]%len(domain3Switches)]
					cs.Nodes[i].NICs[1].V2Group = domain3Switches[(ixNodeCountInStamp[groupVal]+1)%len(domain3Switches)]
				}
			}

			ixNodes = append(ixNodes, cs.Nodes[i])
			ixNodeCountInStamp[groupVal]++
		} else if _, ok = cs.Nodes[i].Properties[resource.RoleBRPropKey]; ok {
			groupVal := strconv.Itoa(len(sxNodes) % stampCount)
			switchesInStamp := SortedKeys(v2StampMap[groupVal])
			cs.Nodes[i].Properties[resource.V2GroupPropertyKey] = switchesInStamp[sxNodeCountInStamp[groupVal]%len(switchesInStamp)]
			sxNodes = append(sxNodes, cs.Nodes[i])
			sxNodeCountInStamp[groupVal]++
		}
	}
	return cs
}

// partitionSwitchesByDomain separates switches into those that have the specified domain port affinities
// and those that don't. For example, domain 3 with 12 total ports and 4 domains would check ports 9, 10, 11.
func partitionSwitchesByDomain(switchesInStamp []string, v2GroupMap map[string]*NodeGroup, targetDomain, totalPorts, numDomains int) (domainSwitches, nonDomainSwitches []string) {
	// Calculate the port numbers for the target domain
	portsPerDomain := totalPorts / numDomains
	startPort := targetDomain * portsPerDomain
	endPort := startPort + portsPerDomain

	for _, switchName := range switchesInStamp {
		isDomainSwitch := false
		for portAffinity := range v2GroupMap[switchName].Properties {
			for port := startPort; port < endPort; port++ {
				if strings.HasSuffix(portAffinity, fmt.Sprintf("-%d", port)) {
					isDomainSwitch = true
					break
				}
			}
			if isDomainSwitch {
				break
			}
		}
		if isDomainSwitch {
			domainSwitches = append(domainSwitches, switchName)
		} else {
			nonDomainSwitches = append(nonDomainSwitches, switchName)
		}
	}
	return domainSwitches, nonDomainSwitches
}

func NewTestClusterConfigMgr(mgr manager.Manager, systemCount, brCount, axCount, workersPerGroup, memorysPerGroup int) *ClusterConfigMgr {
	schema := NewClusterConfigBuilder("test").
		WithCoordNode().
		WithSysMemxWorkerGroups(systemCount, memorysPerGroup, workersPerGroup).
		WithBR(brCount).
		WithAXInGroup("", axCount).
		BuildClusterSchema()
	return NewClusterConfigMgrFromSchema(mgr, schema)
}

func CreateK8sNodes(cfg ClusterSchema, apiClient client.Client) error {
	_, err := CreateK8sNodesWithExplicitNodeParam(cfg, apiClient, nil)
	return err
}

func CreateK8sNodesWithExplicitNodeParam(cfg ClusterSchema, apiClient client.Client, k8sNodeParam *ClusterConfigK8sNodeParam) (*v1.NodeList, error) {
	nodeList := &v1.NodeList{}

	groupMap := map[string]*NodeGroup{}
	for _, group := range cfg.Groups {
		groupMap[group.Name] = group
	}

	for _, node := range cfg.Nodes {
		var group *NodeGroup
		if node.Properties[resource.GroupPropertyKey] != "" {
			group = groupMap[node.Properties[resource.GroupPropertyKey]]
		}
		k8Node := CreateK8sNode(node, group, k8sNodeParam)
		nodeList.Items = append(nodeList.Items, *k8Node)

		err := apiClient.Create(context.TODO(), k8Node)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
	}
	return nodeList, nil
}

func CreateK8sNode(node *Node, group *NodeGroup, k8sNodeParam *ClusterConfigK8sNodeParam) *v1.Node {
	// Use the default
	if k8sNodeParam == nil {
		k8sNodeParam = defaultK8sNodeParam()
	}

	defaultResources := v1.ResourceList{
		v1.ResourceCPU:    *apiresource.NewMilliQuantity(k8sNodeParam.DefaultNodeCpu, apiresource.DecimalExponent),
		v1.ResourceMemory: *apiresource.NewQuantity(k8sNodeParam.DefaultNodeMem<<20, apiresource.BinarySI),
	}
	if node.Role == RoleAny {
		defaultResources = v1.ResourceList{
			v1.ResourceCPU:    *apiresource.NewMilliQuantity(k8sNodeParam.DefaultNodeCpu*8, apiresource.DecimalExponent),
			v1.ResourceMemory: *apiresource.NewQuantity(k8sNodeParam.DefaultNodeMem<<23, apiresource.BinarySI),
		}
	} else if group != nil && group.IncludesLargeMemxNodes() {
		defaultResources = v1.ResourceList{
			v1.ResourceCPU:    *apiresource.NewMilliQuantity(k8sNodeParam.DefaultNodeCpu, apiresource.DecimalExponent),
			v1.ResourceMemory: *apiresource.NewQuantity(k8sNodeParam.LargeNodeMem<<20, apiresource.BinarySI),
		}
	} else if group != nil && group.IncludesXLargeMemxNodes() {
		defaultResources = v1.ResourceList{
			v1.ResourceCPU:    *apiresource.NewMilliQuantity(k8sNodeParam.DefaultNodeCpu, apiresource.DecimalExponent),
			v1.ResourceMemory: *apiresource.NewQuantity(k8sNodeParam.XLargeNodeMem<<20, apiresource.BinarySI),
		}
	}
	var addresses []v1.NodeAddress
	if len(node.NICs) > 0 {
		addresses = append(addresses, v1.NodeAddress{Address: node.NICs[0].Addr, Type: v1.NodeInternalIP})
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: node.Name,
			Labels: map[string]string{
				GroupLabelKey:           node.GetGroup(),
				node.Role.AsNodeLabel(): "",
			},
		},
		Status: v1.NodeStatus{
			Addresses:   addresses,
			Capacity:    defaultResources,
			Allocatable: defaultResources,
		},
	}
}

type MockK8SClient struct {
	mock.Mock
	MockSubClient
}
type MockSubClient struct {
	mock.Mock
}

func (m *MockK8SClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	args := m.Called(ctx, key, obj)
	return args.Error(0)
}

func (m *MockK8SClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	args := m.Called(ctx, list, opts)
	return args.Error(0)
}

func (m *MockK8SClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockK8SClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockK8SClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockK8SClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	args := m.Called(ctx, obj, patch, opts)
	return args.Error(0)
}

func (m *MockK8SClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockK8SClient) Scheme() *runtime.Scheme {
	panic("implement me")
}

func (m *MockK8SClient) RESTMapper() meta.RESTMapper {
	panic("implement me")
}

func (m *MockK8SClient) Status() client.StatusWriter {
	return &m.MockSubClient
}

func (m *MockK8SClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	panic("implement me")
}

func (m *MockK8SClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	panic("implement me")
}

func (m *MockK8SClient) SubResource(subResource string) client.SubResourceClient {
	panic("implement me")
}

func (m *MockSubClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockSubClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	args := m.Called(ctx, obj, opts)
	return args.Error(0)
}

func (m *MockSubClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	args := m.Called(ctx, obj, patch, opts)
	return args.Error(0)
}

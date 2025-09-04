/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	commonapisv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common/resource"
)

// NodeRole defines the expected usage of the node in the cluster. It implies a specialization of the hardware and
// network characteristics of the physical nodes.
type NodeRole string

const (
	RoleAny                      = "any"
	RoleBroadcastReduce NodeRole = "broadcastreduce"
	RoleManagement      NodeRole = "management"
	RoleCoordinator     NodeRole = "coordinator"
	RoleMemory          NodeRole = "memory"
	RoleWorker          NodeRole = "worker"
	RoleActivation      NodeRole = "activation"
	RoleInferenceDriver NodeRole = "inferencedriver"
)

func (r NodeRole) AsMatcherProperty() map[string][]string {
	return map[string][]string{r.AsMatcherPropertyValue(): {""}}
}

func (r NodeRole) AsMatcherPropertyValue() string {
	return "role-" + string(r)
}

func (r NodeRole) AsNodeLabel() string {
	return fmt.Sprintf("k8s.cerebras.com/node-role-%s", r)
}

func (r NodeRole) String() string {
	return string(r)
}

// Node k8s node info
type Node struct {
	Role   NodeRole            `json:"role"`
	Name   string              `json:"name"`
	HostIP string              `json:"-"`
	NICs   []*NetworkInterface `json:"networkInterfaces"`

	// Optional properties that describe the node. One such property is 'group' and if present, maps to a NodeGroup.
	Properties map[string]string `json:"properties"`

	// Single role could expand to multiple roles, e.g. mgmt=>mgmt/crd, any=>worker/mem/br
	ExpandedRoles []NodeRole `json:"-"`

	// HasError means node has either internal error or NIC errors
	HasError bool `json:"-"`
}

func (n *Node) GetName() string {
	return n.Name
}

func (n *Node) IsRailOptimized() bool {
	return n.HasRole(RoleBroadcastReduce) || n.HasRole(RoleActivation) || n.HasRole(RoleInferenceDriver)
}

func (n *Node) GetAvailableCsPorts() (map[int][]string, map[string]int) {
	if !n.IsRailOptimized() {
		return nil, nil
	}
	portNicMap := map[int][]string{}
	nicBwMap := map[string]int{}
	for _, nic := range n.NICs {
		if !nic.HasError {
			if wsapisv1.IsV2Network || nic.CsPort == nil || *nic.CsPort < 0 {
				portNicMap[-1] = append(portNicMap[-1], nic.Name)
			} else {
				portNicMap[*nic.CsPort] = append(portNicMap[*nic.CsPort], nic.Name)
			}
			if nic.BwGbps != nil {
				nicBwMap[nic.Name] = *nic.BwGbps
			}
		}
	}
	return portNicMap, nicBwMap
}

// get total bw in unit of 100gbps, e.g. 1600gbps => 16
func (n *Node) GetTotalBw() int {
	var res int
	for _, nic := range n.NICs {
		if nic.BwGbps == nil {
			res++
		} else {
			res += *nic.BwGbps / resource.DefaultNicBW
		}
	}
	return res
}

func (n *Node) GetErrorNICs() []string {
	var res []string
	for _, nic := range n.NICs {
		if nic.HasError {
			res = append(res, nic.Name)
		}
	}
	return res
}

func (n *Node) GetNicAddress(name string) string {
	for _, nic := range n.NICs {
		if nic.Name == name {
			return nic.Addr
		}
	}
	// should not happen
	logrus.Infof("NIC name %s not found in node %s", name, n.Name)
	return ""
}

func (n *Node) GetNadByNIC(name string) string {
	for i, nic := range n.NICs {
		if nic.Name == name {
			return GetNadByIndex(i)
		}
	}
	// should not happen
	logrus.Infof("NIC name %s not found in node %s", name, n.Name)
	return ""
}

func (n *Node) GetNICByNad(nad string) string {
	if nad == DataNetAttachDef {
		if len(n.NICs) < 1 {
			logrus.Infof("NAD name %s not found in node %s", nad, n.Name)
			return "" // should not happen
		}
		return n.NICs[0].Name
	} else if nad == SecondNadLegacy {
		if len(n.NICs) < 2 {
			logrus.Infof("NAD name %s not found in node %s", nad, n.Name)
			return "" // should not happen
		}
		return n.NICs[1].Name
	}
	var i int
	cnt, err := fmt.Sscanf(nad, DataNetAttachDefFormat, &i)
	if err != nil || cnt != 1 || i < 1 || len(n.NICs) <= i {
		logrus.Infof("NAD name %s not found in node %s", nad, n.Name)
		// should not happen
		return ""
	}
	return n.NICs[i].Name
}

// check if NIC errors can be ignored for scheduling purpose
// i.e. memX node secondary NIC is not in part of the scheduling cycle
func (n *Node) ShouldIgnoreNICErrors(errNICsToCheck map[string]bool) bool {
	return n.HasRole(RoleMemory) && len(n.NICs) > 0 && !errNICsToCheck[n.NICs[0].Name]
}

func (n *Node) GetGroup() string {
	return n.Properties[resource.GroupPropertyKey]
}

func (n *Node) GetV2Group() string {
	v2g := n.Properties[resource.V2GroupPropertyKey]
	if v2g != "" {
		return v2g
	}
	// fallback to v1 for backwards compatible
	return n.GetGroup()
}

func (n *Node) GetV2GroupForNodeNic(nicName string) string {
	for _, nic := range n.NICs {
		if nic.Name == nicName && nic.V2Group != "" {
			return nic.V2Group
		}
	}
	return n.GetV2Group()
}

func (n *Node) GetStamp() string {
	v2g := n.Properties[resource.StampPropertyKey]
	if v2g != "" {
		return v2g
	}
	// fallback to v1 for backwards compatible
	return n.GetGroup()
}

func (n *Node) GetRole() string {
	return string(n.Role)
}

func (n *Node) HasRole(r NodeRole) bool {
	if n.Role == r {
		return true
	}
	for _, role := range n.ExpandedRoles {
		if role == r {
			return true
		}
	}
	return false
}

// returns whether node can be considered as schedulable
// e.g. NIC error may not block scheduling
func (n *Node) IsSchedulable() (string, bool) {
	// return true if no error
	if !n.HasError {
		return resource.HealthOk, true
	}
	// if err, either node error or NIC error
	// no NIC errors indicates node level error which is blocking
	if len(n.GetErrorNICs()) == 0 {
		return resource.HealthErr, false
	}
	if len(n.GetErrorNICs()) == len(n.NICs) {
		return resource.HealthErr, false
	}
	// for NIC level errors, only MemX primary NIC error will block scheduling
	if n.HasRole(RoleMemory) && len(n.NICs) > 0 && n.NICs[0].HasError {
		return resource.HealthErr, false
	}
	return resource.HealthDegraded, true
}

func (n *Node) GetExpandedRoles() map[string]bool {
	roles := make(map[string]bool)
	for _, role := range n.ExpandedRoles {
		roles[string(role)] = true
	}
	return roles
}

func (n *Node) GetProperties() map[string]string {
	return n.Properties
}

func (n *Node) IsDedicatedManagementNode() bool {
	return wsapisv1.ManagementSeparationEnforced && n.GetProperties()[StorageTypeKey] == CephStorageType
}

func (n *Node) IsDedicatedCoordinatorNode() bool {
	return wsapisv1.ManagementSeparationEnforced && n.Role == RoleManagement && n.GetProperties()[StorageTypeKey] != CephStorageType
}

func (n *Node) sortNICs() {
	sort.Slice(n.NICs, func(i, j int) bool {
		// group NICs with the same cs port together
		if n.NICs[i].CsPort != nil && n.NICs[j].CsPort != nil {
			return *n.NICs[i].CsPort < *n.NICs[j].CsPort
		}
		return n.NICs[i].Name < n.NICs[j].Name
	})
}

func (n *Node) GetCsPortsInAffinity(cfg *ClusterConfig) []int {
	ports := map[int]bool{}
	if n != nil && cfg != nil {
		for _, nic := range n.NICs {
			if g := cfg.V2GroupMap[nic.V2Group]; g != nil {
				for _, port := range g.GetCsPortsInAffinity() {
					ports[port] = true
				}
			}
		}
	}
	return SortedKeys(ports)
}

type Nodes []Node

type NetworkInterface struct {
	Name string `json:"name"`
	Addr string `json:"address"`
	// Optional CsPort for v1 BR which only connects with one cs port
	CsPort *int `json:"csPort,omitempty"`
	// Optional BwGbps for v2 consolidated BR NIC which indicates the max number of BRs to be scheduled on this NIC
	BwGbps   *int `json:"gbps,omitempty"`
	HasError bool `json:"-"`
	// Optional group property to indicate which switch the nic is connected to
	// If this group property is not presented, it means all nics from the belonging
	// node should be connected to a single switch and the same information can
	// be found in node.properties.group.
	V2Group string `json:"v2Group,omitempty"`

	Properties map[string]string `json:"-"`
}

type NodeGroup struct {
	Name         string            `json:"name"`
	Properties   map[string]string `json:"properties"`
	SwitchConfig map[string]string `json:"switchConfig"`
}

func (g *NodeGroup) IsPopulatedNodegroup() bool {
	val, _ := g.Properties[wsapisv1.MemxPopNodegroupProp]
	return val == strconv.FormatBool(true)
}

func (g *NodeGroup) IncludesRegularMemxNodes() bool {
	val, _ := g.Properties[wsapisv1.MemxMemClassProp]
	return val == wsapisv1.PopNgTypeRegular
}

func (g *NodeGroup) IncludesLargeMemxNodes() bool {
	val, _ := g.Properties[wsapisv1.MemxMemClassProp]
	return val == wsapisv1.PopNgTypeLarge
}

func (g *NodeGroup) IncludesXLargeMemxNodes() bool {
	val, _ := g.Properties[wsapisv1.MemxMemClassProp]
	return val == wsapisv1.PopNgTypeXLarge
}

func (g *NodeGroup) GetCsPortsInAffinity() []int {
	var ports []int
	if g != nil && g.Properties[resource.PreferredCsPortsPropKey] != "" {
		for _, portStr := range strings.Split(g.Properties[resource.PreferredCsPortsPropKey], ",") {
			if portInt, err := strconv.Atoi(portStr); err == nil {
				ports = append(ports, portInt)
			}
		}
	}
	return ports
}

const (
	CompileJobNodeResource = "cerebras.com/compile-job"

	OriginRoleKey = "origin-role"

	// Original group assignment. Tracked for mgmt nodes so node labeling can still occur which is important in the
	// case of system-namespace job-operator running 2.2+ while user namespaces running <=2.1 since a nodegroup
	// selector is set at scheduling time. Tracking origin role/labeling can be removed once user namespace job-operators
	// all running 2.2+
	OriginGroupKey = "origin-group"
)

// ClusterSchema mirrors the schema of the clusterConfiguration.yaml file
type ClusterSchema struct {
	// cluster configmap resource version
	ResourceVersion string `json:"-"`

	// Name is the name of the cluster, e.g. "mb-system0"
	Name string `json:"name"`

	// Arbitrary properties of the cluster, e.g. "serviceDomain": "xxx.example.com"
	Properties map[string]string `json:"properties"`

	// ServiceDomain is the prefix for common cluster endpoints such as the internal registry and cluster-server.
	// Deprecated. Prefer looking this up from the properties field.
	ServiceDomain string `json:"serviceDomain,omitempty"`

	Systems  []*systemv1.SystemSpec `json:"systems"`
	Nodes    []*Node                `json:"nodes"`
	Groups   []*NodeGroup           `json:"groups"`
	V2Groups []*NodeGroup           `json:"v2Groups,omitempty"`
}

func NewClusterSchemaFromCM(cm *corev1.ConfigMap) (ClusterSchema, error) {
	if cm == nil {
		return ClusterSchema{}, nil
	}
	c := ClusterSchema{ResourceVersion: cm.ResourceVersion}
	if err := yaml.Unmarshal([]byte(cm.Data[ClusterConfigFile]), &c); err != nil {
		return c, err
	}

	return c, nil
}

// ClusterConfig is the cluster level resources for wsJob and resourceLock controllers and indices over them.
type ClusterConfig struct {
	ClusterSchema

	// index from name
	SystemMap  map[string]*systemv1.SystemSpec
	NodeMap    map[string]*Node
	GroupMap   map[string]*NodeGroup
	V2GroupMap map[string]*NodeGroup

	// system name to system
	UnhealthySystems map[string]*systemv1.SystemSpec
	// node name to node
	UnhealthyNodes map[string]*Node

	// for sharing systems with multiple NS
	systemToNsMap     map[string]string
	nsAssignedSystems map[string]map[string]*systemv1.SystemSpec
	nsAssignedNodes   map[string]map[string]*Node
	nsAssignedGroups  map[string]map[string]*NodeGroup
	nsProperties      map[string]map[string]string

	// index to coordinator nodes
	crdNodeMap map[string]*Node

	// BrIpNodeMap index BR NIC IP to BRNode.
	BrIpNodeMap map[string]*Node

	K8sNodeMap map[string]*corev1.Node
	Resources  *resource.Collection
}

func NewClusterConfig(c ClusterSchema) *ClusterConfig {
	cfg := &ClusterConfig{
		ClusterSchema:    c,
		UnhealthyNodes:   map[string]*Node{},
		UnhealthySystems: map[string]*systemv1.SystemSpec{},
		K8sNodeMap:       map[string]*corev1.Node{},
	}

	sort.Slice(c.Systems, func(i, j int) bool {
		return c.Systems[i].Name < c.Systems[j].Name
	})
	sort.Slice(c.Nodes, func(i, j int) bool {
		return c.Nodes[i].Name < c.Nodes[j].Name
	})

	// default with at least one group to ensure nodes assign to same NS of system
	if len(cfg.Groups) == 0 {
		cfg.Groups = append(cfg.Groups, &NodeGroup{
			Name:       "group0",
			Properties: map[string]string{},
		})
		// skip group selector since it's not actually in config
		DisableNodeGroupScheduling = true
		logrus.Infof("No groups found, disableNodeGroupScheduling set to true")
	}
	for _, group := range cfg.Groups {
		group.Properties = EnsureMap(group.Properties)
		if group.Properties[resource.SwitchPropertyKey] != "" {
			GroupSwitchInfoMetricAdded(group.Name, group.Properties[resource.SwitchPropertyKey])
		}
	}

	numMgmtNodes := 0
	for _, node := range cfg.Nodes {
		if node.Properties == nil {
			node.Properties = map[string]string{}
		}
		if node.Role == RoleManagement {
			numMgmtNodes += 1
		}
		for _, nic := range node.NICs {
			DataNetworkInterfaceMetricAdded(node.Name, nic.Name, node.GetGroup())
			// we use all NICs on BRs, depoped MemoryX and AX nodes
			if node.Role != RoleBroadcastReduce && node.Role != RoleMemory && node.Role != RoleActivation && node.Role != RoleInferenceDriver {
				break
			}
		}
		node.sortNICs()
		NodeRoleMetricAdded(node.Name, node.GetGroup(), node.Role.String())
	}
	wsapisv1.IsMultiBox = len(cfg.Nodes) > 1 && len(cfg.Systems) > 1
	wsapisv1.IsMultiMgmt = numMgmtNodes > 1
	if !wsapisv1.IsMultiMgmt {
		wsapisv1.ManagementSeparationEnforced = false
		wsapisv1.PromUrl = fmt.Sprintf("http://%s.%s.svc:9090",
			wsapisv1.PrometheusSvc, wsapisv1.PrometheusNs)
	} else {
		wsapisv1.PromUrl = fmt.Sprintf("http://%s.%s.svc:9090",
			wsapisv1.ThanosQuerySvc, wsapisv1.PrometheusNs)
	}
	logrus.Infof("Management separation enforced: %t, node count: %d, sys count: %d",
		wsapisv1.ManagementSeparationEnforced, len(cfg.Nodes), len(cfg.Systems))

	CSVersionSpec = systemv1.DetectCSVersionSpec(cfg.Systems)

	cfg.BuildNodeSysGroupIndices()
	return cfg
}

// generateBrNICs adds NICs that allow for running full BR test on a single node
func (cfg *ClusterConfig) generateBrNICs() {
	for _, node := range cfg.Nodes {
		if node.Properties["generateBrNICs"] != "true" {
			continue
		}
		if len(node.NICs) > 0 {
			logrus.Warnf("node %s requested generateBrNICs but already had networkInterfaces defined, skipping", node.Name)
			continue
		}

		logrus.Infof("node %s requested to generate BR NICs", node.Name)
		for i := 0; i < 6*12; i++ {
			port := i % 12
			node.NICs = append(node.NICs, &NetworkInterface{
				Name:   fmt.Sprintf("nic-%d", i),
				Addr:   node.HostIP,
				CsPort: &port,
			})
		}
		node.sortNICs()
	}
}

func (cfg *ClusterConfig) CheckV2Network() {
	disableV2NetworkScheduling := func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
		wsapisv1.UseIxScheduling = false
		wsapisv1.RedundantBrPortBwPerSystem = 0
	}

	// we calculate preferred port for v1 network as well, based on v2Group populated for `csctl cluster topology` query
	v2GroupMap := map[string]*NodeGroup{}
	// construct system connectivity group property based on group property
	// for example: m["systemf0"][7] = "leaf-sw-0", where 7 stands for cs-port-7
	sysPortToV2GroupMap := map[string]map[int]string{}
	for _, g := range cfg.V2Groups {
		v2GroupMap[g.Name] = g
		newAffinityMap := map[string]string{}
		csPortsInAffinity := map[int]bool{}
		for k := range g.Properties {
			// This is to enable more clear lookup for port affinity
			// We may want to push this to the CLI side if we need more properties on the v2Groups.
			k = strings.TrimPrefix(k, resource.PortAffinityPropKeyPrefix+"/")

			// for test compatible where sys name can be "system-0"
			id := strings.LastIndex(k, "-")
			if id >= 0 {
				sys := k[:id]
				p := k[id+1:]
				sysPortToV2GroupMap[sys] = EnsureMap(sysPortToV2GroupMap[sys])
				port, _ := strconv.Atoi(p)
				sysPortToV2GroupMap[sys][port] = g.Name
				csPortsInAffinity[port] = true

				newAffinityMap[fmt.Sprintf("%s/%s-%s", resource.PortAffinityPropKeyPrefix, sys, p)] = ""
				delete(g.Properties, k)
			}
		}
		maps.Copy(g.Properties, newAffinityMap)

		if len(csPortsInAffinity) > 0 {
			var sortedPreferredPorts []string
			for _, p := range SortedKeys(csPortsInAffinity) {
				sortedPreferredPorts = append(sortedPreferredPorts, strconv.Itoa(p))
			}
			g.Properties = EnsureMap(g.Properties)
			g.Properties[resource.PreferredCsPortsPropKey] = strings.Join(sortedPreferredPorts, ",")
		}
	}

	if cfg.Properties[resource.TopologyPropertyKey] == "v2" {
		wsapisv1.IsV2Network = true
		logrus.Info("V2 network detected.")

		// for example: m["7:leaf-sw-0, 8:leaf-sw-1, ..."] = []string{"systemf0", "systemf1", ...},
		// where "7:leaf-sw-0, 8:leaf-sw-1, ..." is the group signature
		stampSignatureToSystemList := map[string][]string{}
		// for example: m["leaf-sw-0"] = "7:leaf-sw-0, 8:leaf-sw-1, ..."
		switchToStampSignatureMap := map[string]string{}
		// for example: map["stamp-0"] = []string{"systemf0", "br-x", "ax-y"}
		stampResources := map[string][]string{}

		var crossStampSwitchesDetected bool
		for _, sys := range cfg.Systems {
			if len(sysPortToV2GroupMap[sys.GetName()]) > 0 {
				sys.Properties = EnsureMap(sys.Properties)
				// generate a unique group name by map print which will keep keys sorted
				// https://github.com/golang/go/blob/release-branch.go1.18/src/fmt/print.go#L758
				// systems are in group if they have the same group name which represents the port/switch connection
				// e.g.[0:leaf0,1:leaf1...]
				groupSignature := fmt.Sprintf("%v", sysPortToV2GroupMap[sys.GetName()])
				stampSignatureToSystemList[groupSignature] = append(stampSignatureToSystemList[groupSignature], sys.GetName())

				for _, switchName := range sysPortToV2GroupMap[sys.GetName()] {
					signature := switchToStampSignatureMap[switchName]
					if signature != "" && signature != groupSignature {
						logrus.Infof("CrossStampSwitchesDetected: switch %s has two stamp signature %s, %s",
							switchName, signature, groupSignature)
						crossStampSwitchesDetected = true
						break
					} else {
						switchToStampSignatureMap[switchName] = groupSignature
					}
				}
			}
		}
		wsapisv1.CrossStampSwitchesDetected = crossStampSwitchesDetected

		numSystems := 0
		stampSignatures := SortedKeys(stampSignatureToSystemList)
		v2GroupsHaveStampIdxAssigned := map[string]bool{}
		for i, signature := range stampSignatures {
			for _, systemName := range stampSignatureToSystemList[signature] {
				sys := cfg.SystemMap[systemName]
				sys.Properties = EnsureMap(sys.Properties)
				stampIdx := strconv.Itoa(i)
				if wsapisv1.StampCountOverride > 0 {
					stampIdx = strconv.Itoa(numSystems % wsapisv1.StampCountOverride)
				}
				sys.Properties[resource.StampPropertyKey] = stampIdx
				stampResources[stampIdx] = append(stampResources[stampIdx], systemName)
				numSystems++

				for _, v2GroupName := range sysPortToV2GroupMap[sys.GetName()] {
					if !v2GroupsHaveStampIdxAssigned[v2GroupName] {
						// used for populating cluster topology through cluster server
						cfg.V2GroupMap[v2GroupName].Properties = EnsureMap(cfg.V2GroupMap[v2GroupName].Properties)
						cfg.V2GroupMap[v2GroupName].Properties[resource.StampPropertyKey] = stampIdx
						v2GroupsHaveStampIdxAssigned[v2GroupName] = true
					}
				}
			}
		}

		// in V2 topology, we assign BR affinity props to node directly and assign AX affinity props to node nics
		numAxNodes := 0
		numSxNodes := 0
		numInfDriverNodes := 0
		brTotalPortBw := 0
		for _, node := range cfg.Nodes {
			if node.GetRole() != string(RoleBroadcastReduce) && node.GetRole() != string(RoleActivation) && node.GetRole() != string(RoleInferenceDriver) {
				continue
			}

			// When nicName is not empty, it means the affinity properties are specific to a nic.
			assignAffinityProps := func(nicResName string, srcProps map[string]string, newProps map[string]string) {
				if len(srcProps) == 0 {
					return
				}

				newProps = EnsureMap(newProps)
				for k := range srcProps {
					// Updating cs port affinity properties with the nic name.
					// This helps lookup later on.
					if strings.HasPrefix(k, resource.PortAffinityPropKeyPrefix) {
						if val, ok := newProps[k]; !ok {
							// First time assigning this affinity prop, use the nicName input
							newProps[k] = nicResName
						} else if val != nicResName {
							// Not the first time seeing it. This means both nics have the affinity to this cs port.
							// Assign an empty string.
							newProps[k] = ""
						}
					}
				}
			}

			getStampIdx := func(groupName string) string {
				var stampIdx string
				stampSignature := switchToStampSignatureMap[groupName]
				for i, signature := range stampSignatures {
					if stampSignature == signature {
						stampIdx = strconv.Itoa(i)
						break
					}
				}
				return stampIdx
			}

			// assignActSWDriverNICAffinityAndStamp sets V2-group port affinity on each NIC and the node,
			// assigns a stamp index to them, records the node under that stamp, and increments
			// the provided counter.
			// this helper is used for both activation and inference driver nodes
			assignActSWDriverNICAffinityAndStamp := func(
				node *Node,
				v2GroupMap map[string]*NodeGroup,
				stampResources map[string][]string,
				counter *int,
			) {
				node.Properties = EnsureMap(node.Properties)
				var stampIdx string

				for _, nic := range node.NICs {
					nic.Properties = EnsureMap(nic.Properties)

					if v2Group, ok := v2GroupMap[nic.V2Group]; ok {
						nicResName := resource.NodeNicResName(nic.Name)
						assignAffinityProps(nicResName, v2Group.Properties, nic.Properties)
						assignAffinityProps(nicResName, v2Group.Properties, node.Properties)
					}

					// compute stamp index
					if !wsapisv1.CrossStampSwitchesDetected {
						stampIdx = getStampIdx(nic.V2Group)
						if wsapisv1.StampCountOverride > 0 {
							stampIdx = strconv.Itoa((*counter) % wsapisv1.StampCountOverride)
						}
					}
					nic.Properties[resource.StampPropertyKey] = stampIdx
				}

				node.Properties[resource.StampPropertyKey] = stampIdx
				stampResources[stampIdx] = append(stampResources[stampIdx], node.Name)
				*counter++
			}

			if node.GetRole() == string(RoleBroadcastReduce) {
				brTotalPortBw += node.GetTotalBw()
				node.Properties = EnsureMap(node.Properties)
				if v2Group, ok := v2GroupMap[node.GetV2Group()]; ok {
					assignAffinityProps("", v2Group.Properties, node.Properties)
				}

				stampIdx := getStampIdx(node.GetV2Group())
				if wsapisv1.StampCountOverride > 0 {
					stampIdx = strconv.Itoa(numSxNodes % wsapisv1.StampCountOverride)
				}
				node.Properties[resource.StampPropertyKey] = stampIdx
				stampResources[stampIdx] = append(stampResources[stampIdx], node.Name)
				numSxNodes++
			} else if node.GetRole() == string(RoleActivation) {
				assignActSWDriverNICAffinityAndStamp(node, v2GroupMap, stampResources, &numAxNodes)
			} else if node.GetRole() == string(RoleInferenceDriver) {
				assignActSWDriverNICAffinityAndStamp(node, v2GroupMap, stampResources, &numInfDriverNodes)
			}
		}

		if len(cfg.Systems) > 0 {
			if numAxNodes > 0 {
				wsapisv1.UseAxScheduling = true
				if !wsapisv1.AxCountPerSystemDiscoveryEnabled {
					// defaults the ratio to 1
					wsapisv1.AxCountPerSystem = 1
				} else {
					wsapisv1.AxCountPerSystem = numAxNodes / len(cfg.Systems)
					if wsapisv1.AxCountPerSystem == 0 {
						// only happens during an incremental cluster deploy where number of AX nodes is not exactly
						// multiples of number of CSX's
						wsapisv1.AxCountPerSystem = 1
					}
				}
				wsapisv1.SystemStampsDetected = len(stampSignatures) > 0
			}

			if numInfDriverNodes == 0 {
				wsapisv1.UseIxScheduling = false
			}
			requiredBw := GetRequiredBrPortBw(len(cfg.SystemMap))
			wsapisv1.RedundantBrPortBwPerSystem = float64(brTotalPortBw-requiredBw) / float64(len(cfg.Systems))
		} else {
			disableV2NetworkScheduling()
		}
		logrus.Infof("Generated V2 group system list: system groups: %v, port mappings: %v, "+
			"stamp signatures: %v, stamp resources: %v, ax count per system: %d, "+
			"ax-csx ratio discovery enabled: %t, systems stamps detected: %t, cross-stamp switches detected: %t",
			Values(stampSignatureToSystemList), Keys(stampSignatureToSystemList),
			stampSignatures, stampResources, wsapisv1.AxCountPerSystem,
			wsapisv1.AxCountPerSystemDiscoveryEnabled, wsapisv1.SystemStampsDetected, wsapisv1.CrossStampSwitchesDetected)
	} else {
		disableV2NetworkScheduling()
	}

	ReplicaTypeNodeSpecialization = map[commonapisv1.ReplicaType]NodeRole{
		wsapisv1.WSReplicaTypeActivation:      RoleMemory,
		wsapisv1.WSReplicaTypeBroadcastReduce: RoleBroadcastReduce,
		wsapisv1.WSReplicaTypeChief:           RoleMemory,
		wsapisv1.WSReplicaTypeCommand:         RoleMemory,
		wsapisv1.WSReplicaTypeCoordinator:     RoleCoordinator,
		wsapisv1.WSReplicaTypeWeight:          RoleMemory,
		wsapisv1.WSReplicaTypeWorker:          RoleWorker,
		wsapisv1.WSReplicaTypeSWDriver:        RoleInferenceDriver,
	}
}

// override on existing or empty cfg for dev mode test
func (cfg *ClusterConfig) updateIfDev() {
	if !IsDev {
		DisableScheduleNodeRole = false
		DisableNodeGroupScheduling = false
		return
	}
	logrus.Info("IsDev is set")
	cfg.generateBrNICs()
	DisableScheduleNodeRole = true
	DisableNodeGroupScheduling = true
}

func (cfg *ClusterConfig) BuildNodeSysGroupIndices() {
	cfg.SystemMap = ListToMap(
		func(s *systemv1.SystemSpec) (string, *systemv1.SystemSpec) { return s.Name, s }, cfg.Systems)
	cfg.NodeMap = ListToMap(
		func(a *Node) (string, *Node) { return a.Name, a }, cfg.Nodes)
	cfg.GroupMap = ListToMap(
		func(g *NodeGroup) (string, *NodeGroup) { return g.Name, g }, cfg.Groups)
	cfg.V2GroupMap = ListToMap(
		func(g *NodeGroup) (string, *NodeGroup) { return g.Name, g }, cfg.V2Groups)
}

// builds role related index, e.g. mgmt/br/...
func (cfg *ClusterConfig) buildNodeRoleIndex() {
	cfg.crdNodeMap = map[string]*Node{}
	cfg.BrIpNodeMap = map[string]*Node{}
	for _, node := range cfg.Nodes {
		for _, role := range node.ExpandedRoles {
			switch role {
			case RoleManagement, RoleCoordinator:
				if !node.IsDedicatedManagementNode() {
					cfg.crdNodeMap[node.Name] = node
				}
			case RoleBroadcastReduce:
				for _, nic := range node.NICs {
					cfg.BrIpNodeMap[nic.Addr] = node
				}
				if node.HostIP != "" {
					cfg.BrIpNodeMap[node.HostIP] = node
				}
			}
		}
	}
}

func (cfg *ClusterConfig) InitializeNsAssignment(nsrList namespacev1.NamespaceReservationList) {
	cfg.systemToNsMap = map[string]string{}
	cfg.nsAssignedSystems = map[string]map[string]*systemv1.SystemSpec{}
	cfg.nsAssignedGroups = map[string]map[string]*NodeGroup{}
	cfg.nsAssignedNodes = map[string]map[string]*Node{}
	cfg.nsProperties = map[string]map[string]string{}

	mgmtNodesAssignedToSessions := map[string]map[string]bool{}
	for _, nsr := range nsrList.Items {
		if len(nsr.Labels) > 0 {
			cfg.nsProperties[nsr.Name] = nsr.Labels
		}

		for _, sys := range nsr.Status.Systems {
			if s, ok := cfg.SystemMap[sys]; ok {
				cfg.nsAssignedSystems[nsr.Name] = EnsureMap(cfg.nsAssignedSystems[nsr.Name])
				cfg.nsAssignedSystems[nsr.Name][s.Name] = s
				cfg.systemToNsMap[sys] = nsr.Name
			} else {
				logrus.Warnf("system/%s was assigned to namespace/%s but no longer exists, ignore", sys, nsr.Name)
			}
		}

		for _, ng := range nsr.Status.Nodegroups {
			if g, ok := cfg.GroupMap[ng]; ok {
				cfg.nsAssignedGroups[nsr.Name] = EnsureMap(cfg.nsAssignedGroups[nsr.Name])
				cfg.nsAssignedGroups[nsr.Name][g.Name] = g
				g.Properties = EnsureMap(g.Properties)
				g.Properties[NamespaceKey] = nsr.Name
			} else {
				logrus.Warnf("nodegroup/%s was assigned to namespace/%s but no longer exists, ignore", ng, nsr.Name)
			}
		}

		mgmtNodesAssigned := map[string]bool{}
		for _, node := range nsr.Status.Nodes {
			if n, ok := cfg.NodeMap[node]; ok {
				cfg.nsAssignedNodes[nsr.Name] = EnsureMap(cfg.nsAssignedNodes[nsr.Name])
				cfg.nsAssignedNodes[nsr.Name][n.Name] = n
				n.Properties = EnsureMap(n.Properties)
				n.Properties[NamespaceKey] = nsr.Name
				if n.GetProperties()[StorageTypeKey] == CephStorageType {
					mgmtNodesAssigned[n.Name] = true
					if nsr.Name != SystemNamespace {
						mgmtNodesAssignedToSessions[nsr.Name] = EnsureMap(mgmtNodesAssignedToSessions[nsr.Name])
						mgmtNodesAssignedToSessions[nsr.Name][n.Name] = true
					}
				}
			} else {
				logrus.Warnf("node/%s was assigned to namespace/%s but no longer exists, ignore", node, nsr.Name)
			}
		}

		logrus.Infof("Namespace: %s, assigned systems: %v, "+
			"assigned nodegroups: %v, assigned nodes: %v, assigned mgmt nodes: %v, props: %v",
			nsr.Name,
			SortedKeys(cfg.nsAssignedSystems[nsr.Name]),
			SortedKeys(cfg.nsAssignedGroups[nsr.Name]),
			SortedKeys(cfg.nsAssignedNodes[nsr.Name]),
			SortedKeys(mgmtNodesAssigned),
			cfg.nsProperties[nsr.Name])
	}

	if wsapisv1.ManagementSeparationEnforced && len(mgmtNodesAssignedToSessions) > 0 {
		panic(fmt.Sprintf("Management separation cannot be enforced due to some management nodes still belong to sessions: %v. "+
			"Please disable separation from cluster pkg properties, redeploy the operator and remove these management nodes from sessions first", mgmtNodesAssignedToSessions))
	}
}

func (cfg *ClusterConfig) updateIfMultiCrd() {
	logrus.Infof("EnableMultiCoordinator mode: %t", EnableMultiCoordinator)
	if !EnableMultiCoordinator {
		return
	}
	// group by node-group
	groupWorkerNodes := map[string][]*Node{}
	crdGroup := ""
	for _, node := range cfg.Nodes {
		nodeGroup := node.Properties["group"]
		// if already defined as CRD/mgmt, skip group check, assuming nodes already ordered by group/rack
		if nodeGroup == "" || nodeGroup == crdGroup {
			continue
		}
		if node.Role == RoleCoordinator || node.Role == RoleManagement {
			crdGroup = nodeGroup
			delete(groupWorkerNodes, nodeGroup)
			continue
		}
		// add worker
		if node.Role == RoleWorker {
			groupWorkerNodes[nodeGroup] = append(groupWorkerNodes[nodeGroup], node)
		}
	}
	// mark extra worker as coordinator
	for group, workers := range groupWorkerNodes {
		if len(workers) < 2 {
			logrus.Infof("node group %s has only %d worker node, skip converting", group, len(workers))
			continue
		}
		// update worker role to CRD
		workers[0].Role = RoleCoordinator
		workers[0].Properties[OriginRoleKey] = string(RoleWorker)
		logrus.Infof("Updating worker to be coordinator role: %s", workers[0].Name)
	}
}

// adjust node roles/props based on cluster setup
// e.g. dev test cluster/internal multi-crd cluster/single node cluster
func (cfg *ClusterConfig) AdjustNodesByClusterSetup() {
	cfg.updateIfDev()
	cfg.updateIfMultiCrd()
	// expand node roles needs to be after updateIfDev/updateIfMultiCrd
	// expandedRoles defines how each NodeRole expands to a set of roles for scheduling and matching.
	// expandedRoles are mostly used for dev/single node clusters and testing purposes.
	// - RoleAny is primarily used in kind and e2e testing, and expands to worker, broadcastreduce, and memory roles.
	// - Activation and InferenceDriver roles are intentionally NOT included in RoleAny expansion.
	//   This avoids panics in newCollectionForNsMgmt during e2e tests, where a single node with both activation and inference driver roles
	//   would otherwise be added twice, causing "resource with id already exists" errors.

	expandedRoles := map[NodeRole][]NodeRole{
		RoleManagement: {
			RoleManagement,
			RoleCoordinator,
		},
		RoleCoordinator:     {RoleCoordinator},
		RoleMemory:          {RoleMemory},
		RoleBroadcastReduce: {RoleBroadcastReduce},
		RoleActivation:      {RoleActivation},
		RoleWorker:          {RoleWorker},
		RoleInferenceDriver: {RoleInferenceDriver},
		RoleAny:             {RoleWorker, RoleBroadcastReduce, RoleMemory},
	}
	if len(cfg.Nodes) == 1 {
		// Only one mgmt/crd node is allowed in cluster as current arch
		expandedRoles[RoleAny] = append(expandedRoles[RoleAny], RoleManagement, RoleCoordinator)
	}
	for _, node := range cfg.Nodes {
		node.ExpandedRoles = expandedRoles[node.Role]
		if !wsapisv1.ManagementSeparationEnforced {
			continue
		}
		if node.IsDedicatedManagementNode() {
			node.ExpandedRoles = []NodeRole{RoleManagement}
		} else if node.IsDedicatedCoordinatorNode() {
			node.ExpandedRoles = []NodeRole{RoleCoordinator}
		}
	}
	// remove legacy mgmt node group
	cfg.removeLegacyMgmtGroup()
	// buildNodeRoleIndex needs to be in end of this func since previous steps will adjust roles
	cfg.buildNodeRoleIndex()
}

func (cfg *ClusterConfig) GetCoordNodes() []*Node {
	var nodes []*Node
	for _, node := range cfg.crdNodeMap {
		nodes = append(nodes, node)
	}
	return nodes
}

func (cfg *ClusterConfig) GetServiceDomain() string {
	// Get serviceDomain from properties falling back to the deprecated field, falling back to the default.
	serviceDomain := ""
	if cfg.Properties != nil && cfg.Properties[wsapisv1.ServiceDomainProp] != "" {
		serviceDomain = cfg.Properties[wsapisv1.ServiceDomainProp]
	} else if cfg.ServiceDomain != "" {
		serviceDomain = cfg.ServiceDomain
	} else {
		serviceDomain = wsapisv1.DefaultServiceDomain
	}
	return serviceDomain
}

func (cfg *ClusterConfig) GetClusterServerDomain() string {
	if cfg.Name != "" {
		return fmt.Sprintf("%s.%s.%s", wsapisv1.ClusterServerSubdomain, cfg.Name, cfg.GetServiceDomain())
	} else {
		return fmt.Sprintf("%s.%s", wsapisv1.ClusterServerSubdomain, cfg.GetServiceDomain())
	}
}

func (cfg *ClusterConfig) String() string {
	res := fmt.Sprintln("systems")
	for _, system := range cfg.Systems {
		res += fmt.Sprintf("%+v ", *system)
	}
	res += fmt.Sprintln("groups")
	for _, group := range cfg.Groups {
		res += fmt.Sprintf("%+v ", *group)
	}
	res += fmt.Sprintln("nodes")
	for _, node := range cfg.Nodes {
		res += fmt.Sprintf("%+v ", *node)
	}
	return res
}

func (cfg *ClusterConfig) GetSystemAssignedNs(system string) string {
	return cfg.systemToNsMap[system]
}

func (cfg *ClusterConfig) GetAssignedSystemsMap(namespace string) map[string]*systemv1.SystemSpec {
	sys := cfg.nsAssignedSystems[namespace]
	return sys
}

func (cfg *ClusterConfig) GetAssignedSystemsList(namespace string) []string {
	sys := SortedKeys(cfg.GetAssignedSystemsMap(namespace))
	return sys
}

func (cfg *ClusterConfig) GetAssignedGroupsMap(namespace string) map[string]*NodeGroup {
	ng := cfg.nsAssignedGroups[namespace]
	return ng
}

func (cfg *ClusterConfig) GetAssignedNodesList(namespace string) []string {
	sys := SortedKeys(cfg.nsAssignedNodes[namespace])
	return sys
}

func (cfg *ClusterConfig) GetAssignedNodesMap(namespace string) map[string]*Node {
	return cfg.nsAssignedNodes[namespace]
}

func (cfg *ClusterConfig) GetGroupToNsMap() map[string]string {
	groupNs := map[string]string{}
	for ns, groups := range cfg.nsAssignedGroups {
		for groupName := range groups {
			groupNs[groupName] = ns
		}
	}
	return groupNs
}

func (cfg *ClusterConfig) GetNamespaces() map[string]bool {
	groupNs := cfg.GetGroupToNsMap()
	namespaces := map[string]bool{}
	for _, value := range groupNs {
		namespaces[value] = true
	}
	return namespaces
}

func (cfg *ClusterConfig) GetAssignedGroupsList(namespace string) []string {
	ng := SortedKeys(cfg.GetAssignedGroupsMap(namespace))
	return ng
}

func (cfg *ClusterConfig) GetSessionSystemCount(namespace string) int {
	return len(cfg.nsAssignedSystems[namespace])
}

func (cfg *ClusterConfig) GetSessionNodeCount(namespace string) int {
	return len(cfg.nsAssignedNodes[namespace])
}

func (cfg *ClusterConfig) HasUnhealthyResources() bool {
	return len(cfg.UnhealthySystems) > 0 || len(cfg.UnhealthyNodes) > 0
}

// PrettyHealthInfo returns a human-readable string describing the health status of cluster resources.
// If a namespace is specified, it only includes resources assigned to that namespace plus any sharable
// resources (like broadcast-reduce nodes) that have errors. The format is:
// "X systems Y nodes unhealthy: systemXXX{errorPorts=A,B}, nodeXXX{errorNics=A,B}"
//
// Parameters:
//
//   - ns: Namespace to filter resources by.
//     If empty, includes all resources.
//
//   - showSharedResources: Whether to include shared resources in the output.
//     If true, will include shared (unhealthy) nodes in the output.
//
// Returns:
//   - A formatted string describing unhealthy resources, or "Cluster healthy." if all resources are healthy.
func (cfg *ClusterConfig) PrettyHealthInfo(ns string, showSharedResources bool) string {
	unhealthyNodeNames := []string{}
	unhealthySystemNames := []string{}
	for node := range cfg.UnhealthyNodes {
		if ns == "" ||
			cfg.GetNodeNamespace(node) == ns ||
			(cfg.IsSharableAcrossNs(node) && showSharedResources) {
			unhealthyNodeNames = append(unhealthyNodeNames, node)
		}
	}
	for system := range cfg.UnhealthySystems {
		if ns == "" || cfg.GetSystemAssignedNs(system) == ns {
			unhealthySystemNames = append(unhealthySystemNames, system)
		}
	}
	unhealthyNodeCount := len(unhealthyNodeNames)
	unhealthySysCount := len(unhealthySystemNames)
	if unhealthySysCount == 0 && unhealthyNodeCount == 0 {
		return "Cluster healthy."
	}
	sb := strings.Builder{}
	if unhealthySysCount > 0 {
		sb.WriteString(strconv.Itoa(unhealthySysCount))
		sb.WriteString(" system")
		if unhealthySysCount > 1 {
			sb.WriteString("s")
		}
		if unhealthyNodeCount > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(" ")
	}
	if unhealthyNodeCount > 0 {
		sb.WriteString(strconv.Itoa(unhealthyNodeCount))
		sb.WriteString(" node")
		if unhealthyNodeCount > 1 {
			sb.WriteString("s")
		}
		sb.WriteString(" ")
	}
	sb.WriteString("unhealthy: ")
	if unhealthySysCount > 0 {
		sort.Strings(unhealthySystemNames)
		for i, s := range unhealthySystemNames {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(s)
			errPorts := cfg.UnhealthySystems[s].GetErrorPorts()
			if len(errPorts) > 0 {
				sb.WriteString("{errorPorts=")
				sb.WriteString(strings.Join(errPorts, ","))
				sb.WriteString("}")
			}
		}
	}
	if unhealthyNodeCount > 0 {
		if unhealthySysCount > 0 {
			sb.WriteString(", ")
		}
		sort.Strings(unhealthyNodeNames)
		for i, node := range unhealthyNodeNames {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(node)
			errNics := cfg.UnhealthyNodes[node].GetErrorNICs()
			if len(errNics) > 0 {
				sb.WriteString("{errorNics=")
				for j, nic := range errNics {
					if j > 0 {
						sb.WriteString(",")
					}
					sb.WriteString(nic)
				}
				sb.WriteString("}")
			}
		}
	}
	return strings.TrimSpace(sb.String())
}

// todo: do deepcopy for all fields including node/sys spec
func (cfg *ClusterConfig) Copy() *ClusterConfig {
	if cfg == nil {
		return nil
	}
	c := *cfg
	c.UnhealthySystems = CopyMap(cfg.UnhealthySystems)
	c.UnhealthyNodes = CopyMap(cfg.UnhealthyNodes)
	c.systemToNsMap = CopyMap(cfg.systemToNsMap)
	c.nsAssignedSystems = CopyMapNested(cfg.nsAssignedSystems)
	c.nsAssignedGroups = CopyMapNested(cfg.nsAssignedGroups)
	c.nsAssignedNodes = CopyMapNested(cfg.nsAssignedNodes)
	c.nsProperties = CopyMapNested(cfg.nsProperties)
	return &c
}

func (cfg *ClusterConfig) GetNsrProperty(namespace, key string) string {
	return cfg.nsProperties[namespace][key]
}

func (cfg *ClusterConfig) GetNsSysNameMap() map[string][]string {
	nsSysName := map[string][]string{}
	for ns, sysMap := range cfg.nsAssignedSystems {
		nsSysName[ns] = SortedKeys(sysMap)
	}
	return nsSysName
}

func (cfg *ClusterConfig) GetNodeNamespace(name string) string {
	node := cfg.NodeMap[name]
	if node == nil {
		return ""
	}
	ns := node.Properties[NamespaceKey]
	if ns != "" {
		return ns
	}
	if group := cfg.GroupMap[node.GetGroup()]; group != nil {
		return group.Properties[NamespaceKey]
	}
	return ""
}

func (cfg *ClusterConfig) IsNodeInNamespace(namespace string, node string) bool {
	return cfg.IsSharableAcrossNs(node) || cfg.GetNodeNamespace(node) == namespace
}

// IsSharableAcrossNs indicates whether a node should belong to one namespace.
// Since two jobs in different namespaces may share the same BR nodes, BR nodes
// should not be treated exclusively for one namespace. If the role of a node is "any",
// we treat them exclusively to one namespace. This is needed in compilepod and kind
// where a node can be overloaded in roles and jobs like image build jobs use node
// selector to schedule the pod to the worker (in this case, any) node.
func (cfg *ClusterConfig) IsSharableAcrossNs(node string) bool {
	n := cfg.NodeMap[node]
	if n == nil {
		return false
	}

	return n.GetRole() == RoleBroadcastReduce.String()
}

// Mgmt nodes are not associated with nodegroups as of 2.2. Remove group prop from mgmt nodes to prevent downstream
// usages of the property (collection creation, node labeling)
func (cfg *ClusterConfig) removeLegacyMgmtGroup() {
	if len(cfg.Nodes) == 1 {
		return // keep group property for compile pods
	}
	for _, node := range cfg.Nodes {
		group := node.Properties["group"]
		if group != "" && (node.HasRole(RoleManagement) || node.HasRole(RoleCoordinator)) && !node.HasRole(RoleAny) {
			delete(node.Properties, "group")
			node.Properties[OriginGroupKey] = group
		}
	}
}

func (cfg *ClusterConfig) GetOkSystemsInNamespace(namespace string) string {
	systems := cfg.nsAssignedSystems[namespace]
	var systemInfos []string
	for system, spec := range systems {
		if cfg.UnhealthySystems[system] == nil {
			systemInfos = append(systemInfos, fmt.Sprintf("%s+%s", spec.Name, spec.CmAddr))
		}
	}

	sort.Slice(systemInfos, func(i, j int) bool {
		return systemInfos[i] < systemInfos[j]
	})
	return strings.Join(systemInfos, ",")
}

func (cfg *ClusterConfig) GetV2GroupFromAffinityKey(affinityKey string) string {
	for _, lsw := range cfg.V2GroupMap {
		if _, exists := lsw.Properties[affinityKey]; exists {
			return lsw.Name
		}
	}
	return ""
}

func (cfg *ClusterConfig) GetSystemDataPortName(systemName string, portIdx int) string {
	if system, ok := cfg.SystemMap[systemName]; ok {
		if len(system.Ports) >= portIdx {
			return system.Ports[portIdx].Name
		}
	}
	return ""
}

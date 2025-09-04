package resource_config

import (
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common/resource"
)

const (
	// DefaultPodCountPerNode is the number of pods allows to be scheduled on a node by default
	// In V2 networking cluster, activations pods are consolidated in much less amount of AX nodes.
	// We had seen 60 activations pod per CSX (same as per AX node in V2 network). We are giving
	// some buffer here so that the constant doesn't need to be immediately adjusted after.
	// Note that coordinator nodes typically have a higher number (30+) of utility/background pods,
	// but we schedule much less wsjob pods on them - only coordinator pods get scheduled there.
	DefaultPodCountPerNode = 90

	// MaxSwDriverPodsPerIxNode caps the number of swdriver pods that can be scheduled on a single IX node.
	// Runtime observations have shown contention issues when more than 2 swdriver pods are placed on the same node.
	// To mitigate these resource contention problems, this constant is set to 1 for now.
	// This means by default job sharing is disabled for swdriver pods on IX nodes.
	DefaultSwDriverPodsPerIxNode = 1

	rolePrefix = "role-"

	MemxMemClassLargeThreshold  = 900 << 10
	MemxMemClassXLargeThreshold = 1800 << 10
)

var MaxSwDriverPodsPerIxNode = DefaultSwDriverPodsPerIxNode
var MaxPodCountPerNode = DefaultPodCountPerNode

func init() {
	podCount, _ := strconv.Atoi(os.Getenv("MAX_POD_PER_NODE"))
	if podCount > 0 {
		MaxPodCountPerNode = podCount
		log.Infof("max pod per node count override: %d", podCount)
	}
}

// INodeCfg is an interface for accessing node configuration without creating circular dependencies
type INodeCfg interface {
	GetName() string
	GetRole() string
	GetExpandedRoles() map[string]bool // true if it has role
	GetProperties() map[string]string
	// BR only, cs port id => healthy NIC list
	GetAvailableCsPorts() (map[int][]string, map[string]int)
	IsSchedulable() (string, bool)
}

// ISystemCfg is an interface for accessing system configuration without creating circular dependencies
type ISystemCfg interface {
	GetName() string
	GetType() string
	GetProperties() map[string]string
	// system healthy port count
	GetAvailableCsPorts() int
	IsSchedulable() (string, bool)
}
type groupStats struct {
	nodeCount    int
	replicaStats map[string]resource.Subresources
}

func (s groupStats) add(originRole string, n resource.Resource) groupStats {
	s.nodeCount += 1
	if s.replicaStats[originRole] == nil {
		s.replicaStats[originRole] = resource.Subresources{}
	}

	sr := s.replicaStats[originRole]
	if sr.Cpu() == 0 || n.Subresources().Cpu() < sr.Cpu() {
		sr[resource.CPUSubresource] = n.Subresources().Cpu()
	}
	if sr.Mem() == 0 || n.Subresources().Mem() < sr.Mem() {
		sr[resource.MemSubresource] = n.Subresources().Mem()
	}
	sr["count"] += 1
	return s
}

func (s groupStats) String() string {
	var roleStats []string
	for role, stats := range s.replicaStats {
		roleStats = append(roleStats, role+":"+stats.Pretty())
	}
	return fmt.Sprintf("NodeCount:%d,RoleStats:%v", s.nodeCount, roleStats)
}

// resource name -> true if healthy
type IsResourceHealthy func(string) bool

func InitializeResourceManagement(
	nodes []INodeCfg,
	groupProps map[string]map[string]string,
	systems []ISystemCfg,
	systemNamespace map[string]string,
	nodeCap NodeCapacity) *resource.Collection {

	c := &resource.Collection{}
	for _, sys := range systems {
		healthState, _ := sys.IsSchedulable()
		props := map[string]string{"name": sys.GetName(), "mode": sys.GetType(), resource.HealthProp: healthState}
		maps.Copy(props, sys.GetProperties())
		if systemNamespace[sys.GetName()] != "" {
			props[resource.NamespacePropertyKey] = systemNamespace[sys.GetName()]
			// name as prop for affinity use case
			props[sys.GetName()] = ""
		}
		r := resource.NewResource(rlv1.SystemResourceType, sys.GetName(), props, GetSystemSubresources(sys), nil)
		log.WithField("resource", r).Debug("initialized system resource")
		c.Add(r)
	}
	stats := map[string]groupStats{}
	groupWrkCount := map[string]int{}
	groupMemxCount := map[string]int{}
	groupMemxMinMem := map[string]int{}
	for _, node := range nodes {
		props := map[string]string{"name": node.GetName()}
		for k, v := range extractNodeProperties(node) {
			props[k] = v
		}
		group := node.GetProperties()[resource.GroupPropertyKey]
		// create subresources
		sr := map[string]int{
			resource.PodSubresource: MaxPodCountPerNode,
		}
		if nodeCap != nil {
			cpu := nodeCap.Cpu(node.GetName())
			mem := nodeCap.Mem(node.GetName())
			sr[resource.CPUSubresource] = int(cpu.MilliValue())  // to mCPU
			sr[resource.MemSubresource] = int(mem.Value() >> 20) // to MiB
		}
		// index for BR, ACT and SWDR nic scheduling
		subresourceIndex := map[string]map[string]bool{}
		portNicMap, nicBwMap := node.GetAvailableCsPorts()
		// Subresource limit hack for SwDriver mutual exclusion
		// TODO: remove once SwDriver is scheduled on IX/SX
		if _, ok := props[resource.RoleAxPropKey]; ok {
			sr[resource.Act0SubResource] = 1
		}
		for port, nics := range portNicMap {
			portKey := resource.NodeNicSubresource
			if port >= 0 {
				portKey = resource.NodeNicSubresource + "-" + strconv.Itoa(port)
			}
			subresourceIndex[portKey] = map[string]bool{}
			for _, nic := range nics {
				bw := resource.DefaultNicBW
				if _, ok := props[resource.RoleAxPropKey]; ok {
					bw = resource.DefaultAxNicBW
				} else if _, ok := props[resource.RoleIxPropKey]; ok {
					bw = resource.DefaultIxNicBW
				}
				if nicBwMap[nic] > 0 {
					bw = nicBwMap[nic]
				}

				// The nic subresource amount on AX nodes will be dynamically set per-job in lock reconciler
				nicRes := resource.NodeNicResName(nic)
				sr[nicRes] = bw
				subresourceIndex[portKey][nicRes] = true
			}
		}
		if _, ok := props[resource.RoleBRPropKey]; ok {
			subresourceIndex[resource.PodInstanceIdSubresource] = map[string]bool{}
			for i := 0; i < resource.DefaultMaxBrPodsPerNode; i++ {
				insId := resource.InstIdResName(i)
				sr[insId] = 1
				subresourceIndex[resource.PodInstanceIdSubresource][insId] = true
			}
		}
		// create subresourceRanges
		srr := map[string][]resource.Range{}
		// index for CRD nvmf space scheduling
		subresourceRangeIndex := map[string]map[string]bool{}
		if _, ok := props[resource.RoleCrdPropKey]; ok {
			diskSpaceBytes, err := strconv.ParseInt(props[resource.NvmfDiskBytesPropertyKey], 10, 64)
			if err == nil {
				subresourceRangeIndex[resource.NvmfDiskSubresource] = map[string]bool{}
				// There are in total 3 disks of the same size.
				for i := 1; i <= resource.NvmfDiskCountPerCoordNode; i++ {
					name := resource.NvmfDiskResName(i)
					srr[name] = append(srr[name], resource.Range{Start: 0, End: diskSpaceBytes})
					subresourceRangeIndex[resource.NvmfDiskSubresource][name] = true
				}
			}
		}

		// Override pod limit for IX nodes to enforce a configurable SWDriver pods max
		if _, ok := props[resource.RoleIxPropKey]; ok {
			sr[resource.PodSubresource] = MaxSwDriverPodsPerIxNode
		}

		state, ok := node.IsSchedulable()
		props[resource.HealthProp] = state
		group = props[resource.GroupPropertyKey]
		if _, ok = props[resource.RoleMemoryPropKey]; ok {
			groupMemxCount[group]++
			m, hasMem := groupMemxMinMem[group]
			if !hasMem || m > sr[resource.MemSubresource] {
				groupMemxMinMem[group] = sr[resource.MemSubresource]
			}
		}
		// in single-node clusters, a node can be memory as well as worker
		if _, ok = props[resource.RoleWorkerPropKey]; ok {
			groupWrkCount[group]++
		}

		r := resource.NewResource(rlv1.NodeResourceType, node.GetName(), props, sr, srr).WithIndex(resource.SubresourceIndex{
			Subresources:      subresourceIndex,
			SubresourceRanges: subresourceRangeIndex,
		})
		if wsapisv1.Verbose {
			log.WithField("resource", r).Info("initialized node resource")
		} else {
			log.WithField("resource", r).Debug("initialized node resource")
		}
		c.Add(r)
		if _, ok := stats[group]; !ok {
			stats[group] = groupStats{replicaStats: map[string]resource.Subresources{}}
		}
		stats[group] = stats[group].add(node.GetRole(), r)
	}

	// we skip adding groups if it's a single-node cluster
	for grpName, grpProps := range groupProps {
		// skip a group if it is not a single-node cluster AND contains no worker node
		// - in v1 networking, a depop nodegroup should contain both memx nodes and worker node(s)
		// - in v2 networking, a depop nodegroup should only contain worker node(s)
		// - populated nodegroups should always contain both memx nodes and worker node(s)
		if len(nodes) > 1 && groupWrkCount[grpName] == 0 {
			continue
		}
		// additionally add the populated property which has factored in # healthy memx during node listing above
		// populated = above a threshold of memx in group OR it's a single or no nodegroup cluster
		if _, ok := grpProps[wsapisv1.MemxPopNodegroupProp]; !ok {
			isPop := groupMemxCount[grpName] >= wsapisv1.MinMemxPerPopNodegroup || len(groupProps) < 2
			grpProps[wsapisv1.MemxPopNodegroupProp] = strconv.FormatBool(isPop)
		}
		if _, ok := grpProps[wsapisv1.MemxMemClassProp]; !ok {
			if groupMemxMinMem[grpName] > MemxMemClassXLargeThreshold {
				grpProps[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeXLarge
			} else if groupMemxMinMem[grpName] > MemxMemClassLargeThreshold {
				grpProps[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeLarge
			} else {
				grpProps[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeRegular
			}
		}
		log.WithField("group", grpName).WithField("props", grpProps).WithField("stats", stats[grpName]).Info("initialized group resource")
		delete(stats, grpName)
		c.AddGroup(resource.NewGroup(grpName, grpProps))
	}

	if len(stats) > 0 {
		log.WithField("stats", stats).Info("additional group stats")
	}

	return c
}

func GetSystemSubresources(sys ISystemCfg) resource.Subresources {
	ports := 12
	if sys != nil {
		ports = sys.GetAvailableCsPorts()
	}
	return map[string]int{
		resource.SystemSubresource:     1,
		resource.SystemPortSubresource: ports,
	}
}

// extractNodeProperties extracts the properties from a node. This always includes a 'role-X:""' property which is
// used during scheduling. If the node has a role of "any", then all roles are added. If the node has a property
// for cpu or memory, these are skipped and added as subresources instead.
func extractNodeProperties(node INodeCfg) map[string]string {
	props := map[string]string{}
	for role := range node.GetExpandedRoles() {
		props[rolePrefix+string(role)] = ""
	}
	props["name"] = node.GetName()

	for k, v := range node.GetProperties() {
		if k != resource.CPUSubresource && k != resource.MemSubresource {
			props[k] = v
		}
	}
	return props
}

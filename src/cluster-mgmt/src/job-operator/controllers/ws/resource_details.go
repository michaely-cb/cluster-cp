package ws

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource_config"
)

const (
	memxPrimaryCountDefault     = 12
	memxSecondaryCountV1Default = 4
	axCountPerCsxDefault        = 1
	nicCountPerAx               = 2

	// V1 networking memory tiers
	// memxRegularMemBytesV1Default is the product-of-record memoryX memory capacity in V1 networking, 125Gi.
	memxRegularMemBytesV1Default = 125 << 30
	memxLargeMemBytesV1Default   = 995 << 30

	// V2 networking memory tiers
	// memxRegularMemBytesV2Default is the product-of-record memoryX memory capacity in V2 networking, 188Gi.
	memxRegularMemBytesV2Default = 188 << 30
	memxLargeMemBytesV2Default   = 1133 << 30
	memxXLargeMemBytesV2Default  = 2267 << 30

	// activationMemBytesV2Default is the product-of-record activation memory capacity in V2 networking, 755Gi.
	activationMemBytesV2Default = 755 << 30

	memxPrimaryCountEnv   = "RESOURCE_DETAILS_MEMX_PRIMARY_GROUP_COUNT"
	memxSecondaryCountEnv = "RESOURCE_DETAILS_MEMX_SECONDARY_GROUP_COUNT"
)

// Node group tier types
const (
	NodeGroupTierRegular   = "regular"
	NodeGroupTierLarge     = "large"
	NodeGroupTierXLarge    = "xlarge"
	NodeGroupTierSecondary = "secondary"
)

var (
	memxPrimaryCount     int
	memxSecondaryV1Count int

	// Global variables for memory tiers that can be overridden in tests
	memxRegularMemBytes   int
	memxLargeMemBytes     int
	memxXLargeMemBytes    int
	memxSecondaryMemBytes int
	axMemoryBytes         int

	nodeReservedMemBytes apiresource.Quantity
)

type NodegroupDetails struct {
	Name string `json:"name"`

	// MemoryxCount is the product-of-record memoryx count while MemoryxEffectiveCount is the actual number in the
	// nodegroup. Downstream consumers might want to compile for the POR count, while still knowing the actual count.
	MemoryxCount          int `json:"memoryxCount"`
	MemoryxEffectiveCount int `json:"memoryxEffectiveCount"`

	// MemoryxMemoryBytes is the product-of-record memoryx capacity while MemoryxEffectiveMemoryBytes is actual capacity of the
	// smallest memx in that nodegroup. Downstream consumers might want to compile for the POR memory, while still
	// knowing the actual memory.
	MemoryxMemoryBytes          int64 `json:"memoryxMemoryBytes"`
	MemoryxEffectiveMemoryBytes int64 `json:"memoryxEffectiveMemoryBytes"`

	MemoryxBandwidthBps int64 `json:"memoryxBandwidthBps"`
	Populated           bool  `json:"-"` // for internal sorting only
}

// NodeGroupTier represents information about a specific memory tier
type NodeGroupTier struct {
	Type  string `json:"type"`  // Type of tier (regular, large, xlarge)
	Count int    `json:"count"` // Number of nodegroups in this tier

	// MemoryxCount is the product-of-record memoryx count while MemoryxEffectiveCount is the actual number in the
	// nodegroup. Downstream consumers might want to compile for the POR count, while still knowing the actual count.
	MemoryxCount          int `json:"memoryxCount"`
	MemoryxEffectiveCount int `json:"memoryxEffectiveCount"`

	// MemoryxMemoryBytes is the product-of-record memoryx capacity while MemoryxEffectiveMemoryBytes is actual capacity of the
	// smallest memx in that nodegroup. Downstream consumers might want to compile for the POR memory, while still
	// knowing the actual memory.
	MemoryxMemoryBytes          int64 `json:"memoryxMemoryBytes"`
	MemoryxEffectiveMemoryBytes int64 `json:"memoryxEffectiveMemoryBytes"`

	MemoryxBandwidthBps int64 `json:"memoryxBandwidthBps"`
}

type ActivationNodeDetails struct {
	Name string `json:"name"`

	// ActivationCountPerCsx is the product-of-record activation count per CSX while ActivationEffectiveCountPerCsx is the actual
	// number per CSX. In non-POR setup, we could have more AX nodes per CSX to compensate for slower NIC bandwidth.
	// Downstream consumers might want to compile for the POR count, while still knowing the actual count.
	ActivationCountPerCsx          int `json:"activationCountPerCsx"`
	ActivationEffectiveCountPerCsx int `json:"activationEffectiveCountPerCsx"`

	// ActivationMemoryBytes is the product-of-record AX capacity while ActivationEffectiveMemoryBytes is actual capacity of the
	// smallest AX in the namespace. Downstream consumers might want to compile for the POR memory, while still
	// knowing the actual memory.
	// As of rel-2.4, AX memory capacity should be consistent in a cluster. ActivationEffectiveMemoryBytes is only for forward
	// compatibility.
	ActivationMemoryBytes          int64 `json:"activationMemoryBytes"`
	ActivationEffectiveMemoryBytes int64 `json:"activationEffectiveMemoryBytes"`

	// NicBandwidthBps is the product-of-record NIC bandwidth while NicEffectiveBandwidthBps is actual
	// NIC bandwidth. In non-POR setup, we could have more AX nodes per CSX to compensate for slower NIC bandwidth.
	// Downstream consumers might want to compile for the POR NIC bandwidth, while still knowing the actual bandwidth.
	NicBandwidthBps          int64 `json:"nicBandwidthBps"`
	NicEffectiveBandwidthBps int64 `json:"nicEffectiveBandwidthBps"`

	// NicCount is the product-of-record NIC count on AX nodes while NicEffectiveCount is actual NIC count.
	NicCount          int64 `json:"nicCount"`
	NicEffectiveCount int64 `json:"nicEffectiveCount"`

	// ActivationBandwidthBps is a deprecated name, use NicBandwidthBps instead
	ActivationBandwidthBps int64 `json:"activationBandwidthBps"`
	// ActivationEffectiveBandwidthBps is a deprecated name, use NicEffectiveBandwidthBps instead
	ActivationEffectiveBandwidthBps int64 `json:"activationEffectiveBandwidthBps"`
}

// ActivationNodeTier represents information about a specific activation node tier
type ActivationNodeTier struct {
	Type  string `json:"type"`  // Type of tier (regular, etc.)
	Count int    `json:"count"` // Number of activation nodes in this tier

	ActivationCountPerCsx          int `json:"activationCountPerCsx"`
	ActivationEffectiveCountPerCsx int `json:"activationEffectiveCountPerCsx"`

	ActivationMemoryBytes          int64 `json:"activationMemoryBytes"`
	ActivationEffectiveMemoryBytes int64 `json:"activationEffectiveMemoryBytes"`

	NicBandwidthBps          int64 `json:"nicBandwidthBps"`
	NicEffectiveBandwidthBps int64 `json:"nicEffectiveBandwidthBps"`

	NicCount          int64 `json:"nicCount"`
	NicEffectiveCount int64 `json:"nicEffectiveCount"`
}

type ResourceDetails struct {
	LargestMemoryPrimaryNodeGroup    *NodegroupDetails      `json:"largestMemoryPrimaryNodeGroup,omitempty"`
	SmallestMemorySecondaryNodeGroup *NodegroupDetails      `json:"smallestMemorySecondaryNodeGroup,omitempty"`
	ActivationNode                   *ActivationNodeDetails `json:"activationNode,omitempty"`
}

// ResourceDetailsV2 represents the enhanced resource details containing information about all memory tiers
type ResourceDetailsV2 struct {
	Nodegroups      []*NodeGroupTier      `json:"nodegroups"`
	ActivationNodes []*ActivationNodeTier `json:"activation_nodes"`
}

func (d ResourceDetails) sanityCheck() error {
	if d.LargestMemoryPrimaryNodeGroup != nil &&
		d.LargestMemoryPrimaryNodeGroup.MemoryxEffectiveMemoryBytes < d.LargestMemoryPrimaryNodeGroup.MemoryxMemoryBytes {
		return CriticalErrorf(
			"invalid cluster resource configuration. Largest memory nodegroup %s has less actual memory than the configured cluster default (%d < %d). %s",
			d.LargestMemoryPrimaryNodeGroup.Name,
			d.LargestMemoryPrimaryNodeGroup.MemoryxEffectiveMemoryBytes,
			d.LargestMemoryPrimaryNodeGroup.MemoryxMemoryBytes,
			wsapisv1.ContactCerebrasSupport)
	}
	if d.SmallestMemorySecondaryNodeGroup != nil &&
		d.SmallestMemorySecondaryNodeGroup.MemoryxEffectiveMemoryBytes < d.SmallestMemorySecondaryNodeGroup.MemoryxMemoryBytes {
		return CriticalErrorf(
			"invalid cluster resource configuration. Smallest memory nodegroup %s has less actual memory than the configured cluster default (%d < %d). %s",
			d.SmallestMemorySecondaryNodeGroup.Name,
			d.SmallestMemorySecondaryNodeGroup.MemoryxEffectiveMemoryBytes,
			d.SmallestMemorySecondaryNodeGroup.MemoryxMemoryBytes,
			wsapisv1.ContactCerebrasSupport)
	}
	if d.ActivationNode != nil {
		if d.ActivationNode.ActivationEffectiveMemoryBytes < d.ActivationNode.ActivationMemoryBytes {
			return CriticalErrorf(
				"invalid cluster resource configuration. Activation node %s has less actual memory than the configured cluster default (%d < %d). %s",
				d.ActivationNode.Name,
				d.ActivationNode.ActivationEffectiveMemoryBytes,
				d.ActivationNode.ActivationMemoryBytes,
				wsapisv1.ContactCerebrasSupport)
		}
		if d.ActivationNode.NicEffectiveCount < d.ActivationNode.NicCount {
			return CriticalErrorf(
				"invalid cluster resource configuration. Activation node %s has less NICs than the configured cluster default (%d < %d). %s",
				d.ActivationNode.Name,
				d.ActivationNode.NicEffectiveCount,
				d.ActivationNode.NicCount,
				wsapisv1.ContactCerebrasSupport)
		}
	}
	return nil
}

// sortMemxNgDetails assumes input is only memx, returns list of NG by most optimal for scheduling to least
func sortMemxNgDetails(rs []resource.Resource) []*NodegroupDetails {
	if len(rs) == 0 {
		return nil
	}
	grpDetail := make(map[string]*NodegroupDetails)
	for _, r := range rs {
		g := grpDetail[r.Group().Name()]
		if g == nil {
			g = &NodegroupDetails{}
		}
		mem := int64(r.Subresources()[resource.MemSubresource]) << 20 // Mi to bytes
		if g.MemoryxEffectiveMemoryBytes == 0 || g.MemoryxEffectiveMemoryBytes > mem {
			g.MemoryxEffectiveMemoryBytes = mem
		}
		g.Name = r.Group().Name()
		g.MemoryxEffectiveCount++
		// not using properties.populated since this should not take into account current cluster health state
		g.Populated = g.MemoryxEffectiveCount >= wsapisv1.MinMemxPerPopNodegroup
		g.MemoryxBandwidthBps = wsapisv1.DefaultBandwidthBps
		grpDetail[r.Group().Name()] = g
	}
	rv := common.Values(grpDetail)

	// sort by (populated, mem desc, number replicas desc, name)
	sort.Slice(rv, func(i, j int) bool {
		if rv[i].Populated != rv[j].Populated {
			return rv[i].Populated
		}
		if rv[i].MemoryxEffectiveMemoryBytes != rv[j].MemoryxEffectiveMemoryBytes {
			return rv[i].MemoryxEffectiveMemoryBytes > rv[j].MemoryxEffectiveMemoryBytes
		}
		if rv[i].MemoryxEffectiveCount != rv[j].MemoryxEffectiveCount {
			return rv[i].MemoryxEffectiveCount > rv[j].MemoryxEffectiveCount
		}
		return strings.Compare(rv[i].Name, rv[j].Name) == -1 // stable order
	})

	return rv
}

// buildAxDetails returns the activation node details
func buildAxDetails(rs []resource.Resource) *ActivationNodeDetails {
	if len(rs) == 0 {
		return nil
	}

	axDetails := make(map[string]*ActivationNodeDetails)
	for _, r := range rs {
		n := &ActivationNodeDetails{
			Name:                           r.Name(),
			ActivationCountPerCsx:          axCountPerCsxDefault,
			ActivationEffectiveCountPerCsx: wsapisv1.AxCountPerSystem,
			NicCount:                       nicCountPerAx,
			NicBandwidthBps:                wsapisv1.DefaultAxBandwidthBps,
			ActivationBandwidthBps:         wsapisv1.DefaultAxBandwidthBps,
		}

		if axMemoryBytes == 0 {
			axMemoryBytes = activationMemBytesV2Default - int(nodeReservedMemBytes.Value())
		}
		n.ActivationMemoryBytes = int64(axMemoryBytes)

		nicCount := 0
		for srKey, srVal := range r.Subresources() {
			if srKey == resource.MemSubresource {
				mem := int64(srVal) << 20 // Mi to bytes
				n.ActivationEffectiveMemoryBytes = mem
			} else if strings.HasPrefix(srKey, resource.NodeNicSubresource) {
				nicCount += 1
				bwBps := int64(srVal * wsapisv1.DefaultBandwidthBps / resource.DefaultNicBW)
				if n.NicEffectiveBandwidthBps == 0 || n.NicEffectiveBandwidthBps > bwBps {
					n.NicEffectiveBandwidthBps = bwBps
					n.ActivationEffectiveBandwidthBps = bwBps
				}
			}
		}
		n.NicEffectiveCount = int64(nicCount)
		axDetails[r.Name()] = n
	}
	rv := common.Values(axDetails)

	// sort by (mem desc, nic count desc, bandwidth bps desc, name)
	// as of rel-2.4, memory, nic count and bandwidth bps should be consistent across all AX nodes in a cluster
	sort.Slice(rv, func(i, j int) bool {
		if rv[i].ActivationEffectiveMemoryBytes != rv[j].ActivationEffectiveMemoryBytes {
			return rv[i].ActivationEffectiveMemoryBytes > rv[j].ActivationEffectiveMemoryBytes
		}
		if rv[i].NicEffectiveCount != rv[j].NicEffectiveCount {
			return rv[i].NicEffectiveCount > rv[j].NicEffectiveCount
		}
		if rv[i].NicEffectiveBandwidthBps != rv[j].NicEffectiveBandwidthBps {
			return rv[i].NicEffectiveBandwidthBps > rv[j].NicEffectiveBandwidthBps
		}
		return strings.Compare(rv[i].Name, rv[j].Name) == -1 // stable order
	})

	return axDetails[rv[0].Name]
}

// buildActivationNodeTiers categorizes activation nodes by tier for V2 resource details
func buildActivationNodeTiers(rs []resource.Resource) []*ActivationNodeTier {
	if len(rs) == 0 {
		return nil
	}

	// Currently we only have regular tier for activation nodes
	regularTier := &ActivationNodeTier{
		Type:                           NodeGroupTierRegular,
		Count:                          0,
		ActivationCountPerCsx:          axCountPerCsxDefault,
		ActivationEffectiveCountPerCsx: wsapisv1.AxCountPerSystem,
		NicCount:                       nicCountPerAx,
		NicBandwidthBps:                wsapisv1.DefaultAxBandwidthBps,
	}

	if axMemoryBytes == 0 {
		axMemoryBytes = activationMemBytesV2Default - int(nodeReservedMemBytes.Value())
	}
	regularTier.ActivationMemoryBytes = int64(axMemoryBytes)

	// Group activation nodes by tier
	smallestMemory := int64(0)
	smallestBandwidth := int64(0)
	minNicCount := int64(0)

	for _, r := range rs {
		regularTier.Count++

		nicCount := int64(0)
		mem := int64(0)
		minBw := int64(0)

		for srKey, srVal := range r.Subresources() {
			if srKey == resource.MemSubresource {
				mem = int64(srVal) << 20 // Mi to bytes
				if smallestMemory == 0 || smallestMemory > mem {
					smallestMemory = mem
				}
			} else if strings.HasPrefix(srKey, resource.NodeNicSubresource) {
				nicCount++
				bwBps := int64(srVal * wsapisv1.DefaultBandwidthBps / resource.DefaultNicBW)
				if minBw == 0 || minBw > bwBps {
					minBw = bwBps
				}
			}
		}

		if minNicCount == 0 || minNicCount > nicCount {
			minNicCount = nicCount
		}

		if smallestBandwidth == 0 || smallestBandwidth > minBw {
			smallestBandwidth = minBw
		}
	}

	regularTier.ActivationEffectiveMemoryBytes = smallestMemory
	regularTier.NicEffectiveCount = minNicCount
	regularTier.NicEffectiveBandwidthBps = smallestBandwidth

	return []*ActivationNodeTier{regularTier}
}

// buildNodeGroupTiers categorizes node groups by memory tier for V2 resource details
func buildNodeGroupTiers(memxGroups []*NodegroupDetails) []*NodeGroupTier {
	// Initialize tiers
	regularTier := &NodeGroupTier{
		Type:         NodeGroupTierRegular,
		MemoryxCount: memxPrimaryCount,
	}

	largeTier := &NodeGroupTier{
		Type:         NodeGroupTierLarge,
		MemoryxCount: memxPrimaryCount,
	}

	xlargeTier := &NodeGroupTier{
		Type:         NodeGroupTierXLarge,
		MemoryxCount: memxPrimaryCount,
	}

	var secondaryTier *NodeGroupTier
	if !wsapisv1.IsV2Network {
		secondaryTier = &NodeGroupTier{
			Type:         NodeGroupTierSecondary,
			MemoryxCount: memxSecondaryV1Count, // 4 for secondary tier
		}
	}

	// Set memory values based on networking type
	if wsapisv1.IsV2Network {
		// Initialize V2 memory values if not set
		if memxRegularMemBytes == 0 {
			memxRegularMemBytes = memxRegularMemBytesV2Default - int(nodeReservedMemBytes.Value())
		}
		if memxLargeMemBytes == 0 {
			memxLargeMemBytes = memxLargeMemBytesV2Default - int(nodeReservedMemBytes.Value())
		}
		if memxXLargeMemBytes == 0 {
			memxXLargeMemBytes = memxXLargeMemBytesV2Default - int(nodeReservedMemBytes.Value())
		}

		regularTier.MemoryxMemoryBytes = int64(memxRegularMemBytes)
		largeTier.MemoryxMemoryBytes = int64(memxLargeMemBytes)
		xlargeTier.MemoryxMemoryBytes = int64(memxXLargeMemBytes)
		// Secondary tier doesn't exist in V2
		secondaryTier = nil
	} else {
		// Initialize V1 memory values if not set
		if memxRegularMemBytes == 0 {
			memxRegularMemBytes = memxRegularMemBytesV1Default - int(nodeReservedMemBytes.Value())
		}
		if memxLargeMemBytes == 0 {
			memxLargeMemBytes = memxLargeMemBytesV1Default - int(nodeReservedMemBytes.Value())
		}
		if memxSecondaryMemBytes == 0 {
			memxSecondaryMemBytes = memxRegularMemBytesV1Default - int(nodeReservedMemBytes.Value())
		}

		regularTier.MemoryxMemoryBytes = int64(memxRegularMemBytes)
		largeTier.MemoryxMemoryBytes = int64(memxLargeMemBytes)
		secondaryTier.MemoryxMemoryBytes = int64(memxSecondaryMemBytes)
		// xlarge doesn't exist in V1
		xlargeTier = nil
	}

	tiers := []*NodeGroupTier{regularTier, largeTier}
	if xlargeTier != nil {
		tiers = append(tiers, xlargeTier)
	}
	if secondaryTier != nil {
		tiers = append(tiers, secondaryTier)
	}

	// Categorize each node group into a tier
	for _, ng := range memxGroups {
		memBytes := ng.MemoryxEffectiveMemoryBytes
		targetTier := regularTier
		if wsapisv1.IsV2Network {
			if memBytes >= int64(memxXLargeMemBytes) {
				targetTier = xlargeTier
			} else if memBytes >= int64(memxLargeMemBytes) {
				targetTier = largeTier
			}
		} else {
			if memBytes >= int64(memxLargeMemBytes) {
				targetTier = largeTier
			} else if ng.MemoryxEffectiveCount <= memxSecondaryV1Count {
				targetTier = secondaryTier
			}
		}

		if targetTier != nil {
			targetTier.Count++

			if targetTier.MemoryxEffectiveCount == 0 {
				targetTier.MemoryxEffectiveCount = ng.MemoryxEffectiveCount
			}
			if targetTier.MemoryxEffectiveMemoryBytes == 0 || targetTier.MemoryxEffectiveMemoryBytes > memBytes {
				targetTier.MemoryxEffectiveMemoryBytes = memBytes
			}
			targetTier.MemoryxBandwidthBps = ng.MemoryxBandwidthBps
		}
	}

	// Return all tiers regardless of availability
	return tiers
}

// summarizeDetailsV2 creates the enhanced V2 resource details with tiers
func summarizeDetailsV2(c *resource.Collection, namespace string) (*ResourceDetailsV2, bool) {
	// memxRegularMemBytes is a global var which allows tests to override
	// we will recalculate the value in case not initialized
	if memxRegularMemBytes == 0 {
		defaultMemVal := common.Ternary(wsapisv1.IsV2Network, memxRegularMemBytesV2Default, memxRegularMemBytesV1Default)
		memxRegularMemBytes = defaultMemVal - int(nodeReservedMemBytes.Value())
	}

	propFilter := map[string][]string{
		resource.RoleMemoryPropKey: {""},
		common.NamespaceKey:        {namespace},
	}
	filter := resource.NewFilter(rlv1.NodeResourceType, propFilter, nil)
	memxNodes := c.Filter(filter)

	memxGroups := sortMemxNgDetails(memxNodes.List())

	rd := ResourceDetailsV2{
		Nodegroups: buildNodeGroupTiers(memxGroups),
	}

	// Add activation node details if applicable
	if wsapisv1.UseAxScheduling {
		propFilter = map[string][]string{
			resource.RoleAxPropKey: {""},
			common.NamespaceKey:    {namespace},
		}
		filter = resource.NewFilter(rlv1.NodeResourceType, propFilter, nil)
		axNodes := c.Filter(filter)
		if axNodes.Size() > 0 {
			rd.ActivationNodes = buildActivationNodeTiers(axNodes.List())
		}
	}

	return &rd, true
}

func summarizeDetails(c *resource.Collection, namespace string) (*ResourceDetails, bool) {
	// memxRegularMemBytes is a global var which allows tests to override
	// we will recalculate the value in case not initialized
	if memxRegularMemBytes == 0 {
		defaultMemVal := common.Ternary(wsapisv1.IsV2Network, memxRegularMemBytesV2Default, memxRegularMemBytesV1Default)
		memxRegularMemBytes = defaultMemVal - int(nodeReservedMemBytes.Value())
	}

	rd := ResourceDetails{}
	// emulate the logic of scheduler_hint which sets the ng selector for a group in order to enforce group->ns assignment
	// in future, this would be better as a NS selector
	propFilter := map[string][]string{
		resource.RoleMemoryPropKey: {""},
		common.NamespaceKey:        {namespace},
	}
	filter := resource.NewFilter(rlv1.NodeResourceType, propFilter, nil)
	memxNodes := c.Filter(filter)

	memxGroups := sortMemxNgDetails(memxNodes.List())
	detailsGenerated := false

	// generate nodegroup resource details if there is at least one pop group in the namespace
	if len(memxGroups) > 0 && memxGroups[0].Populated {
		detailsGenerated = true
		rd.LargestMemoryPrimaryNodeGroup = &NodegroupDetails{
			Name:                        memxGroups[0].Name,
			Populated:                   memxGroups[0].Populated,
			MemoryxEffectiveCount:       memxGroups[0].MemoryxEffectiveCount,
			MemoryxEffectiveMemoryBytes: memxGroups[0].MemoryxEffectiveMemoryBytes,
			MemoryxBandwidthBps:         memxGroups[0].MemoryxBandwidthBps,
			MemoryxCount:                memxPrimaryCount,
			MemoryxMemoryBytes:          int64(memxRegularMemBytes),
		}

		// We should only set secondary nodegroup in V1 networking cluster.
		// Prior to rel-2.4, we had been mistakenly setting this field even if in V2 networking clusters.
		// To provide more accurate estimates, consumers should use the activation details further down for
		// V2 networking clusters.
		// Since the mistake did not cause a wide-spread issue, hence we still set this field in rel-2.4 for a
		// more graceful backward compatible handling.
		// TODO: In rel-2.5, set SmallestMemorySecondaryNodeGroup only in V1 networking clusters
		rd.SmallestMemorySecondaryNodeGroup = &NodegroupDetails{
			Name:                        memxGroups[len(memxGroups)-1].Name,
			Populated:                   memxGroups[len(memxGroups)-1].Populated,
			MemoryxEffectiveCount:       memxGroups[len(memxGroups)-1].MemoryxEffectiveCount,
			MemoryxEffectiveMemoryBytes: memxGroups[len(memxGroups)-1].MemoryxEffectiveMemoryBytes,
			MemoryxBandwidthBps:         memxGroups[len(memxGroups)-1].MemoryxBandwidthBps,
			MemoryxCount:                memxSecondaryV1Count,
			MemoryxMemoryBytes:          int64(memxRegularMemBytes),
		}
	}

	// generate activation node details if AX scheduler is enabled and there is at least one AX node
	if wsapisv1.UseAxScheduling {
		propFilter = map[string][]string{
			resource.RoleAxPropKey: {""},
			common.NamespaceKey:    {namespace},
		}
		filter = resource.NewFilter(rlv1.NodeResourceType, propFilter, nil)
		axNodes := c.Filter(filter)
		if axNodes.Size() > 0 {
			// Downstream assumes resource details (if populated) should always contain "LargestMemoryPrimaryNodeGroup".
			// If this is changed in future (inference might not need pop nodegroups in sessions), we can set
			// "detailsGenerated" to true here as well.
			rd.ActivationNode = buildAxDetails(axNodes.List())
		}
	}

	return &rd, detailsGenerated
}

// canCreateResourceDetails prefetches whether resource details could be created for a job.
// This is needed to set the `WsJobResourceDetailsFPEnv` env var of a pod before the pod gets created.
func (jobController *WSJobController) canCreateResourceDetails(ctx context.Context, wsjob *wsapisv1.WSJob) bool {
	details, err := jobController.createResourceDetails(ctx, wsjob)
	return err == nil && details != nil
}

func (jobController *WSJobController) canCreateResourceDetailsV2(ctx context.Context, wsjob *wsapisv1.WSJob) bool {
	details, err := jobController.createResourceDetailsV2(ctx, wsjob)
	return err == nil && details != nil
}

// createResourceDetails for compile jobs, otherwise returning nil. Errors are returned if a resource details was
// created but the resulting resource details has invalid contents due to job-operator misconfiguration
func (jobController *WSJobController) createResourceDetails(ctx context.Context, wsjob *wsapisv1.WSJob) (*ResourceDetails, error) {
	// resource details is only required for compile jobs
	if !wsjob.IsCompile() {
		return nil, nil
	}

	details, ok := summarizeDetails(jobController.Cfg.Resources, wsjob.Namespace)
	if !ok {
		log.FromContext(ctx).Info(
			"Warning: insufficient nodegroups or activation node found during resource details generation, skipping")
		return nil, nil
	}

	if err := details.sanityCheck(); err != nil {
		return nil, err
	}

	return details, nil
}

// createResourceDetailsV2 creates enhanced resource details with tiers for compile jobs
func (jobController *WSJobController) createResourceDetailsV2(ctx context.Context, wsjob *wsapisv1.WSJob) (*ResourceDetailsV2, error) {
	// resource details is only required for compile jobs
	if !wsjob.IsCompile() {
		return nil, nil
	}

	details, ok := summarizeDetailsV2(jobController.Cfg.Resources, wsjob.Namespace)
	if !ok {
		log.FromContext(ctx).Info(
			"Warning: insufficient nodegroups found during v2 resource details generation, skipping")
		return nil, nil
	}

	if err := details.sanityCheck(); err != nil {
		return nil, err
	}

	return details, nil
}

// addResourceDetailsToCM adds namespace specific resource info to compile jobs to be used by compiler
// for topology specific optimizations
func (jobController *WSJobController) addResourceDetailsToCM(ctx context.Context, job *wsapisv1.WSJob, cm *corev1.ConfigMap) error {
	// create/update the Job's NS's CM
	details, err := jobController.createResourceDetails(ctx, job)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to create resource details")
		return err
	}
	if details == nil {
		return nil
	}

	jsonBytes, err := json.MarshalIndent(details, "", "  ")
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to marshal resource details to json")
		return err
	}

	cm.Data[wsapisv1.WsJobResourceDetailsName] = string(jsonBytes)

	// Add v2 resource details
	detailsV2, err := jobController.createResourceDetailsV2(ctx, job)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to create v2 resource details")
		return err
	}
	if detailsV2 != nil {
		jsonBytesV2, err := json.MarshalIndent(detailsV2, "", "  ")
		if err != nil {
			log.FromContext(ctx).Error(err, "failed to marshal v2 resource details to json")
			return err
		}

		cm.Data[wsapisv1.WsJobResourceDetailsV2Name] = string(jsonBytesV2)
	}

	return nil
}

func intEnvOrDefault(k string, defaultVal int) int {
	rv := defaultVal
	v := os.Getenv(k)
	if v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			rv = val
		}
	}
	logrus.WithField(k, rv).Infof("Set Envvar")
	return rv
}

func quantityEnvOrDefault(k string, defaultVal apiresource.Quantity) apiresource.Quantity {
	rv := defaultVal
	v := os.Getenv(k)
	if v != "" {
		if val, err := apiresource.ParseQuantity(v); err == nil {
			rv = val
		}
	}
	logrus.WithField(k, rv.String()).Infof("Set Envvar")
	return rv
}

func init() {
	memxPrimaryCount = intEnvOrDefault(memxPrimaryCountEnv, memxPrimaryCountDefault)
	memxSecondaryV1Count = intEnvOrDefault(memxSecondaryCountEnv, memxSecondaryCountV1Default)
	nodeReservedMemBytes = quantityEnvOrDefault(resource_config.NodeReservedMemEnvvar, resource_config.NodeReservedMemDefault)
}

func (d ResourceDetailsV2) sanityCheck() error {
	for _, tier := range d.Nodegroups {
		if tier.Count == 0 {
			continue
		}
		if tier.MemoryxEffectiveMemoryBytes < tier.MemoryxMemoryBytes {
			return CriticalErrorf(
				"invalid cluster resource configuration. %s tier nodegroup has less actual memory than the configured cluster default (%d < %d). %s",
				tier.Type,
				tier.MemoryxEffectiveMemoryBytes,
				tier.MemoryxMemoryBytes,
				wsapisv1.ContactCerebrasSupport)
		}
	}

	// Check activation nodes if present
	for _, tier := range d.ActivationNodes {
		if tier.Count == 0 {
			continue
		}
		if tier.ActivationEffectiveMemoryBytes < tier.ActivationMemoryBytes {
			return CriticalErrorf(
				"invalid cluster resource configuration. Activation node has less actual memory than the configured cluster default (%d < %d). %s",
				tier.ActivationEffectiveMemoryBytes,
				tier.ActivationMemoryBytes,
				wsapisv1.ContactCerebrasSupport)
		}
	}

	return nil
}

package helpers

import (
	"fmt"
	"strconv"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource_config"
)

// === ClusterConfig ===

const (
	DefaultMemNodePerSys = wsapisv1.DefaultMinMemxPerPopNodegroup
	DefaultWrkNodePerSys = 2

	DefaultMemNodeMem = 32
	DefaultMemNodeCpu = 8
	DefaultWrkNodeMem = 16
	DefaultWrkNodeCpu = 8
	DefaultMgtNodeMem = 64
	DefaultMgtNodeCpu = 32
	DefaultAxNodeMem  = 64
	DefaultAxNodeCpu  = 32
	DefaultIxNodeMem  = 64
	DefaultIxNodeCpu  = 32
)

type ClusterBuilder struct {
	sysCount int
	c        *resource.Collection
	// Plumbing only done for cfg.SystemMap due to specific testing purposes
	cfg *common.ClusterConfig

	isV2Network             bool
	isInferenceCluster      bool
	coordSeparationEnforced bool
	nvmfEnabledCoordinators int
	nvmfPerDiskBytes        int64
	ixJobSharingEnabled     bool
}

func NewClusterBuilder(coordNodeCount int) *ClusterBuilder {
	c := &resource.Collection{}
	for i := 0; i < coordNodeCount; i++ {
		c.Add(resource.NewResource(
			rlv1.NodeResourceType,
			fmt.Sprintf("coordinator-%d", i),
			map[string]string{
				resource.RoleMgmtPropKey:      "",
				resource.RoleCrdPropKey:       "",
				resource.HealthProp:           resource.HealthOk,
				resource.NamespacePropertyKey: common.Namespace,
			},
			resource.Subresources{
				resource.CPUSubresource: DefaultMgtNodeCpu,
				resource.MemSubresource: DefaultMgtNodeMem,
				resource.PodSubresource: 32,
			},
			nil))
	}
	return &ClusterBuilder{
		c:   c,
		cfg: &common.ClusterConfig{SystemMap: map[string]*systemv1.SystemSpec{}},
	}
}

func NewClusterBuilderWithMgmtCoordSeparation(mgmtNodeCount, coordNodeCount int) *ClusterBuilder {
	c := &resource.Collection{}
	for i := 0; i < mgmtNodeCount; i++ {
		c.Add(resource.NewResource(
			rlv1.NodeResourceType,
			fmt.Sprintf("management-%d", i),
			map[string]string{
				resource.RoleMgmtPropKey:      "",
				resource.HealthProp:           resource.HealthOk,
				common.StorageTypeKey:         common.CephStorageType,
				resource.NamespacePropertyKey: common.Namespace,
			},
			resource.Subresources{
				resource.CPUSubresource: DefaultMgtNodeCpu,
				resource.MemSubresource: DefaultMgtNodeMem,
				resource.PodSubresource: 32,
			},
			nil))
	}
	for i := 0; i < coordNodeCount; i++ {

		c.Add(resource.NewResource(
			rlv1.NodeResourceType,
			fmt.Sprintf("coordinator-%d", i),
			map[string]string{
				resource.RoleCrdPropKey:       "",
				resource.HealthProp:           resource.HealthOk,
				resource.NamespacePropertyKey: common.Namespace,
			},
			resource.Subresources{
				resource.CPUSubresource: DefaultMgtNodeCpu,
				resource.MemSubresource: DefaultMgtNodeMem,
				resource.PodSubresource: 32,
			},
			nil))
	}
	return &ClusterBuilder{c: c, cfg: &common.ClusterConfig{SystemMap: map[string]*systemv1.SystemSpec{}}}
}

func (b *ClusterBuilder) WithV2Network() *ClusterBuilder {
	b.isV2Network = true
	return b
}

func (b *ClusterBuilder) IsV2NetworkEnabled() bool {
	return b.isV2Network
}

func (b *ClusterBuilder) WithIxJobSharing() *ClusterBuilder {
	b.ixJobSharingEnabled = true
	return b
}

func (b *ClusterBuilder) IsIxJobSharingEnabled() bool {
	return b.ixJobSharingEnabled
}

func (b *ClusterBuilder) WithCoordinatorSeparationEnforced() *ClusterBuilder {
	b.coordSeparationEnforced = true
	return b
}

func (b *ClusterBuilder) IsManagementSeparationEnforced() bool {
	return b.coordSeparationEnforced
}

func (b *ClusterBuilder) WithNvmfCoordinatorNodesEnabled(count int, perDiskBytes int64) *ClusterBuilder {
	b.nvmfEnabledCoordinators = count
	b.nvmfPerDiskBytes = perDiskBytes
	return b
}

func (b *ClusterBuilder) WithCoordinator(id int) *ClusterBuilder {
	b.c.Add(resource.NewResource(
		rlv1.NodeResourceType,
		fmt.Sprintf("coordinator-%d", id),
		map[string]string{
			resource.RoleCrdPropKey:       "",
			resource.HealthProp:           resource.HealthOk,
			resource.NamespacePropertyKey: common.Namespace,
		},
		resource.Subresources{
			resource.CPUSubresource: DefaultMgtNodeCpu,
			resource.MemSubresource: DefaultMgtNodeMem,
			resource.PodSubresource: 32,
		},
		nil))
	return b
}

func (b *ClusterBuilder) WithSystems(count int, stamp string) *ClusterBuilder {
	for i := 0; i < count; i++ {
		b.WithSystem(stamp)
	}
	return b
}

func (b *ClusterBuilder) WithSystem(stamp string) *ClusterBuilder {
	sysCount := b.c.Filter(resource.NewSystemFilter()).Size()
	systemName := fmt.Sprintf("system-%d", sysCount)
	props := map[string]string{"name": systemName, resource.HealthProp: "ok",
		resource.NamespacePropertyKey: common.Namespace}
	if stamp != "" {
		if wsapisv1.IsV2Network || b.isV2Network {
			props[resource.StampPropertyKey] = stamp
		}
	}
	r := resource.NewResource(
		rlv1.SystemResourceType,
		systemName,
		props,
		map[string]int{
			resource.SystemSubresource:     1,
			resource.SystemPortSubresource: common.CSVersionSpec.NumPorts,
		},
		nil,
	)
	b.c.Add(r)

	var allHealthyPorts []systemv1.Port
	for i := 0; i < common.CSVersionSpec.NumPorts; i++ {
		allHealthyPorts = append(allHealthyPorts, systemv1.Port{
			Name:     fmt.Sprintf("cs-port-%d", i),
			HasError: false,
		})
	}
	b.cfg.SystemMap[systemName] = &systemv1.SystemSpec{
		Name:  systemName,
		Type:  string(common.CSVersionSpec.Version),
		Ports: allHealthyPorts,
	}
	return b
}

func (b *ClusterBuilder) WithPopulatedNodeGroups(count int, memClass string) *ClusterBuilder {
	return b.withPopulatedNodegroupsInternal(count, memClass, false)
}

func (b *ClusterBuilder) WithNvmfEnabledPopulatedNodegroups(count int, memClass string) *ClusterBuilder {
	return b.withPopulatedNodegroupsInternal(count, memClass, true)
}

func (b *ClusterBuilder) withPopulatedNodegroupsInternal(count int, memClass string, nvmfEnabled bool) *ClusterBuilder {
	for i := 0; i < count; i++ {
		b.WithNodegroup(DefaultMemNodePerSys, DefaultWrkNodePerSys, memClass, nvmfEnabled)
	}
	return b
}

// WithDepopulatedNodeGroups adds a nodegroup with "depopulated" memory nodes intended to be used by secondary node
// groups only.
func (b *ClusterBuilder) WithDepopulatedNodeGroups(count int) *ClusterBuilder {
	for i := 0; i < count; i++ {
		b.WithNodegroup(1, 2, "", false)
	}
	return b
}

func (b *ClusterBuilder) WithNodegroup(memCount, workerCount int, memClass string, nvmfEnabled bool) *ClusterBuilder {
	ngName := ""
	ngIdx := -1
	groupProps := map[string]string{}
	ngCount := len(b.c.ListGroups())
	ngIdx = ngCount
	ngName = "nodegroup" + strconv.Itoa(ngCount)
	if memCount >= wsapisv1.MinMemxPerPopNodegroup {
		groupProps[wsapisv1.MemxPopNodegroupProp] = "true"
	}
	nodeMem := DefaultMemNodeMem
	if memClass != "" {
		groupProps[wsapisv1.MemxMemClassProp] = memClass
		if memClass == wsapisv1.PopNgTypeLarge {
			nodeMem = resource_config.MemxMemClassLargeThreshold >> 10
		} else if memClass == wsapisv1.PopNgTypeXLarge {
			nodeMem = resource_config.MemxMemClassXLargeThreshold >> 10
		}
	}
	b.c.AddGroup(resource.NewGroup(ngName, groupProps))

	for n := 0; n < memCount; n++ {
		props := map[string]string{resource.GroupPropertyKey: ngName, resource.RoleMemoryPropKey: "",
			resource.HealthProp: "ok", resource.NamespacePropertyKey: common.Namespace}
		if nvmfEnabled {
			props[resource.NvmfInitiatorPropKey] = ""
		}
		b.c.Add(resource.NewResource(
			rlv1.NodeResourceType,
			fmt.Sprintf("memx-%d-%d", ngIdx, n),
			props,
			resource.Subresources{
				resource.CPUSubresource: DefaultMemNodeCpu,
				resource.MemSubresource: nodeMem,
				resource.PodSubresource: 32,
			},
			nil))
	}
	for n := 0; n < workerCount; n++ {
		b.c.Add(resource.NewResource(
			rlv1.NodeResourceType,
			fmt.Sprintf("worker-%d-%d", ngIdx, n),
			map[string]string{resource.GroupPropertyKey: ngName, resource.RoleWorkerPropKey: "",
				resource.HealthProp: "ok", resource.NamespacePropertyKey: common.Namespace},
			resource.Subresources{
				resource.CPUSubresource: DefaultWrkNodeCpu,
				resource.MemSubresource: DefaultWrkNodeMem,
				resource.PodSubresource: 32,
			},
			nil))
	}
	return b
}

func (b *ClusterBuilder) WithBR(count int) *ClusterBuilder {
	systems := b.c.Filter(resource.NewSystemFilter()).ListResNames()
	for i := 0; i < count; i++ {
		port := i % 12
		res := resource.Subresources{resource.PodSubresource: 32, resource.PodInstanceIdSubresource: 10}
		// Nic subresources are set up in `InitializeResourceManagement` in e2e flow
		// We need to explicitly define them here because the test does not invoke that function
		if i < count/12*12 {
			if wsapisv1.IsV2Network || b.isV2Network {
				res[resource.NodeNicSubresource] = 6
			} else {
				res[fmt.Sprintf("%s-%d", resource.NodeNicSubresource, port)] = 6
			}
		} else {
			// if count is not dividable by 12, it means top level BR will be in half modes
			// e.g. for 8CS2, it will have 30 BR nodes, BR24-BR29 will be connected with 2 CS ports separately
			port = (i - count/12*12) * 2
			if wsapisv1.IsV2Network || b.isV2Network {
				res[resource.NodeNicSubresource] = 6
			} else {
				res[fmt.Sprintf("%s-%d", resource.NodeNicSubresource, port)] = 3
				res[fmt.Sprintf("%s-%d", resource.NodeNicSubresource, port+1)] = 3
			}
		}
		props := map[string]string{resource.RoleBRPropKey: "", resource.HealthProp: "ok"}
		if wsapisv1.IsV2Network || b.isV2Network {
			// Nic assignment on the affinity properties are set up in `InitializeResourceManagement` in e2e flow
			for _, system := range systems {
				props[fmt.Sprintf("%s/%s-%d", resource.PortAffinityPropKeyPrefix, system, i%12)] = ""
			}
		}
		b.c.Add(resource.NewResource(
			rlv1.NodeResourceType,
			fmt.Sprintf("br-%d", i),
			props,
			res,
			nil))
	}
	return b
}

// WithAX adds “Activation” (AX) nodes to the fake cluster.
// stamp may be empty for a single‑stamp test.
func (b *ClusterBuilder) WithAX(count int, stamp string) *ClusterBuilder {
	return b.WithAXorIX(count, stamp, common.RoleActivation)
}

func (b *ClusterBuilder) WithIX(count int, stamp string) *ClusterBuilder {
	return b.WithAXorIX(count, stamp, common.RoleInferenceDriver)
}

func (b *ClusterBuilder) WithAXorIX(count int, stamp string, role common.NodeRole) *ClusterBuilder {
	isAx := role == common.RoleActivation // false: IX
	var res []resource.Resource
	systems := b.c.Filter(resource.NewSystemFilter(fmt.Sprintf("%s:%s", resource.StampPropertyKey, stamp))).ListResNames()
	nic0SrName := fmt.Sprintf("%s/nic-0", resource.NodeNicSubresource)
	nic1SrName := fmt.Sprintf("%s/nic-1", resource.NodeNicSubresource)
	for i := 0; i < count; i++ {
		nodeCount := b.c.Filter(resource.NewFilter(
			rlv1.NodeResourceType, role.AsMatcherProperty(), nil)).Size()
		props := map[string]string{
			resource.HealthProp:           resource.HealthOk,
			resource.NamespacePropertyKey: common.Namespace,
		}
		if isAx {
			props[resource.RoleAxPropKey] = ""
		} else {
			props[resource.RoleIxPropKey] = ""
		}
		if stamp != "" {
			props[resource.StampPropertyKey] = stamp
		}
		for _, system := range systems {
			// Nic assignment on the affinity properties are set up in `InitializeResourceManagement` in e2e flow
			props[fmt.Sprintf("%s/%s-%d", resource.PortAffinityPropKeyPrefix, system, i%12)] = nic0SrName
			props[fmt.Sprintf("%s/%s-%d", resource.PortAffinityPropKeyPrefix, system, (i+3)%12)] = nic1SrName
		}
		r := resource.NewResource(
			rlv1.NodeResourceType,
			fmt.Sprintf("%s-%d", common.Ternary(isAx, "ax", "ix"), nodeCount),
			props,
			map[string]int{
				resource.PodSubresource: 32,
				resource.CPUSubresource: common.Ternary(isAx, DefaultAxNodeCpu, DefaultIxNodeCpu),
				resource.MemSubresource: common.Ternary(isAx, DefaultAxNodeMem, DefaultIxNodeMem),
				// Nic subresources are set up in `InitializeResourceManagement` in e2e flow
				// We need to explicitly define them here because the test does not invoke that function
				nic0SrName: common.Ternary(isAx, resource.DefaultAxNicBW, resource.DefaultIxNicBW),
				nic1SrName: common.Ternary(isAx, resource.DefaultAxNicBW, resource.DefaultIxNicBW),
			},
			nil,
		)
		// Index building and setting are also performed in `InitializeResourceManagement` in e2e flow
		// We need to explicitly define them here because the test does not invoke that function
		r.WithIndex(resource.SubresourceIndex{
			Subresources: map[string]map[string]bool{
				resource.NodeNicSubresource: {
					nic0SrName: true,
					nic1SrName: true,
				},
			},
		})
		b.c.Add(r)
		res = append(res, r)
	}

	return b
}

func (b *ClusterBuilder) Build() *resource.Collection {
	if b.nvmfEnabledCoordinators > 0 {
		var coordNodes []string
		for _, res := range b.c.List() {
			// TODO: Remove mgmt node check after mgmt/coord separation
			if res.IsMgmtNode() || res.IsCrdNode() {
				coordNodes = append(coordNodes, res.Id())
			}
		}

		targetId := 1
		for _, resId := range common.Sorted(coordNodes) {
			if targetId > b.nvmfEnabledCoordinators {
				break
			}
			index := resource.SubresourceIndex{SubresourceRanges: map[string]map[string]bool{
				resource.NvmfDiskSubresource: {},
			}}
			b.c.UpdateProp(resId, resource.NvmfTargetIdPropKey, strconv.Itoa(targetId))
			for i := 1; i <= resource.NvmfDiskCountPerCoordNode; i++ {
				diskName := resource.NvmfDiskResName(i)
				index.SubresourceRanges[resource.NvmfDiskSubresource][diskName] = true
				b.c.UpdateSubresourceRanges(resId, diskName, []resource.Range{{Start: 0, End: b.nvmfPerDiskBytes}})
			}
			res, _ := b.c.Get(resId)
			res.WithIndex(index)
			targetId += 1
		}
	}

	return b.c
}

func (b *ClusterBuilder) GetCfg() *common.ClusterConfig {
	return b.cfg
}

func NewDefaultClusterBuilder(systemCount, brCount int) *ClusterBuilder {
	return NewClusterBuilder(1).
		WithSystems(systemCount, "").
		WithBR(brCount).
		WithPopulatedNodeGroups(systemCount, wsapisv1.PopNgTypeRegular)
}

func NewCluster(systemCount, brCount int) *resource.Collection {
	return NewDefaultClusterBuilder(systemCount, brCount).
		Build()
}

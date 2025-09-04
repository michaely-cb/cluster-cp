package pkg

import (
	"context"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
)

type NodeStoreMocks struct {
	common.MockQuerier
	nodes    []*corev1.Node
	cpuAlloc map[string]float64
	memAlloc map[string]int64
}

func NewNodeStoreMocks() *NodeStoreMocks {
	return &NodeStoreMocks{
		MockQuerier: common.MockQuerier{},
		cpuAlloc:    map[string]float64{},
		memAlloc:    map[string]int64{},
	}
}

func (m *NodeStoreMocks) QueryNodeCpuAllocatable(ctx context.Context) (map[string]float64, error) {
	return m.cpuAlloc, nil
}

func (m *NodeStoreMocks) QueryNodeMemAllocatable(ctx context.Context) (map[string]int64, error) {
	return m.memAlloc, nil
}

func (m *NodeStoreMocks) List() ([]*corev1.Node, error) {
	return m.nodes, nil
}

func (m *NodeStoreMocks) ListByGroup(group string) ([]*corev1.Node, error) {
	results := []*corev1.Node{}
	for _, node := range m.nodes {
		if node.Labels[common.GroupLabelKey] == group {
			results = append(results, node)
		}
	}
	return results, nil
}

func (m *NodeStoreMocks) ListByLabelSet(labelSet labels.Set) ([]*corev1.Node, error) {
	return m.nodes, nil
}

func (m *NodeStoreMocks) AreNodesNamespaceLabeled() bool {
	return true
}

func (m *NodeStoreMocks) MockNode(role, group string, mem, cpu int) *NodeStoreMocks {
	return m.MockNodeNamedAddressed("", "", "", role, group, mem, cpu)
}

func (m *NodeStoreMocks) MockNodeVersioned(version string) *NodeStoreMocks {
	if len(m.nodes) > 0 {
		m.nodes[len(m.nodes)-1].Labels[wsapisv1.PlatformVersionKey] = version
	}
	return m
}

func (m *NodeStoreMocks) MockNodeNamedAddressed(name, ns, address, role, group string, mem, cpu int) *NodeStoreMocks {
	labels := make(map[string]string)
	if role != "" {
		for _, v := range strings.Split(role, ",") {
			labels["k8s.cerebras.com/node-role-"+v] = ""
		}
		labels["role"] = role
	}
	if group != "" {
		labels[common.GroupLabelKey] = group
	}
	labels[common.NamespaceLabelKey] = ns

	memQuantity := resource.NewQuantity(int64(mem<<20), resource.BinarySI)
	cpuQuantity := resource.NewMilliQuantity(int64(cpu*1000), resource.DecimalSI)

	if name == "" {
		name = "n" + strconv.Itoa(len(m.nodes))
	}
	m.nodes = append(m.nodes, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
			Name:   name,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceMemory: *memQuantity,
				corev1.ResourceCPU:    *cpuQuantity,
			},
			Addresses: []corev1.NodeAddress{
				{
					Address: address,
				},
			},
		},
	})

	m.cpuAlloc[name] = float64(cpu)
	m.memAlloc[name] = int64(mem << 20)

	return m
}

func (m *NodeStoreMocks) BuildCfg() common.ClusterConfig {
	cfg := common.ClusterConfig{}
	nsrs := map[string]namespacev1.NamespaceReservation{}
	groups := map[string]*common.NodeGroup{}
	for _, node := range m.nodes {
		group := node.ObjectMeta.Labels[common.GroupLabelKey]
		role := common.NodeRole(node.Labels["role"])
		cfg.Nodes = append(cfg.Nodes, &common.Node{
			Name:       node.Name,
			Role:       role,
			Properties: map[string]string{"group": group},
		})
		namespace := node.Labels[common.NamespaceLabelKey]
		if namespace == "" {
			// place the node in namespace "test" if namespace is not specified
			namespace = "test"
		}
		nsr, ok := nsrs[namespace]
		if !ok {
			nsrs[namespace] = namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			}}
			nsr = nsrs[namespace]
		}
		if _, ok := groups[group]; !ok && group != "" {
			groups[group] = &common.NodeGroup{Name: group, Properties: map[string]string{}}
			nsr.Status.Nodegroups = append(nsr.Status.Nodegroups, group)
		}
		if role == common.RoleManagement {
			nsr.Status.Nodes = append(nsr.Status.Nodes, node.Name)
		}
		nsrs[namespace] = nsr
	}
	cfg.Groups = common.Values(groups)
	cfg.BuildNodeSysGroupIndices()
	cfg.InitializeNsAssignment(namespacev1.NamespaceReservationList{Items: maps.Values(nsrs)})
	return cfg
}

//go:build default

package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
)

func TestGetUpdatePatches(t *testing.T) {
	patcher := newNodePatcher(
		log.FromContext(context.Background()),
	)
	for _, testcase := range []struct {
		test                    string
		k8sNode                 v1.Node
		coordSeparationEnforced bool
		newNamespace            string
		cfg                     common.ClusterSchema

		expect nodePatches
	}{
		{
			test:    "add missing worker label, group and namespace",
			k8sNode: nodeBuilder{}.build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleWorker, Properties: map[string]string{"group": "ng0"}},
					{Name: "b", Role: common.RoleAny}},
			},
			newNamespace: "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-worker"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-group", Value: "ng0"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1namespace", Value: ""},
				},
				status: nil,
			},
		},
		{
			test: "add missing worker label",
			k8sNode: nodeBuilder{}.
				withLabelV(common.GroupLabelKey, "ng0").
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleWorker, Properties: map[string]string{"group": "ng0"}},
					{Name: "b", Role: common.RoleAny}},
			},
			newNamespace: "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-worker"},
				},
				status: nil,
			},
		},
		{
			test: "replace group value",
			k8sNode: nodeBuilder{}.
				withLabelV(common.GroupLabelKey, "ng0").
				withLabel("k8s.cerebras.com/node-role-worker").
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleWorker, Properties: map[string]string{"group": "ng1"}},
					{Name: "b", Role: common.RoleAny}},
			},
			newNamespace: "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "replace", Path: "/metadata/labels/k8s.cerebras.com~1node-group", Value: "ng1"},
				},
				status: nil,
			},
		},
		{
			test: "replace namespace value",
			k8sNode: nodeBuilder{}.
				withLabelV(common.GroupLabelKey, "ng0").
				withLabel("k8s.cerebras.com/node-role-worker").
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleWorker, Properties: map[string]string{"group": "ng0"}},
					{Name: "b", Role: common.RoleAny}},
			},
			newNamespace: "test",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "replace", Path: "/metadata/labels/k8s.cerebras.com~1namespace", Value: "test"},
				},
				status: nil,
			},
		},
		{
			test:    "add partial missing any label multi-node",
			k8sNode: nodeBuilder{}.withLabel("k8s.cerebras.com/node-role-worker").build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleAny},
					{Name: "b", Role: common.RoleAny}},
			},
			newNamespace: "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-memory"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-broadcastreduce"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1namespace", Value: ""},
				},
				status: nil,
			},
		},
		{
			test: "add partial missing any label single-node",
			k8sNode: nodeBuilder{}.withLabel("k8s.cerebras.com/node-role-worker").
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleAny}},
			},
			newNamespace: "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-management"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-memory"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-broadcastreduce"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-coordinator"},
				},
				status: nil,
			},
		},
		{
			test: "remove node group",
			k8sNode: nodeBuilder{}.withLabelV(common.GroupLabelKey, "ng0").
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleManagement},
					{Name: "b", Role: common.RoleAny}},
			},
			newNamespace: "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-management"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-coordinator"},
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-group"},
				},
				status: nil,
			},
		},
		{
			test: "mgmt/coord separation enforcement in action - remove management label for dedicated coordinator node",
			k8sNode: nodeBuilder{}.
				withLabel("k8s.cerebras.com/node-role-management").
				withLabel("k8s.cerebras.com/node-role-coordinator").
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleManagement},
					{Name: "b", Role: common.RoleAny}},
			},
			coordSeparationEnforced: true,
			newNamespace:            "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-role-management"},
					{Op: "add", Path: "/metadata/labels/ceph-csi"},
				},
				status: nil,
			},
		},
		{
			test: "mgmt/coord separation enforcement in action - remove coordinator label for dedicated management node",
			k8sNode: nodeBuilder{}.
				withLabel("k8s.cerebras.com/node-role-management").
				withLabel("k8s.cerebras.com/node-role-coordinator").
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleManagement, Properties: map[string]string{common.StorageTypeKey: common.CephStorageType}},
					{Name: "b", Role: common.RoleAny}},
			},
			coordSeparationEnforced: true,
			newNamespace:            "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-role-coordinator"},
					{Op: "add", Path: "/metadata/labels/ceph-csi"},
				},
				status: nil,
			},
		},
		{
			test: "dedicated coordinator node - add",
			k8sNode: nodeBuilder{}.
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleManagement},
					{Name: "b", Role: common.RoleAny}},
			},
			coordSeparationEnforced: true,
			newNamespace:            "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-coordinator"},
					{Op: "add", Path: "/metadata/labels/ceph-csi"},
				},
				status: nil,
			},
		},
		{
			test: "dedicated management node - add",
			k8sNode: nodeBuilder{}.
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleManagement, Properties: map[string]string{common.StorageTypeKey: common.CephStorageType}},
					{Name: "b", Role: common.RoleAny}},
			},
			coordSeparationEnforced: true,
			newNamespace:            "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-management"},
					{Op: "add", Path: "/metadata/labels/ceph-csi"},
				},
				status: nil,
			},
		},
		{
			test: "add replace remove csPort",
			k8sNode: nodeBuilder{}.
				withLabel("k8s.cerebras.com/node-role-worker").
				withLabelV(common.GroupLabelKey, "ng0").
				withLabel(common.NamespaceLabelKey).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{
						Name:       "a",
						Role:       common.RoleBroadcastReduce,
						NICs:       nicCsPort(1, 6, 2, 3),
						Properties: map[string]string{"group": "ng1"},
					},
					{Name: "b", Role: common.RoleAny}},
			},
			newNamespace: "",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-role-worker"},
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-broadcastreduce"},
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-group", Value: ""},
				},
				status: []jsonPatchInt{},
			},
		},
		// Compile job resource existed prior to rel-1.8. Should be removed automatically.
		{
			test: "remove management node compile resource if present",
			k8sNode: nodeBuilder{}.
				withLabel("k8s.cerebras.com/node-role-management").
				withLabelV(common.NamespaceLabelKey, "test").
				withAlloc(common.CompileJobNodeResource, 3).
				withCap(common.CompileJobNodeResource, 3).
				build("a"),
			cfg: common.ClusterSchema{
				Nodes: []*common.Node{
					{Name: "a", Role: common.RoleManagement},
					{Name: "b", Role: common.RoleAny}},
			},
			newNamespace: "test",
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "add", Path: "/metadata/labels/k8s.cerebras.com~1node-role-coordinator"},
				},
				status: []jsonPatchInt{
					{Op: "remove", Path: "/status/capacity/cerebras.com~1compile-job"},
					{Op: "remove", Path: "/status/allocatable/cerebras.com~1compile-job"},
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("GetUpdatePatch-%s", testcase.test), func(t *testing.T) {
			clusterCfg := common.ClusterConfig{ClusterSchema: testcase.cfg}
			if testcase.coordSeparationEnforced {
				wsapisv1.IsMultiMgmt = true
				wsapisv1.ManagementSeparationEnforced = true
				defer func() {
					wsapisv1.IsMultiMgmt = false
					wsapisv1.ManagementSeparationEnforced = false
				}()
			}
			clusterCfg.AdjustNodesByClusterSetup()
			actual := patcher.getUpdatePatches(testcase.cfg.Nodes[0], &testcase.k8sNode, testcase.newNamespace)
			assert.ElementsMatch(t, testcase.expect.status, actual.status)
			assert.ElementsMatch(t, testcase.expect.node, actual.node)
		})
	}
}

func TestGetRemovePatches(t *testing.T) {
	for _, testcase := range []struct {
		test                    string
		knode                   v1.Node
		coordSeparationEnforced bool
		expect                  nodePatches
	}{
		{
			test: "remove labels",
			knode: nodeBuilder{}.
				withLabel("k8s.cerebras.com/node-role-worker").
				withLabel("k8s.cerebras.com/node-role-memory").
				withLabelV(common.GroupLabelKey, "ng0").
				build("a"),
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-role-worker"},
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-role-memory"},
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-group"},
				},
				status: nil,
			},
		},
		{
			test: "remove csPort and labels",
			knode: nodeBuilder{}.
				withLabel("k8s.cerebras.com/node-role-broadcastreduce").
				build("a"),
			expect: nodePatches{
				node: []jsonPatchStr{
					{Op: "remove", Path: "/metadata/labels/k8s.cerebras.com~1node-role-broadcastreduce"},
				},
				status: []jsonPatchInt{},
			},
		},
	} {
		t.Run(fmt.Sprintf("GetRemovePatch-%s", testcase.test), func(t *testing.T) {
			actual := getRemovePatches(&testcase.knode)
			assert.ElementsMatch(t, testcase.expect.status, actual.status)
			assert.ElementsMatch(t, testcase.expect.node, actual.node)
		})
	}
}

func nicCsPort(csPortCount ...int) []*common.NetworkInterface {
	var rv []*common.NetworkInterface
	for csPortIdx := 0; csPortIdx < len(csPortCount); csPortIdx += 2 {
		for count := 0; count < csPortCount[csPortIdx+1]; count++ {
			rv = append(rv, &common.NetworkInterface{CsPort: common.Pointer(csPortCount[csPortIdx])})
		}
	}
	return rv
}

type nodeBuilder struct {
	labels map[string]string
	alloc  map[string]int64
	cap    map[string]int64
}

func (n nodeBuilder) withLabel(l string) nodeBuilder {
	return n.withLabelV(l, "")
}

func (n nodeBuilder) withLabelV(k, v string) nodeBuilder {
	if n.labels == nil {
		n.labels = map[string]string{}
	}
	n.labels[k] = v
	return n
}

func (n nodeBuilder) withAlloc(r string, val int64) nodeBuilder {
	if n.alloc == nil {
		n.alloc = map[string]int64{}
	}
	n.alloc[r] = val
	return n
}

func (n nodeBuilder) withCap(r string, val int64) nodeBuilder {
	if n.cap == nil {
		n.cap = map[string]int64{}
	}
	n.cap[r] = val
	return n
}

func (n nodeBuilder) build(name string) v1.Node {
	return v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: n.labels,
		},
		Status: v1.NodeStatus{
			Allocatable: xformResourceList(n.alloc),
			Capacity:    xformResourceList(n.cap),
		},
	}
}

func xformResourceList(v map[string]int64) v1.ResourceList {
	rl := v1.ResourceList{}
	for k, q := range v {
		rl[v1.ResourceName(k)] = *resource.NewQuantity(q, resource.DecimalExponent)
	}
	return rl
}

//go:build integration && cluster_controller

package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	wsresource "cerebras.com/job-operator/common/resource"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

func TestNodeLabelerIntegration(t *testing.T) {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	cfgMgr := wscommon.NewTestClusterConfigMgr(mgr, 1, 0, 0, 1, 1)
	labeler := NewNodeLabeler(mgr.GetClient(), mgr.GetLogger())
	cfgMgr.AddWatcher(labeler.cfgCh)

	nodeCount := 5
	for i := 0; i < nodeCount; i++ {
		require.NoError(t, mgr.GetClient().Create(ctx, makeNode(i)))
	}

	minMemxPerPopNodegroup := wsapisv1.MinMemxPerPopNodegroup
	defer func() {
		wsapisv1.MinMemxPerPopNodegroup = minMemxPerPopNodegroup
	}()
	wsapisv1.MinMemxPerPopNodegroup = 0

	for _, testcase := range []struct {
		name                     string
		nodeSpec                 []string
		enableMultiCoordinator   bool
		expectedCoordinatorIndex []int
	}{
		{
			name:                     "1 mgmt 2 groups",
			nodeSpec:                 []string{"management:ng0", "worker:ng1", "broadcastreduce::2-6", "worker:ng0", "memory:ng1"},
			expectedCoordinatorIndex: []int{0},
		},
		{
			name:                     "1 mgmt 2 groups reorder",
			nodeSpec:                 []string{"worker:ng0", "management:ng0", "worker:ng1", "worker:ng1", "memory:ng1"},
			expectedCoordinatorIndex: []int{1},
		},
		{
			name:                     "1 mgmt 1 group",
			nodeSpec:                 []string{"management:ng0", "memory:ng0", "broadcastreduce::0-6"},
			expectedCoordinatorIndex: []int{0},
		},
		{
			name:     "1 mgmt 1 group reverse order",
			nodeSpec: []string{"memory", "management:ng0"}, expectedCoordinatorIndex: []int{1},
		},
		{
			name:     "1 mgmt",
			nodeSpec: []string{"management:ng0"}, expectedCoordinatorIndex: []int{0},
		},
		{
			name:     "1 any",
			nodeSpec: []string{"any:ng0"}, expectedCoordinatorIndex: []int{0},
		},
		// expect two CRD nodes from one mgmt node + one worker
		{
			name:                     "multi-crd mode one mgmt one conversion",
			nodeSpec:                 []string{"worker:ng0", "management:ng0", "worker:ng1", "worker:ng1", "memory:ng1"},
			enableMultiCoordinator:   true,
			expectedCoordinatorIndex: []int{1, 2},
		},
		// expect two CRD nodes from two worker nodes
		{
			name:                     "multi-crd mode two conversion",
			nodeSpec:                 []string{"worker:ng0", "worker:ng0", "worker:ng1", "worker:ng1", "memory:ng1"},
			enableMultiCoordinator:   true,
			expectedCoordinatorIndex: []int{0, 2},
		},
		{
			name:                     "multi-crd mode one conversion",
			nodeSpec:                 []string{"memory:ng0", "worker:ng0", "worker:ng1", "worker:ng1", "memory:ng1"},
			enableMultiCoordinator:   true,
			expectedCoordinatorIndex: []int{2},
		},
	} {
		t.Run("node_label_e2e_test"+testcase.name, func(t *testing.T) {
			wscommon.EnableMultiCoordinator = testcase.enableMultiCoordinator
			defer func() {
				wscommon.EnableMultiCoordinator = false
			}()
			testCtx, testCancel := context.WithCancel(ctx)
			defer testCancel()
			go func() { require.NoError(t, labeler.Start(testCtx)) }()

			schema := newClusterSchema(2)
			ng := map[string]bool{}
			expectNodegroupLabel := map[string]string{}
			for nodeIndex, spec := range testcase.nodeSpec {
				n, group := MakeNodeFromSpec(spec, nodeIndex)
				expectNodegroupLabel[n.Name] = group
				schema.Nodes = append(schema.Nodes, n)
				if !ng[group] {
					ng[group] = true
					schema.Groups = append(schema.Groups, &wscommon.NodeGroup{Name: n.GetGroup()})
				}
			}
			require.NoError(t, cfgMgr.Initialize(schema))

			// give the labeler a few seconds to get the notification and label the nodes
			require.Eventually(t, func() bool {
				return labeler.lastAppliedRV == schema.ResourceVersion
			}, 2*time.Second, 5*time.Millisecond)

			n := &v1.Node{}
			// Could do much more thorough checking of conditions, but this should be done in node_patch_test
			// Just check: nodes that were to be updated got updated and other nodes have properties removed
			// Note: use API reader for methods where sequence-ordering of events matters (reader=nocache)
			crdIndex := len(testcase.expectedCoordinatorIndex) - 1
			for nodeIndex := nodeCount - 1; nodeIndex >= 0; nodeIndex-- {
				name := fmt.Sprintf("n%d", nodeIndex)
				require.NoError(t, mgr.GetAPIReader().Get(ctx, client.ObjectKey{Name: name}, n))
				if nodeIndex < len(schema.Nodes) {
					// check if the current index is expected to be CRD
					if crdIndex >= 0 && testcase.expectedCoordinatorIndex[crdIndex] == nodeIndex {
						// has to be worker/crd/mgmt/any
						assert.Contains(t, []wscommon.NodeRole{wscommon.RoleManagement, wscommon.RoleCoordinator, wscommon.RoleWorker, wscommon.RoleAny},
							cfgMgr.Cfg.NodeMap[name].Role)
						assert.Contains(t, n.Labels, fmt.Sprintf("k8s.cerebras.com/node-role-coordinator"))
						crdIndex--
						if cfgMgr.Cfg.NodeMap[name].Role == wscommon.RoleManagement {
							assert.Contains(t, n.Labels, fmt.Sprintf("k8s.cerebras.com/node-role-management"))
						}
					} else {
						// if not CRD, then should match current role
						assert.Contains(t, n.Labels, fmt.Sprintf("k8s.cerebras.com/node-role-%s",
							cfgMgr.Cfg.NodeMap[name].Role))
					}
				} else {
					for k := range n.Labels {
						if strings.HasPrefix(k, "k8s.cerebras.com/node-role-") {
							assert.Fail(t, "node should be unlabeled since it's not in config")
						}
					}
				}

				expectGroup := expectNodegroupLabel[name]
				if expectGroup != "" {
					require.Equal(t, expectGroup, n.Labels[wscommon.GroupLabelKey])
				}
				delete(expectNodegroupLabel, name)
			}
			require.Len(t, expectNodegroupLabel, 0)
		})
	}
}

func makeNode(i int) *v1.Node {
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   fmt.Sprintf("n%d", i),
		Labels: map[string]string{"index": strconv.Itoa(i)}},
		Status: v1.NodeStatus{
			Capacity: map[v1.ResourceName]resource.Quantity{
				v1.ResourceMemory: resource.MustParse("4Gi"),
				v1.ResourceCPU:    resource.MustParse("4"),
			},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceMemory: resource.MustParse("4Gi"),
				v1.ResourceCPU:    resource.MustParse("4"),
			}},
	}
}

func newClusterSchema(systemCount int) wscommon.ClusterSchema {
	var systems []*systemv1.SystemSpec
	for w := 0; w < systemCount; w++ {
		systems = append(systems, &systemv1.SystemSpec{Name: fmt.Sprintf("system-%d", w), Type: "cs2"})
	}
	return wscommon.ClusterSchema{Systems: systems, ResourceVersion: uuid.NewString()}
}

// spec = ROLE((:GROUP)(:(CSPORT-CSPORTCOUNT)*)?)
func MakeNodeFromSpec(spec string, nodeIndex int) (*wscommon.Node, string) {
	specParts := strings.Split(spec, ":")
	role := wscommon.NodeRole(specParts[0])
	group := ""
	node := &wscommon.Node{
		Name:       "n" + strconv.Itoa(nodeIndex),
		Role:       role,
		Properties: map[string]string{},
	}
	if len(specParts) > 1 && specParts[1] != "" {
		group = specParts[1]
		node.Properties[wsresource.GroupPropertyKey] = group
	}
	if len(specParts) > 2 {
		nicSpecs := strings.Split(specParts[2], ":")
		for _, nicSpec := range nicSpecs {
			portCount := strings.Split(nicSpec, "-")
			port, _ := strconv.Atoi(portCount[0])
			count, _ := strconv.Atoi(portCount[1])
			for nicAtPort := 0; nicAtPort < count; nicAtPort++ {
				name := fmt.Sprintf("10.%d.%d.%d", nodeIndex, port, nicAtPort)
				node.NICs = append(node.NICs, &wscommon.NetworkInterface{
					Name: name, Addr: name, CsPort: &port,
				})
			}
		}
	}
	return node, group
}

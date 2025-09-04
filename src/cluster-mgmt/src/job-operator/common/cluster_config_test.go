//go:build default

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
	"log"
	"sort"
	"testing"

	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netclientfake "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/typed/k8s.cni.cncf.io/v1/fake"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientTesting "k8s.io/client-go/testing"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common/resource"
)

const cfg0 = `
kind: cluster-compute-configuration
properties:
  topology: v2
systems:
  - name: systemf0
    type: cs2
    managementAddress: 172.28.0.0
    controlAddress: "10.0.0.0:9000"
  - name: systemf1
    type: cs2
    managementAddress: 172.28.0.1
    controlAddress: "10.0.0.1:9000"
  - name: systemf2
    type: cs2
    managementAddress: 172.28.0.2
    controlAddress: "10.0.0.2:9000"
  - name: systemf3
    type: cs2
    managementAddress: 172.28.0.3
    controlAddress: "10.0.0.3:9000"
nodes:
  - name: x-br0
    role: broadcastreduce
    networkInterfaces:
      - name: enp197s0f0
        csPort: 2
        address: 10.1.0.0
      - name: enp197s0f1
        csPort: 3
        address: 10.1.0.1
      - name: enp129s0f0
        csPort: 2
        address: 10.1.0.2
      - name: enp129s0f1
        csPort: 3
        address: 10.1.0.3
      - name: enp1s0f0
        address: 10.1.0.4
        csPort: 2
      - name: enp1s0f1
        address: 10.1.0.5
        csPort: 3
        gbps: 400
      - name: eth6100g
        address: 10.1.0.5
        csPort: 3
        gbps: 6100
    properties:
      group: leaf-sw0
  - name: g0-wk0
    role: worker
    properties:
      group: nodegroup0
  - name: g1-any0
    role: any
    properties:
      group: nodegroup1
      generateBrNICs: "true"
  - name: g1-wk0
    role: worker
    properties:
      group: nodegroup1
  - name: g1-mgmt0
    role: management
    properties:
      group: nodegroup1
  - name: x-ax0
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.1
        v2Group: leaf-sw0
      - name: nic1
        address: 10.2.0.2
        v2Group: leaf-sw1
  - name: x-ax1
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.3
        v2Group: leaf-sw1
      - name: nic1
        address: 10.2.0.4
        v2Group: leaf-sw2
  - name: x-ax2
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.5
        v2Group: leaf-sw0
      - name: nic1
        address: 10.2.0.6
        v2Group: leaf-sw2
  - name: x-ax3
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.7
        v2Group: leaf-sw3
      - name: nic1
        address: 10.2.0.8
        v2Group: leaf-sw4
  # test both nics connected to the same switch
  - name: x-ax4
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.9
        v2Group: leaf-sw3
      - name: nic1
        address: 10.2.0.10
        v2Group: leaf-sw3
  # Add inferencedriver node with two nics
  - name: x-ix0
    role: inferencedriver
    networkInterfaces:
      - name: nic0
        address: 10.3.0.1
        v2Group: leaf-sw0
      - name: nic1
        address: 10.3.0.2
        v2Group: leaf-sw1
  - name: x-ix1
    role: inferencedriver
    networkInterfaces:
      - name: nic0
        address: 10.3.0.3
        v2Group: leaf-sw1
groups:
  - name: nodegroup0
    properties:
      systemAffinity: systemf1
  - name: nodegroup1
    properties:
      systemAffinity: systemf2  # not exists
v2Groups:
  - name: leaf-sw0
    properties:
      systemf0-0: ""
      systemf1-0: ""
      systemf2-0: ""
  - name: leaf-sw1
    properties:
      systemf0-1: ""
      systemf1-1: ""
      systemf2-1: ""
  - name: leaf-sw2
    properties:
      systemf0-2: ""
      systemf1-2: ""
      systemf2-2: ""
  - name: leaf-sw3
    properties:
      systemf3-7: ""
  - name: leaf-sw4
    properties:
      systemf3-8: ""
`

const cfg0_without_nodegroups = `
kind: cluster-compute-configuration
properties:
  topology: v2
systems:
  - name: systemf0
    type: cs2
    managementAddress: 172.28.0.0
    controlAddress: "10.0.0.0:9000"
  - name: systemf1
    type: cs2
    managementAddress: 172.28.0.1
    controlAddress: "10.0.0.1:9000"
  - name: systemf2
    type: cs2
    managementAddress: 172.28.0.2
    controlAddress: "10.0.0.2:9000"
  - name: systemf3
    type: cs2
    managementAddress: 172.28.0.3
    controlAddress: "10.0.0.3:9000"
nodes:
  - name: x-br0
    role: broadcastreduce
    networkInterfaces:
      - name: enp197s0f0
        csPort: 2
        address: 10.1.0.0
      - name: enp197s0f1
        csPort: 3
        address: 10.1.0.1
      - name: enp129s0f0
        csPort: 2
        address: 10.1.0.2
      - name: enp129s0f1
        csPort: 3
        address: 10.1.0.3
      - name: enp1s0f0
        address: 10.1.0.4
        csPort: 2
      - name: enp1s0f1
        address: 10.1.0.5
        csPort: 3
        gbps: 400
      - name: eth6100g
        address: 10.1.0.5
        csPort: 3
        gbps: 6100
    properties:
      group: leaf-sw0
  - name: g1-mgmt0
    role: management
  - name: x-ax0
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.1
        v2Group: leaf-sw0
      - name: nic1
        address: 10.2.0.2
        v2Group: leaf-sw1
  - name: x-ax1
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.3
        v2Group: leaf-sw1
      - name: nic1
        address: 10.2.0.4
        v2Group: leaf-sw2
  - name: x-ax2
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.5
        v2Group: leaf-sw0
      - name: nic1
        address: 10.2.0.6
        v2Group: leaf-sw2
  - name: x-ax3
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.7
        v2Group: leaf-sw3
      - name: nic1
        address: 10.2.0.8
        v2Group: leaf-sw4
  # test both nics connected to the same switch
  - name: x-ax4
    role: activation
    networkInterfaces:
      - name: nic0
        address: 10.2.0.9
        v2Group: leaf-sw3
      - name: nic1
        address: 10.2.0.10
        v2Group: leaf-sw3
  # Add inferencedriver node with two nics
  - name: x-ix0
    role: inferencedriver
    networkInterfaces:
      - name: nic0
        address: 10.3.0.1
        v2Group: leaf-sw0
      - name: nic1
        address: 10.3.0.2
        v2Group: leaf-sw1
  - name: x-ix1
    role: inferencedriver
    networkInterfaces:
      - name: nic0
        address: 10.3.0.3
        v2Group: leaf-sw1
v2Groups:
  - name: leaf-sw0
    properties:
      systemf0-0: ""
      systemf1-0: ""
      systemf2-0: ""
  - name: leaf-sw1
    properties:
      systemf0-1: ""
      systemf1-1: ""
      systemf2-1: ""
  - name: leaf-sw2
    properties:
      systemf0-2: ""
      systemf1-2: ""
      systemf2-2: ""
  - name: leaf-sw3
    properties:
      systemf3-7: ""
  - name: leaf-sw4
    properties:
      systemf3-8: ""
`

type mockNsChange struct {
	unassigned []string
	assigned   map[string]string
}

func newMockNsChange() *mockNsChange {
	return &mockNsChange{assigned: map[string]string{}}
}

func (m *mockNsChange) assign(id, ns string) *mockNsChange {
	m.unassign(id)
	m.assigned[id] = ns
	return m
}
func (m *mockNsChange) unassign(id string) *mockNsChange {
	m.unassigned = append(m.unassigned, id)
	return m
}
func (m *mockNsChange) Unassigned() []string        { return m.unassigned }
func (m *mockNsChange) Assigned() map[string]string { return m.assigned }
func (m *mockNsChange) IsEmpty() bool {
	return len(m.assigned) == 0 && len(m.unassigned) == 0
}
func (m *mockNsChange) GetProps() (string, map[string]string) {
	return "", nil
}

func parseTestConfig() *ClusterConfig {
	return parseTestConfigString(cfg0)
}

func parseTestConfigString(config string) *ClusterConfig {
	cm := &corev1.ConfigMap{
		Data: map[string]string{
			ClusterConfigFile: config,
		},
	}
	c, err := NewClusterSchemaFromCM(cm)
	if err != nil {
		log.Fatalf("failed to parse cluster yaml")
	}
	cfg := NewClusterConfig(c)
	cfg.BuildNodeSysGroupIndices()
	cfg.generateBrNICs()
	return cfg
}

func TestParseConfigYAML(t *testing.T) {
	cfg := parseTestConfig()
	cfg.BuildNodeSysGroupIndices()
	cfg.CheckV2Network()
	assert.Equal(t, 4, len(cfg.Systems))
	assert.Equal(t, 12, len(cfg.Nodes))
	assert.Equal(t, 5, len(cfg.V2Groups))

	assert.Equal(t, cfg.V2GroupMap["leaf-sw0"].Properties[resource.StampPropertyKey], "0")
	assert.Equal(t, cfg.V2GroupMap["leaf-sw1"].Properties[resource.StampPropertyKey], "0")
	assert.Equal(t, cfg.V2GroupMap["leaf-sw2"].Properties[resource.StampPropertyKey], "0")
	assert.Equal(t, cfg.V2GroupMap["leaf-sw3"].Properties[resource.StampPropertyKey], "1")
	assert.Equal(t, cfg.V2GroupMap["leaf-sw4"].Properties[resource.StampPropertyKey], "1")

	assert.Contains(t, cfg.SystemMap, "systemf0")
	assert.Contains(t, cfg.SystemMap, "systemf1")
	assert.Contains(t, cfg.SystemMap, "systemf2")
	assert.Contains(t, cfg.SystemMap, "systemf3")

	for _, name := range []string{"g0-wk0", "g1-any0", "g1-wk0", "x-br0", "x-ax0", "x-ax1", "x-ax2", "x-ax3", "x-ax4", "g1-mgmt0", "x-ix0", "x-ix1"} {
		assert.Contains(t, cfg.NodeMap, name)
	}

	brNode := cfg.NodeMap["x-br0"]
	require.Equal(t, RoleBroadcastReduce, brNode.Role)
	assert.Equal(t, 7, len(brNode.NICs))
	assert.Equal(t, 2, *brNode.NICs[0].CsPort)
	assert.Equal(t, 2, *brNode.NICs[1].CsPort)
	assert.Equal(t, 2, *brNode.NICs[2].CsPort)
	assert.Equal(t, 3, *brNode.NICs[3].CsPort)
	assert.Equal(t, 3, *brNode.NICs[4].CsPort)
	assert.Equal(t, 3, *brNode.NICs[5].CsPort)
	assert.Equal(t, 400, *brNode.NICs[5].BwGbps)
	// (70 - 60) / 4 = 2.5
	// 70 is total bw, 60 = 12brs * 5ports(1:4mode), 4 systems
	assert.Equal(t, 2.5, wsapisv1.RedundantBrPortBwPerSystem)

	axNodeInStamp0 := cfg.NodeMap["x-ax0"]
	require.Equal(t, RoleActivation, axNodeInStamp0.Role)
	assert.Equal(t, 2, len(axNodeInStamp0.NICs))
	assert.Equal(t, "leaf-sw0", axNodeInStamp0.NICs[0].V2Group)
	assert.Equal(t, "leaf-sw1", axNodeInStamp0.NICs[1].V2Group)

	axNodeInStamp1 := cfg.NodeMap["x-ax4"]
	require.Equal(t, RoleActivation, axNodeInStamp1.Role)
	assert.Equal(t, 2, len(axNodeInStamp1.NICs))
	assert.Equal(t, "leaf-sw3", axNodeInStamp1.NICs[0].V2Group)
	assert.Equal(t, "leaf-sw3", axNodeInStamp1.NICs[1].V2Group)

	anyNode := cfg.Nodes[1]
	assert.Equal(t, 6*12, len(anyNode.NICs))

	ixNode0 := cfg.NodeMap["x-ix0"]
	require.Equal(t, RoleInferenceDriver, ixNode0.Role)
	assert.Equal(t, 2, len(ixNode0.NICs))
	assert.Equal(t, "leaf-sw0", ixNode0.NICs[0].V2Group)
	assert.Equal(t, "leaf-sw1", ixNode0.NICs[1].V2Group)

	// Test single NIC inferencedriver node
	ixNode1 := cfg.NodeMap["x-ix1"]
	require.Equal(t, RoleInferenceDriver, ixNode1.Role)
	assert.Equal(t, 1, len(ixNode1.NICs))
	assert.Equal(t, "leaf-sw1", ixNode1.NICs[0].V2Group)

	cfg.AdjustNodesByClusterSetup()
	cfg.removeLegacyMgmtGroup()
	mgmtNode := cfg.NodeMap["g1-mgmt0"]
	assert.Equal(t, "", mgmtNode.GetGroup())
	anyNode = cfg.NodeMap["g1-any0"]
	assert.Equal(t, "nodegroup1", anyNode.GetGroup())

	// Test role expansion works correctly for inferencedriver
	expandedRoles := ixNode0.ExpandedRoles
	require.Contains(t, expandedRoles, RoleInferenceDriver)
	assert.Equal(t, 1, len(expandedRoles), "InferenceDriver role should expand only to itself")

	// Test network/stamp assignment for inferencedriver
	ixNode0Prop := ixNode0.GetProperties()
	assert.Contains(t, ixNode0Prop, resource.StampPropertyKey)
	assert.Equal(t, "0", ixNode0Prop[resource.StampPropertyKey])

	m := &ClusterConfigMgr{Cfg: cfg, log: logrus.WithField("component", "ClusterConfigMgr")}
	assert.True(t, wsapisv1.IsV2Network)
	assert.True(t, wsapisv1.UseAxScheduling)
	assert.Equal(t, 1, wsapisv1.AxCountPerSystem)
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
	}()
	m.reloadResourceMgmt(cfg)
	res, _ := cfg.Resources.Get("node/x-br0")
	assert.Equal(t, 70*resource.DefaultNicBW, res.GetSubresourceCountCapacity("cs-port"))
	assert.Equal(t, 0, res.GetSubresourceCountCapacity("cs-port-2"))
	_, ok := res.GetProp(fmt.Sprintf("%s/systemf0-0", resource.PortAffinityPropKeyPrefix))
	assert.True(t, ok)
	_, ok = res.GetProp(fmt.Sprintf("%s/systemf1-0", resource.PortAffinityPropKeyPrefix))
	assert.True(t, ok)

	systems := m.Cfg.Resources.Filter(resource.NewSystemFilter()).List()
	sort.Slice(systems, func(i, j int) bool {
		return systems[i].Name() < systems[j].Name()
	})
	// expect systems are in two groups, group0: f0,f1; group1: f2
	assert.Contains(t, systems[0].Name(), "systemf0")
	assert.Contains(t, systems[1].Name(), "systemf1")
	assert.Contains(t, systems[2].Name(), "systemf2")
	assert.Contains(t, systems[3].Name(), "systemf3")

	// systems from the same stamp should have the same group property value
	assert.Equal(t, "0", systems[0].GetStampIndex())
	assert.Equal(t, "0", systems[1].GetStampIndex())
	assert.Equal(t, "0", systems[2].GetStampIndex())
	assert.Equal(t, systems[0].GetStampIndex(), systems[1].GetStampIndex())
	assert.Equal(t, systems[1].GetStampIndex(), systems[2].GetStampIndex())

	assert.Equal(t, "1", systems[3].GetStampIndex())
	assert.NotEqual(t, systems[0].GetStampIndex(), systems[3].GetStampIndex())

	// affinity property propagation on nodes
	axNodeInStamp0 = cfg.NodeMap["x-ax0"]
	axNodeInStamp1 = cfg.NodeMap["x-ax4"]
	nic0SrName := fmt.Sprintf("%s/nic0", resource.NodeNicSubresource)
	nic1SrName := fmt.Sprintf("%s/nic1", resource.NodeNicSubresource)
	assert.Equal(t, map[string]string{
		resource.StampPropertyKey:                          "0",
		resource.PortAffinityPropKeyPrefix + "/systemf0-0": nic0SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf1-0": nic0SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf2-0": nic0SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf0-1": nic1SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf1-1": nic1SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf2-1": nic1SrName,
	}, axNodeInStamp0.GetProperties())
	assert.Equal(t, map[string]string{
		resource.StampPropertyKey: "1",
		// both nics have the same affinities
		resource.PortAffinityPropKeyPrefix + "/systemf3-7": "",
	}, axNodeInStamp1.GetProperties())

	// affinity property propagation on node nics
	assert.Equal(t, map[string]string{
		resource.StampPropertyKey:                          "0",
		resource.PortAffinityPropKeyPrefix + "/systemf0-0": nic0SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf1-0": nic0SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf2-0": nic0SrName,
	}, axNodeInStamp0.NICs[0].Properties)
	assert.Equal(t, map[string]string{
		resource.StampPropertyKey:                          "0",
		resource.PortAffinityPropKeyPrefix + "/systemf0-1": nic1SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf1-1": nic1SrName,
		resource.PortAffinityPropKeyPrefix + "/systemf2-1": nic1SrName,
	}, axNodeInStamp0.NICs[1].Properties)
	assert.Equal(t, map[string]string{
		resource.StampPropertyKey:                          "1",
		resource.PortAffinityPropKeyPrefix + "/systemf3-7": nic0SrName,
	}, axNodeInStamp1.NICs[0].Properties)
	assert.Equal(t, map[string]string{
		resource.StampPropertyKey:                          "1",
		resource.PortAffinityPropKeyPrefix + "/systemf3-7": nic1SrName,
	}, axNodeInStamp1.NICs[1].Properties)

	//t.Log(systems)
}

func TestClusterConfig_PrettyHealthInfo(t *testing.T) {
	cfg := parseTestConfig()
	require.Equal(t, "Cluster healthy.", cfg.PrettyHealthInfo("", false))

	cfg.UnhealthySystems = map[string]*systemv1.SystemSpec{
		cfg.Systems[0].Name: cfg.Systems[0],
	}
	cfg.Systems[0].InitPorts()
	cfg.Systems[0].Ports[0].HasError = true
	cfg.Systems[0].Ports[1].HasError = true
	require.Equal(t, "1 system unhealthy: systemf0{errorPorts=n00,n01}", cfg.PrettyHealthInfo("", false))

	cfg.UnhealthySystems[cfg.Systems[1].Name] = cfg.Systems[1]
	require.Equal(t, "2 systems unhealthy: systemf0{errorPorts=n00,n01}, systemf1", cfg.PrettyHealthInfo("", false))

	cfg.UnhealthyNodes = map[string]*Node{
		cfg.NodeMap["x-br0"].Name: cfg.NodeMap["x-br0"],
	}
	cfg.NodeMap["x-br0"].NICs[0].HasError = true
	cfg.NodeMap["x-br0"].NICs[1].HasError = true
	require.Equal(t, "2 systems, 1 node unhealthy: systemf0{errorPorts=n00,n01}, systemf1, x-br0{errorNics=enp197s0f0,enp129s0f0}", cfg.PrettyHealthInfo("", false))

	cfg.UnhealthyNodes[cfg.Nodes[0].Name] = cfg.Nodes[0]
	require.Equal(t, "2 systems, 2 nodes unhealthy: systemf0{errorPorts=n00,n01}, systemf1, g0-wk0, x-br0{errorNics=enp197s0f0,enp129s0f0}", cfg.PrettyHealthInfo("", false))

	// Test namespaced versions of showing unhealthy systems and nodes
	nsrList := namespacev1.NamespaceReservationList{
		Items: []namespacev1.NamespaceReservation{
			{ObjectMeta: metav1.ObjectMeta{Name: "NS-A"},
				Status: namespacev1.NamespaceReservationStatus{
					Systems: []string{"systemf0"},
					Nodes:   []string{"x-br0", "g0-wk0"},
				},
			},
			{ObjectMeta: metav1.ObjectMeta{Name: "NS-B"},
				Status: namespacev1.NamespaceReservationStatus{
					Systems: []string{"systemf1"},
				},
			},
		},
	}
	cfg.InitializeNsAssignment(nsrList)

	// Verify BR node IS present when it had resource errors, both for the global view and EACH per-namespace view
	require.Equal(t, "2 systems, 2 nodes unhealthy: systemf0{errorPorts=n00,n01}, systemf1, g0-wk0, x-br0{errorNics=enp197s0f0,enp129s0f0}", cfg.PrettyHealthInfo("", true))
	require.Equal(t, "1 system, 1 node unhealthy: systemf1, x-br0{errorNics=enp197s0f0,enp129s0f0}", cfg.PrettyHealthInfo("NS-B", true))
	require.Equal(t, "1 system, 2 nodes unhealthy: systemf0{errorPorts=n00,n01}, g0-wk0, x-br0{errorNics=enp197s0f0,enp129s0f0}", cfg.PrettyHealthInfo("NS-A", true))

	// When the BR node had errors but no RESOURCE errors, it is present in the global view, and in the view for its namespace, but not in the view for any other namespace
	require.Equal(t, "2 systems, 2 nodes unhealthy: systemf0{errorPorts=n00,n01}, systemf1, g0-wk0, x-br0{errorNics=enp197s0f0,enp129s0f0}", cfg.PrettyHealthInfo("", false))
	require.Equal(t, "1 system unhealthy: systemf1", cfg.PrettyHealthInfo("NS-B", false))
	require.Equal(t, "1 system, 2 nodes unhealthy: systemf0{errorPorts=n00,n01}, g0-wk0, x-br0{errorNics=enp197s0f0,enp129s0f0}", cfg.PrettyHealthInfo("NS-A", false))

	// Verify BR node is NOT present in any view, when it didn't have ANY errors
	cfg.NodeMap["x-br0"].NICs[0].HasError = false
	cfg.NodeMap["x-br0"].NICs[1].HasError = false
	delete(cfg.UnhealthyNodes, cfg.NodeMap["x-br0"].Name)
	require.Equal(t, "2 systems, 1 node unhealthy: systemf0{errorPorts=n00,n01}, systemf1, g0-wk0", cfg.PrettyHealthInfo("", false))
	require.Equal(t, "1 system, 1 node unhealthy: systemf0{errorPorts=n00,n01}, g0-wk0", cfg.PrettyHealthInfo("NS-A", false))
	require.Equal(t, "1 system unhealthy: systemf1", cfg.PrettyHealthInfo("NS-B", false))
}

func TestClusterConfig_UpdateNS(t *testing.T) {
	cfg := parseTestConfig()
	nsrList := namespacev1.NamespaceReservationList{
		Items: []namespacev1.NamespaceReservation{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "n0"},
				Status: namespacev1.NamespaceReservationStatus{
					Systems:    []string{"systemf0"},
					Nodegroups: []string{"nodegroup1"},
					Nodes:      []string{"g1-any0"},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "n1"},
				Status: namespacev1.NamespaceReservationStatus{
					Systems:    []string{"systemf1"},
					Nodegroups: []string{"nodegroup0"},
				},
			},
		},
	}
	m := &ClusterConfigMgr{Cfg: cfg, log: logrus.WithField("component", "ClusterConfigMgr")}
	m.Cfg.InitializeNsAssignment(nsrList)
	t.Log(m.Cfg.nsAssignedSystems)
	t.Log(m.Cfg.nsAssignedGroups)
	t.Log(m.Cfg.nsAssignedNodes)
	require.Equal(t, 1, len(m.Cfg.nsAssignedGroups["n0"]))
	require.Equal(t, 1, len(m.Cfg.nsAssignedGroups["n1"]))
	require.Equal(t, "nodegroup1", m.Cfg.nsAssignedGroups["n0"]["nodegroup1"].Name)
	require.Equal(t, "nodegroup0", m.Cfg.nsAssignedGroups["n1"]["nodegroup0"].Name)
	require.Equal(t, 1, len(m.Cfg.nsAssignedSystems["n0"]))
	require.Equal(t, 1, len(m.Cfg.nsAssignedSystems["n1"]))
	require.Equal(t, "systemf0", m.Cfg.nsAssignedSystems["n0"]["systemf0"].Name)
	require.Equal(t, "systemf1", m.Cfg.nsAssignedSystems["n1"]["systemf1"].Name)
	require.Equal(t, 1, len(m.Cfg.nsAssignedNodes["n0"]))
	require.Equal(t, "g1-any0", m.Cfg.nsAssignedNodes["n0"]["g1-any0"].Name)
	require.Equal(t, "n0", m.Cfg.nsAssignedNodes["n0"]["g1-any0"].Properties[NamespaceKey])

	// move systemf1 from n1 to n0, and nodegroup0 from n1 to n0
	m.updateNsAssignment(newMockNsChange().assign("system/systemf1", "n0").assign("nodegroup/nodegroup0", "n0"))
	require.Equal(t, 2, len(m.Cfg.nsAssignedGroups["n0"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedGroups["n1"]))
	require.Equal(t, 2, len(m.Cfg.nsAssignedSystems["n0"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedSystems["n1"]))
	require.Equal(t, 1, len(m.Cfg.nsAssignedNodes["n0"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedNodes["n1"]))
	require.Equal(t, "n0", m.Cfg.GroupMap["nodegroup0"].Properties["namespace"])
	require.Equal(t, "n0", m.Cfg.GroupMap["nodegroup1"].Properties["namespace"])
	require.Equal(t, "g1-any0", m.Cfg.nsAssignedNodes["n0"]["g1-any0"].Name)
	require.Equal(t, "n0", m.Cfg.nsAssignedNodes["n0"]["g1-any0"].Properties[NamespaceKey])

	// remove systemf1 from n0, move node from n0 to n1, unassign nodegroup0
	m.updateNsAssignment(newMockNsChange().unassign("system/systemf1").unassign("nodegroup/nodegroup0").assign("node/g1-any0", "n1"))
	require.Equal(t, 1, len(m.Cfg.nsAssignedGroups["n0"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedGroups["n1"]))
	require.Equal(t, 1, len(m.Cfg.nsAssignedSystems["n0"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedSystems["n1"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedNodes["n0"]))
	require.Equal(t, 1, len(m.Cfg.nsAssignedNodes["n1"]))
	require.Equal(t, "", m.Cfg.GroupMap["nodegroup0"].Properties["namespace"])
	require.Equal(t, "n0", m.Cfg.GroupMap["nodegroup1"].Properties["namespace"])
	require.Equal(t, "n1", m.Cfg.NodeMap["g1-any0"].Properties["namespace"])

	// unassign all remaining
	m.updateNsAssignment(newMockNsChange().unassign("system/systemf0").unassign("nodegroup/nodegroup1").unassign("node/g1-any0"))
	require.Equal(t, 0, len(m.Cfg.nsAssignedGroups["n0"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedGroups["n1"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedSystems["n0"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedSystems["n1"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedNodes["n0"]))
	require.Equal(t, 0, len(m.Cfg.nsAssignedNodes["n1"]))
	require.Equal(t, "", m.Cfg.GroupMap["nodegroup0"].Properties["namespace"])
	require.Equal(t, "", m.Cfg.GroupMap["nodegroup1"].Properties["namespace"])
	require.Equal(t, "", m.Cfg.NodeMap["g1-any0"].Properties["namespace"])
}

func TestClusterConfig_IsNodeInNamespace(t *testing.T) {
	cfg := parseTestConfig()
	nsrList := namespacev1.NamespaceReservationList{
		Items: []namespacev1.NamespaceReservation{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "n0"},
				Status: namespacev1.NamespaceReservationStatus{
					Systems:    []string{"systemf0"},
					Nodegroups: []string{"nodegroup1"},
					Nodes:      []string{"g1-any0"},
				},
			},
		},
	}
	m := &ClusterConfigMgr{Cfg: cfg, log: logrus.WithField("component", "ClusterConfigMgr")}
	m.Cfg.InitializeNsAssignment(nsrList)
	m.Cfg.AdjustNodesByClusterSetup()
	m.Cfg.removeLegacyMgmtGroup()

	require.True(t, m.Cfg.IsNodeInNamespace("n0", "g1-any0"))
	require.False(t, m.Cfg.IsNodeInNamespace("n1", "g1-any0"))
	require.False(t, m.Cfg.IsNodeInNamespace("n0", "g0-wk0"))
	require.True(t, m.Cfg.IsNodeInNamespace("n0", "g1-wk0"))
	require.Equal(t, "", m.Cfg.GetNodeNamespace("g1-mgmt0"))
	require.False(t, m.Cfg.IsNodeInNamespace("n1", "not_found"))
	require.Equal(t, "", m.Cfg.GetNodeNamespace("not_found"))
}

func TestCheckNadDeploy(t *testing.T) {
	nc := netclientfake.FakeK8sCniCncfIoV1{
		Fake: &clientTesting.Fake{},
	}
	nc.Fake.AddReactor(
		"get",
		"network-attachment-definitions",
		func(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
			if action.(clientTesting.GetActionImpl).Name == "false" {
				return true, nil,
					k8serrors.NewNotFound(netv1.Resource("network-attachment-definitions"), "false")
			}
			return true, &netv1.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "true"},
			}, nil
		},
	)

	m := &ClusterConfigMgr{
		log: logrus.WithField("test", "test"),
	}
	ncClient := nc.NetworkAttachmentDefinitions("test")
	name, ready, err := m.checkNadDeployed(ncClient, "false", "true")
	require.Nil(t, err)
	require.True(t, ready)
	require.Equal(t, "true", name)

	name, ready, err = m.checkNadDeployed(ncClient, "true", "false")
	require.Nil(t, err)
	require.True(t, ready)
	require.Equal(t, "true", name)

	name, ready, err = m.checkNadDeployed(ncClient, "false", "false")
	require.Nil(t, err)
	require.False(t, ready)
	require.Equal(t, "", name)
}

func TestClusterConfigWithoutNodeGroup(t *testing.T) {
	cfg := parseTestConfigString(cfg0_without_nodegroups)
	m := &ClusterConfigMgr{Cfg: cfg, log: logrus.WithField("component", "ClusterConfigMgr")}

	err := m.checkClusterNetwork(cfg)
	require.NoError(t, err)
	assert.True(t, wsapisv1.IsV2Network)
	assert.True(t, wsapisv1.UseAxScheduling)
	assert.True(t, wsapisv1.IsInferenceCluster)
	assert.Equal(t, 1, wsapisv1.AxCountPerSystem)
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.IsInferenceCluster = false
	}()
}

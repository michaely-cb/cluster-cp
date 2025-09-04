package csctl

import (
	"context"
	"testing"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	"github.com/prometheus/common/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

// Constants for property keys used in tests
const (
	stampPropertyKey        = resource.StampPropertyKey
	preferredCsPortsPropKey = resource.PreferredCsPortsPropKey
	switchPropertyKey       = resource.SwitchPropertyKey
	testNsr                 = "test-nsr"
)

// creates a test server with the given config
func createMockServer(cfg *wscommon.ClusterConfig) *Server {
	var configProvider pkg.ClusterCfgProvider
	if cfg != nil {
		configProvider = pkg.StaticCfgProvider(*cfg)
	} else {
		configProvider = nil
	}

	return &Server{
		configProvider: configProvider,
		options:        wsclient.ServerOptions{},
	}
}

func createMockJobTopologyServer(querier *wscommon.MockQuerier) *Server {
	return &Server{
		metricsQuerier: querier,
	}
}

func createMockNsr(nsrName string, sysNames, nodeNames []string) namespacev1.NamespaceReservation {
	return namespacev1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: nsrName,
		},
		Status: namespacev1.NamespaceReservationStatus{
			Systems: sysNames,
			Nodes:   nodeNames,
		},
	}
}

func TestGetClusterTopology(t *testing.T) {
	tests := []struct {
		name           string
		switchOnly     bool
		mockCfg        *wscommon.ClusterConfig
		expectedErr    bool
		expectedCount  int
		targetNs       string
		validateResult func(t *testing.T, resp *csctlpb.GetClusterTopologyResponse)
	}{
		{
			name:          "nil config manager",
			mockCfg:       nil,
			expectedErr:   true,
			expectedCount: 0,
		},
		{
			name:          "switch only view",
			switchOnly:    true,
			mockCfg:       createV2MockConfig(),
			expectedErr:   false,
			expectedCount: 4,
			validateResult: func(t *testing.T, resp *csctlpb.GetClusterTopologyResponse) {
				// Should only have switch entries
				type ExpectedConn struct {
					stampId, peerName, peerPort, switchName string
					ports                                   []string
				}
				expectedConns := []ExpectedConn{
					{"0", "", "", "leaf-sw-0", []string{"0", "1"}},
					{"0", "", "", "leaf-sw-1", []string{"2", "3"}},
					{"1", "", "", "leaf-sw-2", []string{"6", "7"}},
					{"1", "", "", "leaf-sw-3", []string{"8", "9"}},
				} // should be in this exact order.

				assert.True(t, len(expectedConns) == len(resp.Connections), "Expected %v connections but got %v",
					len(expectedConns), len(resp.Connections))
				for idx, conn := range resp.Connections {
					exp := expectedConns[idx]
					flag := exp.stampId == conn.StampId && exp.peerName == conn.PeerName && exp.peerPort == conn.PeerPort && exp.switchName == conn.SwitchName
					assert.True(t, flag, "Expected connection %d to be %s/%s/%s/%s, but got %s/%s/%s/%s",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, conn.StampId, conn.PeerName, conn.PeerPort, conn.SwitchName)
					assert.Equal(t, exp.ports, conn.PreferredCsPorts, "Expected connection %d %s/%s/%s/%s to have ports %v, but got %v",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, exp.ports, conn.PreferredCsPorts)
				}
			},
		},
		{
			name:          "detailed view",
			switchOnly:    false,
			mockCfg:       createV2MockConfig(),
			expectedErr:   false,
			expectedCount: 13,
			validateResult: func(t *testing.T, resp *csctlpb.GetClusterTopologyResponse) {
				type ExpectedConn struct {
					stampId, peerName, peerPort, switchName string
					ports                                   []string
				}
				expectedConns := []ExpectedConn{
					{"0", "sys-0", "n00,n01", "leaf-sw-0", []string{"0", "1"}},
					{"0", "sys-1", "n00,n01", "leaf-sw-0", []string{"0", "1"}},
					{"0", "ax-0", "nic0", "leaf-sw-0", []string{"0", "1"}},
					{"0", "br-0", "nic0,nic1", "leaf-sw-0", []string{"0", "1"}},
					{"0", "sys-0", "n02,n03", "leaf-sw-1", []string{"2", "3"}},
					{"0", "sys-1", "n02,n03", "leaf-sw-1", []string{"2", "3"}},
					{"0", "ax-0", "nic1", "leaf-sw-1", []string{"2", "3"}},
					{"1", "sys-2", "n06,n07", "leaf-sw-2", []string{"6", "7"}},
					{"1", "ax-1", "nic0", "leaf-sw-2", []string{"6", "7"}},
					{"1", "br-1", "nic0,nic1", "leaf-sw-2", []string{"6", "7"}},
					{"1", "sys-2", "n08,n09", "leaf-sw-3", []string{"8", "9"}},
					{"1", "ax-1", "nic1", "leaf-sw-3", []string{"8", "9"}},
					{"-", "mg-0", "eth0", "lw-0", []string{"-"}},
				} // should be in this exact order

				assert.True(t, len(expectedConns) == len(resp.Connections), "Expected %v connections but got %v",
					len(expectedConns), len(resp.Connections))
				for idx, conn := range resp.Connections {
					exp := expectedConns[idx]
					flag := exp.stampId == conn.StampId && exp.peerName == conn.PeerName && exp.peerPort == conn.PeerPort && exp.switchName == conn.SwitchName
					assert.True(t, flag, "Expected connection %d to be %s/%s/%s/%s, but got %s/%s/%s/%s",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, conn.StampId, conn.PeerName, conn.PeerPort, conn.SwitchName)
					assert.Equal(t, exp.ports, conn.PreferredCsPorts, "Expected connection %d %s/%s/%s/%s to have ports %v, but got %v",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, exp.ports, conn.PreferredCsPorts)
				}
			},
		},
		{
			name:          "detailed view - user namespace",
			switchOnly:    false,
			mockCfg:       createV2MockConfig(),
			expectedErr:   false,
			expectedCount: 7,
			targetNs:      testNsr,
			validateResult: func(t *testing.T, resp *csctlpb.GetClusterTopologyResponse) {
				type ExpectedConn struct {
					stampId, peerName, peerPort, switchName string
					ports                                   []string
				}
				expectedConns := []ExpectedConn{
					{"0", "sys-0", "n00,n01", "leaf-sw-0", []string{"0", "1"}},
					{"0", "br-0", "nic0,nic1", "leaf-sw-0", []string{"0", "1"}},
					{"0", "sys-0", "n02,n03", "leaf-sw-1", []string{"2", "3"}},
					{"1", "ax-1", "nic0", "leaf-sw-2", []string{"6", "7"}},
					{"1", "br-1", "nic0,nic1", "leaf-sw-2", []string{"6", "7"}},
					{"1", "ax-1", "nic1", "leaf-sw-3", []string{"8", "9"}},
					{"-", "mg-0", "eth0", "lw-0", []string{"-"}},
				} // should be in this exact order

				assert.True(t, len(expectedConns) == len(resp.Connections), "Expected %v connections but got %v",
					len(expectedConns), len(resp.Connections))
				for idx, conn := range resp.Connections {
					exp := expectedConns[idx]
					flag := exp.stampId == conn.StampId && exp.peerName == conn.PeerName && exp.peerPort == conn.PeerPort && exp.switchName == conn.SwitchName
					assert.True(t, flag, "Expected connection %d to be %s/%s/%s/%s, but got %s/%s/%s/%s",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, conn.StampId, conn.PeerName, conn.PeerPort, conn.SwitchName)
					assert.Equal(t, exp.ports, conn.PreferredCsPorts, "Expected connection %d %s/%s/%s/%s to have ports %v, but got %v",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, exp.ports, conn.PreferredCsPorts)
				}
			},
		},
		{
			name:          "v1 network",
			switchOnly:    false,
			mockCfg:       createV1MockConfig(),
			expectedErr:   false,
			expectedCount: 7,
			validateResult: func(t *testing.T, resp *csctlpb.GetClusterTopologyResponse) {
				type ExpectedConn struct {
					stampId, peerName, peerPort, switchName string
					ports                                   []string
				}
				expectedConns := []ExpectedConn{
					{"0", "sys-v1-0", "n00", "sc-r7rb13-100gsw32-hp-1", []string{"0"}},
					{"0", "sys-v1-1", "n00", "sc-r7rb13-100gsw32-hp-1", []string{"0"}},
					{"0", "br-v1", "nic0,nic1", "sc-r7rb13-100gsw32-hp-1", []string{"0"}},
					{"0", "sys-v1-0", "n01", "sc-r7rb13-100gsw32-hp-2", []string{"1"}},
					{"0", "sys-v1-1", "n01", "sc-r7rb13-100gsw32-hp-2", []string{"1"}},
					{"0", "br-v2", "nic0,nic1", "sc-r7rb13-100gsw32-hp-2", []string{"1"}},
					{"0", "mg-v1", "nic0", "sc-r7rb14-100gsw64", []string{"-"}},
				} // should be in this exact order.

				assert.True(t, len(expectedConns) == len(resp.Connections), "Expected %v connections but got %v",
					len(expectedConns), len(resp.Connections))
				for idx, conn := range resp.Connections {
					exp := expectedConns[idx]
					flag := exp.stampId == conn.StampId && exp.peerName == conn.PeerName && exp.peerPort == conn.PeerPort && exp.switchName == conn.SwitchName
					assert.True(t, flag, "Expected connection %d to be %s/%s/%s/%s, but got %s/%s/%s/%s",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, conn.StampId, conn.PeerName, conn.PeerPort, conn.SwitchName)
					assert.Equal(t, exp.ports, conn.PreferredCsPorts, "Expected connection %d %s/%s/%s/%s to have ports %v, but got %v",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, exp.ports, conn.PreferredCsPorts)
				}
			},
		},
		{
			name:          "v1 network - target namespace",
			switchOnly:    false,
			mockCfg:       createV1MockConfig(),
			expectedErr:   false,
			expectedCount: 4,
			targetNs:      testNsr,
			validateResult: func(t *testing.T, resp *csctlpb.GetClusterTopologyResponse) {
				type ExpectedConn struct {
					stampId, peerName, peerPort, switchName string
					ports                                   []string
				}
				expectedConns := []ExpectedConn{
					{"0", "sys-v1-0", "n00", "sc-r7rb13-100gsw32-hp-1", []string{"0"}},
					{"0", "br-v1", "nic0,nic1", "sc-r7rb13-100gsw32-hp-1", []string{"0"}},
					{"0", "sys-v1-0", "n01", "sc-r7rb13-100gsw32-hp-2", []string{"1"}},
					{"0", "br-v2", "nic0,nic1", "sc-r7rb13-100gsw32-hp-2", []string{"1"}},
				} // should be in this exact order.

				assert.True(t, len(expectedConns) == len(resp.Connections), "Expected %v connections but got %v",
					len(expectedConns), len(resp.Connections))
				for idx, conn := range resp.Connections {
					exp := expectedConns[idx]
					flag := exp.stampId == conn.StampId && exp.peerName == conn.PeerName && exp.peerPort == conn.PeerPort && exp.switchName == conn.SwitchName
					assert.True(t, flag, "Expected connection %d to be %s/%s/%s/%s, but got %s/%s/%s/%s",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, conn.StampId, conn.PeerName, conn.PeerPort, conn.SwitchName)
					assert.Equal(t, exp.ports, conn.PreferredCsPorts, "Expected connection %d %s/%s/%s/%s to have ports %v, but got %v",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, exp.ports, conn.PreferredCsPorts)
				}
			},
		},
		{
			name:          "v1 network switch-only view",
			switchOnly:    true,
			mockCfg:       createV1MockConfig(),
			expectedErr:   false,
			expectedCount: 2,
			validateResult: func(t *testing.T, resp *csctlpb.GetClusterTopologyResponse) {
				type ExpectedConn struct {
					stampId, peerName, peerPort, switchName string
					ports                                   []string
				}
				expectedConns := []ExpectedConn{
					{"0", "", "", "sc-r7rb13-100gsw32-hp-1", []string{"0"}},
					{"0", "", "", "sc-r7rb13-100gsw32-hp-2", []string{"1"}},
				} // should be in this exact order. sc-r7rb14-100gsw64 is hidden because of no CsPort affinity.

				assert.True(t, len(expectedConns) == len(resp.Connections), "Expected %v connections but got %v",
					len(expectedConns), len(resp.Connections))
				for idx, conn := range resp.Connections {
					exp := expectedConns[idx]
					flag := exp.stampId == conn.StampId && exp.peerName == conn.PeerName && exp.peerPort == conn.PeerPort && exp.switchName == conn.SwitchName
					assert.True(t, flag, "Expected connection %d to be %s/%s/%s/%s, but got %s/%s/%s/%s",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, conn.StampId, conn.PeerName, conn.PeerPort, conn.SwitchName)
					assert.Equal(t, exp.ports, conn.PreferredCsPorts, "Expected connection %d %s/%s/%s/%s to have ports %v, but got %v",
						idx, exp.stampId, exp.peerName, exp.peerPort, exp.switchName, exp.ports, conn.PreferredCsPorts)
				}
			},
		},
		{
			name:          "empty config",
			switchOnly:    false,
			mockCfg:       &wscommon.ClusterConfig{},
			expectedErr:   false,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var srv *Server
			if tt.mockCfg != nil {
				srv = createMockServer(tt.mockCfg)
			} else {
				srv = &Server{}
			}

			req := &csctlpb.GetClusterTopologyRequest{
				SwitchOnly: tt.switchOnly,
			}

			ctx := context.Background()
			if tt.targetNs != "" {
				incomingMD, _ := metadata.FromIncomingContext(ctx)
				newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, tt.targetNs)
				ctx = metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
			}
			resp, err := srv.GetClusterTopology(ctx, req)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, resp)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Connections, tt.expectedCount)

				if tt.validateResult != nil {
					tt.validateResult(t, resp)
				}
			}
		})
	}
}

// createV2MockConfig creates a mock cluster config for testing
func createV2MockConfig() *wscommon.ClusterConfig {
	cfg := &wscommon.ClusterConfig{
		ClusterSchema: wscommon.ClusterSchema{
			Properties: map[string]string{
				"topology": "v2",
			},
		},
		NodeMap: map[string]*wscommon.Node{
			// Stamp 0 nodes
			"br-0": {
				Name:       "br-0",
				Role:       wscommon.RoleBroadcastReduce,
				Properties: map[string]string{resource.StampPropertyKey: "0", "v2Group": "leaf-sw-0"},
				NICs: []*wscommon.NetworkInterface{
					{
						Name: "nic0",
						Addr: "10.1.0.0",
					},
					{
						Name: "nic1",
						Addr: "10.1.0.1",
					},
				},
			},
			"ax-0": {
				Name:       "ax-0",
				Role:       wscommon.RoleActivation,
				Properties: map[string]string{resource.StampPropertyKey: "0"},
				NICs: []*wscommon.NetworkInterface{
					{
						Name:    "nic0",
						Addr:    "10.2.0.0",
						V2Group: "leaf-sw-0",
					},
					{
						Name:    "nic1",
						Addr:    "10.2.0.1",
						V2Group: "leaf-sw-1",
					},
				},
			},
			"mg-0": {
				Name:       "mg-0",
				Role:       wscommon.RoleManagement,
				Properties: map[string]string{resource.StampPropertyKey: "0"},
				NICs: []*wscommon.NetworkInterface{
					{
						Name:    "eth0",
						Addr:    "10.3.0.0",
						V2Group: "lw-0",
					},
				},
			},

			// Stamp 1 nodes
			"br-1": {
				Name:       "br-1",
				Role:       wscommon.RoleBroadcastReduce,
				Properties: map[string]string{resource.StampPropertyKey: "1", "v2Group": "leaf-sw-2"},
				NICs: []*wscommon.NetworkInterface{
					{
						Name: "nic0",
						Addr: "10.1.1.0",
					},
					{
						Name: "nic1",
						Addr: "10.1.1.1",
					},
				},
			},
			"ax-1": {
				Name:       "ax-1",
				Role:       wscommon.RoleActivation,
				Properties: map[string]string{resource.StampPropertyKey: "1"},
				NICs: []*wscommon.NetworkInterface{
					{
						Name:    "nic0",
						Addr:    "10.2.1.0",
						V2Group: "leaf-sw-2",
					},
					{
						Name:    "nic1",
						Addr:    "10.2.1.1",
						V2Group: "leaf-sw-3",
					},
				},
			},
		},
		SystemMap: map[string]*systemv1.SystemSpec{
			"sys-0": {
				Name: "sys-0",
				Properties: map[string]string{
					stampPropertyKey: "0",
				},
			},
			"sys-1": {
				Name: "sys-1",
				Properties: map[string]string{
					stampPropertyKey: "0",
				},
			},
			"sys-2": {
				Name: "sys-2",
				Properties: map[string]string{
					stampPropertyKey: "1",
				},
			},
		},
		V2GroupMap: map[string]*wscommon.NodeGroup{
			// Stamp 0 switches
			"leaf-sw-0": {
				Name: "leaf-sw-0",
				Properties: map[string]string{
					stampPropertyKey:        "0",
					preferredCsPortsPropKey: "0,1",
					"sys-0-0":               "", // sys-0 port 0 connected to this switch
					"sys-1-0":               "", // sys-1 port 0 connected to this switch
					"sys-0-1":               "", // sys-0 port 1 connected to this switch
					"sys-1-1":               "", // sys-1 port 1 connected to this switch
				},
			},
			"leaf-sw-1": {
				Name: "leaf-sw-1",
				Properties: map[string]string{
					stampPropertyKey:        "0",
					preferredCsPortsPropKey: "2,3",
					"sys-0-2":               "", // sys-0 port 2 connected to this switch
					"sys-1-2":               "", // sys-1 port 2 connected to this switch
					"sys-0-3":               "", // sys-0 port 3 connected to this switch
					"sys-1-3":               "", // sys-1 port 3 connected to this switch
				},
			},

			// Stamp 1 switches
			"leaf-sw-2": {
				Name: "leaf-sw-2",
				Properties: map[string]string{
					stampPropertyKey:        "1",
					preferredCsPortsPropKey: "6,7",
					"sys-2-6":               "", // sys-2 port 6 connected to this switch
					"sys-2-7":               "", // sys-2 port 7 connected to this switch
				},
			},
			"leaf-sw-3": {
				Name: "leaf-sw-3",
				Properties: map[string]string{
					stampPropertyKey:        "1",
					preferredCsPortsPropKey: "8,9",
					"sys-2-8":               "", // sys-2 port 8 connected to this switch
					"sys-2-9":               "", // sys-2 port 9 connected to this switch
				},
			},
		},
	}
	cfg.InitializeNsAssignment(namespacev1.NamespaceReservationList{
		Items: []namespacev1.NamespaceReservation{createMockNsr(testNsr, []string{"sys-0"}, []string{"mg-0", "ax-1"})}})
	return cfg
}

// createV1MockConfig creates a mock v1 cluster config for testing
func createV1MockConfig() *wscommon.ClusterConfig {
	cfg := &wscommon.ClusterConfig{
		ClusterSchema: wscommon.ClusterSchema{
			Properties: map[string]string{
				"topology": "v1",
			},
		},
		NodeMap: map[string]*wscommon.Node{
			"br-v1": {
				Name: "br-v1",
				Role: wscommon.RoleBroadcastReduce,
				NICs: []*wscommon.NetworkInterface{
					{
						Name:    "nic0",
						Addr:    "10.1.0.10",
						CsPort:  wscommon.Pointer(0),
						V2Group: "sc-r7rb13-100gsw32-hp-1",
					},
					{
						Name:    "nic1",
						Addr:    "10.1.0.11",
						CsPort:  wscommon.Pointer(0),
						V2Group: "sc-r7rb13-100gsw32-hp-1",
					},
				},
			},
			"br-v2": {
				Name: "br-v2",
				Role: wscommon.RoleBroadcastReduce,
				NICs: []*wscommon.NetworkInterface{
					{
						Name:    "nic0",
						Addr:    "10.2.0.10",
						CsPort:  wscommon.Pointer(1),
						V2Group: "sc-r7rb13-100gsw32-hp-2",
					},
					{
						Name:    "nic1",
						Addr:    "10.2.0.11",
						CsPort:  wscommon.Pointer(1),
						V2Group: "sc-r7rb13-100gsw32-hp-2",
					},
				},
			},
			"mg-v1": {
				Name:       "mg-v1",
				Role:       wscommon.RoleManagement,
				Properties: map[string]string{"group": "nodegroup1", "v2Group": "sc-r7rb14-100gsw64"},
				NICs: []*wscommon.NetworkInterface{
					{
						Name: "nic0",
						Addr: "10.3.0.10",
					},
				},
			},
		},
		SystemMap: map[string]*systemv1.SystemSpec{
			"sys-v1-0": {
				Name:       "sys-v1-0",
				Properties: map[string]string{},
			},
			"sys-v1-1": {
				Name:       "sys-v1-1",
				Properties: map[string]string{},
			},
		},
		GroupMap: map[string]*wscommon.NodeGroup{
			"nodegroup0": {
				Name: "nodegroup0",
				Properties: map[string]string{
					switchPropertyKey: "sc-r7rb14-100gsw64",
				},
				SwitchConfig: map[string]string{
					"subnet":              "10.250.91.0/24",
					"gateway":             "10.250.91.254",
					"parentnet":           "10.250.88.0/21",
					"asn":                 "65001.808",
					"virtualStart":        "10.250.91.49",
					"virtualEnd":          "10.250.91.243",
					"virtualSubnetStrict": "10.250.91.128/25",
					"virtualStartStrict":  "10.250.91.128",
					"virtualEndStrict":    "10.250.91.253",
					"memoryNIC":           "eth100g0",
					"workerNIC":           "eth100g0",
					"managementNIC":       "ens2f0np0",
				},
			},
		},
		V2GroupMap: map[string]*wscommon.NodeGroup{
			"sc-r7rb13-100gsw32-hp-1": {
				Name: "sc-r7rb13-100gsw32-hp-1",
				Properties: map[string]string{
					preferredCsPortsPropKey: "0",
					"sys-v1-0-0":            "",
					"sys-v1-1-0":            "",
				},
			},
			"sc-r7rb13-100gsw32-hp-2": {
				Name: "sc-r7rb13-100gsw32-hp-2",
				Properties: map[string]string{
					preferredCsPortsPropKey: "1",
					"sys-v1-0-1":            "",
					"sys-v1-1-1":            "",
				},
			},
			"sc-r7rb14-100gsw64": {
				Name: "sc-r7rb14-100gsw64",
			},
		},
	}

	cfg.InitializeNsAssignment(namespacev1.NamespaceReservationList{
		Items: []namespacev1.NamespaceReservation{createMockNsr(testNsr, []string{"sys-v1-0"}, []string{})}})
	return cfg
}

func TestGetJobTopology(t *testing.T) {
	tests := []struct {
		name           string
		request        *csctlpb.GetJobTopologyRequest
		mockValues     []model.Value
		mockError      error
		expectedErr    bool
		expectedCode   codes.Code
		expectedCount  int
		validateResult func(t *testing.T, resp *csctlpb.GetJobTopologyResponse)
	}{
		{
			name: "successful job topology query",
			request: &csctlpb.GetJobTopologyRequest{
				JobId:        "test-job-123",
				JobNamespace: "test-namespace",
			},
			mockValues: []model.Value{
				model.Vector{
					&model.Sample{
						Metric: model.Metric{
							"pod_id":           "pod-1",
							"node_name":        "ax-1",
							"nic_names":        "eth0,eth1",
							"v2_groups":        "switch-1",
							"target_ports":     "1,2,3",
							"preferred_ports":  "1,2",
							"target_resources": "system-0",
							"target_v2_groups": "switch-1",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":           "pod-2",
							"node_name":        "ax-2",
							"nic_names":        "eth0",
							"v2_groups":        "switch-2",
							"target_ports":     "4,5,6",
							"preferred_ports":  "4",
							"target_resources": "system-1",
							"target_v2_groups": "switch-3",
						},
						Value: 1.0,
					},
				},
				model.Vector{
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-1",
							"br_set_id":         "2",
							"target_br_set_ids": "0,1",
							"target_port":       "3",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-2",
							"br_set_id":         "0",
							"target_br_set_ids": "0",
							"target_port":       "3",
						},
						Value: 1.0,
					},
				},
			},
			expectedErr:   false,
			expectedCount: 2,
			validateResult: func(t *testing.T, resp *csctlpb.GetJobTopologyResponse) {
				assert.Len(t, resp.Connections, 2)

				// Validate first connection
				conn1 := resp.Connections[0]
				assert.Equal(t, "pod-1", conn1.PodId)
				assert.Equal(t, "ax-1", conn1.NodeName)
				assert.Equal(t, "eth0,eth1", conn1.Nics)
				assert.Equal(t, "switch-1", conn1.NodeLeafSwitch)
				assert.Equal(t, "1,2,3", conn1.SystemPorts)
				assert.Equal(t, "1,2", conn1.PodPreferredCsPorts)
				assert.Equal(t, "system-0,pod-2", conn1.EgressTargets)
				assert.Equal(t, "switch-1", conn1.SystemAffinitySwitch)

				// Validate second connection
				conn2 := resp.Connections[1]
				assert.Equal(t, "pod-2", conn2.PodId)
				assert.Equal(t, "ax-2", conn2.NodeName)
				assert.Equal(t, "eth0", conn2.Nics)
				assert.Equal(t, "switch-2", conn2.NodeLeafSwitch)
				assert.Equal(t, "4,5,6", conn2.SystemPorts)
				assert.Equal(t, "4", conn2.PodPreferredCsPorts)
				assert.Equal(t, "system-1", conn2.EgressTargets)
				assert.Equal(t, "switch-3", conn2.SystemAffinitySwitch)
			},
		},
		{
			name: "complicated br topology query",
			request: &csctlpb.GetJobTopologyRequest{
				JobId:        "test-job-br-topo",
				JobNamespace: "test-namespace",
			},
			mockValues: []model.Value{
				model.Vector{
					&model.Sample{
						Metric: model.Metric{
							"pod_id": "pod-1",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id": "pod-2",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id": "pod-3",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id": "pod-4",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id": "pod-5",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id": "pod-6",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id": "pod-7",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id": "pod-8",
						},
						Value: 1.0,
					},
				},
				model.Vector{
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-1",
							"br_set_id":         "0",
							"target_br_set_ids": "0",
							"target_port":       "0",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-2",
							"br_set_id":         "0",
							"target_br_set_ids": "0",
							"target_port":       "1",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-3",
							"br_set_id":         "0",
							"target_br_set_ids": "0",
							"target_port":       "2",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-4",
							"br_set_id":         "1",
							"target_br_set_ids": "",
							"target_port":       "0",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-5",
							"br_set_id":         "1",
							"target_br_set_ids": "",
							"target_port":       "2",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-6",
							"br_set_id":         "2",
							"target_br_set_ids": "0,1",
							"target_port":       "0",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-7",
							"br_set_id":         "2",
							"target_br_set_ids": "0,1",
							"target_port":       "1",
						},
						Value: 1.0,
					},
					&model.Sample{
						Metric: model.Metric{
							"pod_id":            "pod-8",
							"br_set_id":         "2",
							"target_br_set_ids": "0,1",
							"target_port":       "2",
						},
						Value: 1.0,
					},
				},
			},
			expectedErr:   false,
			expectedCount: 8,
			validateResult: func(t *testing.T, resp *csctlpb.GetJobTopologyResponse) {
				assert.Len(t, resp.Connections, 8)

				conn1 := resp.Connections[0]
				assert.Equal(t, "pod-1", conn1.PodId)
				assert.Equal(t, "", conn1.EgressTargets)

				conn2 := resp.Connections[1]
				assert.Equal(t, "pod-2", conn2.PodId)
				assert.Equal(t, "", conn2.EgressTargets)

				conn3 := resp.Connections[2]
				assert.Equal(t, "pod-3", conn3.PodId)
				assert.Equal(t, "", conn3.EgressTargets)

				conn4 := resp.Connections[3]
				assert.Equal(t, "pod-4", conn4.PodId)
				assert.Equal(t, "", conn4.EgressTargets)

				conn5 := resp.Connections[4]
				assert.Equal(t, "pod-5", conn5.PodId)
				assert.Equal(t, "", conn5.EgressTargets)

				conn6 := resp.Connections[5]
				assert.Equal(t, "pod-6", conn6.PodId)
				assert.Equal(t, "pod-1,pod-4", conn6.EgressTargets)

				conn7 := resp.Connections[6]
				assert.Equal(t, "pod-7", conn7.PodId)
				assert.Equal(t, "pod-2", conn7.EgressTargets)

				conn8 := resp.Connections[7]
				assert.Equal(t, "pod-8", conn8.PodId)
				assert.Equal(t, "pod-3,pod-5", conn8.EgressTargets)
			},
		},
		{
			name: "empty job topology result",
			request: &csctlpb.GetJobTopologyRequest{
				JobId:        "test-job-empty",
				JobNamespace: "test-namespace",
			},
			mockValues:    []model.Value{model.Vector{}},
			expectedErr:   false,
			expectedCount: 0,
			validateResult: func(t *testing.T, resp *csctlpb.GetJobTopologyResponse) {
				assert.Empty(t, resp.Connections)
			},
		},
		{
			name: "job topology with missing metric fields",
			request: &csctlpb.GetJobTopologyRequest{
				JobId:        "test-job-partial",
				JobNamespace: "test-namespace",
			},
			mockValues: []model.Value{
				model.Vector{
					&model.Sample{
						Metric: model.Metric{
							"pod_id":    "pod-1",
							"node_name": "ax-1",
							// Missing some fields like stamp_id, nic_names, etc.
						},
						Value: 1.0,
					},
				},
			},
			expectedErr:   false,
			expectedCount: 1,
			validateResult: func(t *testing.T, resp *csctlpb.GetJobTopologyResponse) {
				assert.Len(t, resp.Connections, 1)
				conn := resp.Connections[0]
				assert.Equal(t, "pod-1", conn.PodId)
				assert.Equal(t, "ax-1", conn.NodeName)
				// Missing fields should be empty strings
				assert.Equal(t, "", conn.StampId)
				assert.Equal(t, "", conn.Nics)
				assert.Equal(t, "", conn.NodeLeafSwitch)
				assert.Equal(t, "", conn.SystemPorts)
				assert.Equal(t, "", conn.PodPreferredCsPorts)
				assert.Equal(t, "", conn.EgressTargets)
			},
		},
		{
			name: "missing job id",
			request: &csctlpb.GetJobTopologyRequest{
				JobId:        "",
				JobNamespace: "test-namespace",
			},
			expectedErr:  true,
			expectedCode: codes.InvalidArgument,
		},
		{
			name: "missing job namespace",
			request: &csctlpb.GetJobTopologyRequest{
				JobId:        "test-job-123",
				JobNamespace: "",
			},
			expectedErr:  true,
			expectedCode: codes.InvalidArgument,
		},
		{
			name: "metrics query error",
			request: &csctlpb.GetJobTopologyRequest{
				JobId:        "test-job-123",
				JobNamespace: "test-namespace",
			},
			mockError:    assert.AnError,
			expectedErr:  true,
			expectedCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockQuerier := &wscommon.MockQuerier{
				Values: tt.mockValues,
			}

			server := createMockJobTopologyServer(mockQuerier)

			resp, err := server.GetJobTopology(context.Background(), tt.request)

			if tt.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, resp)

				// Check specific gRPC error code if specified
				if tt.expectedCode != codes.OK {
					st, ok := status.FromError(err)
					require.True(t, ok, "Expected gRPC status error")
					assert.Equal(t, tt.expectedCode, st.Code())
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.Len(t, resp.Connections, tt.expectedCount)

				if tt.validateResult != nil {
					tt.validateResult(t, resp)
				}
			}
		})
	}
}

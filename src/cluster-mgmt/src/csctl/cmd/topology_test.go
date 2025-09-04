package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/csctl/pkg"
)

func TestTopology(t *testing.T) {
	// Configure client factory
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
		ClientFactory:        clientFactory,
		Namespace:            "test-namespace",
	}

	t.Run("detailed view", func(t *testing.T) {
		outputBuf := new(bytes.Buffer)
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(outputBuf)
		cmd.SetArgs([]string{"cluster", "topology"})

		// Create mock connections for a detailed view
		connections := []*pb.SwitchPortConnection{
			{
				StampId:          "0",
				PeerName:         "sys-0",
				PeerPort:         "-",
				SwitchName:       "-",
				PreferredCsPorts: []string{"-"},
			},
			{
				StampId:          "0",
				PeerName:         "ax-0",
				PeerPort:         "nic0",
				SwitchName:       "leaf-sw-0",
				PreferredCsPorts: []string{"0", "1"},
			},
			{
				StampId:          "0",
				PeerName:         "br-0",
				PeerPort:         "nic0",
				SwitchName:       "leaf-sw-0",
				PreferredCsPorts: []string{"0"},
			},
			{
				StampId:          "0",
				PeerName:         "br-0",
				PeerPort:         "nic1",
				SwitchName:       "leaf-sw-0",
				PreferredCsPorts: []string{"1"},
			},
		}

		// Mock response from server
		clientFactory.AppendCsCtlResponse(&pb.GetClusterTopologyResponse{
			Connections: connections,
		})

		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, outputBuf.String(), `
STAMP  RESOURCE  INTERFACE  SWITCH     CS_PORTS
0      sys-0     -          -          -
0      ax-0      nic0       leaf-sw-0  0,1
0      br-0      nic0       leaf-sw-0  0
0      br-0      nic1       leaf-sw-0  1
`)
	})

	t.Run("switch only view", func(t *testing.T) {
		outputBuf := new(bytes.Buffer)
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(outputBuf)
		cmd.SetArgs([]string{"cluster", "topology", "--switch-only"})

		// Create mock connections for a switch-only view
		connections := []*pb.SwitchPortConnection{
			{
				StampId:          "0",
				SwitchName:       "leaf-sw-0",
				PreferredCsPorts: []string{"0", "1"},
				PeerName:         "-",
				PeerPort:         "-",
			},
			{
				StampId:          "0",
				SwitchName:       "leaf-sw-1",
				PreferredCsPorts: []string{"2", "3"},
				PeerName:         "-",
				PeerPort:         "-",
			},
		}

		// Mock response from server
		clientFactory.AppendCsCtlResponse(&pb.GetClusterTopologyResponse{
			Connections: connections,
		})

		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, outputBuf.String(), `
STAMP  SWITCH     CS_PORTS
0      leaf-sw-0  0,1
0      leaf-sw-1  2,3
`)
	})

	t.Run("empty response", func(t *testing.T) {
		outputBuf := new(bytes.Buffer)
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(outputBuf)
		cmd.SetArgs([]string{"cluster", "topology"})

		// Mock empty response from server
		clientFactory.AppendCsCtlResponse(&pb.GetClusterTopologyResponse{
			Connections: []*pb.SwitchPortConnection{},
		})

		require.NoError(t, cmd.Execute())
		result := outputBuf.String()
		assert.Contains(t, result, "No topology information available in the cluster")
	})

	t.Run("empty switch-only response", func(t *testing.T) {
		outputBuf := new(bytes.Buffer)
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(outputBuf)
		cmd.SetArgs([]string{"cluster", "topology", "--switch-only"})

		// Mock empty response from server
		clientFactory.AppendCsCtlResponse(&pb.GetClusterTopologyResponse{
			Connections: []*pb.SwitchPortConnection{},
		})

		require.NoError(t, cmd.Execute())
		result := outputBuf.String()
		assert.Contains(t, result, "No switch information available in the cluster")
	})

	t.Run("v1 network view", func(t *testing.T) {
		outputBuf := new(bytes.Buffer)
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(outputBuf)
		cmd.SetArgs([]string{"cluster", "topology"})

		// Create mock connections for a v1 network (uses switch property for switch names)
		connections := []*pb.SwitchPortConnection{
			{
				StampId:          "0",
				PeerName:         "sys-v1",
				PeerPort:         "",
				SwitchName:       "",
				PreferredCsPorts: []string{},
			},
			{
				StampId:          "0",
				PeerName:         "ax-v1",
				PeerPort:         "nic0",
				SwitchName:       "sc-r7rb11-100gsw65", // Different switch
				PreferredCsPorts: []string{"2", "3"},
			},
			{
				// All stamps default to "0" in v1 networks
				StampId:          "0",
				PeerName:         "br-v1",
				PeerPort:         "nic0",
				SwitchName:       "sc-r7rb11-100gsw64", // Switch name from group's switch property
				PreferredCsPorts: []string{"0"},
			},
			{
				StampId:          "0",
				PeerName:         "br-v1",
				PeerPort:         "nic1",
				SwitchName:       "sc-r7rb11-100gsw64", // Same switch
				PreferredCsPorts: []string{"1"},
			},
		}

		// Mock response from server
		clientFactory.AppendCsCtlResponse(&pb.GetClusterTopologyResponse{
			Connections: connections,
		})

		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, outputBuf.String(), `
STAMP  RESOURCE  INTERFACE  SWITCH              CS_PORTS
0      sys-v1    -          -                   -
0      ax-v1     nic0       sc-r7rb11-100gsw65  2,3
0      br-v1     nic0       sc-r7rb11-100gsw64  0
0      br-v1     nic1       sc-r7rb11-100gsw64  1
`)
	})

	t.Run("v1 network switch-only view", func(t *testing.T) {
		outputBuf := new(bytes.Buffer)
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(outputBuf)
		cmd.SetArgs([]string{"cluster", "topology", "--switch-only"})

		// Create mock connections for a v1 network switch-only view
		connections := []*pb.SwitchPortConnection{
			{
				// All stamps default to "0" in v1 networks
				StampId:          "0",
				SwitchName:       "sc-r7rb11-100gsw64", // Switch name from switch property
				PreferredCsPorts: []string{"0", "1"},
				PeerName:         "",
				PeerPort:         "",
			},
			{
				StampId:          "0",
				SwitchName:       "sc-r7rb11-100gsw65", // Switch name from switch property
				PreferredCsPorts: []string{"2", "3"},
				PeerName:         "",
				PeerPort:         "",
			},
		}

		// Mock response from server
		clientFactory.AppendCsCtlResponse(&pb.GetClusterTopologyResponse{
			Connections: connections,
		})

		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, outputBuf.String(), `
STAMP  SWITCH              CS_PORTS
0      sc-r7rb11-100gsw64  0,1
0      sc-r7rb11-100gsw65  2,3
`)
	})
}

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/csctl/pkg"
)

func TestClearWorkerCache(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
		ClientFactory:        clientFactory,
	}

	t.Run("clearworkercache happy path", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"clear-worker-cache"})

		clientFactory.AppendCsCtlResponse(&pb.ClearWorkerCacheResponse{Message: "Worker caches cleared successfully"})

		require.NoError(t, cmd.Execute())
	})

	t.Run("clearworkercache bad args path", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetErr(output.E)
		cmd.SetArgs([]string{"clear-worker-cache", "extraarg"})

		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "the clear-worker-cache command does not take any arguments")
	})
}

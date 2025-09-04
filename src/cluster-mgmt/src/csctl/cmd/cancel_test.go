package cmd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/csctl/pkg"
)

func TestCancel(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
		ClientFactory:        clientFactory,
		Namespace:            pkg.SystemNamespace,
		Context:              context.Background(),
	}

	t.Run("cancel job happy path", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"cancel", "job", "xxx"})

		clientFactory.AppendCsCtlResponse(&pb.CancelJobResponse{Message: "job canceled"})
		require.NoError(t, cmd.Execute())
	})

	t.Run("cancel job bad args path", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetErr(output.E)
		cmd.SetArgs([]string{"cancel"})

		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid arguments")
	})
}

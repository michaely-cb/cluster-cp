package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/csctl/pkg"
)

func TestResourceTypesCmd(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
	}

	t.Run("happy path", func(t *testing.T) {
		expectedOut := "resourceA\nresourceB\nresourceC"
		var resources []*pb.ResourceType
		for _, name := range strings.Split(expectedOut, "\n") {
			resources = append(resources, &pb.ResourceType{Name: name})
		}
		clientFactory.csCtlClient.responseOrErr = append(
			clientFactory.csCtlClient.responseOrErr,
			&pb.ListResourceTypesResponse{Items: resources},
		)

		cmd := NewResourceTypesCmd(&ctx)
		options := ResourceTypesCmdOptions{}

		err := options.Complete(cmd, &ctx, []string{})
		require.NoError(t, err)

		errOut := NewFakeOutErr()
		options.outErr = errOut
		err = options.Run(context.TODO())
		require.NoError(t, err)
		assert.Equal(t, expectedOut, strings.TrimSpace(errOut.O.String()))
	})
}

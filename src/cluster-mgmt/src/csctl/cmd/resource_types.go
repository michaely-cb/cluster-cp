package cmd

import (
	"context"
	"fmt"
	"strings"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"github.com/spf13/cobra"

	"cerebras.com/cluster/csctl/pkg"
)

type ResourceTypesCmdOptions struct {
	cmdCtx *pkg.CmdCtx
	outErr OutErr
}

func NewResourceTypesCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := ResourceTypesCmdOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "types",
		Short: "Display resource types",
		Long: `Display resource types that the server hosts.

This is to be used in the TYPE argument of commands such as get or label.`,
		Example: `  csctl types`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, cmdCtx, args); err != nil {
				return err
			}

			return options.Run(cmdCtx.Context)
		},
		SilenceUsage: true,
	}
	return SetDefaults(cmd)
}

func (c *ResourceTypesCmdOptions) Complete(cmd *cobra.Command, cmdCtx *pkg.CmdCtx, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("the types command does not take any arguments")
	}
	c.outErr = CmdOutErr{cmd: cmd}
	c.cmdCtx = cmdCtx
	return nil
}

func (c *ResourceTypesCmdOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	res, err := client.ListResourceTypes(ctx, &pb.ListResourceTypesRequest{})
	if err != nil {
		return pkg.FormatGrpcError(err)
	}

	var names []string
	for _, r := range res.Items {
		names = append(names, r.GetName())
	}
	_, err = fmt.Fprintln(c.outErr.Out(), strings.Join(names, "\n"))
	return err
}

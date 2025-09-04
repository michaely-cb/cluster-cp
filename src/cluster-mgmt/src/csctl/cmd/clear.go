package cmd

import (
	"context"
	"fmt"

	"github.com/kr/text"
	"github.com/spf13/cobra"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/csctl/pkg"
)

type ClearWorkerCacheCmdOptions struct {
	cmdCtx *pkg.CmdCtx
	outErr OutErr
}

func NewClearWorkerCacheCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := ClearWorkerCacheCmdOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:     "clear-worker-cache",
		Short:   "Clear the worker cache",
		Long:    `Clear the worker cache on all worker nodes.`,
		Example: text.Indent(`csctl clear-worker-caches`, "  "),
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

func (c *ClearWorkerCacheCmdOptions) Complete(cmd *cobra.Command, cmdCtx *pkg.CmdCtx, args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("the clear-worker-cache command does not take any arguments")
	}
	c.outErr = CmdOutErr{cmd: cmd}
	c.cmdCtx = cmdCtx
	return nil
}

func (c *ClearWorkerCacheCmdOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	res, err := client.ClearWorkerCache(ctx, &pb.ClearWorkerCacheRequest{})
	if err != nil {
		return pkg.FormatGrpcError(err)
	}

	_, err = fmt.Fprintln(c.outErr.Out(), res.Message)
	return err
}

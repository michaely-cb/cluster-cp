package cmd

import (
	"context"
	"fmt"

	"github.com/kr/text"
	"github.com/spf13/cobra"

	"cerebras.com/cluster/csctl/pkg"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
)

type CancelCmdOptions struct {
	Name string
	// Force is deprecated and does not have any effect starting from release 2.1
	// TODO: Remove this option in release 2.3
	Force bool

	cmdCtx *pkg.CmdCtx
	outErr OutErr
}

func NewCancelCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := CancelCmdOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "cancel job [NAME]",
		Short: "Cancel job",
		Long:  `Cancels a running job created by this user. This releases all resources and sets the job to a cancelled state.`,
		Example: text.Indent(`# cancel a job
csctl cancel job wsjob-0001

# cancel a job from a specific namespace
csctl cancel job wsjob-0001 --namespace perf-test
`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, cmdCtx, args); err != nil {
				return err
			}

			return options.Run(cmdCtx.Context)
		},
		SilenceUsage: true,
	}
	cmd.Flags().BoolVarP(&options.Force, "force", "f", false, "Whether to cancel a job owned by other users.")
	cmd.Flags().Lookup("force").Hidden = true
	return SetDefaults(cmd)
}

func (c *CancelCmdOptions) Complete(cmd *cobra.Command, cmdCtx *pkg.CmdCtx, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("invalid arguments, usage: 'csctl cancel job NAME'")
	}
	c.Name = args[1]
	c.outErr = CmdOutErr{cmd: cmd}
	return nil
}

func (c *CancelCmdOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(c.cmdCtx.Context); err != nil {
		return err
	}

	cancelReq := &pb.CancelJobRequest{
		JobId:     c.Name,
		JobStatus: pb.JobStatus_JOB_STATUS_CANCELLED,
	}
	cancelRes, err := client.CancelJobV2(c.cmdCtx.Context, cancelReq)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}
	_, err = fmt.Fprintln(c.outErr.Out(), cancelRes.Message)
	return err
}

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/csctl/pkg"
)

type DebugArtifactStatusOptions struct {
	Name   string
	cmdCtx *pkg.CmdCtx
	outErr OutErr
}

type DebugArtifactUpdateOptions struct {
	Name              string
	RetentionDeadline string
	MarkDelete        bool
	cmdCtx            *pkg.CmdCtx
	outErr            OutErr
}

func NewDebugArtifactCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug-artifact",
		Short: "Manage debug artifacts.",
		Long:  `Manage debug artifacts retention status and expiration.`,
		Example: `
# Get status of a debug artifact
csctl debug-artifact get wsjob-0001

# Update the retention date for a debug artifact
csctl debug-artifact update wsjob-0001 --retention-deadline 2023-12-31

# Mark a debug artifact for deletion (sets retention to today's date)
csctl debug-artifact update wsjob-0001 --mark-delete
`,
	}

	cmd.AddCommand(NewDebugArtifactStatusCmd(cmdCtx))
	cmd.AddCommand(NewDebugArtifactUpdateCmd(cmdCtx))

	return SetDefaults(cmd)
}

func NewDebugArtifactStatusCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := DebugArtifactStatusOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "get JOB_ID",
		Short: "Get the retention status of a debug artifact.",
		Long:  `Get status including retention deadline of a debug artifact.`,
		Example: `
csctl debug-artifact get wsjob-0001
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, cmdCtx, args); err != nil {
				return err
			}
			return options.Run(cmdCtx.Context)
		},
		SilenceUsage: true,
	}

	options.outErr = CmdOutErr{cmd: cmd}
	return cmd
}

func NewDebugArtifactUpdateCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := DebugArtifactUpdateOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "update JOB_ID (--retention-deadline <date> | --mark-delete)",
		Short: "Update the retention timestamp for a debug artifact.",
		Long:  `Update the retention timestamp for a debug artifact, using either a specific date or mark it for deletion.`,
		Example: `
csctl debug-artifact update wsjob-0001 --retention-deadline 2023-12-31
csctl debug-artifact update wsjob-0001 --mark-delete
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, cmdCtx, args); err != nil {
				return err
			}
			return options.Run(cmdCtx.Context)
		},
		SilenceUsage: true,
	}

	cmd.Flags().StringVar(&options.RetentionDeadline, "retention-deadline", "", "The date until which the artifact should be retained (YYYY-MM-DD format).")
	cmd.Flags().BoolVar(&options.MarkDelete, "mark-delete", false, "Mark the artifact for deletion by setting retention to today's date.")
	options.outErr = CmdOutErr{cmd: cmd}
	return cmd
}

func (c *DebugArtifactStatusOptions) Complete(cmd *cobra.Command, _ *pkg.CmdCtx, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing required argument JOB_ID")
	}
	c.Name = args[0]
	return nil
}

func (c *DebugArtifactStatusOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	req := &pb.GetDebugArtifactStatusRequest{
		ExportId: c.Name,
	}
	res, err := client.GetDebugArtifactStatus(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				return fmt.Errorf("debug artifact '%s' does not exist in namespace %s", c.Name, c.cmdCtx.Namespace)
			}
		}
		return pkg.FormatGrpcError(err)
	}

	if res.RetentionDeadline == nil {
		fmt.Println("No retention timestamp set for debug artifact", c.Name)
	} else {
		// Format the timestamp as YYYY-MM-DD
		dateStr := res.RetentionDeadline.AsTime().Format("2006-01-02")
		fmt.Printf("Debug artifact %s is set to be retained until: %s\n", c.Name, dateStr)
	}
	return nil
}

func (c *DebugArtifactUpdateOptions) Complete(cmd *cobra.Command, _ *pkg.CmdCtx, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing required argument JOB_ID")
	}
	c.Name = args[0]

	// Validate that either --retention-deadline or --mark-delete is specified, but not both
	if c.RetentionDeadline != "" && c.MarkDelete {
		return fmt.Errorf("cannot specify both --retention-deadline and --mark-delete")
	}
	if c.RetentionDeadline == "" && !c.MarkDelete {
		return fmt.Errorf("must specify either --retention-deadline or --mark-delete")
	}

	// If --mark-delete is specified, use today's date
	if c.MarkDelete {
		// Go's time format uses a reference date (2006-01-02) to represent YYYY-MM-DD
		c.RetentionDeadline = time.Now().UTC().Format("2006-01-02") // Format as YYYY-MM-DD
	}

	return nil
}

func (c *DebugArtifactUpdateOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	// Parse the date string and convert to timestamp
	parsedTime, err := time.Parse("2006-01-02", c.RetentionDeadline)
	if err != nil {
		return fmt.Errorf("invalid date format: %v", err)
	}

	// Convert to protobuf Timestamp
	pbTimestamp := timestamppb.New(parsedTime)

	req := &pb.UpdateDebugArtifactStatusRequest{
		ExportId:          c.Name,
		RetentionDeadline: pbTimestamp,
	}
	res, err := client.UpdateDebugArtifactStatus(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				return fmt.Errorf("debug artifact '%s' does not exist in namespace %s", c.Name, c.cmdCtx.Namespace)
			}
		}
		return pkg.FormatGrpcError(err)
	}

	fmt.Println(res.Message)
	if c.MarkDelete {
		fmt.Printf("Debug artifact %s has been marked for deletion.\n", c.Name)
	} else {
		fmt.Printf("Debug artifact %s will be retained until %s.\n", c.Name, c.RetentionDeadline)
	}
	return nil
}

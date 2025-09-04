package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/cluster/csctl/pkg"
)

type ClusterTopologyOptions struct {
	SwitchOnly bool
	cmdCtx     *pkg.CmdCtx
	out        io.Writer
}

func NewClusterTopologyCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := ClusterTopologyOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "topology",
		Short: "Display cluster network topology",
		Long:  `Display detailed network topology information of the cluster including nodes, NICs, and switches.`,
		Example: `
# Get detailed topology information
csctl cluster topology

# Get only switch information
csctl cluster topology --switch-only
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.out = cmd.OutOrStdout()
			return options.Run(cmdCtx.Context)
		},
		SilenceUsage: true,
	}

	cmd.Flags().BoolVar(&options.SwitchOnly, "switch-only", false, "Show only switch information")
	return cmd
}

func (c *ClusterTopologyOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	req := &pb.GetClusterTopologyRequest{
		SwitchOnly: c.SwitchOnly,
	}
	res, err := client.GetClusterTopology(ctx, req)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}

	// Format and print the topology information
	if c.SwitchOnly {
		return c.printSwitchOnlyView(c.out, res)
	}
	return c.printDetailedView(c.out, res)
}

func (c *ClusterTopologyOptions) printSwitchOnlyView(out io.Writer, res *pb.GetClusterTopologyResponse) error {
	if len(res.Connections) == 0 {
		fmt.Fprintln(out, "No switch information available in the cluster")
		return nil
	}

	// Create table structure
	table := &csv1.Table{
		Columns: []*csv1.ColumnDefinition{
			{Name: "stamp"},
			{Name: "switch"},
			{Name: "cs_ports"},
		},
		Rows: []*csv1.RowData{},
	}

	printedSwitches := make(map[string]bool)

	for _, conn := range res.Connections {
		if printedSwitches[conn.SwitchName] {
			continue
		}

		printedSwitches[conn.SwitchName] = true

		// Format the preferred CS ports
		portsStr := "-"
		if len(conn.PreferredCsPorts) > 0 {
			portsStr = fmt.Sprintf("%v", strings.Join(conn.PreferredCsPorts, ","))
		}

		row := &csv1.RowData{
			Cells: []string{
				conn.StampId,
				conn.SwitchName,
				portsStr,
			},
		}
		table.Rows = append(table.Rows, row)
	}

	outErr := CmdOutErr{cmd: &cobra.Command{}}
	outErr.cmd.SetOut(out)
	return DisplayTable(table, nil, outErr)
}

func (c *ClusterTopologyOptions) printDetailedView(out io.Writer, res *pb.GetClusterTopologyResponse) error {
	if len(res.Connections) == 0 {
		fmt.Fprintln(out, "No topology information available in the cluster")
		return nil
	}

	// Create table structure
	table := &csv1.Table{
		Columns: []*csv1.ColumnDefinition{
			{Name: "stamp"},
			{Name: "resource"},
			{Name: "interface"},
			{Name: "switch"},
			{Name: "cs_ports"},
		},
		Rows: []*csv1.RowData{},
	}

	for _, conn := range res.Connections {
		// Format the preferred CS ports
		portsStr := "-"
		if len(conn.PreferredCsPorts) > 0 {
			portsStr = fmt.Sprintf("%v", strings.Join(conn.PreferredCsPorts, ","))
		}

		// For nodes without NICs (like systems), show a placeholder
		peerPort := conn.PeerPort
		if peerPort == "" {
			peerPort = "-"
		}

		// For nodes without switches, show a placeholder
		switchName := conn.SwitchName
		if switchName == "" {
			switchName = "-"
		}

		row := &csv1.RowData{
			Cells: []string{
				conn.StampId,
				conn.PeerName,
				peerPort,
				switchName,
				portsStr,
			},
		}
		table.Rows = append(table.Rows, row)
	}

	outErr := CmdOutErr{cmd: &cobra.Command{}}
	outErr.cmd.SetOut(out)
	return DisplayTable(table, nil, outErr)
}

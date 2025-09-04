package cmd

import (
	"github.com/spf13/cobra"

	"cerebras.com/cluster/csctl/pkg"
)

// NewClusterCmd creates a new command for cluster-related commands
func NewClusterCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage cluster resources and information",
		Long:  `Manage Cerebras cluster resources, view status and topology information.`,
		Example: `
# Get detailed cluster topology information
csctl cluster topology

# Get only switch information
csctl cluster topology --switch-only
`,
	}

	cmd.AddCommand(NewClusterTopologyCmd(cmdCtx))

	return SetDefaults(cmd)
}

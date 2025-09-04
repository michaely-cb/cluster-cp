package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"cerebras.com/cluster/csctl/pkg"
)

// NOTE: keeping all but the view config command hidden, possibly forever.
// Config will be useful if we add support for multiple clusters on one usernode or user signin.
const hideConfigCommands = true

type ConfigViewCmdFlags struct {
	Raw bool
}

func NewConfigViewCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	flags := &ConfigViewCmdFlags{}
	cmd := &cobra.Command{
		Use:   "view [--raw]",
		Short: "View the current config",
		Long:  "View client configuration. Optionally view raw base64-encoded certificate data if present.",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := cmdCtx.Cfg
			if !flags.Raw {
				cfg = cfg.OmitData()
			}
			rv, err := yaml.Marshal(cfg.ConvertToNamedConfig())
			if err != nil {
				return err
			}
			_, err = fmt.Fprint(cmd.OutOrStdout(), string(rv))
			return err
		},
	}
	cmd.Flags().BoolVar(&flags.Raw, "raw", false, "Include flags to view raw certificate data.")
	return SetDefaults(cmd)
}

type SetClusterCmdFlags struct {
	Server        string
	Namespace     string
	CertAuthority string
	EmbedCerts    bool
}

func NewConfigSetClusterCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	flags := &SetClusterCmdFlags{}
	cmd := &cobra.Command{
		Use:   "set-cluster NAME [--server address] [--namespace namespace] [--certificate-authority cert-path] [--embed-certs]",
		Short: "Set a cluster entry and/or a namespace certificate authority in csctl config",
		Long: `Set a cluster entry and/or a namespace certificate authority in csctl config.

Specifying a name that already exists will merge new fields on top of existing values for those fields.`,
		Example: `  csctl config set-cluster prod --server localhost:8081

  csctl config set-cluster prod --namespace foo-ns --certificate-authority /home/cert.crt`,
		Hidden: hideConfigCommands,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid argument. Expecting a single cluster name")
			}

			clusterName := args[0]
			cluster := cmdCtx.Cfg.Clusters[clusterName]
			if cluster == nil {
				cluster = &pkg.Cluster{}
			}

			if cmd.Flags().Changed("server") {
				cluster.Server = flags.Server
			}
			if cmd.Flags().Changed("namespace") && cmd.Flags().Changed("certificate-authority") {
				// If the namespace certificate authority was successfully found, then we update the existing entry.
				// Otherwise, we construct a new entry and append to the list.
				found := false
				var nsCertAuth *pkg.NamespaceCertAuthority
				for _, _nsCertAuth := range cluster.Namespaces {
					if _nsCertAuth.Name == flags.Namespace {
						found = true
						nsCertAuth = &_nsCertAuth
					}
				}
				if nsCertAuth == nil {
					nsCertAuth = &pkg.NamespaceCertAuthority{
						Name: flags.Namespace,
					}
				}

				nsCertAuth.CertificateAuthority = ""
				nsCertAuth.CertificateAuthorityData = []byte{}
				if flags.EmbedCerts {
					if flags.CertAuthority != "" {
						data, err := os.ReadFile(flags.CertAuthority)
						if err != nil {
							return err
						}
						nsCertAuth.CertificateAuthorityData = data
					}
				} else {
					nsCertAuth.CertificateAuthority = flags.CertAuthority
				}

				if !found {
					cluster.Namespaces = append(cluster.Namespaces, *nsCertAuth)
				} else {
					for i, oldNsCertAuth := range cluster.Namespaces {
						if oldNsCertAuth.Name == flags.Namespace {
							cluster.Namespaces[i] = *nsCertAuth
						}
					}
				}
			} else if cmd.Flags().Changed("namespace") || cmd.Flags().Changed("certificate-authority") {
				return fmt.Errorf("namespace and certificate authority are required to be set together. " +
					"Please consult `csctl config set-cluster --help` for the example usage")
			}
			cmdCtx.Cfg.Clusters[clusterName] = cluster
			return cmdCtx.Cfg.SaveConfig()
		},
	}
	cmd.Flags().StringVar(&flags.Server, "server", "", "Cluster server URL.")
	cmd.Flags().StringVar(&flags.Namespace, "namespace", "", "Cluster server namespace.")
	cmd.Flags().StringVar(&flags.CertAuthority, "certificate-authority", "",
		"Path to certificate file containing cluster-server cert.")
	cmd.Flags().BoolVar(&flags.EmbedCerts, "embed-certs", false,
		"Embeds cert directly into the config rather than storing the path.")
	return SetDefaults(cmd)
}

func NewConfigDeleteClusterCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-cluster NAME",
		Short: "Delete a specified entry in csctl config",
		Long: `Delete a cluster.

Specifying a cluster that does not exist will not return an error.`,
		Example: `  csctl config delete-cluster prod`,
		Hidden:  hideConfigCommands,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid argument. Expecting a single name")
			}

			name := args[0]
			if _, ok := cmdCtx.Cfg.Clusters[name]; !ok {
				return nil
			}

			delete(cmdCtx.Cfg.Clusters, name)
			return cmdCtx.Cfg.SaveConfig()
		},
	}
	return SetDefaults(cmd)
}

func NewConfigUseClusterCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "use-cluster NAME",
		Short: "Use a particular cluster specified entry in csctl config",
		Long: `Use a cluster.

Internally this creates a context with the name of the target cluster with the target cluster as the cluster
for that context.`,
		Example: `  csctl config use-cluster prod`,
		Hidden:  hideConfigCommands,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid argument. Expecting a single name")
			}

			name := args[0]
			if _, ok := cmdCtx.Cfg.Clusters[name]; !ok {
				return fmt.Errorf("cluster with name '%s' does not exist", name)
			}
			cmdCtx.Cfg.Contexts[name] = &pkg.Context{Cluster: name}
			cmdCtx.Cfg.CurrentContext = name
			return cmdCtx.Cfg.SaveConfig()
		},
	}
	return SetDefaults(cmd)
}

func NewConfigUseContextCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "use-context NAME",
		Short:   "Use a particular context.",
		Long:    `Use a particular context.`,
		Example: `  csctl config use-context prod`,
		Hidden:  hideConfigCommands,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid argument. Expecting a single context name")
			}
			name := args[0]
			if _, ok := cmdCtx.Cfg.Contexts[name]; !ok {
				return fmt.Errorf("no context exists with the name '%s'", name)
			}

			cmdCtx.Cfg.CurrentContext = name
			return cmdCtx.Cfg.SaveConfig()
		},
	}
	return SetDefaults(cmd)
}

type SetContextCmdFlags struct {
	Cluster string
}

func NewConfigSetContextCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	flags := &SetContextCmdFlags{}
	cmd := &cobra.Command{
		Use:     "set-context NAME [--cluster cluster-name]",
		Short:   "Set a particular context.",
		Long:    `Set a particular context. Creates or updates the context depending if one with the given name already exists.`,
		Example: `  csctl config set-context prod --cluster prod`,
		Hidden:  hideConfigCommands,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid argument. Expecting a single context name")
			}

			name := args[0]
			context := cmdCtx.Cfg.Contexts[name]
			if context == nil {
				context = &pkg.Context{}
			}
			if cmd.Flags().Changed("cluster") {
				context.Cluster = flags.Cluster
			}
			cmdCtx.Cfg.Contexts[name] = context
			return cmdCtx.Cfg.SaveConfig()
		},
		SilenceUsage: true,
	}
	cmd.Flags().StringVar(&flags.Cluster, "cluster", "", "Cluster name.")
	return SetDefaults(cmd)
}

func NewConfigDeleteContextCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete-context NAME",
		Short:   "Delete a cluster context.",
		Long:    `Delete a particular cluster context.`,
		Example: `  csctl config delete-context prod`,
		Hidden:  hideConfigCommands,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("invalid argument. Expecting a single cluster name")
			}

			name := args[0]
			if _, ok := cmdCtx.Cfg.Contexts[name]; !ok {
				return nil
			}
			delete(cmdCtx.Cfg.Contexts, name)
			if cmdCtx.Cfg.CurrentContext == name {
				cmd.PrintErrf(
					"Warning: deleted current context. Update to a valid context with the 'use-context' command")
			}
			return cmdCtx.Cfg.SaveConfig()
		},
		SilenceUsage: true,
	}
	return SetDefaults(cmd)
}

func NewConfigCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "View csctl config files",
		Long:  `View csctl config files using subcommands like "csctl config view"`,
		Run:   func(cmd *cobra.Command, args []string) { _ = cmd.Help() },
	}
	configCmd.AddCommand(NewConfigViewCmd(cmdCtx))

	configCmd.AddCommand(NewConfigSetClusterCmd(cmdCtx))
	configCmd.AddCommand(NewConfigDeleteClusterCmd(cmdCtx))
	configCmd.AddCommand(NewConfigUseClusterCmd(cmdCtx))

	configCmd.AddCommand(NewConfigUseContextCmd(cmdCtx))
	configCmd.AddCommand(NewConfigSetContextCmd(cmdCtx))
	configCmd.AddCommand(NewConfigDeleteContextCmd(cmdCtx))
	return SetDefaults(configCmd)
}

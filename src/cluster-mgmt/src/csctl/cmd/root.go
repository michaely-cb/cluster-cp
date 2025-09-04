package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"cerebras.com/cluster/csctl/pkg"
)

type CsCtlFlags struct {
	CfgPath      string
	Namespace    string
	DebugLevel   int
	VerboseLevel string
}

const (
	SysAdminSupportMsg = "Please contact sysadmins for support."
)

func NewCsCtlCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	// Use the injected InClusterNodeChecker if available, otherwise use the default
	if cmdCtx.InClusterNodeChecker == nil {
		cmdCtx.InClusterNodeChecker = pkg.DefaultInClusterNodeChecker{}
	}
	cmdCtx.OnMgmtNode = cmdCtx.InClusterNodeChecker.CheckInClusterNode()

	if cmdCtx.OnMgmtNode {
		cmdCtx.SingleNodeCluster = cmdCtx.InClusterNodeChecker.CheckSingleNodeCluster()
	}
	flags := CsCtlFlags{}
	cmd := &cobra.Command{
		Use:     "csctl",
		Short:   "Cerebras cluster command line tool.",
		Version: fmt.Sprintf("%s (%s)", pkg.SemanticVersion, pkg.CerebrasVersion),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			logger := logrus.New()
			validLevels := []string{"debug", "info", "warn", "error"}
			if !slices.Contains(validLevels, flags.VerboseLevel) {
				return fmt.Errorf("invalid log level '%s'. Please use one of %v",
					flags.VerboseLevel, validLevels)
			}
			level, _ := logrus.ParseLevel(flags.VerboseLevel)
			logger.SetLevel(level)
			logger.SetFormatter(&logrus.TextFormatter{
				FullTimestamp: true,
				DisableColors: false,
			})
			cmdCtx.Logger = logger

			cfg, err := pkg.LoadConfig(flags.CfgPath)
			if err != nil {
				return fmt.Errorf("config error. Failed to read config file %s: %v", flags.CfgPath, err)
			}
			cmdCtx.Cfg = cfg
			cmdCtx.Cfg.OnMgmtNode = cmdCtx.OnMgmtNode
			if cmdCtx.ClientFactory == nil {
				if cmd.Parent().Name() == "config" {
					// We do not need to load namespace cert authority if the command was "config" which only runs locally.
					cmdCtx.Namespace = flags.Namespace
				} else {
					nsCertAuth, err := cmdCtx.Cfg.LoadNamespaceCertAuthority(flags.Namespace)
					if err != nil {
						return err
					}

					// At a high level, "nsCertAuth.Name" decides which namespaced ingress the client should talk to,
					// while "cmdCtx.Namespace" decides which namespaced resources (jobs, log-export) the client
					// queries and updates.
					if nsCertAuth.Name == pkg.SystemNamespace {
						// If "nsCertAuth.Name" is the system namespace, it means the client has access to and
						// will use the system namespace ingress for the subsequent requests.
						if flags.Namespace == "" {
							// If the user did not specify the namespace option, or the namespace option was specified
							// with the default namespace, this will have an effect on the cluster server of retrieving
							// resources of all namespaces.
							cmdCtx.Namespace = pkg.SystemNamespace
						} else {
							// If the user specified a non-empty namespace, we set the context to that namespace, so csctl
							// will only retrieve resources of that namespace. We will only validate if the specified
							// namespace on the server side.
							cmdCtx.Namespace = flags.Namespace
						}
					} else {
						cmdCtx.Namespace = nsCertAuth.Name
					}

					cluster := cmdCtx.Cfg.GetCurrentCluster()
					cmdCtx.ClientFactory = pkg.NewClientFactory(cmdCtx, cluster, nsCertAuth)
				}
			}
			cmdCtx.DebugLevel = flags.DebugLevel

			cmdCtx.Uid, cmdCtx.Gid, cmdCtx.Groups, err = cmdCtx.UserIdentityProvider.GetUidGidGroups()
			if err != nil {
				return err
			}
			return nil
		},
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPostRun: func(_ *cobra.Command, _ []string) {
			if cmdCtx.ClientFactory != nil {
				cmdCtx.ClientFactory.Close() // TODO: log error if any
			}
		},
	}

	cmd.PersistentFlags().StringVar(&flags.CfgPath,
		"csconfig", pkg.DefaultSystemCfgPath,
		fmt.Sprintf("config file %s", pkg.DefaultSystemCfgPath),
	)
	cmd.PersistentFlags().StringVarP(&flags.Namespace,
		"namespace", "n", "",
		fmt.Sprintf("configure csctl to talk to different user namespaces"),
	)
	cmd.PersistentFlags().IntVarP(&flags.DebugLevel,
		"debug", "d", 0,
		"higher debug values will display more fields in output objects",
	)
	cmd.PersistentFlags().StringVarP(&flags.VerboseLevel,
		"verbose", "v", "error",
		"Set the logging verbose level (debug, info, warn, error)")
	cmd.PersistentFlags().Lookup("verbose").Hidden = true

	cmd.AddCommand(NewConfigCmd(cmdCtx))
	cmd.AddCommand(NewGetCmd(cmdCtx))
	cmd.AddCommand(NewLabelCmd(cmdCtx))
	cmd.AddCommand(NewCancelCmd(cmdCtx))
	cmd.AddCommand(NewResourceTypesCmd(cmdCtx))
	cmd.AddCommand(NewLogExportCmd(cmdCtx))
	cmd.AddCommand(NewDebugArtifactCmd(cmdCtx))
	cmd.AddCommand(NewClusterCmd(cmdCtx))
	cmd.AddCommand(NewClearWorkerCacheCmd(cmdCtx))
	cmd.AddCommand(NewCheckVolumesCmd(cmdCtx))
	cmd.AddCommand(NewSessionCmd(cmdCtx))
	cmd.AddCommand(NewJobCmd(cmdCtx))
	cmd.AddCommand(NewSysMaintCmd(cmdCtx))
	cmd.SetOut(os.Stdout)
	return SetDefaults(cmd)
}

func Execute() {
	rootCmdCtx := &pkg.CmdCtx{
		UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
		Context:              context.Background(),
		InClusterNodeChecker: pkg.DefaultInClusterNodeChecker{},
	}
	err := NewCsCtlCmd(rootCmdCtx).Execute()
	if err != nil {
		os.Exit(1)
	}
}

func SetDefaults(cmd *cobra.Command) *cobra.Command {
	cmd.SetHelpCommand(&cobra.Command{Hidden: true})
	cmd.SilenceUsage = true
	return cmd
}

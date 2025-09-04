package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/kr/text"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
	csctlv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"

	"cerebras.com/cluster/csctl/pkg"
)

const (
	sessionHelpMessage = "Run 'csctl session --help' for usage"
	sessionNotAllowMsg = "permission denied, session %s api is only allowed for cluster admin usage"

	DefaultNoneValue = "*None*"
	K8sLabelPrefix   = "labels.k8s.cerebras.com/"
)

type sessionSetResourceSpecFlags struct {
	SystemCount            uint32
	LargeMemoryRackCount   uint32
	XLargeMemoryRackCount  uint32
	ParallelCompileCount   uint32
	ParallelExecuteCount   uint32
	RedundancySessions     string
	Redundant              bool
	SuppressAffinityErrors bool
	WorkloadType           string
	Labels                 string
}

func (f *sessionSetResourceSpecFlags) bind(cmd *cobra.Command, createMode bool) {
	cmd.Flags().Uint32Var(&f.SystemCount, "system-count", 0, "System count needed")
	cmd.Flags().Uint32Var(&f.ParallelExecuteCount, "parallel-execute-count", 0,
		"Optional concurrent executes needed, will affect granted coordinator nodes and populated nodegroups.")
	cmd.Flags().Uint32Var(&f.ParallelCompileCount, "parallel-compile-count", 0,
		"Optional concurrent compiles needed, will affect granted coordinator nodes. "+
			"The system releases held coordinator nodes if current parallel-compile-count is substantially greater than requested")
	cmd.Flags().Uint32Var(&f.LargeMemoryRackCount, "large-memory-rack-count", 0, "Optional large memory (1TB) racks for large models")
	cmd.Flags().Uint32Var(&f.XLargeMemoryRackCount, "xlarge-memory-rack-count", 0, "Optional extra large memory (2.3TB) racks for extra large models")
	cmd.Flags().BoolVarP(&f.SuppressAffinityErrors, "suppress-affinity-errors", "", false,
		"Ignore errors from CS port affinity requirements")
	if createMode {
		cmd.Flags().BoolVar(&f.Redundant, "redundant", false, "Enable redundant mode for new created session")
		cmd.Flags().StringVar(&f.WorkloadType, "workload-type", "", "Workload type of the session")
	} else {
		cmd.Flags().StringVar(&f.RedundancySessions, "redundancy-sessions", DefaultNoneValue,
			"Optional redundancy sessions to provide fault tolerance in case of session resources went to error")
	}
	cmd.Flags().StringVar(&f.Labels, "labels", "", "Optional comma separated list of labels to apply. a=b,c=,d-,e will set a=b, c='', e='' and remove the label d")
}

func (f *sessionSetResourceSpecFlags) toSessionSpec() *pb.SessionSpec {
	return &pb.SessionSpec{
		UpdateMode: pb.SessionUpdateMode_SET_RESOURCE_SPEC,
		UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: &pb.SessionResourceSpec{
			SystemCount:           f.SystemCount,
			LargeMemoryRackCount:  f.LargeMemoryRackCount,
			XlargeMemoryRackCount: f.XLargeMemoryRackCount,
			ParallelExecuteCount:  f.ParallelExecuteCount,
			ParallelCompileCount:  f.ParallelCompileCount,
		},
		},
		SuppressAffinityErrors: f.SuppressAffinityErrors,
	}
}

type sessionDebugUpdateFlags struct {
	UpdateMode                  string
	SystemNameStr               string
	NodegroupNameStr            string
	CoordinatorNodeNameStr      string
	ActivationNodeNameStr       string
	AutoSetActivationNodes      bool
	SuppressAffinityErrors      bool
	InferenceDriverNodeNameStr  string
	AutoSetInferenceDriverNodes bool
	ManagementNodeNameStr       string

	// note that ax nodes have an advanced option to have operator automatically finds the best ax nodes given session capacity
	// this advanced option by default is turned off
}

func (f *sessionDebugUpdateFlags) bind(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&f.UpdateMode, "mode", "m", "set", "Update mode: append, remove, remove-all, or set, default as set")
	cmd.Flags().StringVarP(&f.SystemNameStr, "system-names", "", DefaultNoneValue, "System names to add/remove")
	cmd.Flags().StringVarP(&f.NodegroupNameStr, "nodegroup-names", "", DefaultNoneValue, "Nodegroup names to add/remove")
	cmd.Flags().StringVarP(&f.CoordinatorNodeNameStr, "coordinator-node-names", "", DefaultNoneValue, "Coordinator node names to add/remove")
	cmd.Flags().StringVarP(&f.ActivationNodeNameStr, "activation-node-names", "", DefaultNoneValue, "Activation node names to add/remove")
	cmd.Flags().BoolVarP(&f.AutoSetActivationNodes, "auto-set-activation-nodes", "", true,
		"Automatically select activation nodes to match systems with best affinity coverage in respective resource stamps")
	cmd.Flags().BoolVarP(&f.SuppressAffinityErrors, "suppress-affinity-errors", "", false,
		"Ignore errors from CS port affinity requirements")
	cmd.Flags().StringVarP(&f.InferenceDriverNodeNameStr, "inference-driver-node-names", "", DefaultNoneValue, "Inference driver node names to add/remove")
	cmd.Flags().BoolVarP(&f.AutoSetInferenceDriverNodes, "auto-set-inference-driver-nodes", "", false,
		"Automatically select inference driver node in the possible resource stamp with the most systems")
	// TODO: Remove in rel-2.5
	cmd.Flags().StringVarP(&f.ManagementNodeNameStr, "management-node-names", "", DefaultNoneValue, "Management node names to add/remove")
	cmd.Flags().MarkHidden("management-node-names")
}

func (f *sessionDebugUpdateFlags) toSessionSpec() (*pb.SessionSpec, error) {
	var mode pb.SessionUpdateMode
	var systemNames []string
	var nodegroupNames []string
	var coordNodeNames []string
	var axNodeNames []string
	var ixNodeNames []string
	// use string and parsing to slice explicitly to allow set mode to clean up existing assignment
	// e.g.
	// session debug-update test --mode=set --system-names="s1" --nodegroup-names="" will remove existing nodegroups
	// session debug-update test --mode=set --system-names="s1" will keep existing nodegroups
	if f.SystemNameStr != DefaultNoneValue {
		val := strings.ReplaceAll(f.SystemNameStr, " ", "")
		if val != "" || f.UpdateMode == "set" {
			systemNames = strings.Split(val, ",")
		}
	}
	if f.NodegroupNameStr != DefaultNoneValue {
		val := strings.ReplaceAll(f.NodegroupNameStr, " ", "")
		if val != "" || f.UpdateMode == "set" {
			nodegroupNames = strings.Split(val, ",")
		}
	}
	if f.CoordinatorNodeNameStr != DefaultNoneValue {
		val := strings.ReplaceAll(f.CoordinatorNodeNameStr, " ", "")
		if val != "" || f.UpdateMode == "set" {
			coordNodeNames = strings.Split(val, ",")
		}
	} else if f.ManagementNodeNameStr != DefaultNoneValue {
		val := strings.ReplaceAll(f.ManagementNodeNameStr, " ", "")
		if val != "" || f.UpdateMode == "set" {
			coordNodeNames = strings.Split(val, ",")
		}
	}
	if f.ActivationNodeNameStr != DefaultNoneValue {
		val := strings.ReplaceAll(f.ActivationNodeNameStr, " ", "")
		if val != "" || f.UpdateMode == "set" {
			axNodeNames = strings.Split(val, ",")
		}
	}
	if f.InferenceDriverNodeNameStr != DefaultNoneValue {
		val := strings.ReplaceAll(f.InferenceDriverNodeNameStr, " ", "")
		if val != "" || f.UpdateMode == "set" {
			ixNodeNames = strings.Split(val, ",")
		}
	}

	if len(axNodeNames) > 0 && f.AutoSetActivationNodes {
		return nil, fmt.Errorf("please specify either the '--activation-node-names' or '--auto-set-activation-nodes' option (note --auto-set-activation-nodes is enabled by default, set --auto-set-activation-nodes=false to set activation node names)")
	}

	if len(ixNodeNames) > 0 && f.AutoSetInferenceDriverNodes {
		return nil, fmt.Errorf("please specify either the '--inference-driver-node-names' or '--auto-set-inference-driver-nodes' option")
	}

	noSessionResChange := len(systemNames) == 0 && len(coordNodeNames) == 0 &&
		len(axNodeNames) == 0 && len(ixNodeNames) == 0 && len(nodegroupNames) == 0 &&
		len(nodegroupNames) == 0
	switch f.UpdateMode {
	case "append":
		mode = pb.SessionUpdateMode_DEBUG_APPEND
		if noSessionResChange {
			return nil, fmt.Errorf("can't append with empty resources, please specify the resource names")
		}
	case "remove":
		mode = pb.SessionUpdateMode_DEBUG_REMOVE
		if noSessionResChange {
			return nil, fmt.Errorf("can't remove with empty resources, please specify the resource names")
		}
	case "remove-all":
		mode = pb.SessionUpdateMode_DEBUG_REMOVE_ALL
		if !noSessionResChange {
			return nil, fmt.Errorf("remove-all mode will remove all assigned resources, please don't set the resource names")
		}
	case "set":
		mode = pb.SessionUpdateMode_DEBUG_SET
		if noSessionResChange {
			return nil, fmt.Errorf("can't update/set with empty resources, please specify the resource names")
		}
	default:
		return nil, fmt.Errorf("unrecognized mode '%s'. valid modes: append, remove, remove-all, set", f.UpdateMode)
	}
	return &pb.SessionSpec{
		UpdateMode: mode,
		UpdateParams: &pb.SessionSpec_Resources{Resources: &pb.SessionResources{
			Systems:                        systemNames,
			Nodegroups:                     nodegroupNames,
			ActivationNodes:                axNodeNames,
			AutoSelectActivationNodes:      f.AutoSetActivationNodes,
			InferenceDriverNodes:           ixNodeNames,
			AutoSelectInferenceDriverNodes: f.AutoSetInferenceDriverNodes,
			CoordinatorNodes:               coordNodeNames,
			// Only for backward compatibility handling - newer csctl + older cluster server
			// TODO: Deprecate in rel-2.6
			ManagementNodes: coordNodeNames,
		}},
		SuppressAffinityErrors: f.SuppressAffinityErrors,
	}, nil
}

type sessionCmdCtx struct {
	cmdCtx *pkg.CmdCtx

	Output string // universal output flag -o/--output

	outErr OutErr
	client pb.CsAdmV1Client
}

func (s *sessionCmdCtx) bind(cmd *cobra.Command) {
	cmd.PreRunE = s.preRunE
	cmd.Flags().StringVarP(&s.Output, "output", "o", "table", "Output format. One of table, json, or yaml.")
	cmd.SilenceUsage = true
}

func (s *sessionCmdCtx) preRunE(cmd *cobra.Command, args []string) error {
	if s.Output != "table" && s.Output != "wide" && s.Output != "json" && s.Output != "yaml" {
		return fmt.Errorf(
			"unable to find appropriate output handler for given output '%s', options: table, wide, json, yaml",
			s.Output)
	}
	s.outErr = CmdOutErr{cmd: cmd}

	// all session sub cmds require the client so instantiate it here
	client, err := s.cmdCtx.ClientFactory.NewCsAdmV1(s.cmdCtx.Context)
	s.client = client
	return err
}

func (s *sessionCmdCtx) handleProtoOutput(fn func() (proto.Message, error)) error {
	res, err := fn()
	if err != nil {
		return pkg.FormatGrpcError(err)
	}
	switch s.Output {
	case "table":
		return sessionAsTable(res, false, s.outErr)
	case "wide":
		return sessionAsTable(res, true, s.outErr)
	case "json":
		return AsJson(res, s.outErr)
	case "yaml":
		return AsYaml(res, s.outErr)
	}
	return nil
}

func newSessionCreateCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	sctx := &sessionCmdCtx{cmdCtx: cmdCtx}
	flags := &sessionSetResourceSpecFlags{}
	cmd := &cobra.Command{
		Use:   "create NAME",
		Short: "create a new session",
		Example: text.Indent(`# create session
csctl session create qa-test \
  --system-count=4 
  --large-memory-rack-count=1 \
  --xlarge-memory-rack-count=1 \
  --parallel-compile-count=3 \
  --parallel-execute-count=2

# create an empty session
csctl session create qa-test

# create a compile-only session
csctl session create qa-test --parallel-compile-count 2

# create a session with specified workload type
csctl session create qa-test --workload-type inference

# create a session with labels
csctl session create qa-test --labels no-val=,key=value
`, "  "),
		Args: pkg.RequireNameArg(sessionHelpMessage),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check if user has permission
			if !cmdCtx.OnMgmtNode {
				return fmt.Errorf(sessionNotAllowMsg, "create")
			}

			req := &pb.CreateSessionRequest{
				Name: args[0],
				Spec: flags.toSessionSpec(),
			}
			if flags.Redundant {
				req.Properties = map[string]string{
					namespacev1.IsRedundantModeKey: "true",
				}
			}
			validWorkloadTypes := []string{
				namespacev1.WorkloadTypeNotSet.String(),
				namespacev1.InferenceWorkloadType.String(),
				namespacev1.TrainingWorkloadType.String(),
				namespacev1.SDKWorkloadType.String(),
			}
			typeFound := false
			for _, workloadType := range validWorkloadTypes {
				if flags.WorkloadType == workloadType {
					typeFound = true
				}
			}
			if !typeFound {
				validWorkloadTypes = validWorkloadTypes[1:] // do not print out empty string
				return fmt.Errorf("invalid workload type. Valid options are %v", validWorkloadTypes)
			}
			if flags.WorkloadType != namespacev1.WorkloadTypeNotSet.String() {
				if req.Properties == nil {
					req.Properties = make(map[string]string)
				}
				req.Properties[namespacev1.SessionWorkloadTypeKey] = flags.WorkloadType
			}
			if flags.Labels != "" {
				labelsToUpsert, labelsToRemove, err := parseLabels(strings.Split(flags.Labels, ","))
				if err != nil {
					return err
				}
				req.UserLabelOperations = labelsToOperations(labelsToUpsert, labelsToRemove)
			}
			return sctx.handleProtoOutput(func() (proto.Message, error) {
				return sctx.client.CreateSession(cmdCtx.Context, req)
			})
		},
	}
	sctx.bind(cmd)
	flags.bind(cmd, true)
	return cmd
}

func newSessionUpdateCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	sctx := &sessionCmdCtx{cmdCtx: cmdCtx}
	noConfirmation := false
	flags := &sessionSetResourceSpecFlags{}
	cmd := &cobra.Command{
		Use:   "update NAME",
		Short: "update an existing session",
		Long: `Sets the session's desired capabilities, completely replacing the previous request with the updated request.
The arguments passed in to session update are minimums so the appliance may grant more resources than requested.`,
		Example: text.Indent(`# update session
csctl session update qa-test \
  --system-count=4 \
  --large-memory-rack-count=1 \
  --xlarge-memory-rack-count=1 \
  --parallel-compile-count=3 \
  --parallel-execute-count=3

# update redundancy sessions, note: redundancy can't be updated together with other session level resources
csctl session update qa-test --redundancy-sessions=session1,session2

# remove redundancy sessions, note: redundancy can't be updated together with other session level resources
csctl session update qa-test --redundancy-sessions=""

# release all the session's resources without a confirmation prompt
csctl session update qa-test --system-count=0 -y

# update labels on a session
csctl session update qa-test --labels no-val=,key=value,remove-`, "  "),
		Args: pkg.RequireNameArg(sessionHelpMessage),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Add permission check
			if !cmdCtx.OnMgmtNode {
				return fmt.Errorf(sessionNotAllowMsg, "update")
			}

			spec := flags.toSessionSpec()
			if val := flags.RedundancySessions; val != DefaultNoneValue {
				return singleFlagUpdateOnly("redundancy-sessions", cmd, sctx, cmdCtx, &pb.UpdateSessionRequest{
					Name: args[0],
					Spec: spec,
					Properties: map[string]string{
						namespacev1.AssignedRedundancyKey: strings.ReplaceAll(val, " ", ""),
					},
				})
			}

			if val := flags.Labels; val != "" {
				labelsToUpsert, labelsToRemove, err := parseLabels(strings.Split(flags.Labels, ","))
				if err != nil {
					return err
				}

				return singleFlagUpdateOnly("labels", cmd, sctx, cmdCtx, &pb.UpdateSessionRequest{
					Name:                args[0],
					Spec:                spec,
					UserLabelOperations: labelsToOperations(labelsToUpsert, labelsToRemove),
				})
			}

			if noConfirmation {
				req := &pb.UpdateSessionRequest{
					Name: args[0],
					Spec: spec,
				}
				return sctx.handleProtoOutput(func() (proto.Message, error) {
					return sctx.client.UpdateSession(cmdCtx.Context, req)
				})
			}
			spec.UpdateMode = pb.SessionUpdateMode_SET_RESOURCE_SPEC_DRYRUN
			req := &pb.UpdateSessionRequest{Name: args[0], Spec: spec}
			dryrunRes, err := sctx.client.UpdateSession(cmdCtx.Context, req)

			if err != nil {
				return pkg.FormatGrpcError(err)
			}
			if msg, err := summarizeDryRunResult(dryrunRes); err != nil {
				return err
			} else if msg == "" {
				_, _ = fmt.Fprintln(sctx.outErr.Out(), "No resource changes will occur as a result of this request. Nothing to do")
				return nil
			} else {
				_, _ = fmt.Fprint(sctx.outErr.Out(), msg)
			}
			if pkg.GetUserConfirm(cmd.InOrStdin(), sctx.outErr.Err()) {
				update := &pb.SessionSpec_Resources{Resources: dryrunRes.State.UpdateHistory[0].Spec.GetResources()}
				// explicitly assign empty string to remove existing assignment if necessary
				if len(update.Resources.Systems) == 0 {
					update.Resources.Systems = []string{""}
				}
				if len(update.Resources.Nodegroups) == 0 {
					update.Resources.Nodegroups = []string{""}
				}
				if len(update.Resources.CoordinatorNodes) == 0 {
					update.Resources.CoordinatorNodes = []string{""}
				}
				if len(update.Resources.ActivationNodes) == 0 {
					update.Resources.ActivationNodes = []string{""}
				}
				if len(update.Resources.InferenceDriverNodes) == 0 {
					update.Resources.InferenceDriverNodes = []string{""}
				}
				req = &pb.UpdateSessionRequest{
					Name: args[0],
					Spec: &pb.SessionSpec{
						UpdateMode:             pb.SessionUpdateMode_DEBUG_SET,
						UpdateParams:           update,
						SuppressAffinityErrors: spec.SuppressAffinityErrors,
					},
				}
				return sctx.handleProtoOutput(func() (proto.Message, error) {
					return sctx.client.UpdateSession(cmdCtx.Context, req)
				})
			}
			return nil
		},
	}
	sctx.bind(cmd)
	flags.bind(cmd, false)
	cmd.Flags().BoolVarP(&noConfirmation, "no-confirm", "y", false, "Skip confirmation prompt and immediately apply the change")
	return cmd
}

// returns a string containing a message for end user. If string is empty, there were no changes and dry run should be
// aborted. Error if server response was malformed
func summarizeDryRunResult(res *pb.Session) (string, error) {
	if len(res.State.UpdateHistory) != 2 || res.State.UpdateHistory[1].Spec.GetResourceSpec() == nil || res.State.UpdateHistory[0].Spec.GetResources() == nil {
		return "", fmt.Errorf("server returned an invalid dry run response, aborting")
	}
	rsBefore := res.State.Resources
	capBefore := res.State.GetCurrentResourceSpec()
	rsAfter := res.State.UpdateHistory[0].Spec.GetResources()
	capAfter := res.State.UpdateHistory[1].Spec.GetResourceSpec()

	var capChange []string
	summarizeRsSpecDiff := func(capability string, before, after uint32) {
		if before > after {
			capChange = append(capChange, fmt.Sprintf("%s decreases from %d to %d", capability, before, after))
		} else if before < after {
			capChange = append(capChange, fmt.Sprintf("%s increases from %d to %d", capability, before, after))
		}
	}
	summarizeRsSpecDiff("parallel compile limit", capBefore.ParallelCompileCount, capAfter.ParallelCompileCount)
	summarizeRsSpecDiff("parallel execute limit", capBefore.ParallelExecuteCount, capAfter.ParallelExecuteCount)
	summarizeRsSpecDiff("large memory rack count", capBefore.LargeMemoryRackCount, capAfter.LargeMemoryRackCount)
	summarizeRsSpecDiff("xlarge memory rack count", capBefore.XlargeMemoryRackCount, capAfter.XlargeMemoryRackCount)

	msg := ""
	if len(capChange) > 0 {
		msg = fmt.Sprintf("Update will result in the estimated resource capabilities changes:\n  %s", strings.Join(capChange, "\n  "))
	}

	var rsChange []string
	summarizeRsDiff := func(rtype string, before, after []string) int {
		removed, added := pkg.ArrayDiff(before, after)
		if len(added) > 0 {
			rsChange = append(rsChange, fmt.Sprintf("%d %ss added: %s", len(added), rtype, strings.Join(added, ",")))
		}
		if len(removed) > 0 {
			rsChange = append(rsChange, fmt.Sprintf("%d %ss removed: %s", len(removed), rtype, strings.Join(removed, ",")))
		}
		return len(added) + len(removed)
	}

	changes := 0
	changes += summarizeRsDiff("system", rsBefore.GetSystems(), rsAfter.Systems)
	changes += summarizeRsDiff("nodegroup", rsBefore.GetNodegroups(), rsAfter.Nodegroups)
	changes += summarizeRsDiff("coordinator node", rsBefore.GetCoordinatorNodes(), rsAfter.CoordinatorNodes)
	changes += summarizeRsDiff("activation node", rsBefore.GetActivationNodes(), rsAfter.ActivationNodes)
	changes += summarizeRsDiff("inference driver node", rsBefore.GetInferenceDriverNodes(), rsAfter.InferenceDriverNodes)
	if changes > 0 {
		return fmt.Sprintf("%s\nUpdate will result in the following %d resource changes:\n  %s\n", msg, changes, strings.Join(rsChange, "\n  ")), nil
	}
	return msg, nil
}

func singleFlagUpdateOnly(flagName string, cmd *cobra.Command, sctx *sessionCmdCtx, cmdCtx *pkg.CmdCtx, request *pb.UpdateSessionRequest) error {
	var updatedFlag string
	cmd.Flags().Visit(func(flag *pflag.Flag) {
		if flag.Changed {
			if flag.Name != flagName {
				updatedFlag = flag.Name
			}
		}
	})
	if updatedFlag != "" {
		return fmt.Errorf("can't update '%s' while modifying other resource of '%s'", flagName, updatedFlag)
	}
	return sctx.handleProtoOutput(func() (proto.Message, error) {
		return sctx.client.UpdateSession(cmdCtx.Context, request)
	})
}

func newSessionDebugUpdateCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	sctx := &sessionCmdCtx{cmdCtx: cmdCtx}
	flags := &sessionDebugUpdateFlags{}
	cmd := &cobra.Command{
		Use:   "debug-update NAME",
		Short: "add/remove/remove-all/set specific resources on an existing session",
		Long: `Directly manipulates the resources in a session.

This command is intended for debugging purposes. It will not prevent updates resulting in
unusable sessions (e.g. only depopulated nodegroups).

There are 3 modes: append, remove, set. Append and remove incrementally change the
composition of the session. Set entirely replaces the resources in the session with
the given resources.
`,
		Example: text.Indent(`# append specific resources
csctl session debug-update -m append qa-test --coordinator-node-names coord-9 --activation-node-names ax-0

# remove specific resources
csctl session debug-update -m remove qa-test --system-names sys0,sys1

# remove all resources
csctl session debug-update -m remove-all qa-test

# set the exact resources in the session
csctl session debug-update -m set qa-test \
  --system-names sys0,sys1 \
  --nodegroup-names ng0 \
  --coordinator-node-names coord-9 \
  --activation-node-names ax-0
`, "  "),
		Args: pkg.RequireNameArg(sessionHelpMessage),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Add permission check
			if !cmdCtx.OnMgmtNode {
				return fmt.Errorf(sessionNotAllowMsg, "debug-update")
			}

			spec, err := flags.toSessionSpec()
			if err != nil {
				return err
			}
			req := &pb.UpdateSessionRequest{
				Name: args[0],
				Spec: spec,
			}
			return sctx.handleProtoOutput(func() (proto.Message, error) {
				return sctx.client.UpdateSession(cmdCtx.Context, req)
			})
		},
	}
	sctx.bind(cmd)
	flags.bind(cmd)
	return cmd
}

func newSessionDeleteCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	sctx := &sessionCmdCtx{cmdCtx: cmdCtx}
	delReq := &pb.DeleteSessionRequest{}

	cmd := &cobra.Command{
		Use:   "delete NAME",
		Short: "delete a session",
		Long: text.Indent(`Deletes the session record. Requires the session to
have no jobs executing at time of deletion.
`, "  "),
		Example: text.Indent(`# delete a session
csctl session delete qa-test

# scale down a session prior to delete
csctl session update qa-test --system-count 0 --parallel-compile-count 0

# force delete a session
csctl session delete qa-test -f`, "  "),
		Args: pkg.RequireNameArg(sessionHelpMessage),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Add permission check
			if !cmdCtx.OnMgmtNode {
				return fmt.Errorf(sessionNotAllowMsg, "delete")
			}

			delReq.Name = args[0]
			res, err := sctx.client.DeleteSession(cmdCtx.Context, delReq)
			if err != nil {
				return pkg.FormatGrpcError(err)
			}
			_, err = fmt.Fprintln(sctx.outErr.Out(), res.Message)
			return err
		},
	}
	sctx.bind(cmd)
	return cmd
}

func newSessionGetCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	sctx := &sessionCmdCtx{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "get NAME",
		Short: "get a session",
		Example: text.Indent(`# get session with additional runtime stats
csctl session get qa-test -owide

# get session with json format
csctl session get qa-test -ojson`, "  "),
		Args: pkg.RequireNameArg(sessionHelpMessage),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires one positional argument NAME. %s", sessionHelpMessage)
			}
			return sctx.handleProtoOutput(func() (proto.Message, error) {
				return sctx.client.GetSession(cmdCtx.Context, &pb.GetSessionRequest{Name: args[0]})
			})
		},
	}
	sctx.bind(cmd)
	return cmd
}

func newSessionListCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	sctx := &sessionCmdCtx{cmdCtx: cmdCtx}
	listReq := &pb.ListSessionRequest{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list all sessions",
		Args:  pkg.DisallowPosArg(sessionHelpMessage),
		Example: text.Indent(`# list sessions in yaml format (with unassigned at the end)
csctl session list -oyaml

# list unassigned resources only
csctl session list --unassigned-only`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Add permission check
			if !cmdCtx.OnMgmtNode {
				return fmt.Errorf(sessionNotAllowMsg, "list")
			}

			return sctx.handleProtoOutput(func() (proto.Message, error) {
				return sctx.client.ListSession(cmdCtx.Context, listReq)
			})
		},
	}
	sctx.bind(cmd)
	cmd.Flags().BoolVarP(&listReq.ShowUnassigned, "unassigned-only", "u", false, "Display a row of unassigned resources")
	cmd.Flags().BoolVar(&listReq.Redundant, "redundant", false, "List redundant mode sessions only")
	return cmd
}

func NewSessionCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	var longDesc string
	if cmdCtx.OnMgmtNode {
		longDesc = "Create/Update/Get/List/Delete sessions for resource reservation"
	} else {
		longDesc = "View session information (Get command only). Other session management commands require administrator privileges."
	}

	cmd := &cobra.Command{
		Use:   "session",
		Short: "Session management commands",
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	// Register all commands regardless of user type
	cmd.AddCommand(newSessionGetCmd(cmdCtx))
	cmd.AddCommand(newSessionCreateCmd(cmdCtx))
	cmd.AddCommand(newSessionUpdateCmd(cmdCtx))
	cmd.AddCommand(newSessionListCmd(cmdCtx))
	cmd.AddCommand(newSessionDeleteCmd(cmdCtx))
	cmd.AddCommand(newSessionDebugUpdateCmd(cmdCtx))

	return cmd
}

func sessionAsTable(msg proto.Message, wide bool, outErr OutErr) error {
	var items []*pb.Session
	if session, ok := msg.(*pb.Session); ok {
		items = []*pb.Session{session}
	} else if sessionList, ok := msg.(*pb.ListSessionResponse); ok {
		items = sessionList.Items
	} else {
		panic(fmt.Sprintf("unexpected proto message type passed to sessionAsTable: %v", msg))
	}
	headerCol := []*csctlv1.ColumnDefinition{
		{Name: "Name"},
		{Name: "Systems"},
		{Name: "Nodegroups"},
		{Name: "Crd-Nodes"},
		{Name: "Ax-Nodes"},
		{Name: "Ix-Nodes"},
		{Name: "Compiles"},
		{Name: "Executes"},
		{Name: "Lrg-Mem-Racks"},
		{Name: "XLrg-Mem-Racks"},
		{Name: "Redundancy"},
		{Name: "Last-Active"},
		{Name: "Labels"},
		{Name: "Workload-Type"},
	}
	if wide {
		headerCol = append(headerCol, []*csctlv1.ColumnDefinition{
			{Name: "JobsRunning"},
			{Name: "JobsQueuing"},
		}...)
	}
	var warningNotes []string
	var axWarnings []string
	totalAxNodes := 0
	table := &csctlv1.Table{Columns: headerCol}
	var cells []string
	for _, r := range items {
		for k, v := range r.Properties {
			// print annotations with prefix SessionWarningAnnoKeyPrefix as warning
			if strings.HasPrefix(k, namespacev1.SessionWarningAnnoKeyPrefix) {
				axWarnings = append(axWarnings, v)
			}
		}
		rs := r.State.GetResources()
		if rs == nil {
			rs = &pb.SessionResources{}
		}
		spec := r.State.GetCurrentResourceSpec()
		if spec == nil {
			spec = &pb.SessionResourceSpec{}
		}
		status := r.Status
		if status == nil {
			status = &pb.RuntimeStatus{}
		}
		coordNodes := rs.CoordinatorNodes
		if len(coordNodes) == 0 {
			coordNodes = rs.ManagementNodes
		}
		redundancyStatus := r.Properties[namespacev1.IsRedundantModeKey]
		if redundancyStatus == "true" {
			redundancyStatus = "*redundant"
		} else if r.Properties[namespacev1.AssignedRedundancyKey] != "" {
			redundancyStatus = "[" + arrayCol(strings.Split(r.Properties[namespacev1.AssignedRedundancyKey],
				namespacev1.LabelSeparator), 60) + "]"
		}
		var labels []string
		var workloadType string

		for k, v := range r.Properties {
			// User labels added via `csctl session` will have the K8SLabelPrefix
			if strings.HasPrefix(k, K8sLabelPrefix) {
				labels = append(labels, fmt.Sprintf("%s=%s", k[len(K8sLabelPrefix):], v))
			}
			if k == namespacev1.SessionWorkloadTypeKey {
				workloadType = v
			}
		}
		sort.Strings(labels)
		cells = []string{
			r.Name,
			arrayColWithCount(rs.Systems, 60, wide),
			arrayColWithCount(rs.Nodegroups, 60, wide),
			arrayColWithCount(coordNodes, 60, wide),
			arrayColWithCount(rs.ActivationNodes, 60, wide),
			arrayColWithCount(rs.InferenceDriverNodes, 60, wide),
			strconv.Itoa(int(spec.ParallelCompileCount)),
			strconv.Itoa(int(spec.ParallelExecuteCount)),
			strconv.Itoa(int(spec.LargeMemoryRackCount)),
			strconv.Itoa(int(spec.XlargeMemoryRackCount)),
			redundancyStatus,
			status.InactiveDuration,
			strings.Join(labels, ","),
			workloadType,
		}
		if wide {
			cells = append(cells, []string{
				strconv.Itoa(int(status.RunningJobCount)),
				strconv.Itoa(int(status.QueueingJobCount)),
			}...)
		}
		table.Rows = append(table.Rows, &csctlv1.RowData{Cells: cells})

		totalAxNodes += len(rs.ActivationNodes)
		if r.Name != pkg.SystemNamespace && r.Name != pkg.UnassignedNamespace && len(rs.Systems) != len(rs.ActivationNodes) {
			action := ""
			if len(rs.ActivationNodes) == 0 {
				action = fmt.Sprintf("please consider moving %d activation node(s) into the session, "+
					"otherwise no job can be launched.", len(rs.Systems))
			} else if len(rs.Systems) == 0 {
				action = fmt.Sprintf("please consider moving %d system(s) into the session, "+
					"otherwise no job can be launched.", len(rs.ActivationNodes))
			} else if len(rs.Systems) > len(rs.ActivationNodes) {
				action = fmt.Sprintf("please consider moving %d more activation node(s) into the session, "+
					"otherwise performance may be degraded.", len(rs.Systems)-len(rs.ActivationNodes))
			} else {
				action = fmt.Sprintf("please consider moving %d activation node(s) out of the session, "+
					"since only %d activation node(s) will get utilized in the sesion.", len(rs.ActivationNodes)-len(rs.Systems), len(rs.Systems))
			}
			axWarnings = append(axWarnings, fmt.Sprintf("- Session %s has %d system(s) but %d activation node(s), %s",
				r.Name, len(rs.Systems), len(rs.ActivationNodes), action))
		}
	}
	if totalAxNodes > 0 && len(axWarnings) > 0 {
		sort.Strings(axWarnings)
		warningNotes = append(warningNotes, axWarnings...)
	}
	if len(warningNotes) > 0 {
		warningNotes = append([]string{"", "Warnings:"}, warningNotes...)
	}
	// print detailed port affinity only when getting a specific session
	if _, ok := msg.(*pb.Session); ok {
		var detailedAffinity []string
		for k, v := range items[0].Properties {
			if strings.HasPrefix(k, namespacev1.SessionPortAffinityAnnoKeyPrefix) {
				detailedAffinity = append(detailedAffinity, v)
			}
		}
		if len(detailedAffinity) > 0 {
			sort.Strings(detailedAffinity)
			// prepend detailed affinity before warnings
			detailedAffinity = append([]string{"", "AX Port Affinity:"}, detailedAffinity...)
			warningNotes = append(detailedAffinity, warningNotes...)
		}
	}

	return DisplayTable(table, warningNotes, outErr)
}

func labelsToOperations(labelsToUpsert map[string]string, labelsToRemove []string) []*pb.UserLabelOperation {
	operations := []*pb.UserLabelOperation{}
	for key, val := range labelsToUpsert {
		operations = append(operations, &pb.UserLabelOperation{
			Op:    pb.UserLabelOperation_UPSERT,
			Key:   key,
			Value: val,
		})
	}

	for _, key := range labelsToRemove {
		operations = append(operations, &pb.UserLabelOperation{
			Op:  pb.UserLabelOperation_REMOVE,
			Key: key,
		})
	}
	return operations
}

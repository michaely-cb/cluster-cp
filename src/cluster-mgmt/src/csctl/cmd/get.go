package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kr/text"
	"github.com/spf13/cobra"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/csctl/pkg"
)

type GetCmdOptions struct {
	Type string
	Name string

	// Output format. One of table, json. Empty defaults to table.
	Output     string
	DebugLevel int

	// matching labels: comma-separated key-value pairs in the format of 'key=value'.
	LabelsString string
	// map for key-value pairs after parsing matching labels string.
	Labels map[string]string

	// Job selection options
	Workflow           string
	GetCurrentUserJobs bool
	GetAllStates       bool
	Limit              int32
	SortBy             string
	OrderBy            string

	// Cluster filter options
	SystemName              string
	SystemOnly              bool
	NodeOnly                bool
	ErrorOnly               bool
	Role                    string
	PlatformVersion         string
	PlatformVersionNotEqual string

	cmdCtx *pkg.CmdCtx
	outErr OutErr
}

func NewGetCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	options := GetCmdOptions{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "get TYPE [NAME] [-o json|yaml] [-l labels]",
		Short: "Get resources",
		Long: `Get resources of a given type.

The types which the server supports can be discovered using the 'csctl types' command.`,
		Example: text.Indent(`# list jobs
csctl get jobs

# list jobs from a specific namespace/workflow
csctl get jobs --namespace perf-test [--workflow wf-test]

# list jobs with matching labels (comma-separated key-value pairs)
# return jobs with labels of 'key1=value1'
csctl get jobs -l key1=value1

# list jobs with matching labels (comma-separated key-value pairs)
# return jobs with labels of 'key1=value1 AND key2=value2'
csctl get jobs -l key1=value1,key2=value2

# get a specific job with json output
csctl get job wsjob-0001 -ojson

# get a specific job in yaml with extra debug fields displayed
csctl -d1 get job wsjob-0001 -oyaml

# get a specific job from a specific namespace
csctl get job wsjob-0001 --namespace perf-test

# list volumes
csctl get volumes

# get cluster status - all nodes/systems status, cpu(5min avg), mem...
csctl get cluster

# get cluster status - show error nodes/systems only

csctl get cluster --error-only

# get node status - all nodes status, cpu(5min avg), mem...
csctl get node

# get system status - all systems status...
csctl get system

# get cluster node groups

csctl get nodegroup

# get version - show installed software versions

csctl get version`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			// convert not equal version format
			if options.PlatformVersionNotEqual != "" {
				options.PlatformVersion = "!" + options.PlatformVersionNotEqual
			}
			if err := options.Complete(cmd, cmdCtx, args); err != nil {
				return err
			}

			return options.Run(cmdCtx.Context)
		},
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&options.Output, "output", "o", "table", "Output format. One of table, json, or yaml.")
	cmd.Flags().StringVarP(&options.LabelsString, "labels", "l", "", "Resource matching labels, comma-separated key-value pairs in format of 'key=value'.")
	cmd.Flags().BoolVarP(&options.GetCurrentUserJobs, "my-jobs", "m", false, "Only show my jobs")
	cmd.Flags().BoolVarP(&options.GetAllStates, "all-states", "a", false, "Show cluster/job in all states")
	cmd.Flags().BoolVarP(&options.SystemOnly, "system-only", "", false, "Show only systems in cluster status output")
	cmd.Flags().BoolVarP(&options.NodeOnly, "node-only", "", false, "Show only nodes in cluster status output")
	cmd.Flags().BoolVarP(&options.ErrorOnly, "error-only", "e", false, "Show only error nodes/systems in cluster status output")
	cmd.Flags().StringVar(&options.SystemName, "system", "", "Filter systems by specific name in output")
	cmd.Flags().StringVarP(&options.Workflow, "workflow", "", "", "Filter jobs by specific workflow id")
	cmd.Flags().StringVarP(&options.Role, "role", "", "", "Show nodes/systems with specified role only, e.g management/coordinator/swarmx/memoryx/activation/worker")
	cmd.Flags().StringVarP(&options.PlatformVersion, "version", "", "", "Show nodes/systems with specified platform version, e.g. version=v1 or version!=v1")
	cmd.Flags().StringVarP(&options.PlatformVersionNotEqual, "version!", "", "", "Show nodes/systems without specified platform version, e.g. version!=v1")
	cmd.Flags().Int32VarP(&options.Limit, "max-jobs", "", 5000, "Maximum number of results to return (0 means no limit).")
	cmd.Flags().StringVar(&options.SortBy, "sort", "priority",
		"Sort criteria of returned jobs, e.g. priority/age (default is scheduling priority based on combination of queue/job priority/age, age is the duration since job created)")
	cmd.Flags().StringVar(&options.OrderBy, "order", "desc", "Sort order of returned jobs, e.g. desc/asc (default is desc, highest/oldest first)")
	cmd.Flags().Lookup("version!").Hidden = true

	return SetDefaults(cmd)
}

func extractLabels(labelsString string) (map[string]string, error) {
	labels := map[string]string{}
	if labelsString == "" {
		return labels, nil
	}

	results := strings.Split(labelsString, ",")
	for _, result := range results {
		keyvalues := strings.Split(result, "=")
		if len(keyvalues) != 2 {
			return nil, fmt.Errorf(
				"unexpected labels: '%s' (should be comma-separated key-value pairs with the format of 'key=value'",
				labelsString)
		} else {
			labels[strings.TrimSpace(keyvalues[0])] = strings.TrimSpace(keyvalues[1])
		}
	}
	return labels, nil
}

func (c *GetCmdOptions) Complete(cmd *cobra.Command, cmdCtx *pkg.CmdCtx, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing required argument TYPE. Run 'csctl types' to see available resource types")
	}
	c.Type = args[0]
	if len(args) > 1 {
		c.Name = args[1]
	}

	var err error
	if c.Labels, err = extractLabels(c.LabelsString); err != nil {
		return err
	}

	if c.SortBy != "age" && c.SortBy != "priority" {
		return fmt.Errorf("invalid sort type: %s, allowed types: [priority, age]", c.SortBy)
	}

	if c.OrderBy != "asc" && c.OrderBy != "desc" {
		return fmt.Errorf("invalid order type: %s, allowed types: [desc, asc]", c.OrderBy)
	}

	if _, ok := outputHandler[c.Output]; !ok {
		return fmt.Errorf(
			"unable to find appropriate output handler for given output '%s', options: %v",
			c.Output, pkg.Keys(outputHandler))
	}

	c.outErr = CmdOutErr{cmd: cmd}
	c.DebugLevel = cmdCtx.DebugLevel
	return nil
}

func (c *GetCmdOptions) toListOptionsForJob() *pb.ListOptions {
	listJobOptions := &pb.ListJobOptions{
		LabelFilters:    c.Labels,
		AllStates:       c.GetAllStates,
		Limit:           c.Limit,
		WorkflowId:      c.Workflow,
		CurrentUserOnly: c.GetCurrentUserJobs,
	}
	listOptions := &pb.ListOptions{
		Option:  &pb.ListOptions_JobOptions{JobOptions: listJobOptions},
		SortBy:  c.SortBy,
		OrderBy: c.OrderBy,
	}
	return listOptions
}

func (c *GetCmdOptions) Run(ctx context.Context) error {
	var client pb.CsCtlV1Client
	var err error
	if client, err = c.cmdCtx.ClientFactory.NewCsCtlV1(ctx); err != nil {
		return err
	}

	accept := pb.SerializationMethod_JSON_METHOD
	representation := pb.ObjectRepresentation_OBJECT_REPRESENTATION
	if c.Output == "" || c.Output == "table" {
		accept = pb.SerializationMethod_PROTOBUF_METHOD
		representation = pb.ObjectRepresentation_TABLE_REPRESENTATION
	}

	// list streaming code path
	if c.Name == "" && (c.Type == "job" || c.Type == "jobs") {
		listJobRequest := &pb.ListJobRequest{
			Accept:         accept,
			Representation: representation,
			Options:        c.toListOptionsForJob(),
		}

		err = ListJobs(ctx, listJobRequest, client, c.outErr, c.Output, c.DebugLevel)
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.Unimplemented {
			return pkg.FormatGrpcError(err)
		} // else, fall back to non-streaming code path
	}

	getOptions := &pb.GetOptions{
		LabelFilters: c.Labels,
		Limit:        c.Limit,
		SortBy:       c.SortBy,
		OrderBy:      c.OrderBy,
	}
	getOptions.WorkflowId = c.Workflow
	getOptions.System = c.SystemName
	getOptions.SystemOnly = c.SystemOnly
	getOptions.NodeOnly = c.NodeOnly
	getOptions.ErrorOnly = c.ErrorOnly
	getOptions.AllStates = c.GetAllStates
	getOptions.Role = c.Role
	getOptions.Version = c.PlatformVersion
	getOptions.CurrentUserOnly = &c.GetCurrentUserJobs
	if c.GetCurrentUserJobs {
		getOptions.Uid = int64(os.Getuid())
	} else {
		// keep for backwards compatible
		getOptions.Uid = int64(-1)
	}

	req := &pb.GetRequest{
		Type:           c.Type,
		Name:           c.Name,
		Accept:         accept,
		Representation: representation,
		Options:        getOptions,
	}

	res, err := client.Get(ctx, req)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}

	if err = outputHandler[c.Output]([]GenericMessage{res}, c.outErr, c.DebugLevel); err != nil {
		_, err = fmt.Fprintln(c.outErr.Err(), err.Error())
		return err
	}

	return nil
}

package cmd

import (
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"github.com/kr/text"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"cerebras.com/cluster/csctl/pkg"
)

const (
	jobHelpMessage = "Run 'csctl job --help' for usage"
)

type jobCmdCtx struct {
	cmdCtx *pkg.CmdCtx

	// Output format. One of table, json. Empty defaults to table.
	Output string

	outErr OutErr
	client pb.CsCtlV1Client

	listOptions   jobListOptions
	modifyOptions jobModifyOptions
}

type jobListOptions struct {
	LabelsString       string
	GetCurrentUserJobs bool
	GetAllStates       bool

	// Maximum number of jobs to return (0 means no limit)
	Limit int32
}

type jobModifyOptions struct {
	Priority int
}

type JobPreRunE func(c *jobCmdCtx) pkg.PreRunE

func clientSetup(c *jobCmdCtx) pkg.PreRunE {
	return func(cmd *cobra.Command, args []string) error {
		// all job sub cmds require the client so instantiate it here
		client, err := c.cmdCtx.ClientFactory.NewCsCtlV1(c.cmdCtx.Context)
		if err != nil {
			return err
		}
		c.client = client
		c.outErr = CmdOutErr{cmd: cmd}
		return nil
	}
}

func expectRegularOutput(c *jobCmdCtx) pkg.PreRunE {
	return func(cmd *cobra.Command, args []string) error {
		if _, ok := outputHandler[c.Output]; !ok {
			return fmt.Errorf(
				"unable to find appropriate output handler for given output '%s', options: %v",
				c.Output, pkg.Keys(outputHandler))
		}
		return nil
	}
}

func chainedPreRunE(c *jobCmdCtx, setups ...JobPreRunE) pkg.PreRunE {
	return func(cmd *cobra.Command, args []string) error {
		for _, setup := range setups {
			if err := setup(c)(cmd, args); err != nil {
				return err
			}
		}
		return nil
	}
}

// ListJobs uses the streaming rpc call `ListJobs()` to list jobs.
// If cluster server does not have the streaming api implementation, the caller should fall back to use `Get()` rpc.
func ListJobs(ctx context.Context, request *pb.ListJobRequest, client pb.CsCtlV1Client, outErr OutErr, output string, debugLevel int) error {
	stream, err := client.ListJobs(ctx, request)
	if err != nil {
		return err
	}
	var messages []GenericMessage
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break // stream ended normally
		}
		if err != nil {
			// don't do FormatGrpcError() in this function, keep grpc error for caller to identify and fall back
			return err
		}
		messages = append(messages, resp)
	}

	if err = outputHandler[output](messages, outErr, debugLevel); err != nil {
		_, err = fmt.Fprintln(outErr.Err(), err.Error())
		return err
	}
	return nil
}

func (c *jobCmdCtx) Get(ctx context.Context, name string) error {
	accept := pb.SerializationMethod_JSON_METHOD
	representation := pb.ObjectRepresentation_OBJECT_REPRESENTATION
	if c.Output == "" || c.Output == "table" {
		accept = pb.SerializationMethod_PROTOBUF_METHOD
		representation = pb.ObjectRepresentation_TABLE_REPRESENTATION
	}

	// If name is empty, try to use streaming list API first
	if name == "" {
		labels, err := extractLabels(c.listOptions.LabelsString)
		if err != nil {
			return err
		}

		listJobOptions := &pb.ListJobOptions{
			LabelFilters:    labels,
			AllStates:       c.listOptions.GetAllStates,
			CurrentUserOnly: c.listOptions.GetCurrentUserJobs,
		}
		listOptions := &pb.ListOptions{
			Option: &pb.ListOptions_JobOptions{JobOptions: listJobOptions},
		}
		listJobRequest := &pb.ListJobRequest{
			Accept:         accept,
			Representation: representation,
			Options:        listOptions,
		}

		err = ListJobs(ctx, listJobRequest, c.client, c.outErr, c.Output, c.cmdCtx.DebugLevel)
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.Unimplemented {
			return pkg.FormatGrpcError(err)
		} // else, fall back to non-streaming code path
	}

	getOptions := &pb.GetOptions{}

	// If name is empty, it means the request is "list" instead of "get"
	if name == "" {
		labels, err := extractLabels(c.listOptions.LabelsString)
		if err != nil {
			return err
		}
		getOptions.LabelFilters = labels
		getOptions.AllStates = c.listOptions.GetAllStates
		getOptions.Limit = c.listOptions.Limit
		if c.listOptions.GetCurrentUserJobs {
			getOptions.Uid = int64(os.Getuid())
		} else {
			getOptions.Uid = int64(-1)
		}
	}

	req := &pb.GetRequest{
		Type:           "jobs",
		Name:           name,
		Accept:         accept,
		Representation: representation,
		Options:        getOptions,
	}

	res, err := c.client.Get(ctx, req)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}

	if err = outputHandler[c.Output]([]GenericMessage{res}, c.outErr, c.cmdCtx.DebugLevel); err != nil {
		_, err = fmt.Fprintln(c.outErr.Err(), err.Error())
		return err
	}

	return nil
}

func (c *jobCmdCtx) Patch(ctx context.Context, name string, body []byte) error {
	res, err := c.client.Patch(
		ctx,
		&pb.PatchRequest{
			Type:        "jobs",
			Name:        name,
			PatchType:   pb.PatchRequest_MERGE,
			ContentType: pb.SerializationMethod_JSON_METHOD,
			Accept:      pb.SerializationMethod_JSON_METHOD,
			Body:        body,
		},
	)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}
	if c.Output != "" {
		return outputHandler[c.Output]([]GenericMessage{res}, c.outErr, c.cmdCtx.DebugLevel)
	} else {
		fmt.Printf("%s/%s successfully updated its priority\n", c.cmdCtx.Namespace, name)
	}
	return nil
}

func (c *jobCmdCtx) SetPriority(ctx context.Context, name, priority string) error {
	res, err := c.client.UpdateJobPriority(
		ctx,
		&pb.UpdateJobPriorityRequest{
			JobId:       name,
			JobPriority: priority,
		},
	)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}
	_, err = fmt.Fprintln(c.outErr.Out(), res.Message)
	return err
}

func (c *jobCmdCtx) GetJobTopology(ctx context.Context, jobId string) error {
	req := &pb.GetJobTopologyRequest{
		JobId:        jobId,
		JobNamespace: c.cmdCtx.Namespace,
	}

	res, err := c.client.GetJobTopology(ctx, req)
	if err != nil {
		return pkg.FormatGrpcError(err)
	}
	return c.printTopologyTable(res.Connections)
}

// Helper method to print topology as a table using the same pattern as cluster topology
func (c *jobCmdCtx) printTopologyTable(connections []*pb.JobTopologyConnection) error {
	if len(connections) == 0 {
		fmt.Fprintln(c.outErr.Out(), "No topology connections found for this job")
		return nil
	}

	// Create table structure using the same pattern as cluster topology
	table := &csv1.Table{
		Columns: []*csv1.ColumnDefinition{
			{Name: "POD"},
			{Name: "NODE"},
			{Name: "NICS"},
			{Name: "NODE_LEAF_SWITCH"},
			{Name: "EGRESS_TARGETS"},
			{Name: "SYSTEM_PORTS"},
			{Name: "SYSTEM_AFFINITY_SWITCH"},
			{Name: "POD_PREFERRED_PORTS"},
		},
		Rows: []*csv1.RowData{},
	}

	sort.Slice(connections, func(i, j int) bool {
		return connections[i].PodId < connections[j].PodId
	})

	for _, conn := range connections {
		// Extract field values and handle empty fields with placeholder
		fields := []string{
			conn.PodId,
			conn.NodeName,
			conn.Nics,
			conn.NodeLeafSwitch,
			conn.EgressTargets,
			conn.SystemPorts,
			conn.SystemAffinitySwitch,
			conn.PodPreferredCsPorts,
		}

		row := &csv1.RowData{
			Cells: fields,
		}
		table.Rows = append(table.Rows, row)
	}
	return DisplayTable(table, nil, c.outErr)
}

func newJobGetCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	jctx := &jobCmdCtx{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "get NAME",
		Short: "get a job",
		Example: text.Indent(`# get job
csctl job get wsjob-test

# get job with json format
csctl job get wsjob-test -ojson`, "  "),
		Args: pkg.RequireNameArg(jobHelpMessage),
		PreRunE: chainedPreRunE(jctx,
			clientSetup,
			expectRegularOutput),
		RunE: func(cmd *cobra.Command, args []string) error {
			jobName := args[0]
			return jctx.Get(cmdCtx.Context, jobName)
		},
	}
	cmd.Flags().StringVarP(&jctx.Output, "output", "o", "table", "Output format. One of table, json, or yaml.")
	return cmd
}

func newJobListCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	jctx := &jobCmdCtx{cmdCtx: cmdCtx, listOptions: jobListOptions{}}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list jobs",
		Long: text.Indent(`
List jobs in order of decreasing job priorities.
For jobs with the same priority, sort by age in descending order.
`, "  "),
		Example: text.Indent(`# list jobs
csctl job list

# list jobs with json format
csctl job list -ojson`, "  "),
		Args: pkg.DisallowPosArg(jobHelpMessage),
		PreRunE: chainedPreRunE(jctx,
			clientSetup,
			expectRegularOutput),
		RunE: func(cmd *cobra.Command, args []string) error {
			return jctx.Get(cmdCtx.Context, "")
		},
	}
	cmd.Flags().StringVarP(&jctx.Output, "output", "o", "table", "Output format. One of table, json, or yaml.")
	cmd.Flags().StringVarP(&jctx.listOptions.LabelsString, "labels", "l", "", "Resource matching labels, comma-separated key-value pairs in format of 'key=value'.")
	cmd.Flags().BoolVarP(&jctx.listOptions.GetCurrentUserJobs, "my-jobs", "m", false, "Only show my jobs")
	cmd.Flags().BoolVarP(&jctx.listOptions.GetAllStates, "all-states", "a", false, "Show job in all states")
	cmd.Flags().Int32VarP(&jctx.listOptions.Limit, "max-jobs", "", 5000, "Maximum number of jobs to return (0 means no limit).")
	return cmd
}

func newJobSetPriorityCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	jctx := &jobCmdCtx{cmdCtx: cmdCtx, modifyOptions: jobModifyOptions{}}
	cmd := &cobra.Command{
		Use:   "set-priority NAME PRIORITY",
		Short: "set priority of an existing job",
		Long: text.Indent(`
Valid priority values are between p0 and p3, as well as between 0 and 399, where p0 and 0 stands for highest priority
Note that p0 and the value range [0, 99] could only be applied by admin users.
Regular users could only set priority between p1 and p3, as well as between 100 and 399.
priority value mapping:
P0 - [0, 99]
P1 - [100, 199]
P2 - [200, 299]
P3 - [300, 399]

Job priorities can only be modified by users with the same group ID (gid) as the user who submitted the job.
`, "  "),
		Example: text.Indent(`
# set job priority by bucket name
csctl job set-priority wsjob-test p1

# set job priority by priority value
csctl job set-priority wsjob-test 150
`, "  "),
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return errors.New("requires exactly two arguments: NAME and PRIORITY")
			}
			return nil
		},
		PreRunE: chainedPreRunE(jctx, clientSetup),
		RunE: func(cmd *cobra.Command, args []string) error {
			jobName := args[0]
			priorityStr := args[1]
			return jctx.SetPriority(cmdCtx.Context, jobName, priorityStr)
		},
	}
	cmd.Flags().StringVarP(&jctx.Output, "output", "o", "",
		fmt.Sprintf("Output format. Empty defaults to a success message. Also supports %v", pkg.Keys(acceptedOutFmt)))
	return cmd
}

func newJobTopologyCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	jctx := &jobCmdCtx{cmdCtx: cmdCtx}
	cmd := &cobra.Command{
		Use:   "topology JOB_NAME",
		Short: "get job topology information",
		Long: text.Indent(`
Get topology information for a job:
POD:                      The name of the pod
NODE:                     The name of the physical node (AX, SX, ...) that the pod assigned and runs on
NICS:                     NICs of pod ingress/egress traffic goes through
NODE_LEAF_SWITCH:         The leaf switch that assigned node connects with
EGRESS_TARGETS:           The target node/system of egress network traffic
SYSTEM_PORTS:             The system and ports that the pod talks to
SYSTEM_AFFINITY_SWITCH:   The leaf switch that the system connects with (may not be the same as the pod when spine traffic occurs)
POD_PREFERRED_PORTS:      The preferred cs ports from the stack compile hint that the pod should target
`, "  "),
		Example: text.Indent(`# get job topology
csctl job topology wsjob-test

# get job topology for specific namespace
csctl job topology wsjob-test --namespace my-namespace`, "  "),
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("requires exactly one argument: JOB_NAME")
			}
			return nil
		},
		PreRunE: chainedPreRunE(jctx,
			clientSetup),
		RunE: func(cmd *cobra.Command, args []string) error {
			jobId := args[0]
			return jctx.GetJobTopology(cmdCtx.Context, jobId)
		},
	}

	return cmd
}

func NewJobCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Job management commands",
		// We will remove the experimental note when all job commands are migrated.
		Long: "List/Get jobs and set priority for jobs (experimental)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(newJobGetCmd(cmdCtx))
	cmd.AddCommand(newJobListCmd(cmdCtx))
	cmd.AddCommand(newJobSetPriorityCmd(cmdCtx))
	cmd.AddCommand(newJobTopologyCmd(cmdCtx))
	// TODO: Move cancel/label/log-export under the new job command
	return cmd
}

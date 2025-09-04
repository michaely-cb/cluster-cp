package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/kr/text"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/util/duration"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/cluster/csctl/pkg"
)

const (
	sysMaintHelpMessage = "Run 'csctl system-maintenance --help' for usage"
	WorkflowIdLabelKey  = "cerebras/workflow-id"
	minVddcOffset       = -50
	maxVddcOffset       = 50
)

func executeSystemMaintenanceCommand(
	cmdCtx *pkg.CmdCtx,
	req *pb.CreateSystemMaintenanceRequest,
	outputFormat string,
	cmd *cobra.Command,
) error {
	// Execute the request
	client, err := cmdCtx.ClientFactory.NewCsAdmV1(cmd.Context())
	if err != nil {
		return err
	}

	resp, err := client.CreateSystemMaintenance(cmd.Context(), req)
	if err != nil {
		return err
	}

	// Handle output formatting
	outErr := CmdOutErr{cmd: cmd}
	switch outputFormat {
	case "json":
		return AsJson(resp, outErr)
	case "yaml":
		return AsYaml(resp, outErr)
	case "table":
		return displaySystemMaintenanceJobsTable(resp.GetJobs(), outErr)
	default:
		return displaySystemMaintenanceJobsTable(resp.GetJobs(), outErr)
	}
}

func getSystemsFromStringSlice(systems []string) ([]string, error) {
	if len(systems) == 0 {
		return nil, fmt.Errorf("--systems flag is required")
	}
	// Handle the ALL case - return nil to indicate all systems
	if len(systems) == 1 && strings.ToLower(systems[0]) == "all" {
		return nil, nil
	}

	return systems, nil
}

func newSysMaintCreateMembistCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	var membistOpts struct {
		systems       []string
		vddcOffset    int32
		skipFailedOps bool
		allowError    bool
		dryrun        bool
	}

	var outputFormat string

	cmd := &cobra.Command{
		Use:   "membist --systems <system-names> [flags]",
		Short: "create membist maintenance workflow",
		Long: text.Indent(`Create membist maintenance workflow.
Membist tests memory BIST functionality with configurable VDDC voltage offset.`, "  "),
		Example: text.Indent(`csctl system-maintenance create membist --systems system1,system2 --vddc-offset 30
csctl system-maintenance create membist --systems ALL --skip-failed-ops --allow-error
csctl system-maintenance create membist --systems system1 --dryrun`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			systems, err := getSystemsFromStringSlice(membistOpts.systems)
			if err != nil {
				return err
			}
			if membistOpts.vddcOffset > maxVddcOffset || membistOpts.vddcOffset < minVddcOffset {
				return fmt.Errorf("invalid voltage value: %d, must be a non-zero value between %d and %d",
					membistOpts.vddcOffset, minVddcOffset, maxVddcOffset)
			}
			req := &pb.CreateSystemMaintenanceRequest{
				WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
				AllowError:   membistOpts.allowError,
				Systems:      systems,
				Dryrun:       membistOpts.dryrun,
				Payload: &pb.CreateSystemMaintenanceRequest_Membist{
					Membist: &pb.MembistPayload{
						VddcVoltageOffset: membistOpts.vddcOffset,
						SkipFailedOps:     membistOpts.skipFailedOps,
					},
				},
			}
			return executeSystemMaintenanceCommand(cmdCtx, req, outputFormat, cmd)
		},
	}

	// Membist-specific flags
	cmd.Flags().StringSliceVar(&membistOpts.systems, "systems", []string{}, "Systems to run membist on (required)")
	cmd.Flags().Int32Var(&membistOpts.vddcOffset, "vddc-offset", 0, "VDDC voltage offset (-50 to 50)")
	cmd.Flags().BoolVar(&membistOpts.skipFailedOps, "skip-failed-ops", false, "Skip failed operations")
	cmd.Flags().BoolVar(&membistOpts.allowError, "allow-error", false, "Allow execution against systems in error state")
	cmd.Flags().BoolVar(&membistOpts.dryrun, "dryrun", false, "Execute in dryrun mode")
	cmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "Output format (table|json|yaml)")

	return cmd
}

func newSysMaintCreateVddcCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	var vddcOpts struct {
		systems    []string
		vddcOffset int32
		allowError bool
		dryrun     bool
	}
	var outputFormat string

	cmd := &cobra.Command{
		Use:   "vddc <set|unset> --systems <system-names> [flags]",
		Short: "create vddc maintenance workflow",
		Long: text.Indent(`Create VDDC maintenance workflow to set or unset voltage offsets.
Use 'set' to apply a voltage offset, 'unset' to restore default voltage.`, "  "),
		Example: text.Indent(`csctl system-maintenance create vddc set --systems ALL --vddc-offset -30
csctl system-maintenance create vddc unset --systems system1,system2
csctl system-maintenance create vddc set --systems system1 --vddc-offset 25 --dryrun`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			systems, err := getSystemsFromStringSlice(vddcOpts.systems)
			if err != nil {
				return err
			}
			if len(args) == 0 {
				return fmt.Errorf("vddc maintenance type requires input argument: set/unset")
			}
			operation := args[0]
			var vddcVoltage int32 = 0 // Default for unset

			if operation == "set" {
				if vddcOpts.vddcOffset == 0 || vddcOpts.vddcOffset > maxVddcOffset || vddcOpts.vddcOffset < minVddcOffset {
					return fmt.Errorf("vddc set requires input flag: --vddc-offset, with a non-zero value between %d and %d, current value %d is invalid",
						minVddcOffset, maxVddcOffset, vddcOpts.vddcOffset)
				}
				vddcVoltage = vddcOpts.vddcOffset
			} else if operation != "unset" {
				return fmt.Errorf("invalid operation type: %s, must be 'set' or 'unset'", operation)
			}

			req := &pb.CreateSystemMaintenanceRequest{
				WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_VDDC,
				AllowError:   vddcOpts.allowError,
				Systems:      systems,
				Dryrun:       vddcOpts.dryrun,
				Payload: &pb.CreateSystemMaintenanceRequest_Vddc{
					Vddc: &pb.VddcPayload{VddcVoltageOffset: vddcVoltage},
				},
			}
			return executeSystemMaintenanceCommand(cmdCtx, req, outputFormat, cmd)
		},
	}

	// VDDC-specific flags
	cmd.Flags().StringSliceVar(&vddcOpts.systems, "systems", []string{}, "Systems to run VDDC on (required)")
	cmd.Flags().Int32Var(&vddcOpts.vddcOffset, "vddc-offset", 0, "VDDC voltage offset (-50 to 50, required for 'set')")
	cmd.Flags().BoolVar(&vddcOpts.allowError, "allow-error", false, "Allow execution against systems in error state")
	cmd.Flags().BoolVar(&vddcOpts.dryrun, "dryrun", false, "Execute in dryrun mode")
	cmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "Output format (table|json|yaml)")

	return cmd
}

func newSysMaintCreateWaferDiagCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	var waferDiagOpts struct {
		systems            []string
		variants           []int32
		duration           float32
		sleepScale         float32
		memconfig          bool
		noMath             bool
		continueAfterFail  bool
		coredumpOnFail     bool
		skipConfigApiCheck bool
		testmode           bool
		cbcoreImage        string
		allowError         bool
		dryrun             bool
	}
	var outputFormat string

	cmd := &cobra.Command{
		Use:   "wafer-diag --systems <system-names> [flags]",
		Short: "create wafer diagnostic maintenance workflow",
		Long: text.Indent(`Create wafer diagnostic maintenance workflow.
Wafer-diag runs comprehensive hardware diagnostics with configurable test parameters.`, "  "),
		Example: text.Indent(`csctl system-maintenance create wafer-diag --systems system1,system2 --variants 0,2,3 --duration 10.0
csctl system-maintenance create wafer-diag --systems ALL --memconfig --no-math --continue-after-fail
csctl system-maintenance create wafer-diag --systems system1 --cbcore-image registry.local/cbcore:latest --testmode`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			systems, err := getSystemsFromStringSlice(waferDiagOpts.systems)
			if err != nil {
				return err
			}
			if len(waferDiagOpts.variants) > 0 {
				for _, v := range waferDiagOpts.variants {
					if v < 0 {
						return fmt.Errorf("invalid variant value: %d, must be non-negative", v)
					}
				}
			}
			if waferDiagOpts.duration < 0 {
				return fmt.Errorf("invalid duration: %g, must be non-negative", waferDiagOpts.duration)
			}
			if waferDiagOpts.sleepScale <= 0 {
				return fmt.Errorf("invalid sleep scale: %g, must be greater than 0", waferDiagOpts.sleepScale)
			}

			req := &pb.CreateSystemMaintenanceRequest{
				WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG,
				AllowError:   waferDiagOpts.allowError,
				Systems:      systems,
				Dryrun:       waferDiagOpts.dryrun,
				Payload: &pb.CreateSystemMaintenanceRequest_WaferDiag{
					WaferDiag: &pb.WaferDiagPayload{
						Variants:           waferDiagOpts.variants,
						Duration:           waferDiagOpts.duration,
						SleepScale:         waferDiagOpts.sleepScale,
						Memconfig:          waferDiagOpts.memconfig,
						NoMath:             waferDiagOpts.noMath,
						ContinueAfterFail:  waferDiagOpts.continueAfterFail,
						CoredumpOnFail:     waferDiagOpts.coredumpOnFail,
						SkipConfigApiCheck: waferDiagOpts.skipConfigApiCheck,
						Testmode:           waferDiagOpts.testmode,
						CbcoreImage:        waferDiagOpts.cbcoreImage,
					},
				},
			}
			return executeSystemMaintenanceCommand(cmdCtx, req, outputFormat, cmd)
		},
	}

	// Wafer-diag specific flags
	cmd.Flags().StringSliceVar(&waferDiagOpts.systems, "systems", []string{}, "Systems to run wafer-diag on (required)")
	cmd.Flags().Int32SliceVar(&waferDiagOpts.variants, "variants", []int32{}, "Test variants to run (comma-separated)")
	cmd.Flags().Float32Var(&waferDiagOpts.duration, "duration", 0.0, "Test duration in seconds")
	cmd.Flags().Float32Var(&waferDiagOpts.sleepScale, "sleep-scale", 1.0, "Sleep scale factor")
	cmd.Flags().BoolVar(&waferDiagOpts.memconfig, "memconfig", false, "Enable memconfig testing")
	cmd.Flags().BoolVar(&waferDiagOpts.noMath, "no-math", false, "Skip math operations")
	cmd.Flags().BoolVar(&waferDiagOpts.continueAfterFail, "continue-after-fail", false, "Continue execution after failures")
	cmd.Flags().BoolVar(&waferDiagOpts.coredumpOnFail, "coredump-on-fail", false, "Generate coredump on failure")
	cmd.Flags().BoolVar(&waferDiagOpts.skipConfigApiCheck, "skip-config-api-check", false, "Skip configuration API checks")
	cmd.Flags().BoolVar(&waferDiagOpts.testmode, "testmode", false, "Run in test mode (lower resource requirements)")
	cmd.Flags().StringVar(&waferDiagOpts.cbcoreImage, "cbcore-image", "",
		"Override cbcore image (e.g., 'registry.local/cbcore:tag' or '171496337684.dkr.ecr.us-west-2.amazonaws.com/cbcore:tag')")
	cmd.Flags().BoolVar(&waferDiagOpts.allowError, "allow-error", false, "Allow execution against systems in error state")
	cmd.Flags().BoolVar(&waferDiagOpts.dryrun, "dryrun", false, "Execute in dryrun mode")
	cmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "Output format (table|json|yaml)")

	return cmd
}

func newSysMaintCreateCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create system maintenance workflows",
		Long:  "Create system maintenance workflows for different maintenance types",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help() // Show help when no subcommand is provided
		},
	}

	// Add subcommands for each maintenance type
	cmd.AddCommand(newSysMaintCreateMembistCmd(cmdCtx))
	cmd.AddCommand(newSysMaintCreateVddcCmd(cmdCtx))
	cmd.AddCommand(newSysMaintCreateWaferDiagCmd(cmdCtx))

	return cmd
}

// displaySystemMaintenanceJobsTable builds a table from a slice of SystemMaintenanceJob
// and uses DisplayTable to print it.
func displaySystemMaintenanceJobsTable(jobs []*pb.SystemMaintenanceJob, outErr OutErr) error {
	table := &csv1.Table{
		Columns: []*csv1.ColumnDefinition{
			{Name: "SESSION"},
			{Name: "NAME"},
			{Name: "TYPE"},
			{Name: "PRIORITY"},
			{Name: "AGE"},
			{Name: "DURATION"},
			{Name: "PHASE"},
			{Name: "SYSTEMS"},
			{Name: "USER"},
			{Name: "NOTES"},
			{Name: "WORKFLOW_ID"},
		},
	}

	for _, job := range jobs {
		// Calculate duration if execution time exists
		duration := "0s"
		if job.ExecutionTime != nil {
			if job.CompletionTime != nil {
				duration = humanDuration(job.CompletionTime.AsTime().Sub(job.ExecutionTime.AsTime()))
			} else {
				duration = humanDuration(time.Now().Sub(job.ExecutionTime.AsTime()))
			}
		}

		// Calculate age from create time
		age := "0s"
		if job.CreateTime != nil {
			age = humanDuration(time.Now().Sub(job.CreateTime.AsTime()))
		}

		// Format systems as a comma-separated list with count
		systemsStr := ""
		if len(job.Systems) > 0 {
			slices.Sort(job.Systems)
			systemsStr = fmt.Sprintf("(%d) %s", len(job.Systems), strings.Join(job.Systems, ","))
			if len(systemsStr) > 60 {
				systemsStr = systemsStr[:60] + "..."
			}
		}

		// Get username if available
		username := ""
		if job.User != nil {
			username = job.User.Username
		}

		workflowId := job.Labels[WorkflowIdLabelKey]

		row := &csv1.RowData{
			Cells: []string{
				job.Namespace,
				job.JobId,
				mapWorkflowTypeToHuman(job.WorkflowType),
				fmt.Sprintf("P%d (%d)", job.Priority/100, job.Priority),
				age,
				duration,
				mapJobStatusToHuman(job.Status),
				systemsStr,
				username,
				job.Notes,
				workflowId,
			},
		}
		table.Rows = append(table.Rows, row)
	}

	return DisplayTable(table, nil, outErr)
}

// workflowTypeToHuman converts a SystemMaintenanceType to a human readable string
func mapWorkflowTypeToHuman(wfType pb.SystemMaintenanceType) string {
	switch wfType {
	case pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST:
		return "membist"
	case pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_VDDC:
		return "vddc"
	case pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG:
		return "wafer-diag"
	default:
		return wfType.String()
	}
}

// Converts a JobStatus enum to a human readable string
func mapJobStatusToHuman(jobStatus pb.JobStatus) string {
	switch jobStatus {
	case pb.JobStatus_JOB_STATUS_UNSPECIFIED:
		return "UNSPECIFIED"
	case pb.JobStatus_JOB_STATUS_CREATED:
		return "CREATED"
	case pb.JobStatus_JOB_STATUS_IN_PROGRESS:
		return "IN_PROGRESS"
	case pb.JobStatus_JOB_STATUS_SUCCEEDED:
		return "SUCCEEDED"
	case pb.JobStatus_JOB_STATUS_FAILED:
		return "FAILED"
	case pb.JobStatus_JOB_STATUS_CANCELLED:
		return "CANCELLED"
	case pb.JobStatus_JOB_STATUS_DELETED:
		return "DELETED"
	case pb.JobStatus_JOB_STATUS_SCHEDULED:
		return "SCHEDULED"
	case pb.JobStatus_JOB_STATUS_IN_QUEUE:
		return "IN_QUEUE"
	case pb.JobStatus_JOB_STATUS_INIT:
		return "INIT"
	case pb.JobStatus_JOB_STATUS_FAILING:
		return "FAILING"
	default:
		return jobStatus.String()
	}
}

// humanDuration formats a duration in human readable format
func humanDuration(d time.Duration) string {
	return duration.HumanDuration(d)
}

func newSysMaintGetCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	var allStates bool
	var outputFormat string

	cmd := &cobra.Command{
		Use:   "get <WORKFLOW_ID> [-o {table|json|yaml}]",
		Short: "get the jobs of a system maintenance workflow",
		Long: text.Indent(`Get the jobs of a system maintenance workflow.
The first argument is the workflow id.
The second argument is the output format. The supported ones are table (default), json, and yaml.`, "  "),
		Example: text.Indent(`csctl system-maintenance get workflow-test
csctl system-maintenance get workflow-test -o json
csctl system-maintenance get workflow-test -o json --namespace job-operator
`, "  "),
		Args: pkg.RequireNameArg(sysMaintHelpMessage),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := &pb.GetSystemMaintenanceRequest{
				WorkflowId: args[0],
				AllStates:  allStates,
			}

			// Inject the CsAdmV1Client using the ClientFactory.
			client, err := cmdCtx.ClientFactory.NewCsAdmV1(cmd.Context())
			if err != nil {
				return err
			}

			resp, err := client.GetSystemMaintenance(cmd.Context(), req)
			if err != nil {
				return err
			}

			// Use the output marshaller to display the response.
			outErr := CmdOutErr{cmd: cmd}
			switch outputFormat {
			case "json":
				return AsJson(resp, outErr)
			case "yaml":
				return AsYaml(resp, outErr)
			case "table":
				return displaySystemMaintenanceJobsTable(resp.GetJobs(), outErr)
			default:
				// we should never reach here as table is the default below
				return displaySystemMaintenanceJobsTable(resp.GetJobs(), outErr)
			}
		},
	}

	cmd.Flags().BoolVarP(&allStates, "all-states", "a", false, "Show jobs in all states")
	cmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "Output format. One of table, json, or yaml")

	return cmd
}

func newSysMaintListCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {

	var allStates bool
	var outputFormat string

	// TODO: add an option to list filter different system maintenance type
	// workflows.
	cmd := &cobra.Command{
		Use:   "list [-o {table|json|yaml}]",
		Short: "list all system maintenance workflows in a namespace",
		Long:  text.Indent(`List all system maintenance workflows in a namespace.`, "  "),
		Example: text.Indent(`csctl system-maintenance list
csctl system-maintenance list -o json
csctl system-maintenance list -o json --namespace job-operator 
`, "  "),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := &pb.ListSystemMaintenanceRequest{
				AllStates: allStates,
			}

			// Inject the CsAdmV1Client using the ClientFactory.
			client, err := cmdCtx.ClientFactory.NewCsAdmV1(cmd.Context())
			if err != nil {
				return err
			}

			resp, err := client.ListSystemMaintenance(cmd.Context(), req)
			if err != nil {
				return err
			}

			// Use the output marshaller to display the response.
			outErr := CmdOutErr{cmd: cmd}
			switch outputFormat {
			case "json":
				return AsJson(resp, outErr)
			case "yaml":
				return AsYaml(resp, outErr)
			case "table":
				return displaySystemMaintenanceWorkflowsTable(resp.GetWorkflows(), outErr)
			default:
				// we should never reach here as table is the default below
				return displaySystemMaintenanceWorkflowsTable(resp.GetWorkflows(), outErr)
			}
		},
	}

	cmd.Flags().BoolVarP(&allStates, "all-states", "a", false, "Show workflows in all states")
	cmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "Output format. One of table, json, or yaml")

	return cmd
}

func displaySystemMaintenanceWorkflowsTable(workflows []*pb.SystemMaintenanceWorkflow, outErr OutErr) error {
	table := &csv1.Table{
		Columns: []*csv1.ColumnDefinition{
			{Name: "SESSION"},
			{Name: "WORKFLOW_ID"},
			{Name: "TYPE"},
			{Name: "AGE"},
			{Name: "DURATION"},
			{Name: "STATUS"},
			{Name: "SYSTEMS"},
		},
	}

	for _, workflow := range workflows {
		// For each JobStatus enum, count the number of jobs with this enum.
		// Use array rather than map so as to have consistent order.
		status := [int(pb.JobStatus_JOB_STATUS_FAILING) + 1]int{}
		systems := []string{}

		// A workflow typically contains multiple jobs.  The workflow duration
		// is the duration from the execution time of the earliest job to the
		// completion time of the latest job.
		var earliestExecTime time.Time
		var latestCompletionTime time.Time
		for _, job := range workflow.GetJobs() {
			if job.ExecutionTime != nil {
				if (earliestExecTime.IsZero()) ||
					(job.ExecutionTime.AsTime().Before(earliestExecTime)) {
					earliestExecTime = job.ExecutionTime.AsTime()
				}

				if job.CompletionTime != nil {
					if (latestCompletionTime.IsZero()) ||
						(job.CompletionTime.AsTime().After(latestCompletionTime)) {
						latestCompletionTime = job.CompletionTime.AsTime()
					}
				} else {
					// Job still running
					latestCompletionTime = time.Now()
				}
			}

			systems = append(systems, job.GetSystems()...)
			status[int(job.Status)]++
		}
		duration := "0s"
		if !earliestExecTime.IsZero() {
			duration = humanDuration(latestCompletionTime.Sub(earliestExecTime))
		}

		age := "0s"
		if workflow.CreateTime != nil {
			age = humanDuration(time.Now().Sub(workflow.CreateTime.AsTime()))
		}

		// List each status and its number of jobs, in the form
		// "(1/1/2) CREATED/IN_PROGRESS/TOTAL"
		statusStr := ""
		countStr := "("
		total := 0
		for i := 0; i != len(status); i++ {
			if status[i] == 0 {
				continue
			}
			total += status[i]
			statusStr += fmt.Sprintf("%s/", mapJobStatusToHuman(pb.JobStatus(i)))
			countStr += fmt.Sprintf("%d/", status[i])
		}
		statusStr += "TOTAL"
		countStr += fmt.Sprintf("%d)", total)

		statusStr = countStr + " " + statusStr

		// Format systems as a comma-separated list with count
		systemsStr := ""
		if len(systems) > 0 {
			slices.Sort(systems)
			systemsStr = fmt.Sprintf("(%d) %s", len(systems), strings.Join(systems, ","))
			if len(systemsStr) > 60 {
				systemsStr = systemsStr[:60] + "..."
			}
		}

		row := []string{
			workflow.Namespace,
			workflow.WorkflowId,
			mapWorkflowTypeToHuman(workflow.WorkflowType),
			age,
			duration,
			statusStr,
			systemsStr,
		}
		table.Rows = append(table.Rows, &csv1.RowData{Cells: row})
	}

	return DisplayTable(table, nil, outErr)
}

func NewSysMaintCmd(cmdCtx *pkg.CmdCtx) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "system-maintenance",
		Short: "System maintenance commands",
		Long:  "Commands for create/retrieve system maintenance workflows",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(newSysMaintCreateCmd(cmdCtx))
	cmd.AddCommand(newSysMaintGetCmd(cmdCtx))
	cmd.AddCommand(newSysMaintListCmd(cmdCtx))

	return cmd
}

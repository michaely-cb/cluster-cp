//go:build !e2e

package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"

	"gopkg.in/yaml.v3"

	"cerebras.com/cluster/csctl/pkg"
)

func TestSysMaintCreateTableOutput(t *testing.T) {
	// Create a fixed timestamp for testing
	executionTime := &timestamppb.Timestamp{Seconds: 12400}
	completionTime := &timestamppb.Timestamp{Seconds: 12500}

	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = []interface{}{
		&pb.CreateSystemMaintenanceResponse{
			Jobs: []*pb.SystemMaintenanceJob{
				{
					JobId:          "test-job-id",
					Namespace:      "test-namespace",
					WorkflowType:   pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
					Status:         pb.JobStatus_JOB_STATUS_CREATED,
					Systems:        []string{"system1", "system2"},
					Priority:       299, // P2 priority
					Notes:          "test notes",
					ExecutionTime:  executionTime,
					CompletionTime: completionTime,
					User: &pb.User{
						Username: "testuser",
					},
					Labels: map[string]string{WorkflowIdLabelKey: "test-workflow-id"},
				},
			},
		},
	}

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintCreateCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"membist", "--systems", "system1", "--output", "table"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()

	// Test column headers
	assert.Contains(t, output, "SESSION")
	assert.Contains(t, output, "NAME")
	assert.Contains(t, output, "TYPE")
	assert.Contains(t, output, "PRIORITY")
	assert.Contains(t, output, "AGE")
	assert.Contains(t, output, "DURATION")
	assert.Contains(t, output, "PHASE")
	assert.Contains(t, output, "SYSTEMS")
	assert.Contains(t, output, "USER")
	assert.Contains(t, output, "NOTES")
	assert.Contains(t, output, "WORKFLOW_ID")

	// Test content
	assert.Contains(t, output, "test-job-id")
	assert.Contains(t, output, "test-namespace")
	assert.Contains(t, output, "membist")
	assert.Contains(t, output, "P2 (299)")
	assert.Contains(t, output, "(2) system1,system2")
	assert.Contains(t, output, "testuser")
	assert.Contains(t, output, "test notes")
	assert.Contains(t, output, "test-workflow-id")

	// Verify duration is present (100s in this case)
	assert.Contains(t, output, "100s")
}

func TestSysMaintCreateWaferDiagTableOutput(t *testing.T) {
	// Create a fixed timestamp for testing
	executionTime := &timestamppb.Timestamp{Seconds: 12400}
	completionTime := &timestamppb.Timestamp{Seconds: 12500}

	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = []interface{}{
		&pb.CreateSystemMaintenanceResponse{
			Jobs: []*pb.SystemMaintenanceJob{
				{
					JobId:          "wafer-diag-job-id",
					Namespace:      "test-namespace",
					WorkflowType:   pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG,
					Status:         pb.JobStatus_JOB_STATUS_CREATED,
					Systems:        []string{"system1", "system2"},
					Priority:       199, // P1 priority for wafer-diag
					Notes:          "wafer diag test",
					ExecutionTime:  executionTime,
					CompletionTime: completionTime,
					User: &pb.User{
						Username: "testuser",
					},
					Labels: map[string]string{WorkflowIdLabelKey: "test-workflow-id"},
				},
			},
		},
	}
	// handle the cmdCtx, command setup, and asserts
	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}
	cmd := newSysMaintCreateCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{
		"wafer-diag",
		"--systems", "system1,system2",
		"--variants", "0,2,3",
		"--cbcore-image", "registry.local/cbcore:0.0.0-202505111129-5336-3bba1eea",
		"--output", "table",
	})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()

	// Verify wafer-diag specific content
	assert.Contains(t, output, "wafer-diag-job-id")
	assert.Contains(t, output, "wafer-diag") // Type column
	assert.Contains(t, output, "P1 (199)")   // Priority formatting
	assert.Contains(t, output, "(2) system1,system2")
	assert.Contains(t, output, "testuser")
	assert.Contains(t, output, "wafer diag test")
	assert.Contains(t, output, "test-workflow-id")
	assert.Contains(t, output, "100s") // Duration calculation
}

// TestSysMaintCreateJSONOutput tests the JSON output format
func TestSysMaintCreateJSONOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = []interface{}{
		&pb.CreateSystemMaintenanceResponse{
			Jobs: []*pb.SystemMaintenanceJob{
				{
					JobId:        "test-job-id",
					Namespace:    "test-namespace",
					WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
					Status:       pb.JobStatus_JOB_STATUS_CREATED,
					Systems:      []string{"system1"},
					Priority:     1,
					Labels:       map[string]string{WorkflowIdLabelKey: "test-workflow-id"},
					Notes:        "test notes",
					CreateTime:   &timestamppb.Timestamp{Seconds: 12345},
				},
			},
		},
	}

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintCreateCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"membist", "--systems", "system1", "--output", "json"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	assert.Contains(t, output, "test-job-id")

	// Verify JSON validity
	var js map[string]interface{}
	err = json.Unmarshal([]byte(output), &js)
	require.NoError(t, err)
}

func TestSysMaintCreateWaferDiagJSONOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = []interface{}{
		&pb.CreateSystemMaintenanceResponse{
			Jobs: []*pb.SystemMaintenanceJob{
				{
					JobId:        "wafer-diag-job-id-2",
					Namespace:    "test-namespace",
					WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG,
					Status:       pb.JobStatus_JOB_STATUS_CREATED,
					Systems:      []string{"ALL"},
					Priority:     99,
					Notes:        "wafer-diag with different args",
					Labels:       map[string]string{WorkflowIdLabelKey: "wafer-diag-job-id-2"},
					CreateTime:   &timestamppb.Timestamp{Seconds: 12345},
				},
			},
		},
	}

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintCreateCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{
		"wafer-diag",
		"--systems", "ALL",
		"--memconfig",
		"--no-math",
		"--duration", "10.0",
		"--output", "json",
	})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	assert.Contains(t, output, "wafer-diag-job-id-2")

	// Verify JSON validity
	var js map[string]interface{}
	err = json.Unmarshal([]byte(output), &js)
	require.NoError(t, err)
}

// TestSysMaintCreateTableOutputOrder tests the order of columns in table output
// This test ensures that the columns are in the expected order
// This test uses whitespace to separate columns, so it may break if you add whitespace to any of the column values
func TestSysMaintCreateTableOutputOrder(t *testing.T) {
	// Create fixed timestamps for deterministic testing
	createTime := &timestamppb.Timestamp{Seconds: 12345}
	executionTime := &timestamppb.Timestamp{Seconds: 12400}
	completionTime := &timestamppb.Timestamp{Seconds: 12500}

	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = []interface{}{
		&pb.CreateSystemMaintenanceResponse{
			Jobs: []*pb.SystemMaintenanceJob{
				{
					Namespace:      "test-namespace",
					JobId:          "test-job-id",
					WorkflowType:   pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
					Status:         pb.JobStatus_JOB_STATUS_CREATED,
					Systems:        []string{"system1", "system2"},
					Priority:       299, // P2 priority
					Notes:          "testnotes",
					CreateTime:     createTime,
					ExecutionTime:  executionTime,
					CompletionTime: completionTime,
					User: &pb.User{
						Username: "testuser",
					},
					Labels: map[string]string{WorkflowIdLabelKey: "test-workflow-id"},
				},
			},
		},
	}

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintCreateCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"membist", "--systems", "system1", "--output", "table"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	lines := strings.Split(output, "\n")
	require.GreaterOrEqual(t, len(lines), 2, "Expected at least header and one data row")

	// Verify header order
	expectedHeaders := []string{
		"SESSION",
		"NAME",
		"TYPE",
		"PRIORITY",
		"AGE",
		"DURATION",
		"PHASE",
		"SYSTEMS",
		"USER",
		"NOTES",
		"WORKFLOW_ID",
	}

	// Split header line and trim spaces
	headers := strings.Fields(lines[0])
	require.Equal(t, len(expectedHeaders), len(headers), "Number of headers doesn't match expected")

	// Verify header order
	for i, expected := range expectedHeaders {
		assert.Equal(t, expected, headers[i],
			fmt.Sprintf("Header at position %d should be %s, got %s", i, expected, headers[i]))
	}

	// Verify data row contains expected values in correct order
	dataRow := strings.Fields(lines[1])

	const skip = "skip"
	// Some values like AGE are dynamic so skip them
	// Some values contain whitespace, so we have multiple parts
	expectedValues := []string{
		"test-namespace",   // SESSION
		"test-job-id",      // NAME
		"membist",          // TYPE
		"P2",               // PRIORITY (first part)
		"(299)",            // PRIORITY (second part)
		skip,               // AGE
		"100s",             // DURATION
		"CREATED",          // PHASE
		"(2)",              // SYSTEMS COUNT (first part)
		"system1,system2",  // SYSTEMS (second part)
		"testuser",         // USER
		"testnotes",        // NOTES
		"test-workflow-id", // WORKFLOW_ID
	}

	// Verify each expected value is in the correct position
	for i, expected := range expectedValues {
		if expected == skip {
			continue
		}
		assert.Contains(t, dataRow[i], expected, fmt.Sprintf("Data at position %d should contain %s, got %s", i, expected, dataRow[i]))
	}
}

// TestSysMaintCreateYAMLOutput tests the YAML output format
func TestSysMaintCreateYAMLOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = []interface{}{
		&pb.CreateSystemMaintenanceResponse{
			Jobs: []*pb.SystemMaintenanceJob{
				{
					JobId:        "test-job-id",
					Namespace:    "test-namespace",
					WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
					Status:       pb.JobStatus_JOB_STATUS_CREATED,
					Systems:      []string{"system1"},
					Priority:     1,
					Labels:       map[string]string{WorkflowIdLabelKey: "test-workflow-id"},
					Notes:        "test notes",
					CreateTime:   &timestamppb.Timestamp{Seconds: 12345},
				},
			},
		},
	}

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintCreateCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"membist", "--systems", "system1", "--output", "yaml"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	assert.Contains(t, output, "test-job-id")

	// Verify YAML validity
	var yml map[string]interface{}
	err = yaml.Unmarshal([]byte(output), &yml)
	require.NoError(t, err)
}

func TestSysMaintCreateErrors(t *testing.T) {
	testCases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "invalid maintenance type",
			args:    []string{"invalid-type", "--systems", "system1"},
			wantErr: "unknown command \"invalid-type\" for \"create\"",
		},
		{
			name:    "missing systems flag",
			args:    []string{"membist"},
			wantErr: "--systems flag is required",
		},
		{
			name:    "vddc missing operation type",
			args:    []string{"vddc", "--systems", "system1"},
			wantErr: "vddc maintenance type requires input argument: set/unset",
		},
		{
			name:    "vddc set missing voltage",
			args:    []string{"vddc", "set", "--systems", "system1"},
			wantErr: "vddc set requires input flag: --vddc-offset, with a non-zero value",
		},
		{
			name:    "vddc set voltage out of boundary",
			args:    []string{"vddc", "set", "--vddc-offset", "51", "--systems", "system1"},
			wantErr: "vddc set requires input flag: --vddc-offset, with a non-zero value between -50 and 50, current value 51 is invalid",
		},
		{
			name:    "vddc invalid operation type",
			args:    []string{"vddc", "unknown", "--systems", "system1"},
			wantErr: "invalid operation type: unknown, must be 'set' or 'unset'",
		},
		{
			name:    "membist voltage out of boundary",
			args:    []string{"membist", "--vddc-offset", "-51", "--systems", "system1"},
			wantErr: "invalid voltage value: -51, must be a non-zero value between -50 and 50",
		},
		{
			name:    "wafer-diag invalid variant with negative value",
			args:    []string{"wafer-diag", "--systems", "system1", "--variants", "-1"}, //"--cbcore-tag", ""
			wantErr: "invalid variant value: -1, must be non-negative",
		},
		{
			name:    "wafer-diag invalid variant with empty arg",
			args:    []string{"wafer-diag", "--systems", "system1", "--variants", ""}, //"--cbcore-tag", ""
			wantErr: "invalid argument \"\" for \"--variants\" flag",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeFactory := NewFakeClientFactory()
			cmdCtx := pkg.CmdCtx{
				ClientFactory: fakeFactory,
				DebugLevel:    1,
			}

			cmd := newSysMaintCreateCmd(&cmdCtx)
			fakeOutErr := NewFakeOutErr()
			cmd.SetOut(fakeOutErr.O)
			cmd.SetErr(fakeOutErr.E)

			cmd.SetArgs(tc.args)
			err := cmd.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// Create a fixed timestamp for testing
var executionTime1 = &timestamppb.Timestamp{Seconds: 12400}
var completionTime1 = &timestamppb.Timestamp{Seconds: 12500}
var executionTime2 = &timestamppb.Timestamp{Seconds: 12600}
var completionTime2 = &timestamppb.Timestamp{Seconds: 12700}

var jobs = []*pb.SystemMaintenanceJob{
	{
		JobId:          "test-job-id1",
		Namespace:      "test-namespace",
		WorkflowType:   pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
		Status:         pb.JobStatus_JOB_STATUS_CREATED,
		Systems:        []string{"system1", "system2"},
		Priority:       299, // P2 priority
		Notes:          "test notes",
		ExecutionTime:  executionTime1,
		CompletionTime: completionTime1,
		User: &pb.User{
			Username: "testuser",
		},
	},
	{
		JobId:          "test-job-id2",
		Namespace:      "test-namespace",
		WorkflowType:   pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
		Status:         pb.JobStatus_JOB_STATUS_IN_PROGRESS,
		Systems:        []string{"system3", "system4"},
		Priority:       299, // P2 priority
		Notes:          "test notes",
		ExecutionTime:  executionTime2,
		CompletionTime: completionTime2,
		User: &pb.User{
			Username: "testuser",
		},
	},
}

var get_response = []interface{}{
	&pb.GetSystemMaintenanceResponse{
		Jobs: jobs,
	},
}

// TestSysMaintGetTableOutput tests the table output format
func TestSysMaintGetTableOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = get_response

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintGetCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"test-workflow-id"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	fmt.Println(output)
	assert.Contains(t, output, "SESSION")
	assert.Contains(t, output, "NAME")
	assert.Contains(t, output, "TYPE")
	assert.Contains(t, output, "PRIORITY")
	assert.Contains(t, output, "AGE")
	assert.Contains(t, output, "DURATION")
	assert.Contains(t, output, "PHASE")
	assert.Contains(t, output, "SYSTEMS")
	assert.Contains(t, output, "USER")
	assert.Contains(t, output, "NOTES")

	// Test content
	assert.Contains(t, output, "test-job-id1")
	assert.Contains(t, output, "test-job-id2")
	assert.Contains(t, output, "test-namespace")
	assert.Contains(t, output, "membist")
	assert.Contains(t, output, "P2 (299)")
	assert.Contains(t, output, "(2) system1,system2")
	assert.Contains(t, output, "testuser")
	assert.Contains(t, output, "test notes")
}

// TestSysMaintGetJSONOutput tests the JSON output format
func TestSysMaintGetJSONOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = get_response

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintGetCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"test-workflow-id", "--output", "json"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	assert.Contains(t, output, "test-job-id")

	// Verify JSON validity
	var js map[string]interface{}
	err = json.Unmarshal([]byte(output), &js)
	require.NoError(t, err)
}

// TestSysMaintGetYAMLOutput tests the YAML output format
func TestSysMaintGetYAMLOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = get_response

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintGetCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"test-workflow-id", "--output", "yaml"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	assert.Contains(t, output, "test-job-id")

	// Verify YAML validity
	var yml map[string]interface{}
	err = yaml.Unmarshal([]byte(output), &yml)
	require.NoError(t, err)
}

var list_response = []interface{}{
	&pb.ListSystemMaintenanceResponse{
		Workflows: []*pb.SystemMaintenanceWorkflow{
			{
				WorkflowId:   "test-workflow-id",
				Namespace:    "test-namespace",
				WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
				Jobs:         jobs,
				CreateTime:   &timestamppb.Timestamp{Seconds: 12345},
			},
		},
	},
}

// TestSysMaintListTableOutput tests the table output format
func TestSysMaintListTableOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = list_response

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintListCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	fmt.Println(output)
	assert.Contains(t, output, "SESSION")
	assert.Contains(t, output, "WORKFLOW_ID")
	assert.Contains(t, output, "TYPE")
	assert.Contains(t, output, "AGE")
	assert.Contains(t, output, "DURATION")
	assert.Contains(t, output, "STATUS")
	assert.Contains(t, output, "SYSTEMS")

	// Test content
	assert.Contains(t, output, "test-namespace")
	assert.Contains(t, output, "test-workflow-id")
	assert.Contains(t, output, "membist")
	assert.Contains(t, output, "5m") // duration
	assert.Contains(t, output, "(1/1/2) CREATED/IN_PROGRESS/TOTAL")
	assert.Contains(t, output, "(4) system1,system2,system3,system4")
}

// TestSysMaintListJSONOutput tests the JSON output format
func TestSysMaintListJSONOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = list_response

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintListCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"--output", "json"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	assert.Contains(t, output, "test-workflow-id")

	// Verify JSON validity
	var js map[string]interface{}
	err = json.Unmarshal([]byte(output), &js)
	require.NoError(t, err)
}

// TestSysMaintListYAMLOutput tests the YAML output format
func TestSysMaintListYAMLOutput(t *testing.T) {
	fakeFactory := NewFakeClientFactory()
	fakeFactory.csAdmClient.responseOrErr = list_response

	cmdCtx := pkg.CmdCtx{
		ClientFactory: fakeFactory,
		DebugLevel:    1,
	}

	cmd := newSysMaintListCmd(&cmdCtx)
	fakeOutErr := NewFakeOutErr()
	cmd.SetOut(fakeOutErr.O)
	cmd.SetErr(fakeOutErr.E)

	cmd.SetArgs([]string{"--output", "yaml"})
	err := cmd.Execute()
	require.NoError(t, err)

	output := fakeOutErr.O.String()
	assert.Contains(t, output, "test-job-id")

	// Verify YAML validity
	var yml map[string]interface{}
	err = yaml.Unmarshal([]byte(output), &yml)
	require.NoError(t, err)
}

func TestSysMaintCreateWaferDiagEdgeCases(t *testing.T) {
	testCases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "wafer-diag no variants should be allowed",
			args:    []string{"wafer-diag", "--systems", "system1"},
			wantErr: "", // Should not error
		},
		{
			name: "wafer-diag with all flags",
			args: []string{
				"wafer-diag",
				"--systems", "system1",
				"--variants", "0,1,2,3,4,5,6",
				"--duration", "15.5",
				"--sleep-scale", "2.0",
				"--memconfig",
				"--no-math",
				"--continue-after-fail",
				"--coredump-on-fail",
				"--skip-config-api-check",
				"--cbcore-image", "registry.local/cbcore:0.0.0-test-tag",
			},
			wantErr: "", // Should not error
		},
		{
			name:    "wafer-diag invalid duration",
			args:    []string{"wafer-diag", "--systems", "system1", "--duration", "-1.0"},
			wantErr: "invalid duration: -1, must be non-negative",
		},
		{
			name:    "wafer-diag invalid sleep-scale",
			args:    []string{"wafer-diag", "--systems", "system1", "--sleep-scale", "0"},
			wantErr: "invalid sleep scale: 0, must be greater than 0", // Sleep scale validation might be needed
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeFactory := NewFakeClientFactory()
			fakeFactory.csAdmClient.responseOrErr = []interface{}{
				&pb.CreateSystemMaintenanceResponse{
					Jobs: []*pb.SystemMaintenanceJob{
						{
							JobId:        "test-job",
							Namespace:    "test-namespace",
							WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG,
							Status:       pb.JobStatus_JOB_STATUS_CREATED,
							Systems:      []string{"system1"},
							Priority:     199,
						},
					},
				},
			}

			cmdCtx := pkg.CmdCtx{
				ClientFactory: fakeFactory,
				DebugLevel:    1,
			}

			cmd := newSysMaintCreateCmd(&cmdCtx)
			fakeOutErr := NewFakeOutErr()
			cmd.SetOut(fakeOutErr.O)
			cmd.SetErr(fakeOutErr.E)

			cmd.SetArgs(tc.args)
			err := cmd.Execute()

			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

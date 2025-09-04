package cmd

import (
	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"

	"cerebras.com/cluster/csctl/pkg"
)

func TestDebugArtifact(t *testing.T) {
	// Configure client factory
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
		ClientFactory:        clientFactory,
		Namespace:            "test-namespace",
	}

	existingExportId := "export-123"
	nonExistingExportId := "export-missing"

	t.Run("status with existing timestamp", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "get", existingExportId})

		testDate, err := time.Parse("2006-01-02", "2023-12-31")
		require.NoError(t, err)
		timestampProto := timestamppb.New(testDate)

		// Mock response from server
		clientFactory.AppendCsCtlResponse(&pb.GetDebugArtifactStatusResponse{
			RetentionDeadline: timestampProto,
		})

		require.NoError(t, cmd.Execute())
	})

	t.Run("status with no timestamp", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "get", existingExportId})

		// Mock response from server with empty timestamp
		clientFactory.AppendCsCtlResponse(&pb.GetDebugArtifactStatusResponse{
			RetentionDeadline: nil,
		})

		require.NoError(t, cmd.Execute())
	})

	t.Run("status with non-existing artifact", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "get", nonExistingExportId})

		// Mock error response
		clientFactory.AppendCsCtlErrorCode("artifact not found", codes.NotFound)

		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("update with retention-deadline flag", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "update", existingExportId, "--retention-deadline", "2025-01-01"})

		// Mock response from server
		clientFactory.AppendCsCtlResponse(&pb.UpdateDebugArtifactStatusResponse{
			Message: "Successfully updated retention timestamp for debug artifact " + existingExportId,
		})

		require.NoError(t, cmd.Execute())
	})

	t.Run("update with mark-delete flag", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "update", existingExportId, "--mark-delete"})

		// Mock response from server
		clientFactory.AppendCsCtlResponse(&pb.UpdateDebugArtifactStatusResponse{
			Message: "Successfully updated retention timestamp for debug artifact " + existingExportId,
		})

		require.NoError(t, cmd.Execute())
	})

	t.Run("update with non-existing artifact", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "update", nonExistingExportId, "--retention-deadline", "2025-01-01"})

		// Mock error response
		clientFactory.AppendCsCtlErrorCode("artifact not found", codes.NotFound)

		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("update with no flags", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "update", existingExportId})

		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must specify either --retention-deadline or --mark-delete")
	})

	t.Run("update with conflicting flags", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "update", existingExportId, "--retention-deadline", "2025-01-01", "--mark-delete"})

		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot specify both --retention-deadline and --mark-delete")
	})

	t.Run("with missing job ID", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"debug-artifact", "get"})

		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing required argument JOB_ID")
	})
}

package csctl

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
)

func TestDebugArtifact(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()
	namespace := "test-namespace"
	exportId := "test-export"
	debugArtifactDir := path.Join(tmpDir, namespace)
	exportDir := path.Join(debugArtifactDir, exportId)

	// Create test directories
	require.NoError(t, os.MkdirAll(exportDir, 0755))

	// Create a valid job spec YAML file
	validYamlContent := `
apiVersion: "jobs.cerebras.com/v1"
kind: WSJob
metadata:
  name: test-export
  namespace: test-namespace
spec:
  user:
    uid: 1000
    gid: 1000
    username: testuser
    groups:
    - 1000
    - 2000
`
	validYamlPath := path.Join(exportDir, exportId+".yaml")
	require.NoError(t, os.WriteFile(validYamlPath, []byte(validYamlContent), 0644))

	// Create server with test configuration
	server := &Server{
		options: wsclient.ServerOptions{
			JobPathConfig: wsclient.JobPathConfig{
				DebugArtifactRootPath: tmpDir,
			},
		},
	}

	// Create base context
	ctx := context.TODO()
	// Add namespace to context
	incomingMD, _ := metadata.FromIncomingContext(ctx)
	newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, namespace)
	ctx = metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))

	// Add client metadata to context
	clientMeta := pkg.ClientMeta{
		UID:      1000,
		GID:      1000,
		Username: "testuser",
		Groups:   []int64{1000, 2000},
	}
	ctx = context.WithValue(ctx, pkg.ClientMetaKey, clientMeta)

	t.Run("GetDebugArtifactStatus", func(t *testing.T) {
		t.Run("with existing retention marker", func(t *testing.T) {
			// Create retention marker file
			retentionMarkerPath := path.Join(exportDir, retentionMarkerFileName)
			require.NoError(t, os.WriteFile(retentionMarkerPath, []byte("2023-12-31"), 0644))

			// Test getting status
			req := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: exportId,
			}
			res, err := server.GetDebugArtifactStatus(ctx, req)

			require.NoError(t, err)
			require.NotNil(t, res.RetentionDeadline)
			date := res.RetentionDeadline.AsTime().Format("2006-01-02")
			assert.Equal(t, "2023-12-31", date)
		})

		t.Run("with no retention marker", func(t *testing.T) {
			// Remove retention marker if it exists
			retentionMarkerPath := path.Join(exportDir, retentionMarkerFileName)
			os.Remove(retentionMarkerPath)

			// Test getting status
			req := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: exportId,
			}
			res, err := server.GetDebugArtifactStatus(ctx, req)

			require.NoError(t, err)
			assert.Nil(t, res.RetentionDeadline)
		})

		t.Run("with non-existing export", func(t *testing.T) {
			req := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: "non-existing-export",
			}
			_, err := server.GetDebugArtifactStatus(ctx, req)

			require.Error(t, err)
			status, ok := grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.NotFound, status.Code())
		})

		t.Run("with non-existing namespace", func(t *testing.T) {
			// Create context with non-existing namespace
			newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, "non-existing-namespace")
			newCtx := metadata.NewIncomingContext(ctx, newMD)

			req := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: exportId,
			}
			_, err := server.GetDebugArtifactStatus(newCtx, req)

			require.Error(t, err)
			status, ok := grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.NotFound, status.Code())
		})
	})

	t.Run("UpdateDebugArtifactStatus", func(t *testing.T) {
		t.Run("with valid timestamp", func(t *testing.T) {
			// Create a protobuf timestamp for "2023-12-31"
			testDate, err := time.Parse("2006-01-02", "2023-12-31")
			require.NoError(t, err)
			timestampProto := timestamppb.New(testDate)

			req := &csctlpb.UpdateDebugArtifactStatusRequest{
				ExportId:          exportId,
				RetentionDeadline: timestampProto,
			}
			res, err := server.UpdateDebugArtifactStatus(ctx, req)

			require.NoError(t, err)
			assert.Contains(t, res.Message, "Successfully updated")

			// Verify file was created with correct content
			retentionMarkerPath := path.Join(exportDir, retentionMarkerFileName)
			content, err := os.ReadFile(retentionMarkerPath)
			require.NoError(t, err)
			assert.Equal(t, "2023-12-31", string(content))
		})

		t.Run("with invalid timestamp format", func(t *testing.T) {
			// This test is no longer relevant since we're using proper timestamp objects
			// which are validated by the protobuf library. We'll test nil timestamp instead.
			req := &csctlpb.UpdateDebugArtifactStatusRequest{
				ExportId:          exportId,
				RetentionDeadline: nil,
			}
			_, err := server.UpdateDebugArtifactStatus(ctx, req)

			require.Error(t, err)
			status, ok := grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.InvalidArgument, status.Code())
		})

		t.Run("with non-existing export", func(t *testing.T) {
			// Create a protobuf timestamp for "2023-12-31"
			testDate, err := time.Parse("2006-01-02", "2023-12-31")
			require.NoError(t, err)
			timestampProto := timestamppb.New(testDate)

			req := &csctlpb.UpdateDebugArtifactStatusRequest{
				ExportId:          "non-existing-export",
				RetentionDeadline: timestampProto,
			}
			_, err = server.UpdateDebugArtifactStatus(ctx, req)

			require.Error(t, err)
			status, ok := grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.NotFound, status.Code())
		})

		t.Run("with non-existing namespace", func(t *testing.T) {
			// Create context with non-existing namespace
			newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, "non-existing-namespace")
			newCtx := metadata.NewIncomingContext(ctx, newMD)

			// Create a protobuf timestamp for "2023-12-31"
			testDate, err := time.Parse("2006-01-02", "2023-12-31")
			require.NoError(t, err)
			timestampProto := timestamppb.New(testDate)

			req := &csctlpb.UpdateDebugArtifactStatusRequest{
				ExportId:          exportId,
				RetentionDeadline: timestampProto,
			}
			_, err = server.UpdateDebugArtifactStatus(newCtx, req)

			require.Error(t, err)
			status, ok := grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.NotFound, status.Code())
		})
	})

	t.Run("Permission Checking", func(t *testing.T) {
		// Setup for permission tests
		permissionTestsDir := path.Join(tmpDir, "permission-tests")
		require.NoError(t, os.MkdirAll(permissionTestsDir, 0755))

		// Create an export ID
		testExportId := "permission-test-export"
		testExportDir := path.Join(permissionTestsDir, testExportId)
		require.NoError(t, os.MkdirAll(testExportDir, 0755))

		// Create a valid job spec YAML file
		validYamlContent := `
apiVersion: "jobs.cerebras.com/v1"
kind: WSJob
metadata:
  name: permission-test-export
  namespace: permission-tests
spec:
  user:
    uid: 1000
    gid: 1000
    username: testuser
    groups:
    - 1000
    - 2000
`
		validYamlPath := path.Join(testExportDir, testExportId+".yaml")
		require.NoError(t, os.WriteFile(validYamlPath, []byte(validYamlContent), 0644))

		// Create an invalid YAML file
		invalidYamlPath := path.Join(testExportDir, "invalid.yaml")
		require.NoError(t, os.WriteFile(invalidYamlPath, []byte("invalid yaml content"), 0644))

		// Create a server with test configuration
		permServer := &Server{
			options: wsclient.ServerOptions{
				JobPathConfig: wsclient.JobPathConfig{
					DebugArtifactRootPath: tmpDir,
				},
			},
		}

		// Create context with the test namespace
		permCtx := context.TODO()
		permMD := metadata.Pairs(pkg.ResourceNamespaceHeader, "permission-tests")
		permCtx = metadata.NewIncomingContext(permCtx, permMD)

		t.Run("with user having permission", func(t *testing.T) {
			// Create context with matching UID/GID
			clientMeta := pkg.ClientMeta{
				UID:      1000,
				GID:      1000,
				Username: "testuser",
				Groups:   []int64{1000, 3000},
			}
			permCtxWithUser := context.WithValue(permCtx, pkg.ClientMetaKey, clientMeta)

			// Create retention marker file
			retentionMarkerPath := path.Join(testExportDir, retentionMarkerFileName)
			require.NoError(t, os.WriteFile(retentionMarkerPath, []byte("2024-01-15"), 0644))

			// Test GetDebugArtifactStatus
			getReq := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: testExportId,
			}
			getRes, err := permServer.GetDebugArtifactStatus(permCtxWithUser, getReq)

			require.NoError(t, err)
			require.NotNil(t, getRes)
			date := getRes.RetentionDeadline.AsTime().Format("2006-01-02")
			assert.Equal(t, "2024-01-15", date)

			// Test UpdateDebugArtifactStatus
			testDate, err := time.Parse("2006-01-02", "2024-02-01")
			require.NoError(t, err)
			timestampProto := timestamppb.New(testDate)

			updateReq := &csctlpb.UpdateDebugArtifactStatusRequest{
				ExportId:          testExportId,
				RetentionDeadline: timestampProto,
			}
			updateRes, err := permServer.UpdateDebugArtifactStatus(permCtxWithUser, updateReq)

			require.NoError(t, err)
			require.NotNil(t, updateRes)
			assert.Contains(t, updateRes.Message, "Successfully updated")
		})

		t.Run("with user having permission through groups", func(t *testing.T) {
			// Create context with matching group
			clientMeta := pkg.ClientMeta{
				UID:      1234, // Different user ID
				GID:      5678, // Different primary group
				Username: "otheruser",
				Groups:   []int64{9999, 2000}, // But shares group 2000
			}
			permCtxWithUser := context.WithValue(permCtx, pkg.ClientMetaKey, clientMeta)

			// Test GetDebugArtifactStatus
			getReq := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: testExportId,
			}
			getRes, err := permServer.GetDebugArtifactStatus(permCtxWithUser, getReq)

			require.NoError(t, err)
			require.NotNil(t, getRes)
		})

		t.Run("with user not having permission", func(t *testing.T) {
			// Create context with non-matching UID/GID
			clientMeta := pkg.ClientMeta{
				UID:      2000,
				GID:      2001,
				Username: "unauthorized",
				Groups:   []int64{2001, 3000},
			}
			permCtxWithUser := context.WithValue(permCtx, pkg.ClientMetaKey, clientMeta)

			// Test GetDebugArtifactStatus
			getReq := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: testExportId,
			}
			_, err := permServer.GetDebugArtifactStatus(permCtxWithUser, getReq)

			require.Error(t, err)
			status, ok := grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, status.Code())

			// Test UpdateDebugArtifactStatus
			testDate, err := time.Parse("2006-01-02", "2024-02-01")
			require.NoError(t, err)
			timestampProto := timestamppb.New(testDate)

			updateReq := &csctlpb.UpdateDebugArtifactStatusRequest{
				ExportId:          testExportId,
				RetentionDeadline: timestampProto,
			}
			_, err = permServer.UpdateDebugArtifactStatus(permCtxWithUser, updateReq)

			require.Error(t, err)
			status, ok = grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, status.Code())
		})

		t.Run("with non-existing YAML file", func(t *testing.T) {
			// Use a valid user, but with a non-existent export ID
			clientMeta := pkg.ClientMeta{
				UID:      1000,
				GID:      1000,
				Username: "testuser",
				Groups:   []int64{1000, 2000},
			}
			permCtxWithUser := context.WithValue(permCtx, pkg.ClientMetaKey, clientMeta)

			nonExistentId := "non-existent-export"
			nonExistentDir := path.Join(permissionTestsDir, nonExistentId)
			require.NoError(t, os.MkdirAll(nonExistentDir, 0755))

			// Create retention marker file to pass the directory existence check
			retentionMarkerPath := path.Join(nonExistentDir, retentionMarkerFileName)
			require.NoError(t, os.WriteFile(retentionMarkerPath, []byte("2024-01-15"), 0644))

			// Test GetDebugArtifactStatus
			getReq := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: nonExistentId,
			}
			_, err := permServer.GetDebugArtifactStatus(permCtxWithUser, getReq)

			require.Error(t, err)
			status, ok := grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, status.Code())
		})

		t.Run("with invalid YAML file", func(t *testing.T) {
			// Create an export ID with invalid YAML
			invalidExportId := "invalid-yaml-export"
			invalidExportDir := path.Join(permissionTestsDir, invalidExportId)
			require.NoError(t, os.MkdirAll(invalidExportDir, 0755))

			// Create an invalid YAML file
			invalidYamlPath := path.Join(invalidExportDir, invalidExportId+".yaml")
			require.NoError(t, os.WriteFile(invalidYamlPath, []byte("invalid yaml content"), 0644))

			// Create retention marker file to pass the directory existence check
			retentionMarkerPath := path.Join(invalidExportDir, retentionMarkerFileName)
			require.NoError(t, os.WriteFile(retentionMarkerPath, []byte("2024-01-15"), 0644))

			// Use a valid user
			clientMeta := pkg.ClientMeta{
				UID:      1000,
				GID:      1000,
				Username: "testuser",
				Groups:   []int64{1000, 2000},
			}
			permCtxWithUser := context.WithValue(permCtx, pkg.ClientMetaKey, clientMeta)

			// Test GetDebugArtifactStatus
			getReq := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: invalidExportId,
			}
			_, err := permServer.GetDebugArtifactStatus(permCtxWithUser, getReq)

			require.Error(t, err)
			status, ok := grpcStatus.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.PermissionDenied, status.Code())
		})

		t.Run("with root user", func(t *testing.T) {
			// Root user should always have permission
			clientMeta := pkg.ClientMeta{
				UID:      0,
				GID:      0,
				Username: "root",
				Groups:   []int64{0},
			}
			permCtxWithUser := context.WithValue(permCtx, pkg.ClientMetaKey, clientMeta)

			// Test GetDebugArtifactStatus
			getReq := &csctlpb.GetDebugArtifactStatusRequest{
				ExportId: testExportId,
			}
			getRes, err := permServer.GetDebugArtifactStatus(permCtxWithUser, getReq)

			require.NoError(t, err)
			require.NotNil(t, getRes)
		})
	})
}

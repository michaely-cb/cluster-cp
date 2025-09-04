package csctl

import (
	"cerebras.com/cluster/server/pkg"
	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"os"
	"path"
	"time"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"
)

const retentionMarkerFileName = ".retention-marker.out"

// checkPermissionFromYaml checks if the user has permission to access the debug artifact
// by reading the job spec YAML from disk instead of using k8s client
func (s *Server) checkPermissionFromYaml(ctx context.Context, exportId string, namespace string) error {
	// Path to the job spec YAML
	specYamlPath := path.Join(s.options.JobPathConfig.DebugArtifactRootPath, namespace, exportId, fmt.Sprintf("%s.yaml", exportId))

	// Check if job spec YAML exists
	if _, err := os.Stat(specYamlPath); os.IsNotExist(err) {
		log.Warnf("Job spec YAML does not exist at %s", specYamlPath)
		return grpcStatus.Errorf(codes.PermissionDenied, "Unable to verify permissions: job spec YAML not found. %s", pkg.SysAdminSupportMsg)
	}

	// Read the job spec YAML
	yamlData, err := os.ReadFile(specYamlPath)
	if err != nil {
		log.Warnf("Error reading job spec YAML %s: %v", specYamlPath, err)
		return grpcStatus.Errorf(codes.PermissionDenied, "Unable to verify permissions: failed to read job spec YAML. %s", pkg.SysAdminSupportMsg)
	}

	// Parse the job spec YAML into WSJob struct
	var wsjob wsapisv1.WSJob
	if err := yaml.Unmarshal(yamlData, &wsjob); err != nil {
		log.Warnf("Error unmarshalling job spec YAML %s: %v", specYamlPath, err)
		return grpcStatus.Errorf(codes.PermissionDenied, "Unable to verify permissions: failed to parse job spec YAML. %s", pkg.SysAdminSupportMsg)
	}

	// Get client metadata from context
	clientMeta, err := pkg.GetClientMetaFromContext(ctx)
	if err != nil {
		return err
	}

	// Check user permission
	return pkg.CheckUserPermission(clientMeta, &wsjob)
}

// GetDebugArtifactStatus returns the retention timestamp for a debug artifact
func (s *Server) GetDebugArtifactStatus(ctx context.Context, req *csctlpb.GetDebugArtifactStatusRequest) (*csctlpb.GetDebugArtifactStatusResponse, error) {
	namespace := pkg.GetNamespaceFromContext(ctx)

	// Ensure debug artifact directory exists
	debugArtifactDir := path.Join(s.options.JobPathConfig.DebugArtifactRootPath, namespace, req.ExportId)
	if _, err := os.Stat(debugArtifactDir); os.IsNotExist(err) {
		return nil, grpcStatus.Errorf(codes.NotFound, "Debug artifact directory does not exist for namespace %s", namespace)
	}

	// Check permission
	if err := s.checkPermissionFromYaml(ctx, req.ExportId, namespace); err != nil {
		return nil, err
	}

	// Path to the retention marker file
	retentionMarkerPath := path.Join(debugArtifactDir, retentionMarkerFileName)

	// Check if retention marker file exists
	if _, err := os.Stat(retentionMarkerPath); os.IsNotExist(err) {
		log.Infof("Retention marker file does not exist at %s", retentionMarkerPath)
		return &csctlpb.GetDebugArtifactStatusResponse{
			RetentionDeadline: nil,
		}, nil
	}

	// Read retention timestamp from file
	data, err := os.ReadFile(retentionMarkerPath)
	if err != nil {
		log.Warnf("Error reading retention marker file %s: %v", retentionMarkerPath, err)
		return nil, grpcStatus.Errorf(codes.Internal, "Failed to read retention marker: %v", err)
	}

	// Parse the date string (YYYY-MM-DD) to a timestamp
	dateStr := string(data)
	if dateStr == "" {
		return &csctlpb.GetDebugArtifactStatusResponse{
			RetentionDeadline: nil,
		}, nil
	}

	// Parse the date in YYYY-MM-DD format
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		log.Warnf("Invalid date format in retention marker file: %v", err)
		return nil, grpcStatus.Errorf(codes.Internal, "Failed to parse retention date: %v", err)
	}

	// Convert to a protobuf timestamp
	pbTimestamp := timestamppb.New(t)

	return &csctlpb.GetDebugArtifactStatusResponse{
		RetentionDeadline: pbTimestamp,
	}, nil
}

// UpdateDebugArtifactStatus updates the retention timestamp for a debug artifact
func (s *Server) UpdateDebugArtifactStatus(ctx context.Context, req *csctlpb.UpdateDebugArtifactStatusRequest) (*csctlpb.UpdateDebugArtifactStatusResponse, error) {
	namespace := pkg.GetNamespaceFromContext(ctx)

	// Convert Timestamp to ISO date string (YYYY-MM-DD)
	if req.RetentionDeadline == nil {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "timestamp is required")
	}

	t := req.RetentionDeadline.AsTime()
	dateStr := t.Format("2006-01-02")

	// Ensure export directory exists
	exportDir := path.Join(s.options.JobPathConfig.DebugArtifactRootPath, namespace, req.ExportId)
	if _, err := os.Stat(exportDir); os.IsNotExist(err) {
		return nil, grpcStatus.Errorf(codes.NotFound, "Export directory does not exist for job ID %s in namespace %s", req.ExportId, namespace)
	}

	// Check permission
	if err := s.checkPermissionFromYaml(ctx, req.ExportId, namespace); err != nil {
		return nil, err
	}

	// Path to the retention marker file
	retentionMarkerPath := path.Join(exportDir, retentionMarkerFileName)

	// Write retention timestamp to file
	err := os.WriteFile(retentionMarkerPath, []byte(dateStr), 0666)
	if err != nil {
		log.Warnf("Error writing retention marker file %s: %v", retentionMarkerPath, err)
		return nil, grpcStatus.Errorf(codes.Internal, "Failed to write retention marker: %v", err)
	}

	log.Infof("Updated retention timestamp for debug artifact %s to %s", req.ExportId, dateStr)
	return &csctlpb.UpdateDebugArtifactStatusResponse{
		Message: fmt.Sprintf("Successfully updated retention timestamp for debug artifact %s", req.ExportId),
	}, nil
}

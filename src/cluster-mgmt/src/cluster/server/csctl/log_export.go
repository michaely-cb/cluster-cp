package csctl

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"cerebras.com/job-operator/common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

func (s *Server) checkPermission(ctx context.Context, jobName string, namespace string) (*wsapisv1.WSJob, error) {
	wsjob, err := s.wsJobClient.Get(namespace, jobName)
	if err != nil {
		log.Warnf("wsjob %s in namespace %s does not exist", jobName, namespace)
		wsjob = nil
	}
	clientMeta, err := pkg.GetClientMetaFromContext(ctx)
	return wsjob, pkg.CheckUserPermission(clientMeta, wsjob)
}

// CreateLogExport handles the initial step in requesting logs, creating the k8s job to gather files
func (s *Server) CreateLogExport(ctx context.Context, req *csctlpb.CreateLogExportRequest) (*csctlpb.CreateLogExportResponse, error) {
	namespace := pkg.GetNamespaceFromContext(ctx)
	wsjob, err := s.checkPermission(ctx, req.Name, namespace)
	if err != nil {
		return nil, err
	}

	coordRole := fmt.Sprintf("%scoordinator", K8sRoleLabelPrefix)
	nodeSelector := map[string]string{
		coordRole: "",
	}
	areNodesNsLabeled := false
	if s.nodeLister.Informer != nil && s.nodeLister.AreNodesNamespaceLabeled() {
		areNodesNsLabeled = true
		nodeSelector[common.NamespaceLabelKey] = namespace
	}

	nodes, err := s.nodeLister.List()
	if err != nil {
		return nil, err
	}

	found := false
	for _, n := range nodes {
		if _, ok := n.Labels[coordRole]; ok {
			if areNodesNsLabeled {
				if n.Labels[common.NamespaceLabelKey] == namespace {
					found = true
					break
				}
			} else {
				found = true
				break
			}
		}
	}
	if !found {
		// in the event where we cannot find a suitable node, we fall back to use the same node where the cluster server is running
		nodeSelector = map[string]string{
			"kubernetes.io/hostname": os.Getenv("MY_NODE_NAME"),
		}
	}

	clientMeta, err := pkg.GetClientMetaFromContext(ctx)

	if err != nil {
		return nil, grpcStatus.Errorf(codes.Internal, "Meta details not found in context")
	}

	jobSpec := &wsclient.JobSpec{
		Namespace:             namespace,
		JobName:               req.Name,
		JobPathConfig:         s.options.JobPathConfig,
		LogExportNodeSelector: nodeSelector,
	}

	if req.ExportTargetType == csctlpb.ExportTargetType_EXPORT_TARGET_TYPE_NFS {
		jobSpec.JobUser = wsapisv1.JobUser{
			Uid:      clientMeta.UID,
			Gid:      clientMeta.GID,
			Username: clientMeta.Username,
			Groups:   clientMeta.Groups,
		}
	}

	workdirLogsMountDir := ""
	cachedCompileMountDir := ""
	if wsjob != nil && wsjob.Annotations != nil {
		workdirLogsMountDir = wsjob.Annotations[wsapisv1.WorkdirLogsMountDirAnnot]
		cachedCompileMountDir = wsjob.Annotations[wsapisv1.CachedCompileMountDirAnnot]
	}

	log.Infof("Log export job: %s, workdir logs mount dir: %s, cached compile mount dir: %s",
		fmt.Sprintf("%s/%s", namespace, req.Name), workdirLogsMountDir, cachedCompileMountDir)

	if workdirLogsMountDir != "" {
		volumes, mounts, err := wsclient.ResolveMountDirs(ctx, namespace, s.k8s, []*pb.MountDir{{
			Path:          workdirLogsMountDir,
			ContainerPath: workdirLogsMountDir,
		}})
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		jobSpec.WorkdirLogsVolume = volumes[0]
		jobSpec.WorkdirLogsVolume.Name = wsapisv1.WorkdirVolume
		jobSpec.WorkdirLogsVolumeMount = mounts[0]
		jobSpec.WorkdirLogsVolumeMount.Name = wsapisv1.WorkdirVolume
	} else {
		err = wsclient.GetWorkdirVolumeAndMount(ctx, s.k8s, jobSpec, "")
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
	}
	if cachedCompileMountDir != "" {
		volumes, mounts, err := wsclient.ResolveMountDirs(ctx, namespace, s.k8s, []*pb.MountDir{{
			Path:          cachedCompileMountDir,
			ContainerPath: cachedCompileMountDir,
		}})
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		jobSpec.CachedCompileVolume = volumes[0]
		jobSpec.CachedCompileVolume.Name = wsapisv1.CachedCompileVolume
		jobSpec.CachedCompileVolumeMount = mounts[0]
		jobSpec.CachedCompileVolumeMount.Name = wsapisv1.CachedCompileVolume
	} else {
		err = wsclient.GetCachedCompileVolumeAndMount(ctx, s.k8s, jobSpec, "", s.options.HasMultiMgmtNodes)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
	}

	err = jobSpec.SetLogExportVolumeAndMount(ctx, s.k8s, req.ExportTargetType, req.OutputPath, s.options.HasMultiMgmtNodes)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	logExportJob, err := s.wsJobClient.LogExport(req, jobSpec)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, pkg.MapStatusError(grpcStatus.Errorf(codes.Internal, "failed to create log export job: %v", err))
	}

	return &csctlpb.CreateLogExportResponse{
		ExportId: logExportJob.Name,
	}, nil
}

// GetLogExport provides information about outstanding requests for logs
func (s *Server) GetLogExport(ctx context.Context, req *csctlpb.GetLogExportRequest) (*csctlpb.GetLogExportResponse, error) {
	// Check if job is still running...
	namespace := pkg.GetNamespaceFromContext(ctx)
	job, err := s.wsJobClient.Clientset.BatchV1().Jobs(namespace).Get(ctx, req.ExportId, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, grpcStatus.Errorf(codes.Unknown, "Could not find log-export job: %s (%v)", req.ExportId, err)
		} else {
			return nil, grpcStatus.Errorf(codes.Unknown, "Error attempting to get job: %s (%v)", req.ExportId, err)
		}
	}

	response := &csctlpb.GetLogExportResponse{}
	if s.isJobSuccess(job) {
		response.Status = csctlpb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED
		response.StatusDetail = "Export job done success"
		return response, nil
	} else if s.hadJobTimedOut(job) {
		response.Status = csctlpb.LogExportStatus_LOG_EXPORT_STATUS_FAILED
		response.StatusDetail = fmt.Sprintf("Job timed out after %d seconds", *job.Spec.ActiveDeadlineSeconds)
		return response, nil
	} else if s.isJobFailed(job) {
		logs, err := s.wsJobClient.GetJobLogs(ctx, job.Namespace, job.Name, 20)
		if err != nil {
			return nil, grpcStatus.Errorf(codes.Internal, "Job failed and also failed to get logs: %s (%v)", req.ExportId, err)
		}
		response.Status = csctlpb.LogExportStatus_LOG_EXPORT_STATUS_FAILED
		response.StatusDetail = logs
		return response, nil
	}

	logs, err := s.wsJobClient.GetJobLogs(ctx, job.Namespace, job.Name, 1)
	if err != nil {
		log.Warnf("unable to get job logs %s", err)
	}
	if logs != "" {
		logs = fmt.Sprintf("Export job logs:\n %s", logs)
	}
	response.Status = csctlpb.LogExportStatus_LOG_EXPORT_STATUS_RUNNING
	response.StatusDetail = fmt.Sprintf("Export job is running. \n%s", logs)
	return response, nil
}

// DownloadLogExport is the final step in the process of collecting logs, transmitting it in chunks.
func (s *Server) DownloadLogExport(ctx context.Context, req *csctlpb.DownloadLogExportRequest) (*csctlpb.DownloadLogExportResponse, error) {
	// NOTE: This protocol has three phases: initial request, recurring request, and the final message from server.
	// 			initial - Client sends a req.PageToken that is an empty string ""
	//      	recurring - The 'NextPageToken' from the server tells the client what to ask for next
	//      	final - With the last chunk, the NextPageToken from server is blank, telling client it's done
	// NOTE: PageToken is just the byte offset of the file being transmitted, but is left open to future changes.
	//       Only the server knows that it's the byte offset. The client just writes out what it gets and moves on.
	const pageSize int64 = 2097152 //== 2MB, gRPC max msg size is 4MB
	namespace := pkg.GetNamespaceFromContext(ctx)
	var fileName = path.Join(s.options.JobPathConfig.LogExportRootPath, namespace, fmt.Sprint(req.ExportId, ".zip"))

	var readOffset int64 = 0
	var err error
	if len(req.PageToken) != 0 {
		readOffset, err = strconv.ParseInt(req.PageToken, 10, 64)
		if err != nil {
			return nil, grpcStatus.Errorf(codes.Unknown, "Error attempting to convert page token: %s (%v)", req.PageToken, err)
		}
	}
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		return nil, grpcStatus.Errorf(codes.Unknown, "Error attempting to open log archive: %s (%v)", fileName, err)
	}
	_, err = f.Seek(readOffset, 0)
	if err != nil {
		return nil, grpcStatus.Errorf(codes.Unknown, "Error attempting to seek into log archive: %s @ pos == %d (%v)",
			req.ExportId, readOffset, err)
	}

	err = nil
	dataPage := make([]byte, pageSize)
	bytesRead, err := f.Read(dataPage)
	if err != nil {
		if err != io.EOF {
			// A good Read() can return data + io.EOF, so only issue a hard gRPC error if it's not io.EOF
			return nil, grpcStatus.Errorf(codes.Unknown, "Error attempting to read from log archive: %s @ pos == %d (%v)",
				req.ExportId, readOffset, err)
		}
	}

	// This logic prevents sending a NextPageToken back in the case of a file size that lands
	// exactly on a page boundary, which would lead to a final request and response that didn't need to happen.
	var nextByteOffset = ""
	if int64(bytesRead) == pageSize {
		if err != io.EOF {
			nextByteOffset = fmt.Sprintf("%d", readOffset+pageSize)
		}
	}

	// Compute a unique signature of this archive.
	archiveInfo, _ := f.Stat()
	modifiedTime := fmt.Sprint(archiveInfo.ModTime())

	// Provide metadata useful for estimating "time remaining" in the client UI
	// NOTE: This is NOT how clients should determine the transmission is complete,
	//       rather it is solely for enhancing user interfaces. In cases where the client
	//       is tailing a queue of objects, the 'EstimatedRemainingBytes' value can
	//       actually grow, instead of shrink. Under any circumstances, when the server
	//       is done sending information to the client for this session, it will be
	//       responding back with a blank NextPageToken.
	remainingBytes := int64(0)
	if nextByteOffset != "" {
		remainingBytes = archiveInfo.Size() - readOffset - int64(bytesRead)
	} else {
		// Failing to delete shouldn't fail the entire log export.
		// We have an internal cleanup routine to clean after the artifacts.
		// The benefit of deleting right after transmitting is to promote
		// more efficient disk space usage in the appliance.

		// Best-effort approach to delete the staging artifacts and the log export job
		var logExportZip = path.Join(s.options.JobPathConfig.LogExportRootPath, namespace, fmt.Sprintf("%s.zip", req.ExportId))
		err = os.Remove(logExportZip)
		if err != nil {
			log.Warnf("Error deleting file %s: %v", logExportZip, err)
		} else {
			log.Infof("Successfully deleted file %s", logExportZip)
		}

		// delete job and pod if it succeeded
		bgDelete := metav1.DeletePropagationBackground
		delOptions := metav1.DeleteOptions{
			PropagationPolicy: &bgDelete,
		}
		err = s.wsJobClient.Clientset.BatchV1().Jobs(namespace).Delete(ctx, req.ExportId, delOptions)
		if err != nil && apierrors.IsNotFound(err) {
			log.Warnf("Error failing to delete log export job %s: %v", req.ExportId, err)
		}
	}

	return &csctlpb.DownloadLogExportResponse{
		Chunk:                   dataPage[0:bytesRead],
		NextPageToken:           nextByteOffset,
		Signature:               modifiedTime,
		EstimatedRemainingBytes: remainingBytes,
	}, nil
}

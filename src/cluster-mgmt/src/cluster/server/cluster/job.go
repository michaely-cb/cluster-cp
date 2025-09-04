// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonutil "github.com/kubeflow/common/pkg/util"

	"cerebras.com/cluster/server/csctl"
	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	commonpb "cerebras/pb/workflow/appliance/common"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

const (
	compileCacheAnnoKey = "cerebras/compile_artifact_dir"
)

var cbcoreUnsetError = grpcStatus.Error(codes.InvalidArgument, "Cbcore image is not set")

func (server ClusterServer) InitCompileJob(
	ctx context.Context,
	request *pb.CompileJobRequest,
) (*pb.CompileJobResponse, error) {
	meta, ok := ctx.Value(pkg.ClientMetaKey).(pkg.ClientMeta)
	if !ok {
		return nil, grpcStatus.Errorf(codes.Internal, "meta details not found in context")
	}

	specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)

	if request.JobMode == nil {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "job mode not set")
	}

	jobMode := wscommon.Ternary(request.JobMode.Job == commonpb.JobMode_COMPILE, wsclient.CompileJobMode, wsclient.InferenceCompileJobMode)
	jobSpec, err := server.newJobSpec(ctx, wscommon.Namespace, 0,
		request.GetCompileDirRelativePath(), nil, request.DebugArgs,
		nil, meta, jobMode, request.UserNotifications)
	if err != nil {
		return nil, err
	}
	jobSpec.ClientWorkdir = request.ClientWorkdir

	for _, task := range request.JobMode.ClusterDetails.Tasks {
		wsTaskType, ok := pbToWsTaskType[task.TaskType]
		if !ok {
			continue // skip WSE
		}

		serviceSpec := newServiceSpec(task)
		server.applyServerConfig(&serviceSpec, wsTaskType, request.CbcoreImage, request.DebugArgs)
		if serviceSpec.Image == "" {
			return nil, cbcoreUnsetError
		}

		// One prerequisite of the compile workflow is a fabric.json. We construct the cluster details with
		// CM address set to `~ws_compile_only~:0`. The Framework code will use the fabric.json stored
		// inside cbcore for compile.
		// There will be multiple fabric.json stored in cbcore. To determine which fabric.json to use, we follow
		// the following rules, order by their importance:
		// 1. If COMPILE_FABRIC_TYPE is set, it explicitly defines which fabric.json file to use.
		// 2. If COMPILE_SYSTEM_TYPE is set, it uses the default fabric.json based on the system type.
		// 3. Otherwise, fall back to FYN.
		//
		// The value for COMPILE_FABRIC_TYPE should be the same as the fabric.json filename stored in
		// cbcore inside `/cbcore/src/infra/fabric` directory. For example, a valid fabric type is
		// `fyn_std_5`, since there is a file `/cbcore/src/infra/fabric/fyn_std_5.json` in cbcore.
		// COMPILE_FABRIC_TYPE is defined in INI.
		serviceSpec.Env = append(serviceSpec.Env,
			corev1.EnvVar{Name: wsapisv1.DebugArgsEnv, Value: request.DebugArgsJson},
			corev1.EnvVar{Name: wsapisv1.CompileSystemTypeEnv, Value: server.options.CompileSystemType},
			corev1.EnvVar{Name: wsapisv1.CompileFabricTypeEnv, Value: jobSpec.CompileFabricType},
			corev1.EnvVar{Name: wsapisv1.MaxTrainFabricFreqMhzEnv, Value: server.options.MaxTrainFabricFreqMhz},
			corev1.EnvVar{Name: wsapisv1.CompileRootDirEnv, Value: jobSpec.CachedCompileRootPath},
			corev1.EnvVar{Name: wsapisv1.RelativeCompileDirEnv, Value: jobSpec.CachedCompileRelativePath},
		)

		specs[wsTaskType] = serviceSpec
	}

	wsjob, err := wsclient.BuildWSJobFromSpecs(specs, *jobSpec)
	if err != nil {
		return nil, err
	}

	job, err := server.wsJobClient.Create(wsjob)
	if err != nil {
		return nil, err
	} else {
		log.Infof("compile WSJob %s created", job.Name)
	}

	err = pkg.GenerateClusterDetailsConfig(ctx, server.kubeClientSet, job, request.JobMode.ClusterDetails)
	if err != nil {
		server.cancelJobOnClusterDetailsCreateErrors(job)
		return nil, err
	}

	err = server.createTopicIfApplicable(job.Name, request.DebugArgs)
	if err != nil {
		return nil, err
	}

	response := pb.CompileJobResponse{
		WorkflowId:             meta.WorkflowID,
		JobId:                  job.Name,
		DebugArgs:              request.DebugArgs,
		LogPath:                jobSpec.WorkdirLogPath,
		CompileDirAbsolutePath: jobSpec.CachedCompileVolumeMount.MountPath,
	}

	return &response, nil
}

func (server ClusterServer) FinalizeCompileJob(
	ctx context.Context,
	request *pb.CompileResultRequest,
) (*pb.CompileResultResponse, error) {
	wsjob, err := server.wsJobClient.Get(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, err
	}
	_, err = server.wsJobClient.Annotate(wsjob,
		map[string]string{
			compileCacheAnnoKey: request.CompileArtifactDir,
		})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	return &pb.CompileResultResponse{
		Message: "Successfully finalized compile job",
	}, err
}

func (server ClusterServer) InitExecuteJob(
	ctx context.Context,
	request *pb.ExecuteJobRequest,
) (*pb.ExecuteJobResponse, error) {
	meta, ok := ctx.Value(pkg.ClientMetaKey).(pkg.ClientMeta)
	if !ok {
		return nil, grpcStatus.Errorf(codes.Internal, "meta details not found in context")
	}

	if request.JobMode == nil || request.JobMode.ClusterDetails == nil {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "JobMode.ClusterDetails is not set")
	}

	specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)

	numWafers := 0
	actReplicaCount := 0
	var actToCsxDomains [][]int

	kvssTaskIndex := -1
	chfMemBase := int64(0)
	actMemBase := int64(0)
	kvssMemBase := int64(0)

	swDriverTaskIndex := -1
	swDriverMembase := int64(0)

	for i, task := range request.JobMode.ClusterDetails.Tasks {

		if task.TaskType == commonpb.ClusterDetails_TaskInfo_WSE {
			numWafers = len(task.TaskMap)
			continue
		}

		wsTaskType, ok := pbToWsTaskType[task.TaskType]
		if !ok {
			log.Warnf("unknown task type %s, skipping translation to WsJob task", task.TaskType)
			continue
		}

		serviceSpec := newServiceSpec(task)
		server.applyServerConfig(&serviceSpec, wsTaskType, request.CbcoreImage, request.DebugArgs)
		if serviceSpec.Image == "" {
			return nil, cbcoreUnsetError
		}
		serviceSpec.Env = append(serviceSpec.Env, corev1.EnvVar{Name: wsapisv1.DebugArgsEnv, Value: request.DebugArgsJson})
		specs[wsTaskType] = serviceSpec

		if task.TaskType == commonpb.ClusterDetails_TaskInfo_ACT {
			if task.ResourceInfo != nil {
				actMemBase = task.ResourceInfo.MemoryBytes
			}
			actReplicaCount = len(task.TaskMap)
			for _, taskMap := range task.TaskMap {
				// The first two conditions are here so that request from older clients won't make the server panic.
				// TODO: Remove this in rel-2.4
				if taskMap != nil && taskMap.AddressBook != nil && len(taskMap.AddressBook.WseDomains) > 0 {
					var domains []int
					for _, domain := range taskMap.AddressBook.WseDomains {
						domains = append(domains, int(domain))
					}
					actToCsxDomains = append(actToCsxDomains, domains)
				}
			}
		} else if task.TaskType == commonpb.ClusterDetails_TaskInfo_CHF {
			if task.ResourceInfo != nil {
				chfMemBase = task.ResourceInfo.MemoryBytes
			}
		} else if task.TaskType == commonpb.ClusterDetails_TaskInfo_KVSS {
			if task.ResourceInfo != nil {
				kvssMemBase = task.ResourceInfo.MemoryBytes
			}
			kvssTaskIndex = i
		} else if task.TaskType == commonpb.ClusterDetails_TaskInfo_SWD {
			if task.ResourceInfo != nil {
				swDriverMembase = task.ResourceInfo.MemoryBytes
			}
			swDriverTaskIndex = i
		}
	}

	if len(actToCsxDomains) > 0 {
		// The wio allocation from cluster details are against multiple CSX's and per-CSX allocations are exactly the same
		// Getting the allocations for any CSX is fine.
		numActPerCsx := actReplicaCount / numWafers
		actToCsxDomains = actToCsxDomains[:numActPerCsx]
	}

	if kvssTaskIndex != -1 {
		if actMemBase == 0 {
			return nil, grpcStatus.Errorf(codes.InvalidArgument, "Expected ACT task to have non-zero MemoryBytes")
		}

		if chfMemBase == 0 {
			return nil, grpcStatus.Errorf(codes.InvalidArgument, "Expected CHF task to have non-zero MemoryBytes.")
		}

		if kvssMemBase == 0 {
			return nil, grpcStatus.Errorf(codes.InvalidArgument, "Expected KVSS task to have non-zero MemoryBytes.")
		}
	}

	if swDriverTaskIndex != -1 && swDriverMembase == 0 {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "Expected SWD task to have non-zero MemoryBytes")
	}

	jobMode := wscommon.Ternary(request.JobMode.Job == commonpb.JobMode_EXECUTE, wsclient.ExecuteJobMode, wsclient.InferenceExecuteJobMode)
	jobSpec, err := server.newJobSpec(ctx, wscommon.Namespace, numWafers,
		request.GetCompileDirRelativePath(), actToCsxDomains, request.DebugArgs,
		request.UserSidecarEnv, meta, jobMode, request.UserNotifications)
	if err != nil {
		return nil, err
	}

	for taskType := range specs {
		if taskType == int(wsclient.WsCoordinator) {
			specs[taskType].Env = append(specs[taskType].Env,
				corev1.EnvVar{
					Name:  wsapisv1.CompileRootDirEnv,
					Value: jobSpec.CachedCompileRootPath,
				},
				corev1.EnvVar{
					Name:  wsapisv1.RelativeCompileDirEnv,
					Value: jobSpec.CachedCompileRelativePath,
				},
				corev1.EnvVar{
					Name:  wsapisv1.CompileArtifactDirEnv,
					Value: request.GetCompileArtifactDir(),
				},
				corev1.EnvVar{
					Name:  wsapisv1.DebugVizUrlPrefixEnv,
					Value: server.options.DebugVizUrlPrefix,
				},
			)
		}
	}

	if len(request.GetMountDirs()) > 0 {
		jobSpec.WorkerUserVolumes, jobSpec.WorkerUserVolumeMounts, err = wsclient.ResolveMountDirs(
			ctx, wscommon.Namespace, server.kubeClientSet, request.GetMountDirs())
		if err != nil {
			return nil, grpcStatus.Errorf(codes.InvalidArgument, err.Error())
		}
	}

	jobSpec.WorkerPythonPaths = request.PythonPaths
	jobSpec.ClientWorkdir = request.ClientWorkdir

	wsjob, err := wsclient.BuildWSJobFromSpecs(specs, *jobSpec)
	if err != nil {
		return nil, err
	}

	job, err := server.wsJobClient.Create(wsjob)
	if err != nil {
		return nil, err
	} else {
		log.Infof("execute WSJob %s created", job.Name)
	}

	err = pkg.GenerateClusterDetailsConfig(ctx, server.kubeClientSet, job, request.JobMode.ClusterDetails)
	if err != nil {
		server.cancelJobOnClusterDetailsCreateErrors(job)
		return nil, err
	}

	err = server.createTopicIfApplicable(job.Name, request.DebugArgs)
	if err != nil {
		log.Warnf("Failed to create topic for %s", job.Name)
	}

	response := pb.ExecuteJobResponse{
		WorkflowId: meta.WorkflowID,
		JobId:      job.Name,
		DebugArgs:  request.DebugArgs,
		LogPath:    jobSpec.WorkdirLogPath,
	}
	return &response, nil
}

func (server ClusterServer) InitSdkJob(
	ctx context.Context,
	numWafers int,
	jobMode *commonpb.JobMode,
	wsjobImage string,
	debugArgs *commonpb.DebugArgs,
	compileDirRelativePath string,
) (*wsapisv1.WSJob, string, error) {
	meta, ok := ctx.Value(pkg.ClientMetaKey).(pkg.ClientMeta)
	if !ok {
		return nil, "", grpcStatus.Errorf(codes.Internal, "meta details not found in context")
	}

	specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)

	if jobMode == nil {
		return nil, "", grpcStatus.Errorf(codes.InvalidArgument, "job mode not set")
	}

	// Starting from rel 2.1, SDK appliance client retrieves the cached compile
	// at the end of compile job via gRPC. Once the execute job is brought up,
	// the SDK appliance client will send the cached compile to the execute
	// worker via gRPC. For SDK execute jobs, the worker won't have local access
	// to the cached compile artifacts, and therefore we don't need to apply the
	// corresponding volume mounts.
	wsJobMode := pkg.Ternary(jobMode.Job == commonpb.JobMode_SDK_COMPILE, wsclient.SdkCompileJobMode, wsclient.SdkExecuteJobMode)
	jobSpec, err := server.newJobSpec(ctx, wscommon.Namespace, numWafers, compileDirRelativePath, nil, debugArgs, nil, meta, wsJobMode, nil)
	if err != nil {
		return nil, "", err
	}

	for _, task := range jobMode.ClusterDetails.Tasks {
		wsTaskType, ok := pbToWsTaskType[task.TaskType]
		if !ok {
			continue // skip WSE
		}

		serviceSpec := newServiceSpec(task)
		server.applyServerConfig(&serviceSpec, wsTaskType, wsjobImage, debugArgs)
		if serviceSpec.Image == "" {
			return nil, "", cbcoreUnsetError
		}
		serviceSpec.Env = append(serviceSpec.Env, corev1.EnvVar{Name: wsapisv1.DebugArgsEnv, Value: "{}"})
		specs[wsTaskType] = serviceSpec
	}

	wsjob, err := wsclient.BuildWSJobFromSpecs(specs, *jobSpec)
	if err != nil {
		return nil, "", err
	}

	job, err := server.wsJobClient.Create(wsjob)
	if err != nil {
		return nil, "", err
	} else {
		log.Infof("%s job %s created", jobSpec.WsJobMode, job.Name)
	}

	err = pkg.GenerateClusterDetailsConfig(ctx, server.kubeClientSet, job, jobMode.ClusterDetails)
	if err != nil {
		server.cancelJobOnClusterDetailsCreateErrors(job)
		return nil, "", err
	}
	return job, jobSpec.WorkdirLogPath, nil
}

func (server ClusterServer) InitSdkCompileJob(
	ctx context.Context,
	request *pb.SdkCompileJobRequest,
) (*pb.SdkCompileJobResponse, error) {
	job, logPath, err := server.InitSdkJob(
		ctx, 0, request.JobMode, request.CbcoreImage, request.DebugArgs, request.GetCompileDirRelativePath())
	if err != nil {
		return nil, err
	}

	response := pb.SdkCompileJobResponse{
		WorkflowId: job.GetWorkflowId(),
		JobId:      job.Name,
		LogPath:    logPath,
	}
	return &response, nil
}

func (server ClusterServer) InitSdkExecuteJob(
	ctx context.Context,
	request *pb.SdkExecuteJobRequest,
) (*pb.SdkExecuteJobResponse, error) {
	// The SDK appliance client will be uploading caches to the execute job.
	// The compile relative path here does not have any effect.
	job, logPath, err := server.InitSdkJob(
		ctx, 1, request.JobMode, request.CbcoreImage, request.DebugArgs, "")
	if err != nil {
		return nil, err
	}

	response := pb.SdkExecuteJobResponse{
		WorkflowId: job.GetWorkflowId(),
		JobId:      job.Name,
		LogPath:    logPath,
	}
	return &response, nil
}

// TODO: Add unit tests
func (server ClusterServer) InitImageBuildJob(
	ctx context.Context,
	request *pb.InitImageBuildRequest,
) (*pb.ImageBuildResponse, error) {
	meta, ok := ctx.Value(pkg.ClientMetaKey).(pkg.ClientMeta)
	if !ok {
		return nil, grpcStatus.Errorf(codes.Internal, "meta details not found in context")
	}

	baseImage := server.options.ImageBuildBaseImage
	if len(request.BaseImageOverride) > 0 {
		baseImage = request.BaseImageOverride
		log.Infof("Overriding base image with %s.", request.BaseImageOverride)
	}

	response := pb.ImageBuildResponse{
		ImageReady:    false,
		MountUserVenv: false,
	}

	if server.options.DisableImageBuild {
		response.ImageReference = baseImage
		response.MountUserVenv = true
	} else {
		jobName := wsclient.GenImageBuildJobName(baseImage, request.PipOptions, request.FrozenDependencies)
		jobSpec := &wsclient.JobSpec{
			Namespace:     wscommon.Namespace,
			JobName:       jobName,
			JobPathConfig: server.options.JobPathConfig,
			JobUser: wsapisv1.JobUser{
				Uid:      meta.UID,
				Gid:      meta.GID,
				Username: meta.Username,
				Groups:   meta.Groups,
			},
		}
		// Use local storage for image build workdirs to avoid a separate infra setup step for nfs
		// In case of errors, the build log gets streamed back to client anyways.
		if err := wsclient.GetWorkdirVolumeAndMount(ctx, server.kubeClientSet, jobSpec, ""); err != nil {
			return nil, err
		}

		nodeSelector := map[string]string{}
		if server.nodeLister.AreNodesNamespaceLabeled() {
			nodeSelector[wscommon.NamespaceLabelKey] = jobSpec.Namespace
		}
		nsr, err := server.nsrCli.Get(ctx, jobSpec.Namespace, metav1.GetOptions{})
		if err == nil && len(nsr.Status.Nodegroups) > 0 {
			// every nodegroup has at least one worker node
			// use worker nodes if we have at least one nodegroup
			nodeSelector["k8s.cerebras.com/node-role-worker"] = ""
		} else {
			nodeSelector["k8s.cerebras.com/node-role-coordinator"] = ""
		}
		_, err = server.wsJobClient.BuildImage(jobSpec, baseImage, request.PipOptions, request.FrozenDependencies, nodeSelector)
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				log.Infof("%s already exists.", jobName)
			} else {
				return nil, pkg.MapStatusError(err)
			}
		}
		response.WorkflowId = meta.WorkflowID
		response.JobId = jobName
		response.LogPath = jobSpec.WorkdirLogPath
		response.ImageReference = wsclient.GetDestinationImage(baseImage, request.PipOptions, request.FrozenDependencies)
	}
	return &response, nil
}

func (server ClusterServer) GetJob(
	_ context.Context,
	request *pb.GetJobRequest,
) (*pb.GetJobResponse, error) {
	wsjob, err := server.getWsJob(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, err
	}

	uid := int64(0)
	gid := int64(0)
	var groups []int64
	if wsjob.Spec.User != nil {
		uid = wsjob.Spec.User.Uid
		gid = wsjob.Spec.User.Gid
		groups = wsjob.Spec.User.Groups
	}

	var jobEvents []*pb.JobEvent
	jobConditions := wsjob.Status.Conditions
	var lastCondition commonv1.JobCondition
	message := ""
	if len(jobConditions) > 0 {
		lastCondition = jobConditions[len(jobConditions)-1]
		message = jobConditions[len(jobConditions)-1].Message
	}

	var status pb.JobStatus
	switch lastCondition.Type {
	case commonv1.JobCreated:
		status = pb.JobStatus_JOB_STATUS_CREATED
	case commonv1.JobQueueing:
		status = pb.JobStatus_JOB_STATUS_IN_QUEUE
	case commonv1.JobScheduled:
		status = pb.JobStatus_JOB_STATUS_SCHEDULED
	case commonv1.JobInitializing:
		status = pb.JobStatus_JOB_STATUS_INIT
	case commonv1.JobRunning:
		status = pb.JobStatus_JOB_STATUS_IN_PROGRESS
	case commonv1.JobCancelled:
		status = pb.JobStatus_JOB_STATUS_CANCELLED
	case commonv1.JobSucceeded:
		status = pb.JobStatus_JOB_STATUS_SUCCEEDED
	case commonv1.JobFailing:
		status = pb.JobStatus_JOB_STATUS_FAILING
		jobEvents = server.getJobEventsWithExcludedReasons(wsjob)
	case commonv1.JobFailed:
		status = pb.JobStatus_JOB_STATUS_FAILED
		jobEvents = server.getJobEventsWithExcludedReasons(wsjob)
	default:
		status = pb.JobStatus_JOB_STATUS_UNSPECIFIED
	}

	dashboard := ""
	if !wsjob.Status.StartTime.IsZero() {
		startTime := wsjob.Status.StartTime.UnixMilli() - csctl.JobDashboardWindowBufferMs
		endTime := "now"
		if !wsjob.Status.CompletionTime.IsZero() {
			endTime = fmt.Sprintf("%d", wsjob.Status.CompletionTime.UnixMilli()+csctl.JobDashboardWindowBufferMs)
		}
		dashboard = fmt.Sprintf(
			"https://%s/d/WebHNShVz/wsjob-dashboard?orgId=1&var-wsjob=%s&from=%d&to=%s",
			csctl.GrafanaUrl,
			wsjob.Name,
			startTime,
			endTime)
	}
	response := pb.GetJobResponse{
		Status:           status,
		Message:          message,
		Uid:              uid,
		Gid:              gid,
		Groups:           groups,
		JobEvents:        jobEvents,
		Dashboard:        dashboard,
		ResourceReserved: wsjob.IsReserved(),
	}
	return &response, nil
}

// GetJobEvents retrieves a list of warning job events defined in the "allowReasons" list.
// Please note that Kubernetes events will be lost after 1 hour. As per Jan 2024, this API
// is only called right after ingress is ready and propagates scheduler related warnings to
// end users. If ingress took too long to become ready, then the event would disappear. It
// is an unlikely case for now. If this changes in the future, we can revisit how to propagate
// the events back.
func (server ClusterServer) GetJobEvents(_ context.Context, request *pb.GetJobEventsRequest) (*pb.GetJobEventsResponse, error) {
	wsjob, err := server.getWsJob(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, err
	}
	jobEvents := wscommon.Map(func(e wscommon.JobEvent) *pb.JobEvent {
		return &pb.JobEvent{
			Name:          e.Name,
			Reason:        e.Reason,
			Message:       e.Message,
			LastTimestamp: e.LastTimestamp.String(),
		}
	}, wscommon.GetWarningEventsForJob(server.kubeClientSet, wsjob))

	var events []*pb.JobEvent
	allowedReasons := map[string]bool{
		wscommon.PartialSystemAssignedEventReason: true,
		wscommon.DegradedSchedulingJobEventReason: true,
		wscommon.InconsistentVersionEventReason:   true,
		wscommon.AlertIsFiring:                    true,
	}
	for _, e := range jobEvents {
		if _, ok := allowedReasons[e.Reason]; ok {
			events = append(events, e)
		}
	}
	return &pb.GetJobEventsResponse{
		JobEvents: events,
	}, nil
}

// TODO: Add unit tests
func (server ClusterServer) GetImageBuildJob(
	context context.Context,
	request *pb.ImageBuildRequest,
) (*pb.ImageBuildResponse, error) {
	job, err := server.kubeClientSet.BatchV1().Jobs(wscommon.Namespace).
		Get(context, request.JobId, metav1.GetOptions{})
	if err != nil {
		// TODO: Refactor to make GetImageStatus API more readable
		if apierrors.IsNotFound(err) && request.ImageReference != "" {
			log.Warnf("image build job %s not found, going to fall back and apply user venv mounting", request.JobId)
			return &pb.ImageBuildResponse{MountUserVenv: true}, nil
		} else {
			return nil, pkg.MapStatusError(err)
		}
	}

	response := pb.ImageBuildResponse{
		JobId:         request.JobId,
		ImageReady:    false,
		MountUserVenv: false,
	}

	if job.ObjectMeta.Annotations == nil {
		return nil, grpcStatus.Errorf(codes.Internal, "No job annotations")
	} else if imageRef, ok := job.ObjectMeta.Annotations[wsapisv1.ImageBuilderDestinationImageAnnotKey]; ok {
		response.ImageReference = imageRef
	} else {
		return nil, grpcStatus.Errorf(codes.Internal, "Unable to retrieve image reference from job annotations")
	}

	response.Status = pb.JobStatus_JOB_STATUS_UNSPECIFIED
	if job.Status.Active > 0 {
		response.Status = pb.JobStatus_JOB_STATUS_IN_PROGRESS
	} else if job.Status.Succeeded > 0 {
		response.Status = pb.JobStatus_JOB_STATUS_SUCCEEDED
		response.ImageReady = true
		// Best-effort pod cleanup
		labelSelector := fmt.Sprintf("job-name=%s", request.JobId)
		err = server.kubeClientSet.CoreV1().Pods(wscommon.Namespace).DeleteCollection(
			context, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			log.Warnf("Failed to clean up the pod for a successful image build job")
		}
	} else if job.Status.Failed > 0 {
		response.Status = pb.JobStatus_JOB_STATUS_FAILED
		response.MountUserVenv = true
		jobLogs, err := server.wsJobClient.GetJobLogs(context, wscommon.Namespace, request.JobId, 0)
		if err != nil {
			log.Warnf("Unable to retrieve image build logs: %+v", err)
		} else {
			response.BuildLogContent = jobLogs
		}
	}
	return &response, nil
}

func (server ClusterServer) DeleteJob(
	_ context.Context,
	request *pb.DeleteJobRequest,
) (*pb.DeleteJobResponse, error) {
	err := server.wsJobClient.Delete(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, err
	}

	return &pb.DeleteJobResponse{Message: "Job was successfully deleted"}, nil
}

// TODO: Add unit tests
func (server ClusterServer) DeleteImageBuildJob(
	context context.Context,
	request *pb.ImageBuildRequest,
) (*pb.ImageBuildResponse, error) {
	bgDelete := metav1.DeletePropagationBackground
	delOptions := metav1.DeleteOptions{
		PropagationPolicy: &bgDelete,
	}
	err := server.kubeClientSet.BatchV1().Jobs(wscommon.Namespace).
		Delete(context, request.JobId, delOptions)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	response := pb.ImageBuildResponse{
		JobId:  request.JobId,
		Status: pb.JobStatus_JOB_STATUS_DELETED,
	}
	return &response, nil
}

func (server ClusterServer) CancelJobV2(
	_ context.Context,
	request *pb.CancelJobRequest,
) (*pb.CancelJobResponse, error) {
	wsjob, err := server.wsJobClient.Get(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, err
	}
	// skip if already ended
	if commonutil.IsEnded(wsjob.Status) {
		return &pb.CancelJobResponse{
			Message: "Job already ended",
		}, nil
	}

	jobStatusMap := map[pb.JobStatus]string{
		pb.JobStatus_JOB_STATUS_CANCELLED: wsapisv1.CancelWithStatusCancelled,
		pb.JobStatus_JOB_STATUS_FAILED:    wsapisv1.CancelWithStatusFailed,
		pb.JobStatus_JOB_STATUS_SUCCEEDED: wsapisv1.CancelWithStatusSucceeded,
	}
	if jobStatusMap[request.JobStatus] == "" {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "Invalid JobStatus: %s", request.JobStatus)
	}
	_, err = server.wsJobClient.Annotate(wsjob,
		wscommon.GenCancelAnnotate(jobStatusMap[request.JobStatus], wsapisv1.CanceledByClientReason, request.Message))
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	return &pb.CancelJobResponse{
		Message: "Successfully cancelled job",
	}, err
}

// Deprecated. Use CancelJobV2
func (server ClusterServer) CancelJob(_ context.Context, _ *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	return nil, grpcStatus.Errorf(codes.Unimplemented, "Unsupported api, please upgrade client and use CancelJobV2")
}

func (server ClusterServer) GetJobInitStatus(
	_ context.Context,
	request *pb.GetJobInitStatusRequest,
) (*pb.GetJobInitStatusResponse, error) {
	wsjob, err := server.getWsJob(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, err
	}

	if _, ok := wsjob.Annotations[wsapisv1.CfgUpdatedAnno]; ok {
		return &pb.GetJobInitStatusResponse{
			Status: wsapisv1.UpdatedSignalVal,
		}, nil
	}

	return &pb.GetJobInitStatusResponse{
		Status: wsapisv1.InitSignalVal,
	}, nil
}

func (server ClusterServer) GetJobMeta(
	ctx context.Context,
	request *pb.GetJobMetaRequest,
) (*pb.GetJobMetaResponse, error) {
	clusterDetailsCmName := fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, request.JobId)
	cm, err := server.kubeClientSet.CoreV1().ConfigMaps(wscommon.Namespace).Get(ctx, clusterDetailsCmName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Error getting cluster details config map for %s: %v", request.JobId, err)
		return nil, pkg.MapStatusError(err)
	}
	clusterDetails := &commonpb.ClusterDetails{}
	unmarshalOptions := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}

	err = unmarshalOptions.Unmarshal(wscommon.GetClusterDetailsData(cm), clusterDetails)
	if err != nil {
		log.Errorf("Error unmarshalling cluster details JSON for %s: %v", request.JobId, err)
		return nil, pkg.MapStatusError(err)
	}

	wsjob, err := server.getWsJob(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, err
	}

	softwareVersions := map[string]string{
		"cbcore":         wscommon.GetWsjobImageVersion(wsjob),
		"cluster-server": pkg.CerebrasVersion,
		"job-operator":   wsjob.Annotations[wscommon.JobOperatorVersion],
	}

	systemVersions := make(map[string]string)
	if !wsjob.IsCompile() {
		for _, taskInfo := range clusterDetails.Tasks {
			if taskInfo.TaskType != commonpb.ClusterDetails_TaskInfo_WSE {
				continue
			}

			// Initialize the map with empty object values, as the client expects the values
			// to be serialized JSONs
			for _, taskMap := range taskInfo.TaskMap {
				if taskMap.AddressBook != nil && taskMap.AddressBook.TaskNodeName != "" {
					systemVersions[taskMap.AddressBook.TaskNodeName] = "{}"
				}
			}
		}
	}
	versionMetrics, _ := wscommon.GetSystemVersions(ctx, server.metricsQuerier, wscommon.Keys(systemVersions), false, false)
	maps.Copy(systemVersions, versionMetrics)
	var jobMode commonpb.JobMode_Job
	if wsjob.IsSdk() {
		jobMode = pkg.Ternary(wsjob.IsCompile(), commonpb.JobMode_SDK_COMPILE, commonpb.JobMode_SDK_EXECUTE)
	} else if wsjob.IsInference() {
		jobMode = pkg.Ternary(wsjob.IsCompile(), commonpb.JobMode_INFERENCE_COMPILE, commonpb.JobMode_INFERENCE_EXECUTE)
	} else {
		jobMode = pkg.Ternary(wsjob.IsCompile(), commonpb.JobMode_COMPILE, commonpb.JobMode_EXECUTE)
	}
	log.Infof("Wsjob: %s, mode: %s, software versions: %v, system versions: %v", request.JobId, jobMode, softwareVersions, systemVersions)
	return &pb.GetJobMetaResponse{
		JobMode:          jobMode,
		ClusterDetails:   clusterDetails,
		SoftwareVersions: softwareVersions,
		SystemVersions:   systemVersions,
	}, nil
}

func (server ClusterServer) getWsJob(namespace, jobId string) (*wsapisv1.WSJob, error) {
	var wsjob *wsapisv1.WSJob
	// this is for test simplicity
	// todo: update tests to use informer only
	if server.wsJobInformer != nil {
		job, err := server.wsJobInformer.Lister().Get(namespace + "/" + jobId)
		if err != nil {
			return nil, err
		}
		wsjob = job.(*wsapisv1.WSJob)
	} else {
		job, err := server.wsJobClient.Get(namespace, jobId)
		if err != nil {
			return nil, err
		}
		wsjob = job
	}
	return wsjob, nil
}

// getJobEventsWithExcludedReasons is a helper function to get job events excluding specified reasons
func (server ClusterServer) getJobEventsWithExcludedReasons(wsjob *wsapisv1.WSJob) []*pb.JobEvent {
	excludedReasons := map[string]bool{
		wscommon.DNSConfigFormingJobEventReason:   true,
		wscommon.DegradedSchedulingJobEventReason: true,
	}
	return wscommon.Map(func(e wscommon.JobEvent) *pb.JobEvent {
		eve := &pb.JobEvent{
			Name:    e.Name,
			Reason:  e.Reason,
			Message: e.Message,
		}
		if e.LastTimestamp != nil {
			eve.LastTimestamp = e.LastTimestamp.String()
		}
		return eve
	}, wscommon.GetJobFailureEvents(server.kubeClientSet, wsjob, excludedReasons))
}

// In the event where cluster details configmap creation failed for whatever reasons,
// we cancel the job explicitly so the failure reason is clear.
func (server ClusterServer) cancelJobOnClusterDetailsCreateErrors(job *wsapisv1.WSJob) {
	_, err := server.wsJobClient.Annotate(job, wscommon.GenCancelAnnotate(
		wsapisv1.CancelWithStatusFailed,
		wsapisv1.CanceledByServerReason,
		"failed to create cluster details configmap"))
	if err != nil {
		log.Errorf("error canceling job %s: %v", job.Name, err)
	}
}

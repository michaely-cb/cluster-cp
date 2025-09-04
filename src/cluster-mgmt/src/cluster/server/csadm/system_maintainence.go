package csadm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
	commonpb "cerebras/pb/workflow/appliance/common"

	commonutil "github.com/kubeflow/common/pkg/util"

	"github.com/cenkalti/backoff/v4"
	"github.com/prometheus/common/model"
	"golang.org/x/exp/slices"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

const (
	// Script names
	MembistScriptName   = "membist.py"
	VddcScriptName      = "vddc.py"
	WaferDiagScriptName = "wafer_diag.py"

	// System-Maintenance ConfigMap details
	SystemMaintenanceConfigMapName = "system-maintenance-scripts"
	SystemMaintenanceVolumeName    = SystemMaintenanceConfigMapName
	SystemMaintenanceMountPath     = "/system-maintenance-scripts"

	// Other constants
	defaultSystemMaintenanceJobPriority = 299
	serviceAccountName                  = "system-client-service-account"

	// Job creation retry configuration
	maxRetries         = 3
	initialBackoff     = 1 * time.Second
	maxBackoff         = 10 * time.Second
	dryrunSleepSeconds = 3

	// System client image
	SystemClientImageName = "system-client"
	CBCoreImageName       = "cbcore"

	// InfiniBand volume names and mount paths
	InfiniBandUverbs0VolumeName = "infiniband-uverbs0"
	InfiniBandRdmaCmVolumeName  = "infiniband-rdma-cm"

	InfiniBandUverbs0Path = "/dev/infiniband/uverbs0"
	InfiniBandRdmaCmPath  = "/dev/infiniband/rdma_cm"

	// Wafer-Diag specific container command configmap
	WaferDiagWrapperScript           = "/container-command-scripts/wafer_diag_wrapper.sh"
	WaferDiagInitScript              = "/container-command-scripts/wafer_diag_init.sh"
	ContainerCommandScriptsMountPath = "/container-command-scripts"
)

// Temporary data structure for collecting the workflows
type WorkflowData struct {
	Report     bool // Tell if this workflow needs to be reported
	Workflow   *pb.SystemMaintenanceWorkflow
	CreateTime time.Time
}

func (s *Server) CreateSystemMaintenance(ctx context.Context, req *pb.CreateSystemMaintenanceRequest) (*pb.CreateSystemMaintenanceResponse, error) {
	// Get the namespace from the ctx
	namespace := pkg.GetNamespaceFromContext(ctx)
	// Get the meta details from the ctx
	clientMeta, err := pkg.GetClientMetaFromContext(ctx)
	if err != nil {
		return nil, grpcStatus.Errorf(codes.Internal, "Error retrieving meta details from context: %s", err)
	} else if clientMeta.UID == -1 && clientMeta.GID == -1 { // Unknown user
		return nil, grpcStatus.Errorf(codes.Internal, "Unable to retrieve user metadata")
	}

	// Generate workflow ID
	workflowID := wsclient.GenWorkflowId()

	// Determine parameters based on WorkflowType.
	var scriptName string
	var maintenanceTypeLabel string
	switch req.WorkflowType {
	case pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST:
		scriptName = MembistScriptName
		maintenanceTypeLabel = wsapisv1.SystemMaintTypeMemBist
	case pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_VDDC:
		scriptName = VddcScriptName
		maintenanceTypeLabel = wsapisv1.SystemMaintTypeVddc
	case pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG:
		scriptName = WaferDiagScriptName
		maintenanceTypeLabel = wsapisv1.SystemMaintTypeWaferDiag
	default:
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "unsupported workflow type: %v", req.WorkflowType)
	}

	// Get systems if not specified (ALL case)
	systems := req.Systems
	if len(systems) == 0 {
		systems, err = s.getSessionSystems(ctx, namespace)
		if err != nil {
			return nil, grpcStatus.Errorf(codes.Internal, "failed to get systems: %v", err)
		}
	}
	slices.Sort(systems)
	unsupportedSystems := map[string]bool{}
	for _, sys := range systems {
		unsupportedSystems[sys] = true
	}

	// We only check if systems are eligible for performing maintenance tasks when not in dryrun mode
	if !req.Dryrun && s.metricsQuerier != nil {
		result, err := s.metricsQuerier.QuerySystemInMaintenanceMode(ctx, systems)
		if err != nil {
			return nil, grpcStatus.Errorf(codes.Internal, "system maintenance mode query failed: %v", err)
		}

		for _, sample := range result.(model.Vector) {
			if sample == nil {
				continue
			}
			system := string(sample.Metric["system"])
			delete(unsupportedSystems, system)
		}
		if len(unsupportedSystems) == 0 {
			logrus.Infof("All systems %v are eligible to perform maintenance tasks", systems)
		} else {
			return nil, grpcStatus.Errorf(codes.FailedPrecondition,
				"Requested to perform maintenance tasks for %d systems %v, but systems %v are not eligible since maintenance mode is not supported",
				len(systems), systems, wscommon.SortedKeys(unsupportedSystems))
		}
	}

	// Create jobs for each system
	var jobs []*pb.SystemMaintenanceJob
	var failedJobs []*pb.SystemMaintenanceJob

	for _, system := range systems {
		// Create job spec with proper configuration
		jobSpec := &wsclient.JobSpec{
			// system maintenance job will be of type execute
			WsJobMode: wsclient.ExecuteJobMode,
			Namespace: namespace,
			NumWafers: 1,
			JobUser: wsapisv1.JobUser{
				Uid:      clientMeta.UID,
				Gid:      clientMeta.GID,
				Username: clientMeta.Username,
				Groups:   clientMeta.Groups,
			},
			// anonymous func to convert priority to pointer
			Priority: wscommon.Pointer(defaultSystemMaintenanceJobPriority),
			Labels: map[string]string{
				wsapisv1.WorkflowIdLabelKey:          workflowID,
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				wsapisv1.JobModeLabelKey:             wsapisv1.JobModeWeightStreaming,
				wsapisv1.SystemMaintLabelKey:         maintenanceTypeLabel,
			},
			Annotations: map[string]string{
				wsapisv1.SchedulerHintAllowSys:        system,
				wscommon.ClusterServerVersion:         pkg.CerebrasVersion,
				wsapisv1.SemanticClusterServerVersion: wsclient.SemanticVersion,
				wsapisv1.SystemMaintDryrunAnnot:       strconv.FormatBool(req.Dryrun),
			},
			AllowUnhealthySystems: req.AllowError,
			ServiceAccountName:    serviceAccountName,
			ImagePullSecretHosts:  wsclient.ParseImagePullSecret(s.k8s),
		}

		// Set up volumes using ConfigMap
		jobSpec.ScriptRootVolume = &corev1.Volume{
			Name: SystemMaintenanceVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: SystemMaintenanceConfigMapName,
					},
				},
			},
		}

		jobSpec.ScriptRootVolumeMount = &corev1.VolumeMount{
			Name:      SystemMaintenanceVolumeName,
			MountPath: SystemMaintenanceMountPath,
		}

		jobSpec.JobPathConfig = wsclient.JobPathConfig{
			LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
		}
		if err = jobSpec.SetWorkdirVolumeAndMount(ctx, s.k8s, req.DebugArgs); err != nil {
			return nil, grpcStatus.Errorf(codes.Internal, "failed to set workdir volume and mount: %v", err)
		}

		// Configure the service spec for the WSJob (here we use WsCoordinator)
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)

		scriptPath := filepath.Join(SystemMaintenanceMountPath, scriptName)
		image := getSystemClientImage(req)

		command, err := buildCommand(req, scriptPath, system, scriptName)
		if err != nil {
			return nil, err
		}
		logrus.Infof("System maintenance workflow: %s, image: %s, namespace: %s, command being executed: %s", workflowID, image, namespace, command)

		envVars, err := s.buildEnvVars(ctx, req, system, scriptPath, namespace)
		if err != nil {
			return nil, err
		}

		specs[wsclient.WsCoordinator] = wsclient.ServiceSpec{
			Image:    image,
			Command:  command,
			Env:      envVars,
			Replicas: 1,
		}

		// Only apply resource limits if skipSystemMaintenanceResourceLimits is false
		// This is to allow for testing with lower resource limits
		if !req.Dryrun && !s.serverOpts.SkipSystemMaintenanceResourceLimits {
			// These limits are copied from the execute coordinator spec
			// We plan to adjust and potentially lower these limits as we understand the
			// actual resource usage of the system maintenance scripts.
			specs[wsclient.WsCoordinator].Resources = wsclient.ResourceRequest{
				CpuMilliCores: 16000,                   // 16 cores
				MemoryBytes:   32 * 1024 * 1024 * 1024, // 32GB
			}
		}

		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, *jobSpec)
		if err != nil {
			logrus.Warnf("Failed to build job spec: %v", err)
			return nil, err
		}
		// If wafer-diag, add modifications to WSJob
		if req.WorkflowType == pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG {
			if err := configureWaferDiagPodSpec(wsjob, req); err != nil {
				logrus.Errorf("Failed to add wafer-diag coordinator pod specs: %v", err)
				return nil, err
			}
		}

		// Use the helper function to create the job with retry
		job, err := s.createWSJobWithRetry(ctx, wsjob)
		if err != nil {
			logrus.Errorf("Failed to create WSJob for system %s: %v", system, err)
			failedJobs = append(failedJobs, createFailedSystemMaintenanceJob(req.WorkflowType, system, namespace, clientMeta))
		} else {
			jobs = append(jobs, mapWSJobToSystemMaintenanceJob(job))
		}
	}

	return &pb.CreateSystemMaintenanceResponse{
		Jobs:       jobs,
		FailedJobs: failedJobs,
	}, nil
}

func (s *Server) GetSystemMaintenance(ctx context.Context, req *pb.GetSystemMaintenanceRequest) (*pb.GetSystemMaintenanceResponse, error) {
	sortedJobs, err := s.getSortedSysMaintenanceJobs(
		req.WorkflowId,
		true,
		"", // orderBy
		"", // sortBy
	)
	if err != nil {
		return nil, err
	}

	var jobs []*pb.SystemMaintenanceJob
	for _, j := range sortedJobs {
		// Append the created job to the response.
		jobs = append(jobs, mapWSJobToSystemMaintenanceJob(j))
	}

	return &pb.GetSystemMaintenanceResponse{
		Jobs: jobs,
	}, nil
}

func (s *Server) ListSystemMaintenance(ctx context.Context, req *pb.ListSystemMaintenanceRequest) (*pb.ListSystemMaintenanceResponse, error) {
	// Collect all system maintenance jobs, we will need to include finished
	// jobs in active workflows.

	// Get the namespace from the ctx
	namespace := pkg.GetNamespaceFromContext(ctx)

	// Get the meta details from the ctx
	sortedJobs, err := s.getSortedSysMaintenanceJobs(
		"",   // workflowId
		true, // allStates
		"",   // orderBy
		"",   // sortBy
	)
	if err != nil {
		return nil, err
	}

	// Partition them into workflows, annotate if there are any active jobs
	// in each workflow.
	workflows := make(map[string]WorkflowData)
	for _, wsjob := range sortedJobs {
		reportJob := req.AllStates || !commonutil.IsEnded(wsjob.Status)
		workflowId := wsjob.Labels[wsapisv1.WorkflowIdLabelKey]

		workflowData, exists := workflows[workflowId]
		if !exists {
			// Create a new entry
			workflowData.Report = false
			workflowData.Workflow = &pb.SystemMaintenanceWorkflow{
				WorkflowId:   workflowId,
				Namespace:    namespace,
				WorkflowType: getSystemMaintType(wsjob.Labels[wsapisv1.SystemMaintLabelKey]),
				CreateTime:   timestamppb.New(wsjob.CreationTimestamp.Time),
			}
			workflowData.CreateTime = wsjob.CreationTimestamp.Time
		}

		// Report the workflow if any of its job needs to be reported
		workflowData.Report = workflowData.Report || reportJob

		// Workflow's create time = earliest of all job's create time.
		if wsjob.CreationTimestamp.Time.Before(workflowData.CreateTime) {
			workflowData.CreateTime = wsjob.CreationTimestamp.Time
			workflowData.Workflow.CreateTime = timestamppb.New(wsjob.CreationTimestamp.Time)
		}

		workflowData.Workflow.Jobs = append(workflowData.Workflow.Jobs, mapWSJobToSystemMaintenanceJob(wsjob))
		workflows[workflowId] = workflowData
	}

	resp := &pb.ListSystemMaintenanceResponse{}
	for _, workflowData := range workflows {
		if !workflowData.Report {
			continue
		}
		resp.Workflows = append(resp.Workflows, workflowData.Workflow)
	}

	// Sort workflows by CreateTime from the newest to the oldest
	sort.Slice(resp.Workflows, func(i, j int) bool {
		return resp.Workflows[i].CreateTime.AsTime().After(resp.Workflows[j].CreateTime.AsTime())
	})

	return resp, nil
}

// createWSJobWithRetry creates a WSJob with retry logic
func (s *Server) createWSJobWithRetry(ctx context.Context, wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
	var job *wsapisv1.WSJob

	// Configure exponential backoff
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = initialBackoff
	expBackoff.MaxInterval = maxBackoff
	expBackoff.MaxElapsedTime = maxBackoff * time.Duration(maxRetries)

	// Use backoff with max retries
	retryBackoff := backoff.WithMaxRetries(expBackoff, uint64(maxRetries))

	// Operation to retry
	operation := func() error {
		var createErr error
		// Create the WSJob
		job, createErr = s.wsJobClient.Create(wsjob)
		if createErr != nil {
			logrus.Warnf("Failed to create WSJob (will retry): %v", createErr)
			return createErr
		}
		return nil
	}
	// Notify function for logging retry attempts
	notify := func(err error, backoffTime time.Duration) {
		logrus.Infof("Retrying job creation after %v due to: %v",
			backoffTime, err)
	}

	// Execute with retry
	err := backoff.RetryNotify(operation, retryBackoff, notify)
	if err != nil {
		logrus.Errorf("Failed to create job after %d retries: %v", maxRetries, err)
		return nil, err
	}

	err = pkg.GenerateClusterDetailsConfig(ctx, s.k8s, job, generateSystemMaintClusterDetails())
	if err != nil {
		logrus.Warnf("Failed to generate cluster details config: %v", err)
		return nil, err
	}
	return job, nil
}

// Helper function for configuring volume and specialized pod specs for wafer-diag jobs
func configureWaferDiagPodSpec(jobSpecWSJob *wsapisv1.WSJob, req *pb.CreateSystemMaintenanceRequest) error {
	// Get wafer-diag payload
	waferDiagPayload := req.Payload.(*pb.CreateSystemMaintenanceRequest_WaferDiag)

	// Don't change the configuration for test cases and avoid initcontainer if not using a cbcore image
	if waferDiagPayload.WaferDiag.Testmode {
		return nil
	}

	// Get coordinator pod spec
	coordinatorSpec := jobSpecWSJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator]

	// Add main cbcore container
	var image string
	if waferDiagPayload.WaferDiag.CbcoreImage != "" {
		image = waferDiagPayload.WaferDiag.CbcoreImage
		logrus.Infof("Using explicitly provided cbcore image for wafer-diag: %s", image)
	} else {
		// If no tag is provided, we default to the server-environment cbcore
		var err error
		image, err = getCbcoreImageFromServer()
		if err != nil {
			return fmt.Errorf("failed to get cbcore image from server: %w", err)
		}
		logrus.Warnf("No cbcore image specified in wafer-diag request, using server default: %s", image)
	}

	// Update the coordinator container image - should be 1 main cbcore container
	if len(coordinatorSpec.Template.Spec.Containers) == 0 {
		return fmt.Errorf("coordinator spec has no containers defined")
	}
	coordinatorSpec.Template.Spec.Containers[0].Image = image
	logrus.Infof("Updated wafer-diag coordinator image to: %s", image)

	coordinatorSpec.Template.ObjectMeta.Annotations = wscommon.EnsureMap(coordinatorSpec.Template.ObjectMeta.Annotations)
	coordinatorSpec.Template.ObjectMeta.Annotations[wscommon.NetworkAttachmentKey] = wscommon.DataNetAttachDef

	// Add init container (system-client) for copying the system-maintenance-framework directory
	systemClientImage := getSystemClientImage(req)
	initContainerCommand := getWaferDiagInitContainerCommand()

	// Add command script volume mount to both init and main containers
	containerCommandScriptVolumeMount := corev1.VolumeMount{
		// "-exec" version indicates pod-level permissions to execute container commands, i.e. /wafer_diag_wrapper.sh
		Name:      SystemMaintenanceVolumeName + "-exec",
		MountPath: ContainerCommandScriptsMountPath,
	}

	waferDiagInitContainer := corev1.Container{
		Name:    "copy-framework",
		Image:   systemClientImage,
		Command: initContainerCommand,
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "system-maintenance-framework",
				MountPath: "/shared-framework", // Different mount point to avoid shadowing
			},
			containerCommandScriptVolumeMount,
		},
	}

	// Add init container
	coordinatorSpec.Template.Spec.InitContainers = append(
		coordinatorSpec.Template.Spec.InitContainers,
		waferDiagInitContainer,
	)

	// Add system-maintenance-framework volume to main container
	for i := range coordinatorSpec.Template.Spec.Containers {
		coordinatorSpec.Template.Spec.Containers[i].VolumeMounts = append(
			coordinatorSpec.Template.Spec.Containers[i].VolumeMounts,
			corev1.VolumeMount{
				Name:      "system-maintenance-framework",
				MountPath: "/system-maintenance-framework",
			},
			containerCommandScriptVolumeMount,
		)
	}

	coordinatorSpec.Template.Spec.Volumes = append(
		coordinatorSpec.Template.Spec.Volumes,
		// Framework sharing volume
		corev1.Volume{
			Name: "system-maintenance-framework",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		// Executable scripts volume (needs execute permissions)
		corev1.Volume{
			// "-exec" version of system-maintenance-scripts to indicate pod-level access to container commands
			Name: SystemMaintenanceVolumeName + "-exec",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: SystemMaintenanceConfigMapName,
					},
					DefaultMode: &[]int32{0755}[0], // Execute permission required
				},
			},
		},
	)

	for i := range coordinatorSpec.Template.Spec.Containers {
		coordinatorSpec.Template.Spec.Containers[i].VolumeMounts = append(
			coordinatorSpec.Template.Spec.Containers[i].VolumeMounts,
			corev1.VolumeMount{
				Name:      InfiniBandUverbs0VolumeName,
				MountPath: InfiniBandUverbs0Path,
			},
			corev1.VolumeMount{
				Name:      InfiniBandRdmaCmVolumeName,
				MountPath: InfiniBandRdmaCmPath,
			},
		)

		// Add privileged security context
		if coordinatorSpec.Template.Spec.Containers[i].SecurityContext == nil {
			coordinatorSpec.Template.Spec.Containers[i].SecurityContext = &corev1.SecurityContext{}
		}
		privileged := true
		coordinatorSpec.Template.Spec.Containers[i].SecurityContext.Privileged = &privileged
	}
	// Add all volumes
	charDeviceType := corev1.HostPathCharDev
	coordinatorSpec.Template.Spec.Volumes = append(
		coordinatorSpec.Template.Spec.Volumes,
		corev1.Volume{
			Name: InfiniBandUverbs0VolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: InfiniBandUverbs0Path,
					Type: &charDeviceType,
				},
			},
		},
		corev1.Volume{
			Name: InfiniBandRdmaCmVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: InfiniBandRdmaCmPath,
					Type: &charDeviceType,
				},
			},
		},
	)

	return nil
}

// Utility function to get system maintenance jobs in default order.
// Used by Get and List commands.
func (s *Server) getSortedSysMaintenanceJobs(
	workflowId string,
	allStates bool,
	orderBy string,
	sortBy string,
) ([]*wsapisv1.WSJob, error) {
	var wsjobs []runtime.Object
	var locks []runtime.Object
	var err error

	selector := labels.NewSelector()
	requirement1, _ := labels.NewRequirement(wsapisv1.SystemMaintLabelKey, selection.Exists, nil)
	selector = selector.Add(*requirement1)

	if workflowId != "" {
		requirement2, _ := labels.NewRequirement(wsapisv1.WorkflowIdLabelKey, selection.Equals, []string{workflowId})
		selector = selector.Add(*requirement2)
	}

	if s.wsJobInformer == nil {
		return nil, grpcStatus.Errorf(codes.Internal, "wsJobInformer is nil")
	}

	// TODO: share the following code with the List function in
	// cluster/server/csctl/job.go
	namespace := wscommon.Namespace
	if namespace == wscommon.SystemNamespace {
		wsjobs, err = s.wsJobInformer.Lister().List(selector)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		if s.lockInformer != nil {
			locks, err = s.lockInformer.Lister().List(selector)
			if err != nil {
				return nil, pkg.MapStatusError(err)
			}
		}
	} else {
		wsjobs, err = s.wsJobInformer.Lister().ByNamespace(namespace).List(selector)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		if s.lockInformer != nil {
			locks, err = s.lockInformer.Lister().ByNamespace(namespace).List(selector)
			if err != nil {
				return nil, pkg.MapStatusError(err)
			}
		}
	}

	var uid *int64 = nil
	var limit int32 = 0
	var sortedJobs, _, _ = wscommon.SortJobs(
		wscommon.Map(func(a runtime.Object) *wsapisv1.WSJob {
			return a.(*wsapisv1.WSJob)
		}, wsjobs),
		wscommon.Map(func(a runtime.Object) *rlv1.ResourceLock {
			return a.(*rlv1.ResourceLock)
		}, locks),
		orderBy,
		sortBy,
		allStates,
		uid,
		limit,
	)

	return sortedJobs, nil
}

// createFailedSystemMaintenanceJob constructs a SystemMaintenanceJob for jobs that failed to be created
func createFailedSystemMaintenanceJob(workflowType pb.SystemMaintenanceType, system, namespace string, meta pkg.ClientMeta) *pb.SystemMaintenanceJob {
	return &pb.SystemMaintenanceJob{
		JobId:        "", // Empty JobId indicates creation failure
		Namespace:    namespace,
		WorkflowType: workflowType,
		Status:       pb.JobStatus_JOB_STATUS_UNSPECIFIED,
		Systems:      []string{system},
		CreateTime:   timestamppb.Now(),
		User: &pb.User{
			Username: meta.Username,
			Uid:      meta.UID,
			Gid:      meta.GID,
			Groups:   meta.Groups,
		},
	}
}

func (s *Server) getSessionSystems(ctx context.Context, namespace string) ([]string, error) {
	nsr, err := s.nsrCli.Get(ctx, namespace, metav1.GetOptions{})

	if err != nil {
		return nil, err
	}

	return nsr.Status.Systems, nil
}

// mapWSJobToSystemMaintenanceJob converts a WSJob to a SystemMaintenanceJob proto
func mapWSJobToSystemMaintenanceJob(job *wsapisv1.WSJob) *pb.SystemMaintenanceJob {
	j := &pb.SystemMaintenanceJob{
		JobId:        job.Name,
		Namespace:    job.Namespace,
		WorkflowType: getSystemMaintType(job.Labels[wsapisv1.SystemMaintLabelKey]),
		Status:       getSystemMaintStatus(job.Status.Conditions),
		Systems:      []string{job.Annotations[wsapisv1.SchedulerHintAllowSys]},
		Priority:     int32(*job.Spec.Priority),
		Labels:       job.Labels,
		CreateTime:   timestamppb.New(job.CreationTimestamp.Time),
		User: &pb.User{
			Username: job.Spec.User.Username,
			Uid:      job.Spec.User.Uid,
			Gid:      job.Spec.User.Gid,
			Groups:   job.Spec.User.Groups,
		},
	}

	if job.Status.StartTime != nil {
		j.ExecutionTime = timestamppb.New(job.Status.StartTime.Time)
	}
	if job.Status.CompletionTime != nil {
		j.CompletionTime = timestamppb.New(job.Status.CompletionTime.Time)
	}

	return j
}

// generateSystemMaintClusterDetails generates a ClusterDetails object for system maintenance
// This is mostly hardcoded for now
func generateSystemMaintClusterDetails() *commonpb.ClusterDetails {
	cd := &commonpb.ClusterDetails{
		Tasks: []*commonpb.ClusterDetails_TaskInfo{
			{
				TaskType: commonpb.ClusterDetails_TaskInfo_CRD,
				TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
					{
						TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
							TaskId: int32(0),
							WseId:  int32(-1),
							WseIds: []int32{-1},
						},
					},
				},
			},
			{
				TaskType: commonpb.ClusterDetails_TaskInfo_WSE,
				TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
					{
						TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
							TaskId: int32(0),
							WseId:  int32(0),
							WseIds: []int32{0},
						},
					},
				},
			},
		},
	}

	return cd
}

func getSystemMaintType(sysMaintType string) pb.SystemMaintenanceType {
	switch sysMaintType {
	case wsapisv1.SystemMaintTypeMemBist:
		return pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST
	case wsapisv1.SystemMaintTypeVddc:
		return pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_VDDC
	case wsapisv1.SystemMaintTypeWaferDiag:
		return pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG
	default:
		return pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_UNSPECIFIED
	}
}

func getSystemMaintStatus(conditions []commonv1.JobCondition) pb.JobStatus {
	// Use the latest condition as job status
	if len(conditions) == 0 {
		return pb.JobStatus_JOB_STATUS_CREATED
	}
	cond_type := conditions[len(conditions)-1].Type
	if cond_type == commonv1.JobCreated {
		return pb.JobStatus_JOB_STATUS_CREATED
	} else if cond_type == commonv1.JobQueueing {
		return pb.JobStatus_JOB_STATUS_IN_QUEUE
	} else if cond_type == commonv1.JobScheduled {
		return pb.JobStatus_JOB_STATUS_SCHEDULED
	} else if cond_type == commonv1.JobInitializing {
		return pb.JobStatus_JOB_STATUS_INIT
	} else if cond_type == commonv1.JobRunning {
		return pb.JobStatus_JOB_STATUS_IN_PROGRESS
	} else if cond_type == commonv1.JobSucceeded {
		return pb.JobStatus_JOB_STATUS_SUCCEEDED
	} else if cond_type == commonv1.JobFailed {
		return pb.JobStatus_JOB_STATUS_FAILED
	} else if cond_type == commonv1.JobCancelled {
		return pb.JobStatus_JOB_STATUS_CANCELLED
	} else if cond_type == commonv1.JobFailing {
		return pb.JobStatus_JOB_STATUS_FAILING
	} else if cond_type == commonv1.JobBootstrapping {
		return pb.JobStatus_JOB_STATUS_BOOTSTRAPPNG
	} else if cond_type == commonv1.JobReady {
		return pb.JobStatus_JOB_STATUS_READY
	} else if cond_type == commonv1.JobRestarting {
		return pb.JobStatus_JOB_STATUS_RESTARTING
	} else {
		return pb.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

func getSystemClientImage(req *pb.CreateSystemMaintenanceRequest) string {
	if req.DebugArgs != nil && req.DebugArgs.DebugMgr != nil {
		if debugTaskSpec := req.DebugArgs.DebugMgr.TaskSpecHints[wsapisv1.WSReplicaTypeCoordinator.Lower()]; debugTaskSpec != nil {
			return debugTaskSpec.ContainerImage
		}
	}
	return fmt.Sprintf("%s/%s:%s", wsapisv1.RegistryUrl, SystemClientImageName, os.Getenv("SYSTEM_CLIENT_TAG"))
}

func getWaferDiagInitContainerCommand() []string {
	return []string{WaferDiagInitScript}
}

func appendVddcArgs(command []string, payload *pb.CreateSystemMaintenanceRequest_Vddc) []string {
	voltageOffset := int(payload.Vddc.VddcVoltageOffset)
	if voltageOffset == 0 {
		command = append(command, "unset")
	} else {
		command = append(command, "set")
	}
	command = append(command, strconv.Itoa(voltageOffset))
	return command
}

func appendMembistArgs(command []string, payload *pb.CreateSystemMaintenanceRequest_Membist) []string {
	if payload.Membist.SkipFailedOps {
		command = append(command, "skip-failed-ops")
	} else {
		command = append(command, "not-skip-failed-ops")
	}
	command = append(command, strconv.Itoa(int(payload.Membist.VddcVoltageOffset)))
	return command
}

func appendWaferDiagArgs(command []string, payload *pb.CreateSystemMaintenanceRequest_WaferDiag) []string {
	wd := payload.WaferDiag
	// Add variants (comma-separated)
	if len(wd.Variants) > 0 {
		variants := make([]string, len(wd.Variants))
		for i, v := range wd.Variants {
			variants[i] = strconv.Itoa(int(v))
		}
		command = append(command, "--variants", strings.Join(variants, ","))
	}

	// Add duration if specified
	if wd.Duration > 0 {
		command = append(command, "--duration", fmt.Sprintf("%.1f", wd.Duration))
	}

	// Add sleep scale if specified
	if wd.SleepScale > 0 {
		command = append(command, "--sleep-scale", fmt.Sprintf("%.1f", wd.SleepScale))
	}

	// Add boolean flags
	if wd.Memconfig {
		command = append(command, "--memconfig")
	}

	if wd.NoMath {
		command = append(command, "--no-math")
	}

	if wd.ContinueAfterFail {
		command = append(command, "--continue-after-fail")
	}

	if wd.CoredumpOnFail {
		command = append(command, "--coredump-on-fail")
	}

	if wd.SkipConfigApiCheck {
		command = append(command, "--skip-config-api-check")
	}

	return command
}

func buildWaferDiagCommand(originalCommand []string) []string {
	// Call the wrapper script with the original command as arguments
	wrappedCommand := []string{WaferDiagWrapperScript}
	wrappedCommand = append(wrappedCommand, originalCommand...)
	return wrappedCommand
}

func buildCommand(req *pb.CreateSystemMaintenanceRequest, scriptPath, system, scriptName string) ([]string, error) {
	// python scriptPath system_name system_addr
	// The scripts use tunnel clients that resolve by hostnames, as a result we use the system hostname for both name and addr.
	command := []string{"python", scriptPath, system, system}

	if req.DebugArgs != nil && req.DebugArgs.DebugMgr != nil {
		if debugTaskSpec := req.DebugArgs.DebugMgr.TaskSpecHints[wsapisv1.WSReplicaTypeCoordinator.Lower()]; debugTaskSpec != nil {
			return debugTaskSpec.ContainerCommand, nil
		}
	}

	switch scriptName {
	case VddcScriptName:
		if vddcPayload, ok := req.Payload.(*pb.CreateSystemMaintenanceRequest_Vddc); ok {
			command = appendVddcArgs(command, vddcPayload)
		} else {
			return nil, grpcStatus.Errorf(codes.InvalidArgument, "expected vddc payload but got a different type")
		}

	case MembistScriptName:
		if membistPayload, ok := req.Payload.(*pb.CreateSystemMaintenanceRequest_Membist); ok {
			command = appendMembistArgs(command, membistPayload)
		} else {
			return nil, grpcStatus.Errorf(codes.InvalidArgument, "expected membist payload but got a different type")
		}
	case WaferDiagScriptName:
		if waferDiagPayload, ok := req.Payload.(*pb.CreateSystemMaintenanceRequest_WaferDiag); ok {
			command = appendWaferDiagArgs(command, waferDiagPayload)
			if req.AllowError {
				command = append(command, "--allow-error")
			}
			if !waferDiagPayload.WaferDiag.Testmode {
				command = buildWaferDiagCommand(command)
			}
		} else {
			return nil, grpcStatus.Errorf(codes.InvalidArgument, "expected wafer diag payload but got a different type")
		}
	default:
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "unsupported script: %s", scriptName)
	}

	// Handle AllowError for VDDC and Membist scripts (they use the same format)
	if scriptName == VddcScriptName || scriptName == MembistScriptName {
		if req.AllowError {
			command = append(command, "allow-error")
		} else {
			command = append(command, "not-allow-error")
		}
	}

	return command, nil
}

func (s *Server) buildEnvVars(ctx context.Context, req *pb.CreateSystemMaintenanceRequest, system string, scriptPath string, namespace string) ([]corev1.EnvVar, error) {
	baseEnvVars := []corev1.EnvVar{
		{Name: "SYSTEM_NAME", Value: system},
		{Name: "SCRIPT_PATH", Value: scriptPath},
		{Name: "DRYRUN_MODE", Value: strconv.FormatBool(req.Dryrun)},
		{Name: "DRYRUN_SLEEP_SECONDS", Value: strconv.Itoa(dryrunSleepSeconds)},
	}
	if !req.Dryrun && req.WorkflowType == pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG {
		cmAddr, err := s.resolveCMAddress(ctx, system)
		if err != nil {
			return nil, err
		}
		baseEnvVars = append(baseEnvVars, corev1.EnvVar{Name: "CM_ADDRESS", Value: cmAddr})
	}

	return baseEnvVars, nil
}

func getCbcoreImageFromServer() (string, error) {
	opts := wsclient.ServerOptionsFromEnv()
	defaultImage := opts.WsjobDefaultImage
	if defaultImage == "" {
		return "", fmt.Errorf("no default cbcore image configured in server options")
	}
	return defaultImage, nil
}

// Helper for fetching CMAddress
func (s *Server) resolveCMAddress(ctx context.Context, systemName string) (string, error) {
	// Use existing sysCLI to get system information
	system, err := s.sysCli.Get(ctx, systemName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get system %s: %w", systemName, err)
	}
	if system.Spec.CmAddr == "" {
		return "", fmt.Errorf("empty control address for system %s", systemName)
	}
	return system.Spec.CmAddr, nil
}

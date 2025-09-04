package wsclient

import (
	"context"
	"path"
	"path/filepath"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/coreos/go-semver/semver"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

type UserSidecar struct {
	Enabled    bool
	Image      string
	CpuPercent int
	MemPercent int
}

func (usc *UserSidecar) UpdateFromDebugArgs(args *commonpb.DebugArgs_DebugMGR_UserSidecar) error {
	if args == nil {
		return nil
	}
	usc.Image = args.Image
	usc.CpuPercent = int(args.CpuPercent)
	if usc.CpuPercent >= 100 {
		return grpcStatus.Error(codes.InvalidArgument, "CpuPercent must be less than 100")
	}
	usc.MemPercent = int(args.MemPercent)
	if usc.MemPercent >= 100 {
		return grpcStatus.Error(codes.InvalidArgument, "MemPercent must be less than 100")
	}
	return nil
}

type UserSidecarEnv struct {
	Enabled     bool
	PythonPath  string
	MountDir    *pb.MountDir
	CleanupPath string
}

func (env *UserSidecarEnv) UpdateFromDebugArgs(args *pb.UserSidecarEnv) {
	if args == nil || args.PythonPath == "" {
		return
	}
	env.Enabled = true
	env.PythonPath = args.PythonPath
	env.MountDir = args.MountDir
	if args.MountDir != nil && args.MountDir.Path != args.MountDir.ContainerPath {
		env.CleanupPath = args.MountDir.ContainerPath
	}
}

type JobSpec struct {
	Namespace       string
	JobName         string
	NumWafers       int
	ActToCsxDomains [][]int
	CleanPodPolicy  commonv1.CleanPodPolicy
	Labels          map[string]string
	Annotations     map[string]string

	JobUser       wsapisv1.JobUser
	JobPathConfig JobPathConfig

	// SchedulerHints optional k/v pairs to modify the behavior of scheduling
	SchedulerHints map[string]string

	// ImagePullSecretHosts maps registry hostnames to bool, where true means the registry
	// requires the 'regcred' secret for authentication. When a service uses an image from
	// these registries, imagePullSecrets will be set on the pod.
	ImagePullSecretHosts map[string]bool

	// Grpc settings
	GrpcSettings *commonpb.DebugArgs_DebugMGR_GrpcSettings

	// DropCaches value and path
	DropCachesValue commonpb.DebugArgs_DebugMGR_DropCachesValue
	DropCachesPath  string

	// Whether to enable the coordinator address optimization
	EnableCrdAddrHandoff bool

	// Whether this is compile or execute job mode
	WsJobMode WSJobMode

	// Volume and VolumeMount for script root
	ScriptRootVolume      *corev1.Volume
	ScriptRootVolumeMount *corev1.VolumeMount

	// Volume and VolumeMount for workdir logs
	WorkdirLogsVolume      *corev1.Volume
	WorkdirLogsVolumeMount *corev1.VolumeMount
	WorkdirLogPath         string // informational display only
	WorkdirLogsRootPath    string

	// Volume and VolumeMount for cached compile
	CachedCompileVolume       *corev1.Volume
	CachedCompileVolumeMount  *corev1.VolumeMount
	CachedCompileRootPath     string
	CachedCompileRelativePath string

	// Volume and VolumeMount for NFS-based debug purposes
	CbcoreOverrideVolume      *corev1.Volume
	CbcoreOverrideVolumeMount *corev1.VolumeMount

	// Volume and VolumeMount for fallback user venv
	UserFallbackVenvVolume      *corev1.Volume
	UserFallbackVenvVolumeMount *corev1.VolumeMount

	// Volumes and VolumeMounts for worker sidecars, eg. /cb/ml
	WorkerUserVolumes      []*corev1.Volume
	WorkerUserVolumeMounts []*corev1.VolumeMount

	// Volume and VolumeMount for tensor storage
	TensorStorageVolume      *corev1.Volume
	TensorStorageVolumeMount *corev1.VolumeMount

	// Volume and VolumeMount for worker cache
	WorkerCacheVolume      *corev1.Volume
	WorkerCacheVolumeMount *corev1.VolumeMount

	// Volume and VolumeMount for shared memory
	SharedStorageVolume      *corev1.Volume
	SharedStorageVolumeMount *corev1.VolumeMount

	// Volume and VolumeMount for system secret
	SystemSecretVolume      *corev1.Volume
	SystemSecretVolumeMount *corev1.VolumeMount

	// Volume and VolumeMount for log export
	LogExportStagingVolume      *corev1.Volume
	LogExportStagingVolumeMount *corev1.VolumeMount
	LogExportNodeSelector       map[string]string

	// User node client workdir
	ClientWorkdir string

	// A list of PYTHONPATHs the user sidecar should respect.
	WorkerPythonPaths []string

	// Sidecar fallback environment
	UserSidecarEnv *UserSidecarEnv

	// Worker sidecar container for streaming input data
	WorkerSidecar *UserSidecar

	// Weight sidecar container for weight initialization
	WeightSidecar *UserSidecar

	// Activation sidecar container for activation initialization
	ActivationSidecar *UserSidecar

	CompileFabricType string

	EnableRoCE bool

	EnableNonCRDServiceCreation bool

	DisableNonCRDResourceRequests bool

	// AllowUnhealthySystems enables scheduling with "error" systems that are explicitly specified using `allow-systems` scheduler hint.
	AllowUnhealthySystems bool

	// Priority of the job
	Priority *int

	// Operator semver to decide whether features are supported
	OperatorSemver *semver.Version

	// Notification preferences for this job
	Notifications []*wsapisv1.UserNotification

	// Service Account Name used by the job
	ServiceAccountName string
}

func (jobSpec *JobSpec) SetScriptRootVolumeAndMount() {
	jobSpec.ScriptRootVolume = &corev1.Volume{
		Name: wsapisv1.ScriptRootVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "wsjob-entrypoint-scripts",
				},
			},
		},
	}
	jobSpec.ScriptRootVolumeMount = &corev1.VolumeMount{
		Name:      wsapisv1.ScriptRootVolume,
		MountPath: wsapisv1.WsjobEntrypointMountPath,
	}
}

func (jobSpec *JobSpec) SetWorkdirVolumeAndMount(
	ctx context.Context,
	kubeClientSet kubernetes.Interface,
	debugArgs *commonpb.DebugArgs,
) error {
	// session-specific nfs volume
	volumeRootPath := jobSpec.JobPathConfig.WorkdirLogsNfsPath
	if debugArgs != nil &&
		debugArgs.DebugMgr != nil &&
		debugArgs.DebugMgr.NfsWorkdirLogsPath != "" {
		// job-specific nfs volume override
		volumeRootPath = debugArgs.DebugMgr.NfsWorkdirLogsPath
	}
	return GetWorkdirVolumeAndMount(ctx, kubeClientSet, jobSpec, volumeRootPath)
}

func (jobSpec *JobSpec) SetCachedCompileVolumeAndMount(
	ctx context.Context,
	kubeClientSet kubernetes.Interface,
	debugArgs *commonpb.DebugArgs,
	hasMultiMgmtNodes bool) error {
	// session-specific nfs volume
	volumeRootPath := jobSpec.JobPathConfig.CachedCompileNfsPath
	if debugArgs != nil &&
		debugArgs.DebugMgr != nil &&
		debugArgs.DebugMgr.NfsCachedCompilePath != "" {
		// job-specific nfs volume override
		volumeRootPath = debugArgs.DebugMgr.NfsCachedCompilePath
	}
	return GetCachedCompileVolumeAndMount(ctx, kubeClientSet, jobSpec, volumeRootPath, hasMultiMgmtNodes)
}

func (jobSpec *JobSpec) SetCbcoreEntrypointVolumeAndMount(
	ctx context.Context,
	kubeClientSet kubernetes.Interface,
	debugArgs *commonpb.DebugArgs,
) error {
	if debugArgs != nil &&
		debugArgs.DebugMgr != nil &&
		debugArgs.DebugMgr.CbcoreOverrideScript != "" {
		debugMountDir := &pb.MountDir{
			Path: filepath.Dir(debugArgs.DebugMgr.CbcoreOverrideScript),
		}
		volumes, mounts, err :=
			ResolveMountDirs(ctx, wscommon.Namespace, kubeClientSet,
				[]*pb.MountDir{debugMountDir})
		if err != nil {
			return err
		}
		jobSpec.CbcoreOverrideVolume = volumes[0]
		jobSpec.CbcoreOverrideVolumeMount = mounts[0]
	}
	return nil
}

func (jobSpec *JobSpec) SetUserVenvVolumeAndMount(
	ctx context.Context,
	kubeClientSet kubernetes.Interface,
	sidecarEnv *pb.UserSidecarEnv) error {
	// On successful image builds, sidecar env will not be specified.
	// When image build jobs failed, cluster server attempts to "plug in" the existing
	// user venv Python environment to the needed containers through volume and mount.
	// If the user venv was created in an NFS cluster volume, then the user venv does not
	// need explicit mounting.
	if sidecarEnv == nil || sidecarEnv.MountDir == nil {
		return nil
	}
	var err error
	volumes, mounts, err := ResolveMountDirs(ctx, wscommon.Namespace, kubeClientSet, []*pb.MountDir{sidecarEnv.MountDir})
	jobSpec.UserFallbackVenvVolume = volumes[0]
	jobSpec.UserFallbackVenvVolumeMount = mounts[0]
	return err
}

func (jobSpec *JobSpec) SetSystemSecretVolumeAndMount() {
	optional := true
	jobSpec.SystemSecretVolume = &corev1.Volume{
		Name: wsapisv1.SystemSecretVolume,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: wsapisv1.SystemSecretName,
				Optional:   &optional,
			},
		},
	}
	jobSpec.SystemSecretVolumeMount = &corev1.VolumeMount{
		Name:      wsapisv1.SystemSecretVolume,
		MountPath: wsapisv1.SystemSecretDir,
		ReadOnly:  true,
	}
}

func (jobSpec *JobSpec) SetTensorStorageVolumeAndMount() {
	jobSpec.TensorStorageVolume = BuildHostPathVolume(wsapisv1.TensorStorageVolume, jobSpec.JobPathConfig.TensorStorageRootPath, corev1.HostPathDirectory)
	jobSpec.TensorStorageVolumeMount = &corev1.VolumeMount{
		Name:      wsapisv1.TensorStorageVolume,
		MountPath: jobSpec.JobPathConfig.TensorStorageRootPath,
	}
}

func (jobSpec *JobSpec) SetWorkerCacheVolumeAndMount() {
	// Add worker cache mount. Framework code assumes this location and performs input data movements.
	jobSpec.WorkerCacheVolume = BuildHostPathVolume(wsapisv1.WorkerCacheDirRWVolume, jobSpec.JobPathConfig.WorkerCacheRootPath, corev1.HostPathDirectoryOrCreate)
	jobSpec.WorkerCacheVolumeMount = &corev1.VolumeMount{
		Name:      wsapisv1.WorkerCacheDirRWVolume,
		MountPath: jobSpec.JobPathConfig.WorkerCacheRootPath,
	}
}

func (jobSpec *JobSpec) SetSharedMemoryVolumeAndMount() {
	// Add share memory mount. The default shared memory size is 64MB. Some data loader might
	// need more than that to operate. Create a mount point for share memory to allow these
	// use cases.
	jobSpec.SharedStorageVolume = &corev1.Volume{
		Name: wsapisv1.SharedMemoryVolume,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium: corev1.StorageMediumMemory,
			},
		},
	}
	jobSpec.SharedStorageVolumeMount = &corev1.VolumeMount{
		Name:      wsapisv1.SharedMemoryVolume,
		MountPath: jobSpec.JobPathConfig.WorkerSharedMemoryRootPath,
	}
}

func (jobSpec *JobSpec) SetLogExportVolumeAndMount(ctx context.Context, kubeClientSet kubernetes.Interface,
	targetType csctlpb.ExportTargetType, mountDirOutputPath string, hasMultiMgmtNodes bool) error {
	var err error
	if mountDirOutputPath == "" {
		rootPath := wscommon.Ternary(targetType == csctlpb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME,
			jobSpec.JobPathConfig.DebugArtifactRootPath, jobSpec.JobPathConfig.LogExportRootPath)
		if hasMultiMgmtNodes {
			pvc := wscommon.Ternary(targetType == csctlpb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME,
				wsapisv1.DebugArtifactPVCName, wsapisv1.LogExportPVCName)
			jobSpec.LogExportStagingVolume = &corev1.Volume{
				Name: wsapisv1.LogExportStagingVolume,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc,
						ReadOnly:  false,
					},
				},
			}
		} else {
			jobSpec.LogExportStagingVolume = &corev1.Volume{
				Name: wsapisv1.LogExportStagingVolume,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: rootPath,
					},
				},
			}
		}
		jobSpec.LogExportStagingVolumeMount = &corev1.VolumeMount{
			Name:        wsapisv1.LogExportStagingVolume,
			MountPath:   path.Join(rootPath, jobSpec.Namespace),
			SubPathExpr: jobSpec.Namespace,
		}
	} else {
		jobSpec.LogExportStagingVolume, jobSpec.LogExportStagingVolumeMount, err = getMountDirVolumeAndMount(
			ctx, kubeClientSet, jobSpec.Namespace, wsapisv1.LogExportStagingVolume, mountDirOutputPath, "", false)
	}
	return err
}

func (jobSpec *JobSpec) IsInfraSetupRequired() bool {
	_, ok := jobSpec.Annotations[wsapisv1.InfraSetupAnnot]
	return ok
}

func (jobSpec *JobSpec) SetInfraSetupIfRequired() {
	// Opportunistically use a separate job to create the workdir and cached compile NFS volumes.
	// TODO: Remove operator semver check in rel-2.7
	if jobSpec.OperatorSemver != nil &&
		!jobSpec.OperatorSemver.LessThan(wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ScInfraSetup]) {
		jobSpec.Annotations = wscommon.EnsureMap(jobSpec.Annotations)
		jobSpec.Annotations[wsapisv1.InfraSetupAnnot] = ""
	}
}

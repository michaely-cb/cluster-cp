/*
Copyright 2022 Cerebras Systems, Inc..
*/

package wsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	uuid "github.com/lithammer/shortuuid/v4"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	commonpb "cerebras/pb/workflow/appliance/common"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	rlversion "cerebras.com/job-operator/client-resourcelock/clientset/versioned"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	wsversion "cerebras.com/job-operator/client/clientset/versioned"
	wsv1 "cerebras.com/job-operator/client/clientset/versioned/typed/ws/v1"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	"cerebras.com/job-operator/common"
)

var SemanticVersion string // should be set by "go build"

const (
	DefaultPort = 9000
	MetricsPort = 9986

	UserVolumeAnnotationKey = "k8s.cerebras.com/user-volumes"

	ClientWorkdirAnnotationKey = "cerebras/client-workdir"

	// Secret name where docker credentials live on colovore
	regcredSecret = "regcred"

	schedulerHintPrefix = "k8s.cerebras.com/scheduler-hint-"

	rdmaDevices = "rdma/hca_devices"

	// As of Sept 2024, the appliance client plumbing in informing cluster-mgmt that
	// a job is inference is still in flux. The "replica" label comes from the production
	// deployment code and the "purpose" label comes from the dev/test deployment code.
	// Cluster-mgmt needs this info for:
	// - system frequency check during compile
	//   - systems can only survive when weight-streaming jobs running with a lower frequency
	//   - systems need running with a higher frequency in inference jobs to be performant
	// - scheduling optimization (act0 local) during execute
	inferenceJobReplicaLabelKey = "labels.k8s.cerebras.com/replica"
	inferenceJobCiLabelKey      = "labels.k8s.cerebras.com/purpose"
	inferenceJobCiLabelVal      = "inference-ci"
)

// For release 1.8.0, we are okay with the worker pods to do a `du /n0` on the partition to get the usage.
// Post 1.8.0, the job logs will be moved to `/n1` which is in the root partition. This will be a no-op for
// existing Framework integration, but would in general provide more accurate worker cache disk usage since
// the worker cache should be the only content residing in the SSD-backed partition `/n0`.
const DefaultWorkerCacheDirectory = "/n0/cache"

type WSJobClientProvider interface {
	// TODO: Merge service spec into job spec
	Create(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error)
	Get(namespace, jobName string) (*wsapisv1.WSJob, error)
	List(namespace string) (*wsapisv1.WSJobList, error)
	Delete(namespace, jobName string) error
	Update(*wsapisv1.WSJob) (*wsapisv1.WSJob, error)
	Annotate(wsjob *wsapisv1.WSJob, annotations map[string]string) (*wsapisv1.WSJob, error)
	LogExport(req *pb.CreateLogExportRequest, jobSpec *JobSpec) (*batchv1.Job, error)
	BuildImage(jobSpec *JobSpec, baseImage, pipOptions string, frozenDependencies []string, nodeSelector map[string]string) (*batchv1.Job, error)
	GetJobLogs(ctx context.Context, namespace, labelSelector string, tail int) (string, error)
}

type WSJobClient struct {
	WsV1Interface wsv1.WsV1Interface
	WsV1Client    wsv1.WSJobInterface
	Clientset     kubeclientset.Interface
	ctx           context.Context
}

type VolumeMount struct {
	// Name of the volume to mount
	Name string `json:"name"`

	// MountPath is the path on the container where the contents of Volume.Path will appear.
	MountPath string `json:"mountPath"`

	// SubPath is a subpath into the volume.Path mounted at the MountPath. Empty defaults to the volume's root.
	SubPath string `json:"subPath,omitempty"`
}

type ResourceRequest struct {
	CpuMilliCores           int64
	MemoryBytes             int64
	MemoryBytesUpperBound   int64
	MemoryBytesMaxLimit     int64
	PortCount               int64
	NvmfDiskBytes           int64
	NvmfDiskBytesUpperBound int64
}

// ServiceSpec contains the spec elements for a certain replica type
type ServiceSpec struct {
	Image       string
	Command     []string
	Env         []corev1.EnvVar
	Replicas    int32
	Annotations map[string]string
	Resources   ResourceRequest
}

type WSJobMode string

const (
	CompileJobMode          WSJobMode = "COMPILE"
	ExecuteJobMode          WSJobMode = "EXECUTE"
	UnknownJobMode          WSJobMode = "UNKNOWN"
	SdkCompileJobMode       WSJobMode = "SDK_COMPILE"
	SdkExecuteJobMode       WSJobMode = "SDK_EXECUTE"
	InferenceCompileJobMode WSJobMode = "INFERENCE_COMPILE"
	InferenceExecuteJobMode WSJobMode = "INFERENCE_EXECUTE"
)

func (jm WSJobMode) IsCompile() bool {
	return slices.Contains([]WSJobMode{CompileJobMode, SdkCompileJobMode, InferenceCompileJobMode}, jm)
}

func (jm WSJobMode) IsExecute() bool {
	return slices.Contains([]WSJobMode{ExecuteJobMode, SdkExecuteJobMode, InferenceExecuteJobMode}, jm)
}

type WSService int

const (
	WsCoordinator WSService = iota
	WsChief
	WsWorker
	WsWeight
	WsCommand
	WsActivation
	WsBroadcastReduce
	WSKVStorageServer
	WsSWDriver
	NumWsServices
)

var wsServiceToReplicaType = map[WSService]commonv1.ReplicaType{
	WsCoordinator:     wsapisv1.WSReplicaTypeCoordinator,
	WsChief:           wsapisv1.WSReplicaTypeChief,
	WsWorker:          wsapisv1.WSReplicaTypeWorker,
	WsWeight:          wsapisv1.WSReplicaTypeWeight,
	WsCommand:         wsapisv1.WSReplicaTypeCommand,
	WsActivation:      wsapisv1.WSReplicaTypeActivation,
	WsBroadcastReduce: wsapisv1.WSReplicaTypeBroadcastReduce,
	WSKVStorageServer: wsapisv1.WSReplicaTypeKVStorageServer,
	WsSWDriver:        wsapisv1.WSReplicaTypeSWDriver,
}

func (wsService WSService) String() string {
	return wsService.AsReplicaType().Lower()
}

func (wsService WSService) AsReplicaType() commonv1.ReplicaType {
	if rv, ok := wsServiceToReplicaType[wsService]; ok {
		return rv
	}
	return wsapisv1.WSReplicaTypeInvalid
}

// ServiceSpecs Each element in the array is for the specific replica type.
type ServiceSpecs []ServiceSpec

func NewWSJobClient(namespace string, config *rest.Config) (*WSJobClient, error) {
	if config == nil {
		config = ctrl.GetConfigOrDie()
	}
	// create the WsV1Client
	wsV1Client, err := wsv1.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	clientSet := kubeclientset.NewForConfigOrDie(config)

	wsClient := &WSJobClient{
		WsV1Interface: wsV1Client,
		WsV1Client:    wsV1Client.WSJobs(namespace),
		ctx:           context.Background(),
		Clientset:     clientSet,
	}
	return wsClient, nil
}

func NewWSJobInformer(ctx context.Context, global bool) (wsinformer.GenericInformer, error) {
	config := ctrl.GetConfigOrDie()
	wsV1ClientSet, err := wsversion.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	// wsjob informer - keep consistent with default runtime controller resync
	// https://github.com/kubernetes-sigs/controller-runtime/blob/release-0.12/pkg/cache/cache.go#L145
	// resync is basically doing a requeue which is unnecessary most of the time
	// https://github.com/kubernetes-sigs/controller-runtime/issues/521
	var options []wsinformer.SharedInformerOption
	if !global {
		options = append(options, wsinformer.WithNamespace(common.Namespace))
	}
	factory := wsinformer.NewSharedInformerFactoryWithOptions(wsV1ClientSet, 10*time.Hour, options...)
	informer, _ := factory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("wsjobs"))

	// Start with informer run.
	factory.Start(ctx.Done())

	// factory.Start() releases the execution flow without waiting for all the
	// internal machinery to warm up. We use factory.WaitForCacheSync() here
	// to poll for cmInformer.Informer().HasSynced(). Essentially, it's just a
	// fancy way to write a while-loop checking HasSynced() flags for all the
	// registered informers with 100ms delay between iterations.
	for informerType, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return nil, fmt.Errorf("failed to sync cache for %v", informerType)
		}
	}
	return informer, nil
}

func NewLockInformer(ctx context.Context, global bool) (wsinformer.GenericInformer, error) {
	config := ctrl.GetConfigOrDie()
	rlClient, err := rlversion.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	// lock informer - keep consistent with default runtime controller resync
	// https://github.com/kubernetes-sigs/controller-runtime/blob/release-0.12/pkg/cache/cache.go#L145
	// resync is basically doing a requeue which is unnecessary most of the time
	// https://github.com/kubernetes-sigs/controller-runtime/issues/521
	var options []rlinformer.SharedInformerOption
	if !global {
		options = append(options, rlinformer.WithNamespace(common.Namespace))
	}
	factory := rlinformer.NewSharedInformerFactoryWithOptions(rlClient, 10*time.Hour, options...)
	informer, _ := factory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("resourcelocks"))

	// Start with informer run.
	factory.Start(ctx.Done())

	// factory.Start() releases the execution flow without waiting for all the
	// internal machinery to warm up. We use factory.WaitForCacheSync() here
	// to poll for cmInformer.Informer().HasSynced(). Essentially, it's just a
	// fancy way to write a while-loop checking HasSynced() flags for all the
	// registered informers with 100ms delay between iterations.
	for informerType, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return nil, fmt.Errorf("failed to sync cache for %v", informerType)
		}
	}
	return informer, nil
}

// Simple Create wrapper
func (client *WSJobClient) Create(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
	result, err := client.WsV1Interface.WSJobs(wsjob.Namespace).
		Create(client.ctx, wsjob, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// BuildWSJobFromSpecs creates a WSJob from ServiceSpecs and JobSpec
func BuildWSJobFromSpecs(specs ServiceSpecs, jobSpec JobSpec) (*wsapisv1.WSJob, error) {
	serverOpts := ServerOptionsFromEnv()
	var jobName string
	if jobSpec.JobName != "" {
		jobName = jobSpec.JobName
	} else {
		jobName = GenWsjobId()
	}

	labels := map[string]string{}

	// Validate and set job mode
	var jobModeLabel string
	switch jobSpec.WsJobMode {
	case CompileJobMode, ExecuteJobMode:
		if jobSpec.Labels[inferenceJobReplicaLabelKey] != "" ||
			jobSpec.Labels[inferenceJobCiLabelKey] == inferenceJobCiLabelVal {
			if jobSpec.WsJobMode == CompileJobMode {
				jobSpec.WsJobMode = InferenceCompileJobMode
			} else {
				jobSpec.WsJobMode = InferenceExecuteJobMode
			}
			jobModeLabel = wsapisv1.JobModeInference
		} else {
			jobModeLabel = wsapisv1.JobModeWeightStreaming
		}
	case SdkCompileJobMode, SdkExecuteJobMode:
		jobModeLabel = wsapisv1.JobModeSdk
	case InferenceCompileJobMode, InferenceExecuteJobMode:
		jobModeLabel = wsapisv1.JobModeInference
	default:
		return nil, fmt.Errorf("job mode %s is unimplemented", jobSpec.WsJobMode)
	}

	labels[wsapisv1.JobModeLabelKey] = jobModeLabel

	if jobSpec.WsJobMode.IsCompile() {
		labels[wsapisv1.JobTypeLabelKey] = wsapisv1.JobTypeCompile
	} else {
		labels[wsapisv1.JobTypeLabelKey] = wsapisv1.JobTypeExecute
	}
	for k, v := range jobSpec.Labels {
		labels[k] = v
	}

	annotations := map[string]string{
		ClientWorkdirAnnotationKey:            jobSpec.ClientWorkdir,
		common.ClusterServerVersion:           serverOpts.ClusterServerVersion,
		wsapisv1.SemanticClusterServerVersion: SemanticVersion,
	}
	for k, v := range jobSpec.Annotations {
		annotations[k] = v
	}
	if len(jobSpec.WorkerUserVolumeMounts) > 0 {
		if m, err := json.Marshal(jobSpec.WorkerUserVolumeMounts); err != nil {
			log.WithField("mounts", jobSpec.WorkerUserVolumeMounts).
				WithField("err", err).
				Warn("failed to serialize mounts")
		} else {
			annotations[UserVolumeAnnotationKey] = string(m)
		}
	}
	for hintName, value := range jobSpec.SchedulerHints {
		annotations[schedulerHintPrefix+hintName] = value
	}
	// We do not use a preferred address in SDK execute jobs, since the only
	// existing pod is a worker and does not reside on the same node as Nginx.
	if jobSpec.EnableCrdAddrHandoff && jobSpec.WsJobMode != SdkExecuteJobMode {
		annotations[wsapisv1.WsJobCrdAddrAnnoKey] = ""
	}

	enableSvcCreation := jobSpec.EnableNonCRDServiceCreation
	annotations[wsapisv1.DisableNonCRDServicesAnnotation] = strconv.FormatBool(!enableSvcCreation)

	if !common.EnableStartupProbeAll {
		annotations[wsapisv1.EnableStartupProbeAll] = "false"
	}

	if jobSpec.AllowUnhealthySystems {
		// Sanity check: enabling this for a job ONLY makes sense if we also have a scheduler hint for allow-systems
		if _, ok := jobSpec.SchedulerHints[wsapisv1.SchedulerHintAllowSys]; !ok {
			log.WithField("job", jobName).Warn("enabling allow-unhealthy-systems without a scheduler hint")
			return nil, fmt.Errorf("enabling allow-unhealthy-systems without a scheduler hint on allowed-systems is not supported")
		}
		annotations[wsapisv1.EnableAllowUnhealthySystems] = "true"
	}

	replicaSpecs := make(map[commonv1.ReplicaType]*commonv1.ReplicaSpec)
	for specNo := 0; specNo < len(specs); specNo++ {
		spec := &specs[specNo]
		if spec.Replicas < 1 {
			continue
		}

		terminationGracePeriodSeconds := int64(30)
		if jobSpec.WsJobMode == SdkExecuteJobMode {
			terminationGracePeriodSeconds = int64(90)
		}
		tokens := strings.Split(spec.Image, ":")
		if len(tokens) > 1 {
			annotations[common.WsjobImageVersion] = tokens[len(tokens)-1]
		}

		var podVolumes []corev1.Volume
		var containerVolumeMounts []corev1.VolumeMount
		if jobSpec.ScriptRootVolume != nil {
			podVolumes = append(podVolumes, *jobSpec.ScriptRootVolume)
			containerVolumeMounts = append(containerVolumeMounts, *jobSpec.ScriptRootVolumeMount)
		}
		if jobSpec.WorkdirLogsVolume != nil {
			podVolumes = append(podVolumes, *jobSpec.WorkdirLogsVolume)
			containerVolumeMounts = append(containerVolumeMounts, *jobSpec.WorkdirLogsVolumeMount)
		}
		if jobSpec.CbcoreOverrideVolume != nil {
			podVolumes = append(podVolumes, *jobSpec.CbcoreOverrideVolume)
			containerVolumeMounts = append(containerVolumeMounts, *jobSpec.CbcoreOverrideVolumeMount)
		}
		if specNo == int(WsCoordinator) {
			if jobSpec.CachedCompileVolume != nil {
				podVolumes = append(podVolumes, *jobSpec.CachedCompileVolume)
				containerVolumeMounts = append(containerVolumeMounts, *jobSpec.CachedCompileVolumeMount)
			}
			if jobSpec.TensorStorageVolume != nil {
				podVolumes = append(podVolumes, *jobSpec.TensorStorageVolume)
				containerVolumeMounts = append(containerVolumeMounts, *jobSpec.TensorStorageVolumeMount)
			}
		}

		spec.Env = append(spec.Env,
			corev1.EnvVar{
				Name: "NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
			corev1.EnvVar{
				Name:  "WSJOB_ID",
				Value: jobName,
			},
			corev1.EnvVar{
				Name:  wsapisv1.WSJobModeEnv,
				Value: string(jobSpec.WsJobMode),
			},
			corev1.EnvVar{
				Name:  "WSJOB_SCRIPT_ROOT",
				Value: wsapisv1.WsjobEntrypointMountPath,
			},
			corev1.EnvVar{
				Name:  "WSJOB_INIT_SCRIPT_NAME",
				Value: "init.sh",
			},
			corev1.EnvVar{
				Name:  "WSJOB_RUN_SCRIPT_NAME",
				Value: "run.py",
			},
			corev1.EnvVar{
				Name: "MY_NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			corev1.EnvVar{
				Name:  "CLUSTER_SERVER_ADDRESS",
				Value: fmt.Sprintf("cluster-server.%s:%d", common.Namespace, DefaultPort),
			},
			corev1.EnvVar{
				Name:  wsapisv1.IsUserSidecarEnvVarName,
				Value: strings.ToUpper(strconv.FormatBool(false)),
			},
		)

		if jobSpec.GrpcSettings != nil {
			verbosityLevel := "ERROR"
			switch jobSpec.GrpcSettings.GrpcVerbosity {
			case commonpb.DebugArgs_DebugMGR_GrpcSettings_VERBOSITY_DEBUG:
				verbosityLevel = "DEBUG"
			case commonpb.DebugArgs_DebugMGR_GrpcSettings_VERBOSITY_INFO:
				verbosityLevel = "INFO"
			case commonpb.DebugArgs_DebugMGR_GrpcSettings_VERBOSITY_NONE:
				verbosityLevel = "NONE"
			default:
				verbosityLevel = "ERROR"
			}
			spec.Env = append(spec.Env, corev1.EnvVar{
				Name:  "GRPC_VERBOSITY",
				Value: verbosityLevel,
			})
		}

		repoIndex := strings.Index(spec.Image, "/")
		repo := spec.Image
		if repoIndex > -1 {
			repo = repo[:repoIndex]
		}
		var imagePullSecret []corev1.LocalObjectReference
		if jobSpec.ImagePullSecretHosts != nil {
			if _, shouldAddPullSecret := jobSpec.ImagePullSecretHosts[repo]; shouldAddPullSecret {
				imagePullSecret = []corev1.LocalObjectReference{{Name: regcredSecret}}
			}
		}

		podAnnotations := map[string]string{}
		for key, value := range spec.Annotations {
			podAnnotations[key] = value
		}

		rsRequest := map[corev1.ResourceName]resource.Quantity{}
		rsLimit := map[corev1.ResourceName]resource.Quantity{}

		if !jobSpec.DisableNonCRDResourceRequests || WSService(specNo) == WsCoordinator {
			rsRequest[corev1.ResourceCPU] = *resource.NewMilliQuantity(spec.Resources.CpuMilliCores, resource.DecimalSI)
			rsRequest[corev1.ResourceMemory] = *resource.NewQuantity(spec.Resources.MemoryBytes, resource.DecimalSI)

			// do not set limit for CPU, allow to burst
			rsLimit[corev1.ResourceMemory] = *resource.NewQuantity(spec.Resources.MemoryBytes, resource.DecimalSI)
			if specNo == int(WsActivation) || specNo == int(WsCommand) || specNo == int(WsWeight) || specNo == int(WsChief) || specNo == int(WSKVStorageServer) || specNo == int(WsSWDriver) {
				// as from 1.9, runtime services + chief will only request mem for easier troubleshooting
				delete(rsLimit, corev1.ResourceMemory)
			}

			if spec.Resources.MemoryBytes > 0 {
				podAnnotations[wsapisv1.PodMemoryBaseKey] =
					strconv.FormatInt(spec.Resources.MemoryBytes, 10)
			}
			if spec.Resources.MemoryBytesUpperBound > 0 {
				podAnnotations[wsapisv1.PodMemoryUpperBoundKey] =
					strconv.FormatInt(spec.Resources.MemoryBytesUpperBound, 10)
			}
			if spec.Resources.MemoryBytesMaxLimit > 0 {
				podAnnotations[wsapisv1.PodMemoryMaxLimitKey] =
					strconv.FormatInt(spec.Resources.MemoryBytesMaxLimit, 10)
			}
		}

		// Execute coordinator talks to prometheus for post-stall analysis
		if WSService(specNo) == WsCoordinator && jobSpec.WsJobMode.IsExecute() {
			spec.Env = append(spec.Env, corev1.EnvVar{
				Name:  "PROMETHEUS_URL",
				Value: serverOpts.PrometheusUrl,
			})
		}

		if WSService(specNo) == WsWeight && spec.Resources.NvmfDiskBytes > 0 {
			annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] =
				strconv.FormatInt(spec.Resources.NvmfDiskBytes, 10)

			// Only assign upper bound if the base was assigned
			// In reality it doesn't make a difference to the integration, since upstream
			// always provides both.
			if spec.Resources.NvmfDiskBytesUpperBound > 0 {
				annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] =
					strconv.FormatInt(spec.Resources.NvmfDiskBytesUpperBound, 10)
			}
		}

		securityContext := &corev1.SecurityContext{}
		if jobSpec.EnableRoCE && specNo != int(WsCoordinator) {
			securityContext.Capabilities = &corev1.Capabilities{
				Add: []corev1.Capability{
					"IPC_LOCK",
				},
			}
			// TODO(joy) Should BR nodes have different quantity?
			rsRequest[rdmaDevices] = *resource.NewQuantity(1, resource.DecimalSI)
			rsLimit[rdmaDevices] = *resource.NewQuantity(1, resource.DecimalSI)
		}

		replicaType := WSService(specNo).AsReplicaType()
		replicaPorts := wsapisv1.GenerateDefaultPorts(replicaType, spec.Resources.PortCount)

		wsContainer := corev1.Container{
			Name:            wsapisv1.DefaultContainerName,
			Image:           spec.Image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Env:             spec.Env,
			Command:         spec.Command,
			VolumeMounts:    containerVolumeMounts,
			Resources:       corev1.ResourceRequirements{Requests: rsRequest, Limits: rsLimit},
			SecurityContext: securityContext,
			Ports:           replicaPorts,
			WorkingDir:      jobSpec.WorkdirLogPath,
		}
		containers := []corev1.Container{wsContainer}

		switch WSService(specNo) {
		case WsWeight:
			if jobSpec.WeightSidecar != nil && jobSpec.WeightSidecar.Enabled {
				containers, podVolumes = setupUserSidecarContainer(jobSpec, WsWeight, wsContainer, rsRequest, rsLimit, podVolumes, containerVolumeMounts)
			}
		case WsWorker:
			if jobSpec.WorkerSidecar != nil && jobSpec.WorkerSidecar.Enabled {
				containers, podVolumes = setupUserSidecarContainer(jobSpec, WsWorker, wsContainer, rsRequest, rsLimit, podVolumes, containerVolumeMounts)
			}
		case WsActivation:
			if jobSpec.ActivationSidecar != nil && jobSpec.ActivationSidecar.Enabled {
				containers, podVolumes = setupUserSidecarContainer(jobSpec, WsActivation, wsContainer, rsRequest, rsLimit, podVolumes, containerVolumeMounts)
			}
		}

		for i := range containers {
			tokens := strings.Split(containers[i].Image, ":")
			imageTag := tokens[len(tokens)-1]
			// Setting the image tag environment variable for logging purposes only.
			containers[i].Env = append(containers[i].Env,
				corev1.EnvVar{
					Name:  "MY_IMAGE_TAG",
					Value: imageTag,
				},
			)
		}

		replicaSpec := &commonv1.ReplicaSpec{
			Replicas: &spec.Replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					Containers:                    containers,
					Volumes:                       podVolumes,
					ImagePullSecrets:              imagePullSecret,
					TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
				},
			},
			RestartPolicy: commonv1.RestartPolicyNever,
		}

		// Set uid/gid if uid is not 0 and CbcoreOverrideVolume is nil. CbcoreOverrideVolume stores the replacement files
		// for cbcore, so it requires root access to copy over these files.
		if jobSpec.JobUser.Uid != 0 && jobSpec.CbcoreOverrideVolume == nil {
			replicaSpec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
				RunAsUser:          &jobSpec.JobUser.Uid,
				RunAsGroup:         &jobSpec.JobUser.Gid,
				SupplementalGroups: jobSpec.JobUser.Groups,
			}
		}

		replicaSpec.Template.Spec.ServiceAccountName = jobSpec.ServiceAccountName

		if specNo == int(WsBroadcastReduce) &&
			jobSpec.DropCachesValue != commonpb.DebugArgs_DebugMGR_DROPCACHES_UNSPECIFIED {
			dropCachesPath := wsapisv1.DropCachesPath
			if jobSpec.DropCachesPath != "" {
				dropCachesPath = jobSpec.DropCachesPath
			}
			replicaSpec.Template.Spec.InitContainers = append(replicaSpec.Template.Spec.InitContainers, corev1.Container{
				Image:           wsapisv1.KubectlImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Name:            wsapisv1.DropCachesInitContainerName,
				Command: []string{
					"/bin/sh",
					"-c",
					fmt.Sprintf("sync; echo %d > %s", jobSpec.DropCachesValue,
						wsapisv1.DropCachesMountFile),
				},
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      wsapisv1.DropCachesVolumeName,
						MountPath: wsapisv1.DropCachesMountFile,
					},
				},
				// The /proc/sys/vm/drop_caches are only writable by root.
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:  &wsapisv1.RootUser.Uid,
					RunAsGroup: &wsapisv1.RootUser.Gid,
				},
			})
			hostPathType := corev1.HostPathFileOrCreate
			replicaSpec.Template.Spec.Volumes = append(replicaSpec.Template.Spec.Volumes,
				corev1.Volume{
					Name: wsapisv1.DropCachesVolumeName,
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: dropCachesPath,
							Type: &hostPathType,
						},
					},
				},
			)
		}
		replicaSpecs[replicaType] = replicaSpec
	}

	runPolicy := commonv1.RunPolicy{
		CleanPodPolicy: &jobSpec.CleanPodPolicy,
	}

	return &wsapisv1.WSJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   jobSpec.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: wsapisv1.WSJobSpec{
			RunPolicy:       runPolicy,
			WSReplicaSpecs:  replicaSpecs,
			NumWafers:       uint32(jobSpec.NumWafers),
			ActToCsxDomains: jobSpec.ActToCsxDomains,
			User: &wsapisv1.JobUser{
				Uid:      jobSpec.JobUser.Uid,
				Gid:      jobSpec.JobUser.Gid,
				Username: jobSpec.JobUser.Username,
				Groups:   jobSpec.JobUser.Groups,
			},
			Priority:      jobSpec.Priority,
			Notifications: jobSpec.Notifications,
		},
	}, nil
}

func (client *WSJobClient) Get(namespace, jobName string) (*wsapisv1.WSJob, error) {
	wsjob, err := client.WsV1Interface.WSJobs(namespace).Get(client.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return wsjob, nil
}

func (client *WSJobClient) Update(newJob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
	wsjob, err := client.WsV1Interface.WSJobs(newJob.Namespace).Update(client.ctx, newJob, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return wsjob, nil
}

func (client *WSJobClient) Annotate(wsjob *wsapisv1.WSJob, annotations map[string]string) (*wsapisv1.WSJob, error) {
	wsjob, err := client.WsV1Interface.WSJobs(wsjob.Namespace).Patch(client.ctx,
		wsjob.Name, types.MergePatchType,
		common.GetAnnotationPatch(annotations), metav1.PatchOptions{})
	if err == nil {
		log.Infof("successfully annotate wsjob: %s, anno: %v", wsjob.Name, annotations)
		return wsjob, nil
	}
	log.Warnf("failed to annotate wsjob, %s: %v", wsjob.Name, err)
	return nil, err
}

func (client *WSJobClient) List(namespace string) (*wsapisv1.WSJobList, error) {
	wsjobs, err := client.WsV1Interface.WSJobs(namespace).List(client.ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return wsjobs, nil
}

func (client *WSJobClient) Delete(namespace, jobName string) error {
	return client.WsV1Interface.WSJobs(namespace).Delete(client.ctx, jobName, metav1.DeleteOptions{})
}

func (client *WSJobClient) LogExport(req *pb.CreateLogExportRequest, jobSpec *JobSpec) (*batchv1.Job, error) {
	makeExclusionUsageError := func(arg1, arg2 string) error {
		return grpcStatus.Errorf(codes.InvalidArgument, "incompatible args: %s cannot be used with %s", arg1, arg2)
	}
	if req.NumLines != 0 {
		if req.Binaries {
			return nil, makeExclusionUsageError("numLines", "binaries")
		} else if req.IncludeCompileArtifacts {
			return nil, makeExclusionUsageError("numLines", "includeCompileArtifacts")
		} else if req.SkipClusterLogs {
			return nil, makeExclusionUsageError("numLines", "skipClusterLogs")
		} else if req.ExportTargetType == pb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME {
			return nil, makeExclusionUsageError("numLines", "debugVolumeExportType")
		}
	}
	if req.ExportTargetType == pb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME {
		if req.Binaries {
			return nil, makeExclusionUsageError("debugVolumeExportType", "binaries")
		} else if req.IncludeCompileArtifacts {
			return nil, makeExclusionUsageError("debugVolumeExportType", "includeCompileArtifacts")
		} else if req.OutputPath != "" {
			return nil, makeExclusionUsageError("debugVolumeExportType", "outputPath")
		}
		// The "copy-nfs-logs" option is not well known, relaxing the behavior to always copy here so users don't need to be aware
		req.CopyNfsLogs = true
	}

	job := logExportJob(req, jobSpec)
	_, err := client.Clientset.BatchV1().Jobs(jobSpec.Namespace).Create(client.ctx, job, metav1.CreateOptions{})
	return job, err
}

func (client *WSJobClient) BuildImage(jobSpec *JobSpec, baseImage, pipOptions string,
	packageDependencies []string, nodeSelector map[string]string) (*batchv1.Job, error) {
	job, err := client.Clientset.BatchV1().Jobs(jobSpec.Namespace).Create(client.ctx,
		buildImage(jobSpec, baseImage, pipOptions, packageDependencies, nodeSelector), metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (client *WSJobClient) String() string {
	serverOpts := ServerOptionsFromEnv()
	imagePullSecretHosts := ParseImagePullSecret(client.Clientset)
	return fmt.Sprintf("WsJobClient{imagePullRegistryHosts: %v, jobPathConfig: %v}",
		common.Keys(imagePullSecretHosts), serverOpts.JobPathConfig)
}

func setupUserSidecarContainer(
	jobSpec JobSpec,
	wsService WSService,
	wsContainer corev1.Container,
	rsRequest, rsLimit map[corev1.ResourceName]resource.Quantity,
	podVolumes []corev1.Volume,
	containerVolumeMounts []corev1.VolumeMount,
) ([]corev1.Container, []corev1.Volume) {
	userSidecarContainer := wsContainer.DeepCopy()
	for i, envVar := range userSidecarContainer.Env {
		if envVar.Name == wsapisv1.IsUserSidecarEnvVarName {
			userSidecarContainer.Env[i].Value = strings.ToUpper(strconv.FormatBool(true))
			break
		}
	}

	image := ""
	var pythonPathsAddon []string
	pyPathDelimiter := ":"
	sidecarMemResPct := int64(0)
	sidecarCpuResPct := int64(0)

	splitResources := func(original corev1.ResourceList, cpuPercent, memPercent int64) (corev1.ResourceList, corev1.ResourceList) {
		split := make(corev1.ResourceList)
		remaining := make(corev1.ResourceList)
		for k, v := range original {
			switch k {
			case corev1.ResourceCPU:
				splitQuantity := v.DeepCopy()
				splitQuantity.SetMilli(v.MilliValue() / 100 * cpuPercent)
				split[k] = splitQuantity
				remainingQuantity := v.DeepCopy()
				remainingQuantity.SetMilli(v.MilliValue() / 100 * (100 - cpuPercent))
				remaining[k] = remainingQuantity
			case corev1.ResourceMemory:
				splitQuantity := v.DeepCopy()
				splitQuantity.Set(v.Value() / 100 * memPercent)
				split[k] = splitQuantity
				remainingQuantity := v.DeepCopy()
				remainingQuantity.Set(v.Value() / 100 * (100 - memPercent))
				remaining[k] = remainingQuantity
			case rdmaDevices:
				// keep rdmaDevices the same on both main and sidecar container
				split[k] = v.DeepCopy()
				remaining[k] = v.DeepCopy()
			default:
				// This should not happen
				log.Errorf("Unexpected resource %s", k)
			}
		}
		return split, remaining
	}

	// Add common volumes and mounts before adding role-specific ones
	if jobSpec.UserFallbackVenvVolume != nil {
		podVolumes = append(podVolumes, *jobSpec.UserFallbackVenvVolume)
	}
	var sidecarMounts []corev1.VolumeMount
	sidecarMounts = append(sidecarMounts, containerVolumeMounts...)
	if jobSpec.UserFallbackVenvVolumeMount != nil {
		sidecarMounts = append(sidecarMounts, *jobSpec.UserFallbackVenvVolumeMount)
	}

	if wsService == WsWeight {
		userSidecarContainer.Name = wsapisv1.UserSidecarContainerName
		image = jobSpec.WeightSidecar.Image

		// Add the shared storage volume for the weight sidecar, if enabled
		if jobSpec.SharedStorageVolume != nil {
			podVolumes = append(podVolumes, *jobSpec.SharedStorageVolume)
			wsContainer.VolumeMounts = append(wsContainer.VolumeMounts, *jobSpec.SharedStorageVolumeMount)
			sidecarMounts = append(sidecarMounts, *jobSpec.SharedStorageVolumeMount)
		}

		sidecarMemResPct = int64(jobSpec.WeightSidecar.MemPercent)
		sidecarCpuResPct = int64(jobSpec.WeightSidecar.CpuPercent)
	} else if wsService == WsActivation {
		userSidecarContainer.Name = wsapisv1.UserSidecarContainerName
		image = jobSpec.ActivationSidecar.Image

		// Add the shared storage volume for the activation sidecar, if enabled
		if jobSpec.SharedStorageVolume != nil {
			podVolumes = append(podVolumes, *jobSpec.SharedStorageVolume)
			wsContainer.VolumeMounts = append(wsContainer.VolumeMounts, *jobSpec.SharedStorageVolumeMount)
			sidecarMounts = append(sidecarMounts, *jobSpec.SharedStorageVolumeMount)
		}

		sidecarMemResPct = int64(jobSpec.ActivationSidecar.MemPercent)
		sidecarCpuResPct = int64(jobSpec.ActivationSidecar.CpuPercent)

	} else if wsService == WsWorker {
		// We use the legacy streamer container name here to work with operators with lower version.
		// In CGs, we cannot update the job operator as we wish and the new pod spec must still work with
		// existing assumptions from the operator.
		// TODO: Update this name in rel-2.6 when operator runs the rel-2.5 or later version
		userSidecarContainer.Name = wsapisv1.StreamerContainerName
		image = jobSpec.WorkerSidecar.Image

		// set up volumes and mounts for the worker sidecar
		for _, v := range jobSpec.WorkerUserVolumes {
			if v != nil {
				podVolumes = append(podVolumes, *v)
			}
		}
		for _, vm := range jobSpec.WorkerUserVolumeMounts {
			if vm != nil {
				sidecarMounts = append(sidecarMounts, *vm)
			}
		}
		if jobSpec.WsJobMode == ExecuteJobMode {
			if jobSpec.WorkerCacheVolume != nil {
				podVolumes = append(podVolumes, *jobSpec.WorkerCacheVolume)
				wsContainer.VolumeMounts = append(wsContainer.VolumeMounts, *jobSpec.WorkerCacheVolumeMount)
				sidecarMounts = append(sidecarMounts, *jobSpec.WorkerCacheVolumeMount)
			}
			if jobSpec.SharedStorageVolume != nil {
				podVolumes = append(podVolumes, *jobSpec.SharedStorageVolume)
				wsContainer.VolumeMounts = append(wsContainer.VolumeMounts, *jobSpec.SharedStorageVolumeMount)
				sidecarMounts = append(sidecarMounts, *jobSpec.SharedStorageVolumeMount)
			}
		} else if jobSpec.WsJobMode == SdkExecuteJobMode {
			if jobSpec.SystemSecretVolume != nil {
				podVolumes = append(podVolumes, *jobSpec.SystemSecretVolume)
				wsContainer.VolumeMounts = append(wsContainer.VolumeMounts, *jobSpec.SystemSecretVolumeMount)
			}
		}
		// set up python paths addon
		pythonPathsAddon = append(pythonPathsAddon, jobSpec.WorkerPythonPaths...)
		sidecarMemResPct = int64(jobSpec.WorkerSidecar.MemPercent)
		sidecarCpuResPct = int64(jobSpec.WorkerSidecar.CpuPercent)
	}
	userSidecarContainer.VolumeMounts = sidecarMounts

	if image != "" {
		userSidecarContainer.Image = image
	}

	if jobSpec.UserSidecarEnv.PythonPath != "" {
		pythonPathsAddon = append(pythonPathsAddon, jobSpec.UserSidecarEnv.PythonPath)
	}

	// set up environment variables for weight
	sidecarEnvs := append(userSidecarContainer.Env,
		corev1.EnvVar{
			Name:  wsapisv1.UserVenvPathEnvVarName,
			Value: jobSpec.UserSidecarEnv.CleanupPath,
		}, corev1.EnvVar{
			Name:  wsapisv1.PythonPathAddonEnvVarName,
			Value: strings.Join(pythonPathsAddon, pyPathDelimiter),
		},
	)
	userSidecarContainer.Env = sidecarEnvs

	// set up resource requests and limits for ws and sidecar
	userSidecarContainerLimits, wsContainerLimits := splitResources(rsLimit, sidecarCpuResPct, sidecarMemResPct)
	userSidecarContainerRequests, wsContainerRequests := splitResources(rsRequest, sidecarCpuResPct, sidecarMemResPct)
	userSidecarContainer.Resources = corev1.ResourceRequirements{
		Limits:   userSidecarContainerLimits,
		Requests: userSidecarContainerRequests,
	}
	wsContainer.Resources = corev1.ResourceRequirements{
		Limits:   wsContainerLimits,
		Requests: wsContainerRequests,
	}
	return []corev1.Container{wsContainer, *userSidecarContainer}, podVolumes
}

func ParseImagePullSecret(client kubeclientset.Interface) map[string]bool {
	rv := map[string]bool{}
	secret, err := client.CoreV1().Secrets(common.Namespace).
		Get(context.Background(), regcredSecret, metav1.GetOptions{})
	if err != nil {
		log.Warnf("failed to get image registry secret from k8s: %v", err)
		return rv
	}
	log.WithField("secret", regcredSecret).Info("found image registry secret")
	const regcredDataPath = ".dockerconfigjson"
	type dockerJson struct {
		Auths map[string]interface{}
	}
	if jsondata, ok := secret.Data[regcredDataPath]; ok {
		parsed := &dockerJson{}
		err = json.NewDecoder(bytes.NewReader(jsondata)).Decode(parsed)
		if err != nil {
			log.Warnf("error parsing secret %v", err)
			return rv
		}
		for k := range parsed.Auths {
			rv[k] = true
		}
		return rv
	}
	log.Warnf("%s was not present in regcred secret data", regcredDataPath)
	return rv
}

// Get job logs, assuming latest pod if there were multiple
func (client *WSJobClient) GetJobLogs(ctx context.Context, namespace, jobName string, tail int) (string, error) {
	pods, err := client.Clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil || len(pods.Items) == 0 {
		return "", fmt.Errorf("job pods not found %v", err)
	}
	options := &corev1.PodLogOptions{}
	if tail != 0 {
		lines := int64(tail)
		options.TailLines = &lines
	}

	latestPod := pods.Items[0]
	for i := 1; i < len(pods.Items); i++ {
		if pods.Items[i].CreationTimestamp.After(latestPod.CreationTimestamp.Time) {
			latestPod = pods.Items[i]
		}
	}
	req := client.Clientset.CoreV1().Pods(namespace).GetLogs(latestPod.Name, options)
	podLogs, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer podLogs.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func GenWsjobId() string {
	return "wsjob-" + GenUUID()
}

func GenWorkflowId() string {
	return "wflow-" + GenUUID()
}

func GenUUID() string {
	return strings.ToLower(uuid.New())
}

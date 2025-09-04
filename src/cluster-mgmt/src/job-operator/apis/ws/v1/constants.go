/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	commonapisv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

const (
	// DefaultPortName is name of the port used to communicate between different services.
	DefaultPortName = "wsjob-port"

	// DefaultWhostPortName is prefix of name of the port used to communicate between RT Host (Act/Wgt etc.).
	DefaultWhostPortNamePrefix = "whost-port-"

	// DefaultDebugPortName is name of the port used for debug orchestration purposes.
	DefaultDebugPortName = "debug-port"

	// DefaultContainerName is the name of the main WSJob container.
	DefaultContainerName = "ws"

	// DefaultInfraSetupContainerName is the name of the container which
	// sets up the infrastructure for other containers.
	DefaultInfraSetupContainerName = "infra-setup"

	// UserSidecarContainerName is the name of the sidecar container which represents the user environment.
	UserSidecarContainerName = "user-sidecar"

	// StreamerContainerName is the name of the streamer container in worker pods.
	// We introduced UserSidecarContainerName in rel-2.5 which is more generic than StreamerContainerName.
	// TODO: Remove all references of this variable
	StreamerContainerName = "streamer"

	// DefaultPort is default value of the port.
	DefaultPort = 9000

	// DefaultWhostPort is default value of the port which is used for whost communication.
	DefaultWhostPortStart = 9100

	// number of ports to open per WGT server
	DefaultWgtPortCount = 5

	// number of ports to open per ACT server
	DefaultActKvssPortCount = 2

	// number of ports to open per SWDR server
	DefaultSwdrPortCount = 2

	// DefaultDebugPort is default value of the port which is used for debug workflow.
	DefaultDebugPort = 9002

	// DefaultUserSidecarPort is the default value of the port which is used for user sidecar.
	DefaultUserSidecarPort = 9005

	// DefaultRestartPolicy is default RestartPolicy for WSReplicaSpec.
	DefaultRestartPolicy = commonapisv1.RestartPolicyNever

	// Kind is the kind name.
	Kind = "WSJob"

	// Plural is the Plural form of wsJob.
	Plural = "wsjobs"

	// Singular is the singular form of wsJob.
	Singular = "wsjob"

	ReplicaId      = "REPLICA_ID"
	ReplicaType    = "REPLICA_TYPE"
	ReplicaWorkdir = "REPLICA_WORKDIR"

	DummyIpPort = "0.0.0.0:0"
	DummyIp     = "0.0.0.0"

	DefaultServiceDomain = "cerebrassc.local"
	// ServiceDomainProp is the key for properties field in cluster configuration for serviceDomain.
	ServiceDomainProp = "serviceDomain"
	// ClusterServerSubdomain is the subdomain of the ServiceDomain which GRPC traffic to cluster-server ingresses on.
	ClusterServerSubdomain = "cluster-server"
	// EnvCustomClusterDomain is the custom defined cluster domain, such as "svc.cluster.local".
	// Ref: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#a-records
	EnvCustomClusterDomain = "CUSTOM_CLUSTER_DOMAIN"

	ConfigMapType               = "k8s.cerebras.com/wsjob-configmap-type"
	ConfigMapTypeBRConfig       = "br-config"
	ConfigMapTypeClusterDetails = "cluster-details"
	ConfigMapTypeNICLock        = "nic-lock"
	BRNodeLabel                 = "node"
	BRLockHolderLabel           = "holder"
	BRLockHolderNamespaceLabel  = "holder-ns"

	// BrConfigName BRConfig related
	BrConfigName               = "br-config-%s"
	BrConfigVolume             = "br-config-volume"
	BrConfigMountPath          = "/opt/cerebras/br_cfg"
	BrConfigFileName           = "br_cfg_json"
	BrConfigCompressedFileName = "br_cfg_json_gz"
	BrConfigUpdatedSignal      = "br_cfg_updated_signal"
	BrConfigPathEnv            = "WS_BR_CONFIG_FP"
	BrConfigSignalEnv          = "WS_BR_UPDATE_SIGNAL"
	BrConfigAnno               = "cerebras/cfg_br_generated"

	// ClusterServerIngressName is the ingress name of the cluster server
	ClusterServerIngressName = "cluster-server"

	WSJobModeEnv = "WS_JOB_MODE"
	DebugArgsEnv = "WS_DEBUG_ARGS"
	// CompileRootDirEnv example: /n1/wsjob/compile_root
	CompileRootDirEnv = "COMPILE_ROOT_DIR"
	// RelativeCompileDirEnv example: cached_compile
	// Used for init container to create the cached compile dir
	RelativeCompileDirEnv = "RELATIVE_COMPILE_DIR"
	// CompileArtifactDirEnv example: /n1/wsjob/compile_root/<ns>/cached_compile/cs_123
	// Used for cached compile heartbeat for the execute job
	CompileArtifactDirEnv    = "COMPILE_ARTIFACT_DIR"
	CompileSystemTypeEnv     = "COMPILE_SYSTEM_TYPE"
	CompileFabricTypeEnv     = "COMPILE_FABRIC_TYPE"
	MaxTrainFabricFreqMhzEnv = "MAX_TRAIN_FABRIC_FREQ_MHZ"
	DebugVizUrlPrefixEnv     = "DEBUG_VIZ_URL_PREFIX"

	InitSignalVal            = "init"
	UpdatedSignalVal         = "updated"
	CfgUpdatedAnno           = "cerebras/cfg_update_done"
	SystemSecretDir          = "/opt/cerebras/secret/system"
	DefaultInitContainerName = "init"

	SystemSecretName = "system-basic-auth"

	WsjobEntrypointMountPath = "/opt/cerebras/cluster_mgmt"

	DropCachesMountFile         = "/host/proc/sys/vm/drop_caches"
	DropCachesPath              = "/proc/sys/vm/drop_caches"
	DropCachesVolumeName        = "drop-caches-volume"
	DropCachesInitContainerName = "drop-caches-init"

	// ClusterDetailsConfig related constants
	ClusterDetailsConfigName               = "cluster-details-config-%s"
	ClusterDetailsConfigVolume             = "cluster-details-config-volume"
	ClusterDetailsConfigMountPath          = "/opt/cerebras/cfg"
	ClusterDetailsConfigFileName           = "cluster_details_cfg_json"
	ClusterDetailsConfigCompressedFileName = "cluster_details_cfg_json_gz"
	ClusterDetailsConfigUpdatedSignal      = "cluster_details_cfg_updated_signal"
	ClusterDetailsConfigPathEnv            = "WS_CLUSTER_DETAILS_FP"
	ClusterDetailsConfigSignalEnv          = "WS_CLUSTER_DETAILS_UPDATE_SIGNAL"
	ClusterDetailsAnno                     = "cerebras/cfg_cluster_details_generated"
	ClusterDetailsActToCsxDomains          = "cluster_details_act_to_csx_domains_gz"
	ClusterDetailsKvssToCsxDomains         = "cluster_details_kvss_to_csx_domains_gz"
	ClusterDetailsSWDriverToCsxDomains     = "cluster_details_swdriver_to_csx_domains_gz"
	OperatorGeneratedConfigAnnot           = "operator-generated"

	// this is used for backwards compatible features
	// global operator can use info to decide whether a new feature applies for current job
	// e.g. version <=1.0.1 don't support compressed br/cd config
	// this is helpful to avoid a new annotation for each new feature
	SemanticApplianceClientVersion = "cerebras/semantic-version-appliance-client"
	SemanticClusterServerVersion   = "cerebras/semantic-version-cluster-server"
	SemanticJobOperatorVersion     = "cerebras/semantic-version-job-operator"

	WsJobResourceDetailsFPEnv   = "WS_CLUSTER_RESOURCE_DETAILS_FP"
	WsJobResourceDetailsV2FPEnv = "WS_CLUSTER_RESOURCE_DETAILS_V2_FP"
	WsJobResourceDetailsName    = "cluster_resource_details_json"
	WsJobResourceDetailsV2Name  = "cluster_resource_details_v2_json"
	DefaultBandwidthBps         = 100_000_000_000 // hardcode 100Gbps
	DefaultAxBandwidthBps       = 400_000_000_000 // hardcode 400Gbps

	JobTypeLabelKey           = "k8s.cerebras.com/wsjob-job-type"
	JobTypePrioritizedCompile = "prioritized-compile"
	JobTypeCompile            = "compile"
	JobTypeExecute            = "execute"
	JobModeLabelKey           = "k8s.cerebras.com/job-mode"
	JobModeWeightStreaming    = "weight-streaming"
	JobModeInference          = "inference"
	JobModeSdk                = "sdk"

	// System maintenance labels
	SystemMaintLabelKey      = "cerebras/system-maintenance-type"
	SystemMaintTypeMemBist   = "membist"
	SystemMaintTypeVddc      = "vddc"
	SystemMaintTypeWaferDiag = "wafer-diag"

	// System maintenance annotations
	SystemMaintDryrunAnnot = "dryrun"

	AffinityPrefix        = "cerebras/affinity-"
	AntiAffinityPrefix    = "cerebras/anti-affinity-"
	JobAffinityPrefix     = "cerebras/job-allocate-affinity-"
	JobAntiAffinityPrefix = "cerebras/job-allocate-anti-affinity-"
	BrPreferIdleSx        = "cerebras/prefer-idle-sx-node"
	JobSkipBRKey          = "cerebras/skip-br"

	LabelSeparator     = "__"
	PlatformVersionKey = "cerebras/platform-version"
	SystemVersionKey   = "cerebras/system-version"

	PrometheusSvc  = "prometheus-prometheus"
	ThanosQuerySvc = "thanos-query"
	PrometheusNs   = "prometheus"

	UseAxOnlyForRuntimeKey   = "cerebras/use-ax-only-for-runtime"
	UseAxOnlyForRuntimeValue = "true"

	// WorkflowIdLabelKey defines a workflow id value that belongs to all jobs invoked from a single
	// user-side invocation
	WorkflowIdLabelKey               = "cerebras/workflow-id"
	HasReservationKey                = "cerebras/has-reservation"
	QueryReservationKey              = "cerebras/query-reservation"
	QueryReservationContextKey       = "cerebras/query-reservation-context"
	SubsidiaryReservationKey         = "cerebras/subsidiary-reservation"
	ChildJobAnnotKey                 = "cerebras/child-job"
	ParentJobAnnotKey                = "cerebras/parent-job"
	DefaultJobPriorityValue          = 299
	MinJobPriorityValue              = 0
	MaxJobPriorityValue              = 399
	MaxP0JobPriorityValue            = 99
	MaxP1JobPriorityValue            = 199
	MaxP2JobPriorityValue            = DefaultJobPriorityValue
	MaxP3JobPriorityValue            = MaxJobPriorityValue
	AppliedDefaultLimitsKey          = "cerebras/wsjob-job-default-limits"
	PodMemoryBaseKey                 = "memory-bytes-base"
	PodMemoryUpperBoundKey           = "memory-bytes-upper-bound"
	PodMemoryMaxLimitKey             = "memory-bytes-max-limit"
	RuntimeMemoryEnvVarName          = "WS_RT_SET_MEM_MIB"
	RuntimeMemoryBytesEnvVarName     = "WS_RT_SET_MEM_B"
	RuntimeMinMemoryEnvVarName       = "WS_RT_MIN_%s_SET_MEM_MIB"
	RuntimeCsPortEnvVarName          = "WS_RT_SET_PORT"
	RuntimeCsPortAffinityEnvVarName  = "WS_RT_PORT_AFFINITY"
	RuntimeLinearMemRangeXEnvVarName = "WS_RT_SET_LMR_X"
	IsUserSidecarEnvVarName          = "IS_USER_SIDECAR"

	// Example value: "/dev/datanvme101:0-100,/dev/datanvme102:20-120"
	NvmfUsableRangesEnvVarName = "NVMF_USABLE_RANGES"
	// domains each activation pod needs to talk to per csx
	// After cluster server generates cluster details in rel-2.5, the domains information should be
	// derived from cluster details, rather than from wsjob spec.
	// TODO: Remove all legacy usage in rel-2.7.
	RuntimeTargetedDomainsKey = "runtime-targeted-domains"
	// domains each activation pod needs to talk to for all csx
	AllRuntimeTargetedDomainsKey = "all-runtime-targeted-domains"
	// domains each KVSS pod needs to talk to for all csx
	AllPromptCachingTargetedDomainsKey = "all-prompt-caching-targeted-domains"
	// domains each SWDR pod needs to talk to for all csx
	AllSwdrTargetedDomainsKey = "all-swdr-targeted-domains"
	BrInstanceIdEnvVarName    = "BR_CFG_INDEX"

	// allow jobs to share the same AX node
	InferenceJobSharingAnnotKey = "inference-job-sharing"

	// disallow swdrivers to land on the same node
	// TODO: remove once SwDriver is scheduled on IX/SX
	SwDriverMutualExclusionAnnotKey = "sw-driver-mutual-exclusion"

	// todo: remove all cerebras.com
	// DisableNonCRDServicesAnnotation set to "true" disables the creation of services + endpoints for Non CRD-like
	// replicas (e.g. only creates services for CRD or WRK in case of SDK execute job)
	DisableNonCRDServicesAnnotation = "cerebras/disable-non-crd-services"
	// EnableStartupProbeAll indicates that all replicas needs a startup probe
	EnableStartupProbeAll = "cerebras/startup-probe-all"

	// CancelWithStatusKey Define a key/value pair to be used to indicate a user issued cancellation action to a job
	// The cancellation can mark a job as 'cancelled/succeeded/failed'.
	CancelWithStatusKey       = "k8s.cerebras.com/cancel-with-status"
	CancelWithStatusCancelled = "cancelled"
	CancelWithStatusFailed    = "failed"
	CancelWithStatusSucceeded = "succeeded"
	// CancelJobContextKey captures the message to describe what triggered the job canceling
	CancelJobContextKey = "k8s.cerebras.com/wsjob-cancel-context"
	// CancelReasonKey key for cancel reason
	CancelReasonKey = "k8s.cerebras.com/wsjob-cancel-reason"
	// ExpiredLeaseCancelReason indicates job was canceled due to lease expiration
	ExpiredLeaseCancelReason = "expired-lease"
	// CanceledByClientReason indicates job was canceled/terminated by client
	CanceledByClientReason = "canceled-by-client"
	// CanceledByCsctlReason indicates job was canceled by csctl
	CanceledByCsctlReason = "canceled-by-csctl"
	// CanceledByServerReason indicates job was canceled by server, likely due to some fatal errors.
	CanceledByServerReason = "canceled-by-server"
	// ClientLeaseDisabledLabelKey indicates whether a job expects heartbeats from clients to renew leases
	ClientLeaseDisabledLabelKey = "k8s.cerebras.com/client-lease-disabled"
	// InitContainerOverrideKey indicates whether init container is already set and override default one
	InitContainerOverrideKey = "cerebras/init-container-override"

	PodDataNetNotAttached = "PodDataNetNotAttached"

	MemxPopNodegroupProp          = "memxPopulated"
	DefaultMinMemxPerPopNodegroup = 10

	// Cluster server can set this annotation to trigger job operator to acquire more populated nodegroups
	// for execute jobs. When not set, the number of populated nodegroups should always be 1.
	WsJobPopulatedNodegroupCount = "cerebras/populated-nodegroup-count"

	// Cluster server can set this annotation to trigger job operator to only consider populated nodegroups
	// of certain types, separated by commas. When not set, job operator only considers using regular and
	// large nodegroups.
	WsJobPopulatedNodegroupTypes = "cerebras/populated-nodegroup-types"
	MemxMemClassProp             = "memClass"
	PopNgTypeRegular             = "regular"
	PopNgTypeLarge               = "large"
	PopNgTypeXLarge              = "xlarge"

	// In V2 networking clusters, by default jobs will acquire the same number of activation nodes as systems.
	// This knob allows to override the default behavior and makes a best effort to acquire a number of activation
	// nodes that is greater than the number of systems.
	WsJobActivationNodeCount = "cerebras/activation-node-count"

	// Client can set this annotation key to trigger job-operator to update the value with the specific
	// address of the node the coordinator is running on as an network routing optimization.
	WsJobCrdAddrAnnoKey = "cerebras/coordinator-preferred-addr"

	// Client can set this annotation key so the scheduler can opportunistically grant disk space for
	// fast checkpointing. What Stack passes to cluster-mgmt is the bare-minimum disk space requirement
	// for each weight shard. From Runtime's perspective, having 2x the requested memory is generally helpful
	// in improving checkpoint performance. Therefore the nvmf granting routine would first attempt acquiring
	// the upperbound (2x the baseline memory request), if failed followed by a the baseline memory request.
	WsJobPerWeightShardNvmfBytesAnnotKey   = "cerebras/per-weight-shard-nvmf-disk-bytes-requested"
	WsJobPerWeightShardNvmfBytesUbAnnotKey = "cerebras/per-weight-shard-nvmf-disk-bytes-upperbound-requested"

	EnablePartialSystemKey = "ENABLE_PARTIAL_SYSTEM"
	BuildBalancedBrTreeKey = "BUILD_BALANCED_BR_TREE"
	BrTreePolicyEnv        = "BR_TREE_POLICY"
	BrTreePolicyKey        = "br_tree_policy"
	BrTreePolicySpine      = "SPINE"
	BrTreePolicyPort       = "PORT"

	RtNodeMaxLimitReservedMemGbKey    = "RT_NODE_MAX_LIMIT_RESERVED_MEM_GB"
	RtNodeMaxLimitReservedMemRatioKey = "RT_NODE_MAX_LIMIT_RESERVED_MEM_RATIO"

	// Modifier-specific constants
	UserVenvPathEnvVarName    = "USER_VENV_PATH"
	PythonPathAddonEnvVarName = "PYTHONPATH_ADDON"
	ScriptRootVolume          = "script-root-volume"
	WorkdirVolume             = "workdir-volume"
	CachedCompileVolume       = "cached-compile-volume"
	TensorStorageVolume       = "tensor-storage-volume"
	WorkerCacheDirRWVolume    = "worker-cache-dir-volume"
	SharedMemoryVolume        = "dev-shm-volume"
	SystemSecretVolume        = "system-secret-volume"
	LogExportStagingVolume    = "log-export-staging-volume"
	CachedCompilePVCName      = "cached-compile-static-pvc"
	LogExportPVCName          = "log-export-static-pvc"
	DebugArtifactPVCName      = "debug-artifact-static-pvc"
	NvmfVolumePrefix          = "nvmf-volume"

	// LogExport-specific constants
	LogExportAppLabelKey   = "app"
	LogExportAppLabelValue = "log-export"

	// ImageBuilder-specific constants
	ContainerdCertsConfigDir             = "/etc/containerd/certs.d"
	ContainerdCertsConfigVolume          = "containerd-certs-config-volume"
	ContainerdSocketPath                 = "/run/containerd/containerd.sock"
	ContainerdSocketVolume               = "containerd-socket-volume"
	RegistryCertPath                     = "/opt/cerebras/certs/registry_tls.crt"
	RegistrySecretVolume                 = "registry-secret-volume"
	ImageBuilderScriptRootDir            = "/kaniko-utils"
	ImageBuilderServiceAccount           = "image-builder-sa"
	ImageBuilderBaseImageAnnotKey        = "image-builder/base-image"
	ImageBuilderDestinationImageAnnotKey = "image-builder/destination-image"
	ImageBuilderImageSuffixAnnotKey      = "image-builder/image-suffix"
	ImageBuilderUserAnnotKey             = "image-builder/user"
	ImageBuilderImageBuildLog            = "image-build.log"
	ImageBuilderAppLabelKey              = "app"
	ImageBuilderAppLabelValue            = "image-builder"

	WorkdirLogsMountDirAnnot   = "cerebras/workdir-logs-mount-dir"
	CachedCompileMountDirAnnot = "cerebras/cached-compile-mount-dir"
	InfraSetupAnnot            = "cerebras/infra-setup"

	RegistryUrl = "registry.local"

	// ContactSysAdminSupport Raise this message when internal system administrators should be able to resolve issues
	// without having to reach out to the Cerebras Support Team.
	// TODO: Add an interceptor for wrapping the actionable messages when they are broadly needed.
	ContactSysAdminSupport = "Please contact your organization's system administrators for support."
	// ContactCerebrasSupport Raise this message when the internal errors need to be resolved by the Cerebras Support Team.
	ContactCerebrasSupport = "Please contact the Cerebras Support Team."

	DefaultWsjobTTLDays = 7

	CompileOnlyCmAddr  = "~ws_compile_only~:0"
	MockCsAddressAnnot = "cerebras/mock-cs-address"

	SchedulerHintPrefix   = "k8s.cerebras.com/scheduler-hint-"
	SchedulerHintAllowSys = SchedulerHintPrefix + "allow-systems"
	SchedulerHintAllowNG  = SchedulerHintPrefix + "allow-nodegroups"
	SchedulerHintWGProps  = SchedulerHintPrefix + "weight-group-props"

	EnableAllowUnhealthySystems = "cerebras/enable-allow-unhealthy-systems"

	// LMRReplicaMemoryBufferMiB The memory buffer room (in MiB) to be allocated per replica with LMR
	LMRReplicaMemoryBufferMiB = 1
)

var (
	MinMemxPerPopNodegroup   int
	WsjobCleanupTTLSeconds   int
	InitContainerScript      string
	PodInfraSetupScript      string
	CrdCompileMinMemoryBytes int
	CrdExecuteMinMemoryBytes int
	// EnablePartialSystemMode will enable scheduling with partial system ports/br config
	EnablePartialSystemMode = false

	KubectlImageName      = "alpine-kubectl"
	KubectlImageTag       = os.Getenv("ALPINE_KUBECTL_TAG")
	ContainerdImageName   = "alpine-containerd"
	ContainerdImageTag    = os.Getenv("ALPINE_CONTAINERD_TAG")
	KubeUserAuthImageName = "alpine-kube-user-auth"
	KubeUserAuthImageTag  = os.Getenv("ALPINE_KUBE_USER_AUTH_TAG")
	KubectlImage          = fmt.Sprintf("%s/%s:%s", RegistryUrl, KubectlImageName, KubectlImageTag)
	ContainerdImage       = fmt.Sprintf("%s/%s:%s", RegistryUrl, ContainerdImageName, ContainerdImageTag)
	KubeUserAuthImage     = fmt.Sprintf("%s/%s:%s", RegistryUrl, KubeUserAuthImageName, KubeUserAuthImageTag)
	PromUrl               = ""

	// V2 network has full connection which removed BR port connection restriction on specific system port
	IsV2Network bool

	// Inference-only cluster is a type of V2 network clusters that does not have any MemX node nor any node groups
	IsInferenceCluster bool

	// DisableMultusForBR default as true and go with hostnetwork
	// there's some routing issues needs to be fixed before enable
	// https://cerebras.atlassian.net/browse/SW-122833
	DisableMultusForBR = true

	// Redundant BR BW(100gpbs unit) per system, used for br tree ports utilization check
	RedundantBrPortBwPerSystem float64

	// BrTreePolicy: spine/port, default is hybrid
	BrTreePolicy string

	// Use AX scheduling only when AX node exists
	// As per April 2024, some of the V2 networking clusters do not have AX nodes.
	// When all V2 networking clusters have upgraded to use AX nodes, we can safely
	// retire this flag.
	// TODO: Retire this flag when all V2 clusters are guaranteed to have AX nodes
	UseAxScheduling bool

	// Use IX scheduling only when IX node exists, and SWDriver pods separated from ACT0
	// Only V2 networking clusters dedicated for inference jobs have IX nodes.
	// However as of May 2024, SWDriver pods are not separated from ACT0 so IX should not be scheduled.
	// When separation happens, env var ENABLE_SWDRIVER_SEPARATION should be set to enable IX scheduling.
	UseIxScheduling bool

	// Before systems are being installed in clusters during cluster bringup time, developers can add fake
	// systems to the cluster and run jobs in dropflow mode. System port affinity information will not be
	// available with fake systems. We will disable some optimizations like reserving AX nodes within stamp
	// when performing scheduling and resource management.
	SystemStampsDetected bool

	// By design of V2 networking, a leaf switch should only have downstream connectivities within a stamp.
	// For clusters that violates this design principle, we will disable some optimizations like reserving
	// AX nodes within stamp when performing scheduling and resource management.
	CrossStampSwitchesDetected bool

	// By default, we disable the ratio discovery for stability during incremental cluster build.
	AxCountPerSystemDiscoveryEnabled bool

	// As of rel-2.3, the ratio is 1. In future this ratio can be 4 for engineering clusters.
	AxCountPerSystem int

	// Enforce a number of stamps by evenly dividing systems, AX nodes and SX nodes.
	// This option should only be used during testing - treat a cluster as if it consists of
	// the specified stamp count.
	StampCountOverride int

	// Default reserved mem overhead only for node hetero mem assign, max(10G, 5%)
	// if mem is only satisfied for low/upper, this overhead won't be applied
	RtNodeMaxLimitReservedMemGB    = 10
	RtNodeMaxLimitReservedMemRatio = 0.05

	// As of rel-2.2, only the prioritized-compile locks have this grace period. The idea is to strike a balance
	// point between killing the train-and-eval workflows aggressively and allowing the execute job idle time to
	// lower system utilization.
	PendingLockGracePeriodAnnotKey   = "cerebras/pending-lock-grace-period-seconds"
	PendingLockGracePeriodValue      = 3600
	PendingLockGracePeriodAnnotValue = strconv.Itoa(PendingLockGracePeriodValue)

	ConstTrue = strconv.FormatBool(true)

	// In rel-2.4, separation between management nodes and coordinator nodes was introduced.
	// Only when the separation is explicitly enforced in the cluster properties AND the cluster
	// contains multiple management nodes, the operator mandates the separation. When that happens,
	// session management enforces a rule where management/Ceph nodes cannot be moved into any user
	// sessions. If operator sees a management/Ceph node is still in a user session, the operator
	// will panic.
	// ManagementSeparationEnforced will be set to false if operator detected it's a single-mgmt cluster
	// during initialization
	ManagementSeparationEnforced bool
	IsMultiMgmt                  bool
	IsMultiBox                   bool

	AutoSetAxOnNsrInit bool

	// compare with default milan server, genoa server has more powerful cpus, cut to half of cpu requests
	IsGenoaServer bool

	SkipIngressReconcile bool

	Verbose bool
)

func init() {
	wsjobTTL, err := strconv.Atoi(os.Getenv("WSJOB_CLEANUP_TTL_DAYS"))
	if err != nil {
		wsjobTTL = DefaultWsjobTTLDays
	}
	logrus.Infof("Default wsjob clean ttl days: %d", wsjobTTL)
	WsjobCleanupTTLSeconds = 60 * 60 * 24 * wsjobTTL

	if strings.ToLower(os.Getenv("ENABLE_BR_MULTUS")) == "true" {
		logrus.Info("BR Multus enabled by env ENABLE_BR_MULTUS")
		DisableMultusForBR = false
	}

	if policy := strings.ToUpper(os.Getenv(BrTreePolicyEnv)); policy == BrTreePolicyPort || policy == BrTreePolicySpine {
		logrus.Infof("BR tree policy override to %s", policy)
		BrTreePolicy = policy
	}

	if strings.ToLower(os.Getenv(EnablePartialSystemKey)) == "true" {
		logrus.Infof("PartialSystem Support disabled")
		EnablePartialSystemMode = true
	}

	if strings.ToLower(os.Getenv("ENABLE_MANAGEMENT_SEPARATION")) == "true" {
		logrus.Infof("Management separation enabled by env ENABLE_MANAGEMENT_SEPARATION")
		ManagementSeparationEnforced = true
	}

	MinMemxPerPopNodegroup, err = strconv.Atoi(os.Getenv("MIN_MEMX_PER_POP_NODEGROUP"))
	if err != nil || MinMemxPerPopNodegroup < 1 {
		MinMemxPerPopNodegroup = DefaultMinMemxPerPopNodegroup
	}
	logrus.Infof("Memx required to consider a nodegroup populated: %d", MinMemxPerPopNodegroup)

	if compileMinGi, _ := strconv.ParseFloat(os.Getenv("CRD_COMPILE_MIN_GI"), 64); compileMinGi > 0 {
		logrus.Infof("CRD_COMPILE_MIN_GI override: %f", compileMinGi)
		CrdCompileMinMemoryBytes = int(compileMinGi * float64(1<<30))
		if CrdCompileMinMemoryBytes < 0 {
			logrus.Warnf("CRD_COMPILE_MIN_GI is too big for conversion, ignore")
			CrdCompileMinMemoryBytes = 0
		}
	}
	if executeMinGi, _ := strconv.ParseFloat(os.Getenv("CRD_EXECUTE_MIN_GI"), 64); executeMinGi > 0 {
		logrus.Infof("CRD_EXECUTE_MIN_GI override: %f", executeMinGi)
		CrdExecuteMinMemoryBytes = int(executeMinGi * float64(1<<30))
		if CrdExecuteMinMemoryBytes < 0 {
			logrus.Warnf("CRD_EXECUTE_MIN_GI is too big for conversion, ignore")
			CrdExecuteMinMemoryBytes = 0
		}
	}

	if val, _ := strconv.Atoi(os.Getenv(RtNodeMaxLimitReservedMemGbKey)); val > 0 {
		logrus.Infof("%s override: %d", RtNodeMaxLimitReservedMemGbKey, val)
		RtNodeMaxLimitReservedMemGB = val
	}
	if val, _ := strconv.ParseFloat(os.Getenv(RtNodeMaxLimitReservedMemRatioKey), 64); val > 0 {
		logrus.Infof("%s override: %f", RtNodeMaxLimitReservedMemRatioKey, val)
		RtNodeMaxLimitReservedMemRatio = val
	}

	if strings.ToLower(os.Getenv("ENABLE_AX_CSX_RATIO_DISCOVERY")) == "true" {
		logrus.Infof("AX to CSX ratio discovery was enabled")
		AxCountPerSystemDiscoveryEnabled = true
	}

	StampCountOverride, err = strconv.Atoi(os.Getenv("STAMP_COUNT_OVERRIDE"))
	if StampCountOverride > 0 {
		logrus.Infof("Number of stamps enforced: %d", StampCountOverride)
	}

	if strings.ToLower(os.Getenv("ENABLE_SWDRIVER_SEPARATION")) == "true" {
		UseIxScheduling = true
	}

	if strings.ToLower(os.Getenv("GENOA_SERVER")) == "true" {
		logrus.Info("Genoa server detected")
		IsGenoaServer = true
	}

	if strings.ToLower(os.Getenv("VERBOSE")) == "true" {
		Verbose = true
	}

	InitContainerScript = os.Getenv("INIT_CONTAINER_SCRIPT")
	PodInfraSetupScript = os.Getenv("POD_INFRA_SETUP_SCRIPT")
}

package common

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	systemv1 "cerebras.com/job-operator/apis/system/v1"

	commonapisv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

const (
	LeaseName = "7c33ca02.cerebras.com"
	// DefaultNameSpace is default namespace of controller and wsjobs
	DefaultNamespace = "job-operator"

	// ClusterConfigFile is the file in cluster configmap
	ClusterConfigFile           = "clusterConfiguration.yaml"
	DefaultClusterConfigMapName = "cluster"

	// keep last 25 lines of logs of failed pod by default
	// This can be overridden by "POD_LOG_LINES" env
	DefaultPodLogLines = 25

	// This can be overridden by "PENDING_POD_TTL_SECONDS" env
	DefaultPendingPodGracePeriod = 5 * time.Minute

	// This can be overridden by the label key in job-spec or env
	DefaultFailingJobGracePeriod = 10 * time.Minute
	FailingJobGracePeriodLabel   = "FAILING_JOB_GRACE_PERIOD_SECONDS"

	DefaultLockCleanupGracePeriod = 5 * time.Minute

	// This can be overridden by "CLIENT_LEASE_DURATION_SECONDS" env
	DefaultClientLeaseDuration = 1 * time.Minute

	GroupLabelKey      = "k8s.cerebras.com/node-group"
	NamespaceLabelKey  = "k8s.cerebras.com/namespace"
	RoleLabelKeyPrefix = "k8s.cerebras.com/node-role"

	DisableClusterModeKey = "DISABLE_CLUSTER_MODE"

	NamespaceKey      = "namespace"
	StorageTypeKey    = "storage-type"
	CephStorageType   = "ceph"
	CephAccessibleKey = "ceph-csi"

	SecondNadLegacy = "job-operator/multus-secondary-data-net"
	SecondNad       = "job-operator/multus-data-net-1"

	EnableStartupProbeAllKey = "ENABLE_STARTUP_PROBE_ALL"
	BrV2SpineLoad            = "cerebras/br-spine-load"
	ActV2SpineLoad           = "cerebras/act-spine-load"
	PartialSystemAnnotation  = "cerebras/partial-system-assigned"
	NvmfGrants               = "cerebras/nvmf-grants"
	NvmfGrantNotes           = "cerebras/nvmf-grant-notes"
	AppClientVersion         = "cerebras/version-appliance-client"
	ClusterServerVersion     = "cerebras/version-cluster-server"
	JobOperatorVersion       = "cerebras/version-job-operator"
	WsjobImageVersion        = "cerebras/version-wsjob"
	UserSidecarVersion       = "cerebras/version-user-sidecar"

	// health condition prefix
	ClusterMgmtConditionPrefix   = "ClusterMgmt"
	CsadmConditionPrefix         = "Csadm"
	ClusterDeployConditionPrefix = "ClusterDeploy"
	NicNameAlertLabel            = "device"
	SystemErrorAlertType         = "SystemError"
	SystemPortAlertType          = "SystemPortError"
	NodeNICAlertType             = "NodeNICError"
	NodeErrorAlertType           = "NodeError"
	NodeSwitchPortAlertType      = "NodeSwitchPortError"
	NetworkAttachmentKey         = "k8s.v1.cni.cncf.io/networks"
	ClusterDeployNotAppliedType  = "ClusterDeployNotApplied"

	// job event reasons
	AlertIsFiring                    = "AlertIsFiring"
	AlertIsNotFiring                 = "AlertIsNotFiring"
	AlertsUnavailable                = "AlertsUnavailable"
	InconsistentVersionEventReason   = "InconsistentVersion"
	PartialSystemAssignedEventReason = "PartialSystemAssigned"
	DegradedSchedulingJobEventReason = "DegradedScheduling"
	DNSConfigFormingJobEventReason   = "DNSConfigForming"
	OOMKilledJobEventReason          = "OOMKilled"

	DefaultNamespaceScaleDownCheckInterval = time.Duration(0) // disabled by default

	// this is for compile fabric json check only
	DisableFabricJsonCheckKey = "DISABLE_FABRIC_JSON_CHECK"
	OkSystemsInNamespace      = "OK_SYSTEMS_IN_NAMESPACE"
	AssignedSystems           = "ASSIGNED_SYSTEMS"
	PromUrl                   = "PROM_URL"

	Age        string = "age"
	Priority   string = "priority"
	Ascending  string = "asc"
	Descending string = "desc"

	// LinearMemoryRangeGlobalAnnotPrefix is the annotation key prefix for a task‐type's global linear‐memory settings
	LinearMemoryRangeGlobalAnnotPrefix = "linear_memory_range_global_"
	// LinearMemoryRangeCoefficientsAnnotPrefix is the annotation key prefix for replica linear‐memory coefficient overrides
	LinearMemoryRangeCoefficientsAnnotPrefix = "linear_memory_range_coefficients_"

	// allow job sharing for session
	InferenceJobSharingSessionLabelKey = "labels.k8s.cerebras.com/inference-job-sharing-enabled"

	// Session label key for enabling swdriver mutual exclusion
	// TODO: remove once SwDriver is scheduled on IX/SX
	SwDriverMutualExclusionSessionLabelKey = "labels.k8s.cerebras.com/sw-driver-mutual-exclusion-enabled"

	K8sServiceNameLabelKey           = "kubernetes.io/service-name"
	EndpointSliceManagedByLabelKey   = "endpointslice.kubernetes.io/managed-by"
	EndpointSliceManagedByLabelValue = "jobs.cerebras.com-controller-manager"
	GRPCServerPortName               = "grpc"
)

var (
	// DisableScheduleNodeRole disables WSJob Pods/Node assignment based on respective node types. E.g. worker pods
	// scheduled on worker nodes, and so on. Disable in unlabeled clusters.
	DisableScheduleNodeRole bool

	// DisableMultus disables the setup for multus related configs.
	DisableMultus bool

	// Generic access to the first data NIC.
	DataNetAttachDef = fmt.Sprintf("%s/multus-data-net", DefaultNamespace)
	// Generic access to the second data NIC.
	// default to SecondNadLegacy for backwards compatible but will be updated at init time
	SecondNetAttachDef = SecondNadLegacy

	// Format to access the i>1 data NIC.
	DataNetAttachDefFormat = fmt.Sprintf("%s-%%d", DataNetAttachDef)

	// DisableSecondaryDataNic disables the second data NIC on nodes.
	// In rel-2.2, this feature is to introduced to gain additional bandwidth on the depop'd nodegroups.
	DisableSecondaryDataNic bool

	// DisableNodeGroupScheduling disables locking of nodeGroups and assigning nodeGroup selectors to pods of a WsJob
	// This is automatically disabled if there are no nodeGroups in the cluster configuration.
	// See https://cerebras.atlassian.net/wiki/spaces/runtime/pages/2290647574/NodeGroup+Scheduling
	DisableNodeGroupScheduling bool

	// EnableStrictAllowSystems fails any wsjob request which does not specify the scheduler hint `allow-systems`.
	// Usage: forces Cerebras users of a large cluster to reserve particular systems so that multiple teams (some using
	// k8s, some not) isolate their activities to their set of systems.
	EnableStrictAllowSystems bool

	// Dev mode
	IsDev bool

	// Cluster Config cm name
	ClusterConfigMapName = DefaultClusterConfigMapName

	// JobResourcesTracker is used to tracking active jobs resource granted for hardware error events generation
	JobResourcesTracker = map[types.NamespacedName]map[string]bool{}

	// Namespace the current namespace
	Namespace string

	// SystemNamespace the only system namespace for cluster wide control against potential multiple user namespaces
	SystemNamespace string

	// NamespaceScaleDownCheckInterval is interval of periodic check/scaledown of zero-resource namespace reservations
	// with cluster-servers deployed. 0 duration disables
	NamespaceScaleDownCheckInterval time.Duration

	// 	DisableSystemVersionCheck disable checks on system version for resources during job scheduling/session update.
	DisableSystemVersionCheck bool

	// 	EnablePlatformVersionCheck enables checks on platform version for resources during job scheduling/session update.
	EnablePlatformVersionCheck bool

	// 	DisableRedundantSession disables using redundant sessions in scheduling.
	DisableRedundantSession bool

	// EnableMultiCoordinator enables multiple coordinator mode with shared storage and
	// enable label extra worker in NG to coordinator type
	// This is internal usage only, not for production
	EnableMultiCoordinator bool

	// 	EnableSwitchAlertDetection enables switch alerts on inter-switch errors for v1 network only.
	//	The entire node group will be isolated if it's detected.
	EnableSwitchAlertDetection = true

	// EnableStartupProbeAll enables startup probe to ensure grpc service up for all replicas
	EnableStartupProbeAll = true

	// EnableClusterMode will watch/reconcile jobs/locks in all namespaces except non-cluster mode NS
	// systems/node-groups still get assigned for each NS in cluster mode while crd/br nodes are shared
	EnableClusterMode = true

	// ClusterModeDisabledNamespaceMap tracking non-cluster mode NS for isolation of mgmt node
	// this could be removed if we support isolation of mgmt node in cluster mode
	ClusterModeDisabledNamespaceMap = map[string]bool{}

	// DisableInferenceJobSharing will disable the inference jobs sharing feature at global level,
	// disregarding the session level flag for job sharing.
	DisableInferenceJobSharing = false

	// DisableWioBandwidthLB will disable port load balancing by wio bandwidth,
	// this env var is mostly used for testing purposes
	DisableWioBandwidthLB = false

	// Starting from rel-2.5, cluster server generates the cluster details configmap
	// immediately after the wsjob was created. If for whatever reason, the configmap
	// does not exist 10 seconds after the wsjob was created, it's almost guaranteed the
	// client would had errored out before knowing the new wsjob id. We cancel the job
	// in that case with a clear message. In a rare case, where job operator gets upgraded
	// when the job was in queue, job operator would backfill that responsibility.
	DefaultClusterDetailsGenerationTimeout = 10 * time.Second

	CerebrasVersion string // should be set by "go build"
	SemanticVersion string // should be set by "go build"

	OperatorSemanticVersion *semver.Version

	// default requeue waiting for config init
	ConfigInitRequeue = ctrl.Result{RequeueAfter: time.Millisecond * 100}

	PodLogLines = DefaultPodLogLines

	PendingPodGracePeriod = DefaultPendingPodGracePeriod

	LockCleanupGracePeriod = DefaultLockCleanupGracePeriod

	ClientLeaseDuration = DefaultClientLeaseDuration

	// This is the default grace period for a failing job to be considered failed after the first pod failure.
	FailingJobGracePeriod = DefaultFailingJobGracePeriod

	KubeApiQPS = 300

	// node specialization relies on an earlier v2 network check to determine if AX scheduling should be used
	ReplicaTypeNodeSpecialization map[commonapisv1.ReplicaType]NodeRole

	NginxNameSpace   = "ingress-nginx"
	NginxPodLabelKey = "app.kubernetes.io/instance"
	NginxPodLabelVal = "ingress-nginx"

	PodIp = ""

	GRPCServerEndpointSliceName = ""
	K8sServiceNameLabelValue    = ""
	// This global variable is used to determine the CS version spec.
	// The CS version defaults to CS2, and is reset each time the cluster config is updated.
	// This determines the number of ports per CS and domains per CS.
	// In tests, this will be set and unset for test isolation.
	CSVersionSpec systemv1.CSVersionSpec = systemv1.GetCSVersionSpec(systemv1.CSVersion2)
)

func init() {
	ParseEnvVar()
}

func ParseEnvVar() {
	if SemanticVersion != "" {
		OperatorSemanticVersion = semver.New(SemanticVersion)
	}
	SystemNamespace = os.Getenv("SYSTEM_NAMESPACE")
	if SystemNamespace == "" {
		SystemNamespace = DefaultNamespace
	}
	Namespace = os.Getenv("NAMESPACE")
	if Namespace == "" {
		Namespace = DefaultNamespace
	}
	GRPCServerEndpointSliceName = Namespace + "-controller-manager-grpc-service-endpointslice"
	K8sServiceNameLabelValue = Namespace + "-controller-manager-grpc-service"

	if strings.ToLower(os.Getenv("DISABLE_SYSTEM_VERSION_CHECK")) == "true" {
		DisableSystemVersionCheck = true
	}
	if strings.ToLower(os.Getenv("ENABLE_PLATFORM_VERSION_CHECK")) == "true" {
		EnablePlatformVersionCheck = true
	}
	if strings.ToLower(os.Getenv("DISABLE_REDUNDANT_SESSION")) == "true" {
		DisableRedundantSession = true
	}
	if strings.ToLower(os.Getenv("ENABLE_MULTI_COORDINATOR")) == "true" {
		EnableMultiCoordinator = true
	}
	if strings.ToLower(os.Getenv("ENABLE_SWITCH_ALERT")) == "false" {
		EnableSwitchAlertDetection = false
	}
	if strings.ToLower(os.Getenv("DISABLE_INFERENCE_JOB_SHARING")) == "true" {
		DisableInferenceJobSharing = true
	}
	logrus.Infof("DisableInferenceJobSharing is set to: %s", strconv.FormatBool(DisableInferenceJobSharing))
	if checkInterval, err := time.ParseDuration(os.Getenv("NAMESPACE_SCALE_DOWN_CHECK_INTERVAL")); err == nil {
		NamespaceScaleDownCheckInterval = checkInterval
	} else {
		NamespaceScaleDownCheckInterval = DefaultNamespaceScaleDownCheckInterval
	}
	logrus.Infof("NamespaceScaleDownCheckInterval: %s", NamespaceScaleDownCheckInterval.String())

	if strings.ToLower(os.Getenv(DisableClusterModeKey)) == "true" {
		EnableClusterMode = false
		ClusterModeDisabledNamespaceMap[Namespace] = true
	}
	if strings.ToLower(os.Getenv(EnableStartupProbeAllKey)) == "false" {
		logrus.Infof("Startup probe for all disabled")
		EnableStartupProbeAll = false
	}
	podTTL, err := strconv.Atoi(os.Getenv("PENDING_POD_TTL_SECONDS"))
	if err == nil && podTTL > 0 {
		PendingPodGracePeriod = time.Duration(podTTL) * time.Second
	}
	if podFailureGracePeriod, err := strconv.Atoi(os.Getenv(FailingJobGracePeriodLabel)); err == nil && podFailureGracePeriod > 0 {
		FailingJobGracePeriod = time.Duration(podFailureGracePeriod) * time.Second
	}
	leaseDuration, err := strconv.Atoi(os.Getenv("CLIENT_LEASE_DURATION_SECONDS"))
	if err == nil && leaseDuration > 0 {
		ClientLeaseDuration = time.Duration(leaseDuration) * time.Second
	}
	lines, _ := strconv.Atoi(os.Getenv("POD_LOG_LINES"))
	if lines > 0 {
		PodLogLines = lines
	}
	qps, _ := strconv.Atoi(os.Getenv("KUBE_API_QPS"))
	if qps > 0 {
		KubeApiQPS = qps
	}
	if strings.ToLower(os.Getenv("DISABLE_MULTUS")) == "true" {
		DisableMultus = true
	}
	if strings.ToLower(os.Getenv("DISABLE_SECONDARY_DATA_NIC")) == "true" {
		DisableSecondaryDataNic = true
	}
	PodIp = os.Getenv("POD_IP")

	logrus.Infof("System-Namespace: %s, Current-Namespace: %s, Cluster-Mode: %v",
		SystemNamespace, Namespace, EnableClusterMode)

	// Duration stdout looks like: 5m0s
	logrus.Infof("Pending pod grace period: %s", PendingPodGracePeriod)

	// Duration stdout looks like: 5m0s
	logrus.Infof("Lock cleanup grace period: %s", LockCleanupGracePeriod)

	// Duration stdout looks like: 2m0s
	logrus.Infof("Client lease duration: %s", ClientLeaseDuration)

	logrus.Infof("Pod failure grace period: %s", FailingJobGracePeriod)

	logrus.Infof("Kube api qps: %d", KubeApiQPS)
}

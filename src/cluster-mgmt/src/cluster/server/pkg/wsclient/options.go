package wsclient

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"cerebras.com/cluster/server/pkg"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	log "github.com/sirupsen/logrus"
)

const (
	PrometheusOperatedSvc = "prometheus-prometheus"
	ThanosQuerySvc        = "thanos-query"
	PrometheusNs          = "prometheus"
)

type ServerOptions struct {
	DisableImageBuild                   bool
	ImageBuildBaseImage                 string
	SupportedSidecarImages              []string
	HasMultiMgmtNodes                   bool
	EnableRoCE                          bool
	DisableUserAuth                     bool
	EnableNonCRDServiceCreation         bool
	DisableNonCRDResourceRequests       bool
	EnableIsolatedDashboards            bool
	AllowNonDefaultJobPriority          bool
	BuildBalancedBrTree                 bool
	DisableFabricJsonCheck              bool
	EnablePartialSystem                 bool
	CompileSystemType                   string
	MaxTrainFabricFreqMhz               string
	SessionUpdateTimeoutSecs            int
	DebugVizUrlPrefix                   string
	JobPathConfig                       JobPathConfig
	SkipSystemMaintenanceResourceLimits bool
	WsjobDefaultImage                   string
	WsjobDefaultCommand                 string
	ClusterServerVersion                string
	PrometheusUrl                       string
}

type JobPathConfig struct {
	WorkdirLogsNfsPath   string
	CachedCompileNfsPath string

	// LocalWorkdirLogsRootPath default workdir hostpath of wsjobs
	// Example: /n1/wsjob/workdir
	LocalWorkdirLogsRootPath string

	// CachedCompileRootPath is the prefix of compile dir path
	// For multi-mgmt node environment, this path resides in Ceph storage
	// Otherwise, this path is a hostpath
	// Example: /n1/wsjob/compile_root
	CachedCompileRootPath string

	// LogExportRootPath is the prefix of user-driven log exports path
	// For multi-mgmt node environment, this path resides in Ceph storage
	// Otherwise, this path is a hostpath
	// Example: /n1/log-export
	LogExportRootPath string

	// DebugArtifactRootPath is the prefix of debug-driven log exports path
	// For multi-mgmt node environment, this path resides in Ceph storage
	// Otherwise, this path is a hostpath
	// Example: /n1/debug-artifact
	DebugArtifactRootPath string

	// TensorStorageRootPath is the prefix of path for
	// tensor scratch space on the coordinator
	// Example: /n1/tensor-storage
	TensorStorageRootPath string

	// WorkerCacheRootPath is the prefix of path for
	// workers to read from local SSD for training data
	// Example: /n0/cache
	WorkerCacheRootPath string

	// WorkerSharedMemoryRootPath is the prefix of path for
	// data loaders to access larger shared memory. The default amount
	// of shared memory is 64MB.
	WorkerSharedMemoryRootPath string
}

func boolEnvvar(env string) bool {
	val, err := strconv.ParseBool(strings.ToLower(os.Getenv(env)))
	if err != nil {
		return false
	}
	return val
}

func ServerOptionsFromEnv() ServerOptions {
	// SW-165456: While working on the needed optimizations,
	// the timeout is temporarily increased from 10 seconds to 5 minutes to unblock automation.
	sessionUpdateTimeoutSecs := 300
	sessionUpdateTimeoutEnv := os.Getenv("SESSION_UPDATE_TIMEOUT_SECONDS")
	if sessionUpdateTimeoutEnv != "" {
		sessionUpdateTimeoutSecs, _ = strconv.Atoi(sessionUpdateTimeoutEnv)
		if sessionUpdateTimeoutSecs <= 0 {
			log.Fatalf("Session update timeout (in seconds) should be a non-negative integer")
		}
	}
	jp := JobPathConfig{
		WorkdirLogsNfsPath:         os.Getenv("WORKDIR_LOGS_NFS_MOUNTED_PATH"),
		CachedCompileNfsPath:       os.Getenv("CACHED_COMPILE_NFS_MOUNTED_PATH"),
		LocalWorkdirLogsRootPath:   os.Getenv("WORKDIR_LOGS_ROOT_PATH"),
		CachedCompileRootPath:      os.Getenv("CACHED_COMPILE_ROOT_PATH"),
		LogExportRootPath:          os.Getenv("LOG_EXPORT_ROOT_PATH"),
		DebugArtifactRootPath:      os.Getenv("DEBUG_ARTIFACT_ROOT_PATH"),
		TensorStorageRootPath:      os.Getenv("TENSOR_STORAGE_ROOT_PATH"),
		WorkerCacheRootPath:        os.Getenv("WORKER_CACHE_ROOT_PATH"),
		WorkerSharedMemoryRootPath: os.Getenv("WORKER_SHARED_MEMORY_ROOT_PATH"),
	}

	supportedSidecarImagesEnv := os.Getenv("IMAGE_BUILDER_SUPPORTED_SIDECAR_IMAGES")
	var supportedSidecarImages []string
	if supportedSidecarImagesEnv != "" {
		supportedSidecarImages = strings.Split(supportedSidecarImagesEnv, ",")
	}

	hasMultiMgmtNodes := boolEnvvar("MULTI_MGMT_NODES")

	promUrl := fmt.Sprintf("http://%s.%s.svc:9090",
		pkg.Ternary(
			hasMultiMgmtNodes,
			ThanosQuerySvc,
			PrometheusOperatedSvc,
		),
		PrometheusNs,
	)

	opts := ServerOptions{
		DisableImageBuild:                   boolEnvvar("DISABLE_IMAGE_BUILDER"),
		ImageBuildBaseImage:                 os.Getenv("IMAGE_BUILDER_BASE_IMAGE"),
		SupportedSidecarImages:              supportedSidecarImages,
		HasMultiMgmtNodes:                   hasMultiMgmtNodes,
		EnableRoCE:                          boolEnvvar("ENABLE_ROCE"),
		DisableUserAuth:                     boolEnvvar("DISABLE_USER_AUTH"),
		EnableNonCRDServiceCreation:         !boolEnvvar("DISABLE_NON_CRD_SERVICES"),
		DisableNonCRDResourceRequests:       boolEnvvar("DISABLE_NON_CRD_RESOURCE_REQUESTS"),
		EnableIsolatedDashboards:            boolEnvvar("ENABLE_ISOLATED_DASHBOARDS"),
		AllowNonDefaultJobPriority:          boolEnvvar("ALLOW_NON_DEFAULT_JOB_PRIORITY"),
		BuildBalancedBrTree:                 boolEnvvar(wsapisv1.BuildBalancedBrTreeKey),
		DisableFabricJsonCheck:              boolEnvvar(wscommon.DisableFabricJsonCheckKey),
		EnablePartialSystem:                 boolEnvvar(wsapisv1.EnablePartialSystemKey),
		CompileSystemType:                   os.Getenv("COMPILE_SYSTEM_TYPE"),
		MaxTrainFabricFreqMhz:               os.Getenv("MAX_TRAIN_FABRIC_FREQ_MHZ"),
		DebugVizUrlPrefix:                   os.Getenv("DEBUG_VIZ_INGRESS_URL"),
		SessionUpdateTimeoutSecs:            sessionUpdateTimeoutSecs,
		JobPathConfig:                       jp,
		SkipSystemMaintenanceResourceLimits: boolEnvvar("SKIP_SYSTEM_MAINTENANCE_RESOURCE_LIMITS"),
		WsjobDefaultImage:                   os.Getenv("WSJOB_DEFAULT_IMAGE"),
		WsjobDefaultCommand:                 os.Getenv("WSJOB_DEFAULT_COMMAND"),
		ClusterServerVersion:                pkg.CerebrasVersion,
		PrometheusUrl:                       promUrl,
	}
	return opts
}

// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	wscommon "cerebras.com/job-operator/common"

	commonpb "cerebras/pb/workflow/appliance/common"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"

	"cerebras.com/cluster/server/pkg/wsclient"

	"cerebras.com/cluster/server/csctl"
	"cerebras.com/cluster/server/pkg"
)

func newServiceSpec(task *commonpb.ClusterDetails_TaskInfo) wsclient.ServiceSpec {
	spec := wsclient.ServiceSpec{}
	if task != nil {
		if task.ResourceInfo != nil {
			spec.Resources = wsclient.ResourceRequest{
				CpuMilliCores:           task.ResourceInfo.CpuMillicore,
				MemoryBytes:             task.ResourceInfo.MemoryBytes,
				MemoryBytesUpperBound:   task.ResourceInfo.MemoryBytesUpperBound,
				MemoryBytesMaxLimit:     task.ResourceInfo.MemoryBytesMaxLimit,
				PortCount:               task.ResourceInfo.PortCount,
				NvmfDiskBytes:           task.ResourceInfo.NvmfDiskBytes,
				NvmfDiskBytesUpperBound: task.ResourceInfo.NvmfDiskBytesUpperBound,
			}
		}

		spec.Replicas = int32(len(task.TaskMap))
		if spec.Replicas < 1 {
			// Default to 1 replica if no tasks are specified.
			spec.Replicas = 1
		}
	}

	return spec
}

func getCleanPodPolicy(debugArgs *commonpb.DebugArgs) commonv1.CleanPodPolicy {
	// Backdoor to set up cleanPodPolicy by setting an environment variable.
	cleanPodPolicyStr := os.Getenv("CleanPodPolicy")
	cleanPodPolicy := commonv1.CleanPodPolicyRunning
	if cleanPodPolicyStr == "CleanPodPolicyRunning" {
		cleanPodPolicy = commonv1.CleanPodPolicyRunning
	} else if cleanPodPolicyStr == "CleanPodPolicyNone" {
		cleanPodPolicy = commonv1.CleanPodPolicyNone
	} else if cleanPodPolicyStr == "CleanPodPolicyAll" {
		cleanPodPolicy = commonv1.CleanPodPolicyAll
	}
	if debugArgs != nil && debugArgs.DebugMgr != nil {
		if debugArgs.DebugMgr.CleanPodPolicy == commonpb.DebugArgs_DebugMGR_NONE {
			cleanPodPolicy = commonv1.CleanPodPolicyNone
		} else if debugArgs.DebugMgr.CleanPodPolicy == commonpb.DebugArgs_DebugMGR_RUNNING {
			cleanPodPolicy = commonv1.CleanPodPolicyRunning
		} else if debugArgs.DebugMgr.CleanPodPolicy == commonpb.DebugArgs_DebugMGR_ALL {
			cleanPodPolicy = commonv1.CleanPodPolicyAll
		}
	}
	return cleanPodPolicy
}

func convertNotificationType(notificationType pb.NotificationType) (wsapisv1.NotificationType, error) {
	switch notificationType {
	case pb.NotificationType_NOTIFICATION_TYPE_EMAIL:
		return wsapisv1.NotificationTypeEmail, nil
	case pb.NotificationType_NOTIFICATION_TYPE_SLACK:
		return wsapisv1.NotificationTypeSlack, nil
	case pb.NotificationType_NOTIFICATION_TYPE_PAGERDUTY:
		return wsapisv1.NotificationTypePagerduty, nil
	}
	return wsapisv1.NotificationTypeEmail, grpcStatus.Errorf(codes.InvalidArgument, "invalid notification type: %s", notificationType)
}

func (server ClusterServer) newJobSpec(
	ctx context.Context,
	namespace string,
	numWafers int,
	relativeCompileDir string,
	actToCsxDomains [][]int,
	debugArgs *commonpb.DebugArgs,
	sidecarEnv *pb.UserSidecarEnv,
	clientMeta pkg.ClientMeta,
	jobMode wsclient.WSJobMode,
	notifications []*pb.UserNotification,
) (*wsclient.JobSpec, error) {
	jobName := wsclient.GenWsjobId()
	schedulerHints := map[string]string{}
	imagePullSecretHosts := map[string]bool{}
	prefixedLabels := map[string]string{}
	annotations := map[string]string{}
	dropCachesPath := ""
	dropCachesValue := commonpb.DebugArgs_DebugMGR_DROPCACHES_UNSPECIFIED
	numPopNgs := 1
	popNgTypes := ""
	compileFabricType := ""
	partialSystemMode := commonpb.DebugArgs_DebugMGR_PARTIAL_SYSTEM_MODE_UNSPECIFIED
	fallbackEnv := &wsclient.UserSidecarEnv{}
	workerSidecar := &wsclient.UserSidecar{}
	weightSidecar := &wsclient.UserSidecar{}
	activationSidecar := &wsclient.UserSidecar{}
	enableCrdAddressHandoff := server.options.HasMultiMgmtNodes
	var rtNodeMaxLimitReservedMemGb int
	var rtNodeMaxLimitReservedMemRatio float64
	var jobPriority *int
	var grpcSettings *commonpb.DebugArgs_DebugMGR_GrpcSettings
	var err error
	var allowUnhealthySystems bool
	var buildBalancedBr bool
	var disableFabricJsonCheck bool
	var workflowID string
	var failingJobGracePeriod int

	if debugArgs != nil {
		if debugArgs.DebugMgr != nil {
			partialSystemMode = debugArgs.DebugMgr.PartialSystemMode
			rtNodeMaxLimitReservedMemGb = int(debugArgs.DebugMgr.RtNodeMaxLimitReservedMemGb)
			rtNodeMaxLimitReservedMemRatio = debugArgs.DebugMgr.RtNodeMaxLimitReservedMemRatio
			failingJobGracePeriod = int(debugArgs.DebugMgr.FailingJobTtlSeconds)
			// deprecated, remove after all flows adopting server side workflow
			workflowID = debugArgs.DebugMgr.WorkflowId

			for k, v := range debugArgs.DebugMgr.Labels {
				// This kludge is to support scheduler hints for clients that have not upgraded to the latest protos.
				// This SHOULD be removed after 1.7.0 release.
				if strings.HasPrefix(k, "!scheduler-hint-") {
					if debugArgs.DebugMgr.SchedulerHints == nil {
						debugArgs.DebugMgr.SchedulerHints = map[string]string{}
					}
					debugArgs.DebugMgr.SchedulerHints[k[len("!scheduler-hint-"):]] = v
					continue
				}

				if err = csctl.ValidateLabelKey(k); err != nil {
					return nil, grpcStatus.Errorf(codes.InvalidArgument, err.Error())
				} else if err = csctl.ValidateLabelValue(&v); err != nil {
					return nil, grpcStatus.Errorf(codes.InvalidArgument, err.Error())
				}
				prefixedLabels[csctl.K8sLabelPrefix+k] = v
			}
			if server.options.AllowNonDefaultJobPriority {
				priority := debugArgs.DebugMgr.JobPriority
				if priority == commonpb.DebugArgs_DebugMGR_JOB_PRIORITY_UNSPECIFIED {
					priority = commonpb.DebugArgs_DebugMGR_JOB_PRIORITY_P2
				} else if priority == commonpb.DebugArgs_DebugMGR_JOB_PRIORITY_P0 {
					return nil, grpcStatus.Errorf(codes.InvalidArgument,
						"Job priority should be one of [p1, p2, p3] when launched.")
				}

				if priority == commonpb.DebugArgs_DebugMGR_JOB_PRIORITY_P1 {
					jobPriority = wscommon.Pointer(wsapisv1.MaxP1JobPriorityValue)
				} else if priority == commonpb.DebugArgs_DebugMGR_JOB_PRIORITY_P2 {
					jobPriority = wscommon.Pointer(wsapisv1.MaxP2JobPriorityValue)
				} else {
					jobPriority = wscommon.Pointer(wsapisv1.MaxP3JobPriorityValue)
				}
			} else {
				jobPriority = wscommon.Pointer(wsapisv1.DefaultJobPriorityValue)
			}
			grpcSettings = debugArgs.DebugMgr.GrpcSettings
			schedulerHints = debugArgs.DebugMgr.SchedulerHints
			dropCachesValue = debugArgs.DebugMgr.DropCachesValue
			if debugArgs.DebugMgr.EnableCrdAddressHandoff {
				enableCrdAddressHandoff = true
			}

			fallbackEnv.UpdateFromDebugArgs(sidecarEnv)

			if debugArgs.DebugMgr.WorkerSidecar == nil ||
				debugArgs.DebugMgr.WorkerSidecar.Strategy != commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_DISABLED {
				workerSidecar.Enabled = true
				if err = workerSidecar.UpdateFromDebugArgs(debugArgs.DebugMgr.WorkerSidecar); err != nil {
					return nil, err
				}
			}

			// We default to disabling the weight sidecar for now.
			// When the integration is soaked in, we can update to enabling the weight sidecar by default.
			if debugArgs.DebugMgr.WeightSidecar != nil &&
				debugArgs.DebugMgr.WeightSidecar.Strategy == commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED {
				weightSidecar.Enabled = true
				if err = weightSidecar.UpdateFromDebugArgs(debugArgs.DebugMgr.WeightSidecar); err != nil {
					return nil, err
				}
			}

			// We default to disabling the activation sidecar for now.
			// When the integration is soaked in, we can update to enabling the activation sidecar by default.
			if debugArgs.DebugMgr.ActivationSidecar != nil &&
				debugArgs.DebugMgr.ActivationSidecar.Strategy == commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED {
				activationSidecar.Enabled = true
				if err = activationSidecar.UpdateFromDebugArgs(debugArgs.DebugMgr.ActivationSidecar); err != nil {
					return nil, err
				}
			}

			// Adding the user sidecar image annotations to make registry cleanup easier.
			// As of April 2025, all sidecar containers use the same image. If this changes in the future,
			// we should adjust the logic accordingly.
			setSidecarImageTagAnnot := func(sidecar *commonpb.DebugArgs_DebugMGR_UserSidecar) {
				if sidecar == nil {
					return
				}
				if tokens := strings.Split(sidecar.Image, ":"); len(tokens) > 1 {
					annotations[wscommon.UserSidecarVersion] = tokens[len(tokens)-1]
				}
			}
			setSidecarImageTagAnnot(debugArgs.DebugMgr.WorkerSidecar)
			setSidecarImageTagAnnot(debugArgs.DebugMgr.WeightSidecar)
			setSidecarImageTagAnnot(debugArgs.DebugMgr.ActivationSidecar)

			if debugArgs.DebugMgr.NumPopulatedNodegroups != 0 {
				numPopNgs = int(debugArgs.DebugMgr.NumPopulatedNodegroups)
			}

			if debugArgs.DebugMgr.PopulatedGroupTypes != nil {
				var _ngTypes []string
				for _, ngType := range debugArgs.DebugMgr.PopulatedGroupTypes {
					if ngType == commonpb.DebugArgs_DebugMGR_POPULATED_GROUP_TYPE_REGULAR {
						_ngTypes = append(_ngTypes, wsapisv1.PopNgTypeRegular)
					} else if ngType == commonpb.DebugArgs_DebugMGR_POPULATED_GROUP_TYPE_LARGE {
						_ngTypes = append(_ngTypes, wsapisv1.PopNgTypeLarge)
					} else if ngType == commonpb.DebugArgs_DebugMGR_POPULATED_GROUP_TYPE_XLARGE {
						_ngTypes = append(_ngTypes, wsapisv1.PopNgTypeXLarge)
					}
				}
				// As of rel-2.4, xlarge nodegroups need to be explicitly specified in order to be used
				// for normal jobs that could be fit on non-xlarge nodegroups due to performance degradation.
				// Cluster clients do not use xlarge and non-xlarge nodegroups in the same job.
				// If we were to relax this restriction in the future, the server-side interface should
				// still be forward compatible.
				if len(_ngTypes) > 0 {
					popNgTypes = strings.Join(_ngTypes, ",")
				}
			}
			if debugArgs.DebugMgr.UseAxOnlyForRuntime {
				annotations[wsapisv1.UseAxOnlyForRuntimeKey] = wsapisv1.UseAxOnlyForRuntimeValue
			}
			if int(debugArgs.DebugMgr.NumAxNodes) > 0 {
				annotations[wsapisv1.WsJobActivationNodeCount] = strconv.Itoa(int(debugArgs.DebugMgr.NumAxNodes))
			}
			if debugArgs.DebugMgr.AllowUnhealthySystems {
				allowUnhealthySystems = true
			}
			if debugArgs.DebugMgr.BuildBalancedBrTree {
				buildBalancedBr = true
			}
			if debugArgs.DebugMgr.DisableFabricJsonCheck {
				disableFabricJsonCheck = true
			}
			if debugArgs.DebugMgr.BrTreePolicy == commonpb.DebugArgs_DebugMGR_BR_TREE_POLICY_PORT {
				annotations[wsapisv1.BrTreePolicyKey] = wsapisv1.BrTreePolicyPort
			} else if debugArgs.DebugMgr.BrTreePolicy == commonpb.DebugArgs_DebugMGR_BR_TREE_POLICY_SPINE {
				annotations[wsapisv1.BrTreePolicyKey] = wsapisv1.BrTreePolicySpine
			}
		}
		if debugArgs.Ini != nil {
			shouldUseMockCs := debugArgs.Ini.Bools["ws_rt_using_mock_cs2"]
			mockCsAddr := debugArgs.Ini.Strings["ws_rt_use_existing_mock_cs2_ip_port"]
			if shouldUseMockCs && mockCsAddr != "" && jobMode.IsCompile() {
				// If it's a compile job, make sure we fall back to the offline_compile/ fabric json
				mockCsAddr = wsapisv1.CompileOnlyCmAddr
			}
			if mockCsAddr != "" {
				annotations[wsapisv1.MockCsAddressAnnot] = mockCsAddr
			}

			if jobMode.IsCompile() && debugArgs.Ini.Bools["fetch_fabric_for_compile"] {
				numWafers = 1
			}
			compileFabricType = debugArgs.Ini.Strings["compile_fabric_type"]
		}
	}
	if clientMeta.WorkflowID != "" {
		workflowID = clientMeta.WorkflowID
	}
	if workflowID != "" {
		prefixedLabels[wsapisv1.WorkflowIdLabelKey] = workflowID
	}
	if clientMeta.ClientBuildVersion != "" {
		annotations[wscommon.AppClientVersion] = clientMeta.ClientBuildVersion
	}
	if clientMeta.ClientSemanticVersion != "" {
		annotations[wsapisv1.SemanticApplianceClientVersion] = clientMeta.ClientSemanticVersion
	}
	imagePullSecretHosts = wsclient.ParseImagePullSecret(server.kubeClientSet)

	msgPrefix := fmt.Sprintf("%s job spec:", jobName)
	operatorSemver, _ := server.getOperatorSemver()
	if operatorSemver != nil {
		log.Infof("%s operator version: %s", msgPrefix, operatorSemver.String())
		annotations[wsapisv1.SemanticJobOperatorVersion] = operatorSemver.String()
	}
	if !server.enableClientLeaseStrategy {
		prefixedLabels[wsapisv1.ClientLeaseDisabledLabelKey] = ""
		log.Infof("%s lease disabled", msgPrefix)
	}
	// enable by client debug args or server env
	if server.options.EnablePartialSystem ||
		partialSystemMode == commonpb.DebugArgs_DebugMGR_PARTIAL_SYSTEM_MODE_ENABLED {
		annotations[wsapisv1.EnablePartialSystemKey] = strconv.FormatBool(true)
		log.Infof("%s partial system mode allowed on", msgPrefix)
	}
	// enable by client debug args or server env
	if server.options.BuildBalancedBrTree || buildBalancedBr {
		annotations[wsapisv1.BuildBalancedBrTreeKey] = strconv.FormatBool(true)
		log.Infof("%s build balanced BR tree", msgPrefix)
	}
	if (server.options.DisableFabricJsonCheck || disableFabricJsonCheck) &&
		jobMode.IsCompile() {
		annotations[wscommon.DisableFabricJsonCheckKey] = strconv.FormatBool(true)
		log.Infof("%s disable fabric json check", msgPrefix)
	}
	// Set default dropCachesValue to THREE if not specified.
	if dropCachesValue == commonpb.DebugArgs_DebugMGR_DROPCACHES_UNSPECIFIED {
		dropCachesValue = commonpb.DebugArgs_DebugMGR_DROPCACHES_THREE
		log.Infof("%s drop cache val set to 3", msgPrefix)
	}

	// Set hetero mem overhead
	if rtNodeMaxLimitReservedMemGb > 0 {
		annotations[wsapisv1.RtNodeMaxLimitReservedMemGbKey] = strconv.Itoa(rtNodeMaxLimitReservedMemGb)
		log.Infof("%s rtNodeMaxLimitReservedMemGb set to %d", msgPrefix, rtNodeMaxLimitReservedMemGb)
	}
	if rtNodeMaxLimitReservedMemRatio > 0 && rtNodeMaxLimitReservedMemRatio < 1 {
		annotations[wsapisv1.RtNodeMaxLimitReservedMemRatioKey] = fmt.Sprintf("%f", rtNodeMaxLimitReservedMemRatio)
		log.Infof("%s rtNodeMaxLimitReservedMemRatio set to %f", msgPrefix, rtNodeMaxLimitReservedMemRatio)
	}
	// set number of populated nodegroups if required
	if numPopNgs > 1 {
		annotations[wsapisv1.WsJobPopulatedNodegroupCount] = strconv.Itoa(numPopNgs)
		log.Infof("%s number of pop nodegroups set to %d", msgPrefix, numPopNgs)
	}
	// set the populated nodegroup types if required
	if popNgTypes != "" {
		annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = popNgTypes
		log.Infof("%s pop nodegroup types set to %s", msgPrefix, popNgTypes)
	}
	if failingJobGracePeriod > 0 {
		annotations[wscommon.FailingJobGracePeriodLabel] = strconv.Itoa(failingJobGracePeriod)
	}

	var userNotifications []*wsapisv1.UserNotification
	for _, notification := range notifications {
		notificationType, err := convertNotificationType(notification.NotificationType)
		if err != nil {
			log.Errorf("invalid notification type: %s", err)
			continue
		}
		userNotification := &wsapisv1.UserNotification{
			NotificationType: notificationType,
			Target:           notification.Target,
		}
		if notification.SeverityThreshold != nil {
			val := *notification.SeverityThreshold
			userNotification.SeverityThreshold = &val
		}
		userNotifications = append(userNotifications, userNotification)
	}

	jobSpec := &wsclient.JobSpec{
		Namespace:       namespace,
		JobName:         jobName,
		NumWafers:       numWafers,
		ActToCsxDomains: actToCsxDomains,
		CleanPodPolicy:  getCleanPodPolicy(debugArgs),
		Labels:          prefixedLabels,
		Annotations:     annotations,
		JobUser: wsapisv1.JobUser{
			Uid:      clientMeta.UID,
			Gid:      clientMeta.GID,
			Username: clientMeta.Username,
			Groups:   clientMeta.Groups,
		},
		JobPathConfig:               server.options.JobPathConfig,
		ImagePullSecretHosts:        imagePullSecretHosts,
		SchedulerHints:              schedulerHints,
		GrpcSettings:                grpcSettings,
		DropCachesPath:              dropCachesPath,
		DropCachesValue:             dropCachesValue,
		CompileFabricType:           compileFabricType,
		EnableRoCE:                  server.options.EnableRoCE,
		WsJobMode:                   jobMode,
		UserSidecarEnv:              fallbackEnv,
		WorkerSidecar:               workerSidecar,
		WeightSidecar:               weightSidecar,
		ActivationSidecar:           activationSidecar,
		EnableCrdAddrHandoff:        enableCrdAddressHandoff,
		Priority:                    jobPriority,
		EnableNonCRDServiceCreation: server.options.EnableNonCRDServiceCreation,
		AllowUnhealthySystems:       allowUnhealthySystems,
		CachedCompileRelativePath:   strings.TrimPrefix(path.Clean(relativeCompileDir), "/"),
		OperatorSemver:              operatorSemver,
		Notifications:               userNotifications,
	}

	jobSpec.SetScriptRootVolumeAndMount()
	if err = jobSpec.SetWorkdirVolumeAndMount(ctx, server.kubeClientSet, debugArgs); err != nil {
		return nil, err
	}
	if err = jobSpec.SetCachedCompileVolumeAndMount(ctx, server.kubeClientSet, debugArgs, server.options.HasMultiMgmtNodes); err != nil {
		return nil, err
	}
	if err = jobSpec.SetCbcoreEntrypointVolumeAndMount(ctx, server.kubeClientSet, debugArgs); err != nil {
		return nil, err
	}
	if err = jobSpec.SetUserVenvVolumeAndMount(ctx, server.kubeClientSet, sidecarEnv); err != nil {
		return nil, err
	}
	jobSpec.SetSystemSecretVolumeAndMount()
	jobSpec.SetTensorStorageVolumeAndMount()
	jobSpec.SetWorkerCacheVolumeAndMount()
	jobSpec.SetSharedMemoryVolumeAndMount()

	return jobSpec, nil
}

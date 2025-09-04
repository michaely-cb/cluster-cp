// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	log "github.com/sirupsen/logrus"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"

	nsv1 "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	commonpb "cerebras/pb/workflow/appliance/common"

	rlCliv1 "cerebras.com/job-operator/client-resourcelock/clientset/versioned/typed/resourcelock/v1"
	sysv1 "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1"

	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	wscommon "cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

var pbToWsTaskType = map[commonpb.ClusterDetails_TaskInfo_TaskType]wsclient.WSService{
	commonpb.ClusterDetails_TaskInfo_ACT:  wsclient.WsActivation,
	commonpb.ClusterDetails_TaskInfo_BR:   wsclient.WsBroadcastReduce,
	commonpb.ClusterDetails_TaskInfo_CHF:  wsclient.WsChief,
	commonpb.ClusterDetails_TaskInfo_CMD:  wsclient.WsCommand,
	commonpb.ClusterDetails_TaskInfo_CRD:  wsclient.WsCoordinator,
	commonpb.ClusterDetails_TaskInfo_WGT:  wsclient.WsWeight,
	commonpb.ClusterDetails_TaskInfo_WRK:  wsclient.WsWorker,
	commonpb.ClusterDetails_TaskInfo_KVSS: wsclient.WSKVStorageServer,
	commonpb.ClusterDetails_TaskInfo_SWD:  wsclient.WsSWDriver,
}

type ClusterServer struct {
	kubeClientSet    kubernetes.Interface
	wsJobClient      wsclient.WSJobClientProvider
	metricsQuerier   wscommon.MetricsQuerier
	metricsPublisher MetricsPublisher
	sysCli           sysv1.SystemInterface
	lockCli          rlCliv1.ResourceLockInterface
	wsJobInformer    wsinformer.GenericInformer
	lockInformer     rlinformer.GenericInformer
	nodeLister       pkg.KubeNodeInformerLister
	nsrCli           nsv1.NamespaceReservationInterface
	kafkaClient      *KafkaClient
	options          wsclient.ServerOptions
	pb.UnimplementedClusterManagementServer
	hv1.UnimplementedHealthServer
	// ingressPollInterval poll interval for ingress status
	ingressPollInterval time.Duration
	// enableClientLeaseStrategy should always be true unless explicitly tested which is for internal only
	enableClientLeaseStrategy bool
	// s3ClientFactory is used to create S3 clients for connectivity checks
	s3ClientFactory s3ClientFactory
}

type s3ClientWrapper struct {
	client *s3.Client
}

func (w *s3ClientWrapper) ListBuckets(ctx context.Context, input *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return w.client.ListBuckets(ctx, input)
}

func NewClusterServer(
	kubeClientSet kubernetes.Interface,
	wsJobClient wsclient.WSJobClientProvider,
	metricsQuerier wscommon.MetricsQuerier,
	metricsPublisher MetricsPublisher,
	lockCli rlCliv1.ResourceLockInterface,
	sysCli sysv1.SystemInterface,
	wsJobInformer wsinformer.GenericInformer,
	lockInformer rlinformer.GenericInformer,
	nodeInformer v1.NodeInformer,
	nsrCli nsv1.NamespaceReservationInterface,
	kafkaClient *KafkaClient,
	options wsclient.ServerOptions,
) *ClusterServer {
	nodeLister := pkg.KubeNodeInformerLister{Informer: nodeInformer}
	return &ClusterServer{
		kubeClientSet:             kubeClientSet,
		wsJobClient:               wsJobClient,
		metricsQuerier:            metricsQuerier,
		metricsPublisher:          metricsPublisher,
		lockCli:                   lockCli,
		sysCli:                    sysCli,
		wsJobInformer:             wsJobInformer,
		lockInformer:              lockInformer,
		nodeLister:                nodeLister,
		nsrCli:                    nsrCli,
		kafkaClient:               kafkaClient,
		options:                   options,
		ingressPollInterval:       IngressPollInterval,
		enableClientLeaseStrategy: true,
		s3ClientFactory:           &prodS3ClientFactory{},
	}
}

func (server ClusterServer) Check(_ context.Context, _ *hv1.HealthCheckRequest) (*hv1.HealthCheckResponse, error) {
	return &hv1.HealthCheckResponse{Status: hv1.HealthCheckResponse_SERVING}, nil
}

func (server ClusterServer) applyServerConfig(
	serviceSpec *wsclient.ServiceSpec,
	taskType wsclient.WSService,
	image string,
	debugArgs *commonpb.DebugArgs,
) {
	// The image specified in debug args takes precedence over the image in the job request,
	// which in turn takes precedence over the image in the default task spec mapping.
	serviceSpec.Image = server.options.WsjobDefaultImage
	serviceSpec.Command = strings.Split(server.options.WsjobDefaultCommand, " ")
	if image != "" {
		serviceSpec.Image = image
	}
	if debugArgs != nil && debugArgs.DebugMgr != nil {
		if debugArgs.DebugMgr.TaskSpecHints != nil {
			hint := wscommon.Ternary(
				debugArgs.DebugMgr.TaskSpecHints[pkg.DefaultTaskSpecKey] != nil,
				debugArgs.DebugMgr.TaskSpecHints[pkg.DefaultTaskSpecKey],
				debugArgs.DebugMgr.TaskSpecHints[taskType.String()])
			if hint != nil {
				if hint.ContainerImage != "" {
					serviceSpec.Image = hint.ContainerImage
				}
				if len(hint.ContainerCommand) > 0 {
					serviceSpec.Command = hint.ContainerCommand
				}
			}
		}
		if debugArgs.DebugMgr.CbcoreOverrideScript != "" {
			serviceSpec.Env = append(serviceSpec.Env, corev1.EnvVar{
				Name:  "CBCORE_OVERRIDE_SCRIPT",
				Value: debugArgs.DebugMgr.CbcoreOverrideScript,
			})
		}
	}
}

func (server ClusterServer) GetServerVersions(
	_ context.Context,
	_ *pb.GetServerVersionsRequest,
) (*pb.GetServerVersionsResponse, error) {
	versions := make([]*pb.ComponentVersion, 2)
	versions[0] = &pb.ComponentVersion{
		Name:    pb.ComponentName_COMPONENT_NAME_CLUSTER_SERVER,
		Version: pkg.CerebrasVersion,
	}
	versions[1] = &pb.ComponentVersion{
		Name:    pb.ComponentName_COMPONENT_NAME_JOB_OPERATOR,
		Version: pkg.CerebrasVersion,
	}

	return &pb.GetServerVersionsResponse{
		Versions: versions,
	}, nil
}

func (server ClusterServer) HasMultiMgmtNodes(
	_ context.Context,
	_ *pb.HasMultiMgmtNodesRequest,
) (*pb.HasMultiMgmtNodesResponse, error) {
	return &pb.HasMultiMgmtNodesResponse{
		HasMultiMgmtNodes: server.options.HasMultiMgmtNodes,
	}, nil
}

func (server ClusterServer) GetServerConfig(
	_ context.Context,
	_ *pb.GetServerConfigRequest,
) (*pb.GetServerConfigResponse, error) {
	jobOperatorSemanticVersion, err := pkg.GetJobOperatorSemanticVersion(server.kubeClientSet)
	if err != nil {
		log.Warnf("could not retrieve job operator version: %v", err)
	}
	return &pb.GetServerConfigResponse{
		IsUserAuthEnabled:            !server.options.DisableUserAuth,
		HasMultiMgmtNodes:            server.options.HasMultiMgmtNodes,
		IsMessageBrokerAvailable:     server.kafkaClient != nil,
		JobOperatorSemanticVersion:   jobOperatorSemanticVersion,
		ClusterServerSemanticVersion: wsclient.SemanticVersion,
		DebugvizUrlPrefix:            server.options.DebugVizUrlPrefix,
		SupportedSidecarImages:       server.options.SupportedSidecarImages,
	}, nil
}

func (server ClusterServer) getJobOperatorSemanticVersion() (string, error) {
	cm, err := server.kubeClientSet.CoreV1().
		ConfigMaps(wscommon.SystemNamespace).
		Get(context.Background(), pkg.ClusterEnvConfigMapName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to read job operator version ConfigMap: %w", err)
	}

	version, exists := cm.Data[pkg.JobOperatorSemanticVersion]
	if !exists {
		return "", fmt.Errorf("%s key not found in ConfigMap", pkg.JobOperatorSemanticVersion)
	}
	return version, nil
}

func (server ClusterServer) getOperatorSemver() (*semver.Version, error) {
	operatorSemverStr, err := pkg.GetJobOperatorSemanticVersion(server.kubeClientSet)
	if err != nil {
		return nil, err
	}
	operatorSemver, err := semver.NewVersion(operatorSemverStr)
	if err != nil {
		return nil, err
	}

	return operatorSemver, nil
}

// set enableClientLeaseStrategy for cluster for e2e test only
func (server ClusterServer) setEnableClientLeaseStrategy(
	enableClientLeaseStrategy bool,
) {
	server.enableClientLeaseStrategy = enableClientLeaseStrategy
}

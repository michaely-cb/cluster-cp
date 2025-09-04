package csctl

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientcorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"

	commonutil "github.com/kubeflow/common/pkg/util"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"golang.org/x/exp/slices"

	"cerebras.com/cluster/server/pkg"

	nsv1 "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	sysv1 "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg/wsclient"
)

type Resource interface {
	Proto() proto.Message
}

type ResourceStore interface {
	Get(ctx context.Context, namespace, name string) (Resource, error)

	List(ctx context.Context, namespace string, options *pb.GetOptions) (Resource, error)

	ListV2(ctx context.Context, namespace string, options *pb.ListOptions) (Resource, error)

	PatchMerge(ctx context.Context, namespace, name string, patch []byte) (Resource, error)

	AsTable(resource Resource) *csv1.Table
}

type Server struct {
	pb.UnimplementedCsCtlV1Server

	k8s           kubernetes.Interface
	wsJobClient   *wsclient.WSJobClient
	wsJobInformer wsinformer.GenericInformer
	nodeLister    pkg.KubeNodeInformerLister

	metricsQuerier common.MetricsQuerier
	configProvider pkg.ClusterCfgProvider

	options wsclient.ServerOptions

	resourceAltName map[string]string
	stores          map[string]ResourceStore
}

func NewServer(
	wsJobInformer wsinformer.GenericInformer,
	wsJobClient *wsclient.WSJobClient,
	metricsQuerier common.MetricsQuerier,
	sysCli sysv1.SystemInterface,
	nsCli nsv1.NamespaceReservationInterface,
	lockInformer rlinformer.GenericInformer,
	nodeInformer clientcorev1.NodeInformer,
	options wsclient.ServerOptions) *Server {
	return newServer(wsJobInformer, wsJobClient.Clientset, wsJobClient, metricsQuerier, sysCli,
		nsCli, lockInformer, nodeInformer, options)
}

func newServer(
	wsJobInformer wsinformer.GenericInformer,
	k8s kubernetes.Interface,
	wsJobClient *wsclient.WSJobClient,
	metricsQuerier common.MetricsQuerier,
	sysCli sysv1.SystemInterface,
	nsCli nsv1.NamespaceReservationInterface,
	lockInformer rlinformer.GenericInformer,
	nodeInformer clientcorev1.NodeInformer,
	options wsclient.ServerOptions,
) *Server {
	nodeLister := pkg.KubeNodeInformerLister{Informer: nodeInformer}
	cfgProvider := pkg.NewK8sClusterCfgProvider(k8s, nsCli)

	s := &Server{
		k8s:            k8s,
		wsJobInformer:  wsJobInformer,
		wsJobClient:    wsJobClient,
		nodeLister:     nodeLister,
		metricsQuerier: metricsQuerier,
		configProvider: cfgProvider,
		options:        options,
		resourceAltName: map[string]string{
			"no":            "node",
			"node":          "node",
			"nodes":         "node",
			"system":        "system",
			"systems":       "system",
			"cluster":       "cluster",
			"job":           "job",
			"jobs":          "job",
			"nodegroup":     "nodegroup",
			"nodegroups":    "nodegroup",
			"version":       "version",
			"versions":      "version",
			"volume":        "volume",
			"volumes":       "volume",
			"worker-cache":  "worker-cache",
			"worker-caches": "worker-cache",
		},
		stores: map[string]ResourceStore{
			"node": &ClusterStore{nodeLister: nodeLister, metricsQuerier: metricsQuerier,
				sysClient: sysCli, lockInformer: lockInformer, wsJobInformer: wsJobInformer, cfgProvider: cfgProvider},
			"system": &ClusterStore{nodeLister: nodeLister, metricsQuerier: metricsQuerier,
				sysClient: sysCli, lockInformer: lockInformer, wsJobInformer: wsJobInformer, cfgProvider: cfgProvider},
			"cluster": &ClusterStore{nodeLister: nodeLister, metricsQuerier: metricsQuerier,
				sysClient: sysCli, lockInformer: lockInformer, wsJobInformer: wsJobInformer, cfgProvider: cfgProvider},
			"job": &JobStore{kubeClient: k8s, WsJobClient: wsJobClient, WsJobInformer: wsJobInformer,
				lockInformer: lockInformer, Options: options, nsCli: nsCli},
			"nodegroup": &NodegroupStore{cfgProvider: cfgProvider, sysClient: sysCli,
				lockInformer: lockInformer, metricsQuerier: metricsQuerier, nodeLister: nodeLister},
			"version":      &VersionStore{kubeClient: k8s, metricsQuerier: metricsQuerier},
			"volume":       &VolumeStore{kubeClient: k8s},
			"worker-cache": &WorkerCacheStore{kubeClient: k8s, metricsQuerier: metricsQuerier, nodeLister: nodeLister},
		},
	}
	return s
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	namespace := pkg.GetNamespaceFromContext(ctx)
	if req.Options != nil && req.Options.Namespace != "" {
		namespace = req.Options.Namespace
	}
	if req.Accept == pb.SerializationMethod_SERIALIZATION_METHOD_UNSPECIFIED {
		req.Accept = pb.SerializationMethod_JSON_METHOD
	}
	if req.Representation == pb.ObjectRepresentation_REPRESENTATION_UNSPECIFIED {
		req.Representation = pb.ObjectRepresentation_OBJECT_REPRESENTATION
	}

	resourceType, ok := s.resourceAltName[strings.ToLower(req.Type)]
	if !ok {
		return nil, grpcStatus.Errorf(codes.NotFound, "resource type %s not found", req.Type)
	}
	name := req.Name
	store, ok := s.stores[resourceType]
	if !ok {
		return nil, grpcStatus.Errorf(codes.Internal, "resource store %s not found", req.Type)
	}

	var result Resource
	var err error
	if resourceType == "node" {
		req.Options.NodeOnly = true
	} else if resourceType == "system" {
		req.Options.SystemOnly = true
	}
	if name == "" {
		result, err = store.List(ctx, namespace, req.Options)
	} else {
		result, err = store.Get(ctx, namespace, name)
	}
	if err != nil {
		return nil, err
	}

	var resultProto proto.Message
	if req.Representation == pb.ObjectRepresentation_TABLE_REPRESENTATION {
		resultProto = store.AsTable(result)
	} else {
		resultProto = result.Proto()
	}

	var raw []byte
	if req.Accept == pb.SerializationMethod_SERIALIZATION_METHOD_UNSPECIFIED ||
		req.Accept == pb.SerializationMethod_JSON_METHOD {
		raw, err = protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(resultProto)
	} else if req.Accept == pb.SerializationMethod_PROTOBUF_METHOD {
		raw, err = proto.Marshal(resultProto)
	} else {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "unrecognized serialization format. %v", err)
	}

	if err != nil {
		return nil, grpcStatus.Errorf(codes.Internal, "failed to serialize response. %v", err)
	}

	res := &pb.GetResponse{
		Type:        resourceType,
		ContentType: req.Accept,
		Raw:         raw,
	}
	return res, nil
}

func (s *Server) Patch(ctx context.Context, req *pb.PatchRequest) (*pb.PatchResponse, error) {
	namespace := pkg.GetNamespaceFromContext(ctx)
	if req.Accept == pb.SerializationMethod_SERIALIZATION_METHOD_UNSPECIFIED {
		req.Accept = pb.SerializationMethod_JSON_METHOD
	}
	if req.PatchType != pb.PatchRequest_MERGE {
		return nil, grpcStatus.Errorf(codes.InvalidArgument,
			"server only accepts patchType=%s, was %s", pb.PatchRequest_MERGE, req.PatchType)
	}

	resourceType, ok := s.resourceAltName[strings.ToLower(req.Type)]
	if !ok {
		return nil, grpcStatus.Errorf(codes.NotFound, "resource type %s not found", req.Type)
	}
	store, ok := s.stores[resourceType]
	if !ok {
		return nil, grpcStatus.Errorf(codes.Internal, "resource store %s not found", req.Type)
	}
	res, err := store.PatchMerge(ctx, namespace, req.Name, req.Body)
	if err != nil {
		return nil, err
	}

	var raw []byte
	if req.Accept == pb.SerializationMethod_SERIALIZATION_METHOD_UNSPECIFIED ||
		req.Accept == pb.SerializationMethod_JSON_METHOD {
		raw, err = protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(res.Proto())
	} else if req.Accept == pb.SerializationMethod_PROTOBUF_METHOD {
		raw, err = proto.Marshal(res.Proto())
	} else {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "unrecognized serialization format. %v", err)
	}
	if err != nil {
		return nil, grpcStatus.Errorf(codes.Internal, "failed to serialize result: %v", err)
	}

	return &pb.PatchResponse{
		Type:        resourceType,
		ContentType: req.Accept,
		Raw:         raw,
	}, nil
}

func (s *Server) ListResourceTypes(_ context.Context, req *pb.ListResourceTypesRequest) (*pb.ListResourceTypesResponse, error) {
	resourceNames := common.Keys(s.stores)
	sort.Strings(resourceNames)

	return &pb.ListResourceTypesResponse{
		Items: common.Map(func(name string) *pb.ResourceType { return &pb.ResourceType{Name: name} }, resourceNames),
	}, nil
}

func (s *Server) GetServerConfig(
	ctx context.Context,
	request *pb.GetServerConfigRequest,
) (*pb.GetServerConfigResponse, error) {
	return &pb.GetServerConfigResponse{
		IsUserAuthEnabled: !s.options.DisableUserAuth,
	}, nil
}

func (s *Server) CancelJobV2(
	ctx context.Context,
	request *pb.CancelJobRequest,
) (*pb.CancelJobResponse, error) {
	namespace := pkg.GetNamespaceFromContext(ctx)
	clientMeta, err := pkg.GetClientMetaFromContext(ctx)

	jobStatusMap := map[pb.JobStatus]string{
		pb.JobStatus_JOB_STATUS_CANCELLED: wsapisv1.CancelWithStatusCancelled,
		pb.JobStatus_JOB_STATUS_FAILED:    wsapisv1.CancelWithStatusFailed,
		pb.JobStatus_JOB_STATUS_SUCCEEDED: wsapisv1.CancelWithStatusSucceeded,
	}
	if jobStatusMap[request.JobStatus] == "" {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "Invalid JobStatus: %s", request.JobStatus)
	}

	e, err := s.wsJobInformer.Lister().Get(namespace + "/" + request.JobId)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &pb.CancelJobResponse{
				Message: fmt.Sprintf("Job %s does not exist", request.JobId),
			}, nil
		}
		return nil, pkg.MapStatusError(err)
	}

	wsjob := e.(*wsapisv1.WSJob)
	err = pkg.CheckUserPermission(clientMeta, wsjob)
	if err != nil {
		return nil, err
	}
	// skip if already ended
	if commonutil.IsEnded(wsjob.Status) {
		return &pb.CancelJobResponse{
			Message: "Job already ended",
		}, nil
	}

	_, err = s.wsJobClient.Annotate(wsjob,
		common.GenCancelAnnotate(jobStatusMap[request.JobStatus], wsapisv1.CanceledByCsctlReason, ""))
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	return &pb.CancelJobResponse{
		Message: "Successfully cancelled job",
	}, err
}

func (s *Server) UpdateJobPriority(
	ctx context.Context,
	request *pb.UpdateJobPriorityRequest,
) (*pb.UpdateJobPriorityResponse, error) {
	if !s.options.AllowNonDefaultJobPriority {
		return nil, grpcStatus.Errorf(
			codes.FailedPrecondition, "Job priority update is not allowed due to server configuration.")
	}

	namespace := pkg.GetNamespaceFromContext(ctx)
	clientMeta, err := pkg.GetClientMetaFromContext(ctx)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	var minPriorityVal int
	var validPriorityBuckets []string
	if clientMeta.UID != 0 {
		validPriorityBuckets = []string{"p1", "p2", "p3"}
		minPriorityVal = wsapisv1.MaxP0JobPriorityValue + 1
	} else {
		validPriorityBuckets = []string{"p0", "p1", "p2", "p3"}
		minPriorityVal = wsapisv1.MinJobPriorityValue
	}
	priorityVal, err := sanitizePriorityInput(request.JobPriority, minPriorityVal, validPriorityBuckets)
	if err != nil {
		return nil, err
	}

	e, err := s.wsJobInformer.Lister().Get(namespace + "/" + request.JobId)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	wsjob := e.(*wsapisv1.WSJob)
	if commonutil.IsEnded(wsjob.Status) {
		return nil, grpcStatus.Errorf(
			codes.FailedPrecondition, "priority update failed: wsjob %s already ended", request.JobId)
	}

	err = pkg.CheckUserPermission(clientMeta, wsjob)
	if err != nil {
		return nil, err
	}

	err = common.PatchWsjobPriority(ctx, s.wsJobClient.WsV1Interface, namespace, request.JobId, priorityVal)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	msg := fmt.Sprintf("Successfully updated %s job priority to %s", request.JobId, request.JobPriority)
	return &pb.UpdateJobPriorityResponse{Message: msg}, nil
}

func (s *Server) isJobCompleted(job *v1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == v1.JobComplete || c.Type == v1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (s *Server) isJobFailed(job *v1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == v1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (s *Server) hadJobTimedOut(job *v1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == v1.JobFailed) && c.Reason == "DeadlineExceeded" {
			return true
		}
	}
	return false
}

func (s *Server) isJobSuccess(job *v1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == v1.JobComplete) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func sanitizePriorityInput(priorityInput string, minPriorityVal int, validPriorityBuckets []string) (int, error) {
	priorityInput = strings.ToLower(priorityInput)
	priorityVal, err := strconv.Atoi(priorityInput)
	errMsg := fmt.Sprintf(
		"Priority input should be one of the priority bucket %v, or have a value between %d and %d.",
		validPriorityBuckets, minPriorityVal, wsapisv1.MaxJobPriorityValue)
	if err != nil {
		if !slices.Contains(validPriorityBuckets, priorityInput) {
			return -1, grpcStatus.Errorf(codes.InvalidArgument, errMsg)
		}
		if priorityInput == "p0" {
			return wsapisv1.MaxP0JobPriorityValue, nil
		} else if priorityInput == "p1" {
			return wsapisv1.MaxP1JobPriorityValue, nil
		} else if priorityInput == "p2" {
			return wsapisv1.MaxP2JobPriorityValue, nil
		} else {
			return wsapisv1.MaxP3JobPriorityValue, nil
		}
	}
	if priorityVal < minPriorityVal || priorityVal > wsapisv1.MaxJobPriorityValue {
		return -1, grpcStatus.Errorf(codes.InvalidArgument, errMsg)
	}
	return priorityVal, nil
}

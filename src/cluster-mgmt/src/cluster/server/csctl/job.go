package csctl

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/client-go/kubernetes"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	nsv1 "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	"cerebras.com/job-operator/common"
	"cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"
	commonpb "cerebras/pb/workflow/appliance/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonctrlv1 "github.com/kubeflow/common/pkg/controller.v1/common"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

const (
	k8sSystemNameAnnotation  = "k8s.cerebras.com/wsjob-system-names"
	kubeflowJobLabel         = "training.kubeflow.org/job-name"
	kubeflowReplicaTypeLabel = "training.kubeflow.org/replica-type"
	clusterDetailsCMPrefix   = "cluster-details-config-"
	// 10minute buffer to avoid missing events/metrics at start/end
	JobDashboardWindowBufferMs = 600 * 1000

	// The average row size we observed is 400B, so conservatively we say 1KB.
	// 10MiB max gRPC payload size / 1KB = 10000 jobs per response chunk.
	maxNumRowsPerChunk = 10000
	// When formatting as JSON, the average item size is 3KB, so conservatively we say 10KB.
	// 10MiB max gRPC payload size / 10KB = 1000 jobs per response chunk.
	maxNumJsonItemsPerChunk = 1000
)

var conditionMap = map[commonv1.JobConditionType]csv1.JobStatus_Phase{
	commonv1.JobCreated:      csv1.JobStatus_CREATED,
	commonv1.JobQueueing:     csv1.JobStatus_QUEUED,
	commonv1.JobScheduled:    csv1.JobStatus_SCHEDULED,
	commonv1.JobInitializing: csv1.JobStatus_INITIALIZING,
	commonv1.JobRunning:      csv1.JobStatus_RUNNING,
	commonv1.JobFailing:      csv1.JobStatus_FAILING,
	commonv1.JobFailed:       csv1.JobStatus_FAILED,
	commonv1.JobSucceeded:    csv1.JobStatus_SUCCEEDED,
	commonv1.JobCancelled:    csv1.JobStatus_CANCELLED,
}

var GrafanaUrl string

type Job struct {
	wsjob                   *wsapisv1.WSJob
	queue                   string
	pods                    []corev1.Pod
	clusterDetailsConfigMap corev1.ConfigMap
}

func (j *Job) Proto() proto.Message {
	return MapJob(j)
}

type JobList struct {
	items []*Job
}

type commonJobOptions struct {
	LabelFilters    map[string]string
	AllStates       bool
	Uid             int64
	Namespace       string
	Limit           int32
	SortBy          string
	OrderBy         string
	WorkflowId      string
	CurrentUserOnly *bool
}

func newJobList(
	wsjobs []*wsapisv1.WSJob,
	locks []*rlv1.ResourceLock,
	options *commonJobOptions,
) *JobList {
	var uid *int64
	if options == nil {
		options = &commonJobOptions{AllStates: true}
	} else {
		uid = &options.Uid
	}
	// CurrentUserOnly defaults to true due to legacy reason, can switch to false as default at rel-2.8
	if options.CurrentUserOnly != nil && !*options.CurrentUserOnly {
		uid = nil
	}
	jobs, queueMap, _ := common.SortJobs(wsjobs, locks,
		options.OrderBy, options.SortBy, options.AllStates, uid, options.Limit)
	return &JobList{items: common.Map(func(wsjob *wsapisv1.WSJob) *Job {
		return &Job{wsjob: wsjob, queue: queueMap[wsjob.Name]}
	}, jobs)}
}

func (j *JobList) Proto() proto.Message {
	return &csv1.JobList{Items: common.Map(MapJob, j.items)}
}

type JobStore struct {
	WsJobInformer            wsinformer.GenericInformer
	lockInformer             rlinformer.GenericInformer
	kubeClient               kubernetes.Interface
	WsJobClient              *wsclient.WSJobClient
	Options                  wsclient.ServerOptions
	enableIsolatedDashboards bool
	nsCli                    nsv1.NamespaceReservationInterface
}

func (js *JobStore) Get(ctx context.Context, namespace, name string) (Resource, error) {
	var err error
	wsjob, err := js.WsJobInformer.Lister().Get(namespace + "/" + name)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			err = validateNamespace(ctx, namespace, js.nsCli)
			if err != nil {
				return nil, pkg.MapStatusError(err)
			} else {
				return nil, grpcStatus.Errorf(codes.NotFound, "Job id %s not found", name)
			}
		}
		return nil, pkg.MapStatusError(err)
	}
	queue := wsjob.(*wsapisv1.WSJob).GetJobType()
	if js.lockInformer != nil {
		lock, err := js.lockInformer.Lister().Get(namespace + "/" + name)
		if err == nil {
			queue = lock.(*rlv1.ResourceLock).Spec.QueueName
		}
	}
	pods, err := js.kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: kubeflowJobLabel + "=" + name,
	})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	// In the event where pods were cleaned up, the server will depend on the cluster
	// details config to populate the state of the replicas.
	clusterDetailsConfigMap := &corev1.ConfigMap{}
	clusterDetailsConfigMap, err = js.getClusterDetailsConfigMap(ctx, name, namespace)
	if err != nil {
		log.Warnf("Unable to get cluster details: %s", err)
	}
	return &Job{wsjob: wsjob.(*wsapisv1.WSJob), queue: queue,
		pods: pods.Items, clusterDetailsConfigMap: *clusterDetailsConfigMap}, nil
}

// List returns a list of jobs, forwarding any status errors
func (js *JobStore) List(ctx context.Context, namespace string, options *csctl.GetOptions) (Resource, error) {
	var commonOptions *commonJobOptions = nil
	if options != nil {
		commonOptions = &commonJobOptions{
			LabelFilters:    options.LabelFilters,
			AllStates:       options.AllStates,
			Uid:             options.Uid,
			Namespace:       options.Namespace,
			Limit:           options.Limit,
			SortBy:          options.SortBy,
			OrderBy:         options.OrderBy,
			WorkflowId:      options.WorkflowId,
			CurrentUserOnly: options.CurrentUserOnly,
		}
	}
	return js.list(ctx, namespace, commonOptions)
}

// ListV2 returns a list of jobs, forwarding any status errors
func (js *JobStore) ListV2(ctx context.Context, namespace string, options *csctl.ListOptions) (Resource, error) {
	var jobOpt *csctl.ListJobOptions = nil
	switch opt := options.Option.(type) {
	case *csctl.ListOptions_JobOptions:
		jobOpt = opt.JobOptions
	default:
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "JobStore received incorrect ListOptions type %v", opt)
	}

	var commonOptions *commonJobOptions = nil
	if jobOpt != nil {
		commonOptions = &commonJobOptions{
			LabelFilters:    jobOpt.LabelFilters,
			AllStates:       jobOpt.AllStates,
			Limit:           jobOpt.Limit,
			SortBy:          options.SortBy,
			OrderBy:         options.OrderBy,
			WorkflowId:      jobOpt.WorkflowId,
			CurrentUserOnly: &jobOpt.CurrentUserOnly,
		}
	}
	// fill UID from intercepted header
	meta, err := pkg.GetClientMetaFromContext(ctx)
	if err != nil {
		return nil, err
	}
	commonOptions.Uid = meta.UID
	return js.list(ctx, namespace, commonOptions)
}

func (js *JobStore) list(ctx context.Context, namespace string, options *commonJobOptions) (Resource, error) {
	var jobs []runtime.Object
	var locks []runtime.Object
	var err error
	selector := labels.NewSelector()
	if options != nil {
		for key, value := range options.LabelFilters {
			requirement, _ := labels.NewRequirement(K8sLabelPrefix+key, selection.Equals, []string{value})
			selector = selector.Add(*requirement)
		}
		if options.WorkflowId != "" {
			requirement, _ := labels.NewRequirement(wsapisv1.WorkflowIdLabelKey, selection.Equals, []string{options.WorkflowId})
			selector = selector.Add(*requirement)
		}
	}
	if namespace == common.SystemNamespace {
		jobs, err = js.WsJobInformer.Lister().List(selector)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		if js.lockInformer != nil {
			locks, err = js.lockInformer.Lister().List(selector)
			if err != nil {
				return nil, pkg.MapStatusError(err)
			}
		}
	} else {
		jobs, err = js.WsJobInformer.Lister().ByNamespace(namespace).List(selector)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		if js.lockInformer != nil {
			locks, err = js.lockInformer.Lister().ByNamespace(namespace).List(selector)
			if err != nil {
				return nil, pkg.MapStatusError(err)
			}
		}
	}

	if len(jobs) == 0 && len(locks) == 0 {
		err = validateNamespace(ctx, namespace, js.nsCli)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		// validate workflow
		if options != nil && options.WorkflowId != "" {
			_, err = js.kubeClient.CoordinationV1().
				Leases(namespace).Get(ctx, options.WorkflowId, metav1.GetOptions{})
			if err != nil {
				return nil, pkg.MapStatusError(err)
			}
		}
	}

	return newJobList(
		common.Map(func(a runtime.Object) *wsapisv1.WSJob {
			return a.(*wsapisv1.WSJob)
		}, jobs),
		common.Map(func(a runtime.Object) *rlv1.ResourceLock {
			return a.(*rlv1.ResourceLock)
		}, locks),
		options), nil
}

func (js *JobStore) AsTable(resource Resource) *csv1.Table {
	switch val := resource.(type) {
	case *Job:
		table := emptyJobTablePb()
		table.Rows = []*csv1.RowData{js.mapJobTableRow(MapJob(val))}
		return table
	case *JobList:
		table := emptyJobTablePb()
		table.Rows = common.Map(js.mapJobTableRow, common.Map(MapJob, val.items))
		return table
	default:
		panic(fmt.Sprintf("unhandled table conversion for type: %T", val))
	}
}

func (s *Server) ListJobs(req *csctl.ListJobRequest, stream csctl.CsCtlV1_ListJobsServer) error {
	ctx := stream.Context()
	namespace := pkg.GetNamespaceFromContext(ctx)
	if req.Accept == csctl.SerializationMethod_SERIALIZATION_METHOD_UNSPECIFIED {
		req.Accept = csctl.SerializationMethod_JSON_METHOD
	}
	if req.Representation == csctl.ObjectRepresentation_REPRESENTATION_UNSPECIFIED {
		req.Representation = csctl.ObjectRepresentation_OBJECT_REPRESENTATION
	}

	store, ok := s.stores["job"]
	if !ok {
		return grpcStatus.Errorf(codes.Internal, "resource store %s not found", "job")
	}

	var result Resource
	var err error
	result, err = store.ListV2(ctx, namespace, req.Options) // result: *JobList
	if err != nil {
		return err
	}

	var resultProto proto.Message
	var numJobs int
	var numJobsPerChunk int
	if req.Representation == csctl.ObjectRepresentation_TABLE_REPRESENTATION {
		resultProto = store.AsTable(result)
		table, ok := resultProto.(*csv1.Table)
		if !ok {
			return grpcStatus.Errorf(codes.Internal, "expected *csctl.Table, got %T", resultProto)
		}
		numJobs = len(table.GetRows())
		numJobsPerChunk = maxNumRowsPerChunk
	} else {
		resultProto = result.Proto()
		jobList, ok := resultProto.(*csv1.JobList)
		if !ok {
			return grpcStatus.Errorf(codes.Internal, "expected *csctl.JobList, got %T", resultProto)
		}
		numJobs = len(jobList.GetItems())
		numJobsPerChunk = maxNumJsonItemsPerChunk
	}
	numChunks := (numJobs + numJobsPerChunk - 1) / numJobsPerChunk // ceil(numJobs/numJobsPerChunk)
	if numJobs == 0 {
		numChunks = 1
	}

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		start := chunkIdx * numJobsPerChunk
		end := start + numJobsPerChunk
		if end > numJobs {
			end = numJobs
		}
		var resultProtoChunk proto.Message

		if numJobs == 0 {
			resultProtoChunk = resultProto
		} else {
			if req.Representation == csctl.ObjectRepresentation_TABLE_REPRESENTATION {
				table, _ := resultProto.(*csv1.Table)
				resultProtoChunk = &csv1.Table{
					Columns: table.Columns,
					Rows:    table.Rows[start:end],
				}
			} else {
				jobList, _ := resultProto.(*csv1.JobList)
				resultProtoChunk = &csv1.JobList{
					Items: jobList.Items[start:end],
				}
			}
		}

		var raw []byte
		if req.Accept == csctl.SerializationMethod_SERIALIZATION_METHOD_UNSPECIFIED ||
			req.Accept == csctl.SerializationMethod_JSON_METHOD {
			raw, err = protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(resultProtoChunk)
		} else if req.Accept == csctl.SerializationMethod_PROTOBUF_METHOD {
			raw, err = proto.Marshal(resultProtoChunk)
		} else {
			return grpcStatus.Errorf(codes.InvalidArgument, "unrecognized serialization format. %v", err)
		}

		if err != nil {
			return grpcStatus.Errorf(codes.Internal, "failed to serialize response. %v", err)
		}

		res := &csctl.ListJobResponse{
			ContentType: req.Accept,
			Raw:         raw,
		}
		if err = stream.Send(res); err != nil {
			return grpcStatus.Errorf(codes.Internal, "failed to send response. %v", err)
		}
	}

	return nil
}

type objPatch struct {
	Meta *metaPatch `json:"meta,omitempty"`
}

type k8sObjPatch struct {
	Meta *metaPatch `json:"metadata,omitempty"`
}

type metaPatch struct {
	Labels map[string]*string `json:"labels"`
}

func (p *objPatch) validate() error {
	if p.Meta == nil || p.Meta.Labels == nil {
		return nil
	}
	for k, v := range p.Meta.Labels {
		if err := ValidateLabelKey(k); err != nil {
			return grpcStatus.Error(codes.InvalidArgument, err.Error())
		} else if err := ValidateLabelValue(v); err != nil {
			return grpcStatus.Error(codes.InvalidArgument, err.Error())
		}
	}
	return nil
}

func (js *JobStore) PatchMerge(ctx context.Context, namespace, name string, patch []byte) (Resource, error) {
	p := &objPatch{}
	if err := json.Unmarshal(patch, p); err != nil {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "failed to deserialize patch, %s", err.Error())
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	if p.Meta == nil || len(p.Meta.Labels) == 0 {
		return js.Get(ctx, namespace, name)
	}

	k8sPatch := k8sObjPatch{Meta: &metaPatch{Labels: map[string]*string{}}}
	for k, v := range p.Meta.Labels {
		k8sPatch.Meta.Labels[K8sLabelPrefix+k] = v
	}
	reqBody, err := json.Marshal(k8sPatch)
	if err != nil {
		return nil, grpcStatus.Error(codes.Internal, "failed to serialize backend request")
	}
	job, err := js.WsJobClient.WsV1Interface.WSJobs(namespace).Patch(ctx,
		name, types.MergePatchType, reqBody, metav1.PatchOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	return &Job{wsjob: job}, nil
}

func MapJob(j *Job) *csv1.Job {
	job := j.wsjob
	pods := j.pods
	clusterDetailsConfigMap := j.clusterDetailsConfigMap

	// build labels
	kvLabels := map[string]string{}
	for k, v := range job.Labels {
		if strings.HasPrefix(k, K8sLabelPrefix) {
			kvLabels[k[len(K8sLabelPrefix):]] = v
		}
	}

	// build volumes
	var volumeMounts []*csv1.VolumeMount
	if anno, ok := job.Annotations[wsclient.UserVolumeAnnotationKey]; ok {
		var vm []wsclient.VolumeMount
		if err := json.Unmarshal([]byte(anno), &vm); err != nil {
			log.WithField("annotationValue", anno).
				Warn("unable to parse user volume annotation json")
		} else {
			volumeMounts = common.Map(func(a wsclient.VolumeMount) *csv1.VolumeMount {
				return &csv1.VolumeMount{
					Name:      a.Name,
					MountPath: a.MountPath,
					SubPath:   a.SubPath,
				}
			}, vm)
		}
	}

	// build status
	status := &csv1.JobStatus{
		Conditions: common.Map(MapCondition, job.Status.Conditions),
	}
	if len(job.Status.Conditions) > 0 {
		lastCond := job.Status.Conditions[len(job.Status.Conditions)-1]
		if phase, ok := conditionMap[lastCond.Type]; ok {
			status.Phase = phase
		}
	}
	for _, rs := range job.Status.ReplicaStatuses {
		for _, s := range rs.FailedPodStatuses {
			status.ErrorReplicas = append(status.ErrorReplicas, MapFailedPodsStatus(s))
		}
	}
	sort.Slice(status.ErrorReplicas, func(i, j int) bool {
		return status.ErrorReplicas[i].ExitTime.AsTime().Before(status.ErrorReplicas[j].ExitTime.AsTime())
	})
	if job.Status.StartTime != nil {
		status.ExecutionTime = timestamppb.New(job.Status.StartTime.Time)
	}
	if job.Status.CompletionTime != nil {
		status.CompletionTime = timestamppb.New(job.Status.CompletionTime.Time)
	}
	// build assigned systems
	systems := make([]string, job.Spec.NumWafers)
	if job.Spec.NumWafers > 0 && job.Annotations[k8sSystemNameAnnotation] != "" {
		systems = strings.Split(job.Annotations[k8sSystemNameAnnotation], ",")
	}
	status.Systems = systems

	// build instance/replicas
	replicasByType := map[string][]*csv1.JobStatus_ReplicaInstance{}
	podToReplicaMap := make(map[string]*csv1.JobStatus_ReplicaInstance)
	clusterDetailsConfig := &commonpb.ClusterDetails{}
	if data := common.GetClusterDetailsData(&clusterDetailsConfigMap); len(data) > 0 {
		options := protojson.UnmarshalOptions{
			AllowPartial:   true,
			DiscardUnknown: true,
		}
		if err := options.Unmarshal(data, clusterDetailsConfig); err != nil {
			log.Warnf("unable to parse cluster details json: %s", string(data))
		}
		for _, taskInfo := range clusterDetailsConfig.Tasks {
			role := wsapisv1.GetReplicaTypeFull(taskInfo.TaskType.String()).Lower()
			for _, taskMap := range taskInfo.TaskMap {
				endpoint := taskMap.GetAddressBook().GetTaskIpPort()
				if len(endpoint) == 0 || taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_WSE {
					continue
				}
				podName := commonctrlv1.GenGeneralName(j.wsjob.Name, role, fmt.Sprintf("%d", taskMap.TaskId.TaskId))
				replica := &csv1.JobStatus_ReplicaInstance{
					Name:  podName,
					Node:  taskMap.GetAddressBook().GetTaskNodeName(),
					Phase: "Terminated",
				}
				podToReplicaMap[podName] = replica
				replicasByType[role] = append(replicasByType[role], replica)
			}
		}
	}
	// Iterates through all pods and update the pod phases accordingly
	for _, p := range pods {
		rt := common.Get(p.Labels, kubeflowReplicaTypeLabel, "NOT_SET")
		if replica, ok := podToReplicaMap[p.Name]; ok {
			replica.Phase = string(p.Status.Phase)
		} else {
			// The cluster details file should include all pods that need to be launched.
			// If by any chance some pods are not in the cluster details, it means that
			// the cluster details may be corrupted for some reason.
			replicasByType[rt] = append(replicasByType[rt], &csv1.JobStatus_ReplicaInstance{
				Name:  p.Name,
				Node:  p.Spec.NodeName,
				Phase: string(p.Status.Phase),
			})
			log.Warnf("cluster details config map does not contain pod %v", p.Name)
		}
	}
	var replicas []*csv1.JobStatus_ReplicaGroup
	for t, i := range replicasByType {
		replicas = append(replicas, &csv1.JobStatus_ReplicaGroup{Type: t, Instances: i})
	}
	status.Replicas = replicas

	// build user
	var user *csv1.JobUser
	if j.wsjob.Spec.User != nil {
		wsUser := j.wsjob.Spec.User
		user = &csv1.JobUser{
			Uid:      wsUser.Uid,
			Gid:      wsUser.Gid,
			Username: wsUser.Username,
		}
	}

	properties := map[string]string{
		wsapisv1.SemanticApplianceClientVersion: job.Annotations[wsapisv1.SemanticApplianceClientVersion],
		wsapisv1.SemanticClusterServerVersion:   job.Annotations[wsapisv1.SemanticClusterServerVersion],
		wsapisv1.SemanticJobOperatorVersion:     job.Annotations[wsapisv1.SemanticJobOperatorVersion],
		common.AppClientVersion:                 job.Annotations[common.AppClientVersion],
		common.ClusterServerVersion:             job.Annotations[common.ClusterServerVersion],
		common.JobOperatorVersion:               job.Annotations[common.JobOperatorVersion],
		common.WsjobImageVersion:                job.Annotations[common.WsjobImageVersion],
		common.UserSidecarVersion:               job.Annotations[common.UserSidecarVersion],
	}
	if job.Annotations[common.ActV2SpineLoad] != "" {
		properties[common.ActV2SpineLoad] = job.Annotations[common.ActV2SpineLoad]
	}
	if job.Annotations[common.BrV2SpineLoad] != "" {
		properties[common.BrV2SpineLoad] = job.Annotations[common.BrV2SpineLoad]
	}
	if val, ok := job.Annotations[commonv1.ResourceReservationStatus]; ok {
		properties[commonv1.ResourceReservationStatus] = val
	}
	return &csv1.Job{
		Meta: &csv1.ObjectMeta{
			Type:       "job",
			Namespace:  job.Namespace,
			Name:       job.Name,
			Workflow:   job.GetWorkflowId(),
			CreateTime: MapProtoTime(job.CreationTimestamp),
			Labels:     kvLabels,
			FieldPriority: map[string]int32{
				"meta.fieldPriority": 2,
				"status.replicas":    1,
			},
			Properties: properties,
		},
		Spec: &csv1.JobSpec{
			ClientWorkdir: job.Annotations[wsclient.ClientWorkdirAnnotationKey],
			VolumeMounts:  volumeMounts,
			User:          user,
			Type:          j.queue,
			Priority:      displayJobPriority(j.wsjob.GetPriority()),
		},
		Status: status,
	}
}

func (js *JobStore) getClusterDetailsConfigMap(ctx context.Context, jobName, namespace string) (*corev1.ConfigMap, error) {
	return js.kubeClient.CoreV1().ConfigMaps(namespace).Get(
		ctx,
		fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, jobName),
		metav1.GetOptions{},
	)
}

func MapCondition(condition commonv1.JobCondition) *csv1.JobStatus_Condition {
	return &csv1.JobStatus_Condition{
		Type:               string(condition.Type),
		LastTransitionTime: MapProtoTime(condition.LastUpdateTime),
		Message:            condition.Message,
	}
}

func MapFailedPodsStatus(podStatus commonv1.FailedPodStatus) *csv1.JobStatus_ReplicaErrorStatus {
	s := &csv1.JobStatus_ReplicaErrorStatus{
		Name:      podStatus.Name,
		Node:      podStatus.NodeName,
		Container: podStatus.Container,
		Message:   podStatus.Message,
		Log:       podStatus.Log,
		ExitCode:  podStatus.ExitCode,
	}
	if podStatus.FinishedAt != nil {
		s.ExitTime = MapProtoTime(*podStatus.FinishedAt)
	}
	return s
}

func arrayCol(vals []string, length int) string {
	sort.Strings(vals)
	if len(vals) == 0 || vals[0] == "" {
		return ""
	}
	rv := strings.Join(vals, ",")
	if len(rv) > length {
		rv = rv[0:length-3] + "..."
	}
	return rv
}

func mapCol(vals map[string]string, length int) string {
	var kvArr []string
	for k, v := range vals {
		kvArr = append(kvArr, k+"="+v)
	}
	return arrayCol(kvArr, length)
}

var jobTableHeader = &csv1.Table{
	Columns: []*csv1.ColumnDefinition{
		{Name: "session"},
		{Name: "name"},
		{Name: "type"},
		{Name: "priority"},
		{Name: "age"},
		{Name: "duration"},
		{Name: "phase"},
		{Name: "systems"},
		{Name: "user"},
		{Name: "labels"},
		{Name: "workflow"},
		{Name: "dashboard"},
	}}

func emptyJobTablePb() *csv1.Table {
	return jobTableHeader
}

// visible for testing
var now = time.Now

func (js *JobStore) mapJobTableRow(job *csv1.Job) *csv1.RowData {
	// if job hasn't started execution OR failed with an error before executing, then duration = 0s,
	// if executing, then duration = time since start
	// else if executing but not finished then duration = time between start and completion
	dur := "0s"
	start := ""
	end := "now"
	dashboard := "job not started yet"
	if GrafanaUrl == "" {
		dashboard = "grafana not deployed"
	}
	if job.Status.ExecutionTime != nil {
		start = fmt.Sprintf("%d", job.Status.ExecutionTime.GetSeconds()*1000-JobDashboardWindowBufferMs)
		if job.Status.CompletionTime != nil {
			end = fmt.Sprintf("%d", job.Status.CompletionTime.GetSeconds()*1000+JobDashboardWindowBufferMs)
			dur = duration.HumanDuration(job.Status.CompletionTime.AsTime().Sub(job.Status.ExecutionTime.AsTime()))
		} else {
			dur = duration.HumanDuration(now().Sub(job.Status.ExecutionTime.AsTime()))
		}
		names := strings.Split(job.Meta.Name, "/")
		if GrafanaUrl != "" {
			dashboardId := "WebHNShVz"
			// We should only enable isolated dashboards on user namespaces.
			// There is a check on the deployment side that ignores enabling isolated dashboards on the system NS.
			if js.enableIsolatedDashboards {
				dashboardId = fmt.Sprintf("%s-wsjob", common.Namespace)
			}

			dashboard = fmt.Sprintf("https://%s/d/%s/wsjob-dashboard?orgId=1&var-wsjob=%s&from=%s&to=%s",
				GrafanaUrl, dashboardId, names[len(names)-1], start, end)
		}
	}

	phase := strings.ToTitle(job.Status.Phase.String())
	// only add reserving status for ended jobs to reduce confusion
	if job.Status.Phase == csv1.JobStatus_CANCELLED ||
		job.Status.Phase == csv1.JobStatus_FAILED ||
		job.Status.Phase == csv1.JobStatus_SUCCEEDED {
		if job.Meta.Properties[commonv1.ResourceReservationStatus] == commonv1.ResourceReservedValue {
			phase += "(RESERVED)"
		}
	}

	var user string
	var jobType string
	var systems string
	if job.Spec != nil {
		if job.Spec.User != nil {
			user = job.Spec.User.Username
		}
		jobType = job.Spec.Type
	}
	if jobType != wsapisv1.JobTypeCompile && jobType != wsapisv1.JobTypePrioritizedCompile &&
		len(job.Status.Systems) > 0 {
		systems = fmt.Sprintf("(%d) %s", len(job.Status.Systems), arrayCol(job.Status.Systems, 60))
		if len(systems) > 60 {
			systems = systems[:60]
		}
	}
	age := duration.HumanDuration(now().Sub(job.Meta.CreateTime.AsTime()))
	return &csv1.RowData{Cells: []string{
		job.Meta.Namespace,
		job.Meta.Name,
		jobType,
		job.Spec.Priority,
		age,
		dur,
		phase,
		systems,
		user,
		mapCol(job.Meta.Labels, 60),
		job.Meta.Workflow,
		dashboard,
	}}
}

func displayJobPriority(priority int) string {
	if priority <= wsapisv1.MaxP0JobPriorityValue {
		return fmt.Sprintf("P0 (%d)", priority)
	} else if priority <= wsapisv1.MaxP1JobPriorityValue {
		return fmt.Sprintf("P1 (%d)", priority)
	} else if priority <= wsapisv1.MaxP2JobPriorityValue {
		return fmt.Sprintf("P2 (%d)", priority)
	} else {
		return fmt.Sprintf("P3 (%d)", priority)
	}
}

func validateNamespace(ctx context.Context, namespace string, nsCli nsv1.NamespaceReservationInterface) error {
	if common.Namespace != common.SystemNamespace {
		return nil
	}
	_, err := nsCli.Get(ctx, namespace, metav1.GetOptions{})
	return err
}

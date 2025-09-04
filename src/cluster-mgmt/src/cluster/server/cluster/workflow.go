// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/kubeflow/common/pkg/util"
)

var queryReservationTimeout = 60 * time.Second

func init() {
	duration, err := strconv.Atoi(os.Getenv("QUERY_RESERVATION_TIMEOUT_SEC"))
	if err == nil && duration > 0 {
		queryReservationTimeout = time.Duration(duration) * time.Second
	}
}

// can be refactored by introducing new CRD if needed later
func (server ClusterServer) InitWorkflow(
	ctx context.Context,
	request *pb.InitWorkflowRequest,
) (*pb.InitWorkflowResponse, error) {
	workflowId := wsclient.GenWorkflowId()
	now := &metav1.MicroTime{Time: time.Now()}
	leaseDuration := request.LeaseDurationSecondsOverride
	if leaseDuration == 0 {
		leaseDuration = int32(wscommon.ClientLeaseDuration.Seconds())
	}
	lease := &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: wscommon.Namespace,
			Name:      workflowId,
			Labels: map[string]string{
				wsapisv1.WorkflowIdLabelKey: workflowId,
			},
		},
		Spec: coordv1.LeaseSpec{
			AcquireTime:          now,
			RenewTime:            now,
			HolderIdentity:       &workflowId,
			LeaseDurationSeconds: &leaseDuration,
		},
	}
	if request.ResourceReserve {
		lease.Annotations = map[string]string{
			commonv1.ResourceReservationStatus: commonv1.ResourceReservedValue,
		}
	}
	_, err := server.kubeClientSet.CoordinationV1().
		Leases(wscommon.Namespace).Create(ctx, lease, metav1.CreateOptions{})
	if err != nil {
		log.Warnf("failed to create lease for %s: %v", workflowId, err)
		return nil, pkg.MapStatusError(err)
	}
	return &pb.InitWorkflowResponse{WorkflowId: workflowId}, nil
}

func (server ClusterServer) ReserveJobResources(ctx context.Context,
	request *pb.ReserveJobResourcesRequest) (*pb.ReserveJobResourcesResponse, error) {
	wsjob, err := server.wsJobClient.Get(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	// disallow reserving if job not granted or ended
	if !util.IsActive(wsjob.Status) {
		return nil, grpcStatus.Error(codes.FailedPrecondition,
			"Job has not scheduled yet or already ended, can't be reserved")
	}
	// disallow reserving on job with reservation already
	if wsjob.HasReservation() {
		return nil, grpcStatus.Errorf(codes.FailedPrecondition,
			"Job already inherited reservation from %s and can't be reserved again",
			wsjob.ParentJobName())
	}
	// skip if already reserved
	if wsjob.IsReserved() {
		return &pb.ReserveJobResourcesResponse{
			Message: "Job already reserved",
		}, nil
	}
	annotations := map[string]string{
		commonv1.ResourceReservationStatus: commonv1.ResourceReservedValue}
	_, err = server.wsJobClient.Annotate(wsjob, annotations)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	return &pb.ReserveJobResourcesResponse{
		Message: "Job reserved successfully",
	}, nil
}

func (server ClusterServer) ReleaseJobResources(ctx context.Context,
	request *pb.ReleaseJobResourcesRequest) (*pb.ReleaseJobResourcesResponse, error) {
	wsjob, err := server.wsJobClient.Get(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	// skip if not reserved
	if !wsjob.IsReserved() {
		return &pb.ReleaseJobResourcesResponse{
			Message: "Job is not reserved, skip release",
		}, nil
	}
	annotations := map[string]string{
		commonv1.ResourceReservationStatus: commonv1.ResourceReleasedValue}
	_, err = server.wsJobClient.Annotate(wsjob, annotations)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	return &pb.ReleaseJobResourcesResponse{
		Message: "Job unreserved successfully",
	}, nil
}

func (server ClusterServer) ReleaseWorkflowResources(ctx context.Context,
	request *pb.ReleaseWorkflowResourcesRequest) (*pb.ReleaseWorkflowResourcesResponse, error) {
	lease, err := server.kubeClientSet.CoordinationV1().
		Leases(wscommon.Namespace).Get(ctx, request.WorkflowId, metav1.GetOptions{})
	if err != nil {
		log.Warnf("failed to get workflow lease for %s: %v", request.WorkflowId, err)
		return nil, pkg.MapStatusError(err)
	}
	if lease.Annotations[commonv1.ResourceReservationStatus] != commonv1.ResourceReservedValue {
		// skip if not reserved
		return &pb.ReleaseWorkflowResourcesResponse{
			Message: "Workflow is not reserved, skip release",
		}, nil
	}
	annotations := map[string]string{
		commonv1.ResourceReservationStatus: commonv1.ResourceReleasedValue,
	}
	_, err = server.kubeClientSet.CoordinationV1().
		Leases(wscommon.Namespace).Patch(ctx,
		lease.Name, types.MergePatchType,
		wscommon.GetAnnotationPatch(annotations), metav1.PatchOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	return &pb.ReleaseWorkflowResourcesResponse{
		Message: "Workflow unreserved successfully",
	}, nil
}

func (server ClusterServer) GetReservationStatus(ctx context.Context,
	request *pb.GetReservationStatusRequest) (*pb.GetReservationStatusResponse, error) {
	wsjob, err := server.wsJobClient.Get(wscommon.Namespace, request.JobId)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	// error out if not reserved
	if !wsjob.IsReserved() {
		return nil, grpcStatus.Error(codes.FailedPrecondition,
			"Job is not reserved, can't get reservation status")
	}
	// start query
	lock, err := server.lockCli.Get(ctx, wscommon.ReservingLockName(request.JobId), metav1.GetOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	lock.Annotations = wscommon.EnsureMap(lock.Annotations)
	lock.QueryReservation()
	lock, err = server.lockCli.Update(ctx, lock, metav1.UpdateOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	// start waiting for lock reservation status update
	w, err := server.lockCli.Watch(ctx, metav1.ListOptions{
		Watch:           true,
		ResourceVersion: lock.ResourceVersion,
		FieldSelector:   "metadata.name=" + lock.Name,
	})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	log.Infof("watch on lock %s (rv=%s) started.", lock.Name, lock.ResourceVersion)
	defer w.Stop()
	timer := time.NewTimer(queryReservationTimeout)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-w.ResultChan():
			if !ok {
				log.Errorf("watch on lock %s (rv=%s) closed.", lock.Name, lock.ResourceVersion)
				return nil, grpcStatus.Errorf(codes.Unavailable,
					"failed to query reservation status: %s", request.JobId)
			}
			rl, ok := event.Object.(*rlv1.ResourceLock)
			if !ok {
				continue
			}
			lock = rl
			count, message, err := lock.GetReservedSystems()
			if err == nil {
				return &pb.GetReservationStatusResponse{
					SystemCount: int32(count),
					Message:     message,
				}, nil
			}
		case <-timer.C:
			return nil, grpcStatus.Errorf(codes.Unavailable,
				"timed out querying reservation status: %s", request.JobId)
		}
	}
}

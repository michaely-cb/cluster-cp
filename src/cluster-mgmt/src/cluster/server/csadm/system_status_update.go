package csadm

import (
	"context"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"cerebras.com/cluster/server/pkg"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
)

const (
	CsadmSystemError    = "CsadmSystemError"
	ClusterServerUpdate = "ClusterServerUpdate"
)

func (s *Server) UpdateSystemStatus(ctx context.Context,
	request *pb.UpdateSystemStatusRequest) (*pb.UpdateSystemStatusResponse, error) {
	system, err := s.sysCli.Get(ctx, request.SystemName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, grpcStatus.Errorf(codes.NotFound,
				"System %s not found in cluster", request.SystemName)
		} else {
			return nil, pkg.MapStatusError(err)
		}
	}

	// checking existing condition
	conditionId := -1
	status := system.Status.DeepCopy()
	for id, c := range status.Conditions {
		if c.Type == CsadmSystemError {
			conditionId = id
			break
		}
	}

	// create/update condition
	requestErrorStatus := BoolToConditionStatus(!request.Healthy)
	condition := corev1.NodeCondition{
		Type:               CsadmSystemError,
		Status:             requestErrorStatus,
		Reason:             ClusterServerUpdate,
		Message:            request.Message,
		LastTransitionTime: metav1.Now(),
		LastHeartbeatTime:  metav1.Now(),
	}
	if conditionId >= 0 {
		if status.Conditions[conditionId].Status == requestErrorStatus {
			condition.Message = status.Conditions[conditionId].Message + ";" + condition.Message
		}
		status.Conditions[conditionId] = condition
	} else {
		status.Conditions = append(status.Conditions, condition)
	}
	system.Status = *status

	// update system condition
	if _, err = s.sysCli.UpdateStatus(ctx, system, metav1.UpdateOptions{}); err != nil {
		return nil, pkg.MapStatusError(err)
	}
	return &pb.UpdateSystemStatusResponse{Message: "Update system status success"}, nil
}

func BoolToConditionStatus(value bool) corev1.ConditionStatus {
	if value {
		return corev1.ConditionTrue
	}
	return corev1.ConditionFalse
}

package cluster

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
)

func (server ClusterServer) ReportMetrics(
	_ context.Context,
	request *pb.ReportMetricRequest,
) (*pb.ReportMetricResponse, error) {
	if request.JobId == "" {
		return nil, grpcStatus.Errorf(codes.InvalidArgument, "Job id not found")
	}
	for _, metric := range request.Metrics {
		server.metricsPublisher.TrackMetric(
			metric.Name,
			metric.Value,
			metric.TimestampSec,
			request.JobId,
			metric.Labels,
		)
	}
	response := &pb.ReportMetricResponse{
		Code:    pb.ReportMetricResponse_SUCCESS,
		Message: fmt.Sprintf("Logged metrics for %s", request.JobId),
	}
	return response, nil
}

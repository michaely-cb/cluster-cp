//go:build !e2e

package cluster

import (
	"bytes"
	"context"
	"testing"

	log "github.com/sirupsen/logrus"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	"github.com/stretchr/testify/assert"
)

func TestReportMetricWithLabels(t *testing.T) {

	jan1st2025EpochSec := uint64(1735689600)

	request := &pb.ReportMetricRequest{
		JobId: "wsjob-123",
		Metrics: []*pb.Metric{
			{
				Name:         "metric_name",
				Value:        1.5,
				TimestampSec: jan1st2025EpochSec,
				Labels: map[string]string{
					"key_1": "value_1",
					"key_2": "value_2",
				},
			},
		},
	}

	expectedLogContent := []string{
		" wsjob=wsjob-123",
		" time_override_epoch_sec=1735689600",
		" namespace=job-operator",
		"msg=\"{" +
			"\\\"labels\\\":{\\\"key_1\\\":\\\"value_1\\\",\\\"key_2\\\":\\\"value_2\\\"}," +
			"\\\"metric\\\":\\\"metric_name\\\"," +
			"\\\"timestampSec\\\":1735689600," +
			"\\\"value\\\":1.5" +
			"}\"",
	}

	RunTest(t, request, expectedLogContent)
}

func TestReportMetricWithoutLabels(t *testing.T) {

	jan1st2025EpochSec := uint64(1735689600)

	request := &pb.ReportMetricRequest{
		JobId: "wsjob-123",
		Metrics: []*pb.Metric{
			{
				Name:         "metric_name",
				Value:        1.5,
				TimestampSec: jan1st2025EpochSec,
			},
		},
	}

	expectedLogContent := []string{
		" wsjob=wsjob-123",
		" time_override_epoch_sec=1735689600",
		" namespace=job-operator",
		"msg=\"{" +
			"\\\"labels\\\":null," +
			"\\\"metric\\\":\\\"metric_name\\\"," +
			"\\\"timestampSec\\\":1735689600," +
			"\\\"value\\\":1.5" +
			"}\"",
	}

	RunTest(t, request, expectedLogContent)
}

func RunTest(t *testing.T, request *pb.ReportMetricRequest, expectedLogContent []string) {
	metricsPublisher := NewLogMetricsPublisher()
	clusterServer := &ClusterServer{
		metricsPublisher: metricsPublisher,
	}

	// capture log output, so that we can verify the output matches what we expect
	logBuffer := bytes.Buffer{}
	log.SetOutput(&logBuffer)

	response, err := clusterServer.ReportMetrics(context.Background(), request)

	logOutput := logBuffer.String()

	assert.NoError(t, err)
	assert.Equal(t, pb.ReportMetricResponse_SUCCESS, response.Code)

	for _, expectedContent := range expectedLogContent {
		assert.Contains(t, logOutput, expectedContent)
	}
}

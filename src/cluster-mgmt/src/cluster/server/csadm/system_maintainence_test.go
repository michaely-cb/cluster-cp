//go:build !e2e

package csadm

import (
	"context"
	"errors"
	"testing"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

// Helper to create standard test context
func createTestContext(namespace string) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, pkg.ResourceNamespaceHeader, namespace)
	ctx = context.WithValue(ctx, pkg.ClientMetaKey, pkg.ClientMeta{
		UID:      1001,
		GID:      1001,
		Username: "testuser",
		Groups:   []int64{1001},
	})
	return ctx
}

// Helper to create scheduler hint matcher
func createSchedulerHintMatcher(expectedSystem string) interface{} {
	return mock.MatchedBy(func(jobSpec *wsapisv1.WSJob) bool {
		return jobSpec.Annotations[wsapisv1.SchedulerHintAllowSys] == expectedSystem
	})
}

// Test basic validation cases
func TestCreateSystemMaintenance_UnsupportedWorkflowType(t *testing.T) {
	// Set up a context with required values.
	ctx := createTestContext("default")

	// Initialize a minimal Server.
	srv := &Server{}

	req := &pb.CreateSystemMaintenanceRequest{
		WorkflowType: pb.SystemMaintenanceType(999), // unsupported type
		Systems:      []string{"systemA"},
		AllowError:   false,
	}

	_, err := srv.CreateSystemMaintenance(ctx, req)
	if err == nil {
		t.Fatal("expected error for unsupported workflow type, got nil")
	}
	st, _ := grpcStatus.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected error code %v, got %v; error: %v", codes.InvalidArgument, st.Code(), err)
	}
}

func TestCreateSystemMaintenance_MissingClientMetaKey(t *testing.T) {
	// Context without ClientMetaKey.
	ctx := context.Background()
	ctx = context.WithValue(ctx, pkg.ResourceNamespaceHeader, "default")
	srv := &Server{}
	req := &pb.CreateSystemMaintenanceRequest{
		WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
		Systems:      []string{"systemA"},
		AllowError:   false,
	}
	_, err := srv.CreateSystemMaintenance(ctx, req)
	if err == nil {
		t.Fatal("expected error for missing client meta key, got nil")
	}
	st, _ := grpcStatus.FromError(err)
	if st.Code() != codes.Internal {
		t.Errorf("expected error code %v, got %v; error: %v", codes.Internal, st.Code(), err)
	}
}

// Test partial success scenario
func TestCreateSystemMaintenance_PartialSuccess(t *testing.T) {
	// Create context with required values
	namespace := "test_ns"
	ctx := createTestContext(namespace)

	mockWSJobClient := new(MockWSJobClient)
	mockNSRClient := new(MockNSRClient)

	clusterServerVersion := wsclient.SemanticVersion
	wsclient.SemanticVersion = "1.0.7"
	defer func() { wsclient.SemanticVersion = clusterServerVersion }()

	// Create a successful job for system1
	successJob := &wsapisv1.WSJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wsjob-success123",
			Namespace:         namespace,
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				wsapisv1.SchedulerHintAllowSys:  "system1",
				wsapisv1.SystemMaintDryrunAnnot: "true",
			},
			Labels: map[string]string{
				wsapisv1.SystemMaintLabelKey: wsapisv1.SystemMaintTypeMemBist,
			},
		},
		Spec: wsapisv1.WSJobSpec{
			Priority: wscommon.Pointer(defaultSystemMaintenanceJobPriority),
			User: &wsapisv1.JobUser{
				Username: "testuser",
				Uid:      1001,
				Gid:      1001,
				Groups:   []int64{1001},
			},
			WSReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
				wsapisv1.WSReplicaTypeCoordinator: {
					Replicas: wscommon.Pointer(int32(1)),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "coordinator"}},
						},
					},
				},
			},
		},
	}

	// Mock `Create` to return a success job on "system1" and an error on "system2"
	mockWSJobClient.On("Create", createSchedulerHintMatcher("system1")).
		Return(successJob, nil)

	mockWSJobClient.On("Create", createSchedulerHintMatcher("system2")).
		Return(nil, errors.New("failed to create job"))

	// Create a server with the mock clients
	server := &Server{
		wsJobClient: mockWSJobClient,
		nsrCli:      mockNSRClient,
		k8s:         fake.NewSimpleClientset(),
	}

	// Create the request
	req := &pb.CreateSystemMaintenanceRequest{
		WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
		Systems:      []string{"system1", "system2"},
		AllowError:   false,
		Payload: &pb.CreateSystemMaintenanceRequest_Membist{
			Membist: &pb.MembistPayload{
				VddcVoltageOffset: -30,
				SkipFailedOps:     false,
			},
		},
	}

	// Call the function
	resp, err := server.CreateSystemMaintenance(ctx, req)

	// Check overall response
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify successful jobs
	assert.Len(t, resp.Jobs, 1)
	assert.Equal(t, "wsjob-success123", resp.Jobs[0].JobId)
	assert.Equal(t, pb.JobStatus_JOB_STATUS_CREATED, resp.Jobs[0].Status)
	assert.Equal(t, []string{"system1"}, resp.Jobs[0].Systems)
	assert.Equal(t, int32(defaultSystemMaintenanceJobPriority), resp.Jobs[0].Priority)
	assert.Equal(t, namespace, resp.Jobs[0].Namespace)
	assert.NotNil(t, resp.Jobs[0].User)
	assert.Equal(t, "testuser", resp.Jobs[0].User.Username)

	// Verify failed jobs
	assert.Len(t, resp.FailedJobs, 1)
	assert.Empty(t, resp.FailedJobs[0].JobId) // Empty JobId indicates failure
	assert.Equal(t, pb.JobStatus_JOB_STATUS_UNSPECIFIED, resp.FailedJobs[0].Status)
	assert.Equal(t, []string{"system2"}, resp.FailedJobs[0].Systems)
	assert.Equal(t, "testuser", resp.FailedJobs[0].User.Username)

	// Verify mock expectations
	mockWSJobClient.AssertExpectations(t)
}

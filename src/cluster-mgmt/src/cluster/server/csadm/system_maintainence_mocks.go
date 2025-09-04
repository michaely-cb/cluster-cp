package csadm

import (
	"context"

	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"github.com/stretchr/testify/mock"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	nsv1 "cerebras.com/job-operator/apis/namespace/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"

	"cerebras.com/cluster/server/pkg/wsclient"
)

// Mock WSJob Client for testing
type MockWSJobClient struct {
	mock.Mock
}

func (m *MockWSJobClient) Create(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
	args := m.Called(wsjob)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*wsapisv1.WSJob), args.Error(1)
}

// All other methods return nil values
func (m *MockWSJobClient) Get(namespace, jobName string) (*wsapisv1.WSJob, error) {
	return nil, nil
}

func (m *MockWSJobClient) List(namespace string) (*wsapisv1.WSJobList, error) {
	return nil, nil
}

func (m *MockWSJobClient) Delete(namespace, jobName string) error {
	return nil
}

func (m *MockWSJobClient) Update(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
	return nil, nil
}

func (m *MockWSJobClient) Annotate(wsjob *wsapisv1.WSJob, annotations map[string]string) (*wsapisv1.WSJob, error) {
	return nil, nil
}

func (m *MockWSJobClient) LogExport(req *csctlpb.CreateLogExportRequest, jobSpec *wsclient.JobSpec) (*batchv1.Job, error) {
	return nil, nil
}

func (m *MockWSJobClient) BuildImage(jobSpec *wsclient.JobSpec, baseImage, pipOptions string,
	frozenDependencies []string, nodeSelector map[string]string) (*batchv1.Job, error) {
	return nil, nil
}

func (m *MockWSJobClient) GetJobLogs(ctx context.Context, namespace, labelSelector string, tail int) (string, error) {
	return "", nil
}

// Mock NSR Client for testing
type MockNSRClient struct {
	mock.Mock
}

// Only get method needs a real implementation
func (m *MockNSRClient) Get(ctx context.Context, name string, options metav1.GetOptions) (*nsv1.NamespaceReservation, error) {
	args := m.Called(ctx, name, options)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*nsv1.NamespaceReservation), args.Error(1)
}

// All other methods return nil values
func (m *MockNSRClient) Create(ctx context.Context, namespaceReservation *nsv1.NamespaceReservation, opts metav1.CreateOptions) (*nsv1.NamespaceReservation, error) {
	return nil, nil
}

func (m *MockNSRClient) Update(ctx context.Context, namespaceReservation *nsv1.NamespaceReservation, opts metav1.UpdateOptions) (*nsv1.NamespaceReservation, error) {
	return nil, nil
}

func (m *MockNSRClient) UpdateStatus(ctx context.Context, namespaceReservation *nsv1.NamespaceReservation, opts metav1.UpdateOptions) (*nsv1.NamespaceReservation, error) {
	return nil, nil
}

func (m *MockNSRClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return nil
}

func (m *MockNSRClient) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return nil
}

func (m *MockNSRClient) List(ctx context.Context, opts metav1.ListOptions) (*nsv1.NamespaceReservationList, error) {
	return nil, nil
}

func (m *MockNSRClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return nil, nil
}

func (m *MockNSRClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *nsv1.NamespaceReservation, err error) {
	return nil, nil
}

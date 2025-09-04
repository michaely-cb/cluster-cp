package cmd

import (
	"bytes"
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/proto"

	pbadm "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
)

type FakeOutErr struct {
	O *bytes.Buffer
	E *bytes.Buffer
}

func (f FakeOutErr) Out() io.Writer {
	return f.O
}

func (f FakeOutErr) Err() io.Writer {
	return f.E
}

func NewFakeOutErr() FakeOutErr {
	return FakeOutErr{
		O: &bytes.Buffer{},
		E: &bytes.Buffer{},
	}
}

type fakeGrpcClient struct {
	requests      []proto.Message
	responseOrErr []interface{}
}

func (c *fakeGrpcClient) Response(in proto.Message) (proto.Message, error) {
	c.requests = append(c.requests, in)
	responseIndex := len(c.requests) - 1
	if len(c.responseOrErr) <= responseIndex {
		panic("no response set")
	}

	res := c.responseOrErr[responseIndex]
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res.(proto.Message), nil
}

type FakeCsadmV1 struct {
	fakeGrpcClient
}

func (c *FakeCsadmV1) CreateSession(ctx context.Context,
	in *pbadm.CreateSessionRequest, opts ...grpc.CallOption) (*pbadm.Session, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.Session), nil
}

func (c *FakeCsadmV1) UpdateSession(ctx context.Context,
	in *pbadm.UpdateSessionRequest, opts ...grpc.CallOption) (*pbadm.Session, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.Session), nil
}

func (c *FakeCsadmV1) GetSession(ctx context.Context,
	in *pbadm.GetSessionRequest, opts ...grpc.CallOption) (*pbadm.Session, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.Session), nil
}

func (c *FakeCsadmV1) ListSession(ctx context.Context,
	in *pbadm.ListSessionRequest, opts ...grpc.CallOption) (*pbadm.ListSessionResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.ListSessionResponse), nil
}

func (c *FakeCsadmV1) DeleteSession(ctx context.Context,
	in *pbadm.DeleteSessionRequest, opts ...grpc.CallOption) (*pbadm.DeleteSessionResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.DeleteSessionResponse), nil
}

func (c *FakeCsadmV1) UpdateSystemStatus(ctx context.Context,
	request *pbadm.UpdateSystemStatusRequest, opts ...grpc.CallOption) (*pbadm.UpdateSystemStatusResponse, error) {
	res, err := c.Response(request)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.UpdateSystemStatusResponse), nil
}

func (c *FakeCsadmV1) CreateSystemMaintenance(ctx context.Context,
	request *pbadm.CreateSystemMaintenanceRequest, opts ...grpc.CallOption) (*pbadm.CreateSystemMaintenanceResponse, error) {
	res, err := c.Response(request)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.CreateSystemMaintenanceResponse), nil
}

func (c *FakeCsadmV1) GetSystemMaintenance(ctx context.Context,
	request *pbadm.GetSystemMaintenanceRequest, opts ...grpc.CallOption) (*pbadm.GetSystemMaintenanceResponse, error) {
	res, err := c.Response(request)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.GetSystemMaintenanceResponse), nil
}

func (c *FakeCsadmV1) ListSystemMaintenance(ctx context.Context,
	request *pbadm.ListSystemMaintenanceRequest, opts ...grpc.CallOption) (*pbadm.ListSystemMaintenanceResponse, error) {
	res, err := c.Response(request)
	if err != nil {
		return nil, err
	}
	return res.(*pbadm.ListSystemMaintenanceResponse), nil
}

func getLastRequest[T proto.Message](c *FakeCsadmV1) T {
	lastReq := c.requests[len(c.requests)-1]
	return lastReq.(T)
}

type FakeCsCtlV1 struct {
	fakeGrpcClient
	listStreamingImplemented bool // for fallback testing
}

func (c *FakeCsCtlV1) GetJobTopology(_ context.Context, in *pb.GetJobTopologyRequest, _ ...grpc.CallOption) (*pb.GetJobTopologyResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.GetJobTopologyResponse), nil
}

func (c *FakeCsCtlV1) ListResourceTypes(_ context.Context, in *pb.ListResourceTypesRequest, _ ...grpc.CallOption) (*pb.ListResourceTypesResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.ListResourceTypesResponse), nil
}

func (c *FakeCsCtlV1) Get(_ context.Context, in *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.GetResponse), nil
}

type FakeListJobsClient struct {
	grpc.ClientStream
	responseOrErr []interface{}
	index         int
}

func (f *FakeListJobsClient) Recv() (*pb.ListJobResponse, error) {
	// one request, streaming back all response
	if f.index >= len(f.responseOrErr) {
		return nil, io.EOF
	}
	res := f.responseOrErr[f.index]
	f.index++
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res.(*pb.ListJobResponse), nil
}

func (c *FakeCsCtlV1) ListJobs(_ context.Context, in *pb.ListJobRequest, _ ...grpc.CallOption) (pb.CsCtlV1_ListJobsClient, error) {
	if !c.listStreamingImplemented {
		return nil, status.Error(codes.Unimplemented, "ListJobs not yet implemented")
	}
	return &FakeListJobsClient{
		responseOrErr: c.responseOrErr,
		index:         0,
	}, nil
}

func (c *FakeCsCtlV1) Patch(_ context.Context, in *pb.PatchRequest, _ ...grpc.CallOption) (*pb.PatchResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.PatchResponse), nil
}

func (c *FakeCsCtlV1) CreateLogExport(ctx context.Context, in *pb.CreateLogExportRequest, opts ...grpc.CallOption) (*pb.CreateLogExportResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.CreateLogExportResponse), nil
}
func (c *FakeCsCtlV1) GetLogExport(ctx context.Context, in *pb.GetLogExportRequest, opts ...grpc.CallOption) (*pb.GetLogExportResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.GetLogExportResponse), nil
}

func (c *FakeCsCtlV1) DownloadLogExport(ctx context.Context, in *pb.DownloadLogExportRequest, opts ...grpc.CallOption) (*pb.DownloadLogExportResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.DownloadLogExportResponse), nil
}

func (c *FakeCsCtlV1) ClearWorkerCache(ctx context.Context, in *pb.ClearWorkerCacheRequest, opts ...grpc.CallOption) (*pb.ClearWorkerCacheResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.ClearWorkerCacheResponse), nil
}

func (c *FakeCsCtlV1) GetServerConfig(ctx context.Context, in *pb.GetServerConfigRequest, opts ...grpc.CallOption) (*pb.GetServerConfigResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.GetServerConfigResponse), nil
}

func (c *FakeCsCtlV1) CancelJobV2(ctx context.Context, in *pb.CancelJobRequest, opts ...grpc.CallOption) (*pb.CancelJobResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.CancelJobResponse), nil
}

func (c *FakeCsCtlV1) UpdateJobPriority(ctx context.Context, in *pb.UpdateJobPriorityRequest, opts ...grpc.CallOption) (*pb.UpdateJobPriorityResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.UpdateJobPriorityResponse), nil
}

func (c *FakeCsCtlV1) GetDebugArtifactStatus(ctx context.Context, in *pb.GetDebugArtifactStatusRequest, opts ...grpc.CallOption) (*pb.GetDebugArtifactStatusResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.GetDebugArtifactStatusResponse), nil
}

func (c *FakeCsCtlV1) UpdateDebugArtifactStatus(ctx context.Context, in *pb.UpdateDebugArtifactStatusRequest, opts ...grpc.CallOption) (*pb.UpdateDebugArtifactStatusResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.UpdateDebugArtifactStatusResponse), nil
}

func (c *FakeCsCtlV1) GetClusterTopology(ctx context.Context, in *pb.GetClusterTopologyRequest, opts ...grpc.CallOption) (*pb.GetClusterTopologyResponse, error) {
	res, err := c.Response(in)
	if err != nil {
		return nil, err
	}
	return res.(*pb.GetClusterTopologyResponse), nil
}

type FakeClientFactory struct {
	csAdmClient *FakeCsadmV1
	csCtlClient *FakeCsCtlV1
}

func NewFakeClientFactory() FakeClientFactory {
	return FakeClientFactory{
		csAdmClient: &FakeCsadmV1{},
		csCtlClient: &FakeCsCtlV1{},
	}
}

func (f FakeClientFactory) AppendCsCtlResponse(r interface{}) {
	f.csCtlClient.responseOrErr = append(f.csCtlClient.responseOrErr, r)
}

func (f FakeClientFactory) AppendCsCtlErrorCode(msg string, code codes.Code) {
	err := grpc.Errorf(code, msg)
	f.csCtlClient.responseOrErr = append(f.csCtlClient.responseOrErr, err)
}

func (f FakeClientFactory) NewCsCtlV1(ctx context.Context) (pb.CsCtlV1Client, error) {
	return f.csCtlClient, nil
}

func (f FakeClientFactory) NewCsAdmV1(ctx context.Context) (pbadm.CsAdmV1Client, error) {
	return f.csAdmClient, nil
}

func (f FakeClientFactory) SetListStreaming(implemented bool) {
	f.csCtlClient.listStreamingImplemented = implemented
}

func (f FakeClientFactory) Close() error {
	f.csCtlClient, f.csAdmClient = nil, nil
	return nil
}

type MockRootUserIdentityProvider struct{}

func (m MockRootUserIdentityProvider) GetUidGidGroups() (int64, int64, []int64, error) {
	return 0, 0, nil, nil // Simulate root user
}

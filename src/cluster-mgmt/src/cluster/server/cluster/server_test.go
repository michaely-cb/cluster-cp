//go:build !e2e

// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	batchv1 "k8s.io/api/batch/v1"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8testing "k8s.io/client-go/testing"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	commonpb "cerebras/pb/workflow/appliance/common"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"

	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/server/csctl"
	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

type WsJobClientMock struct {
	MockedCreate func(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error)
	MockedGet    func(namespace, jobName string) (*wsapisv1.WSJob, error)
	MockedList   func(namespace string) (*wsapisv1.WSJobList, error)
	MockedDelete func(namespace, jobName string) error
	MockedUpdate func(newJob *wsapisv1.WSJob) (*wsapisv1.WSJob, error)
	MockedCancel func(namespace, jobName string, annotations map[string]string) (*wsapisv1.WSJob, error)
}

func (m *WsJobClientMock) Create(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
	return m.MockedCreate(wsjob)
}

func (m *WsJobClientMock) Get(namespace, jobName string) (*wsapisv1.WSJob, error) {
	return m.MockedGet(namespace, jobName)
}

func (m *WsJobClientMock) List(namespace string) (*wsapisv1.WSJobList, error) {
	return m.MockedList(namespace)
}

func (m *WsJobClientMock) Delete(namespace, jobName string) error {
	return m.MockedDelete(namespace, jobName)
}

func (m *WsJobClientMock) Update(newJob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
	return m.MockedUpdate(newJob)
}

func (m *WsJobClientMock) Annotate(wsJob *wsapisv1.WSJob, annotations map[string]string) (*wsapisv1.WSJob, error) {
	return m.MockedCancel(wsJob.Namespace, wsJob.Name, annotations)
}

func (m *WsJobClientMock) LogExport(req *csctlpb.CreateLogExportRequest, jobSpec *wsclient.JobSpec) (*batchv1.Job, error) {
	return nil, nil
}

func (m *WsJobClientMock) BuildImage(jobSpec *wsclient.JobSpec, baseImage, pipOptions string,
	frozenDependencies []string, nodeSelector map[string]string) (*batchv1.Job, error) {
	return nil, nil
}

func (m *WsJobClientMock) GetJobLogs(ctx context.Context, namespace, labelSelector string, tail int) (string, error) {
	return "", nil
}

var mockedWsJobClient = new(WsJobClientMock)

type PromClientMock struct {
	MockedGet func() (model.Value, error)
}

func (m *PromClientMock) QueryNodeDiskUsage(ctx context.Context) (model.Value, error) {
	return m.MockedGet()
}

func (m *PromClientMock) QueryNodeCpuUsage(ctx context.Context) (model.Value, error) {
	return m.MockedGet()
}

func (m *PromClientMock) QueryNodeMemUsage(ctx context.Context) (model.Value, error) {
	return m.MockedGet()
}

func (m *PromClientMock) QuerySystemVersion(ctx context.Context, _ []string) (model.Value, error) {
	return m.MockedGet()
}

func (m *PromClientMock) QueryNodeCpuAllocatable(ctx context.Context) (map[string]float64, error) {
	return nil, nil
}

func (m *PromClientMock) QueryNodeMemAllocatable(ctx context.Context) (map[string]int64, error) {
	return nil, nil
}

func (m *PromClientMock) QueryPodContainerInfo(ctx context.Context, container string, namespace string) (model.Value, error) {
	return m.MockedGet()
}

func (m *PromClientMock) QuerySystemInMaintenanceMode(ctx context.Context, _ []string) (model.Value, error) {
	return m.MockedGet()
}

func (m *PromClientMock) QueryJobOperatorVersion(ctx context.Context, namespace string) (string, string, error) {
	return "unknown", "unknown", nil
}

func (m *PromClientMock) QueryClusterServerVersion(ctx context.Context, namespace string) (string, string, error) {
	return "unknown", "unknown", nil
}

func (m *PromClientMock) QueryPodPortAffinity(ctx context.Context, jobId, namespace string) (model.Value, error) {
	return m.MockedGet()
}

func (m *PromClientMock) QueryBrSetTopology(ctx context.Context, jobId, namespace string) (model.Value, error) {
	return m.MockedGet()
}

var mockedPromClient = new(PromClientMock)

var fakeKubeClient = fake.NewSimpleClientset()

func TestMain(m *testing.M) {
	logrus.SetOutput(io.Discard)
	os.Exit(m.Run())
}

type ServiceSpecExpectation func(*testing.T, wsclient.ServiceSpecs)

type JobSpecExpectation func(t *testing.T, spec wsclient.JobSpec)

var defaultOperatorVersion = "1.0.5"

func setOperatorVersion(t *testing.T, fakeClientSet *fake.Clientset, version string) {
	clusterEnvConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: pkg.ClusterEnvConfigMapName, Namespace: common.SystemNamespace},
		Data: map[string]string{
			"SEMANTIC_VERSION": version,
		},
	}
	_, err := fakeClientSet.CoreV1().ConfigMaps(common.DefaultNamespace).Create(context.Background(), clusterEnvConfigMap, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		cm, err := fakeClientSet.CoreV1().ConfigMaps(common.SystemNamespace).Get(context.Background(), pkg.ClusterEnvConfigMapName, metav1.GetOptions{})
		assert.NoError(t, err)

		cm.Data["SEMANTIC_VERSION"] = version
		_, err = fakeClientSet.CoreV1().ConfigMaps(common.SystemNamespace).Update(context.Background(), cm, metav1.UpdateOptions{})
		assert.NoError(t, err)
	} else {
		assert.NoError(t, err)
	}
}

func TestCreateCompileJob(t *testing.T) {
	compileDirRelativePath := "/some/compile/dir"
	expectedWsJobError := "this is some mocked error"
	tests := []struct {
		name                    string
		namespace               string
		req                     *pb.CompileJobRequest
		expectWsjobMode         wsclient.WSJobMode
		expectErrContains       string
		serviceSpecExpectations []ServiceSpecExpectation
		jobSpecExpectations     []JobSpecExpectation
	}{
		{
			name:      "Positive Test - Compile Mode - With Debug Args",
			namespace: common.SystemNamespace,
			req: &pb.CompileJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{
						TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
							pkg.DefaultTaskSpecKey: {
								ContainerImage:   "cerebras/wsappliance:latest",
								ContainerCommand: []string{"appliance_run"},
							},
						},
					},
					Ini: &commonpb.DebugArgs_DebugINI{
						Strings: map[string]string{
							"compile_fabric_type":                 "cs3_std",
							"ws_rt_use_existing_mock_cs2_ip_port": "x.x.x.x",
						},
						Bools: map[string]bool{
							"fetch_fabric_for_compile": true,
							"ws_rt_using_mock_cs2":     true,
						},
					},
				},
				CompileDirRelativePath: compileDirRelativePath,
				DebugArgsJson:          `{"override": true}`,
			},
			expectWsjobMode: wsclient.CompileJobMode,
			serviceSpecExpectations: []ServiceSpecExpectation{
				expectServiceSpecEnvvar(wsapisv1.DebugArgsEnv, `{"override": true}`),
				expectServiceSpecEnvvar(wsapisv1.CompileRootDirEnv, "/n1/wsjob/compile_root"),
				expectServiceSpecEnvvar(wsapisv1.RelativeCompileDirEnv, "some/compile/dir"),
				expectServiceSpecEnvvar(wsapisv1.CompileSystemTypeEnv, "cs2"),
				expectServiceSpecEnvvar(wsapisv1.CompileFabricTypeEnv, "cs3_std"),
				expectServiceSpecEnvvar(wsapisv1.MaxTrainFabricFreqMhzEnv, "900"),
			},
			jobSpecExpectations: []JobSpecExpectation{
				expectJobSpecAnnotations(map[string]string{
					wsapisv1.MockCsAddressAnnot: wsapisv1.CompileOnlyCmAddr,
				}),
				expectNumWafers(1),
			},
		},
		{
			name:      "Positive Test - Compile Mode - With Overridden Workdir Host Path",
			namespace: common.SystemNamespace,
			req: &pb.CompileJobRequest{
				JobMode:                pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs:              &commonpb.DebugArgs{},
				CompileDirRelativePath: compileDirRelativePath,
			},
			expectWsjobMode: wsclient.CompileJobMode,
			jobSpecExpectations: []JobSpecExpectation{
				expectNumWafers(0),
			},
		},
		{
			name:      "Positive Test - Compile Mode - With Shorthanded Compile Dir",
			namespace: common.SystemNamespace,
			req: &pb.CompileJobRequest{
				JobMode:                pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs:              &commonpb.DebugArgs{},
				CompileDirRelativePath: compileDirRelativePath[1:],
			},
			expectWsjobMode: wsclient.CompileJobMode,
		},
		{
			name:      "Positive Test - Compile Mode - User namespace",
			namespace: "user-namespace",
			req: &pb.CompileJobRequest{
				JobMode:                pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs:              &commonpb.DebugArgs{},
				CompileDirRelativePath: compileDirRelativePath,
			},
			expectWsjobMode: wsclient.CompileJobMode,
		},
		{
			name:      "Positive Test - Inference Compile Mode",
			namespace: "user-namespace",
			req: &pb.CompileJobRequest{
				JobMode:                pkg.MockJobMode(commonpb.JobMode_INFERENCE_COMPILE, false),
				DebugArgs:              &commonpb.DebugArgs{},
				CompileDirRelativePath: compileDirRelativePath,
			},
			expectWsjobMode: wsclient.InferenceCompileJobMode,
		},
		{
			name:      "Positive Test - Compile Mode - With Valid Labels",
			namespace: common.SystemNamespace,
			req: &pb.CompileJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{
						Labels: map[string]string{
							"foo": "bar",
							"baz": "raz",
						},
					},
				},
				CompileDirRelativePath: compileDirRelativePath,
			},
			expectWsjobMode: wsclient.CompileJobMode,
		},
		{
			name:      "Negative Test - Compile - With Invalid Labels",
			namespace: common.SystemNamespace,
			req: &pb.CompileJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{Labels: map[string]string{"foo!": "invalid-key"}},
				},
				CompileDirRelativePath: compileDirRelativePath,
			},
			expectWsjobMode:   wsclient.CompileJobMode,
			expectErrContains: "invalid label",
		},
		{
			name:      "Negative Test - Compile Mode - Unsupported Overridden Workdir Host Path",
			namespace: common.SystemNamespace,
			req: &pb.CompileJobRequest{
				JobMode:                pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs:              &commonpb.DebugArgs{},
				CompileDirRelativePath: compileDirRelativePath,
			},
			expectWsjobMode:   wsclient.CompileJobMode,
			expectErrContains: "not one of the supported",
		},
		{
			name:      "Negative Test - Broken WS Job Client",
			namespace: common.SystemNamespace,
			req: &pb.CompileJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
			},
			expectWsjobMode:   wsclient.CompileJobMode,
			expectErrContains: expectedWsJobError,
		},
	}
	ctx, cleanup, _, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			common.Namespace = test.namespace
			defer func() {
				common.Namespace = common.SystemNamespace
			}()

			mockResponse := makeWsjobResponse()
			var capturedWSJob *wsapisv1.WSJob

			mockedWsJobClient.MockedCreate = func(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
				capturedWSJob = wsjob
				if test.expectErrContains == "" {
					mockResponse.Labels = wsjob.Labels
					mockResponse.Annotations = wsjob.Annotations
					return mockResponse, nil
				}
				return &wsapisv1.WSJob{}, fmt.Errorf("create error: %s", test.expectErrContains)
			}

			setOperatorVersion(t, fakeKubeClient, defaultOperatorVersion)
			jobRes, err := client.InitCompileJob(ctx, test.req)

			if test.expectErrContains == "" {
				require.NoError(t, err)
				require.NotNil(t, capturedWSJob, "WSJob should not be nil")
				assert.Equal(t, mockResponse.Name, jobRes.GetJobId())
				assert.Equal(t, path.Join(wsclient.CachedCompileRootPath, test.namespace, compileDirRelativePath),
					jobRes.GetCompileDirAbsolutePath())

				js := extractJobSpecFromWSJob(capturedWSJob)
				for _, jobSpecExpectation := range test.jobSpecExpectations {
					jobSpecExpectation(t, js)
				}
				ss := extractServiceSpecsFromWSJob(capturedWSJob)
				for _, svcSpecExpectation := range test.serviceSpecExpectations {
					svcSpecExpectation(t, ss)
				}
			} else {
				message := status.Convert(err).Message()
				assert.Contains(t, message, test.expectErrContains)
			}
		})
	}
}

func TestCreateExecuteJob(t *testing.T) {
	compileDirRelativePath := "/some/compile/dir"
	compileDirAbsolutePath := path.Join(wsclient.CachedCompileRootPath, common.DefaultNamespace, compileDirRelativePath)
	compileArtifactDir := fmt.Sprintf("%s/cs_foobar", compileDirAbsolutePath)
	expectedWsJobError := "this is some mocked error"
	expectedUnspecifiedJobModeError := "Unspecified job mode"
	tests := []struct {
		name                    string
		operatorSemVer          string
		executeReq              *pb.ExecuteJobRequest
		expectWsjobMode         wsclient.WSJobMode
		expectCbcore            string
		expectEntrypoint        []string
		expectErrContains       string
		serviceSpecExpectations []ServiceSpecExpectation
		actToCsxDomains         [][]int
	}{
		{
			name:           "Positive Test - Execute Mode - With Full Task Spec Hint",
			operatorSemVer: defaultOperatorVersion,
			executeReq: &pb.ExecuteJobRequest{
				JobMode:     pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
				CbcoreImage: "cerebras/hellow-world:latest",
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{
						TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
							pkg.DefaultTaskSpecKey: {
								// prioritize image in debug args over in the request body
								ContainerImage:   "cerebras/wsappliance:latest",
								ContainerCommand: []string{"appliance_run"},
							},
						},
					},
				},
				CompileArtifactDir: compileArtifactDir,
				DebugArgsJson:      `{"override": true}`,
			},
			expectWsjobMode:  wsclient.ExecuteJobMode,
			expectCbcore:     "cerebras/wsappliance:latest",
			expectEntrypoint: []string{"appliance_run"},
			serviceSpecExpectations: []ServiceSpecExpectation{
				expectServiceSpecEnvvar(wsapisv1.DebugArgsEnv, `{"override": true}`)},
		},
		{
			name:           "Positive Test - Execute Mode - With Partial Task Spec Hint",
			operatorSemVer: defaultOperatorVersion,
			executeReq: &pb.ExecuteJobRequest{
				JobMode:     pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
				CbcoreImage: "cerebras/hellow-world:latest",
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{
						TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
							pkg.DefaultTaskSpecKey: {
								// prioritize image in debug args over in the request body
								ContainerImage: "cerebras/wsappliance:latest",
							},
						},
					},
				},
				CompileArtifactDir: compileArtifactDir,
				DebugArgsJson:      `{"override": true}`,
			},
			expectWsjobMode:  wsclient.ExecuteJobMode,
			expectCbcore:     "cerebras/wsappliance:latest",
			expectEntrypoint: mockContainerCommand,
			serviceSpecExpectations: []ServiceSpecExpectation{
				expectServiceSpecEnvvar(wsapisv1.DebugArgsEnv, `{"override": true}`)},
		},
		{
			name: "Positive Test - Execute Mode - Cbcore in Request",
			executeReq: &pb.ExecuteJobRequest{
				JobMode:            pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
				CompileArtifactDir: compileArtifactDir,
				CbcoreImage:        "cerebras/hellow-world:latest",
			},
			expectWsjobMode:  wsclient.ExecuteJobMode,
			expectCbcore:     "cerebras/hellow-world:latest",
			expectEntrypoint: mockContainerCommand,
		},
		{
			name: "Positive Test - Execute Mode - Without Task Specs",
			executeReq: &pb.ExecuteJobRequest{
				JobMode:            pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
				CompileArtifactDir: compileArtifactDir,
			},
			expectWsjobMode:  wsclient.ExecuteJobMode,
			expectCbcore:     mockContainerImage,
			expectEntrypoint: mockContainerCommand,
		},
		{
			name: "Positive Test - Inference Execute Mode",
			executeReq: &pb.ExecuteJobRequest{
				JobMode:            pkg.MockJobMode(commonpb.JobMode_INFERENCE_EXECUTE, false),
				CompileArtifactDir: compileArtifactDir,
			},
			expectWsjobMode:  wsclient.InferenceExecuteJobMode,
			expectCbcore:     mockContainerImage,
			expectEntrypoint: mockContainerCommand,
		},
		{
			name: "Positive Test - Execute Mode - With Valid Labels",
			executeReq: &pb.ExecuteJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{Labels: map[string]string{
						"foo": "bar",
						"baz": "raz",
					}},
				},
				CompileArtifactDir: compileArtifactDir,
				DebugArgsJson:      `{"labels": {"foo": "bar", "baz": "raz"}}`,
			},
			expectWsjobMode:  wsclient.ExecuteJobMode,
			expectCbcore:     mockContainerImage,
			expectEntrypoint: mockContainerCommand,
			serviceSpecExpectations: []ServiceSpecExpectation{
				expectServiceSpecEnvvar(wsapisv1.DebugArgsEnv, `{"labels": {"foo": "bar", "baz": "raz"}}`)},
		},
		{
			name: "Positive Test - Execute Mode - With wio allocation",
			executeReq: &pb.ExecuteJobRequest{
				JobMode:            pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
				CompileArtifactDir: compileArtifactDir,
			},
			actToCsxDomains:  [][]int{{2}, {3}},
			expectWsjobMode:  wsclient.ExecuteJobMode,
			expectCbcore:     mockContainerImage,
			expectEntrypoint: mockContainerCommand,
		},
		{
			name: "Positive Test - Execute Mode - Operator supports feature",
			executeReq: &pb.ExecuteJobRequest{
				JobMode:            pkg.MockJobMode(commonpb.JobMode_EXECUTE, true),
				CompileArtifactDir: compileArtifactDir,
			},
			actToCsxDomains:  [][]int{{2}, {3}},
			expectWsjobMode:  wsclient.ExecuteJobMode,
			expectCbcore:     mockContainerImage,
			expectEntrypoint: mockContainerCommand,
		},
		{
			name:           "Negative Test - Execute Mode - Operator does not support feature",
			operatorSemVer: "1.0.4",
			executeReq: &pb.ExecuteJobRequest{
				JobMode:            pkg.MockJobMode(commonpb.JobMode_EXECUTE, true),
				CompileArtifactDir: compileArtifactDir,
			},
			actToCsxDomains:   [][]int{{2}, {3}},
			expectWsjobMode:   wsclient.ExecuteJobMode,
			expectCbcore:      mockContainerImage,
			expectEntrypoint:  mockContainerCommand,
			expectErrContains: "operator does not support parallel experts",
		},
		{
			name:           "Negative Test - Unspecified Job Mode",
			operatorSemVer: defaultOperatorVersion,
			executeReq: &pb.ExecuteJobRequest{
				JobMode:            pkg.MockJobMode(commonpb.JobMode_EXECUTE, true),
				CompileArtifactDir: compileArtifactDir,
			},
			expectWsjobMode:   wsclient.ExecuteJobMode,
			expectCbcore:      mockContainerImage,
			expectEntrypoint:  mockContainerCommand,
			expectErrContains: expectedUnspecifiedJobModeError,
		},
		{
			name: "Negative Test - Broken WS Job Client",
			executeReq: &pb.ExecuteJobRequest{
				JobMode:            pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
				CompileArtifactDir: compileArtifactDir,
			},
			expectWsjobMode:   wsclient.ExecuteJobMode,
			expectCbcore:      mockContainerImage,
			expectEntrypoint:  mockContainerCommand,
			expectErrContains: expectedWsJobError,
		},
		{
			name: "Negative Test - Execute - With Invalid Labels",
			executeReq: &pb.ExecuteJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{Labels: map[string]string{"foo!": "invalid-key"}},
				},
				CompileArtifactDir: compileArtifactDir,
			},
			expectWsjobMode:   wsclient.ExecuteJobMode,
			expectCbcore:      mockContainerImage,
			expectEntrypoint:  mockContainerCommand,
			expectErrContains: "invalid label",
		},
	}

	ctx, cleanup, _, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockResponse := makeWsjobResponse()
			var capturedWSJob *wsapisv1.WSJob

			mockedWsJobClient.MockedCreate = func(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
				capturedWSJob = wsjob
				if wsjob.Annotations != nil {
					mockResponse.Annotations = common.EnsureMap(mockResponse.Annotations)
					mockResponse.Annotations[wsapisv1.SemanticJobOperatorVersion] = wsjob.Annotations[wsapisv1.SemanticJobOperatorVersion]
				}

				if test.expectErrContains == "" {
					return mockResponse, nil
				}
				return nil, fmt.Errorf("create error: %s", test.expectErrContains)
			}
			if test.operatorSemVer != "" {
				setOperatorVersion(t, fakeKubeClient, test.operatorSemVer)
			}
			jobRes, err := client.InitExecuteJob(ctx, test.executeReq)

			if test.expectErrContains == "" {
				assert.NoError(t, err)
				assert.NotNil(t, capturedWSJob, "WSJob should not be nil")
				assert.Equal(t, mockResponse.Name, jobRes.GetJobId())

				js := extractJobSpecFromWSJob(capturedWSJob)
				ss := extractServiceSpecsFromWSJob(capturedWSJob)

				assertJobPositiveResponse(t, jobRes.GetDebugArgs(), &js)
				assert.Equal(t, test.expectWsjobMode, js.WsJobMode)
				if len(test.actToCsxDomains) > 0 {
					assert.Equal(t, [][]int{{2, 3}, {1, 4}}, js.ActToCsxDomains)
				}
				if int(wsclient.WsCoordinator) < len(ss) {
					coordinatorSpec := ss[int(wsclient.WsCoordinator)]
					assert.Equal(t, test.expectCbcore, coordinatorSpec.Image)
					assert.Equal(t, test.expectEntrypoint, coordinatorSpec.Command)
				}
				for _, svcSpecExpectation := range test.serviceSpecExpectations {
					svcSpecExpectation(t, ss)
				}
			} else {
				assert.Error(t, err)
				message := status.Convert(err).Message()
				assert.Contains(t, message, test.expectErrContains)
			}
		})
	}
}

func TestDenyUserNamespaceCreateJobInSystemNamespace(t *testing.T) {
	ctx, cleanup, _, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	// set server to system namespace, client to some user namespace
	defer func(originalNs string) {
		clientIngressNamespace = ""
		common.Namespace = originalNs
	}(common.Namespace)
	clientIngressNamespace = "user-session-0"
	common.Namespace = common.SystemNamespace

	errContains := "User attempted to launch or modify a job in the system namespace"
	_, err := client.InitCompileJob(ctx, &pb.CompileJobRequest{})
	require.ErrorContains(t, err, errContains)

	_, err = client.InitExecuteJob(ctx, &pb.ExecuteJobRequest{})
	require.ErrorContains(t, err, errContains)

	_, err = client.CancelJobV2(ctx, &pb.CancelJobRequest{})
	require.ErrorContains(t, err, errContains)

	_, err = client.DeleteJob(ctx, &pb.DeleteJobRequest{})
	require.ErrorContains(t, err, errContains)
}

func TestCreateJobWithRoot(t *testing.T) {
	tests := []struct {
		name                string
		executeReq          *pb.ExecuteJobRequest
		compileReq          *pb.CompileJobRequest
		wsJobClientResponse *wsapisv1.WSJob
		expectErrContains   string
	}{
		{
			name: "Negative Test - Compile - With root user",
			compileReq: &pb.CompileJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
			},
			expectErrContains: "is not allowed to execute this method",
		},
		{
			name: "Negative Test - Execute - With root user",
			executeReq: &pb.ExecuteJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_EXECUTE, false),
			},
			expectErrContains: "is not allowed to execute this method",
		},
	}

	ctx, cleanup, _, client := setup(true, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockResponse := makeWsjobResponse()
			mockedWsJobClient.MockedCreate = func(wsjob *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
				if test.expectErrContains == "" {
					return mockResponse, nil
				}
				return nil, fmt.Errorf("create error: %s", test.expectErrContains)
			}

			var jobRes interface {
				GetJobId() string
			}
			var err error
			if test.executeReq != nil {
				jobRes, err = client.InitExecuteJob(ctx, test.executeReq)
			} else {
				jobRes, err = client.InitCompileJob(ctx, test.compileReq)
			}

			if test.expectErrContains == "" {
				assert.Nil(t, err)
				assert.Equal(t, test.wsJobClientResponse.Name, jobRes.GetJobId())
			} else {
				message := status.Convert(err).Message()
				assert.Contains(t, message, test.expectErrContains)
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	expectedWsJobError := errors.New("this is some mocked error")
	tests := []struct {
		name                string
		shouldPass          bool
		wsJobClientResponse *wsapisv1.WSJob
		wsJobClientError    error
		expectedJobStatus   pb.JobStatus
		expectedJobMessage  string
	}{
		{
			"Positive Test - Job Created",
			true,
			&wsapisv1.WSJob{
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:    "Created",
							Status:  "True",
							Reason:  "WSJobCreated",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is created.",
						},
					},
				},
			},
			nil,
			pb.JobStatus_JOB_STATUS_CREATED,
			"WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is created.",
		},
		{
			"Positive Test - Job Running",
			true,
			&wsapisv1.WSJob{
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:    "Created",
							Status:  "True",
							Reason:  "WSJobCreated",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is created.",
						},
						{
							Type:    "Running",
							Status:  "False",
							Reason:  "WSJobRunning",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is running.",
						},
					},
				},
			},
			nil,
			pb.JobStatus_JOB_STATUS_IN_PROGRESS,
			"WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is running.",
		},
		{
			"Positive Test - Job Completed",
			true,
			&wsapisv1.WSJob{
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:    "Created",
							Status:  "True",
							Reason:  "WSJobCreated",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is created.",
						},
						{
							Type:    "Running",
							Status:  "False",
							Reason:  "WSJobRunning",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is running.",
						},
						{
							Type:    "Succeeded",
							Status:  "True",
							Reason:  "WSJobSucceeded",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d successfully completed.",
						},
					},
				},
			},
			nil,
			pb.JobStatus_JOB_STATUS_SUCCEEDED,
			"WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d successfully completed.",
		},
		{
			"Positive Test - Job Failed",
			true,
			&wsapisv1.WSJob{
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:    "Created",
							Status:  "True",
							Reason:  "WSJobCreated",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is created.",
						},
						{
							Type:    "Running",
							Status:  "False",
							Reason:  "WSJobRunning",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d is running.",
						},
						{
							Type:    "Failed",
							Status:  "True",
							Reason:  "WSJobFailed",
							Message: "WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d failed.",
						},
					},
				},
			},
			nil,
			pb.JobStatus_JOB_STATUS_FAILED,
			"WSJob wsjob-f599c748-61b2-4227-8ad2-e840661c705d failed.",
		},
		{
			"Negative Test",
			false,
			nil,
			expectedWsJobError,
			pb.JobStatus_JOB_STATUS_UNSPECIFIED,
			"",
		},
	}

	ctx, cleanup, _, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := &pb.GetJobRequest{JobId: "mock job ID"}
			mockedWsJobClient.MockedGet = func(namespace, jobName string) (*wsapisv1.WSJob, error) {
				return test.wsJobClientResponse, test.wsJobClientError
			}
			response, err := client.GetJob(ctx, req)

			if test.shouldPass {
				assert.Equal(t, test.expectedJobStatus, response.Status)
				assert.Equal(t, test.expectedJobMessage, response.Message)
			} else {
				message := status.Convert(err).Message()
				assert.Equal(t, message, test.wsJobClientError.Error())
			}
		})
	}
}

func TestGetJobEvents(t *testing.T) {
	ctx, cleanup, clusterServer, client := setup(false, fakeKubeClient, mockedWsJobClient, mockedPromClient, false)
	defer cleanup()

	fakeK8sClient := fake.NewSimpleClientset()
	k8Event := &corev1.Event{
		InvolvedObject: corev1.ObjectReference{
			Kind:      "WSJOB",
			Name:      "wsjob-1234",
			Namespace: common.DefaultNamespace,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wsjob-1234",
			Namespace: common.DefaultNamespace,
		},
		Reason:        common.DegradedSchedulingJobEventReason,
		Message:       "Degraded scheduling details",
		LastTimestamp: metav1.NewTime(time.Now().Add(time.Duration(1) * time.Hour)),
	}
	_, err := fakeK8sClient.CoreV1().Events(common.DefaultNamespace).
		Create(ctx, k8Event, metav1.CreateOptions{})
	assert.NoError(t, err)

	clusterServer.kubeClientSet = fakeK8sClient

	mockedWsJobClient.MockedGet = func(namespace, jobName string) (*wsapisv1.WSJob, error) {
		return &wsapisv1.WSJob{ObjectMeta: metav1.ObjectMeta{Name: "wsjob-1234"}}, nil
	}

	response, err := client.GetJobEvents(ctx, &pb.GetJobEventsRequest{JobId: "wsjob-1234"})
	assert.NoError(t, err)

	assert.Equal(t, 1, len(response.JobEvents))
	je := response.JobEvents[0]
	assert.Equal(t, k8Event.Name, je.Name)
	assert.Equal(t, k8Event.Reason, je.Reason)
	assert.Equal(t, k8Event.Message, je.Message)
}

func TestDeleteJob(t *testing.T) {
	expectedWsJobError := errors.New("this is some mocked error")
	tests := []struct {
		name             string
		shouldPass       bool
		wsJobClientError error
	}{
		{
			"Positive Test - Job Deleted",
			true,
			nil,
		},
		{
			"Negative Test - Job Failed to Be Deleted",
			false,
			expectedWsJobError,
		},
	}

	ctx, cleanup, _, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	user, err := user.Current()
	assert.Nil(t, err)
	uid, err := strconv.ParseInt(user.Uid, 10, 64)
	assert.NoError(t, err)
	gid, err := strconv.ParseInt(user.Gid, 10, 64)
	assert.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := &pb.DeleteJobRequest{JobId: "mock job ID"}
			mockedWsJobClient.MockedGet = func(namespace, jobName string) (*wsapisv1.WSJob, error) {
				return &wsapisv1.WSJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
						Name:      jobName,
					},
					Spec: wsapisv1.WSJobSpec{
						User: &wsapisv1.JobUser{
							Uid: uid,
							Gid: gid,
						},
					},
				}, nil
			}
			mockedWsJobClient.MockedDelete = func(namespace, jobName string) error {
				return test.wsJobClientError
			}
			_, err := client.DeleteJob(ctx, req)

			if test.shouldPass {
				assert.Nil(t, err)
			} else {
				message := status.Convert(err).Message()
				assert.Equal(t, test.wsJobClientError.Error(), message)
			}
		})
	}
}

func TestLeaseRenewal(t *testing.T) {
	ctx, cleanup, clusterServer, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	tests := []struct {
		name                         string
		leaseDurationSecondsOverride int32
		leaseRenewalSucceeds         bool
	}{
		{
			name:                         "happy path",
			leaseDurationSecondsOverride: 0,
			leaseRenewalSucceeds:         true,
		},
		{
			name:                         "happy path with lease duration override",
			leaseDurationSecondsOverride: 5,
			leaseRenewalSucceeds:         true,
		},
		{
			name:                         "failed-to-renew-lease",
			leaseDurationSecondsOverride: 0,
			leaseRenewalSucceeds:         false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeK8sClient := fake.NewSimpleClientset()

			if !test.leaseRenewalSucceeds {
				reaction := func(action k8testing.Action) (handled bool, ret runtime.Object, err error) {
					conflictError := k8serrors.NewConflict(coordv1.Resource("leases"), "", fmt.Errorf("conflict"))
					return true, nil, conflictError
				}
				fakeK8sClient.PrependReactor("update", "leases", reaction)
			}

			leaseCreationTime := metav1.MicroTime{Time: time.Now()}
			leaseDuration := int32(60)
			lease := &coordv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: common.DefaultNamespace,
				},
				Spec: coordv1.LeaseSpec{
					LeaseDurationSeconds: &leaseDuration,
					RenewTime:            &leaseCreationTime,
				},
			}
			_, err := fakeK8sClient.CoordinationV1().Leases(common.DefaultNamespace).
				Create(context.Background(), lease, metav1.CreateOptions{})
			assert.Nil(t, err)

			clusterServer.kubeClientSet = fakeK8sClient
			_, err = client.SendClientHeartbeat(context.TODO(), &pb.HeartbeatRequest{
				JobId:                        "test",
				LeaseDurationSecondsOverride: test.leaseDurationSecondsOverride,
			})

			leases, _ := fakeK8sClient.CoordinationV1().Leases(common.DefaultNamespace).List(ctx, metav1.ListOptions{})
			if test.leaseRenewalSucceeds {
				assert.Equal(t, 1, len(leases.Items))
				renewTime := leases.Items[0].Spec.RenewTime
				assert.True(t, renewTime.After(leaseCreationTime.Time))
				if test.leaseDurationSecondsOverride != 0 {
					assert.Equal(t, test.leaseDurationSecondsOverride, *leases.Items[0].Spec.LeaseDurationSeconds)
				}
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func TestCancelJobV2(t *testing.T) {
	_, cleanup, clusterServer, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	testsV2 := []struct {
		name                  string
		cancelLeaseSucceeds   bool
		currentJobCondition   commonv1.JobConditionType
		requestJobStatus      pb.JobStatus
		expectLeaseAnnotation string
	}{
		{
			name:                  "success-happy-case",
			cancelLeaseSucceeds:   true,
			currentJobCondition:   commonv1.JobRunning,
			requestJobStatus:      pb.JobStatus_JOB_STATUS_SUCCEEDED,
			expectLeaseAnnotation: wsapisv1.CancelWithStatusSucceeded,
		},
		{
			name:                "skip-ended-job",
			cancelLeaseSucceeds: true,
			currentJobCondition: commonv1.JobFailed,
			requestJobStatus:    pb.JobStatus_JOB_STATUS_CANCELLED,
			// no annotation updated if job already ended
			expectLeaseAnnotation: "",
		},
		{
			name:                "fail-to-cancel-lease",
			cancelLeaseSucceeds: false,
			currentJobCondition: commonv1.JobRunning,
			requestJobStatus:    pb.JobStatus_JOB_STATUS_CANCELLED,
			// no annotation updated if failed to get/update lease
			expectLeaseAnnotation: "",
		},
	}

	for _, test := range testsV2 {
		t.Run(test.name, func(t *testing.T) {
			var testWsjob = &wsapisv1.WSJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test",
					Namespace:   common.DefaultNamespace,
					Annotations: make(map[string]string),
				},
				Status: commonv1.JobStatus{
					Conditions: []commonv1.JobCondition{
						{
							Type:   test.currentJobCondition,
							Status: "True",
						},
					},
				},
			}
			mockedWsJobClient.MockedGet = func(namespace, jobName string) (*wsapisv1.WSJob, error) {
				return testWsjob, nil
			}
			fakeK8sClient := fake.NewSimpleClientset()

			mockedWsJobClient.MockedCancel = func(namespace, jobName string, annotations map[string]string) (*wsapisv1.WSJob, error) {
				testWsjob.Annotations[wsapisv1.CancelWithStatusKey] = test.expectLeaseAnnotation
				return nil, nil
			}
			if !test.cancelLeaseSucceeds {
				mockedWsJobClient.MockedCancel = func(namespace, jobName string, annotations map[string]string) (*wsapisv1.WSJob, error) {
					conflictError := k8serrors.NewConflict(coordv1.Resource("leases"), "", fmt.Errorf("conflict"))
					return nil, conflictError
				}
			}
			if test.currentJobCondition == commonv1.JobRunning {
				leaseDuration := int32(60)
				lease := &coordv1.Lease{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test",
						Namespace: common.DefaultNamespace,
						Labels: map[string]string{
							"job-name": "test",
						},
					},
					Spec: coordv1.LeaseSpec{
						LeaseDurationSeconds: &leaseDuration,
						RenewTime:            &metav1.MicroTime{Time: time.Now()},
					},
				}
				_, err := fakeK8sClient.CoordinationV1().Leases(common.DefaultNamespace).
					Create(context.Background(), lease, metav1.CreateOptions{})
				assert.Nil(t, err)
			}

			clusterServer.kubeClientSet = fakeK8sClient
			_, err := client.CancelJobV2(context.TODO(), &pb.CancelJobRequest{
				JobId:     "test",
				JobStatus: test.requestJobStatus,
			})

			if test.currentJobCondition == commonv1.JobRunning {
				if test.cancelLeaseSucceeds {
					assert.Nil(t, err)
					assert.Equal(t, test.expectLeaseAnnotation, testWsjob.Annotations[wsapisv1.CancelWithStatusKey])
				} else {
					assert.NotNil(t, err)
					_, ok := testWsjob.Annotations[wsapisv1.CancelWithStatusKey]
					assert.False(t, ok)
				}
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestGetJobMeta(t *testing.T) {
	ctx, cleanup, clusterServer, client := setup(false, fakeKubeClient, mockedWsJobClient, mockedPromClient, false)
	defer cleanup()

	clusterDetailsData, err := os.ReadFile("../csctl/testdata/single_box_cluster_details_mock.json")
	require.NoError(t, err)

	cdConfig := &commonpb.ClusterDetails{}
	options := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}
	err = options.Unmarshal(clusterDetailsData, cdConfig)
	require.NoError(t, err)
	compressed, err := common.Gzip(clusterDetailsData)
	require.NoError(t, err)

	cbcoreImageTag := "0.0.0-202306032329-3964-216a12dc"

	mockClusterDetailsCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, "wsjob-foobar"),
			Namespace: common.DefaultNamespace,
		},
		Data: map[string]string{
			wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.UpdatedSignalVal,
		},
		BinaryData: map[string][]byte{
			wsapisv1.ClusterDetailsConfigCompressedFileName: compressed,
		},
	}

	mockJob := &wsapisv1.WSJob{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				wsapisv1.JobModeLabelKey: wsapisv1.JobModeWeightStreaming,
				wsapisv1.JobTypeLabelKey: wsapisv1.JobTypeExecute,
			},
		},
		Spec: wsapisv1.WSJobSpec{
			WSReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
				wsapisv1.WSReplicaTypeCoordinator: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Image: fmt.Sprintf("registry.local/cbcore:%s", cbcoreImageTag),
								},
							},
						},
					},
				},
			},
		},
	}

	systemSoftwareFile, err := os.ReadFile("../csctl/testdata/system_software.json")
	require.NoError(t, err)
	mockPromResult := model.Vector{
		&model.Sample{
			Value: model.SampleValue(1),
			Metric: map[model.LabelName]model.LabelValue{
				model.InstanceLabel: "192.168.131.11:8003",
				"software":          model.LabelValue(systemSoftwareFile),
				"system":            "systemf47",
			},
		},
	}
	type Product struct {
		Name    string `json:"name"`
		Version string `json:"version"`
		BuildId string `json:"buildid"`
	}
	type SystemSoftware struct {
		Product Product `json:"product"`
	}

	tests := []struct {
		name          string
		promAvailable bool
		shouldFail    bool
	}{
		{
			"happy-case",
			true,
			false,
		},
		{
			"prometheus-not-available",
			false,
			false,
		},
		{
			"cluster-details-deleted",
			true,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockedWsJobClient.MockedGet = func(namespace, jobName string) (*wsapisv1.WSJob, error) {
				return mockJob, nil
			}
			if test.promAvailable {
				mockedPromClient.MockedGet = func() (model.Value, error) {
					return mockPromResult, nil
				}
			} else {
				mockedPromClient.MockedGet = func() (model.Value, error) {
					return nil, fmt.Errorf("not available")
				}
			}
			fakeK8sClient := fake.NewSimpleClientset()
			if !test.shouldFail {
				_, err := fakeK8sClient.CoreV1().ConfigMaps(common.DefaultNamespace).
					Create(ctx, mockClusterDetailsCM, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			clusterServer.kubeClientSet = fakeK8sClient

			res, err := client.GetJobMeta(ctx, &pb.GetJobMetaRequest{
				JobId: "wsjob-foobar",
			})
			if test.shouldFail {
				require.NotNil(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, commonpb.JobMode_EXECUTE, res.JobMode)
				assert.Equal(t, cbcoreImageTag, res.SoftwareVersions["cbcore"])
				assert.Equal(t, 1, len(res.SystemVersions))
				_, ok := res.SystemVersions["systemf47"]
				assert.True(t, ok)
				var ss SystemSoftware
				err = json.Unmarshal([]byte(res.SystemVersions["systemf47"]), &ss)
				require.NoError(t, err)
				if test.promAvailable {
					assert.Equal(t, ss.Product.Version, "1.9.0")
					assert.Equal(t, ss.Product.BuildId, "202306021618-5-58e0545b")
				} else {
					assert.Equal(t, ss.Product.Version, "")
					assert.Equal(t, ss.Product.BuildId, "")
				}
			}
		})
	}
}

func TestDropCaches(t *testing.T) {
	tests := []struct {
		name            string
		dropCachesValue commonpb.DebugArgs_DebugMGR_DropCachesValue
		expectEchoValue commonpb.DebugArgs_DebugMGR_DropCachesValue
	}{
		{
			name:            "With DropCachesValue PageCaches",
			dropCachesValue: commonpb.DebugArgs_DebugMGR_DROPCACHES_PAGECACHES,
			expectEchoValue: commonpb.DebugArgs_DebugMGR_DROPCACHES_PAGECACHES,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
			specs[wsclient.WsBroadcastReduce] = wsclient.ServiceSpec{
				Replicas: 1,
				Image:    "dummy-image",
			}

			jobSpec := wsclient.JobSpec{
				DropCachesValue: tc.dropCachesValue,
				WsJobMode:       wsclient.ExecuteJobMode,
				JobUser:         wsapisv1.JobUser{Uid: 1000, Gid: 1000, Username: "testuser"},
			}

			wsjob, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
			require.NoError(t, err)

			brReplica, exists := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce]
			require.True(t, exists, "Expected BroadcastReduce replica but not found")
			foundDropCaches := false
			for _, c := range brReplica.Template.Spec.InitContainers {
				if c.Name == wsapisv1.DropCachesInitContainerName {
					foundDropCaches = true
					cmd := strings.Join(c.Command, " ")
					require.Contains(t, cmd, fmt.Sprintf("echo %d", tc.expectEchoValue))
				}
			}
			require.True(t, foundDropCaches, "Expected DropCaches init container not found")

		})
	}
}

func TestCbcoreOverrideScript(t *testing.T) {
	debugVolumeName := "debug-volume"
	tests := []struct {
		name             string
		volume           *corev1.Volume
		volumeMount      *corev1.VolumeMount
		expectVolumeName string
	}{
		{
			name: "With Cbcore Override Volume",
			volume: &corev1.Volume{
				Name: debugVolumeName,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/cb/debug",
					},
				},
			},
			volumeMount: &corev1.VolumeMount{
				Name:      debugVolumeName,
				MountPath: "/cb/debug",
			},
			expectVolumeName: debugVolumeName,
		},
		{
			name:             "Without Cbcore Override Volume",
			volume:           nil,
			volumeMount:      nil,
			expectVolumeName: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
			specs[wsclient.WsCoordinator] = wsclient.ServiceSpec{
				Replicas: 1,
				Image:    "dummy-image",
			}

			jobSpec := wsclient.JobSpec{
				WsJobMode:                 wsclient.ExecuteJobMode,
				JobUser:                   wsapisv1.JobUser{Uid: 1000, Gid: 1000, Username: "testuser"},
				CbcoreOverrideVolume:      tc.volume,
				CbcoreOverrideVolumeMount: tc.volumeMount,
			}
			wsjob, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
			require.NoError(t, err)

			// Inspect volumes and volume mounts anywhere inside WSReplicaSpecs
			foundVolume := false
			foundVolumeMount := false
			for _, r := range wsjob.Spec.WSReplicaSpecs {
				// Check volumes
				for _, vol := range r.Template.Spec.Volumes {
					if vol.Name == tc.expectVolumeName {
						foundVolume = true
						break
					}
				}
				// Check mounts in all containers
				for _, c := range r.Template.Spec.Containers {
					for _, vm := range c.VolumeMounts {
						if vm.Name == tc.expectVolumeName {
							foundVolumeMount = true
							break
						}
					}
					if foundVolumeMount {
						break
					}
				}
				if foundVolume && foundVolumeMount {
					break
				}
			}
			if tc.expectVolumeName == "" {
				assert.False(t, foundVolume, "Expected no volume '%s'", tc.expectVolumeName)
				assert.False(t, foundVolumeMount, "Expected no volume mount '%s'", tc.expectVolumeName)
			} else {
				assert.True(t, foundVolume, "Expected volume '%s' not found", tc.expectVolumeName)
				assert.True(t, foundVolumeMount, "Expected volume mount '%s' not found", tc.expectVolumeName)
			}
		})
	}
}

func TestGetJobOperatorSemanticVersion(t *testing.T) {
	ctx, cleanup, _, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	expectedJobOperatorSemanticVersion := "1.0.1"
	expectedClusterServerSemanticVersion := "1.1.0"
	oldClusterServerSemanticVersion := wsclient.SemanticVersion
	wsclient.SemanticVersion = expectedClusterServerSemanticVersion
	defer func() {
		wsclient.SemanticVersion = oldClusterServerSemanticVersion
	}()

	setOperatorVersion(t, fakeKubeClient, expectedJobOperatorSemanticVersion)

	config, err := client.GetServerConfig(ctx, &pb.GetServerConfigRequest{})
	assert.Nil(t, err)
	assert.Equal(t, expectedJobOperatorSemanticVersion, config.JobOperatorSemanticVersion)
	assert.Equal(t, expectedClusterServerSemanticVersion, config.ClusterServerSemanticVersion)
}

func expectServiceSpecEnvvar(envName, expectedValue string) ServiceSpecExpectation {
	return func(t *testing.T, spec wsclient.ServiceSpecs) {
		crdSpec := spec[wsclient.WsCoordinator]
		for _, e := range crdSpec.Env {
			if e.Name == envName {
				if e.Name == wsapisv1.DebugArgsEnv {
					assert.JSONEq(t, expectedValue, e.Value)
				} else {
					assert.Equal(t, expectedValue, e.Value)
				}
			}
		}
	}
}

func expectJobSpecAnnotations(annots map[string]string) JobSpecExpectation {
	return func(t *testing.T, jobSpec wsclient.JobSpec) {
		assert.Equal(t, annots, jobSpec.Annotations)
	}
}

func expectNumWafers(numWafers int) JobSpecExpectation {
	return func(t *testing.T, jobSpec wsclient.JobSpec) {
		assert.Equal(t, numWafers, jobSpec.NumWafers)
	}
}

func assertJobPositiveResponse(t *testing.T, debugArgs *commonpb.DebugArgs, passedSpec *wsclient.JobSpec) {
	if debugArgs != nil && debugArgs.GetDebugMgr() != nil {
		for k, v := range debugArgs.GetDebugMgr().Labels {
			if passedSpec.Labels[csctl.K8sLabelPrefix+k] != v {
				t.Errorf("missing expected for label %s=%s", k, v)
			}
		}
	}
	assert.Equal(t, passedSpec.JobUser.Uid, testUser.UID)
	assert.Equal(t, passedSpec.JobUser.Gid, testUser.GID)
	assert.Equal(t, passedSpec.JobUser.Username, testUser.Username)
}

func TestHasMultiMgmtNodes(t *testing.T) {
	tests := []struct {
		name              string
		hasMultiMgmtNodes bool
	}{
		{
			name:              "Has multiple management nodes",
			hasMultiMgmtNodes: true,
		},
		{
			name:              "Do not have multiple management nodes",
			hasMultiMgmtNodes: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cleanup, _, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, test.hasMultiMgmtNodes)
			defer cleanup()

			res, err := client.HasMultiMgmtNodes(ctx, &pb.HasMultiMgmtNodesRequest{})
			assert.Nil(t, err)
			assert.Equal(t, test.hasMultiMgmtNodes, res.GetHasMultiMgmtNodes())
		})
	}
}

func makeWsjobResponse() *wsapisv1.WSJob {
	return &wsapisv1.WSJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "wsjob-" + wsclient.GenUUID(),
		},
		Spec: wsapisv1.WSJobSpec{
			WSReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
				wsapisv1.WSReplicaTypeCoordinator: {
					Replicas: common.Pointer(int32(1)),
				},
				wsapisv1.WSReplicaTypeChief: {
					Replicas: common.Pointer(int32(1)),
				},
				wsapisv1.WSReplicaTypeActivation: {
					Replicas: common.Pointer(int32(1)),
				},
				wsapisv1.WSReplicaTypeWorker: {
					Replicas: common.Pointer(int32(1)),
				},
				wsapisv1.WSReplicaTypeWeight: {
					Replicas: common.Pointer(int32(1)),
				},
				wsapisv1.WSReplicaTypeCommand: {
					Replicas: common.Pointer(int32(1)),
				},
				wsapisv1.WSReplicaTypeKVStorageServer: {
					Replicas: common.Pointer(int32(1)),
				},
				wsapisv1.WSReplicaTypeSWDriver: {
					Replicas: common.Pointer(int32(1)),
				},
			},
		},
	}
}

func extractJobSpecFromWSJob(wsjob *wsapisv1.WSJob) wsclient.JobSpec {
	filteredAnnotations := make(map[string]string)

	for k, v := range wsjob.Annotations {
		if k == wsapisv1.MockCsAddressAnnot {
			filteredAnnotations[k] = v
		}
	}

	user := wsjob.Spec.User

	// Reverse-map from label values back to WSJobMode constants
	jobModeLabel := wsjob.Labels[wsapisv1.JobModeLabelKey]
	jobTypeLabel := wsjob.Labels[wsapisv1.JobTypeLabelKey]

	var wsJobMode wsclient.WSJobMode
	switch {
	case jobModeLabel == wsapisv1.JobModeWeightStreaming && jobTypeLabel == wsapisv1.JobTypeCompile:
		wsJobMode = wsclient.CompileJobMode
	case jobModeLabel == wsapisv1.JobModeWeightStreaming && jobTypeLabel == wsapisv1.JobTypeExecute:
		wsJobMode = wsclient.ExecuteJobMode
	case jobModeLabel == wsapisv1.JobModeSdk && jobTypeLabel == wsapisv1.JobTypeCompile:
		wsJobMode = wsclient.SdkCompileJobMode
	case jobModeLabel == wsapisv1.JobModeSdk && jobTypeLabel == wsapisv1.JobTypeExecute:
		wsJobMode = wsclient.SdkExecuteJobMode
	case jobModeLabel == wsapisv1.JobModeInference && jobTypeLabel == wsapisv1.JobTypeCompile:
		wsJobMode = wsclient.InferenceCompileJobMode
	case jobModeLabel == wsapisv1.JobModeInference && jobTypeLabel == wsapisv1.JobTypeExecute:
		wsJobMode = wsclient.InferenceExecuteJobMode
	default:
		// Fallback - use the label value directly
		wsJobMode = wsclient.WSJobMode(jobModeLabel)
	}

	var jobUser wsapisv1.JobUser
	if user != nil {
		jobUser = wsapisv1.JobUser{
			Uid:      user.Uid,
			Gid:      user.Gid,
			Username: user.Username,
			Groups:   user.Groups,
		}
	}

	return wsclient.JobSpec{
		JobUser:         jobUser,
		WsJobMode:       wsJobMode,
		NumWafers:       int(wsjob.Spec.NumWafers),
		Labels:          wsjob.Labels,
		Annotations:     filteredAnnotations,
		ActToCsxDomains: wsjob.Spec.ActToCsxDomains,
	}
}

func extractServiceSpecsFromWSJob(wsjob *wsapisv1.WSJob) wsclient.ServiceSpecs {
	specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)

	if coordSpec, exists := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator]; exists {
		if len(coordSpec.Template.Spec.Containers) > 0 {
			container := coordSpec.Template.Spec.Containers[0]
			specs[int(wsclient.WsCoordinator)] = wsclient.ServiceSpec{
				Image:       container.Image,
				Command:     container.Command,
				Env:         container.Env,
				Annotations: coordSpec.Template.Annotations,
			}
		}
	}

	return specs
}

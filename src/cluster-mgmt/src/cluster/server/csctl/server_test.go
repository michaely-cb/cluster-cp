//go:build !e2e

package csctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	coreinformers "k8s.io/client-go/informers"
	clientcorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	wsfake "cerebras.com/job-operator/client/clientset/versioned/fake"
	wsv1 "cerebras.com/job-operator/client/clientset/versioned/typed/ws/v1"
	wsv1fake "cerebras.com/job-operator/client/clientset/versioned/typed/ws/v1/fake"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	"cerebras.com/job-operator/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	"cerebras.com/cluster/server/pkg"
	wc "cerebras.com/cluster/server/pkg/wsclient"

	cspb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	nsfake "cerebras.com/job-operator/client-namespace/clientset/versioned/fake"
	nsv1 "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1"
)

const (
	MockUid = 1000
	MockGid = 1001
)

func newTestServer(
	wsJobInformer wsinformer.GenericInformer,
	k8s kubernetes.Interface,
	wsjobClient *wc.WSJobClient,
	nsCli nsv1.NamespaceReservationInterface,
	nodeInformer clientcorev1.NodeInformer,
) *Server {
	options := wc.ServerOptions{
		HasMultiMgmtNodes:          false,
		DisableUserAuth:            true,
		EnableIsolatedDashboards:   false,
		AllowNonDefaultJobPriority: true,
	}
	return newServer(wsJobInformer, k8s, wsjobClient, nil, nil,
		nsCli, nil, nodeInformer, options)
}

type FakeWsV1Interface struct {
	wsJobs *wsv1fake.FakeWSJobs
}

func (f *FakeWsV1Interface) WSJobs(namespace string) wsv1.WSJobInterface {
	return f.wsJobs
}

func (f *FakeWsV1Interface) RESTClient() rest.Interface {
	return nil
}

func TestGet(t *testing.T) {
	k8s := fakeclient.NewSimpleClientset()
	wsjobName := mockWsJobName()
	wsjob1Name := fmt.Sprintf("%s-%s", wsjobName, "a")
	wsjob2Name := fmt.Sprintf("%s-%s", wsjobName, "b")
	wsJobListRes := []runtime.Object{
		mockWsJob(wsjob1Name, map[string]string{K8sLabelPrefix + "test-a": "a", K8sLabelPrefix + "test-b": "a"}, nil),
		mockWsJob(wsjob2Name, map[string]string{K8sLabelPrefix + "test-a": "b", K8sLabelPrefix + "test-b": "b"}, nil),
	}
	jobClient := wsfake.NewSimpleClientset(wsJobListRes...)
	jobFactory := wsinformer.NewSharedInformerFactoryWithOptions(jobClient, 0)
	jobInformer, _ := jobFactory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("wsjobs"))
	jobFactory.Start(context.Background().Done())
	jobFactory.WaitForCacheSync(context.Background().Done())

	nsCli := nsfake.NewSimpleClientset(
		&namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: wsjob1Name},
		},
		&namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: wsjob2Name},
		},
		&namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "job-operator"},
		},
	).NamespaceV1().NamespaceReservations()

	csCtlServer := newTestServer(jobInformer, k8s, &wc.WSJobClient{}, nsCli, nil)

	createClusterDetailsConfigMap(t, k8s, wsjob1Name)
	createClusterDetailsConfigMap(t, k8s, wsjob2Name)

	t.Run("list as JSON", func(t *testing.T) {
		listRequest := cspb.GetRequest{
			Type:           "jobs",
			Name:           "",
			Accept:         cspb.SerializationMethod_JSON_METHOD,
			Representation: cspb.ObjectRepresentation_OBJECT_REPRESENTATION,
		}
		getRes, err := csCtlServer.Get(context.TODO(), &listRequest)
		require.NoError(t, err)

		jobList := &csv1.JobList{}
		require.NoError(t, protojson.Unmarshal(getRes.Raw, jobList))
		assert.Equal(t, 2, len(jobList.Items))
	})

	t.Run("list as table", func(t *testing.T) {
		listRequest := cspb.GetRequest{
			Type:           "jobs",
			Name:           "",
			Accept:         cspb.SerializationMethod_PROTOBUF_METHOD,
			Representation: cspb.ObjectRepresentation_TABLE_REPRESENTATION}
		getRes, err := csCtlServer.Get(context.TODO(), &listRequest)
		require.NoError(t, err)

		tableRes := &csv1.Table{}
		require.NoError(t, proto.Unmarshal(getRes.Raw, tableRes))
		assert.Equal(t, 2, len(tableRes.Rows))
	})

	t.Run("list failed due to namespace does not exist", func(t *testing.T) {
		listRequest := cspb.GetRequest{
			Type: "jobs",
			Name: "",
			Options: &cspb.GetOptions{
				Namespace: "invalid-ns-in-body",
			},
			Accept:         cspb.SerializationMethod_PROTOBUF_METHOD,
			Representation: cspb.ObjectRepresentation_TABLE_REPRESENTATION}
		_, err := csCtlServer.Get(context.TODO(), &listRequest)
		statusErr := err.(interface{ GRPCStatus() *grpcStatus.Status }).GRPCStatus()
		assert.Equal(t, codes.NotFound, statusErr.Code())
		require.ErrorContains(t, err, "invalid-ns-in-body")
	})

	t.Run("list failed due to invalid namespace format", func(t *testing.T) {
		listRequest := cspb.GetRequest{
			Type: "jobs",
			Name: "",
			Options: &cspb.GetOptions{
				Namespace: "invalid/ns-in-body",
			},
			Accept:         cspb.SerializationMethod_PROTOBUF_METHOD,
			Representation: cspb.ObjectRepresentation_TABLE_REPRESENTATION}

		reaction := func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
			invalidError := apierrors.NewInternalError(fmt.Errorf("invalid"))
			return true, nil, invalidError
		}
		clientMock := nsfake.NewSimpleClientset()
		nsCliMock := clientMock.NamespaceV1().NamespaceReservations()

		clientMock.PrependReactor("get", "namespacereservations", reaction)
		csCtlServerMock := newTestServer(jobInformer, k8s, &wc.WSJobClient{}, nsCliMock, nil)

		_, err := csCtlServerMock.Get(context.TODO(), &listRequest)
		statusErr := err.(interface{ GRPCStatus() *grpcStatus.Status }).GRPCStatus()
		assert.Equal(t, codes.Internal, statusErr.Code())
		require.ErrorContains(t, err, "invalid")
	})

	t.Run("get failed due to the specified namespace does not exist", func(t *testing.T) {
		// TODO: Deprecate the "namespace" property after release 2.3
		getRequest := cspb.GetRequest{
			Type: "jobs",
			Name: wsjob1Name,
			Options: &cspb.GetOptions{
				Namespace: "invalid-ns-in-body",
			},
			Accept:         cspb.SerializationMethod_PROTOBUF_METHOD,
			Representation: cspb.ObjectRepresentation_TABLE_REPRESENTATION}
		_, err := csCtlServer.Get(context.TODO(), &getRequest)
		statusErr := err.(interface{ GRPCStatus() *grpcStatus.Status }).GRPCStatus()
		assert.Equal(t, codes.NotFound, statusErr.Code())
		require.ErrorContains(t, err, "invalid-ns-in-body")

		// Extract current outgoing metadata, if any.
		ctx := context.TODO()
		incomingMD, _ := metadata.FromIncomingContext(ctx)
		newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, "invalid-ns-in-header")
		newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))

		getRequest = cspb.GetRequest{
			Type:           "jobs",
			Name:           wsjob1Name,
			Accept:         cspb.SerializationMethod_PROTOBUF_METHOD,
			Representation: cspb.ObjectRepresentation_TABLE_REPRESENTATION}
		_, err = csCtlServer.Get(newCtx, &getRequest)
		statusErr = err.(interface{ GRPCStatus() *grpcStatus.Status }).GRPCStatus()
		assert.Equal(t, codes.NotFound, statusErr.Code())
		require.ErrorContains(t, err, "invalid-ns-in-header")

		// We prioritize using the namespace info from request body until release 2.3.
		// See comments in namespace_interceptor.go for detailed explanation.
		getRequest = cspb.GetRequest{
			Type: "jobs",
			Name: wsjob1Name,
			Options: &cspb.GetOptions{
				Namespace: "invalid-ns-in-body",
			},
			Accept:         cspb.SerializationMethod_PROTOBUF_METHOD,
			Representation: cspb.ObjectRepresentation_TABLE_REPRESENTATION}
		_, err = csCtlServer.Get(newCtx, &getRequest)
		statusErr = err.(interface{ GRPCStatus() *grpcStatus.Status }).GRPCStatus()
		assert.Equal(t, codes.NotFound, statusErr.Code())
		require.ErrorContains(t, err, "invalid-ns-in-body")
	})

	t.Run("get success", func(t *testing.T) {
		_, err := k8s.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: wsjob1Name,
		}}, metav1.CreateOptions{})
		require.Nil(t, err)

		getRequest := cspb.GetRequest{
			Type: "jobs",
			Name: wsjob1Name,
			Options: &cspb.GetOptions{
				Namespace: wsjob1Name,
			},
			Accept:         cspb.SerializationMethod_PROTOBUF_METHOD,
			Representation: cspb.ObjectRepresentation_TABLE_REPRESENTATION}
		_, err = csCtlServer.Get(context.TODO(), &getRequest)
		require.Nil(t, err)
	})

	t.Run("get not found error", func(t *testing.T) {
		getRequest := cspb.GetRequest{
			Type:           "jobs",
			Name:           "not_exists",
			Accept:         cspb.SerializationMethod_PROTOBUF_METHOD,
			Representation: cspb.ObjectRepresentation_TABLE_REPRESENTATION}
		_, err := csCtlServer.Get(context.TODO(), &getRequest)
		require.Error(t, err)
		statusErr := err.(interface{ GRPCStatus() *grpcStatus.Status }).GRPCStatus()
		assert.Equal(t, codes.NotFound, statusErr.Code())
		require.ErrorContains(t, err, "not_exists")
	})

	t.Run("list with options", func(t *testing.T) {
		listRequest := cspb.GetRequest{
			Type:           "jobs",
			Name:           "",
			Accept:         cspb.SerializationMethod_JSON_METHOD,
			Representation: cspb.ObjectRepresentation_OBJECT_REPRESENTATION,
			Options: &cspb.GetOptions{
				LabelFilters: map[string]string{
					"test-a": "a",
				},
				AllStates: true,
				Uid:       -1,
			},
		}
		getRes, err := csCtlServer.Get(context.TODO(), &listRequest)
		require.NoError(t, err)

		jobList := &csv1.JobList{}
		require.NoError(t, protojson.Unmarshal(getRes.Raw, jobList))
		assert.Equal(t, 1, len(jobList.Items))
		assert.Equal(t, wsjob1Name, jobList.Items[0].GetMeta().Namespace)
		assert.Equal(t, wsjob1Name, jobList.Items[0].GetMeta().Name)

		listRequest = cspb.GetRequest{
			Type:           "jobs",
			Name:           "",
			Accept:         cspb.SerializationMethod_JSON_METHOD,
			Representation: cspb.ObjectRepresentation_OBJECT_REPRESENTATION,
			Options: &cspb.GetOptions{
				Uid:       MockUid,
				AllStates: true,
			},
		}
		getRes, err = csCtlServer.Get(context.TODO(), &listRequest)
		require.NoError(t, err)

		jobList = &csv1.JobList{}
		require.NoError(t, protojson.Unmarshal(getRes.Raw, jobList))
		assert.Equal(t, 2, len(jobList.Items))
	})

}

func TestPatch(t *testing.T) {
	k8s := fakeclient.NewSimpleClientset()
	wsjobName := mockWsJobName()

	t.Run("update happy path", func(t *testing.T) {
		fakeWsJobs := &wsv1fake.FakeWSJobs{Fake: &wsv1fake.FakeWsV1{Fake: &k8stesting.Fake{}}}
		fakeWsJobs.Fake.AddReactor(
			"patch",
			"wsjobs",
			func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				mock := mockWsJob(wsjobName, map[string]string{"test-a": "0"}, nil)
				mock.Name = "test0"
				mock.Labels = map[string]string{
					K8sLabelPrefix + "a":                     "x",
					K8sLabelPrefix + strings.Repeat("a", 63): "123",
				}
				return true, mock, nil
			},
		)
		fakeWsV1Interface := &FakeWsV1Interface{wsJobs: fakeWsJobs}
		wcJobClient := &wc.WSJobClient{WsV1Interface: fakeWsV1Interface}
		csCtlServer := newTestServer(nil, k8s, wcJobClient, nil, nil)
		patchBody := map[string]interface{}{
			"meta": map[string]interface{}{
				"labels": map[string]*string{
					"a":                     ptr("x"),
					strings.Repeat("a", 63): ptr("123"),
					"remove":                nil,
				},
			},
		}
		updateBodyBytes, err := json.Marshal(patchBody)
		require.NoError(t, err)
		req := &cspb.PatchRequest{
			ContentType: cspb.SerializationMethod_JSON_METHOD,
			Accept:      cspb.SerializationMethod_JSON_METHOD,
			PatchType:   cspb.PatchRequest_MERGE,
			Type:        "jobs",
			Name:        "test0",
			Body:        updateBodyBytes,
		}

		res, err := csCtlServer.Patch(context.TODO(), req)
		require.NoError(t, err)
		assert.Equal(t, cspb.SerializationMethod_JSON_METHOD, res.ContentType)

		out := map[string]interface{}{}
		require.NoError(t, json.Unmarshal(res.Raw, &out))
	})

	t.Run("update with wrong message type", func(t *testing.T) {
		csCtlServer := newTestServer(nil, k8s, &wc.WSJobClient{}, nil, nil)
		corrupt := make([]byte, 16)
		rand.Read(corrupt)
		req := &cspb.PatchRequest{
			ContentType: cspb.SerializationMethod_PROTOBUF_METHOD,
			Accept:      cspb.SerializationMethod_JSON_METHOD,
			PatchType:   cspb.PatchRequest_MERGE,
			Type:        "jobs",
			Name:        "test0",
			Body:        corrupt,
		}

		res, err := csCtlServer.Patch(context.TODO(), req)
		require.Nil(t, res)
		status, _ := grpcStatus.FromError(err)
		assert.Equal(t, codes.InvalidArgument, status.Code())
	})

	t.Run("update resource not supported", func(t *testing.T) {
		csCtlServer := newTestServer(nil, k8s, &wc.WSJobClient{}, nil, nil)
		raw, _ := proto.Marshal(tspb.Now())
		req := &cspb.PatchRequest{
			ContentType: cspb.SerializationMethod_PROTOBUF_METHOD,
			Accept:      cspb.SerializationMethod_JSON_METHOD,
			PatchType:   cspb.PatchRequest_MERGE,
			Type:        "timestamp",
			Name:        "now",
			Body:        raw,
		}

		res, err := csCtlServer.Patch(context.TODO(), req)
		require.Nil(t, res)
		status, _ := grpcStatus.FromError(err)
		assert.Equal(t, codes.NotFound, status.Code())
	})

	t.Run("update resource not found", func(t *testing.T) {
		fakeWsJobs := &wsv1fake.FakeWSJobs{Fake: &wsv1fake.FakeWsV1{Fake: &k8stesting.Fake{}}}
		fakeWsJobs.Fake.AddReactor(
			"patch",
			"wsjobs",
			func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				return true, nil, apierrors.NewNotFound(wsapisv1.Resource("wsjobs"), "not found")
			},
		)
		fakeWsV1Interface := &FakeWsV1Interface{wsJobs: fakeWsJobs}
		wcJobClient := &wc.WSJobClient{WsV1Interface: fakeWsV1Interface}
		csCtlServer := newTestServer(nil, k8s, wcJobClient, nil, nil)
		updateBodyBytes, err := json.Marshal(map[string]map[string]map[string]string{
			"meta": {"labels": {"x": "y"}},
		})
		require.NoError(t, err)
		req := &cspb.PatchRequest{
			ContentType: cspb.SerializationMethod_PROTOBUF_METHOD,
			Accept:      cspb.SerializationMethod_JSON_METHOD,
			PatchType:   cspb.PatchRequest_MERGE,
			Type:        "jobs",
			Name:        "test0",
			Body:        updateBodyBytes,
		}

		res, err := csCtlServer.Patch(context.TODO(), req)

		require.Nil(t, res)
		status, _ := grpcStatus.FromError(err)
		assert.Equal(t, codes.NotFound, status.Code())
	})

	t.Run("update invalid label", func(t *testing.T) {
		csCtlServer := newTestServer(nil, k8s, &wc.WSJobClient{}, nil, nil)
		for _, testcase := range []map[string]string{
			{"hello!world": "bad-key"},
			{"bad-value": "hello!world"},
		} {
			t.Run("input-"+mashName(testcase), func(t *testing.T) {
				updateBody := map[string]map[string]map[string]string{
					"meta": {"labels": testcase},
				}
				updateBodyBytes, err := json.Marshal(updateBody)
				require.NoError(t, err)
				req := &cspb.PatchRequest{
					ContentType: cspb.SerializationMethod_JSON_METHOD,
					Accept:      cspb.SerializationMethod_JSON_METHOD,
					PatchType:   cspb.PatchRequest_MERGE,
					Type:        "jobs",
					Name:        "test0",
					Body:        updateBodyBytes,
				}

				res, err := csCtlServer.Patch(context.TODO(), req)

				require.Nil(t, res)
				status, _ := grpcStatus.FromError(err)
				assert.Equal(t, codes.InvalidArgument, status.Code())
				assert.Contains(t, status.Message(), "regex")
			})
		}
	})
}

func TestListResourceTypes(t *testing.T) {
	k8s := fakeclient.NewSimpleClientset()

	server := newTestServer(nil, k8s, &wc.WSJobClient{}, nil, nil)
	res, err := server.ListResourceTypes(context.TODO(), &cspb.ListResourceTypesRequest{})
	require.NoError(t, err)

	names := common.Map(func(i *cspb.ResourceType) string { return i.Name }, res.Items)
	assert.Equal(t, []string{"cluster", "job", "node", "nodegroup", "system", "version", "volume", "worker-cache"}, names)
}

func TestListWithOptions(t *testing.T) {
	k8s := fakeclient.NewSimpleClientset()
	wsjobNamePrefix := mockWsJobName()

	jobs := make([]runtime.Object, 4)
	for i := 0; i < 4; i++ {
		wsjobName := fmt.Sprintf("%s-%d", wsjobNamePrefix, i)
		jobs[i] = mockWsJob(wsjobName, map[string]string{
			K8sLabelPrefix + "test-a": fmt.Sprintf("%d", i%2),
			K8sLabelPrefix + "test-b": fmt.Sprintf("%d", i),
		}, nil)
		createClusterDetailsConfigMap(t, k8s, wsjobName)
	}

	jobClient := wsfake.NewSimpleClientset(jobs...)
	jobFactory := wsinformer.NewSharedInformerFactoryWithOptions(jobClient, 0)
	jobInformer, _ := jobFactory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("wsjobs"))
	jobFactory.Start(context.Background().Done())
	jobFactory.WaitForCacheSync(context.Background().Done())

	csCtlServer := newTestServer(jobInformer, k8s, &wc.WSJobClient{}, nil, nil)

	for _, testcase := range []struct {
		name             string
		labels           map[string]string
		expectedJobNames []string
	}{
		{
			name:   "onelabel",
			labels: map[string]string{"test-a": "0"},
			expectedJobNames: []string{wsjobNamePrefix + "-0/" + wsjobNamePrefix + "-0",
				wsjobNamePrefix + "-2/" + wsjobNamePrefix + "-2"},
		},
		{
			name:             "twolabel",
			labels:           map[string]string{"test-a": "0", "test-b": "2"},
			expectedJobNames: []string{wsjobNamePrefix + "-2/" + wsjobNamePrefix + "-2"},
		},
	} {
		t.Run("list-with-option-"+testcase.name, func(t *testing.T) {
			listRequest := cspb.GetRequest{
				Type:           "jobs",
				Name:           "",
				Accept:         cspb.SerializationMethod_JSON_METHOD,
				Representation: cspb.ObjectRepresentation_OBJECT_REPRESENTATION,
				Options: &cspb.GetOptions{
					LabelFilters: testcase.labels,
					AllStates:    true,
					Uid:          -1,
				},
			}
			getRes, err := csCtlServer.Get(context.TODO(), &listRequest)
			require.NoError(t, err)

			jobList := &csv1.JobList{}
			require.NoError(t, protojson.Unmarshal(getRes.Raw, jobList))
			assert.Equal(t, len(testcase.expectedJobNames), len(jobList.Items))
			returnJobNames := make([]string, len(testcase.expectedJobNames))
			for i := 0; i < len(testcase.expectedJobNames); i++ {
				returnJobNames[i] = fmt.Sprintf("%s/%s", jobList.Items[i].GetMeta().Namespace, jobList.Items[i].GetMeta().Name)
			}
			sort.Strings(returnJobNames)
			for i := 0; i < len(testcase.expectedJobNames); i++ {
				assert.Equal(t, testcase.expectedJobNames[i], returnJobNames[i])
			}
		})
	}
}

func TestUpdateJobPriority(t *testing.T) {
	k8s := fakeclient.NewSimpleClientset()
	wsjobName := mockWsJobName()
	mockJob := mockWsJob(wsjobName, map[string]string{K8sLabelPrefix + "test-a": "a", K8sLabelPrefix + "test-b": "a"}, nil)
	mockJob.Namespace = common.Namespace
	mockJob.Status.Conditions = []commonv1.JobCondition{
		{
			Type: commonv1.JobScheduled,
		},
	}
	fakeWsJobs := &wsv1fake.FakeWSJobs{Fake: &wsv1fake.FakeWsV1{Fake: &k8stesting.Fake{}}}
	fakeWsJobs.Fake.AddReactor(
		"patch",
		"wsjobs",
		func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
			mock := mockWsJob(wsjobName, map[string]string{"test-a": "0"}, nil)
			mock.Spec.Priority = common.Pointer(5)
			return true, mock, nil
		},
	)
	fakeWsV1Interface := &FakeWsV1Interface{wsJobs: fakeWsJobs}
	wcJobClient := &wc.WSJobClient{WsV1Interface: fakeWsV1Interface}
	wsJobListRes := []runtime.Object{mockJob}
	jobClient := wsfake.NewSimpleClientset(wsJobListRes...)
	jobFactory := wsinformer.NewSharedInformerFactoryWithOptions(jobClient, 0)
	jobInformer, _ := jobFactory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("wsjobs"))
	jobFactory.Start(context.Background().Done())
	jobFactory.WaitForCacheSync(context.Background().Done())

	csCtlServer := newTestServer(jobInformer, k8s, wcJobClient, nil, nil)
	createClusterDetailsConfigMap(t, k8s, wsjobName)

	_, err := k8s.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: mockJob.Name,
	}}, metav1.CreateOptions{})
	require.Nil(t, err)

	t.Run("update job priority success", func(t *testing.T) {
		validInputs := []string{"p1", "299"}
		for _, input := range validInputs {
			updateJobPriorityRequest := cspb.UpdateJobPriorityRequest{
				JobId:       wsjobName,
				JobPriority: input,
			}
			ctx := context.TODO()
			incomingMD, _ := metadata.FromIncomingContext(ctx)
			newMD := metadata.Pairs(pkg.AuthUid, fmt.Sprintf("%d", MockUid+1000), pkg.AuthGid, fmt.Sprintf("%d", MockGid))
			newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
			_, err = csCtlServer.UpdateJobPriority(newCtx, &updateJobPriorityRequest)
			require.Nil(t, err)
		}
	})

	t.Run("fail to update job priority due to restricted priorities", func(t *testing.T) {
		// uid is not 0, so p0 and the corresponding range is not allowed
		invalidInputs := []string{"p0", "50", "999"}
		for _, input := range invalidInputs {
			updateJobPriorityRequest := cspb.UpdateJobPriorityRequest{
				JobId:       wsjobName,
				JobPriority: input,
			}
			ctx := context.TODO()
			incomingMD, _ := metadata.FromIncomingContext(ctx)
			newMD := metadata.Pairs(pkg.AuthUid, "2000", pkg.AuthGid, "1001")
			newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
			_, err = csCtlServer.UpdateJobPriority(newCtx, &updateJobPriorityRequest)
			require.ErrorContains(t, err, "input should be one of the priority bucket")
		}
	})

	t.Run("admin update job priority to p0", func(t *testing.T) {
		validInputs := []string{"p0", "50"}
		for _, input := range validInputs {
			updateJobPriorityRequest := cspb.UpdateJobPriorityRequest{
				JobId:       wsjobName,
				JobPriority: input,
			}
			ctx := context.TODO()
			incomingMD, _ := metadata.FromIncomingContext(ctx)
			newMD := metadata.Pairs(pkg.AuthUid, "0", pkg.AuthGid, "0")
			newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
			_, err = csCtlServer.UpdateJobPriority(newCtx, &updateJobPriorityRequest)
			require.Nil(t, err)
		}
	})
}

func TestSanitizePriorityInput(t *testing.T) {
	gaBuckets := []string{"p1", "p2", "p3"}
	errMsgPrefix := "Priority input should be one of"
	for _, testcase := range []struct {
		input                 string
		highestPriorityBucket string
		expectedPriorityVal   int
		expectErrMsg          string
	}{
		{
			input:                 "",
			highestPriorityBucket: "p1",
			expectedPriorityVal:   -1,
			expectErrMsg:          errMsgPrefix,
		},
		{
			input:                 "p0",
			highestPriorityBucket: "p1",
			expectedPriorityVal:   -1,
			expectErrMsg:          errMsgPrefix,
		},
		{
			input:                 "1",
			highestPriorityBucket: "p1",
			expectedPriorityVal:   -1,
			expectErrMsg:          errMsgPrefix,
		},
		{
			input:                 "p0",
			highestPriorityBucket: "p0",
			expectedPriorityVal:   wsapisv1.MaxP0JobPriorityValue,
			expectErrMsg:          "",
		},
		{
			input:                 "p1",
			highestPriorityBucket: "p1",
			expectedPriorityVal:   wsapisv1.MaxP1JobPriorityValue,
			expectErrMsg:          "",
		},
		{
			input:                 "p2",
			highestPriorityBucket: "p1",
			expectedPriorityVal:   wsapisv1.MaxP2JobPriorityValue,
			expectErrMsg:          "",
		},
		{
			input:                 "p3",
			highestPriorityBucket: "p1",
			expectedPriorityVal:   wsapisv1.MaxP3JobPriorityValue,
			expectErrMsg:          "",
		},
		{
			input:                 "150",
			highestPriorityBucket: "p1",
			expectedPriorityVal:   150,
			expectErrMsg:          "",
		},
	} {
		var minPriorityVal int
		var validBuckets []string
		if testcase.highestPriorityBucket == "p0" {
			minPriorityVal = 1
			validBuckets = append([]string{"p0"}, gaBuckets...)
		} else {
			minPriorityVal = 101
			validBuckets = gaBuckets
		}
		t.Run("test input sanitizing", func(t *testing.T) {
			priorityVal, err := sanitizePriorityInput(testcase.input, minPriorityVal, validBuckets)
			assert.Equal(t, testcase.expectedPriorityVal, priorityVal)
			if err != nil {
				assert.True(t, strings.Contains(err.Error(), testcase.expectErrMsg))
			} else {
				assert.Equal(t, testcase.expectErrMsg, "")
			}
		})
	}
}

func TestLogExport(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
			Labels: map[string]string{
				fmt.Sprintf("%scoordinator", K8sRoleLabelPrefix): "",
			},
		},
	}
	k8s := fakeclient.NewSimpleClientset(node)
	wsjobName := mockWsJobName()
	jobLabels := map[string]string{"test-a": "0"}
	jobAnnotations := map[string]string{
		wsapisv1.WorkdirLogsMountDirAnnot:   "/temp/nfspath1/mbx/ns/wsjob-id",
		wsapisv1.CachedCompileMountDirAnnot: "/temp/nfspath2/compile_root/ns/cs_123",
	}
	mockJob1 := mockWsJob(wsjobName, jobLabels, jobAnnotations)
	fakeWsJobs := &wsv1fake.FakeWSJobs{Fake: &wsv1fake.FakeWsV1{Fake: &k8stesting.Fake{}}}
	fakeWsJobs.Fake.AddReactor(
		"get",
		"wsjobs",
		func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
			return true, mockJob1, nil
		},
	)
	fakeWsV1Interface := &FakeWsV1Interface{wsJobs: fakeWsJobs}
	wcJobClient := &wc.WSJobClient{WsV1Interface: fakeWsV1Interface, Clientset: k8s}

	wsJobListRes := []runtime.Object{mockJob1}
	jobClient := wsfake.NewSimpleClientset(wsJobListRes...)
	jobFactory := wsinformer.NewSharedInformerFactoryWithOptions(jobClient, 0)
	jobInformer, _ := jobFactory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("wsjobs"))
	jobFactory.Start(context.Background().Done())
	jobFactory.WaitForCacheSync(context.Background().Done())

	nodeFactory := coreinformers.NewSharedInformerFactory(k8s, 0)
	nodeInformer := nodeFactory.Core().V1().Nodes()
	nodeFactory.Start(context.Background().Done())
	nodeFactory.WaitForCacheSync(context.Background().Done())
	// Check and manually seed if necessary
	store := nodeInformer.Informer().GetIndexer()
	if _, exists, _ := store.Get(node); !exists {
		err := store.Add(node)
		require.NoError(t, err)
	}

	csCtlServer := newTestServer(jobInformer, k8s, wcJobClient, nil, nodeInformer)
	createClusterVolumesConfigMap(t, k8s, mockJob1.Name)

	t.Run("log-export job happy path", func(t *testing.T) {
		req := &cspb.CreateLogExportRequest{
			Name: wsjobName,
		}

		ctx := context.TODO()
		user := pkg.ClientMeta{
			UID: MockUid,
			GID: MockGid,
		}
		ctx = context.WithValue(ctx, pkg.ClientMetaKey, user)
		incomingMD, _ := metadata.FromIncomingContext(ctx)
		newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, wsjobName)
		newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
		_, err := csCtlServer.CreateLogExport(newCtx, req)
		require.NoError(t, err)
	})

	t.Run("log-export job with different uid and same gid", func(t *testing.T) {
		req := &cspb.CreateLogExportRequest{
			Name: wsjobName,
		}

		ctx := context.TODO()
		user := pkg.ClientMeta{
			UID: MockUid + 1000,
			GID: MockGid,
		}
		ctx = context.WithValue(ctx, pkg.ClientMetaKey, user)
		incomingMD, _ := metadata.FromIncomingContext(ctx)
		newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, wsjobName)
		newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
		_, err := csCtlServer.CreateLogExport(newCtx, req)
		require.NoError(t, err)
	})

	t.Run("log-export job with wrong gid", func(t *testing.T) {
		req := &cspb.CreateLogExportRequest{
			Name: wsjobName,
		}

		ctx := context.TODO()
		user := pkg.ClientMeta{
			UID: MockUid + 1000,
			GID: MockGid + 1000,
		}
		ctx = context.WithValue(ctx, pkg.ClientMetaKey, user)
		incomingMD, _ := metadata.FromIncomingContext(ctx)
		newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, wsjobName)
		newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
		res, err := csCtlServer.CreateLogExport(newCtx, req)
		require.Nil(t, res)
		status, _ := grpcStatus.FromError(err)
		assert.Equal(t, codes.PermissionDenied, status.Code())
	})

	t.Run("log-export job with different uid and gid, but same groups", func(t *testing.T) {
		req := &cspb.CreateLogExportRequest{
			Name: wsjobName,
		}

		ctx := context.TODO()
		user := pkg.ClientMeta{
			UID:    MockUid + 1000,
			GID:    MockGid + 1000,
			Groups: []int64{MockGid, MockGid + 1000},
		}
		ctx = context.WithValue(ctx, pkg.ClientMetaKey, user)
		incomingMD, _ := metadata.FromIncomingContext(ctx)
		newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, wsjobName)
		newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
		_, err := csCtlServer.CreateLogExport(newCtx, req)
		require.NoError(t, err)
	})

	t.Run("log-export job with root user", func(t *testing.T) {
		req := &cspb.CreateLogExportRequest{
			Name: wsjobName,
		}

		ctx := context.TODO()
		user := pkg.ClientMeta{
			UID: 0,
			GID: 0,
		}
		ctx = context.WithValue(ctx, pkg.ClientMetaKey, user)
		incomingMD, _ := metadata.FromIncomingContext(ctx)
		newMD := metadata.Pairs(pkg.ResourceNamespaceHeader, wsjobName)
		newCtx := metadata.NewIncomingContext(ctx, metadata.Join(incomingMD, newMD))
		_, err := csCtlServer.CreateLogExport(newCtx, req)
		require.NoError(t, err)
	})
}

func mashName(m map[string]string) string {
	for k, v := range m {
		return k + ":" + v
	}
	return ""
}

func mockWsJobName() string {
	return fmt.Sprintf("wsjob-%08d", rand.Intn(1000))
}

func mockWsJob(name string, labels map[string]string, annotations map[string]string) *wsapisv1.WSJob {
	createTimeBefore := time.Duration(-rand.Intn(int(time.Hour) * 24 * 60))
	createTime := time.Now().Add(createTimeBefore)
	annotations = common.EnsureMap(annotations)
	annotations["k8s.cerebras.com/wsjob-system-names"] = "systemf0,systemf1"
	return &wsapisv1.WSJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         name,
			Name:              name,
			CreationTimestamp: metav1.NewTime(createTime),
			Labels:            labels,
			Annotations:       annotations,
		},
		Spec: wsapisv1.WSJobSpec{
			User: &wsapisv1.JobUser{
				Uid: MockUid,
				Gid: MockGid,
			},
		},
		Status: commonv1.JobStatus{
			Conditions: []commonv1.JobCondition{
				{
					Type:    "Created",
					Status:  "True",
					Reason:  "WSJobCreated",
					Message: "WSJob is created.",
				},
				{
					Type:    "Running",
					Status:  "False",
					Reason:  "WSJobRunning",
					Message: "WSJob is running.",
				},
				{
					Type:    "Succeeded",
					Status:  "True",
					Reason:  "WSJobSucceeded",
					Message: "WSJob successfully completed.",
				},
			},
		},
	}
}

func createClusterDetailsConfigMap(t *testing.T, k8s *fakeclient.Clientset, jobName string) {
	clusterDetailsConfigMap := mockClusterDetails(jobName)
	_, err := k8s.CoreV1().ConfigMaps(jobName).Create(
		context.TODO(),
		clusterDetailsConfigMap,
		metav1.CreateOptions{},
	)
	require.NoError(t, err)
}

func mockClusterDetails(name string) *corev1.ConfigMap {
	clusterDetailsData, _ := ioutil.ReadFile("testdata/2_kapi_cluster_details_mock.json")

	cdConfig := &commonpb.ClusterDetails{}
	options := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}
	options.Unmarshal(clusterDetailsData, cdConfig)
	compressed, _ := common.Gzip(clusterDetailsData)

	clusterDetailsConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: name,
			Name:      fmt.Sprintf("cluster-details-config-%s", name),
		},
		Data: map[string]string{
			wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.UpdatedSignalVal,
		},
		BinaryData: map[string][]byte{
			wsapisv1.ClusterDetailsConfigCompressedFileName: compressed,
		},
	}
	return clusterDetailsConfigMap
}

func createClusterVolumesConfigMap(t *testing.T, k8s *fakeclient.Clientset, jobName string) {
	clusterVolumesConfigMap := mockClusterVolumes(jobName)
	_, err := k8s.CoreV1().ConfigMaps(jobName).Create(
		context.TODO(),
		clusterVolumesConfigMap,
		metav1.CreateOptions{},
	)
	require.NoError(t, err)
}

func mockClusterVolumes(ns string) *corev1.ConfigMap {
	clusterVolumesConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "cluster-server-volumes",
		},
		Data: map[string]string{
			"workdir-volume": `{"type":"nfs","server":"nfs.example.com","serverPath":"/nfspath1","containerPath":"/temp/nfspath1"}`,
			"compile-volume": `{"type":"nfs","server":"nfs.example.com","serverPath":"/nfspath2","containerPath":"/temp/nfspath2"}`,
		},
	}
	return clusterVolumesConfigMap
}

func ptr(s string) *string {
	return &s
}

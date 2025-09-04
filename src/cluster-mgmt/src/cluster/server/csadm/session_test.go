//go:build !e2e

package csadm

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"cerebras.com/cluster/server/csctl"

	"cerebras/pb/workflow/appliance/cluster_mgmt/csadm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/yaml"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
	nsapisv1 "cerebras.com/job-operator/apis/namespace/v1"
	v1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	nsfake "cerebras.com/job-operator/client-namespace/clientset/versioned/fake"
	nsrv1fake "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1/fake"
	rlfake "cerebras.com/job-operator/client-resourcelock/clientset/versioned/fake"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	wsfake "cerebras.com/job-operator/client/clientset/versioned/fake"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	"cerebras.com/job-operator/common"
	wscommon "cerebras.com/job-operator/common"
)

func TestCreateSession(t *testing.T) {

	ctx := context.Background()
	testcases := []struct {
		name              string
		existing          *nsapisv1.NamespaceReservation
		existingNS        *corev1.Namespace
		req               *csadm.CreateSessionRequest
		watchNsr          []*nsapisv1.NamespaceReservation
		expectErrContains string
	}{
		{
			name: "test create success",
			watchNsr: []*nsapisv1.NamespaceReservation{
				&nsr,
			},
		},
		{
			name: "test create success - redundant mode",
			req:  sessionCreateWithRedundantRequest,
			watchNsr: []*nsapisv1.NamespaceReservation{
				&redundantNsr,
			},
		},
		{
			name: "test create success - with workload type",
			req:  sessionCreateWithWorkloadType,
			watchNsr: []*nsapisv1.NamespaceReservation{
				&nsrWithWorkloadType,
			},
		},
		{
			name: "test create success - with labels",
			req:  sessionCreateWithLabels,
			watchNsr: []*nsapisv1.NamespaceReservation{
				&nsrWithLabels,
			},
		},
		{
			name:       "test create success - user ns already exists",
			existingNS: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{nsapisv1.UserNSLabel: ""}}},
			watchNsr: []*nsapisv1.NamespaceReservation{
				&nsr,
			},
		},
		{
			name: "test grant failed",
			watchNsr: []*nsapisv1.NamespaceReservation{
				&failedNsr,
			},
			expectErrContains: "not enough systems",
		},
		{
			name:              "test timeout",
			expectErrContains: "timed out waiting for session to complete create/update",
		},
		{
			name:              "create reserved name fails",
			existingNS:        &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}},
			expectErrContains: "invalid session name: 'test' is reserved for system use",
		},
	}
	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			k8s := fake.NewSimpleClientset()
			if test.existingNS != nil {
				_, err := k8s.CoreV1().Namespaces().Create(ctx, test.existingNS, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			cli := nsrv1fake.FakeNamespaceV1{Fake: &k8stesting.Fake{}}
			s := NewServer(k8s, nil, cli.NamespaceReservations(), nil, nil,
				nil, wsclient.ServerOptions{SessionUpdateTimeoutSecs: 1}, nil)
			clusterConfig.AdjustNodesByClusterSetup()
			s.cfgProvider = pkg.StaticCfgProvider(*clusterConfig)
			watcher := watch.NewFakeWithChanSize(5, false)
			cli.AddWatchReactor(
				"namespacereservations",
				func(action k8stesting.Action) (handled bool, ret watch.Interface, err error) {
					return true, watcher, nil
				},
			)
			for _, event := range test.watchNsr {
				watcher.Add(event)
			}
			cli.AddReactor(
				"get",
				"namespacereservations",
				func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, errors.NewNotFound(schema.GroupResource{Group: "", Resource: ""}, "")
				},
			)
			request := test.req
			if request == nil {
				request = sessionCreateRequest
			}
			res, err := s.CreateSession(ctx, request)
			if test.expectErrContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectErrContains)
			} else {
				require.NoError(t, err)
				require.Equal(t, request.Name, res.Name)
				require.Equal(t, nsr.Status.Systems, res.State.Resources.Systems)
				require.Equal(t, nsr.Status.Nodegroups, res.State.Resources.Nodegroups)
				require.Equal(t, nsr.Status.Nodes, res.State.Resources.CoordinatorNodes)
				require.Equal(t, nsr.Status.Nodes, res.State.Resources.ManagementNodes)
				require.EqualValues(t, nsr.Status.CurrentResourceSpec.ParallelExecuteCount, res.State.CurrentResourceSpec.ParallelExecuteCount)
				require.EqualValues(t, nsr.Status.CurrentResourceSpec.ParallelCompileCount, res.State.CurrentResourceSpec.ParallelCompileCount)
				require.EqualValues(t, nsr.Status.CurrentResourceSpec.LargeMemoryRackCount, res.State.CurrentResourceSpec.LargeMemoryRackCount)
				require.EqualValues(t, nsr.Status.CurrentResourceSpec.XLargeMemoryRackCount, res.State.CurrentResourceSpec.XlargeMemoryRackCount)
				require.Equal(t, len(nsr.Status.UpdateHistory), len(res.State.UpdateHistory))
				if len(request.UserLabelOperations) != 0 {
					require.Subset(t, map[string]string{
						csctl.K8sLabelPrefix + "empty": "",
						csctl.K8sLabelPrefix + "key":   "value",
						"existing":                     "not-user-label",
					}, res.Properties)
				}
			}
		})
	}
}

func TestCreateUpdateSession(t *testing.T) {
	ctx := context.Background()

	t.Run("update with old client", func(t *testing.T) {
		k8s := fake.NewSimpleClientset()
		cli := nsrv1fake.FakeNamespaceV1{Fake: &k8stesting.Fake{}}
		s := NewServer(k8s, nil, cli.NamespaceReservations(), nil, nil,
			nil, wsclient.ServerOptions{SessionUpdateTimeoutSecs: 10}, nil)
		_, err := s.CreateSession(ctx, &csadm.CreateSessionRequest{
			Name: name,
			Spec: &csadm.SessionSpec{
				UpdateMode: csadm.SessionUpdateMode_UNSPECIFIED,
			},
		})
		require.ErrorContains(t, err, clientOutOfDateError.Error())

		_, err = s.UpdateSession(ctx, &csadm.UpdateSessionRequest{
			Name: name,
			Spec: &csadm.SessionSpec{
				UpdateMode: csadm.SessionUpdateMode_UNSPECIFIED,
			},
		})
		require.ErrorContains(t, err, clientOutOfDateError.Error())
	})

	t.Run("update with labels", func(t *testing.T) {
		var rlInformer rlinformer.GenericInformer
		k8s := fake.NewSimpleClientset()
		nsrClientSet := nsfake.NewSimpleClientset()
		nsrCli := nsrClientSet.NamespaceV1().NamespaceReservations()

		s := NewServer(k8s, nil, nsrCli, rlInformer, nil,
			nil, wsclient.ServerOptions{SessionUpdateTimeoutSecs: 1}, nil)
		clusterConfig.AdjustNodesByClusterSetup()
		s.cfgProvider = pkg.StaticCfgProvider(*clusterConfig)

		sessionBefore, err := nsrCli.Create(ctx, &nsrWithLabels, metav1.CreateOptions{})
		require.NoError(t, err)
		require.Equal(t, 3, len(sessionBefore.ObjectMeta.Labels))

		watcher := watch.NewFakeWithChanSize(5, false)
		nsrClientSet.PrependWatchReactor(
			"namespacereservations",
			func(action k8stesting.Action) (handled bool, ret watch.Interface, err error) {
				return true, watcher, nil
			},
		)
		watcher.Modify(&nsrWithLabels)

		_, err = s.UpdateSession(ctx,
			&csadm.UpdateSessionRequest{Name: nsrWithLabels.Name, Spec: &csadm.SessionSpec{
				UpdateMode: csadm.SessionUpdateMode_SET_RESOURCE_SPEC,
			}, UserLabelOperations: []*csadm.UserLabelOperation{
				{Op: csadm.UserLabelOperation_UPSERT, Key: "key", Value: "new-value"},
				{Op: csadm.UserLabelOperation_UPSERT, Key: "new-key", Value: "value"},
				{Op: csadm.UserLabelOperation_REMOVE, Key: "empty"},
			}})

		require.NoError(t, err)
		expectedMap := map[string]string{
			csctl.K8sLabelPrefix + "key":     "new-value",
			csctl.K8sLabelPrefix + "new-key": "value",
			"existing":                       "not-user-label",
		}

		sessionAfter, err := nsrCli.Get(ctx, nsrWithLabels.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, nsrWithLabels.Name, sessionAfter.Name)
		require.Equal(t, expectedMap, sessionAfter.Labels)
	})

	t.Run("update with dry run response", func(t *testing.T) {
		nsrDryRun := nsr.DeepCopy()
		nsrDryRun.Status.UpdateHistory = []nsapisv1.RequestStatus{
			{Request: v1.ReservationRequest{
				RequestParams: nsapisv1.RequestParams{
					UpdateMode:               nsapisv1.SetResourceSpecDryRunMode,
					SystemCount:              common.Pointer(5),
					ParallelCompileCount:     common.Pointer(10),
					ParallelExecuteCount:     common.Pointer(5),
					LargeMemoryRackCount:     common.Pointer(3),
					XLargeMemoryRackCount:    common.Pointer(2),
					SystemNames:              []string{"s0", "s1", "s2", "s3", "4"},
					NodegroupNames:           []string{"ng0", "ng1", "ng2", "ng3", "ng4"},
					CoordinatorNodeNames:     []string{"n0", "n1"},
					ActivationNodeNames:      []string{"ax-0", "ax-1", "ax-2", "ax-3", "ax-4"},
					InferenceDriverNodeNames: []string{"ix-1"},
					ManagementNodeNames:      []string{"n0", "n1"},
				},
			}},
		}
		k8s := fake.NewSimpleClientset()
		cli := nsrv1fake.FakeNamespaceV1{Fake: &k8stesting.Fake{}}
		s := NewServer(k8s, nil, cli.NamespaceReservations(), nil, nil, nil,
			wsclient.ServerOptions{SessionUpdateTimeoutSecs: 10}, nil)
		clusterConfig.AdjustNodesByClusterSetup()
		s.cfgProvider = pkg.StaticCfgProvider(*clusterConfig)
		res, err := s.nsrToSessionDryRun(*nsrDryRun)
		require.NoError(t, err)
		assert.Len(t, res.State.UpdateHistory, 2)
		assert.Len(t, res.State.UpdateHistory[0].Spec.GetResources().Systems, 5)
		assert.Len(t, res.State.UpdateHistory[0].Spec.GetResources().Nodegroups, 5)
		assert.Len(t, res.State.UpdateHistory[0].Spec.GetResources().CoordinatorNodes, 2)
		assert.Len(t, res.State.UpdateHistory[0].Spec.GetResources().ActivationNodes, 5)
		assert.Len(t, res.State.UpdateHistory[0].Spec.GetResources().InferenceDriverNodes, 1)
		assert.Len(t, res.State.UpdateHistory[0].Spec.GetResources().ManagementNodes, 2)
		assert.Equal(t, res.State.UpdateHistory[1].Spec.GetResourceSpec(), &csadm.SessionResourceSpec{
			SystemCount:           5,
			LargeMemoryRackCount:  3,
			XlargeMemoryRackCount: 2,
			ParallelCompileCount:  10,
			ParallelExecuteCount:  5,
		})
	})

}

func TestListSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clusterConfig.AdjustNodesByClusterSetup()
	cfgProvider := pkg.StaticCfgProvider(*clusterConfig)

	var lockList = []runtime.Object{
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-1",
				Namespace: "session0",
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockPending,
			},
		},
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-2",
				Namespace: "session0",
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
			},
		},
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-3",
				Namespace: "session0",
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
			},
		},
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-4",
				Namespace: "session1",
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockError,
			},
		},
	}

	var jobList = []runtime.Object{
		&wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-1",
				Namespace: "session1",
			},
			Status: commonv1.JobStatus{
				CompletionTime: &metav1.Time{
					Time: time.Now().Add(-6 * 24 * time.Hour),
				},
			},
		},
		&wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-2",
				Namespace: "session1",
			},
			Status: commonv1.JobStatus{
				CompletionTime: &metav1.Time{
					Time: time.Now().Add(-3 * time.Hour),
				},
			},
		},
		&wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-3",
				Namespace: "session1",
			},
			Status: commonv1.JobStatus{
				CompletionTime: &metav1.Time{
					Time: time.Now().Add(-30 * time.Minute),
				},
			},
		},
	}

	// init locks
	rlClient := rlfake.NewSimpleClientset(lockList...)
	factory := rlinformer.NewSharedInformerFactoryWithOptions(rlClient, 0)
	rlInformer, _ := factory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("resourcelocks"))
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	// init jobs
	jobClient := wsfake.NewSimpleClientset(jobList...)
	jobFactory := wsinformer.NewSharedInformerFactoryWithOptions(jobClient, 0)
	jobInformer, _ := jobFactory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("wsjobs"))
	jobFactory.Start(ctx.Done())
	jobFactory.WaitForCacheSync(ctx.Done())

	labels := map[string]string{csctl.K8sLabelPrefix + "k1": "v1", "k2": "v2"}

	unassignedPayload := &nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{Name: "*unassigned", Labels: labels},
		Status: nsapisv1.NamespaceReservationStatus{
			Systems:    []string{"system-2"},
			Nodegroups: []string{"group1", "group2"},
			Nodes:      []string{"0-coord-1", "0-coord-2"},
			CurrentResourceSpec: nsapisv1.ResourceSpec{
				ParallelCompileCount:  4,
				ParallelExecuteCount:  1,
				LargeMemoryRackCount:  1,
				XLargeMemoryRackCount: 2,
			},
		},
	}

	sessions := []*nsapisv1.NamespaceReservation{
		{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("session0"), Labels: labels},
			Status: nsapisv1.NamespaceReservationStatus{Systems: []string{"system-0"}, Nodes: []string{"0-coord-0", "ax-0"}, Nodegroups: []string{"group0"},
				CurrentResourceSpec: nsapisv1.ResourceSpec{ParallelExecuteCount: 1, ParallelCompileCount: 2, LargeMemoryRackCount: 0, XLargeMemoryRackCount: 1}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("session1"), Labels: labels},
			Status: nsapisv1.NamespaceReservationStatus{Systems: []string{"system-1"}, Nodes: []string{"0-coord-1", "ax-1"}, Nodegroups: []string{"group1"},
				CurrentResourceSpec: nsapisv1.ResourceSpec{ParallelExecuteCount: 1, ParallelCompileCount: 3, LargeMemoryRackCount: 1, XLargeMemoryRackCount: 0},
			},
		},
	}
	redundantSessions := []*nsapisv1.NamespaceReservation{
		{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("session2"), Labels: map[string]string{
				nsapisv1.IsRedundantModeKey: "true",
				csctl.K8sLabelPrefix + "k1": "v1",
				"k2":                        "v2",
			}},
			Status: nsapisv1.NamespaceReservationStatus{Systems: []string{"system-2"}, Nodes: []string{"0-coord-2", "ax-2"}, Nodegroups: []string{"group2"},
				CurrentResourceSpec: nsapisv1.ResourceSpec{ParallelExecuteCount: 1, ParallelCompileCount: 3, LargeMemoryRackCount: 1, XLargeMemoryRackCount: 0},
			},
		},
	}
	expectedResourcesAssigned := []*csadm.SessionState{
		{Resources: &csadm.SessionResources{
			Systems:              []string{"system-0"},
			Nodegroups:           []string{"group0"},
			CoordinatorNodes:     []string{"0-coord-0"},
			ActivationNodes:      []string{"ax-0"},
			InferenceDriverNodes: []string{},
			ManagementNodes:      []string{"0-coord-0"}},
			CurrentResourceSpec: &csadm.SessionResourceSpec{SystemCount: 1, ParallelExecuteCount: 1, ParallelCompileCount: 2, LargeMemoryRackCount: 0, XlargeMemoryRackCount: 1}},
		{Resources: &csadm.SessionResources{
			Systems:              []string{"system-1"},
			Nodegroups:           []string{"group1"},
			CoordinatorNodes:     []string{"0-coord-1"},
			ActivationNodes:      []string{"ax-1"},
			InferenceDriverNodes: []string{},
			ManagementNodes:      []string{"0-coord-1"}},
			CurrentResourceSpec: &csadm.SessionResourceSpec{SystemCount: 1, ParallelExecuteCount: 1, ParallelCompileCount: 3, LargeMemoryRackCount: 1, XlargeMemoryRackCount: 0}},
	}
	expectedResourcesUnassigned := []*csadm.SessionState{
		{Resources: &csadm.SessionResources{
			Systems:              []string{"system-2"},
			Nodegroups:           []string{"group1", "group2"},
			CoordinatorNodes:     []string{"0-coord-1", "0-coord-2"},
			ActivationNodes:      []string{},
			InferenceDriverNodes: []string{},
			ManagementNodes:      []string{"0-coord-1", "0-coord-2"}},
			CurrentResourceSpec: &csadm.SessionResourceSpec{SystemCount: 1, ParallelExecuteCount: 1, ParallelCompileCount: 4, LargeMemoryRackCount: 1, XlargeMemoryRackCount: 2}},
	}
	expectedStatusAssigned := []*csadm.RuntimeStatus{
		{RunningJobCount: 2, QueueingJobCount: 1, InactiveDuration: "active"},
		{RunningJobCount: 0, QueueingJobCount: 0, InactiveDuration: "30m"},
	}
	expectedStatusUnassigned := []*csadm.RuntimeStatus{
		{RunningJobCount: 0, QueueingJobCount: 0, InactiveDuration: ">7 days"},
	}

	// Test matrix: with/without unassigned resources, with/without "--unassigned-only" flag

	testcases := []struct {
		name                             string
		sessions                         []*nsapisv1.NamespaceReservation
		redundantSessions                []*nsapisv1.NamespaceReservation
		unassignedNsr                    *nsapisv1.NamespaceReservation
		expectSessionResourcesAssigned   []*csadm.SessionState
		expectSessionResourcesUnassigned []*csadm.SessionState
		expectSessionStatusAssigned      []*csadm.RuntimeStatus
		expectSessionStatusUnassigned    []*csadm.RuntimeStatus
		redundant                        bool
	}{
		{
			name:                             "list with both assigned and unassigned resources",
			unassignedNsr:                    unassignedPayload,
			sessions:                         sessions,
			expectSessionResourcesAssigned:   expectedResourcesAssigned,
			expectSessionResourcesUnassigned: expectedResourcesUnassigned,
			expectSessionStatusAssigned:      expectedStatusAssigned,
			expectSessionStatusUnassigned:    expectedStatusUnassigned,
		},
		// TODO: Do we need a test case for no unassigned resources? (is that a possible scenario?)
		{
			name:                             "list with no assigned resources",
			unassignedNsr:                    unassignedPayload,
			expectSessionResourcesUnassigned: expectedResourcesUnassigned,
			expectSessionStatusUnassigned:    expectedStatusUnassigned,
		},
		{
			name:              "list redundant session only",
			unassignedNsr:     unassignedPayload,
			sessions:          sessions,
			redundantSessions: redundantSessions,
			redundant:         true,
		},
	}
	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			nsrCli := nsfake.NewSimpleClientset()
			for _, ss := range test.sessions {
				_, err := nsrCli.NamespaceV1().NamespaceReservations().Create(ctx, ss, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			for _, ss := range test.redundantSessions {
				_, err := nsrCli.NamespaceV1().NamespaceReservations().Create(ctx, ss, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			k8 := fake.NewSimpleClientset()
			if test.unassignedNsr != nil {
				b, err := yaml.Marshal(test.unassignedNsr)
				require.NoError(t, err)
				cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: nsapisv1.UnassignedResourcesCM},
					Data: map[string]string{nsapisv1.UnassignedResourcesCMDataKey: string(b)}}
				cm, err = k8.CoreV1().ConfigMaps(common.SystemNamespace).Create(ctx, cm, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			s := NewServer(k8, nil, nsrCli.NamespaceV1().NamespaceReservations(), rlInformer, jobInformer,
				nil, wsclient.ServerOptions{SessionUpdateTimeoutSecs: 10}, nil)
			s.cfgProvider = cfgProvider

			// Verify both with and without the "show unassigned" flag
			for _, showUnassigned := range []bool{true, false} {
				if test.redundant {
					showUnassigned = false
				}
				res, err := s.ListSession(ctx, &csadm.ListSessionRequest{
					ShowUnassigned: showUnassigned, Redundant: test.redundant})
				require.NoError(t, err)

				var states []*csadm.SessionState
				var statuses []*csadm.RuntimeStatus
				var names []string

				expectedLabels := map[string]string{csctl.K8sLabelPrefix + "k1": "v1", "k2": "v2"}
				for _, ses := range res.Items {
					names = append(names, ses.Name)
					states = append(states, ses.State)
					statuses = append(statuses, ses.Status)
					require.Subset(t, ses.Properties, expectedLabels)
				}

				if test.redundant {
					for _, ss := range test.redundantSessions {
						require.Contains(t, names, ss.Name)
					}
					break
				}

				// If we set the flag, there should be ONLY unassigned resources
				if showUnassigned {
					require.Equal(t, len(res.Items), 1)
				}

				if showUnassigned {
					assert.Equal(t, test.expectSessionResourcesUnassigned, states)
					assert.Equal(t, test.expectSessionStatusUnassigned, statuses)
				} else {
					expectResourcesCombined := append(test.expectSessionResourcesAssigned, test.expectSessionResourcesUnassigned...)
					expectStatusCombined := append(test.expectSessionStatusAssigned, test.expectSessionStatusUnassigned...)
					assert.Equal(t, expectResourcesCombined, states)
					assert.Equal(t, expectStatusCombined, statuses)
				}

				// Always show unassigned resources if present
				sessionPb := res.Items[len(res.Items)-1]
				require.EqualValues(t, sessionPb.State.CurrentResourceSpec.ParallelExecuteCount, unassignedPayload.Status.CurrentResourceSpec.ParallelExecuteCount)
				require.EqualValues(t, sessionPb.State.CurrentResourceSpec.ParallelCompileCount, unassignedPayload.Status.CurrentResourceSpec.ParallelCompileCount)
				require.EqualValues(t, sessionPb.State.CurrentResourceSpec.LargeMemoryRackCount, unassignedPayload.Status.CurrentResourceSpec.LargeMemoryRackCount)
				require.EqualValues(t, sessionPb.State.CurrentResourceSpec.XlargeMemoryRackCount, unassignedPayload.Status.CurrentResourceSpec.XLargeMemoryRackCount)
			}
		})
	}
}

func TestSessionRedundancy(t *testing.T) {
	ctx := context.Background()
	testcases := []struct {
		name           string
		assignSessions string
		existing       []*nsapisv1.NamespaceReservation
		errContains    string
		lock           *rlv1.ResourceLock
	}{
		{
			name:           "assign success",
			assignSessions: "session-b,session-a",
			existing: []*nsapisv1.NamespaceReservation{
				&nsr,
				{ObjectMeta: metav1.ObjectMeta{Name: "session-a",
					Labels: map[string]string{nsapisv1.IsRedundantModeKey: "true"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "session-b",
					Labels: map[string]string{nsapisv1.IsRedundantModeKey: "true"}}},
			},
		},
		{
			name:           "clear assign success",
			assignSessions: "",
			existing: []*nsapisv1.NamespaceReservation{
				&nsr,
			},
		},
		{
			name:           "clear assign fail session have active jobs",
			assignSessions: "",
			existing:       []*nsapisv1.NamespaceReservation{&nsr},
			lock: &rlv1.ResourceLock{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lock-1",
					Namespace: nsr.Name,
				},
				Status: rlv1.ResourceLockStatus{
					State: rlv1.LockPending,
				},
			},
			errContains: "remove redundancy 'redundant' is not supported when session 'test' has active jobs",
		},
		{
			name:           "assign fail due to session not in redundant mode",
			assignSessions: "session-a,session-b",
			existing: []*nsapisv1.NamespaceReservation{
				&nsr,
				{ObjectMeta: metav1.ObjectMeta{Name: "session-a",
					Labels: map[string]string{nsapisv1.IsRedundantModeKey: "true"}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "session-b"}},
			},
			errContains: "not in redundant mode",
		},
		{
			name:           "assign fail candidate not found",
			assignSessions: "session-a,session-b",
			existing:       []*nsapisv1.NamespaceReservation{&nsr},
			errContains:    "not found",
		},
		{
			name:           "assign fail session not found",
			assignSessions: "session-a,session-b",
			existing:       []*nsapisv1.NamespaceReservation{},
			errContains:    "not found",
		},
	}
	for _, test := range testcases {
		var rlInformer rlinformer.GenericInformer
		if test.lock != nil {
			rlClient := rlfake.NewSimpleClientset(test.lock)
			factory := rlinformer.NewSharedInformerFactoryWithOptions(rlClient, 0)
			rlInformer, _ = factory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("resourcelocks"))
			factory.Start(ctx.Done())
			factory.WaitForCacheSync(ctx.Done())
		}
		t.Run(test.name, func(t *testing.T) {
			k8s := fake.NewSimpleClientset()
			cli := nsrv1fake.FakeNamespaceV1{Fake: &k8s.Fake}
			s := NewServer(k8s, nil, cli.NamespaceReservations(), rlInformer, nil,
				nil, wsclient.ServerOptions{SessionUpdateTimeoutSecs: 10}, nil)
			clusterConfig.AdjustNodesByClusterSetup()
			s.cfgProvider = pkg.StaticCfgProvider(*clusterConfig)
			for _, ss := range test.existing {
				_, err := cli.NamespaceReservations().Create(ctx, ss, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			res, err := s.UpdateSession(ctx,
				&csadm.UpdateSessionRequest{Name: nsr.Name, Spec: &csadm.SessionSpec{
					UpdateMode: csadm.SessionUpdateMode_SET_RESOURCE_SPEC,
				}, Properties: map[string]string{
					nsapisv1.AssignedRedundancyKey: test.assignSessions,
				}})
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
				expectSessions := strings.Split(test.assignSessions, ",")
				sort.Strings(expectSessions)
				require.Equal(t, res.Properties[nsapisv1.AssignedRedundancyKey],
					strings.Join(expectSessions, wsapisv1.LabelSeparator))
			}
		})
	}
}

func TestDeleteSession(t *testing.T) {
	ctx := context.Background()
	testcases := []struct {
		name          string
		deleteRequest *csadm.DeleteSessionRequest
		locks         []runtime.Object
		existing      []*nsapisv1.NamespaceReservation
		errContains   string
	}{
		{
			name:          "test delete success",
			deleteRequest: &csadm.DeleteSessionRequest{Name: nsr.Name},
			existing:      []*nsapisv1.NamespaceReservation{{ObjectMeta: metav1.ObjectMeta{Name: nsr.Name}}},
		},
		{
			name:          "test delete with running jobs",
			locks:         []runtime.Object{&rlv1.ResourceLock{ObjectMeta: metav1.ObjectMeta{Name: "rl0", Namespace: nsr.Name}, Status: rlv1.ResourceLockStatus{State: rlv1.LockGranted}}},
			deleteRequest: &csadm.DeleteSessionRequest{Name: nsr.Name},
			existing:      []*nsapisv1.NamespaceReservation{&nsr},
			errContains:   "session has running jobs and cannot be deleted",
		},
		{
			name:          "test delete redundant mode in use",
			deleteRequest: &csadm.DeleteSessionRequest{Name: "redundant"},
			existing:      []*nsapisv1.NamespaceReservation{&nsr, &redundantNsr},
			errContains:   "Session 'redundant' is assigned as redundancy to sessions:[test]",
		},
		{
			name:          "test delete not found",
			deleteRequest: &csadm.DeleteSessionRequest{Name: "foo"},
			existing:      []*nsapisv1.NamespaceReservation{&nsr},
			errContains:   "session 'foo' not found",
		},
		{
			name:          "test delete default",
			deleteRequest: &csadm.DeleteSessionRequest{Name: common.SystemNamespace},
			existing:      []*nsapisv1.NamespaceReservation{&nsr},
			errContains:   "is the default session and cannot be deleted",
		},
	}
	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			k8s := fake.NewSimpleClientset()
			nsrCli := nsfake.NewSimpleClientset()
			rlClient := rlfake.NewSimpleClientset(test.locks...)
			factory := rlinformer.NewSharedInformerFactoryWithOptions(rlClient, 0)
			rlInformer, _ := factory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("resourcelocks"))
			factory.Start(ctx.Done())
			factory.WaitForCacheSync(ctx.Done())
			s := NewServer(k8s, nil, nsrCli.NamespaceV1().NamespaceReservations(), rlInformer, nil,
				nil, wsclient.ServerOptions{SessionUpdateTimeoutSecs: 10}, nil)
			for _, ss := range test.existing {
				_, err := nsrCli.NamespaceV1().NamespaceReservations().Create(ctx, ss, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			_, err := s.DeleteSession(ctx, test.deleteRequest)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestParamsToSessionSpecRoundTrip(t *testing.T) {
	for _, testcase := range []struct {
		name   string
		params *nsapisv1.RequestParams
	}{
		{
			name: "set spec mode",
			params: &nsapisv1.RequestParams{
				UpdateMode:            nsapisv1.SetResourceSpecMode,
				SystemCount:           pointer.Int(10),
				LargeMemoryRackCount:  nil,
				XLargeMemoryRackCount: nil,
				ParallelCompileCount:  pointer.Int(3),
				ParallelExecuteCount:  pointer.Int(7),
			},
		},
		{
			name: "debug mode",
			params: &nsapisv1.RequestParams{
				UpdateMode:               nsapisv1.DebugAppendMode,
				SystemNames:              []string{"system1", "system2"},
				NodegroupNames:           []string{"ng0", "ng1"},
				ActivationNodeNames:      []string{"a0", "a1"},
				InferenceDriverNodeNames: []string{},
				CoordinatorNodeNames:     []string{"n0", "n1"},
				ManagementNodeNames:      []string{"n0", "n1"},
			},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			spec := ParamsToSessionSpec(testcase.params)
			roundTripParams, err := SessionSpecToParams(spec)
			require.NoError(t, err)

			if !reflect.DeepEqual(testcase.params, &roundTripParams) {
				t.Errorf("Round trip conversion did not result in the original RequestParams\noriginal: %#v\nround trip: %#v",
					testcase.params, roundTripParams)
			}
		})
	}
}

var (
	name       = "test"
	preUid     = "pre-uid"
	uid        = "uid"
	sysCount   = 2
	largeRack  = 1
	xlargeRack = 1
	preRequest = nsapisv1.ReservationRequest{
		RequestUid: preUid,
		RequestParams: nsapisv1.RequestParams{
			UpdateMode:  nsapisv1.SetResourceSpecMode,
			SystemCount: common.Pointer(1),
		},
	}
	req = nsapisv1.ReservationRequest{
		RequestUid: uid,
		RequestParams: nsapisv1.RequestParams{
			UpdateMode:            nsapisv1.SetResourceSpecMode,
			SystemCount:           &sysCount,
			LargeMemoryRackCount:  &largeRack,
			XLargeMemoryRackCount: &xlargeRack,
		},
	}
	updatingNsr = nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: nsapisv1.NamespaceReservationSpec{
			ReservationRequest: nsapisv1.ReservationRequest{
				RequestUid: "updating-uid",
				RequestParams: nsapisv1.RequestParams{
					UpdateMode:  nsapisv1.SetResourceSpecMode,
					SystemCount: common.Pointer(4),
				},
			},
		},
	}
	failedNsr = nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: nsapisv1.NamespaceReservationSpec{
			ReservationRequest: req,
		},
		Status: nsapisv1.NamespaceReservationStatus{
			UpdateHistory: []nsapisv1.RequestStatus{
				{
					Request:   req,
					Succeeded: false,
					Message:   "not enough systems",
				},
			},
		},
	}
	preNsr = nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: nsapisv1.NamespaceReservationSpec{
			ReservationRequest: preRequest,
		},
		Status: nsapisv1.NamespaceReservationStatus{
			Systems:    []string{"system-0"},
			Nodegroups: []string{"group0"},
			Nodes:      []string{"0-coord-0"},
			CurrentResourceSpec: nsapisv1.ResourceSpec{
				ParallelCompileCount:  1,
				ParallelExecuteCount:  1,
				LargeMemoryRackCount:  0,
				XLargeMemoryRackCount: 0,
			},
			UpdateHistory: []nsapisv1.RequestStatus{
				{
					Request:   preRequest,
					Succeeded: true,
				},
			},
		},
	}
	nsr = nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				nsapisv1.AssignedRedundancyKey: "redundant",
			},
		},
		Spec: nsapisv1.NamespaceReservationSpec{
			ReservationRequest: req,
		},
		Status: nsapisv1.NamespaceReservationStatus{
			Systems:    []string{"system-0", "system-1"},
			Nodegroups: []string{"group0", "group1"},
			Nodes:      []string{"0-coord-0"},
			CurrentResourceSpec: nsapisv1.ResourceSpec{
				ParallelCompileCount:  1,
				ParallelExecuteCount:  2,
				LargeMemoryRackCount:  1,
				XLargeMemoryRackCount: 1,
			},
			GrantedRequest: req,

			UpdateHistory: []nsapisv1.RequestStatus{
				{
					Request:   req,
					Succeeded: true,
				},
				{
					Request:   preRequest,
					Succeeded: true,
				},
			},
		},
	}
	redundantNsr = nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redundant",
			Labels: map[string]string{
				nsapisv1.IsRedundantModeKey: "true",
			},
		},
		Spec: nsapisv1.NamespaceReservationSpec{
			ReservationRequest: req,
		},
		Status: nsapisv1.NamespaceReservationStatus{
			Systems:    []string{"system-0", "system-1"},
			Nodegroups: []string{"group0", "group1"},
			Nodes:      []string{"0-coord-0"},
			CurrentResourceSpec: nsapisv1.ResourceSpec{
				ParallelCompileCount:  1,
				ParallelExecuteCount:  2,
				LargeMemoryRackCount:  1,
				XLargeMemoryRackCount: 1,
			},
			GrantedRequest: req,

			UpdateHistory: []nsapisv1.RequestStatus{
				{
					Request:   req,
					Succeeded: true,
				},
				{
					Request:   preRequest,
					Succeeded: true,
				},
			},
		},
	}
	nsrWithWorkloadType = nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				nsapisv1.SessionWorkloadTypeKey: "inference",
			},
		},
		Spec: nsapisv1.NamespaceReservationSpec{
			ReservationRequest: req,
		},
		Status: nsapisv1.NamespaceReservationStatus{
			Systems:    []string{"system-0", "system-1"},
			Nodegroups: []string{"group0", "group1"},
			Nodes:      []string{"0-coord-0"},
			CurrentResourceSpec: nsapisv1.ResourceSpec{
				ParallelCompileCount:  1,
				ParallelExecuteCount:  2,
				LargeMemoryRackCount:  1,
				XLargeMemoryRackCount: 1,
			},
			GrantedRequest: req,

			UpdateHistory: []nsapisv1.RequestStatus{
				{
					Request:   req,
					Succeeded: true,
				},
				{
					Request:   preRequest,
					Succeeded: true,
				},
			},
		},
	}
	nsrWithLabels = nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: "labels-nsr",
			Labels: map[string]string{
				csctl.K8sLabelPrefix + "empty": "",
				csctl.K8sLabelPrefix + "key":   "value",
				"existing":                     "not-user-label",
			},
		},
		Spec: nsapisv1.NamespaceReservationSpec{
			ReservationRequest: req,
		},
		Status: nsapisv1.NamespaceReservationStatus{
			Systems:    []string{"system-0", "system-1"},
			Nodegroups: []string{"group0", "group1"},
			Nodes:      []string{"0-coord-0"},
			CurrentResourceSpec: nsapisv1.ResourceSpec{
				ParallelCompileCount:  1,
				ParallelExecuteCount:  2,
				LargeMemoryRackCount:  1,
				XLargeMemoryRackCount: 1,
			},
			GrantedRequest: req,

			UpdateHistory: []nsapisv1.RequestStatus{
				{
					Request:   req,
					Succeeded: true,
				},
				{
					Request:   preRequest,
					Succeeded: true,
				},
			},
		},
	}
	sessionCreateRequest = &csadm.CreateSessionRequest{
		Name: name,
		Spec: ParamsToSessionSpec(&req.RequestParams),
	}
	sessionCreateWithRedundantRequest = &csadm.CreateSessionRequest{
		Name: "redundant",
		Spec: ParamsToSessionSpec(&req.RequestParams),
		Properties: map[string]string{
			nsapisv1.IsRedundantModeKey: "true",
		},
	}

	sessionCreateWithWorkloadType = &csadm.CreateSessionRequest{
		Name: "test",
		Spec: ParamsToSessionSpec(&req.RequestParams),
		Properties: map[string]string{
			nsapisv1.SessionWorkloadTypeKey: "inference",
		},
	}
	sessionCreateWithLabels = &csadm.CreateSessionRequest{
		Name: "labels-nsr",
		Spec: ParamsToSessionSpec(&req.RequestParams),
		UserLabelOperations: []*csadm.UserLabelOperation{
			{Op: csadm.UserLabelOperation_UPSERT, Key: "empty", Value: ""},
			{Op: csadm.UserLabelOperation_UPSERT, Key: "key", Value: "value"},
		},
	}
	sessionUpdateRequest = &csadm.UpdateSessionRequest{
		Name: name,
		Spec: ParamsToSessionSpec(&req.RequestParams),
	}

	clusterCfgBuilder = common.NewClusterConfigBuilder("test").
				WithCoordNode().
				WithCoordNode().
				WithCoordNode().
				WithAXInGroup("group0", 1).
				WithAXInGroup("group1", 1).
				WithSysMemxWorkerGroups(2, 2, 1)
	clusterConfig = clusterCfgBuilder.BuildClusterConfig()
)

func TestGetSessionPermissions(t *testing.T) {
	ctx := context.Background()

	// Define test-specific system namespace for clarity
	systemNamespace := "job-operator"

	testCases := []struct {
		name             string
		requestedSession string // Session name in the request
		contextNamespace string // Namespace in the context metadata
		envNamespace     string // Value of wscommon.Namespace
		existingSessions []string
		expectError      bool
		errorContains    string
	}{
		{
			name:             "user accessing own session",
			requestedSession: "user1-session",
			contextNamespace: "user1-session",
			envNamespace:     "", // Not relevant for this test case
			existingSessions: []string{"user1-session", "user2-session", systemNamespace},
			expectError:      false,
		},
		{
			name:             "user accessing system namespace",
			requestedSession: systemNamespace,
			contextNamespace: "user1-session",
			envNamespace:     "user1-session",
			existingSessions: []string{"user1-session", systemNamespace},
			expectError:      true,
			errorContains:    "access denied",
		},
		{
			name:             "user accessing another user's session",
			requestedSession: "user2-session",
			contextNamespace: "user1-session",
			envNamespace:     "", // Not relevant for this test case
			existingSessions: []string{"user1-session", "user2-session", systemNamespace},
			expectError:      true,
			errorContains:    "access denied",
		},
		{
			name:             "system namespace user accessing own session",
			requestedSession: systemNamespace,
			contextNamespace: systemNamespace,
			envNamespace:     systemNamespace,
			existingSessions: []string{systemNamespace, "user1-session"},
			expectError:      false,
		},
		{
			name:             "system namespace user accessing another session",
			requestedSession: "user1-session",
			contextNamespace: systemNamespace,
			envNamespace:     systemNamespace,
			existingSessions: []string{systemNamespace, "user1-session"},
			expectError:      false, // Should pass because system user can access any namespace
		},
		{
			name:             "no context namespace",
			requestedSession: "user1-session",
			contextNamespace: "", // Empty namespace in context
			envNamespace:     "regular-namespace",
			existingSessions: []string{"user1-session", systemNamespace},
			expectError:      true,
			errorContains:    "access denied",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set the test environment namespace
			originalNamespace := wscommon.Namespace
			originalSystemNamespace := wscommon.SystemNamespace
			defer func() {
				wscommon.Namespace = originalNamespace
				wscommon.SystemNamespace = originalSystemNamespace
			}()

			wscommon.Namespace = tc.envNamespace

			// Setup namespace reservations for the test case
			existingSessions := make([]*nsapisv1.NamespaceReservation, 0, len(tc.existingSessions))
			for _, name := range tc.existingSessions {
				existingSessions = append(existingSessions, &nsapisv1.NamespaceReservation{
					ObjectMeta: metav1.ObjectMeta{Name: name},
				})
			}

			nsrCli := nsfake.NewSimpleClientset()
			for _, nsr := range existingSessions {
				_, err := nsrCli.NamespaceV1().NamespaceReservations().Create(ctx, nsr, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			// Create server
			k8s := fake.NewSimpleClientset()
			s := NewServer(k8s, nil, nsrCli.NamespaceV1().NamespaceReservations(), nil, nil,
				nil, wsclient.ServerOptions{SessionUpdateTimeoutSecs: 10}, nil)

			clusterConfig.AdjustNodesByClusterSetup()
			s.cfgProvider = pkg.StaticCfgProvider(*clusterConfig)

			// Create context with namespace
			testCtx := ctx
			if tc.contextNamespace != "" {
				md := metadata.New(map[string]string{
					pkg.ResourceNamespaceHeader: tc.contextNamespace,
				})
				testCtx = metadata.NewIncomingContext(ctx, md)
			}

			// Create request
			req := &csadm.GetSessionRequest{Name: tc.requestedSession}

			// Call GetSession
			res, err := s.GetSession(testCtx, req)

			// Verify results
			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
				require.Nil(t, res)
			} else {
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Equal(t, tc.requestedSession, res.Name)
			}
		})
	}
}

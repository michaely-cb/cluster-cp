//go:build !e2e

package csctl

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"
	commonpb "cerebras/pb/workflow/appliance/common"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	"cerebras.com/cluster/server/pkg/wsclient"
)

const testJobName = "wsjob-foobar"

func TestMapToTable(t *testing.T) {
	startTime, _ := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	endTime := startTime.Add(time.Hour * 30)
	now = func() time.Time { return endTime }
	defer func() { now = time.Now }()

	for _, testcase := range []struct {
		Name                     string
		Job                      *csv1.Job
		EnableIsolatedDashboards bool
		ExpectCells              []string
	}{
		{
			Name: "with user, short labels - global dashboard",
			Job: &csv1.Job{
				Meta: &csv1.ObjectMeta{
					CreateTime: timestamppb.New(startTime),
					Name:       "foo",
					Labels:     map[string]string{"foo": "bar", "user": "jenkins"},
					Properties: map[string]string{commonv1.ResourceReservationStatus: commonv1.ResourceReservedValue},
				},
				Spec: &csv1.JobSpec{
					User:     &csv1.JobUser{Username: "testuser", Uid: 11, Gid: 10},
					Type:     "compile",
					Priority: "P0 (3)",
				},
				Status: &csv1.JobStatus{
					Phase:          csv1.JobStatus_SUCCEEDED,
					Systems:        []string{"b", "a"},
					ExecutionTime:  timestamppb.New(startTime.Add(time.Hour * 10)),
					CompletionTime: timestamppb.New(startTime.Add(time.Hour * 20)),
				}},
			EnableIsolatedDashboards: false,
			ExpectCells: []string{"", "foo", "compile", "P0 (3)", "30h", "10h", "SUCCEEDED(RESERVED)", "", "testuser", "foo=bar,user=jenkins", "",
				"https://grafana.test.cerebras.com/d/WebHNShVz/wsjob-dashboard?orgId=1&var-wsjob=foo&from=1577872200000&to=1577909400000"},
		},
		{
			Name: "with user, short labels - isolated dashboards",
			Job: &csv1.Job{
				Meta: &csv1.ObjectMeta{
					CreateTime: timestamppb.New(startTime),
					Name:       "foo",
					Labels:     map[string]string{"foo": "bar", "user": "jenkins"},
				},
				Spec: &csv1.JobSpec{
					User:     &csv1.JobUser{Username: "testuser", Uid: 11, Gid: 10},
					Type:     "compile",
					Priority: "P0 (3)",
				},
				Status: &csv1.JobStatus{
					Phase:          csv1.JobStatus_SUCCEEDED,
					Systems:        []string{"b", "a"},
					ExecutionTime:  timestamppb.New(startTime.Add(time.Hour * 10)),
					CompletionTime: timestamppb.New(startTime.Add(time.Hour * 20)),
				}},
			EnableIsolatedDashboards: true,
			ExpectCells: []string{"", "foo", "compile", "P0 (3)", "30h", "10h", "SUCCEEDED", "", "testuser", "foo=bar,user=jenkins", "",
				"https://grafana.test.cerebras.com/d/job-operator-wsjob/wsjob-dashboard?orgId=1&var-wsjob=foo&from=1577872200000&to=1577909400000"},
		},
		{
			Name: "without, long labels",
			Job: &csv1.Job{
				Meta: &csv1.ObjectMeta{
					CreateTime: timestamppb.New(startTime),
					Name:       "bar",
					Labels:     map[string]string{"foo": "bar", "long": strings.Repeat("x", 60)},
				},
				Spec: &csv1.JobSpec{
					Priority: "P0 (3)",
				},
				Status: &csv1.JobStatus{
					ExecutionTime: timestamppb.New(startTime.Add(time.Hour * 10)),
					Phase:         csv1.JobStatus_RUNNING,
				}},
			EnableIsolatedDashboards: false,
			ExpectCells: []string{"", "bar", "", "P0 (3)", "30h", "20h", "RUNNING", "", "",
				"foo=bar,long=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...", "",
				"https://grafana.test.cerebras.com/d/WebHNShVz/wsjob-dashboard?orgId=1&var-wsjob=bar&from=1577872200000&to=now"},
		},
		{
			Name: "completed job, has duration",
			Job: &csv1.Job{
				Meta: &csv1.ObjectMeta{
					CreateTime: timestamppb.New(startTime),
					Name:       "bar",
					Namespace:  "test",
					Labels:     map[string]string{},
				},
				Spec: &csv1.JobSpec{
					Priority: "P0 (3)",
				},
				Status: &csv1.JobStatus{
					ExecutionTime:  timestamppb.New(startTime.Add(time.Hour * 2)),
					CompletionTime: timestamppb.New(startTime.Add(time.Hour * 5)),
					Phase:          csv1.JobStatus_FAILED,
				}},
			EnableIsolatedDashboards: false,
			ExpectCells: []string{"test", "bar", "", "P0 (3)", "30h", "3h", "FAILED", "", "", "", "",
				"https://grafana.test.cerebras.com/d/WebHNShVz/wsjob-dashboard?orgId=1&var-wsjob=bar&from=1577843400000&to=1577855400000"},
		},
		{
			Name: "not started job job, has 0s duration",
			Job: &csv1.Job{
				Meta: &csv1.ObjectMeta{
					CreateTime: timestamppb.New(startTime),
					Name:       "bar",
					Labels:     map[string]string{},
				},
				Spec: &csv1.JobSpec{
					Priority: "P0 (3)",
				},
				Status: &csv1.JobStatus{
					Phase:   csv1.JobStatus_QUEUED,
					Systems: make([]string, 2),
				}},
			EnableIsolatedDashboards: false,
			ExpectCells:              []string{"", "bar", "", "P0 (3)", "30h", "0s", "QUEUED", "(2) ", "", "", "", "job not started yet"},
		},
		{
			Name: "failed job not executed, has 0s duration",
			Job: &csv1.Job{
				Meta: &csv1.ObjectMeta{
					CreateTime: timestamppb.New(startTime),
					Name:       "bar",
					Labels:     map[string]string{},
				},
				Spec: &csv1.JobSpec{
					Priority: "P0 (3)",
				},
				Status: &csv1.JobStatus{
					CompletionTime: timestamppb.New(startTime.Add(time.Hour * 5)),
					Phase:          csv1.JobStatus_FAILED,
				}},
			EnableIsolatedDashboards: false,
			ExpectCells:              []string{"", "bar", "", "P0 (3)", "30h", "0s", "FAILED", "", "", "", "", "job not started yet"},
		},
	} {
		GrafanaUrl = "grafana.test.cerebras.com"
		js := &JobStore{enableIsolatedDashboards: testcase.EnableIsolatedDashboards}
		tableRow := js.mapJobTableRow(testcase.Job)
		assert.Equal(t, testcase.ExpectCells, tableRow.Cells)
	}
}

func TestMap(t *testing.T) {
	expectedReplicas := map[string]int{
		"coordinator":     1,
		"weight":          2,
		"activation":      2,
		"command":         1,
		"chief":           2,
		"worker":          4,
		"broadcastreduce": 12,
	}

	for _, testcase := range []struct {
		Name           string
		Job            *Job
		ExpectMounts   []*csv1.VolumeMount
		ExpectReplicas map[string]int
		ExpectWorkdir  string
	}{
		{
			Name: "with volumeMounts, no replicas",
			Job: &Job{
				wsjob: &wsapisv1.WSJob{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							wsclient.UserVolumeAnnotationKey:    `[{"name":"x","mountPath":"/foo","subPath":"bar"}]`,
							wsclient.ClientWorkdirAnnotationKey: "/n0/lab/tests/workdir-ts/model_dir/cerebras_logs",
						},
					},
				},
			},
			ExpectMounts:  []*csv1.VolumeMount{{Name: "x", MountPath: "/foo", SubPath: "bar"}},
			ExpectWorkdir: "/n0/lab/tests/workdir-ts/model_dir/cerebras_logs",
		},
		{
			Name: "with replicas - when pods are alive",
			Job: &Job{
				wsjob: mockJob(),
				pods: []corev1.Pod{
					mockPod("coordinator", "Running"),
					mockPod("weight", "Running"),
					mockPod("activation", "Running"),
					mockPod("command", "Running"),
					mockPod("chief", "Running"),
					mockPod("worker", "Succeeded"),
				},
				clusterDetailsConfigMap: mockConfigMap(t),
			},
			ExpectReplicas: expectedReplicas,
		},
		{
			Name: "with replicas - when pods are terminated",
			Job: &Job{
				wsjob:                   mockJob(),
				pods:                    []corev1.Pod{},
				clusterDetailsConfigMap: mockConfigMap(t),
			},
			ExpectReplicas: expectedReplicas,
		},
		{
			Name: "with replicas - when only some pods are terminated",
			Job: &Job{
				wsjob: mockJob(),
				pods: []corev1.Pod{
					mockPod("coordinator", "Running"),
					mockPod("chief", "Running"),
					mockPod("activation", "Running"),
					mockPod("command", "Running"),
				},
				clusterDetailsConfigMap: mockConfigMap(t),
			},
			ExpectReplicas: expectedReplicas,
		},
	} {
		job := MapJob(testcase.Job)
		assert.Len(t, job.Spec.VolumeMounts, len(testcase.ExpectMounts))
		for _, v := range job.Spec.VolumeMounts {
			assert.Contains(t, job.Spec.VolumeMounts, v)
		}
		assert.Equal(t, job.Spec.ClientWorkdir, testcase.ExpectWorkdir)

		if testcase.Job.wsjob.Spec.User != nil {
			assert.Equal(t, "testuser", job.Spec.User.Username)
			assert.Equal(t, int64(10), job.Spec.User.Uid)
			assert.Equal(t, int64(11), job.Spec.User.Gid)
		}

		assert.Len(t, job.Status.Replicas, len(testcase.ExpectReplicas))
		for rt, count := range testcase.ExpectReplicas {
			foundReplica := 0
			for _, r := range job.Status.Replicas {
				if r.Type == rt {
					assert.Len(t, r.Instances, count)
					foundReplica += 1
				}
			}
			assert.Equal(t, 1, foundReplica)
		}
	}
}

func TestNewJobList(t *testing.T) {
	// no filter
	jobList := newJobList(
		[]*wsapisv1.WSJob{mockJob()}, nil, &commonJobOptions{AllStates: true, Uid: -1},
	)
	assert.Len(t, jobList.items, 1)

	// filter by job id
	otherUserJobList := newJobList(
		[]*wsapisv1.WSJob{mockJob()}, nil, &commonJobOptions{AllStates: true, Uid: 1},
	)
	assert.Len(t, otherUserJobList.items, 0)

	// filter by non-ended status
	mj := mockJob()
	mj.Status = commonv1.JobStatus{
		Conditions: []commonv1.JobCondition{
			{
				Type:   "Succeeded",
				Status: "True",
			},
		},
	}
	completedStateJobList := newJobList(
		[]*wsapisv1.WSJob{mj}, nil, &commonJobOptions{AllStates: false, Uid: -1},
	)
	assert.Len(t, completedStateJobList.items, 0)

	// filter reservation status
	// parent will not show as inherited
	parent := mockJob()
	parent.Name = "parent"
	parent.Status = commonv1.JobStatus{
		Conditions: []commonv1.JobCondition{
			{
				Type:   "Succeeded",
				Status: "True",
			},
		},
	}
	parent.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReservedValue
	parent.Labels[wsapisv1.WorkflowIdLabelKey] = "w0"
	// child will show as active
	child := mockJob()
	child.Name = "child"
	child.Annotations[wsapisv1.ParentJobAnnotKey] = common.ReservingLockName(parent.Name)
	child.Labels[wsapisv1.WorkflowIdLabelKey] = "w0"
	child.Status = commonv1.JobStatus{
		Conditions: []commonv1.JobCondition{
			{
				Type:   commonv1.JobScheduled,
				Status: "True",
			},
		},
	}

	// parent1 will show as not inherited
	parent1 := mockJob()
	parent1.Name = "parent1"
	parent1.Status = commonv1.JobStatus{
		Conditions: []commonv1.JobCondition{
			{
				Type:   "Succeeded",
				Status: "True",
			},
		},
	}
	parent1.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReservedValue
	parent1.Labels[wsapisv1.WorkflowIdLabelKey] = "w1"

	// parent2 will not show as first compile with no execute
	parent2 := mockJob()
	parent2.Name = "parent2"
	parent2.Status = commonv1.JobStatus{
		Conditions: []commonv1.JobCondition{
			{
				Type:   "Succeeded",
				Status: "True",
			},
		},
	}
	parent2.Labels[wsapisv1.JobTypeLabelKey] = wsapisv1.JobTypeCompile
	parent2.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReservedValue
	parent2.Labels[wsapisv1.WorkflowIdLabelKey] = "w2"

	// parent3 will show as first compile with new execute
	parent3 := mockJob()
	parent3.Name = "parent3"
	parent3.Status = commonv1.JobStatus{
		Conditions: []commonv1.JobCondition{
			{
				Type:   "Succeeded",
				Status: "True",
			},
		},
	}
	parent3.Labels[wsapisv1.JobTypeLabelKey] = wsapisv1.JobTypeCompile
	parent3.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReservedValue
	parent3.Labels[wsapisv1.WorkflowIdLabelKey] = "w3"

	// exe3 will show as active
	exe3 := mockJob()
	exe3.Name = "exe3"
	exe3.Status = commonv1.JobStatus{
		Conditions: []commonv1.JobCondition{
			{
				Type:   commonv1.JobScheduled,
				Status: "True",
			},
		},
	}
	exe3.Labels[wsapisv1.JobTypeLabelKey] = wsapisv1.JobTypeExecute
	exe3.Labels[wsapisv1.WorkflowIdLabelKey] = "w3"

	jobList = newJobList(
		[]*wsapisv1.WSJob{parent, child, parent1, parent2, parent3, exe3},
		nil, &commonJobOptions{AllStates: false, Uid: -1},
	)
	jobNames := map[string]bool{}
	for _, it := range jobList.items {
		jobNames[it.wsjob.Name] = true
	}
	assert.Len(t, jobNames, 4)
	assert.Contains(t, jobNames, "child")
	assert.Contains(t, jobNames, "parent1")
	assert.Contains(t, jobNames, "parent3")
	assert.Contains(t, jobNames, "exe3")

}

func TestMaxJobsReturned(t *testing.T) {
	mockJobWithName := func(name string, creationTimestamp time.Time) *wsapisv1.WSJob {
		job := mockJob()
		job.ObjectMeta.Name = name
		job.ObjectMeta.Labels[kubeflowJobLabel] = name
		job.CreationTimestamp = metav1.Time{Time: creationTimestamp}
		return job
	}
	wsjobs := []*wsapisv1.WSJob{
		mockJobWithName("wsjob-foo", time.Now().Add(-time.Hour*3)),
		mockJobWithName("wsjob-bar", time.Now().Add(-time.Hour*2)),
		mockJobWithName("wsjob-baz", time.Now().Add(-time.Hour*1)),
	}

	options := &commonJobOptions{
		Limit: 2,
		Uid:   -1,
	}

	jobList := newJobList(wsjobs, nil, options)

	// Should only return 2 jobs (and they should be the last 2 created)
	assert.Equal(t, 2, len(jobList.items))
	assert.Equal(t, "wsjob-bar", jobList.items[0].wsjob.Name)
	assert.Equal(t, "wsjob-baz", jobList.items[1].wsjob.Name)
}

func TestSortOrder(t *testing.T) {
	mockJobWrapper := func(name string, creationTimestamp time.Time, jobType string,
		priority int, ended, reserved bool) *wsapisv1.WSJob {
		job := mockJob()
		job.ObjectMeta.Name = name
		job.CreationTimestamp = metav1.Time{Time: creationTimestamp}
		job.Spec.Priority = &priority
		job.ObjectMeta.Annotations = common.EnsureMap(job.ObjectMeta.Annotations)
		if reserved {
			job.ObjectMeta.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReservedValue
		}
		job.ObjectMeta.Labels = common.EnsureMap(job.ObjectMeta.Labels)
		job.ObjectMeta.Labels[wsapisv1.JobTypeLabelKey] = jobType
		if ended {
			job.Status = commonv1.JobStatus{
				Conditions: []commonv1.JobCondition{
					{
						Type:   commonv1.JobFailed,
						Status: corev1.ConditionTrue,
					},
				},
			}
		}
		return job
	}
	mockLock := func(name, queue string) *rlv1.ResourceLock {
		return &rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: rlv1.ResourceLockSpec{
				QueueName: queue,
			},
		}
	}

	wsjobs := []*wsapisv1.WSJob{
		mockJobWrapper("wsjob-a", time.Now().Add(-time.Hour*2), wsapisv1.JobTypeExecute,
			100, false, false),
		mockJobWrapper("wsjob-b", time.Now().Add(-time.Hour*1), wsapisv1.JobTypeExecute,
			90, false, false),
		mockJobWrapper("wsjob-c", time.Now().Add(-time.Hour*3), wsapisv1.JobTypeExecute,
			80, false, false),
		mockJobWrapper("wsjob-d", time.Now().Add(-time.Hour*4), wsapisv1.JobTypeExecute,
			200, true, false),
		mockJobWrapper("wsjob-e", time.Now().Add(-time.Hour*5), wsapisv1.JobTypeExecute,
			300, true, true),
	}

	locks := []*rlv1.ResourceLock{
		mockLock("wsjob-b", rlv1.PrioritizedExecuteQueueName),
	}

	// default return by highest priority first
	options := &commonJobOptions{Uid: -1}
	jobList := newJobList(wsjobs, locks, options)
	assert.Equal(t, 4, len(jobList.items))
	assert.Equal(t, "wsjob-e", jobList.items[0].wsjob.Name)
	assert.Equal(t, "wsjob-b", jobList.items[1].wsjob.Name)
	assert.Equal(t, "wsjob-c", jobList.items[2].wsjob.Name)
	assert.Equal(t, "wsjob-a", jobList.items[3].wsjob.Name)

	// all jobs, active/lowest priority first
	options = &commonJobOptions{Uid: -1, OrderBy: common.Ascending, AllStates: true}
	jobList = newJobList(wsjobs, locks, options)
	assert.Equal(t, 5, len(jobList.items))
	assert.Equal(t, "wsjob-a", jobList.items[0].wsjob.Name)
	assert.Equal(t, "wsjob-c", jobList.items[1].wsjob.Name)
	assert.Equal(t, "wsjob-b", jobList.items[2].wsjob.Name)
	assert.Equal(t, "wsjob-e", jobList.items[3].wsjob.Name)
	assert.Equal(t, "wsjob-d", jobList.items[4].wsjob.Name)

	// oldest job first
	options = &commonJobOptions{Uid: -1, SortBy: common.Age}
	jobList = newJobList(wsjobs, locks, options)
	assert.Equal(t, 4, len(jobList.items))
	assert.Equal(t, "wsjob-e", jobList.items[0].wsjob.Name)
	assert.Equal(t, "wsjob-c", jobList.items[1].wsjob.Name)
	assert.Equal(t, "wsjob-a", jobList.items[2].wsjob.Name)
	assert.Equal(t, "wsjob-b", jobList.items[3].wsjob.Name)

	// youngest job first
	options = &commonJobOptions{Uid: -1, SortBy: common.Age, OrderBy: common.Ascending, AllStates: true}
	jobList = newJobList(wsjobs, locks, options)
	assert.Equal(t, 5, len(jobList.items))
	assert.Equal(t, "wsjob-b", jobList.items[0].wsjob.Name)
	assert.Equal(t, "wsjob-a", jobList.items[1].wsjob.Name)
	assert.Equal(t, "wsjob-c", jobList.items[2].wsjob.Name)
	assert.Equal(t, "wsjob-d", jobList.items[3].wsjob.Name)
	assert.Equal(t, "wsjob-e", jobList.items[4].wsjob.Name)

}

func TestClusterDetailsMarshalUnmarshal(t *testing.T) {
	clusterDetails := &commonpb.ClusterDetails{
		Tasks: []*commonpb.ClusterDetails_TaskInfo{
			{
				TaskType: commonpb.ClusterDetails_TaskInfo_CRD,
				TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
					{
						TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
							WseIds: []int32{0},
							TaskId: 0,
						},
						AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
							WseIp:            "",
							TaskIpPort:       "wsjob-foobar-coordinator-0:9000",
							TaskDebugAddress: "",
						},
					},
				},
			},
		},
	}
	marshalOptions := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	serialized, err := marshalOptions.Marshal(clusterDetails)
	require.NoError(t, err)

	deserialized := &commonpb.ClusterDetails{}

	unmarshalOptions := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}
	err = unmarshalOptions.Unmarshal(serialized, deserialized)
	require.NoError(t, err)
	assert.Equal(t, clusterDetails, deserialized)
}

type MockStore struct {
	mock.Mock
	JobStore
}

func (m *MockStore) ListV2(ctx context.Context, namespace string, listOptions *pb.ListOptions) (Resource, error) {
	args := m.Called(ctx, namespace, listOptions)
	return args.Get(0).(Resource), args.Error(1)
}

type MockListJobsServer struct {
	mock.Mock
	grpc.ServerStream
}

func (m *MockListJobsServer) Send(response *pb.ListJobResponse) error {
	args := m.Called(response)
	return args.Error(0)
}

func (m *MockListJobsServer) Context() context.Context {
	return context.Background()
}

func generateDummyJobs(count int, t *testing.T) []*Job {
	jobs := make([]*Job, count)
	for i := range jobs {
		name := fmt.Sprintf("wsjob-%d", i)
		job := mockJob()
		job.ObjectMeta.Name = name
		job.ObjectMeta.Labels[kubeflowJobLabel] = name

		jobs[i] = &Job{
			wsjob:                   job,
			clusterDetailsConfigMap: mockConfigMap(t),
		}
	}
	return jobs
}

func TestListJobs_JSON(t *testing.T) {
	testCases := []int{0, 1, 999, 1000, 1001, 5000, 6750}
	for _, numJobs := range testCases {
		// Setup store.ListV2 to return mockResource
		mockStore := new(MockStore)
		mockJobList := &JobList{
			items: generateDummyJobs(numJobs, t),
		}
		mockStore.On("ListV2", mock.Anything, mock.Anything, mock.Anything).Return(mockJobList, nil)

		req := &pb.ListJobRequest{
			Options:        &pb.ListOptions{},
			Accept:         pb.SerializationMethod_JSON_METHOD,
			Representation: pb.ObjectRepresentation_OBJECT_REPRESENTATION,
		}
		numJobsPerChunk := maxNumJsonItemsPerChunk
		numChunks := (numJobs + numJobsPerChunk - 1) / numJobsPerChunk // ceil(numJobs/numJobsPerChunk)
		if numJobs == 0 {
			numChunks = 1
		}

		// Set up the server with the mock store, mock the stream
		server := &Server{
			stores: map[string]ResourceStore{"job": mockStore},
		}
		mockStream := new(MockListJobsServer)

		chunksReceived := make([]bool, numChunks)
		mockStream.On("Send", mock.MatchedBy(func(res *pb.ListJobResponse) bool {
			var jobList csv1.JobList
			if err := protojson.Unmarshal(res.Raw, &jobList); err != nil {
				t.Errorf("Failed to unmarshal response: %v", err)
				return false
			}

			if len(jobList.Items) == 0 {
				if numJobs == 0 {
					chunksReceived[0] = true
					return true
				}
				t.Errorf("Received empty job list")
				return false
			}

			actualName := jobList.Items[0].Meta.Name
			var jobIndex int
			if n, err := fmt.Sscanf(actualName, "wsjob-%d", &jobIndex); n != 1 || err != nil {
				t.Errorf("Job name doesn't match expected pattern: %s", actualName)
				return false
			}

			chunkIndex := jobIndex / numJobsPerChunk
			if chunkIndex < 0 || chunkIndex >= numChunks {
				t.Errorf("Invalid chunk index %d for job %s", chunkIndex, actualName)
				return false
			}

			// Verify expected chunk size
			expectedSize := numJobsPerChunk
			if chunkIndex == numChunks-1 && numJobs%numJobsPerChunk != 0 { // Last chunk, not divisible by chunkSize
				expectedSize = numJobs % numJobsPerChunk
			}
			if len(jobList.Items) != expectedSize {
				t.Errorf("Chunk %d: expected %d items, got %d", chunkIndex, expectedSize, len(jobList.Items))
				return false
			}

			chunksReceived[chunkIndex] = true
			return true
		})).Return(nil).Times(numChunks)

		err := server.ListJobs(req, mockStream)
		assert.NoError(t, err)

		// Verify all chunks were received
		for i := 0; i < numChunks; i++ {
			if !chunksReceived[i] {
				t.Errorf("Chunk %d was not received", i)
			}
		}
		mockStream.AssertExpectations(t)
		mockStore.AssertExpectations(t)
	}
}

func TestListJobs_Table(t *testing.T) {
	testCases := []int{0, 1, 9999, 10000, 10001, 50000, 67500}
	for _, numJobs := range testCases {
		// Setup store.ListV2 to return mockResource
		mockStore := new(MockStore)
		mockJobList := &JobList{
			items: generateDummyJobs(numJobs, t),
		}
		mockStore.On("ListV2", mock.Anything, mock.Anything, mock.Anything).Return(mockJobList, nil)

		req := &pb.ListJobRequest{
			Options:        &pb.ListOptions{},
			Accept:         pb.SerializationMethod_PROTOBUF_METHOD,
			Representation: pb.ObjectRepresentation_TABLE_REPRESENTATION,
		}
		numJobsPerChunk := maxNumRowsPerChunk
		numChunks := (numJobs + numJobsPerChunk - 1) / numJobsPerChunk // ceil(numJobs/numJobsPerChunk)
		if numJobs == 0 {
			numChunks = 1
		}

		// Set up the server with the mock store, mock the stream
		server := &Server{
			stores: map[string]ResourceStore{"job": mockStore},
		}
		mockStream := new(MockListJobsServer)

		chunksReceived := make([]bool, numChunks)
		mockStream.On("Send", mock.MatchedBy(func(res *pb.ListJobResponse) bool {
			var table csv1.Table
			if err := proto.Unmarshal(res.Raw, &table); err != nil {
				t.Errorf("Failed to unmarshal response: %v", err)
				return false
			}

			if len(table.Rows) == 0 {
				if numJobs == 0 {
					chunksReceived[0] = true
					return true
				}
				t.Errorf("Received empty job list")
				return false
			}

			actualName := table.Rows[0].Cells[1]
			var jobIndex int
			if n, err := fmt.Sscanf(actualName, "wsjob-%d", &jobIndex); n != 1 || err != nil {
				t.Errorf("Job name doesn't match expected pattern: %s", actualName)
				return false
			}

			chunkIndex := jobIndex / numJobsPerChunk
			if chunkIndex < 0 || chunkIndex >= numChunks {
				t.Errorf("Invalid chunk index %d for job %s", chunkIndex, actualName)
				return false
			}

			// Verify expected chunk size
			expectedSize := numJobsPerChunk
			if chunkIndex == numChunks-1 && numJobs%numJobsPerChunk != 0 { // Last chunk, not divisible by chunkSize
				expectedSize = numJobs % numJobsPerChunk
			}
			if len(table.Rows) != expectedSize {
				t.Errorf("Chunk %d: expected %d items, got %d", chunkIndex, expectedSize, len(table.Rows))
				return false
			}

			chunksReceived[chunkIndex] = true
			return true
		})).Return(nil).Times(numChunks)

		err := server.ListJobs(req, mockStream)
		assert.NoError(t, err)

		// Verify all chunks were received
		for i := 0; i < numChunks; i++ {
			if !chunksReceived[i] {
				t.Errorf("Chunk %d was not received", i)
			}
		}
		mockStream.AssertExpectations(t)
		mockStore.AssertExpectations(t)
	}
}

func mockConfigMap(t *testing.T) corev1.ConfigMap {
	clusterDetailsData, err := os.ReadFile("testdata/2_kapi_cluster_details_mock.json")
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

	return corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterDetailsCMPrefix + testJobName,
			Namespace: common.DefaultNamespace,
		},
		Data: map[string]string{
			wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.UpdatedSignalVal,
		},
		BinaryData: map[string][]byte{
			wsapisv1.ClusterDetailsConfigCompressedFileName: compressed,
		},
	}
}

func mockJob() *wsapisv1.WSJob {
	var replicaCount int32 = 1
	return &wsapisv1.WSJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testJobName,
			Annotations: make(map[string]string),
			Labels: map[string]string{
				kubeflowJobLabel: testJobName,
			},
		},
		Spec: wsapisv1.WSJobSpec{
			User: &wsapisv1.JobUser{
				Username: "testuser",
				Uid:      10,
				Gid:      11,
			},
			WSReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
				wsapisv1.WSReplicaTypeCoordinator: {
					Replicas: &replicaCount,
				},
				wsapisv1.WSReplicaTypeWeight: {
					Replicas: &replicaCount,
				},
				wsapisv1.WSReplicaTypeActivation: {
					Replicas: &replicaCount,
				},
				wsapisv1.WSReplicaTypeCommand: {
					Replicas: &replicaCount,
				},
				wsapisv1.WSReplicaTypeChief: {
					Replicas: &replicaCount,
				},
				wsapisv1.WSReplicaTypeWorker: {
					Replicas: &replicaCount,
				},
			},
		},
	}
}

func mockPod(kind, phase string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   fmt.Sprintf("wsjob-foobar-%s-0", kind),
			Labels: map[string]string{kubeflowJobLabel: testJobName},
		},
		Spec:   corev1.PodSpec{},
		Status: corev1.PodStatus{Phase: corev1.PodPhase(phase)},
	}
}

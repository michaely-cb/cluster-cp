package common

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

var pending = int32(16)
var running = int32(3)
var succeeded = int32(4)
var failed = int32(5)
var rtype = apiv1.ReplicaType("worker")
var pendingGracePeriod = 5 * time.Minute

func getMainContainerWaitingReason(index int32) string {
	var waitingReason string
	if index/2%4 == 0 {
		waitingReason = "CreateContainerConfigError"
	} else if index/2%4 == 1 {
		waitingReason = "CreateContainerError"
	} else if index/2%4 == 2 {
		waitingReason = "ImagePullBackOff"
	} else {
		waitingReason = "InvalidImageName"
	}
	return waitingReason
}

func TestAllRunningInitContainer(t *testing.T) {
	jobStatus := apiv1.JobStatus{}
	initializeReplicaStatuses(&jobStatus, rtype)
	rs, ok := jobStatus.ReplicaStatuses[rtype]
	// assert ReplicaStatus exists
	assert.True(t, ok)
	podStartTime := &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
	errorsSeen := setStatusForPendingPodsInitContainer(&jobStatus, podStartTime, 0)
	assert.Equal(t, 0, errorsSeen)
	assert.Equal(t, int32(16), rs.Init)
	assert.Equal(t, 0, len(rs.FailedPodStatuses))
}

func TestBeforeInitContainerGracePeriod(t *testing.T) {
	jobStatus := apiv1.JobStatus{}
	initializeReplicaStatuses(&jobStatus, rtype)
	rs, ok := jobStatus.ReplicaStatuses[rtype]
	// assert ReplicaStatus exists
	assert.True(t, ok)
	podStartTime := &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
	waitingPods := 4
	errorsSeen := setStatusForPendingPodsInitContainer(&jobStatus, podStartTime, waitingPods)
	assert.Equal(t, waitingPods, errorsSeen)
	assert.Equal(t, pending-int32(waitingPods), rs.Init)
	assert.Equal(t, 0, len(rs.FailedPodStatuses))
}

func TestAfterInitContainerGracePeriod(t *testing.T) {
	var expectedFailedPods []string
	var expectedFailedPodStatuses []apiv1.FailedPodStatus

	jobStatus := apiv1.JobStatus{}
	initializeReplicaStatuses(&jobStatus, rtype)
	rs, ok := jobStatus.ReplicaStatuses[rtype]
	// assert ReplicaStatus exists
	assert.True(t, ok)
	podStartTime := &metav1.Time{Time: time.Now().Add(-5 * time.Minute)}
	waitingPods := 4
	for i := 0; i < waitingPods; i++ {
		podName := fmt.Sprintf("%s-%d", rtype, i)
		expectedFailedPodStatuses = append(expectedFailedPodStatuses, apiv1.FailedPodStatus{
			Name:    podName,
			Message: "PodInitializing",
		})
		expectedFailedPods = append(expectedFailedPods, podName)
	}
	errorsSeen := setStatusForPendingPodsInitContainer(&jobStatus, podStartTime, waitingPods)
	assert.Equal(t, 0, errorsSeen)
	assert.Equal(t, pending-int32(waitingPods), rs.Init)
	assert.True(t, len(rs.FailedPodStatuses) > 0)
	for _, status := range rs.FailedPodStatuses {
		found := false
		for _, s := range expectedFailedPodStatuses {
			if s.Name == status.Name {
				found = true
				assert.Contains(t, status.Message, s.Message)
			}
		}
		assert.True(t, found)
	}
}

func TestBeforeMainContainerGracePeriod(t *testing.T) {
	var expectedFailedPods []string
	var expectedFailedPodStatuses []apiv1.FailedPodStatus

	jobStatus := apiv1.JobStatus{}
	initializeReplicaStatuses(&jobStatus, rtype)
	rs, ok := jobStatus.ReplicaStatuses[rtype]
	// assert ReplicaStatus exists
	assert.True(t, ok)
	podStartTime := &metav1.Time{Time: time.Now().Add(-15 * time.Minute)}
	initCompletionTime := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	// failed pods resulted from both unrecoverable and transient errors
	expectedFailedPods = nil
	expectedFailedPodStatuses = nil
	for i := int32(7); i < pending; i += 8 {
		podName := fmt.Sprintf("%s-%d", rtype, i)
		expectedFailedPodStatuses = append(expectedFailedPodStatuses, apiv1.FailedPodStatus{
			Name:    podName,
			Message: getMainContainerWaitingReason(i),
		})
		expectedFailedPods = append(expectedFailedPods, podName)
	}
	for i := int32(0); i < failed; i++ {
		podName := fmt.Sprintf("%s-%d", rtype, pending+running+succeeded+i)
		expectedFailedPodStatuses = append(expectedFailedPodStatuses, apiv1.FailedPodStatus{
			Name:    podName,
			Message: "pod failed",
		})
		expectedFailedPods = append(expectedFailedPods, podName)
	}
	errorsSeen := setStatusForPendingPodsMainContainer(&jobStatus, podStartTime, initCompletionTime, pending)
	assert.Equal(t, 6, errorsSeen)
	errorsSeen = setStatusForNonPendingPods(&jobStatus, podStartTime, running, succeeded, failed)
	assert.Equal(t, 0, errorsSeen)
	assert.Equal(t, int32(8), rs.Init)
	assert.Equal(t, int32(3), rs.Active)
	assert.Equal(t, int32(4), rs.Succeeded)
	assert.Equal(t, int32(7), rs.Failed)
	assert.True(t, len(rs.FailedPodStatuses) > 0)
	for _, status := range rs.FailedPodStatuses {
		found := false
		for _, s := range expectedFailedPodStatuses {
			if s.Name == status.Name {
				found = true
				assert.Contains(t, status.Message, s.Message)
			}
		}
		assert.True(t, found)
	}
}

func TestAfterMainContainerGracePeriod(t *testing.T) {
	var expectedFailedPods []string
	var expectedFailedPodStatuses []apiv1.FailedPodStatus

	jobStatus := apiv1.JobStatus{}
	initializeReplicaStatuses(&jobStatus, rtype)
	rs, ok := jobStatus.ReplicaStatuses[rtype]
	// assert ReplicaStatus exists
	assert.True(t, ok)
	podStartTime := &metav1.Time{Time: time.Now().Add(-15 * time.Minute)}
	initCompletionTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	// failed pods resulted from both unrecoverable and transient errors
	expectedFailedPods = nil
	expectedFailedPodStatuses = nil
	for i := int32(1); i < pending; i += 2 {
		podName := fmt.Sprintf("%s-%d", rtype, i)
		expectedFailedPodStatuses = append(expectedFailedPodStatuses, apiv1.FailedPodStatus{
			Name:    podName,
			Message: getMainContainerWaitingReason(i),
		})
		expectedFailedPods = append(expectedFailedPods, podName)
	}
	for i := int32(0); i < failed; i++ {
		podName := fmt.Sprintf("%s-%d", rtype, pending+running+succeeded+i)
		expectedFailedPodStatuses = append(expectedFailedPodStatuses, apiv1.FailedPodStatus{
			Name:    podName,
			Message: "pod failed",
		})
		expectedFailedPods = append(expectedFailedPods, podName)
	}
	errorsSeen := setStatusForPendingPodsMainContainer(&jobStatus, podStartTime, initCompletionTime, pending)
	assert.Equal(t, 0, errorsSeen)
	errorsSeen = setStatusForNonPendingPods(&jobStatus, podStartTime, running, succeeded, failed)
	assert.Equal(t, 0, errorsSeen)
	assert.Equal(t, int32(8), rs.Init)
	assert.Equal(t, int32(3), rs.Active)
	assert.Equal(t, int32(4), rs.Succeeded)
	assert.Equal(t, int32(13), rs.Failed)
	assert.True(t, len(rs.FailedPodStatuses) > 0)
	for _, status := range rs.FailedPodStatuses {
		found := false
		for _, s := range expectedFailedPodStatuses {
			if s.Name == status.Name {
				found = true
				assert.Contains(t, status.Message, s.Message)
			}
		}
		assert.True(t, found)
	}
}

func TestPodFailedStatusFiltering(t *testing.T) {
	var errorsSeen int
	jobStatus := &apiv1.JobStatus{}
	initializeReplicaStatuses(jobStatus, rtype)

	tt := metav1.Now()
	containerNameList := []string{"ws", "init", "ws", "ws", "ws", "init", "ws"}
	errorList := []string{"OOM", "error", "OOM", "error", "OOM", "error", "OOM"}
	exitCodeList := []int32{137, 1, 137, 13, 137, 15, 137}
	finishTimeList := []time.Duration{-5, -10, -3, -20, -15, -12, -1}
	expectedFailedPodId := []int{3, 4, 5}
	for i, name := range containerNameList {
		pod := resetPodStatus(int32(i), nil)
		pod.Status.Phase = corev1.PodFailed
		containerStatus := corev1.ContainerStatus{
			Name: name,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Reason:     errorList[i],
					ExitCode:   exitCodeList[i],
					FinishedAt: metav1.Time{Time: tt.Add(finishTimeList[i] * time.Second)},
				},
			},
		}
		if name == "init" {
			pod.Status.InitContainerStatuses = append(pod.Status.InitContainerStatuses, containerStatus)
		} else {
			pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, containerStatus)
		}
		if err := updateJobReplicaStatuses(nil, jobStatus, rtype, pod, pendingGracePeriod); err != nil {
			errorsSeen++
		}
	}

	assert.Equal(t, 0, errorsSeen)
	assert.Equal(t, 3, len(jobStatus.ReplicaStatuses[rtype].FailedPodStatuses))
	for i, status := range jobStatus.ReplicaStatuses[rtype].FailedPodStatuses {
		id := expectedFailedPodId[i]
		assert.Equal(t, fmt.Sprintf("%s-%d", rtype, id), status.Name)
		assert.Equal(t, containerNameList[id], status.Container)
		assert.Contains(t, status.Message, errorList[id])
		assert.Equal(t, exitCodeList[id], status.ExitCode)
		assert.Equal(t, tt.Add(finishTimeList[id]*time.Second), status.FinishedAt.Time)
	}
}

func setStatusForPendingPodsInitContainer(jobStatus *apiv1.JobStatus, podStartTime *metav1.Time, waiting int) int {
	errorsSeen := 0
	for i, id := 0, 0; i < int(pending); i, id = i+1, id+1 {
		pod := resetPodStatus(int32(id), podStartTime)
		pod.Status.Phase = corev1.PodPending
		if i < waiting {
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: "PodInitializing",
					},
				},
			}}
		} else {
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			}}
		}
		if err := updateJobReplicaStatuses(nil, jobStatus, rtype, pod, pendingGracePeriod); err != nil {
			errorsSeen += 1
		}
	}
	return errorsSeen
}

func setStatusForPendingPodsMainContainer(jobStatus *apiv1.JobStatus, podStartTime *metav1.Time, initCompletionTime v1.Time, pending int32) int {
	var i int32
	id := int32(0)
	var pod *corev1.Pod
	errorsSeen := 0
	for i = 0; i < pending; i, id = i+1, id+1 {
		pod = resetPodStatus(id, podStartTime)
		pod.Status.Phase = corev1.PodPending
		if i%2 == 0 {
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			}}
			if err := updateJobReplicaStatuses(nil, jobStatus, rtype, pod, pendingGracePeriod); err != nil {
				errorsSeen += 1
			}
			continue
		}

		waitingReason := getMainContainerWaitingReason(i)
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					FinishedAt: initCompletionTime,
				},
			},
		}}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "ws",
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason:  waitingReason,
					Message: waitingReason,
				},
			},
		}}
		if err := updateJobReplicaStatuses(nil, jobStatus, rtype, pod, pendingGracePeriod); err != nil {
			errorsSeen += 1
		}
	}

	return errorsSeen
}

func setStatusForNonPendingPods(jobStatus *apiv1.JobStatus, podStartTime *metav1.Time, running, succeeded, failed int32) int {
	var i int32
	id := pending
	var pod *corev1.Pod
	errorsSeen := 0
	for i = 0; i < running; i, id = i+1, id+1 {
		pod = resetPodStatus(id, podStartTime)
		pod.Status.Phase = corev1.PodRunning
		if err := updateJobReplicaStatuses(nil, jobStatus, rtype, pod, pendingGracePeriod); err != nil {
			errorsSeen += 1
		}
	}

	for i = 0; i < succeeded; i, id = i+1, id+1 {
		pod = resetPodStatus(id, podStartTime)
		pod.Status.Phase = corev1.PodSucceeded
		if err := updateJobReplicaStatuses(nil, jobStatus, rtype, pod, pendingGracePeriod); err != nil {
			errorsSeen += 1
		}
	}

	for i = 0; i < failed; i, id = i+1, id+1 {
		pod = resetPodStatus(id, podStartTime)
		pod.Status.Phase = corev1.PodFailed
		pod.Status.Message = "pod failed"
		if err := updateJobReplicaStatuses(nil, jobStatus, rtype, pod, pendingGracePeriod); err != nil {
			errorsSeen += 1
		}
	}

	return errorsSeen
}

func resetPodStatus(id int32, podStartTime *metav1.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: v1.ObjectMeta{
			Name: fmt.Sprintf("%s-%d", rtype, id),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ws"}},
		},
		Status: corev1.PodStatus{
			StartTime: podStartTime,
		},
	}
}

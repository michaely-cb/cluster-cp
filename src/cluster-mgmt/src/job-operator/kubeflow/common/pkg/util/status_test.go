package util

import (
	"testing"

	apiv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestIsSucceeded(t *testing.T) {
	jobStatus := apiv1.JobStatus{
		Conditions: []apiv1.JobCondition{
			{
				Type:   apiv1.JobSucceeded,
				Status: corev1.ConditionTrue,
			},
		},
	}
	assert.True(t, IsSucceeded(jobStatus))
}

func TestIsFailed(t *testing.T) {
	jobStatus := apiv1.JobStatus{
		Conditions: []apiv1.JobCondition{
			{
				Type:   apiv1.JobFailed,
				Status: corev1.ConditionTrue,
			},
		},
	}
	assert.True(t, IsFailed(jobStatus))
}

func TestUpdateJobConditions(t *testing.T) {
	type testCase struct {
		name           string
		podFailFirst   bool
		finalCondition apiv1.JobConditionType
	}

	testCases := []testCase{
		// Note: we don't have a case where the job fails without a pod failure
		{
			name:           "Job fails after pod failure",
			podFailFirst:   true,
			finalCondition: apiv1.JobFailed,
		},
		{
			name:           "Job cancelled before pod failure",
			podFailFirst:   false,
			finalCondition: apiv1.JobCancelled,
		},
		{
			name:           "Job cancelled after pod failure",
			podFailFirst:   true,
			finalCondition: apiv1.JobCancelled,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testUpdateJobConditions(t, tc.podFailFirst, tc.finalCondition)
		})
	}
}

func testUpdateJobConditions(t *testing.T, didPodFailFirst bool, finalCondition apiv1.JobConditionType) {
	assert.True(t, finalCondition == apiv1.JobFailed || finalCondition == apiv1.JobCancelled)

	jobStatus := apiv1.JobStatus{}
	conditionType := apiv1.JobCreated
	reason := "Job Created"
	message := "Job Created"

	err := UpdateJobConditions(&jobStatus, conditionType, reason, message)
	if assert.NoError(t, err) {
		// Check JobCreated condition is appended
		conditionInStatus := jobStatus.Conditions[0]
		assert.Equal(t, conditionInStatus.Type, conditionType)
		assert.Equal(t, conditionInStatus.Reason, reason)
		assert.Equal(t, conditionInStatus.Message, message)
	}

	conditionType = apiv1.JobRunning
	reason = "Job Running"
	message = "Job Running"
	err = UpdateJobConditions(&jobStatus, conditionType, reason, message)
	if assert.NoError(t, err) {
		// Check JobRunning condition is appended
		conditionInStatus := jobStatus.Conditions[1]
		assert.Equal(t, conditionInStatus.Type, conditionType)
		assert.Equal(t, conditionInStatus.Reason, reason)
		assert.Equal(t, conditionInStatus.Message, message)
	}

	conditionType = apiv1.JobRestarting
	reason = "Job Restarting"
	message = "Job Restarting"
	err = UpdateJobConditions(&jobStatus, conditionType, reason, message)
	if assert.NoError(t, err) {
		// Check JobRunning condition is filtered out and JobRestarting state is appended
		conditionInStatus := jobStatus.Conditions[1]
		assert.Equal(t, conditionInStatus.Type, conditionType)
		assert.Equal(t, conditionInStatus.Reason, reason)
		assert.Equal(t, conditionInStatus.Message, message)
	}

	conditionType = apiv1.JobRunning
	reason = "Job Running"
	message = "Job Running"
	err = UpdateJobConditions(&jobStatus, conditionType, reason, message)
	if assert.NoError(t, err) {
		// Again, Check JobRestarting condition is filtered and JobRestarting is appended
		conditionInStatus := jobStatus.Conditions[1]
		assert.Equal(t, conditionInStatus.Type, conditionType)
		assert.Equal(t, conditionInStatus.Reason, reason)
		assert.Equal(t, conditionInStatus.Message, message)
	}

	// If the pod failed first, we need to add the JobFailurePending condition
	if didPodFailFirst {
		conditionType = apiv1.JobFailing
		reason = "JobFailing"
		message = "Job failing"
		err = UpdateJobConditions(&jobStatus, conditionType, reason, message)
		if assert.NoError(t, err) {
			// Check JobFailurePending condition is appended
			conditionInStatus := jobStatus.Conditions[2]
			assert.Equal(t, conditionInStatus.Type, conditionType)
			assert.Equal(t, conditionInStatus.Reason, reason)
			assert.Equal(t, conditionInStatus.Message, message)
		}
	}

	conditionType = finalCondition
	if finalCondition == apiv1.JobFailed {
		reason = JobFailedReason
		message = "Job Failed"
	} else {
		reason = JobCancelledReason
		message = "Job Cancelled"
	}
	err = UpdateJobConditions(&jobStatus, conditionType, reason, message)
	if assert.NoError(t, err) {
		// Check JobRunning condition is set to false
		jobRunningCondition := jobStatus.Conditions[1]
		assert.Equal(t, jobRunningCondition.Type, apiv1.JobRunning)
		assert.Equal(t, jobRunningCondition.Status, corev1.ConditionFalse)

		// Check JobFailed/JobCancelled state is appended
		conditionInStatus := jobStatus.Conditions[len(jobStatus.Conditions)-1]
		assert.Equal(t, conditionInStatus.Type, conditionType)
		assert.Equal(t, conditionInStatus.Reason, reason)
		assert.Equal(t, conditionInStatus.Message, message)
	}
}

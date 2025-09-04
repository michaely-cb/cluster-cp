//go:build e2e

package cluster

import (
	"context"
	"path"
	"testing"
	"time"

	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"cerebras/pb/workflow/appliance/cluster_mgmt"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
)

func applyVolumesAndMounts(jobSpec *wsclient.JobSpec, withReplicaSubPath bool) {
	compileDirSubPath := "cached_compile"
	subPathExpr := path.Join(jobSpec.Namespace, jobSpec.JobName)
	if withReplicaSubPath {
		subPathExpr = path.Join(subPathExpr, "$(REPLICA_TYPE)-$(REPLICA_ID)")
	}
	jobSpec.WorkdirLogPath = path.Join(wsclient.WorkdirLogsRootPath, jobSpec.Namespace, jobSpec.JobName)
	jobSpec.WorkdirLogsVolume = wsclient.BuildHostPathVolume(wsapisv1.WorkdirVolume, wsclient.WorkdirLogsRootPath, v1.HostPathDirectory)
	jobSpec.WorkdirLogsVolumeMount = &v1.VolumeMount{
		Name:        wsapisv1.WorkdirVolume,
		MountPath:   path.Join(wsclient.WorkdirLogsRootPath, jobSpec.Namespace, jobSpec.JobName),
		SubPathExpr: subPathExpr,
	}
	jobSpec.CachedCompileVolume = wsclient.BuildHostPathVolume(wsapisv1.CachedCompileVolume, wsclient.CachedCompileRootPath, v1.HostPathDirectory)
	jobSpec.CachedCompileVolumeMount = &v1.VolumeMount{
		Name:      wsapisv1.CachedCompileVolume,
		MountPath: path.Join(wsclient.CachedCompileRootPath, jobSpec.Namespace, compileDirSubPath),
		SubPath:   path.Join(jobSpec.Namespace, compileDirSubPath),
	}
	jobSpec.TensorStorageVolume = wsclient.BuildHostPathVolume(wsapisv1.TensorStorageVolume, wsclient.TensorStorageRootHostPath, v1.HostPathDirectory)
	jobSpec.TensorStorageVolumeMount = &v1.VolumeMount{
		Name:      wsapisv1.TensorStorageVolume,
		MountPath: wsclient.TensorStorageRootHostPath,
	}
}

// This function is used to assert initial job status prior to lease renewal as well as job status after various lease renewal strategies.
// The timeout of 60 seconds is chosen to stabilize the initial job status check, but as a side effect relaxes the expectation towards
// how soon the job status should change when reacting to different lease renewal strategies. There is an opportunity here to tighten the
// timeout further for the second check, but would take some time to do testing to ensure tests are not flaky.
func assertWsJobStatusEventually(status cluster_mgmt.JobStatus) func(
	*testing.T, cluster_mgmt.ClusterManagementClient, string) *wsapisv1.WSJob {
	assertFn := func(t *testing.T, clusterClient cluster_mgmt.ClusterManagementClient, jobId string) *wsapisv1.WSJob {
		require.Eventually(t, func() bool {
			res, err := clusterClient.GetJob(context.Background(), &cluster_mgmt.GetJobRequest{JobId: jobId})
			if err != nil {
				t.Logf("Error getting job %s: %v", jobId, err)
				return false // Don't panic, return false to continue polling
			}
			t.Logf("%s current status: %s, expected status: %s", jobId, res.Status.String(), status.String())
			return res.Status == status
		}, 60*time.Second, 1*time.Second)
		job := &wsapisv1.WSJob{}
		err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: common.DefaultNamespace, Name: jobId}, job)
		require.NoError(t, err)
		return job
	}
	return assertFn
}

var assertWsJobFailEventually = assertWsJobStatusEventually(cluster_mgmt.JobStatus_JOB_STATUS_FAILED)
var assertWsJobCancelledEventually = assertWsJobStatusEventually(cluster_mgmt.JobStatus_JOB_STATUS_CANCELLED)
var assertWsJobRunningEventually = assertWsJobStatusEventually(cluster_mgmt.JobStatus_JOB_STATUS_IN_PROGRESS)
var assertWsJobFailingEventually = assertWsJobStatusEventually(cluster_mgmt.JobStatus_JOB_STATUS_FAILING)

func assertJobConditionTypeEventually(status commonv1.JobConditionType) func(*testing.T, string) *wsapisv1.WSJob {
	assertFn := func(t *testing.T, jobName string) *wsapisv1.WSJob {
		job := &wsapisv1.WSJob{}
		require.Eventually(t, func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: common.DefaultNamespace, Name: jobName}, job)
			if err != nil {
				t.Logf("Error getting job %s: %v", jobName, err)
				return false
			}
			conditions := job.Status.Conditions
			if len(conditions) == 0 {
				return false
			}
			t.Logf("%s job status %s, current time: %s", jobName, conditions[len(conditions)-1].Type, time.Now().UTC())
			return conditions[len(conditions)-1].Type == status
		}, 120*time.Second, 1*time.Second)
		return job
	}
	return assertFn
}

func assertFailedJobConditionTypeEventually(t *testing.T, jobName string, failedReplicaType commonv1.ReplicaType) *wsapisv1.WSJob {
	job := &wsapisv1.WSJob{}
	rv := assert.Eventually(t, func() bool {
		err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: common.DefaultNamespace, Name: jobName}, job)
		if err != nil {
			t.Logf("Error getting job %s: %v", jobName, err)
			return false
		}
		conditions := job.Status.Conditions
		if len(conditions) == 0 {
			return false
		}
		status := commonv1.JobFailed
		replicaStatusUpdated := true
		if string(failedReplicaType) != "" {
			replicaStatus := job.Status.ReplicaStatuses[failedReplicaType]
			if replicaStatus == nil || replicaStatus.Failed == 0 {
				replicaStatusUpdated = false
			}
		}
		t.Logf("%s job status %s, replicaStatusUpdated: %v, current time: %s", jobName, conditions[len(conditions)-1].Type, replicaStatusUpdated, time.Now().UTC())
		return conditions[len(conditions)-1].Type == status && replicaStatusUpdated
	}, 120*time.Second, 1*time.Second)
	if !rv {
		t.Logf("failed wsjob=\n%s", jsonify(job.Status))
		require.Fail(t, "failed to await replica failed status in time", "replicaType=%s", failedReplicaType)
	}
	return job
}

var assertRunningJobConditionEventually = assertJobConditionTypeEventually(commonv1.JobRunning)
var assertSucceededJobConditionEventually = assertJobConditionTypeEventually(commonv1.JobSucceeded)
var assertFailedJobConditionEventually = assertJobConditionTypeEventually(commonv1.JobFailed)

func awaitPodsSucceeded(t *testing.T, jobName string) {
	expectingCount := int(wsclient.NumWsServices) - 1
	var p = &v1.PodList{}
	require.Eventually(t, func() bool {
		err := k8sClient.List(context.Background(), p,
			client.InNamespace(common.DefaultNamespace),
			client.MatchingLabels{commonv1.JobNameLabel: jobName})
		if err != nil {
			t.Logf("Error listing pods for job %s: %v", jobName, err)
			return false
		}
		successPods := common.Filter(func(pod v1.Pod) bool { return pod.Status.Phase == v1.PodSucceeded }, p.Items)
		//t.Logf("expecting completed: %d, got: %d", len(successPods), expectingCount)
		return len(successPods) == expectingCount
	}, 120*time.Second, 1*time.Second)
}

func awaitJobSucceeded(t *testing.T, jobName string) {
	var j = &batchv1.Job{}
	require.Eventually(t, func() bool {
		err := k8sClient.Get(context.Background(),
			client.ObjectKey{Namespace: common.DefaultNamespace, Name: jobName}, j)
		if err != nil {
			t.Logf("Error getting job %s: %v", jobName, err)
			return false
		}
		if j.Status.Succeeded == 0 {
			return false
		}
		return true
	}, 60*time.Second, 1*time.Second)
}

func updateNodeSchedulable(t *testing.T, clientSet kubeclientset.Interface, unschedulable, shouldTest bool) {
	// skip if no need to test
	if !shouldTest {
		return
	}
	nodes, err := clientSet.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, nodes.Items, 1)
	node := nodes.Items[0]
	node.Spec.Unschedulable = unschedulable
	_, err = clientSet.CoreV1().Nodes().Update(context.TODO(), &node, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func getClusterServerIngress(namespace string) *networkingv1.Ingress {
	return &networkingv1.Ingress{
		ObjectMeta: ctrl.ObjectMeta{
			Name:      wsapisv1.ClusterServerIngressName,
			Namespace: namespace,
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{
					Host: "wsjob",
				},
			},
			TLS: []networkingv1.IngressTLS{
				{
					SecretName: "grpc-secret",
				},
			},
		},
	}
}

func assertClusterServerIngressSetup(t *testing.T, namespace string) {
	err := k8sClient.Create(context.Background(), getClusterServerIngress(namespace))
	if err != nil && apierrors.IsAlreadyExists(err) {
		err = nil
	}
	require.NoError(t, err)
}

func assertClusterServerIngressCleanup(t *testing.T, namespace string) {
	err := k8sClient.Delete(context.Background(), getClusterServerIngress(namespace))
	if err != nil && apierrors.IsNotFound(err) {
		err = nil
	}
	require.NoError(t, err)
}

func setOperatorVersion(t *testing.T, k8s kubernetes.Interface, version string) {
	clusterEnvConfigMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: pkg.ClusterEnvConfigMapName, Namespace: common.SystemNamespace},
		Data: map[string]string{
			"SEMANTIC_VERSION": version,
		},
	}
	_, err := k8s.CoreV1().ConfigMaps(common.DefaultNamespace).Create(context.Background(), clusterEnvConfigMap, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		cm, err := k8s.CoreV1().ConfigMaps(common.SystemNamespace).Get(context.Background(), pkg.ClusterEnvConfigMapName, metav1.GetOptions{})
		assert.NoError(t, err)

		cm.Data["SEMANTIC_VERSION"] = version
		_, err = k8s.CoreV1().ConfigMaps(common.SystemNamespace).Update(context.Background(), cm, metav1.UpdateOptions{})
		assert.NoError(t, err)
	} else {
		assert.NoError(t, err)
	}
}

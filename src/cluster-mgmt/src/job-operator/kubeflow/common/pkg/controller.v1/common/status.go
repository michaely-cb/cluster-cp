package common

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"

	apiv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/kubeflow/common/pkg/core"
)

// initializeReplicaStatuses initializes the ReplicaStatuses for replica.
func initializeReplicaStatuses(jobStatus *apiv1.JobStatus, rtype apiv1.ReplicaType) {
	core.InitializeReplicaStatuses(jobStatus, rtype)
}

// updateJobReplicaStatuses updates the JobReplicaStatuses according to the pod.
func updateJobReplicaStatuses(client clientset.Interface, jobStatus *apiv1.JobStatus, rtype apiv1.ReplicaType, pod *corev1.Pod,
	pendingGracePeriod time.Duration) *apiv1.RequeueAfterTTL {
	return core.UpdateJobReplicaStatuses(client, jobStatus, rtype, pod, pendingGracePeriod)
}

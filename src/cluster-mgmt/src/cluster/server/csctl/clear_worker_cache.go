package csctl

import (
	"context"
	"fmt"
	"time"

	"cerebras.com/job-operator/common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

func (s *Server) ClearWorkerCache(
	ctx context.Context,
	request *pb.ClearWorkerCacheRequest,
) (*pb.ClearWorkerCacheResponse, error) {
	namespace := pkg.GetNamespaceFromContext(ctx)
	jobName := fmt.Sprintf("clear-cache-%s", wsclient.GenUUID())
	mode := int32(0o755)
	retry := int32(0)

	// Sets the maximum active period to be 30min
	activeDeadlineSeconds := int64(60 * 30)
	// Delete a failed job after 1 hour if not deleted by admins earlier
	// (successful jobs are deleted immediately)
	ttlSecondsAfterFinished := int32(60 * 60 * 1)

	nodeSelector := map[string]string{
		"node-role.kubernetes.io/control-plane": "",
	}
	if s.nodeLister.AreNodesNamespaceLabeled() {
		nodeSelector[common.NamespaceLabelKey] = namespace
	}
	clearJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			BackoffLimit:            &retry,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:       "clear-worker-cache",
							Image:      wsapisv1.ContainerdImage,
							Command:    []string{"sh", "/opt/cerebras/script/clear-worker-cache.sh"},
							WorkingDir: "/",
							VolumeMounts: []corev1.VolumeMount{
								{
									MountPath: "/usr/bin/kubectl",
									Name:      "kubectl",
								},
								{
									MountPath: "/root/.ssh",
									Name:      "ssh",
								},
								{
									MountPath: "/opt/cerebras/script",
									Name:      "clear-worker-cache-script",
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "NAMESPACE",
									Value: namespace,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "ssh",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/root/.ssh",
								},
							},
						},
						{
							Name: "kubectl",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/usr/bin/kubectl",
								},
							},
						},
						{
							Name: "clear-worker-cache-script",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "clear-worker-cache-script",
									},
									DefaultMode: &mode,
								},
							},
						},
					},
					// use this account for listing node permission, can optimize to use a separate one
					ServiceAccountName: "cluster-server",
					NodeSelector:       nodeSelector,
					Tolerations: []corev1.Toleration{
						{
							Operator: corev1.TolerationOpEqual,
							Key:      "node-role.kubernetes.io/master",
							Effect:   corev1.TaintEffectNoSchedule,
						},
						{
							Operator: corev1.TolerationOpEqual,
							Key:      "node-role.kubernetes.io/control-plane",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	_, err := s.wsJobClient.Clientset.BatchV1().Jobs(namespace).Create(ctx,
		clearJob, metav1.CreateOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	var w watch.Interface
	if w, err = s.wsJobClient.Clientset.BatchV1().Jobs(namespace).Watch(ctx, metav1.ListOptions{
		Watch:         true,
		FieldSelector: "metadata.name=" + jobName,
	}); err != nil {
		return nil, err
	}
	defer w.Stop()
	timer := time.NewTimer(30 * time.Minute)
	defer timer.Stop()

	var latestJob *batchv1.Job
	done := false
	for {
		select {
		case event, ok := <-w.ResultChan():
			if !ok {
				return nil, grpcStatus.Errorf(codes.Internal, "Job created but failed to get update: %s", jobName)
			}
			job, ok := event.Object.(*batchv1.Job)
			if !ok {
				continue
			}
			latestJob = job
			if s.isJobCompleted(job) {
				log.Infof("clear cache job complete: %s", job.Name)
				done = true
				break
			}
		case <-timer.C:
			log.Warnf("Timed out waiting for cache clear job to complete")
			done = true
			break
		}
		if done {
			break
		}
	}

	if !s.isJobSuccess(latestJob) {
		logs, err := s.wsJobClient.GetJobLogs(ctx, namespace, jobName, 20)
		if err != nil {
			log.Warnf("Get job logs failed %v", err)
			return nil, grpcStatus.Errorf(codes.Internal, "Cache clear job fail and logs retrieval also failed: %s", err)
		}
		log.Warnf("Cache clear job failed: %s", logs)
		return nil, grpcStatus.Errorf(codes.Internal, "Cache clear job failed: \n%s", logs)
	} else {
		// delete job and pod if it succeeded
		bgDelete := metav1.DeletePropagationBackground
		delOptions := metav1.DeleteOptions{
			PropagationPolicy: &bgDelete,
		}
		err = s.wsJobClient.Clientset.BatchV1().Jobs(namespace).Delete(ctx, jobName, delOptions)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
	}

	return &pb.ClearWorkerCacheResponse{Message: "Worker caches cleared successfully"}, nil
}

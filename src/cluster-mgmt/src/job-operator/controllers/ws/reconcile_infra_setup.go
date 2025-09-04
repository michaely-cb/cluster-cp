package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	apiv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

func (r *WSJobReconciler) reconcileInfraSetup(ctx context.Context, wsjob *wsapisv1.WSJob) (bool, error) {
	if !wsjob.RequireInfraSetup() {
		return true, nil
	}

	logger := r.Log.WithValues(wsapisv1.Singular, wsjob.Name)
	phaseMetric := string(apiv1.JobBootstrapping)
	logger.Info("Job phase metric updated",
		"namespace", wsjob.Namespace,
		"status", phaseMetric)
	common.JobPhaseMetricAdded(wsjob.Namespace, wsjob.Name, phaseMetric)

	if wsjob.Annotations[wsapisv1.InfraSetupAnnot] != "" {
		return true, nil
	}

	pendingPodGracePeriod := 60 * time.Second
	if common.IsDev {
		pendingPodGracePeriod = 20 * time.Second
	}
	gitter := time.Second
	requeueTTL := &apiv1.RequeueAfterTTL{TTL: pendingPodGracePeriod + gitter}

	// idempotent logic for already created job
	job := batchv1.Job{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: wsjob.Namespace, Name: newInfraSetupJobName(wsjob)}, &job); err != nil && !apierrors.IsNotFound(err) {
		return false, err
	} else if err == nil {
		msg := "volumes could not be set up"
		if job.Status.CompletionTime != nil {
			// Once the annotation on wsjob is set, we would exit early in this reconcile function if request came next time.
			wsjob.Annotations[wsapisv1.InfraSetupAnnot] = strconv.FormatBool(true)
			return false, r.Client.Update(ctx, wsjob)
		} else if job.Status.Failed > 0 {
			errorMessage := ""
			jobPod, getErr := r.getInfraSetupJobPod(ctx, wsjob)
			if getErr != nil {
				return false, getErr
			}

			if jobPod != nil {
				errorMessage, _ = common.GetPodLogs(r.KubeClientSet, wsjob.Namespace, jobPod.Name, wsapisv1.DefaultInfraSetupContainerName, 1)
			}
			// only opportunistically retrieving the failure message
			if errorMessage != "" {
				msg = fmt.Sprintf("%s: %s", msg, errorMessage)
			}
			return false, CriticalErrorf(msg)
		} else if job.Status.StartTime != nil && time.Now().Sub(job.Status.StartTime.Time) > pendingPodGracePeriod {
			errorMessage := ""
			jobPod, getErr := r.getInfraSetupJobPod(ctx, wsjob)
			if getErr != nil {
				return false, getErr
			}
			// pod should not be stuck in pending state for this long
			if jobPod != nil && jobPod.Status.Phase == v1.PodPending {
				events := common.GetEvents(r.KubeClientSet, wsjob.Namespace, jobPod.Name, v1.EventTypeWarning)
				if len(events) > 0 {
					tokens := strings.Split(events[0].Message, "\n")
					errorMessage = tokens[len(tokens)-1]
					// error out on stuck pending pod
					return false, CriticalErrorf(fmt.Sprintf("%s: %s", msg, errorMessage))
				} else {
					// if pod was in pending state but there was no warning event, we'd give the benefit of doubt
					// and requeue to reevaluate
					logger.Info("pod status: %v", jobPod.Status)
					return false, &apiv1.RequeueAfterTTL{TTL: 5 * time.Second}
				}
			}
			// do not proceed when the job is still running
			return false, nil
		}
		// requeue when job start time not available, or pod is still in the pending grace period
		return false, requeueTTL
	}

	logger.Info("start creating infra setup job")
	_, err := r.KubeClientSet.BatchV1().Jobs(wsjob.Namespace).Create(ctx, makeInfraSetupJob(wsjob), metav1.CreateOptions{})
	if err != nil {
		logger.Error(err, "failed to create infra setup job")
		return false, err
	}
	// We should requeue with a TTL just in case we have a stuck pending pod
	return false, requeueTTL
}

func (r *WSJobReconciler) cleanupInfraSetup(ctx context.Context, wsjob *wsapisv1.WSJob) error {
	job := batchv1.Job{}
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: wsjob.Namespace, Name: newInfraSetupJobName(wsjob)}, &job)
	if err != nil && apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	} else if job.Annotations[wsapisv1.InfraSetupAnnot] != strconv.FormatBool(true) {
		// Proactively deleting those pods if found as part of the cleanup.
		jobPod, getErr := r.getInfraSetupJobPod(ctx, wsjob)
		if getErr != nil {
			return getErr
		}

		if jobPod != nil {
			return r.Client.Delete(ctx, jobPod)
		}
		job.Annotations = common.EnsureMap(job.Annotations)
		job.Annotations[wsapisv1.InfraSetupAnnot] = strconv.FormatBool(true)
		return r.Client.Update(ctx, &job)
	}
	return nil
}

func newInfraSetupJobName(wsjob *wsapisv1.WSJob) string {
	return fmt.Sprintf("%s-%s", wsjob.Name, wsapisv1.DefaultInfraSetupContainerName)
}

func makeInfraSetupJob(wsjob *wsapisv1.WSJob) *batchv1.Job {
	workdirRootDir := ""
	compileRootDir := ""
	relativeCompileDir := ""
	var volumes []v1.Volume
	var volumeMounts []v1.VolumeMount
	replicaMap := map[string]int{}
	trimLowerDir := func(input, lowerDir string) string {
		return path.Clean(strings.TrimSuffix(input, lowerDir))
	}

	for replicaType, val := range wsjob.Spec.WSReplicaSpecs {
		replicaMap[replicaType.Lower()] = int(*val.Replicas)

		// In rel-2.5, we only use infra setup for creating directories in NFS.
		// Since the job-level workdir will be the same for all replicas, just
		// parsing the coordinator will be sufficient.
		if !wsjob.IsIngressReplicaType(replicaType) {
			continue
		}
		for _, c := range val.Template.Spec.Containers {
			if c.Name != wsapisv1.DefaultContainerName {
				continue
			}
			for _, env := range c.Env {
				if env.Name != wsapisv1.RelativeCompileDirEnv {
					continue
				}
				relativeCompileDir = env.Value
				break
			}
			for _, vm := range c.VolumeMounts {
				if vm.Name == wsapisv1.WorkdirVolume && wsjob.Annotations[wsapisv1.WorkdirLogsMountDirAnnot] != "" {
					_vm := v1.VolumeMount{
						Name:      wsapisv1.WorkdirVolume,
						MountPath: trimLowerDir(vm.MountPath, path.Join(wsjob.Namespace, wsjob.Name)),
					}
					volumeMounts = append(volumeMounts, _vm)
					workdirRootDir = _vm.MountPath
				} else if vm.Name == wsapisv1.CachedCompileVolume && wsjob.Annotations[wsapisv1.CachedCompileMountDirAnnot] != "" {
					_vm := v1.VolumeMount{
						Name:      wsapisv1.CachedCompileVolume,
						MountPath: trimLowerDir(vm.MountPath, path.Join(wsjob.Namespace, relativeCompileDir)),
					}
					volumeMounts = append(volumeMounts, _vm)
					compileRootDir = _vm.MountPath
				}
			}
		}
		for _, v := range val.Template.Spec.Volumes {
			if v.Name == wsapisv1.WorkdirVolume && wsjob.Annotations[wsapisv1.WorkdirLogsMountDirAnnot] != "" {
				vCopy := *v.DeepCopy()
				if vCopy.NFS != nil {
					vCopy.NFS.Path = trimLowerDir(vCopy.NFS.Path, path.Join(wsjob.Namespace, wsjob.Name))
				} else if vCopy.HostPath != nil {
					// for test purposes only
					// hostpath volume would hang on a non-existing directory, in a similar fashion as in nfs
					directory := v1.HostPathDirectory
					vCopy.HostPath.Type = &directory
					vCopy.HostPath.Path = trimLowerDir(vCopy.HostPath.Path, path.Join(wsjob.Namespace, wsjob.Name))
				}
				volumes = append(volumes, vCopy)
			} else if v.Name == wsapisv1.CachedCompileVolume && wsjob.Annotations[wsapisv1.CachedCompileMountDirAnnot] != "" {
				vCopy := *v.DeepCopy()
				if vCopy.NFS != nil {
					vCopy.NFS.Path = trimLowerDir(vCopy.NFS.Path, path.Join(wsjob.Namespace, relativeCompileDir))
				} else if vCopy.HostPath != nil {
					// for test purposes only
					directory := v1.HostPathDirectory
					vCopy.HostPath.Type = &directory
					vCopy.HostPath.Path = trimLowerDir(vCopy.HostPath.Path, path.Join(wsjob.Namespace, relativeCompileDir))
				}
				volumes = append(volumes, vCopy)
			}
		}
	}

	replicaMapBytes, _ := json.Marshal(replicaMap)
	podTemplateSpec := &v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			SecurityContext: &v1.PodSecurityContext{
				RunAsUser:          common.Pointer(wsjob.Spec.User.Uid),
				RunAsGroup:         common.Pointer(wsjob.Spec.User.Gid),
				SupplementalGroups: wsjob.Spec.User.Groups,
			},
			Containers: []v1.Container{{
				Name:    wsapisv1.DefaultInfraSetupContainerName,
				Image:   fmt.Sprintf("%s/%s:%s", wsapisv1.RegistryUrl, wsapisv1.KubectlImageName, wsapisv1.KubectlImageTag),
				Command: []string{"/bin/bash", "-c", wsapisv1.PodInfraSetupScript},
				Env: []v1.EnvVar{
					{Name: "NAMESPACE", Value: wsjob.Namespace},
					{Name: "WSJOB_ID", Value: wsjob.Name},
					{Name: "WORKDIR_MOUNT_DIR_ROOT", Value: workdirRootDir},
					{Name: "COMPILE_MOUNT_DIR_ROOT", Value: compileRootDir},
					{Name: "RELATIVE_COMPILE_DIR", Value: relativeCompileDir},
					{Name: "REPLICA_MAP", Value: string(replicaMapBytes)},
				},
				VolumeMounts: volumeMounts,
			}},
			NodeSelector: map[string]string{
				common.NamespaceLabelKey: wsjob.Namespace,
				fmt.Sprintf(RoleLabelKeyF, wsapisv1.WSReplicaTypeCoordinator.Lower()): "",
			},
			Tolerations: []v1.Toleration{
				{
					Effect:   v1.TaintEffectNoSchedule,
					Key:      "node-role.kubernetes.io/master",
					Operator: v1.TolerationOpEqual,
				},
				{
					Effect:   v1.TaintEffectNoSchedule,
					Key:      "node-role.kubernetes.io/control-plane",
					Operator: v1.TolerationOpEqual,
				},
			},
			Volumes:       volumes,
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	common.DedupeMountDirVolumes(podTemplateSpec)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: wsjob.Namespace,
			Name:      newInfraSetupJobName(wsjob),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "jobs.cerebras.com/v1",
					BlockOwnerDeletion: common.Pointer(true),
					Controller:         common.Pointer(true),
					Kind:               "WSJob",
					Name:               wsjob.Name,
					UID:                wsjob.ObjectMeta.UID,
				},
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: common.Pointer(int32(1800)),
			ActiveDeadlineSeconds:   common.Pointer(int64(300)),
			// any failure should be fatal and retries should not help
			BackoffLimit: common.Pointer(int32(0)),
			Template:     *podTemplateSpec,
		},
	}
	return job
}

func (r *WSJobReconciler) getInfraSetupJobPod(ctx context.Context, wsjob *wsapisv1.WSJob) (*v1.Pod, error) {
	podList := &v1.PodList{}
	err := r.Client.List(ctx, podList, client.InNamespace(wsjob.Namespace), client.MatchingLabels{"job-name": newInfraSetupJobName(wsjob)})

	if err != nil {
		logger := r.Log.WithValues(wsapisv1.Singular, wsjob.Name)
		logger.Error(err, "failed to list infra setup job pods")
		return nil, err
	} else if len(podList.Items) > 0 {
		return &podList.Items[0], nil
	}
	return nil, nil
}

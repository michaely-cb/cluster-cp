package wsclient

import (
	"crypto/md5"
	"fmt"
	"strconv"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
)

func logExportJob(
	req *pb.CreateLogExportRequest,
	jobSpec *JobSpec,
) *batchv1.Job {
	mode := int32(0o755)
	// Purposed only for backward compatibility
	// Old csctl client would not provide the timeout, we default the timeout to 1 hour in those cases
	defaultTimeoutSeconds := int64(3600)
	if req.TimeoutSeconds == 0 {
		req.TimeoutSeconds = defaultTimeoutSeconds
	}

	// retry will happen internally
	retry := int32(0)
	ttlSecondsAfterFinished := int32(60 * 60 * 24 * 1)
	activeDeadlineSeconds := req.TimeoutSeconds
	jobName := "log-export-" + req.Name
	options := strconv.FormatInt(req.TimeoutSeconds, 10)
	if req.Binaries {
		options += "-bin"
	}
	if req.IncludeCompileArtifacts {
		options += "-compile"
	}
	if req.SkipClusterLogs {
		options += "-skipcluster"
	}
	if req.NumLines > 0 {
		ttlSecondsAfterFinished = int32(15)
		options += "-tail"
	}
	if req.Tasks != "" {
		options += req.Tasks
	}
	if req.CopyNfsLogs {
		options += "-copynfs"
	}
	if req.ExportTargetType == pb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME {
		options += "-debug"
	}
	if req.ExportTargetType == pb.ExportTargetType_EXPORT_TARGET_TYPE_NFS {
		options += fmt.Sprintf("-%s", req.OutputPath)
	}
	if options != "" {
		// "options" input can be quite long so we are using hashing to create a shorter string
		// This should bring very little (practically none) risk in name collision to the export.
		fullHash := fmt.Sprintf("%x", md5.Sum([]byte(options)))
		// Truncate to the first 8 bytes and encode to base64
		shortHash := fullHash[:8]
		jobName += "-" + shortHash
		log.Infof("Log export job name: %s, options: %s", jobName, options)
	}

	var securityContext *corev1.PodSecurityContext
	var initContainers []corev1.Container
	var volumes []corev1.Volume

	volumeMounts := []corev1.VolumeMount{
		{
			MountPath: "/usr/bin/kubectl",
			Name:      "kubectl",
		},
		{
			MountPath: "/n0/cluster-mgmt",
			Name:      "cluster-mgmt-log-volume",
		},
		{
			MountPath: "/opt/cerebras/script",
			Name:      "log-export-script",
		},
		*jobSpec.LogExportStagingVolumeMount,
		*jobSpec.CachedCompileVolumeMount,
		*jobSpec.WorkdirLogsVolumeMount,
	}

	if req.ExportTargetType == pb.ExportTargetType_EXPORT_TARGET_TYPE_NFS {
		sshPath := "/home/myuser/.ssh"
		securityContext = &corev1.PodSecurityContext{
			RunAsUser:          common.Pointer(jobSpec.JobUser.Uid),
			RunAsGroup:         common.Pointer(jobSpec.JobUser.Gid),
			SupplementalGroups: jobSpec.JobUser.Groups,
		}

		initCmd := []string{
			"sh", "-c", fmt.Sprintf(`
				mkdir -p %[1]s &&
				cp -r /host_root/.ssh/* %[1]s &&
				chown -R %[2]d:%[3]d %[1]s &&
				chmod -R 700 %[1]s &&
				rm -f %[1]s/known_hosts &&
				groupadd -g %[3]d mygroup &&
				useradd -u %[2]d -g %[3]d -G mygroup myuser &&
				cp /etc/passwd /tmp/staged-passwd
			`, sshPath, jobSpec.JobUser.Uid, jobSpec.JobUser.Gid),
		}

		initContainers = append(initContainers, corev1.Container{
			Name:            "log-export-init",
			Image:           wsapisv1.ContainerdImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         initCmd,
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "ssh",
					MountPath: sshPath,
				},
				{
					Name:      "ssh-host",
					MountPath: "/host_root/.ssh",
					ReadOnly:  true,
				},
				{
					// Mounting the passwd file to ensure the user is created with the corresponding UID and GID
					// Necessary for NFS mount dir target where the host root is not accessible
					Name:      "passwd",
					MountPath: "/tmp/staged-passwd",
				},
			},

			// Running as root to ensure we can mount the host_root SSH keys and passwd file to main container
			SecurityContext: &corev1.SecurityContext{
				RunAsUser:  common.Pointer(int64(0)),
				RunAsGroup: common.Pointer(int64(0)),
			},
		})

		// NFS-specific volumes and volume mounts
		volumes = []corev1.Volume{
			{
				Name: "ssh",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
			{
				Name: "ssh-host",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/root/.ssh",
					},
				},
			},
			{
				Name: "passwd",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}
		// Extra volume mounts for NFS
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{
				Name:      "passwd",
				MountPath: "/etc/passwd",
				SubPath:   "passwd",
			},
			corev1.VolumeMount{
				MountPath: sshPath,
				Name:      "ssh",
				ReadOnly:  true,
			},
		)
	} else {
		// Default volumes and volume mounts for other types
		volumes = []corev1.Volume{
			{
				Name: "ssh",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/root/.ssh",
					},
				},
			},
		}

		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{
				MountPath: "/host_root/.ssh",
				Name:      "ssh",
				ReadOnly:  true,
			},
		)
	}

	additionalVolumes := []corev1.Volume{
		{
			Name: "kubectl",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/usr/bin/kubectl",
				},
			},
		},
		{
			Name: "cluster-mgmt-log-volume",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/n0/cluster-mgmt",
				},
			},
		},
		{
			Name: "log-export-script",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "log-export-script",
					},
					DefaultMode: &mode,
				},
			},
		},
		*jobSpec.LogExportStagingVolume,
		*jobSpec.CachedCompileVolume,
		*jobSpec.WorkdirLogsVolume,
	}

	volumes = append(volumes, additionalVolumes...)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: jobSpec.Namespace,
			Labels: map[string]string{
				wsapisv1.LogExportAppLabelKey: wsapisv1.LogExportAppLabelValue,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			BackoffLimit:            &retry,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						wsapisv1.LogExportAppLabelKey: wsapisv1.LogExportAppLabelValue,
					},
				},
				Spec: corev1.PodSpec{
					SecurityContext: securityContext,
					InitContainers:  initContainers,
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: map[corev1.ResourceName]resource.Quantity{
									"cpu":    resource.MustParse("1"),
									"memory": resource.MustParse("500Mi"),
								},
							},
							Name:            "log-export",
							Image:           wsapisv1.ContainerdImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"bash", "/opt/cerebras/script/log-export.sh"},
							Env: []corev1.EnvVar{
								{
									Name:  "JOB_ID",
									Value: req.Name,
								},
								{
									Name:  "EXPORT_ID",
									Value: jobName,
								},
								{
									Name:  "EXPORT_LOG_PATH",
									Value: jobSpec.LogExportStagingVolumeMount.MountPath,
								},
								{
									Name:  "CLUSTER_MGMT_LOG_PATH",
									Value: "/n0/cluster-mgmt",
								},
								{
									Name:  "SYSTEM_NAMESPACE",
									Value: common.SystemNamespace,
								},
								{
									Name:  "WITH_BINARY",
									Value: strconv.FormatBool(req.Binaries),
								},
								{
									Name:  "WITH_COMPILE_ARTIFACTS",
									Value: strconv.FormatBool(req.IncludeCompileArtifacts),
								},
								{
									Name:  "COPY_NFS_LOGS",
									Value: strconv.FormatBool(req.CopyNfsLogs),
								},
								{
									Name:  "SKIP_CLUSTER_LOGS",
									Value: strconv.FormatBool(req.SkipClusterLogs),
								},
								{
									Name:  "NUM_LINES",
									Value: strconv.FormatInt(req.NumLines, 10),
								},
								{
									Name:  "USE_DEBUG_VOLUME",
									Value: strconv.FormatBool(req.ExportTargetType == pb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME),
								},
								{
									Name:  "USE_MOUNT_DIR_TARGET",
									Value: strconv.FormatBool(req.ExportTargetType == pb.ExportTargetType_EXPORT_TARGET_TYPE_NFS),
								},
								{
									Name:  "TASK_FILTER",
									Value: req.Tasks,
								},
								{
									Name: "NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
							},
							VolumeMounts: volumeMounts,
						},
					},
					Volumes:            volumes,
					ServiceAccountName: "log-export",
					NodeSelector:       jobSpec.LogExportNodeSelector,
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
	return job
}

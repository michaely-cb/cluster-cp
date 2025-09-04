package wsclient

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

func buildImage(jobSpec *JobSpec, baseImage, pipOptions string, frozenDependencies []string, nodeSelector map[string]string) *batchv1.Job {
	destinationImage := GetDestinationImage(baseImage, pipOptions, frozenDependencies)
	imageSuffix := calculateImageTagSuffix(pipOptions, frozenDependencies)
	log.Infof("Base image: %s, destination image: %s", baseImage, destinationImage)

	// Retries a failed job once
	// The image build job could error out on transient PyPI errors during downloading packages
	retry := int32(1)
	// Sets the maximum active period to be an hour
	activeDeadlineSeconds := int64(60 * 60 * 1)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   jobSpec.Namespace,
			Name:        jobSpec.JobName,
			Annotations: getAnnotations(baseImage, destinationImage, imageSuffix, jobSpec.JobUser.Username),
			Labels: map[string]string{
				wsapisv1.ImageBuilderAppLabelKey: wsapisv1.ImageBuilderAppLabelValue,
				"job-name":                       jobSpec.JobName,
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &activeDeadlineSeconds,
			BackoffLimit:          &retry,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: getAnnotations(baseImage, destinationImage, imageSuffix, jobSpec.JobUser.Username),
					Labels: map[string]string{
						wsapisv1.ImageBuilderAppLabelKey: wsapisv1.ImageBuilderAppLabelValue,
					},
				},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector: &metav1.LabelSelector{
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      wsapisv1.ImageBuilderAppLabelKey,
													Operator: metav1.LabelSelectorOpIn,
													Values:   []string{wsapisv1.ImageBuilderAppLabelValue},
												},
											},
										},
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
					},
					InitContainers: []corev1.Container{{
						Name:            "dockerfile-gen",
						Image:           wsapisv1.ContainerdImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Args:            []string{"/bin/sh", "-c", path.Join(wsapisv1.ImageBuilderScriptRootDir, "generate-dockerfile.sh")},
						Env:             getCommonEnvVars(baseImage, pipOptions, frozenDependencies),
						VolumeMounts: []corev1.VolumeMount{
							*jobSpec.WorkdirLogsVolumeMount,
							getContainerdCertVolumeMount(),
						},
						Resources:  getResourceRequirements(),
						WorkingDir: jobSpec.WorkdirLogsVolumeMount.MountPath,
					}},
					Containers: []corev1.Container{
						{
							Name:            "main",
							Image:           wsapisv1.ContainerdImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command: []string{
								fmt.Sprintf("%s/build-image.sh", wsapisv1.ImageBuilderScriptRootDir),
							},
							Env: getCommonEnvVars(baseImage, pipOptions, frozenDependencies),
							VolumeMounts: []corev1.VolumeMount{
								*jobSpec.WorkdirLogsVolumeMount,
								getContainerdCertVolumeMount(),
								getRegistrySecretVolumeMount(),
							},
							Resources:  getResourceRequirements(),
							WorkingDir: jobSpec.WorkdirLogPath,
						},
					},
					Volumes:            getVolumes(jobSpec),
					ServiceAccountName: wsapisv1.ImageBuilderServiceAccount,
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
}

func GetDestinationImage(baseImage, pipOptions string, frozenDependencies []string) string {
	destinationImage := fmt.Sprintf("%s/%s:%s",
		wsapisv1.RegistryUrl,
		os.Getenv("IMAGE_BUILDER_DESTINATION_IMAGE_NAME"),
		GetDestinationImageTag(baseImage, pipOptions, frozenDependencies),
	)
	return destinationImage
}

func GetDestinationImageTag(baseImage, pipOptions string, frozenDependencies []string) string {
	imageTagSuffix := calculateImageTagSuffix(pipOptions, frozenDependencies)
	tokens := strings.Split(baseImage, ":")
	baseImageTag := tokens[len(tokens)-1]
	return fmt.Sprintf("%s-%s", baseImageTag, imageTagSuffix)
}

func GenImageBuildJobName(baseImage, pipOptions string, frozenDependencies []string) string {
	imageTag := GetDestinationImageTag(baseImage, pipOptions, frozenDependencies)
	hash := md5.Sum([]byte(imageTag))
	hashedImageTag := hex.EncodeToString(hash[:])
	return fmt.Sprintf("imgjob-%s", hashedImageTag)
}

func serializePackageDeps(frozenDependencies []string) string {
	copiedDeps := make([]string, len(frozenDependencies))
	copy(copiedDeps, frozenDependencies)
	sort.Strings(copiedDeps)

	serialized := ""
	for _, dep := range copiedDeps {
		serialized += fmt.Sprintf("\"%s\",", dep)
	}
	// Remove last comma if "serialized" is not empty
	if len(serialized) > 0 {
		serialized = serialized[:len(serialized)-1]
	}
	serialized = fmt.Sprintf("[%s]", serialized)
	return serialized
}

func calculateImageTagSuffix(pipOptions string, frozenDependencies []string) string {
	serializedPackageDeps := serializePackageDeps(frozenDependencies)
	packageDepsHash := fmt.Sprintf("%x", md5.Sum([]byte(pipOptions+serializedPackageDeps)))
	return packageDepsHash
}

func getAnnotations(baseImage, destinationImage, imageSuffix, username string) map[string]string {
	return map[string]string{
		wsapisv1.ImageBuilderBaseImageAnnotKey:        baseImage,
		wsapisv1.ImageBuilderDestinationImageAnnotKey: destinationImage,
		wsapisv1.ImageBuilderImageSuffixAnnotKey:      imageSuffix,
		wsapisv1.ImageBuilderUserAnnotKey:             username,
	}
}

func getCommonEnvVars(baseImage, pipOptions string, frozenDependencies []string) []corev1.EnvVar {
	serializedPackageDeps := serializePackageDeps(frozenDependencies)
	imageTagSuffix := calculateImageTagSuffix(pipOptions, frozenDependencies)
	return []corev1.EnvVar{
		{
			Name:  "BASE_IMAGE",
			Value: baseImage,
		},
		{
			Name:  "PRIVATE_REGISTRY_SERVER_HOSTNAME",
			Value: wsapisv1.RegistryUrl,
		},
		{
			Name:  "DESTINATION_IMAGE_NAME",
			Value: os.Getenv("IMAGE_BUILDER_DESTINATION_IMAGE_NAME"),
		},
		{
			Name:  "DESTINATION_IMAGE_TAG_SUFFIX",
			Value: imageTagSuffix,
		},
		{
			Name:  "PIP_OPTIONS",
			Value: pipOptions,
		},
		{
			Name:  "DEPENDENCY_LIST",
			Value: serializedPackageDeps,
		},
		{
			Name:  "ALPINE_CONTAINERD_TAG",
			Value: wsapisv1.ContainerdImageTag,
		},
		{
			Name:  "SHELL_XTRACE",
			Value: os.Getenv("SHELL_XTRACE"),
		},
		{
			Name:  "CTR_DEBUG",
			Value: os.Getenv("CTR_DEBUG"),
		},
		{
			Name:  "KANIKO_LOG_LEVEL",
			Value: os.Getenv("IMAGE_BUILDER_KANIKO_LOG_LEVEL"),
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
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
	}
}

func getContainerdCertVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      wsapisv1.ContainerdCertsConfigVolume,
		MountPath: wsapisv1.ContainerdCertsConfigDir,
		ReadOnly:  true,
	}
}

func getRegistrySecretVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      wsapisv1.RegistrySecretVolume,
		MountPath: wsapisv1.RegistryCertPath,
		ReadOnly:  true,
	}
}

func getVolumes(jobSpec *JobSpec) []corev1.Volume {
	fileType := corev1.HostPathFile
	directoryType := corev1.HostPathDirectory
	socketType := corev1.HostPathSocket
	return []corev1.Volume{
		*jobSpec.WorkdirLogsVolume,
		{
			Name: wsapisv1.ContainerdCertsConfigVolume,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: wsapisv1.ContainerdCertsConfigDir,
					Type: &directoryType,
				},
			},
		},
		{
			Name: wsapisv1.ContainerdSocketVolume,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: wsapisv1.ContainerdSocketPath,
					Type: &socketType,
				},
			},
		},
		{
			Name: wsapisv1.RegistrySecretVolume,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: wsapisv1.RegistryCertPath,
					Type: &fileType,
				},
			},
		},
	}
}

// Prior to rel-2.0.0, the limit was set to 3Gi.
// In rel-2.0.0, since the gpu version of torch is installed on the user venv,
// we need the bump the memory limit to at least 5Gi for not hitting an OOM.
// Setting it to 6Gi to leave some buffer room.
func getResourceRequirements() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			"memory": resource.MustParse("6Gi"),
		},
		Requests: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse("2"),
			"memory": resource.MustParse("6Gi"),
		},
	}
}

//go:build default

package common

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

func TestDedupeMountDirVolumes(t *testing.T) {
	cmVolume := corev1.Volume{
		Name: wsapisv1.ClusterDetailsConfigVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: wsapisv1.ClusterDetailsConfigName,
				},
			},
		},
	}
	highV1 := corev1.Volume{
		Name: "high-v1",
		VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server: "x.x.x.x",
				Path:   "/tests1",
			},
		},
	}
	highV1Copy := *highV1.DeepCopy()
	highV1Copy.Name = "high-v1-copy"
	highV2 := corev1.Volume{
		Name: "high-v2",
		VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server: "y.y.y.y",
				Path:   "/tests2",
			},
		},
	}
	highV2LowV3 := corev1.Volume{
		Name: "high-v2-low-v3",
		VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server: "y.y.y.y",
				Path:   "/tests2/lower",
			},
		},
	}
	highV2LowV4 := corev1.Volume{
		Name: "high-v2-low-v4",
		VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server: "y.y.y.y",
				Path:   "/tests2/another-lower",
			},
		},
	}
	highV2LowV5 := corev1.Volume{
		Name: "high-v2-low-v5",
		VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server: "y.y.y.y",
				Path:   "/tests2/another-lower/user-venv/venv-1719-247",
			},
		},
	}
	subpathV1 := corev1.Volume{
		Name: "subpath-v1",
		VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server: "x.x.x.x",
				Path:   "/testsx",
			},
		},
	}
	hostPathHigherVol := corev1.Volume{
		Name: "hostpath-higher",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/n1/user-venv",
			},
		},
	}
	hostPathLowerVol := corev1.Volume{
		Name: "hostpath-lower",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/n1/user-venv/venv-1719-247",
			},
		},
	}
	cmVolumeMount := corev1.VolumeMount{
		Name:      wsapisv1.ClusterDetailsConfigVolume,
		MountPath: wsapisv1.ClusterDetailsConfigMountPath,
	}
	highVM1 := corev1.VolumeMount{
		Name:      "high-v1",
		MountPath: "/cb/tests1",
	}
	highVM1Copy := *highVM1.DeepCopy()
	highVM1Copy.Name = "high-v1-copy"
	highVM2 := corev1.VolumeMount{
		Name:      "high-v2",
		MountPath: "/cb/tests2",
	}
	highV2LowVM3 := corev1.VolumeMount{
		Name:      "high-v2-low-v3",
		MountPath: "/cb/tests2/lower",
	}
	highV2LowVM4 := corev1.VolumeMount{
		Name:      "high-v2-low-v4",
		MountPath: "/cb/tests2/another-lower",
	}
	highV2LowVM5 := corev1.VolumeMount{
		Name:      "high-v2-low-v5",
		MountPath: "/n0/lab/venv",
	}
	subpathVM1 := corev1.VolumeMount{
		Name:        "subpath-v1",
		MountPath:   "/cb/testsx/aaa",
		SubPathExpr: "aaa",
	}
	hostPathHigherVM := corev1.VolumeMount{
		Name:      "hostpath-higher",
		MountPath: "/n1/user-venv",
	}
	hostPathLowerVM := corev1.VolumeMount{
		Name:      "hostpath-lower",
		MountPath: "/opt/cerebras/pytorch-venv/lib",
	}
	objectMeta := metav1.ObjectMeta{Name: "task-0"}
	for _, tc := range []struct {
		name        string
		podTemplate *corev1.PodTemplateSpec

		expectedInitContainerVMs [][]corev1.VolumeMount
		expectedContainerVMs     [][]corev1.VolumeMount
		expectedVolumes          []corev1.Volume
	}{
		{
			name: "higher and lower mount dirs",
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: objectMeta,
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1, highVM2, cmVolumeMount}},
					},
					Containers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1, highV2LowVM3}},
						{VolumeMounts: []corev1.VolumeMount{highV2LowVM3}},
						{VolumeMounts: []corev1.VolumeMount{highVM1, subpathVM1}},
					},
					Volumes: []corev1.Volume{highV1, highV2, highV2LowV3, cmVolume, subpathV1},
				},
			},
			expectedVolumes: []corev1.Volume{highV1, highV2, cmVolume, subpathV1},
			expectedInitContainerVMs: [][]corev1.VolumeMount{
				{highVM1, highVM2},
			},
			expectedContainerVMs: [][]corev1.VolumeMount{
				{highVM1, highVM2},
				{highVM2},
				{highVM1, subpathVM1},
			},
		},
		{
			name: "lower and another lower mount dirs",
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: objectMeta,
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1, highV2LowVM4}},
					},
					Containers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1, highV2LowVM3}},
						{VolumeMounts: []corev1.VolumeMount{highV2LowVM3}},
						{VolumeMounts: []corev1.VolumeMount{highVM1, cmVolumeMount, subpathVM1}},
						{VolumeMounts: []corev1.VolumeMount{highV2LowVM4}},
					},
					Volumes: []corev1.Volume{highV1, highV2LowV3, highV2LowV4, cmVolume, subpathV1},
				},
			},
			expectedVolumes: []corev1.Volume{highV1, highV2LowV3, highV2LowV4, cmVolume, subpathV1},
			expectedInitContainerVMs: [][]corev1.VolumeMount{
				{highVM1, highV2LowVM4},
			},
			expectedContainerVMs: [][]corev1.VolumeMount{
				{highVM1, highV2LowVM3},
				{highV2LowVM3},
				{highVM1, cmVolumeMount, subpathVM1},
				{highV2LowVM4},
			},
		},
		{
			name: "higher, lower and another lower mount dirs",
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: objectMeta,
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1, highVM2}},
					},
					Containers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1, highV2LowVM3}},
						{VolumeMounts: []corev1.VolumeMount{highVM2}},
						{VolumeMounts: []corev1.VolumeMount{highV2LowVM3, highV2LowVM4, subpathVM1}},
						{VolumeMounts: []corev1.VolumeMount{highVM2, highV2LowVM4}},
					},
					Volumes: []corev1.Volume{highV1, highV2, highV2LowV3, highV2LowV4, subpathV1},
				},
			},
			expectedVolumes: []corev1.Volume{highV1, highV2, subpathV1},
			expectedInitContainerVMs: [][]corev1.VolumeMount{
				{highVM1, highVM2},
			},
			expectedContainerVMs: [][]corev1.VolumeMount{
				{highVM1, highVM2},
				{highVM2},
				{highVM2, subpathVM1}, // both highV2LowVM3 and highV2LowVM4 got removed and highVM2 was added
				{highVM2},
			},
		},
		{
			name: "venv copy mount dirs",
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: objectMeta,
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highV2LowVM4, highV2LowVM5}},
					},
					Volumes: []corev1.Volume{highV2LowV4, highV2LowV5},
				},
			},
			expectedVolumes: []corev1.Volume{highV2LowV4, highV2LowV5},
			expectedContainerVMs: [][]corev1.VolumeMount{
				{highV2LowVM4, highV2LowVM5},
			},
		},
		{
			name: "same volume test",
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: objectMeta,
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1, highVM2}},
						{VolumeMounts: []corev1.VolumeMount{highVM1Copy, highVM2}},
						{VolumeMounts: []corev1.VolumeMount{highVM1Copy, highVM1}},
					},
					Containers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM2, highVM1Copy}},
						{VolumeMounts: []corev1.VolumeMount{highVM2, highV2LowVM3, subpathVM1}},
					},
					Volumes: []corev1.Volume{highV2LowV3, highV1, highV1Copy, highV2, subpathV1},
				},
			},
			expectedInitContainerVMs: [][]corev1.VolumeMount{
				{highVM1, highVM2}, {highVM1, highVM2}, {highVM1},
			},
			expectedContainerVMs: [][]corev1.VolumeMount{
				{highVM2, highVM1}, {highVM2, subpathVM1},
			},
			expectedVolumes: []corev1.Volume{highV1, highV2, subpathV1},
		},
		{
			name: "hostpath test - only exercised in canary",
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: objectMeta,
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{hostPathHigherVM, hostPathLowerVM}},
					},
					Volumes: []corev1.Volume{hostPathHigherVol, hostPathLowerVol},
				},
			},
			expectedContainerVMs: [][]corev1.VolumeMount{
				{hostPathHigherVM, hostPathLowerVM},
			},
			expectedVolumes: []corev1.Volume{hostPathHigherVol, hostPathLowerVol},
		},
		{
			name: "no dupe",
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: objectMeta,
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1, highVM2}},
					},
					Containers: []corev1.Container{
						{VolumeMounts: []corev1.VolumeMount{highVM1}},
					},
					Volumes: []corev1.Volume{highV1, highV2},
				},
			},
			expectedInitContainerVMs: [][]corev1.VolumeMount{
				{highVM1, highVM2},
			},
			expectedContainerVMs: [][]corev1.VolumeMount{
				{highVM1},
			},
			expectedVolumes: []corev1.Volume{highV1, highV2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.podTemplate.Spec.Volumes = randomizeSlice(tc.podTemplate.Spec.Volumes)
			for i := range tc.podTemplate.Spec.InitContainers {
				tc.podTemplate.Spec.InitContainers[i].VolumeMounts = randomizeSlice(tc.podTemplate.Spec.InitContainers[i].VolumeMounts)
			}
			for i := range tc.podTemplate.Spec.Containers {
				tc.podTemplate.Spec.Containers[i].VolumeMounts = randomizeSlice(tc.podTemplate.Spec.Containers[i].VolumeMounts)
			}
			DedupeMountDirVolumes(tc.podTemplate)
			for _, v := range tc.expectedVolumes {
				assert.Contains(t, tc.podTemplate.Spec.Volumes, v)
			}
			assert.Equal(t, len(tc.expectedInitContainerVMs), len(tc.podTemplate.Spec.InitContainers))
			for i := range tc.expectedInitContainerVMs {
				for _, vm := range tc.expectedInitContainerVMs[i] {
					assert.Contains(t, tc.podTemplate.Spec.InitContainers[i].VolumeMounts, vm)
				}
			}
			assert.Equal(t, len(tc.expectedContainerVMs), len(tc.podTemplate.Spec.Containers))
			for i := range tc.expectedContainerVMs {
				assert.Equal(t, len(tc.expectedContainerVMs[i]), len(tc.podTemplate.Spec.Containers[i].VolumeMounts))
				for _, vm := range tc.expectedContainerVMs[i] {
					assert.Contains(t, tc.podTemplate.Spec.Containers[i].VolumeMounts, vm)
				}
			}
		})
	}
}

func randomizeSlice[T any](slice []T) []T {
	rand.Seed(time.Now().UnixNano())
	sliceCopy := append([]T{}, slice...)
	rand.Shuffle(len(sliceCopy), func(i, j int) {
		sliceCopy[i], sliceCopy[j] = sliceCopy[j], sliceCopy[i]
	})
	return sliceCopy
}

//go:build !e2e

package wsclient

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	v1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
)

func TestDecodeRegcredSecret(t *testing.T) {
	testcases := []struct {
		input    string
		expected map[string]bool
	}{
		{
			input:    `{"auths":{"reg0.com": {}}}`,
			expected: map[string]bool{"reg0.com": true},
		},
		{
			input:    `{"auths":{"reg0.com":{"data":"x"},"reg1.com":{"data":"x"}}}`,
			expected: map[string]bool{"reg0.com": true, "reg1.com": true},
		},
		{
			input:    `{"auths":{},"otherkey":{}}`,
			expected: map[string]bool{},
		},
		{
			input:    `[]`,
			expected: map[string]bool{},
		}}
	for _, testcase := range testcases {
		client := fake.NewSimpleClientset()
		_, err := client.CoreV1().Secrets(common.DefaultNamespace).Create(context.TODO(),
			&corev1.Secret{ObjectMeta: ctrl.ObjectMeta{Name: "regcred", Namespace: common.DefaultNamespace},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(testcase.input),
				},
			}, metav1.CreateOptions{})
		require.Nil(t, err)
		assert.Equal(t, testcase.expected, ParseImagePullSecret(client))
	}
}

func TestDuplicateLogExport(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	wsjobClient := WSJobClient{
		Clientset: fakeClient,
	}

	req := &csctlpb.CreateLogExportRequest{
		Name:                    "wsjob-duplicate-log-export",
		Binaries:                true,
		IncludeCompileArtifacts: true,
		SkipClusterLogs:         true,
		Level:                   int64(0),
		NumLines:                int64(0),
		TimeoutSeconds:          int64(3600),
		Tasks:                   "",
	}
	jobSpec := &JobSpec{
		Namespace: common.DefaultNamespace,
		LogExportNodeSelector: map[string]string{
			"k8s.cerebras.com/node-role-coordinator": "",
			common.NamespaceKey:                      common.Namespace,
		},
	}
	err := jobSpec.SetWorkdirVolumeAndMount(context.Background(), fakeClient, nil)
	require.Nil(t, err)
	err = jobSpec.SetCachedCompileVolumeAndMount(context.Background(), fakeClient, nil, false)
	require.Nil(t, err)
	err = jobSpec.SetLogExportVolumeAndMount(context.Background(), fakeClient,
		csctlpb.ExportTargetType_EXPORT_TARGET_TYPE_CLIENT_LOCAL, "", false)
	require.Nil(t, err)

	// first time would success
	job, err := wsjobClient.LogExport(req, jobSpec)
	require.Nil(t, err)
	require.NotNil(t, job)

	// second time err would be job alreadyexist and job should not be nil
	job, err = wsjobClient.LogExport(req, jobSpec)
	require.True(t, errors.IsAlreadyExists(err), "error is not AlreadyExists")
	require.NotNil(t, job)
}

func TestCreateWsJobSpec(t *testing.T) {
	nfsVol := func(name, server, path string, readonly bool) corev1.Volume {
		return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
			NFS: &corev1.NFSVolumeSource{
				Server:   server,
				Path:     path,
				ReadOnly: readonly,
			},
		}}
	}

	vol0 := nfsVol("vol0", "nfs.example.com", "/data", true)
	vol1 := nfsVol("vol1", "nfs2.example.com", "", false)
	m0 := corev1.VolumeMount{Name: "vol0", MountPath: "/data"}
	m1 := corev1.VolumeMount{Name: "vol0", MountPath: "/project/myproject"}
	m2 := corev1.VolumeMount{Name: "vol1", MountPath: "/scratch/wsjob-0001"}

	workerSidecarCpuRequest := 35
	workerSidecarMemRequest := 65
	weightSidecarCpuRequest := 20
	weightSidecarMemRequest := 20
	// since the activation sidecar uses the same app as the weight sidecar, we can use the same values
	activationSidecarMemRequest := weightSidecarMemRequest
	activationSidecarCpuRequest := weightSidecarCpuRequest

	for _, testcase := range []struct {
		Name        string
		JobSpec     JobSpec
		ExpectError bool
	}{
		{
			Name: "unimplemented wsjob mode",
			JobSpec: JobSpec{
				WsJobMode: WSJobMode("UNKNOWN"),
			},
			ExpectError: true,
		},
		{
			Name: "spec with mount option and CleanPolicyAll",
			JobSpec: JobSpec{
				WsJobMode:              ExecuteJobMode,
				CleanPodPolicy:         v1.CleanPodPolicyAll,
				WorkerUserVolumes:      []*corev1.Volume{&vol0, &vol1},
				WorkerUserVolumeMounts: []*corev1.VolumeMount{&m0, &m1, &m2},
				WorkerSidecar: &UserSidecar{
					Enabled:    true,
					CpuPercent: workerSidecarCpuRequest,
					MemPercent: workerSidecarMemRequest,
				},
				WeightSidecar:     &UserSidecar{},
				ActivationSidecar: &UserSidecar{},
				UserSidecarEnv:    &UserSidecarEnv{},
			},
		},
		{
			Name: "spec with mount option and CleanPolicyNone",
			JobSpec: JobSpec{
				WsJobMode:              ExecuteJobMode,
				CleanPodPolicy:         v1.CleanPodPolicyNone,
				WorkerUserVolumes:      []*corev1.Volume{&vol0, &vol1},
				WorkerUserVolumeMounts: []*corev1.VolumeMount{&m0, &m1, &m2},
				WorkerSidecar: &UserSidecar{
					Enabled:    true,
					CpuPercent: workerSidecarCpuRequest,
					MemPercent: workerSidecarMemRequest,
				},
				WeightSidecar:     &UserSidecar{},
				ActivationSidecar: &UserSidecar{},
				UserSidecarEnv:    &UserSidecarEnv{},
			},
		},
		{
			Name: "spec with mount option and CleanPolicyRunning",
			JobSpec: JobSpec{
				WsJobMode:              ExecuteJobMode,
				CleanPodPolicy:         v1.CleanPodPolicyRunning,
				WorkerUserVolumes:      []*corev1.Volume{&vol0, &vol1},
				WorkerUserVolumeMounts: []*corev1.VolumeMount{&m0, &m1, &m2},
				WorkerSidecar: &UserSidecar{
					Enabled:    true,
					CpuPercent: workerSidecarCpuRequest,
					MemPercent: workerSidecarMemRequest,
				},
				WeightSidecar:     &UserSidecar{},
				ActivationSidecar: &UserSidecar{},
				UserSidecarEnv:    &UserSidecarEnv{},
			},
		},
		{
			Name: "spec with labels - weight streaming",
			JobSpec: JobSpec{
				Labels: map[string]string{
					"foo":                    "bar",
					"baz":                    "",
					wsapisv1.JobTypeLabelKey: wsapisv1.JobTypeExecute,
					wsapisv1.JobModeLabelKey: wsapisv1.JobModeWeightStreaming,
				},
				WsJobMode: ExecuteJobMode,
				CachedCompileVolume: &corev1.Volume{
					Name: wsapisv1.CachedCompileVolume,
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/cs_compile_dir",
						},
					},
				},
				CachedCompileVolumeMount: &corev1.VolumeMount{
					Name:      wsapisv1.CachedCompileVolume,
					MountPath: "/cs_compile_dir",
				},
				WorkerUserVolumes:      []*corev1.Volume{&vol0, &vol1},
				WorkerUserVolumeMounts: []*corev1.VolumeMount{&m0, &m1, &m2},
				UserSidecarEnv: &UserSidecarEnv{
					Enabled:     true,
					CleanupPath: "/local/user-venv",
					MountDir: &pb.MountDir{
						Path:          "/user-venv",
						ContainerPath: "/local/user-venv",
					},
				},
				WorkerSidecar: &UserSidecar{
					Enabled:    true,
					CpuPercent: workerSidecarCpuRequest,
					MemPercent: workerSidecarMemRequest,
					Image:      "new-image",
				},
				WeightSidecar: &UserSidecar{
					Enabled:    true,
					CpuPercent: weightSidecarCpuRequest,
					MemPercent: weightSidecarMemRequest,
					Image:      "new-image",
				},
				ActivationSidecar: &UserSidecar{
					Enabled:    true,
					CpuPercent: activationSidecarCpuRequest,
					MemPercent: activationSidecarMemRequest,
					Image:      "new-image",
				},
			},
		},
		{
			Name: "spec with labels - sdk",
			JobSpec: JobSpec{
				Labels: map[string]string{
					"foo": "bar", "baz": "",
					wsapisv1.JobTypeLabelKey: wsapisv1.JobTypeExecute,
					wsapisv1.JobModeLabelKey: wsapisv1.JobModeSdk,
				},
				WsJobMode:              SdkExecuteJobMode,
				WorkerUserVolumes:      []*corev1.Volume{&vol0, &vol1},
				WorkerUserVolumeMounts: []*corev1.VolumeMount{&m0, &m1, &m2},
				WorkerSidecar:          &UserSidecar{},
				WeightSidecar:          &UserSidecar{},
				ActivationSidecar:      &UserSidecar{},
				UserSidecarEnv:         &UserSidecarEnv{},
			},
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			rsRequest := ResourceRequest{
				CpuMilliCores: 24000,
				MemoryBytes:   4 * (1 << 32),
			}
			specs := make(ServiceSpecs, NumWsServices)
			defaultImage := "example.com/cb:latest"
			for specNo := 0; specNo < len(specs); specNo++ {
				specs[specNo] = ServiceSpec{
					Image:     defaultImage,
					Command:   []string{"sleep", "1"},
					Env:       []corev1.EnvVar{},
					Replicas:  1,
					Resources: rsRequest,
				}
			}
			wsjob, err := BuildWSJobFromSpecs(specs, testcase.JobSpec)
			if testcase.ExpectError {
				require.ErrorContains(t, err, "unimplemented")
				return
			} else {
				require.NoError(t, err)
			}

			for i := range wsjob.Spec.WSReplicaSpecs {
				template := wsjob.Spec.WSReplicaSpecs[i].Template
				podRequestedMem, _ := strconv.Atoi(template.Annotations[wsapisv1.PodMemoryBaseKey])

				workerSidecarRequired := i == wsapisv1.WSReplicaTypeWorker && testcase.JobSpec.WorkerSidecar.Enabled
				weightSidecarRequired := i == wsapisv1.WSReplicaTypeWeight && testcase.JobSpec.WeightSidecar.Enabled
				activationSidecarRequired := i == wsapisv1.WSReplicaTypeActivation && testcase.JobSpec.ActivationSidecar.Enabled
				if workerSidecarRequired || weightSidecarRequired || activationSidecarRequired {
					sidecarMemResPct := int64(pkg.Ternary(workerSidecarRequired, workerSidecarMemRequest, weightSidecarMemRequest))
					sidecarCpuResPct := int64(pkg.Ternary(workerSidecarRequired, workerSidecarCpuRequest, weightSidecarCpuRequest))
					wsMemResPct := 100 - sidecarMemResPct
					wsCpuResPct := 100 - sidecarCpuResPct
					if workerSidecarRequired {
						for _, v := range testcase.JobSpec.WorkerUserVolumes {
							found := false
							for _, _v := range template.Spec.Volumes {
								if _v.NFS != nil && v.NFS != nil {
									if _v.NFS.Server == v.NFS.Server && _v.NFS.Path == v.NFS.Path {
										found = true
										break
									}
								}
							}
							assert.True(t, found)
						}
					}
					for _, c := range template.Spec.Containers {
						sidecarEnvs := 0
						for _, e := range c.Env {
							if e.Name == wsapisv1.UserVenvPathEnvVarName {
								assert.Equal(t, testcase.JobSpec.UserSidecarEnv.CleanupPath, e.Value)
								sidecarEnvs += 1
							} else if e.Name == wsapisv1.PythonPathAddonEnvVarName {
								sidecarEnvs += 1
							}
						}

						if wsapisv1.IsUserSidecarContainer(&c) {
							assert.Equal(t, 2, sidecarEnvs)
							if workerSidecarRequired && testcase.JobSpec.WorkerSidecar.Image != "" {
								assert.Equal(t, testcase.JobSpec.WorkerSidecar.Image, c.Image)
							} else if weightSidecarRequired && testcase.JobSpec.WeightSidecar.Image != "" {
								assert.Equal(t, testcase.JobSpec.WeightSidecar.Image, c.Image)
							} else if activationSidecarRequired && testcase.JobSpec.ActivationSidecar.Image != "" {
								assert.Equal(t, testcase.JobSpec.ActivationSidecar.Image, c.Image)
							} else {
								assert.Equal(t, defaultImage, c.Image)
							}
							if workerSidecarRequired {
								for _, vm := range testcase.JobSpec.WorkerUserVolumeMounts {
									found := false
									for _, _vm := range c.VolumeMounts {
										if _vm.MountPath == vm.MountPath {
											found = true
											break
										}
									}
									assert.True(t, found)
								}
								assert.Equal(t, len(testcase.JobSpec.WorkerUserVolumeMounts), len(c.VolumeMounts))
							}
							assert.Equal(t, pkg.Ternary(workerSidecarRequired, rsRequest.MemoryBytes/100*sidecarMemResPct, 0), c.Resources.Limits.Memory().Value())
							assert.Equal(t, rsRequest.MemoryBytes/100*sidecarMemResPct, c.Resources.Requests.Memory().Value())
							assert.Equal(t, rsRequest.CpuMilliCores/100*sidecarCpuResPct, c.Resources.Requests.Cpu().MilliValue())
							assert.Equal(t, int(rsRequest.MemoryBytes), podRequestedMem)

							isUserSidecar := ""
							for _, e := range c.Env {
								if e.Name == wsapisv1.IsUserSidecarEnvVarName {
									isUserSidecar = e.Value
									break
								}
							}
							assert.Equal(t, strings.ToUpper(strconv.FormatBool(true)), isUserSidecar)
						} else if c.Name == wsapisv1.DefaultContainerName {
							assert.Equal(t, 0, sidecarEnvs)
							assert.Equal(t, defaultImage, c.Image)

							for _, vm := range testcase.JobSpec.WorkerUserVolumeMounts {
								assert.NotContains(t, c.VolumeMounts, vm)
							}
							assert.Equal(t, pkg.Ternary(workerSidecarRequired, rsRequest.MemoryBytes/100*wsMemResPct, 0), c.Resources.Limits.Memory().Value())
							assert.Equal(t, rsRequest.MemoryBytes/100*wsMemResPct, c.Resources.Requests.Memory().Value())
							assert.Equal(t, rsRequest.CpuMilliCores/100*wsCpuResPct, c.Resources.Requests.Cpu().MilliValue())
							assert.Equal(t, int(rsRequest.MemoryBytes), podRequestedMem)

							isUserSidecar := ""
							for _, e := range c.Env {
								if e.Name == wsapisv1.IsUserSidecarEnvVarName {
									isUserSidecar = e.Value
									break
								}
							}
							assert.Equal(t, strings.ToUpper(strconv.FormatBool(false)), isUserSidecar)
						}
					}
				} else {
					for _, v := range testcase.JobSpec.WorkerUserVolumes {
						assert.NotContains(t, template.Spec.Volumes, v)
					}
					for _, vm := range testcase.JobSpec.WorkerUserVolumeMounts {
						for j := range template.Spec.Containers {
							assert.NotContains(t, template.Spec.Containers[j].VolumeMounts, vm)
						}
					}
					for _, c := range template.Spec.Containers {
						for _, vm := range testcase.JobSpec.WorkerUserVolumeMounts {
							assert.NotContains(t, c.VolumeMounts, vm)
						}

						if c.Name == wsapisv1.DefaultContainerName {
							if i == wsapisv1.WSReplicaTypeActivation || i == wsapisv1.WSReplicaTypeCommand || i == wsapisv1.WSReplicaTypeWeight ||
								i == wsapisv1.WSReplicaTypeChief || i == wsapisv1.WSReplicaTypeKVStorageServer || i == wsapisv1.WSReplicaTypeSWDriver {
								assert.True(t, c.Resources.Limits.Memory().IsZero())
							} else {
								assert.Equal(t, rsRequest.MemoryBytes, c.Resources.Limits.Memory().Value())
							}
							assert.Equal(t, rsRequest.MemoryBytes, c.Resources.Requests.Memory().Value())
							assert.Equal(t, rsRequest.CpuMilliCores, c.Resources.Requests.Cpu().MilliValue())
						}
					}
				}
			}

			if vmjson, ok := wsjob.Annotations[UserVolumeAnnotationKey]; ok {
				var v []corev1.VolumeMount
				require.NoError(t, json.Unmarshal([]byte(vmjson), &v))
				require.Len(t, v, len(testcase.JobSpec.WorkerUserVolumeMounts))
				for _, m := range testcase.JobSpec.WorkerUserVolumeMounts {
					assert.Contains(t, v, *m)
				}
			} else {
				t.Error("missing expected user volume annotation")
			}

			assert.Equal(t, testcase.JobSpec.CleanPodPolicy, *wsjob.Spec.RunPolicy.CleanPodPolicy)
			if testcase.JobSpec.Labels != nil {
				assert.Equal(t, testcase.JobSpec.Labels, wsjob.Labels)
			}
		})
	}
}

func TestDisableNonCRDResourceRequests(t *testing.T) {
	testcases := []struct {
		name     string
		spec     ServiceSpecs
		disabled bool
	}{
		{
			"disabled",
			ServiceSpecs{
				ServiceSpec{Replicas: 1, Resources: ResourceRequest{CpuMilliCores: 1000, MemoryBytes: 1 << 30}}, // CRD
				ServiceSpec{Replicas: 1, Resources: ResourceRequest{CpuMilliCores: 1000, MemoryBytes: 1 << 30,
					MemoryBytesUpperBound: 2 << 30, MemoryBytesMaxLimit: 2 << 31}}, // CHF
				ServiceSpec{Replicas: 1, Resources: ResourceRequest{CpuMilliCores: 1000, MemoryBytes: 1 << 30}}, // WRK
				ServiceSpec{Replicas: 1, Resources: ResourceRequest{CpuMilliCores: 1000, MemoryBytes: 1 << 30,
					MemoryBytesUpperBound: 2 << 30, MemoryBytesMaxLimit: 2 << 31, NvmfDiskBytes: 1 << 30, NvmfDiskBytesUpperBound: 2 << 30}}, // WGT
			},
			true,
		}, {
			"enabled",
			ServiceSpecs{
				ServiceSpec{Replicas: 1, Resources: ResourceRequest{CpuMilliCores: 1000, MemoryBytes: 1 << 30}}, // CRD
				ServiceSpec{Replicas: 1, Resources: ResourceRequest{CpuMilliCores: 1000, MemoryBytes: 1 << 30,
					MemoryBytesUpperBound: 2 << 30, MemoryBytesMaxLimit: 2 << 31}}, // CHF
				ServiceSpec{Replicas: 1, Resources: ResourceRequest{CpuMilliCores: 1000, MemoryBytes: 1 << 30}}, // WRK
				ServiceSpec{Replicas: 1, Resources: ResourceRequest{CpuMilliCores: 1000, MemoryBytes: 1 << 30,
					MemoryBytesUpperBound: 2 << 30, MemoryBytesMaxLimit: 2 << 31, NvmfDiskBytes: 1 << 30, NvmfDiskBytesUpperBound: 2 << 30}}, // WGT
			},
			false,
		}}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			wsjob, err := BuildWSJobFromSpecs(
				testcase.spec,
				JobSpec{
					WsJobMode:                     ExecuteJobMode,
					NumWafers:                     1,
					DisableNonCRDResourceRequests: testcase.disabled,
				},
			)
			require.NoError(t, err)
			require.Equal(t, len(testcase.spec), len(wsjob.Spec.WSReplicaSpecs))
			for rt, spec := range wsjob.Spec.WSReplicaSpecs {
				rs := spec.Template.Spec.Containers[0].Resources
				anno := spec.Template.Annotations

				expectResources := !testcase.disabled || rt == wsapisv1.WSReplicaTypeCoordinator
				assert.Equal(t, expectResources, len(rs.Requests) > 0)
				if rt != wsapisv1.WSReplicaTypeCoordinator && rt != wsapisv1.WSReplicaTypeWorker {
					_, hasUpperBoundsAnno := anno[wsapisv1.PodMemoryUpperBoundKey]
					assert.Equal(t, expectResources, hasUpperBoundsAnno)
					_, hasLimitAnno := anno[wsapisv1.PodMemoryMaxLimitKey]
					assert.Equal(t, expectResources, hasLimitAnno)
				}
			}
			assert.True(t, wsjob.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] != "")
			assert.True(t, wsjob.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] != "")
		})
	}
}

func TestSpecValidation(t *testing.T) {
	specs := make(ServiceSpecs, NumWsServices)
	for specNo := 0; specNo < len(specs); specNo++ {
		specs[specNo] = ServiceSpec{
			Image:    "example.com/cb:latest",
			Command:  []string{"sleep", "1"},
			Env:      []corev1.EnvVar{},
			Replicas: 1,
		}
	}

	t.Run("Enable RoCE", func(t *testing.T) {
		wsjob, err := BuildWSJobFromSpecs(
			specs,
			JobSpec{
				WsJobMode:  ExecuteJobMode,
				EnableRoCE: true,
			},
		)
		require.NoError(t, err)

		for i := range wsjob.Spec.WSReplicaSpecs {
			template := wsjob.Spec.WSReplicaSpecs[i].Template
			for _, container := range template.Spec.Containers {
				assert.True(t, container.SecurityContext != nil)
				if i != wsapisv1.WSReplicaTypeCoordinator {
					assert.Equal(t, &corev1.Capabilities{
						Add: []corev1.Capability{
							"IPC_LOCK",
						}}, container.SecurityContext.Capabilities)
					assert.Equal(t, *resource.NewQuantity(1, resource.DecimalSI), container.Resources.Requests[rdmaDevices])
					assert.Equal(t, *resource.NewQuantity(1, resource.DecimalSI), container.Resources.Limits[rdmaDevices])
				} else {
					assert.True(t, container.SecurityContext.Capabilities == nil)
					_, exists := container.Resources.Requests[rdmaDevices]
					assert.False(t, exists)
					_, exists = container.Resources.Limits[rdmaDevices]
					assert.False(t, exists)
				}
			}
		}

	})

	t.Run("EnableAllowUnhealthySystems", func(t *testing.T) {
		// Invalid without a hint
		wsJobInvalid, err := BuildWSJobFromSpecs(
			specs,
			JobSpec{
				WsJobMode:             ExecuteJobMode,
				AllowUnhealthySystems: true,
			},
		)
		require.Error(t, err)
		assert.Nil(t, wsJobInvalid)

		// Valid with a hint
		wsJobValid, err := BuildWSJobFromSpecs(
			specs,
			JobSpec{
				WsJobMode:             ExecuteJobMode,
				AllowUnhealthySystems: true,
				SchedulerHints: map[string]string{
					wsapisv1.SchedulerHintAllowSys: "system-1",
				},
			},
		)
		require.NoError(t, err)
		assert.NotNil(t, wsJobValid)
	})
}

func TestUserNotification(t *testing.T) {
	testCases := []struct {
		name    string
		jobType string
	}{
		{
			name:    "compile-job",
			jobType: wsapisv1.JobTypeCompile,
		},
		{
			name:    "execute-job",
			jobType: wsapisv1.JobTypeExecute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create notification settings
			notifications := []*wsapisv1.UserNotification{
				{
					NotificationType:  wsapisv1.NotificationTypeEmail,
					Target:            "test@example.com",
					SeverityThreshold: common.Pointer(int32(2)),
				},
				{
					NotificationType:  wsapisv1.NotificationTypeSlack,
					Target:            "https://hooks.slack.com/services/T00000000/B00000000/X00000000",
					SeverityThreshold: common.Pointer(int32(3)),
				},
				{
					NotificationType: wsapisv1.NotificationTypePagerduty,
					Target:           "test@example.com",
				},
			}

			// Create the job using the WSJobClient
			specs := make(ServiceSpecs, NumWsServices)
			for specNo := 0; specNo < len(specs); specNo++ {
				specs[specNo] = ServiceSpec{
					Image:    "example.com/cb:latest",
					Command:  []string{"sleep", "1"},
					Env:      []corev1.EnvVar{},
					Replicas: 1,
				}
			}
			createdJob, err := BuildWSJobFromSpecs(
				specs,
				JobSpec{
					WsJobMode:     ExecuteJobMode,
					Notifications: notifications,
					Labels: map[string]string{
						wsapisv1.JobTypeLabelKey: tc.jobType,
					},
				},
			)
			require.NoError(t, err)
			require.NotNil(t, createdJob)

			// Verify the notifications were properly set
			require.NotNil(t, createdJob.Spec.Notifications)
			assert.Equal(t, notifications[0].NotificationType, createdJob.Spec.Notifications[0].NotificationType)
			assert.Equal(t, notifications[0].Target, createdJob.Spec.Notifications[0].Target)
			assert.Equal(t, notifications[0].SeverityThreshold, createdJob.Spec.Notifications[0].SeverityThreshold)
			assert.Equal(t, notifications[1].NotificationType, createdJob.Spec.Notifications[1].NotificationType)
			assert.Equal(t, notifications[1].Target, createdJob.Spec.Notifications[1].Target)
			assert.Equal(t, notifications[1].SeverityThreshold, createdJob.Spec.Notifications[1].SeverityThreshold)
			assert.Equal(t, notifications[2].NotificationType, createdJob.Spec.Notifications[2].NotificationType)
			assert.Equal(t, notifications[2].Target, createdJob.Spec.Notifications[2].Target)
			assert.Equal(t, notifications[2].SeverityThreshold, createdJob.Spec.Notifications[2].SeverityThreshold)
		})
	}
}

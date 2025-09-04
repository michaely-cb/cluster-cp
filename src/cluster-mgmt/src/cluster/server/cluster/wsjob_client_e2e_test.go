//go:build e2e

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubectl/pkg/util/podutils"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	commonpb "cerebras/pb/workflow/appliance/common"

	commonctrlv1 "github.com/kubeflow/common/pkg/controller.v1/common"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"

	// +kubebuilder:scaffold:imports

	"cerebras.com/cluster/server/pkg/wsclient"
)

func TestWSClient(t *testing.T) {
	mgr, testEnv, ctx, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()

	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	assertClusterServerIngressSetup(t, common.Namespace)
	defer assertClusterServerIngressCleanup(t, common.Namespace)

	defer printFailureContext(t, ctx, mgr.GetClient())

	t.Run("test wsjob with health check", func(t *testing.T) {
		jobName := "health-check"
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
		specs[int(wsclient.WsCoordinator)] = wsclient.ServiceSpec{
			Image:    common.DefaultImage,
			Command:  []string{"sh", "-c", "touch svc_up && sleep 300"},
			Replicas: 1,
		}
		jobSpec := wsclient.JobSpec{
			Namespace:      common.Namespace,
			JobName:        jobName,
			WsJobMode:      wsclient.ExecuteJobMode,
			CleanPodPolicy: commonv1.CleanPodPolicyRunning,
			NumWafers:      0,
			Labels: map[string]string{
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeCompile,
			},
		}
		applyVolumesAndMounts(&jobSpec, true)
		wsjobClient := getWsJobClient(t, mgr)
		job, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
		require.NoError(t, err)
		svcName := commonctrlv1.GenSvcName(job.Name, "coordinator", "0")
		livenessProb := &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"ping", "-w", "1", "-c", "1", svcName,
					},
				},
			},
			InitialDelaySeconds: int32(1),
			PeriodSeconds:       int32(1),
			FailureThreshold:    3,
		}
		startupProb := &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"ls", "svc_up",
					},
				},
			},
			InitialDelaySeconds: int32(1),
			PeriodSeconds:       int32(1),
			FailureThreshold:    3,
		}
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template.Spec.Containers[0].LivenessProbe = livenessProb
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template.Spec.Containers[0].StartupProbe = startupProb
		_, err = wsjobClient.WsV1Client.Create(context.Background(), job, metav1.CreateOptions{})
		require.NoError(t, err)

		// check pod readiness 3 success
		var p = &v1.PodList{}
		successCount := 0
		require.Eventually(t, func() bool {
			require.NoError(t, k8sClient.List(context.Background(), p,
				client.InNamespace(common.Namespace),
				client.MatchingLabels{commonv1.JobNameLabel: job.Name}))
			if len(p.Items) > 0 && podutils.IsPodReady(&p.Items[0]) {
				successCount++
			}
			return successCount > 3
		}, 60*time.Second, 1*time.Second)
		require.NoError(t, wsjobClient.Delete(common.Namespace, job.Name))
	})

	for _, testcase := range []struct {
		name      string
		enableSvc bool
	}{
		{
			name:      "test wsjob enable service creation",
			enableSvc: true,
		},
		{
			name:      "test wsjob disable service creation",
			enableSvc: false,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			jobName := "svc-creation-" + strconv.FormatBool(testcase.enableSvc)
			specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
			numSvc := 0
			for i := wsclient.WsCoordinator; i < wsclient.NumWsServices; i++ {
				if i == wsclient.WsBroadcastReduce {
					continue
				}
				specs[int(i)] = wsclient.ServiceSpec{
					Image:    common.DefaultImage,
					Command:  []string{"sh", "-c", "touch svc_up && sleep 300"},
					Replicas: 1,
				}
				numSvc += 1
			}
			jobSpec := wsclient.JobSpec{
				Namespace:      common.Namespace,
				JobName:        jobName,
				WsJobMode:      wsclient.ExecuteJobMode,
				CleanPodPolicy: commonv1.CleanPodPolicyRunning,
				NumWafers:      1,
				Labels: map[string]string{
					wsapisv1.ClientLeaseDisabledLabelKey: "",
					wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				},
				EnableNonCRDServiceCreation: testcase.enableSvc,
			}
			applyVolumesAndMounts(&jobSpec, true)
			wsjobClient := getWsJobClient(t, mgr)
			job, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
			require.NoError(t, err)
			_, err = wsjobClient.WsV1Client.Create(context.Background(), job, metav1.CreateOptions{})
			require.NoError(t, err)
			defer wsjobClient.Delete(common.Namespace, job.Name)

			assertRunningJobConditionEventually(t, jobName)

			svcList := &v1.ServiceList{}
			require.NoError(t, k8sClient.List(context.Background(), svcList,
				client.InNamespace(common.Namespace),
				client.MatchingLabels{commonv1.JobNameLabel: job.Name}))

			if testcase.enableSvc {
				assert.Len(t, svcList.Items, numSvc)
			} else {
				assert.Len(t, svcList.Items, 1)
			}
		})
	}

	t.Run("test job with activation sidecar completes successfully", func(t *testing.T) {
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)

		specs[wsclient.WsActivation] = wsclient.ServiceSpec{
			Image:    common.DefaultImage,
			Command:  []string{"sleep", "10"},
			Replicas: 1,
		}

		jobSpec := wsclient.JobSpec{
			Namespace:      common.Namespace,
			JobName:        "wsjob-activation-sidecar",
			WsJobMode:      wsclient.ExecuteJobMode,
			CleanPodPolicy: commonv1.CleanPodPolicyNone,
			NumWafers:      1,
			ActivationSidecar: &wsclient.UserSidecar{
				Enabled: true,
			},
			UserSidecarEnv: &wsclient.UserSidecarEnv{},
			Labels: map[string]string{
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
			},
		}

		applyVolumesAndMounts(&jobSpec, true)
		wsjobClient := getWsJobClient(t, mgr)
		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
		require.NoError(t, err)
		job, err := wsjobClient.Create(wsjob)
		require.NoError(t, err)

		// Wait for job to be running
		assertRunningJobConditionEventually(t, job.Name)

		// Now verify that activation pods have the sidecar container
		var pods v1.PodList
		require.NoError(t, k8sClient.List(context.Background(), &pods,
			client.InNamespace(common.Namespace),
			client.MatchingLabels{
				commonv1.JobNameLabel: job.Name,
				"replica-type":        strings.ToLower(string(wsapisv1.WSReplicaTypeActivation)),
			}))

		// There should be at least one activation pod
		require.NotEmpty(t, pods.Items, "No activation pods found")

		// Check each activation pod for user-sidecar container
		for _, pod := range pods.Items {
			t.Logf("Checking pod %s for sidecar container", pod.Name)

			// Check if any container is the user-sidecar
			foundSidecar := false
			for _, container := range pod.Spec.Containers {
				if wsapisv1.IsUserSidecarContainer(&container) {
					foundSidecar = true
					t.Logf("Found user-sidecar container in pod %s", pod.Name)
					break
				}
			}

			assert.True(t, foundSidecar, "Activation pod %s should have a user-sidecar container", pod.Name)
		}

		// Wait for job to complete successfully
		assertSucceededJobConditionEventually(t, job.Name)

		// Clean up
		require.NoError(t, wsjobClient.Delete(job.Namespace, job.Name))
		awaitPodsCleanedUp(t, job)
	})
	t.Run("test client create wsjob with image pull secret", func(t *testing.T) {
		k8sClient.Delete(context.Background(),
			&v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "regcred", Namespace: common.Namespace}})
		require.NoError(t, k8sClient.Create(context.Background(),
			&v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "regcred", Namespace: common.Namespace},
				Data: map[string][]byte{
					".dockerconfigjson": []byte(`{"auths":{"example.com": {}}}`),
				},
			}))

		wsjobClient := getWsJobClient(t, mgr)

		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
		for i := range specs {
			if i == int(wsclient.WsBroadcastReduce) {
				continue
			}
			specs[i] = wsclient.ServiceSpec{
				Image:    "example.com/busybox",
				Command:  []string{"echo", "helloworld"},
				Replicas: 1,
			}
		}
		jobName := "test-imagepullsecret"
		jobSpec := wsclient.JobSpec{
			Namespace:      common.Namespace,
			JobName:        jobName,
			WsJobMode:      wsclient.ExecuteJobMode,
			CleanPodPolicy: commonv1.CleanPodPolicyNone,
			NumWafers:      0,
			Labels: map[string]string{
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeCompile,
			},
			ImagePullSecretHosts: wsclient.ParseImagePullSecret(wsjobClient.Clientset),
		}
		applyVolumesAndMounts(&jobSpec, true)
		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
		require.NoError(t, err)
		job, err := wsjobClient.Create(wsjob)
		require.NoError(t, err)
		var p = &v1.PodList{}
		require.Eventually(t, func() bool {
			require.NoError(t, k8sClient.List(context.Background(), p,
				client.InNamespace(common.Namespace),
				client.MatchingLabels{commonv1.JobNameLabel: job.Name}))
			return len(p.Items) > 0
		}, 60*time.Second, 1*time.Second)
		for _, pod := range p.Items {
			require.Equal(t, 1, len(pod.Spec.ImagePullSecrets))
			assert.Equal(t, "regcred", pod.Spec.ImagePullSecrets[0].Name)
		}
		require.NoError(t, wsjobClient.Delete(common.Namespace, job.Name))
		require.NoError(t, k8sClient.Delete(context.Background(),
			&v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "regcred", Namespace: common.Namespace}}))
	})

	for _, testcase := range []struct {
		name                     string
		coordinatorImageOverride string
		isMultiboxJob            bool
		testNodeScheduleStatus   bool
		jobSpec                  wsclient.JobSpec
		initContainer            *corev1.Container
		failedReplicaType        commonv1.ReplicaType
		failedPodMessage         string
		failedJobMessage         string
	}{
		{
			name:                     "wsjob failure due to stuck at init stage after deadline",
			coordinatorImageOverride: common.DefaultImage,
			isMultiboxJob:            false,
			testNodeScheduleStatus:   false,
			jobSpec: wsclient.JobSpec{
				Namespace:      common.Namespace,
				JobName:        "wsjob-stuck-init",
				WsJobMode:      wsclient.ExecuteJobMode,
				CleanPodPolicy: commonv1.CleanPodPolicyNone,
				NumWafers:      1,
				Labels: map[string]string{
					wsapisv1.ClientLeaseDisabledLabelKey: "",
					wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				},
			},
			initContainer: &corev1.Container{
				Image:           common.DefaultImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Name:            "test",
				Command: []string{
					"sleep",
					"3000",
				},
			},
			failedReplicaType: "",
			failedPodMessage:  "",
			failedJobMessage:  "failed due to stuck at init phase",
		},
		{
			name:                     "wsjob failure due to node not schedulable",
			coordinatorImageOverride: common.DefaultImage,
			testNodeScheduleStatus:   true,
			jobSpec: wsclient.JobSpec{
				Namespace:      common.Namespace,
				JobName:        "wsjob-node-not-schedulable",
				WsJobMode:      wsclient.ExecuteJobMode,
				CleanPodPolicy: commonv1.CleanPodPolicyNone,
				NumWafers:      1,
				Labels: map[string]string{
					wsapisv1.ClientLeaseDisabledLabelKey: "",
					wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				},
			},
			failedPodMessage: "unschedulable",
		},
		{
			name:                     "wsjob init failure due to invalid nfs volume",
			coordinatorImageOverride: common.DefaultImage,
			jobSpec: wsclient.JobSpec{
				Namespace:      common.Namespace,
				JobName:        "wsjob-invalid-volume",
				WsJobMode:      wsclient.ExecuteJobMode,
				CleanPodPolicy: commonv1.CleanPodPolicyNone,
				NumWafers:      1,
				WorkerUserVolumes: []*v1.Volume{
					{
						Name: "invalid-nfs-volume",
						VolumeSource: v1.VolumeSource{
							NFS: &v1.NFSVolumeSource{
								Path:   "/tmp",
								Server: "x.x.x.x",
							},
						},
					},
				},
				WorkerUserVolumeMounts: []*v1.VolumeMount{
					{
						Name:      "invalid-nfs-volume",
						MountPath: "/xyz",
					},
				},
				WorkerSidecar: &wsclient.UserSidecar{
					Enabled: true,
				},
				UserSidecarEnv: &wsclient.UserSidecarEnv{},
				Labels: map[string]string{
					wsapisv1.ClientLeaseDisabledLabelKey: "",
					wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				},
			},
			failedReplicaType: wsapisv1.WSReplicaTypeWorker,
			failedPodMessage:  "PodInitializing",
		},
		{
			name:                     "wsjob init failure due to invalid image name used in main container",
			coordinatorImageOverride: "invalid-image???",
			jobSpec: wsclient.JobSpec{
				Namespace:      common.Namespace,
				JobName:        "wsjob-invalid-tag",
				WsJobMode:      wsclient.ExecuteJobMode,
				CleanPodPolicy: commonv1.CleanPodPolicyNone,
				NumWafers:      1,
				Labels: map[string]string{
					wsapisv1.ClientLeaseDisabledLabelKey: "",
					wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				},
			},
			failedReplicaType: wsapisv1.WSReplicaTypeCoordinator,
			failedPodMessage:  "Failed to apply default image tag",
		},
		{
			name:                     "wsjob init failure due to image pull back error occurred in main container",
			coordinatorImageOverride: "alpine:invalid-tag-xyz",
			isMultiboxJob:            true,
			jobSpec: wsclient.JobSpec{
				Namespace:      common.Namespace,
				JobName:        "wsjob-image-pull-error",
				WsJobMode:      wsclient.ExecuteJobMode,
				CleanPodPolicy: commonv1.CleanPodPolicyNone,
				NumWafers:      2,
				Labels: map[string]string{
					wsapisv1.ClientLeaseDisabledLabelKey: "",
					wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				},
			},
			failedReplicaType: wsapisv1.WSReplicaTypeCoordinator,
			failedPodMessage:  "alpine:invalid-tag-xyz",
		},
		{
			name:                     "worker sidecar errored out in main container stage",
			coordinatorImageOverride: common.DefaultImage,
			jobSpec: wsclient.JobSpec{
				Namespace:      common.Namespace,
				JobName:        "wsjob-sidecar-error",
				WsJobMode:      wsclient.ExecuteJobMode,
				CleanPodPolicy: commonv1.CleanPodPolicyNone,
				NumWafers:      1,
				WorkerSidecar: &wsclient.UserSidecar{
					Enabled: true,
				},
				UserSidecarEnv: &wsclient.UserSidecarEnv{},
				Labels: map[string]string{
					wsapisv1.ClientLeaseDisabledLabelKey: "",
					wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				},
			},
			failedReplicaType: wsapisv1.WSReplicaTypeWorker,
			failedPodMessage:  "had exited with code 1",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
			for specId := wsclient.WsCoordinator; specId < wsclient.NumWsServices; specId += 1 {
				if !testcase.isMultiboxJob && specId == wsclient.WsBroadcastReduce {
					continue
				}
				specs[specId] = wsclient.ServiceSpec{
					Image:    common.DefaultImage,
					Command:  []string{"touch", string(specId.AsReplicaType())},
					Replicas: 1,
				}
				if testcase.isMultiboxJob {
					if specId != wsclient.WsCoordinator && specId != wsclient.WsCommand && specId != wsclient.WsBroadcastReduce {
						specs[specId].Replicas = 2
					}
				}
				if specId == wsclient.WsCoordinator {
					specs[specId].Image = testcase.coordinatorImageOverride
				}
			}

			applyVolumesAndMounts(&testcase.jobSpec, true)
			wsjobClient := getWsJobClient(t, mgr)
			updateNodeSchedulable(t, wsjobClient.Clientset, true, testcase.testNodeScheduleStatus)
			defer updateNodeSchedulable(t, wsjobClient.Clientset, false, testcase.testNodeScheduleStatus)

			// override pending ttl
			// Note: it can take more than 20 seconds for a 2-box job to have cluster details configmap updated.
			// Please do not further decrease this TTL unless thoroughly tested.
			// keep pending ttl on label to pass to pod side for kubeflow process
			testcase.jobSpec.Labels["PENDING_POD_TTL_SECONDS"] = "30"
			testcase.jobSpec.Annotations = common.EnsureMap(testcase.jobSpec.Annotations)
			testcase.jobSpec.Annotations[common.FailingJobGracePeriodLabel] = "2"
			wsjob, err := wsclient.BuildWSJobFromSpecs(specs, testcase.jobSpec)
			require.NoError(t, err)
			if testcase.initContainer != nil {
				for _, replica := range wsjob.Spec.WSReplicaSpecs {
					replica.Template.Spec.InitContainers = append(replica.Template.Spec.InitContainers,
						*testcase.initContainer)
				}
			}

			if testcase.failedReplicaType == wsapisv1.WSReplicaTypeWorker {
				for rt, replica := range wsjob.Spec.WSReplicaSpecs {
					if rt != wsapisv1.WSReplicaTypeWorker {
						continue
					}
					for i, c := range replica.Template.Spec.Containers {
						if wsapisv1.IsUserSidecarContainer(&c) {
							replica.Template.Spec.Containers[i].Command = []string{"bash", "-c", "exit 1"}
						} else if c.Name == wsapisv1.DefaultContainerName {
							command := []string{"bash", "-c", "sleep infinity"}
							replica.Template.Spec.Containers[i].Command = command
						}
					}
				}
			}

			job, err := wsjobClient.WsV1Interface.WSJobs(wsjob.Namespace).
				Create(context.TODO(), wsjob, metav1.CreateOptions{})
			require.NoError(t, err)
			job = assertFailedJobConditionTypeEventually(t, job.Name, testcase.failedReplicaType)
			if testcase.failedJobMessage != "" {
				assert.Contains(t, job.Status.Conditions[len(job.Status.Conditions)-1].Message,
					testcase.failedJobMessage)
			} else {
				for rType, replicaStatus := range job.Status.ReplicaStatuses {
					t.Logf("%s %s replica status: %v", job.Name, rType, replicaStatus)
					if rType == testcase.failedReplicaType || testcase.failedReplicaType == "" {
						if len(replicaStatus.FailedPodStatuses) != 1 {
							// print debug information before failing
							pod0Name := wsjob.Name + "-" + strings.ToLower(string(rType)) + "-" + "0"
							if initLogs, err := common.GetPodLogs(wsjobClient.Clientset, wsjob.Namespace, pod0Name, wsapisv1.DefaultInitContainerName, 0); err == nil {
								t.Logf("%s-0 init container logs:\n %s", rType, initLogs)
							} else {
								t.Logf("failed to retrieve logs for %s-0: %s", rType, err.Error())
							}
							assert.Failf(t, "replica type did not have any failed pod statuses", "replicaType: %s", string(rType))
						} else {
							assert.Contains(t, replicaStatus.FailedPodStatuses[0].Message, testcase.failedPodMessage)
						}
					} else {
						assert.Equal(t, 0, len(replicaStatus.FailedPodStatuses))
					}
				}
			}

			// cleanup
			require.NoError(t, wsjobClient.Delete(job.Namespace, job.Name))
			awaitPodsCleanedUp(t, job)
		})
	}

	t.Run("test log export happy path", func(t *testing.T) {
		// Have each container touch a file with their replica name. Check in /n1/wsjob/workdir/<namespace>/<jobId>/<replica>-0/
		expectedFiles := []string{fmt.Sprintf("%s/%s/%%s.zip", wsclient.LogExportRootPath, common.Namespace)}
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
		for specId := wsclient.WsCoordinator; specId < wsclient.NumWsServices; specId += 1 {
			if specId == wsclient.WsBroadcastReduce {
				continue
			}
			expectedFiles = append(expectedFiles,
				fmt.Sprintf("%s/%s/%s/%s-0/%s", wsclient.LogExportRootPath, common.Namespace, "%s", specId.String(), specId.AsReplicaType()))
			specs[specId] = wsclient.ServiceSpec{
				Image:    common.DefaultImage,
				Command:  []string{"touch", string(specId.AsReplicaType())},
				Replicas: 1,
			}
		}

		// create job, await finish
		jobSpec := &wsclient.JobSpec{
			Namespace:      common.Namespace,
			JobName:        "wsjob-log-export",
			WsJobMode:      wsclient.ExecuteJobMode,
			CleanPodPolicy: commonv1.CleanPodPolicyNone,
			NumWafers:      1,
			Labels: map[string]string{
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
			},
			JobPathConfig: defaultJpConfig,
			LogExportNodeSelector: map[string]string{
				"k8s.cerebras.com/node-role-coordinator": "",
				common.NamespaceLabelKey:                 common.Namespace,
			},
		}
		applyVolumesAndMounts(jobSpec, false)
		wsjobClient := getWsJobClient(t, mgr)
		err := jobSpec.SetLogExportVolumeAndMount(ctx, wsjobClient.Clientset,
			pb.ExportTargetType_EXPORT_TARGET_TYPE_CLIENT_LOCAL, "", false)
		require.NoError(t, err)
		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, *jobSpec)
		require.NoError(t, err)
		job, err := wsjobClient.Create(wsjob)
		require.NoError(t, err)
		awaitPodsSucceeded(t, job.Name)

		req := &pb.CreateLogExportRequest{
			Name:                    job.Name,
			Binaries:                true,
			IncludeCompileArtifacts: true,
			SkipClusterLogs:         true,
			Level:                   int64(0),
			NumLines:                int64(0),
			TimeoutSeconds:          int64(3600),
			Tasks:                   "",
		}
		exportJob, err := wsjobClient.LogExport(req, jobSpec)
		require.NoError(t, err)
		awaitJobSucceeded(t, exportJob.Name)

		// Simulates how log export job gathers workdirs to the staging area
		fileCopyCmd := fmt.Sprintf("cp -r %s/%s %s/%s/%s",
			common.Namespace, job.Name, wsclient.LogExportRootPath, common.Namespace, exportJob.Name)

		// create a pod to snoop on the filesystem of the host machine
		containerName := "check-hostpath-log-export"
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: common.Namespace,
				Name:      containerName,
			},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyNever,
				Containers: []v1.Container{
					{
						Name:  containerName,
						Image: common.DefaultImage,
						Command: []string{"sh", "-c", fmt.Sprintf("%s; find %s/%s/ -type f; rm -rf %s/%s/*; find -type f; rm -rf %s/*",
							fileCopyCmd, wsclient.LogExportRootPath, common.Namespace, wsclient.LogExportRootPath, common.Namespace, wsclient.WorkdirLogsRootPath)}, // rm for cleanup
						WorkingDir: wsclient.WorkdirLogsRootPath,
						VolumeMounts: []v1.VolumeMount{
							{Name: "local", MountPath: wsclient.WorkdirLogsRootPath},
							{Name: "export", MountPath: path.Join(wsclient.LogExportRootPath, common.Namespace)},
						},
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "local",
						VolumeSource: v1.VolumeSource{
							HostPath: &v1.HostPathVolumeSource{Path: wsclient.WorkdirLogsRootPath},
						},
					},
					{
						Name: "export",
						VolumeSource: v1.VolumeSource{
							HostPath: &v1.HostPathVolumeSource{Path: path.Join(wsclient.LogExportRootPath, common.Namespace)},
						},
					},
				},
			},
		}
		err = k8sClient.Create(context.Background(), &pod)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var p = &v1.Pod{}
			require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{Namespace: common.Namespace, Name: pod.Name}, p))
			return p.Status.Phase == v1.PodSucceeded
		}, 60*time.Second, 1*time.Second)

		logs, err := common.GetPodLogs(wsjobClient.Clientset, pod.Namespace, pod.Name, containerName, 0)
		require.NoError(t, err)
		files := map[string]bool{}
		for _, fp := range strings.Split(logs, "\n") {
			files[fp] = true
		}

		for _, expectedFile := range expectedFiles {
			assert.Contains(t, files, fmt.Sprintf(expectedFile, exportJob.Name))
		}
		require.NoError(t, wsjobClient.Delete(common.Namespace, job.Name))
		require.NoError(t, k8sClient.Delete(context.Background(), &pod))
		bgDelete := metav1.DeletePropagationBackground
		delOptions := metav1.DeleteOptions{
			PropagationPolicy: &bgDelete,
		}
		require.NoError(t, wsjobClient.Clientset.BatchV1().Jobs(common.Namespace).Delete(ctx, exportJob.Name, delOptions))
	})

	t.Run("test client create wsjob log export num lines", func(t *testing.T) {
		// Have each container touch a file with their replica name. Check in /n1/wsjob/workdir/<namespace>/<jobId>/<replica>-0/
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
		for specId := wsclient.WsCoordinator; specId < wsclient.NumWsServices; specId += 1 {
			if specId == wsclient.WsBroadcastReduce {
				continue
			}
			specs[specId] = wsclient.ServiceSpec{
				Image:    common.DefaultImage,
				Command:  []string{"touch", string(specId.AsReplicaType())},
				Replicas: 1,
			}
		}

		// create job, await finish
		jobSpec := &wsclient.JobSpec{
			Namespace:      common.Namespace,
			JobName:        "wsjob-fetch-logs",
			WsJobMode:      wsclient.ExecuteJobMode,
			CleanPodPolicy: commonv1.CleanPodPolicyNone,
			NumWafers:      1,
			Labels: map[string]string{
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
			},
			JobPathConfig: defaultJpConfig,
			LogExportNodeSelector: map[string]string{
				"k8s.cerebras.com/node-role-coordinator": "",
				common.NamespaceLabelKey:                 common.Namespace,
			},
		}
		applyVolumesAndMounts(jobSpec, false)
		wsjobClient := getWsJobClient(t, mgr)
		err := jobSpec.SetLogExportVolumeAndMount(ctx, wsjobClient.Clientset,
			pb.ExportTargetType_EXPORT_TARGET_TYPE_CLIENT_LOCAL, "", false)
		require.NoError(t, err)
		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, *jobSpec)
		require.NoError(t, err)
		job, err := wsjobClient.Create(wsjob)
		require.NoError(t, err)
		awaitPodsSucceeded(t, job.Name)

		req := &pb.CreateLogExportRequest{
			Name:                    job.Name,
			Binaries:                false,
			IncludeCompileArtifacts: false,
			SkipClusterLogs:         false,
			Level:                   int64(0),
			NumLines:                int64(10),
			TimeoutSeconds:          int64(3600),
			Tasks:                   "",
		}
		exportJob, err := wsjobClient.LogExport(req, jobSpec)
		require.NoError(t, err)
		awaitJobSucceeded(t, exportJob.Name)
		require.NoError(t, wsjobClient.Delete(common.Namespace, job.Name))
		bgDelete := metav1.DeletePropagationBackground
		delOptions := metav1.DeleteOptions{
			PropagationPolicy: &bgDelete,
		}
		require.NoError(t, wsjobClient.Clientset.BatchV1().Jobs(common.Namespace).Delete(ctx, exportJob.Name, delOptions))
	})

	t.Run("test log export with debug volume", func(t *testing.T) {
		// Have each container touch a file with their replica name. Check in /n1/wsjob/workdir/<namespace>/<jobId>/<replica>-0/
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
		for specId := wsclient.WsCoordinator; specId < wsclient.NumWsServices; specId += 1 {
			if specId == wsclient.WsBroadcastReduce {
				continue
			}
			specs[specId] = wsclient.ServiceSpec{
				Image:    common.DefaultImage,
				Command:  []string{"touch", string(specId.AsReplicaType())},
				Replicas: 1,
			}
		}

		// create job, await finish
		jobSpec := &wsclient.JobSpec{
			Namespace:      common.Namespace,
			JobName:        "wsjob-debug-volume-export",
			WsJobMode:      wsclient.ExecuteJobMode,
			CleanPodPolicy: commonv1.CleanPodPolicyNone,
			NumWafers:      1,
			Labels: map[string]string{
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
			},
			JobPathConfig: defaultJpConfig,
			LogExportNodeSelector: map[string]string{
				"k8s.cerebras.com/node-role-coordinator": "",
				common.NamespaceLabelKey:                 common.Namespace,
			},
		}
		applyVolumesAndMounts(jobSpec, false)
		wsjobClient := getWsJobClient(t, mgr)
		err := jobSpec.SetLogExportVolumeAndMount(ctx, wsjobClient.Clientset,
			pb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME, "", false)
		require.NoError(t, err)
		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, *jobSpec)
		require.NoError(t, err)
		job, err := wsjobClient.Create(wsjob)
		require.NoError(t, err)
		awaitPodsSucceeded(t, job.Name)

		req := &pb.CreateLogExportRequest{
			Name:                    job.Name,
			Binaries:                false,
			IncludeCompileArtifacts: false,
			SkipClusterLogs:         true,
			Level:                   int64(0),
			NumLines:                int64(0),
			TimeoutSeconds:          int64(3600),
			Tasks:                   "",
			ExportTargetType:        pb.ExportTargetType_EXPORT_TARGET_TYPE_DEBUG_VOLUME,
		}
		exportJob, err := wsjobClient.LogExport(req, jobSpec)
		require.NoError(t, err)
		awaitJobSucceeded(t, exportJob.Name)

		// create a pod to snoop on the filesystem of the host machine
		containerName := "check-debug-volume"
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: common.Namespace,
				Name:      containerName,
			},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyNever,
				Containers: []v1.Container{
					{
						Name:  containerName,
						Image: common.DefaultImage,
						Command: []string{"sh", "-c", fmt.Sprintf("ls %s/%s/%s/.retention-marker.out",
							wsclient.DebugArtifactRootPath, jobSpec.Namespace, jobSpec.JobName)}, // rm for cleanup
						VolumeMounts: []v1.VolumeMount{
							{Name: "debug", MountPath: wsclient.DebugArtifactRootPath},
						},
					},
				},
				Volumes: []v1.Volume{
					{
						Name: "debug",
						VolumeSource: v1.VolumeSource{
							HostPath: &v1.HostPathVolumeSource{Path: wsclient.DebugArtifactRootPath},
						},
					},
				},
			},
		}
		err = k8sClient.Create(context.Background(), &pod)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var p = &v1.Pod{}
			require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{Namespace: common.Namespace, Name: pod.Name}, p))
			return p.Status.Phase == v1.PodSucceeded
		}, 60*time.Second, 1*time.Second)
		require.NoError(t, k8sClient.Delete(context.Background(), &pod))
		require.NoError(t, wsjobClient.Delete(common.Namespace, job.Name))
		bgDelete := metav1.DeletePropagationBackground
		delOptions := metav1.DeleteOptions{
			PropagationPolicy: &bgDelete,
		}
		require.NoError(t, wsjobClient.Clientset.BatchV1().Jobs(common.Namespace).Delete(ctx, exportJob.Name, delOptions))
	})

	t.Run("create wsjob with drop-caches", func(t *testing.T) {
		numWafers := 2
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
		for i := range specs {
			specs[i] = wsclient.ServiceSpec{
				Image:    common.DefaultImage,
				Command:  []string{"sleep", "1"},
				Replicas: 1,
			}
			if i == int(wsclient.WsCoordinator) || i == int(wsclient.WsCommand) ||
				i == int(wsclient.WsWeight) || i == int(wsclient.WsBroadcastReduce) {
				specs[i].Replicas = 1
			} else if i == int(wsclient.WsWorker) {
				// Intended to test multi-worker-per-csx
				specs[i].Replicas = int32(numWafers * 2)
			} else {
				specs[i].Replicas = int32(numWafers)
			}
		}
		dropCachesValue := commonpb.DebugArgs_DebugMGR_DROPCACHES_DENTRIES_INODES
		dropCachesPath := "/tmp/drop_caches"
		jobSpec := wsclient.JobSpec{
			Namespace:       common.Namespace,
			JobName:         "wsjob-drop-cache",
			WsJobMode:       wsclient.ExecuteJobMode,
			CleanPodPolicy:  commonv1.CleanPodPolicyNone,
			NumWafers:       numWafers,
			DropCachesPath:  dropCachesPath,
			DropCachesValue: dropCachesValue,
			Labels: map[string]string{
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
			},
		}
		applyVolumesAndMounts(&jobSpec, true)
		wsjobClient := getWsJobClient(t, mgr)
		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
		require.NoError(t, err)
		job, err := wsjobClient.Create(wsjob)
		require.NoError(t, err)
		assertSucceededJobConditionEventually(t, job.Name)

		var pods = &v1.PodList{}
		require.NoError(t, k8sClient.List(context.Background(), pods,
			client.InNamespace(common.Namespace),
			client.MatchingLabels{commonv1.JobNameLabel: job.Name}))

		for _, pod := range pods.Items {
			if pod.Labels["replica-type"] == strings.ToLower(string(wsapisv1.WSReplicaTypeBroadcastReduce)) {
				assert.Equal(t, len(pod.Spec.InitContainers), 2)
				assert.Equal(t, pod.Spec.InitContainers[0].Name, wsapisv1.DropCachesInitContainerName)
			} else {
				assert.Equal(t, len(pod.Spec.InitContainers), 1)
			}
		}

		containerName := "validate-cache-val"
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: common.Namespace,
				Name:      containerName,
			},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyNever,
				Containers: []v1.Container{
					{
						Name:    containerName,
						Image:   common.DefaultImage,
						Command: []string{"sh", "-c", fmt.Sprintf("cat /host/%s", dropCachesPath)},
						VolumeMounts: []v1.VolumeMount{{Name: "tmp-drop-caches",
							MountPath: fmt.Sprintf("/host/%s", dropCachesPath)}},
					},
				},
				Volumes: []v1.Volume{{
					Name: "tmp-drop-caches",
					VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{
						Path: dropCachesPath,
						Type: common.Pointer(v1.HostPathFile),
					},
					},
				}},
			},
		}
		err = k8sClient.Create(context.TODO(), &pod)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var p = &v1.Pod{}
			require.NoError(t, k8sClient.Get(context.TODO(),
				client.ObjectKey{Namespace: common.Namespace, Name: pod.Name}, p))
			return p.Status.Phase == v1.PodSucceeded
		}, 60*time.Second, 1*time.Second)

		logs, err := common.GetPodLogs(wsjobClient.Clientset, pod.Namespace, pod.Name, containerName, 0)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%d", dropCachesValue), strings.TrimSpace(logs))

		require.NoError(t, k8sClient.Delete(context.Background(), &pod))
		require.NoError(t, wsjobClient.Delete(common.Namespace, job.Name))
	})

	t.Run("validate wsjob br/cd CM updated", func(t *testing.T) {
		numWafers := 2
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
		for i := range specs {
			specs[i] = wsclient.ServiceSpec{
				Image: common.DefaultImage,
				Command: []string{"bash", "-c", fmt.Sprintf("sleep 1; cat %s/%s; grep -q updated %s/%s",
					wsapisv1.ClusterDetailsConfigMountPath, wsapisv1.ClusterDetailsConfigUpdatedSignal,
					wsapisv1.ClusterDetailsConfigMountPath, wsapisv1.ClusterDetailsConfigUpdatedSignal)},
				Replicas: 1,
			}
			if i == int(wsclient.WsCoordinator) || i == int(wsclient.WsCommand) ||
				i == int(wsclient.WsWeight) || i == int(wsclient.WsBroadcastReduce) {
				specs[i].Replicas = 1
			} else if i == int(wsclient.WsWorker) {
				// Intended to test multi-worker-per-csx
				specs[i].Replicas = int32(numWafers * 2)
			} else {
				specs[i].Replicas = int32(numWafers)
			}
		}

		jobSpec := wsclient.JobSpec{
			Namespace:      common.Namespace,
			JobName:        "wsjob-config-update",
			WsJobMode:      wsclient.ExecuteJobMode,
			CleanPodPolicy: commonv1.CleanPodPolicyNone,
			NumWafers:      numWafers,
			Labels: map[string]string{
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
			},
		}

		applyVolumesAndMounts(&jobSpec, true)
		wsjobClient := getWsJobClient(t, mgr)
		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
		require.NoError(t, err)
		job, err := wsjobClient.Create(wsjob)
		require.NoError(t, err)
		assertSucceededJobConditionEventually(t, job.Name)
		require.NoError(t, wsjobClient.Delete(common.Namespace, job.Name))
	})
}

func printFailureContext(t *testing.T, ctx context.Context, client client.Client) {
	if !t.Failed() {
		return
	}
	t.Log("test failed, logging failure context")
	events := &v1.EventList{}
	type eventSummary struct {
		creationTime metav1.Time
		objectKind   string
		objectName   string
		reason       string
		message      string
	}
	if err := client.List(ctx, events); err == nil {
		eventSummaries := common.Map(func(e corev1.Event) eventSummary {
			return eventSummary{
				creationTime: e.CreationTimestamp,
				objectKind:   e.InvolvedObject.Kind,
				objectName:   e.InvolvedObject.Name,
				reason:       e.Reason,
				message:      e.Message,
			}
		}, events.Items)
		sort.Slice(eventSummaries, func(i, j int) bool {
			return eventSummaries[i].creationTime.Before(&eventSummaries[j].creationTime)
		})
		for _, es := range eventSummaries {
			t.Logf("%s %s/%s [%s] %s", es.creationTime, es.objectKind, es.objectName, es.reason, es.message)
		}
	}
	pods := &v1.PodList{}
	if err := client.List(ctx, pods); err == nil {
		t.Logf("%+v", pods)
	}
}

func awaitPodsCleanedUp(t *testing.T, job *wsapisv1.WSJob) {
	pods := &v1.PodList{}
	labels := map[string]string{"job-name": job.Name}
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: labels})
	require.NoError(t, err)
	podStr := ""
	ok := assert.Eventuallyf(t, func() bool {
		require.NoError(t, k8sClient.List(context.Background(), pods, client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(job.Namespace)))
		ps := fmt.Sprintf("%v", common.Sorted(common.Map(func(p corev1.Pod) string { return p.Name }, pods.Items)))
		if podStr != ps {
			podStr = ps
			t.Logf("%s pods for job %s - %d (%s)", time.Now().String(), job.Name, len(pods.Items), podStr)
		}
		return len(pods.Items) == 0
	}, 3*time.Minute, 1*time.Second, "failed to clean up pods in time")
	if !ok {
		t.Logf("remaining pod json: %s", jsonify(pods))
		require.Failf(t, "pods not cleaned up in time: %s", podStr)
	}
}

func jsonify(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

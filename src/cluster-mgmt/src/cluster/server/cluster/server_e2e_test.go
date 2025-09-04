//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"cerebras/pb/workflow/appliance/cluster_mgmt"
	commonpb "cerebras/pb/workflow/appliance/common"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

func TestGetWsjobLogAsEvent(t *testing.T) {
	mgr, testEnv, _, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	assertClusterServerIngressSetup(t, common.Namespace)
	defer assertClusterServerIngressCleanup(t, common.Namespace)

	wsjobClient := getWsJobClient(t, mgr)
	ctx, cleanup, _, clusterClient := setup(false, wsjobClient.Clientset, wsjobClient, nil, false)
	defer cleanup()

	specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)
	specs[int(wsclient.WsCoordinator)] = wsclient.ServiceSpec{
		Image:    common.DefaultImage,
		Command:  []string{"bash", "-c", "echo 'test start'; echo 'test running'; ls non-existent"},
		Replicas: 1,
	}
	initContainer := corev1.Container{
		Image:           wsapisv1.KubectlImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Name:            "init",
		Command:         []string{"sleep", "1"},
	}
	jobName := "test-get-logs"
	jobSpec := wsclient.JobSpec{
		Namespace:      common.Namespace,
		JobName:        jobName,
		WsJobMode:      wsclient.ExecuteJobMode,
		CleanPodPolicy: commonv1.CleanPodPolicyRunning,
		NumWafers:      0,
		Annotations: map[string]string{
			common.FailingJobGracePeriodLabel: "2",
		},
		Labels: map[string]string{
			wsapisv1.ClientLeaseDisabledLabelKey: "",
			wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeCompile,
			wsapisv1.InitContainerOverrideKey:    "true",
		},
	}
	applyVolumesAndMounts(&jobSpec, true)
	job, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
	require.NoError(t, err)
	job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template.Spec.InitContainers = []corev1.Container{initContainer}
	_, err = wsjobClient.WsV1Client.Create(context.Background(), job, metav1.CreateOptions{})
	require.NoError(t, err)

	jobId := job.Name
	assertWsJobFailEventually(t, clusterClient, jobId)

	// Deletes the job at the end
	defer func() {
		_, err = clusterClient.DeleteJob(ctx, &cluster_mgmt.DeleteJobRequest{
			JobId: jobId,
		})
		require.NoError(t, err)
	}()

	ret, err := clusterClient.GetJob(context.Background(), &cluster_mgmt.GetJobRequest{JobId: jobId})
	require.NoError(t, err)
	t.Log(ret.String())
	require.NotNil(t, ret.JobEvents)
	require.Contains(t, ret.Message,
		fmt.Sprintf("has failed because total 1 replica(s) failed, first failed pods: [%s-coordinator-0]", jobId))
	logFetched := false
	for _, event := range ret.JobEvents {
		require.Contains(t, event.Name, jobId)
		if strings.Contains(event.Message, "test start") &&
			strings.Contains(event.Message, "test running") &&
			strings.Contains(event.Message, "non-existent") {
			logFetched = true
		}
	}
	assert.True(t, logFetched)
}

func TestInferenceExecuteJobWithMissingWseDomains(t *testing.T) {
	mgr, testEnv, _, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	assertClusterServerIngressSetup(t, common.Namespace)
	defer assertClusterServerIngressCleanup(t, common.Namespace)

	wsjobClient := getWsJobClient(t, mgr)
	ctx, cleanup, clusterServer, clusterClient := setup(false, wsjobClient.Clientset, wsjobClient, nil, false)
	defer cleanup()

	clusterServerVersion := wsclient.SemanticVersion
	wsclient.SemanticVersion = "1.0.8"
	defer func() { wsclient.SemanticVersion = clusterServerVersion }()

	setOperatorVersion(t, wsjobClient.Clientset, wsclient.SemanticVersion)
	clusterServer.setEnableClientLeaseStrategy(false)

	taskSpecHint := &commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
		ContainerImage:   common.DefaultImage,
		ContainerCommand: []string{"echo", "hello"},
	}
	jobMode := &commonpb.JobMode{
		Job: commonpb.JobMode_EXECUTE,
		ClusterDetails: &commonpb.ClusterDetails{
			Tasks: []*commonpb.ClusterDetails_TaskInfo{
				{
					TaskType: commonpb.ClusterDetails_TaskInfo_CRD,
					TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 0,
								WseIds: []int32{-1},
							},
						},
					},
				},
				{
					TaskType: commonpb.ClusterDetails_TaskInfo_CHF,
					TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 0,
								WseIds: []int32{0},
							},
						},
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 1,
								WseIds: []int32{1},
							},
						},
					},
					ResourceInfo: &commonpb.ResourceInfo{
						MemoryBytes:         128 << 20,
						MemoryBytesMaxLimit: 512 << 20,
					},
				},
				{
					TaskType: commonpb.ClusterDetails_TaskInfo_ACT,
					TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 0,
								WseIds: []int32{0},
							},
							AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
								WseDomains: []int32{3},
							},
						},
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 1,
								WseIds: []int32{1},
							},
							// no address book and no wse domains
						},
					},
					ResourceInfo: &commonpb.ResourceInfo{
						MemoryBytes:         128 << 20,
						MemoryBytesMaxLimit: 512 << 20,
					},
				},
				{
					TaskType: commonpb.ClusterDetails_TaskInfo_KVSS,
					TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 0,
								WseIds: []int32{0},
							},
							AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
								WseDomains: []int32{3},
							},
						},
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 1,
								WseIds: []int32{1},
							},
							// no address book and no wse domains
						},
					},
					ResourceInfo: &commonpb.ResourceInfo{
						MemoryBytes: 128 << 20,
					},
				},
				{
					TaskType: commonpb.ClusterDetails_TaskInfo_WSE,
					TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 0,
								WseIds: []int32{0},
							},
						},
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 1,
								WseIds: []int32{1},
							},
						},
					},
				},
				{
					TaskType: commonpb.ClusterDetails_TaskInfo_SWD,
					TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
						{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 0,
								WseIds: []int32{0},
							},
							AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
								WseDomains: []int32{3},
							},
						},
					},
					ResourceInfo: &commonpb.ResourceInfo{
						MemoryBytes: 128 << 20,
					},
				},
			},
		},
	}
	res, err := clusterClient.InitExecuteJob(ctx, &cluster_mgmt.ExecuteJobRequest{
		JobMode: jobMode,
		DebugArgs: &commonpb.DebugArgs{
			DebugMgr: &commonpb.DebugArgs_DebugMGR{
				TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
					wsapisv1.WSReplicaTypeCoordinator.Lower():     taskSpecHint,
					wsapisv1.WSReplicaTypeChief.Lower():           taskSpecHint,
					wsapisv1.WSReplicaTypeActivation.Lower():      taskSpecHint,
					wsapisv1.WSReplicaTypeKVStorageServer.Lower(): taskSpecHint,
					wsapisv1.WSReplicaTypeSWDriver.Lower():        taskSpecHint,
				},
			},
		},
		CompileDirRelativePath: "test-compile",
	})
	require.NoError(t, err)
	jobId := res.JobId

	// Deletes the job at the end
	defer func() {
		_, err = clusterClient.DeleteJob(ctx, &cluster_mgmt.DeleteJobRequest{
			JobId: jobId,
		})
		require.NoError(t, err)
	}()

	assertSucceededJobConditionEventually(t, jobId)

	// validate cluster details generated/updated
	clusterDetailsConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, jobId),
			Namespace: common.Namespace,
		},
	}
	err = k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
	require.NoError(t, err)
	data, err := common.Gunzip(clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName])
	require.NoError(t, err)
	cdConfig := &commonpb.ClusterDetails{}
	options := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}
	err = options.Unmarshal(data, cdConfig)
	require.NoError(t, err)
	for _, task := range cdConfig.Tasks {
		if task.TaskType == commonpb.ClusterDetails_TaskInfo_ACT {
			for i := range task.TaskMap {
				// every activation should have the same wse domains
				assert.Equal(t, []int32{3}, task.TaskMap[i].AddressBook.WseDomains)
			}
		}
	}
}

func TestPodInfraSetup(t *testing.T) {
	mgr, testEnv, _, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	assertClusterServerIngressSetup(t, common.Namespace)
	defer assertClusterServerIngressCleanup(t, common.Namespace)

	wsjobClient := getWsJobClient(t, mgr)
	ctx, cleanup, clusterServer, clusterClient := setup(false, wsjobClient.Clientset, wsjobClient, nil, false)
	defer cleanup()

	relativeCompileDir := "test-compile"
	differentVolumes := map[string]string{
		"workdir-logs-volume":   `{"type":"hostPath","containerPath":"/n1/wsjob/workdir","readonly":false}`,
		"cached-compile-volume": `{"type":"hostPath","containerPath":"/n1/wsjob/compile_root","readonly":false}`,
	}
	sameVolume := map[string]string{
		"tmp-volume": `{"type":"hostPath","containerPath":"/tmp","readonly":false}`,
	}

	setClusterVolumes := func(data map[string]string) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      wsclient.VolumesCmName,
				Namespace: common.Namespace,
			},
			Data: data,
		}
		_, err := wsjobClient.Clientset.CoreV1().ConfigMaps(common.Namespace).Create(context.Background(), cm, metav1.CreateOptions{})
		if k8serrors.IsAlreadyExists(err) {
			cm, err = wsjobClient.Clientset.CoreV1().ConfigMaps(common.Namespace).Get(context.Background(), wsclient.VolumesCmName, metav1.GetOptions{})
			assert.NoError(t, err)

			cm.Data = data
			_, err = wsjobClient.Clientset.CoreV1().ConfigMaps(common.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
			assert.NoError(t, err)
		} else {
			assert.NoError(t, err)
		}
	}

	execCmd := func(podPrefix, command string, mountDirRoots []string) {
		var volumes []corev1.Volume
		var volumeMounts []corev1.VolumeMount
		for i, mountDirRoot := range mountDirRoots {
			volumes = append(volumes, corev1.Volume{
				Name: fmt.Sprintf("vol-%d", i),
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: mountDirRoot},
				},
			})
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      fmt.Sprintf("vol-%d", i),
				MountPath: mountDirRoot,
			})
		}
		pod := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: common.Namespace,
				Name:      fmt.Sprintf("%s-%s", podPrefix, strings.ToLower(time.Now().UTC().Format("20060102T150405Z"))),
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:         "main",
					Image:        common.DefaultImage,
					Command:      []string{"sh", "-c", command},
					VolumeMounts: volumeMounts,
				}},
				Volumes: volumes,
			},
		}
		err := k8sClient.Create(context.Background(), &pod)
		defer func() {
			err = k8sClient.Delete(context.Background(), &pod)
			require.NoError(t, err)
		}()
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var p = &corev1.Pod{}
			require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{Namespace: common.Namespace, Name: pod.Name}, p))
			return p.Status.Phase == corev1.PodSucceeded
		}, 60*time.Second, 1*time.Second)
	}

	clusterServerVersion := wsclient.SemanticVersion
	wsclient.SemanticVersion = "1.0.7"
	defer func() { wsclient.SemanticVersion = clusterServerVersion }()

	for _, tc := range []struct {
		name                  string
		clusterVolumes        map[string]string
		logsMountDirPath      string
		compileMountDirPath   string
		operatorVersion       string
		skipInitialCleanup    bool
		injectPermissionError bool
		shouldFail            bool
		expectedErrorMsg      string
	}{
		{
			"happy path - different volumes",
			differentVolumes,
			"/n1/wsjob/workdir",
			"/n1/wsjob/compile_root",
			wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ScInfraSetup].String(),
			false,
			false,
			false,
			"",
		},
		{
			"unhappy path - permission denied during mkdir",
			differentVolumes,
			"/n1/wsjob/workdir",
			"/n1/wsjob/compile_root",
			wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ScInfraSetup].String(),
			false,
			true,
			true,
			"Permission denied",
		},
		{
			"happy path - one mount dir and one without",
			sameVolume,
			"/tmp",
			"",
			wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ScInfraSetup].String(),
			false,
			false,
			false,
			"",
		},
		{
			"happy path - same volume",
			sameVolume,
			"/tmp",
			"/tmp",
			wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ScInfraSetup].String(),
			false,
			false,
			false,
			"",
		},
		{
			"unhappy path - volume creation for infra setup job failed",
			sameVolume,
			"/tmp/does-not-exist",
			"/tmp/does-not-exist",
			wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ScInfraSetup].String(),
			true,
			false,
			true,
			// nfs and hostPath have the same behaviour here - pod stuck in pending state
			"hostPath type check failed: /tmp/does-not-exist is not a directory",
		},
		{
			"old operator uses subpath in volume mount",
			sameVolume,
			"/tmp/does-not-exist",
			"/tmp/does-not-exist",
			wsapisv1.SoftwareFeatureSemverMap[wsapisv1.RestartWorkflow].String(),
			false,
			false,
			false,
			"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mountDirs []string
			var commands []string
			if tc.logsMountDirPath != "" {
				mountDirs = append(mountDirs, tc.logsMountDirPath)
				commands = append(commands, fmt.Sprintf("rm -rf %s/*", tc.logsMountDirPath))
			}
			if tc.compileMountDirPath != "" && tc.compileMountDirPath != tc.logsMountDirPath {
				mountDirs = append(mountDirs, tc.compileMountDirPath)
				commands = append(commands, fmt.Sprintf("rm -rf %s/*", tc.compileMountDirPath))
			}
			if len(mountDirs) > 0 && !tc.skipInitialCleanup {
				execCmd("remove-mount-dir-artifacts", strings.Join(commands, "; "), mountDirs)
			}

			setClusterVolumes(tc.clusterVolumes)
			setOperatorVersion(t, wsjobClient.Clientset, tc.operatorVersion)
			clusterServer.setEnableClientLeaseStrategy(false)

			if tc.injectPermissionError {
				execCmd("permission-tightening", fmt.Sprintf("chmod 444 %s", tc.logsMountDirPath), mountDirs)
				defer func() {
					execCmd("permission-restoring", fmt.Sprintf("chmod 777 %s", tc.logsMountDirPath), mountDirs)
				}()
			}

			res, err := clusterClient.InitCompileJob(ctx, &cluster_mgmt.CompileJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{
						TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
							wsapisv1.WSReplicaTypeCoordinator.Lower(): {
								ContainerImage:   common.DefaultImage,
								ContainerCommand: []string{"echo", "hello"},
							},
						},
						NfsWorkdirLogsPath:   tc.logsMountDirPath,
						NfsCachedCompilePath: tc.compileMountDirPath,
					},
				},
				CompileDirRelativePath: relativeCompileDir,
			})
			require.NoError(t, err)
			jobId := res.JobId

			// Deletes the job at the end
			defer func() {
				_, err = clusterClient.DeleteJob(ctx, &cluster_mgmt.DeleteJobRequest{
					JobId: jobId,
				})
				require.NoError(t, err)
			}()

			if tc.shouldFail {
				assertFailedJobConditionEventually(t, jobId)
				if tc.expectedErrorMsg != "" {
					wsjob := &wsapisv1.WSJob{}
					require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{Namespace: common.Namespace, Name: jobId}, wsjob))
					assert.Contains(t, wsjob.Status.Conditions[len(wsjob.Status.Conditions)-1].Message, tc.expectedErrorMsg)
				}
			} else {
				assertSucceededJobConditionEventually(t, jobId)
			}

			jobName := fmt.Sprintf("%s-%s", jobId, wsapisv1.DefaultInfraSetupContainerName)
			infraSetupJob := &batchv1.Job{}
			infraSetupJob, err = wsjobClient.Clientset.BatchV1().Jobs(common.Namespace).Get(context.Background(), jobName, metav1.GetOptions{})

			wsjob := &wsapisv1.WSJob{}
			require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{Namespace: common.Namespace, Name: jobId}, wsjob))
			_, annotExists := wsjob.Annotations[wsapisv1.InfraSetupAnnot]
			if tc.operatorVersion != wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ScInfraSetup].String() {
				require.True(t, k8serrors.IsNotFound(err))
				require.False(t, annotExists)
				return
			}
			require.True(t, annotExists)
			require.NoError(t, err)

			if !tc.shouldFail {
				assert.True(t, infraSetupJob.Status.CompletionTime != nil)
				// make sure the namespace level permission follows the root
				// this should be guaranteed by pod_infra_setup.sh
				// idea is that if root allows multiple users to write, the namespace level should
				// also allow multiple users to write
				commands = nil
				if tc.logsMountDirPath != "" {
					commands = append(commands, fmt.Sprintf(`[ "$(stat -c "%%a" %s)" = "$(stat -c "%%a" %s/%s)" ]`,
						tc.logsMountDirPath, tc.logsMountDirPath, common.Namespace))
				}
				if tc.compileMountDirPath != "" {
					commands = append(commands, fmt.Sprintf(`[ "$(stat -c "%%a" %s)" = "$(stat -c "%%a" %s/%s/%s)" ]`,
						tc.compileMountDirPath, tc.compileMountDirPath, common.Namespace, relativeCompileDir))
				}
				execCmd("check-permission", strings.Join(commands, " && "), mountDirs)
			}
		})
	}
}

func TestSDKJobs(t *testing.T) {
	mgr, testEnv, ctx, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()
	assertClusterServerIngressSetup(t, common.Namespace)
	defer assertClusterServerIngressCleanup(t, common.Namespace)

	wsjobClient := getWsJobClient(t, mgr)
	ctx, cleanup, clusterServer, clusterClient := setup(false, wsjobClient.Clientset, wsjobClient, nil, false)
	defer cleanup()

	clusterServerVersion := wsclient.SemanticVersion
	wsclient.SemanticVersion = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
	defer func() { wsclient.SemanticVersion = clusterServerVersion }()
	setOperatorVersion(t, wsjobClient.Clientset, wsclient.SemanticVersion)

	clusterServer.setEnableClientLeaseStrategy(false)
	initCompileJobResp, err := clusterClient.InitSdkCompileJob(ctx, &cluster_mgmt.SdkCompileJobRequest{
		JobMode: &commonpb.JobMode{
			Job: commonpb.JobMode_SDK_COMPILE,
			ClusterDetails: &commonpb.ClusterDetails{
				Tasks: []*commonpb.ClusterDetails_TaskInfo{
					{
						TaskType: commonpb.ClusterDetails_TaskInfo_CRD,
						TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{{
							TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: 0,
								WseIds: []int32{-1},
							},
						}},
					},
				},
			},
		},
		DebugArgs: &commonpb.DebugArgs{
			DebugMgr: &commonpb.DebugArgs_DebugMGR{
				TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
					"coordinator": {
						ContainerImage:   common.DefaultImage,
						ContainerCommand: []string{"sleep", "infinity"},
					},
				},
			},
		},
		CompileDirRelativePath: "test-compile",
	})
	require.NoError(t, err)
	compileJobId := initCompileJobResp.JobId

	defer func() {
		_, err = clusterClient.DeleteJob(ctx, &cluster_mgmt.DeleteJobRequest{
			JobId: compileJobId,
		})
		require.NoError(t, err)
	}()

	compileJob, err := wsjobClient.WsV1Client.Get(context.Background(), compileJobId, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, 0, int(compileJob.Spec.NumWafers))

	clusterDetails := &commonpb.ClusterDetails{
		Tasks: []*commonpb.ClusterDetails_TaskInfo{
			{
				TaskType: commonpb.ClusterDetails_TaskInfo_WRK,
				// Commenting out task map to test taskmap hydration
				// TODO: Uncomment when we remove the taskmap hydration in rel-2.7
				//TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{{
				//	TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
				//		TaskId: 0,
				//		WseIds: []int32{0},
				//	},
				//}},
			},
			// Commenting out wse task info to test task info hydration
			// TODO: Uncomment when we remove the task info hydration in rel-2.7
			//{
			//	TaskType: commonpb.ClusterDetails_TaskInfo_WSE,
			//	TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{{
			//		TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
			//			TaskId: 0,
			//			WseIds: []int32{0},
			//		},
			//	}},
			//},
		},
	}
	initExecuteJobResp, err := clusterClient.InitSdkExecuteJob(ctx, &cluster_mgmt.SdkExecuteJobRequest{
		JobMode: &commonpb.JobMode{
			Job:            commonpb.JobMode_SDK_EXECUTE,
			ClusterDetails: clusterDetails,
		},
		DebugArgs: &commonpb.DebugArgs{
			DebugMgr: &commonpb.DebugArgs_DebugMGR{
				TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
					"worker": {
						ContainerImage:   common.DefaultImage,
						ContainerCommand: []string{"sleep", "infinity"},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	executeJobId := initExecuteJobResp.JobId

	defer func() {
		_, err = clusterClient.DeleteJob(ctx, &cluster_mgmt.DeleteJobRequest{
			JobId: executeJobId,
		})
		require.NoError(t, err)
	}()

	executeJob, err := wsjobClient.WsV1Client.Get(context.Background(), executeJobId, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, int(executeJob.Spec.NumWafers))

	// validate cluster details generated/updated
	clusterDetailsConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, executeJobId),
			Namespace: common.Namespace,
		},
	}
	err = k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
	require.NoError(t, err)
	data, err := common.Gunzip(clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName])
	require.NoError(t, err)
	cdConfig := &commonpb.ClusterDetails{}
	options := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}
	err = options.Unmarshal(data, cdConfig)
	require.NoError(t, err)

	foundWorker := false
	foundWse := false
	for _, task := range cdConfig.Tasks {
		if task.TaskType == commonpb.ClusterDetails_TaskInfo_WRK {
			foundWorker = true
			assert.Equal(t, 1, len(task.TaskMap))
			assert.Equal(t, []int32{0}, task.TaskMap[0].TaskId.WseIds)
		} else if task.TaskType == commonpb.ClusterDetails_TaskInfo_WSE {
			foundWse = true
			assert.Equal(t, 1, len(task.TaskMap))
			assert.Equal(t, []int32{0}, task.TaskMap[0].TaskId.WseIds)
		}
	}
	assert.True(t, foundWorker)
	assert.True(t, foundWse)
}

func TestClientLiveness(t *testing.T) {
	mgr, testEnv, _, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	assertClusterServerIngressSetup(t, common.Namespace)
	defer assertClusterServerIngressCleanup(t, common.Namespace)

	var devLeaseDurationSeconds = int32(5)
	wsjobClient := getWsJobClient(t, mgr)
	ctx, cleanup, clusterServer, clusterClient := setup(false, wsjobClient.Clientset, wsjobClient, nil, false)
	defer cleanup()

	for _, testcase := range []struct {
		name                            string
		clientLeaseStrategy             bool
		shouldCancelLeaseRightAfterInit bool
		shouldCancelLeaseWhileRunning   bool
		shouldExpireLease               bool
		expectedJobStatus               cluster_mgmt.JobStatus
	}{
		{
			name:                            "client lease not enabled",
			clientLeaseStrategy:             false,
			shouldCancelLeaseRightAfterInit: false,
			shouldCancelLeaseWhileRunning:   false,
			shouldExpireLease:               false,
			expectedJobStatus:               cluster_mgmt.JobStatus_JOB_STATUS_IN_PROGRESS,
		},
		{
			name:                            "client lease enabled and renewed periodically",
			clientLeaseStrategy:             true,
			shouldCancelLeaseRightAfterInit: false,
			shouldCancelLeaseWhileRunning:   false,
			shouldExpireLease:               false,
			expectedJobStatus:               cluster_mgmt.JobStatus_JOB_STATUS_IN_PROGRESS,
		},
		{
			name:                            "client lease enabled but canceled immediately after init",
			clientLeaseStrategy:             true,
			shouldCancelLeaseRightAfterInit: true,
			shouldCancelLeaseWhileRunning:   false,
			shouldExpireLease:               false,
			expectedJobStatus:               cluster_mgmt.JobStatus_JOB_STATUS_CANCELLED,
		},
		{
			name:                            "client lease enabled but canceled while job running",
			clientLeaseStrategy:             true,
			shouldCancelLeaseRightAfterInit: false,
			shouldCancelLeaseWhileRunning:   true,
			shouldExpireLease:               false,
			expectedJobStatus:               cluster_mgmt.JobStatus_JOB_STATUS_CANCELLED,
		},
		{
			name:                            "client lease enabled but expired",
			clientLeaseStrategy:             true,
			shouldCancelLeaseRightAfterInit: false,
			shouldCancelLeaseWhileRunning:   false,
			shouldExpireLease:               true,
			expectedJobStatus:               cluster_mgmt.JobStatus_JOB_STATUS_FAILED,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			clusterServer.setEnableClientLeaseStrategy(testcase.clientLeaseStrategy)
			res, err := clusterClient.InitCompileJob(ctx, &cluster_mgmt.CompileJobRequest{
				JobMode: pkg.MockJobMode(commonpb.JobMode_COMPILE, false),
				DebugArgs: &commonpb.DebugArgs{
					DebugMgr: &commonpb.DebugArgs_DebugMGR{
						TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
							"coordinator": {
								ContainerImage:   common.DefaultImage,
								ContainerCommand: []string{"sleep", "infinity"},
							},
						},
					},
				},
				CompileDirRelativePath: "test-compile",
			})
			require.NoError(t, err)
			jobId := res.JobId

			// Deletes the job at the end
			defer func() {
				_, err = clusterClient.DeleteJob(ctx, &cluster_mgmt.DeleteJobRequest{
					JobId: jobId,
				})
				require.NoError(t, err)
			}()

			stopCh := make(chan struct{})
			if testcase.clientLeaseStrategy {
				if !testcase.shouldCancelLeaseRightAfterInit {
					assertWsJobRunningEventually(t, clusterClient, jobId)
					// Updates the lease duration once to speed up testing
					t.Logf("Setting the lease duration for %s to be %d seconds", jobId, devLeaseDurationSeconds)
					_, err = clusterClient.SendClientHeartbeat(context.Background(), &cluster_mgmt.HeartbeatRequest{
						JobId:                        jobId,
						LeaseDurationSecondsOverride: devLeaseDurationSeconds,
					})
					require.NoError(t, err)
				}

				if !testcase.shouldExpireLease {
					go renewClientLeasePeriodically(t, stopCh, clusterClient, jobId)
					// Cleans up the lease renew routine
					defer func() {
						stopCh <- struct{}{}
					}()
				}
			}

			if testcase.shouldExpireLease {
				time.Sleep(time.Duration(devLeaseDurationSeconds+1) * time.Second)
				job := assertWsJobFailEventually(t, clusterClient, jobId)
				_, ok := job.Annotations[wsapisv1.CancelWithStatusKey]
				assert.True(t, ok)
				assert.Equal(t, wsapisv1.ExpiredLeaseCancelReason, job.Annotations[wsapisv1.CancelReasonKey])
				assert.Equal(t, wsapisv1.CancelWithStatusFailed, job.Annotations[wsapisv1.CancelWithStatusKey])
			} else if testcase.shouldCancelLeaseRightAfterInit || testcase.shouldCancelLeaseWhileRunning {
				_, err = clusterClient.CancelJobV2(ctx, &cluster_mgmt.CancelJobRequest{
					JobId:     jobId,
					JobStatus: cluster_mgmt.JobStatus_JOB_STATUS_CANCELLED,
				})

				// Note: canceling the lease right after init could at times witness the lease not yet created.
				// In the event where Framework's appliance manager invoked the API but the lease had not been created
				// at that point, the call will error out but indicate the job ultimately would get canceled
				// due to lease expiration.
				if err != nil {
					assert.True(t, testcase.shouldCancelLeaseRightAfterInit)
					status, ok := grpcStatus.FromError(err)
					require.True(t, ok)
					require.Equal(t, codes.Internal, status.Code())
				} else {
					job := assertWsJobCancelledEventually(t, clusterClient, jobId)
					_, ok := job.Annotations[wsapisv1.CancelWithStatusKey]
					assert.True(t, ok)
					assert.Equal(t, wsapisv1.CanceledByClientReason, job.Annotations[wsapisv1.CancelReasonKey])
					assert.Equal(t, wsapisv1.CancelWithStatusCancelled, job.Annotations[wsapisv1.CancelWithStatusKey])
				}
			} else {
				time.Sleep(time.Duration(devLeaseDurationSeconds+1) * time.Second)
				job := assertWsJobRunningEventually(t, clusterClient, jobId)
				_, ok := job.Annotations[wsapisv1.CancelWithStatusKey]
				assert.False(t, ok)
			}
		})
	}
}

func renewClientLeasePeriodically(t *testing.T, stopCh chan struct{}, clusterClient cluster_mgmt.ClusterManagementClient, jobId string) {
	for {
		select {
		case <-stopCh:
			return
		default:
			t.Logf("Renewing client lease for %s", jobId)
			_, err := clusterClient.SendClientHeartbeat(context.Background(), &cluster_mgmt.HeartbeatRequest{
				JobId: jobId,
			})
			if err != nil {
				t.Logf("Failed to renew lease for %s: %v", jobId, err)
			} else {
				t.Logf("Lease has been renewed for %s", jobId)
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// Tests the ability to detect Out-of-Memory (OOM) events in pods with
// multiple containers and verify appropriate job failure reporting through replica events.
// This test uses a WSJob with a coordinator pod that has two containers, both of which will OOM.
// TODO: Discuss with team the best way to run e2e with a real client that can trigger OOM
func TestOOMDetectionThroughReplicaEvents(t *testing.T) {
	// Standard test setup
	mgr, testEnv, _, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	assertClusterServerIngressSetup(t, common.Namespace)
	defer assertClusterServerIngressCleanup(t, common.Namespace)

	wsjobClient := getWsJobClient(t, mgr)
	ctx, cleanup, _, clusterClient := setup(false, wsjobClient.Clientset, wsjobClient, nil, false)
	defer cleanup()

	// Create a WSJob spec that will trigger OOM
	specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)

	// Configure coordinator pod with main container that will OOM
	specs[int(wsclient.WsCoordinator)] = wsclient.ServiceSpec{
		Image:    common.DefaultImage,
		Command:  []string{"bash", "-c", "for i in $(seq 1 10); do a=$a$(printf \"%.0s\" {1..500000000}); done"},
		Replicas: 1,
		Resources: wsclient.ResourceRequest{
			MemoryBytes: 64 * 1024 * 1024, // 64Mi - should eventually OOM
		},
	}

	jobName := "test-multi-container-oom"
	jobSpec := wsclient.JobSpec{
		Namespace:      common.Namespace,
		JobName:        jobName,
		WsJobMode:      wsclient.ExecuteJobMode,
		CleanPodPolicy: commonv1.CleanPodPolicyRunning,
		NumWafers:      1,
		Annotations: map[string]string{
			common.FailingJobGracePeriodLabel: "3600", // 1 hour to ensure we are in failing state
		},
		Labels: map[string]string{
			wsapisv1.ClientLeaseDisabledLabelKey: "",
			wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
		},
	}

	applyVolumesAndMounts(&jobSpec, true)
	job, err := wsclient.BuildWSJobFromSpecs(specs, jobSpec)
	require.NoError(t, err)

	// Add OOM-prone sidecar container to coordinator pod
	oomSidecar := corev1.Container{
		Image:           common.DefaultImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Name:            "oom-init",
		Command:         []string{"bash", "-c", "for i in $(seq 1 10); do a=$a$(printf \"%.0s\" {1..500000000}); done"},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: *resource.NewQuantity(64*1024*1024, resource.BinarySI), // 64Mi
			},
		},
	}

	// Add containers to coordinator pod
	job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template.Spec.Containers = append(
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template.Spec.Containers,
		oomSidecar,
	)

	// Create job
	_, err = wsjobClient.WsV1Client.Create(context.Background(), job, metav1.CreateOptions{})
	require.NoError(t, err)

	jobId := job.Name

	// Wait for job to fail due to OOM
	assertWsJobFailingEventually(t, clusterClient, jobId)

	// Cleanup at the end
	defer func() {
		_, err = clusterClient.DeleteJob(ctx, &cluster_mgmt.DeleteJobRequest{
			JobId: jobId,
		})
		require.NoError(t, err)
	}()

	// Get job status and verify OOM is detected
	ret, err := clusterClient.GetJob(context.Background(), &cluster_mgmt.GetJobRequest{JobId: jobId})
	require.NoError(t, err)

	// Get Job using wsjob client to access Status.Conditions
	job, err = wsjobClient.WsV1Client.Get(context.Background(), jobId, metav1.GetOptions{})
	require.NoError(t, err)

	// Find the failing condition
	var failingCondition *commonv1.JobCondition
	for i := range job.Status.Conditions {
		if job.Status.Conditions[i].Type == commonv1.JobFailing {
			failingCondition = &job.Status.Conditions[i]
			break
		}
	}
	require.NotNil(t, failingCondition, "Job should have a Failing condition")

	// Parse failing condition timestamp
	failingTime := failingCondition.LastTransitionTime.Time
	t.Logf("Job entered failing state at: %v", failingTime)
	t.Logf("Failing condition message: %s", failingCondition.Message)

	// Verify that the job failed due to OOM
	oomContainers := make(map[string]bool)

	// Sort job events by timestamp earliest first
	sort.Slice(ret.JobEvents, func(i, j int) bool {
		return ret.JobEvents[i].LastTimestamp < ret.JobEvents[j].LastTimestamp
	})

	// Find the earliest OOM event
	var earliestOOMEvent *cluster_mgmt.JobEvent
	for i, event := range ret.JobEvents {
		t.Logf("Event %d: %s - %s", i, event.Reason, event.Message)
		// Simple check if container name appears in OOM event message
		if event.Reason == "OOMKilled" {
			earliestOOMEvent = event
			break
		}
	}
	// Check if the earliest OOM event is not nil
	require.NotNil(t, earliestOOMEvent, "No OOM event found in job events")

	if strings.Contains(earliestOOMEvent.Message, "container ws") {
		oomContainers["ws"] = true
	}
	if strings.Contains(earliestOOMEvent.Message, "container oom-init") {
		oomContainers["oom-init"] = true
	}

	// Parse timestamp using the actual format in the events
	eventTime, err := time.Parse("2006-01-02 15:04:05 -0700 MST", strings.TrimSpace(earliestOOMEvent.LastTimestamp))
	require.NoError(t, err, "Failed to parse timestamp: %s", earliestOOMEvent.LastTimestamp)

	t.Logf("OOM event at: %v", eventTime)
	t.Logf("Status failing time: %s", failingTime)

	// Assert that the OOM event occurred before or at the same time as the job entered failing state
	assert.True(
		t, !eventTime.After(failingTime),
		"OOM event at %s should occur before or at the same time as job enters failing state at %s",
		eventTime.Format(time.RFC3339), failingTime.Format(time.RFC3339),
	)

	assert.True(t, oomContainers["ws"] || oomContainers["oom-init"], "At least one container (ws or oom-init) should have an OOM event")

	// Log which container(s) experienced OOM for debugging
	t.Logf("OOM detected in containers: ws=%v, oom-init=%v", oomContainers["ws"], oomContainers["oom-init"])
}

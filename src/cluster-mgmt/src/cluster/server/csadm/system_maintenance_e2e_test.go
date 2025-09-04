//go:build e2e

package csadm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/controller-runtime/pkg/envtest"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

func TestCreateSystemMaintenance(t *testing.T) {
	// Set up the test environment.
	wsjobClient, ctx, clusterClient, testEnv, cancel, cleanup := createTestEnv(t)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
		cleanup()
		assertClusterServerIngressCleanup(t, common.Namespace)
	}()

	version := "1.0.7"

	clusterServerVersion := wsclient.SemanticVersion
	wsclient.SemanticVersion = version
	defer func() { wsclient.SemanticVersion = clusterServerVersion }()

	// Set the operator version to 1.0.8.
	setOperatorVersion(t, wsjobClient.Clientset, version)

	// Create a request for system maintenance with two systems.
	req := &pb.CreateSystemMaintenanceRequest{
		WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
		Systems:      []string{"cs-0", "cs-1"},
		AllowError:   false,
		DebugArgs: &commonpb.DebugArgs{
			DebugMgr: &commonpb.DebugArgs_DebugMGR{
				TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
					wsapisv1.WSReplicaTypeCoordinator.Lower(): {
						ContainerImage:   common.DefaultImage,
						ContainerCommand: []string{},
					},
				},
			},
		},
	}

	// Call CreateSystemMaintenance.
	res, err := clusterClient.CreateSystemMaintenance(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Greater(t, len(res.Jobs), 0, "No maintenance jobs created")

	// Clean up the job after completion or failure
	defer deleteSysMaintWsjobs(t, wsjobClient, res.Jobs, ctx)

	// Verify the response fields for each created maintenance job.
	for _, job := range res.Jobs {
		assert.NotEmpty(t, job.JobId)
		assert.Equal(t, pb.JobStatus_JOB_STATUS_CREATED, job.Status)
		assert.Equal(t, pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST, job.WorkflowType)
		// Each job should be for one of the requested systems.
		assert.Contains(t, []string{"cs-0", "cs-1"}, job.Systems[0])
		assert.Equal(t, int32(defaultSystemMaintenanceJobPriority), job.Priority)
		assert.NotNil(t, job.CreateTime)
		assert.NotNil(t, job.User)
		assert.NotEmpty(t, job.User.Username)

		// For each created job, retrieve the wsjob and verify its spec.
		createdJob, err := wsjobClient.WsV1Client.Get(context.Background(), job.JobId, metav1.GetOptions{})

		// Verify the job was created successfully.
		require.NoError(t, err)
		require.NotNil(t, createdJob)

		// Updated assertions checking the new pod template field.
		podTemplate := createdJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template
		require.NotNil(t, podTemplate, "PodTemplate must be set in the WSJob spec")

		// Verify that the pod spec has the correct service account name
		assert.Equal(t, serviceAccountName, podTemplate.Spec.ServiceAccountName)

		// Verify that the pod template has a volume named "system-maintenance-scripts".
		var volumeFound bool
		for _, vol := range podTemplate.Spec.Volumes {
			if vol.Name == SystemMaintenanceVolumeName {
				volumeFound = true
				require.NotNil(t, vol.ConfigMap)
				assert.Equal(t, SystemMaintenanceConfigMapName, vol.ConfigMap.Name)
			}
		}
		assert.True(t, volumeFound, "Expected volume %q not found in pod template")

		// Verify that the container has the correct volume mount.
		require.Len(t, podTemplate.Spec.Containers, 1)
		container := podTemplate.Spec.Containers[0]
		var mountFound bool
		for _, mount := range container.VolumeMounts {
			if mount.Name == SystemMaintenanceVolumeName && mount.MountPath == SystemMaintenanceMountPath {
				mountFound = true
			}
		}
		assert.True(t, mountFound, "Expected volume mount %q with mount path %q not found", SystemMaintenanceVolumeName, SystemMaintenanceMountPath)

		// Verify that the container has the expected environment variables.
		var systemNameEnvFound, scriptPathEnvFound bool
		for _, env := range container.Env {
			if env.Name == "SYSTEM_NAME" && env.Value == job.Systems[0] {
				systemNameEnvFound = true
			}
			if env.Name == "SCRIPT_PATH" && env.Value == filepath.Join(SystemMaintenanceMountPath, MembistScriptName) {
				scriptPathEnvFound = true
			}
		}
		assert.True(t, systemNameEnvFound, "Expected env var SYSTEM_NAME not found or incorrect")
		assert.True(t, scriptPathEnvFound, "Expected env var SCRIPT_PATH not found or incorrect")

		assertWsJobCompletion(t, wsjobClient, job.JobId)
	}
}

func TestCreateWaferDiagSystemMaintenance(t *testing.T) {
	// Set up the test environment
	wsjobClient, ctx, clusterClient, testEnv, cancel, cleanup := createTestEnv(t)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
		cleanup()
		assertClusterServerIngressCleanup(t, common.Namespace)
	}()

	namespace := pkg.GetNamespaceFromContext(ctx)
	t.Logf("Test using namespace: %s", namespace)

	version := "1.0.7"
	clusterServerVersion := wsclient.SemanticVersion
	wsclient.SemanticVersion = version
	defer func() { wsclient.SemanticVersion = clusterServerVersion }()

	// Set the operator version to 1.0.8
	setOperatorVersion(t, wsjobClient.Clientset, version)

	// Create wafer-diag request
	req := &pb.CreateSystemMaintenanceRequest{
		WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG,
		Systems:      []string{"cs-0"},
		AllowError:   false,
		Dryrun:       true,
		Payload: &pb.CreateSystemMaintenanceRequest_WaferDiag{
			WaferDiag: &pb.WaferDiagPayload{
				Variants:           []int32{0, 2, 3},
				Duration:           10.5,
				SleepScale:         2.0,
				Memconfig:          true,
				NoMath:             false,
				ContinueAfterFail:  true,
				CoredumpOnFail:     false,
				SkipConfigApiCheck: true,
				Testmode:           true,
			},
		},
		DebugArgs: &commonpb.DebugArgs{
			DebugMgr: &commonpb.DebugArgs_DebugMGR{
				TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
					wsapisv1.WSReplicaTypeCoordinator.Lower(): {
						ContainerImage:   common.DefaultImage,
						ContainerCommand: []string{},
					},
				},
			},
		},
	}

	// Call CreateSystemMaintenance
	res, err := clusterClient.CreateSystemMaintenance(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Jobs, 1, "Expected exactly one wafer-diag job")
	require.Empty(t, res.FailedJobs, "No jobs should have failed")

	// Clean up the job after completion or failure
	defer deleteSysMaintWsjobs(t, wsjobClient, res.Jobs, ctx)

	job := res.Jobs[0]

	// Basic job validation
	assert.NotEmpty(t, job.JobId)
	assert.Equal(t, pb.JobStatus_JOB_STATUS_CREATED, job.Status)
	assert.Equal(t, pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_WAFER_DIAG, job.WorkflowType)
	assert.Contains(t, job.Systems, "cs-0")
	assert.Equal(t, int32(defaultSystemMaintenanceJobPriority), job.Priority, "Job should have correct priority")
	assert.NotNil(t, job.CreateTime, "Job should have creation time")
	assert.NotNil(t, job.User, "Job should have user information")
	assert.NotEmpty(t, job.User.Username, "Job should have username")

	// Retrieve the actual WSJob for detailed validation
	createdJob, err := wsjobClient.WsV1Client.Get(context.Background(), job.JobId, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, createdJob)

	// Verify WSJob has the correct labels for wafer-diag
	assert.Equal(t, wsapisv1.SystemMaintTypeWaferDiag, createdJob.Labels[wsapisv1.SystemMaintLabelKey],
		"WSJob should have wafer-diag maintenance type label")
	assert.NotEmpty(t, createdJob.Labels[wsapisv1.WorkflowIdLabelKey],
		"WSJob should have workflow ID label")

	// Get pod template for validation
	podTemplate := createdJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template
	require.NotNil(t, podTemplate, "PodTemplate must be set in the WSJob spec")

	t.Log("=== BASIC WAFER-DIAG SCHEDULING VALIDATION ===")

	// Verify basic pod configuration
	require.Len(t, podTemplate.Spec.Containers, 1, "Should have exactly one main container")
	mainContainer := podTemplate.Spec.Containers[0]

	// Check that it's using the test image (from DebugArgs)
	assert.Equal(t, common.DefaultImage, mainContainer.Image, "Should use test image from DebugArgs")

	// Verify basic environment variables are set
	envMap := make(map[string]string)
	for _, env := range mainContainer.Env {
		envMap[env.Name] = env.Value
	}

	assert.Equal(t, "cs-0", envMap["SYSTEM_NAME"], "SYSTEM_NAME should be set correctly")
	assert.Equal(t, "true", envMap["DRYRUN_MODE"], "DRYRUN_MODE should be enabled")
	assert.Contains(t, envMap["SCRIPT_PATH"], WaferDiagScriptName, "SCRIPT_PATH should reference wafer_diag.py")

	// Check for basic volume configuration
	volumeNames := make([]string, 0)
	for _, vol := range podTemplate.Spec.Volumes {
		volumeNames = append(volumeNames, vol.Name)
	}

	// Should have scripts volume
	assert.Contains(t, volumeNames, SystemMaintenanceVolumeName, "Should have system-maintenance-scripts volume")

	// Should have framework volume for wafer-diag

	t.Logf("✓ WSJob created successfully with ID: %s", job.JobId)
	t.Logf("✓ Job type: %v", job.WorkflowType)
	t.Logf("✓ Target system: %v", job.Systems)
	t.Logf("✓ Dryrun mode: %s", envMap["DRYRUN_MODE"])
	t.Logf("✓ Testmode enabled in payload")
	t.Logf("✓ Container image: %s", mainContainer.Image)
	t.Logf("✓ Volumes configured: %v", volumeNames)

	// Wait for job completion
	completedJob := assertWsJobCompletion(t, wsjobClient, job.JobId)
	t.Logf("Job %s completed successfully at %v", job.JobId, completedJob.Status.CompletionTime)
}

func TestGetSystemMaintenance(t *testing.T) {
	// Set up the test environment.
	wsjobClient, ctx, clusterClient, testEnv, cancel, cleanup := createTestEnv(t)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
		cleanup()
		assertClusterServerIngressCleanup(t, common.Namespace)
	}()

	// Create low level system maintenance membist wsjobs directly so to
	// isolate any potential issues with CreateSystemMaintenance.
	wsjobs := createSysMaintWsJobs(t, wsjobClient, ctx, clusterClient)

	// Add defer function to clean up jobs immediately after creation
	defer func() {
		// Convert WSJob slice to SystemMaintenanceJob slice for cleanup
		var jobs []*pb.SystemMaintenanceJob
		for _, wsjob := range wsjobs {
			jobs = append(jobs, &pb.SystemMaintenanceJob{
				JobId: wsjob.Name,
			})
		}
		if len(jobs) > 0 {
			deleteSysMaintWsjobs(t, wsjobClient, jobs, ctx)
		}
	}()

	// informer cache can take a second to fully populate the entries
	var getRes *pb.GetSystemMaintenanceResponse
	var getErr error
	require.Eventually(t, func() bool {
		getReq := &pb.GetSystemMaintenanceRequest{
			WorkflowId: wsjobs[0].Labels[wsapisv1.WorkflowIdLabelKey],
		}
		getRes, getErr = clusterClient.GetSystemMaintenance(ctx, getReq)
		require.NoError(t, getErr)
		require.NotNil(t, getRes)
		return len(wsjobs) == len(getRes.Jobs)
	}, 3*time.Second, 500*time.Millisecond)

	require.Equal(
		t,
		pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
		getRes.GetJobs()[0].WorkflowType,
		"Incorrect job workflow type",
	)
	require.Equal(
		t,
		wsjobs[0].Namespace,
		getRes.GetJobs()[0].Namespace,
		"Incorrect job namespace",
	)
}

func TestListSystemMaintenance(t *testing.T) {
	// Set up the test environment.
	wsjobClient, ctx, clusterClient, testEnv, cancel, cleanup := createTestEnv(t)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
		cleanup()
		assertClusterServerIngressCleanup(t, common.Namespace)
	}()

	// Create a request for system maintenance with one system.
	createReq := &pb.CreateSystemMaintenanceRequest{
		WorkflowType: pb.SystemMaintenanceType_SYSTEM_MAINTENANCE_TYPE_MEM_BIST,
		Systems:      []string{"cs-0"},
		AllowError:   false,
		DebugArgs: &commonpb.DebugArgs{
			DebugMgr: &commonpb.DebugArgs_DebugMGR{
				TaskSpecHints: map[string]*commonpb.DebugArgs_DebugMGR_DebugTaskSpec{
					wsapisv1.WSReplicaTypeCoordinator.Lower(): {
						ContainerImage:   common.DefaultImage,
						ContainerCommand: []string{"sleep", "5"},
					},
				},
			},
		},
	}

	// Call CreateSystemMaintenance.
	createRes1, createErr1 := clusterClient.CreateSystemMaintenance(ctx, createReq)
	require.NoError(t, createErr1)
	require.NotNil(t, createRes1)
	require.Equal(t, 1, len(createRes1.Jobs), "No maintenance jobs created")

	// Add defer function to clean up jobs immediately after first creation
	defer deleteSysMaintWsjobs(t, wsjobClient, createRes1.Jobs, ctx)

	// Sleep 2 seconds to ensure different create timestamp
	time.Sleep(2 * time.Second)

	// Call another CreateSystemMaintenance with 2 systems
	createReq.Systems = append(createReq.Systems, "cs-1")
	createRes2, createErr2 := clusterClient.CreateSystemMaintenance(ctx, createReq)
	require.NoError(t, createErr2)
	require.NotNil(t, createRes2)
	require.Equal(t, 2, len(createRes2.Jobs), "No maintenance jobs created")

	// Add defer function to clean up jobs immediately after second creation
	defer deleteSysMaintWsjobs(t, wsjobClient, createRes2.Jobs, ctx)

	// informer cache can take a second to fully populate the entries
	listReq := &pb.ListSystemMaintenanceRequest{
		AllStates: false,
	}
	var listRes *pb.ListSystemMaintenanceResponse
	var listErr error
	require.Eventually(t, func() bool {
		listRes, listErr = clusterClient.ListSystemMaintenance(ctx, listReq)
		require.NoError(t, listErr)
		require.NotNil(t, listRes)

		return len(listRes.GetWorkflows()) == 2
	}, 3*time.Second, 500*time.Millisecond)

	// First workflow should have 2 jobs
	require.Equal(
		t,
		len(listRes.GetWorkflows()[0].Jobs),
		len(createRes2.Jobs),
		"Mismatch number of maintenance jobs",
	)
	// Second workflow should have 1 jobs
	require.Equal(
		t,
		len(createRes1.Jobs),
		len(listRes.GetWorkflows()[1].Jobs),
		"Mismatch number of maintenance jobs",
	)
	// Workflow type and namespace should match job type
	for i := 0; i < len(listRes.GetWorkflows()); i++ {
		require.Equal(
			t,
			listRes.GetWorkflows()[i].Jobs[0].WorkflowType,
			listRes.GetWorkflows()[i].WorkflowType,
			"Incorrect workflow type",
		)
		require.Equal(
			t,
			listRes.GetWorkflows()[i].Jobs[0].Namespace,
			listRes.GetWorkflows()[i].Namespace,
			"Incorrect workflow namespace",
		)
	}
	// Wait up to 30 seconds for the second workflow to start
	require.Eventually(t, func() bool {
		listRes, listErr = clusterClient.ListSystemMaintenance(ctx, listReq)
		require.NoError(t, listErr)
		require.NotNil(t, listRes)
		jobStatus := listRes.GetWorkflows()[1].Jobs[0].Status
		return jobStatus != pb.JobStatus_JOB_STATUS_CREATED
	}, 30*time.Second, 1*time.Second)

	// First workflow should have one IN_QUEUE job
	var inQueueJobs = 0
	for j := 0; j < len(listRes.GetWorkflows()[0].GetJobs()); j++ {
		jobStatus := listRes.GetWorkflows()[0].Jobs[j].Status
		if jobStatus == pb.JobStatus_JOB_STATUS_IN_QUEUE {
			inQueueJobs++
		}
	}
	require.Equal(
		t,
		1,
		inQueueJobs,
		"Mismatch number of in-queue jobs",
	)

	// Wait up to 60 seconds for active workflows to reduce from 2 to 1
	require.Eventually(t, func() bool {
		listReq = &pb.ListSystemMaintenanceRequest{
			AllStates: false,
		}
		listRes, listErr = clusterClient.ListSystemMaintenance(ctx, listReq)
		require.NoError(t, listErr)
		require.NotNil(t, listRes)

		return (len(listRes.GetWorkflows()) == 1)
	}, 60*time.Second, 1*time.Second)

	// Get ALL workflows again
	listReq.AllStates = true
	listRes, listErr = clusterClient.ListSystemMaintenance(ctx, listReq)
	require.NoError(t, listErr)
	require.NotNil(t, listRes)

	// Expect 2 workflows
	require.Equal(t, 2, len(listRes.GetWorkflows()), "Number of workflows not equal to 2")

	// Wait up to 60 seconds for active workflows to reduce from 1 to 0
	require.Eventually(t, func() bool {
		listReq = &pb.ListSystemMaintenanceRequest{
			AllStates: false,
		}
		listRes, listErr = clusterClient.ListSystemMaintenance(ctx, listReq)
		require.NoError(t, listErr)
		require.NotNil(t, listRes)

		return (len(listRes.GetWorkflows()) == 0)
	}, 60*time.Second, 1*time.Second)

	// List ALL workflows again
	listReq.AllStates = true
	listRes, listErr = clusterClient.ListSystemMaintenance(ctx, listReq)
	require.NoError(t, listErr)
	require.NotNil(t, listRes)

	// Expect 2 workflows
	require.Equal(t, 2, len(listRes.GetWorkflows()), "Number of workflows not equal to 2")

	// All jobs should have succeeded
	workflows := listRes.GetWorkflows()
	for i := 0; i < len(workflows); i++ {
		jobs := workflows[i].GetJobs()
		for j := 0; j < len(jobs); j++ {
			require.Equal(
				t,
				pb.JobStatus_JOB_STATUS_SUCCEEDED,
				jobs[j].Status,
				"Incorrect job status",
			)
			// Make sure we have all timestamps populated
			require.NotNil(t, jobs[j].CreateTime)
			require.NotNil(t, jobs[j].ExecutionTime)
			require.NotNil(t, jobs[j].CompletionTime)
		}
	}
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

// assertWsJobCompletion polls until a WSJob has completed (CompletionTime is set)
// Returns the completed WSJob object
func assertWsJobCompletion(t *testing.T, wsjobClient *wsclient.WSJobClient, jobId string) *wsapisv1.WSJob {
	var completedJob *wsapisv1.WSJob

	require.Eventually(t, func() bool {
		job, err := wsjobClient.WsV1Client.Get(context.Background(), jobId, metav1.GetOptions{})
		if err != nil {
			t.Logf("Error getting job %s: %v", jobId, err)
			return false
		}

		if job.Status.CompletionTime != nil && !job.Status.CompletionTime.IsZero() {
			t.Logf("Job %s has completed at %v", jobId, job.Status.CompletionTime)
			completedJob = job
			return true
		}

		// Log current status for debugging
		t.Logf("Job %s still running", jobId)
		return false
	}, 60*time.Second, 1*time.Second, "Timed out waiting for job %s to complete", jobId)

	require.NotNil(t, completedJob, "Job should be retrieved after completion")

	return completedJob
}

func createTestEnv(t *testing.T) (
	*wsclient.WSJobClient,
	context.Context,
	pb.CsAdmV1Client,
	*envtest.Environment,
	context.CancelFunc,
	func(),
) {
	mgr, testEnv, _, cancel := common.EnvSetup(true, t)
	k8sClient = mgr.GetClient()

	kubeconfigPath := "../../../job-operator/.kindconfig"
	if _, err := os.Stat(kubeconfigPath); err != nil {
		if os.IsNotExist(err) {
			t.Fatalf("Expected .kindconfig file not found at %s. Make sure to run 'make kind-start' first.", kubeconfigPath)
		} else {
			t.Fatalf("Error accessing .kindconfig file at %s: %v", kubeconfigPath, err)
		}
	}
	t.Setenv("KUBECONFIG", kubeconfigPath)

	// Get the WSJob client and initialize the server and cluster client.
	wsjobClient := getWsJobClient(t, mgr)
	ctx, cleanupTestSetup, clusterServer, clusterClient := setup(false, wsjobClient.Clientset, wsjobClient, false)

	// Combine cleanup functions
	cleanup := func() {
		cleanupTestSetup()
	}

	assertClusterServerIngressSetup(t, common.Namespace)

	// Now the informers should work with the test environment config
	wsJobInformer, err := wsclient.NewWSJobInformer(ctx, true)
	require.NoError(t, err)
	clusterServer.wsJobInformer = wsJobInformer

	lockInformer, err := wsclient.NewLockInformer(ctx, true)
	require.NoError(t, err)
	clusterServer.lockInformer = lockInformer

	return wsjobClient, ctx, clusterClient, testEnv, cancel, cleanup

}

// Create 2 system maintenance wsjobs
func createSysMaintWsJobs(
	t *testing.T,
	wsjobClient *wsclient.WSJobClient,
	ctx context.Context,
	clusterClient pb.CsAdmV1Client,
) []*wsapisv1.WSJob {
	meta, _ := ctx.Value(pkg.ClientMetaKey).(pkg.ClientMeta)
	workflowID := wsclient.GenWorkflowId()
	maintenanceTypeLabel := wsapisv1.SystemMaintTypeMemBist
	allowError := false

	// Create a wsjob for each system
	var jobs []*wsapisv1.WSJob

	for _, system := range [2]string{"cs-0", "cs-1"} {
		// Create job spec with proper configuration
		jobSpec := &wsclient.JobSpec{
			// system maint job will be of type execute
			WsJobMode: wsclient.ExecuteJobMode,
			Namespace: common.Namespace,
			NumWafers: 1,
			JobUser: wsapisv1.JobUser{
				Uid:      meta.UID,
				Gid:      meta.GID,
				Username: meta.Username,
				Groups:   meta.Groups,
			},
			// anonymous func to convert priority to pointer
			Priority: common.Pointer(defaultSystemMaintenanceJobPriority),
			Labels: map[string]string{
				wsapisv1.WorkflowIdLabelKey:          workflowID,
				wsapisv1.ClientLeaseDisabledLabelKey: "",
				wsapisv1.JobTypeLabelKey:             wsapisv1.JobTypeExecute,
				wsapisv1.JobModeLabelKey:             wsapisv1.JobModeWeightStreaming,
				wsapisv1.SystemMaintLabelKey:         maintenanceTypeLabel,
			},
			Annotations: map[string]string{
				wsapisv1.SchedulerHintAllowSys:        system,
				common.ClusterServerVersion:           pkg.CerebrasVersion,
				wsapisv1.SemanticClusterServerVersion: wsclient.SemanticVersion,
			},
			AllowUnhealthySystems: allowError,
		}

		// Set up volumes using ConfigMap
		jobSpec.ScriptRootVolume = &v1.Volume{
			Name: SystemMaintenanceVolumeName,
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: SystemMaintenanceConfigMapName,
					},
				},
			},
		}
		jobSpec.ScriptRootVolumeMount = &v1.VolumeMount{
			Name:      SystemMaintenanceVolumeName,
			MountPath: SystemMaintenanceMountPath,
		}

		// Configure the service spec for the WSJob (here we use WsCoordinator)
		specs := make(wsclient.ServiceSpecs, wsclient.NumWsServices)

		scriptPath := filepath.Join(SystemMaintenanceMountPath, "membist.py")
		image := fmt.Sprintf("%s/python:3.11", wsapisv1.RegistryUrl)
		command := []string{"python", scriptPath}
		specs[wsclient.WsCoordinator] = wsclient.ServiceSpec{
			Image:   image,
			Command: command,
			Env: []v1.EnvVar{
				{Name: "SYSTEM_NAME", Value: system},
				// Use dynamic script path based on workflow type.
				{Name: "SCRIPT_PATH", Value: scriptPath},
			},
			Replicas: 1,
		}

		wsjob, err := wsclient.BuildWSJobFromSpecs(specs, *jobSpec)
		require.NoError(t, err)
		job, err := wsjobClient.Create(wsjob)
		require.NoError(t, err)

		jobs = append(jobs, job)
	}

	return jobs
}

func deleteSysMaintWsjobs(
	t *testing.T,
	wsjobClient *wsclient.WSJobClient,
	wsjobs []*pb.SystemMaintenanceJob,
	ctx context.Context) {
	t.Log("Cleaning up created maintenance jobs...")
	for _, job := range wsjobs {
		t.Logf("Deleting job %s", job.JobId)
		deleteErr := wsjobClient.WsV1Client.Delete(ctx, job.JobId, metav1.DeleteOptions{})
		if deleteErr != nil {
			// Log but don't fail the test for cleanup errors
			t.Logf("Warning: Failed to delete job %s: %v", job.JobId, deleteErr)
		}
	}
}

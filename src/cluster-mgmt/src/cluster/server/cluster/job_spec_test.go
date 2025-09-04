//go:build !e2e

// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	commonpb "cerebras/pb/workflow/appliance/common"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

func TestGetCleanPodPolicy(t *testing.T) {
	tests := []struct {
		name           string
		providedPolicy commonpb.DebugArgs_DebugMGR_CleanPodPolicy
		returnPolicy   commonv1.CleanPodPolicy
	}{
		{
			"Test CleanPodPolicyAll",
			commonpb.DebugArgs_DebugMGR_ALL,
			commonv1.CleanPodPolicyAll,
		},
		{
			"Test CleanPodPolicyNone",
			commonpb.DebugArgs_DebugMGR_NONE,
			commonv1.CleanPodPolicyNone,
		},
		{
			"Test CleanPodPolicyRunning",
			commonpb.DebugArgs_DebugMGR_RUNNING,
			commonv1.CleanPodPolicyRunning,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			debugArgs := commonpb.DebugArgs{
				DebugMgr: &commonpb.DebugArgs_DebugMGR{
					CleanPodPolicy: test.providedPolicy,
				},
			}
			cleanPodPolicy := getCleanPodPolicy(&debugArgs)
			assert.True(t, cleanPodPolicy == test.returnPolicy, "cleanPodPolicy should be %s", test.providedPolicy)
		})
	}

	// Test nil DebugMgr.
	debugArgs := commonpb.DebugArgs{}
	cleanPodPolicy := getCleanPodPolicy(&debugArgs)
	assert.True(t, cleanPodPolicy == commonv1.CleanPodPolicyRunning, "cleanPodPolicy should be All")
}

func TestNewJobSpec(t *testing.T) {
	fakeK8s := fake.NewSimpleClientset()
	clusterVolumesConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: wscommon.Namespace,
			Name:      "cluster-server-volumes",
		},
		Data: map[string]string{
			"workdir-volume-1": `{"type":"nfs","server":"nfs.example.com","serverPath":"/nfspath1","containerPath":"/temp/nfspath1"}`,
			"compile-volume-1": `{"type":"nfs","server":"nfs.example.com","serverPath":"/nfspath2","containerPath":"/temp/nfspath2"}`,
			"workdir-volume-2": `{"type":"nfs","server":"nfs.example.com","serverPath":"/nfspath3","containerPath":"/temp/nfspath3"}`,
			"compile-volume-2": `{"type":"nfs","server":"nfs.example.com","serverPath":"/nfspath4","containerPath":"/temp/nfspath4"}`,
		},
	}
	_, err := fakeK8s.CoreV1().ConfigMaps(wscommon.Namespace).Create(context.TODO(), clusterVolumesConfigMap, metav1.CreateOptions{})
	require.NoError(t, err)
	ctx, cleanup, server, _ := setup(false, fakeK8s, mockedWsJobClient, nil, false)
	defer cleanup()
	numWafers := 1
	relativeCompileDir := "/my-compile"
	userDetails := pkg.ClientMeta{
		UID:                   int64(1000),
		GID:                   int64(1001),
		Username:              "some-user",
		ClientBuildVersion:    "build-xyz",
		ClientSemanticVersion: "2.0.0",
	}
	for _, testcase := range []struct {
		name                            string
		jobMode                         wsclient.WSJobMode
		jpConfig                        wsclient.JobPathConfig
		debugArgs                       *commonpb.DebugArgs
		expectedWorkdirLogPathPrefix    string
		expectedCachedCompilePathPrefix string
		expectedNumPopNgs               int
		expectedPopNgTypes              string
		isMultiMgmtCluster              bool

		expectLeaseDisabled     bool
		expectCrdAddressHandoff bool
		expectedErrorContains   string

		notificationArgs    []*pb.UserNotification
		expectNotifications []*wsapisv1.UserNotification
	}{
		{
			"with debug args set - nfs volume as default",
			wsclient.CompileJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
				WorkdirLogsNfsPath:       "/temp/nfspath1/workdir_path",
				CachedCompileNfsPath:     "/temp/nfspath2/cached_compile_path",
			},
			&commonpb.DebugArgs{
				DebugMgr: &commonpb.DebugArgs_DebugMGR{
					Labels: map[string]string{
						"wsjob_model": "fcmnist",
					},
					NumPopulatedNodegroups: 2,
					PopulatedGroupTypes: []commonpb.DebugArgs_DebugMGR_PopulatedGroupType{
						commonpb.DebugArgs_DebugMGR_POPULATED_GROUP_TYPE_XLARGE,
					},
					NfsWorkdirLogsPath:   "/temp/nfspath3/workdir_path",
					NfsCachedCompilePath: "/temp/nfspath4/cached_compile_path",
				},
			},
			"/temp/nfspath3/workdir_path",
			"/temp/nfspath4/cached_compile_path",
			2,
			wsapisv1.PopNgTypeXLarge,
			false,
			false,
			false,
			"",
			nil, nil,
		},
		{
			"with debug args set - local as default",
			wsclient.CompileJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
			},
			&commonpb.DebugArgs{
				DebugMgr: &commonpb.DebugArgs_DebugMGR{
					Labels: map[string]string{
						"wsjob_model": "fcmnist",
					},
					NumPopulatedNodegroups: 2,
					PopulatedGroupTypes: []commonpb.DebugArgs_DebugMGR_PopulatedGroupType{
						commonpb.DebugArgs_DebugMGR_POPULATED_GROUP_TYPE_XLARGE,
					},
					NfsWorkdirLogsPath:      "/temp/nfspath3/workdir_path",
					NfsCachedCompilePath:    "/temp/nfspath4/cached_compile_path",
					EnableCrdAddressHandoff: true,
				},
			},
			"/temp/nfspath3/workdir_path",
			"/temp/nfspath4/cached_compile_path",
			2,
			wsapisv1.PopNgTypeXLarge,
			false,
			false,
			true,
			"",
			nil, nil,
		},
		{
			"without debug args set - nfs volume as default",
			wsclient.CompileJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
				WorkdirLogsNfsPath:       "/temp/nfspath1/workdir_path",
				CachedCompileNfsPath:     "/temp/nfspath2/cached_compile_path",
			},
			&commonpb.DebugArgs{
				DebugMgr: &commonpb.DebugArgs_DebugMGR{},
			},
			"/temp/nfspath1/workdir_path",
			"/temp/nfspath2/cached_compile_path",
			1,
			"",
			true,
			false,
			true,
			"",
			nil, nil,
		},
		{
			"without debug args set - local as default",
			wsclient.CompileJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
			},
			&commonpb.DebugArgs{
				DebugMgr: &commonpb.DebugArgs_DebugMGR{},
			},
			wsclient.WorkdirLogsRootPath,
			wsclient.CachedCompileRootPath,
			1,
			"",
			false,
			false,
			false,
			"",
			nil, nil,
		},
		{
			"error due to invalid volume overrides",
			wsclient.CompileJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
			},
			&commonpb.DebugArgs{
				DebugMgr: &commonpb.DebugArgs_DebugMGR{
					NfsWorkdirLogsPath: "/foo/bar",
				},
			},
			"",
			"",
			1,
			"",
			false,
			false,
			false,
			"Invalid volume mount '/foo/bar': use one of [/temp/nfspath1 /temp/nfspath2 /temp/nfspath3 /temp/nfspath4] as path prefix",
			nil, nil,
		},
		{
			"email notifications",
			wsclient.CompileJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
			},
			&commonpb.DebugArgs{},
			wsclient.WorkdirLogsRootPath,
			wsclient.CachedCompileRootPath,
			1,
			"",
			false,
			false,
			false,
			"",
			[]*pb.UserNotification{
				{
					NotificationType: pb.NotificationType_NOTIFICATION_TYPE_EMAIL,
					Target:           "test@example.com",
				},
			},
			[]*wsapisv1.UserNotification{
				{
					NotificationType: wsapisv1.NotificationTypeEmail,
					Target:           "test@example.com",
				},
			},
		},
		{
			"slack & pagerduty notifications",
			wsclient.CompileJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
			},
			&commonpb.DebugArgs{},
			wsclient.WorkdirLogsRootPath,
			wsclient.CachedCompileRootPath,
			1,
			"",
			false,
			false,
			false,
			"",
			[]*pb.UserNotification{
				{
					NotificationType: pb.NotificationType_NOTIFICATION_TYPE_SLACK,
					Target:           "slack-webhook-1",
				},
				{
					NotificationType: pb.NotificationType_NOTIFICATION_TYPE_PAGERDUTY,
					Target:           "pagerduty-key-1",
				},
			},
			[]*wsapisv1.UserNotification{
				{
					NotificationType: wsapisv1.NotificationTypeSlack,
					Target:           "slack-webhook-1",
				},
				{
					NotificationType: wsapisv1.NotificationTypePagerduty,
					Target:           "pagerduty-key-1",
				},
			},
		},
		{
			"multiple notifications",
			wsclient.CompileJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
			},
			&commonpb.DebugArgs{},
			wsclient.WorkdirLogsRootPath,
			wsclient.CachedCompileRootPath,
			1,
			"",
			false,
			false,
			false,
			"",
			[]*pb.UserNotification{
				{
					NotificationType: pb.NotificationType_NOTIFICATION_TYPE_EMAIL,
					Target:           "test@example.com",
				},
				{
					NotificationType:  pb.NotificationType_NOTIFICATION_TYPE_EMAIL,
					Target:            "test2@example.com",
					SeverityThreshold: wscommon.Pointer(int32(2)),
				},
				{
					NotificationType:  pb.NotificationType_NOTIFICATION_TYPE_EMAIL,
					Target:            "test3@example.com",
					SeverityThreshold: wscommon.Pointer(int32(3)),
				},
				{
					NotificationType:  pb.NotificationType_NOTIFICATION_TYPE_SLACK,
					Target:            "slack-webhook-2",
					SeverityThreshold: wscommon.Pointer(int32(4)),
				},
				{
					NotificationType:  pb.NotificationType_NOTIFICATION_TYPE_PAGERDUTY,
					Target:            "pagerduty-key-2",
					SeverityThreshold: wscommon.Pointer(int32(4)),
				},
			},
			[]*wsapisv1.UserNotification{
				{
					NotificationType: wsapisv1.NotificationTypeEmail,
					Target:           "test@example.com",
				},
				{
					NotificationType:  wsapisv1.NotificationTypeEmail,
					Target:            "test2@example.com",
					SeverityThreshold: wscommon.Pointer(int32(2)),
				},
				{
					NotificationType:  wsapisv1.NotificationTypeEmail,
					Target:            "test3@example.com",
					SeverityThreshold: wscommon.Pointer(int32(3)),
				},
				{
					NotificationType:  wsapisv1.NotificationTypeSlack,
					Target:            "slack-webhook-2",
					SeverityThreshold: wscommon.Pointer(int32(4)),
				},
				{
					NotificationType:  wsapisv1.NotificationTypePagerduty,
					Target:            "pagerduty-key-2",
					SeverityThreshold: wscommon.Pointer(int32(4)),
				},
			},
		},
		{
			"weight sidecars",
			wsclient.ExecuteJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
			},
			&commonpb.DebugArgs{
				DebugMgr: &commonpb.DebugArgs_DebugMGR{
					// Enable sidecars for both activation and weight pods
					WorkerSidecar: &commonpb.DebugArgs_DebugMGR_UserSidecar{
						Strategy:   commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED,
						CpuPercent: 25, // 25% CPU for worker sidecar
						MemPercent: 25, // 25% memory for worker sidecar
						Image:      "sidecar-image:latest",
					},
					WeightSidecar: &commonpb.DebugArgs_DebugMGR_UserSidecar{
						Strategy:   commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED,
						CpuPercent: 25, // 25% CPU for weight sidecar
						MemPercent: 25, // 25% memory for weight sidecar
						Image:      "sidecar-image:latest",
					},
				},
			},
			wsclient.WorkdirLogsRootPath,
			wsclient.CachedCompileRootPath,
			1,
			"",
			false,
			false,
			false,
			"",
			nil, nil,
		},
		{
			"activation sidecars",
			wsclient.InferenceExecuteJobMode,
			wsclient.JobPathConfig{
				LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
				CachedCompileRootPath:    wsclient.CachedCompileRootPath,
			},
			&commonpb.DebugArgs{
				DebugMgr: &commonpb.DebugArgs_DebugMGR{
					// Enable sidecars for both activation and weight pods
					WorkerSidecar: &commonpb.DebugArgs_DebugMGR_UserSidecar{
						Strategy:   commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED,
						CpuPercent: 25, // 25% CPU for worker sidecar
						MemPercent: 25, // 25% memory for worker sidecar
						Image:      "sidecar-image:latest",
					},
					ActivationSidecar: &commonpb.DebugArgs_DebugMGR_UserSidecar{
						Strategy:   commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED,
						CpuPercent: 25, // 25% CPU for activation sidecar
						MemPercent: 25, // 25% memory for activation sidecar
						Image:      "sidecar-image:latest",
					},
				},
			},
			wsclient.WorkdirLogsRootPath,
			wsclient.CachedCompileRootPath,
			1,
			"",
			false,
			false,
			false,
			"",
			nil, nil,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			server.options.JobPathConfig = testcase.jpConfig
			server.options.HasMultiMgmtNodes = testcase.isMultiMgmtCluster
			setOperatorVersion(t, fakeK8s, defaultOperatorVersion)
			jobSpec, err := server.newJobSpec(ctx, wscommon.Namespace, numWafers, relativeCompileDir, nil,
				testcase.debugArgs, nil, userDetails, testcase.jobMode, testcase.notificationArgs)
			if testcase.expectedErrorContains == "" {
				require.NoError(t, err)
				assert.Equal(t, "build-xyz", jobSpec.Annotations[wscommon.AppClientVersion])
				assert.Equal(t, "2.0.0", jobSpec.Annotations[wsapisv1.SemanticApplianceClientVersion])
				if testcase.jpConfig.WorkdirLogsNfsPath != "" || (testcase.debugArgs != nil && testcase.debugArgs.DebugMgr != nil && testcase.debugArgs.DebugMgr.NfsWorkdirLogsPath != "") {
					assert.True(t, strings.HasPrefix(jobSpec.Annotations[wsapisv1.WorkdirLogsMountDirAnnot], testcase.expectedWorkdirLogPathPrefix))
				} else {
					assert.True(t, jobSpec.Annotations[wsapisv1.WorkdirLogsMountDirAnnot] == "")
				}
				if testcase.jpConfig.CachedCompileNfsPath != "" || (testcase.debugArgs != nil && testcase.debugArgs.DebugMgr != nil && testcase.debugArgs.DebugMgr.NfsCachedCompilePath != "") {
					assert.True(t, strings.HasPrefix(jobSpec.Annotations[wsapisv1.CachedCompileMountDirAnnot], testcase.expectedCachedCompilePathPrefix))
				} else {
					assert.True(t, jobSpec.Annotations[wsapisv1.CachedCompileMountDirAnnot] == "")
				}
				assert.Equal(t, path.Join(testcase.expectedWorkdirLogPathPrefix, jobSpec.Namespace, jobSpec.JobName), jobSpec.WorkdirLogPath)
				assert.Equal(t, path.Join(testcase.expectedWorkdirLogPathPrefix, jobSpec.Namespace, jobSpec.JobName), jobSpec.WorkdirLogsVolumeMount.MountPath)
				assert.Equal(t, path.Join(testcase.expectedCachedCompilePathPrefix, jobSpec.Namespace, relativeCompileDir), jobSpec.CachedCompileVolumeMount.MountPath)
				assert.Equal(t, testcase.expectCrdAddressHandoff, jobSpec.EnableCrdAddrHandoff)
				_, ok := jobSpec.Labels[wsapisv1.ClientLeaseDisabledLabelKey]
				assert.Equal(t, testcase.expectLeaseDisabled, ok)

				if testcase.expectedNumPopNgs > 1 {
					numPopNgs, err := strconv.Atoi(jobSpec.Annotations[wsapisv1.WsJobPopulatedNodegroupCount])
					assert.NoError(t, err)
					assert.Equal(t, testcase.expectedNumPopNgs, numPopNgs)
				} else {
					_, ok := jobSpec.Annotations[wsapisv1.WsJobPopulatedNodegroupCount]
					assert.False(t, ok)
				}

				if testcase.expectedPopNgTypes != "" {
					popNgTypes := jobSpec.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes]
					assert.Equal(t, testcase.expectedPopNgTypes, popNgTypes)
				} else {
					_, ok := jobSpec.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes]
					assert.False(t, ok)
				}

				if testcase.notificationArgs != nil {
					assert.Equal(t, testcase.expectNotifications, jobSpec.Notifications)
				}

				// Verify activation sidecar configuration
				if testcase.debugArgs != nil && testcase.debugArgs.DebugMgr != nil &&
					testcase.debugArgs.DebugMgr.ActivationSidecar != nil &&
					testcase.debugArgs.DebugMgr.ActivationSidecar.Strategy == commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED {
					assert.True(t, jobSpec.ActivationSidecar.Enabled)
					assert.Equal(t, int(testcase.debugArgs.DebugMgr.ActivationSidecar.CpuPercent), jobSpec.ActivationSidecar.CpuPercent)
					assert.Equal(t, int(testcase.debugArgs.DebugMgr.ActivationSidecar.MemPercent), jobSpec.ActivationSidecar.MemPercent)
					assert.Equal(t, testcase.debugArgs.DebugMgr.ActivationSidecar.Image, jobSpec.ActivationSidecar.Image)
				}

				// Verify weight sidecar configuration
				if testcase.debugArgs != nil && testcase.debugArgs.DebugMgr != nil &&
					testcase.debugArgs.DebugMgr.WeightSidecar != nil &&
					testcase.debugArgs.DebugMgr.WeightSidecar.Strategy == commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED {
					assert.True(t, jobSpec.WeightSidecar.Enabled)
					assert.Equal(t, int(testcase.debugArgs.DebugMgr.WeightSidecar.CpuPercent), jobSpec.WeightSidecar.CpuPercent)
					assert.Equal(t, int(testcase.debugArgs.DebugMgr.WeightSidecar.MemPercent), jobSpec.WeightSidecar.MemPercent)
					assert.Equal(t, testcase.debugArgs.DebugMgr.WeightSidecar.Image, jobSpec.WeightSidecar.Image)
				}

				// Verify worker sidecar configuration
				if testcase.debugArgs != nil && testcase.debugArgs.DebugMgr != nil &&
					testcase.debugArgs.DebugMgr.WorkerSidecar != nil &&
					testcase.debugArgs.DebugMgr.WorkerSidecar.Strategy == commonpb.DebugArgs_DebugMGR_UserSidecar_STRATEGY_ENABLED {
					assert.True(t, jobSpec.WorkerSidecar.Enabled)
					assert.Equal(t, int(testcase.debugArgs.DebugMgr.WorkerSidecar.CpuPercent), jobSpec.WorkerSidecar.CpuPercent)
					assert.Equal(t, int(testcase.debugArgs.DebugMgr.WorkerSidecar.MemPercent), jobSpec.WorkerSidecar.MemPercent)
					assert.Equal(t, testcase.debugArgs.DebugMgr.WorkerSidecar.Image, jobSpec.WorkerSidecar.Image)
				}
			} else {
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), testcase.expectedErrorContains)
			}
		})
	}
}

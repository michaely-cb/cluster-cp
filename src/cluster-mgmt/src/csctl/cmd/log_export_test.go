package cmd

import (
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csctlv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/cluster/csctl/pkg"
)

// MockInClusterNodeChecker implements InClusterNodeChecker for testing
type MockInClusterNodeChecker struct {
	IsInClusterNode     bool
	IsSingleNodeCluster bool
}

func (m MockInClusterNodeChecker) CheckInClusterNode() bool {
	return m.IsInClusterNode
}

func (m MockInClusterNodeChecker) CheckSingleNodeCluster() bool {
	return m.IsSingleNodeCluster
}

func TestLogExport(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
		ClientFactory:        clientFactory,
		Namespace:            "foo",
		InClusterNodeChecker: MockInClusterNodeChecker{
			IsInClusterNode:     false, // Mock as user node
			IsSingleNodeCluster: false, // Mock as not single-node cluster
		},
	}

	t.Run("CreateLogExport happy path", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "csctl-logexport*")
		require.NoError(t, err)
		defer func() { _ = os.RemoveAll(tempDir) }()

		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"log-export", "happy-xxx", "-p", tempDir})

		uid, gid, groups, err := ctx.UserIdentityProvider.GetUidGidGroups()
		require.NoError(t, err)

		job := &csctlv1.Job{
			Meta: &csctlv1.ObjectMeta{
				Name: "happy-xxx",
			},
			Spec: &csctlv1.JobSpec{
				User: &csctlv1.JobUser{
					Uid:    uid,
					Gid:    gid,
					Groups: groups,
				},
			},
		}
		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		res := &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}

		clientFactory.AppendCsCtlResponse(res)
		clientFactory.AppendCsCtlResponse(&pb.CreateLogExportResponse{ExportId: "happy-xxx"})
		clientFactory.AppendCsCtlResponse(&pb.GetLogExportResponse{Status: pb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED})
		clientFactory.AppendCsCtlResponse(&pb.DownloadLogExportResponse{EstimatedRemainingBytes: 0})

		require.NoError(t, cmd.Execute())
		assert.FileExists(t, tempDir+"/happy-xxx.zip")
	})

	t.Run("CreateLogExport with tasks filter - happy path", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "csctl-logexport*")
		require.NoError(t, err)
		defer func() { _ = os.RemoveAll(tempDir) }()

		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"log-export", "happy-xxx", "-p", tempDir, "--tasks", "coordinator-0,chief-0"})

		uid, gid, groups, err := ctx.UserIdentityProvider.GetUidGidGroups()
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(makeJob("happy-xxx", uid, gid, groups))
		require.NoError(t, err)
		res := &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}

		clientFactory.AppendCsCtlResponse(res)
		clientFactory.AppendCsCtlResponse(&pb.CreateLogExportResponse{ExportId: "happy-xxx"})
		clientFactory.AppendCsCtlResponse(&pb.GetLogExportResponse{Status: pb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED})
		clientFactory.AppendCsCtlResponse(&pb.DownloadLogExportResponse{EstimatedRemainingBytes: 0})

		require.NoError(t, cmd.Execute())
		assert.FileExists(t, tempDir+"/happy-xxx.zip")

		cmd = NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"log-export", "happy-yyy", "-p", tempDir, "--tasks", "coordinator"})
		jobBytes, err = proto.Marshal(makeJob("happy-yyy", uid, gid, groups))
		require.NoError(t, err)
		res = &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}

		clientFactory.AppendCsCtlResponse(res)
		clientFactory.AppendCsCtlResponse(&pb.CreateLogExportResponse{ExportId: "happy-yyy"})
		clientFactory.AppendCsCtlResponse(&pb.GetLogExportResponse{Status: pb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED})
		clientFactory.AppendCsCtlResponse(&pb.DownloadLogExportResponse{EstimatedRemainingBytes: 0})

		require.NoError(t, cmd.Execute())
		assert.FileExists(t, tempDir+"/happy-yyy.zip")

		cmd = NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"log-export", "happy-zzz", "-p", tempDir, "--tasks", "coordinator", "--target-type", "debug-volume"})
		jobBytes, err = proto.Marshal(makeJob("happy-zzz", uid, gid, groups))
		require.NoError(t, err)
		res = &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}

		clientFactory.AppendCsCtlResponse(res)
		clientFactory.AppendCsCtlResponse(&pb.CreateLogExportResponse{ExportId: "happy-zzz"})
		clientFactory.AppendCsCtlResponse(&pb.GetLogExportResponse{Status: pb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED})

		require.NoError(t, cmd.Execute())
	})

	t.Run("CreateLogExport with tasks filter - invalid tasks", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "csctl-logexport*")
		require.NoError(t, err)
		defer func() { _ = os.RemoveAll(tempDir) }()

		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"log-export", "happy-xxx", "-p", tempDir, "--tasks", "coordinator-0,chief-0,chief-1"})

		uid, gid, groups, err := ctx.UserIdentityProvider.GetUidGidGroups()
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(makeJob("happy-xxx", uid, gid, groups))
		require.NoError(t, err)
		res := &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}

		clientFactory.AppendCsCtlResponse(res)
		err = cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "found invalid task(s)")
	})
}

func TestLogExportMgmtNode(t *testing.T) {
	t.Run("Mgmt node without force flag should fail", func(t *testing.T) {
		clientFactory := NewFakeClientFactory()
		ctx := pkg.CmdCtx{
			UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
			ClientFactory:        clientFactory,
			Namespace:            pkg.SystemNamespace,
			InClusterNodeChecker: MockInClusterNodeChecker{
				IsInClusterNode:     true,  // Mock as management node
				IsSingleNodeCluster: false, // Mock as not single-node cluster
			},
		}

		uid, gid, groups, err := ctx.UserIdentityProvider.GetUidGidGroups()
		require.NoError(t, err)
		jobBytes, err := proto.Marshal(makeJob("test-job", uid, gid, groups))
		require.NoError(t, err)
		res := &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}
		clientFactory.AppendCsCtlResponse(res)

		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"log-export", "test-job"})

		err = cmd.Execute()
		require.Error(t, err, "Command should fail on mgmt node without --force")
		assert.Contains(t, err.Error(), "log export is not allowed from within the cluster unless --force is specified")
	})

	t.Run("Mgmt node with force flag should succeed", func(t *testing.T) {
		clientFactory := NewFakeClientFactory()
		ctx := pkg.CmdCtx{
			UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
			ClientFactory:        clientFactory,
			Namespace:            pkg.SystemNamespace,
			InClusterNodeChecker: MockInClusterNodeChecker{
				IsInClusterNode:     true,  // Mock as management node
				IsSingleNodeCluster: false, // Mock as not single-node cluster
			},
		}

		tempDir, err := os.MkdirTemp("", "csctl-logexport*")
		require.NoError(t, err)
		defer func() { _ = os.RemoveAll(tempDir) }()

		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetErr(output.E)
		cmd.SetArgs([]string{"log-export", "test-job", "-p", tempDir, "--force"})

		uid, gid, groups, err := ctx.UserIdentityProvider.GetUidGidGroups()
		require.NoError(t, err)
		jobBytes, err := proto.Marshal(makeJob("test-job", uid, gid, groups))
		require.NoError(t, err)
		res := &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}

		clientFactory.AppendCsCtlResponse(res)
		clientFactory.AppendCsCtlResponse(&pb.CreateLogExportResponse{ExportId: "test-job"})
		clientFactory.AppendCsCtlResponse(&pb.GetLogExportResponse{Status: pb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED})
		clientFactory.AppendCsCtlResponse(&pb.DownloadLogExportResponse{EstimatedRemainingBytes: 0})

		require.NoError(t, cmd.Execute())
		assert.FileExists(t, tempDir+"/test-job.zip")
		assert.Contains(t, output.O.String(), "Warning: Exporting logs inside the cluster is not recommended")
	})

	t.Run("User node should work normally", func(t *testing.T) {
		clientFactory := NewFakeClientFactory()
		ctx := pkg.CmdCtx{
			UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
			ClientFactory:        clientFactory,
			Namespace:            "foo",
			InClusterNodeChecker: MockInClusterNodeChecker{
				IsInClusterNode:     false, // Mock as user node
				IsSingleNodeCluster: false, // Mock as not single-node cluster
			},
		}

		tempDir, err := os.MkdirTemp("", "csctl-logexport*")
		require.NoError(t, err)
		defer func() { _ = os.RemoveAll(tempDir) }()

		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"log-export", "test-job", "-p", tempDir})

		uid, gid, groups, err := ctx.UserIdentityProvider.GetUidGidGroups()
		require.NoError(t, err)
		jobBytes, err := proto.Marshal(makeJob("test-job", uid, gid, groups))
		require.NoError(t, err)
		res := &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}

		clientFactory.AppendCsCtlResponse(res)
		clientFactory.AppendCsCtlResponse(&pb.CreateLogExportResponse{ExportId: "test-job"})
		clientFactory.AppendCsCtlResponse(&pb.GetLogExportResponse{Status: pb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED})
		clientFactory.AppendCsCtlResponse(&pb.DownloadLogExportResponse{EstimatedRemainingBytes: 0})

		require.NoError(t, cmd.Execute())
		assert.FileExists(t, tempDir+"/test-job.zip")
		// Should not contain warning message for user nodes
		assert.NotContains(t, output.O.String(), "Warning: Exporting logs inside the cluster is not recommended")
	})

	t.Run("Mgmt node single-node cluster should print only single-node warning", func(t *testing.T) {

		clientFactory := NewFakeClientFactory()
		ctx := pkg.CmdCtx{
			UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
			ClientFactory:        clientFactory,
			Namespace:            pkg.SystemNamespace,
			InClusterNodeChecker: MockInClusterNodeChecker{
				IsInClusterNode:     true, // Mock as management node
				IsSingleNodeCluster: true, // Mock as single-node cluster
			},
		}

		tempDir, err := os.MkdirTemp("", "csctl-logexport*")
		require.NoError(t, err)
		defer func() { _ = os.RemoveAll(tempDir) }()

		output := NewFakeOutErr()
		cmd := NewCsCtlCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetErr(output.E)
		cmd.SetArgs([]string{"log-export", "test-job", "-p", tempDir})

		uid, gid, groups, err := ctx.UserIdentityProvider.GetUidGidGroups()
		require.NoError(t, err)
		jobBytes, err := proto.Marshal(makeJob("test-job", uid, gid, groups))
		require.NoError(t, err)
		res := &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: jobBytes}

		clientFactory.AppendCsCtlResponse(res)
		clientFactory.AppendCsCtlResponse(&pb.CreateLogExportResponse{ExportId: "test-job"})
		clientFactory.AppendCsCtlResponse(&pb.GetLogExportResponse{Status: pb.LogExportStatus_LOG_EXPORT_STATUS_COMPLETED})
		clientFactory.AppendCsCtlResponse(&pb.DownloadLogExportResponse{EstimatedRemainingBytes: 0})

		require.NoError(t, cmd.Execute())
		assert.FileExists(t, tempDir+"/test-job.zip")
		assert.Contains(t, output.O.String(), "Warning: Allowing log-export without --force since cluster is single-node. This may cause disk pressure")

	})
}

func makeJob(jobName string, uid, gid int64, groups []int64) *csctlv1.Job {
	return &csctlv1.Job{
		Meta: &csctlv1.ObjectMeta{
			Name: jobName,
		},
		Spec: &csctlv1.JobSpec{
			User: &csctlv1.JobUser{
				Uid:    uid,
				Gid:    gid,
				Groups: groups,
			},
		},
		Status: &csctlv1.JobStatus{
			Replicas: []*csctlv1.JobStatus_ReplicaGroup{
				{
					Type: "coordinator",
					Instances: []*csctlv1.JobStatus_ReplicaInstance{
						{
							Name: fmt.Sprintf("%s-coordinator-0", jobName),
						},
					},
				},
				{
					Type: "chief",
					Instances: []*csctlv1.JobStatus_ReplicaInstance{
						{
							Name: fmt.Sprintf("%s-chief-0", jobName),
						},
					},
				},
			},
		},
	}
}

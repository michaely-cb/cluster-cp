package cmd

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"

	"cerebras.com/cluster/csctl/pkg"
)

func TestSessionsPermission(t *testing.T) {
	// Test non-management node permissions
	t.Run("non-management node", func(t *testing.T) {
		clientFactory := NewFakeClientFactory()
		ctx := pkg.CmdCtx{
			ClientFactory: clientFactory,
			OnMgmtNode:    false, // Non-management node
		}

		// Get session should now be ALLOWED for non-admin users
		t.Run("get session allowed", func(t *testing.T) {
			output := NewFakeOutErr()
			cmd := NewSessionCmd(&ctx)
			cmd.SetOut(output.O)
			cmd.SetArgs([]string{"get", "session-a"})

			setGetResponse(&clientFactory, testSessionA)
			require.NoError(t, cmd.Execute())
			require.Contains(t, output.O.String(), "session-a")
		})

		// Other commands should now return permission denied errors
		t.Run("create session permission denied", func(t *testing.T) {
			output := NewFakeOutErr()
			cmd := NewSessionCmd(&ctx)
			cmd.SetOut(output.O)
			cmd.SetArgs([]string{"create", "session-a"})
			err := cmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf(sessionNotAllowMsg, "create"))
		})

		t.Run("update session permission denied", func(t *testing.T) {
			output := NewFakeOutErr()
			cmd := NewSessionCmd(&ctx)
			cmd.SetOut(output.O)
			cmd.SetArgs([]string{"update", "session-a"})
			err := cmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf(sessionNotAllowMsg, "update"))
		})

		t.Run("list session permission denied", func(t *testing.T) {
			output := NewFakeOutErr()
			cmd := NewSessionCmd(&ctx)
			cmd.SetOut(output.O)
			cmd.SetArgs([]string{"list"})
			err := cmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf(sessionNotAllowMsg, "list"))
		})

		t.Run("delete session permission denied", func(t *testing.T) {
			output := NewFakeOutErr()
			cmd := NewSessionCmd(&ctx)
			cmd.SetOut(output.O)
			cmd.SetArgs([]string{"delete", "session-a"})
			err := cmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf(sessionNotAllowMsg, "delete"))
		})

		t.Run("debug-update session permission denied", func(t *testing.T) {
			output := NewFakeOutErr()
			cmd := NewSessionCmd(&ctx)
			cmd.SetOut(output.O)
			cmd.SetArgs([]string{"debug-update", "session-a"})
			err := cmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf(sessionNotAllowMsg, "debug-update"))
		})
	})

	// Add explicit management node permission tests
	t.Run("management node", func(t *testing.T) {
		clientFactory := NewFakeClientFactory()
		ctx := pkg.CmdCtx{
			ClientFactory: clientFactory,
			OnMgmtNode:    true, // Management node
		}

		// Only need to test one command - the other tests already exercise full admin functionality
		t.Run("create session allowed", func(t *testing.T) {
			output := NewFakeOutErr()
			cmd := NewSessionCmd(&ctx)
			cmd.SetOut(output.O)
			cmd.SetArgs([]string{"create", "session-a"})

			setCreateResponse(&clientFactory, testSessionA)
			require.NoError(t, cmd.Execute())
		})
	})
}

func TestGetSessions(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		OnMgmtNode:    true,
	}

	t.Run("get session", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"get", "session-a"})

		setGetResponse(&clientFactory, testSessionA)
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t,
			`
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS     WORKLOAD-TYPE
session-a  2        2           2          1         0         2         2         1              1                           active       k1=,k2=v2  

Warnings:
- Session session-a has 2 system(s) but 1 activation node(s), please consider moving 1 more activation node(s) into the session, otherwise performance may be degraded.
`, output.O.String())
	})

	t.Run("get session with affinity detail and suggestion", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"get", "session-a"})

		setGetResponse(&clientFactory, testSessionAWithAffinitySuggestions)
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t,
			`
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS  WORKLOAD-TYPE
session-a  2        2           2          1         1         2         2         1              1                           active               inference

AX Port Affinity:
stamp stamp-0: [port0:0,port1:5,port2:0,port3:0,port4:5,port5:0,port6:0,port7:1,port8:0,port9:0,port10:1,port11:0]
stamp stamp-1: [port0:1,port1:1,port2:1,port3:1,port4:1,port5:1,port6:1,port7:1,port8:1,port9:1,port10:1,port11:1]

Warnings:
- Session session-a has 2 system(s) but 1 activation node(s), please consider moving 1 more activation node(s) into the session, otherwise performance may be degraded.
- Session session-a: AX nodes allocated in stamp stamp-0 do not have affinity to port(s) [0 2 3 5 6 8 9 11]. Please consider adding AX node(s) to cover all ports.
- Session session-a: AX nodes allocated in stamp stamp-0 have aggregated port affinity difference of 5, which is greater than threshold 3. Please consider choosing AX nodes with balanced affinity.
`, output.O.String())
	})

	t.Run("list sessions with redundant only", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"list", "--redundant"})

		setListResponse(&clientFactory, &pb.ListSessionResponse{
			Items: []*pb.Session{
				testSessionC,
			},
		})
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t,
			`
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS  WORKLOAD-TYPE
session-c  1        1           1          1         0         2         2         1              1               *redundant  active               
`, output.O.String())
	})

	t.Run("list sessions with affinity suggestions", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"list"})

		setListResponse(&clientFactory, &pb.ListSessionResponse{
			Items: []*pb.Session{
				testSessionAWithAffinitySuggestions,
				testSessionCWithAffinitySuggestions,
			},
		})
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t,
			`
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS  WORKLOAD-TYPE
session-a  2        2           2          1         1         2         2         1              1                           active               inference
session-c  1        1           1          1         0         2         2         1              1               *redundant  active               

Warnings:
- Session session-a has 2 system(s) but 1 activation node(s), please consider moving 1 more activation node(s) into the session, otherwise performance may be degraded.
- Session session-a: AX nodes allocated in stamp stamp-0 do not have affinity to port(s) [0 2 3 5 6 8 9 11]. Please consider adding AX node(s) to cover all ports.
- Session session-a: AX nodes allocated in stamp stamp-0 have aggregated port affinity difference of 5, which is greater than threshold 3. Please consider choosing AX nodes with balanced affinity.
- Session session-c: AX nodes allocated in stamp stamp-1 do not have affinity to port(s) [0 2 3 5 6 8 9 11]. Please consider adding AX node(s) to cover all ports.
- Session session-c: AX nodes allocated in stamp stamp-1 have aggregated port affinity difference of 6, which is greater than threshold 3. Please consider choosing AX nodes with balanced affinity.
`, output.O.String())
	})

	t.Run("get session wide", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"get", "session-a", "-o", "wide"})

		setGetResponse(&clientFactory, testSessionA)
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t,
			`
NAME       SYSTEMS    NODEGROUPS   CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS     WORKLOAD-TYPE  JOBSRUNNING  JOBSQUEUING
session-a  (2) s0,s1  (2) ng0,ng1  (2) n0,n1  (1) a0    (0)       2         2         1              1                           active       k1=,k2=v2                 2            1

Warnings:
- Session session-a has 2 system(s) but 1 activation node(s), please consider moving 1 more activation node(s) into the session, otherwise performance may be degraded.
`, output.O.String())
	})

	t.Run("get sessions with yaml", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"get", "session-a", "-oyaml"})
		setGetResponse(&clientFactory, testSessionA)
		require.NoError(t, cmd.Execute())
		str := output.O.String()
		t.Log(str)
		requireStringsEqual(t, `
name: session-a
properties:
  labels.k8s.cerebras.com/k1: ""
  labels.k8s.cerebras.com/k2: v2
  system-tag: not-shown
state:
  currentResourceSpec:
    largeMemoryRackCount: 1
    parallelCompileCount: 2
    parallelExecuteCount: 2
    systemCount: 2
    xlargeMemoryRackCount: 1
  resources:
    activationNodes:
    - a0
    autoSelectActivationNodes: false
    autoSelectInferenceDriverNodes: false
    coordinatorNodes:
    - n0
    - n1
    inferenceDriverNodes: []
    managementNodes:
    - n0
    - n1
    nodegroups:
    - ng0
    - ng1
    systems:
    - s0
    - s1
  updateHistory:
  - message: ""
    reconcileTime: "0001-01-01T00:00:00Z"
    requestUid: request_uid
    spec:
      resourceSpec:
        largeMemoryRackCount: 1
        parallelCompileCount: 2
        parallelExecuteCount: 2
        systemCount: 2
        xlargeMemoryRackCount: 1
      suppressAffinityErrors: false
      updateMode: SET_RESOURCE_SPEC
    succeeded: true
status:
  inactiveDuration: active
  queueingJobCount: 1
  runningJobCount: 2
`, output.O.String())
	})
}

func TestCreateSession(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		OnMgmtNode:    true,
	}

	t.Run("create sessions success - v2 networking", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"create", "session-a",
			"--system-count", "8",
			"--parallel-compile-count", "7",
			"--parallel-execute-count", "6",
			"--large-memory-rack-count", "1",
			"--xlarge-memory-rack-count", "1",
		})
		setCreateResponse(&clientFactory, testSessionA)
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, `
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS     WORKLOAD-TYPE
session-a  2        2           2          1         0         2         2         1              1                           active       k1=,k2=v2  

Warnings:
- Session session-a has 2 system(s) but 1 activation node(s), please consider moving 1 more activation node(s) into the session, otherwise performance may be degraded.
`, output.O.String())

		var createReq *pb.CreateSessionRequest
		createReq = getLastRequest[*pb.CreateSessionRequest](clientFactory.csAdmClient)
		require.Equal(t,
			&pb.CreateSessionRequest{
				Name: "session-a",
				Spec: &pb.SessionSpec{
					UpdateMode:             pb.SessionUpdateMode_SET_RESOURCE_SPEC,
					SuppressAffinityErrors: false,
					UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: &pb.SessionResourceSpec{
						SystemCount:           8,
						ParallelCompileCount:  7,
						ParallelExecuteCount:  6,
						LargeMemoryRackCount:  1,
						XlargeMemoryRackCount: 1,
					}},
				}}, createReq)
	})

	t.Run("create sessions success - v1 networking", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"create", "session-a",
			"--system-count", "8",
			"--parallel-compile-count", "7",
			"--parallel-execute-count", "6",
			"--large-memory-rack-count", "1",
			"--xlarge-memory-rack-count", "1",
		})
		setCreateResponse(&clientFactory, testSessionB)
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, `
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS  WORKLOAD-TYPE
session-b  2        2           2          0         0         2         2         1              1                           active               
`, output.O.String())

		var createReq *pb.CreateSessionRequest
		createReq = getLastRequest[*pb.CreateSessionRequest](clientFactory.csAdmClient)
		require.Equal(t,
			&pb.CreateSessionRequest{
				Name: "session-a",
				Spec: &pb.SessionSpec{
					UpdateMode:             pb.SessionUpdateMode_SET_RESOURCE_SPEC,
					SuppressAffinityErrors: false,
					UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: &pb.SessionResourceSpec{
						SystemCount:           8,
						ParallelCompileCount:  7,
						ParallelExecuteCount:  6,
						LargeMemoryRackCount:  1,
						XlargeMemoryRackCount: 1,
					}},
				}}, createReq)
	})

	t.Run("create sessions failure", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{
			"create", "session-a",
			"--system-count", "2",
			"--large-memory-rack-count", "1",
			"--xlarge-memory-rack-count", "1"})
		setCreateErrorResponse(&clientFactory)
		err := cmd.Execute()
		require.Equal(t, "session create/update failed: not enough systems", err.Error())
	})

	t.Run("create redundant session", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{
			"create", "session-a", "--system-count", "1", "--redundant"})
		setCreateResponse(&clientFactory, testSessionC)
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, `
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS  WORKLOAD-TYPE
session-c  1        1           1          1         0         2         2         1              1               *redundant  active               
`, output.O.String())
	})

	t.Run("create sessions success - with workload-type & suppress affinity flag", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"create", "session-a",
			"--system-count", "8",
			"--parallel-compile-count", "7",
			"--parallel-execute-count", "6",
			"--large-memory-rack-count", "1",
			"--xlarge-memory-rack-count", "1",
			"--workload-type", "inference",
			"--suppress-affinity-errors",
		})
		setCreateResponse(&clientFactory, testSessionAWithAffinitySuggestions)
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, `
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS  WORKLOAD-TYPE
session-a  2        2           2          1         1         2         2         1              1                           active               inference

AX Port Affinity:
stamp stamp-0: [port0:0,port1:5,port2:0,port3:0,port4:5,port5:0,port6:0,port7:1,port8:0,port9:0,port10:1,port11:0]
stamp stamp-1: [port0:1,port1:1,port2:1,port3:1,port4:1,port5:1,port6:1,port7:1,port8:1,port9:1,port10:1,port11:1]

Warnings:
- Session session-a has 2 system(s) but 1 activation node(s), please consider moving 1 more activation node(s) into the session, otherwise performance may be degraded.
- Session session-a: AX nodes allocated in stamp stamp-0 do not have affinity to port(s) [0 2 3 5 6 8 9 11]. Please consider adding AX node(s) to cover all ports.
- Session session-a: AX nodes allocated in stamp stamp-0 have aggregated port affinity difference of 5, which is greater than threshold 3. Please consider choosing AX nodes with balanced affinity.
`, output.O.String())

		var createReq *pb.CreateSessionRequest
		createReq = getLastRequest[*pb.CreateSessionRequest](clientFactory.csAdmClient)
		require.Equal(t,
			&pb.CreateSessionRequest{
				Name: "session-a",
				Spec: &pb.SessionSpec{
					UpdateMode:             pb.SessionUpdateMode_SET_RESOURCE_SPEC,
					SuppressAffinityErrors: true,
					UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: &pb.SessionResourceSpec{
						SystemCount:           8,
						ParallelCompileCount:  7,
						ParallelExecuteCount:  6,
						LargeMemoryRackCount:  1,
						XlargeMemoryRackCount: 1,
					}},
				},
				Properties: map[string]string{
					namespacev1.SessionWorkloadTypeKey: namespacev1.InferenceWorkloadType.String(),
				},
			}, createReq)
	})

	t.Run("create sessions success - with labels", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"create", "session-a",
			"--system-count", "8",
			"--parallel-compile-count", "7",
			"--parallel-execute-count", "6",
			"--large-memory-rack-count", "1",
			"--xlarge-memory-rack-count", "1",
			"--labels", "k1=,k2-,k3=v3,k4",
		})
		setCreateResponse(&clientFactory, testSessionB)
		require.NoError(t, cmd.Execute())

		var createReq *pb.CreateSessionRequest
		createReq = getLastRequest[*pb.CreateSessionRequest](clientFactory.csAdmClient)

		operations := []*pb.UserLabelOperation{
			{Op: pb.UserLabelOperation_UPSERT, Key: "k1", Value: ""},
			{Op: pb.UserLabelOperation_UPSERT, Key: "k3", Value: "v3"},
			{Op: pb.UserLabelOperation_UPSERT, Key: "k4", Value: ""},
			{Op: pb.UserLabelOperation_REMOVE, Key: "k2"},
		}
		require.Equal(t, "session-a", createReq.Name)
		require.Equal(t, &pb.SessionSpec{
			UpdateMode:             pb.SessionUpdateMode_SET_RESOURCE_SPEC,
			SuppressAffinityErrors: false,
			UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: &pb.SessionResourceSpec{
				SystemCount:           8,
				ParallelCompileCount:  7,
				ParallelExecuteCount:  6,
				LargeMemoryRackCount:  1,
				XlargeMemoryRackCount: 1,
			}},
		}, createReq.Spec)
		require.ElementsMatch(t, operations, createReq.UserLabelOperations)
	})

	t.Run("create sessions failure - inference session without enough AX", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"create", "session-a",
			"--system-count", "2",
			"--parallel-compile-count", "7",
			"--parallel-execute-count", "6",
			"--large-memory-rack-count", "1",
			"--xlarge-memory-rack-count", "1",
			"--workload-type", "inference",
		})
		setCreateAffinityErrorResponse(&clientFactory)
		err := cmd.Execute()
		require.Equal(t, "session create/update failed: - Session session-a: AX nodes allocated in stamp stamp-0 do not have affinity to port(s) [0 2 3 5 6 8 9 11]. Please consider adding AX node(s) to cover all ports.", err.Error())
	})

	t.Run("create sessions failure - invalid workload-type", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"create", "session-a",
			"--system-count", "8",
			"--parallel-compile-count", "7",
			"--parallel-execute-count", "6",
			"--large-memory-rack-count", "1",
			"--xlarge-memory-rack-count", "1",
			"--workload-type", "randomstring",
		})
		setCreateErrorResponse(&clientFactory)
		err := cmd.Execute()
		require.Equal(t, "invalid workload type. Valid options are [inference training sdk]", err.Error())
	})
}

func TestSessionRedundancyUpdate(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		OnMgmtNode:    true,
	}

	t.Run("assign sessions", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"update", "session-a",
			"--redundancy-sessions", "session-c, session-b ",
		})
		setGetResponse(&clientFactory, testSessionAWithRedundancy)
		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, `
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY             LAST-ACTIVE  LABELS  WORKLOAD-TYPE
session-a  2        2           2          1         0         2         2         1              1               [session-b,session-c]  active               

Warnings:
- Inconsistent systems versions detected, please consider upgrade systems to keep consistent: ['rel-2.4': 's0', 'master': 's1']
- Session session-a has 2 system(s) but 1 activation node(s), please consider moving 1 more activation node(s) into the session, otherwise performance may be degraded.
`, output.O.String())
	})

	t.Run("disallow assign with other flags", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"update", "session-a",
			"--system-count", "1",
			"--redundancy-sessions", "session-c, session-b ",
		})
		require.Contains(t, cmd.Execute().Error(),
			"can't update 'redundancy-sessions' while modifying other resource of 'system-count'")
	})
}

func TestUpdateSession(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		OnMgmtNode:    true,
	}

	t.Run("update sessions scale-up success", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"update", "session-a",
			"--system-count", "4",
			"--parallel-compile-count", "6",
			"--parallel-execute-count", "4",
			"--large-memory-rack-count", "2",
			"--xlarge-memory-rack-count", "2",
		})
		updateResources := &pb.SessionResources{
			Systems:              []string{"s0", "s1", "s2", "s3"},
			Nodegroups:           []string{"ng0", "ng2", "ng3", "ng4", "ng5", "ng6"},
			CoordinatorNodes:     []string{"n1"},
			InferenceDriverNodes: []string{"i1"},
			ActivationNodes:      []string{"a0", "a1", "a2", "a3"},
		}
		updateRsSpec := &pb.SessionResourceSpec{
			ParallelCompileCount:  6,
			ParallelExecuteCount:  2,
			LargeMemoryRackCount:  2,
			XlargeMemoryRackCount: 2,
		}
		dryRunUpdate := proto.Clone(testSessionA).(*pb.Session)
		dryRunUpdate.State.UpdateHistory = []*pb.RequestStatus{
			{Spec: &pb.SessionSpec{UpdateParams: &pb.SessionSpec_Resources{Resources: updateResources}}},
			{Spec: &pb.SessionSpec{UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: updateRsSpec}}},
		}
		clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, dryRunUpdate)
		effectiveUpdate := proto.Clone(testSessionA).(*pb.Session)
		effectiveUpdate.State.Resources = updateResources
		effectiveUpdate.State.CurrentResourceSpec = updateRsSpec
		clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, effectiveUpdate)

		stdin := bytes.NewBufferString("y\n")
		cmd.SetIn(stdin)

		require.NoError(t, cmd.Execute())
		requireStringsEqual(t, `
Update will result in the estimated resource capabilities changes:
  parallel compile limit increases from 2 to 6
  large memory rack count increases from 1 to 2
  xlarge memory rack count increases from 1 to 2
Update will result in the following 13 resource changes:
  2 systems added: s2,s3
  5 nodegroups added: ng2,ng3,ng4,ng5,ng6
  1 nodegroups removed: ng1
  1 coordinator nodes removed: n0
  3 activation nodes added: a1,a2,a3
  1 inference driver nodes added: i1
NAME       SYSTEMS  NODEGROUPS  CRD-NODES  AX-NODES  IX-NODES  COMPILES  EXECUTES  LRG-MEM-RACKS  XLRG-MEM-RACKS  REDUNDANCY  LAST-ACTIVE  LABELS     WORKLOAD-TYPE
session-a  4        6           1          4         1         6         2         2              2                           active       k1=,k2=v2  `, output.O.String())

		req := getLastRequest[*pb.UpdateSessionRequest](clientFactory.csAdmClient)
		require.Equal(t, false, req.Spec.SuppressAffinityErrors)
	})

	t.Run("update sessions scale-down success", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"update", "session-a", "--system-count", "0"})
		updateResources := &pb.SessionResources{
			Systems:              []string{},
			Nodegroups:           []string{},
			CoordinatorNodes:     []string{},
			ActivationNodes:      []string{},
			InferenceDriverNodes: []string{},
		}
		updateRsSpec := &pb.SessionResourceSpec{}
		dryRunUpdate := proto.Clone(testSessionA).(*pb.Session)
		dryRunUpdate.State.UpdateHistory = []*pb.RequestStatus{
			{Spec: &pb.SessionSpec{UpdateMode: pb.SessionUpdateMode_DEBUG_SET, UpdateParams: &pb.SessionSpec_Resources{Resources: updateResources}}},
			{Spec: &pb.SessionSpec{UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: updateRsSpec}}},
		}
		clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, dryRunUpdate)
		effectiveUpdate := proto.Clone(testSessionA).(*pb.Session)
		effectiveUpdate.State.Resources = updateResources
		effectiveUpdate.State.CurrentResourceSpec = updateRsSpec
		clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, effectiveUpdate)

		stdin := bytes.NewBufferString("y\n")
		cmd.SetIn(stdin)

		require.NoError(t, cmd.Execute())
		requireStringsContain(t, output.O.String(), `
Update will result in the estimated resource capabilities changes:
  parallel compile limit decreases from 2 to 0
  parallel execute limit decreases from 2 to 0
  large memory rack count decreases from 1 to 0
  xlarge memory rack count decreases from 1 to 0
Update will result in the following 7 resource changes:
  2 systems removed: s0,s1
  2 nodegroups removed: ng0,ng1
  2 coordinator nodes removed: n0,n1
  1 activation nodes removed: a0`)

		req := getLastRequest[*pb.UpdateSessionRequest](clientFactory.csAdmClient)
		require.Equal(t, "session-a", req.Name)
		// explicitly assign empty string to remove existing assignment if necessary
		require.Equal(t, []string{""}, req.Spec.GetResources().Systems)
		require.Equal(t, []string{""}, req.Spec.GetResources().Nodegroups)
		require.Equal(t, []string{""}, req.Spec.GetResources().CoordinatorNodes)
		require.Equal(t, []string{""}, req.Spec.GetResources().ActivationNodes)
		require.Equal(t, []string{""}, req.Spec.GetResources().InferenceDriverNodes)
		require.Equal(t, false, req.Spec.SuppressAffinityErrors)
	})

	t.Run("update session with suppress affinity flag", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"update", "session-a", "--system-count", "0", "--suppress-affinity-errors"})

		updateResources := &pb.SessionResources{
			Systems:          []string{},
			Nodegroups:       []string{},
			CoordinatorNodes: []string{},
			ActivationNodes:  []string{},
		}

		updateRsSpec := &pb.SessionResourceSpec{}
		dryRunUpdate := proto.Clone(testSessionA).(*pb.Session)
		dryRunUpdate.State.UpdateHistory = []*pb.RequestStatus{
			{Spec: &pb.SessionSpec{UpdateMode: pb.SessionUpdateMode_DEBUG_SET, UpdateParams: &pb.SessionSpec_Resources{Resources: updateResources}}},
			{Spec: &pb.SessionSpec{UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: updateRsSpec}}},
		}
		clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, dryRunUpdate)
		effectiveUpdate := proto.Clone(testSessionA).(*pb.Session)
		effectiveUpdate.State.Resources = updateResources
		effectiveUpdate.State.CurrentResourceSpec = updateRsSpec
		clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, effectiveUpdate)

		stdin := bytes.NewBufferString("y\n")
		cmd.SetIn(stdin)

		require.NoError(t, cmd.Execute())
		req := getLastRequest[*pb.UpdateSessionRequest](clientFactory.csAdmClient)
		require.Equal(t, "session-a", req.Name)
		require.Equal(t, true, req.Spec.SuppressAffinityErrors)
	})

	t.Run("update session with labels with no confirmation", func(t *testing.T) {
		output := NewFakeOutErr()
		cmd := NewSessionCmd(&ctx)
		cmd.SetOut(output.O)
		cmd.SetArgs([]string{"update", "session-a", "--labels", "k1=,k2-,k3=v3,k4"})

		setGetResponse(&clientFactory, testSessionA)
		require.NoError(t, cmd.Execute())
		req := getLastRequest[*pb.UpdateSessionRequest](clientFactory.csAdmClient)
		require.Equal(t, "session-a", req.Name)

		operations := []*pb.UserLabelOperation{
			{Op: pb.UserLabelOperation_UPSERT, Key: "k1", Value: ""},
			{Op: pb.UserLabelOperation_UPSERT, Key: "k3", Value: "v3"},
			{Op: pb.UserLabelOperation_UPSERT, Key: "k4", Value: ""},
			{Op: pb.UserLabelOperation_REMOVE, Key: "k2"},
		}
		require.ElementsMatch(t, operations, req.UserLabelOperations)
	})
}

func TestDebugUpdateSession(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		OnMgmtNode:    true,
	}

	for _, testcase := range []struct {
		name                   string
		expectMode             pb.SessionUpdateMode
		expectErrContains      string
		suppressErrors         bool
		activationNodeNameStr  string
		autoSetActivationNodes string // Empty string represents null, otherwise use true or false
	}{
		{
			name:       "append",
			expectMode: pb.SessionUpdateMode_DEBUG_APPEND,
		},
		{
			name:           "append", // with suppress
			expectMode:     pb.SessionUpdateMode_DEBUG_APPEND,
			suppressErrors: true,
		},
		{
			name:       "remove",
			expectMode: pb.SessionUpdateMode_DEBUG_REMOVE,
		},
		{
			name:           "remove", // with suppress
			expectMode:     pb.SessionUpdateMode_DEBUG_REMOVE,
			suppressErrors: true,
		},
		{
			name:              "remove-all",
			expectMode:        pb.SessionUpdateMode_DEBUG_REMOVE_ALL,
			expectErrContains: "remove-all mode will remove all assigned resources, please don't set the resource names",
		},
		{
			name:       "set",
			expectMode: pb.SessionUpdateMode_DEBUG_SET,
		},
		{
			name:           "set", // with suppress
			expectMode:     pb.SessionUpdateMode_DEBUG_SET,
			suppressErrors: true,
		},
		{
			name:              "unknown",
			expectErrContains: "unrecognized mode 'unknown'",
		},
		{
			name:                   "append",
			expectMode:             pb.SessionUpdateMode_DEBUG_APPEND,
			autoSetActivationNodes: "true",
		},
		{
			name:                   "append",
			expectMode:             pb.SessionUpdateMode_DEBUG_APPEND,
			autoSetActivationNodes: "true",
			activationNodeNameStr:  "a0,a1",
			expectErrContains:      "note --auto-set-activation-nodes is enabled by default",
		},
		{
			name:                  "append",
			expectMode:            pb.SessionUpdateMode_DEBUG_APPEND,
			activationNodeNameStr: "a0,a1",
			expectErrContains:     "note --auto-set-activation-nodes is enabled by default",
		},
		{
			name:                   "append",
			expectMode:             pb.SessionUpdateMode_DEBUG_APPEND,
			autoSetActivationNodes: "false",
			activationNodeNameStr:  "a0,a1",
		},
	} {
		t.Run("debug mode update "+testcase.name, func(t *testing.T) {
			output := NewFakeOutErr()
			cmd := NewSessionCmd(&ctx)
			cmd.SetOut(output.O)
			args := []string{"session-a",
				"--system-names", "s0,s2",
				"--nodegroup-names", "ng0",
				"--coordinator-node-names", "n1",
			}
			if testcase.name != "" {
				args = append(args, []string{"-m", testcase.name}...)
			}
			args = append([]string{"debug-update"}, args...)
			if testcase.suppressErrors {
				args = append(args, "--suppress-affinity-errors")
			}
			var expectedActivationNodes []string = nil
			if testcase.activationNodeNameStr != "" {
				args = append(args, "--activation-node-names", testcase.activationNodeNameStr)
				expectedActivationNodes = strings.Split(testcase.activationNodeNameStr, ",")
			}

			// Default should be true
			expectedAutoSetActivationNodes := true
			if testcase.autoSetActivationNodes != "" {
				args = append(args, fmt.Sprintf("--auto-set-activation-nodes=%s", testcase.autoSetActivationNodes))
				expectedAutoSetActivationNodes, _ = strconv.ParseBool(testcase.autoSetActivationNodes)
			}

			cmd.SetArgs(args)
			setCreateResponse(&clientFactory, testSessionA)
			err := cmd.Execute()
			if testcase.expectErrContains != "" {
				require.ErrorContains(t, err, testcase.expectErrContains)
			} else {
				require.NoError(t, err)
				req := getLastRequest[*pb.UpdateSessionRequest](clientFactory.csAdmClient)
				require.Equal(t, testcase.expectMode, req.Spec.UpdateMode)
				require.Equal(t, "session-a", req.Name)
				require.Equal(t, []string{"s0", "s2"}, req.Spec.GetResources().Systems)
				require.Equal(t, []string{"ng0"}, req.Spec.GetResources().Nodegroups)
				require.Equal(t, []string{"n1"}, req.Spec.GetResources().CoordinatorNodes)
				require.Equal(t, expectedActivationNodes, req.Spec.GetResources().ActivationNodes)
				require.Equal(t, testcase.suppressErrors, req.Spec.SuppressAffinityErrors)
				require.Equal(t, expectedAutoSetActivationNodes, req.Spec.GetResources().AutoSelectActivationNodes)
			}
		})
	}
}

func setListResponse(clientFactory *FakeClientFactory, testSession *pb.ListSessionResponse) {
	clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, testSession)
}

func setGetResponse(clientFactory *FakeClientFactory, testSession *pb.Session) {
	clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, testSession)
}

func setCreateResponse(clientFactory *FakeClientFactory, testSession *pb.Session) {
	clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr, testSession)
}

func setCreateErrorResponse(clientFactory *FakeClientFactory) {
	clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr,
		fmt.Errorf("session create/update failed: not enough systems"))
}

func setCreateAffinityErrorResponse(clientFactory *FakeClientFactory) {
	clientFactory.csAdmClient.responseOrErr = append(clientFactory.csAdmClient.responseOrErr,
		fmt.Errorf("session create/update failed: - Session session-a: AX nodes allocated in stamp stamp-0 do "+
			"not have affinity to port(s) [0 2 3 5 6 8 9 11]. Please consider adding AX node(s) to cover all ports."))
}

func requireStringsEqual(t *testing.T, a, b string) {
	require.Equal(t, strings.Trim(a, "\n"), strings.Trim(b, "\n"))
}

func requireStringsContain(t *testing.T, a, b string) {
	require.Contains(t, strings.Trim(a, "\n"), strings.Trim(b, "\n"))
}

var resourceSpec = &pb.SessionResourceSpec{
	SystemCount:           2,
	LargeMemoryRackCount:  1,
	XlargeMemoryRackCount: 1,
	ParallelCompileCount:  2,
	ParallelExecuteCount:  2,
}

var updateHistory = []*pb.RequestStatus{{
	Succeeded:     true,
	Message:       "",
	RequestUid:    "request_uid",
	ReconcileTime: timestamppb.New(time.Time{}),
	Spec: &pb.SessionSpec{
		UpdateMode:   pb.SessionUpdateMode_SET_RESOURCE_SPEC,
		UpdateParams: &pb.SessionSpec_ResourceSpec{ResourceSpec: resourceSpec},
	},
}}

var runtimeStatus = &pb.RuntimeStatus{
	RunningJobCount:  2,
	QueueingJobCount: 1,
	InactiveDuration: "active",
}

var testSessionA = &pb.Session{
	Name: "session-a",
	State: &pb.SessionState{
		Resources: &pb.SessionResources{
			Nodegroups:       []string{"ng0", "ng1"},
			Systems:          []string{"s0", "s1"},
			ActivationNodes:  []string{"a0"},
			CoordinatorNodes: []string{"n0", "n1"},
			ManagementNodes:  []string{"n0", "n1"},
		},
		CurrentResourceSpec: resourceSpec,
		UpdateHistory:       updateHistory,
	},
	Status:     runtimeStatus,
	Properties: map[string]string{K8sLabelPrefix + "k1": "", K8sLabelPrefix + "k2": "v2", "system-tag": "not-shown"},
}

var testSessionAWithRedundancy = &pb.Session{
	Name: "session-a",
	State: &pb.SessionState{
		Resources: &pb.SessionResources{
			Nodegroups:       []string{"ng0", "ng1"},
			Systems:          []string{"s0", "s1"},
			ActivationNodes:  []string{"a0"},
			CoordinatorNodes: []string{"n0", "n1"},
			ManagementNodes:  []string{"n0", "n1"},
		},
		CurrentResourceSpec: resourceSpec,
		UpdateHistory:       updateHistory,
	},
	Status: runtimeStatus,
	Properties: map[string]string{
		namespacev1.AssignedRedundancyKey: "session-b,session-c",
		namespacev1.SessionWarningAnnoKeyPrefix: "- Inconsistent systems versions detected, " +
			"please consider upgrade systems to keep consistent: ['rel-2.4': 's0', 'master': 's1']",
	},
}

var testSessionAWithAffinitySuggestions = &pb.Session{
	Name: "session-a",
	State: &pb.SessionState{
		Resources: &pb.SessionResources{
			Nodegroups:           []string{"ng0", "ng1"},
			Systems:              []string{"s0", "s1"},
			ActivationNodes:      []string{"a0"},
			InferenceDriverNodes: []string{"i0"},
			CoordinatorNodes:     []string{"n0", "n1"},
			ManagementNodes:      []string{"n0", "n1"},
		},
		CurrentResourceSpec: resourceSpec,
		UpdateHistory:       updateHistory,
	},
	Status: runtimeStatus,
	Properties: map[string]string{
		namespacev1.SessionWorkloadTypeKey:                  namespacev1.InferenceWorkloadType.String(),
		namespacev1.SessionWarningAnnoKeyPrefix + "-0":      "- Session session-a: AX nodes allocated in stamp stamp-0 do not have affinity to port(s) [0 2 3 5 6 8 9 11]. Please consider adding AX node(s) to cover all ports.",
		namespacev1.SessionWarningAnnoKeyPrefix + "-1":      "- Session session-a: AX nodes allocated in stamp stamp-0 have aggregated port affinity difference of 5, which is greater than threshold 3. Please consider choosing AX nodes with balanced affinity.",
		namespacev1.SessionPortAffinityAnnoKeyPrefix + "-0": "stamp stamp-0: [port0:0,port1:5,port2:0,port3:0,port4:5,port5:0,port6:0,port7:1,port8:0,port9:0,port10:1,port11:0]",
		namespacev1.SessionPortAffinityAnnoKeyPrefix + "-1": "stamp stamp-1: [port0:1,port1:1,port2:1,port3:1,port4:1,port5:1,port6:1,port7:1,port8:1,port9:1,port10:1,port11:1]",
	},
}

var testSessionB = &pb.Session{
	Name: "session-b",
	State: &pb.SessionState{
		Resources: &pb.SessionResources{
			Nodegroups:       []string{"ng0", "ng1"},
			Systems:          []string{"s0", "s1"},
			CoordinatorNodes: []string{"n0", "n1"},
			ManagementNodes:  []string{"n0", "n1"},
		},
		CurrentResourceSpec: resourceSpec,
		UpdateHistory:       updateHistory,
	},
	Status: runtimeStatus,
}

var testSessionC = &pb.Session{
	Name: "session-c",
	State: &pb.SessionState{
		Resources: &pb.SessionResources{
			Nodegroups:       []string{"ng0"},
			Systems:          []string{"s0"},
			ActivationNodes:  []string{"a0"},
			CoordinatorNodes: []string{"n0"},
			ManagementNodes:  []string{"n0"},
		},
		CurrentResourceSpec: resourceSpec,
		UpdateHistory:       updateHistory,
	},
	Status: runtimeStatus,
	Properties: map[string]string{
		namespacev1.IsRedundantModeKey: "true",
	},
}

var testSessionCWithAffinitySuggestions = &pb.Session{
	Name: "session-c",
	State: &pb.SessionState{
		Resources: &pb.SessionResources{
			Nodegroups:       []string{"ng0"},
			Systems:          []string{"s0"},
			ActivationNodes:  []string{"a0"},
			CoordinatorNodes: []string{"n0"},
			ManagementNodes:  []string{"n0"},
		},
		CurrentResourceSpec: resourceSpec,
		UpdateHistory:       updateHistory,
	},
	Status: runtimeStatus,
	Properties: map[string]string{
		namespacev1.IsRedundantModeKey:                      "true",
		namespacev1.SessionWarningAnnoKeyPrefix + "-0":      "- Session session-c: AX nodes allocated in stamp stamp-1 do not have affinity to port(s) [0 2 3 5 6 8 9 11]. Please consider adding AX node(s) to cover all ports.",
		namespacev1.SessionWarningAnnoKeyPrefix + "-1":      "- Session session-c: AX nodes allocated in stamp stamp-1 have aggregated port affinity difference of 6, which is greater than threshold 3. Please consider choosing AX nodes with balanced affinity.",
		namespacev1.SessionPortAffinityAnnoKeyPrefix + "-0": "stamp stamp-0: [port0:1,port1:1,port2:1,port3:1,port4:1,port5:1,port6:1,port7:1,port8:1,port9:1,port10:1,port11:1]",
		namespacev1.SessionPortAffinityAnnoKeyPrefix + "-1": "stamp stamp-1: [port0:0,port1:1,port2:0,port3:0,port4:1,port5:0,port6:0,port7:6,port8:0,port9:0,port10:6,port11:0]",
	},
}

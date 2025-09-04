package cmd

import (
	"context"
	"testing"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csctlv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"cerebras.com/cluster/csctl/pkg"
)

func TestGetJob(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	clientFactory.SetListStreaming(false) // should be able to fall back to return GetResponse
	cmdCtx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		DebugLevel:    1,
	}

	t.Run("list job as table", func(t *testing.T) {
		output := NewFakeOutErr()
		c := jobCmdCtx{
			cmdCtx: &cmdCtx,
			client: clientFactory.csCtlClient,
			Output: "table",
			outErr: output,
		}
		tableOut := &csctlv1.Table{
			Columns: []*csctlv1.ColumnDefinition{{Name: "name"}, {Name: "age"}},
			Rows: []*csctlv1.RowData{
				{Cells: []string{"one", "1d"}},
				{Cells: []string{"two", "1m30s"}},
			},
		}
		raw, err := proto.Marshal(tableOut)
		require.NoError(t, err)
		res := &pb.GetResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: raw}
		clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr, res)

		require.NoError(t, c.Get(context.Background(), ""))
		assert.Equal(t, "NAME  AGE\none   1d\ntwo   1m30s\n", output.O.String())

	})

	t.Run("get job as json", func(t *testing.T) {
		for _, testcase := range []struct {
			Name         string
			ServerJson   string
			ExpectedJson string
			DebugLevel   int
		}{
			{
				Name:         "arbitrary json",
				ServerJson:   `{"items":[{},{}]}`,
				ExpectedJson: `{"items":[{},{}]}`,
			},
			{
				Name: "with field mask on list",
				ServerJson: `{"items":[
						{"meta":{"name":"invisible","fieldPriority":{"spec.mask":2}},"spec":{"mask":"invisible"}},
						{"meta":{"name":"visible"},"spec":{"mask":"visible"}}
					]}`,
				ExpectedJson: `{"items":[
						{"meta":{"name":"invisible","fieldPriority":{"spec.mask":2}},"spec":{}},
						{"meta":{"name":"visible"},"spec":{"mask":"visible"}}
					]}`,
				DebugLevel: 1,
			},
			{
				Name:         "with bad field mask",
				ServerJson:   `{"meta":{"name":"visible","fieldPriority":{"spec.nonexist":2}},"spec":{"mask":"invisible"}}`,
				ExpectedJson: `{"meta":{"name":"visible","fieldPriority":{"spec.nonexist":2}},"spec":{"mask":"invisible"}}`,
				DebugLevel:   1,
			},
		} {
			t.Run(testcase.Name, func(t *testing.T) {
				output := NewFakeOutErr()
				c := jobCmdCtx{
					cmdCtx: &cmdCtx,
					client: clientFactory.csCtlClient,
					Output: "json",
					outErr: output,
				}

				res := &pb.GetResponse{ContentType: pb.SerializationMethod_JSON_METHOD, Raw: []byte(testcase.ServerJson)}
				clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr, res)

				require.NoError(t, c.Get(context.TODO(), ""))
				if !jsonpatch.Equal([]byte(testcase.ExpectedJson), output.O.Bytes()) {
					t.Logf("expected: %s\nactual:%s", testcase.ExpectedJson, output.O.String())
					t.Fail()
				}
			})
		}
	})
}

func TestListStreamingJobs(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	clientFactory.SetListStreaming(true)
	cmdCtx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		DebugLevel:    1,
	}

	t.Run("list job as table", func(t *testing.T) {
		output := NewFakeOutErr()
		c := jobCmdCtx{
			cmdCtx: &cmdCtx,
			client: clientFactory.csCtlClient,
			Output: "table",
			outErr: output,
		}
		tableOut1 := &csctlv1.Table{
			Columns: []*csctlv1.ColumnDefinition{{Name: "name"}, {Name: "age"}},
			Rows: []*csctlv1.RowData{
				{Cells: []string{"one", "1d"}},
				{Cells: []string{"two", "1m30s"}},
			},
		}
		raw1, err := proto.Marshal(tableOut1)
		require.NoError(t, err)
		res1 := &pb.ListJobResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: raw1}
		tableOut2 := &csctlv1.Table{
			Columns: []*csctlv1.ColumnDefinition{{Name: "name"}, {Name: "age"}},
			Rows: []*csctlv1.RowData{
				{Cells: []string{"three", "2d"}},
				{Cells: []string{"four", "2m30s"}},
			},
		}
		raw2, err := proto.Marshal(tableOut2)
		require.NoError(t, err)
		res2 := &pb.ListJobResponse{ContentType: pb.SerializationMethod_PROTOBUF_METHOD, Raw: raw2}
		// streaming API of FakeCsCtlV1 returns all response in responseOrErr, therefore need to clear previous resp
		clientFactory.csCtlClient.responseOrErr = []interface{}{res1, res2}

		require.NoError(t, c.Get(context.Background(), ""))
		assert.Equal(t, "NAME   AGE\none    1d\ntwo    1m30s\nthree  2d\nfour   2m30s\n", output.O.String())
	})

	t.Run("get job as json", func(t *testing.T) {
		for _, testcase := range []struct {
			Name         string
			ServerJsons  []string
			ExpectedJson string
			DebugLevel   int
		}{
			{
				Name:         "arbitrary json",
				ServerJsons:  []string{`{"items":[{},{}]}`, `{"items":[{},{},{}]}`},
				ExpectedJson: `{"items":[{},{},{},{},{}]}`,
			},
			{
				Name: "with field mask on list",
				ServerJsons: []string{
					`{"items":[
						{"meta":{"name":"invisible","fieldPriority":{"spec.mask":2}},"spec":{"mask":"invisible"}},
						{"meta":{"name":"visible"},"spec":{"mask":"visible"}}
					]}`,
					`{"items":[
						{"meta":{"name":"invisible","fieldPriority":{"spec.mask":1, "spec.mask2":2}},"spec":{"mask":"visible", "mask2":"invisible"}},
						{"meta":{"name":"visible"},"spec":{"mask":"visible2"}}
					]}`,
				},
				ExpectedJson: `{"items":[
						{"meta":{"name":"invisible","fieldPriority":{"spec.mask":2}},"spec":{}},
						{"meta":{"name":"visible"},"spec":{"mask":"visible"}},
						{"meta":{"name":"invisible","fieldPriority":{"spec.mask":1, "spec.mask2":2}},"spec":{"mask":"visible"}},
						{"meta":{"name":"visible"},"spec":{"mask":"visible2"}}
					]}`,
				DebugLevel: 1,
			},
			{
				Name:         "with bad field mask",
				ServerJsons:  []string{`{"meta":{"name":"visible","fieldPriority":{"spec.nonexist":2}},"spec":{"mask":"invisible"}}`},
				ExpectedJson: `{"meta":{"name":"visible","fieldPriority":{"spec.nonexist":2}},"spec":{"mask":"invisible"}}`,
				DebugLevel:   1,
			},
			{
				Name: "list with bad field mask",
				ServerJsons: []string{
					`{"items":[
						{"meta":{"name":"visible","fieldPriority":{"spec.nonexist":2}},"spec":{"mask":"invisible"}}
					]}`,
					`{"items":[
						{"meta":{"name":"visible","fieldPriority":{"spec.nonexist":2}},"spec":{"mask":"invisible"}}
					]}`,
				},
				ExpectedJson: `{"items":[
						{"meta":{"name":"visible","fieldPriority":{"spec.nonexist":2}},"spec":{"mask":"invisible"}},
						{"meta":{"name":"visible","fieldPriority":{"spec.nonexist":2}},"spec":{"mask":"invisible"}}
					]}`,
				DebugLevel: 1,
			},
		} {
			t.Run(testcase.Name, func(t *testing.T) {
				output := NewFakeOutErr()
				c := jobCmdCtx{
					cmdCtx: &cmdCtx,
					client: clientFactory.csCtlClient,
					Output: "json",
					outErr: output,
				}

				// streaming API of FakeCsCtlV1 returns all response in responseOrErr, therefore need to clear previous resp
				clientFactory.csCtlClient.responseOrErr = []interface{}{}
				for _, serverJson := range testcase.ServerJsons {
					res := &pb.ListJobResponse{ContentType: pb.SerializationMethod_JSON_METHOD, Raw: []byte(serverJson)}
					clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr, res)
				}

				require.NoError(t, c.Get(context.TODO(), ""))
				if !jsonpatch.Equal([]byte(testcase.ExpectedJson), output.O.Bytes()) {
					t.Logf("expected: %s\nactual:%s", testcase.ExpectedJson, output.O.String())
					t.Fail()
				}
			})
		}
	})
}

func TestJobPriorityUpdate(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	cmdCtx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		DebugLevel:    1,
	}
	outErr := NewFakeOutErr()
	c := jobCmdCtx{
		cmdCtx: &cmdCtx,
		client: clientFactory.csCtlClient,
		outErr: outErr,
	}

	clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr,
		&pb.UpdateJobPriorityResponse{
			Message: "Succeeded",
		},
	)
	err := c.SetPriority(context.Background(), "foo", "5")
	require.NoError(t, err)
}

func TestGetJobTopology(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	cmdCtx := pkg.CmdCtx{
		ClientFactory: clientFactory,
		DebugLevel:    1,
		Namespace:     "test-namespace",
	}

	t.Run("get job topology as table", func(t *testing.T) {
		output := NewFakeOutErr()
		c := jobCmdCtx{
			cmdCtx: &cmdCtx,
			client: clientFactory.csCtlClient,
			Output: "table",
			outErr: output,
		}

		// Mock response with topology connections
		res := &pb.GetJobTopologyResponse{
			Connections: []*pb.JobTopologyConnection{
				{
					PodId:                "server-001",
					NodeName:             "node-001",
					Nics:                 "eth0,eth1",
					NodeLeafSwitch:       "switch-001",
					SystemPorts:          "target-port1",
					PodPreferredCsPorts:  "1,2",
					EgressTargets:        "system-0",
					SystemAffinitySwitch: "switch-1",
				},
				{
					PodId:                "server-002",
					NodeName:             "node-002",
					Nics:                 "eth0",
					NodeLeafSwitch:       "switch-002",
					SystemPorts:          "target-port2",
					PodPreferredCsPorts:  "4",
					EgressTargets:        "system-1",
					SystemAffinitySwitch: "switch-2",
				},
			},
		}
		clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr, res)

		require.NoError(t, c.GetJobTopology(context.Background(), "test-job"))

		outputStr := output.O.String()
		assert.Contains(t, outputStr, "server-001")
		assert.Contains(t, outputStr, "node-001")
		assert.Contains(t, outputStr, "eth0,eth1")
		assert.Contains(t, outputStr, "switch-001")
		assert.Contains(t, outputStr, "1,2")
		assert.Contains(t, outputStr, "system-0")
		assert.Contains(t, outputStr, "target-port1")
		assert.Contains(t, outputStr, "switch-1")
		assert.Contains(t, outputStr, "server-002")
		assert.Contains(t, outputStr, "node-002")
		assert.Contains(t, outputStr, "eth0")
		assert.Contains(t, outputStr, "switch-002")
		assert.Contains(t, outputStr, "4")
		assert.Contains(t, outputStr, "system-1")
		assert.Contains(t, outputStr, "target-port2")
		assert.Contains(t, outputStr, "switch-2")
	})

	t.Run("get job topology with empty connections", func(t *testing.T) {
		output := NewFakeOutErr()
		c := jobCmdCtx{
			cmdCtx: &cmdCtx,
			client: clientFactory.csCtlClient,
			Output: "table",
			outErr: output,
		}

		// Mock response with no connections
		res := &pb.GetJobTopologyResponse{
			Connections: []*pb.JobTopologyConnection{},
		}
		clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr, res)

		require.NoError(t, c.GetJobTopology(context.Background(), "test-job"))
		assert.Contains(t, output.O.String(), "No topology connections found for this job")
	})

	t.Run("get job topology with empty fields", func(t *testing.T) {
		output := NewFakeOutErr()
		c := jobCmdCtx{
			cmdCtx: &cmdCtx,
			client: clientFactory.csCtlClient,
			Output: "table",
			outErr: output,
		}

		// Mock response with connections having empty fields
		res := &pb.GetJobTopologyResponse{
			Connections: []*pb.JobTopologyConnection{
				{
					StampId:             "",
					PodId:               "server-001",
					NodeName:            "",
					Nics:                "eth0",
					NodeLeafSwitch:      "",
					SystemPorts:         "1,2,3",
					PodPreferredCsPorts: "",
				},
			},
		}
		clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr, res)

		require.NoError(t, c.GetJobTopology(context.Background(), "test-job"))

		// Verify empty fields are replaced with "-"
		outputStr := output.O.String()
		assert.Contains(t, outputStr, "server-001")
		assert.Contains(t, outputStr, "eth0")
		assert.Contains(t, outputStr, "1,2,3")
		assert.Contains(t, outputStr, "")
	})
}

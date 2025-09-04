package cmd

import (
	"context"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csctlv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/cluster/csctl/pkg"
)

func TestGet(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	clientFactory.SetListStreaming(false) // should be able to fall back to return GetResponse
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
	}

	t.Run("get job list as table", func(t *testing.T) {
		output := NewFakeOutErr()
		getCmdOpts := GetCmdOptions{
			Type:   "jobs",
			Output: "table",
			cmdCtx: &ctx,
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

		require.NoError(t, getCmdOpts.Run(context.TODO()))
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
				getCmdOpts := GetCmdOptions{
					Type:       "jobs",
					Output:     "json",
					cmdCtx:     &ctx,
					outErr:     output,
					DebugLevel: testcase.DebugLevel,
				}

				res := &pb.GetResponse{ContentType: pb.SerializationMethod_JSON_METHOD, Raw: []byte(testcase.ServerJson)}
				clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr, res)

				require.NoError(t, getCmdOpts.Run(context.TODO()))
				if !jsonpatch.Equal([]byte(testcase.ExpectedJson), output.O.Bytes()) {
					t.Logf("expected: %s\nactual:%s", testcase.ExpectedJson, output.O.String())
					t.Fail()
				}
			})
		}
	})
}

func TestListStreaming(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	clientFactory.SetListStreaming(true)
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
	}

	t.Run("get job list as table", func(t *testing.T) {
		output := NewFakeOutErr()
		getCmdOpts := GetCmdOptions{
			Type:   "jobs",
			Output: "table",
			cmdCtx: &ctx,
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

		require.NoError(t, getCmdOpts.Run(context.TODO()))
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
				getCmdOpts := GetCmdOptions{
					Type:       "jobs",
					Output:     "json",
					cmdCtx:     &ctx,
					outErr:     output,
					DebugLevel: testcase.DebugLevel,
				}

				// streaming API of FakeCsCtlV1 returns all response in responseOrErr, therefore need to clear previous resp
				clientFactory.csCtlClient.responseOrErr = []interface{}{}
				for _, serverJson := range testcase.ServerJsons {
					res := &pb.ListJobResponse{ContentType: pb.SerializationMethod_JSON_METHOD, Raw: []byte(serverJson)}
					clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr, res)
				}

				require.NoError(t, getCmdOpts.Run(context.TODO()))
				if !jsonpatch.Equal([]byte(testcase.ExpectedJson), output.O.Bytes()) {
					t.Logf("expected: %s\nactual:%s", testcase.ExpectedJson, output.O.String())
					t.Fail()
				}
			})
		}
	})
}

func TestLabels(t *testing.T) {
	labels, err := extractLabels("key1=value1")
	require.NoError(t, err)
	assert.Equal(t, labels, map[string]string{"key1": "value1"})

	labels, err = extractLabels("key1=value1,key2=value2")
	require.NoError(t, err)
	assert.Equal(t, labels, map[string]string{"key1": "value1", "key2": "value2"})

	labels, err = extractLabels("key1")
	require.Error(t, err)

	labels, err = extractLabels("key1=value1,key2")
	require.Error(t, err)
}

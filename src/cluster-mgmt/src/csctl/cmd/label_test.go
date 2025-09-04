package cmd

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/cluster/csctl/pkg"
)

func TestLabel(t *testing.T) {
	clientFactory := NewFakeClientFactory()
	ctx := pkg.CmdCtx{
		ClientFactory: clientFactory,
	}

	t.Run("command line parsing", func(t *testing.T) {
		for _, testcase := range []struct {
			args      string
			err       bool
			expectedL map[string]string
			expectedR []string
		}{
			{
				args:      "a b=c",
				err:       false,
				expectedL: map[string]string{"a": "", "b": "c"},
				expectedR: []string{},
			},
			{
				args:      "a=a b- c=c d=",
				err:       false,
				expectedL: map[string]string{"a": "a", "c": "c", "d": ""},
				expectedR: []string{"b"},
			},
			{
				args: "c=c=d=d",
				err:  true,
			},
			{
				args: "",
				err:  true,
			},
		} {
			t.Run("args["+testcase.args+"]", func(t *testing.T) {
				cmd := NewLabelCmd(&ctx)
				labelCmdOptions := LabelCmdOptions{}
				err := labelCmdOptions.Complete(cmd, &pkg.CmdCtx{}, strings.Split(strings.TrimSpace("job test "+testcase.args), " "))
				if testcase.err {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)
					assert.Equal(t, testcase.expectedL, labelCmdOptions.Labels)
					assert.Equal(t, testcase.expectedR, labelCmdOptions.RemoveLabels)
				}
			})
		}
	})

	t.Run("run", func(t *testing.T) {
		mockJob := &csv1.Job{
			Meta:   &csv1.ObjectMeta{Name: "foo", Labels: map[string]string{"x": "y"}},
			Status: &csv1.JobStatus{Phase: csv1.JobStatus_QUEUED},
		}
		r := protojson.MarshalOptions{EmitUnpopulated: true}.Format(mockJob)
		clientFactory.csCtlClient.responseOrErr = append(clientFactory.csCtlClient.responseOrErr,
			&pb.PatchResponse{
				Type:        "job",
				ContentType: pb.SerializationMethod_JSON_METHOD,
				Raw:         []byte(r),
			})
		outErr := NewFakeOutErr()
		opt := LabelCmdOptions{
			Type:   "job",
			Name:   "foo",
			Labels: map[string]string{"x": "y"},
			Output: "json",
			cmdCtx: &ctx,
			outErr: outErr,
		}

		err := opt.Run(context.TODO())
		require.NoError(t, err)

		// should output parsable json
		out := map[string]interface{}{}
		require.NoError(t, json.Unmarshal(outErr.O.Bytes(), &out))
	})
}

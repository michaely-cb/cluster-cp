package cmd

import (
	"strings"
	"testing"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputYaml(t *testing.T) {
	msg := &pb.GetResponse{
		Type:        "job",
		ContentType: pb.SerializationMethod_JSON_METHOD,
		Raw:         []byte(`{"baz":[1,2,3],"foo":"bar"}`),
	}
	outErr := NewFakeOutErr()
	require.NoError(t, YamlMarshaller([]GenericMessage{msg}, outErr, 0))
	expect := `baz:
- 1
- 2
- 3
foo: bar`
	assert.Equal(t, expect, strings.TrimSpace(outErr.O.String()))
}

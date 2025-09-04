//go:build !e2e

package csctl

import (
	"context"
	"testing"

	"cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	wscommon "cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg/wsclient"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclient "k8s.io/client-go/kubernetes/fake"
)

var testVolumesCm = corev1.ConfigMap{
	ObjectMeta: metav1.ObjectMeta{Name: wsclient.VolumesCmName, Namespace: wscommon.DefaultNamespace},
	Data: map[string]string{
		"v0": `{"type":"nfs","server":"nfs.example.com","serverPath":"/nfspath","containerPath":"/temp/nfspath","readonly":true,"labels":{"cerebras-internal":"true", "allow-venv":"true"}}`,
		"v1": `{"type":"hostPath","containerPath":"/opt/cerebras","readonly":false}`,
	}}

func TestGetListVolume(t *testing.T) {
	k8s := fakeclient.NewSimpleClientset(&testVolumesCm)
	vs := &VolumeStore{kubeClient: k8s}
	for _, testcase := range []struct {
		name        string
		expectedRow []string
	}{
		{
			name:        "v0",
			expectedRow: []string{"v0", "nfs", "/temp/nfspath", "nfs.example.com", "/nfspath", "true", "allow-venv=true,cerebras-internal=true"},
		},
		{
			name:        "v1",
			expectedRow: []string{"v1", "hostPath", "/opt/cerebras", "", "", "false", ""},
		},
	} {
		t.Run("get-"+testcase.name, func(t *testing.T) {
			res, err := vs.Get(context.TODO(), wscommon.DefaultNamespace, testcase.name)
			assert.Nil(t, err)
			r := res.(*Volume)
			assert.Equal(t, testcase.name, r.volume.Name)

			tableRes := vs.AsTable(res)
			assert.Equal(t, testcase.expectedRow, tableRes.GetRows()[0].GetCells())
		})
	}

	t.Run("list", func(t *testing.T) {
		res, err := vs.List(context.TODO(), wscommon.DefaultNamespace, &csctl.GetOptions{})
		assert.Nil(t, err)
		vl := res.(*VolumeList)
		assert.Len(t, vl.volumes, 2)

		tableRes := vs.AsTable(res)
		assert.Len(t, tableRes.GetRows(), 2)
		assert.Equal(t,
			[]string{"v0", "nfs", "/temp/nfspath", "nfs.example.com", "/nfspath", "true", "allow-venv=true,cerebras-internal=true"},
			tableRes.GetRows()[0].GetCells())
		assert.Equal(t,
			[]string{"v1", "hostPath", "/opt/cerebras", "", "", "false", ""},
			tableRes.GetRows()[1].GetCells())
	})
}

//go:build !e2e

package wsclient

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclient "k8s.io/client-go/kubernetes/fake"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	wscommon "cerebras.com/job-operator/common"
)

func TestParseMountDirs(t *testing.T) {
	testcases := []struct {
		input    map[string]string
		expected UserMountDirDb
		err      error
	}{
		{
			input: map[string]string{
				"v0": `{"type":"nfs","server":"nfs.example.com","containerPath":"/cb/nfspath","serverPath":"/nfspath","readonly":true}`,
			},
			expected: UserMountDirDb{
				"/cb/nfspath": *nfsMountDirSpec("v0", "/cb/nfspath", "nfs.example.com", "/nfspath", true),
			},
			err: nil,
		},
		{
			input: map[string]string{
				"v0": `{"type":"hostPath","server":"","containerPath":"/opt/cerebras/test/e2e/client/dependencies","readonly":false}`,
			},
			expected: UserMountDirDb{
				"/opt/cerebras/test/e2e/client/dependencies": *hostPathMountDirSpec("v0", "/opt/cerebras/test/e2e/client/dependencies", false),
			},
			err: nil,
		},
		{
			input: map[string]string{
				"v0": `{"type":"zzz","server":"","containerPath":"/opt/cerebras/test/e2e/client/dependencies","readonly":false}`,
			},
			expected: UserMountDirDb{
				"/opt/cerebras/test/e2e/client/dependencies": *hostPathMountDirSpec("v0", "/opt/cerebras/test/e2e/client/dependencies", false),
			},
			err: fmt.Errorf("corrupt"),
		},
		{
			input:    map[string]string{"v0": `{"badjson"}`},
			expected: nil,
			err:      fmt.Errorf("corrupt"),
		},
	}
	for _, test := range testcases {
		vcm := &UserMountDirDb{}
		err := vcm.parseCMData(test.input)
		if test.err == nil {
			require.Nil(t, err)
			assert.Equal(t, &test.expected, vcm)
		} else {
			require.NotNil(t, err)
			assert.Contains(t, err.Error(), test.err.Error())
		}
	}
}

func TestResolveMountDirs(t *testing.T) {
	ctx := context.Background()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: VolumesCmName, Namespace: wscommon.DefaultNamespace},
		Data: map[string]string{
			"v0": `{"type":"nfs","containerPath":"/cb","server":"nfs.example.com","serverPath":"/cb","readonly":false}`,
			"v1": `{"type":"nfs","containerPath":"/cb/ml","server":"nfs.example.com","serverPath":"/ml","readonly":false}`,
			"v2": `{"type":"nfs","containerPath":"/old-cb/ml","server":"old-nfs.example.com","serverPath":"/ml","readonly":false}`,
			"v3": `{"type":"nfs","containerPath":"/root-cb","server":"nfs.example.com","serverPath":"/","readonly":false}`,
			"v4": `{"type":"nfs","containerPath":"/home","server":"another-nfs.example.com","serverPath":"/srv/home","readonly":false}`,
			"v5": `{"type":"hostPath","containerPath":"/n1/user-venv","server":"","serverPath":"","readonly":false}`,
		},
	}

	testcases := []struct {
		k8scm         *corev1.ConfigMap
		reqs          []*pb.MountDir
		expectVolumes []MountDirSpec
		expectMounts  []*VolumeMount
		err           string
	}{
		// no mount requests is valid
		{
			k8scm: cm,
			reqs:  []*pb.MountDir{},
			err:   "",
		},

		// multiple valid mounts
		{
			k8scm: cm,
			reqs: []*pb.MountDir{
				{Path: "/cb/ml/wsmount/data"},
				{Path: "/cb"},
				{Path: "/cb/ml"},
				{Path: "/old-cb/ml"},
				{Path: "/cb/ml/wsmount/data2/spath"},
				{Path: "/root-cb"},
				{Path: "/root-cb/ml"},
				{Path: "/home/user"},
				{Path: "/n1/user-venv/venv-1000", ContainerPath: "/opt/cerebras/venv"},
			},
			expectVolumes: []MountDirSpec{
				*nfsMountDirSpec("v1", "/cb/ml/wsmount/data", "nfs.example.com", "/ml/wsmount/data", false),
				*nfsMountDirSpec("v0", "/cb", "nfs.example.com", "/cb", false),
				*nfsMountDirSpec("v1", "/cb/ml", "nfs.example.com", "/ml", false),
				*nfsMountDirSpec("v2", "/old-cb/ml", "old-nfs.example.com", "/ml", false),
				*nfsMountDirSpec("v1", "/cb/ml/wsmount/data2/spath", "nfs.example.com", "/ml/wsmount/data2/spath", false),
				*nfsMountDirSpec("v3", "/root-cb", "root-nfs.example.com", "/", false),
				*nfsMountDirSpec("v3", "/root-cb/ml", "root-nfs.example.com", "/ml", false),
				*nfsMountDirSpec("v4", "/home", "another-nfs.example.com", "/srv/home", false),
				*hostPathMountDirSpec("v5", "/n1/user-venv/venv-1000", false),
			},
			expectMounts: []*VolumeMount{
				{Name: "v1", MountPath: "/cb/ml/wsmount/data"},
				{Name: "v0", MountPath: "/cb"},
				{Name: "v1", MountPath: "/cb/ml"},
				{Name: "v2", MountPath: "/old-cb/ml"},
				{Name: "v1", MountPath: "/cb/ml/wsmount/data2/spath"},
				{Name: "v3", MountPath: "/root-cb"},
				{Name: "v3", MountPath: "/root-cb/ml"},
				{Name: "v4", MountPath: "/home/user"},
				{Name: "v5", MountPath: "/opt/cerebras/venv"},
			},
			err: "",
		},

		// no suitable volume found
		{
			k8scm: cm,
			reqs:  []*pb.MountDir{{Path: "/"}},
			err:   "invalid volume mount",
		},

		// no suitable volume found
		{
			k8scm: cm,
			reqs:  []*pb.MountDir{{Path: "/cb/../y"}},
			err:   "path '/cb/../y' is not allowed",
		},
	}
	for _, testcase := range testcases {
		var client *fakeclient.Clientset
		if testcase.k8scm != nil {
			client = fakeclient.NewSimpleClientset(testcase.k8scm)
		} else {
			client = fakeclient.NewSimpleClientset()
		}

		vols, mounts, err := ResolveMountDirs(ctx, wscommon.DefaultNamespace, client, testcase.reqs)
		if testcase.err != "" {
			require.Error(t, err)
			assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(testcase.err))
		} else {
			require.NoError(t, err)
			assert.Equal(t, len(testcase.expectMounts), len(mounts))
			assert.Equal(t, len(testcase.reqs), len(mounts))
			for i := 0; i < len(testcase.reqs); i++ {
				volName := mounts[i].Name
				for j := 0; j < len(vols); j++ {
					if vols[j].Name != volName {
						continue
					}

					assert.True(t, strings.HasPrefix(volName, testcase.expectVolumes[j].Name))
				}
			}
		}
	}
}

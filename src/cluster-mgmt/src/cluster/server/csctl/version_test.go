//go:build !e2e

package csctl

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wscommon "cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
)

func TestVersionStore(t *testing.T) {
	for _, testcase := range []struct {
		name         string
		versionMocks *versionStoreMocks
		nsArg        string
		expectRows   []string
	}{
		{
			name:         "no data case",
			versionMocks: newVersionStoreMocks("fake:fake", "", "", "", "", true),
			nsArg:        "testns",
			expectRows: []string{
				"csctl,csctl-v0",
				"csctl-semantic-version,unknown",
				"cluster-server,unknown",
				"cluster-server-semantic-version,unknown",
				"default-cbcore,unknown",
				"job-operator,unknown",
				"job-operator-semantic-version,unknown",
			},
		},
		{
			name: "ecm off, no user ns",
			versionMocks: newVersionStoreMocks(
				"test-cbcore:cbval1",
				"printthis1",
				"notthis",
				"testns",
				"csval1",
				false),
			nsArg: "job-operator",
			expectRows: []string{
				"csctl,csctl-v0",
				"csctl-semantic-version,unknown",
				"cluster-server,csval1",
				"cluster-server-semantic-version,unknown",
				"default-cbcore,cbval1",
				"job-operator,printthis1",
				"job-operator-semantic-version,unknown",
			},
		},
		{
			name: "ecm off, user ns",
			versionMocks: newVersionStoreMocks(
				"test-cbcore:cbval2",
				"notthis",
				"printthis2",
				"testns",
				"0.0.0-202309111129-4172-ef516896",
				false),
			nsArg: "testns",
			expectRows: []string{
				"csctl,csctl-v0",
				"csctl-semantic-version,unknown",
				"cluster-server,0.0.0-202309111129-4172-ef516896",
				"cluster-server-semantic-version,unknown",
				"default-cbcore,cbval2",
				"job-operator,printthis2",
				"job-operator-semantic-version,unknown",
			},
		},
		{
			name: "ecm on",
			versionMocks: newVersionStoreMocks(
				"test-cbcore:cbval3",
				"printthis3",
				"notthis",
				"",
				"csval3",
				true),
			nsArg: "test2",
			expectRows: []string{
				"csctl,csctl-v0",
				"csctl-semantic-version,unknown",
				"cluster-server,csval3",
				"cluster-server-semantic-version,unknown",
				"default-cbcore,cbval3",
				"job-operator,printthis3",
				"job-operator-semantic-version,unknown",
			},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			po := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "server",
					Labels: map[string]string{
						"app.kubernetes.io/instance": "cluster-server",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Env: []corev1.EnvVar{
								{
									Name:  "WSJOB_DEFAULT_IMAGE",
									Value: testcase.versionMocks.cbcoreVer,
								},
							},
						},
					},
				},
			}
			testcase.versionMocks.mockK8s.CoreV1().Pods(testcase.nsArg).Create(context.TODO(), po, metav1.CreateOptions{})
			defer func() {
				require.NoError(t, testcase.versionMocks.mockK8s.CoreV1().Pods(testcase.nsArg).Delete(context.TODO(),
					po.Name, metav1.DeleteOptions{}))
			}()
			vs := VersionStore{kubeClient: testcase.versionMocks.mockK8s,
				metricsQuerier: testcase.versionMocks}
			testcase.versionMocks.Setup()
			ctx := metadata.NewIncomingContext(context.TODO(), map[string][]string{
				"user-agent": {"csctl-v0"},
			})
			res, err := vs.List(ctx, testcase.nsArg, nil)
			require.NoError(t, err)
			rows := vs.AsTable(res)
			require.Len(t, rows.Rows, 7)
			for i, row := range rows.Rows {
				assert.Equal(t, strings.Split(testcase.expectRows[i], ","), row.Cells)
			}
		})
	}
}

type versionStoreMocks struct {
	wscommon.MockQuerier
	mockK8s           kubernetes.Interface
	cbcoreVer         string
	sysJobOpVer       string
	privateJobOpVer   string
	privateJobOpNs    string
	enableClusterMode bool
	csVer             string
}

func (v *versionStoreMocks) QueryJobOperatorVersion(ctx context.Context, nsName string) (string, string, error) {
	buildVersion := pkg.Ternary(v.enableClusterMode || nsName == wscommon.SystemNamespace, v.sysJobOpVer, v.privateJobOpVer)
	if buildVersion == "" {
		buildVersion = "unknown"
	}
	return buildVersion, "unknown", nil
}

func (v *versionStoreMocks) QueryClusterServerVersion(ctx context.Context, nsName string) (string, string, error) {
	buildVersion := v.csVer
	if buildVersion == "" {
		buildVersion = "unknown"
	}
	return buildVersion, "unknown", nil
}

func newVersionStoreMocks(cbcoreVer string, sysJobOpVer string, privateJobOpVer string, privateJobOpNs string, csVer string, enableClusterMode bool) *versionStoreMocks {
	return &versionStoreMocks{
		MockQuerier:       wscommon.MockQuerier{},
		cbcoreVer:         cbcoreVer,
		sysJobOpVer:       sysJobOpVer,
		privateJobOpVer:   privateJobOpVer,
		privateJobOpNs:    privateJobOpNs,
		enableClusterMode: enableClusterMode,
		csVer:             csVer,
		mockK8s:           fake.NewSimpleClientset(),
	}
}

func (v versionStoreMocks) Setup() {
	wscommon.EnableClusterMode = v.enableClusterMode
}

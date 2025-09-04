//go:build !e2e

package csadm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"

	"cerebras.com/cluster/server/pkg/wsclient"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	systemv1fake "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1/fake"
)

func TestUpdateSystem(t *testing.T) {
	testcases := []struct {
		name            string
		conditionExists bool
	}{
		{
			name:            "update with new failure",
			conditionExists: false,
		},
		{
			name:            "update on existing failure",
			conditionExists: true,
		},
	}
	for _, test := range testcases {
		k8s := fake.NewSimpleClientset()
		sysC := systemv1fake.FakeSystemV1{Fake: &k8stesting.Fake{}}
		s := NewServer(k8s, sysC.Systems(), nil, nil, nil, nil, wsclient.ServerOptions{}, nil)
		t.Run(test.name, func(t *testing.T) {
			testSystem := &systemv1.System{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
			oldMsg := "cm hang"
			if test.conditionExists {
				testSystem.Status = systemv1.SystemStatus{
					Conditions: []corev1.NodeCondition{
						{
							Type:    CsadmSystemError,
							Status:  corev1.ConditionTrue,
							Message: oldMsg,
						},
					},
				}
			}
			sysC.AddReactor(
				"get",
				"systems",
				func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, testSystem, nil
				},
			)

			newMsg := "System is detected with stall status"
			res, err := s.UpdateSystemStatus(context.TODO(), &pb.UpdateSystemStatusRequest{
				SystemName: testSystem.Name,
				Healthy:    false,
				Message:    newMsg,
			})
			require.NoError(t, err, "res", res, "conditions", testSystem.Status.Conditions)
			require.Len(t, testSystem.Status.Conditions, 1)
			require.Equal(t, corev1.ConditionTrue, testSystem.Status.Conditions[0].Status)
			require.Equal(t, corev1.NodeConditionType(CsadmSystemError), testSystem.Status.Conditions[0].Type)

			if test.conditionExists {
				require.Contains(t, testSystem.Status.Conditions[0].Message, newMsg)
				require.Contains(t, testSystem.Status.Conditions[0].Message, oldMsg)
			} else {
				require.Contains(t, testSystem.Status.Conditions[0].Message, newMsg)
			}
		})
	}
}

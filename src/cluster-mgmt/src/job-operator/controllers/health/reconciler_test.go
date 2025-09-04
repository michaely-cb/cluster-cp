//go:build default

package health

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

var (
	// consts for testing condition heartbeat/transition time
	reallyOldTime = v1.Date(2023, 4, 7, 12, 33, 45, 0, time.UTC)
	oldTime       = v1.Date(2023, 4, 8, 12, 33, 45, 0, time.UTC)
	currentTime   = v1.Date(2023, 4, 8, 13, 33, 45, 0, time.UTC)
)

func Test_getUpdates(t *testing.T) {
	tests := []struct {
		name         string
		node         *corev1.Node
		expectedNode *corev1.Node
		expectAlerts bool
		expectErr    bool
		updateMock   func(client *mockAlertCache)
	}{
		{
			name: "Test update node with single alert",
			node: newNode(corev1.NodeCondition{
				Status: "True",
				Type:   "Ready",
			}),
			expectedNode: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "True",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsFiring",
					Message:            "Node has error",
					LastHeartbeatTime:  currentTime,
					LastTransitionTime: currentTime,
				},
			),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "Node has error",
							},
							Alert: models.Alert{
								Labels: map[string]string{
									"type": "NodeError",
								},
							},
						},
					},
					currentTime,
					nil,
				)
			},
			expectAlerts: true,
		},
		{
			name: "Test update node with multiple alerts",
			node: newNode(corev1.NodeCondition{
				Status: "True",
				Type:   "Ready",
			}),
			expectedNode: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "True",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsFiring",
					Message:            "Node has error",
					LastHeartbeatTime:  currentTime,
					LastTransitionTime: currentTime,
				},
				corev1.NodeCondition{
					Status:             "True",
					Type:               "ClusterMgmtNodeNICError_eno1",
					Reason:             "AlertIsFiring",
					Message:            "Node has NIC error",
					LastHeartbeatTime:  currentTime,
					LastTransitionTime: currentTime,
				},
				corev1.NodeCondition{
					Status:             "True",
					Type:               "ClusterMgmtNodeSwitchPortError",
					Reason:             "AlertIsFiring",
					Message:            "Node has switch port error",
					LastHeartbeatTime:  currentTime,
					LastTransitionTime: currentTime,
				},
			),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "Node has error",
							},
							Alert: models.Alert{
								Labels: map[string]string{
									"type": "NodeError",
								},
							},
						},
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "Node has NIC error",
							},
							Alert: models.Alert{
								Labels: map[string]string{
									"type":   "NodeNICError",
									"device": "eno1",
								},
							},
						},
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "Node has switch port error",
							},
							Alert: models.Alert{
								Labels: map[string]string{
									"type": "NodeSwitchPortError",
								},
							},
						},
					},
					currentTime,
					nil,
				)
			},
			expectAlerts: true,
		},
		{
			name: "Test update with malformed alert",
			node: newNode(corev1.NodeCondition{
				Status: "True",
				Type:   "Ready",
			}),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "fake",
							},
							Alert: models.Alert{
								Labels: map[string]string{},
							},
						},
					},
					currentTime,
					nil,
				)
			},
			expectErr: true,
		},
		{
			name: "Test update healthy node with failure on getting alert",
			node: newNode(corev1.NodeCondition{
				Status: "True",
				Type:   "Ready",
			}),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					nil,
					currentTime,
					errors.New("cannot get alerts"),
				)
			},
		},
		{
			name: "Test update unhealthy node with failure getting alert",
			node: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "True",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsFiring",
					Message:            "Node has error",
					LastHeartbeatTime:  oldTime,
					LastTransitionTime: oldTime,
				},
			),
			expectedNode: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "Unknown",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertsUnavailable",
					Message:            "",
					LastHeartbeatTime:  currentTime,
					LastTransitionTime: currentTime,
				},
			),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					nil,
					currentTime,
					errors.New("cannot get alerts"),
				)
			},
		},
		{
			name: "Test update with last heartbeat on same condition",
			node: newNode(
				corev1.NodeCondition{
					Status:             "True",
					Type:               "Ready",
					LastHeartbeatTime:  oldTime,
					LastTransitionTime: oldTime,
				},
				corev1.NodeCondition{
					Status:             "True",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsFiring",
					Message:            "Node has error",
					LastHeartbeatTime:  oldTime,
					LastTransitionTime: oldTime,
				},
			),
			expectedNode: newNode(
				corev1.NodeCondition{
					Status:             "True",
					Type:               "Ready",
					LastHeartbeatTime:  oldTime,
					LastTransitionTime: oldTime,
				},
				corev1.NodeCondition{
					Status:             "True",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsFiring",
					Message:            "Node has error",
					LastHeartbeatTime:  currentTime,
					LastTransitionTime: oldTime,
				},
			),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "Node has error",
							},
							Alert: models.Alert{
								Labels: map[string]string{
									"type": "NodeError",
								},
							},
						},
					},
					currentTime,
					nil,
				)
			},
			expectAlerts: true,
		},
		{
			name: "Test update unhealthy node back to healthy",
			node: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "True",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsFiring",
					Message:            "Node has error",
					LastHeartbeatTime:  oldTime,
					LastTransitionTime: oldTime,
				},
			),
			expectedNode: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "False",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsNotFiring",
					Message:            "",
					LastHeartbeatTime:  currentTime,
					LastTransitionTime: currentTime,
				},
			),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{},
					currentTime,
					nil,
				)
			},
		},
		{
			name: "Test condition persists with false status if ttl not expired",
			node: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "False",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsNotFiring",
					LastHeartbeatTime:  oldTime,
					LastTransitionTime: oldTime,
				},
			),
			expectedNode: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "False",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsNotFiring",
					Message:            "",
					LastHeartbeatTime:  currentTime,
					LastTransitionTime: oldTime,
				},
			),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{},
					currentTime,
					nil,
				)
			},
		},
		{
			name: "Test condition deleted if ttl expired",
			node: newNode(
				corev1.NodeCondition{
					Status: "True",
					Type:   "Ready",
				},
				corev1.NodeCondition{
					Status:             "False",
					Type:               "ClusterMgmtNodeError",
					Reason:             "AlertIsNotFiring",
					LastHeartbeatTime:  oldTime,
					LastTransitionTime: reallyOldTime,
				},
			),
			expectedNode: newNode(corev1.NodeCondition{
				Status: "True",
				Type:   "Ready",
			}),
			updateMock: func(client *mockAlertCache) {
				client.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{},
					currentTime,
					nil,
				)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockAlertCache{}
			mockMetricClient := &mockMetricCache{}
			if tt.updateMock != nil {
				tt.updateMock(mockClient)
			}
			r := reconciler{
				log:         zap.New(),
				recorder:    record.NewFakeRecorder(3),
				client:      nil,
				AlertCache:  mockClient,
				MetricCache: mockMetricClient,
			}

			nr := &NodeReconciler{
				reconciler: r,
			}
			updateNode, err := nr.getUpdates(zap.New(), tt.node)
			if (err != nil) != tt.expectErr {
				t.Errorf("getUpdates() error = %v, expectErr %v", err, tt.expectErr)
			}
			if !tt.expectErr && !equality.Semantic.DeepEqual(tt.expectedNode, updateNode) {
				t.Errorf("getUpdates() diff = %v", cmp.Diff(tt.expectedNode, updateNode))
			}
			errorConditionFound := false
			if updateNode != nil {
				for _, cond := range updateNode.Status.Conditions {
					if !strings.HasPrefix(string(cond.Type), wscommon.ClusterMgmtConditionPrefix) {
						continue
					}
					if cond.Status == statusTrue {
						errorConditionFound = true
						break
					}
				}
			}
			if tt.expectAlerts != errorConditionFound {
				t.Errorf("getUpdates() errorConditionFound = %v, expectAlerts %v", errorConditionFound, tt.expectAlerts)
			}
			mock.AssertExpectationsForObjects(t, mockClient)
		})
	}
}

func Test_Reconcile(t *testing.T) {
	tests := []struct {
		reconcilerType string
		name           string
		updateMocks    func(c *wscommon.MockK8SClient, cache *mockAlertCache, metricCache *mockMetricCache)
		expectRes      reconcile.Result
		expectErr      bool
	}{
		{
			name:           "update node",
			reconcilerType: "node",
			updateMocks: func(k8sCli *wscommon.MockK8SClient, cache *mockAlertCache, metricCache *mockMetricCache) {
				cache.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "Node has error",
							},
							Alert: models.Alert{
								Labels: map[string]string{
									"type": "NodeError",
								},
							},
						},
					},
					currentTime,
					nil,
				)
				k8sCli.On("Get", mock.Anything, types.NamespacedName{Name: "node1"}, mock.Anything).Run(
					func(args mock.Arguments) {
						node := args.Get(2).(*corev1.Node)
						node.Name = "node1"
						node.Status.Conditions = []corev1.NodeCondition{
							{
								Type:   "Ready",
								Status: "True",
							},
						}
					},
				).Return(nil)
				match := func(obj interface{}) bool {
					return equality.Semantic.DeepEqual(obj, newNode(
						corev1.NodeCondition{
							Type:   "Ready",
							Status: "True",
						},
						corev1.NodeCondition{
							Status:             "True",
							Type:               "ClusterMgmtNodeError",
							Reason:             "AlertIsFiring",
							Message:            "Node has error",
							LastHeartbeatTime:  currentTime,
							LastTransitionTime: currentTime,
						},
					))
				}
				k8sCli.MockSubClient.On("Patch", mock.Anything, mock.MatchedBy(match), mock.Anything, mock.Anything).Return(nil)
			},
			expectRes: reconcile.Result{RequeueAfter: syncInterval},
			expectErr: false,
		},
		{
			name:           "update system",
			reconcilerType: "system",
			updateMocks: func(k8sCli *wscommon.MockK8SClient, cache *mockAlertCache, metricCache *mockMetricCache) {
				cache.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "System Overall Health error",
							},
							Alert: models.Alert{
								Labels: map[string]string{
									"type": "SystemError",
								},
							},
						},
					},
					currentTime,
					nil,
				)
				metricCache.On("GetSystemVersion", "node1").Return("rel-2.5")
				k8sCli.On("Get", mock.Anything, types.NamespacedName{Name: "node1"}, mock.Anything).Run(
					func(args mock.Arguments) {
						sys := args.Get(2).(*systemv1.System)
						sys.Name = "node1"
					},
				).Return(nil)
				match0 := func(obj interface{}) bool {
					return equality.Semantic.DeepEqual(obj, newSystem(
						nil,
						corev1.NodeCondition{
							Status:             "True",
							Type:               "ClusterMgmtSystemError",
							Reason:             "AlertIsFiring",
							Message:            "System Overall Health error",
							LastHeartbeatTime:  currentTime,
							LastTransitionTime: currentTime,
						},
					))
				}
				match1 := func(obj interface{}) bool {
					return equality.Semantic.DeepEqual(obj, newSystem(
						map[string]string{
							wsapisv1.SystemVersionKey: "rel-2.5",
						},
					))
				}

				// Status gets updated for both systems, but spec is only updated for match1
				k8sCli.MockSubClient.On("Patch", mock.Anything, mock.MatchedBy(match0), mock.Anything, mock.Anything).Return(nil)
				k8sCli.MockSubClient.On("Patch", mock.Anything, mock.MatchedBy(match1), mock.Anything, mock.Anything).Return(nil)
				k8sCli.On("Patch", mock.Anything, mock.MatchedBy(match1), mock.Anything, mock.Anything).Return(nil)
			},
			expectRes: reconcile.Result{RequeueAfter: syncInterval},
			expectErr: false,
		},
		{
			name:           "no update",
			reconcilerType: "node",
			updateMocks: func(k8sCli *wscommon.MockK8SClient, cache *mockAlertCache, metricCache *mockMetricCache) {
				cache.On("Get", []string{"node1"}).Return(
					models.GettableAlerts{
						&models.GettableAlert{
							Annotations: map[string]string{
								"summary": "Node has error",
							},
							Alert: models.Alert{
								Labels: map[string]string{
									"type": "NodeError",
								},
							},
						},
					},
					oldTime,
					nil,
				)
				k8sCli.On("Get", mock.Anything, types.NamespacedName{Name: "node1"}, mock.Anything).Run(
					func(args mock.Arguments) {
						node := args.Get(2).(*corev1.Node)
						node.Name = "node1"
						node.Status.Conditions = []corev1.NodeCondition{
							{
								Type:   "Ready",
								Status: "True",
							},
							{
								Status:             "True",
								Type:               "ClusterMgmtNodeError",
								Reason:             "AlertIsFiring",
								Message:            "Node has error",
								LastHeartbeatTime:  oldTime,
								LastTransitionTime: oldTime,
							},
						}
					},
				).Return(nil)
			},
			expectRes: reconcile.Result{RequeueAfter: syncInterval},
			expectErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "node1"},
			}
			c := &wscommon.MockK8SClient{MockSubClient: wscommon.MockSubClient{}}
			ac := &mockAlertCache{}
			mc := &mockMetricCache{}
			tt.updateMocks(c, ac, mc)

			r := reconciler{
				log:         logr.Discard(),
				recorder:    record.NewFakeRecorder(3),
				client:      c,
				AlertCache:  ac,
				MetricCache: mc,
			}

			var rr reconcile.Reconciler
			if tt.reconcilerType == "node" {
				rr = &NodeReconciler{
					reconciler: r,
				}
			} else {
				rr = &SystemReconciler{
					reconciler: r,
				}
			}

			got, err := rr.Reconcile(context.Background(), request)
			if (err != nil) != tt.expectErr {
				t.Errorf("Reconcile() error = %v, expectErr %v", err, tt.expectErr)
				return
			}
			if !reflect.DeepEqual(got, tt.expectRes) {
				t.Errorf("Reconcile() got = %v, expectRes %v", got, tt.expectRes)
			}
			mock.AssertExpectationsForObjects(t, c, ac)
		})
	}
}

type mockAlertCache struct {
	mock.Mock
}

func (m *mockAlertCache) Get(nodeName ...string) (models.GettableAlerts, v1.Time, error) {
	args := m.Called(nodeName)
	alerts := args.Get(0)
	someTime := args.Get(1).(v1.Time)
	if alerts == nil {
		return nil, someTime, args.Error(2)
	}
	return alerts.(models.GettableAlerts), someTime, args.Error(2)
}

type mockMetricCache struct {
	mock.Mock
}

func (m *mockMetricCache) GetSystemVersion(name string) string {
	args := m.Called(name)
	version := args.Get(0).(string)
	return version
}

func newNode(conditions ...corev1.NodeCondition) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Name: "node1",
		},
		Status: corev1.NodeStatus{
			Conditions: conditions,
		},
	}
}

func newSystem(labels map[string]string, conditions ...corev1.NodeCondition) *systemv1.System {
	return &systemv1.System{
		ObjectMeta: v1.ObjectMeta{
			Name:   "node1",
			Labels: labels,
		},
		Status: systemv1.SystemStatus{
			Conditions: conditions,
		},
	}
}

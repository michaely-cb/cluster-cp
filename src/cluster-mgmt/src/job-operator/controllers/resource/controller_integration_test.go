//go:build integration && resource_controller

package resource

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource/helpers"
	"cerebras.com/job-operator/controllers/resource/lock"
	"cerebras.com/job-operator/controllers/resource/namespace"
)

const (
	reconcileWait = 15 * time.Second
	reconcileTick = 20 * time.Millisecond

	compilesPerMgmtNode = 2
	executesPerMgmtNode = 4
)

// 2 pop (1 large mem) w mgmt node groups, 2 depop groups
func newTestClusterSchema(props map[string]string) wscommon.ClusterSchema {
	schema := wscommon.NewClusterConfigBuilder("test").
		WithCoordNode().
		WithSysMemxWorkerGroups(1, helpers.DefaultMemNodePerSys, helpers.DefaultWrkNodePerSys).
		WithCoordNode().
		WithSysMemxWorkerGroups(1, helpers.DefaultMemNodePerSys, helpers.DefaultWrkNodePerSys).
		WithBR(12).
		WithSysMemxWorkerGroups(2, 4, 1).
		WithAXInGroup("", 4).
		BuildClusterSchema().
		SplitNStamps(1, 4)
	schema.Properties = resource.CopyMap(props)
	return schema
}

type TestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	Manager manager.Manager
	Client  client.Client
	TestEnv *envtest.Environment
	CfgMgr  *wscommon.ClusterConfigMgr

	controller *Controller
}

func (t *TestSuite) SetupSuite() {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t.T())
	t.ctx = ctx
	t.cancel = cancel
	t.Manager = mgr
	t.Client = mgr.GetClient()
	t.TestEnv = testEnv

	wsapisv1.CrdCompileMinMemoryBytes = (wscommon.DefaultNodeMem / compilesPerMgmtNode) << 20
	wsapisv1.CrdExecuteMinMemoryBytes = (wscommon.DefaultNodeMem / executesPerMgmtNode) << 20
	namespace.InitMemConstants()
	namespace.DisableSystemNsAutoAssign = true

	schema := newTestClusterSchema(nil)
	require.NoError(t.T(), wscommon.CreateK8sNodes(schema, t.Client))
	t.createK8sSystems(schema)
	t.CfgMgr = wscommon.NewClusterConfigMgrFromSchema(t.Manager, schema)

	t.controller = NewController(t.CfgMgr)
	t.NoError(t.controller.SetupWithManager(mgr))
	t.CfgMgr.AddWatcher(t.controller.CfgChan)
	t.CfgMgr.Notify()
}

func (t *TestSuite) createK8sSystems(schema wscommon.ClusterSchema) {
	for _, sys := range schema.Systems {
		t.Require().NoError(t.Client.Create(t.ctx, &systemv1.System{
			ObjectMeta: metav1.ObjectMeta{Name: sys.Name},
			Spec:       *sys,
		}))
	}
}

// TearDownAllSuite has a TearDownSuite method, which will run after
// all the tests in the suite have been run.
func (t *TestSuite) TearDownSuite() {
	_ = t.TestEnv.Stop()
	t.cancel()

	wsapisv1.CrdCompileMinMemoryBytes = 0
	wsapisv1.CrdExecuteMinMemoryBytes = 0
	namespace.InitMemConstants()
	namespace.DisableSystemNsAutoAssign = false
}

func (t *TestSuite) BeforeTest(suiteName, testName string) {
	logrus.Infof("BeforeTest suite/test %s/%s", suiteName, testName)
}

func (t *TestSuite) AfterTest(suiteName, testName string) {
	logrus.Infof("AfterTest suite/test %s/%s", suiteName, testName)
}

func TestTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func (t *TestSuite) createLockAssertState(l *rlv1.ResourceLock, state rlv1.LockState) {
	t.Require().NoError(t.Client.Create(t.ctx, l))
	t.Assert().Eventually(func() bool {
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(l), l))
		t.Require().NotEqual(rlv1.LockError, l.Status.State)
		return l.Status.State == state
	}, reconcileWait, reconcileTick)
}

func (t *TestSuite) createNSR(nsr *namespacev1.NamespaceReservation) {
	_ = t.Client.Create(t.ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsr.Name}})
	t.createOrUpdateNsr(func(ctx context.Context, obj client.Object) error { return t.Client.Create(ctx, obj) }, nsr)
	t.Require().True(nsr.Status.GetLastAttemptedRequest().Succeeded)
}

func (t *TestSuite) updateNSR(nsr *namespacev1.NamespaceReservation) {
	t.createOrUpdateNsr(func(ctx context.Context, obj client.Object) error { return t.Client.Update(ctx, obj) }, nsr)
}

func (t *TestSuite) createOrUpdateNsr(fn func(ctx context.Context, obj client.Object) error, nsr *namespacev1.NamespaceReservation) {
	reqId := uuid.NewString()
	nsr.Spec.RequestUid = reqId
	t.Require().NoError(fn(t.ctx, nsr))
	t.Require().Eventually(func() bool {
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(nsr), nsr))
		return reqId == nsr.Status.GetLastAttemptedRequest().Request.RequestUid
	}, reconcileWait, reconcileTick)
}

func (t *TestSuite) assertSystemsLabeled(name string, systems ...string) {
	expectSystems := wscommon.MapBool(systems)
	var errs []string
	t.Assert().Eventually(func() bool {
		sl := &systemv1.SystemList{}
		errs = nil
		t.Require().NoError(t.Client.List(t.ctx, sl))
		for _, sys := range sl.Items {
			if expectSystems[sys.Name] && sys.Labels[wscommon.NamespaceKey] != name {
				errs = append(errs, fmt.Sprintf("%s did not have namespace:%s label", sys.Name, name))
			} else if !expectSystems[sys.Name] && sys.Labels[wscommon.NamespaceKey] == name {
				errs = append(errs, fmt.Sprintf("%s should not have namespace:%s label but did", sys.Name, name))
			}
		}
		return len(errs) == 0
	}, reconcileWait, reconcileTick)

	if len(errs) > 0 {
		t.FailNow(strings.Join(errs, ", "))
	}
}

func (t *TestSuite) createMockClusterServer(ns string) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-server",
			Namespace: ns,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: wscommon.Pointer(int32(2)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "cluster-server"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "cluster-server"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "server",
						Image: "server:latest",
					}},
				},
			},
		},
	}

	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-server",
			Namespace: ns,
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{{
				Host: fmt.Sprintf("%s.cluster-server.cerebrassc.local", ns),
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							Path:     "/",
							PathType: wscommon.Pointer(networkingv1.PathTypePrefix),
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: "cluster-server",
									Port: networkingv1.ServiceBackendPort{
										Number: 9000,
									},
								},
							},
						}},
					},
				},
			}},
		},
	}

	t.Require().NoError(t.Client.Create(t.ctx, deployment))
	t.Require().NoError(t.Client.Create(t.ctx, ingress))
}

// reset k8s resets k8s state between tests in a suite: delete locks, delete NSR, recreate systems
func (t *TestSuite) resetK8s() {
	t.Require().NoError(t.CfgMgr.Initialize(newTestClusterSchema(nil)))

	rl := &rlv1.ResourceLockList{}
	t.Require().NoError(t.Client.List(t.ctx, rl))
	for i := range rl.Items {
		l := &rl.Items[i]
		t.Require().NoError(t.Client.Delete(t.ctx, l))
	}
	nsrl := &namespacev1.NamespaceReservationList{}
	t.Require().NoError(t.Client.List(t.ctx, nsrl))
	for i := range nsrl.Items {
		nsr := nsrl.Items[i]
		t.Require().NoError(t.Client.Delete(t.ctx, &nsr))
	}
	// recreate systems
	sysList := &systemv1.SystemList{}
	t.Require().NoError(t.Client.List(t.ctx, sysList))
	for _, sys := range sysList.Items {
		s := &systemv1.System{ObjectMeta: metav1.ObjectMeta{Name: sys.Name}}
		t.Require().NoError(t.Client.Delete(t.ctx, s))
	}
	t.createK8sSystems(t.CfgMgr.Cfg.ClusterSchema)
}

func (t *TestSuite) TestController() {
	t.Run("outstanding lock prevents nsr release", func() {
		defer t.resetK8s()
		// assign sys0,2, lock sys, attempt release
		nsr := newNsrForSystemCount("ns00", 2)
		t.createNSR(nsr)
		t.assertSystemsLabeled(nsr.Name, "system-0", "system-1")

		l := lock.NewLockBuilder("l00").
			WithCoordinatorRequest(helpers.DefaultMgtNodeCpu/4, helpers.DefaultMgtNodeMem/4, nil).
			WithSystemRequest(2).
			WithPrimaryNodeGroup().WithSecondaryNodeGroup(1).
			BuildPendingForNS(nsr.Name)
		t.createLockAssertState(l, rlv1.LockGranted)

		// try shrinking the ns and ensure that resource is not released
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(nsr), nsr))
		nsr.Spec.RequestParams.SystemCount = wscommon.Pointer(1)
		t.updateNSR(nsr)
		t.Require().False(nsr.Status.GetLastAttemptedRequest().Succeeded)
		t.Assert().Len(nsr.Status.Systems, 2)
		t.Assert().Len(nsr.Status.Nodegroups, 2)
		t.Assert().Len(nsr.Status.Nodes, 1)
		t.assertSystemsLabeled(nsr.Name, "system-0", "system-1")

		// await release lock and then re-try ns release
		t.Require().NoError(t.Client.Delete(t.ctx, l))
		t.Require().Eventually(func() bool {
			return t.controller.resources.IsUnallocated()
		}, reconcileWait, reconcileTick)

		t.updateNSR(nsr)
		t.Require().True(nsr.Status.GetLastAttemptedRequest().Succeeded)
		t.Assert().Len(nsr.Status.Systems, 1)
		t.assertSystemsLabeled(nsr.Name, "system-0")
	})

	t.Run("nsr shrink triggers pending lock failure", func() {
		defer t.resetK8s()

		// create 1 sys job in 2 sys NS, then remove 1 sys, expect pending 2 sys lock to fail
		nsr := newNsrForSystemCount("ns10", 2)
		t.createNSR(nsr)
		t.assertSystemsLabeled(nsr.Name, "system-0", "system-1")

		l0 := lock.NewLockBuilder("l10").
			WithCoordinatorRequest(helpers.DefaultMgtNodeCpu/4, helpers.DefaultMgtNodeMem/4, nil).
			WithSystemRequest(1, "name:system-0").
			WithPrimaryNodeGroup("group:group0").
			BuildPendingForNS(nsr.Name)
		t.createLockAssertState(l0, rlv1.LockGranted)

		l1 := lock.NewLockBuilder("l11").
			WithCoordinatorRequest(helpers.DefaultMgtNodeCpu/4, helpers.DefaultMgtNodeMem/4, nil).
			WithSystemRequest(2).
			WithPrimaryNodeGroup().WithSecondaryNodeGroup(1).
			BuildPendingForNS(nsr.Name)
		t.createLockAssertState(l1, rlv1.LockPending)

		nsr.Spec.RequestParams.SystemCount = wscommon.Pointer(1)
		t.updateNSR(nsr)
		t.Require().True(nsr.Status.GetLastAttemptedRequest().Succeeded)
		t.Assert().Len(nsr.Status.Systems, 1)
		t.Assert().Len(nsr.Status.Nodegroups, 1)
		t.Assert().Len(nsr.Status.Nodes, 1)
		t.assertSystemsLabeled(nsr.Name, "system-0")

		t.Assert().Eventually(func() bool {
			t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(l1), l1))
			return l1.Status.State == rlv1.LockError
		}, reconcileWait, reconcileTick)
	})

	t.Run("nsr expansion triggers lock grant", func() {
		defer t.resetK8s()

		// create 2 sys job in 2 sys NS, then add 1 sys, expect pending 1 sys lock to succeed
		nsr := newNsrForSystemCount("ns20", 2)
		t.createNSR(nsr)

		l0 := lock.NewLockBuilder("l20").
			WithCoordinatorRequest(helpers.DefaultMgtNodeCpu/4, helpers.DefaultMgtNodeMem/4, nil).
			WithSystemRequest(2).
			WithPrimaryNodeGroup().WithSecondaryNodeGroup(1).
			BuildPendingForNS(nsr.Name)
		t.createLockAssertState(l0, rlv1.LockGranted)

		l1 := lock.NewLockBuilder("l21").
			WithCoordinatorRequest(helpers.DefaultMgtNodeCpu/4, helpers.DefaultMgtNodeMem/4, nil).
			WithSystemRequest(1).
			WithPrimaryNodeGroup().
			BuildPendingForNS(nsr.Name)
		t.createLockAssertState(l1, rlv1.LockPending)

		nsr.Spec.RequestParams.SystemCount = wscommon.Pointer(3)
		nsr.Spec.RequestParams.ParallelExecuteCount = wscommon.Pointer(2)
		t.updateNSR(nsr)
		t.Require().True(nsr.Status.GetLastAttemptedRequest().Succeeded)
		t.assertSystemsLabeled(nsr.Name, "system-0", "system-1", "system-2")

		t.Assert().Eventually(func() bool {
			t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(l1), l1))
			return l1.Status.State == rlv1.LockGranted
		}, reconcileWait, reconcileTick)
	})

	for _, testcase := range []struct {
		name      string
		deleteNSR bool
	}{
		{
			name:      "nsr delete cascade",
			deleteNSR: true,
		},
		{
			name:      "ns delete cascade",
			deleteNSR: false,
		},
	} {
		t.Run(testcase.name, func() {
			defer t.resetK8s()

			name := strings.Replace(testcase.name, " ", "-", -1)

			// create associated ns, persistentvolume
			nsOwnerLabel := map[string]string{namespacev1.NSLabel: name}
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{namespacev1.UserNSLabel: name}}}
			t.Require().NoError(t.Client.Create(t.ctx, ns))
			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pv-%s", name), Labels: nsOwnerLabel},
				Spec: corev1.PersistentVolumeSpec{
					Capacity:                      corev1.ResourceList{corev1.ResourceStorage: apiresource.MustParse("10Gi")},
					AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
					StorageClassName:              "fake-storage-class",
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:       "ceph.fake",
							VolumeHandle: "unique-volume-handle",
							FSType:       "xfs",
						},
					},
				},
			}
			t.Require().NoError(t.Client.Create(t.ctx, pv))

			nsr := newNsrForSystemCount(name, 4)
			t.createNSR(nsr)
			t.assertSystemsLabeled(nsr.Name, "system-0", "system-1", "system-2", "system-3")

			if testcase.deleteNSR {
				t.Require().NoError(t.Client.Delete(t.ctx, nsr))
			} else {
				t.Require().NoError(t.Client.Delete(t.ctx, ns))
			}

			// ensure system unlabeled after NSR delete
			t.assertSystemsLabeled(nsr.Name)

			// ensure cascade delete
			t.Assert().Eventually(func() bool {
				nsErr := t.Client.Get(t.ctx, client.ObjectKeyFromObject(ns), ns)
				pvErr := t.Client.Get(t.ctx, client.ObjectKeyFromObject(pv), pv)
				// DeletionTimestamp because envtest doesn't actually delete NS/PV: https://book.kubebuilder.io/reference/envtest#namespace-usage-limitation
				return nsErr == nil && ns.DeletionTimestamp != nil && pvErr == nil && pv.DeletionTimestamp != nil
			}, reconcileWait, reconcileTick)
		})
	}
}

func (t *TestSuite) TestController_ConfigChanges() {
	simulateSysSwap := func(schema wscommon.ClusterSchema, sysIdx int) {
		// swap system0 for system0p, simulate system updater's create+delete of corresponding k8 systems
		originName := schema.Systems[sysIdx].Name
		schema.Systems[sysIdx].Name = originName + "p"
		t.Require().NoError(t.CfgMgr.Initialize(schema))
		t.Require().NoError(t.Client.Create(t.ctx, &systemv1.System{
			ObjectMeta: metav1.ObjectMeta{Name: schema.Systems[sysIdx].Name, Labels: map[string]string{
				wscommon.NamespaceKey: "",
			}},
			Spec: *schema.Systems[sysIdx],
		}))
		t.Require().NoError(t.Client.Delete(t.ctx, &systemv1.System{ObjectMeta: metav1.ObjectMeta{Name: originName}}))
	}

	requireNsHasAllSystems := func(nsr *namespacev1.NamespaceReservation) {
		// ensure system namespace has new system and test namespace doesn't have any of the affinity resources
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(nsr), nsr)
			if err != nil {
				return false
			}
			haveSys := wscommon.MapBool(nsr.Status.Systems)
			for sys := range t.CfgMgr.Cfg.SystemMap {
				if !haveSys[sys] {
					return false
				}
				delete(haveSys, sys)
			}
			return len(haveSys) == 0
		}, reconcileWait, reconcileTick)
	}

	t.Run("cluster config system swap system namespace only", func() {
		// when system namespace is the only NS, new resources, should auto-assign to the system namespace
		defer t.resetK8s()
		namespace.DisableSystemNsAutoAssign = false
		defer func() {
			namespace.DisableSystemNsAutoAssign = true
		}()
		schema := newTestClusterSchema(nil)

		// trigger a config update to force initialization of NS reconciler
		t.Require().NoError(t.CfgMgr.Initialize(t.CfgMgr.Cfg.ClusterSchema))

		// the system NS should be created and obtain all the resources
		sysNsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: wscommon.SystemNamespace}}
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr)
			return err == nil && strings.HasPrefix(sysNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)

		// scaling down the system namespace manually should not immediately re-scale
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr))
		sysNsr.Spec.RequestParams = namespacev1.RequestParams{}
		t.updateNSR(sysNsr)
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr)
			return err == nil && !strings.HasPrefix(sysNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)

		// config update should re-assign all resources to system-namespace
		simulateSysSwap(schema, 0)
		requireNsHasAllSystems(sysNsr)
	})

	t.Run("cluster config system swap with user namespaces", func() {
		// when system + user namespace exist, new systems should be added to the free pool
		defer t.resetK8s()
		namespace.DisableSystemNsAutoAssign = false
		defer func() {
			namespace.DisableSystemNsAutoAssign = true
		}()
		schema := newTestClusterSchema(nil)

		// trigger a config update to force initialization of NS reconciler
		t.Require().NoError(t.CfgMgr.Initialize(t.CfgMgr.Cfg.ClusterSchema))
		sysNsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: wscommon.SystemNamespace}}
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr)
			return err == nil && strings.HasPrefix(sysNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)

		userNsr := &namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "user"},
			Spec:       namespacev1.NamespaceReservationSpec{ReservationRequest: namespacev1.ReservationRequest{RequestUid: uuid.NewString()}},
		}
		t.Require().NoError(t.Client.Create(t.ctx, userNsr))
		t.waitForNSRGrantedAndK8sNamespaceCreated(userNsr)

		simulateSysSwap(schema, 0)

		t.Require().Eventually(func() bool {
			t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr))
			return len(sysNsr.Status.Systems) == len(t.CfgMgr.Cfg.Systems)-1
		}, reconcileWait, reconcileTick)

		lastUpdate := sysNsr.Status.UpdateHistory[0]
		setParams := lastUpdate.Request.RequestParams
		t.Require().Equal(setParams.UpdateMode, namespacev1.DebugSetMode)
		t.Require().Equal(setParams.SystemNames, []string{"system-1", "system-2", "system-3"})
	})

	t.Run("cluster config system swap with user namespaces - v2 network", func() {
		// when system + user namespace exist, new systems should be added to the free pool
		defer t.resetK8s()
		namespace.DisableSystemNsAutoAssign = false
		defer func() {
			namespace.DisableSystemNsAutoAssign = true
		}()
		schema := newTestClusterSchema(map[string]string{"topology": "v2"})

		// trigger a config update to force initialization of NS reconciler
		t.Require().NoError(t.CfgMgr.Initialize(schema))
		sysNsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: wscommon.SystemNamespace}}
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr)
			return err == nil && strings.HasPrefix(sysNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)

		sysNsr.Spec.RequestParams = namespacev1.RequestParams{}
		t.updateNSR(sysNsr)
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr)
			return err == nil && !strings.HasPrefix(sysNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)

		userNsr := &namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "user-test-v2"},
			Spec: namespacev1.NamespaceReservationSpec{
				ReservationRequest: namespacev1.ReservationRequest{
					RequestUid: uuid.NewString(),
					RequestParams: namespacev1.RequestParams{
						UpdateMode:           namespacev1.SetResourceSpecMode,
						SystemCount:          wscommon.Pointer(4),
						ParallelCompileCount: wscommon.Pointer(2),
						ParallelExecuteCount: wscommon.Pointer(1),
					},
				}},
		}
		t.Require().NoError(t.Client.Create(t.ctx, userNsr))

		t.waitForNSRGrantedAndK8sNamespaceCreated(userNsr)
		t.Require().ElementsMatch([]string{"system-0", "system-1", "system-2", "system-3"}, userNsr.Status.Systems)
		t.Require().Equal(6, len(userNsr.Status.Nodes))
		t.Require().Contains(userNsr.Status.Nodes, "ax-0")
		t.Require().Contains(userNsr.Status.Nodes, "ax-1")
		t.Require().Contains(userNsr.Status.Nodes, "ax-2")
		t.Require().Contains(userNsr.Status.Nodes, "ax-3")

		// create a lock that uses all systems and ax nodes
		l := lock.NewLockBuilder("l000").WithAxScheduling().
			WithCoordinatorRequest(helpers.DefaultMgtNodeCpu/4, helpers.DefaultMgtNodeMem/4, nil).
			WithSystemRequest(4).
			WithPrimaryNodeGroup().
			WithGroupRequest(rlv1.ChiefRequestName, 1).
			WithNodeRequest(rlv1.ChiefRequestName, 4).Role("activation").CpuMem(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
			BuildGroupRequest().
			WithActToCsxDomainsMap([][]int{{0}}).
			WithGroupRequest(rlv1.ActivationRequestName, 1).
			WithNodeRequest(rlv1.ActivationRequestName, 1).Role("activation").CpuMem(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
			BuildGroupRequest().
			BuildPendingForNS(userNsr.Name)
		t.createLockAssertState(l, rlv1.LockGranted)

		simulateSysSwap(schema, 0)

		lastUpdate := namespacev1.RequestStatus{}
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(userNsr), userNsr)
			lastUpdate = userNsr.Status.UpdateHistory[0]
			return err == nil && strings.HasPrefix(userNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)
		// should not kick out ax nodes on system swap
		t.Require().Equal(6, len(userNsr.Status.Nodes))

		t.Require().Equal(true, lastUpdate.Succeeded)
		setParams := lastUpdate.Request.RequestParams
		t.Require().Equal(namespacev1.DebugSetMode, setParams.UpdateMode)
		t.Require().ElementsMatch([]string{"system-1", "system-2", "system-3"}, setParams.SystemNames)
		t.Require().Equal(false, setParams.AutoSelectActivationNodes)
		// should not kick out ax nodes on system swap
		t.Require().Contains(userNsr.Status.Nodes, "ax-0")
		t.Require().Contains(userNsr.Status.Nodes, "ax-1")
		t.Require().Contains(userNsr.Status.Nodes, "ax-2")
		t.Require().Contains(userNsr.Status.Nodes, "ax-3")

		// Confirm nsr reconciling should fail due to both ax nodes are in use
		// This is NOT a production flow, though the test ensures ns reconciler init happens
		// only after lock reconciler init on config changes
		wsapisv1.AutoSetAxOnNsrInit = true
		defer func() { wsapisv1.AutoSetAxOnNsrInit = false }()
		simulateSysSwap(schema, 1)

		// first confirm that we are handling the request of interest
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(userNsr), userNsr)
			lastUpdate = userNsr.Status.UpdateHistory[0]
			return err == nil && lastUpdate.Request.RequestParams.AutoSelectActivationNodes
		}, reconcileWait, reconcileTick)

		// wait for a short period of time to confirm that we should not see the request of interest succeed
		t.Require().Never(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(userNsr), userNsr)
			if err != nil {
				logrus.Error("failed due to error retrieving nsr")
				return true
			} else if userNsr.Status.UpdateHistory[0].Request.RequestUid != lastUpdate.Request.RequestUid {
				logrus.Error("failed due to an unexpected new nsr update was seen later than the update of interest")
				return true
			} else if lastUpdate.Succeeded {
				logrus.Error("failed due to last nsr update succeeded but should not")
				return true
			}
			return false // false means succeeding for "Never()"
		}, 5*time.Second, reconcileTick)
		// assert that operator was aware of ax nodes were in use
		t.Require().Contains(lastUpdate.Message, "update was forced to select 4 in-use activation node(s)")
	})

	t.Run("cluster config system swap with user namespaces last system - v2 network", func() {
		// when system + user namespace exist, new systems should be added to the free pool
		defer t.resetK8s()
		namespace.DisableSystemNsAutoAssign = false
		defer func() {
			namespace.DisableSystemNsAutoAssign = true
		}()
		schema := newTestClusterSchema(map[string]string{"topology": "v2"})

		// trigger a config update to force initialization of NS reconciler
		t.Require().NoError(t.CfgMgr.Initialize(schema))
		sysNsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: wscommon.SystemNamespace}}
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr)
			return err == nil && strings.HasPrefix(sysNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)

		sysNsr.Spec.RequestParams = namespacev1.RequestParams{}
		t.updateNSR(sysNsr)
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(sysNsr), sysNsr)
			return err == nil && !strings.HasPrefix(sysNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)

		userNsr := &namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "user-test-last-v2"},
			Spec: namespacev1.NamespaceReservationSpec{
				ReservationRequest: namespacev1.ReservationRequest{
					RequestUid: uuid.NewString(),
					RequestParams: namespacev1.RequestParams{
						UpdateMode:           namespacev1.SetResourceSpecMode,
						SystemCount:          wscommon.Pointer(1),
						ParallelCompileCount: wscommon.Pointer(2),
						ParallelExecuteCount: wscommon.Pointer(1),
					},
				}},
		}
		t.Require().NoError(t.Client.Create(t.ctx, userNsr))

		t.waitForNSRGrantedAndK8sNamespaceCreated(userNsr)
		t.Require().ElementsMatch([]string{"system-0"}, userNsr.Status.Systems)
		t.Require().Equal(3, len(userNsr.Status.Nodes))
		t.Require().Contains(userNsr.Status.Nodes, "ax-0")

		// create a lock that uses all systems and ax nodes
		l := lock.NewLockBuilder("l001").WithAxScheduling().
			WithCoordinatorRequest(helpers.DefaultMgtNodeCpu/4, helpers.DefaultMgtNodeMem/4, nil).
			WithSystemRequest(1).
			WithPrimaryNodeGroup().
			WithGroupRequest(rlv1.ChiefRequestName, 1).
			WithNodeRequest(rlv1.ChiefRequestName, 1).Role("activation").CpuMem(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
			BuildGroupRequest().
			WithActToCsxDomainsMap([][]int{{0}}).
			WithGroupRequest(rlv1.ActivationRequestName, 1).
			WithNodeRequest(rlv1.ActivationRequestName, 1).Role("activation").CpuMem(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
			BuildGroupRequest().
			BuildPendingForNS(userNsr.Name)
		t.createLockAssertState(l, rlv1.LockGranted)

		simulateSysSwap(schema, 0)

		lastUpdate := namespacev1.RequestStatus{}
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(userNsr), userNsr)
			lastUpdate = userNsr.Status.UpdateHistory[0]
			return err == nil && strings.HasPrefix(userNsr.Status.GrantedRequest.RequestUid, "operator-")
		}, reconcileWait, reconcileTick)
		// should not kick out ax nodes on system swap
		t.Require().Equal(3, len(userNsr.Status.Nodes))

		t.Require().Equal(true, lastUpdate.Succeeded)
		setParams := lastUpdate.Request.RequestParams
		t.Require().Equal(namespacev1.DebugSetMode, setParams.UpdateMode)
		t.Require().ElementsMatch([]string{""}, setParams.SystemNames)
		t.Require().Equal(0, len(userNsr.Status.Systems))
		t.Require().Equal(false, setParams.AutoSelectActivationNodes)
		// should not kick out ax nodes on system swap
		t.Require().Contains(userNsr.Status.Nodes, "ax-0")
	})

	t.Run("changing compile execute crd mem config updates NSR", func() {
		// when system + user namespace exist, new systems should be added to the free pool
		defer t.resetK8s()

		userNsr := &namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "user"},
			Spec: namespacev1.NamespaceReservationSpec{
				ReservationRequest: namespacev1.ReservationRequest{
					RequestUid: uuid.NewString(),
					RequestParams: namespacev1.RequestParams{
						UpdateMode:           namespacev1.SetResourceSpecMode,
						SystemCount:          wscommon.Pointer(2),
						ParallelCompileCount: wscommon.Pointer(2),
						ParallelExecuteCount: wscommon.Pointer(1),
					},
				}},
		}
		t.Require().NoError(t.Client.Create(t.ctx, userNsr))

		t.waitForNSRGrantedAndK8sNamespaceCreated(userNsr)

		// simulate updating memory update, then re-init simulating job-operator restart
		wsapisv1.CrdCompileMinMemoryBytes /= 2
		wsapisv1.CrdExecuteMinMemoryBytes /= 2
		namespace.InitMemConstants()
		defer func() {
			wsapisv1.CrdCompileMinMemoryBytes = (wscommon.DefaultNodeMem / compilesPerMgmtNode) << 20
			wsapisv1.CrdExecuteMinMemoryBytes = (wscommon.DefaultNodeMem / executesPerMgmtNode) << 20
			namespace.InitMemConstants()
		}()
		t.Require().NoError(t.CfgMgr.Initialize(t.CfgMgr.Cfg.ClusterSchema))

		// should see estimates update
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(userNsr), userNsr)
			return err == nil && userNsr.Status.CurrentResourceSpec.ParallelExecuteCount == 1 && userNsr.Status.CurrentResourceSpec.ParallelCompileCount == 7
		}, reconcileWait, reconcileTick)
	})

	t.Run("cluster config deleted node and nodegroup removed from NSR status", func() {
		// ensure that nodes + nodegroups which are deleted (or renamed which is in effect a delete of the old name)
		// are removed from the status of a currently assigned NSR
		schema := newTestClusterSchema(nil)
		t.Require().NoError(t.CfgMgr.Initialize(schema))
		defer t.resetK8s()

		var mgmtNodes []*wscommon.Node
		for _, node := range schema.Nodes {
			if node.HasRole(wscommon.RoleManagement) || node.HasRole(wscommon.RoleCoordinator) {
				mgmtNodes = append(mgmtNodes, node)
			}
		}
		t.Require().Greater(len(mgmtNodes), 1)

		// add each system to test namespace
		testNsr := newNsrForSystemCount("test", len(schema.Systems))
		testNsr.Spec.RequestParams.ParallelCompileCount = wscommon.Pointer(compilesPerMgmtNode * (len(mgmtNodes) - 1))
		t.createNSR(testNsr)
		t.Require().Eventually(func() bool {
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(testNsr), testNsr)
			return err == nil && len(testNsr.Status.Systems) == len(schema.Systems) && len(testNsr.Status.Nodes) == len(mgmtNodes)
		}, reconcileWait, reconcileTick)

		// remove a mgmt node and update the CfgMgr, triggering a re-reconcile of cfgMgr
		nodes := wscommon.Filter(func(n *wscommon.Node) bool { return n.Name != mgmtNodes[0].Name }, schema.Nodes)
		schema.Nodes = nodes
		t.Require().NoError(t.CfgMgr.Initialize(schema))

		t.Require().Eventually(func() bool {
			t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(testNsr), testNsr))
			return len(testNsr.Status.Nodes) == len(mgmtNodes)-1
		}, reconcileWait, reconcileTick)

		// re-name a nodegroup and ensure that the nodegroup is no longer present in the testNsr
		prevGroupCount := len(testNsr.Status.Nodegroups)
		for _, node := range schema.Nodes {
			if node.GetGroup() == schema.Groups[0].Name {
				node.Properties[resource.GroupPropertyKey] = "newgroupname"
			}
		}
		schema.Groups[0].Name = "newgroupname"
		t.Require().NoError(t.CfgMgr.Initialize(schema))
		t.Require().Eventually(func() bool {
			t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(testNsr), testNsr))
			return len(testNsr.Status.Nodegroups) == prevGroupCount-1
		}, reconcileWait, reconcileTick)
	})

	t.Run("defer processing updates until cfg initialized", func() {
		testNsr := newNsrForSystemCount("test", 1)
		t.createNSR(testNsr)

		// simulate a job-operator re-init and leader election
		t.controller.isInitialized = false
		t.controller.Cfg = nil

		testNsr.Spec.RequestParams.SystemCount = wscommon.Pointer(2)
		testNsr.Spec.RequestUid = uuid.NewString()
		t.Require().NoError(t.Client.Update(t.ctx, testNsr))

		time.Sleep(500 * time.Millisecond)
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(testNsr), testNsr))
		t.Require().Len(testNsr.Status.UpdateHistory, 1)

		t.CfgMgr.Notify()

		t.Require().Eventually(func() bool {
			t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(testNsr), testNsr))
			return testNsr.Status.GetLastAttemptedRequest().Request.RequestUid == testNsr.Spec.RequestUid
		}, reconcileWait, reconcileTick)

		t.Require().NoError(t.Client.Delete(t.ctx, testNsr))
	})
}

func (t *TestSuite) TestController_NSScaleDownScenarios() {
	t.Run("test scale down and up NS", func() {
		defer t.resetK8s()
		defer func(durationOrig, origThreshold time.Duration, thresholdOrig int) {
			namespace.ClusterServerScaleDownMinAge = durationOrig
			namespace.MaxAllowedUpSessionsOverPopNGThreshold = thresholdOrig
			namespace.SessionIdleThresholdForScaleDown = origThreshold
		}(namespace.ClusterServerScaleDownMinAge, namespace.ClusterServerScaleDownMinAge, namespace.MaxAllowedUpSessionsOverPopNGThreshold)
		namespace.ClusterServerScaleDownMinAge = 0
		namespace.MaxAllowedUpSessionsOverPopNGThreshold = 0
		namespace.SessionIdleThresholdForScaleDown = 0

		// create 3 user namespaces, trigger a scale down and check that the oldest was scaled down
		var nsrs []*namespacev1.NamespaceReservation
		for i := 0; i < 3; i++ {
			nsr := newNsrForSystemCount(fmt.Sprintf("test%d", i), 1)
			t.createNSR(nsr)
			t.createMockClusterServer(nsr.Name)
			nsr.Spec.RequestParams = namespacev1.RequestParams{SystemCount: wscommon.Pointer(0)}
			nsr.Spec.RequestUid = uuid.NewString()
			t.updateNSR(nsr)
			nsrs = append(nsrs, nsr)
		}
		t.controller.internalReconcileEventCh <- triggerNsrScale

		t.Require().Eventually(func() bool {
			deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "cluster-server", Namespace: nsrs[0].Name}}
			t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(deploy), deploy))
			return *deploy.Spec.Replicas == 0
		}, reconcileWait, reconcileTick)
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: namespace.ClusterSvrRedirectSvc, Namespace: nsrs[0].Name}}
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(svc), svc))
		ingress := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "cluster-server", Namespace: nsrs[0].Name}}
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(ingress), ingress))
		for _, path := range ingress.Spec.Rules[0].HTTP.Paths {
			t.Require().Equal(path.Backend.Service.Name, namespace.ClusterSvrRedirectSvc)
		}

		// should have been the only NSR updated
		deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "cluster-server", Namespace: nsrs[1].Name}}
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(deploy), deploy))
		t.Require().Greater(int(*deploy.Spec.Replicas), 0)

		// add resources to the scaled down namespace and validate the scale up
		nsrs[0].Spec.RequestParams = namespacev1.RequestParams{SystemCount: wscommon.Pointer(1)}
		nsrs[0].Spec.RequestUid = uuid.NewString()
		t.updateNSR(nsrs[0])
		t.Require().Eventually(func() bool {
			svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: namespace.ClusterSvrRedirectSvc, Namespace: nsrs[0].Name}}
			err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(svc), svc)
			return err != nil && apierrors.IsNotFound(err)
		}, reconcileWait, reconcileTick)

		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(ingress), ingress))
		for _, path := range ingress.Spec.Rules[0].HTTP.Paths {
			t.Require().Equal(path.Backend.Service.Name, "cluster-server")
		}

		deploy = &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "cluster-server", Namespace: nsrs[0].Name}}
		t.Require().NoError(t.Client.Get(t.ctx, client.ObjectKeyFromObject(deploy), deploy))
		t.Require().EqualValues(*deploy.Spec.Replicas, 2)
	})
}

func newNsrForSystemCount(name string, count int) *namespacev1.NamespaceReservation {
	return &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: namespacev1.NamespaceReservationSpec{
			ReservationRequest: namespacev1.ReservationRequest{
				RequestParams: namespacev1.RequestParams{
					SystemCount: wscommon.Pointer(count),
				},
				RequestUid: uuid.NewString(),
			}}}
}

func (t *TestSuite) waitForNSRGrantedAndK8sNamespaceCreated(nsr *namespacev1.NamespaceReservation) {
	t.Require().Eventually(func() bool {
		// First check that NSR request was granted
		err := t.Client.Get(t.ctx, client.ObjectKeyFromObject(nsr), nsr)
		if err != nil || nsr.Status.GrantedRequest.RequestUid != nsr.Spec.RequestUid {
			return false
		}

		// Then wait for the actual Kubernetes namespace to be created
		// This is needed since there appears to be a race with subsequent operations
		// which complain that the namespace does not exist.
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsr.Name}}
		nsErr := t.Client.Get(t.ctx, client.ObjectKeyFromObject(ns), ns)
		return nsErr == nil
	}, reconcileWait, reconcileTick)
}

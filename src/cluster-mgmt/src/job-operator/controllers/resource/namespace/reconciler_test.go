//go:build default

/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package namespace

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	networkingv1 "k8s.io/api/networking/v1"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	rtutil "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource/helpers"
)

const (
	compilesPerMgmtNode = 2
	executesPerMgmtNode = 4
)

// smallClusterCollection generates a collection including the following resources:
// nodegroup0: pop + large mem
// nodegroup1: pop
// nodegroup2: pop + xlarge mem
// nodegroup[3-4]: depop
func smallClusterCollection() *resource.Collection {
	c := helpers.NewClusterBuilder(2).
		WithSystems(4, "").
		WithBR(12).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeLarge).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeRegular).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeXLarge).
		WithDepopulatedNodeGroups(2).
		Build()
	return c
}

// redundantSysCluster generates a collection including the following resources:
// nodegroup[0-1]: pop
func redundantSysCluster() *resource.Collection {
	c := helpers.NewClusterBuilder(1).
		WithSystems(4, "").
		WithPopulatedNodeGroups(2, wsapisv1.PopNgTypeRegular).
		Build()
	return c
}

// largeClusterCollection generates a collection including the following resources:
// nodegroup[0-3]: pop + large mem
// nodegroup[4-30]: pop
// nodegroup[31]: pop + xlarge mem
// nodegroup[32-63]: depop
func largeClusterCollection() *resource.Collection {
	c := helpers.NewClusterBuilder(16).
		WithSystems(64, "").
		WithBR(128).
		WithPopulatedNodeGroups(4, wsapisv1.PopNgTypeLarge).
		WithPopulatedNodeGroups(27, wsapisv1.PopNgTypeRegular).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeXLarge).
		WithDepopulatedNodeGroups(32).
		Build()
	return c
}

// v2LargeClusterCollection generates a collection including the following resources:
// nodegroup0: pop + large mem
// nodegroup1: pop
// nodegroup2: pop + xlarge mem
// nodegroup[3-8]: depop
func v2LargeClusterCollection() *resource.Collection {
	c := helpers.NewClusterBuilderWithMgmtCoordSeparation(3, 2).WithV2Network().
		WithSystems(2, "stamp-0").
		WithSystems(2, "stamp-1").
		WithSystems(2, "stamp-2").
		WithSystems(2, "stamp-3").
		WithAX(2, "stamp-0").
		WithAX(2, "stamp-1").
		WithAX(2, "stamp-2").
		WithAX(2, "stamp-3").
		WithBR(30).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeLarge).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeRegular).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeXLarge).
		WithDepopulatedNodeGroups(6).
		Build()
	return c
}

func multiMgmtCoordSeparationCollection() *resource.Collection {
	c := helpers.NewClusterBuilderWithMgmtCoordSeparation(4, 4).
		WithSystems(2, "stamp-0").
		WithAX(2, "stamp-0").
		WithBR(5).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeLarge).
		Build()
	return c
}

func multiMgmtNoCoordSeparationCollection() *resource.Collection {
	c := helpers.NewClusterBuilder(4).
		WithSystems(2, "stamp-0").
		WithAX(2, "stamp-0").
		WithBR(5).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeLarge).
		Build()
	return c
}

// v2NonPorClusterCollection generates a collection including the following resources:
// nodegroup0: pop + large mem
// nodegroup1: pop
// nodegroup2: pop + xlarge mem
// nodegroup[3-8]: depop
func v2NonPorClusterCollection(axCountPerSystem, ixCountPerStamp, perStampSystems, numStamps int) *resource.Collection {
	clusterBuilder := helpers.NewClusterBuilder(2).WithV2Network()
	stamp := ""
	if numStamps == 0 {
		clusterBuilder = clusterBuilder.
			WithSystems(perStampSystems, stamp).
			WithAX(perStampSystems*axCountPerSystem, stamp).
			WithIX(ixCountPerStamp, stamp)
	} else {
		for i := 0; i < numStamps; i++ {
			stamp = fmt.Sprintf("stamp-%d", i)
			clusterBuilder = clusterBuilder.
				WithSystems(perStampSystems, stamp).
				WithAX(perStampSystems*axCountPerSystem, stamp).
				WithIX(ixCountPerStamp, stamp)
		}
	}

	c := clusterBuilder.
		WithBR(30).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeLarge).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeRegular).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeXLarge).
		WithDepopulatedNodeGroups(6).
		Build()
	return c
}

// v2SmallClusterCollection generates a collection including the following resources:
// nodegroup0: pop + large mem
// nodegroup[1-5]: pop
// nodegroup6: pop + xlarge mem
func v2SmallClusterCollection() *resource.Collection {
	c := helpers.NewClusterBuilder(6).WithV2Network().
		WithSystem("g0").
		WithSystem("g1").
		WithSystem("g1").
		WithSystem("g1").
		WithSystem("g2").
		WithSystem("g2").
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeLarge).
		WithPopulatedNodeGroups(5, wsapisv1.PopNgTypeRegular).
		WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeXLarge).
		Build()
	return c
}

// v2InferenceClusterCollection generates a collection without any node groups.
func v2InferenceClusterCollection(axCountPerSystem, ixCountPerStamp, perStampSystems, numStamps int) *resource.Collection {
	clusterBuilder := helpers.NewClusterBuilder(2).WithV2Network()
	stamp := ""
	if numStamps == 0 {
		clusterBuilder = clusterBuilder.
			WithSystems(perStampSystems, stamp).
			WithAX(perStampSystems*axCountPerSystem, stamp).
			WithIX(ixCountPerStamp, stamp)
	} else {
		for i := 0; i < numStamps; i++ {
			stamp = fmt.Sprintf("stamp-%d", i)
			clusterBuilder = clusterBuilder.
				WithSystems(perStampSystems, stamp).
				WithAX(perStampSystems*axCountPerSystem, stamp).
				WithIX(ixCountPerStamp, stamp)
		}
	}

	c := clusterBuilder.
		WithBR(30).
		Build()
	return c
}

type testReconciler struct {
	Reconciler
	t                *testing.T
	lastModifiedRIDs nsChange
	sourceCollection *resource.Collection
}

func newTestReconcilerForCollection(t *testing.T, col *resource.Collection) *testReconciler {
	tr := testReconciler{
		sourceCollection: col,
		t:                t,
	}

	tr.Reconciler = Reconciler{
		client:                  nil,
		nsState:                 nsState{collection: newCollectionForNsMgmt(col)},
		notifyNsrChangeCallback: func(m common.NSChange) { tr.lastModifiedRIDs = m.(nsChange) },
		resourceStatus:          placeholderResourceStatus{},
	}

	return &tr
}

func (r *testReconciler) setClient(c client.Client) {
	r.client = c
}

func (r *testReconciler) reconcileAll(reqs ...ctrl.Request) {
	for _, rq := range reqs {
		_, err := r.Reconcile(context.Background(), rq)
		require.NoError(r.t, err)
	}
}

func (r *testReconciler) get(obj ...client.Object) {
	for _, o := range obj {
		require.NoError(r.t, r.client.Get(context.Background(), client.ObjectKeyFromObject(o), o))
	}
}

func setupTests() func() {
	compileMemMi, executeMemMi := perCompileCrdMemEstimateMi, perExecuteCrdMemEstimateMi
	perCompileCrdMemEstimateMi = helpers.DefaultMgtNodeMem / compilesPerMgmtNode
	perExecuteCrdMemEstimateMi = helpers.DefaultMgtNodeMem / executesPerMgmtNode
	return func() {
		perCompileCrdMemEstimateMi = compileMemMi
		perExecuteCrdMemEstimateMi = executeMemMi
	}
}

func TestNSR_EventSequence(t *testing.T) {
	afterTest := setupTests()
	defer afterTest()
	ctx := context.Background()
	common.InitLogging()
	type reconcilerAction func(reconciler *testReconciler)

	expectSuccess := func(nsr string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			require.NoError(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(n), n))
			if n.Spec.RequestParams.UpdateMode != namespacev1.SetResourceSpecDryRunMode {
				require.Equalf(tr.t, n.Spec.RequestUid, n.Status.GrantedRequest.RequestUid,
					"%s:%d: nsr %s was not granted but should have been", file, line, nsr)
			} else {
				require.NotEqualf(tr.t, n.Spec.RequestUid, n.Status.GrantedRequest.RequestUid,
					"%s:%d: nsr %s dry run update should not have updated GrantedRequest but did", file, line, nsr)
				require.Equalf(tr.t, n.Spec.RequestUid, n.Status.UpdateHistory[0].Request.RequestUid,
					"%s:%d: nsr %s dry run update should not have updated GrantedRequest but did", file, line, nsr)
				require.Truef(tr.t, n.Status.UpdateHistory[0].Succeeded,
					"%s:%d: nsr %s dry run update should have succeeded but didn't", file, line, nsr)
			}
		}
	}

	expectFailWithMsg := func(nsr, msgSubstring string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			require.NoError(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(n), n))
			require.NotEqualf(tr.t, n.Spec.RequestUid, n.Status.GrantedRequest.RequestUid,
				"%s:%d: nsr %s was granted but should not have been", file, line, nsr)
			require.Containsf(tr.t, n.Status.GetLastAttemptedRequest().Message, msgSubstring,
				"%s:%d: nsr %s failed with incorrect err message", file, line, nsr)
		}
	}

	expectSystems := func(nsr string, systems ...string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			require.NoError(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(n), n))
			require.ElementsMatchf(tr.t, systems, n.Status.Systems, "%s:%d: expected systems did not match", file, line)

			expectSys := common.MapBool(systems)
			sysList := &systemv1.SystemList{}
			require.NoError(t, tr.client.List(ctx, sysList))
			for _, sys := range sysList.Items {
				if expectSys[sys.Name] {
					require.Equalf(t, nsr, sys.Labels[common.NamespaceKey], "%s:%d: expected system label not set", file, line)
				} else {
					require.NotEqualf(t, nsr, sys.Labels[common.NamespaceKey], "%s:%d: unexpected system label set", file, line)
				}
			}
		}
	}

	expectNodegroups := func(nsr string, nodegroups ...string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			t.Helper()
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			require.NoError(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(n), n))
			require.ElementsMatchf(tr.t, nodegroups, n.Status.Nodegroups, "%s:%d: nodegroup assignments differed for %s", file, line, nsr)
		}
	}

	expectNodes := func(nsr string, nodes ...string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			require.NoError(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(n), n))
			assert.ElementsMatchf(tr.t, nodes, n.Status.Nodes, "%s:%d: expected nodes did not match", file, line)
		}
	}

	expectReleaseEvents := func(rids ...string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			require.ElementsMatchf(tr.t, tr.lastModifiedRIDs.Unassigned(), rids, "%s:%d: expected release events did not match", file, line)
		}
	}

	expectAddEvents := func(nsr string, rids ...string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			m := map[string]string{}
			for _, rid := range rids {
				m[rid] = nsr
			}
			require.Equalf(tr.t, m, tr.lastModifiedRIDs.Assigned(), "%s:%d: expected resource add events for %s differed from actual", file, line, nsr)
		}
	}

	expectDeploymentPodAnnotation := func(deployment string, nsr string, annotations map[string]string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployment, Namespace: nsr}}
			require.NoError(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(deployment), deployment))

			for key, value := range annotations {
				if val, ok := deployment.Spec.Template.Annotations[key]; ok {
					require.Equalf(tr.t, value, val, "%s:%d: expected annotation %s with value %s but got value %s", file, line, key, value, val)
				} else {
					// Empty value is treated as meaning it was not set
					if value != "" {
						require.Failf(tr.t, "expected label not found", "%s:%d: expected annotation %s not found in deployment", file, line, key)
					}
				}
			}
		}
	}

	setResourceHealth := func(status string, rids ...string) reconcilerAction {
		return func(tr *testReconciler) {
			for _, rid := range rids {
				if r, ok := tr.collection.Get(rid); ok {
					tr.collection.UpdateProp(r.Id(), resource.HealthProp, status)
					if strings.HasPrefix(rid, "system/") {
						tr.collection.UpdateSubresource(rid, resource.SystemPortSubresource, 11)
					}
				} else {
					require.Failf(tr.t, "resource not found", rid)
				}
			}
		}
	}

	setInUseResources := func(rids ...string) reconcilerAction {
		return func(tr *testReconciler) {
			tr.resourceStatus = mockResourceStatus(common.MapBool(rids))
			tr.Reconciler.resourceStatus = tr.resourceStatus
		}
	}

	createOrUpdateNSROption := func(nsr string, params namespacev1.RequestParams, overwriteSystemReq bool) reconcilerAction {
		return func(tr *testReconciler) {
			req := namespacev1.ReservationRequest{
				RequestParams: params,
				RequestUid:    uuid.NewString(),
			}
			if overwriteSystemReq && nsr == common.SystemNamespace {
				req = tr.newSystemNamespaceSelectAllRequest()
			}
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			if strings.HasSuffix(nsr, "-inference-test") {
				// hack to add workload type without changing param of this function
				n.SetLabels(map[string]string{
					namespacev1.SessionWorkloadTypeKey: "inference",
				})
			}
			if err := tr.client.Get(ctx, client.ObjectKeyFromObject(n), n); err != nil {
				n.Spec = namespacev1.NamespaceReservationSpec{ReservationRequest: req}
				require.NoError(tr.t, tr.client.Create(ctx, n))
			} else {
				n.Spec.ReservationRequest = req
				require.NoError(tr.t, tr.client.Update(ctx, n))
			}
			_, err := tr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: NSREvent + nsr}})
			require.NoError(tr.t, err)
		}
	}

	createOrUpdateNSR := func(nsr string, params namespacev1.RequestParams) reconcilerAction {
		return createOrUpdateNSROption(nsr, params, true)
	}

	createOrUpdateSystemNSR := func(nsr string, params namespacev1.RequestParams) reconcilerAction {
		return createOrUpdateNSROption(nsr, params, false)
	}

	createDeployment := func(name string, namespace string, containsCrdNodeAnnot string) reconcilerAction {
		return func(tr *testReconciler) {
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec:       appsv1.DeploymentSpec{Replicas: common.Pointer(int32(1))},
			}
			if containsCrdNodeAnnot != "" {
				deployment.Spec.Template = corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{ContainsCrdNodesAnno: containsCrdNodeAnnot}},
				}
			}
			require.NoError(tr.t, tr.client.Create(ctx, deployment))
		}
	}

	deleteNSR := func(nsr string) reconcilerAction {
		return func(tr *testReconciler) {
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			require.NoError(tr.t, tr.client.Delete(ctx, n))
			_, err := tr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: NSREvent + nsr}})
			require.NoError(tr.t, err)
		}
	}

	expectUnassignedResources := func(params namespacev1.RequestParams, rids ...string) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: namespacev1.UnassignedResourcesCM, Namespace: common.SystemNamespace}}
			require.NoError(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(cm), cm))
			yamlData := cm.Data[namespacev1.UnassignedResourcesCMDataKey]
			nsr := &namespacev1.NamespaceReservation{}
			require.NoError(tr.t, yaml.Unmarshal([]byte(yamlData), nsr))
			require.Equalf(tr.t, params, nsr.Status.GrantedRequest.RequestParams, "%s:%d: unassigned resource params did not match expected", file, line)
			if len(rids) == 1 && rids[0] == "*" {
				allIds := common.Map(func(rs resource.Resource) string { return rs.Id() }, tr.collection.List())
				require.ElementsMatchf(tr.t, allIds, nsr.Status.GetAllocatedRIDs(), "%s:%d: unassigned resource ids did not match expected", file, line)
			} else {
				require.ElementsMatchf(tr.t, rids, nsr.Status.GetAllocatedRIDs(), "%s:%d: unassigned resource ids did not match expected", file, line)
			}
		}
	}

	expectAllResourcesUnassigned := func() reconcilerAction {
		return expectUnassignedResources(
			namespacev1.RequestParams{
				SystemCount:           common.Pointer(4),
				ParallelCompileCount:  common.Pointer(2),
				ParallelExecuteCount:  common.Pointer(3),
				LargeMemoryRackCount:  common.Pointer(1),
				XLargeMemoryRackCount: common.Pointer(1),
			},
			"*",
		)
	}

	expectDryrunResponse := func(nsr string, params namespacev1.RequestParams) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			require.NoError(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(n), n))
			last := n.Status.UpdateHistory[0]
			require.Equal(tr.t, params.UpdateMode, namespacev1.SetResourceSpecDryRunMode)
			assert.Equalf(tr.t, params, last.Request.RequestParams, "%s:%d: expected dryrun request params did not match", file, line)
			// check only 1 dry run history present
			for _, e := range n.Status.UpdateHistory[1:] {
				assert.NotEqualf(tr.t, e.Request.RequestParams.UpdateMode, namespacev1.SetResourceSpecDryRunMode,
					"%s:%d should not have had more than one dry run history entry", file, line)
			}
		}
	}

	expectResourceSpec := func(nsr string, compiles, executes, lrgMemRacks, xlrgMemRacks int) reconcilerAction {
		_, file, line, _ := runtime.Caller(1)
		return func(tr *testReconciler) {
			n := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsr}}
			require.NoErrorf(tr.t, tr.client.Get(ctx, client.ObjectKeyFromObject(n), n), "%s:%d: unexpected err", file, line)
			require.Equalf(tr.t, namespacev1.ResourceSpec{
				ParallelCompileCount:  compiles,
				ParallelExecuteCount:  executes,
				LargeMemoryRackCount:  lrgMemRacks,
				XLargeMemoryRackCount: xlrgMemRacks,
			}, n.Status.CurrentResourceSpec, "%s:%d: currentResourceSpec differed from expected", file, line)
		}
	}

	updateClusterCfg := func(collection *resource.Collection) reconcilerAction {
		return func(tr *testReconciler) {
			newTr := newTestReconcilerForCollection(tr.t, collection)
			newTr.client = tr.client
			tr.Reconciler = newTr.Reconciler
			tr.sourceCollection = collection
			_, err := tr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: common.ConfigEventName}})
			require.NoError(tr.t, err)
		}
	}

	for _, tc := range []struct {
		name                       string
		collection                 *resource.Collection
		actionSequence             []reconcilerAction
		useAxScheduling            bool
		useIxScheduling            bool
		mgmtSeparationCheckEnabled bool
		isInferenceCluster         bool
	}{
		{
			name:       "error validation static errors - capabilities mode",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr-fail-too-many-sys", namespacev1.RequestParams{SystemCount: common.Pointer(5)}),
				expectFailWithMsg("nsr-fail-too-many-sys", "cluster only has 4 available of 4 total systems but 5 requested"),

				createOrUpdateNSR("nsr-fail-too-many-pop-ng", namespacev1.RequestParams{SystemCount: common.Pointer(4), ParallelExecuteCount: common.Pointer(4)}),
				expectFailWithMsg("nsr-fail-too-many-pop-ng", "cluster only has 3 available of 3 total nodegroups capable of running execute jobs but 4 requested"),

				createOrUpdateNSR("nsr-fail-too-many-large-ng", namespacev1.RequestParams{SystemCount: common.Pointer(4), LargeMemoryRackCount: common.Pointer(2)}),
				expectFailWithMsg("nsr-fail-too-many-large-ng", "cluster only has 1 available of 1 total large memory racks but 2 requested"),

				createOrUpdateNSR("nsr-fail-too-many-large-ng", namespacev1.RequestParams{SystemCount: common.Pointer(4), XLargeMemoryRackCount: common.Pointer(2)}),
				expectFailWithMsg("nsr-fail-too-many-large-ng", "cluster only has 1 available of 1 total xlarge memory racks but 2 requested"),

				createOrUpdateNSR("nsr-fail-too-many-parallel-compiles", namespacev1.RequestParams{SystemCount: common.Pointer(4), ParallelCompileCount: common.Pointer(compilesPerMgmtNode * 2)}),
				expectFailWithMsg("nsr-fail-too-many-parallel-compiles", "requested a total of 144Mi coordinator memory capacity but could only allocate 128Mi from 2 available nodes"),

				createOrUpdateNSR("nsr-fail-too-few-systems", namespacev1.RequestParams{SystemCount: common.Pointer(2), ParallelExecuteCount: common.Pointer(3)}),
				expectFailWithMsg("nsr-fail-too-few-systems", "requested 3 parallel executes which is greater than 2 requested systems"),
			},
		},
		{
			name:       "error validation static errors - resource mode",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr-fail-unknown-sys", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, SystemNames: []string{"system-x"}}),
				expectFailWithMsg("nsr-fail-unknown-sys", "system 'system-x' was not found"),

				createOrUpdateNSR("nsr-fail-unknown-ng", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, NodegroupNames: []string{"ng-x"}}),
				expectFailWithMsg("nsr-fail-unknown-ng", "nodegroup 'ng-x' was not found"),

				createOrUpdateNSR("nsr-unknown-mgmt", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, ManagementNodeNames: []string{"xxx"}}),
				expectFailWithMsg("nsr-unknown-mgmt", "node 'xxx' was not found"),

				createOrUpdateNSR("nsr-unknown-coord", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, CoordinatorNodeNames: []string{"xxx"}}),
				expectFailWithMsg("nsr-unknown-coord", "node 'xxx' was not found"),

				createOrUpdateNSR("nsr-fail-unknown-sys", namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode, SystemNames: []string{"system-x"}}),
				expectFailWithMsg("nsr-fail-unknown-sys", "system 'system-x' was not found"),

				createOrUpdateNSR("nsr-fail-unknown-ng", namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode, NodegroupNames: []string{"ng-x"}}),
				expectFailWithMsg("nsr-fail-unknown-ng", "nodegroup 'ng-x' was not found"),

				createOrUpdateNSR("nsr-unknown-mgmt", namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode, ManagementNodeNames: []string{"xxx"}}),
				expectFailWithMsg("nsr-unknown-mgmt", "node 'xxx' was not found"),

				createOrUpdateNSR("nsr-unknown-coord", namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode, CoordinatorNodeNames: []string{"xxx"}}),
				expectFailWithMsg("nsr-unknown-coord", "node 'xxx' was not found"),
			},
		},
		{
			name:       "error validation already assigned resources",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(1), LargeMemoryRackCount: common.Pointer(1)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0"),
				expectNodegroups("nsr0", "nodegroup0"),
				expectNodes("nsr0", "coordinator-0"),

				// capabilities mode
				createOrUpdateNSR("nsr-fail", namespacev1.RequestParams{SystemCount: common.Pointer(4)}),
				expectFailWithMsg("nsr-fail", "cluster only has 3 available of 4 total systems but 4 requested"),
				createOrUpdateNSR("nsr-fail", namespacev1.RequestParams{SystemCount: common.Pointer(1), LargeMemoryRackCount: common.Pointer(1)}),
				expectFailWithMsg("nsr-fail", "cluster only has 0 available of 1 total large memory racks but 1 requested"),
				createOrUpdateNSR("nsr-fail", namespacev1.RequestParams{SystemCount: common.Pointer(2), XLargeMemoryRackCount: common.Pointer(2)}),
				expectFailWithMsg("nsr-fail", "cluster only has 1 available of 1 total xlarge memory racks but 2 requested"),
				createOrUpdateNSR("nsr-fail", namespacev1.RequestParams{SystemCount: common.Pointer(3), ParallelExecuteCount: common.Pointer(3)}),
				expectFailWithMsg("nsr-fail", "cluster only has 2 available of 3 total nodegroups capable of running execute jobs but 3 requested"),

				// resource mode
				createOrUpdateNSR("nsr-fail", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, SystemNames: []string{"system-0"}}),
				expectFailWithMsg("nsr-fail", "requested system system-0 is already assigned to session nsr0"),
				createOrUpdateNSR("nsr-fail", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, NodegroupNames: []string{"nodegroup0"}}),
				expectFailWithMsg("nsr-fail", "requested nodegroup nodegroup0 is already assigned to session nsr0"),
				createOrUpdateNSR("nsr-fail", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, ManagementNodeNames: []string{"coordinator-0"}}),
				expectFailWithMsg("nsr-fail", "requested node coordinator-0 is already assigned to session nsr0"),
				createOrUpdateNSR("nsr-fail", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectFailWithMsg("nsr-fail", "requested node coordinator-0 is already assigned to session nsr0"),
			},
		},
		{
			name:       "error validation in-use systems",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1"),
				setInUseResources("system/system-0"),

				// capabilities mode
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(0)}),
				expectFailWithMsg("nsr0", "change from 2 to 0 systems but 1 systems have jobs running"),

				// debug mode
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, SystemNames: []string{"system-0"}}),
				expectFailWithMsg("nsr0", "system system-0 has jobs running and cannot be released"),
			},
		},
		{
			name:       "error validation in-use nodegroups",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				// create NSR with 1 pop and 1 depop group
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr0"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup3"),
				setInUseResources("nodegroup/nodegroup1", "nodegroup/nodegroup3"),

				// capabilities mode
				// shrink - xxx: technically the system-in-use failure message will always appear in real scenarios since NG in use implies sys in use...
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(0)}),
				expectFailWithMsg("nsr0", "update was forced to select 2 in-use nodegroups which exceeds requested system count 0"),

				// shrink, cannot release depop
				setInUseResources("nodegroup/nodegroup3"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2), ParallelExecuteCount: common.Pointer(2)}),
				expectFailWithMsg("nsr0",
					"update was forced to select 1 in-use nodegroups that do not support execute jobs but "+
						"need 2 more to meet parallelExecutes requirement which would surpass 2 systems requested"),

				// request large memory not possible due to in-use regular memory
				setInUseResources("nodegroup/nodegroup1"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(1), LargeMemoryRackCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0",
					"update was forced to select 1 in-use nodegroups that have regular memory but "+
						"need 1 more to meet large memory rack count requirement which would surpass 1 systems requested"),

				// request xlarge memory not possible due to in-use regular memory
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(1), XLargeMemoryRackCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0",
					"update was forced to select 1 in-use nodegroups that have regular memory but "+
						"need 1 more to meet xlarge memory rack count requirement which would surpass 1 systems requested"),

				// request both large memory and xlarge memory node possible due to in-use regular memory
				setInUseResources("nodegroup/nodegroup1", "nodegroup/nodegroup3"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2), LargeMemoryRackCount: common.Pointer(1), XLargeMemoryRackCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0",
					"update was forced to select 2 in-use nodegroups that have regular memory but "+
						"need 1 more to meet large memory rack count requirement and "+
						"1 more to meet xlarge memory rack count requirement which would surpass 2 systems requested"),

				// debug mode
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, NodegroupNames: []string{"nodegroup1"}}),
				expectFailWithMsg("nsr0",
					"nodegroup nodegroup1 has jobs running and cannot be released"),
			},
		},
		{
			name:       "error validation in-use coordinator nodes",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2), ParallelCompileCount: common.Pointer(compilesPerMgmtNode)}),
				expectSuccess("nsr0"),
				expectNodes("nsr0", "coordinator-0", "coordinator-1"),

				// capabilities mode
				// shrink, cannot release mgmt
				setInUseResources("node/coordinator-0", "node/coordinator-1"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(1), ParallelCompileCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0",
					"update was forced to select 2 in-use coordinator nodes with 128Mi memory which exceeds "+
						"requested 48Mi memory from requested 1 compiles and 1 executes by more than a complete node"),

				// shrink
				setInUseResources("node/coordinator-1"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(1), ParallelCompileCount: common.Pointer(1)}),
				expectSuccess("nsr0"),
				expectNodes("nsr0", "coordinator-1"),

				// debug mode
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, ManagementNodeNames: []string{"coordinator-1"}}),
				expectFailWithMsg("nsr0", "node coordinator-1 has jobs running and cannot be released"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-1"}}),
				expectFailWithMsg("nsr0", "node coordinator-1 has jobs running and cannot be released"),
			},
		},
		{
			name:       "append remove debug mode scenarios",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					SystemCount:          common.Pointer(4),
					ParallelCompileCount: common.Pointer(compilesPerMgmtNode)}),
				expectSuccess("nsr0"),
				expectNodes("nsr0", "coordinator-0", "coordinator-1"),
				expectSystems("nsr0", "system-0", "system-1", "system-2", "system-3"),
				expectResourceSpec("nsr0", 3, 2, 1, 0),

				// remove - allow unusable session
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugRemoveMode,
					SystemNames:         []string{"system-0", "system-1"},
					NodegroupNames:      []string{"nodegroup0"},
					ManagementNodeNames: []string{"coordinator-0", "coordinator-1"}}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-2", "system-3"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup3", "nodegroup4"),
				expectNodes("nsr0"),
				expectReleaseEvents("system/system-0", "system/system-1", "nodegroup/nodegroup0", "node/coordinator-0", "node/coordinator-1"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(2),
						ParallelCompileCount:  common.Pointer(3),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-0", "system/system-1", "nodegroup/nodegroup0", "nodegroup/nodegroup2", "node/coordinator-0", "node/coordinator-1",
				),
				expectResourceSpec("nsr0", 0, 0, 0, 0),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugAppendMode,
					SystemNames:         []string{"system-0", "system-1"},
					NodegroupNames:      []string{"nodegroup0"},
					ManagementNodeNames: []string{"coordinator-0", "coordinator-1"}}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1", "system-2", "system-3"),
				expectNodegroups("nsr0", "nodegroup0", "nodegroup1", "nodegroup3", "nodegroup4"),
				expectNodes("nsr0", "coordinator-0", "coordinator-1"),
				expectAddEvents("nsr0", "system/system-0", "system/system-1", "nodegroup/nodegroup0", "node/coordinator-0", "node/coordinator-1"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(0),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(0),
						LargeMemoryRackCount:  common.Pointer(0),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"nodegroup/nodegroup2",
				),
				expectResourceSpec("nsr0", 3, 2, 1, 0),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.DebugAppendMode,
					SystemNames:          []string{"system-0", "system-1"},
					NodegroupNames:       []string{"nodegroup0"},
					CoordinatorNodeNames: []string{"coordinator-0", "coordinator-1"}}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1", "system-2", "system-3"),
				expectNodegroups("nsr0", "nodegroup0", "nodegroup1", "nodegroup3", "nodegroup4"),
				expectNodes("nsr0", "coordinator-0", "coordinator-1"),
				expectAddEvents("nsr0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(0),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(0),
						LargeMemoryRackCount:  common.Pointer(0),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"nodegroup/nodegroup2",
				),
				expectResourceSpec("nsr0", 3, 2, 1, 0),

				// remove large mem group only
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:     namespacev1.DebugRemoveMode,
					NodegroupNames: []string{"nodegroup0"}}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1", "system-2", "system-3"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup3", "nodegroup4"),
				expectNodes("nsr0", "coordinator-0", "coordinator-1"),
				expectReleaseEvents("nodegroup/nodegroup0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(0),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(0),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"nodegroup/nodegroup0", "nodegroup/nodegroup2",
				),
				expectResourceSpec("nsr0", 3, 1, 0, 0),

				// add xlarge mem group only
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:     namespacev1.DebugAppendMode,
					NodegroupNames: []string{"nodegroup2"}}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1", "system-2", "system-3"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup2", "nodegroup3", "nodegroup4"),
				expectNodes("nsr0", "coordinator-0", "coordinator-1"),
				expectAddEvents("nsr0", "nodegroup/nodegroup2"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(0),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(0),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(0),
					},
					"nodegroup/nodegroup0",
				),
				expectResourceSpec("nsr0", 3, 2, 0, 1),

				deleteNSR("nsr0"),
				expectAllResourcesUnassigned(),
			},
		},
		{
			name:       "debug mode set scenarios",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-2", "system-3"},
					NodegroupNames:      []string{"nodegroup0"},
					ManagementNodeNames: []string{"coordinator-1"},
				}),
				expectSuccess("nsr0"),
				expectAddEvents("nsr0", "system/system-2", "system/system-3", "nodegroup/nodegroup0", "node/coordinator-1"),
				expectNodes("nsr0", "coordinator-1"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-0", "system-1", "system-3"},
					NodegroupNames:      []string{"nodegroup1", "nodegroup2", "nodegroup3"},
					ManagementNodeNames: []string{"coordinator-0"},
				}),
				expectSuccess("nsr0"),
				expectAddEvents("nsr0", "system/system-0", "system/system-1", "nodegroup/nodegroup1", "nodegroup/nodegroup2", "nodegroup/nodegroup3", "node/coordinator-0"),
				expectReleaseEvents("system/system-2", "nodegroup/nodegroup0", "node/coordinator-1"),
				expectSystems("nsr0", "system-0", "system-1", "system-3"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup2", "nodegroup3"),
				expectNodes("nsr0", "coordinator-0"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.DebugSetMode,
					SystemNames:          []string{"system-0", "system-1", "system-3"},
					NodegroupNames:       []string{"nodegroup1", "nodegroup2", "nodegroup3"},
					CoordinatorNodeNames: []string{"coordinator-0"},
				}),
				expectSuccess("nsr0"),
				expectAddEvents("nsr0"),
				expectReleaseEvents(),
				expectSystems("nsr0", "system-0", "system-1", "system-3"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup2", "nodegroup3"),
				expectNodes("nsr0", "coordinator-0"),

				// test the default keep behavior if not set but remove if explicitly set to empty string
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-0"},
					NodegroupNames:      []string{},
					ManagementNodeNames: []string{""}}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup2", "nodegroup3"),
				expectNodes("nsr0"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.DebugSetMode,
					SystemNames:          []string{"system-0"},
					NodegroupNames:       []string{},
					CoordinatorNodeNames: []string{""}}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup2", "nodegroup3"),
				expectNodes("nsr0"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-0", "system-3"},
					NodegroupNames:      []string{"nodegroup1", "nodegroup2"},
					ManagementNodeNames: []string{"coordinator-0"},
				}),

				// error handling still applies
				setInUseResources("system/system-0"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:  namespacev1.DebugSetMode,
					SystemNames: []string{"system-3"},
				}),
				expectFailWithMsg("nsr0", "session request not possible because system system-0 has jobs running and cannot be released"),
				expectSystems("nsr0", "system-0", "system-3"),

				setInUseResources(),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:  namespacev1.DebugSetMode,
					SystemNames: []string{"system-xx"},
				}),
				expectFailWithMsg("nsr0", "system 'system-xx' was not found"),

				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:  namespacev1.DebugSetMode,
					SystemNames: []string{"system-3"},
				}),
				expectFailWithMsg("nsr1", "requested system system-3 is already assigned to session nsr0."),

				deleteNSR("nsr0"),
				expectAllResourcesUnassigned(),
			},
		},
		{
			name:       "dry run scenarios",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-2", "system-3"},
					NodegroupNames:      []string{"nodegroup0"},
					ManagementNodeNames: []string{"coordinator-1"},
				}),
				expectSuccess("nsr0"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.SetResourceSpecDryRunMode,
					SystemCount:          common.Pointer(4),
					ParallelCompileCount: common.Pointer(2),
				}),
				expectSuccess("nsr0"),
				expectDryrunResponse("nsr0", namespacev1.RequestParams{
					UpdateMode:            namespacev1.SetResourceSpecDryRunMode,
					SystemCount:           common.Pointer(4),
					ParallelCompileCount:  common.Pointer(3),
					ParallelExecuteCount:  common.Pointer(2),
					LargeMemoryRackCount:  common.Pointer(1),
					XLargeMemoryRackCount: common.Pointer(0),
					SystemNames:           []string{"system-0", "system-1", "system-2", "system-3"},
					NodegroupNames:        []string{"nodegroup0", "nodegroup1", "nodegroup3", "nodegroup4"},
					CoordinatorNodeNames:  []string{"coordinator-0", "coordinator-1"},
					ManagementNodeNames:   []string{"coordinator-0", "coordinator-1"},
				}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.SetResourceSpecMode,
					SystemCount:          common.Pointer(4),
					ParallelCompileCount: common.Pointer(2),
				}),
				expectSuccess("nsr0"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.SetResourceSpecDryRunMode,
					SystemCount:          common.Pointer(1),
					ParallelCompileCount: common.Pointer(1),
				}),
				expectSuccess("nsr0"),
				expectDryrunResponse("nsr0", namespacev1.RequestParams{
					UpdateMode:            namespacev1.SetResourceSpecDryRunMode,
					SystemCount:           common.Pointer(1),
					ParallelCompileCount:  common.Pointer(1),
					ParallelExecuteCount:  common.Pointer(1),
					LargeMemoryRackCount:  common.Pointer(0),
					XLargeMemoryRackCount: common.Pointer(0),
					SystemNames:           []string{"system-0"},
					NodegroupNames:        []string{"nodegroup1"},
					CoordinatorNodeNames:  []string{"coordinator-0"},
					ManagementNodeNames:   []string{"coordinator-0"},
				}),
				expectSystems("nsr0", "system-0", "system-1", "system-2", "system-3"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:            namespacev1.SetResourceSpecDryRunMode,
					SystemCount:           common.Pointer(1),
					ParallelCompileCount:  common.Pointer(1),
					XLargeMemoryRackCount: common.Pointer(1),
				}),
				expectSuccess("nsr0"),
				expectDryrunResponse("nsr0", namespacev1.RequestParams{
					UpdateMode:            namespacev1.SetResourceSpecDryRunMode,
					SystemCount:           common.Pointer(1),
					ParallelCompileCount:  common.Pointer(1),
					ParallelExecuteCount:  common.Pointer(1),
					LargeMemoryRackCount:  common.Pointer(0),
					XLargeMemoryRackCount: common.Pointer(1),
					SystemNames:           []string{"system-0"},
					NodegroupNames:        []string{"nodegroup2"},
					CoordinatorNodeNames:  []string{"coordinator-0"},
					ManagementNodeNames:   []string{"coordinator-0"},
				}),
				expectSystems("nsr0", "system-0", "system-1", "system-2", "system-3"),

				setInUseResources("system/system-0"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:  namespacev1.SetResourceSpecDryRunMode,
					SystemCount: common.Pointer(0),
				}),
				expectFailWithMsg("nsr0", "requested change from 4 to 0 systems but 1 systems have jobs running"),
			},
		},
		{
			name:       "large memory assignment scenarios",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				// avoid large memory rack when possible
				createOrUpdateNSR("nsr-2-sys-single-pop-0", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr-2-sys-single-pop-0"),
				expectAddEvents("nsr-2-sys-single-pop-0", "system/system-0", "system/system-1", "nodegroup/nodegroup1", "nodegroup/nodegroup3", "node/coordinator-0"),
				expectNodegroups("nsr-2-sys-single-pop-0", "nodegroup1", "nodegroup3"),
				expectSystems("nsr-2-sys-single-pop-0", "system-0", "system-1"),
				expectResourceSpec("nsr-2-sys-single-pop-0", 1, 1, 0, 0),

				// check unassigned resources consistent
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(2),
						ParallelCompileCount:  common.Pointer(1),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-2", "system/system-3", "nodegroup/nodegroup0", "nodegroup/nodegroup2", "nodegroup/nodegroup4", "node/coordinator-1",
				),

				// use large memory rack when no other option
				createOrUpdateNSR("nsr-2-sys-single-pop-1", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr-2-sys-single-pop-1"),
				expectAddEvents("nsr-2-sys-single-pop-1", "system/system-2", "system/system-3", "nodegroup/nodegroup0", "nodegroup/nodegroup4", "node/coordinator-1"),
				expectNodegroups("nsr-2-sys-single-pop-1", "nodegroup0", "nodegroup4"),
				expectResourceSpec("nsr-2-sys-single-pop-1", 1, 1, 1, 0),

				// check that the labels are still set on first NSR's systems
				expectSystems("nsr-2-sys-single-pop-0", "system-0", "system-1"),

				// check unassigned resources consistent
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(0),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(0),
						LargeMemoryRackCount:  common.Pointer(0),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"nodegroup/nodegroup2",
				),

				// release usage of large memory rack when update triggered
				deleteNSR("nsr-2-sys-single-pop-0"),
				createOrUpdateNSR("nsr-2-sys-single-pop-1", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr-2-sys-single-pop-1"),
				expectNodegroups("nsr-2-sys-single-pop-1", "nodegroup1", "nodegroup4"),
				expectReleaseEvents("nodegroup/nodegroup0"),

				// check unassigned resources consistent
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(2),
						ParallelCompileCount:  common.Pointer(1),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-0", "system/system-1", "nodegroup/nodegroup0", "nodegroup/nodegroup2", "nodegroup/nodegroup3", "node/coordinator-0",
				),

				// release the regular memory group and acquire the xlarge memory group
				createOrUpdateNSR("nsr-2-sys-single-pop-1", namespacev1.RequestParams{
					SystemCount:           common.Pointer(2),
					XLargeMemoryRackCount: common.Pointer(1)}),
				expectSuccess("nsr-2-sys-single-pop-1"),
				expectNodegroups("nsr-2-sys-single-pop-1", "nodegroup2", "nodegroup4"),
				expectReleaseEvents("nodegroup/nodegroup1"),

				// check unassigned resources consistent
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(2),
						ParallelCompileCount:  common.Pointer(1),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(0),
					},
					"system/system-0", "system/system-1", "nodegroup/nodegroup0", "nodegroup/nodegroup1", "nodegroup/nodegroup3", "node/coordinator-0",
				),

				// execute the same request again and should expect no change
				createOrUpdateNSR("nsr-2-sys-single-pop-1", namespacev1.RequestParams{
					SystemCount:           common.Pointer(2),
					XLargeMemoryRackCount: common.Pointer(1)}),
				expectSuccess("nsr-2-sys-single-pop-1"),
				expectNodegroups("nsr-2-sys-single-pop-1", "nodegroup2", "nodegroup4"),
				expectReleaseEvents(),

				// check unassigned resources consistent
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(2),
						ParallelCompileCount:  common.Pointer(1),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(0),
					},
					"system/system-0", "system/system-1", "nodegroup/nodegroup0", "nodegroup/nodegroup1", "nodegroup/nodegroup3", "node/coordinator-0",
				),

				deleteNSR("nsr-2-sys-single-pop-1"),
				expectReleaseEvents("system/system-2", "system/system-3", "nodegroup/nodegroup2", "nodegroup/nodegroup4", "node/coordinator-1"),
				expectAllResourcesUnassigned(),

				// able to select large mem by name
				createOrUpdateNSR("nsr-lrg-mem-by-name", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugAppendMode,
					SystemNames:         []string{"system-0"},
					NodegroupNames:      []string{"nodegroup0"},
					ManagementNodeNames: []string{"coordinator-0"},
				}),
				expectSuccess("nsr-lrg-mem-by-name"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(3),
						ParallelCompileCount:  common.Pointer(1),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(0),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-1", "system/system-2", "system/system-3", "nodegroup/nodegroup1", "nodegroup/nodegroup2", "nodegroup/nodegroup3", "nodegroup/nodegroup4", "node/coordinator-1",
				),
			},
		},
		{
			name:       "unassigned resources cm consistent",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				// trigger initialization
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(0)}),
				expectSuccess("nsr0"),

				// check unassigned resources consistent
				expectAllResourcesUnassigned(),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					SystemCount:           common.Pointer(4),
					ParallelCompileCount:  common.Pointer(3),
					ParallelExecuteCount:  common.Pointer(2),
					LargeMemoryRackCount:  common.Pointer(1),
					XLargeMemoryRackCount: common.Pointer(0),
				}),
				expectSuccess("nsr0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(0),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(0),
						LargeMemoryRackCount:  common.Pointer(0),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"nodegroup/nodegroup2",
				),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					SystemCount:           common.Pointer(4),
					ParallelCompileCount:  common.Pointer(3),
					ParallelExecuteCount:  common.Pointer(2),
					LargeMemoryRackCount:  common.Pointer(0),
					XLargeMemoryRackCount: common.Pointer(1),
				}),
				expectSuccess("nsr0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(0),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(0),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(0),
					},
					"nodegroup/nodegroup0",
				),
				deleteNSR("nsr0"),

				expectAllResourcesUnassigned(),

				// update memory estimates, expect more parallel compiles/executes
				func(tr *testReconciler) {
					perCompileCrdMemEstimateMi /= 2
					perExecuteCrdMemEstimateMi /= 2
				},
				updateClusterCfg(smallClusterCollection()),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(4),
						ParallelCompileCount:  common.Pointer(6),
						ParallelExecuteCount:  common.Pointer(3),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"*",
				),
				func(tr *testReconciler) {
					perCompileCrdMemEstimateMi *= 2
					perExecuteCrdMemEstimateMi *= 2
				},

				// changing the cluster config should cause the unassigned resources CM to be updated
				updateClusterCfg(redundantSysCluster()),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(4),
						ParallelCompileCount:  common.Pointer(1),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(0),
						XLargeMemoryRackCount: common.Pointer(0),
					},
					"system/system-0", "system/system-1", "system/system-2", "system/system-3",
					"nodegroup/nodegroup0", "nodegroup/nodegroup1", "node/coordinator-0",
				),
			},
		},
		{
			name:       "resource in-use release scenarios",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				// shrink does not release in-use resources
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					SystemCount:          common.Pointer(2),
					ParallelExecuteCount: common.Pointer(2),
					ParallelCompileCount: common.Pointer(compilesPerMgmtNode)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1"),
				expectNodes("nsr0", "coordinator-0", "coordinator-1"),

				setInUseResources("system/system-1", "nodegroup/nodegroup0", "node/coordinator-1"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(1), ParallelExecuteCount: common.Pointer(1)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-1"),
				expectNodegroups("nsr0", "nodegroup0"),
				expectNodes("nsr0", "coordinator-1"),

				// add does not add resources assigned to another ns
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-0", "system-2", "system-3"),
				expectNodegroups("nsr1", "nodegroup1", "nodegroup3", "nodegroup4"),
				expectNodes("nsr1", "coordinator-0"),

				// cannot expand further - retains old resources
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{SystemCount: common.Pointer(4)}),
				expectFailWithMsg("nsr1", "cluster only has 3 available of 4 total systems but 4 requested"),
				expectSystems("nsr1", "system-0", "system-2", "system-3"),
				expectNodegroups("nsr1", "nodegroup1", "nodegroup3", "nodegroup4"),
				expectNodes("nsr1", "coordinator-0"),
			},
		},
		{
			name:       "health priority",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				setResourceHealth("error", "system/system-0", "system/system-1", "node/coordinator-0"),

				// assign avoids unhealthy when possible
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-2", "system-3"),
				expectNodes("nsr0", "coordinator-1"),

				// uses nodes when no choice
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(4), ParallelCompileCount: common.Pointer(compilesPerMgmtNode)}),
				expectSuccess("nsr0"),
			},
		},
		{
			name:       "v2 group priority",
			collection: v2SmallClusterCollection(),
			actionSequence: []reconcilerAction{
				// prefer health + group + health > group when adding resources
				setResourceHealth("error", "system/system-1"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-2", "system-3", "system-4"),

				// prefer keep group ones when reduce resources
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-2", "system-3"),

				// take account of existing in use ones for group count
				setResourceHealth("ok", "system/system-1"),
				setInUseResources("system/system-2", "system/system-3"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-1", "system-2", "system-3"),
			},
		},
		{
			name:                       "ax session mgmt - por",
			collection:                 v2LargeClusterCollection(),
			useAxScheduling:            true,
			mgmtSeparationCheckEnabled: true,
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1", "system-2"),
				// stamp-aware AX picking
				expectNodes("nsr0", "ax-0", "ax-1", "ax-2", "coordinator-0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(5),
						ParallelCompileCount:  common.Pointer(1),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-3", "system/system-4", "system/system-5", "system/system-6", "system/system-7",
					"nodegroup/nodegroup0", "nodegroup/nodegroup2", "nodegroup/nodegroup5", "nodegroup/nodegroup6", "nodegroup/nodegroup7", "nodegroup/nodegroup8",
					"node/coordinator-1", // no showing on dedicated management nodes
					"node/ax-3", "node/ax-4", "node/ax-5", "node/ax-6", "node/ax-7",
				),

				// scale down would prefer systems in sorted order
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1"),
				// stamp-aware AX picking remains
				expectNodes("nsr0", "ax-0", "ax-1", "coordinator-0"),

				// allow unhealthy AX node
				setResourceHealth("error", "node/ax-0"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1"),
				expectNodes("nsr0", "ax-0", "ax-1", "coordinator-0"),

				// unhealthy AX node stays when growing the session due to no other healthy nodes in the stamp
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(4)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1", "system-2", "system-3"),
				expectNodes("nsr0", "ax-0", "ax-1", "ax-2", "ax-3", "coordinator-0"),

				// setting a system in the stamp to error and refresh the session
				setResourceHealth("error", "system/system-1"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(4)}),
				expectSuccess("nsr0"),
				// 4 systems cross 3 stamps, which is a known limitation given CSX picking isn't stamp-aware yet
				expectSystems("nsr0", "system-0", "system-2", "system-3", "system-4"),
				// with a system removed, we also free up the unhealthy node from the same stamp
				expectNodes("nsr0", "ax-1", "ax-2", "ax-3", "ax-4", "coordinator-0"),

				// refuse to release in-use resources, test ax alone without a system
				setInUseResources("node/ax-1", "node/ax-4"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0", "update was forced to select 1 in-use activation node(s) in stamp "+
					"stamp-2 which exceeds requested count 0"),

				// shrink the namespace while jobs are running
				setInUseResources("system/system-0", "system/system-4"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-4"),
				// AX nodes in the same stamp will stay
				expectNodes("nsr0", "ax-1", "ax-4", "coordinator-0"),

				// grow the namespace while jobs are running
				setInUseResources("system/system-4", "node/ax-4"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0"),
				// system-4 stays due to in-use, system-1 excluded due to unhealthy, system-2 is the next available system
				expectSystems("nsr0", "system-0", "system-2", "system-4"),
				// ax-4 stays due to in-use, ax-0 excluded due to unhealthy, ax-2 is the selected in the same timestamp as system-2
				expectNodes("nsr0", "ax-1", "ax-2", "ax-4", "coordinator-0"),

				// AX in a stamp goes bad, refreshing the session with same spec should pick the healthy alternative nodes
				setResourceHealth("error", "node/ax-2"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-2", "system-4"),
				// ax-2 replaced by ax-3
				expectNodes("nsr0", "ax-1", "ax-3", "ax-4", "coordinator-0"),

				// if all nodes in the stamp go bad, existing node wins
				setResourceHealth("error", "node/ax-3"),
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-2", "system-4"),
				// ax-3 replaced by ax-2 again
				expectNodes("nsr0", "ax-1", "ax-3", "ax-4", "coordinator-0"),

				// create another session
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr1"),
				// first select system-6 and system-7, because it's a stamp of highest capability at this point
				expectSystems("nsr1", "system-6", "system-7", "system-3"),
				// ax nodes matching stamps of systems
				expectNodes("nsr1", "ax-6", "ax-7", "ax-2", "coordinator-1"),

				// attempt to grow session by introducing more dedicated coordinator nodes but failed due to no more resources
				// dedicated management nodes should not be used
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{SystemCount: common.Pointer(3), ParallelCompileCount: common.Pointer(2)}),
				expectFailWithMsg("nsr1", "requested a total of 80Mi coordinator memory capacity but could only allocate 64Mi from 1 available nodes"),
				expectSystems("nsr1", "system-6", "system-7", "system-3"),
				expectNodes("nsr1", "ax-6", "ax-7", "ax-2", "coordinator-1"),

				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:  namespacev1.DebugSetMode,
					SystemNames: []string{"system-6", "system-7", "system-5"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-6", "system-7", "system-5"),
				// system-7,system-7 and ax-6,ax-7 are in the same stamp
				// system-5 and ax-3 are in different stamps
				// it is up to the users to get the stamp alignment right
				expectNodes("nsr1", "ax-6", "ax-7", "ax-2", "coordinator-1"),

				// debug-set can still auto-select the ax nodes if users explicitly want it
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:                namespacev1.DebugSetMode,
					SystemNames:               []string{"system-6", "system-7", "system-5"},
					AutoSelectActivationNodes: true,
				}),
				expectSuccess("nsr1"),
				// ax-3 gets kicked out and ax-5 moves in
				expectNodes("nsr1", "ax-6", "ax-7", "ax-5", "coordinator-1"),

				// deterministic auto-select
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:                namespacev1.DebugSetMode,
					SystemNames:               []string{"system-6", "system-7", "system-5"},
					AutoSelectActivationNodes: true,
				}),
				expectSuccess("nsr1"),
				expectNodes("nsr1", "ax-6", "ax-7", "ax-5", "coordinator-1"),

				// shrink a session using debug-set
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-6"},
					ActivationNodeNames: []string{"ax-6"},
				}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-6"),
				expectNodes("nsr1", "ax-6", "coordinator-1"),

				// update with the same spec again
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-6"},
					ActivationNodeNames: []string{"ax-6"},
				}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-6"),
				expectNodes("nsr1", "ax-6", "coordinator-1"),

				// invalid set with a dedicated management node
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugSetMode, ManagementNodeNames: []string{"management-0"}}),
				expectFailWithMsg("nsr1", "node 'management-0' is a dedicated management node and cannot be assigned"),
				expectSystems("nsr1", "system-6"),
				expectNodes("nsr1", "ax-6", "coordinator-1"),

				// grow a session using debug-append with auto-select ax nodes
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:                namespacev1.DebugAppendMode,
					AutoSelectActivationNodes: true,
					SystemNames:               []string{"system-7", "system-5"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-6", "system-7", "system-5"),
				expectNodes("nsr1", "ax-6", "ax-7", "ax-5", "coordinator-1"),

				// shrink a session using debug-remove with auto-select ax nodes
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:                namespacev1.DebugRemoveMode,
					AutoSelectActivationNodes: true,
					SystemNames:               []string{"system-7", "system-5"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-6"),
				expectNodes("nsr1", "ax-6", "coordinator-1"),

				// grow a session using debug-append
				// debug-append will not auto-select ax nodes in corresponding stamps
				// number of systems can be more than number of ax nodes
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugAppendMode, SystemNames: []string{"system-7"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-6", "system-7"),
				expectNodes("nsr1", "ax-6", "coordinator-1"),

				// allow debug-append resources already assigned
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugAppendMode, SystemNames: []string{"system-7"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-6", "system-7"),

				// invalid session growth by introducing a dedicated management node
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugAppendMode, CoordinatorNodeNames: []string{"management-0"}}),
				expectFailWithMsg("nsr1", "node 'management-0' is a dedicated management node and cannot be assigned"),
				expectSystems("nsr1", "system-6", "system-7"),
				expectNodes("nsr1", "ax-6", "coordinator-1"),

				// shrink a session using debug-remove
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugRemoveMode, SystemNames: []string{"system-6"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-7"),
				expectNodes("nsr1", "ax-6", "coordinator-1"),

				// allow debug-remove resources not yet assigned
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugRemoveMode, SystemNames: []string{"system-x"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-7"),
				expectNodes("nsr1", "ax-6", "coordinator-1"),

				// number of systems can be less than number of ax nodes
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugAppendMode, ActivationNodeNames: []string{"ax-7"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-7"),
				expectNodes("nsr1", "ax-6", "ax-7", "coordinator-1"),

				// remove-all
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveAllMode}),
				expectSuccess("nsr1"),
				expectSystems("nsr1"),
				expectNodes("nsr1"),

				// remove-all
				setInUseResources("node/ax-4"), // just a reminder that resources are still in use
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveAllMode}),
				expectFailWithMsg("nsr0", "node ax-4 has jobs running and cannot be released"),

				setInUseResources("system/system-4"), // same check for systems
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveAllMode}),
				expectFailWithMsg("nsr0", "system-4 has jobs running and cannot be released"),

				setInUseResources(""), // nothing is in-use now
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveAllMode}),
				expectSuccess("nsr0"),
				expectSystems("nsr0"),
				expectNodes("nsr0"),

				// Selecting all systems from stamp 0, 1, 2 and all ax nodes from stamp 2, 3, 4
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-0", "system-1", "system-2", "system-3", "system-4", "system-5"},
					ActivationNodeNames: []string{"ax-2", "ax-3", "ax-4", "ax-5", "ax-6", "ax-7"},
				}),
				expectSuccess("nsr1"),

				// Leaving systems in stamp 3 and ax in stamp 0 available
				// Capability mode will fail in this case
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0", "requested a total of 1 activation node(s) in stamp stamp-3 "+
					"but could only allocate 0 available node(s)"),

				// Resource mode will fail as well if users want ax nodes to be auto selected
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:                namespacev1.DebugSetMode,
					SystemNames:               []string{"system-6"},
					AutoSelectActivationNodes: true,
				}),
				expectFailWithMsg("nsr0", "requested a total of 1 activation node(s) in stamp stamp-3 "+
					"but could only allocate 0 available node(s)"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveAllMode}),
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveAllMode}),
			},
		},
		{
			name:            "ax session mgmt - non-por - multi-stamp",
			collection:      v2NonPorClusterCollection(2, 0, 2, 3),
			useAxScheduling: true,
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1", "system-2"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup3", "nodegroup4"),
				// auto stamp-aware AX picking only in capability mode
				expectNodes("nsr0", "ax-0", "ax-1", "ax-2", "ax-3", "ax-4", "ax-5", "coordinator-0"),

				// shrink a session using debug-set by choosing a system in a different stamp
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-4"},
					ActivationNodeNames: []string{"ax-8", "ax-9"},
				}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-4"),
				expectNodegroups("nsr0", "nodegroup1", "nodegroup3", "nodegroup4"),
				expectNodes("nsr0", "ax-8", "ax-9", "coordinator-0"),

				// create another session
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{SystemCount: common.Pointer(1)}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-0"),
				expectNodegroups("nsr1", "nodegroup0"),
				// auto stamp-aware AX picking only in capability mode
				expectNodes("nsr1", "ax-0", "ax-1", "coordinator-1"),

				// choose a system in the same stamp as nsr0
				// also choose a xlarge memory nodegroup in nsr1
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-5"},
					NodegroupNames:      []string{"nodegroup2"},
					ActivationNodeNames: []string{"ax-10", "ax-11"},
				}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-5"),
				expectNodegroups("nsr1", "nodegroup2"),
				expectNodes("nsr1", "ax-10", "ax-11", "coordinator-1"),

				// update with the same spec again
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-5"},
					NodegroupNames:      []string{"nodegroup2"},
					ActivationNodeNames: []string{"ax-10", "ax-11"},
				}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-5"),
				expectNodegroups("nsr1", "nodegroup2"),
				expectNodes("nsr1", "ax-10", "ax-11", "coordinator-1"),

				// set, no-change, remove in one debug-set
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-0"},
					NodegroupNames:      []string{},
					ManagementNodeNames: []string{""},
					ActivationNodeNames: []string{""},
				}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-0"),
				expectNodegroups("nsr1", "nodegroup2"),
				expectNodes("nsr1"),

				// scale down nsr through dryrun
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:            namespacev1.SetResourceSpecDryRunMode,
					SystemCount:           common.Pointer(0),
					ParallelCompileCount:  common.Pointer(0),
					ParallelExecuteCount:  common.Pointer(0),
					LargeMemoryRackCount:  common.Pointer(0),
					XLargeMemoryRackCount: common.Pointer(0),
				}),
				expectSuccess("nsr0"),
				expectDryrunResponse("nsr0", namespacev1.RequestParams{
					UpdateMode:            namespacev1.SetResourceSpecDryRunMode,
					SystemCount:           common.Pointer(0),
					ParallelCompileCount:  common.Pointer(0),
					ParallelExecuteCount:  common.Pointer(0),
					LargeMemoryRackCount:  common.Pointer(0),
					XLargeMemoryRackCount: common.Pointer(0),
				}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{""},
					NodegroupNames:      []string{""},
					ManagementNodeNames: []string{""},
					ActivationNodeNames: []string{""},
				}),
				expectSuccess("nsr0"),
				expectSystems("nsr0"),
				expectNodegroups("nsr0"),
				expectNodes("nsr0"),

				// scale down the nsr through default mode
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					SystemCount: common.Pointer(0),
					UpdateMode:  namespacev1.ModeNotSet,
				}),
				expectSuccess("nsr1"),
				expectSystems("nsr1"),
				expectNodegroups("nsr1"),
				expectNodes("nsr1"),
			},
		},
		{
			name:            "ax session mgmt - non-por - no-stamp",
			collection:      v2NonPorClusterCollection(2, 0, 6, 0),
			useAxScheduling: true,
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-0", "system-1", "system-2"),
				expectNodes("nsr0", "ax-0", "ax-1", "ax-2", "ax-3", "ax-4", "ax-5", "coordinator-0"),

				// shrink a session using debug-set by choosing a system
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:          namespacev1.DebugSetMode,
					SystemNames:         []string{"system-4"},
					ActivationNodeNames: []string{"ax-0", "ax-1"},
				}),
				expectSuccess("nsr0"),
				expectSystems("nsr0", "system-4"),
				expectNodes("nsr0", "ax-0", "ax-1", "coordinator-0"),

				// create another session
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{SystemCount: common.Pointer(1)}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-0"),
				expectNodes("nsr1", "ax-2", "ax-3", "coordinator-1"),

				// choose another system
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugSetMode, SystemNames: []string{"system-5"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-5"),
				expectNodes("nsr1", "ax-2", "ax-3", "coordinator-1"),

				// update with the same spec again
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					UpdateMode: namespacev1.DebugSetMode, SystemNames: []string{"system-5"}}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-5"),
				expectNodes("nsr1", "ax-2", "ax-3", "coordinator-1"),
			},
		},
		{
			name:            "ax session mgmt - test workload type",
			collection:      v2NonPorClusterCollection(2, 0, 6, 2),
			useAxScheduling: true,
			actionSequence: []reconcilerAction{
				// '-inference-test' suffix adds workload type in createOrUpdateNSR()
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount: common.Pointer(2),
				}),
				expectFailWithMsg("nsr0-inference-test", "Please consider adding AX node(s) to cover all ports"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount:            common.Pointer(2),
					SuppressAffinityErrors: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-1"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "ax-2", "ax-6", "coordinator-0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount: common.Pointer(10),
						// (DefaultMgtNodeMem - 3 * perExecuteCrdMemEstimateMi) / perCompileCrdMemEstimateMi = (64 - 48) / 32 = 0
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(3), // 3 unassigned popNG
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-2", "system/system-3", "system/system-4", "system/system-5", "system/system-6",
					"system/system-7", "system/system-8", "system/system-9", "system/system-10", "system/system-11",
					"nodegroup/nodegroup0", "nodegroup/nodegroup1", "nodegroup/nodegroup2", "nodegroup/nodegroup3", "nodegroup/nodegroup4",
					"nodegroup/nodegroup5", "nodegroup/nodegroup6", "nodegroup/nodegroup7", "nodegroup/nodegroup8",
					"node/coordinator-1", // no showing on dedicated management nodes
					"node/ax-3", "node/ax-4", "node/ax-5", "node/ax-7", "node/ax-8", "node/ax-9",
					"node/ax-10", "node/ax-11", "node/ax-12", "node/ax-13", "node/ax-14", "node/ax-15", "node/ax-16",
					"node/ax-17", "node/ax-18", "node/ax-19", "node/ax-20", "node/ax-21", "node/ax-22", "node/ax-23",
				),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{SystemCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0-inference-test", "Please consider adding AX node(s) to cover all ports"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount:            common.Pointer(1),
					SuppressAffinityErrors: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0"),
				// stamp-aware AX picking remains
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "coordinator-0"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-1", "system-2"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "ax-2", "ax-6", "ax-7", "ax-8", "coordinator-0"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{SystemCount: common.Pointer(0)}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test"),
				expectNodes("nsr0-inference-test"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					UpdateMode:               namespacev1.DebugAppendMode,
					InferenceDriverNodeNames: []string{"ix-0"},
					SuppressAffinityErrors:   true,
				}),
			},
		},
		{
			name:               "ax session mgmt - test workload type, inference-only cluster",
			collection:         v2InferenceClusterCollection(2, 0, 6, 2),
			useAxScheduling:    true,
			isInferenceCluster: true, // hack for wsapisv1.IsInferenceCluster for v2InferenceClusterCollection
			actionSequence: []reconcilerAction{
				// '-inference-test' suffix adds workload type in createOrUpdateNSR()
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount: common.Pointer(2),
				}),
				expectFailWithMsg("nsr0-inference-test", "Please consider adding AX node(s) to cover all ports"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount:            common.Pointer(2),
					SuppressAffinityErrors: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-1"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "ax-2", "ax-6", "coordinator-0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(10),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(4), // DefaultMgtNodeMem / perExecuteCrdMemEstimateMi = 4
						LargeMemoryRackCount:  common.Pointer(0),
						XLargeMemoryRackCount: common.Pointer(0),
					},
					"system/system-2", "system/system-3", "system/system-4", "system/system-5", "system/system-6",
					"system/system-7", "system/system-8", "system/system-9", "system/system-10", "system/system-11",
					"node/coordinator-1", // no showing on dedicated management nodes
					"node/ax-3", "node/ax-4", "node/ax-5", "node/ax-7", "node/ax-8", "node/ax-9",
					"node/ax-10", "node/ax-11", "node/ax-12", "node/ax-13", "node/ax-14", "node/ax-15", "node/ax-16",
					"node/ax-17", "node/ax-18", "node/ax-19", "node/ax-20", "node/ax-21", "node/ax-22", "node/ax-23",
				),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{SystemCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0-inference-test", "Please consider adding AX node(s) to cover all ports"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount:            common.Pointer(1),
					SuppressAffinityErrors: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0"),
				// stamp-aware AX picking remains
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "coordinator-0"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{SystemCount: common.Pointer(3)}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-1", "system-2"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "ax-2", "ax-6", "ax-7", "ax-8", "coordinator-0"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{SystemCount: common.Pointer(0)}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test"),
				expectNodes("nsr0-inference-test"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					UpdateMode:               namespacev1.DebugAppendMode,
					InferenceDriverNodeNames: []string{"ix-0"},
					SuppressAffinityErrors:   true,
				}),

				// when cluster is inference-only, even when workload type is not specified,
				// still should not assign nodegroup
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					SystemCount: common.Pointer(2),
				}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-0", "system-1"),
				expectNodes("nsr1", "ax-0", "ax-1", "ax-2", "ax-6", "coordinator-0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(10),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(4),
						LargeMemoryRackCount:  common.Pointer(0),
						XLargeMemoryRackCount: common.Pointer(0),
					},
					"system/system-2", "system/system-3", "system/system-4", "system/system-5", "system/system-6",
					"system/system-7", "system/system-8", "system/system-9", "system/system-10", "system/system-11",
					"node/coordinator-1", // no showing on dedicated management nodes
					"node/ax-3", "node/ax-4", "node/ax-5", "node/ax-7", "node/ax-8", "node/ax-9",
					"node/ax-10", "node/ax-11", "node/ax-12", "node/ax-13", "node/ax-14", "node/ax-15", "node/ax-16",
					"node/ax-17", "node/ax-18", "node/ax-19", "node/ax-20", "node/ax-21", "node/ax-22", "node/ax-23",
				),
			},
		},
		{
			name:            "ix session mgmt",
			collection:      v2NonPorClusterCollection(2, 2, 6, 2),
			useAxScheduling: true,
			useIxScheduling: true,
			actionSequence: []reconcilerAction{
				// '-inference-test' suffix adds workload type in createOrUpdateNSR()
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount: common.Pointer(2),
				}),
				expectFailWithMsg("nsr0-inference-test", "Please consider adding AX node(s) to cover all ports"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount:            common.Pointer(2),
					SuppressAffinityErrors: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-1"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "ax-2", "ax-6", "coordinator-0", "ix-0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(10),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(3),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-2", "system/system-3", "system/system-4", "system/system-5", "system/system-6",
					"system/system-7", "system/system-8", "system/system-9", "system/system-10", "system/system-11",
					"nodegroup/nodegroup0", "nodegroup/nodegroup1", "nodegroup/nodegroup2", "nodegroup/nodegroup3", "nodegroup/nodegroup4",
					"nodegroup/nodegroup5", "nodegroup/nodegroup6", "nodegroup/nodegroup7", "nodegroup/nodegroup8",
					"node/coordinator-1", // no showing on dedicated management nodes
					"node/ax-3", "node/ax-4", "node/ax-5", "node/ax-7", "node/ax-8", "node/ax-9",
					"node/ax-10", "node/ax-11", "node/ax-12", "node/ax-13", "node/ax-14", "node/ax-15", "node/ax-16",
					"node/ax-17", "node/ax-18", "node/ax-19", "node/ax-20", "node/ax-21", "node/ax-22", "node/ax-23",
					"node/ix-1", "node/ix-2", "node/ix-3",
				),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{SystemCount: common.Pointer(1)}),
				expectFailWithMsg("nsr0-inference-test", "Please consider adding AX node(s) to cover all ports"),

				// scale down stamp 0 first
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount:            common.Pointer(1),
					SuppressAffinityErrors: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "coordinator-0", "ix-0"),

				setInUseResources("node/ix-0"),
				// scale up stamp 1, however ix-0 is in use, so stay chosen
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					UpdateMode:                     namespacev1.DebugAppendMode,
					SystemNames:                    []string{"system-6", "system-7"},
					SuppressAffinityErrors:         true,
					AutoSelectInferenceDriverNodes: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-6", "system-7"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "coordinator-0", "ix-0"),

				setInUseResources(),
				// now IX should be in the stamp containing most systems
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					UpdateMode:                     namespacev1.DebugAppendMode,
					SystemNames:                    []string{"system-6", "system-7"},
					SuppressAffinityErrors:         true,
					AutoSelectInferenceDriverNodes: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-6", "system-7"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "coordinator-0", "ix-2"),

				// systems are all moved back to stamp 0, so IX should do the same
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount:            common.Pointer(3),
					SuppressAffinityErrors: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-1", "system-2"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "ax-2", "ax-6", "ax-7", "ax-8", "coordinator-0", "ix-0"),

				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{SystemCount: common.Pointer(0)}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test"),
				expectNodes("nsr0-inference-test"),

				// session without workload type should not have IX
				createOrUpdateNSR("nsr1", namespacev1.RequestParams{SystemCount: common.Pointer(2)}),
				expectSuccess("nsr1"),
				expectSystems("nsr1", "system-0", "system-1"),
				expectNodes("nsr1", "ax-0", "ax-1", "ax-2", "ax-6", "coordinator-0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(10),
						ParallelCompileCount:  common.Pointer(1),
						ParallelExecuteCount:  common.Pointer(2),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-2", "system/system-3", "system/system-4", "system/system-5", "system/system-6",
					"system/system-7", "system/system-8", "system/system-9", "system/system-10", "system/system-11",
					"nodegroup/nodegroup0", "nodegroup/nodegroup2", "nodegroup/nodegroup4", "nodegroup/nodegroup5",
					"nodegroup/nodegroup6", "nodegroup/nodegroup7", "nodegroup/nodegroup8",
					"node/coordinator-1",
					"node/ax-3", "node/ax-4", "node/ax-5", "node/ax-7", "node/ax-8", "node/ax-9",
					"node/ax-10", "node/ax-11", "node/ax-12", "node/ax-13", "node/ax-14", "node/ax-15", "node/ax-16",
					"node/ax-17", "node/ax-18", "node/ax-19", "node/ax-20", "node/ax-21", "node/ax-22", "node/ax-23",
					"node/ix-0", "node/ix-1", "node/ix-2", "node/ix-3",
				),
			},
		},
		{
			name:            "ix session mgmt - without enabling IX scheduling",
			collection:      v2NonPorClusterCollection(2, 2, 6, 2),
			useAxScheduling: true,
			useIxScheduling: false,
			actionSequence: []reconcilerAction{
				// '-inference-test' suffix adds workload type in createOrUpdateNSR()
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount: common.Pointer(2),
				}),
				expectFailWithMsg("nsr0-inference-test", "Please consider adding AX node(s) to cover all ports"),

				// even with inference type IX should not be scheduled
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					SystemCount:            common.Pointer(2),
					SuppressAffinityErrors: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-1"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "ax-2", "ax-6", "coordinator-0"),
				expectUnassignedResources(
					namespacev1.RequestParams{
						SystemCount:           common.Pointer(10),
						ParallelCompileCount:  common.Pointer(0),
						ParallelExecuteCount:  common.Pointer(3),
						LargeMemoryRackCount:  common.Pointer(1),
						XLargeMemoryRackCount: common.Pointer(1),
					},
					"system/system-2", "system/system-3", "system/system-4", "system/system-5", "system/system-6",
					"system/system-7", "system/system-8", "system/system-9", "system/system-10", "system/system-11",
					"nodegroup/nodegroup0", "nodegroup/nodegroup1", "nodegroup/nodegroup2", "nodegroup/nodegroup3", "nodegroup/nodegroup4",
					"nodegroup/nodegroup5", "nodegroup/nodegroup6", "nodegroup/nodegroup7", "nodegroup/nodegroup8",
					"node/coordinator-1", // no showing on dedicated management nodes
					"node/ax-3", "node/ax-4", "node/ax-5", "node/ax-7", "node/ax-8", "node/ax-9",
					"node/ax-10", "node/ax-11", "node/ax-12", "node/ax-13", "node/ax-14", "node/ax-15", "node/ax-16",
					"node/ax-17", "node/ax-18", "node/ax-19", "node/ax-20", "node/ax-21", "node/ax-22", "node/ax-23",
					"node/ix-0", "node/ix-1", "node/ix-2", "node/ix-3",
				),

				// debug-update with AutoSelectInferenceDriverNodes == true should not schedule IX node
				createOrUpdateNSR("nsr0-inference-test", namespacev1.RequestParams{
					UpdateMode:                     namespacev1.DebugAppendMode,
					SystemNames:                    []string{"system-6"},
					SuppressAffinityErrors:         true,
					AutoSelectInferenceDriverNodes: true,
				}),
				expectSuccess("nsr0-inference-test"),
				expectSystems("nsr0-inference-test", "system-0", "system-1", "system-6"),
				expectNodes("nsr0-inference-test", "ax-0", "ax-1", "ax-2", "ax-6", "coordinator-0"),
			},
		},
		{
			name:       "mgmt node only",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{SystemCount: common.Pointer(0), ParallelCompileCount: common.Pointer(1)}),
				expectSuccess("nsr0"),
				expectSystems("nsr0"),
				expectNodegroups("nsr0"),
				expectNodes("nsr0", "coordinator-0"),
			},
		},
		{
			name:       "system request selects all resources",
			collection: smallClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR(common.SystemNamespace, namespacev1.RequestParams{}),
				expectSuccess(common.SystemNamespace),
				expectSystems(common.SystemNamespace, "system-0", "system-1", "system-2", "system-3"),
				expectNodegroups(common.SystemNamespace, "nodegroup0", "nodegroup1", "nodegroup2", "nodegroup3", "nodegroup4"),
				expectNodes(common.SystemNamespace, "coordinator-0", "coordinator-1"),

				updateClusterCfg(redundantSysCluster()),
				func(reconciler *testReconciler) {
					// config update triggers a DebugRemove update since mgmt node was removed
					// reconcile once more to reconcile the DebugSet caused by the assign-all during config update
					_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: NSREvent + common.SystemNamespace}})
					require.NoError(reconciler.t, err)
				},
				expectSuccess(common.SystemNamespace),
				expectSystems(common.SystemNamespace, "system-0", "system-1", "system-2", "system-3"),
				expectNodegroups(common.SystemNamespace, "nodegroup0", "nodegroup1"),
				expectNodes(common.SystemNamespace, "coordinator-0"),
			},
		},
		{
			name:       "large cluster validation scenarios",
			collection: largeClusterCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					SystemCount:          common.Pointer(32),
					LargeMemoryRackCount: common.Pointer(2),
					ParallelCompileCount: common.Pointer(8),
					ParallelExecuteCount: common.Pointer(8),
				}),
				expectSuccess("nsr0"),

				createOrUpdateNSR("nsr1", namespacev1.RequestParams{
					SystemCount:          common.Pointer(32),
					LargeMemoryRackCount: common.Pointer(2),
					ParallelCompileCount: common.Pointer(8),
					ParallelExecuteCount: common.Pointer(8),
				}),
				expectSuccess("nsr1"),
				deleteNSR("nsr0"),

				createOrUpdateNSR("nsr2", namespacev1.RequestParams{
					SystemCount:          common.Pointer(32),
					LargeMemoryRackCount: common.Pointer(3),
					ParallelCompileCount: common.Pointer(8),
					ParallelExecuteCount: common.Pointer(8),
				}),
				expectFailWithMsg("nsr2", "cluster only has 2 available of 4 total large memory racks but 3 requested"),

				deleteNSR("nsr1"),

				createOrUpdateNSR("nsr2", namespacev1.RequestParams{
					SystemCount:          common.Pointer(32),
					LargeMemoryRackCount: common.Pointer(3),
					ParallelCompileCount: common.Pointer(8),
					ParallelExecuteCount: common.Pointer(8),
				}),
				expectSuccess("nsr2"),
			},
		},
		{
			name:                       "mgmt coord separation - coordinator node changes scenarios - user namespace",
			collection:                 multiMgmtCoordSeparationCollection(),
			mgmtSeparationCheckEnabled: true,
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					SystemCount:          common.Pointer(1),
					LargeMemoryRackCount: common.Pointer(1),
					ParallelCompileCount: common.Pointer(1),
					ParallelExecuteCount: common.Pointer(1),
				}),
				expectSuccess("nsr0"),
				createDeployment("cluster-server", "nsr0", "true"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.DebugRemoveMode,
					CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "false"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "true"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "false"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode, CoordinatorNodeNames: []string{"coordinator-0", "coordinator-1"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "true"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "true"}),
			},
		},
		{
			name:       "no mgmt coord separation - coordinator node changes scenarios - user namespace",
			collection: multiMgmtNoCoordSeparationCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					SystemCount:          common.Pointer(1),
					LargeMemoryRackCount: common.Pointer(1),
					ParallelCompileCount: common.Pointer(1),
					ParallelExecuteCount: common.Pointer(1),
				}),
				expectSuccess("nsr0"),
				createDeployment("cluster-server", "nsr0", "false"),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.DebugRemoveMode,
					CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "false"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "true"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "false"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode, CoordinatorNodeNames: []string{"coordinator-0", "coordinator-1"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "true"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "true"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-1"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "false"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode, CoordinatorNodeNames: []string{"coordinator-0", "coordinator-1"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "true"}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: "true"}),
			},
		},
		{
			name:                       "mgmt coord separation - coordinator node changes scenarios - system namespace",
			collection:                 multiMgmtCoordSeparationCollection(),
			mgmtSeparationCheckEnabled: true,
			actionSequence: []reconcilerAction{
				createOrUpdateSystemNSR("job-operator", namespacev1.RequestParams{
					SystemCount:          common.Pointer(1),
					LargeMemoryRackCount: common.Pointer(1),
					ParallelCompileCount: common.Pointer(1),
					ParallelExecuteCount: common.Pointer(1),
				}),
				expectSuccess("job-operator"),
				createDeployment("cluster-server", "job-operator", ""),

				createOrUpdateSystemNSR("job-operator", namespacev1.RequestParams{
					UpdateMode:           namespacev1.DebugRemoveMode,
					CoordinatorNodeNames: []string{"coordinator-0", "coordinator-1", "coordinator-2", "coordinator-3"}}),
				expectSuccess("job-operator"),
				expectDeploymentPodAnnotation("cluster-server", "job-operator", map[string]string{ContainsCrdNodesAnno: ""}),

				createOrUpdateSystemNSR("job-operator", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("job-operator"),
				expectDeploymentPodAnnotation("cluster-server", "job-operator", map[string]string{ContainsCrdNodesAnno: ""}),
			},
		},
		{
			name:       "no mgmt coord separation - coordinator node changes scenarios - system namespace",
			collection: multiMgmtNoCoordSeparationCollection(),
			actionSequence: []reconcilerAction{
				createOrUpdateSystemNSR("job-operator", namespacev1.RequestParams{
					SystemCount:          common.Pointer(1),
					LargeMemoryRackCount: common.Pointer(1),
					ParallelCompileCount: common.Pointer(1),
					ParallelExecuteCount: common.Pointer(1),
				}),
				expectSuccess("job-operator"),
				createDeployment("cluster-server", "job-operator", ""),

				createOrUpdateSystemNSR("job-operator", namespacev1.RequestParams{
					UpdateMode:           namespacev1.DebugRemoveMode,
					CoordinatorNodeNames: []string{"coordinator-0", "coordinator-1", "coordinator-2", "coordinator-3"}}),
				expectSuccess("job-operator"),
				expectDeploymentPodAnnotation("cluster-server", "job-operator", map[string]string{ContainsCrdNodesAnno: ""}),

				createOrUpdateSystemNSR("job-operator", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("job-operator"),
				expectDeploymentPodAnnotation("cluster-server", "job-operator", map[string]string{ContainsCrdNodesAnno: ""}),
			},
		},
		{
			name:                       "mgmt coord separation - coordinator node changes scenarios - no previous annotation",
			collection:                 multiMgmtCoordSeparationCollection(),
			mgmtSeparationCheckEnabled: true,
			actionSequence: []reconcilerAction{
				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					SystemCount:          common.Pointer(1),
					LargeMemoryRackCount: common.Pointer(1),
					ParallelCompileCount: common.Pointer(1),
					ParallelExecuteCount: common.Pointer(1),
				}),
				expectSuccess("nsr0"),
				createDeployment("cluster-server", "nsr0", ""),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{
					UpdateMode:           namespacev1.DebugRemoveMode,
					CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: ""}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugAppendMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: ""}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: ""}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugSetMode, CoordinatorNodeNames: []string{"coordinator-0", "coordinator-1"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: ""}),

				createOrUpdateNSR("nsr0", namespacev1.RequestParams{UpdateMode: namespacev1.DebugRemoveMode, CoordinatorNodeNames: []string{"coordinator-0"}}),
				expectSuccess("nsr0"),
				expectDeploymentPodAnnotation("cluster-server", "nsr0", map[string]string{ContainsCrdNodesAnno: ""}),
			},
		},
	} {

		t.Run(tc.name, func(t *testing.T) {

			wsapisv1.UseAxScheduling = tc.useAxScheduling
			if tc.useAxScheduling {
				wsapisv1.UseAxScheduling = true

				numStamps := map[string]bool{}
				numAxNodes := 0
				for _, res := range tc.collection.List() {
					if strings.HasPrefix(res.Name(), "ax-") {
						numAxNodes++
						if val, ok := res.GetProps()[resource.StampPropertyKey]; ok {
							numStamps[val] = true
						}
					}
				}

				numSystems := 0
				for _, res := range tc.collection.List() {
					if strings.HasPrefix(res.Name(), "system-") {
						if _, ok := res.GetProps()[resource.StampPropertyKey]; len(numStamps) == 0 || ok {
							numSystems++
						}
					}
				}
				wsapisv1.AxCountPerSystem = numAxNodes / numSystems
				wsapisv1.SystemStampsDetected = len(numStamps) > 0
			}
			defer func() {
				wsapisv1.UseAxScheduling = false
				wsapisv1.AxCountPerSystem = 0
				wsapisv1.SystemStampsDetected = true
			}()

			if tc.useIxScheduling {
				wsapisv1.UseIxScheduling = true
			}
			defer func() {
				wsapisv1.UseIxScheduling = false
			}()

			if tc.mgmtSeparationCheckEnabled {
				wsapisv1.ManagementSeparationEnforced = true
			}
			defer func() { wsapisv1.ManagementSeparationEnforced = false }()

			if tc.isInferenceCluster {
				wsapisv1.IsInferenceCluster = true
			}
			defer func() { wsapisv1.IsInferenceCluster = false }()

			tr := newTestReconcilerForCollection(t, tc.collection)
			tr.resourceStatus = placeholderResourceStatus{}

			var systems []client.Object
			for _, r := range tr.collection.Filter(resource.NewSystemFilter()).List() {
				systems = append(systems, newSystem(r.Name(), ""))
			}
			tr.client = newClientBuilder().
				addSystems(systems...).
				addNSRs(newNSR("fake")).
				build()
			tr.Reconciler.client = tr.client

			for _, action := range tc.actionSequence {
				action(tr)
			}
		})
	}
}

type mockResourceStatus map[string]bool

func (m mockResourceStatus) IsInUse(resourceId string) bool { return m[resourceId] }

// workaround to add nsr GKV
func newNSR(name string) *namespacev1.NamespaceReservation {
	return &namespacev1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
}

func newSystem(name, ns string) *systemv1.System {
	labels := map[string]string{}
	if ns != "" {
		labels[common.NamespaceKey] = ns
	}
	return &systemv1.System{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: systemv1.SystemSpec{
			Type:     "cs2",
			CmAddr:   "~" + name + "~",
			MgmtAddr: "~" + name + "~",
		},
	}
}

type clientBuilder struct {
	objs map[string]client.Object
}

func newClientBuilder() *clientBuilder {
	return &clientBuilder{objs: map[string]client.Object{}}
}

func (o *clientBuilder) addSystems(obj ...client.Object) *clientBuilder {
	for i := range obj {
		o.objs["system/"+obj[i].GetName()] = obj[i]
	}
	return o
}

func (o *clientBuilder) addNSRs(obj ...client.Object) *clientBuilder {
	for i := range obj {
		o.objs["namespacereservation/"+obj[i].GetName()] = obj[i]
	}
	return o
}

func (o *clientBuilder) addNSs(obj ...client.Object) *clientBuilder {
	for i := range obj {
		o.objs["namespace/"+obj[i].GetName()] = obj[i]
	}
	return o
}

func (o *clientBuilder) removeSystem(name string) *clientBuilder {
	delete(o.objs, "system/"+name)
	return o
}

func (o *clientBuilder) build() client.Client {
	scheme := k8runtime.NewScheme()
	rtutil.Must(namespacev1.AddToScheme(scheme))
	rtutil.Must(systemv1.AddToScheme(scheme))
	rtutil.Must(corev1.AddToScheme(scheme))
	rtutil.Must(appsv1.AddToScheme(scheme))
	rtutil.Must(networkingv1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(common.Values(o.objs)...).
		WithStatusSubresource(common.Values(o.objs)...).
		Build()
}

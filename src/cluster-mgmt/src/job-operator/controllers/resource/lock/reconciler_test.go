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
package lock

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/strings/slices"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"

	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource/helpers"
	"cerebras.com/job-operator/common/resource_config"
)

func TestReconciler_validateIfNew(t *testing.T) {
	ctx := context.TODO()
	common.InitLogging()

	sysN := 4
	cluster := helpers.NewClusterBuilder(1).
		WithSystems(sysN, "").
		WithBR(12).
		WithPopulatedNodeGroups(sysN, wsapisv1.PopNgTypeRegular).
		Build()
	reconciler := newLockReconciler(cluster, &common.ClusterConfig{})

	for _, testcase := range []struct {
		lockBuilder     *LockBuilder
		clusterBuilder  *helpers.ClusterBuilder
		useAxScheduling bool
		errContains     string
	}{
		{
			lockBuilder: NewLockBuilder("bad-queue").WithSystemRequest(sysN).WithQueue("foo"),
			errContains: "requested queue 'foo' was not found",
		},
		{
			lockBuilder: NewLockBuilder("too-many-sys").WithQueue(rlv1.ExecuteQueueName).WithSystemRequest(sysN + 1),
			errContains: "requested 5 system but only 4 available with matching properties",
		},
		{
			lockBuilder: NewLockBuilder("too-many-groups").WithQueue(rlv1.ExecuteQueueName).WithPrimaryNodeGroup().WithSecondaryNodeGroup(sysN),
			errContains: "requested 5 nodegroups in total but only 4 available",
		},
		{
			lockBuilder: NewLockBuilder("too-few-0").WithQueue(rlv1.ExecuteQueueName).WithSystemRequest(1, "name:system-x"),
			errContains: "requested 1 system[name:system-x] but only 0 available",
		},
		{
			lockBuilder: NewLockBuilder("too-few-1").WithQueue(rlv1.ExecuteQueueName).WithSystemRequest(2, "name:system-1"),
			errContains: "requested 2 system[name:system-1] but only 1 available",
		},
		{
			lockBuilder: NewLockBuilder("too-few-2").WithQueue(rlv1.ExecuteQueueName).WithSystemRequest(4, "name:system-1,system-2,system-3"),
			errContains: "requested 4 system[name:system-1,system-2,system-3] but only 3 available",
		},
		{
			lockBuilder: NewLockBuilder("duplicate-names-0").WithSystemRequest(1).
				WithGroupRequest("ng-primary-0", 1).WithNodeRequest("node", 1).CpuMem(2, 2).BuildGroupRequest().
				WithGroupRequest("ng-primary-0", 1).WithNodeRequest("node", 1).CpuMem(2, 2).BuildGroupRequest(),
			errContains: "duplicate",
		},
		{
			lockBuilder: NewLockBuilder("duplicate-names-1").
				WithQueue(rlv1.ExecuteQueueName).
				WithGroupRequest("ng-primary-0", 1).
				WithNodeRequest("node", 1).CpuMem(1, 1).
				WithNodeRequest("node", 1).CpuMem(1, 1).
				BuildGroupRequest(),
			errContains: "duplicate",
		},
		{
			lockBuilder: NewLockBuilder("impossible-subresources").
				WithQueue(rlv1.ExecuteQueueName).
				WithCoordinatorRequest(helpers.DefaultMgtNodeCpu, helpers.DefaultMgtNodeMem+1, nil),
			errContains: "requested 1 node[role:coordinator]{cpu:32m, mem:65Mi} but 1 available with insufficient capacity {cpu:32m, mem:64Mi}",
		},
		{
			lockBuilder: NewLockBuilder("multiple-group-failures").
				WithQueue(rlv1.ExecuteQueueName).
				WithCoordinatorRequest(helpers.DefaultMgtNodeCpu, helpers.DefaultMgtNodeMem, nil).
				WithSystemRequest(1).
				WithGroupRequest("ng-primary", 1).
				WithNodeRequest("Activation", 1).Role("memory").CpuMem(helpers.DefaultMemNodeCpu, helpers.DefaultMemNodeMem).
				WithNodeRequest("Weight", helpers.DefaultMemNodePerSys).Role("memory").CpuMem(helpers.DefaultMemNodeCpu, helpers.DefaultMemNodeMem).
				BuildGroupRequest(),
			clusterBuilder: helpers.NewClusterBuilder(1).
				WithSystems(4, "").
				WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeRegular).
				WithDepopulatedNodeGroups(3),
			errContains: "group request ng-primary: 0 of 4 groups scheduled but required 1 (largest candidate group" +
				" failed with 10 nodes[role:memory]{cpu:8m, mem:32Mi} could only schedule 1 of 1 Activation{cpu:8m, mem:32Mi}, 9 of 10 Weight{cpu:8m, mem:32Mi}).",
		},
		{
			// both primary and secondary groups (total=4) require populated groups but only 3 populated groups exist
			// both requests match but do not have mutual matches
			lockBuilder: NewLockBuilder("impossible-nodegroup-search").
				WithQueue(rlv1.ExecuteQueueName).
				WithCoordinatorRequest(helpers.DefaultMgtNodeCpu, helpers.DefaultMgtNodeMem, nil).
				WithSystemRequest(4).
				WithGroupRequest("ng-primary", 1).
				WithNodeRequest("Weight", helpers.DefaultMemNodePerSys).Role("memory").CpuMem(helpers.DefaultMemNodeCpu, helpers.DefaultMemNodeMem).
				BuildGroupRequest().
				WithGroupRequest("ng-secondary", 3).
				WithNodeRequest("Activation", helpers.DefaultMemNodePerSys).Role("memory").CpuMem(helpers.DefaultMemNodeCpu, helpers.DefaultMemNodeMem).
				BuildGroupRequest(),
			clusterBuilder: helpers.NewClusterBuilder(1).
				WithSystems(4, "").
				WithBR(30).
				WithPopulatedNodeGroups(3, wsapisv1.PopNgTypeRegular).
				WithDepopulatedNodeGroups(1),
			errContains: "requested 4 nodegroups in total but only 3 match request",
		},
		{
			lockBuilder: NewLockBuilder("impossible-ax-scheduling").
				WithAxScheduling().
				WithQueue(rlv1.ExecuteQueueName).
				WithCoordinatorRequest(helpers.DefaultMgtNodeCpu, helpers.DefaultMgtNodeMem, nil).
				WithSystemRequest(4).
				WithGroupRequest(rlv1.ChiefRequestName, 1).
				WithNodeRequest("Chief", 1).Role("activation").CpuMem(helpers.DefaultAxNodeCpu*2, helpers.DefaultAxNodeMem*2).
				BuildGroupRequest().
				WithActToCsxDomainsMap([][]int{
					// 16 acts per csx
					{0}, {1}, {2}, {3}, {0}, {1}, {2}, {3}, {0}, {1}, {2}, {3}, {0}, {1}, {2}, {3},
				}).
				WithGroupRequest(rlv1.ActivationRequestName, 1).
				WithNodeRequest("Activation", 1).Role("activation").CpuMem(helpers.DefaultAxNodeCpu, helpers.DefaultAxNodeMem).
				BuildGroupRequest(),
			clusterBuilder: helpers.NewClusterBuilder(1).WithV2Network().
				// 3 CSX and 3 AX in one stamp
				// 1 CSX and 1 AX in another stamp
				WithSystems(3, "0").WithSystems(1, "1").
				WithAX(3, "0").WithAX(1, "1"),
			useAxScheduling: true,
			errContains: "chief group request 1 nodes[role:activation]{cpu:32m, mem:64Mi} could only schedule 0 of 1 " +
				"Chief{cpu:64m, mem:128Mi}, 3 nodes[role:activation]{cpu:32m, mem:64Mi} could only schedule 0 of 3 " +
				"Chief{cpu:64m, mem:128Mi}. activation group request 1 nodes[role:activation]{cpu:32m, mem:64Mi} could " +
				"only schedule 1 of 1 0{cpu:32m, cs-port:1, mem:64Mi}, 0 of 1 1{cpu:32m, cs-port:1, mem:64Mi}, " +
				"3 nodes[role:activation]{cpu:32m, mem:64Mi} could only schedule 1 of 1 16{cpu:32m, cs-port:1, " +
				"mem:64Mi}, 1 of 1 32{cpu:32m, cs-port:1, mem:64Mi}, 1 of 1 48{cpu:32m, cs-port:1, mem:64Mi}, " +
				"0 of 1 17{cpu:32m, cs-port:1, mem:64Mi}",
		},
		{
			lockBuilder: NewLockBuilder("impossible-multi-primary-ng-grant-1").
				WithAxScheduling().
				WithQueue(rlv1.ExecuteQueueName).
				WithSystemRequest(3).
				WithGroupRequest("ng-primary", 1).
				WithNodeRequest("Weight", helpers.DefaultMemNodePerSys*2).Role("memory").CpuMem(helpers.DefaultMemNodeCpu, helpers.DefaultMemNodeMem).
				BuildGroupRequest().
				WithGroupRequest("ng-secondary", 2).
				WithNodeRequest("Worker", helpers.DefaultWrkNodePerSys).Role("worker").CpuMem(helpers.DefaultWrkNodeCpu, helpers.DefaultWrkNodeMem).
				BuildGroupRequest(),
			clusterBuilder: helpers.NewClusterBuilder(1).WithV2Network().
				WithSystems(3, "0").
				WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeRegular).
				WithDepopulatedNodeGroups(2),
			useAxScheduling: true,
			// multi pop nodegroups grant check attempted but failed given not enough pop nodegroups
			// best progress made is partially granting the single pop group request
			errContains: "0 of 3 groups scheduled but required 1 (largest candidate group failed with 10 nodes[role:memory]{cpu:8m, mem:32Mi} " +
				"could only schedule 10 of 20 Weight{cpu:8m, mem:32Mi}",
		},
		{
			lockBuilder: NewLockBuilder("impossible-multi-primary-ng-grant-2").
				WithAxScheduling().
				WithQueue(rlv1.ExecuteQueueName).
				WithSystemRequest(3).
				WithGroupRequest("ng-primary", 1).
				WithNodeRequest("Weight", helpers.DefaultMemNodePerSys*2).Role("memory").CpuMem(helpers.DefaultMemNodeCpu, helpers.DefaultMemNodeMem).
				BuildGroupRequest().
				WithGroupRequest("ng-secondary", 2).
				WithNodeRequest("Worker", helpers.DefaultWrkNodePerSys).Role("worker").CpuMem(helpers.DefaultWrkNodeCpu, helpers.DefaultWrkNodeMem).
				BuildGroupRequest(),
			clusterBuilder: helpers.NewClusterBuilder(1).WithV2Network().
				WithSystems(3, "0").
				WithPopulatedNodeGroups(2, wsapisv1.PopNgTypeRegular).
				WithDepopulatedNodeGroups(1),
			useAxScheduling: true,
			// multi pop nodegroups grant check attempted but failed given not enough depop nodegroups
			// best progress made is partially granting the single pop group request
			errContains: "0 of 3 groups scheduled but required 1 (largest candidate group failed with 10 nodes[role:memory]{cpu:8m, mem:32Mi} " +
				"could only schedule 10 of 20 Weight{cpu:8m, mem:32Mi}",
		},
		{
			lockBuilder: NewSystemLockBuilder("ax-nodes-only-for-runtime-no-resource", 2, 0).
				WithAxScheduling().
				WithAxOnlyForRuntime().
				WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
				WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
				WithWgtCmdGroupRequest(helpers.DefaultMemNodePerSys-1, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8, 0, 0).
				WithAxOnlyPrimaryNodeGroup().
				WithSecondaryNodeGroup(1),
			clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
				WithSystems(2, "0").WithSystems(2, "1").
				WithAX(2, "0").WithAX(2, "1").
				WithBR(12).
				WithDepopulatedNodeGroups(4),
			useAxScheduling: true,
			errContains: "cluster lacks requested capacity: weight/command group request 2 nodes[role:activation]{cpu:32m, mem:64Mi} " +
				"could only schedule 6 of 9 Weight{cpu:4m, mem:8Mi}",
		},
	} {
		t.Run(testcase.lockBuilder.name, func(t *testing.T) {
			if testcase.useAxScheduling {
				wsapisv1.IsV2Network = true
				wsapisv1.SystemStampsDetected = true
				wsapisv1.UseAxScheduling = true
				wsapisv1.AxCountPerSystem = 1
				defer func() {
					wsapisv1.IsV2Network = false
					wsapisv1.SystemStampsDetected = false
					wsapisv1.UseAxScheduling = false
					wsapisv1.AxCountPerSystem = 1
				}()
			}
			rec := &fakeEventRecorder{}
			c := cluster
			cfg := &common.ClusterConfig{}
			if testcase.clusterBuilder != nil {
				c = testcase.clusterBuilder.Build()
				cfg = testcase.clusterBuilder.GetCfg()
			}
			r := newLockReconciler(c, cfg)
			rl := testcase.lockBuilder.BuildPending()
			r.client = newClientBuilder().add(rl).build()
			r.recorder = rec

			l, err := r.validateNewLock(ctx, rl, nil)

			require.NoError(t, err)
			require.Nil(t, l)
			assert.Equal(t, EventSchedulingFailed, rec.events[0].reason)
			assert.Contains(t, rec.events[0].message, testcase.errContains)
		})
	}

	t.Run("ok", func(t *testing.T) {
		rl := NewLockBuilder("ok").WithQueue(rlv1.ExecuteQueueName).WithSystemRequest(1).BuildPending()
		rec := &fakeEventRecorder{}
		reconciler.recorder = rec

		l, err := reconciler.validateNewLock(ctx, rl, nil)

		assert.NoError(t, err)
		assert.NotNil(t, l)
		assert.Len(t, rec.events, 0)
	})

	t.Run("idempotent retry on api error", func(t *testing.T) {
		rec := &fakeEventRecorder{}
		reconciler.recorder = rec
		rl := NewLockBuilder("retry-api-error").WithQueue(rlv1.ExecuteQueueName).
			WithSystemRequest(sysN).WithSkipBR(true).BuildPending()

		failApi := true
		thrownErr := errors.New("update api failed")
		reconciler.client = newClientBuilder().
			withInterceptorFuncs(&interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, client client.Client, subResoruceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if failApi {
						return thrownErr
					}
					return client.SubResource(subResoruceName).Update(ctx, obj, opts...)
				},
			}).
			add(rl).
			build()
		_, e := reconciler.Initialize(ctx)
		require.NoError(t, e)

		// first schedule failed at status update
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rl.Namespace, Name: rl.Name}}
		_, err := reconciler.Reconcile(ctx, req)
		assert.Equal(t, thrownErr, err)
		assert.NoError(t, reconciler.client.Get(ctx, client.ObjectKeyFromObject(rl), rl))
		assert.True(t, rl.IsPending())

		// retry works validates api error doesn't have any side effect on reservation
		failApi = false
		_, err = reconciler.Reconcile(ctx, req)
		assert.NoError(t, err)
		assert.NoError(t, reconciler.client.Get(ctx, client.ObjectKeyFromObject(rl), rl))
		assert.True(t, rl.IsGranted())
	})

	singleSysWorkerLock := func(workersPerNode float64, workerMemReq, workerMemUpperBound, limit int) *rlv1.ResourceLock {
		return NewLockBuilder("workerbound").
			WithQueue(rlv1.ExecuteQueueName).
			WithSystemRequest(1).
			WithGroupRequest("ng-primary-0", 1).
			WithNodeRequest("worker", int(helpers.DefaultWrkNodePerSys*workersPerNode)).
			Role("worker").
			CpuMem(helpers.DefaultWrkNodeCpu/4, workerMemReq).
			BuildGroupRequest().
			WithUpperBoundsAndLimitRequest("worker", workerMemUpperBound, limit).
			BuildPending()
	}

	singleSysActWgtLock := func(actPerNode float64, wgtPerNode float64,
		actMemReq, actMemUpperBound, wgtMemReq, wgtMemUpperBound, limit int) *rlv1.ResourceLock {
		gr := NewLockBuilder("workerbound").
			WithQueue(rlv1.ExecuteQueueName).
			WithSystemRequest(1).
			WithGroupRequest("ng-primary-0", 1)
		gr.WithNodeRequest("act", int(helpers.DefaultMemNodePerSys*actPerNode)).
			Role("memory").
			CpuMem(helpers.DefaultWrkNodeCpu/4, actMemReq)
		gr.WithNodeRequest("wgt", int(helpers.DefaultMemNodePerSys*wgtPerNode)).
			Role("memory").
			CpuMem(helpers.DefaultWrkNodeCpu/4, wgtMemReq)

		return gr.BuildGroupRequest().
			WithUpperBoundsAndLimitRequest("act", actMemUpperBound, limit).
			WithUpperBoundsAndLimitRequest("wgt", wgtMemUpperBound, limit).
			BuildPending()
	}

	fourSysWorkerLock := func(wPerNodeNg0, wPerNodeNg1 float64, workerMemReq, workerMemUpperBound, limit int) *rlv1.ResourceLock {
		return NewLockBuilder("workerbound").
			WithQueue(rlv1.ExecuteQueueName).
			WithSystemRequest(4).
			WithGroupRequest("ng-primary-0", 1).
			WithNodeRequest("worker", int(helpers.DefaultWrkNodePerSys*wPerNodeNg0)).
			Role("worker").
			CpuMem(helpers.DefaultWrkNodeCpu/4, workerMemReq).
			BuildGroupRequest().
			WithGroupRequest("ng-secondary", 3).
			WithNodeRequest("worker", int(helpers.DefaultWrkNodePerSys*wPerNodeNg1)).
			Role("worker").
			CpuMem(helpers.DefaultWrkNodeCpu/4, workerMemReq).
			BuildGroupRequest().
			WithUpperBoundsAndLimitRequest("worker", workerMemUpperBound, limit).
			BuildPending()
	}
	// test with ratio of 20%
	wsapisv1.RtNodeMaxLimitReservedMemGB = 0
	wsapisv1.RtNodeMaxLimitReservedMemRatio = 0.2
	defer func() {
		wsapisv1.RtNodeMaxLimitReservedMemGB = 10
		wsapisv1.RtNodeMaxLimitReservedMemRatio = 0.05
	}()

	t.Run("upperBounds + override", func(t *testing.T) {
		for _, testcase := range []struct {
			name           string
			clusterBuilder *helpers.ClusterBuilder
			l              *rlv1.ResourceLock

			expectedUpperBound int
			// indices represent the mem override value and the values represent how many times such override were seen
			expectedMemOverrides map[int]int
			expectedBrNicCount   int
			expectedActNicCount  int
		}{
			{
				name:               "1 worker per node allocs whole node upper bound exact",
				l:                  singleSysWorkerLock(1, helpers.DefaultWrkNodeMem, helpers.DefaultWrkNodeMem, 0),
				expectedUpperBound: helpers.DefaultWrkNodeMem,
			},
			{
				name:               "1 worker per node allocs whole node upper bound overflows",
				l:                  singleSysWorkerLock(1, helpers.DefaultWrkNodeMem/2, helpers.DefaultWrkNodeMem*2, 0),
				expectedUpperBound: helpers.DefaultWrkNodeMem,
			},
			{
				name:               "1 worker per node allocs with limits overflows",
				l:                  singleSysWorkerLock(1, helpers.DefaultWrkNodeMem/2, helpers.DefaultWrkNodeMem/2, helpers.DefaultWrkNodeMem*2),
				expectedUpperBound: helpers.DefaultWrkNodeMem / 2,
				expectedMemOverrides: map[int]int{
					helpers.DefaultWrkNodeMem - int(helpers.DefaultWrkNodeMem*wsapisv1.RtNodeMaxLimitReservedMemRatio): 2},
			},
			{
				name:                 "1 worker per node allocs with limits under node capacity",
				l:                    singleSysWorkerLock(1, helpers.DefaultWrkNodeMem/2, helpers.DefaultWrkNodeMem/2, helpers.DefaultWrkNodeMem*0.75),
				expectedUpperBound:   helpers.DefaultWrkNodeMem / 2,
				expectedMemOverrides: map[int]int{helpers.DefaultWrkNodeMem * 0.75: 2},
			},
			{
				name:               "1.5 worker per node allocs with limits overflows",
				l:                  singleSysWorkerLock(1.5, helpers.DefaultWrkNodeMem/2, helpers.DefaultWrkNodeMem/2, helpers.DefaultWrkNodeMem*2),
				expectedUpperBound: helpers.DefaultWrkNodeMem / 2,
				expectedMemOverrides: map[int]int{
					// one worker sits on one node
					helpers.DefaultWrkNodeMem - int(helpers.DefaultWrkNodeMem*wsapisv1.RtNodeMaxLimitReservedMemRatio): 1,
					// two workers sit on another node
					helpers.DefaultWrkNodeMem / 2: 2,
				},
			},
			{
				name:               "4 sys 1 worker per node allocs whole node upper bound overflows",
				l:                  fourSysWorkerLock(1, 1, helpers.DefaultWrkNodeMem/4, helpers.DefaultWrkNodeMem*2, 0),
				expectedUpperBound: helpers.DefaultWrkNodeMem,
				expectedBrNicCount: 60, // 1:4 mode = 5 nics * 12 ports
			},
			{
				name:               "1 worker per node allocs partial node upper bound under node capacity",
				l:                  singleSysWorkerLock(1, helpers.DefaultWrkNodeMem/2, (helpers.DefaultWrkNodeMem/4)*3, 0),
				expectedUpperBound: (helpers.DefaultWrkNodeMem / 4) * 3,
			},
			{
				// Upper bound request is less than the total memory of the node, should allocate the upper bound
				name:               "4 sys 1 worker per node allocs partial node upper bound under node capacity",
				l:                  fourSysWorkerLock(1, 1, helpers.DefaultWrkNodeMem/2, (helpers.DefaultWrkNodeMem/4)*3, 0),
				expectedUpperBound: (helpers.DefaultWrkNodeMem / 4) * 3,
				expectedBrNicCount: 60, // 1:4 mode = 5 nics * 12 ports
			},
			{
				name:               "2 workers per node allocs whole node upper bound overflows",
				l:                  singleSysWorkerLock(2, helpers.DefaultWrkNodeMem/4, helpers.DefaultWrkNodeMem*2, 0),
				expectedUpperBound: helpers.DefaultWrkNodeMem / 2,
			},
			{
				name:               "2 workers per node allocs whole node upper bound under node capacity",
				l:                  singleSysWorkerLock(2, helpers.DefaultWrkNodeMem/2, helpers.DefaultWrkNodeMem*2, 0),
				expectedUpperBound: helpers.DefaultWrkNodeMem / 2,
			},
			{
				// 1.5 workers per node = some nodes in a group have 1 worker, some have 2. Should allocate all workers
				// the same amount of memory leaving some memory unused
				name:               "1.5 workers per node allocs partial node upper bound overflows",
				l:                  singleSysWorkerLock(1.5, helpers.DefaultWrkNodeMem/4, helpers.DefaultWrkNodeMem*2, 0),
				expectedUpperBound: helpers.DefaultWrkNodeMem / 2,
			},
			{
				name:               "4 sys 1.5 workers per node allocs partial node upper bound overflows",
				l:                  fourSysWorkerLock(1.5, 1.5, helpers.DefaultWrkNodeMem/4, helpers.DefaultWrkNodeMem*2, 0),
				expectedUpperBound: helpers.DefaultWrkNodeMem / 2,
				expectedBrNicCount: 60, // 1:4 mode = 5 nics * 12 ports
			},
			{
				name:               "4 sys 1 and 1.5 workers per node allocs partial node upper bound overflows",
				l:                  fourSysWorkerLock(1, 1.5, helpers.DefaultWrkNodeMem/4, helpers.DefaultWrkNodeMem*2, 0),
				expectedUpperBound: helpers.DefaultWrkNodeMem / 2,
				expectedBrNicCount: 60, // 1:4 mode = 5 nics * 12 ports
			},
			{
				name:               "4 sys 1 and 1.5 workers per node limit overflows",
				l:                  fourSysWorkerLock(1, 1.5, helpers.DefaultWrkNodeMem/4, helpers.DefaultWrkNodeMem, helpers.DefaultWrkNodeMem*2),
				expectedUpperBound: helpers.DefaultWrkNodeMem / 2,
				expectedMemOverrides: map[int]int{
					// 2 workers on 1 primary NGs + 3 workers on 3 secondary NGs
					helpers.DefaultWrkNodeMem - int(helpers.DefaultWrkNodeMem*wsapisv1.RtNodeMaxLimitReservedMemRatio): 5,
					// 6 workers sit on 3 worker node on 3 secondary NGs,
					helpers.DefaultWrkNodeMem / 2: 6,
				},
				expectedBrNicCount: 60, // 1:4 mode = 5 nics * 12 ports
			},
			{
				name: "0.5 act + 1 wgt per node limit overflows",
				l: singleSysActWgtLock(0.5, 1,
					helpers.DefaultMemNodeMem/4, helpers.DefaultMemNodeMem/2,
					helpers.DefaultMemNodeMem/4, helpers.DefaultMemNodeMem/2,
					helpers.DefaultMemNodeMem*2),
				expectedUpperBound: helpers.DefaultMemNodeMem / 2,
				expectedMemOverrides: map[int]int{
					// every act sits on one node
					helpers.DefaultMemNodeMem - int(helpers.DefaultMemNodeMem*wsapisv1.RtNodeMaxLimitReservedMemRatio): 5,
					// every two wgts sit on one node
					helpers.DefaultMemNodeMem / 2: 10,
				},
			},
			{
				name: "0.5 act + 1.5 wgt per node limit overflows",
				l: singleSysActWgtLock(0.5, 1.5,
					helpers.DefaultMemNodeMem/4, helpers.DefaultMemNodeMem/4,
					helpers.DefaultMemNodeMem/8, helpers.DefaultMemNodeMem/4,
					helpers.DefaultMemNodeMem*2),
				expectedUpperBound: helpers.DefaultMemNodeMem / 4,
				expectedMemOverrides: map[int]int{
					// every node has (1 * act + 1 * wgt) or (2 * wgt)
					(helpers.DefaultMemNodeMem - int(helpers.DefaultMemNodeMem*wsapisv1.RtNodeMaxLimitReservedMemRatio)) / 2: 20,
				},
			},
			{
				name: "mem override in ax scheduling",
				clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
					WithSystems(2, "0").WithSystems(2, "1").WithSystems(1, "2").
					WithAX(2, "0").WithAX(2, "1").
					WithBR(12),
				l: NewSystemLockBuilder("l0", 3, 0).
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					BuildPending(),
				expectedUpperBound: helpers.DefaultAxNodeMem / 8,
				expectedMemOverrides: map[int]int{
					// expectedMemOverride = act_upperbound + (node_total - 1 * chief - 4 * act_upperbound) / 4
					// 3 nodes * 4 acts per node
					helpers.DefaultAxNodeMem/8 +
						((helpers.DefaultAxNodeMem-int(helpers.DefaultAxNodeMem*wsapisv1.RtNodeMaxLimitReservedMemRatio))-
							helpers.DefaultAxNodeMem/8-4*helpers.DefaultAxNodeMem/8)/4: 12,
					// 3 nodes * 1 chief per node
					helpers.DefaultAxNodeMem / 8: 3,
				},
				expectedActNicCount: 12,
				// 1:3 mode = 4 nics * 12 ports
				expectedBrNicCount: 48,
			},
			{
				name: "train with AX nodes only for runtime servers and upper limit",
				clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
					WithSystems(2, "0").WithSystems(2, "1").
					WithAX(2, "0").WithAX(2, "1").
					WithBR(12).
					WithDepopulatedNodeGroups(4),

				l: NewSystemLockBuilder("l0", 3, 0).
					WithAxScheduling().
					WithAxOnlyForRuntime().
					WithChfGroupRequest(0, 0).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithWgtCmdGroupRequest(3, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8, 0, 0).
					WithUpperBoundsAndLimitRequest("Weight", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					BuildPending(),
				expectedUpperBound: helpers.DefaultAxNodeMem / 8,
				expectedMemOverrides: map[int]int{
					// expectedMemOverride = act_upperbound + (node_total - 1 * wgt - 4 * act_upperbound) / 5
					// 3 nodes * (4 acts + 1 wgt) per node
					helpers.DefaultAxNodeMem/8 +
						((helpers.DefaultAxNodeMem-int(helpers.DefaultAxNodeMem*wsapisv1.RtNodeMaxLimitReservedMemRatio))-
							helpers.DefaultAxNodeMem/8-4*helpers.DefaultAxNodeMem/8)/5: 15,
					// 3 nodes * 1 chf per node + 1 command
					0: 4,
				},
				expectedActNicCount: 12,
				// 1:3 mode = 4 nics * 12 ports
				expectedBrNicCount: 48,
			},
		} {
			t.Run(testcase.name, func(t *testing.T) {
				rec := &fakeEventRecorder{}
				c := cluster
				// TODO: consider using a builder for the cluster config
				cfg := &common.ClusterConfig{}
				if testcase.clusterBuilder != nil {
					c = testcase.clusterBuilder.Build()
					cfg = testcase.clusterBuilder.GetCfg()
					if testcase.clusterBuilder.IsV2NetworkEnabled() {
						wsapisv1.IsV2Network = true
						defer func() {
							wsapisv1.IsV2Network = false
						}()

						numAxNodes := 0
						for _, res := range c.List() {
							if _, ok := res.GetProp(resource.RoleAxPropKey); ok {
								numAxNodes++
							}
						}
						if numAxNodes > 0 {
							wsapisv1.UseAxScheduling = true
							wsapisv1.AxCountPerSystem = int(math.Ceil(float64(numAxNodes) / float64(len(cfg.SystemMap))))
							defer func() {
								wsapisv1.UseAxScheduling = false
								wsapisv1.AxCountPerSystem = 0
							}()
						}
					}
				}
				r := newLockReconciler(c, cfg)
				r.client = newClientBuilder().build()
				_, e := r.Initialize(ctx)
				require.NoError(t, e)

				r.client = newClientBuilder().add(testcase.l).build()
				r.recorder = rec

				req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: common.Namespace, Name: testcase.l.Name}}
				res, err := r.Reconcile(ctx, req)
				require.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)

				reconciledLock := r.locks[common.Namespace+"/"+testcase.l.Name]
				require.NotNil(t, reconciledLock)
				require.True(t, reconciledLock.IsGranted())

				for _, bound := range reconciledLock.GroupGrantUpperBounds {
					memGrant := bound[resource.MemSubresource]
					assert.Equal(t, testcase.expectedUpperBound, memGrant)
				}

				rl := reconciledLock.toK8s()
				actualMemOverrides := map[int]int{}
				actualBrNicCount := 0
				actualActNicCount := 0
				for _, gg := range rl.Status.GroupResourceGrants {
					for _, g := range gg.ResourceGrants {
						for _, r := range g.Resources {
							for k, v := range r.Subresources {
								if k == resource.MemSubresource {
									actualMemOverrides[v]++
								} else if strings.HasPrefix(k, resource.NodeNicSubresource) {
									if strings.HasPrefix(gg.Name, rlv1.BrRequestName) {
										actualBrNicCount++
									} else if strings.HasPrefix(gg.Name, rlv1.ActivationRequestName) {
										actualActNicCount++
									}
								}
							}
						}
					}
				}
				testcase.expectedMemOverrides = common.EnsureMap(testcase.expectedMemOverrides)
				if len(testcase.expectedMemOverrides) > 0 {
					require.Equal(t, testcase.expectedMemOverrides, actualMemOverrides)
				}

				require.Equal(t, testcase.expectedBrNicCount, actualBrNicCount)
				require.Equal(t, testcase.expectedActNicCount, actualActNicCount)

				// delete flow by re-init client to mock lock not exists/deleted case
				r.client = newClientBuilder().build()
				res, err = r.Reconcile(ctx, req)
				require.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
				assert.True(t, r.resources.IsUnallocated())
			})
		}
	})
}

func TestReconciler_EventSequence(t *testing.T) {
	common.InitLogging()

	type deleteLock string
	type updateLockPriority struct {
		lockName string
		priority int
	}
	type updateLockGrantDeadline struct {
		lockName       string
		gracePeriodSec int
	}
	type setResourceHealth struct {
		resourceId string
		status     string
	}
	type reinitializeServer struct{}
	type assertCondition func(t *testing.T, reconciler *Reconciler) bool

	assertGranted := func(lockNames ...string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			return assert.ElementsMatch(t, lockNames, common.Keys(getGranted(reconciler)))
		}
	}

	assertPending := func(lockNames ...string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			pendingNames := getPendingNames(reconciler)
			if !assert.ElementsMatch(t, lockNames, pendingNames) {
				t.Logf("actual pending: %v", pendingNames)
				return false
			}
			return true
		}
	}

	assertPendingOrdered := func(lockNames ...string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			pendingNames := getPendingNamesOrdered(reconciler)
			if !assert.Equal(t, lockNames, pendingNames) {
				t.Logf("actual pending: %v", pendingNames)
				return false
			}
			return true
		}
	}

	assertFailed := func(lockName string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			nsn := fmt.Sprintf("%s/%s", common.Namespace, lockName)
			return reconciler.locks[nsn].Obj.Status.State == rlv1.LockError
		}
	}

	assertNotExist := func(lockName string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			nsn := fmt.Sprintf("%s/%s", common.Namespace, lockName)
			_, ok := reconciler.locks[nsn]
			return ok == false
		}
	}

	assertHasPrimaryNodegroupsIn := func(lock string, ngNames ...string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			ng := getNodeGroups(reconciler, lock)
			return assert.Subsetf(t, ngNames, ng["ng-primary-0"], "actual nodegroups: %v", ng["ng-primary-0"])
		}
	}

	assertHasSecondaryNodegroupsIn := func(lock string, ngNames ...string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			ng := getNodeGroups(reconciler, lock)
			return assert.Subsetf(t, ngNames, ng["ng-secondary"], "actual nodegroups: %v", ng["ng-secondary"])
		}
	}

	assertMostRecentEvent := func(eventType, reason, message string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			var event fakeEvent
			recorder := reconciler.recorder.(*fakeEventRecorder)
			if len(recorder.getEvents()) > 0 {
				event = recorder.getEvents()[len(recorder.getEvents())-1]
			}
			return assert.True(t, event.eventtype == eventType && event.reason == reason && strings.Contains(event.message, message),
				"expected a '%s' event with reason '%s' and message '%s', but saw %v", eventType, reason, message, event)
		}
	}

	assertResourcesUnallocated := assertCondition(func(t *testing.T, r *Reconciler) bool {
		unallocated := r.resources.IsUnallocated()
		return assert.True(t, unallocated, "some resources are not unallocated")
	})

	// checks that coordinators are evenly spread on mgmt nodes (ensure all coordinators have same mem request if using this function)
	assertCoordinatorsBalanced := assertCondition(func(t *testing.T, r *Reconciler) bool {
		nodeCrdCount := map[string]int{}
		for _, node := range r.resources.Filter(resource.NewNodeFilter("management", 0, 0)).List() {
			nodeCrdCount[node.Id()] = 0
		}
		maxCount := 0
		for _, l := range r.locks {
			if !l.IsGranted() {
				continue
			}
			for name, g := range l.ResourceGrants {
				if name == rlv1.CoordinatorRequestName {
					for _, rid := range g.ResourceIds {
						nodeCrdCount[rid] += 1
						if nodeCrdCount[rid] > maxCount {
							maxCount = nodeCrdCount[rid]
						}
					}
				}
			}
		}
		if maxCount == 0 {
			return assert.True(t, false, "no coordinators scheduled")
		}
		for node, count := range nodeCrdCount {
			if maxCount-count > 1 {
				return assert.Truef(t, false,
					"node %s had %d coordinators but expecting %d or %d for balance",
					node, count, maxCount, maxCount-1)
			}
		}
		return true
	})

	assertNvmfSpaceGranted := func(lockName, targetIdAnnot string, subresourceRanges resource.SubresourceRanges) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			grantedLocks := getGranted(reconciler)
			l := grantedLocks[lockName]
			if l == nil {
				return assert.Fail(t, "Lock is not granted")
			}
			for reqName, rg := range l.ResourceGrants {
				if reqName != rlv1.CoordinatorRequestName {
					continue
				}
				if targetIdAnnot != rg.Annotations[rlv1.NvmfTargetIdAnnotKey] {
					return assert.Fail(t, "Nvmf targets do not match")
				}
				if !subresourceRanges.Equals(rg.SubresourceRanges) {
					return assert.Fail(t, "Subresource ranges do not match")
				}
			}
			return true
		}
	}

	assertResourceGranted := func(resType string, resNames ...string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			for _, resName := range resNames {
				res, ok := reconciler.resources.Get(fmt.Sprintf("%s/%s", resType, resName))
				if !ok || !res.HasAllocation() {
					return assert.Failf(t, "resource  does not have any allocation:", "%s/%s %+v", resType, resName, res)
				}
			}
			return true
		}
	}

	assertSubresourceRangesGranted := func(resType, resName string, subresourceRanges resource.SubresourceRanges) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			res, ok := reconciler.resources.Get(fmt.Sprintf("%s/%s", resType, resName))
			if !ok || !res.HasAllocation() {
				return assert.Failf(t, "subresource does not have any allocation: ", " %+v", res)
			}
			if !res.GetAllocatedSubresourceRanges().Equals(subresourceRanges) {
				return assert.Fail(t, "subresource ranges do not match: expected %v, got %v",
					subresourceRanges, res.GetAllocatedSubresourceRanges())
			}
			return true
		}
	}

	assertNodesGranted := func(nodeNames ...string) assertCondition {
		return assertResourceGranted(resource.NodeType, nodeNames...)
	}

	assertSystemsGranted := func(sysNames ...string) assertCondition {
		return assertResourceGranted(resource.SystemType, sysNames...)
	}

	assertNodegroupsGranted := func(ngNames ...string) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			var grantedGroups []string
			for _, l := range reconciler.locks {
				for _, grants := range l.GroupGrants {
					for _, gg := range grants {
						logrus.Infof("lock name: %s, group name: %s", l.Name, gg.GroupVal)
						grantedGroups = append(grantedGroups, gg.GroupVal)
					}
				}
			}
			return slices.Equal(common.Sorted(ngNames), common.Sorted(grantedGroups))
		}
	}

	assertV2ActBalancedScheduling := func(lockName string, expectedSpineLoad int) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			axNodes := reconciler.resources.Filter(resource.NewFilter(resource.NodeType, common.RoleActivation.AsMatcherProperty(), nil))
			for _, no := range axNodes.List() {
				for k := range no.GetProps() {
					if !strings.HasPrefix(k, resource.PortAffinityPropKeyPrefix) {
						continue
					}
				}
			}

			rl := &rlv1.ResourceLock{}
			err := reconciler.client.Get(context.TODO(), client.ObjectKey{Name: lockName, Namespace: common.Namespace}, rl)
			require.NoError(t, err)

			podsPerSystem := map[string][]int{}
			podsPerNode := map[string][]int{}
			nicLoadPerNode := map[string]map[string]int{}
			portLoadPerSystem := map[string]map[int]int{}
			spineLoad := 0

			indexedSystems := map[int]string{}
			for _, rg := range rl.Status.ResourceGrants {
				if rg.Name != rlv1.SystemsRequestName {
					continue
				}
				for i, res := range rg.Resources {
					indexedSystems[i] = res.Name
				}
			}

			resourceGrants := map[string]rlv1.ResourceGrant{}
			for _, gg := range rl.Status.GroupResourceGrants {
				if !strings.HasPrefix(gg.Name, rlv1.ActivationRequestName) {
					continue
				}
				for _, rg := range gg.ResourceGrants {
					resourceGrants[rg.Name] = rg
				}
			}
			numActsPerCsx := len(resourceGrants) / len(indexedSystems)
			// Sort the keys so it's easier for debugging
			for _, grantName := range resource.V2ActKeySortForGrant(resourceGrants) {
				rg := resourceGrants[grantName]
				grantedResources := rg.Resources

				podIdx, _ := strconv.Atoi(rg.Name)
				sysIdx, _ := strconv.Atoi(rg.Annotations[rlv1.SystemIdAnnotKey])
				csPort, _ := strconv.Atoi(rg.Annotations[rlv1.TargetCsPortAnnotKey])

				systemName := indexedSystems[sysIdx]
				podsPerSystem[systemName] = append(podsPerSystem[systemName], podIdx)
				if _, ok := portLoadPerSystem[systemName]; !ok {
					portLoadPerSystem[systemName] = map[int]int{}
				}
				portLoadPerSystem[systemName][csPort] += 1

				nodeName := grantedResources[0].Name
				podsPerNode[nodeName] = append(podsPerNode[nodeName], podIdx)

				node, _ := reconciler.resources.Get("node/" + nodeName)
				var nicResNames []string
				for k := range node.Subresources() {
					if strings.HasPrefix(k, resource.NodeNicSubresource) {
						nicResNames = append(nicResNames, k)
					}
				}
				if _, ok := nicLoadPerNode[nodeName]; !ok {
					nicLoadPerNode[nodeName] = map[string]int{}
				}
				for k := range grantedResources[0].Subresources {
					if slices.Contains(nicResNames, k) {
						nicLoadPerNode[nodeName][k] += 1
						csPortIndexKey := fmt.Sprintf("%s/%s-%d", resource.PortAffinityPropKeyPrefix, indexedSystems[sysIdx], csPort)
						if val, ok := node.GetProps()[csPortIndexKey]; !ok || val != k {
							spineLoad += 1
						}

						preferredCsPorts := map[string]map[string]bool{}
						for subResName := range node.Subresources() {
							if !strings.HasPrefix(subResName, resource.NodeNicSubresource) {
								continue
							}
							for k, v := range node.GetProps() {
								if !strings.HasPrefix(k, resource.PortAffinityPropKeyPrefix) {
									continue
								}
								if v == subResName {
									tokens := strings.Split(k, "-")
									preferredCsPorts[subResName] = common.EnsureMap(preferredCsPorts[subResName])
									preferredCsPorts[subResName][tokens[len(tokens)-1]] = true
								}
							}
						}
						var preferredAffinities []string
						for _, k := range common.SortedKeys(preferredCsPorts) {
							preferredAffinities = append(preferredAffinities, fmt.Sprintf("%s: %v", k, common.SortedKeys(preferredCsPorts[k])))
						}

						logrus.Infof("Pod %d targeted %s cs-port-%d, selected %s in %s (%s)",
							podIdx, indexedSystems[sysIdx], csPort, k, nodeName, strings.Join(preferredAffinities, ", "))
						break
					}
				}
			}
			if spineLoad != expectedSpineLoad {
				logrus.Errorf("Expected a spine load of %d but got %d", expectedSpineLoad, spineLoad)
				return false
			}

			for k, v := range podsPerSystem {
				if len(v) != numActsPerCsx {
					logrus.Errorf("Expected %d pods to talk to %s, but got %d", numActsPerCsx, k, len(v))
					return false
				}
			}
			for k, v := range podsPerNode {
				if len(v) != numActsPerCsx {
					logrus.Errorf("Expected %d pods scheduled on %s, but got %d", numActsPerCsx, k, len(v))
					return false
				}
			}
			for k, v := range nicLoadPerNode {
				minLoad := math.MaxInt
				maxLoad := 0
				for _, load := range v {
					if load < minLoad {
						minLoad = load
					}
					if load > maxLoad {
						maxLoad = load
					}
				}
				if maxLoad-minLoad > 1 {
					logrus.Errorf("Expected nic load on %s to be balanced, but got a load difference of %d",
						k, maxLoad-minLoad)
					return false
				}
			}
			for k, portLoads := range portLoadPerSystem {
				// Iterate through 4 domains
				for i := 0; i < 4; i++ {
					// Iterate through 3 ports, tests currently assumes all healthy cs ports
					minLoad := math.MaxInt
					maxLoad := 0
					for j := 0; j < 2; j++ {
						portIdx := i*3 + j
						portLoad := portLoads[portIdx]
						if portLoad < minLoad {
							minLoad = portLoad
						}
						if portLoad > maxLoad {
							maxLoad = portLoad
						}
					}
					if maxLoad-minLoad > 1 {
						logrus.Errorf("Expected cs port load from domain %d on %s to be balanced, but got a load difference of %d",
							i, k, maxLoad-minLoad)
						return false
					}
				}
			}
			return true
		}
	}

	assertWgtCmdInAxNodes := func(lockName string, expectedWgtPods int) assertCondition {
		return func(t *testing.T, reconciler *Reconciler) bool {
			rl := &rlv1.ResourceLock{}
			err := reconciler.client.Get(context.TODO(), client.ObjectKey{Name: lockName, Namespace: common.Namespace}, rl)
			require.NoError(t, err)

			numSystems := 0
			for _, request := range rl.Spec.ResourceRequests {
				if request.Name == rlv1.SystemsRequestName {
					numSystems = request.Count
				}
			}
			maxWgtsPerAxNode := int(math.Ceil(float64(expectedWgtPods) / float64(numSystems*wsapisv1.AxCountPerSystem)))

			wgtsPerAxNode := map[string]int{}
			totalWgtPods := 0
			for _, gg := range rl.Status.GroupResourceGrants {
				if !strings.HasPrefix(gg.Name, rlv1.WgtCmdGroupPrefix) {
					continue
				}
				for _, rg := range gg.ResourceGrants {
					if strings.HasPrefix(rg.Name, string(wsapisv1.WSReplicaTypeWeight)) {
						for _, res := range rg.Resources {
							if _, ok := wgtsPerAxNode[res.Name]; ok {
								wgtsPerAxNode[res.Name] += 1
							} else {
								wgtsPerAxNode[res.Name] = 1
							}
							totalWgtPods += 1
						}
					}
				}
			}
			for axNode, wgts := range wgtsPerAxNode {
				if wgts != maxWgtsPerAxNode && wgts != maxWgtsPerAxNode-1 {
					logrus.Errorf("unexpected number of weights for AX node %s: expecting %d or %d, seeing %d",
						axNode, maxWgtsPerAxNode, maxWgtsPerAxNode-1, wgts)
					return false
				}
			}
			if totalWgtPods != expectedWgtPods {
				logrus.Errorf("unexpected number of total number of weight servers, expecting %d, seeing %d",
					expectedWgtPods, totalWgtPods)
				return false
			}
			return true
		}
	}

	ctx := context.TODO()
	systemsTotal := 4
	clusterBuilder := helpers.NewDefaultClusterBuilder(systemsTotal, 12)

	for _, testcase := range []struct {
		name           string
		clusterBuilder *helpers.ClusterBuilder

		initPend  []*rlv1.ResourceLock
		initGrant []*rlv1.ResourceLock

		events []interface{}
	}{
		{
			name:           "fifo order - primary groupRequest only",
			clusterBuilder: clusterBuilder,

			initPend: []*rlv1.ResourceLock{
				NewSystemCoordinatorLockBuilder("pending-0", 1, systemsTotal, 10).BuildPending(),
				NewSystemCoordinatorLockBuilder("pending-1", 1, systemsTotal, 20).BuildPending(),
				NewSystemCoordinatorLockBuilder("pending-2", 1, systemsTotal, 30).BuildPending(),
			},
			initGrant: []*rlv1.ResourceLock{
				NewSystemCoordinatorLockBuilder("granted-0", 2, systemsTotal, 0).BuildGranted().
					WithSystemsGrant("system/system-0", "system/system-1").
					WithCoordinatorGrant("node/management").Build(),

				NewSystemCoordinatorLockBuilder("granted-1", 2, systemsTotal, 0).BuildGranted().
					WithSystemsGrant("system/system-2", "system/system-3").
					WithCoordinatorGrant("node/management").Build(),
			},

			events: []interface{}{
				deleteLock("granted-0"), // now p0, p1 granted
				deleteLock("pending-1"), // now p2 granted

				NewSystemCoordinatorLockBuilder("l0", 1, systemsTotal, 100).BuildPending(), // l0 waits
				NewSystemCoordinatorLockBuilder("l1", 1, systemsTotal, 100).BuildPending(), // l1 waits
				deleteLock("pending-2"), // l0 granted
				assertPending("l1"),
				assertGranted("l0", "granted-1", "pending-0"),

				deleteLock("granted-1"), // l1 granted
				assertGranted("l0", "l1", "pending-0"),

				deleteLock("pending-0"),
				assertGranted("l0", "l1"),

				deleteLock("l0"),
				assertGranted("l1"),

				deleteLock("l1"),
				assertResourcesUnallocated,
			},
		},
		{
			name: "ix_scheduling_happy_path",
			clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
				WithSystems(2, "0").WithSystems(2, "1").
				WithAX(2, "0").WithAX(2, "1").
				WithIX(2, "0").WithIX(2, "1"). // IX nodes in both stamps
				WithBR(12),

			events: []interface{}{
				reinitializeServer{},
				// Single lock requesting 2 systems from stamps that have IX nodes
				NewSystemLockBuilder("l0", 2, 0).
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithKvssGroupRequest(2, 2, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithUpperBoundsAndLimitRequest("KVStorageServer", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithSWDriverGroupRequest(helpers.DefaultIxNodeCpu/8, helpers.DefaultIxNodeMem/8, 3, 1).
					BuildPending(),
				assertGranted("l0"),
				assertSystemsGranted("system-0", "system-1"), // 2 systems from stamps 0 and 1
				assertNodesGranted("ax-0", "ax-1"),           // AX nodes granted
				assertNodesGranted("ix-0"),                   // IX node granted for SWDriver
				assertV2ActBalancedScheduling("l0", 4),
				// Second lock requesting 1 system
				NewSystemLockBuilder("l1", 1, 100).
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithKvssGroupRequest(2, 2, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithUpperBoundsAndLimitRequest("KVStorageServer", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithSWDriverGroupRequest(helpers.DefaultIxNodeCpu/8, helpers.DefaultIxNodeMem/8, 3, 1).
					BuildPending(),
				assertGranted("l0", "l1"),
				assertSystemsGranted("system-0", "system-1", "system-2"),
				assertNodesGranted("ax-0", "ax-1", "ax-2"),
				assertNodesGranted("ix-0", "ix-2"), // IX nodes granted for SWDriver
				assertV2ActBalancedScheduling("l1", 2),

				deleteLock("l0"),
				assertGranted("l1"),
				assertSystemsGranted("system-2"),
				assertNodesGranted("ax-2"),
				assertNodesGranted("ix-2"),

				deleteLock("l1"),
				assertResourcesUnallocated,
			},
		},
		{
			name: "ix_scheduling_happy_path_with_job_sharing",
			clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
				WithSystems(2, "0").
				WithAX(2, "0").WithAX(2, "1").
				WithIX(1, "0").
				WithIxJobSharing().
				WithBR(12),

			events: []interface{}{
				reinitializeServer{},
				// Single lock requesting 1 system from stamps that have IX nodes
				NewSystemLockBuilder("l0", 1, 0).
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithSWDriverGroupRequest(helpers.DefaultIxNodeCpu/8, helpers.DefaultIxNodeMem/8, 3, 1).
					BuildPending(),
				assertGranted("l0"),
				assertSystemsGranted("system-0"), // 2 systems from stamps 0 and 1
				assertNodesGranted("ix-0"),       // IX node granted for SWDriver

				// Second lock requesting 1 system
				NewSystemLockBuilder("l1", 1, 0).
					EnableJobSharing().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithSWDriverGroupRequest(helpers.DefaultIxNodeCpu/8, helpers.DefaultIxNodeMem/8, 3, 1).
					BuildPending(),
				assertGranted("l0", "l1"),
				assertSystemsGranted("system-1"),
				assertNodesGranted("ix-0"), // IX nodes granted for SWDriver

				deleteLock("l0"),
				assertGranted("l1"),
				assertSystemsGranted("system-1"),
				assertNodesGranted("ix-0"),

				deleteLock("l1"),
				assertResourcesUnallocated,
			},
		},
		{
			name: "ix_scheduling_respects_system_ordering",
			clusterBuilder: helpers.NewClusterBuilder(2).WithV2Network().
				WithSystems(4, "0").
				WithSystems(6, "1").
				WithIX(1, "0").
				WithIX(1, "1").
				WithBR(50),
			events: []interface{}{
				reinitializeServer{},
				// Request 8 systems with domain 3 affinity
				NewSystemLockBuilder("l0", 8, 0).
					EnableJobSharing().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithSWDriverGroupRequest(helpers.DefaultIxNodeCpu/8, helpers.DefaultIxNodeMem/8, 3, 7). // 7 sysIndex, so it should get stamp 1's IX node
					BuildPending(),
				assertGranted("l0"),
				assertSystemsGranted("system-0", "system-1", "system-4", "system-5", "system-6", "system-7", "system-8", "system-9"), // Updated to match actual allocation
				assertNodesGranted("ix-1"), // SWDriver scheduled to stamp 1
				deleteLock("l0"),
				assertResourcesUnallocated,
			},
		},
		{
			name: "ix_scheduling_job_sharing_two_jobs_on_same_ix_nodes",
			clusterBuilder: helpers.NewClusterBuilder(2).WithV2Network().
				WithSystems(4, "0").
				WithSystems(6, "1").
				WithIX(1, "1").
				WithIxJobSharing().
				WithBR(50),
			events: []interface{}{
				reinitializeServer{},
				// First job requesting 8 systems with job sharing enabled
				NewSystemLockBuilder("l0", 8, 0).
					EnableJobSharing().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithSWDriverGroupRequest(helpers.DefaultIxNodeCpu/8, helpers.DefaultIxNodeMem/8, 3, 1).
					BuildPending(),
				assertGranted("l0"),
				assertSystemsGranted("system-0", "system-1", "system-4", "system-5", "system-6", "system-7", "system-8", "system-9"),
				assertNodesGranted("ix-0"), // SWDriver scheduled to stamp 1's IX node (the only IX node)
				// Second job requesting 2 systems with job sharing enabled
				// Should be able to share the same IX node since there's sufficient CPU/memory
				NewSystemLockBuilder("l1", 2, 100).
					EnableJobSharing().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithSWDriverGroupRequest(helpers.DefaultIxNodeCpu/8, helpers.DefaultIxNodeMem/8, 3, 1).
					BuildPending(),
				assertGranted("l0", "l1"),
				// l1 gets the remaining 2 systems from stamp 0
				assertSystemsGranted("system-2", "system-3", "system-4", "system-5", "system-6", "system-7", "system-8", "system-9"),
				assertNodesGranted("ix-0"), // Both jobs share the same IX node (ix-0 is the only IX node)

				// Verify both jobs can coexist
				deleteLock("l0"),
				assertGranted("l1"),
				assertSystemsGranted("system-2", "system-3"),
				assertNodesGranted("ix-0"), // l1 still uses the same IX node

				deleteLock("l1"),
				assertResourcesUnallocated,
			},
		},
		{
			name:           "fifo order - two groupRequests",
			clusterBuilder: clusterBuilder,

			events: []interface{}{
				NewSystemCoordinatorLockBuilder("l0", 1, systemsTotal, 0).
					WithPrimaryNodeGroup().
					BuildPending(), // l0 granted
				NewSystemCoordinatorLockBuilder("l1", 4, systemsTotal, 100).
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(3).
					BuildPending(), // l1 waits
				NewSystemCoordinatorLockBuilder("l2", 2, systemsTotal, 200).
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(1).
					BuildPending(), // l2 waits
				assertGranted("l0"),
				assertPending("l1", "l2"),

				deleteLock("l0"), // l1 granted
				assertGranted("l1"),
				assertPending("l2"),

				deleteLock("l1"), // l2 granted
				assertGranted("l2"),

				deleteLock("l2"), // now no locks granted
				assertResourcesUnallocated,
			},
		},

		{
			name:           "fifo order - groupRequests require search for secondary groups",
			clusterBuilder: clusterBuilder,

			events: []interface{}{
				NewSystemCoordinatorLockBuilder("l0", 1, systemsTotal, 0).
					WithPrimaryNodeGroup("group:nodegroup0").
					BuildPending(), // l0 grants
				assertGranted("l0"),

				NewSystemCoordinatorLockBuilder("l1", 1, systemsTotal, 100).
					WithPrimaryNodeGroup("group:nodegroup0,nodegroup2").
					WithSecondaryNodeGroup(2, "group:nodegroup0,nodegroup1,nodegroup2,nodegroup3").
					BuildPending(), // l1 granted
				assertGranted("l0", "l1"),

				deleteLock("l0"),
				deleteLock("l1"),
				assertResourcesUnallocated,
			},
		},

		{
			name:           "fifo order - nodegroups reserved by higher priority lock",
			clusterBuilder: clusterBuilder,

			events: []interface{}{
				NewSystemCoordinatorLockBuilder("l0", 2, systemsTotal, 0).
					WithPrimaryNodeGroup("group:nodegroup0").
					WithSecondaryNodeGroup(1).
					BuildPending(),
				assertGranted("l0"),

				NewSystemCoordinatorLockBuilder("l1", 2, systemsTotal, 100).
					WithPrimaryNodeGroup("group:nodegroup0").
					WithSecondaryNodeGroup(1, "group:nodegroup1").
					BuildPending(),
				assertPending("l1"),

				NewSystemCoordinatorLockBuilder("l2", 2, systemsTotal, 100).
					WithPrimaryNodeGroup("group:nodegroup1").
					WithSecondaryNodeGroup(1).
					BuildPending(), // l2 waits because l1 reserved group:1
				assertPending("l1", "l2"),

				deleteLock("l0"),
				assertGranted("l1"),
				assertPending("l2"),

				deleteLock("l1"),
				assertGranted("l2"),

				deleteLock("l2"),
				assertResourcesUnallocated,
			},
		},

		{
			name:           "fifo order - train queue takes precedence",
			clusterBuilder: clusterBuilder,

			events: []interface{}{
				NewSystemCoordinatorLockBuilder("l0", systemsTotal, systemsTotal, 0).
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(systemsTotal - 1).
					BuildPending(), // lock everything
				assertGranted("l0"),

				// create jobs in the compile queue that require all systems and
				// a job in the default queue that requires 1 system
				NewSystemCoordinatorLockBuilder("c0", systemsTotal/2, systemsTotal, 500).
					WithQueue(rlv1.CompileQueueName).
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(systemsTotal/2 - 1).
					BuildPending(),

				NewSystemCoordinatorLockBuilder("c1", systemsTotal/2, systemsTotal, 1000).
					WithQueue(rlv1.CompileQueueName).
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(systemsTotal/2 - 1).
					BuildPending(),

				NewSystemCoordinatorLockBuilder("e0", systemsTotal/2, systemsTotal, 1500).
					WithPrimaryNodeGroup().
					BuildPending(),

				assertPending("c0", "c1", "e0"),

				deleteLock("l0"),
				assertGranted("e0", "c0"),
				assertPending("c1"),

				deleteLock("c0"),
				assertGranted("e0", "c1"),

				deleteLock("c1"),
				deleteLock("e0"),
				assertResourcesUnallocated,
			},
		},

		{
			name: "resource selection order - depop groups chosen as secondaries",
			clusterBuilder: helpers.NewClusterBuilder(1).
				WithSystems(4, "").
				WithBR(12).
				WithPopulatedNodeGroups(2, wsapisv1.PopNgTypeRegular).
				WithDepopulatedNodeGroups(2),

			events: []interface{}{
				NewSystemCoordinatorLockBuilder("l0", 2, 4, 0).
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(1).
					BuildPending(),
				NewSystemCoordinatorLockBuilder("l1", 2, 4, 100).
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(1).
					BuildPending(),
				assertHasPrimaryNodegroupsIn("l0", "nodegroup0", "nodegroup1"),
				assertHasSecondaryNodegroupsIn("l0", "nodegroup2", "nodegroup3"),

				assertHasPrimaryNodegroupsIn("l1", "nodegroup0", "nodegroup1"),
				assertHasSecondaryNodegroupsIn("l1", "nodegroup2", "nodegroup3"),

				deleteLock("l0"),
				deleteLock("l1"),
				assertResourcesUnallocated,
			},
		},

		{
			name: "resource selection order - pop groups chosen as secondaries when depop groups are unavailable",
			clusterBuilder: helpers.NewClusterBuilder(1).
				WithSystems(4, "").
				WithBR(12).
				WithPopulatedNodeGroups(2, wsapisv1.PopNgTypeRegular).
				WithDepopulatedNodeGroups(2),

			events: []interface{}{
				NewSystemCoordinatorLockBuilder("l0", 4, 4, 0).
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(3).
					BuildPending(),
				assertHasPrimaryNodegroupsIn("l0", "nodegroup0", "nodegroup1"),

				deleteLock("l0"),
				assertResourcesUnallocated,
			},
		},

		{
			name: "multi-mgmt node - coordinators load balanced",
			clusterBuilder: helpers.NewClusterBuilder(3).
				WithSystems(4, "").
				WithPopulatedNodeGroups(4, wsapisv1.PopNgTypeRegular),

			events: []interface{}{
				NewCompileLockBuilder("c0", 1, 2).BuildPending(),
				NewCompileLockBuilder("c1", 1, 2).BuildPending(),
				NewCompileLockBuilder("c2", 1, 2).BuildPending(),
				assertGranted("c0", "c1", "c2"),
				assertCoordinatorsBalanced,

				NewSystemCoordinatorLockBuilder("l0", 1, 1, 100).BuildPending(),
				NewSystemCoordinatorLockBuilder("l1", 1, 1, 100).BuildPending(),
				NewSystemCoordinatorLockBuilder("l2", 1, 1, 100).BuildPending(),
				deleteLock("c0"),
				assertGranted("l0", "c1", "c2"),
				assertCoordinatorsBalanced,

				deleteLock("c1"),
				deleteLock("c2"),
				assertCoordinatorsBalanced,

				deleteLock("l0"),
				deleteLock("l1"),
				deleteLock("l2"),
				assertResourcesUnallocated,
			},
		},

		{
			name: "br cs-ports recovered after re-init",
			clusterBuilder: helpers.NewClusterBuilder(3).
				WithSystems(6, "").
				WithBR(12).
				WithPopulatedNodeGroups(6, wsapisv1.PopNgTypeRegular),

			events: []interface{}{
				NewSystemCoordinatorLockBuilder("l0", 2, 4, 100).BuildPending(),
				assertGranted("l0"),

				// re-initialize forces re-loading locks from k8s, deserializing their state and re-subtracting
				// granted locks usages from nodes. If BR deserializes correctly, should remember BRs have only half
				// their cs-ports used and can therefore schedule another 2 CS job on the remaining unused cs-ports
				reinitializeServer{},

				NewSystemCoordinatorLockBuilder("l1", 2, 4, 200).BuildPending(),
				assertGranted("l0", "l1"),

				NewSystemCoordinatorLockBuilder("l2", 2, 4, 300).BuildPending(), // sanity check to not oversubscribe BR
				assertPending("l2"),

				deleteLock("l0"),
				assertGranted("l1", "l2"),

				deleteLock("l1"),
				deleteLock("l2"),
				assertResourcesUnallocated,
			},
		},

		{
			name: "compile with system request",
			clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
				WithSystems(2, "0").WithSystems(2, "1").WithSystems(1, "2").
				WithAX(2, "0").WithAX(2, "1").
				WithBR(12),

			events: []interface{}{
				NewSystemCoordinatorLockBuilder("l0", 2, 4, 100).WithAxScheduling().BuildPending(),
				assertGranted("l0"),

				deleteLock("l0"),
				assertResourcesUnallocated,
			},
		},
		{
			name: "train with AX nodes only for runtime servers",
			clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
				WithSystems(2, "0").WithSystems(2, "1").
				WithAX(2, "0").WithAX(2, "1").
				WithBR(12).
				WithDepopulatedNodeGroups(4),

			events: []interface{}{
				NewSystemLockBuilder("l0", 3, 0).
					WithAxScheduling().
					WithAxOnlyForRuntime().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithWgtCmdGroupRequest(helpers.DefaultMemNodePerSys-1, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8, 0, 0).
					WithAxOnlyPrimaryNodeGroup().
					WithSecondaryNodeGroup(2).
					BuildPending(),
				assertGranted("l0"),
				assertV2ActBalancedScheduling("l0", 6),
				assertWgtCmdInAxNodes("l0", helpers.DefaultMemNodePerSys-1),
				deleteLock("l0"),
				assertResourcesUnallocated,
			},
		},

		{
			// As of rel-2.4, normal jobs only utilize the regular and large memx groups for the primary nodegroup
			// requests, while the xlarge jobs (250B+ parameters) only utilize the xlarge memx groups for the primary
			// nodegroup requests. Regardless of job sizes, we consider all nodegroups (regular/large/xlarge/depop)
			// when matching the secondary nodegroup requests.
			name: "memory class nodegroup tests",
			clusterBuilder: helpers.NewClusterBuilder(1).WithV2Network().
				WithSystems(8, "0").
				WithAX(8, "0").
				WithBR(30).
				// nodegroup0: pop_xlarge
				// nodegroup1: pop_large
				// nodegroup2: pop_regular
				// nodegroup3: depop
				WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeXLarge).
				WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeLarge).
				WithPopulatedNodeGroups(1, wsapisv1.PopNgTypeRegular).
				WithDepopulatedNodeGroups(1),

			events: []interface{}{
				reinitializeServer{},
				// need 1 from primary ng candidates: pop_regular, pop_large
				// need 1 from secondary ng candidates: depop, pop_regular, pop_large, pop_xlarge
				// granted: pop_regular + depop
				NewSystemLockBuilder("l0", 2, 0).
					WithAxScheduling().
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(1).
					BuildPending(),
				assertGranted("l0"),
				assertSystemsGranted("system-0", "system-1"),
				assertNodegroupsGranted("nodegroup2", "nodegroup3"),

				// need 1 from primary ng candidates: pop_regular, pop_large
				// need 2 from secondary ng candidates: depop, pop_regular, pop_large, pop_xlarge
				// pending: not enough ngs to fulfill the secondary ng requests
				// 1 primary request + 1 secondary request found resource, 1 secondary request pending
				NewSystemLockBuilder("l1", 3, 10).
					WithAxScheduling().
					WithPrimaryNodeGroup().
					WithSecondaryNodeGroup(2).
					BuildPending(),
				assertPending("l1"),

				deleteLock("l0"),
				assertGranted("l1"),
				assertSystemsGranted("system-0", "system-1", "system-2"),
				// granted: pop_regular + depop + pop_large (used as depop)
				assertNodegroupsGranted("nodegroup1", "nodegroup2", "nodegroup3"),

				// 2 pop groups can host 2 set of workers
				// we only need 1 additional depop group for secondary nodegroup grant
				NewSystemLockBuilder("l2", 3, 20).
					WithAxScheduling().
					WithPrimaryNodeGroup().
					WithNumPopGroupAnnotation(2).
					WithSecondaryNodeGroup(2).
					BuildPending(),
				assertPending("l2"),

				deleteLock("l1"),
				assertGranted("l2"),
				assertSystemsGranted("system-0", "system-1", "system-2"),
				// granted: pop_regular + depop + pop_large (used as depop)
				assertNodegroupsGranted("nodegroup1", "nodegroup2", "nodegroup3"),

				// need 1 from primary ng candidates: pop_xlarge
				// granted: pop_xlarge
				NewSystemLockBuilder("l3", 1, 30).
					WithAxScheduling().
					WithPrimaryNodeGroup("memClass:xlarge").
					BuildPending(),
				assertGranted("l2", "l3"),
				assertSystemsGranted("system-0", "system-1", "system-2", "system-3"),
				assertNodegroupsGranted("nodegroup1", "nodegroup2", "nodegroup3", "nodegroup0"),

				// need 1 from primary ng candidates: pop_xlarge
				// need 1 from secondary ng candidates: depop, pop_regular, pop_large, pop_xlarge
				// pending given all nodegroups in use
				NewSystemLockBuilder("l4", 2, 40).
					WithAxScheduling().
					WithPrimaryNodeGroup("memClass:xlarge").
					WithSecondaryNodeGroup(1).
					BuildPending(),
				assertPending("l4"),

				// releasing the pop_xlarge only won't grant l4,
				// given the lock still needs a depop group
				deleteLock("l3"),
				assertPending("l4"),

				// releasing the depop group grants l4
				deleteLock("l2"),
				assertGranted("l4"),
				assertSystemsGranted("system-0", "system-1"),
				// granted: pop_xlarge + depop
				assertNodegroupsGranted("nodegroup0", "nodegroup3"),

				// asking for 2 xlarge groups which we don't have
				NewSystemLockBuilder("l5", 3, 50).
					WithAxScheduling().
					WithPrimaryNodeGroup("memClass:xlarge").
					WithNumPopGroupAnnotation(2).
					WithSecondaryNodeGroup(2).
					BuildPending(),
				// "l5" will not be in cache given it fails the grantable checks
				assertNotExist("l5"),

				// need 1 from primary ng candidates: pop_large
				// need 2 from secondary ng candidates: depop, pop_regular, pop_large, pop_xlarge
				// 1 primary request + 1 secondary request found resource, 1 secondary request pending
				NewSystemLockBuilder("l6", 3, 60).
					WithAxScheduling().
					WithPrimaryNodeGroup("memClass:large").
					WithSecondaryNodeGroup(2).
					BuildPending(),
				assertPending("l6"),

				// a lock with higher priority and less resource requirement
				// need 1 from primary ng candidates: pop_large
				// need 1 from secondary ng candidates: depop, pop_regular, pop_large, pop_xlarge
				// granted: pop_large + pop_regular (used as depop)
				NewSystemLockBuilder("l7", 2, 70).
					WithPriority(50).
					WithAxScheduling().
					WithPrimaryNodeGroup("memClass:large").
					WithSecondaryNodeGroup(1).
					BuildPending(),
				assertGranted("l4", "l7"),
				assertNodegroupsGranted("nodegroup0", "nodegroup3", "nodegroup1", "nodegroup2"),

				NewSystemLockBuilder("l8", 1, 80).
					WithPriority(50).
					WithAxScheduling().
					WithPrimaryNodeGroup("memClass:regular").
					BuildPending(),
				assertPending("l8", "l6"),

				deleteLock("l7"),
				assertGranted("l4", "l8"),
				// only nodegroup1 (pop_large) not spoken for
				assertNodegroupsGranted("nodegroup0", "nodegroup3", "nodegroup2"),

				deleteLock("l4"),
				assertGranted("l8", "l6"),

				deleteLock("l8"),
				assertGranted("l6"),
				// granted: pop_large + depop + pop_xlarge (used as depop)
				// pop_regular wasn't used because l8 had it acquired when l6 got granted
				assertNodegroupsGranted("nodegroup1", "nodegroup3", "nodegroup0"),

				// 2 pop groups can host 2 set of workers
				// we only need 2 additional depop groups for secondary nodegroup grant
				NewSystemLockBuilder("l9", 4, 90).
					WithAxScheduling().
					WithPrimaryNodeGroup().
					WithNumPopGroupAnnotation(2).
					WithSecondaryNodeGroup(3).
					BuildPending(),

				deleteLock("l6"),
				assertGranted("l9"),
				assertSystemsGranted("system-0", "system-1", "system-2", "system-3"),
				// granted: pop_large + pop_regular + depop + pop_xlarge (used as depop)
				assertNodegroupsGranted("nodegroup0", "nodegroup1", "nodegroup2", "nodegroup3"),

				deleteLock("l9"),
				assertResourcesUnallocated,
			},
		},

		{
			name: "ax scheduling happy path",
			clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
				WithSystems(2, "0").WithSystems(2, "1").WithSystems(1, "2").
				WithAX(2, "0").WithAX(2, "1").
				WithBR(12),
			events: []interface{}{
				reinitializeServer{},
				NewSystemLockBuilder("l0", 3, 0).
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithKvssGroupRequest(4, 3, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithUpperBoundsAndLimitRequest("KVStorageServer", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					BuildPending(),
				assertGranted("l0"),
				assertSystemsGranted("system-0", "system-1", "system-2"),
				assertNodesGranted("ax-0", "ax-1", "ax-2"),
				// Pod 0 (ax-0 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-0) -> (system-0: cs-port-0)
				// Pod 4 (ax-0 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-0) -> (system-1: cs-port-0)
				// Pod 8 (ax-2 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-0) -> (system-2: cs-port-0)
				// Pod 1 (ax-0 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-1) -> (system-0: cs-port-3)
				// Pod 5 (ax-0 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-1) -> (system-1: cs-port-3)
				// Pod 9 (ax-2 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-1) -> (system-2: cs-port-3)
				// Pod 2 (ax-1 [index: map[nic-0:cs-port-1 nic-1:cs-port-4]], selected nic-0) -> (system-0: cs-port-6)
				// Pod 6 (ax-1 [index: map[nic-0:cs-port-1 nic-1:cs-port-4]], selected nic-0) -> (system-1: cs-port-6)
				// Pod 10 (ax-2 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-0) -> (system-2: cs-port-6)
				// Pod 3 (ax-1 [index: map[nic-0:cs-port-1 nic-1:cs-port-4]], selected nic-1) -> (system-0: cs-port-9)
				// Pod 7 (ax-1 [index: map[nic-0:cs-port-1 nic-1:cs-port-4]], selected nic-1) -> (system-1: cs-port-9)
				// Pod 11 (ax-2 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-1) -> (system-2: cs-port-9)
				// 6 spine load incurred from connectivity to domain 2 and 3
				assertV2ActBalancedScheduling("l0", 6),

				reinitializeServer{},
				assertNodesGranted("ax-0", "ax-1", "ax-2"),

				NewSystemLockBuilder("l1", 1, 100).
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithKvssGroupRequest(2, 2, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithUpperBoundsAndLimitRequest("KVStorageServer", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					BuildPending(),
				assertGranted("l0", "l1"),
				assertSystemsGranted("system-0", "system-1", "system-2", "system-3"),
				assertNodesGranted("ax-0", "ax-1", "ax-2", "ax-3"),
				// Pod 0 (ax-3 [index: map[nic-0:cs-port-1 nic-1:cs-port-4]], selected nic-0) -> (system-3: cs-port-0)
				// Pod 1 (ax-3 [index: map[nic-0:cs-port-1 nic-1:cs-port-4]], selected nic-1) -> (system-3: cs-port-3)
				// Pod 2 (ax-3 [index: map[nic-0:cs-port-1 nic-1:cs-port-4]], selected nic-0) -> (system-3: cs-port-6)
				// Pod 3 (ax-3 [index: map[nic-0:cs-port-1 nic-1:cs-port-4]], selected nic-1) -> (system-3: cs-port-9)
				// pod-level cs-port assignment currently is not ax node affinity aware
				// this is a known limitation for small size jobs with very few activations
				// can be optimized later
				assertV2ActBalancedScheduling("l1", 4),

				reinitializeServer{},
				NewSystemLockBuilder("l2", 1, 200).
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithKvssGroupRequest(2, 2, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithUpperBoundsAndLimitRequest("KVStorageServer", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					BuildPending(),
				// pending due to lacking AX nodes in stamp "2"
				assertPending("l2"),

				deleteLock("l0"),
				assertGranted("l1", "l2"),
				assertSystemsGranted("system-3", "system-0"),
				assertNodesGranted("ax-3", "ax-0"),
				// Pod 0 (ax-0 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-0) -> (system-0: cs-port-0)
				// Pod 1 (ax-0 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-1) -> (system-0: cs-port-3)
				// Pod 2 (ax-0 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-0) -> (system-0: cs-port-6)
				// Pod 3 (ax-0 [index: map[nic-0:cs-port-0 nic-1:cs-port-3]], selected nic-1) -> (system-0: cs-port-9)
				// Same job spec as "l1", but we are "lucky" to have less spine load here
				assertV2ActBalancedScheduling("l2", 2),

				deleteLock("l1"),
				deleteLock("l2"),
				assertResourcesUnallocated,
			},
		},
		{
			name: "ax scheduling happy path with job sharing",
			clusterBuilder: helpers.NewClusterBuilder(3).WithV2Network().
				WithSystems(2, "0").WithSystems(2, "1").WithSystems(1, "2").
				WithAX(2, "0").WithAX(2, "1").
				WithBR(12),

			events: []interface{}{
				reinitializeServer{},
				NewSystemLockBuilder("l0", 3, 0).
					EnableJobSharing().
					IsInference().
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithKvssGroupRequest(4, 3, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithUpperBoundsAndLimitRequest("KVStorageServer", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					BuildPending(),
				assertGranted("l0"),
				assertSystemsGranted("system-0", "system-1", "system-2"),
				assertNodesGranted("ax-0", "ax-1", "ax-2"),

				reinitializeServer{},
				assertNodesGranted("ax-0", "ax-1", "ax-2"),

				NewSystemLockBuilder("l1", 1, 100).
					EnableJobSharing().
					IsInference().
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithKvssGroupRequest(2, 2, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithUpperBoundsAndLimitRequest("KVStorageServer", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					BuildPending(),
				assertGranted("l0", "l1"),
				assertSystemsGranted("system-0", "system-1", "system-2", "system-3"),
				assertNodesGranted("ax-0", "ax-1", "ax-2", "ax-3"),

				reinitializeServer{},
				NewSystemLockBuilder("l2", 1, 200).
					EnableJobSharing().
					IsInference().
					WithAxScheduling().
					WithChfGroupRequest(helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/8).
					WithActGroupRequest(4, helpers.DefaultAxNodeCpu/8, helpers.DefaultAxNodeMem/16).
					WithUpperBoundsAndLimitRequest("Activation", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					WithKvssGroupRequest(2, 2, helpers.DefaultAxNodeCpu/16, helpers.DefaultAxNodeMem/32).
					WithUpperBoundsAndLimitRequest("KVStorageServer", helpers.DefaultAxNodeMem/8, helpers.DefaultAxNodeMem/4).
					BuildPending(),
				// pending due to lacking AX nodes in stamp "2"
				assertPending("l2"),

				deleteLock("l0"),
				assertGranted("l1", "l2"),
				assertSystemsGranted("system-3", "system-0"),
				assertNodesGranted("ax-3", "ax-0"),

				deleteLock("l1"),
				deleteLock("l2"),
				assertResourcesUnallocated,
			},
		},

		{
			name:           "updating lock priority",
			clusterBuilder: clusterBuilder,

			events: []interface{}{
				reinitializeServer{},
				NewSystemCoordinatorLockBuilder("l0", 2, systemsTotal, 0).
					WithPrimaryNodeGroup("group:nodegroup0").
					WithSecondaryNodeGroup(1).
					WithPriority(50).
					BuildPending(),
				assertGranted("l0"),

				NewSystemCoordinatorLockBuilder("l1", 2, systemsTotal, 100).
					WithPrimaryNodeGroup("group:nodegroup0").
					WithSecondaryNodeGroup(1, "group:nodegroup1").
					WithPriority(50).
					BuildPending(),
				assertPending("l1"),

				NewSystemCoordinatorLockBuilder("l2", 2, systemsTotal, 100).
					WithPrimaryNodeGroup("group:nodegroup0").
					WithSecondaryNodeGroup(1, "group:nodegroup1").
					WithPriority(50).
					BuildPending(),
				assertPendingOrdered("l1", "l2"),

				NewSystemCoordinatorLockBuilder("l3", 2, systemsTotal, 200).
					WithPrimaryNodeGroup("group:nodegroup0").
					WithSecondaryNodeGroup(1, "group:nodegroup1").
					WithPriority(50).
					BuildPending(),
				assertPendingOrdered("l1", "l2", "l3"),

				updateLockPriority{lockName: "l2", priority: 10},
				assertPendingOrdered("l2", "l1", "l3"),

				updateLockPriority{lockName: "l3", priority: 5},
				assertPendingOrdered("l3", "l2", "l1"),

				deleteLock("l0"),
				deleteLock("l1"),
				deleteLock("l2"),
				deleteLock("l3"),
				assertResourcesUnallocated,
			},
		},

		{
			name:           "prioritized compile deadlock detection",
			clusterBuilder: clusterBuilder,

			events: []interface{}{
				reinitializeServer{},
				NewSystemCoordinatorLockBuilder("execute-0", 1, 6, 0).
					WithPrimaryNodeGroup("group:nodegroup0").
					WithWorkflowId("workflow-0").
					BuildPending(),
				NewSystemCoordinatorLockBuilder("execute-1", 1, 6, 10).
					WithPrimaryNodeGroup("group:nodegroup1").
					WithWorkflowId("workflow-1").
					BuildPending(),
				NewSystemCoordinatorLockBuilder("execute-2", 1, 6, 20).
					WithPrimaryNodeGroup("group:nodegroup2").
					WithWorkflowId("workflow-2").
					BuildPending(),
				assertGranted("execute-0", "execute-1", "execute-2"), // 3/6 mem used

				NewPrioritizedCompileLockBuilder("compile-0", 3, 6).
					WithWorkflowId("workflow-0").
					WithParentLock("execute-0").
					BuildPending(),
				assertGranted("execute-0", "execute-1", "execute-2", "compile-0"),
				deleteLock("compile-0"),

				NewPrioritizedCompileLockBuilder("compile-0a", 6, 6).
					WithWorkflowId("workflow-0").
					WithParentLock("execute-0").
					BuildPending(),
				assertFailed("compile-0a"), // deadlock candidate that immediately fails
				assertMostRecentEvent(
					corev1.EventTypeWarning,
					EventSchedulingFailed,
					"Prioritized-compile job job-operator/compile-0a could not be scheduled due to resource contention "+
						"from execute job job-operator/execute-0 from the same workflow."),
				deleteLock("compile-0a"),

				NewCompileLockBuilder("compile-x", 6, 6).
					WithWorkflowId("workflow-x").
					BuildPending(),
				assertPending("compile-x"), // not a deadlock candidate
				deleteLock("compile-x"),

				NewPrioritizedCompileLockBuilder("compile-1a", 4, 6).
					WithWorkflowId("workflow-1").
					WithParentLock("execute-1").
					WithPriority(299).
					BuildPending(),
				assertPending("compile-1a"), // deadlock candidate on a timeout

				NewPrioritizedCompileLockBuilder("compile-0b", 4, 6).
					WithWorkflowId("workflow-0").
					WithParentLock("execute-0").
					WithPriority(299).
					BuildPending(),
				// "compile-0b" is prioritized over "compile-1a" given the "execute-0" starts earlier than "execute-1"
				// "compile-0b" sees "execute-0:1", "execute-1:1" and "compile-0b:4" while total mem is 6
				// this means no deadlock - "compile-0" can be granted once "execute-2" completes
				assertPending("compile-0b", "compile-1a"),

				deleteLock("compile-1a"),
				NewPrioritizedCompileLockBuilder("compile-1b", 4, 6).
					WithWorkflowId("workflow-1").
					WithParentLock("execute-1").
					WithPriority(199).
					BuildPending(),
				// "compile-1b" is prioritized over "compile-0b" given higher priority
				// "compile-1b" sees "execute-1:1", "execute-0:1" and "compile-1b:4" while total mem is 6
				// this means no deadlock - "compile-1b" can be granted once "execute-2" completes
				assertPending("compile-1b", "compile-0b"),

				NewPrioritizedCompileLockBuilder("compile-2a", 1, 6).
					WithWorkflowId("workflow-2").
					WithParentLock("execute-2").
					WithPriority(399).
					BuildPending(),
				// "compile-2a" introduces the third deadlock candidate
				// "compile-1b" sees "execute-1:1", "execute-0:1", "execute-2:1" and "compile-1b:4" which is greater than 6
				// The least prioritized deadlock candidate ("compile-2a") should get evicted
				assertFailed("compile-2a"),
				assertMostRecentEvent(
					corev1.EventTypeWarning,
					EventSchedulingFailed,
					"Prioritized-compile job job-operator/compile-2a could not be scheduled due to resource contention "+
						"and lost to prioritized-compile job(s) [job-operator/compile-1b job-operator/compile-0b] of higher priority from being scheduled."),
				assertPendingOrdered("compile-1b", "compile-0b"),
				deleteLock("compile-2a"),

				NewPrioritizedCompileLockBuilder("compile-2b", 5, 6).
					WithWorkflowId("workflow-2").
					WithParentLock("execute-2").
					WithPriority(99).
					BuildPending(),
				// "compile-2b" introduces the third deadlock candidate, though with the highest priority this time
				// "compile-2b" sees "execute-2:1", "execute-1:1", "execute-0:1" and "compile-2b:5", which is greater than 6
				// Candidates to be evicted: "compile-0b", "compile-1b" and evict "compile-0b" first
				assertFailed("compile-0b"),
				assertMostRecentEvent(
					corev1.EventTypeWarning,
					EventSchedulingFailed,
					"Prioritized-compile job job-operator/compile-1b could not be scheduled due to resource contention "+
						"and lost to prioritized-compile job(s) [job-operator/compile-2b] of higher priority from being scheduled."),
				deleteLock("compile-0b"),
				deleteLock("execute-0"),
				assertFailed("compile-1b"),
				deleteLock("compile-1b"),

				assertGranted("execute-2", "execute-1"),
				assertPending("compile-2b"),
				updateLockGrantDeadline{"compile-2b", 1},
				// "compile-2b" should time out
				assertFailed("compile-2b"),
				assertMostRecentEvent(
					corev1.EventTypeWarning,
					EventSchedulingFailed,
					"Prioritized-compile job-operator/compile-2b was stuck in pending state for too long and could not be granted."),

				deleteLock("compile-2b"),
				deleteLock("execute-1"),
				deleteLock("execute-2"),
				assertResourcesUnallocated,
			},
		},

		{
			name: "nvmf disk space granting",
			// coordinator-[0-1]: nvmf enabled
			// coordinator-2: nvmf disabled
			// nodegroup[0-1]: nvmf disabled
			// nodegroup[2-3]: nvmf enabled
			clusterBuilder: helpers.NewClusterBuilder(3).WithNvmfCoordinatorNodesEnabled(2, 200).
				WithV2Network().
				WithSystems(2, "0").WithSystems(2, "1").WithSystems(1, "2").
				WithAX(2, "0").WithAX(2, "1").
				WithBR(12).
				WithPopulatedNodeGroups(2, wsapisv1.PopNgTypeRegular).
				WithNvmfEnabledPopulatedNodegroups(2, wsapisv1.PopNgTypeRegular),

			events: []interface{}{
				reinitializeServer{},
				// force jobs to land on "coordinator-0" for simpler testing
				// not doing this would make coordinators load balanced amongst nodes
				setResourceHealth{"node/coordinator-1", resource.HealthErr},
				setResourceHealth{"node/coordinator-2", resource.HealthErr},

				// l0: coordinator-0, nodegroup2, 1x nvmf enabled
				NewSystemLockBuilder("l0", 1, 0).
					WithAxScheduling().
					WithCoordinatorRequest(1, 1, map[string]string{
						wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "60",
						wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "120",
					}).
					// use a nodegroup where nvmf is enabled
					WithPrimaryNodeGroupWithNumWeights(4, "group:nodegroup2").
					BuildPending(),
				assertGranted("l0"),
				assertNodegroupsGranted("nodegroup2"),
				assertNvmfSpaceGranted("l0", "1", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 120}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 60}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 60}},
				}),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-0", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 120}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 60}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 60}},
				}),

				// grants should persist on reinitializing the server
				reinitializeServer{},
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-0", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 120}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 60}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 60}},
				}),

				// l0: coordinator-0, nodegroup2, 1x nvmf granted
				// l1: coordinator-0, nodegroup0, nvmf not enabled on ng
				NewSystemLockBuilder("l1", 1, 10).
					WithAxScheduling().
					WithCoordinatorRequest(1, 1, map[string]string{
						wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "60",
						wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "120",
					}).
					// use a nodegroup where nvmf is disabled
					WithPrimaryNodeGroup("group:nodegroup0").
					BuildPending(),
				assertGranted("l0", "l1"),
				assertNodegroupsGranted("nodegroup2", "nodegroup0"),
				assertNvmfSpaceGranted("l1", "", nil),

				// l0: coordinator-0, nodegroup2, 1x nvmf granted
				// l1: coordinator-0, nodegroup0, nvmf not enabled on ng
				// l2: coordinator-0, nodegroup3, 2x nvmf granted
				NewSystemLockBuilder("l2", 1, 20).
					WithAxScheduling().
					WithCoordinatorRequest(1, 1, map[string]string{
						wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "10",
						wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "20",
					}).
					// use a nodegroup where nvmf is enabled
					WithPrimaryNodeGroupWithNumWeights(2, "group:nodegroup3").
					BuildPending(),
				assertGranted("l0", "l1", "l2"),
				assertNodegroupsGranted("nodegroup2", "nodegroup0", "nodegroup3"),
				assertNvmfSpaceGranted("l2", "1", resource.SubresourceRanges{
					resource.NvmfDiskResName(2): []resource.Range{{Start: 60, End: 80}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 60, End: 80}},
				}),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-0", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 120}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 80}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 80}},
				}),

				deleteLock("l0"),
				assertGranted("l1", "l2"),
				assertNodegroupsGranted("nodegroup0", "nodegroup3"),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-0", resource.SubresourceRanges{
					resource.NvmfDiskResName(2): []resource.Range{{Start: 60, End: 80}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 60, End: 80}},
				}),

				// l1: coordinator-0, nodegroup0, nvmf not enabled on ng
				// l2: coordinator-0, nodegroup3, 2x nvmf granted
				// l3: coordinator-0, nodegroup2, 2x nvmf granted
				NewSystemLockBuilder("l3", 1, 30).
					WithAxScheduling().
					WithCoordinatorRequest(1, 1, map[string]string{
						wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "10",
						wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "20",
					}).
					// use a nodegroup where nvmf is enabled
					WithPrimaryNodeGroupWithNumWeights(5, "group:nodegroup2").
					BuildPending(),
				assertGranted("l1", "l2", "l3"),
				assertNodegroupsGranted("nodegroup0", "nodegroup3", "nodegroup2"),
				// grants first ranges in the beginning
				assertNvmfSpaceGranted("l3", "1", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 40}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 40}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 20}},
				}),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-0", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 40}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 40}, {Start: 60, End: 80}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 20}, {Start: 60, End: 80}},
				}),

				deleteLock("l3"),
				// l1: coordinator-0, nodegroup0, nvmf not enabled on ng
				// l2: coordinator-0, nodegroup3, 2x nvmf granted
				// l4: coordinator-0, nodegroup2, nvmf enabled but not granted due to unavailability
				NewSystemLockBuilder("l4", 1, 40).
					WithAxScheduling().
					// nvmf disk space is not granted, given no contiguous space of this size is available
					WithCoordinatorRequest(1, 1, map[string]string{
						wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "191",
						wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "382",
					}).
					// use a nodegroup where nvmf is enabled
					WithPrimaryNodeGroupWithNumWeights(3, "group:nodegroup2").
					BuildPending(),
				assertGranted("l1", "l2", "l4"),
				assertNodegroupsGranted("nodegroup0", "nodegroup3", "nodegroup2"),
				assertNvmfSpaceGranted("l4", "", nil),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-0", resource.SubresourceRanges{
					resource.NvmfDiskResName(2): []resource.Range{{Start: 60, End: 80}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 60, End: 80}},
				}),

				deleteLock("l4"),
				setResourceHealth{"node/coordinator-1", resource.HealthOk},
				// l1: coordinator-0, nodegroup0, nvmf not enabled on ng
				// l2: coordinator-0, nodegroup3, 2x nvmf granted
				// l5: coordinator-1, nodegroup2, 1x nvmf granted
				NewSystemLockBuilder("l5", 1, 50).
					WithAxScheduling().
					// coordinator-1 gets chosen due to given coordinator-0 does not have sufficient memory resource remaining
					WithCoordinatorRequest(1, helpers.DefaultMgtNodeMem, map[string]string{
						wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "191",
						wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "382",
					}).
					// use a nodegroup where nvmf is enabled
					WithPrimaryNodeGroupWithNumWeights(3, "group:nodegroup2").
					BuildPending(),
				assertGranted("l1", "l2", "l5"),
				assertNodegroupsGranted("nodegroup0", "nodegroup3", "nodegroup2"),
				assertNvmfSpaceGranted("l5", "2", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 191}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 191}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 191}},
				}),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-0", resource.SubresourceRanges{
					resource.NvmfDiskResName(2): []resource.Range{{Start: 60, End: 80}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 60, End: 80}},
				}),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-1", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 191}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 191}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 191}},
				}),

				deleteLock("l2"),
				setResourceHealth{"node/coordinator-2", resource.HealthOk},
				// l1: coordinator-0, nodegroup0, nvmf not enabled on ng
				// l5: coordinator-1, nodegroup2, 1x nvmf granted
				// l6: coordinator-2, nodegroup3, nvmf not enabled on coord
				NewSystemLockBuilder("l6", 1, 60).
					WithAxScheduling().
					// coordinator-2 gets chosen due to given other nodes do not have sufficient memory resource remaining
					// this coord node does not have nvmf enabled
					WithCoordinatorRequest(1, helpers.DefaultMgtNodeMem, map[string]string{
						wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey:   "191",
						wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey: "382",
					}).
					// use a nodegroup where nvmf is enabled
					WithPrimaryNodeGroupWithNumWeights(3, "group:nodegroup3").
					BuildPending(),
				assertGranted("l1", "l5", "l6"),
				assertNodegroupsGranted("nodegroup0", "nodegroup2", "nodegroup3"),
				assertNvmfSpaceGranted("l6", "", nil),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-0", nil),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-1", resource.SubresourceRanges{
					resource.NvmfDiskResName(1): []resource.Range{{Start: 0, End: 191}},
					resource.NvmfDiskResName(2): []resource.Range{{Start: 0, End: 191}},
					resource.NvmfDiskResName(3): []resource.Range{{Start: 0, End: 191}},
				}),
				assertSubresourceRangesGranted(resource.NodeType, "coordinator-2", nil),

				deleteLock("l1"),
				deleteLock("l5"),
				deleteLock("l6"),
				assertResourcesUnallocated,
			},
		},

		{
			name: "management/coordinator separation",
			clusterBuilder: helpers.NewClusterBuilderWithMgmtCoordSeparation(4, 1).
				WithV2Network().WithSystems(4, "0"),

			events: []interface{}{
				reinitializeServer{},
				NewSystemLockBuilder("l0", 0, 0).
					WithCoordinatorRequest(1, 1, nil).
					BuildPending(),
				assertGranted("l0"),
				assertNodesGranted("coordinator-0"),

				NewSystemLockBuilder("l1", 0, 10).
					WithCoordinatorRequest(helpers.DefaultMgtNodeCpu-1, helpers.DefaultMgtNodeMem-1, nil).
					BuildPending(),
				assertGranted("l0", "l1"),
				assertNodesGranted("coordinator-0"),

				NewSystemLockBuilder("l2", 0, 20).
					WithCoordinatorRequest(helpers.DefaultMgtNodeCpu, helpers.DefaultMgtNodeMem, nil).
					BuildPending(),
				assertPending("l2"),
				assertNodesGranted("coordinator-0"),

				reinitializeServer{},

				deleteLock("l0"),
				assertGranted("l1"),
				assertNodesGranted("coordinator-0"),
				assertPending("l2"),

				deleteLock("l1"),
				assertGranted("l2"),
				assertNodesGranted("coordinator-0"),

				deleteLock("l2"),
				assertResourcesUnallocated,
			},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {

			clientBldr := newClientBuilder(testcase.initPend, testcase.initGrant)
			c := testcase.clusterBuilder.Build()
			cfg := testcase.clusterBuilder.GetCfg()

			if testcase.clusterBuilder.IsV2NetworkEnabled() {
				wsapisv1.IsV2Network = true
				defer func() {
					wsapisv1.IsV2Network = false
				}()

				numAxNodes := 0
				numIxNodes := 0
				for _, res := range c.List() {
					if _, ok := res.GetProp(resource.RoleAxPropKey); ok {
						numAxNodes++
					}
					if _, ok := res.GetProp(resource.RoleIxPropKey); ok {
						numIxNodes++
					}
				}
				if numAxNodes > 0 {
					wsapisv1.UseAxScheduling = true
					wsapisv1.SystemStampsDetected = true
					wsapisv1.AxCountPerSystem = int(math.Ceil(float64(numAxNodes) / float64(len(cfg.SystemMap))))
					defer func() {
						wsapisv1.UseAxScheduling = false
						wsapisv1.SystemStampsDetected = false
						wsapisv1.AxCountPerSystem = 0
					}()
				}

				if numIxNodes > 0 {
					wsapisv1.UseIxScheduling = true
					wsapisv1.SystemStampsDetected = true
					// Check if IX job sharing should be enabled
					if testcase.clusterBuilder.IsIxJobSharingEnabled() {
						resource_config.MaxSwDriverPodsPerIxNode = 2
						defer func() {
							resource_config.MaxSwDriverPodsPerIxNode = resource_config.DefaultSwDriverPodsPerIxNode
						}()
					}
					defer func() {
						wsapisv1.UseIxScheduling = false
						wsapisv1.SystemStampsDetected = false
					}()
				}
			}
			if testcase.clusterBuilder.IsManagementSeparationEnforced() {
				wsapisv1.ManagementSeparationEnforced = true
				defer func() {
					wsapisv1.ManagementSeparationEnforced = false
				}()
			}
			reconciler := newLockReconciler(c, cfg)
			recorder := fakeEventRecorder{}
			reconciler.recorder = &recorder
			reconciler.client = clientBldr.build()

			_, e := reconciler.Initialize(ctx)
			require.NoError(t, e)

			assert.True(t, reconciler.initialized)
			assert.Len(t, getGranted(reconciler), len(testcase.initGrant))
			assert.Len(t, getPendingNames(reconciler), len(testcase.initPend))
			assert.ElementsMatch(t,
				common.Map(func(l *rlv1.ResourceLock) string { return l.Name }, testcase.initPend),
				getPendingNames(reconciler),
			)
			require.NoError(t, reconciler.grantPending(ctx))

			for i, e := range testcase.events {
				var req ctrl.Request
				if name, isDelete := e.(deleteLock); isDelete {
					clientBldr.remove(string(name))
					req = ctrl.Request{NamespacedName: types.NamespacedName{Name: string(name), Namespace: common.Namespace}}
				} else if _, isReinitialize := e.(reinitializeServer); isReinitialize {
					reconciler.resources = reconciler.resources.UnallocatedCopy()
					reconciler.initialized = false
					// ok if req is empty, should only initialize
				} else if upd1, isLockPriorityUpdate := e.(updateLockPriority); isLockPriorityUpdate {
					clientBldr.updateLockPriority(upd1.lockName, upd1.priority)
					req = ctrl.Request{NamespacedName: types.NamespacedName{Name: upd1.lockName, Namespace: common.Namespace}}
				} else if upd2, isGrantDeadlineUpdate := e.(updateLockGrantDeadline); isGrantDeadlineUpdate {
					clientBldr.updateGracePeriod(upd2.lockName, upd2.gracePeriodSec)
					req = ctrl.Request{NamespacedName: types.NamespacedName{Name: upd2.lockName, Namespace: common.Namespace}}
				} else if up3, isHealthUpdate := e.(setResourceHealth); isHealthUpdate {
					_, ok := reconciler.resources.Get(up3.resourceId)
					require.True(t, ok)
					reconciler.resources.UpdateProp(up3.resourceId, resource.HealthProp, up3.status)
				} else if rl, isAdd := e.(*rlv1.ResourceLock); isAdd {
					clientBldr.add(rl)
					req = ctrl.Request{NamespacedName: types.NamespacedName{Name: rl.Name, Namespace: rl.Namespace}}
				} else if condition, isCondition := e.(assertCondition); isCondition {
					if !condition(t, reconciler) {
						t.Fatalf("condition at step %d failed, discontinuing test", i)
					}
				} else {
					panic("unexpected event type")
				}

				reconciler.client = clientBldr.build()
				_, err := reconciler.Reconcile(ctx, req)
				require.NoError(t, err)

				updatedLocks := rlv1.ResourceLockList{}
				require.NoError(t, reconciler.client.List(ctx, &updatedLocks))
				var updatedClientObjs []client.Object
				for i := range updatedLocks.Items {
					updatedClientObjs = append(updatedClientObjs, &updatedLocks.Items[i])
				}
				clientBldr.objs = updatedClientObjs
			}
		})
	}
}

func TestReconciler_StaleCacheOnInit(t *testing.T) {
	// ensure that a pending lock cannot be added to reconciler on initialize() after it was previously granted
	// this can happen at runtime due to caching in the controller-manager's client
	ctx := context.Background()
	cluster := helpers.NewCluster(2, 6)
	clientBldr := newClientBuilder()
	reconciler := newLockReconciler(cluster, &common.ClusterConfig{})
	reconciler.recorder = &fakeEventRecorder{}
	reconciler.client = clientBldr.build()

	_, e := reconciler.Initialize(ctx)
	require.NoError(t, e)

	l := NewSystemCoordinatorLockBuilder("l0", 1, 2, 0).BuildPending()
	clientBldr.add(l)
	reconciler.client = clientBldr.build()
	levent := ctrl.Request{NamespacedName: types.NamespacedName{Name: l.Name, Namespace: common.Namespace}}
	_, err := reconciler.Reconcile(ctx, levent)
	require.NoError(t, err)

	updatedLock := &rlv1.ResourceLock{}
	err = reconciler.client.Get(ctx, client.ObjectKeyFromObject(l), updatedLock)
	require.NoError(t, err)
	require.Equal(t, rlv1.LockGranted, updatedLock.Status.State)

	reconciler.initialized = false
	reconciler.resources = reconciler.resources.UnallocatedCopy()
	updatedClient := reconciler.client
	reconciler.client = clientBldr.build()
	_, err = reconciler.Reconcile(ctx, levent)
	require.Error(t, err)
	require.Contains(t, err.Error(), "found known granted lock")

	reconciler.client = updatedClient
	_, err = reconciler.Reconcile(ctx, levent)
	require.NoError(t, err)
}

func getPendingNames(r *Reconciler) []string {
	var pendingNames []string
	for _, l := range r.locks {
		if !l.IsGranted() {
			pendingNames = append(pendingNames, strings.Split(l.Name, "/")[1])
		}
	}
	return pendingNames
}

func getPendingNamesOrdered(r *Reconciler) []string {
	q := newQueue("test", nil)
	for _, l := range r.locks {
		if !l.IsGranted() && !l.HadFailed() {
			q.Requeue(l)
		}
	}
	var pendingNames []string
	for _, l := range q.data {
		pendingNames = append(pendingNames, l.Obj.Name)
	}
	return pendingNames
}

func getGranted(r *Reconciler) map[string]*lock {
	granted := map[string]*lock{}
	for i, lock := range r.locks {
		if lock.IsGranted() {
			granted[strings.Split(lock.Name, "/")[1]] = r.locks[i]
		}
	}
	return granted
}

// example rv: {"primary": [nodegroup1], "secondary": [nodegroup2, nodegroup3]}
func getNodeGroups(r *Reconciler, lock string) map[string][]string {
	rv := map[string][]string{}
	l := getGranted(r)[lock]
	if l == nil {
		return rv
	}
	for _, ggList := range l.GroupGrants {
		for _, gg := range ggList {
			rv[gg.Request.Id] = append(rv[gg.Request.Id], gg.GroupVal)
		}
	}
	return rv
}

//go:build default

package lock

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	cr "sigs.k8s.io/controller-runtime"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource/helpers"
)

/**
Perf test

Simple performance test that can be used to check the speed of a reconcile.
The basic idea is that we do a warm up of the scheduler by adding locks until
a certain set point is reached. Then we begin timing, creating or deleting
locks randomly but always keeping the number of pending locks within a certain
bounds.

You can run the perf test with the following command

  go test ./controllers/resourcelock -test.run=^$ -test.bench=.
  ...

Previous tests
  2023-02 M1-Pro-8core BenchmarkReconcile-8        1196           1063099 ns/op
  2023-01 ============= large code change to support cpu/mem alloc ============
  2022-10 M1-Pro-8core BenchmarkReconcile-8       10000            100187 ns/op
  2022-09 M1-Pro-8core BenchmarkReconcile-8       12199             99508 ns/op
*/

var lockCounter = 0

const (
	nsys          = 16
	brCount       = 60
	targetPending = 20
)

func generateLock() *rlv1.ResourceLock {
	defer func() {
		lockCounter += 1
	}()

	return NewLockBuilder(fmt.Sprintf("l%d", lockCounter)).
		WithCreateTime(int64(lockCounter)).
		WithSystemRequest(2).
		WithQueue(rlv1.ExecuteQueueName).
		WithCoordinatorRequest(helpers.DefaultMgtNodeCpu/nsys, helpers.DefaultMemNodeMem/nsys, nil).
		WithPrimaryNodeGroup().
		WithSecondaryNodeGroup(1).
		BuildPending()
}

func BenchmarkReconcile(b *testing.B) {
	ctx := context.TODO()
	c := helpers.NewCluster(nsys, brCount)

	// increase the memory on the mgmt node so that we can run more jobs
	c.Remove("node/management")
	c.Add(resource.NewResource(
		rlv1.NodeResourceType,
		"management",
		map[string]string{resource.RoleMgmtPropKey: ""},
		resource.Subresources{
			resource.CPUSubresource: helpers.DefaultMgtNodeCpu,
			resource.MemSubresource: helpers.DefaultMgtNodeMem,
			resource.PodSubresource: 32},
		nil))

	lockNames := make([]string, 0, targetPending*2)
	clientBldr := newClientBuilder()
	reconciler := newLockReconciler(c, &common.ClusterConfig{})
	recorder := fakeEventRecorder{}
	reconciler.recorder = &recorder
	reconciler.client = clientBldr.build()

	_, e := reconciler.Initialize(ctx)
	require.NoError(b, e)
	for len(reconciler.getQueue(rlv1.ExecuteQueueName).data) < targetPending {
		l := generateLock()
		lockNames = append(lockNames, l.Name)
		reconciler.client = clientBldr.add(l).build()
		_, err := reconciler.Reconcile(ctx, cr.Request{NamespacedName: types.NamespacedName{
			Namespace: l.Namespace,
			Name:      l.Name,
		}})
		if err != nil {
			b.Fatalf("error during initialization reconcile, %v", err)
		}
	}

	loBound, hiBound := int(float64(targetPending)*0.8), int(float64(targetPending)*1.2)
	createCount, deleteCount, hitLoBound, hitHiBound := 0, 0, 0, 0
	recorder.grantCount = 0
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		recorder.dropEvents()
		b.StopTimer()
		create := true
		countPend := len(reconciler.getQueue(rlv1.ExecuteQueueName).data)
		if countPend >= hiBound {
			create = false
			hitHiBound += 1
		} else if countPend > loBound {
			create = rand.Intn(2) == 0
		} else {
			hitLoBound += 1
		}

		var r cr.Request
		if create {
			l := generateLock()
			lockNames = append(lockNames, l.Name)
			reconciler.client = clientBldr.add(l).build()
			r = cr.Request{NamespacedName: types.NamespacedName{Namespace: l.Namespace, Name: l.Name}}
			createCount += 1
		} else {
			var target string
			target, lockNames = selectDeleteTarget(reconciler, lockNames)
			reconciler.client = clientBldr.remove(target).build()
			r = cr.Request{NamespacedName: types.NamespacedName{Namespace: common.Namespace, Name: target}}
			deleteCount += 1
		}
		b.StartTimer()
		_, err := reconciler.Reconcile(ctx, r)
		if err != nil {
			b.Fatalf("failed during create=%v, %v", create, err)
		}
	}
	if recorder.invalidCount > 0 {
		b.Fatalf("produced some invalid events %d but should have produced none", recorder.invalidCount)
	}
	b.Logf("create/delete/grant (%d/%d/%d) locks total", createCount, deleteCount, recorder.grantCount)
	b.Logf("hit lo/hi bounds (%d/%d) total", hitLoBound, hitHiBound)
}

// selectDeleteTarget chooses granted locks at a higher rate than pending locks as targets for deletion since
// these deletes will likely trigger a reconcile+grant cycle resulting in grants which should be more expensive and
// realistic.
func selectDeleteTarget(reconciler *Reconciler, lockNames []string) (string, []string) {
	var target string
	if rand.Intn(10) < 8 {
		var granted []string
		for _, l := range reconciler.locks {
			if l.IsGranted() {
				granted = append(granted, l.Obj.Name)
			}
		}
		if len(granted) == 0 {
			return remove(lockNames, rand.Intn(len(lockNames)))
		}
		target = granted[rand.Intn(len(granted))]
	} else {
		defaultQ := reconciler.getQueue(rlv1.ExecuteQueueName).data
		target = defaultQ[rand.Intn(len(defaultQ))].Obj.Name
	}
	for i, name := range lockNames {
		if name == target {
			return remove(lockNames, i)
		}
	}
	panic("target lock wasn't found in lock names")
}

func remove(arr []string, i int) (string, []string) {
	v := arr[i]
	last := len(arr) - 1
	arr[i] = arr[last]
	return v, arr[:last]
}

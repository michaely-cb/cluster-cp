//go:build integration && resource_controller

/*
Copyright 2024 Cerebras Systems, Inc..

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
package resource

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	"cerebras.com/job-operator/common/resource/helpers"
	"cerebras.com/job-operator/controllers/resource/lock"

	wscommon "cerebras.com/job-operator/common"
	//+kubebuilder:scaffold:imports
)

// Generates random config reload events. Should also generate random NS + Health updates for completeness
// While running wsjobs in order to flush out concurrency related bugs
func TestRandomSmoke(t *testing.T) {
	wscommon.InitLogging()
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
	k8sClient = mgr.GetClient()
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
	}()

	collection := helpers.NewCluster(systemCount, brNum)
	controller := newController(collection)
	err := controller.SetupWithManager(mgr)
	require.NoError(t, err)

	t.Run("randomization smoke test", func(t *testing.T) {
		// TODO: make the test data more realistic: a mix of compile and train jobs using a variety of counts of CS2

		// Create some number of workers who request locks, and once being granted locks, they wait some random amount
		// of time before releasing it. The total set of workers should finish prior to the test timeout.
		randomLock := func(id int) *rlv1.ResourceLock {
			name := fmt.Sprintf("random-lock-%d", id)
			if rand.Intn(2) == 1 { // half of locks are type compile
				return lock.NewLockBuilder(name).
					WithQueue(rlv1.CompileQueueName).
					WithCompileSystemRequest().
					WithCoordinatorRequest(helpers.DefaultMemNodeCpu/4, helpers.DefaultMemNodeMem/4, nil).
					BuildPending()
			}
			sysReqCount := rand.Intn(systemCount) + 1

			systemNames := ""
			if rand.Intn(2) == 1 { // half of locks will request specific system names
				selected := rand.Perm(systemCount)[:sysReqCount]
				for _, i := range selected {
					systemNames += fmt.Sprintf("system-%d,", i)
				}
				systemNames = "name:" + systemNames[:len(systemNames)-1]
			}

			lb := lock.NewLockBuilder(name).
				WithQueue(rlv1.ExecuteQueueName).
				WithSystemRequest(sysReqCount, systemNames).
				WithCoordinatorRequest(helpers.DefaultMemNodeCpu/8, helpers.DefaultMemNodeMem/8, nil).
				WithPrimaryNodeGroup()

			if rand.Intn(2) == 1 { // half of locks will get upperbounds requests
				lb = lb.WithUpperBoundsRequest(rlv1.WeightRequestName, helpers.DefaultWrkNodeMem*2)
			}

			if sysReqCount == 1 {
				return lb.BuildPending()
			}
			return lb.WithSecondaryNodeGroup(sysReqCount - 1).BuildPending()
		}

		const TimeLimit = time.Second * 30
		timeout := time.Now().Add(TimeLimit)
		const NumWorkers = 100
		wg := sync.WaitGroup{}
		wg.Add(NumWorkers)
		errmu := sync.Mutex{}
		var errors []error
		for i := 0; i < NumWorkers; i++ {
			go func(id int) {
				time.Sleep(time.Duration(rand.Intn(3000)+1) * time.Millisecond)
				lock := randomLock(id)
				assert.NoError(t, k8sClient.Create(ctx, lock))

				lockName := client.ObjectKeyFromObject(lock)
				for time.Now().Before(timeout) {
					err = k8sClient.Get(ctx, lockName, lock)
					if err != nil && !apierrors.IsNotFound(err) {
						errmu.Lock()
						errors = append(errors, err)
						errmu.Unlock()
						break
					}

					if lock != nil && lock.Status.State == rlv1.LockError {
						errmu.Lock()
						errors = append(errors, fmt.Errorf("lock error: %s", lock.Status.Message))
						errmu.Unlock()
						break
					}

					if lock != nil && lock.Status.State == rlv1.LockGranted {
						time.Sleep(time.Duration(rand.Intn(30)+1) * time.Millisecond)
						assert.NoError(t, k8sClient.Delete(ctx, lock))
						require.Eventually(t, func() bool {
							err := k8sClient.Get(ctx, lockName, lock)
							return err != nil && apierrors.IsNotFound(err)
						}, eventuallyTimeout, eventuallyTick)
						break
					} else {
						time.Sleep(time.Millisecond * 10)
					}
				}
				wg.Done()
			}(i)
		}

		testRunning := make(chan struct{})
		go func() {
			// simulate periodic cluster config updates
			for {
				select {
				case <-time.After(time.Second * 1):
					controller.CfgChan <- wscommon.ConfigUpdateEvent{
						Cfg:    &wscommon.ClusterConfig{Resources: collection.Copy()},
						Reason: wscommon.ConfigUpdateReload,
					}
				case <-testRunning:
					return
				}
			}
		}()

		wg.Wait()
		close(testRunning)
		t.Log(errors)
		assert.Equal(t, 0, len(errors))

		// everything should have finished
		list := &rlv1.ResourceLockList{}
		require.NoError(t, k8sClient.List(ctx, list))
		assert.Equal(t, 0, len(list.Items))
		assert.True(t, controller.resources.IsUnallocated())
	})

}

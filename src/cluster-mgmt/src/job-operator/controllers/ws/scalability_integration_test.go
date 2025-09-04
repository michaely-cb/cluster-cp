//go:build integration && scalability

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
package ws

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	commonpb "cerebras/pb/workflow/appliance/common"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	"cerebras.com/job-operator/controllers/cluster"
	rl "cerebras.com/job-operator/controllers/resource"
	"cerebras.com/job-operator/controllers/resource/namespace"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	//+kubebuilder:scaffold:imports
)

func TestLargeJobConfigSize(t *testing.T) {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
	kubeConfigFile := CreateKubeconfigFileForRestConfig(testEnv.Config)
	logrus.Infof("Envtest kubeconfig path: %s", kubeConfigFile)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
		os.Remove(kubeConfigFile)
	}()
	k8sClient = mgr.GetClient()
	k8sReader = mgr.GetAPIReader()

	wscommon.IsDev = true
	wsapisv1.IsV2Network = true
	defer func() {
		wscommon.IsDev = false
		wsapisv1.IsV2Network = false
	}()
	wscommon.EnableMultiCoordinator = false
	wscommon.DisableMultus = true
	wscommon.Namespace = wscommon.DefaultNamespace
	memxRegularMemBytes = wscommon.DefaultNodeMem << 20
	wsapisv1.CrdExecuteMinMemoryBytes = int(defaultCrdMem.Value())
	wsapisv1.CrdCompileMinMemoryBytes = int(defaultCrdMem.Value())
	namespace.InitMemConstants()
	cluster.DisableLabeler = true // not required but speeds up the test
	popGroupCount, depopGroupCount := 500, 588
	largeClusterCfg := wscommon.NewClusterConfigBuilder("test-1088").
		WithCoordNodes(popGroupCount).
		WithBR(10000). // 144 per 64 systems in cg3/4/5, 1088 systems = 144*64~= 10k
		WithSysMemxWorkerGroups(popGroupCount, 12, 2).
		WithSysMemxWorkerGroups(depopGroupCount, 4, 1).
		BuildClusterSchema()

	cfgMgr := wscommon.NewClusterConfigMgr(mgr)
	require.NoError(t, cfgMgr.Initialize(largeClusterCfg))
	jobReconciler := NewJobOperatorReconciler(cfgMgr.Cfg, mgr, false)
	cfgMgr.AddWatcher(jobReconciler.CfgChan)

	// from cg1(64system) of 60 sys job
	// 600k CD config, 120k BR config, 280k cluster yaml, 240k lock
	// the first three are configmap (size limit 1mb), lock is CRD(size limit 1.5mb)
	// todo: add compression for cluster yaml/lock for 1088 cluster
	for _, testcase := range []struct {
		numSystems int
	}{
		{
			numSystems: 192,
		},
		{
			numSystems: 1088,
		},
	} {
		t.Run(fmt.Sprintf("%d system job", testcase.numSystems), func(t *testing.T) {
			setRequest := func(job *wsapisv1.WSJob, replicaType commonv1.ReplicaType, replicas int) {
				job.Spec.WSReplicaSpecs[replicaType].Replicas = wscommon.Pointer(int32(replicas))
			}
			job := newWsExecuteJob(wscommon.Namespace, "large-"+strconv.Itoa(testcase.numSystems), uint32(testcase.numSystems))
			job.Annotations[wsapisv1.SemanticClusterServerVersion] = "1.0.2"
			setRequest(job, wsapisv1.WSReplicaTypeWeight, 24)
			// from cg1/2, largest job has 3600pods/60system=>60 per system
			// use 100 to ensure largest case covered, 1088 system => 100k pods
			setRequest(job, wsapisv1.WSReplicaTypeActivation, 100*testcase.numSystems)

			require.NoError(t, k8sClient.Create(ctx, job))
			defer func() {
				require.NoError(t, k8sClient.Delete(context.Background(), job))
			}()

			combo := generateSystemCombo(testcase.numSystems)
			var orderedSystems []string
			for stamp := range combo {
				for _, systemName := range combo[stamp] {
					orderedSystems = append(orderedSystems, systemName)
				}
			}
			_, _, _, tree := wscommon.BuildBrGroupTrees(combo, orderedSystems,
				false, wsapisv1.BrTreePolicyPort)
			res := jobResources{
				systems: wscommon.SortedKeys(cfgMgr.Cfg.SystemMap)[:testcase.numSystems],
				replicaToNodeMap: map[string]map[int]string{
					rlv1.BrRequestName: make(map[int]string),
				},
				replicaToResourceMap: map[string]map[int]map[string]int{
					rlv1.BrRequestName: make(map[int]map[string]int),
				},
				replicaToAnnoMap: map[string]map[string]string{
					rlv1.BrRequestName: {
						rlv1.BrLogicalTreeKey: tree.String(),
					},
				},
			}
			// for 1088, br count ~= 4500, assign with 10k for simplicity
			for i := 0; i < 10000; i++ {
				res.replicaToNodeMap[rlv1.BrRequestName][i] = fmt.Sprintf("br-%d", i)
				res.replicaToResourceMap[rlv1.BrRequestName][i] =
					map[string]int{
						fmt.Sprintf("cs-port/nic%d", i%3): 500,
					}
			}

			// create/check br config
			// for 1088, size 2335622=>173001
			_, err := jobReconciler.reconcileBrConfig(context.TODO(), job, res)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				cm, br, err := jobReconciler.getBRConfig(context.TODO(), job)
				if err == nil {
					t.Log(len(br.BRNodes))
					//t.Log(*br)
					t.Log(len(cm.BinaryData[wsapisv1.BrConfigCompressedFileName]))
					x, _ := wscommon.Gunzip(cm.BinaryData[wsapisv1.BrConfigCompressedFileName])
					t.Log(len(x))
				}
				return err == nil
			}, eventuallyTimeout, eventuallyTick)

			// create/check cd config
			// for 1088, size 23497538=>775727
			_, _, err = jobReconciler.reconcileClusterDetailsConfig(context.TODO(), job, res)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				cm, cd, err := jobReconciler.getClusterDetailsConfig(job)
				if err == nil {
					for _, tm := range cd.(*commonpb.ClusterDetails).Tasks {
						t.Log(tm.TaskType, len(tm.TaskMap))
					}
					//t.Log(fmt.Sprintf("%v", *cd.(*common_pb.ClusterDetails))[:1000])
					t.Log(len(cm.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName]))
					x, _ := wscommon.Gunzip(cm.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName])
					t.Log(len(x))
				}
				return err == nil
			}, eventuallyTimeout, eventuallyTick)
		})
	}
}

// generate sys combo based on group distribution
func generateSystemCombo(groups ...int) map[string][]string {
	sysId := 0
	groupSys := map[string][]string{}
	for i := 0; i < len(groups); i++ {
		group := fmt.Sprintf("group%d", i)
		for j := 0; j < groups[i]; j++ {
			sys := fmt.Sprintf("system-%d", sysId)
			groupSys[group] = append(groupSys[group], sys)
			sysId++
		}
	}
	return groupSys
}

// Generates random config reload events. Should also generate random NS + Health updates for completeness
// While running wsjobs in order to flush out concurrency related bugs
func TestConcurrentUpdateFuzz(t *testing.T) {
	mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
	kubeConfigFile := CreateKubeconfigFileForRestConfig(testEnv.Config)
	logrus.Infof("Envtest kubeconfig path: %s", kubeConfigFile)
	defer func() {
		cancel()
		require.NoError(t, testEnv.Stop())
		os.Remove(kubeConfigFile)
	}()
	k8sClient = mgr.GetClient()
	k8sReader = mgr.GetAPIReader()
	defaultCfgSchema := wscommon.NewTestClusterConfigSchema(systemCount, 1, brCount, 0, 0, workersPerGroup, memoryXPerGroup)
	cfgMgr := wscommon.NewTestClusterConfigMgr(mgr, systemCount, brCount, 0, workersPerGroup, memoryXPerGroup)
	_, err := cluster.NewConfigReconciler(cfgMgr, mgr)
	require.NoError(t, err)

	systemUpdater := cluster.NewSystemUpdater(mgr.GetClient(), mgr.GetLogger())
	cfgMgr.AddWatcher(systemUpdater.GetCfgCh())
	require.NoError(t, mgr.Add(systemUpdater))

	resourcesController := rl.NewController(cfgMgr)
	require.NoError(t, resourcesController.SetupWithManager(mgr))
	cfgMgr.AddWatcher(resourcesController.CfgChan)

	jobReconciler := NewJobOperatorReconciler(cfgMgr.Cfg, mgr, false)
	cfgMgr.AddWatcher(jobReconciler.CfgChan)
	require.NoError(t, jobReconciler.SetupWithManager(mgr))

	updateClusterConfig(t, cfgMgr, defaultCfgSchema)
	assertClusterServerIngressSetup(t, wscommon.Namespace)

	// namespace should have all resources
	nsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: wscommon.Namespace}}
	err = k8sClient.Get(ctx, client.ObjectKeyFromObject(nsr), nsr)
	if err != nil && apierrors.IsNotFound(err) {
		nsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:  namespacev1.SetResourceSpecMode,
				SystemCount: wscommon.Pointer(systemCount),
			},
			RequestUid: uuid.NewString(),
		}
		err = k8sClient.Create(ctx, nsr)
		require.True(t, err == nil || apierrors.IsAlreadyExists(err))
	}
	require.Eventually(t, func() bool {
		err = k8sClient.Get(ctx, client.ObjectKeyFromObject(nsr), nsr)
		require.NoError(t, err)
		return nsr.Status.GetLastAttemptedRequest().Request.RequestUid == nsr.Spec.RequestUid
	}, eventuallyTimeout, eventuallyTick)

	jobWorkerCount := 4
	workerWg := sync.WaitGroup{}
	maxJobs, currentJob := int32(16), wscommon.Pointer(int32(0))

	for i := 0; i < jobWorkerCount; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()

			for {
				jobId := atomic.AddInt32(currentJob, 1)
				if jobId > maxJobs {
					t.Log("done, exit")
					return
				}

				numSys := uint32(math.Pow(2, float64(rand.Intn(3))))
				jobName := fmt.Sprintf("concurrent-job-%d-%d", jobId, numSys)
				job := newWsExecuteJob(wscommon.Namespace, jobName, numSys)
				require.NoError(t, k8sClient.Create(ctx, job))

				// wait for scheduled
				require.Eventually(t, func() bool {
					require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job))
					if len(job.Status.Conditions) == 0 {
						return false
					}
					cond := job.Status.Conditions[len(job.Status.Conditions)-1]
					t.Logf("%s - %s", jobName, cond.Type)
					return cond.Type == commonv1.JobScheduled
				}, time.Second*30, time.Millisecond*100)

				// wait for pod creation
				expectedPodCount := countNonBrPods(job)
				if numSys > 1 {
					expectedPodCount += 12
				}
				pods := v1.PodList{}
				require.Eventually(t, func() bool {
					require.NoError(t, k8sClient.List(ctx, &pods, client.MatchingLabels{commonv1.JobNameLabel: job.Name}))
					t.Logf("%s - %d/%d", jobName, len(pods.Items), expectedPodCount)
					return len(pods.Items) == expectedPodCount
				}, time.Second*30, time.Millisecond*100)

				// set pods as Running
				for i := range pods.Items {
					pod := pods.Items[i]
					pod.Status.Phase = corev1.PodRunning
					require.NoError(t, k8sClient.Status().Update(ctx, &pod))
				}

				assertWsJobRunningEventually(t, job.Namespace, job.Name)

				assertWsjobCleanedUp(t, job)
			}
		}()
	}

	go func() {
		workerWg.Wait()
		select {
		case <-ctx.Done():
		default:
			cancel()
		}
	}()

	// continually update the cluster CM triggering config reloads
	go func() {
		cfgPatchCount := 0
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name:      wscommon.ClusterConfigMapName,
			Namespace: wscommon.SystemNamespace}}
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(cm), cm))

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(50)+100)):
				cfgPatchCount += 1
				patchString := fmt.Sprintf(`[
					{
						"op": "add",
						"path": "/metadata/labels/patchNumber",
						"value": "%d"
					}
				]`, cfgPatchCount)
				patch := client.RawPatch(types.JSONPatchType, []byte(patchString))
				err := k8sClient.Patch(ctx, cm, patch)
				if err != nil && !errors.Is(err, context.Canceled) {
					require.NoError(t, err)
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		t.Log("completed test")
	case <-time.After(120 * time.Second):
		cancel()
		t.Fatal("timed out waiting for workers to finish")
	}
}

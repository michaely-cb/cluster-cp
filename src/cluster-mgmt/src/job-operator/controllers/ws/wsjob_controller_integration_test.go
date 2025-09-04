//go:build integration && wsjob_controller

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
package ws

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonpb "cerebras/pb/workflow/appliance/common"

	nsv1 "cerebras.com/job-operator/apis/namespace/v1"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonutil "github.com/kubeflow/common/pkg/util"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
	//+kubebuilder:scaffold:imports
)

type WSJobControllerTestSuite struct {
	TestSuite // Embed the base suite
}

// SetupSubTest runs before each subtest and ensures clean lock state
func (t *WSJobControllerTestSuite) SetupSubTest() {
	assertLockCountEventually(t.T(), 0)
}

// BeforeTest prepares the test environment by ensuring a clean node state before each test.
func (t *WSJobControllerTestSuite) BeforeTest(suiteName, testName string) {
	logrus.Infof("WSJobController BeforeTest suite/test %s/%s", suiteName, testName)
	// This is necessary because TestSuite is embedded and the base suite's BeforeTest won't run automatically
	t.TestSuite.BeforeTest(suiteName, testName)

}

// AfterTest cleans up the test environment after each test.
func (t *WSJobControllerTestSuite) AfterTest(suiteName, testName string) {
	logrus.Infof("WSJobController AfterTest suite/test %s/%s", suiteName, testName)
	// This is necessary because TestSuite is embedded and the base suite's AfterTest won't run automatically
	t.TestSuite.AfterTest(suiteName, testName)
}

func TestWSJobControllerTestSuite(t *testing.T) {
	suite.Run(t, new(WSJobControllerTestSuite))
}

func setPodsReady(t *WSJobControllerTestSuite, pods *corev1.PodList, filter func(pod v1.Pod) bool) {
	for i := range pods.Items {
		po := pods.Items[i]
		if filter == nil || filter(po) {
			po.Status.Conditions = append(po.Status.Conditions, corev1.PodCondition{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			})
			po.Status.Phase = v1.PodRunning
			t.Require().NoError(k8sClient.Status().Update(t.Ctx, &po))
		}
	}
}

func (t *WSJobControllerTestSuite) TestReconcileJobs() {
	ctx := context.Background()
	t.Run("non_system_request_job_should_be_success", func() {
		t.SetupSubTest()
		job := newWsCompileJob(wscommon.Namespace, "ws-no-system", 0)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		// await the job finished
		assertServiceCountEventually(t.T(), job.Namespace, job.Name, "", 1)
		pod := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 1).Items[0]
		pod.Status.Phase = v1.PodSucceeded
		require.NoError(t.T(), k8sClient.Status().Update(context.TODO(), &pod))
		require.Eventually(t.T(), func() bool {
			require.NoError(t.T(), k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(job), job))
			return job.Status.CompletionTime != nil
		}, 30*time.Second, eventuallyTick)

		// require locking of the mgmt node
		assert.Contains(t.T(), getConditions(t.T(), job.Namespace, job.Name), commonv1.JobQueueing, "should be lock for a job with tasks")
	})

	t.Run("scheduling_compile_job_with_no_system_should_succeed", func() {
		t.SetupSubTest()
		compileJobName := "ws-compile"
		compileJob := newWsCompileJob(wscommon.Namespace, compileJobName, 0)

		defer assertWsjobCleanedUp(t.T(), compileJob)
		require.NoError(t.T(), k8sClient.Create(ctx, compileJob))

		assertPodCountEventually(t.T(), compileJob.Namespace, compileJobName, rlv1.CoordinatorRequestName, 1)
		lock := assertLockExists(t.T(), compileJob.Namespace, compileJob.Name)
		assert.True(t.T(), lock.Status.State == rlv1.LockGranted)
		assert.Equal(t.T(), 1, len(lock.Spec.ResourceRequests))

		clusterDetailsConfigMap := requireClusterDetailsCreatedEventually(t.T(), compileJobName, compileJob.Namespace)
		clusterDetailsConfig := &commonpb.ClusterDetails{}
		options := protojson.UnmarshalOptions{
			AllowPartial:   true,
			DiscardUnknown: true,
		}
		data, err := wscommon.Gunzip(clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName])
		require.NoError(t.T(), err)
		err = options.Unmarshal(data, clusterDetailsConfig)
		require.NoError(t.T(), err)
		hasWSE := false
		for _, task := range clusterDetailsConfig.Tasks {
			if task.TaskType == commonpb.ClusterDetails_TaskInfo_WSE {
				hasWSE = true
			}
		}
		// JO with semver >=1.3.0 will not add WSE task info for 0-system compile job
		assert.False(t.T(), hasWSE, "should not have WSE task info")
	})

	t.Run("scheduling_compile_job_with_1_system_should_succeed", func() {
		t.SetupSubTest()
		targetSystem := t.JobReconciler.Cfg.Systems[rand.Intn(len(t.JobReconciler.Cfg.Systems))]
		targetSystemName := targetSystem.Name
		compileJobName := "ws-compile-with-system"
		compileJob := newWsCompileJob(wscommon.Namespace, compileJobName, 1)
		compileJob.Annotations[wsapisv1.SchedulerHintAllowSys] = targetSystemName

		defer assertWsjobCleanedUp(t.T(), compileJob)
		require.NoError(t.T(), k8sClient.Create(ctx, compileJob))

		assertPodCountEventually(t.T(), compileJob.Namespace, compileJobName, rlv1.CoordinatorRequestName, 1)
		lock := assertLockExists(t.T(), compileJob.Namespace, compileJob.Name)
		assert.True(t.T(), lock.Status.State == rlv1.LockGranted)
		assert.Equal(t.T(), 2, len(lock.Spec.ResourceRequests))

		for _, req := range lock.Spec.ResourceRequests {
			if req.Type == rlv1.NodeResourceType {
				assert.Contains(t.T(), req.Properties[wsresource.RoleCrdPropKey], "")
			} else if req.Type == rlv1.SystemResourceType {
				assert.Equal(t.T(), []string{targetSystemName}, req.Properties["name"])
				expectSystemSubresource := 0
				assert.Equal(t.T(), expectSystemSubresource, req.Subresources[wsresource.SystemSubresource])
			}
		}

		// system names should still be set but they are not locked
		require.NoError(t.T(), k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(compileJob), compileJob))
		systemName := compileJob.Annotations[SystemNameAnnotationKey]
		allSystemNames := wscommon.Map(func(sys *systemv1.SystemSpec) string { return sys.Name }, t.JobReconciler.Cfg.Systems)
		require.Contains(t.T(), allSystemNames, systemName)

		clusterDetailsConfigMap := requireClusterDetailsCreatedEventually(t.T(), compileJobName, compileJob.Namespace)
		clusterDetailsConfig := &commonpb.ClusterDetails{}
		options := protojson.UnmarshalOptions{
			AllowPartial:   true,
			DiscardUnknown: true,
		}
		data, err := wscommon.Gunzip(clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName])
		require.NoError(t.T(), err)
		err = options.Unmarshal(data, clusterDetailsConfig)
		require.NoError(t.T(), err)
		hasWSE := false
		for _, task := range clusterDetailsConfig.Tasks {
			if task.TaskType == commonpb.ClusterDetails_TaskInfo_WSE {
				hasWSE = true
				assert.Equal(t.T(), 1, len(task.TaskMap), "should be just 1 WSE")
				assert.Equal(t.T(), targetSystem.CmAddr, task.TaskMap[0].AddressBook.WseIp, "WseIp should be %s", targetSystem.CmAddr)
			}
		}
		assert.True(t.T(), hasWSE, "should have 1 WSE")
	})

	t.Run("scheduling_train_job_should_succeed", func() {
		t.SetupSubTest()
		targetSystemName := t.JobReconciler.Cfg.Systems[rand.Intn(len(t.JobReconciler.Cfg.Systems))].Name
		trainJob := newWsExecuteJob(wscommon.Namespace, "ws-soft-grant-train", 1)
		trainJob.Annotations[wsapisv1.SchedulerHintAllowSys] = targetSystemName

		defer assertWsjobCleanedUp(t.T(), trainJob)
		require.NoError(t.T(), k8sClient.Create(ctx, trainJob))

		assertPodCountEventually(t.T(), trainJob.Namespace, trainJob.Name, rlv1.CoordinatorRequestName, 1)

		lock := assertLockExists(t.T(), trainJob.Namespace, trainJob.Name)
		assert.True(t.T(), lock.Status.State == rlv1.LockGranted)
		assert.Equal(t.T(), 2, len(lock.Spec.ResourceRequests))

		for _, req := range lock.Spec.ResourceRequests {
			if req.Type == rlv1.NodeResourceType {
				assert.Contains(t.T(), req.Properties[wsresource.RoleCrdPropKey], "")
			} else if req.Type == rlv1.SystemResourceType {
				assert.Equal(t.T(), []string{targetSystemName}, req.Properties["name"])
				expectSystemSubresource := 1
				assert.Equal(t.T(), expectSystemSubresource, req.Subresources[wsresource.SystemSubresource])
			}
		}

		wscommon.JobHasNicForTesting(trainJob.Namespace, trainJob.Name,
			"ws-soft-grant-train-coordinator-0", "0-coord-0", "enp0", "127.0.0.0")
		wscommon.JobHasNicForTesting(trainJob.Namespace, trainJob.Name,
			"ws-soft-grant-train-worker-1", "group2-worker-1", "nic1", "127.5.1.1")
		wscommon.JobHasNicForTesting(trainJob.Namespace, trainJob.Name,
			"ws-soft-grant-train-weight-1", "group2-memory-10", "nic1", "127.6.10.1")
	})

	for _, testcase := range []struct {
		name      string
		wsjob     func() *wsapisv1.WSJob
		expectErr string
	}{
		{
			name: "invalid job spec of grant requesting more systems than exist",
			wsjob: func() *wsapisv1.WSJob {
				return newWsExecuteJob(wscommon.Namespace, "wsjob-err", systemCount+1)
			},
			expectErr: `job requested 9 system(s) but session 'job-operator' has only 8 system(s) assigned`,
		},
		{
			name: "invalid job spec of 1 system but having BR specified",
			wsjob: func() *wsapisv1.WSJob {
				// update to 1 to after BR specified
				job := newWsExecuteJob(wscommon.Namespace, "ws-br-fail-0", 2)
				job.Spec.NumWafers = 1
				return job
			},
			expectErr: "no BR should be specified for job with <= 1 csx request",
		},
		{
			name: "invalid job spec having pre-defined BR replica count",
			wsjob: func() *wsapisv1.WSJob {
				job := newWsExecuteJob(wscommon.Namespace, "ws-br-fail-2", 2)
				job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce].Replicas = wscommon.Pointer(int32(2))
				return job
			},
			expectErr: "don't specify BR replica to larger than 1",
		},
		{
			name: "invalid job spec having negative memory request",
			wsjob: func() *wsapisv1.WSJob {
				job := newWsExecuteJob(wscommon.Namespace, "ws-negative-mem", 1)
				job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker].Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(-1_000_000, resource.BinarySI),
				}
				return job
			},
			expectErr: "Worker has negative resource request memory",
		},
	} {
		t.Run(testcase.name, func() {
			assertLockCountEventually(t.T(), 0)
			job := testcase.wsjob()
			require.NoError(t.T(), k8sClient.Create(ctx, job))
			defer assertWsjobCleanedUp(t.T(), job)

			require.Eventually(t.T(), func() bool {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job)
				if err != nil && apierrors.IsNotFound(err) {
					return false
				}
				require.NoError(t.T(), err)
				return commonutil.IsFailed(job.Status)
			}, eventuallyTimeout, eventuallyTick)

			if testcase.expectErr != "" {
				actualMsg := job.Status.Conditions[len(job.Status.Conditions)-1].Message
				t.Contains(actualMsg, testcase.expectErr)
			}
		})
	}

	t.Run("creating_initContainer_success_with_2_systems", func() {
		t.SetupSubTest()
		jobName := "wsjob-initcontainers"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, 2)

		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		defer assertWsjobCleanedUp(t.T(), wsjob)

		expectedPodCount := countNonBrPods(wsjob) + 12
		pods := assertPodCountEventually(t.T(), wsjob.Namespace, wsjob.Name, "", expectedPodCount)
		for _, pod := range pods.Items {
			// Should have InitContainers.
			assert.Equal(t.T(), 1, len(pod.Spec.InitContainers))
			assert.Equal(t.T(), pod.Spec.InitContainers[0].Name, wsapisv1.DefaultInitContainerName)
		}
	})

	t.Run("custom_ingress_domain", func() {
		t.SetupSubTest()
		t.JobReconciler.Cfg.ServiceDomain = "example.com"
		defer func() { t.JobReconciler.Cfg.ServiceDomain = "" }()

		assert.True(t.T(), strings.HasSuffix(t.JobReconciler.Cfg.GetClusterServerDomain(), ".example.com"))
		job := newWsCompileJob(wscommon.Namespace, "custom-domain", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		pods := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 1)
		for _, po := range pods.Items {
			po.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
			}
			for i := range po.Status.ContainerStatuses {
				po.Status.ContainerStatuses[i].Ready = true
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &po))
		}

		ingresses := assertIngressCountEventually(t.T(), job.Namespace, job.Name, 1)
		ingress := ingresses.Items[0]
		assert.True(t.T(), strings.HasSuffix(ingress.Spec.Rules[0].Host, "."+t.JobReconciler.Cfg.GetClusterServerDomain()))
		assert.True(t.T(), strings.HasSuffix(ingress.Spec.TLS[0].Hosts[0], t.JobReconciler.Cfg.GetClusterServerDomain()))
	})

	t.Run("test_having_1_predefined_non-default_initContainer_in_spec", func() {
		t.SetupSubTest()
		job := newWsExecuteJob(wscommon.Namespace, "ws-1-non-default-init-container", 2)
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce].Template.Spec.InitContainers =
			[]corev1.Container{
				{
					Image:           wsapisv1.KubectlImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Name:            "another-init",
					Command:         []string{"/bin/sleep", "10"},
				},
			}
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		lock := assertLockExists(t.T(), job.Namespace, job.Name)
		assertLocked(t.T(), lock)

		expectedPodCount := countNonBrPods(job) + 12 // 1 coordinator, 12 BRs
		pods := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", expectedPodCount)
		for _, pod := range pods.Items {
			if pod.Labels["replica-type"] == strings.ToLower(rlv1.BrRequestName) {
				assert.Equal(t.T(), len(pod.Spec.InitContainers), 2)
				assert.Equal(t.T(), pod.Spec.InitContainers[0].Name, "another-init")
			} else {
				assert.Equal(t.T(), len(pod.Spec.InitContainers), 1)
			}
		}
	})

	t.Run("per-job_ingress_creation_test", func() {
		t.SetupSubTest()
		// 1. create a wsjob
		// 2. ensure that the ingress is created and pointing towards the newly created service
		// 3. on job completion, ensure that the ingress is cleaned up

		jobName := "wsjob-ingress"
		wsjob := newWsCompileJob(wscommon.Namespace, jobName, 0)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		defer assertWsjobCleanedUp(t.T(), wsjob)
		podList := assertPodCountEventually(t.T(), wsjob.Namespace, jobName, "", 1)
		serviceList := assertServiceCountEventually(t.T(), wsjob.Namespace, jobName, "", 1)

		pod := &podList.Items[0]
		pod.Status = corev1.PodStatus{
			Phase: corev1.PodRunning,
		}
		for i := range pod.Status.ContainerStatuses {
			pod.Status.ContainerStatuses[i].Ready = true
		}
		require.NoError(t.T(), k8sClient.Status().Update(ctx, pod))
		ingressList := assertIngressCountEventually(t.T(), wsjob.Namespace, jobName, 1)

		service := &serviceList.Items[0]
		ingress := &ingressList.Items[0]

		require.Equal(t.T(), service.Name, ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Name)
		require.Equal(t.T(), ingress.Annotations["nginx.ingress.kubernetes.io/backend-protocol"], "GRPC")

		// should be cleaned up after job ended
		podList = assertPodCountEventually(t.T(), wsjob.Namespace, jobName, "", 1)
		pod.Status.Phase = v1.PodSucceeded
		require.NoError(t.T(), k8sClient.Status().Update(ctx, pod))
		assertIngressCountEventually(t.T(), wsjob.Namespace, jobName, 0)
	})

	for _, testcase := range []struct {
		name             string
		servicesDisabled bool
	}{
		{
			name:             "CRD multus enabled services disabled",
			servicesDisabled: true,
		},
		{
			name:             "CRD multus enabled services enabled",
			servicesDisabled: false,
		},
	} {
		t.Run(testcase.name, func() {
			assertLockCountEventually(t.T(), 0)
			wscommon.DisableMultus = false
			t.JobReconciler.Config.EnableMultus = true
			defer func() {
				wscommon.DisableMultus = true
				t.JobReconciler.Config.EnableMultus = false
			}()

			jobName := "wsjob-multus"
			wsjob := newWsExecuteJob(wscommon.Namespace, jobName, 1)
			wsjob.Annotations[wsapisv1.DisableNonCRDServicesAnnotation] = strconv.FormatBool(testcase.servicesDisabled)
			require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
			defer assertWsjobCleanedUp(t.T(), wsjob)

			expectedPodCount := countNonBrPods(wsjob)
			pods := assertPodCountEventually(t.T(), wsjob.Namespace, wsjob.Name, "", expectedPodCount)
			for i := range pods.Items {
				pod := pods.Items[i]
				t.Require().NotEmpty(pod.Annotations[wscommon.NetworkAttachmentKey])
				pod.Annotations = map[string]string{
					netv1.NetworkStatusAnnot: fmt.Sprintf(
						`[{"name":"cilium","interface":"eth0","ips":["192.168.1.%d"]},{"name":"multus","interface":"net1","ips":["10.0.1.%d"]}]`, i, i,
					),
				}
				t.Require().NoError(k8sClient.Update(ctx, &pod))
				setPodsReady(t, &corev1.PodList{Items: []corev1.Pod{pod}}, nil)
			}

			// check single endpoint + svc created for coordinator
			epList := &corev1.EndpointsList{}
			t.Require().Eventually(func() bool {
				t.Require().NoError(k8sClient.List(ctx, epList, client.MatchingLabels{commonv1.JobNameLabel: jobName}))
				if testcase.servicesDisabled {
					return len(epList.Items) > 0
				} else {
					return len(epList.Items) >= len(pods.Items)
				}
			}, eventuallyTimeout, eventuallyTick)
			svcList := &corev1.ServiceList{}
			t.Require().NoError(k8sClient.List(ctx, svcList, client.MatchingLabels{commonv1.JobNameLabel: jobName}))

			if testcase.servicesDisabled {
				t.Assert().Len(epList.Items, 1)
				t.Require().Len(svcList.Items, 1)
				ep := epList.Items[0]
				t.Assert().Contains(ep.Name, "coordinator")
				t.Require().Equal(svcList.Items[0].Name, ep.Name)
			} else {
				t.Assert().Len(epList.Items, len(pods.Items))
				t.Assert().Len(svcList.Items, len(pods.Items))
			}
		})
	}

	t.Run("namespace_contains_no_resources", func() {
		t.SetupSubTest()
		emptyNsr := &nsv1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "empty"},
			Spec: nsv1.NamespaceReservationSpec{
				ReservationRequest: nsv1.ReservationRequest{
					RequestParams: nsv1.RequestParams{SystemCount: wscommon.Pointer(0)},
					RequestUid:    uuid.NewString(),
				},
			},
		}
		t.Require().NoError(k8sClient.Create(ctx, emptyNsr))
		defer func() { t.Require().NoError(k8sClient.Delete(ctx, emptyNsr)) }()
		t.Require().Eventually(func() bool {
			t.Require().NoError(k8sClient.Get(ctx, client.ObjectKeyFromObject(emptyNsr), emptyNsr))
			return emptyNsr.Status.GrantedRequest.RequestUid == emptyNsr.Spec.RequestUid
		}, eventuallyTimeout, eventuallyTick)

		job := newWsExecuteJob("empty", "no-resource-in-ns", uint32(1))
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)
		job = assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
		lastMsg := job.Status.Conditions[len(job.Status.Conditions)-1].Message
		t.Require().Contains(lastMsg, "requested 1 system(s) but session 'empty' has only 0 system(s) assigned")
	})

	t.Run("prioritized_compile_deadlock_detection", func() {
		t.SetupSubTest()

		// Track jobs for cleanup
		var jobsToCleanup []*wsapisv1.WSJob
		defer func() {
			for _, job := range jobsToCleanup {
				assertWsjobCleanedUp(t.T(), job)
			}
		}()

		workflow0 := "workflow-0"
		workflow1 := "workflow-1"

		makeExecuteJob := func(workflowId string, priority, sysCount int) *wsapisv1.WSJob {
			j := newWsExecuteJob(wscommon.Namespace, workflowId+"-execute", uint32(sysCount))
			j.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
			j.SetPriority(priority)
			return j
		}

		makeCompileJob := func(workflowId, name string, memBytes int64) *wsapisv1.WSJob {
			_mem := defaultCrdMem.DeepCopy()
			defaultCrdMem = *resource.NewQuantity(memBytes, resource.BinarySI)
			defer func() {
				defaultCrdMem = _mem
			}()
			j := newWsCompileJob(wscommon.Namespace, workflowId+"-"+name, 0)
			j.Labels[wsapisv1.WorkflowIdLabelKey] = workflowId
			return j
		}

		assertJobLastCondition := func(j *wsapisv1.WSJob, conditionType commonv1.JobConditionType, message string) {
			require.NoError(t.T(), k8sClient.Get(context.TODO(), client.ObjectKeyFromObject(j), j))
			lastCondition := j.Status.Conditions[len(j.Status.Conditions)-1]
			assert.Equal(t.T(), conditionType, lastCondition.Type)
			assert.Contains(t.T(), lastCondition.Message, message)
		}

		// - mem_execute + mem_compile << mem_node
		var nonDeadlockMemBytes int64 = 1

		// - mem_execute + mem_compile < mem_node
		// - 2 * mem_execute + mem_compile > mem_node
		pendingDeadlockMemBytes := (wscommon.DefaultNodeMem+1)<<20 - 2*defaultCrdMem.Value()
		require.Greater(t.T(), int64(wscommon.DefaultNodeMem), (defaultCrdMem.Value()+pendingDeadlockMemBytes)>>20)
		require.Less(t.T(), int64(wscommon.DefaultNodeMem), (2*defaultCrdMem.Value()+pendingDeadlockMemBytes)>>20)

		// - mem_execute + mem_compile > mem_node
		var instantFailedDeadlockMemBytes int64 = wscommon.DefaultNodeMem << 20

		// Create a scheduled execute job in workflow0
		workflow0Execute := makeExecuteJob(workflow0, 300, systemCount/2)
		require.NoError(t.T(), k8sClient.Create(ctx, workflow0Execute))
		jobsToCleanup = append(jobsToCleanup, workflow0Execute)
		workflow0ExecuteLock := assertLockExists(t.T(), workflow0Execute.Namespace, workflow0Execute.Name)
		assertLocked(t.T(), workflow0ExecuteLock)

		// Create a small compile job of the same workflow
		workflow0SmallCompile := makeCompileJob(workflow0, "small-prioritized-compile", nonDeadlockMemBytes)
		require.NoError(t.T(), k8sClient.Create(ctx, workflow0SmallCompile))
		jobsToCleanup = append(jobsToCleanup, workflow0SmallCompile)
		l := assertLockExists(t.T(), workflow0SmallCompile.Namespace, workflow0SmallCompile.Name)
		assertLocked(t.T(), l)
		assert.Equal(t.T(), rlv1.PrioritizedCompileQueueName, l.Spec.QueueName)
		assertWsjobCleanedUp(t.T(), workflow0SmallCompile)

		// Create a large compile job of the same workflow, should fail immediately due to deadlock
		workflow0LargeCompile := makeCompileJob(workflow0, "large-prioritized-compile-0", instantFailedDeadlockMemBytes)
		require.NoError(t.T(), k8sClient.Create(ctx, workflow0LargeCompile))
		jobsToCleanup = append(jobsToCleanup, workflow0LargeCompile)
		assertWsJobFailEventually(t.T(), workflow0LargeCompile.Namespace, workflow0LargeCompile.Name)
		assertJobLastCondition(workflow0LargeCompile, commonv1.JobFailed, "from the same workflow")
		assertWsjobCleanedUp(t.T(), workflow0LargeCompile)

		// Create a compile job with the same large spec but not in the same workflow, should be pending
		workflow1LargeCompile := makeCompileJob(workflow1, "large-prioritized-compile-1", instantFailedDeadlockMemBytes)
		require.NoError(t.T(), k8sClient.Create(ctx, workflow1LargeCompile))
		jobsToCleanup = append(jobsToCleanup, workflow1LargeCompile)
		l = assertLockExists(t.T(), workflow1LargeCompile.Namespace, workflow1LargeCompile.Name)
		assertPending(t.T(), l)
		assert.Equal(t.T(), rlv1.CompileQueueName, l.Spec.QueueName)
		assertWsjobCleanedUp(t.T(), workflow1LargeCompile)

		// Create another scheduled execute job in workflow1, which has a priority higher than workflow0
		workflow1Execute := makeExecuteJob(workflow1, 100, systemCount/2)
		require.NoError(t.T(), k8sClient.Create(ctx, workflow1Execute))
		jobsToCleanup = append(jobsToCleanup, workflow1Execute)
		workflow1ExecuteLock := assertLockExists(t.T(), workflow1Execute.Namespace, workflow1Execute.Name)
		assertLocked(t.T(), workflow1ExecuteLock)

		// Create a deadlock candidate in workflow0
		workflow0DeadlockCandidate := makeCompileJob(workflow0, "deadlock-candidate-0", pendingDeadlockMemBytes)
		require.NoError(t.T(), k8sClient.Create(ctx, workflow0DeadlockCandidate))
		jobsToCleanup = append(jobsToCleanup, workflow0DeadlockCandidate)
		l = assertLockExists(t.T(), workflow0DeadlockCandidate.Namespace, workflow0DeadlockCandidate.Name)
		assertPending(t.T(), l)
		assert.Equal(t.T(), rlv1.PrioritizedCompileQueueName, l.Spec.QueueName)

		// Create another deadlock candidate in workflow1
		workflow1DeadlockCandidate := makeCompileJob(workflow1, "deadlock-candidate-1", pendingDeadlockMemBytes)
		require.NoError(t.T(), k8sClient.Create(ctx, workflow1DeadlockCandidate))
		jobsToCleanup = append(jobsToCleanup, workflow1DeadlockCandidate)

		// Expect deadlock candidate in workflow0 to fail immediately due to lower priority
		assertWsJobFailEventually(t.T(), workflow0DeadlockCandidate.Namespace, workflow0DeadlockCandidate.Name)
		assertJobLastCondition(workflow0DeadlockCandidate, commonv1.JobFailed,
			"due to resource contention and lost to prioritized-compile job(s)")
		assertWsjobCleanedUp(t.T(), workflow0DeadlockCandidate)

		// Deadlock candidate in workflow1 still in pending state
		l = assertLockExists(t.T(), workflow1DeadlockCandidate.Namespace, workflow1DeadlockCandidate.Name)
		assertPending(t.T(), l)
		assert.Equal(t.T(), rlv1.PrioritizedCompileQueueName, l.Spec.QueueName)
		assertWsjobCleanedUp(t.T(), workflow1DeadlockCandidate)
	})

	t.Run("defer_job_processing_until_cfg_update", func() {
		t.SetupSubTest()
		t.Require().Eventually(func() bool {
			return t.JobReconciler.cfgInitialized
		}, eventuallyTimeout, eventuallyTick)

		t.JobReconciler.cfgInitialized = false
		job := newWsExecuteJob(wscommon.Namespace, "cfg-defer-job-invalid", uint32(2))
		brReplicas := int32(-1)
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce].Replicas = &brReplicas
		t.Require().NoError(k8sClient.Create(t.Ctx, job))
		time.Sleep(500 * time.Millisecond)
		t.Require().NoError(k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(job), job))
		t.Require().Nil(job.Status.CompletionTime) // invalid job should not be processed until config update

		t.CfgMgr.Notify()
		t.Require().Eventually(func() bool {
			t.Require().NoError(k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(job), job))
			return job.Status.CompletionTime != nil
		}, eventuallyTimeout, eventuallyTick)
	})
}

func (t *WSJobControllerTestSuite) TestLockIntegration() {
	ctx := context.Background()
	t.Run("resource-lock_happy_path", func() {
		t.SetupSubTest()
		// 1. lock all the cs machines
		// 2. create a wsjob, ensure job does not instantiate pods
		// 3. delete the cs machine lock, ensure the job proceeds
		// 4. set the job as finished, ensure the lock is deleted as part of cleanup
		prelock := &rlv1.ResourceLock{
			ObjectMeta: ctrl.ObjectMeta{Name: "pre-lock", Namespace: wscommon.SystemNamespace},
			Spec: rlv1.ResourceLockSpec{
				ResourceRequests: []rlv1.ResourceRequest{
					{
						Name:         rlv1.SystemsRequestName,
						Count:        systemCount,
						Type:         rlv1.SystemResourceType,
						Subresources: map[string]int{wsresource.SystemSubresource: 1}},
				},
			},
		}
		require.NoError(t.T(), k8sClient.Create(ctx, prelock))
		assertLocked(t.T(), prelock)
		defer func() {
			err := k8sClient.Delete(ctx, prelock)
			if err != nil && !apierrors.IsNotFound(err) {
				require.NoError(t.T(), err)
			}
		}()

		nginxNS := wscommon.NginxNameSpace
		wscommon.NginxNameSpace = wscommon.Namespace
		defer func() {
			wscommon.NginxNameSpace = nginxNS
		}()
		setNginxPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "nginx",
				Namespace: wscommon.NginxNameSpace,
				Labels: map[string]string{
					wscommon.NginxPodLabelKey: wscommon.NginxPodLabelVal,
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "0-coord-0",
				Containers: []corev1.Container{
					{
						Name:  "nginx",
						Image: "nginx",
					},
				},
			},
		}
		require.NoError(t.T(), k8sClient.Create(ctx, setNginxPod))
		setNginxPod.Status = corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Status: corev1.ConditionTrue,
					Type:   corev1.PodReady,
				},
			}}
		require.NoError(t.T(), k8sClient.Status().Update(ctx, setNginxPod))

		jobName := "wsjob-lock"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, 1)
		wsjob.Annotations[wsapisv1.WsJobCrdAddrAnnoKey] = "" // request the coordinator node's NIC addr to be set
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		defer func() { assertWsjobCleanedUp(t.T(), wsjob) }()

		lock := assertLockExists(t.T(), wsjob.Namespace, wsjob.Name)
		defer func() {
			err := k8sClient.Delete(ctx, lock)
			if err != nil && !apierrors.IsNotFound(err) {
				require.NoError(t.T(), err)
			}
		}()

		assert.True(t.T(), lock.Status.State != rlv1.LockGranted)

		condStatus, condExists := getConditions(t.T(), wsjob.Namespace, jobName)[commonv1.JobCreated]
		assert.True(t.T(), condExists)
		condStatus, condExists = getConditions(t.T(), wsjob.Namespace, jobName)[commonv1.JobQueueing]
		assert.True(t.T(), condExists && condStatus.Status == corev1.ConditionTrue)
		condStatus, condExists = getConditions(t.T(), wsjob.Namespace, jobName)[commonv1.JobScheduled]
		assert.False(t.T(), condExists)

		require.NoError(t.T(), k8sClient.Delete(ctx, prelock))

		lock = assertLockExists(t.T(), wsjob.Namespace, jobName)
		assertLocked(t.T(), lock)
		pods := getJobPods(t.T(), wsjob.Namespace, jobName, "")
		condStatus, condExists = getConditions(t.T(), wsjob.Namespace, jobName)[commonv1.JobScheduled]
		assert.True(t.T(), condExists && condStatus.Status == corev1.ConditionTrue)

		// preferredAddr is set to the coordinator node's NIC addr
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: jobName, Namespace: wsjob.Namespace}, wsjob))
		preferredIp := wsjob.Annotations[wsapisv1.WsJobCrdAddrAnnoKey]
		crdNode := t.JobReconciler.Cfg.NodeMap[wsjob.Annotations[CoordinatorNodeNameAnnotationKey]]
		assert.Equal(t.T(), crdNode.NICs[0].Addr, preferredIp)

		// set pod as finished to auto clean up lock
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}
		assertLockCountEventually(t.T(), 0)
	})

	t.Run("resource_lock_cleanup_wait_till_pods_terminated", func() {
		t.SetupSubTest()
		jobName := "running-clean-up"
		wsjob := newWsCompileJob(wscommon.Namespace, jobName, 1)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		defer assertWsjobCleanedUp(t.T(), wsjob)

		pods := assertPodCountEventually(t.T(), wsjob.Namespace, jobName, "", 1)
		for _, pod := range pods.Items {
			// set annotation to simulate pod stuck in terminating status but still running
			pod.Annotations = wscommon.EnsureMap(pod.Annotations)
			pod.Annotations["fake-deleted"] = "true"
			require.NoError(t.T(), k8sClient.Update(ctx, &pod))
		}
		pods = assertPodCountEventually(t.T(), wsjob.Namespace, jobName, "", 1)
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}

		// assert pod running without cleanup
		cancelJobEventually(t.T(), wsjob, wsapisv1.CancelWithStatusSucceeded)
		time.Sleep(1 * time.Second)
		assertPodCountEventually(t.T(), wsjob.Namespace, jobName, "", 1)
		assertLockExists(t.T(), wsjob.Namespace, jobName)

		tt := metav1.Time{Time: time.Now().Add(-wscommon.LockCleanupGracePeriod + 3*time.Second)}
		wsjob.Status.CompletionTime = &tt
		require.NoError(t.T(), k8sClient.Status().Update(ctx, wsjob))
		time.Sleep(1 * time.Second)
		assertLockExists(t.T(), wscommon.Namespace, jobName)
		// envtest don't have controller-manager to assign node and node name field is a server side field only
		// fake the test to use empty field
		t.JobReconciler.Cfg.UnhealthyNodes[""] = &wscommon.Node{}
		defer func() {
			delete(t.JobReconciler.Cfg.UnhealthyNodes, "")
		}()

		// assert lock/pod cleaned up after detected node unhealthy within grace period
		time.Sleep(5 * time.Second)
		assertLockNotExists(t.T(), wscommon.Namespace, jobName)
		assertPodCountEventually(t.T(), wsjob.Namespace, jobName, "", 0)
	})

	t.Run("pods_have_k8s_scheduler_constraints", func() {
		t.SetupSubTest()
		roleFromSelector := func(s map[string]string) string {
			prefix := RoleLabelKeyF[:len(RoleLabelKeyF)-2]
			for k := range s {
				if strings.HasPrefix(k, prefix) {
					return k[len(prefix):]
				}
			}
			return ""
		}

		job := newWsExecuteJob(wscommon.Namespace, "ws-node-selectors", 2)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		expectedPodCount := countNonBrPods(job) + 12 // 12 to account for 12 total BRs created at runtime

		pods := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", expectedPodCount)
		for _, pod := range pods.Items {
			tolerationKeys := wscommon.Map(func(t v1.Toleration) string { return t.Key }, pod.Spec.Tolerations)
			if pod.Labels["replica-type"] == strings.ToLower(rlv1.CoordinatorRequestName) {
				assert.Contains(t.T(), tolerationKeys, "node-role.kubernetes.io/master")
				assert.Contains(t.T(), tolerationKeys, "node-role.kubernetes.io/control-plane")
			} else {
				assert.NotContains(t.T(), tolerationKeys, "node-role.kubernetes.io/master")
				assert.NotContains(t.T(), tolerationKeys, "node-role.kubernetes.io/control-plane")
			}

			assert.NotEmpty(t.T(), roleFromSelector(pod.Spec.NodeSelector))
			assert.NotEmpty(t.T(), pod.Spec.NodeSelector[corev1.LabelHostname])

			// check that the memory env var is set for runtime pods
			if quantity, ok := map[string]resource.Quantity{
				strings.ToLower(rlv1.WeightRequestName):     defaultWgtMem,
				strings.ToLower(rlv1.ActivationRequestName): defaultActMem,
				strings.ToLower(rlv1.CommandRequestName):    defaultCmdMem,
			}[pod.Labels["replica-type"]]; ok {
				assert.Contains(t.T(), pod.Spec.Containers[0].Env, corev1.EnvVar{
					Name:  wsapisv1.RuntimeMemoryEnvVarName,
					Value: strconv.Itoa(int(quantity.Value() >> 20)),
				})
			}
		}
	})

	t.Run("lock_mem_upper_bound", func() {
		t.SetupSubTest()
		trainJob := newWsExecuteJob(wscommon.Namespace, "ws-lock-upper-bound", 1)
		workerMemQuantity := *resource.NewQuantity(int64(wscommon.DefaultNodeMem/8)<<20, resource.BinarySI)
		streamerMemQuantity := *resource.NewQuantity(int64(wscommon.DefaultNodeMem*3/8)<<20, resource.BinarySI)
		for rt, spec := range trainJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/4)<<20, resource.BinarySI),
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(1))
			} else if rt == wsapisv1.WSReplicaTypeWeight {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/4)<<20, resource.BinarySI),
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(1))
			} else if rt == wsapisv1.WSReplicaTypeWorker {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: workerMemQuantity,
				}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: workerMemQuantity,
				}
				spec.Template.Spec.Containers[1].Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: streamerMemQuantity,
				}
				spec.Template.Spec.Containers[1].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: streamerMemQuantity,
				}
			}
		}
		require.NoError(t.T(), k8sClient.Create(ctx, trainJob))
		defer assertWsjobCleanedUp(t.T(), trainJob)

		pods := assertPodCountEventually(t.T(), trainJob.Namespace, "", rlv1.ActivationRequestName, 1)
		pod := pods.Items[0]
		limits := pod.Spec.Containers[0].Resources.Limits
		assert.Equal(t.T(), wscommon.DefaultNodeMem, int(limits.Memory().Value()>>20))
		foundMemEnvVar := false
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
				assert.Equal(t.T(), strconv.Itoa(wscommon.DefaultNodeMem), env.Value)
				foundMemEnvVar = true
			}
		}
		assert.True(t.T(), foundMemEnvVar)

		// Should not set limit if only request was originally specified
		pods = assertPodCountEventually(t.T(), trainJob.Namespace, "", rlv1.WeightRequestName, 1)
		pod = pods.Items[0]
		assert.Equal(t.T(), len(pod.Spec.Containers[0].Resources.Limits), 0)
		requests := pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), wscommon.DefaultNodeMem, int(requests.Memory().Value()>>20))

		lock := assertLockExists(t.T(), trainJob.Namespace, trainJob.Name)
		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)
		for _, ub := range lock.Status.GroupResourceGrantUpperBounds {
			assert.Equal(t.T(), wscommon.DefaultNodeMem, ub.Subresources[wsresource.MemSubresource])
		}

		// Multi-container worker pod should have the resource plumbed through to the container level
		pods = assertPodCountEventually(t.T(), trainJob.Namespace, "", rlv1.WorkerRequestName, 2)
		pod = pods.Items[0]
		assert.Equal(t.T(), wscommon.DefaultNodeMem/8, int(pod.Spec.Containers[0].Resources.Limits.Memory().Value()>>20))
		assert.Equal(t.T(), wscommon.DefaultNodeMem/8, int(pod.Spec.Containers[0].Resources.Requests.Memory().Value()>>20))
		assert.Equal(t.T(), 3*wscommon.DefaultNodeMem/8, int(pod.Spec.Containers[1].Resources.Limits.Memory().Value()>>20))
		assert.Equal(t.T(), 3*wscommon.DefaultNodeMem/8, int(pod.Spec.Containers[1].Resources.Requests.Memory().Value()>>20))
	})

	t.Run("lock_mem_upper_bound_+_limit", func() {
		t.SetupSubTest()
		workflow := "ws-lock-max-limit"
		trainJob := newWsExecuteJob(wscommon.Namespace, "ws-lock-max-limit", 2)
		trainJob.Labels[wsapisv1.WorkflowIdLabelKey] = workflow
		// set limit overhead to 1gb, 4.5g*0.1<1g
		overheadMB := 1 << 10
		trainJob.Annotations[wsapisv1.RtNodeMaxLimitReservedMemGbKey] = "1"
		trainJob.Annotations[wsapisv1.RtNodeMaxLimitReservedMemRatioKey] = "0.1"
		for rt, spec := range trainJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeWeight {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/4)<<20, resource.BinarySI),
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 2) << 20),
					wsapisv1.PodMemoryMaxLimitKey:   strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(15))
			} else if rt == wsapisv1.WSReplicaTypeActivation {
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wscommon.DefaultNodeMem/4)<<20, resource.BinarySI),
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa((wscommon.DefaultNodeMem / 2) << 20),
					wsapisv1.PodMemoryMaxLimitKey:   strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(2))
			}
		}
		require.NoError(t.T(), k8sClient.Create(ctx, trainJob))
		defer assertWsjobCleanedUp(t.T(), trainJob)

		nodeAllocations := map[string][]int{}
		replicaTypeAllocations := map[string]map[string][]int{}
		replicaTypeMinMem := map[string]int{}
		replicaToNode := map[string]string{}
		replicaToRequest := map[string]int{}
		// 1 CRD + 15 WGT + 2 CHF + 2 ACT + 1 CMD + 12 BR + 4 WRK
		pods := assertPodCountEventually(t.T(), trainJob.Namespace, trainJob.Name, "", 1+15+2+2+1+12+4)
		runtimeType := map[string]bool{
			strings.ToLower(rlv1.ActivationRequestName): true,
			strings.ToLower(rlv1.ChiefRequestName):      true,
			strings.ToLower(rlv1.CommandRequestName):    true,
			strings.ToLower(rlv1.WeightRequestName):     true,
		}
		lock := assertLockExists(t.T(), trainJob.Namespace, trainJob.Name)
		assertLocked(t.T(), lock)
		for _, pod := range pods.Items {
			rt := pod.Labels[commonv1.ReplicaTypeLabel]
			if !runtimeType[rt] {
				continue
			}
			requests := pod.Spec.Containers[0].Resources.Requests
			request := int(requests.Memory().Value() >> 20)
			limits := pod.Spec.Containers[0].Resources.Limits
			limit := int(limits.Memory().Value() >> 20)
			if rt == strings.ToLower(rlv1.ActivationRequestName) {
				assert.Equal(t.T(), request, limit)
			} else {
				assert.Zero(t.T(), limit)
			}
			if val, ok := replicaTypeMinMem[rt]; !ok || request < val {
				replicaTypeMinMem[rt] = request
			}
			no := pod.Spec.NodeSelector["kubernetes.io/hostname"]
			nodeAllocations[no] = append(nodeAllocations[no], request)
			replicaToNode[pod.Name] = no
			replicaToRequest[pod.Name] = request
			replicaTypeAllocations[rt] = wscommon.EnsureMap(replicaTypeAllocations[rt])
			replicaTypeAllocations[rt][no] = append(replicaTypeAllocations[rt][no], request)
			foundMemEnvVar := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
					assert.Equal(t.T(), strconv.Itoa(request), env.Value)
					foundMemEnvVar = true
				}
			}
			if request > 0 {
				assert.True(t.T(), foundMemEnvVar)
			}
		}

		//t.T().Log(nodeAllocations)
		//t.T().Log(replicaTypeAllocations)
		//t.T().Log(replicaToRequest)
		//t.T().Log(replicaToNode)
		for _, vals := range nodeAllocations {
			sums := 0
			for _, val := range vals {
				sums += val
			}
			assert.True(t.T(), sums <= wscommon.DefaultNodeMem)
		}

		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)
		for _, ub := range lock.Status.GroupResourceGrantUpperBounds {
			//t.T().Log(ub)
			// evenly share upper bound
			assert.Equal(t.T(), wscommon.DefaultNodeMem/2, ub.Subresources[wsresource.MemSubresource])
		}

		for po, val := range replicaToRequest {
			if strings.Contains(po, strings.ToLower(rlv1.ActivationRequestName)) {
				if len(nodeAllocations[replicaToNode[po]]) == 1 {
					// max percent if single assigned
					assert.Equal(t.T(), wscommon.DefaultNodeMem-overheadMB, val)
				} else {
					// evenly sharing with WGT upper bound
					assert.Equal(t.T(), wscommon.DefaultNodeMem/2, val)
				}
			} else if strings.Contains(po, strings.ToLower(rlv1.WeightRequestName)) {
				if len(nodeAllocations[replicaToNode[po]]) == 1 {
					// max percent if single assigned
					assert.Equal(t.T(), wscommon.DefaultNodeMem-overheadMB, val)
				} else if len(nodeAllocations[replicaToNode[po]]) == 2 {
					if len(replicaTypeAllocations[strings.ToLower(rlv1.WeightRequestName)][replicaToNode[po]]) == 2 {
						// even split with max percent
						assert.Equal(t.T(), wscommon.DefaultNodeMem/2, val)
					} else {
						// evenly sharing with ACT upper bound
						assert.Equal(t.T(), wscommon.DefaultNodeMem/2, val)
					}
				} else {
					// shared with CMD, assign max percent left over
					assert.Equal(t.T(),
						wscommon.DefaultNodeMem-overheadMB-wscommon.DefaultNodeMem/8, val)
				}
			}
		}

		// create a compile job of same flow to validate set min env var
		compile := newWsCompileJob(wscommon.Namespace, "eval-compile-0", 0)
		compile.Labels[wsapisv1.WorkflowIdLabelKey] = workflow
		require.NoError(t.T(), k8sClient.Create(ctx, compile))
		defer assertWsjobCleanedUp(t.T(), compile)
		pods = assertPodCountEventually(t.T(), compile.Namespace, compile.Name, rlv1.CoordinatorRequestName, 1)
		l := assertLockExists(t.T(), compile.Namespace, compile.Name)
		t.T().Log(l.Annotations)
		assertLocked(t.T(), l)
		assert.Equal(t.T(), rlv1.PrioritizedCompileQueueName, l.Spec.QueueName)
		foundMinAct := false
		foundMinMgt := false
		//t.T().Log(pods.Items[0].Spec.Containers[0].Env)
		//t.T().Log(replicaTypeMinMem)
		for _, env := range pods.Items[0].Spec.Containers[0].Env {
			if env.Name == "WS_RT_MIN_ACT_SET_MEM_MIB" {
				assert.Equal(t.T(), strconv.Itoa(replicaTypeMinMem[wsapisv1.WSReplicaTypeActivation.Lower()]), env.Value)
				foundMinAct = true
			}
			if env.Name == "WS_RT_MIN_WGT_SET_MEM_MIB" {
				assert.Equal(t.T(), strconv.Itoa(replicaTypeMinMem[wsapisv1.WSReplicaTypeActivation.Lower()]), env.Value)
				foundMinMgt = true
			}
		}
		assert.True(t.T(), foundMinAct)
		assert.True(t.T(), foundMinMgt)
	})

	t.Run("multi-container_mem_redistribution_for_upper_bound_granting", func() {
		t.SetupSubTest()
		trainJob := newWsExecuteJob(wscommon.Namespace, "multi-container-mem-granting", 1)

		// Weight replica memory values
		wgtMainContainerMemBase, _ := wscommon.ScaleAndRoundUp(wscommon.DefaultNodeMem, 16, 3)
		wgtSidecarContainerMemBase, _ := wscommon.ScaleAndRoundUp(wscommon.DefaultNodeMem, 16, 1)
		wgtMainContainerMemExpectedGrant, _ := wscommon.ScaleAndRoundUp(wscommon.DefaultNodeMem, 8, 3)
		wgtSidecarContainerMemExpectedGrant, _ := wscommon.ScaleAndRoundUp(wscommon.DefaultNodeMem, 8, 1)

		// Activation replica memory values for the new split (activation main + activation sidecar)
		actMainContainerMemBase, _ := wscommon.ScaleAndRoundUp(wscommon.DefaultNodeMem, 16, 3)
		actSidecarContainerMemBase, _ := wscommon.ScaleAndRoundUp(wscommon.DefaultNodeMem, 16, 1)
		actMainContainerMemExpectedGrant, _ := wscommon.ScaleAndRoundUp(wscommon.DefaultNodeMem, 8, 3)
		actSidecarContainerMemExpectedGrant, _ := wscommon.ScaleAndRoundUp(wscommon.DefaultNodeMem, 8, 1)

		// Expected runtime memory environment variable for both main and sidecar containers:
		expectedRuntimeMem := strconv.Itoa(wscommon.DefaultNodeMem / 2)

		// Iterate through each replica type in the job spec.
		for rt, spec := range trainJob.Spec.WSReplicaSpecs {
			if rt == wsapisv1.WSReplicaTypeActivation {
				// For activation replica, remove any preset limits and set requests on the main container.
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(actMainContainerMemBase)<<20, resource.BinarySI),
				}
				// Append the activation sidecar container.
				spec.Template.Spec.Containers = append(spec.Template.Spec.Containers, *spec.Template.Spec.Containers[0].DeepCopy())
				spec.Template.Spec.Containers[1].Name = wsapisv1.UserSidecarContainerName
				// Set memory requests for the sidecar.
				spec.Template.Spec.Containers[1].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(actSidecarContainerMemBase)<<20, resource.BinarySI),
				}
				// Set pod-level annotations (common for both activation and weight replicas).
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryBaseKey:       strconv.Itoa(wscommon.DefaultNodeMem / 4 << 20),
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(12))
			} else if rt == wsapisv1.WSReplicaTypeWeight {
				// For weight replica, clear limits and set requests on the main container.
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wgtMainContainerMemBase)<<20, resource.BinarySI),
				}
				// Append the weight sidecar.
				spec.Template.Spec.Containers = append(spec.Template.Spec.Containers, *spec.Template.Spec.Containers[0].DeepCopy())
				spec.Template.Spec.Containers[1].Name = wsapisv1.UserSidecarContainerName
				spec.Template.Spec.Containers[1].Resources.Requests = corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(int64(wgtSidecarContainerMemBase)<<20, resource.BinarySI),
				}
				spec.Template.Annotations = map[string]string{
					wsapisv1.PodMemoryBaseKey:       strconv.Itoa(wscommon.DefaultNodeMem / 4 << 20),
					wsapisv1.PodMemoryUpperBoundKey: strconv.Itoa(wscommon.DefaultNodeMem << 20),
				}
				spec.Replicas = wscommon.Pointer(int32(12))
			} else if rt == wsapisv1.WSReplicaTypeCommand {
				// For command replicas, no resource settings.
				spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
				spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{}
				spec.Replicas = wscommon.Pointer(int32(1))
			}
		}
		require.NoError(t.T(), k8sClient.Create(ctx, trainJob))
		defer assertWsjobCleanedUp(t.T(), trainJob)

		// Validate activation pods (two containers similiar to weight pods)
		pods := assertPodCountEventually(t.T(), trainJob.Namespace, "", rlv1.ActivationRequestName, 12)
		pod := pods.Items[0]

		// Main activation container assertions (container index 0)
		mainRequests := pod.Spec.Containers[0].Resources.Requests
		assert.Equal(t.T(), int(actMainContainerMemExpectedGrant), int(mainRequests.Memory().Value()>>20))
		mainRuntimeMem := ""
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
				mainRuntimeMem = env.Value
				break
			}
		}
		assert.Equal(t.T(), expectedRuntimeMem, mainRuntimeMem)

		// Activation sidecar container assertions (container index 1)
		sidecarRequests := pod.Spec.Containers[1].Resources.Requests
		// (Mimic the weight sidecar adjustment if needed; here we subtract 1 as in the weight branch)
		assert.Equal(t.T(), int(actSidecarContainerMemExpectedGrant-1), int(sidecarRequests.Memory().Value()>>20))
		sidecarRuntimeMem := ""
		for _, env := range pod.Spec.Containers[1].Env {
			if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
				sidecarRuntimeMem = env.Value
				break
			}
		}
		assert.Equal(t.T(), expectedRuntimeMem, sidecarRuntimeMem)

		// Validate weight pods
		pods = assertPodCountEventually(t.T(), trainJob.Namespace, "", rlv1.WeightRequestName, 12)
		pod = pods.Items[0]
		assert.Equal(t.T(), len(pod.Spec.Containers[0].Resources.Limits), 0)
		assert.Equal(t.T(), int(wgtMainContainerMemExpectedGrant), int(pod.Spec.Containers[0].Resources.Requests.Memory().Value()>>20))
		wgtMainContainerSetMem := ""
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
				wgtMainContainerSetMem = env.Value
				break
			}
		}
		assert.Equal(t.T(), strconv.Itoa(wscommon.DefaultNodeMem/2), wgtMainContainerSetMem)
		assert.Equal(t.T(), len(pod.Spec.Containers[1].Resources.Limits), 0)
		assert.Equal(t.T(), int(wgtSidecarContainerMemExpectedGrant-1), int(pod.Spec.Containers[1].Resources.Requests.Memory().Value()>>20))
		wgtSidecarContainerSetMem := ""
		for _, env := range pod.Spec.Containers[1].Env {
			if env.Name == wsapisv1.RuntimeMemoryEnvVarName {
				wgtSidecarContainerSetMem = env.Value
				break
			}
		}
		assert.Equal(t.T(), strconv.Itoa(wscommon.DefaultNodeMem/2), wgtSidecarContainerSetMem)

		// Validate lock upper bounds remain consistent (each group should have half the default node memory granted)
		lock := assertLockExists(t.T(), trainJob.Namespace, trainJob.Name)
		require.Len(t.T(), lock.Status.GroupResourceGrantUpperBounds, 2)
		for _, ub := range lock.Status.GroupResourceGrantUpperBounds {
			assert.Equal(t.T(), wscommon.DefaultNodeMem/2, ub.Subresources[wsresource.MemSubresource])
		}
	})

	t.Run("node_group_label_and_node_assignment", func() {
		t.SetupSubTest()
		sysCount := 4
		require.LessOrEqual(t.T(), sysCount, systemCount)
		wsjob := newWsExecuteJob(wscommon.Namespace, "ng-req", uint32(sysCount))
		wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(12))
		clearResourceLimits(wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight])
		wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker].Replicas = wscommon.Pointer(int32(32))
		wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeActivation].Replicas = wscommon.Pointer(int32(sysCount) * 2)
		defer assertWsjobCleanedUp(t.T(), wsjob)

		// 1 CRD + 4 ACT + 32 WRK + 4 CHF + 1 CMD + 12 WGT + 12 BRs
		expectedPodCount := countNonBrPods(wsjob) + 12
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))

		pods := assertPodCountEventually(t.T(), wsjob.Namespace, wsjob.Name, "", expectedPodCount)

		type ReplicaTypeGroup struct {
			ReplicaType string
			Group       string
		}
		type ReplicaTypeGroupInfo struct {
			Count          int
			ReplicaIndexes []int
		}
		rtgCount := map[ReplicaTypeGroup]ReplicaTypeGroupInfo{}
		for _, pod := range pods.Items {
			rt := pod.Labels[commonv1.ReplicaTypeLabel]
			nodeName := pod.Spec.NodeSelector[corev1.LabelHostname]
			ng := t.CfgMgr.Cfg.NodeMap[nodeName].GetGroup()
			key := ReplicaTypeGroup{ReplicaType: rt, Group: ng}
			info := rtgCount[key]
			info.Count++
			index, _ := strconv.Atoi(pod.Labels[commonv1.ReplicaIndexLabel])
			info.ReplicaIndexes = append(info.ReplicaIndexes, index)
			rtgCount[key] = info
		}

		// Each group should hold consecutive replica indexes.
		for key, info := range rtgCount {
			sort.Ints(info.ReplicaIndexes)
			for i := 1; i < len(info.ReplicaIndexes); i++ {
				assert.Equalf(t.T(), info.ReplicaIndexes[i-1]+1, info.ReplicaIndexes[i],
					"replica indexes should be consecutive but got %s/%s, %v", key.ReplicaType, key.Group, info.ReplicaIndexes)
			}
		}

		getNgCounts := func(rt commonv1.ReplicaType) []int {
			var c []int
			for key, info := range rtgCount {
				if key.ReplicaType == strings.ToLower(string(rt)) {
					c = append(c, info.Count)
				}
			}
			return c
		}

		t.Require().ElementsMatch([]int{8, 8, 8, 8}, getNgCounts(wsapisv1.WSReplicaTypeWorker))
		t.Require().ElementsMatch([]int{2, 2, 2, 2}, getNgCounts(wsapisv1.WSReplicaTypeActivation))
		t.Require().ElementsMatch([]int{1, 1, 1, 1}, getNgCounts(wsapisv1.WSReplicaTypeChief))
		t.Require().ElementsMatch([]int{12}, getNgCounts(wsapisv1.WSReplicaTypeWeight))
	})

	t.Run("coordinator_limit_override_if_min_provided", func() {
		t.SetupSubTest()
		wsapisv1.CrdExecuteMinMemoryBytes = wscommon.DefaultNodeMem << 20
		wsapisv1.IsGenoaServer = true
		defer func() {
			wsapisv1.CrdExecuteMinMemoryBytes = 0
			wsapisv1.IsGenoaServer = false
		}()
		job := newWsExecuteJob(wscommon.Namespace, "crd-without-resource-requests", 1)
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		container := assertCoordinatorWsContainerCreated(t.T(), job)
		requests := container.Resources.Requests
		limits := container.Resources.Limits
		assert.True(t.T(), requests.Cpu().IsZero())
		assert.True(t.T(), limits.Cpu().IsZero())
		assert.True(t.T(), requests.Memory().IsZero())
		assert.True(t.T(), limits.Memory().IsZero())
		assertWsjobCleanedUp(t.T(), job)

		job = newWsExecuteJob(wscommon.Namespace, "crd-with-lower-requests", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		container = assertCoordinatorWsContainerCreated(t.T(), job)
		requests = container.Resources.Requests
		limits = container.Resources.Limits
		assert.Equal(t.T(), wsapisv1.CrdExecuteMinMemoryBytes, int(limits.Memory().Value()))
		assert.Equal(t.T(), wsapisv1.CrdExecuteMinMemoryBytes, int(requests.Memory().Value()))
		assert.Equal(t.T(), defaultCrdCpu.MilliValue()/2, requests.Cpu().MilliValue())
		assertWsjobCleanedUp(t.T(), job)

		// update min to very small number
		wsapisv1.CrdExecuteMinMemoryBytes = 1
		job = newWsExecuteJob(wscommon.Namespace, "crd-with-larger-requests", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		container = assertCoordinatorWsContainerCreated(t.T(), job)
		requests = container.Resources.Requests
		limits = container.Resources.Limits
		assert.True(t.T(), wsapisv1.CrdExecuteMinMemoryBytes < int(limits.Memory().Value()))
		assert.True(t.T(), wsapisv1.CrdExecuteMinMemoryBytes < int(requests.Memory().Value()))
		assert.Equal(t.T(), defaultCrdCpu.MilliValue()/2, requests.Cpu().MilliValue())
		assertWsjobCleanedUp(t.T(), job)
	})

	t.Run("coordinator_resources_requests_preserved_when_present", func() {
		t.SetupSubTest()
		job := newWsExecuteJob(wscommon.Namespace, "crd-with-resource-requests", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		container := assertCoordinatorWsContainerCreated(t.T(), job)
		requests := container.Resources.Requests
		assert.True(t.T(), defaultCrdCpu.Equal(*requests.Cpu()))
		assert.True(t.T(), defaultCrdMem.Equal(*requests.Memory()))

		lock := assertLockExists(t.T(), job.Namespace, job.Name)
		requestExists := false
		for _, rr := range lock.Spec.ResourceRequests {
			if rr.Name == rlv1.CoordinatorRequestName {
				assert.Equal(t.T(), defaultCrdCpu.MilliValue(), int64(rr.Subresources[wsresource.CPUSubresource]))
				assert.Equal(t.T(), defaultCrdMem.Value(), int64(rr.Subresources[wsresource.MemSubresource]<<20))
				requestExists = true
			}
		}
		assert.True(t.T(), requestExists)
	})

	t.Run("coordinator_OOM_killed_event_custom_error_message", func() {
		t.SetupSubTest()
		job := newWsExecuteJob(wscommon.Namespace, "crd-oom", 1)
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator].Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: *resource.NewQuantity((1<<20)*132+1, resource.BinarySI),
			},
		}
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		// simulate job in Running state
		pods := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", countNonBrPods(job))
		for _, pod := range pods.Items {
			pod.Status.Phase = corev1.PodRunning
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}
		assertWsJobStatusEventually("Running")(t.T(), job.Namespace, job.Name)

		// simulate coordinator OOM killed
		pods = assertPodCountEventually(t.T(), job.Namespace, job.Name, rlv1.CoordinatorRequestName, 1)
		pod := pods.Items[0]
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
			Name: wsapisv1.DefaultContainerName,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 1, Message: "OOMKilled"},
			},
		})
		pod.Status.Phase = corev1.PodFailed
		require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		assertWsJobFailEventually(t.T(), job.Namespace, job.Name)

		// ensure Warning event with detail OOM message generated
		events := &corev1.EventList{}
		require.NoError(t.T(), k8sReader.List(ctx, events,
			client.MatchingFieldsSelector{
				Selector: fields.AndSelectors(
					fields.ParseSelectorOrDie("involvedObject.kind=Pod"),
					fields.OneTermEqualSelector("type", "Warning"),
				),
			}))
		for _, e := range events.Items {
			t.T().Log(e.Message)
		}
		require.NotEmpty(t.T(), events.Items)
		assert.Contains(t.T(), events.Items[0].Message, "killed due to an out of memory")
		assert.Contains(t.T(), events.Items[0].Message, "132Mi")
	})

	t.Run("parallel_executes_do_not_starve_compiles", func() {
		t.SetupSubTest()
		// create as many execute jobs are there are cs2 resource limits, add 1, ensure queued, then create a compile
		// and ensure it is not queued

		var trains []*wsapisv1.WSJob
		for i := 0; i < systemCount+1; i++ {
			job := newWsExecuteJob(wscommon.Namespace, fmt.Sprintf("train-starve-%d", i), 1)
			require.NoError(t.T(), k8sClient.Create(ctx, job))
			trains = append(trains, job)
		}

		defer cleanupAllJobs(t.T(), trains)

		assertPodCountEventually(t.T(), "", "", rlv1.CoordinatorRequestName, systemCount)

		// create a compile job and ensure it starts
		compile := newWsCompileJob(wscommon.Namespace, "compile-starve", 0)
		require.NoError(t.T(), k8sClient.Create(ctx, compile))
		defer assertWsjobCleanedUp(t.T(), compile)
		assertPodCountEventually(t.T(), compile.Namespace, compile.Name, rlv1.CoordinatorRequestName, 1)

		locks := &rlv1.ResourceLockList{}
		require.NoError(t.T(), k8sClient.List(context.Background(), locks, client.MatchingLabels{}))
		var lockGranted, lockPending int
		for _, lock := range locks.Items {
			if lock.Status.State == rlv1.LockGranted {
				lockGranted++
			} else if lock.Status.State == rlv1.LockPending {
				lockPending++
			}
		}
		assert.Equal(t.T(), systemCount+1, lockGranted)
		assert.Equal(t.T(), 1, lockPending)
	})
}

func (t *WSJobControllerTestSuite) TestSchedulerHint() {
	ctx := context.Background()
	t.Run("scheduler-hint-allow-systems_set", func() {
		t.SetupSubTest()
		sysNames := wscommon.Map(func(sys *systemv1.SystemSpec) string { return sys.Name }, t.JobReconciler.Cfg.Systems)
		rand.Shuffle(len(sysNames), func(i, j int) { sysNames[i], sysNames[j] = sysNames[j], sysNames[i] })
		jobs := map[string]*wsapisv1.WSJob{}
		for i := 0; i < len(sysNames); i += 2 {
			nsys := wscommon.Min(len(sysNames)-i, 2)
			name := fmt.Sprintf("hint-%s", strings.Join(sysNames[i:i+nsys], "-"))
			job := newWsExecuteJob(wscommon.Namespace, name, uint32(nsys))
			job.Annotations[wsapisv1.SchedulerHintAllowSys] = strings.Join(sysNames[i:i+nsys], ",")
			jobs[name] = job
			require.NoError(t.T(), k8sClient.Create(ctx, job))
		}

		awaitJobs := wscommon.Values(jobs)
		assert.Eventually(t.T(), func() bool {
			var unstarted []*wsapisv1.WSJob
			for _, job := range awaitJobs {
				assert.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job))
				conditions := job.Status.Conditions
				if len(conditions) > 0 && conditions[len(conditions)-1].Type == commonv1.JobScheduled &&
					job.Annotations[SystemNameAnnotationKey] != "" {
					assert.ElementsMatch(t.T(),
						strings.Split(job.Annotations[wsapisv1.SchedulerHintAllowSys], ","),
						strings.Split(job.Annotations[SystemNameAnnotationKey], ","))
				} else {
					unstarted = append(unstarted, job)
				}
			}
			awaitJobs = unstarted
			return len(awaitJobs) == 0
		}, eventuallyTimeout, eventuallyTick)

		cleanupAllJobs(t.T(), wscommon.Values(jobs))
		assertLockCountEventually(t.T(), 0)
	})

	t.Run("scheduler-hint-allow-nodegroups_set", func() {
		t.SetupSubTest()

		var ng []string
		for _, g := range t.JobReconciler.Cfg.Groups {
			ng = append(ng, g.Name)
		}

		// create list of groupings where the first half of systems are in groups of 2, the second, groups of 1
		// e.g. [a,b] [d] [c]
		shuffleSublists := func(a []string) [][]string {
			rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
			var rv [][]string
			ia := 0
			for ia < len(a) {
				ian := ia + 1
				if ia < len(a)/2 {
					ian += 1
				}
				if ian > len(a) {
					ian = len(a)
				}
				rv = append(rv, a[ia:ian])
				ia = ian
			}
			return rv
		}

		jobs := map[string]*wsapisv1.WSJob{}
		for _, ngs := range shuffleSublists(ng) {
			name := fmt.Sprintf("hint-%s", strings.Join(ngs, "-"))
			job := newWsExecuteJob(wscommon.Namespace, name, uint32(len(ngs)))
			job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(1))
			job.Annotations[wsapisv1.SchedulerHintAllowNG] = strings.Join(ngs, ",")
			jobs[name] = job
			require.NoError(t.T(), k8sClient.Create(ctx, job))
		}

		awaitJobs := wscommon.Values(jobs)
		assert.Eventually(t.T(), func() bool {
			var unstarted []*wsapisv1.WSJob
			for _, job := range awaitJobs {
				assert.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job))
				conditions := job.Status.Conditions
				if len(conditions) > 0 && conditions[len(conditions)-1].Type == commonv1.JobScheduled &&
					job.Annotations[NodeGroupAnnotationKey] != "" {
					assert.ElementsMatch(t.T(),
						strings.Split(job.Annotations[wsapisv1.SchedulerHintAllowNG], ","),
						strings.Split(job.Annotations[NodeGroupAnnotationKey], ","))
				} else {
					unstarted = append(unstarted, job)
				}
			}
			awaitJobs = unstarted
			return len(awaitJobs) == 0
		}, eventuallyTimeout, eventuallyTick)

		cleanupAllJobs(t.T(), wscommon.Values(jobs))
		assertLockCountEventually(t.T(), 0)
	})
	t.Run("scheduler-hint-allow-systems-unhealthy", func() {
		t.SetupSubTest()

		// Set a system to be unhealthy
		sysName := t.JobReconciler.Cfg.Systems[0].Name
		ns := wscommon.Namespace
		s := &systemv1.System{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: sysName}, s))
		s.Spec.Unschedulable = true
		require.NoError(t.T(), k8sClient.Update(ctx, s))
		require.Eventually(t.T(), func() bool {
			_, exists := t.ResourceController.Cfg.UnhealthySystems[sysName]
			return exists
		}, eventuallyTimeout, eventuallyTick)

		// 1. we can schedule jobs with unhealthy systems whenEnableAllowUnhealthySystems is set.
		jobAllowSystemWithEnablement := newWsExecuteJob(ns, "job-allowed-unhealthy", 1)
		jobAllowSystemWithEnablement.Annotations[wsapisv1.EnableAllowUnhealthySystems] = "true"
		jobAllowSystemWithEnablement.Annotations[wsapisv1.SchedulerHintAllowSys] = sysName
		require.NoError(t.T(), k8sClient.Create(ctx, jobAllowSystemWithEnablement))
		assertWsJobScheduledEventually(t.T(), ns, jobAllowSystemWithEnablement.Name)

		// 2. spec-creation fails when EnableAllowUnhealthySystems is NOT set, but allow-systems are specified
		// See TestSpecValidation in wsjob_client_test.go for this

		// 3. scheduling fails when the hint system is unhealthy and EnableAllowUnhealthySystems is NOT set
		jobOnUnhealthySystem := newWsExecuteJob(ns, "job-on-unhealthy-system", 1)
		jobOnUnhealthySystem.Annotations[wsapisv1.SchedulerHintAllowSys] = sysName
		require.NoError(t.T(), k8sClient.Create(ctx, jobOnUnhealthySystem))
		assertWsJobFailEventually(t.T(), ns, jobOnUnhealthySystem.Name)

		// Clean up these two before the next test
		cleanupAllJobs(t.T(), []*wsapisv1.WSJob{jobAllowSystemWithEnablement, jobOnUnhealthySystem})

		// 4. scheduling two jobs in sequence causes the second job to queue up behind the first one
		job1 := newWsExecuteJob(ns, "job-1", 1)
		job1.Annotations[wsapisv1.SchedulerHintAllowSys] = sysName
		job1.Annotations[wsapisv1.EnableAllowUnhealthySystems] = "true"
		require.NoError(t.T(), k8sClient.Create(ctx, job1))
		assertWsJobScheduledEventually(t.T(), ns, job1.Name)

		// Second job should be queued up behind the first one
		job2 := newWsExecuteJob(ns, "job-2", 1)
		job2.Annotations[wsapisv1.EnableAllowUnhealthySystems] = "true"
		job2.Annotations[wsapisv1.SchedulerHintAllowSys] = sysName
		require.NoError(t.T(), k8sClient.Create(ctx, job2))

		// Wait to ensure we're really queued and waiting, instead of ephemerally queued before scheduling
		time.Sleep(1 * time.Second)
		assertWsJobQueuedEventually(t.T(), ns, job2.Name)

		// Complete the first job, then verify that the second job is scheduled
		pods := getJobPods(t.T(), job1.Namespace, job1.Name, "")
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}

		assertWsJobSuccessEventually(t.T(), ns, job1.Name)
		assertWsJobScheduledEventually(t.T(), ns, job2.Name)

		// Remove the unhealthy systems (it affects other tests!)
		s.Spec.Unschedulable = false
		require.NoError(t.T(), k8sClient.Update(ctx, s))
		require.Never(t.T(), func() bool {
			_, exists := t.ResourceController.Cfg.UnhealthySystems[sysName]
			return exists
		}, eventuallyTimeout, eventuallyTick)

		cleanupAllJobs(t.T(), []*wsapisv1.WSJob{job1, job2})
		assertLockCountEventually(t.T(), 0)
	})

	for _, testcase := range []struct {
		name     string
		job      *wsapisv1.WSJob
		errorMsg string
	}{
		{
			name: "hint-on-non-existing-system",
			job:  wsjobWithAnnotations(newWsCompileJob(wscommon.Namespace, "hint-non-exist", uint32(1)), wsapisv1.SchedulerHintAllowSys+":system-non"),
			errorMsg: "Hint system: system-non not found for current NS assignment. Existing systems: " +
				"[system-0 system-1 system-2 system-3 system-4 system-5 system-6 system-7]. Cluster status: Cluster healthy",
		},
		{
			name:     "hint-systems-too-few",
			job:      wsjobWithAnnotations(newWsExecuteJob(wscommon.Namespace, "hint-too-few", uint32(2)), wsapisv1.SchedulerHintAllowSys+":system0"),
			errorMsg: "Job requested 2 systems but only specified 1 hint system(s)",
		},
		{
			name:     "hint-err",
			job:      wsjobWithAnnotations(newWsCompileJob(wscommon.Namespace, "hint-err", uint32(1)), wsapisv1.SchedulerHintPrefix+"foo:bar"),
			errorMsg: "unknown scheduler-hint 'foo'",
		},
		{
			name:     "hint-err-bad-key-value",
			job:      wsjobWithAnnotations(newWsCompileJob(wscommon.Namespace, "hint-err-bad-key-value", uint32(1)), wsapisv1.SchedulerHintWGProps+":"+"xxx"),
			errorMsg: "invalid weight-group-props list",
		},
		{
			name:     "hint-err-required-not-set",
			job:      newWsCompileJob(wscommon.Namespace, "hint-err-required-not-set", uint32(1)),
			errorMsg: "is not set",
		},
	} {
		t.Run(testcase.name, func() {
			t.SetupSubTest()

			wscommon.EnableStrictAllowSystems = true
			defer func() { wscommon.EnableStrictAllowSystems = false }()

			require.NoError(t.T(), k8sClient.Create(ctx, testcase.job))
			defer assertWsjobCleanedUp(t.T(), testcase.job)

			job := assertWsJobFailEventually(t.T(), testcase.job.Namespace, testcase.job.Name)
			conds := job.Status.Conditions
			assert.Contains(t.T(), conds[len(conds)-1].Message, testcase.errorMsg)
		})
	}
}

func (t *WSJobControllerTestSuite) TestCompileCheckSystemFabrics() {
	ctx := context.Background()
	t.Run("set_ok_systems_in_namespace_for_compile_job", func() {
		t.SetupSubTest()
		s := &systemv1.System{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: t.CfgMgr.Cfg.Systems[0].Name}, s))
		s.Spec.Unschedulable = true
		require.NoError(t.T(), k8sClient.Update(ctx, s))
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 1
		}, eventuallyTimeout, eventuallyTick)

		// update config to trigger reload
		updateClusterConfig(t.T(), t.CfgMgr, t.DefaultCfgSchema)
		require.Eventually(t.T(), func() bool {
			return len(t.CfgMgr.Cfg.UnhealthySystems) == 1 && len(t.ResourceController.Cfg.UnhealthySystems) == 1
		}, eventuallyTimeout, eventuallyTick)

		compileJobName := "ok-systems-compile"
		compileJob := newWsCompileJob(wscommon.Namespace, compileJobName, 0)

		defer assertWsjobCleanedUp(t.T(), compileJob)
		require.NoError(t.T(), k8sClient.Create(ctx, compileJob))

		podList := assertPodCountEventually(t.T(), compileJob.Namespace, compileJobName, rlv1.CoordinatorRequestName, 1)
		pod := podList.Items[0]
		assert.True(t.T(), len(pod.Spec.Containers) == 1, "should have only 1 container")
		container := pod.Spec.Containers[0]
		hasOkSystemsEnv := false
		for _, env := range container.Env {
			if env.Name == wscommon.OkSystemsInNamespace {
				hasOkSystemsEnv = true
				var expectedSystems []string
				for index, system := range t.CfgMgr.Cfg.Systems {
					if index != 0 {
						expectedSystems = append(expectedSystems, fmt.Sprintf("%s+%s", system.Name, system.CmAddr))
					}
				}
				sort.Slice(expectedSystems, func(i, j int) bool {
					return expectedSystems[i] < expectedSystems[j]
				})
				assert.Equal(t.T(), strings.Join(expectedSystems, ","), env.Value, "%s value does not match", wscommon.OkSystemsInNamespace)
			}
		}
		assert.True(t.T(), hasOkSystemsEnv, fmt.Sprintf("should include %s", wscommon.OkSystemsInNamespace))

		// update back to healthy
		s.Spec.Unschedulable = false
		require.NoError(t.T(), k8sClient.Update(ctx, s))
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 0
		}, eventuallyTimeout, eventuallyTick)
	})

	t.Run("disable_fabric_check_for_compile_job", func() {
		t.SetupSubTest()
		compileJobName := "disable-fabric-compile"
		compileJob := newWsCompileJob(wscommon.Namespace, compileJobName, 0)
		compileJob.Annotations[wscommon.DisableFabricJsonCheckKey] = "true"

		defer assertWsjobCleanedUp(t.T(), compileJob)
		require.NoError(t.T(), k8sClient.Create(ctx, compileJob))

		podList := assertPodCountEventually(t.T(), compileJob.Namespace, compileJobName, rlv1.CoordinatorRequestName, 1)
		pod := podList.Items[0]
		assert.True(t.T(), len(pod.Spec.Containers) == 1, "should have only 1 container")
		container := pod.Spec.Containers[0]
		hasOkSystemsEnv := false
		for _, env := range container.Env {
			if env.Name == wscommon.OkSystemsInNamespace {
				hasOkSystemsEnv = true
			}
		}
		assert.False(t.T(), hasOkSystemsEnv, fmt.Sprintf("should not include %s", wscommon.OkSystemsInNamespace))
	})
}

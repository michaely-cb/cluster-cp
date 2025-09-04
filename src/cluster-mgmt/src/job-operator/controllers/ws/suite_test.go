package ws

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "cerebras/pb/workflow/appliance/common"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/google/uuid"
	"github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	resourcelockv1 "cerebras.com/job-operator/client-resourcelock/clientset/versioned/typed/resourcelock/v1"
	wsv1 "cerebras.com/job-operator/client/clientset/versioned/typed/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/controllers/cluster"
	"cerebras.com/job-operator/controllers/health"
	rsctrl "cerebras.com/job-operator/controllers/resource"
	// +kubebuilder:scaffold:imports
)

var (
	k8sClient     client.Client
	k8sReader     client.Reader // bypass Cache
	wsV1Interface wsv1.WsV1Interface
	rlV1Interface resourcelockv1.ResourcelockV1Interface
)

type TestSuite struct {
	suite.Suite
	Manager            manager.Manager
	UseExistingCluster bool
	Ctx                context.Context
	Cancel             context.CancelFunc
	TestEnv            *envtest.Environment
	KubeConfigFile     string
	JobReconciler      *WSJobReconciler
	ResourceController *rsctrl.Controller
	LeaseReconciler    *LeaseReconciler
	ConfigReconciler   *cluster.ConfigReconciler
	NodeReconciler     *health.NodeReconciler
	SystemReconciler   *health.SystemReconciler
	WorkflowReconciler *WorkflowReconciler
	CfgMgr             *wscommon.ClusterConfigMgr
	DefaultCfgSchema   wscommon.ClusterSchema
}

const (
	eventuallyTimeout = 10 * time.Second
	eventuallyTick    = 500 * time.Millisecond
	neverTimeout      = 3 * time.Second
	neverTick         = 100 * time.Millisecond

	leaseEventuallyTimeout = 2 * time.Second
	leaseEventuallyTick    = time.Second

	systemCount     = 8
	brCount         = 30
	axCount         = 8
	workersPerGroup = 4
	memoryXPerGroup = 12
)

func (t *TestSuite) TearDownSuite() {
	log.Log.Info("tearing down the test environment")
	t.Cancel()
	require.NoError(t.T(), t.TestEnv.Stop())
	os.Remove(t.KubeConfigFile)
}

func TestTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func CreateKubeconfigFileForRestConfig(restConfig *rest.Config) string {
	clusters := make(map[string]*clientcmdapi.Cluster)
	clusters["default-cluster"] = &clientcmdapi.Cluster{
		Server:                   restConfig.Host,
		CertificateAuthorityData: restConfig.CAData,
	}
	contexts := make(map[string]*clientcmdapi.Context)
	contexts["default-context"] = &clientcmdapi.Context{
		Cluster:  "default-cluster",
		AuthInfo: "default-user",
	}
	authInfos := make(map[string]*clientcmdapi.AuthInfo)
	authInfos["default-user"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: restConfig.CertData,
		ClientKeyData:         restConfig.KeyData,
	}
	clientConfig := clientcmdapi.Config{
		Kind:           "Config",
		APIVersion:     "v1",
		Clusters:       clusters,
		Contexts:       contexts,
		CurrentContext: "default-context",
		AuthInfos:      authInfos,
	}
	kubeConfigFile, _ := os.CreateTemp("", "kubeconfig")
	_ = clientcmd.WriteToFile(clientConfig, kubeConfigFile.Name())
	return kubeConfigFile.Name()
}

func (t *TestSuite) ensureNsUpdateSuccess(nsr *namespacev1.NamespaceReservation) {
	t.ensureNsUpdate(nsr, true)
}

func (t *TestSuite) ensureNsUpdateFailed(nsr *namespacev1.NamespaceReservation) {
	t.ensureNsUpdate(nsr, false)
}

func (t *TestSuite) ensureNsUpdate(nsr *namespacev1.NamespaceReservation, shouldSucceed bool) {
	t.Require().Eventually(func() bool {
		err := k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(nsr), nsr)
		if err != nil {
			t.T().Logf("Error getting NSR: %v", err)
			return false
		}
		return nsr.Status.GetLastAttemptedRequest().Request.RequestUid == nsr.Spec.RequestUid
	}, 2*eventuallyTimeout, eventuallyTick)
	lastAttempt := nsr.Status.GetLastAttemptedRequest()
	t.Require().Equal(shouldSucceed, lastAttempt.Succeeded)
}

func (t *TestSuite) createNsIfNotExists(name string) *namespacev1.NamespaceReservation {
	nsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(nsr), nsr)
	if err != nil && apierrors.IsNotFound(err) {
		nsr.Spec.ReservationRequest.RequestUid = uuid.NewString()
		err = k8sClient.Create(t.Ctx, nsr)
	}
	if err != nil {
		// The NSR could be created between "k8sClient.Get" and "k8sClient.Create"
		t.Require().True(apierrors.IsAlreadyExists(err))
	}
	t.ensureNsUpdateSuccess(nsr)
	return nsr
}

func (t *TestSuite) assignNS(name string, mode namespacev1.NSRUpdateMode,
	systems []string, sysCount, parallelExecuteCount int) {
	nsr := t.createNsIfNotExists(name)
	nsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
		RequestParams: namespacev1.RequestParams{
			UpdateMode:           mode,
			SystemNames:          systems,
			SystemCount:          wscommon.Pointer(sysCount),
			ParallelExecuteCount: wscommon.Pointer(parallelExecuteCount),
		},
		RequestUid: uuid.NewString(),
	}
	require.Eventually(t.T(), func() bool {
		if k8sClient.Update(t.Ctx, nsr) != nil {
			return false
		}
		t.ensureNsUpdateSuccess(nsr)
		return true
	}, eventuallyTimeout, eventuallyTick)
}

func (t *TestSuite) assignEverythingToNS(name string, sysCount int) {
	// We often add/remove systems in tests. Prior to moving all resource to a namespace, we should
	// make sure the cluster config had been updated with the desired number of systems.
	require.Eventually(t.T(), func() bool {
		noPendingConfig := t.ResourceController.NewCfg == nil
		systemCountMatches := len(t.ResourceController.Cfg.Systems) == len(t.CfgMgr.Cfg.Systems)
		return noPendingConfig && systemCountMatches
	}, eventuallyTimeout, eventuallyTick)

	nsr := t.createNsIfNotExists(name)
	nsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
		RequestParams: namespacev1.RequestParams{
			UpdateMode:  namespacev1.SetResourceSpecMode,
			SystemCount: wscommon.Pointer(sysCount),
		},
		RequestUid: uuid.NewString(),
	}
	t.Require().NoError(k8sClient.Update(t.Ctx, nsr))
	t.ensureNsUpdateSuccess(nsr)
}

func (t *TestSuite) updateSessionLabel(name string, key, val string) {
	nsr := t.createNsIfNotExists(name)
	nsr.Labels = wscommon.EnsureMap(nsr.Labels)
	nsr.Labels[key] = val
	t.Require().NoError(k8sClient.Update(t.Ctx, nsr))
}

func (t *TestSuite) unassignEverythingFromNS(name string) {
	require.Eventually(t.T(), func() bool {
		nsr := &namespacev1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: name}}
		t.Require().NoError(k8sClient.Get(t.Ctx, client.ObjectKeyFromObject(nsr), nsr))
		nsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{},
			RequestUid:    uuid.NewString(),
		}
		if k8sClient.Update(t.Ctx, nsr) != nil {
			return false
		}
		t.ensureNsUpdateSuccess(nsr)
		if name != wscommon.DefaultNamespace {
			return k8sClient.Delete(t.Ctx, nsr) == nil
		}
		return true
	}, eventuallyTimeout, eventuallyTick)
}

var (
	assertPending      = assertLockStateEventually(rlv1.LockPending)
	assertLocked       = assertLockStateEventually(rlv1.LockGranted)
	assertNeverGranted = assertLockStateNever(rlv1.LockGranted)

	assertLockStateEventually = func(lockState rlv1.LockState) func(*testing.T, *rlv1.ResourceLock) {
		assertFn := func(t *testing.T, lock *rlv1.ResourceLock) {
			require.Eventually(t, func() bool {
				err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(lock), lock)
				if err != nil {
					t.Logf("Error getting lock %s: %v", lock.NamespacedName(), err)
					return false
				}
				t.Logf("lock name = %s, lock state = %s", lock.NamespacedName(), lock.Status.State)
				return lock.Status.State == lockState
			}, eventuallyTimeout, eventuallyTick)
		}
		return assertFn
	}

	assertLockStateNever = func(lockState rlv1.LockState) func(*testing.T, *rlv1.ResourceLock) {
		assertFn := func(t *testing.T, lock *rlv1.ResourceLock) {
			require.Never(t, func() bool {
				err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(lock), lock)
				if err != nil {
					t.Logf("Error getting lock %s: %v", lock.NamespacedName(), err)
					return false
				}
				t.Logf("lock name = %s, lock state = %s", lock.NamespacedName(), lock.Status.State)
				return lock.Status.State == lockState
			}, neverTimeout, neverTick)
		}
		return assertFn
	}

	assertNgCountsAcquired = func(t *testing.T, wsjob *wsapisv1.WSJob, expectedPopGroups, expectedDepopGroups int) {
		require.Eventually(t, func() bool {
			lock := assertLockExists(t, wsjob.Namespace, wsjob.Name)
			actualPopGroups, actualDepopGroups := lock.GetNgCountAcquired()
			t.Logf("lock name = %s, actualPopGroups = %d, actualDepopGroups = %d",
				lock.NamespacedName(), actualPopGroups, actualDepopGroups)
			return expectedPopGroups == actualPopGroups && expectedDepopGroups == actualDepopGroups
		}, eventuallyTimeout, eventuallyTick)
	}

	assertNgNamesAcquired = func(t *testing.T, wsjob *wsapisv1.WSJob, expectedNgNames []string) {
		require.Eventually(t, func() bool {
			lock := assertLockExists(t, wsjob.Namespace, wsjob.Name)
			var ngNames []string
			for _, gg := range lock.Status.GroupResourceGrants {
				if strings.HasPrefix(gg.Name, rlv1.NodegroupRequestPrefix) {
					ngNames = append(ngNames, gg.GroupingValue)
				}
			}
			return slices.Equal(wscommon.Sorted(expectedNgNames), wscommon.Sorted(ngNames))
		}, eventuallyTimeout, eventuallyTick)
	}

	assertPriorityForJob = func(t *testing.T, wsjob *wsapisv1.WSJob, priority int) {
		require.Eventually(t, func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(wsjob), wsjob)
			if err != nil {
				t.Logf("Error getting job %s: %v", wsjob.Name, err)
				return false
			}
			return wsjob.GetPriority() == priority
		}, eventuallyTimeout, eventuallyTick)
	}

	assertPriorityForJobs = func(t *testing.T, priority int, wsjobs ...*wsapisv1.WSJob) {
		for _, j := range wsjobs {
			assertPriorityForJob(t, j, priority)
		}
	}

	assertPriorityForLock = func(t *testing.T, lock *rlv1.ResourceLock, priority int) {
		require.Eventually(t, func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(lock), lock)
			if err != nil {
				t.Logf("Error getting lock %s: %v", lock.NamespacedName(), err)
				return false
			}
			return lock.GetPriority() == priority
		}, eventuallyTimeout, eventuallyTick)
	}

	assertPriorityForLocks = func(t *testing.T, priority int, locks ...*rlv1.ResourceLock) {
		for _, l := range locks {
			assertPriorityForLock(t, l, priority)
		}
	}

	assertLockNotExists = func(t *testing.T, ns, name string) *rlv1.ResourceLock {
		return assertLockExistence(t, ns, name, false)
	}

	assertLockExists = func(t *testing.T, ns, name string) *rlv1.ResourceLock {
		return assertLockExistence(t, ns, name, true)
	}

	assertLockExistence = func(t *testing.T, ns, name string, exists bool) *rlv1.ResourceLock {
		var lock = &rlv1.ResourceLock{}
		require.Eventually(t, func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, lock)
			if err != nil {
				t.Logf("Error getting lock %s/%s: %v", ns, name, err)
				return !exists
			}
			return exists
		}, 2*eventuallyTimeout, eventuallyTick)
		return lock
	}

	cleanupLock = func(t *testing.T, ns, name string) error {
		var lock = &rlv1.ResourceLock{}
		err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, lock)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		return k8sClient.Delete(context.Background(), lock)
	}

	assertLockCountEventually = func(t *testing.T, count int) *rlv1.ResourceLockList {
		list := &rlv1.ResourceLockList{}
		require.Eventually(t, func() bool {
			err := k8sClient.List(context.Background(), list)
			if err != nil {
				t.Logf("Error listing locks: %v", err)
				return false
			}
			t.Logf("current count %d, locks: %+v", len(list.Items), list.Items)
			return len(list.Items) == count
		}, eventuallyTimeout, eventuallyTick)
		return list
	}

	getConditions = func(t *testing.T, ns, jobName string) map[commonv1.JobConditionType]commonv1.JobCondition {
		var job = &wsapisv1.WSJob{}
		require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jobName}, job))
		rv := map[commonv1.JobConditionType]commonv1.JobCondition{}
		for _, cond := range job.Status.Conditions {
			rv[cond.Type] = cond
		}
		return rv
	}

	assertPodCountEventually = func(t *testing.T, ns, jobName string, rt string, count int) *v1.PodList {
		var pods *v1.PodList
		require.Eventually(t, func() bool {
			pods = getJobPods(t, ns, jobName, rt)
			var podNames []string
			for _, p := range pods.Items {
				podNames = append(podNames, p.ObjectMeta.Name)
				t.Logf("pod %s status: %v", p.Name, p.Status.Phase)
			}
			return len(pods.Items) == count
		}, 3*eventuallyTimeout, eventuallyTick)
		return pods
	}

	assertEventEventually = func(t *testing.T, ns, jobName, reason string) {
		list := &corev1.EventList{}
		require.Eventually(t, func() bool {
			err := k8sClient.List(context.Background(), list, &client.ListOptions{Namespace: ns, Limit: int64(500)})
			if err != nil {
				t.Logf("Error listing events: %v", err)
				return false
			}

			for _, event := range list.Items {
				if event.InvolvedObject.Name == jobName && event.Reason == reason && event.Type == corev1.EventTypeWarning {
					return true
				}
			}
			return false
		}, eventuallyTimeout, eventuallyTick)
	}

	assertCoordinatorWsContainerCreated = func(t *testing.T, job *wsapisv1.WSJob) *v1.Container {
		pods := assertPodCountEventually(t, job.Namespace, job.Name, rlv1.CoordinatorRequestName, 1)
		crdContainers := pods.Items[0].Spec.Containers
		var container *corev1.Container
		for i := range crdContainers {
			if crdContainers[0].Name == wsapisv1.DefaultContainerName {
				container = &crdContainers[i]
			}
		}
		require.NotNil(t, container)
		return container
	}

	updateJobPriorityEventually = func(t *testing.T, wsjob *wsapisv1.WSJob, priority int) {
		require.Eventually(t, func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(wsjob), wsjob)
			if err != nil {
				return false
			}
			wsjob.SetPriority(priority)
			return k8sClient.Update(context.Background(), wsjob) == nil
		}, 60*time.Second, eventuallyTick)
	}

	cancelJobEventually = func(t *testing.T, wsjob *wsapisv1.WSJob, cancelWithStatus string) {
		setCanceled := false
		require.Eventually(t, func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(wsjob), wsjob)
			if err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
			if apierrors.IsNotFound(err) {
				return true
			}
			if !setCanceled {
				if wsjob.Annotations == nil {
					wsjob.Annotations = map[string]string{}
				}
				wsjob.Annotations[wsapisv1.CancelWithStatusKey] = cancelWithStatus
				setCanceled = k8sClient.Update(context.Background(), wsjob) == nil
				return false
			}

			return wsjob.Status.CompletionTime != nil
		}, 60*time.Second, eventuallyTick)
	}

	cleanupAllJobs = func(t *testing.T, jobs []*wsapisv1.WSJob) {
		wg := sync.WaitGroup{} // clean up in parallel for speed
		wg.Add(len(jobs))
		for i := range jobs {
			job := jobs[i]
			go func(j *wsapisv1.WSJob) {
				assertWsjobCleanedUp(t, j)
				wg.Done()
			}(job)
		}
		wg.Wait()
	}

	assertInferenceJobSharing = func(t *testing.T, lock *rlv1.ResourceLock, shouldEnable bool) {
		annot := lock.GetAnnotations()

		// If no annotations, treat as disabled
		enabledInLock := false
		if annot != nil {
			enabledInLock = annot[wsapisv1.InferenceJobSharingAnnotKey] == strconv.FormatBool(true)
		}

		assert.True(t, enabledInLock == shouldEnable)
	}

	assertWsJobStatus = func(status commonv1.JobConditionType) func(*testing.T, string, string) *wsapisv1.WSJob {
		assertFn := func(t *testing.T, ns, jobName string) *wsapisv1.WSJob {
			job := &wsapisv1.WSJob{}
			require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jobName}, job))
			conditions := job.Status.Conditions
			if len(conditions) == 0 {
				t.Fail()
				return job
			}
			t.Logf("%s job status %s, current time: %s", jobName, conditions[len(conditions)-1].Type, time.Now().UTC())
			require.Equal(t, status, conditions[len(conditions)-1].Type)
			return job
		}
		return assertFn
	}
	assertWsJobScheduled = assertWsJobStatus(commonv1.JobScheduled)

	assertWsJobStatusEventually = func(status commonv1.JobConditionType) func(*testing.T, string, string) *wsapisv1.WSJob {
		assertFn := func(t *testing.T, ns, jobName string) *wsapisv1.WSJob {
			job := &wsapisv1.WSJob{}
			require.Eventually(t, func() bool {
				err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jobName}, job)
				if err != nil {
					t.Logf("Error getting job %s: %v", jobName, err)
					return false
				}
				conditions := job.Status.Conditions
				if len(conditions) == 0 {
					return false
				}
				t.Logf("%s job status %s, current time: %s", jobName, conditions[len(conditions)-1].Type, time.Now().UTC())
				return conditions[len(conditions)-1].Type == status
			}, 90*time.Second, eventuallyTick)
			return job
		}
		return assertFn
	}

	assertWsJobQueuedEventually    = assertWsJobStatusEventually(commonv1.JobQueueing)
	assertWsJobScheduledEventually = assertWsJobStatusEventually(commonv1.JobScheduled)
	assertWsJobRunningEventually   = assertWsJobStatusEventually(commonv1.JobRunning)
	assertWsJobSuccessEventually   = assertWsJobStatusEventually(commonv1.JobSucceeded)
	assertWsJobFailEventually      = assertWsJobStatusEventually(commonv1.JobFailed)
	assertWsJobCancelledEventually = assertWsJobStatusEventually(commonv1.JobCancelled)

	assertWsJobAnnotatedEventually = func(key, value string) func(*testing.T, string, string) *wsapisv1.WSJob {
		assertFn := func(t *testing.T, ns, jobName string) *wsapisv1.WSJob {
			job := &wsapisv1.WSJob{}
			require.Eventually(t, func() bool {
				err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jobName}, job)
				if err != nil {
					t.Logf("Error getting job %s: %v", jobName, err)
					return false
				}
				if val, ok := job.Annotations[key]; !ok {
					return value == ""
				} else {
					return val == value
				}
			}, eventuallyTimeout, eventuallyTick)
			return job
		}
		return assertFn
	}

	assertWsJobAnnotatedNotCanceled = assertWsJobAnnotatedEventually(wsapisv1.CancelWithStatusKey, "")
	assertWsJobAnnotatedCanceled    = assertWsJobAnnotatedEventually(wsapisv1.CancelWithStatusKey, wsapisv1.CancelWithStatusCancelled)
	assertWsJobAnnotatedSucceeded   = assertWsJobAnnotatedEventually(wsapisv1.CancelWithStatusKey, wsapisv1.CancelWithStatusSucceeded)
	assertWsJobAnnotatedFailed      = assertWsJobAnnotatedEventually(wsapisv1.CancelWithStatusKey, wsapisv1.CancelWithStatusFailed)
	assertWsJobAnnotatedExpired     = assertWsJobAnnotatedEventually(wsapisv1.CancelWithStatusKey, wsapisv1.CancelWithStatusFailed)

	assertWsJobLabeledEventually = func(key, value string) func(*testing.T, string, string) *wsapisv1.WSJob {
		assertFn := func(t *testing.T, ns, jobName string) *wsapisv1.WSJob {
			job := &wsapisv1.WSJob{}
			require.Eventually(t, func() bool {
				err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: jobName}, job)
				if err != nil {
					t.Logf("Error getting job %s: %v", jobName, err)
					return false
				}
				if val, ok := job.Labels[key]; !ok {
					return value == ""
				} else {
					return val == value
				}
			}, eventuallyTimeout, eventuallyTick)
			return job
		}
		return assertFn
	}

	assertWsJobLabeledNotCanceled = assertWsJobAnnotatedEventually(wsapisv1.CancelReasonKey, "")
	assertWsJobLabeledCanceled    = assertWsJobAnnotatedEventually(wsapisv1.CancelReasonKey, wsapisv1.CanceledByClientReason)
	assertWsJobLabeledSucceeded   = assertWsJobAnnotatedEventually(wsapisv1.CancelReasonKey, wsapisv1.CanceledByClientReason)
	assertWsJobLabeledFailed      = assertWsJobAnnotatedEventually(wsapisv1.CancelReasonKey, wsapisv1.CanceledByClientReason)
	assertWsJobLabeledExpired     = assertWsJobAnnotatedEventually(wsapisv1.CancelReasonKey, wsapisv1.ExpiredLeaseCancelReason)

	assertServiceCountEventually = func(t *testing.T, ns, jobName string, rt string, count int) *v1.ServiceList {
		list := &v1.ServiceList{}
		labels := map[string]string{}
		labels["job-name"] = jobName
		if rt != "" {
			labels[wsReplicaTypeLabel] = strings.ToLower(rt)
		}
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: labels})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			err := k8sClient.List(context.Background(), list, client.MatchingLabelsSelector{Selector: selector},
				client.InNamespace(ns))
			if err != nil {
				t.Logf("Error listing services: %v", err)
				return false
			}
			t.Logf("current count %d, services: %+v", len(list.Items), list.Items)
			return len(list.Items) == count
		}, eventuallyTimeout, eventuallyTick)
		return list
	}

	assertEndpointCountEventually = func(t *testing.T, ns, jobName string, rt string, count int) *v1.EndpointsList {
		list := &v1.EndpointsList{}
		labels := map[string]string{}
		labels["job-name"] = jobName
		if rt != "" {
			labels[wsReplicaTypeLabel] = strings.ToLower(rt)
		}
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: labels})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			err := k8sClient.List(context.Background(), list, client.MatchingLabelsSelector{Selector: selector},
				client.InNamespace(ns))
			if err != nil {
				t.Logf("Error listing endpoints: %v", err)
				return false
			}
			t.Logf("current count %d, endpoints: %+v", len(list.Items), list.Items)
			return len(list.Items) == count
		}, eventuallyTimeout, eventuallyTick)
		return list
	}

	assertIngressCountEventually = func(t *testing.T, ns, jobName string, count int) *networkingv1.IngressList {
		list := &networkingv1.IngressList{}
		labels := map[string]string{}
		labels["job-name"] = jobName
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: labels})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			err := k8sClient.List(context.Background(), list, client.MatchingLabelsSelector{Selector: selector},
				client.InNamespace(ns))
			if err != nil {
				t.Logf("Error listing ingresses: %v", err)
				return false
			}
			t.Logf("current count %d, ingresses: %+v", len(list.Items), list.Items)
			return len(list.Items) == count
		}, eventuallyTimeout, eventuallyTick)
		return list
	}

	assertBrNICLockCountEventually = func(t *testing.T, jobName string, count int) *corev1.ConfigMapList {
		list := &corev1.ConfigMapList{}
		selector, _ := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				"job-name":             jobName,
				wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeNICLock,
			},
		})
		require.Eventually(t, func() bool {
			err := k8sClient.List(context.Background(), list, client.MatchingLabelsSelector{Selector: selector},
				client.InNamespace(wscommon.SystemNamespace))
			if err != nil {
				t.Logf("Error listing BrNICLocks: %v", err)
				return false
			}
			t.Logf("current count %d, BrNICLocks: %+v", len(list.Items), list.Items)
			return len(list.Items) == count
		}, eventuallyTimeout, eventuallyTick)
		return list
	}

	requireClusterDetailsCreatedEventually = func(t *testing.T, jobName string, jobNamespace string) *corev1.ConfigMap {
		clusterDetailsConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, jobName),
				Namespace: jobNamespace,
			},
		}

		require.Eventually(t, func() bool {
			err := k8sClient.Get(
				context.Background(),
				client.ObjectKeyFromObject(clusterDetailsConfigMap),
				clusterDetailsConfigMap)
			return err == nil
		}, eventuallyTimeout, eventuallyTick)
		return clusterDetailsConfigMap
	}

	assertWsjobCleanedUp = func(t *testing.T, job *wsapisv1.WSJob) {
		cancelJobEventually(t, job, wsapisv1.CancelWithStatusCancelled)
		assertLockNotExists(t, job.Namespace, job.Name)
		err := k8sClient.Delete(context.Background(), job)
		require.True(t, err == nil || errors.IsNotFound(err))
		// delete all pods individually since envtest doesn't support DeleteAllOf
		pods := &v1.PodList{}
		require.NoError(t, k8sClient.List(context.Background(), pods, client.MatchingLabels{
			commonv1.JobNameLabel: job.Name,
		}))
		for _, pod := range pods.Items {
			err = k8sClient.Delete(context.Background(), &pod)
			require.True(t, err == nil || errors.IsNotFound(err))
		}
		require.Eventually(t, func() bool {
			err := k8sClient.List(context.Background(), pods, client.MatchingLabels{
				commonv1.JobNameLabel: job.Name,
			})
			if err != nil {
				t.Logf("Error listing pods: %v", err)
				return false
			}
			return len(pods.Items) == 0
		}, eventuallyTimeout, eventuallyTick)
	}

	assertWsjobLeaseEventually = func(t *testing.T, ns, jobName string, leaseExists bool, canceledWithReason string) *coordv1.Lease {
		labels := map[string]string{
			"job-name": jobName,
		}
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: labels})
		require.NoError(t, err)
		list := &coordv1.LeaseList{}
		require.Eventually(t, func() bool {
			err := k8sClient.List(context.Background(), list, client.MatchingLabelsSelector{Selector: selector},
				client.InNamespace(ns))
			if err != nil {
				t.Logf("Error listing leases: %v", err)
				return false
			}
			t.Logf("%d lease found with label %v", len(list.Items), labels)
			if leaseExists {
				return len(list.Items) == 1
			} else {
				return len(list.Items) == 0
			}
		}, 10*time.Second, eventuallyTick)

		if leaseExists {
			lease := &list.Items[0]
			t.Logf("lease: %v", lease)
			_, exists := lease.Annotations[wsapisv1.CancelReasonKey]
			assert.Equal(t, canceledWithReason != "", exists)
			return lease
		}
		return nil
	}

	updateClusterConfig = func(t *testing.T, cfgMgr *wscommon.ClusterConfigMgr, schema wscommon.ClusterSchema) {
		cmYaml, err := yaml.Marshal(schema)
		require.NoError(t, err)
		cfgMgr.Cfg.ResourceVersion = "non-null"
		cm := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name:      wscommon.DefaultClusterConfigMapName,
			Namespace: wscommon.SystemNamespace}}
		cm.Data = map[string]string{wscommon.ClusterConfigFile: string(cmYaml)}
		cm.Labels = map[string]string{
			"test": uuid.New().String(),
		}
		if k8sClient.Create(context.TODO(), cm) != nil {
			require.NoError(t, k8sClient.Update(context.TODO(), cm))
		}
		require.Eventually(t, func() bool {
			return cfgMgr.Cfg.ResourceVersion != "non-null"
		}, eventuallyTimeout, eventuallyTick)
	}

	assertReplicaTypeFailure = func(replicaType commonv1.ReplicaType) func(*testing.T, *wsapisv1.WSJob) {
		return func(t *testing.T, job *wsapisv1.WSJob) {
			job = assertWsJobFailEventually(t, job.Namespace, job.Name)
			conditions := job.Status.Conditions
			assert.True(t, len(conditions) > 0)
			for rType, replicaStatus := range job.Status.ReplicaStatuses {
				if rType == replicaType {
					assert.Equal(t, 1, len(replicaStatus.FailedPodStatuses))
					assert.Equal(t, fmt.Sprintf("%s-%s-0", job.Name, strings.ToLower(string(replicaType))), replicaStatus.FailedPodStatuses[0].Name)
				} else {
					assert.Equal(t, 0, len(replicaStatus.FailedPodStatuses))
				}
			}
		}
	}

	assertCoordinatorPodFailure = assertReplicaTypeFailure(wsapisv1.WSReplicaTypeCoordinator)
	assertWorkerPodFailre       = assertReplicaTypeFailure(wsapisv1.WSReplicaTypeWorker)

	getClusterServerIngress = func(namespace string) *networkingv1.Ingress {
		return &networkingv1.Ingress{
			ObjectMeta: ctrl.ObjectMeta{
				Name:      wsapisv1.ClusterServerIngressName,
				Namespace: namespace,
			},
			Spec: networkingv1.IngressSpec{
				Rules: []networkingv1.IngressRule{
					{
						Host: "wsjob",
					},
				},
				TLS: []networkingv1.IngressTLS{
					{
						SecretName: "grpc-secret",
					},
				},
			},
		}
	}

	assertClusterServerIngressSetup = func(t *testing.T, namespace string) {
		err := k8sClient.Create(context.Background(), getClusterServerIngress(namespace))
		if err != nil && apierrors.IsAlreadyExists(err) {
			err = nil
		}
		require.NoError(t, err)
	}

	assertClusterServerIngressCleanup = func(t *testing.T, namespace string) {
		err := k8sClient.Delete(context.Background(), getClusterServerIngress(namespace))
		if err != nil && apierrors.IsNotFound(err) {
			err = nil
		}
		require.NoError(t, err)
	}

	// assertNSRReady waits for a NamespaceReservation to be successfully assigned with resources
	assertNSRReady = func(t *testing.T, ctx context.Context, nsName string, expectedNodegroups int) {
		require.Eventually(t, func() bool {
			nsr := &namespacev1.NamespaceReservation{}
			err := k8sClient.Get(ctx, client.ObjectKey{Name: nsName}, nsr)
			if err != nil {
				t.Logf("Error getting NSR %s: %v", nsName, err)
				return false
			}

			return nsr.Status.GetLastAttemptedRequest().Succeeded &&
				nsr.Status.HasResources() &&
				len(nsr.Status.Nodegroups) == expectedNodegroups &&
				len(nsr.Status.Systems) > 0
		}, eventuallyTimeout, eventuallyTick)
	}
)

func getJobPods(t *testing.T, ns, name, replicaType string) *v1.PodList {
	labels := map[string]string{}
	if name != "" {
		labels["job-name"] = name
	}
	if replicaType != "" {
		labels[wsReplicaTypeLabel] = strings.ToLower(replicaType)
	}
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: labels})
	require.NoError(t, err)

	pods := &v1.PodList{}
	require.NoError(t, k8sClient.List(context.Background(), pods, client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(ns)))
	return pods
}

func setPodsInit(t *testing.T, nodeMap map[string]*wscommon.Node, pods *corev1.PodList) {
	for i := range pods.Items {
		po := &pods.Items[i]
		no := po.Spec.NodeSelector[corev1.LabelHostname]
		po.Status = corev1.PodStatus{
			Phase:  corev1.PodPending,
			HostIP: fmt.Sprintf(nodeMap[no].HostIP),
			PodIP:  fmt.Sprintf("127.0.%d.%d", i/256, i%256),
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{
							StartedAt: metav1.Now(),
						},
					},
				},
			},
			StartTime: &metav1.Time{
				Time: time.Now(),
			},
		}
		require.NoError(t, k8sClient.Status().Update(context.TODO(), po))
	}
}

func deleteNodes(t *testing.T) {
	nodes := &corev1.NodeList{}
	require.NoError(t, k8sClient.List(context.Background(), nodes))
	for _, node := range nodes.Items {
		require.NoError(t, k8sClient.Delete(context.Background(), &node))
	}
}

func patchNodeCondition(t *TestSuite, nodeName string, conditions []corev1.NodeCondition) {
	currentNode := &corev1.Node{}
	require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKey{Name: nodeName}, currentNode))

	patchedNode := currentNode.DeepCopy()
	patchedNode.Status.Conditions = conditions
	require.NoError(t.T(), k8sClient.Status().Patch(context.Background(), patchedNode, client.MergeFrom(currentNode)))
}

func patchNode(t *TestSuite, nodeName string, conditions []corev1.NodeCondition) error {
	patchNodeCondition(t, nodeName, conditions)
	require.Eventually(t.T(), func() bool {
		_, foundUnhealthyNode := t.ResourceController.Cfg.UnhealthyNodes[nodeName]
		return foundUnhealthyNode == (len(conditions) > 0)
	}, eventuallyTimeout, eventuallyTick)
	return nil
}

type mockAlertCache struct {
	Cache map[string]models.GettableAlerts
}

func (m *mockAlertCache) Get(names ...string) (models.GettableAlerts, metav1.Time, error) {
	res := models.GettableAlerts{}
	for _, n := range names {
		res = append(res, m.Cache[n]...)
	}
	return res, metav1.NewTime(time.Now().UTC().Truncate(time.Second)), nil
}

type mockMetricCache struct {
	Cache map[string]string
}

func (m *mockMetricCache) GetSystemVersion(name string) string {
	return m.Cache[name]
}

func countNonBrPods(job *wsapisv1.WSJob) int {
	sum := 0
	for rt, r := range job.Spec.WSReplicaSpecs {
		if rt == wsapisv1.WSReplicaTypeBroadcastReduce || r.Replicas == nil {
			continue
		}
		sum += int(*r.Replicas)
	}
	return sum
}

func clearResourceLimits(spec *commonv1.ReplicaSpec) {
	for ci := range spec.Template.Spec.Containers {
		spec.Template.Spec.Containers[ci].Resources = corev1.ResourceRequirements{}
	}
}

func newWsJobTemplate(ns, name string, numSystems uint32) *wsapisv1.WSJob {
	return &wsapisv1.WSJob{
		ObjectMeta: ctrl.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"job-name":               name,
				wsapisv1.JobModeLabelKey: wsapisv1.JobModeWeightStreaming,
			},
			Annotations: map[string]string{
				wscommon.EnableStartupProbeAllKey:        "true",
				wsapisv1.DisableNonCRDServicesAnnotation: "true",
				wsapisv1.SemanticClusterServerVersion:    "1.0.2",
			},
		},
		Spec: wsapisv1.WSJobSpec{
			RunPolicy:     commonv1.RunPolicy{},
			SuccessPolicy: nil,
			WSReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
				wsapisv1.WSReplicaTypeCoordinator: wsjobContainerSpec(ns, wsapisv1.WSReplicaTypeCoordinator, 1),
			},
			NumWafers: numSystems,
		},
	}
}

func newWsCompileJob(ns, name string, numSystems uint32) *wsapisv1.WSJob {
	job := newWsJobTemplate(ns, name, numSystems)
	job.Labels[wsapisv1.JobTypeLabelKey] = wsapisv1.JobTypeCompile

	return job
}

func newWsExecuteJob(ns, name string, numSystems uint32) *wsapisv1.WSJob {
	job := newWsJobTemplate(ns, name, numSystems)
	job.Labels[wsapisv1.JobTypeLabelKey] = wsapisv1.JobTypeExecute
	job.Annotations[wsapisv1.EnablePartialSystemKey] = "false"

	brCount := 0
	if numSystems > 1 {
		brCount = 1
	}

	actCount := defaultActPerSys * int(numSystems)
	if wsapisv1.UseAxScheduling && !useConfigMapForAct {
		actCount = len(defaultActToCsxDomains) * int(numSystems)
		job.Spec.ActToCsxDomains = defaultActToCsxDomains
	}

	replicaTypeCount := map[commonv1.ReplicaType]int{
		wsapisv1.WSReplicaTypeCommand:         1,
		wsapisv1.WSReplicaTypeWeight:          defaultWgtPerJob,
		wsapisv1.WSReplicaTypeWorker:          defaultWrkPerSys * int(numSystems),
		wsapisv1.WSReplicaTypeChief:           int(numSystems),
		wsapisv1.WSReplicaTypeActivation:      actCount,
		wsapisv1.WSReplicaTypeBroadcastReduce: brCount,
	}

	for rt, count := range replicaTypeCount {
		if count == 0 {
			continue
		}
		job.Spec.WSReplicaSpecs[rt] = wsjobContainerSpec(ns, rt, numSystems)
		job.Spec.WSReplicaSpecs[rt].Replicas = int32Ptr(count)
	}

	return job
}

func newInfExecuteJob(ns, name string, numSystems, kvssReplicaCount, swdriverReplicaCount uint32) *wsapisv1.WSJob {
	job := newWsExecuteJob(ns, name, numSystems)
	job.Labels[wsapisv1.JobModeLabelKey] = wsapisv1.JobModeInference
	delete(job.Spec.WSReplicaSpecs, wsapisv1.WSReplicaTypeCommand)
	delete(job.Spec.WSReplicaSpecs, wsapisv1.WSReplicaTypeWeight)
	delete(job.Spec.WSReplicaSpecs, wsapisv1.WSReplicaTypeWorker)
	delete(job.Spec.WSReplicaSpecs, wsapisv1.WSReplicaTypeBroadcastReduce)
	if kvssReplicaCount > 0 {
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeKVStorageServer] = wsjobContainerSpec(ns, wsapisv1.WSReplicaTypeKVStorageServer, numSystems)
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeKVStorageServer].Replicas = int32Ptr(int(kvssReplicaCount))
	}
	if swdriverReplicaCount > 0 {
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeSWDriver] = wsjobContainerSpec(ns, wsapisv1.WSReplicaTypeSWDriver, numSystems)
		job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeSWDriver].Replicas = int32Ptr(int(swdriverReplicaCount))
	}
	return job
}

func newWsExecuteJobWithPartialMode(ns, name string, numSystems uint32) *wsapisv1.WSJob {
	job := newWsExecuteJob(ns, name, numSystems)
	job.Annotations[wsapisv1.EnablePartialSystemKey] = "true"
	return job
}

func newSdkExecuteJob(name string) *wsapisv1.WSJob {
	sdkJob := &wsapisv1.WSJob{
		ObjectMeta: ctrl.ObjectMeta{
			Name:      name,
			Namespace: wscommon.Namespace,
			Labels: map[string]string{
				"job-name":               name,
				wsapisv1.JobTypeLabelKey: wsapisv1.JobTypeExecute,
				wsapisv1.JobModeLabelKey: wsapisv1.JobModeSdk,
			},
			Annotations: map[string]string{
				wscommon.EnableStartupProbeAllKey:        "true",
				wsapisv1.DisableNonCRDServicesAnnotation: "true",
				wsapisv1.SemanticClusterServerVersion:    "1.0.2",
			},
		},
		Spec: wsapisv1.WSJobSpec{
			RunPolicy:     commonv1.RunPolicy{},
			SuccessPolicy: nil,
			WSReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
				wsapisv1.WSReplicaTypeWorker: wsjobContainerSpec(wscommon.Namespace, wsapisv1.WSReplicaTypeWorker, 1),
			},
			// we only support 1 CSX for SDK job
			NumWafers: 1,
		},
	}
	return sdkJob
}

func wsjobWithAnnotations(job *wsapisv1.WSJob, annotations ...string) *wsapisv1.WSJob {
	for k, v := range wscommon.MustSplitProps(annotations) {
		job.Annotations[k] = v
	}
	return job
}

func wsjobContainerSpec(ns string, replicaType commonv1.ReplicaType, numSystems uint32) *commonv1.ReplicaSpec {
	replicas := int32Ptr(1)
	if replicaType == wsapisv1.WSReplicaTypeActivation || replicaType == wsapisv1.WSReplicaTypeChief ||
		replicaType == wsapisv1.WSReplicaTypeWorker || replicaType == wsapisv1.WSReplicaTypeKVStorageServer {
		replicas = int32Ptr(int(numSystems))
	}
	var rsReq v1.ResourceList
	var rsLim v1.ResourceList
	if replicaType == wsapisv1.WSReplicaTypeCoordinator {
		rsLim = v1.ResourceList{
			v1.ResourceMemory: defaultCrdMem,
		}
		rsReq = v1.ResourceList{
			v1.ResourceCPU: defaultCrdCpu,
		}
	} else if replicaType == wsapisv1.WSReplicaTypeWeight {
		rsReq = v1.ResourceList{
			v1.ResourceCPU:    defaultWgtCpu,
			v1.ResourceMemory: defaultWgtMem,
		}
	} else if replicaType == wsapisv1.WSReplicaTypeActivation {
		rsReq = v1.ResourceList{
			v1.ResourceCPU:    defaultActCpu,
			v1.ResourceMemory: defaultActMem,
		}
	} else if replicaType == wsapisv1.WSReplicaTypeCommand {
		rsReq = v1.ResourceList{
			v1.ResourceCPU:    defaultCmdCpu,
			v1.ResourceMemory: defaultCmdMem,
		}
	} else if replicaType == wsapisv1.WSReplicaTypeKVStorageServer {
		rsReq = v1.ResourceList{
			v1.ResourceCPU:    defaultKvssCpu,
			v1.ResourceMemory: defaultKvssMem,
		}
	} else if replicaType == wsapisv1.WSReplicaTypeSWDriver {
		rsReq = v1.ResourceList{
			v1.ResourceCPU:    defaultSwdriverCpu,
			v1.ResourceMemory: defaultSwdriverMem,
		}
	}
	var containers []v1.Container
	makeContainers := func(names ...string) {
		for _, name := range names {
			containers = append(containers, v1.Container{
				Image:           wscommon.DefaultImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Name:            name,
				// sleep time matters for e2e tests: on container exit, wsjob cleansup
				// a cleaner solution for e2e would involve an exit signal sent by the test for the pods to exit
				Command:   []string{"sleep", "30"},
				Resources: v1.ResourceRequirements{Limits: rsLim, Requests: rsReq},
			})
		}
	}
	if replicaType != wsapisv1.WSReplicaTypeWorker {
		makeContainers(wsapisv1.DefaultContainerName)
	} else {
		makeContainers(wsapisv1.DefaultContainerName, wsapisv1.UserSidecarContainerName)
	}
	return &commonv1.ReplicaSpec{
		Replicas: replicas,
		Template: v1.PodTemplateSpec{
			ObjectMeta: ctrl.ObjectMeta{Name: string(replicaType), Namespace: ns},
			Spec: v1.PodSpec{
				Containers: containers,
			},
		},
		RestartPolicy: commonv1.RestartPolicyNever,
	}
}

func newLeaseBuilder(name, workflow string, durationSeconds int32) *coordv1.Lease {
	now := &metav1.MicroTime{Time: time.Now()}
	boolPtr := func(b bool) *bool { return &b }
	labels := map[string]string{}
	if name == workflow {
		labels[wsapisv1.WorkflowIdLabelKey] = workflow
	} else {
		labels[commonv1.JobNameLabel] = name
	}
	return &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: wscommon.Namespace,
			Name:      name,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         wsapisv1.ApiVersion,
					Kind:               wsapisv1.Kind,
					Name:               name,
					UID:                "xyz",
					BlockOwnerDeletion: boolPtr(true),
					Controller:         boolPtr(true),
				},
			},
			Labels: map[string]string{
				commonv1.JobNameLabel:       name,
				wsapisv1.WorkflowIdLabelKey: workflow,
			},
		},
		Spec: coordv1.LeaseSpec{
			AcquireTime:          now,
			RenewTime:            now,
			LeaseDurationSeconds: &durationSeconds,
		},
	}
}

func assertLease(shouldCancel, shouldExpire, shouldExist bool) func(*testing.T, string) *coordv1.Lease {
	ctx := context.TODO()
	assertFn := func(t *testing.T, name string) *coordv1.Lease {
		lease := &coordv1.Lease{}
		t.Logf("Lease %s should cancel: %t, should expire: %t", name, shouldCancel, shouldExpire)
		require.Eventually(t, func() bool {
			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: wscommon.Namespace, Name: name}, lease)
			if !shouldExist {
				return apierrors.IsNotFound(err)
			}
			if err != nil {
				t.Logf("Error getting lease %s: %v", name, err)
				return false
			}

			leaseDuration := time.Second * time.Duration(*lease.Spec.LeaseDurationSeconds)
			leaseExpirationTime := lease.Spec.RenewTime.Time.Add(leaseDuration)
			t.Logf("Lease %s expired: %t, annotation: %v", name, leaseExpirationTime.Before(time.Now()), lease.Annotations)
			if shouldCancel {
				return true
			} else if shouldExpire {
				return leaseExpirationTime.Before(time.Now())
			} else {
				return !leaseExpirationTime.Before(time.Now())
			}
		}, leaseEventuallyTimeout, leaseEventuallyTick)
		return lease
	}
	return assertFn
}

var assertValidLease = assertLease(false, false, true)
var assertExpiredLease = assertLease(false, true, true)
var assertLeaseDeleted = assertLease(false, false, false)

func int32Ptr(i int) *int32 {
	cast := int32(i)
	return &cast
}

var (
	// defaults are set to reflect target product scenario: N training jobs running against N systems
	// will have enough headroom for N compiles to run concurrently
	targetCompiles = int64(systemCount)
	targetJobs     = systemCount + targetCompiles
	defaultCrdCpu  = *apiresource.NewMilliQuantity(wscommon.DefaultNodeCpu/targetJobs, apiresource.DecimalSI)
	defaultCrdMem  = *apiresource.NewQuantity((wscommon.DefaultNodeMem/targetJobs)<<20, apiresource.BinarySI)

	defaultWgtPerJob = 4
	defaultWgtCpu    = *apiresource.NewMilliQuantity(wscommon.DefaultNodeCpu/2, apiresource.DecimalSI)
	defaultWgtMem    = *apiresource.NewQuantity((wscommon.DefaultNodeMem/2)<<20, apiresource.BinarySI)

	defaultActPerSys       = 1
	defaultActToCsxDomains = [][]int{{0}, {1}, {2}, {3}}
	defaultActCpu          = *apiresource.NewMilliQuantity(wscommon.DefaultNodeCpu/8, apiresource.DecimalSI)
	defaultActMem          = *apiresource.NewQuantity((wscommon.DefaultNodeMem/8)<<20, apiresource.BinarySI)

	defaultKvssCpu = *apiresource.NewMilliQuantity(wscommon.DefaultNodeCpu/16, apiresource.DecimalSI)
	defaultKvssMem = *apiresource.NewQuantity((wscommon.DefaultNodeMem/16)<<20, apiresource.BinarySI)

	defaultSwdriverCpu = *apiresource.NewMilliQuantity(wscommon.DefaultNodeCpu/16, apiresource.DecimalSI)
	defaultSwdriverMem = *apiresource.NewQuantity((wscommon.DefaultNodeMem/16)<<20, apiresource.BinarySI)

	defaultCmdCpu = *apiresource.NewMilliQuantity(wscommon.DefaultNodeCpu/8, apiresource.DecimalSI)
	defaultCmdMem = *apiresource.NewQuantity((wscommon.DefaultNodeMem/8)<<20, apiresource.BinarySI)

	actOnlyWgtCpu = *apiresource.NewMilliQuantity(wscommon.DefaultNodeCpu/8, apiresource.DecimalSI)
	actOnlyWgtMem = *apiresource.NewQuantity((wscommon.DefaultNodeMem/8)<<20, apiresource.BinarySI)

	actOnlySmallWgtCpu = *apiresource.NewMilliQuantity(wscommon.DefaultNodeCpu/16, apiresource.DecimalSI)
	actOnlySmallWgtMem = *apiresource.NewQuantity((wscommon.DefaultNodeMem/16)<<20, apiresource.BinarySI)

	defaultWrkPerSys = 2

	useConfigMapForAct = false
)

// Generic function to create cluster details config map with optional domain extraction
func createClusterDetailsConfigMap(t *testing.T, execJob *wsapisv1.WSJob, testDataFileName string, domainExtractor func(*testing.T, *corev1.ConfigMap, string)) *corev1.ConfigMap {
	clusterDetailsConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, execJob.Name),
			Namespace: execJob.Namespace,
		},
	}

	// expect cluster details config not to exist in the first few seconds
	require.Never(t, func() bool {
		err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
		return !apierrors.IsNotFound(err)
	}, 3*time.Second, eventuallyTick)

	// simulating how server would generate the configmap
	require.Eventually(t, func() bool {
		err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(execJob), execJob)
		return err == nil
	}, eventuallyTimeout, eventuallyTick)

	// simulate cluster server creating the config
	clusterDetailsData, err := ioutil.ReadFile(testDataFileName)
	require.NoError(t, err)

	clusterDetailsConfigMap = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, execJob.Name),
			Namespace: execJob.Namespace,
			Labels: map[string]string{
				"job-name":             execJob.Name,
				wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeClusterDetails,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "jobs.cerebras.com/v1",
				BlockOwnerDeletion: wscommon.Pointer(true),
				Controller:         wscommon.Pointer(true),
				Kind:               "WSJob",
				Name:               execJob.Name,
				UID:                execJob.ObjectMeta.UID,
			}},
		},
		Data: map[string]string{
			wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.InitSignalVal,
		},
	}

	compressed, err := wscommon.Gzip(clusterDetailsData)
	require.NoError(t, err)

	clusterDetailsConfigMap.BinaryData = wscommon.EnsureMap(clusterDetailsConfigMap.BinaryData)
	clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName] = compressed

	// Apply domain-specific extraction if provided
	if domainExtractor != nil {
		domainExtractor(t, clusterDetailsConfigMap, testDataFileName)
	}

	return clusterDetailsConfigMap
}

// Domain extractors for different replica types
func addSwDriverDomainsToConfigMap(t *testing.T, configMap *corev1.ConfigMap, testDataFileName string) {
	cdConfig := &commonpb.ClusterDetails{}
	options := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}

	clusterDetailsData, err := ioutil.ReadFile(testDataFileName)
	require.NoError(t, err)

	err = options.Unmarshal(clusterDetailsData, cdConfig)
	require.NoError(t, err)

	var swDriverToCsxDomains []string
	for _, taskInfo := range cdConfig.Tasks {
		if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_SWD {
			for _, taskMap := range taskInfo.TaskMap {
				swDriverToCsxDomains = append(swDriverToCsxDomains, wscommon.SerializeWioTaskMap(taskMap))
			}
		}
	}

	compressed, err := wscommon.Gzip([]byte(strings.Join(swDriverToCsxDomains, ",")))
	require.NoError(t, err)
	configMap.BinaryData[wsapisv1.ClusterDetailsSWDriverToCsxDomains] = compressed
}

func addActKvssDomainsToConfigMap(t *testing.T, configMap *corev1.ConfigMap, testDataFileName string) {
	cdConfig := &commonpb.ClusterDetails{}
	options := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}

	clusterDetailsData, err := ioutil.ReadFile(testDataFileName)
	require.NoError(t, err)

	err = options.Unmarshal(clusterDetailsData, cdConfig)
	require.NoError(t, err)

	var kvssToCsxDomains []string
	for _, taskInfo := range cdConfig.Tasks {
		if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_KVSS {
			for _, taskMap := range taskInfo.TaskMap {
				kvssToCsxDomains = append(kvssToCsxDomains, wscommon.SerializeWioTaskMap(taskMap))
			}
		}
	}

	if len(kvssToCsxDomains) > 0 {
		compressed, err := wscommon.Gzip([]byte(strings.Join(kvssToCsxDomains, ",")))
		require.NoError(t, err)
		configMap.BinaryData[wsapisv1.ClusterDetailsKvssToCsxDomains] = compressed
	}

	var actToCsxDomains []string
	for _, taskInfo := range cdConfig.Tasks {
		if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_ACT {
			for _, taskMap := range taskInfo.TaskMap {
				actToCsxDomains = append(actToCsxDomains, wscommon.SerializeWioTaskMap(taskMap))
			}
		}
	}

	if len(actToCsxDomains) > 0 {
		compressed, err := wscommon.Gzip([]byte(strings.Join(actToCsxDomains, ",")))
		require.NoError(t, err)
		configMap.BinaryData[wsapisv1.ClusterDetailsActToCsxDomains] = compressed
	}
}

// Convenience wrapper functions that handle creation and K8s operations
func createSwDriverConfigMap(t *testing.T, execJob *wsapisv1.WSJob, testDataFileName string) {
	configMap := createClusterDetailsConfigMap(t, execJob, testDataFileName, addSwDriverDomainsToConfigMap)

	require.NoError(t, k8sClient.Create(context.Background(), configMap))

	// cluster details config generated
	require.Eventually(t, func() bool {
		err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(configMap), configMap)
		return err == nil
	}, eventuallyTimeout, eventuallyTick)
}

func createActKvssConfigMap(t *testing.T, execJob *wsapisv1.WSJob, testDataFileName string) {
	configMap := createClusterDetailsConfigMap(t, execJob, testDataFileName, addActKvssDomainsToConfigMap)

	require.NoError(t, k8sClient.Create(context.Background(), configMap))

	// cluster details config generated
	require.Eventually(t, func() bool {
		err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(configMap), configMap)
		return err == nil
	}, eventuallyTimeout, eventuallyTick)
}

// For basic config maps without domain extraction
func createBasicClusterDetailsConfigMap(t *testing.T, execJob *wsapisv1.WSJob, testDataFileName string) {
	configMap := createClusterDetailsConfigMap(t, execJob, testDataFileName, nil)

	require.NoError(t, k8sClient.Create(context.Background(), configMap))

	// cluster details config generated
	require.Eventually(t, func() bool {
		err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(configMap), configMap)
		return err == nil
	}, eventuallyTimeout, eventuallyTick)
}

func assertNodesScheduled(t *TestSuite, expectedNodes map[string]bool, lock *rlv1.ResourceLock, requestName string) {
	actualNodes := map[string]bool{}
	for _, gg := range lock.Status.GroupResourceGrants {
		if !strings.HasPrefix(gg.Name, requestName) {
			continue
		}
		for _, rg := range gg.ResourceGrants {
			for _, res := range rg.Resources {
				actualNodes[res.Name] = true
			}
		}
	}
	assert.Equal(t.T(), expectedNodes, actualNodes)
}

func assertSystemsOrdered(t *TestSuite, expectedSystemsOrdered []string, lock *rlv1.ResourceLock) {
	var actualSystems []string
	for _, rg := range lock.Status.ResourceGrants {
		if rg.Name != rlv1.SystemsRequestName {
			continue
		}
		for _, res := range rg.Resources {
			actualSystems = append(actualSystems, res.Name)
		}
	}
	assert.Equal(t.T(), expectedSystemsOrdered, actualSystems)
}

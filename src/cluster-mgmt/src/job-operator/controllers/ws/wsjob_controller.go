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
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/lru"
	"sigs.k8s.io/controller-runtime/pkg/event"

	commonapisv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonctrlv1 "github.com/kubeflow/common/pkg/controller.v1/common"
	"github.com/kubeflow/common/pkg/controller.v1/control"
	commonutil "github.com/kubeflow/common/pkg/util"
	utillabels "github.com/kubeflow/common/pkg/util/labels"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	res "cerebras.com/job-operator/common/resource"

	"sigs.k8s.io/controller-runtime/pkg/client"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

type WSJobController struct {
	commonctrlv1.JobController
	client.Client
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	ApiReader client.Reader
	Log       logr.Logger

	Cfg            *wscommon.ClusterConfig
	cfgInitialized bool
	// JobResourceCache is used to Cache locked job resources as an optimization.
	JobResourceCache *lru.Cache
	// WorkflowCache is used to cache workflow states as an optimization.
	// At a high level, WSJobReconciler will decide on the desired workflow priority and write to this cache.
	// WorkflowReconciler on the other hand will always respect the cache value and bring all jobs and locks
	// in the same workflow to that desired priority value.
	WorkflowCache *lru.Cache
	// For prom metrics query
	MetricsQuerier wscommon.MetricsQuerier

	// channel/cfg to detect resource update
	CfgChan chan wscommon.ConfigUpdateEvent
	NewCfg  *wscommon.ClusterConfig
	sync.Mutex

	WorkflowEventChan chan event.GenericEvent
}

// fail job if not expected to recover by retrying on next reconcile
type CriticalError struct {
	message string
}

func CriticalErrorf(f string, args ...any) *CriticalError {
	return &CriticalError{message: fmt.Sprintf(f, args...)}
}

func (error *CriticalError) Error() string {
	return error.message
}

const (
	// wsjobSucceededReason is added in a wsjob when it is succeeded.
	wsjobSucceededReason = "WSJobSucceeded"
	// wsjobRunningReason is added in a wsjob when it is running.
	wsjobRunningReason = "WSJobRunning"
	// wsjobFailedReason is added in a wsjob when it is failed.
	wsjobFailedReason = "WSJobFailed"
	// wsjobRestarting is added in a wsjob when it is restarting.
	wsjobRestartingReason = "WSJobRestarting"
	// wsjobCancelledReason wsjobCancelled is added in a wsjob when it is cancelled.
	wsjobCancelledReason = "WSJobCancelled"
	// wsjobQueueingReason is added in a wsjob when it is queueing.
	wsjobQueueingReason = "WSJobQueueing"
	// wsjobScheduledReason is added in a wsjob when it is scheduled.
	wsjobScheduledReason = "WSJobScheduled"
	// wsjobScheduleFailedReason is added in a wsjob when failed to schedule.
	wsjobScheduleFailedReason = "WSJobScheduleFailed"
	// wsjobInitializingReason is added in a wsjob when it is initializing.
	wsjobInitializingReason = "WSJobInitializing"
	// wsjobFailingReason is added in a wsjob when it has pod failures but is not yet failed.
	wsjobFailingReason = "WSJobFailing"

	failedDeleteJobReason     = "FailedDeleteJob"
	successfulDeleteJobReason = "SuccessfulDeleteJob"

	wsReplicaTypeLabel  = "replica-type"
	wsReplicaIndexLabel = "replica-index"

	RoleLabelKeyF = "k8s.cerebras.com/node-role-%s"

	SystemType = "SYSTEM_TYPE"
	SYSEMU     = "SYSEMU"
	KAPI       = "KAPI"
	CSX        = "CSX"

	podTemplateRestartPolicyReason   = "SettedPodTemplateRestartPolicy"
	podTemplateSchedulerNameReason   = "SettedPodTemplateSchedulerName"
	gangSchedulingPodGroupAnnotation = "scheduling.k8s.io/group-name"
	volcanoTaskSpecKey               = "volcano.sh/task-spec"
	gangSchedulerName                = "volcano"
)

var (
	controlPlaneTolerations = []corev1.Toleration{{
		Key:      "node-role.kubernetes.io/master",
		Operator: corev1.TolerationOpEqual,
		Effect:   corev1.TaintEffectNoSchedule,
	}, {
		Key:      "node-role.kubernetes.io/control-plane",
		Operator: corev1.TolerationOpEqual,
		Effect:   corev1.TaintEffectNoSchedule,
	}}
)

var CancelStatusToReason = map[string]string{
	wsapisv1.CancelWithStatusCancelled: wsjobCancelledReason,
	wsapisv1.CancelWithStatusFailed:    wsjobFailedReason,
	wsapisv1.CancelWithStatusSucceeded: wsjobSucceededReason,
}

var cancelStatusToConditionType = map[string]commonapisv1.JobConditionType{
	wsapisv1.CancelWithStatusCancelled: commonapisv1.JobCancelled,
	wsapisv1.CancelWithStatusFailed:    commonapisv1.JobFailed,
	wsapisv1.CancelWithStatusSucceeded: commonapisv1.JobSucceeded,
}

func (*WSJobController) ControllerName() string {
	return "wsjob-operator"
}

func (*WSJobController) GetAPIGroupVersionKind() schema.GroupVersionKind {
	return wsapisv1.SchemeGroupVersion.WithKind(wsapisv1.Kind)
}

func (*WSJobController) GetAPIGroupVersion() schema.GroupVersion {
	return wsapisv1.SchemeGroupVersion
}

func (*WSJobController) GetGroupNameLabelValue() string {
	return wsapisv1.SchemeGroupVersion.Group
}

func (*WSJobController) GetDefaultContainerPortName() string {
	return wsapisv1.DefaultPortName
}

func (*WSJobController) GetDefaultContainerName() string {
	return wsapisv1.DefaultContainerName
}

func (jobController *WSJobController) GetJobFromInformerCache(namespace, name string) (metav1.Object, error) {
	wsjob := &wsapisv1.WSJob{}
	err := jobController.Get(context.Background(), types.NamespacedName{
		Namespace: namespace, Name: name,
	}, wsjob)
	if err != nil {
		return nil, err
	}
	return wsjob, err
}

func (jobController *WSJobController) GetPodFromInformerCache(namespace, name string) (metav1.Object, error) {
	pod := &corev1.Pod{}
	err := jobController.Get(context.Background(), types.NamespacedName{
		Namespace: namespace, Name: name,
	}, pod)
	if err != nil {
		return nil, err
	}
	return pod, err
}

func (jobController *WSJobController) GetServiceFromInformerCache(namespace, name string) (metav1.Object, error) {
	service := &corev1.Service{}
	err := jobController.Get(context.Background(), types.NamespacedName{
		Namespace: namespace, Name: name,
	}, service)
	if err != nil {
		return nil, err
	}
	return service, nil
}

func (jobController *WSJobController) GetJobFromAPIClient(namespace, name string) (metav1.Object, error) {
	wsjob := &wsapisv1.WSJob{}
	err := jobController.ApiReader.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, wsjob)
	if err != nil {
		if errors.IsNotFound(err) {
			jobController.Log.Error(err, "WSJob not found", "namespace", namespace, "name", name)
		} else {
			jobController.Log.Error(err, "Failed to get WSJob from api-server", "namespace", namespace, "name", name)
		}
		return nil, err
	}
	return wsjob, nil
}

func (jobController *WSJobController) DeleteJob(job interface{}) error {
	wsjob, ok := job.(*wsapisv1.WSJob)
	if !ok {
		return fmt.Errorf("%v is not a type of WSJob", wsjob)
	}

	log := jobController.Log.WithValues(wsapisv1.Singular, wsjob.Name)
	if err := jobController.Delete(context.Background(), wsjob); err != nil && !errors.IsNotFound(err) {
		jobController.Recorder.Eventf(wsjob, corev1.EventTypeWarning, failedDeleteJobReason, "Error deleting: %v", err)
		log.Error(err, "failed to delete WSJob")
		return err
	}

	jobController.Recorder.Eventf(wsjob, corev1.EventTypeNormal, successfulDeleteJobReason, "Deleted WSJob: %v", wsjob.Name)
	log.Info("WSJob has been deleted")
	wscommon.DeletedJobsCounterInc(wsjob.Namespace, wsapisv1.Kind)
	return nil
}

func (jobController *WSJobController) failJob(
	wsjob *wsapisv1.WSJob,
	msg string,
	jobStatus *commonapisv1.JobStatus,
) error {
	if jobStatus.CompletionTime == nil {
		now := metav1.Now()
		jobStatus.CompletionTime = &now
	}
	_ = commonutil.UpdateJobConditions(jobStatus, commonapisv1.JobFailed, wsjobFailedReason, msg)
	wscommon.FailedJobsCounterInc(wsjob.Namespace, wsapisv1.Kind)
	return jobController.UpdateJobStatusInApiServer(wsjob, jobStatus)
}

func (jobController *WSJobController) updateJobWithPodFailureTime(
	wsjob *wsapisv1.WSJob,
	msg string,
	jobStatus *commonapisv1.JobStatus,
) error {
	_ = commonutil.UpdateJobConditions(jobStatus, commonapisv1.JobFailing, wsjobFailingReason, msg)
	return jobController.UpdateJobStatusInApiServer(wsjob, jobStatus)
}

func (jobController *WSJobController) CancelJob(
	wsjob *wsapisv1.WSJob,
	msg string,
	cancelStatus string,
	jobStatus *commonapisv1.JobStatus,
) error {
	logger := jobController.Log.WithValues(wsapisv1.Singular, wsjob.Name)
	if commonutil.IsEnded(*jobStatus) {
		logger.Info("Job already ended, skip canceling", "id", wsjob.Name, "status", jobStatus)
		return nil
	}

	jobController.Recorder.Event(wsjob, corev1.EventTypeNormal, CancelStatusToReason[cancelStatus], msg)
	now := metav1.Now()
	jobStatus.CompletionTime = &now
	if msg == "" {
		msg = fmt.Sprintf("job terminated by client with status: %s", cancelStatusToConditionType[cancelStatus])
	}
	logger.Info(msg)

	// todo: extend to watch on events and persist as job/replica error to replace server side events pull
	// only check if there's no pod failure. if pod failed already, pod status message will include eviction message
	if commonutil.IsRunning(wsjob.Status) && cancelStatus == wsapisv1.CancelWithStatusFailed {
		eviction := wscommon.EvictionCheck(jobController.KubeClientSet, wsjob.Namespace, wsjob.Name)
		if eviction != "" {
			msg = fmt.Sprintf("%s, cluster side failure detected: %s", msg, eviction)
		}
	}

	err := commonutil.UpdateJobConditions(jobStatus,
		cancelStatusToConditionType[cancelStatus], CancelStatusToReason[cancelStatus], msg)
	if err != nil {
		logger.Error(err, "Update WSJob cancel condition error")
		return err
	}
	wscommon.CancelledJobsCounterInc(wsjob.Namespace, wsapisv1.Kind)

	wsjob.Status = *jobStatus.DeepCopy()
	err = jobController.UpdateJobStatusInApiServer(wsjob, &wsjob.Status)
	if err != nil {
		return err
	}

	return nil
}

func (jobController *WSJobController) UpdateJobStatus(
	job interface{},
	replicas map[commonapisv1.ReplicaType]*commonapisv1.ReplicaSpec,
	jobStatus *commonapisv1.JobStatus,
) error {
	wsjob, ok := job.(*wsapisv1.WSJob)
	if !ok {
		return fmt.Errorf("%v is not a type of WSJob", wsjob)
	}

	wsjobKey, err := commonctrlv1.KeyFunc(wsjob)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for WSJob object %#v: %v", wsjob, err))
		return err
	}

	logger := jobController.Log.WithValues(wsapisv1.Singular, wsjob.Name)

	// Set StartTime.
	if wsjob.Status.StartTime == nil && commonutil.HasCondition(wsjob.Status, commonapisv1.JobRunning) {
		now := metav1.Now()
		jobStatus.StartTime = &now
		// enqueue a sync to check if job past ActiveDeadlineSeconds
		if wsjob.Spec.RunPolicy.ActiveDeadlineSeconds != nil {
			logger.Info("Job with ActiveDeadlineSeconds will sync later", "seconds", *wsjob.Spec.RunPolicy.ActiveDeadlineSeconds)
			jobController.WorkQueue.AddAfter(wsjobKey, time.Duration(*wsjob.Spec.RunPolicy.ActiveDeadlineSeconds)*time.Second)
		}
	}

	totalReplicas := int32(0)
	totalExpected := int32(0)
	totalInit := int32(0)
	totalRunning := int32(0)
	totalFailed := int32(0)
	totalReady := int32(0)
	var failedPodsStatus []commonapisv1.FailedPodStatus
	var initPods []string
	for rtype := range replicas {
		if replicas[rtype] == nil {
			continue
		}
		spec := replicas[rtype]
		status := jobStatus.ReplicaStatuses[rtype]

		// Expect to have `replicas - succeeded` pods alive.
		succeeded := status.Succeeded
		expected := *(spec.Replicas) - succeeded
		init := status.Init
		running := status.Active
		ready := status.Ready
		failed := status.Failed

		logger.Info("status", "ReplicaType", rtype,
			"expected", expected,
			"init", init,
			"running", running,
			"ready", ready,
			"succeeded", succeeded,
			"failed", failed)

		totalExpected += expected
		totalRunning += running
		totalInit += init
		totalReady += ready
		totalReplicas += *(spec.Replicas)

		if failed > 0 {
			restart := false
			for _, condition := range jobStatus.Conditions {
				if condition.Type == commonapisv1.JobRestarting {
					restart = true
				}
			}
			if restart {
				// job is restarting, no need to set it failed
				// we know it because we update the status condition when reconciling the replicas
				wscommon.RestartedJobsCounterInc(wsjob.Namespace, wsapisv1.Kind)
			} else {
				msg := fmt.Sprintf("%s/%s has %d %s replica(s) failed",
					wsjob.Namespace, wsjob.Name, failed, rtype)
				jobController.Recorder.Event(wsjob, corev1.EventTypeNormal, wsjobFailedReason, msg)
				totalFailed += failed
			}
			// Continue collecting failed pod info
			for i, poStatus := range status.FailedPodStatuses {
				failedPodsStatus = append(failedPodsStatus, poStatus)
				// put here as workaround due to lack of access to kube-client in status update logic
				logs, err := wscommon.GetPodLogs(jobController.KubeClientSet,
					wsjob.Namespace, poStatus.Name, poStatus.Container, int64(wscommon.PodLogLines))
				if err != nil {
					logger.Error(err, "unable to fetch pod log",
						"ns", wsjob.Namespace, "pod", poStatus.Name)
				}
				status.FailedPodStatuses[i].Log = logs
			}
		}
		initPods = append(initPods, status.InitPods...)
	}

	if totalFailed > 0 {
		var podFailureMsg string
		if len(failedPodsStatus) > 0 {
			sort.Slice(failedPodsStatus, func(i, j int) bool {
				if failedPodsStatus[i].FinishedAt.IsZero() {
					return true
				}
				if failedPodsStatus[j].FinishedAt.IsZero() {
					return false
				}
				return failedPodsStatus[i].FinishedAt.Time.Before(failedPodsStatus[j].FinishedAt.Time)
			})
			podFailureMsg = fmt.Sprintf("first failed pods: %v",
				wscommon.Map(func(status commonapisv1.FailedPodStatus) string {
					return status.Name
				}, failedPodsStatus))
		}

		// Check if this is the first failure
		if jobController.getPodFailureCondition(jobStatus) == nil {
			jobFailureMsg := fmt.Sprintf("job failing with pod failure detected, "+
				"starting grace period (until client cancel or timeout [%s])", wscommon.FailingJobGracePeriod)
			logger.Info(jobFailureMsg)
			// Early return; this is all we need to do for the first failure
			return jobController.updateJobWithPodFailureTime(wsjob, fmt.Sprintf("%s, %s", jobFailureMsg, podFailureMsg), jobStatus)
		}

		if err := jobController.requeueAfterPodFailure(wsjob, jobStatus, podFailureMsg); err != nil {
			return err
		}

		jobFailureMsg := fmt.Sprintf("job has failed because total %d replica(s) failed", totalFailed)
		return jobController.failJob(wsjob, fmt.Sprintf("%s, %s", jobFailureMsg, podFailureMsg), jobStatus)
	}
	var phaseMetric string
	if totalInit > 0 {
		phaseMetric = string(commonapisv1.JobBootstrapping)
	} else if totalReady == totalReplicas {
		phaseMetric = string(commonapisv1.JobReady)
	}
	logger.Info("Job phase metric updated",
		"namespace", wsjob.Namespace,
		"status", phaseMetric)
	wscommon.JobPhaseMetricAdded(wsjob.Namespace, wsjob.Name, phaseMetric)

	// only check when all pods initialized to account for pod creation time and avoid blocking pod status update
	// use running+init to account for cases where part of the pods already finished init and started running
	if totalInit > 0 && totalInit+totalRunning == totalExpected {
		for _, condition := range jobStatus.Conditions {
			if condition.Type == commonapisv1.JobInitializing {
				timeNow := time.Now()

				gracePeriod := commonctrlv1.GetGracePeriodByJobSize(wscommon.PendingPodGracePeriod, int(totalExpected))
				if podTTL, err := strconv.Atoi(wsjob.Labels["PENDING_POD_TTL_SECONDS"]); err == nil {
					// for test override
					gracePeriod = time.Duration(podTTL) * time.Second
				}
				deadline := condition.LastTransitionTime.Add(gracePeriod)
				if timeNow.After(deadline) {
					msg := fmt.Sprintf("job failed due to stuck at init phase for more than %s", gracePeriod)
					if len(initPods) > 0 {
						logs, err := wscommon.GetPodLogs(jobController.KubeClientSet,
							wsjob.Namespace, initPods[0], wsapisv1.DefaultInitContainerName, int64(wscommon.PodLogLines))
						if err != nil {
							logger.Error(err, "unable to fetch pod init container log",
								"ns", wsjob.Namespace, "pod", initPods[0])
						}
						msg = fmt.Sprintf("%s, logs of one init pod %s: %s", msg, initPods[0], logs)
					}
					return jobController.failJob(wsjob, msg, jobStatus)
				} else {
					requeueInterval := deadline.Sub(timeNow)
					logger.Info(fmt.Sprintf("all pods of job %s are in init phase within grace period, requeue after %s",
						wsjob.NamespacedName(), requeueInterval.String()))
					return &commonapisv1.RequeueAfterTTL{TTL: requeueInterval}
				}
			}
		}

		err = commonutil.UpdateJobConditions(jobStatus,
			commonapisv1.JobInitializing, wsjobInitializingReason, "job initializing with image pulling/config update/...")
		if err != nil {
			logger.Error(err, "Update WSJob condition error")
			return err
		}
	}

	if totalRunning > 0 {
		err := commonutil.UpdateJobConditions(jobStatus,
			commonapisv1.JobRunning, wsjobRunningReason, "job started running")
		if err != nil {
			logger.Error(err, "Update WSJob condition error")
			return err
		}
	}

	if totalExpected == 0 {
		msg := fmt.Sprintf("job successfully completed")
		jobController.Recorder.Event(wsjob, corev1.EventTypeNormal, wsjobSucceededReason, msg)
		if jobStatus.CompletionTime == nil {
			now := metav1.Now()
			jobStatus.CompletionTime = &now
		}
		err := commonutil.UpdateJobConditions(jobStatus,
			commonapisv1.JobSucceeded, wsjobSucceededReason, msg)
		if err != nil {
			logger.Error(err, "Update WSJob condition error")
			return err
		}
		wscommon.SuccessfulJobsCounterInc(wsjob.Namespace, wsapisv1.Kind)
	}

	// we assign the jobStatus to the wsjob.Status for testing purpose
	// it won't affect the main reconcile logic
	// because we already use oldStatus := jobStatus.DeepCopy() to record the oldStatus
	// and use !reflect.DeepEqual(*oldStatus, jobStatus) to decide whether to update the wsjob or not
	wsjob.Status = *jobStatus.DeepCopy()

	return nil
}

func (jobController *WSJobController) UpdateJobStatusInApiServer(
	job interface{}, jobStatus *commonapisv1.JobStatus,
) error {
	wsjob, ok := job.(*wsapisv1.WSJob)
	if !ok {
		return fmt.Errorf("%v is not a type of WSJob", wsjob)
	}
	now := metav1.Now()
	logger := jobController.Log.WithValues(wsapisv1.Singular, wsjob.Name)

	wsjob = wsjob.DeepCopy()
	wsjob.Status = *jobStatus.DeepCopy()

	wsjob.Status.LastReconcileTime = &now
	if wsjob.Status.CompletionTime == nil && commonutil.IsEnded(wsjob.Status) {
		wsjob.Status.CompletionTime = &now
	}

	if wsjob.Status.StartTime == nil && commonutil.HasCondition(wsjob.Status, commonapisv1.JobRunning) {
		wsjob.Status.StartTime = &now
	}

	if wsjob.Status.ReplicaStatuses == nil {
		wsjob.Status.ReplicaStatuses = map[commonapisv1.ReplicaType]*commonapisv1.ReplicaStatus{}
	}

	if wsjob.Status.Conditions == nil {
		wsjob.Status.Conditions = []commonapisv1.JobCondition{}
	} else {
		logger.Info("Job phase metric updated",
			"namespace", wsjob.Namespace,
			"status", wsjob.Status.Conditions[len(wsjob.Status.Conditions)-1].Type)
		wscommon.JobPhaseMetricAdded(wsjob.Namespace, wsjob.Name,
			string(wsjob.Status.Conditions[len(wsjob.Status.Conditions)-1].Type))
	}

	logger.Info("WSJob Status update starts", "current-rv", wsjob.ResourceVersion, "status", jobStatus)
	for t, c := range jobStatus.ReplicaStatuses {
		logger.Info("Replica Status", "type", t, "status", *c)
	}
	if err := jobController.Status().Update(context.Background(), wsjob); err != nil {
		logger.Info("Error updating WSJob.status. Potential stale object",
			"current-rv", wsjob.ResourceVersion,
			"err", err,
		)
		return err
	}
	logger.Info("Successfully updated WSJob Status")
	return nil
}

func shouldCreateService(wsjob *wsapisv1.WSJob, rtype string) bool {
	if wsjob.Annotations[wsapisv1.DisableNonCRDServicesAnnotation] != "true" {
		return true
	}

	// SDK Execute worker is equivalent to a coordinator
	if wsjob.IsSdk() && !wsjob.IsCompile() {
		return rtype == wsapisv1.WSReplicaTypeWorker.Lower()
	}
	return rtype == wsapisv1.WSReplicaTypeCoordinator.Lower()
}

// svcShouldExist returns true if the service should have been created during
// job creation
func svcShouldExist(wsjob *wsapisv1.WSJob, serviceId string) bool {
	// svc-JOBID-REPLICATYPE-INDEX
	svcIdParts := strings.Split(serviceId, "-")
	if len(svcIdParts) < 4 {
		logrus.Warnf("unexpected service ID format: '%s'", serviceId)
		return true // unexpected wsjob service format
	}
	rtype := svcIdParts[len(svcIdParts)-2]
	if wsjob.IsSdk() && !wsjob.IsCompile() {
		return rtype == wsapisv1.WSReplicaTypeWorker.Lower()
	}
	return rtype == wsapisv1.WSReplicaTypeCoordinator.Lower()
}

// ReconcileServices creates services for replica types that directly communicate with nginx. All other replica
// types communicate directly through pod IP. See kubeflow/common/pkg/reconciler.v1/common/service.go
func (jc *WSJobController) ReconcileServices(
	job metav1.Object,
	services []*corev1.Service,
	rtype commonapisv1.ReplicaType,
	spec *commonapisv1.ReplicaSpec) error {

	wsjob, isWsjob := job.(*wsapisv1.WSJob)
	rt := rtype.Lower()
	if !isWsjob || shouldCreateService(wsjob, rt) {
		// delegate to kubeflow controller
		return jc.JobController.ReconcileServices(job, services, rtype, spec)
	}
	return nil
}

// ReconcilePods delegates to kubeflow for main reconcile logic. Then creates endpoint objects for required pods
func (jc *WSJobController) ReconcilePods(
	job interface{},
	jobStatus *commonapisv1.JobStatus,
	pods []*corev1.Pod,
	rType commonapisv1.ReplicaType,
	spec *commonapisv1.ReplicaSpec,
	replicas map[commonapisv1.ReplicaType]*commonapisv1.ReplicaSpec) error {

	// delegate main pod reconciliation to kubeflow
	if err := jc.JobController.ReconcilePods(job, jobStatus, pods, rType, spec, replicas); err != nil {
		return err
	}

	wsjob, isWsjob := job.(*wsapisv1.WSJob)
	if !isWsjob {
		return nil
	}

	rt := rType.Lower()
	if !jc.Config.EnableMultus {
		return nil
	}
	if !shouldCreateService(wsjob, rt) {
		return nil
	}

	pods, err := jc.FilterPodsForReplicaType(pods, rt)
	if err != nil {
		return err
	}
	podSlices := jc.GetPodSlices(pods, int(*spec.Replicas), commonutil.LoggerForReplica(wsjob, rt))
	for _, podSlice := range podSlices {
		if len(podSlice) < 1 {
			continue
		}
		pod := podSlice[0]
		// simulating the endpoint controller behavior, only create ep when pod ready
		if !wscommon.PodHasCondition(pod, corev1.PodReady) {
			continue
		}

		ips, err := jc.Controller.GetPodDataIP(pod)
		if err != nil {
			jc.Log.Error(err, "Failed to GetPodDataIP", "namespace", pod.Namespace, "pod", pod.Name)
			jc.Recorder.Event(wsjob, corev1.EventTypeWarning, wsapisv1.PodDataNetNotAttached, err.Error())
			jc.Recorder.Event(pod, corev1.EventTypeWarning, wsapisv1.PodDataNetNotAttached, err.Error())
			return err
		}

		if len(ips) == 0 {
			continue
		}

		index, err := utillabels.ReplicaIndex(pod.Labels)
		if err != nil {
			jc.Log.Error(err, "Error obtaining replica index from Pod", "namespace", pod.Namespace, "pod", pod.Name)
			continue
		}

		epName := commonctrlv1.GenSvcName(wsjob.GetName(), rt, strconv.Itoa(index))
		ep, err := jc.Controller.GetEndpoint(pod.Namespace, epName)
		if err != nil {
			return err
		}
		// create endpoint if not yet created
		if ep == nil {
			if err := jc.createNewEndpoint(epName, ips[0], pod, spec); err != nil {
				return err
			}
		}
	}

	return nil
}

// createNewEndpoint creates a new endpoint for the given index and type.
func (jc *WSJobController) createNewEndpoint(epName, addr string, pod *corev1.Pod, spec *commonapisv1.ReplicaSpec) error {
	ports, err := jc.GetPortsFromJob(spec)
	if err != nil {
		jc.Log.Error(err, "failed to GetPortsFromJob")
		return err
	}
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:   epName,
			Labels: pod.Labels,
		},
		Subsets: []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{
					{
						IP: addr,
						TargetRef: &corev1.ObjectReference{
							APIVersion: pod.APIVersion,
							Kind:       pod.Kind,
							Name:       pod.GetName(),
							UID:        pod.GetUID(),
						},
					},
				},
			},
		},
	}
	for name, port := range ports {
		epPort := corev1.EndpointPort{Name: name, Port: port}
		ep.Subsets[0].Ports = append(ep.Subsets[0].Ports, epPort)
	}
	ep.OwnerReferences = append(ep.OwnerReferences, metav1.OwnerReference{
		APIVersion: pod.APIVersion,
		Kind:       pod.Kind,
		Name:       pod.GetName(),
		UID:        pod.GetUID(),
	})

	if err = jc.PodControl.CreateEndpoint(pod.Namespace, ep); err != nil {
		jc.Log.Error(err, "failed to CreateEndpoint")
		return err
	}
	return nil
}

// nodeFromReplicaId returns the resource id assigned to the replica at the given index by the resource lock.
func (jobController *WSJobController) nodeFromReplicaId(wsjobKey client.ObjectKey, rType commonapisv1.ReplicaType, index int) (*jobResources, string, error) {
	jr := jobResources{}
	if wscommon.DisableNodeGroupScheduling {
		return nil, "", nil
	}

	entry, ok := jobController.JobResourceCache.Get(wsjobKey)
	if !ok {
		rl := &rlv1.ResourceLock{}
		if err := jobController.Client.Get(context.Background(), wsjobKey, rl); err != nil {
			return nil, "", err
		}
		entry = newJobResources(jobController.Cfg, wsjobKey, rl)
		jobController.JobResourceCache.Add(wsjobKey, entry)
	}
	jr = entry.(jobResources)
	no := jr.replicaToNodeMap[string(rType)][index]
	if no == "" {
		logrus.Warnf("%s %s node id %d not found in resource %+v",
			wsjobKey, rType, index, jr.replicaToNodeMap[string(rType)])
	}

	return &jr, no, nil
}

// brNodeIdFromReplicaId returns the resource id assigned to the replica at the given index by the resource lock.
func (jobController *WSJobController) brNodeIdFromReplicaId(wsjobKey client.ObjectKey, index int) (int, error) {
	entry, ok := jobController.JobResourceCache.Get(wsjobKey)
	if !ok {
		rl := &rlv1.ResourceLock{}
		if err := jobController.Client.Get(context.Background(), wsjobKey, rl); err != nil {
			jobController.Log.Info("failed to get job resource", "job", wsjobKey, "err", err)
			return -1, err
		}
		entry = newJobResources(jobController.Cfg, wsjobKey, rl)
		jobController.JobResourceCache.Add(wsjobKey, entry)
	}
	jr := entry.(jobResources)
	if index >= len(jr.replicaToBrNodeIdMap) {
		return -1,
			CriticalErrorf("granted br replicas(%d) are less than requested index(%d)",
				len(jr.replicaToBrNodeIdMap), index)
	}
	return jr.replicaToBrNodeIdMap[index], nil
}

func (jobController *WSJobController) SetClusterSpec(
	job interface{}, podTemplate *corev1.PodTemplateSpec, rType commonapisv1.ReplicaType, index string) error {

	rt := rType.Lower()
	wsjob, ok := job.(*wsapisv1.WSJob)
	if !ok {
		return fmt.Errorf("%v is not a type of WSJob", job)
	}
	var err error
	ridInt, _ := strconv.Atoi(index)
	if rType == wsapisv1.WSReplicaTypeBroadcastReduce {
		ridInt, err = jobController.brNodeIdFromReplicaId(wsjob.NamespacedName(), ridInt)
		if err != nil {
			return err
		}
	}
	ridStr := strconv.Itoa(ridInt)
	jr, nodeName, err := jobController.nodeFromReplicaId(wsjob.NamespacedName(), rType, ridInt)
	if err != nil {
		return err
	}
	podTemplate.Name = fmt.Sprintf("%s-%s-%s", wsjob.Name, rType.Lower(), ridStr)

	// set node selector by node name/group/role
	podTemplate.Spec.NodeSelector = wscommon.EnsureMap(podTemplate.Spec.NodeSelector)
	node := jobController.Cfg.NodeMap[nodeName]
	if !wscommon.DisableNodeGroupScheduling && node != nil {
		podTemplate.Spec.NodeSelector[corev1.LabelHostname] = nodeName
	}
	nodeRoleMap := getJobNodeRoleSpecialization(wsjob)
	if nodeRole, ok := nodeRoleMap[rType]; ok {
		if nodeRole == wscommon.RoleCoordinator && wscommon.IsDev {
			// If dev mode is enabled and there is a worker node in the cluster, then re-assign the coordinator
			// to a worker node to reduce disk space requirement incurred by containerd image management.
			// TODO: move this to the lock creation request
			for _, n := range jobController.Cfg.Nodes {
				if n.HasRole(wscommon.RoleWorker) {
					podTemplate.Spec.NodeSelector[corev1.LabelHostname] = n.Name
					nodeRole = wscommon.RoleWorker
					break
				}
			}
		}
		if !wscommon.DisableScheduleNodeRole {
			podTemplate.Spec.NodeSelector[fmt.Sprintf(RoleLabelKeyF, nodeRole)] = ""
		}
	}

	// set annotation
	wsjob.Annotations = wscommon.EnsureMap(wsjob.Annotations)
	podTemplate.Annotations = wscommon.EnsureMap(podTemplate.Annotations)
	if !wscommon.DisableMultus {
		if jr == nil {
			// dev mode disable nodegroup scheduling
			podTemplate.Annotations[wscommon.NetworkAttachmentKey] = wscommon.GetNadByIndex(0)
		} else {
			if val, ok := jr.replicaToNadMap[string(rType)][ridInt]; ok {
				podTemplate.Annotations[wscommon.NetworkAttachmentKey] = strings.Join(val, ",")
				jobController.Log.V(1).Info("set nad for pod",
					"node", nodeName, "pod", commonctrlv1.GenGeneralName(wsjob.GetName(), rt, ridStr),
					"rtype", rType, "rid", ridStr, "nad", podTemplate.Annotations[wscommon.NetworkAttachmentKey])
			}
		}
	}

	// bypass k8s control-plane & master taints for coordinator or a single-node k8s deployment for now.
	if wsjob.IsIngressReplicaType(rType) || len(jobController.Cfg.Nodes) == 1 {
		podTemplate.Spec.Tolerations = controlPlaneTolerations
	}

	// The working dir prefix was set in the hostpath modifier due to the replica index was unknown.
	// We are now populating the full path for the working dir.
	replicaDir := fmt.Sprintf("%s-%s", rt, ridStr)
	if wsjob.RequireInfraSetup() {
		for k, v := range podTemplate.Spec.Volumes {
			if v.Name == wsapisv1.WorkdirVolume {
				if v.NFS != nil {
					podTemplate.Spec.Volumes[k].NFS.Path = path.Join(podTemplate.Spec.Volumes[k].NFS.Path, replicaDir)
				} else if v.HostPath != nil {
					// for test purposes only
					podTemplate.Spec.Volumes[k].HostPath.Path = path.Join(podTemplate.Spec.Volumes[k].HostPath.Path, replicaDir)
				}
			}
		}
	}

	// set envs
	for i := range podTemplate.Spec.Containers {
		c := &podTemplate.Spec.Containers[i]
		if !wsapisv1.IsWsjobContainer(c) {
			continue
		}

		c.Env = append(c.Env,
			corev1.EnvVar{
				Name:  wsapisv1.ReplicaId,
				Value: ridStr,
			},
			corev1.EnvVar{
				Name:  wsapisv1.ReplicaType,
				Value: rt,
			},
		)

		if rType == wsapisv1.WSReplicaTypeCoordinator &&
			strings.ToLower(wsjob.Annotations[wscommon.DisableFabricJsonCheckKey]) != "true" &&
			wsjob.IsCompile() {
			okSystems := jobController.Cfg.GetOkSystemsInNamespace(wsjob.Namespace)
			if okSystems != "" {
				c.Env = append(c.Env,
					corev1.EnvVar{
						Name:  wscommon.OkSystemsInNamespace,
						Value: okSystems,
					})
			}
		}

		// this is needed for some extra envs to be exported on the all replica start time
		if len(jobController.Cfg.Systems) > 0 {
			if strings.Contains(strings.ToUpper(jobController.Cfg.Systems[0].Name), KAPI) {
				c.Env = append(c.Env,
					corev1.EnvVar{
						Name:  SystemType,
						Value: KAPI,
					})
			} else if strings.Contains(strings.ToUpper(jobController.Cfg.Systems[0].Name), SYSEMU) {
				c.Env = append(c.Env,
					corev1.EnvVar{
						Name:  SystemType,
						Value: SYSEMU,
					})
			} else {
				c.Env = append(c.Env,
					corev1.EnvVar{
						Name:  SystemType,
						Value: CSX,
					})
			}
		}

		// set mem env
		setMem := *c.Resources.Limits.Memory()
		memLimitExists := true
		if setMem.IsZero() {
			setMem = *c.Resources.Requests.Memory()
			if jr != nil && !jr.IsInferenceJobSharingEnabled(rType) {
				memLimitExists = false
			}
		}

		cpuLimit := *c.Resources.Limits.Cpu()
		cpuLimitExists := true
		if cpuLimit.IsZero() {
			cpuLimit = *c.Resources.Requests.Cpu()
			if jr != nil && !jr.IsInferenceJobSharingEnabled(rType) {
				cpuLimitExists = false
			}
		}

		if !cpuLimit.IsZero() && cpuLimitExists {
			c.Resources.Limits = wscommon.EnsureMap(c.Resources.Limits)
			c.Resources.Limits[corev1.ResourceCPU] = cpuLimit
		}

		if !setMem.IsZero() {
			var podMemOverride resource.Quantity
			if jr != nil && len(jr.replicaToResourceMap[string(rType)][ridInt]) > 0 {
				sr := jr.replicaToResourceMap[string(rType)][ridInt]
				podMemOverride = res.Subresources(sr).ToQuantities()[corev1.ResourceMemory].DeepCopy()
				memOverride := podMemOverride.DeepCopy()

				// We are only overriding for memory
				// If down the road we need to also override for other resources, we will extend the same pattern
				//
				// We split the overridden memory by respecting the existing split among containers in the same pod according to a specific ratio
				// such that each container has a memory limit according to the ratio. For example, for a ratio of 8:2, main container can use upto
				// 80% of the memory, and sidecar can use upto 20%.
				// This is however not ideal for memory utilization - especially during job sharing. Given the fact that workload from main container and sidecar
				// do not overlap, it's possible that we allow main container to use upto 100%, and expect sidecar to use 0%.
				// We will attempt this strategy by disabling the ratio based memory limit allocation for job sharing, by allowing both main container and sidecar to
				// use upto 100% of the memory limit, and rely on the dynamic adjustment provided by the nature of the workload.
				if requestedPodMem, _ := strconv.Atoi(podTemplate.Annotations[wsapisv1.PodMemoryBaseKey]); requestedPodMem > 0 && !jr.IsInferenceJobSharingEnabled(rType) {
					// memOverride is the pod-level memory override after optional memory granting
					// requestedPodMem is the pod-level originally requested memory lower bound
					// setMem is the container-level originally requested memory lower bound
					newVal, err := wscommon.ScaleAndRoundUp(memOverride.Value(), int64(requestedPodMem), setMem.Value())
					if err != nil {
						return err
					}
					memOverride.Set(newVal)
				}

				jobController.Log.V(1).Info("set mem requests",
					"node", nodeName, "pod", commonctrlv1.GenGeneralName(wsjob.GetName(), rt, ridStr),
					"rType", rType, "rid", ridStr, "default", setMem.String(), "override", memOverride.String())
				setMem = memOverride
				if memLimitExists {
					c.Resources.Limits = wscommon.EnsureMap(c.Resources.Limits)
					c.Resources.Limits[corev1.ResourceMemory] = memOverride
				}

				if jr.IsInferenceJobSharingEnabled(rType) && wsapisv1.IsUserSidecarContainer(c) {
					c.Resources.Requests[corev1.ResourceMemory] = *resource.NewQuantity(int64(0), resource.BinarySI)
				} else {
					c.Resources.Requests[corev1.ResourceMemory] = memOverride
				}
			}

			// Needed for runtime, but doesn't hurt to add to others as well.
			// If memory limits do not exist, we set the environment variable to honour the granted memory at pod level.
			// Context is that pods like weight may have multi containers where their workload do not overlap in time.
			setMemMi := int(wscommon.Ternary(memLimitExists, setMem.Value(), podMemOverride.Value()) >> 20) // round down to avoid communicating a higher value than set
			c.Env = append(c.Env, corev1.EnvVar{
				Name:  wsapisv1.RuntimeMemoryEnvVarName,
				Value: strconv.Itoa(setMemMi),
			})

			// Convert the rounded overrideMem in MiB to bytes, and use as WS_RT_SET_MEM_B
			// TODO: Once both cluster_mgmt and runtime has adopted the transition from using MiB to bytes for
			// accounting everywhere, we can use the more precise overrideMemBytes as-is.
			if jr != nil && jr.IsInferenceJobSharingEnabled(rType) {
				c.Env = append(c.Env, corev1.EnvVar{
					Name:  wsapisv1.RuntimeMemoryBytesEnvVarName,
					Value: strconv.Itoa(setMemMi << 20),
				})
			}
		}

		// Nodegroup scheduling is disabled in kind, where job resource can be nil. Job resource should never
		// be nil in production flow.
		if jr != nil {
			if rType == wsapisv1.WSReplicaTypeBroadcastReduce && jr.getAssignedInstanceId(rType, ridInt) != "" {
				c.Env = append(c.Env, corev1.EnvVar{
					Name:  wsapisv1.BrInstanceIdEnvVarName,
					Value: jr.getAssignedInstanceId(rType, ridInt),
				})
			}
			// In rel-2.3, we plumbed through WS_RT_SET_PORT for activation
			if rType == wsapisv1.WSReplicaTypeActivation || rType == wsapisv1.WSReplicaTypeKVStorageServer || rType == wsapisv1.WSReplicaTypeSWDriver {
				c.Env = append(c.Env, jr.getEnvVars(rType, ridInt)...)
			}
			// In rel-2.4, we plumbed through NVMF_USABLE_RANGES for coordinator and weight
			// We also add a new block device mount to them
			if len(jr.nvmfGrants) > 0 && slices.Contains([]commonapisv1.ReplicaType{
				wsapisv1.WSReplicaTypeCoordinator, wsapisv1.WSReplicaTypeWeight}, rType) {
				if c.SecurityContext == nil {
					c.SecurityContext = &corev1.SecurityContext{}
				}
				c.SecurityContext.Privileged = wscommon.Pointer(true)
				c.Env = append(c.Env, jr.getEnvVars(rType, ridInt)...)
				for _, env := range c.Env {
					if env.Name != wsapisv1.NvmfUsableRangesEnvVarName {
						continue
					}
					devicePaths := map[string]bool{}
					// "env.Value" examples:
					// coordinator - "/dev/datanvme101:2000-8000,/dev/datanvme102:4000-9000"
					// weight - "/dev/datanvme101:2000-8000"
					for _, nvmfGrant := range strings.Split(env.Value, ",") {
						devicePath := strings.Split(nvmfGrant, ":")[0]
						devicePaths[devicePath] = true
					}
					for i, devicePath := range wscommon.Keys(devicePaths) {
						name := fmt.Sprintf("%s-%d", wsapisv1.NvmfVolumePrefix, i)
						c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
							Name:      name,
							MountPath: devicePath,
						})
						hostpathType := corev1.HostPathBlockDev
						podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes, corev1.Volume{
							Name: name,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: devicePath,
									Type: &hostpathType,
								},
							},
						})
					}
				}
			}
			// short term workaround to pass in exec job min res assign for train-eval flow
			if jr.lockType == rlv1.PrioritizedCompileQueueName {
				parentJr, _, err := jobController.nodeFromReplicaId(client.ObjectKey{
					Namespace: wsjob.Namespace,
					Name:      jr.parentJobName,
				}, rType, ridInt)
				// continue as best effort if parent job not found
				if err != nil && !errors.IsNotFound(err) {
					jobController.Log.Info("get parent lock failed",
						"job", wsjob.GetName(), "parent", jr.parentJobName)
					return err
				}
				if parentJr != nil {
					for _, parentRt := range []commonapisv1.ReplicaType{
						wsapisv1.WSReplicaTypeActivation,
						wsapisv1.WSReplicaTypeWeight,
						wsapisv1.WSReplicaTypeKVStorageServer,
					} {
						minMem := parentJr.getReplicaTypeMinAssign(parentRt, res.MemSubresource)
						if minMem != "" {
							c.Env = append(c.Env, corev1.EnvVar{
								Name:  fmt.Sprintf(wsapisv1.RuntimeMinMemoryEnvVarName, wsapisv1.GetReplicaTypeShort(parentRt).Upper()),
								Value: parentJr.getReplicaTypeMinAssign(parentRt, res.MemSubresource),
							})
							jobController.Log.V(1).Info("set parent min mem requests",
								"job", wsjob.GetName(), "parent", jr.parentJobName,
								"rType", parentRt, "minMem", minMem)
						}
					}
				}
			}
		}

		for j, vm := range c.VolumeMounts {
			// When cluster server sets up the volume, the volume is at replica type level.
			// We will need to hydrate the volume to the level of individual pods.
			if vm.Name == wsapisv1.WorkdirVolume {
				c.WorkingDir = path.Join(c.WorkingDir, replicaDir)
				c.VolumeMounts[j].MountPath = path.Join(c.VolumeMounts[j].MountPath, replicaDir)
				c.VolumeMounts[j].SubPathExpr = path.Join(wsjob.Namespace, wsjob.Name, replicaDir)
				c.Env = append(c.Env, corev1.EnvVar{
					Name:  wsapisv1.ReplicaWorkdir,
					Value: c.VolumeMounts[j].MountPath,
				})

				// When infra setup is required, we avoid kubelet to create directories, which
				// may lead to permission denied error.
				if wsjob.RequireInfraSetup() {
					c.WorkingDir = ""
					c.VolumeMounts[j].SubPathExpr = ""
				}
			}
		}
	}
	wscommon.DedupeMountDirVolumes(podTemplate)
	return nil
}

func (jobController *WSJobController) IsMasterRole(
	_ map[commonapisv1.ReplicaType]*commonapisv1.ReplicaSpec,
	_ commonapisv1.ReplicaType,
	_ int,
) bool {
	return false
}

// GetPodsForJob returns the set of pods that this job should manage.
// It also reconciles ControllerRef by adopting/orphaning.
// Note that the returned Pods are pointers into the Cache.
func (jobController *WSJobController) GetPodsForJob(jobObject interface{}) ([]*corev1.Pod, error) {
	job, ok := jobObject.(metav1.Object)
	if !ok {
		return nil, fmt.Errorf("job is not of type metav1.Object")
	}

	// Create selector.
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: jobController.GenLabels(job.GetName()),
	})

	if err != nil {
		return nil, fmt.Errorf("couldn't convert job selector: %v", err)
	}
	// List all pods to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller.
	podlist := &corev1.PodList{}
	err = jobController.List(context.Background(), podlist,
		client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(job.GetNamespace()))
	if err != nil {
		return nil, err
	}

	pods := wscommon.PointerList(podlist.Items)

	// If any adoptions are attempted, we should first recheck for deletion
	// with an uncached quorum read sometime after listing Pods.
	canAdoptFunc := commonctrlv1.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh, err := jobController.GetJobFromAPIClient(job.GetNamespace(), job.GetName())
		if err != nil {
			return nil, err
		}
		if fresh.GetUID() != job.GetUID() {
			return nil, fmt.Errorf("original Job %v/%v is gone: got uid %v, wanted %v",
				job.GetNamespace(), job.GetName(), fresh.GetUID(), job.GetUID())
		}
		return fresh, nil
	})
	cm := control.NewPodControllerRefManager(
		jobController.PodControl, job, selector, jobController.Controller.GetAPIGroupVersionKind(), canAdoptFunc)
	return cm.ClaimPods(pods)
}

func (jobController *WSJobController) GetServicesForJob(
	jobObject interface{},
) ([]*corev1.Service, error) {
	job, ok := jobObject.(metav1.Object)
	if !ok {
		return nil, fmt.Errorf("job is not of type metav1.Object")
	}

	// Create selector
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: jobController.GenLabels(job.GetName()),
	})

	if err != nil {
		return nil, fmt.Errorf("couldn't convert job selector: %v", err)
	}
	// List all services to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller.
	svclist := &corev1.ServiceList{}
	err = jobController.List(context.Background(), svclist,
		client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(job.GetNamespace()))
	if err != nil {
		return nil, fmt.Errorf("couldn't get Service: %v", err)
	}

	// If any adoptions are attempted, we should first recheck for deletion
	// with an uncached quorum read sometime after listing services (see #42639).
	canAdoptFunc := commonctrlv1.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh, err := jobController.GetJobFromInformerCache(job.GetNamespace(), job.GetName())
		if err != nil {
			return nil, err
		}
		if fresh.GetUID() != job.GetUID() {
			return nil, fmt.Errorf("original job %v/%v is gone: got uid %v, wanted %v",
				job.GetNamespace(), job.GetName(), fresh.GetUID(), job.GetUID())
		}
		return fresh, nil
	})
	cm := control.NewServiceControllerRefManager(
		jobController.ServiceControl, job, selector, jobController.Controller.GetAPIGroupVersionKind(), canAdoptFunc)

	return cm.ClaimServices(wscommon.PointerList(svclist.Items))
}

// GetEndpoint returns the endpoint managed pointing to the pod. return nil if not found.
func (jobController *WSJobController) GetEndpoint(namespace, name string) (*corev1.Endpoints, error) {
	ep := &corev1.Endpoints{}
	err := jobController.Get(context.Background(), types.NamespacedName{
		Namespace: namespace, Name: name}, ep)
	if err != nil {
		// return nil if not found
		if errors.IsNotFound(err) {
			return nil, nil
		}
		logrus.Warnf("Failed to query endpoint of %s/%s", namespace, name)
		return nil, err
	}
	return ep, nil
}

// GetPodDataIP returns multus data ip for wsjob endpoint.
func (jobController *WSJobController) GetPodDataIP(pod *corev1.Pod) ([]string, error) {
	// br goes with hostnetwork if multus disabled
	if wsapisv1.DisableMultusForBR &&
		pod.Labels[wsReplicaTypeLabel] == wsapisv1.WSReplicaTypeBroadcastReduce.Lower() {
		return nil, nil
	}
	addr := pod.Status.PodIP
	// return pod ip if for non-multus setup
	if wscommon.DisableMultus {
		if addr == "" {
			errMsg := fmt.Sprintf("pod %s ip not assigned yet, skip and retry next cycle", pod.Name)
			return nil, fmt.Errorf(errMsg)
		}
		return []string{addr}, nil
	}
	netStatusesJson, ok := pod.Annotations[netv1.NetworkStatusAnnot]
	if !ok {
		if !wscommon.PodHasCondition(pod, corev1.PodScheduled) {
			errMsg := fmt.Sprintf("pod %s not scheduled yet, skip and retry network status next cycle", pod.Name)
			return nil, fmt.Errorf(errMsg)
		}
		errMsg := fmt.Sprintf("cannot find network status for pod %s on node %s, annotations: %v, status: %v",
			pod.Name, pod.Spec.NodeName, pod.Annotations, pod.Status)
		return nil, fmt.Errorf(errMsg)
	}
	var ips []string
	var netStatuses []netv1.NetworkStatus
	if err := json.Unmarshal([]byte(netStatusesJson), &netStatuses); err != nil {
		return nil, err
	}
	for i, netStatus := range netStatuses {
		if i == 0 {
			// skip pod default network which will always be the first one
			continue
		}
		ips = append(ips, netStatus.IPs[0])
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("multus network not ready yet for pod %s on node %s, netstatus: %v, status: %v",
			pod.Name, pod.Spec.NodeName, netStatuses, pod.Status)
	}
	return ips, nil
}

// Helper to get the "failing after pod failure" condition, if it exists.
func (jobController *WSJobController) getPodFailureCondition(jobStatus *commonapisv1.JobStatus) *commonapisv1.JobCondition {
	for _, condition := range jobStatus.Conditions {
		if condition.Type == commonapisv1.JobFailing {
			return &condition
		}
	}
	return nil
}

// Helper that checks if the failure timed out, and returns the duration if it did.
func (jobController *WSJobController) failureTimedOut(logger logr.Logger, jobStatus *commonapisv1.JobStatus,
	wsjob *wsapisv1.WSJob) (bool, time.Duration) {
	failingCondition := jobController.getPodFailureCondition(jobStatus)
	if failingCondition == nil {
		return false, 0
	}
	gracePeriod := wscommon.FailingJobGracePeriod
	// Allow tests to override the grace period
	if failingGracePeriod, err := strconv.Atoi(wsjob.Annotations[wscommon.FailingJobGracePeriodLabel]); err == nil {
		logger.Info("pod failure grace period override", "gracePeriod", failingGracePeriod)
		gracePeriod = time.Duration(failingGracePeriod) * time.Second
	}
	deadline := failingCondition.LastTransitionTime.Add(gracePeriod)
	timeNow := time.Now()
	if timeNow.After(deadline) {
		return true, 0
	}
	return false, deadline.Sub(timeNow)
}

// requeueAfterPodFailure returns a requeue interval if the pod failure is within the grace period. It relies on the caller to
// fail the job if not.
func (jobController *WSJobController) requeueAfterPodFailure(
	wsjob *wsapisv1.WSJob,
	jobStatus *commonapisv1.JobStatus,
	podFailureMsg string,
) error {
	logger := jobController.Log.WithValues(wsapisv1.Singular, wsjob.Name)

	// Check if grace period has expired
	if timedOut, requeueInterval := jobController.failureTimedOut(logger, jobStatus, wsjob); !timedOut {
		logger.Info(fmt.Sprintf("job %s/%s pod failure within grace period, %s [requeue after %s]",
			wsjob.Namespace, wsjob.Name, podFailureMsg, requeueInterval.String()))
		return &commonapisv1.RequeueAfterTTL{TTL: requeueInterval}
	}

	return nil
}

package core

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	corev1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"

	apiv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

var (
	UnrecoverableErrors = []string{
		"InvalidImageName",
	}
	// Configuration errors that occur before image pull - these don't depend on previous containers
	ConfigErrors = []string{
		"CreateContainerError",
		"CreateContainerConfigError",
	}
	// Image pull errors that depend on previous container completion
	ImagePullErrors = []string{
		"ImagePullBackOff",
		"ErrImagePull",
	}
	//  TransientErrors as union of both config and image pull errors.
	TransientErrors = append([]string{}, append(ConfigErrors, ImagePullErrors...)...)
)

// InitializeReplicaStatuses initializes the ReplicaStatuses for replica.
func InitializeReplicaStatuses(jobStatus *apiv1.JobStatus, rtype apiv1.ReplicaType) {
	if jobStatus.ReplicaStatuses == nil {
		jobStatus.ReplicaStatuses = make(map[apiv1.ReplicaType]*apiv1.ReplicaStatus)
	}
	jobStatus.ReplicaStatuses[rtype] = &apiv1.ReplicaStatus{}
}

// UpdateJobReplicaStatuses updates the JobReplicaStatuses according to the pod.
func UpdateJobReplicaStatuses(client clientset.Interface, jobStatus *apiv1.JobStatus, rtype apiv1.ReplicaType, pod *corev1.Pod,
	gracePeriod time.Duration) *apiv1.RequeueAfterTTL {
	switch pod.Status.Phase {
	case corev1.PodPending:
		// Per‑container grace‑period logic
		// Deadline start defaults to pod creation, but fall back to Status.StartTime if present.
		deadlineStart := pod.Status.StartTime
		if deadlineStart == nil {
			// adding this condition check so envtest won't be going through the hanging check
			// note: this means you need to set the start time if you want to add one envtest
			if len(pod.Status.Conditions) > 0 {
				if err := failJobIfHanging(jobStatus, rtype, pod, nil,
					pod.CreationTimestamp.Time, pod.CreationTimestamp.Time, gracePeriod, false); err != nil {
					return err
				}
			}
			break
		}

		// Handle init-containers
		// Init container are run sequentially, so we can check them one by one.
		for _, initContainerStatus := range pod.Status.InitContainerStatuses {
			switch {
			case initContainerStatus.State.Waiting != nil:
				if err := failJobIfHanging(jobStatus, rtype, pod, &initContainerStatus,
					deadlineStart.Time, deadlineStart.Time, gracePeriod, false); err != nil {
					return err
				}
			case initContainerStatus.State.Running != nil:
				jobStatus.ReplicaStatuses[rtype].Init++
				if len(jobStatus.ReplicaStatuses[rtype].InitPods) < 3 {
					jobStatus.ReplicaStatuses[rtype].InitPods = append(jobStatus.ReplicaStatuses[rtype].InitPods, pod.Name)
				}
				return nil
			case initContainerStatus.State.Terminated != nil:
				deadlineStart = &initContainerStatus.State.Terminated.FinishedAt
			}
		}

		// Process main containers with error-type based timeout logic.
		//
		// For each waiting container, we determine timeout start based on error type:
		// - Config errors (CreateContainerError, CreateContainerConfigError) → timeout from pod start
		// - Image pull errors (ImagePullBackOff, ErrImagePull) → timeout from last container progress
		//
		// Example: Pod with 3 containers started at 10:00
		// - Container 0: Started 10:01, finished 10:03
		// - Container 1: Started 10:04, still running  (lastContainerStartTime = 10:04)
		// - Container 2: CreateContainerConfigError → timeout from 10:00 (pod start)
		// - Container 3: ImagePullBackOff → timeout from 10:04 (last progress)
		//
		// This ensures config errors get immediate attention while image pull errors
		// respect container startup dependencies.
		lastContainerStartTime := deadlineStart.Time // Default to pod start time
		for _, st := range pod.Status.ContainerStatuses {
			switch {
			case st.State.Waiting != nil:
				if err := failJobIfHanging(jobStatus, rtype, pod, &st,
					deadlineStart.Time, lastContainerStartTime, gracePeriod, true); err != nil {
					return err
				}
			case st.State.Running != nil:
				// If container is running, we count it as active.
				lastContainerStartTime = st.State.Running.StartedAt.Time
			case st.State.Terminated != nil:
				// If container is terminated, we update the last start time if it was running.
				lastContainerStartTime = st.State.Terminated.StartedAt.Time
			}
		}
	case corev1.PodRunning:
		jobStatus.ReplicaStatuses[rtype].Active++
		ready := true
		for _, containerStatus := range pod.Status.ContainerStatuses {
			// Kubernetes relies on application logic to exit pod if one of multiple containers died in the
			// main stage of the pod. If we did not handle explicitly, the pod would still be stuck in the running
			// state and the job would not be terminated.
			// As of rel-2.2, the only multi-container pod is worker pods (with streamer and ws container). If
			// this changed in the future, we should update following handling to be more specific to conditions.
			// In rel-2.5, we further extend the multi-container pod pattern to weight pods.
			if len(pod.Spec.Containers) > 1 &&
				containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
				msg := containerStatus.State.Terminated.Message
				if msg == "" {
					msg = fmt.Sprintf("container %s in %s had exited with code %d",
						containerStatus.Name, pod.Name, containerStatus.State.Terminated.ExitCode)
				}
				updateFailedReplicaStatus(jobStatus.ReplicaStatuses[rtype], pod, msg)
				return nil
			}
			if !containerStatus.Ready {
				ready = false
			}
		}
		if ready {
			jobStatus.ReplicaStatuses[rtype].Ready++
		}
	case corev1.PodSucceeded:
		jobStatus.ReplicaStatuses[rtype].Succeeded++
	case corev1.PodFailed:
		updateFailedReplicaStatus(jobStatus.ReplicaStatuses[rtype], pod, pod.Status.Message)
	}
	return nil
}

// fail hanging job if error not recoverable or persist after deadline
func failJobIfHanging(jobStatus *apiv1.JobStatus, rtype apiv1.ReplicaType, pod *corev1.Pod,
	containerStatus *corev1.ContainerStatus, podStartTime time.Time, lastContainerStartTime time.Time,
	gracePeriod time.Duration, checkErrorType bool) *apiv1.RequeueAfterTTL {

	if gracePeriod < 0 {
		return nil
	}

	message := ""
	if containerStatus != nil {
		message = containerStatus.State.Waiting.Reason
		if containerStatus.State.Waiting.Message != "" {
			message = containerStatus.State.Waiting.Message
		}
	} else {
		message = pod.Status.Conditions[0].Message
	}

	containerName := "no container up yet, pod just created"
	if containerStatus != nil {
		containerName = containerStatus.Name
	}

	// Determine which start time to use based on error type
	startTime := podStartTime // default for config errors and non-container failures
	if containerStatus != nil && slices.Contains(ImagePullErrors, containerStatus.State.Waiting.Reason) {
		startTime = lastContainerStartTime // use last container start time for image pull errors
	}

	log.Infof("pending pod %s(container: %s), start time: %s, message: %s", pod.Name, containerName, startTime, message)

	// fail job immediately if not recoverable
	if checkErrorType && slices.Contains(UnrecoverableErrors, containerStatus.State.Waiting.Reason) {
		message = fmt.Sprintf("pod %s container %s failed due to unrecoverable failure: %s",
			pod.Name, containerName, message)
		log.Warnf(message)
		updateFailedReplicaStatus(jobStatus.ReplicaStatuses[rtype], pod, message)
		return nil
	}

	// for test override
	podTTL, err := strconv.Atoi(pod.Labels["PENDING_POD_TTL_SECONDS"])
	if err == nil {
		log.Debugf("grace period override: %d seconds", podTTL)
		gracePeriod = time.Duration(podTTL) * time.Second
	}

	// requeue the event if deadline not reached
	timeNow := time.Now()
	deadline := startTime.Add(gracePeriod)
	if timeNow.Before(deadline) {
		requeueInterval := deadline.Add(1 * time.Second).Sub(timeNow)
		if startTime.Before(time.Now().Add(-30 * time.Second)) {
			log.Infof("pod %s pending in grace period, requeueing after %s", pod.Name, requeueInterval)
		}
		return &apiv1.RequeueAfterTTL{TTL: requeueInterval}
	}

	// fail job if deadline expired
	if !checkErrorType || checkErrorType && slices.Contains(TransientErrors, containerStatus.State.Waiting.Reason) {
		message = fmt.Sprintf("pod %s (container %s) failed due to stuck at pending for more than %s: %s",
			pod.Name, containerName, gracePeriod.String(), message)
		log.Warnf(message)
		logPodVolumeConfig(pod)
		updateFailedReplicaStatus(jobStatus.ReplicaStatuses[rtype], pod, message)
		return nil
	}

	// could be other reasons like image pulling which is expected
	log.Warnf("pod %s stuck at pending longer than grace period, message: %s", pod.Name, message)
	return nil
}

func updateFailedReplicaStatus(replicaStatus *apiv1.ReplicaStatus, pod *corev1.Pod, message string) {
	log.Warnf("pod %s captured as failed, message: %s", pod.Name, message)
	defer func() {
		replicaStatus.Failed++
	}()

	var failedPodStatuses []apiv1.FailedPodStatus
	for _, container := range pod.Status.InitContainerStatuses {
		newStatus := createFailedPodStatusFromContainer(&container, pod.Name, pod.Spec.NodeName, message)
		if newStatus != nil {
			failedPodStatuses = append(failedPodStatuses, *newStatus)
		}
	}
	for _, container := range pod.Status.ContainerStatuses {
		newStatus := createFailedPodStatusFromContainer(&container, pod.Name, pod.Spec.NodeName, message)
		if newStatus != nil {
			failedPodStatuses = append(failedPodStatuses, *newStatus)
		}
	}

	// this can happen if pod is evicted and graceful exit without error
	// or in e2e test where containerStatuses is empty
	// so add a generic failure message if no container failure found
	if len(failedPodStatuses) == 0 {
		// add a generic failure message if no container failure found
		failedPodStatuses = append(failedPodStatuses, apiv1.FailedPodStatus{
			Name:     pod.Name,
			NodeName: pod.Spec.NodeName,
			Message:  message,
		})
	}

	// Append all new statuses, sort them, and then trim to 3 if necessary.
	replicaStatus.FailedPodStatuses = append(replicaStatus.FailedPodStatuses, failedPodStatuses...)

	sort.Slice(replicaStatus.FailedPodStatuses, func(i, j int) bool {
		if replicaStatus.FailedPodStatuses[i].FinishedAt.IsZero() {
			return true
		}
		if replicaStatus.FailedPodStatuses[j].FinishedAt.IsZero() {
			return false
		}
		return replicaStatus.FailedPodStatuses[i].FinishedAt.Time.Before(
			replicaStatus.FailedPodStatuses[j].FinishedAt.Time)
	})
	// keep 3 at most to avoid wsjob spec too big which will be rejected with "etcdserver: request is too large"
	if len(replicaStatus.FailedPodStatuses) > 3 {
		replicaStatus.FailedPodStatuses = replicaStatus.FailedPodStatuses[:3]
	}
}

func createFailedPodStatusFromContainer(container *corev1.ContainerStatus, podName, nodeName, message string) *apiv1.FailedPodStatus {
	var status *apiv1.FailedPodStatus
	if container.State.Terminated != nil && container.State.Terminated.ExitCode != 0 {
		status = &apiv1.FailedPodStatus{
			Container: container.Name,
			ExitCode:  container.State.Terminated.ExitCode,
			Name:      podName,
			NodeName:  nodeName,
			Message:   fmt.Sprintf("%s %s", container.State.Terminated.Reason, message),
		}
		if !container.State.Terminated.FinishedAt.IsZero() {
			status.FinishedAt = &container.State.Terminated.FinishedAt
		}
	}
	return status
}

func logPodVolumeConfig(pod *corev1.Pod) {
	type volumeConfig struct {
		name     string
		nfs      string
		hostPath string
	}
	type containerConfig struct {
		name       string
		mounts     []corev1.VolumeMount
		workingDir string
	}
	type podConfig struct {
		name            string
		securityContext string
		volumes         []volumeConfig
		initContainers  []containerConfig
		containers      []containerConfig
	}
	containerMapper := func(c corev1.Container) containerConfig {
		return containerConfig{
			name:       c.Name,
			mounts:     c.VolumeMounts,
			workingDir: c.WorkingDir,
		}
	}
	var volumes []volumeConfig
	for _, v := range pod.Spec.Volumes {
		nfs := ""
		if v.NFS != nil {
			nfs = v.NFS.String()
		}
		hostPath := ""
		if v.HostPath != nil {
			hostPath = v.HostPath.String()
		}
		volumes = append(volumes, volumeConfig{
			name:     v.Name,
			nfs:      nfs,
			hostPath: hostPath,
		})
	}
	var initContainers []containerConfig
	for _, c := range pod.Spec.InitContainers {
		initContainers = append(initContainers, containerMapper(c))
	}
	var containers []containerConfig
	for _, c := range pod.Spec.Containers {
		containers = append(containers, containerMapper(c))
	}
	config := podConfig{
		name:            pod.Name,
		securityContext: pod.Spec.SecurityContext.String(),
		volumes:         volumes,
		initContainers:  initContainers,
		containers:      containers,
	}
	log.Warnf("Pod volume config: %+v", config)
}

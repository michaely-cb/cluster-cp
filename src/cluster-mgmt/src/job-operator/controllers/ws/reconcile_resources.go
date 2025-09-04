package ws

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/go-logr/logr"
	apiv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/kubeflow/common/pkg/core"
	commonutil "github.com/kubeflow/common/pkg/util"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubectl/pkg/util/podutils"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource_config"
)

// buildLinearMemoryRangeAnnotations extracts per‐task‐type global ranges and replica coefficients
// from the compressed ClusterDetails ConfigMap.
//
// Validation performed:
//   - Ensures the configmap contains the expected compressed data.
//   - Decompresses and parses the ClusterDetails proto.
//   - For each task, validates that the global LinearMemoryRange object has all required fields set (intercept, coefficient, minX, maxX).
//   - Checks that intercept and minX are >= 0, coefficient is >= -1, and maxX is -1 or >= minX.
//   - For each pod, ensures that if the global coefficient is -1, an override is present and non-negative.
//   - Ensures all coefficients (global or override) used are >= 0.
//   - Returns errors if any validation fails.
func buildLinearMemoryRangeAnnotations(cm *corev1.ConfigMap) (map[string]string, error) {
	raw, ok := cm.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName]
	if !ok {
		return nil, nil
	}
	data, err := wscommon.Gunzip(raw)
	if err != nil {
		return nil, err
	}
	cd := &commonpb.ClusterDetails{}
	// options with allow partial unknown fields
	opts := protojson.UnmarshalOptions{
		DiscardUnknown: true,
		AllowPartial:   true,
	}
	if err := opts.Unmarshal(data, cd); err != nil {
		return nil, err
	}

	annos := make(map[string]string, len(cd.GetTasks()))
	for _, ti := range cd.GetTasks() {
		lm := ti.GetResourceInfo().GetLinearMemoryRange()
		if lm == nil {
			continue
		}
		// Validation: Ensure presence of every field in the global LMR object
		if lm.Intercept == nil || lm.Coefficient == nil || lm.MinX == nil || lm.MaxX == nil {
			return nil, fmt.Errorf("task %v: global LinearMemoryRange must have intercept, coefficient, minX and maxX all set", ti.TaskType)
		}

		// now pull values
		intercept := lm.GetIntercept()
		globalCoeff := lm.GetCoefficient()
		minX := lm.GetMinX()
		maxX := lm.GetMaxX()

		// Validation: Check value ranges
		if intercept < 0 {
			return nil, fmt.Errorf("task %v: intercept must be ≥0, got %d", ti.TaskType, intercept)
		}
		if globalCoeff < -1 {
			return nil, fmt.Errorf("task %v: coefficient must be ≥-1, got %d", ti.TaskType, globalCoeff)
		}
		if minX < 0 {
			return nil, fmt.Errorf("task %v: minX must be ≥0, got %d", ti.TaskType, minX)
		}
		if !(maxX == -1 || maxX >= minX) {
			return nil, fmt.Errorf("task %v: maxX must be -1 or ≥minX (%d), got %d", ti.TaskType, minX, maxX)
		}

		// map TaskType → ReplicaType
		replicaType, known := wsapisv1.TaskTypeClusterDetailsMapping[ti.TaskType]
		if !known {
			continue
		}

		// emit the global annotation
		annos[wscommon.LinearMemoryRangeGlobalAnnotPrefix+string(replicaType)] =
			fmt.Sprintf("%d,%d,%d,%d", intercept, globalCoeff, minX, maxX)

		// replica coefficient overrides (always emit if there are pods)
		tm := ti.GetTaskMap()
		if len(tm) == 0 {
			continue
		}
		coefs := make([]string, len(tm))
		for i, entry := range tm {
			ov := entry.GetLinearMemoryRangeOverride()
			var c int64
			switch {
			// Case 1: If an override exists for this pod, use its coefficient.
			case ov != nil:
				// If an override exists for this pod:
				// - If the global coefficient is -1, the override coefficient must be set.
				if globalCoeff == -1 && ov.Coefficient == nil {
					return nil, fmt.Errorf(
						"task %v: pod index %d: override coefficient must be set",
						ti.TaskType, i,
					)
				}
				if ov.Coefficient == nil {
					// If the override coefficient is not set, use the global coefficient.
					c = globalCoeff
				} else {
					// Otherwise, use the override coefficient.
					c = ov.GetCoefficient()
				}
			// Case 2: If the global coefficient is -1 (invalid), but no pod override is present, error out.
			case globalCoeff == -1:
				return nil, fmt.Errorf(
					"task %v: global coefficient is -1, override required for pod index %d",
					ti.TaskType, i,
				)
			// Case 3: Otherwise, use the global coefficient.
			default:
				c = globalCoeff
			}
			// The final coefficient chosen (either global or override) must be non-negative.
			// Note: The case where the global coefficient is -1 is handled above,
			// requiring an override to be present for that pod.
			if c < 0 {
				return nil, fmt.Errorf(
					"task %v: coefficient for pod index %d must be ≥0, got %d",
					ti.TaskType, i, c,
				)
			}
			coefs[i] = strconv.FormatInt(c, 10)
		}
		annos[wscommon.LinearMemoryRangeCoefficientsAnnotPrefix+string(replicaType)] =
			strings.Join(coefs, ",")
	}

	return annos, nil
}

func buildAnnotationsFromClusterDetails(cm *corev1.ConfigMap) (map[string]string, error) {
	ann := make(map[string]string)

	// 1) linear memory ranges
	lmAnnos, err := buildLinearMemoryRangeAnnotations(cm)
	if err != nil {
		return nil, err
	}
	// Base64‐encode each comma‐list before publishing as an annotation
	for k, v := range lmAnnos {
		ann[k] = base64.StdEncoding.EncodeToString([]byte(v))
	}

	// validate linear memory ranges
	_, err = wscommon.ParseLinearMemoryConfigs(ann)
	if err != nil {
		return nil, err
	}

	// 2) AX-domains (if enabled)
	if wsapisv1.UseAxScheduling {
		if val, ok := cm.BinaryData[wsapisv1.ClusterDetailsActToCsxDomains]; ok {
			key := wsapisv1.AllRuntimeTargetedDomainsKey
			enc := base64.StdEncoding.EncodeToString(val)
			if _, err := wscommon.ParseTargetedDomainsAnnot(enc); err != nil {
				return nil, err
			}
			ann[key] = enc
		}
		if val, ok := cm.BinaryData[wsapisv1.ClusterDetailsKvssToCsxDomains]; ok {
			key := wsapisv1.AllPromptCachingTargetedDomainsKey
			enc := base64.StdEncoding.EncodeToString(val)
			if _, err := wscommon.ParseTargetedDomainsAnnot(enc); err != nil {
				return nil, err
			}
			ann[key] = enc
		}
	}

	// 3) SW driver domains (if enabled)
	if wsapisv1.UseIxScheduling {
		if val, ok := cm.BinaryData[wsapisv1.ClusterDetailsSWDriverToCsxDomains]; ok {
			key := wsapisv1.AllSwdrTargetedDomainsKey
			enc := base64.StdEncoding.EncodeToString(val)
			if _, err := wscommon.ParseTargetedDomainsAnnot(enc); err != nil {
				return nil, err
			}
			ann[key] = enc
		}
	}

	return ann, nil
}

type jobResources struct {
	// name is the job/lock name
	name string
	// compile/execute/prioritized-compile
	lockType string
	// prioritized-compile parent job name
	parentJobName string

	// coordinator is the name of the node that is elected for the coordinator.
	coordinator string

	// systems allocated for a job. May not be locked as in the case of compile.
	systems []string

	// nodeGroups allocated for a job. The first entry is considered the primary entry and may have selected special
	// properties.
	nodeGroups []string

	// replicaToNodeMap is a map of replica type to node names assigned for each replica at index i
	// replicaToNodeMap[rType][idx] == nodeName
	replicaToNodeMap map[string]map[int]string

	// replicaToResourceMap is a map of replica type to resource assigned for each replica at index i
	// replicaToResourceMap[rType][idx][rsType] == rsVal
	// This map is later used in wsjob_controller.go for pod-level resource request/limit assignments.
	// On a related note, workers currently do not have this map assigned, given we have multiple
	// containers for worker pods and downstream logic is not capable of redistributing memory among
	// containers.
	replicaToResourceMap map[string]map[int]map[string]int

	// replicaToNadMap is a map of replica type to net-attach-def assigned for each replica at index i
	// replicaToNadMap[rType][idx] == {nadName1,nad2,...}
	replicaToNadMap map[string]map[int][]string

	// replicaToBrNodeIdMap pod replica id(pod name postfix) mapping to br node id(id in br config).
	// pod replica id/node id should be same for full mode
	// but can diverge in partial mode, e.g. br pod-0 => br node-1 in case of skip group 0
	replicaToBrNodeIdMap []int

	// replicaToAnnoMap is a map of replica type to grant anno
	replicaToAnnoMap map[string]map[string]string

	// replicaToEnvVarMap is a map of replica type to env var assigned for each replica at index i
	replicaToEnvVarMap map[string]map[int]map[string]string

	// replicaToWioTargetMap is a map of replica type to wio target properties for each replica at index i
	replicaToWioTargetMap map[string]map[int]map[string]string

	// resGranted is map of resources granted to the job.
	resGranted map[string]bool

	// priority is priority of the lock
	priority int

	// nvmfGrants captures the shard-specific nvmf grant in the form of "/dev/datanvme101:2000-8000"
	nvmfGrants []string
}

type NicAssignment struct {
	replicaType string
	replicaIdx  string
	node        string
	device      string
}

func newJobResources(cfg *wscommon.ClusterConfig, name types.NamespacedName, lock *rlv1.ResourceLock) jobResources {
	var coordinator string
	for _, rg := range lock.Status.ResourceGrants {
		if rg.Name == rlv1.CoordinatorRequestName && len(rg.Resources) > 0 {
			coordinator = rg.Resources[0].Name
			break
		}
	}

	jr := hydrateJobResources(cfg, lock)
	for _, grant := range lock.Status.NodeGroupGrants {
		jr.nodeGroups = append(jr.nodeGroups, grant.NodeGroups...)
	}
	jr.coordinator = coordinator
	jr.systems = wscommon.Map(func(g rlv1.SystemGrant) string { return g.Name }, lock.Status.SystemGrants)
	jr.priority = lock.GetPriority()
	wscommon.JobResourcesTracker[name] = jr.resGranted

	return jr
}

func (r *WSJobReconciler) updateAnnotations(j jobResources, wsjob *wsapisv1.WSJob, lock *rlv1.ResourceLock) {
	wsjob.Annotations = wscommon.EnsureMap(wsjob.Annotations)
	wsjob.Annotations[CoordinatorNodeNameAnnotationKey] = j.coordinator
	wsjob.Annotations[SystemNameAnnotationKey] = strings.Join(j.systems, ",")
	if len(j.nodeGroups) > 0 {
		wsjob.Annotations[NodeGroupAnnotationKey] = strings.Join(j.nodeGroups, ",")
	}

	r.semanticVersionCheckAnnotation(wsjob)
	r.addBRV2AffinityAnnotation(wsjob, j)
	r.addPartialSystemsAnnotation(wsjob, j)

	if !wsapisv1.UseAxScheduling {
		r.addSecondaryDataNicAnnotation(wsjob, j)
	} else {
		r.addWioV2AffinityAnnotation(wsjob, lock, j)
	}
	r.addNvmfGrantAnnotations(wsjob, j)
}

// reconcileResources returns a list of systems and nodegroups granted to the job, whether reconcile loop should
// proceed, and a potential error if something went wrong while reconciling the state of the grants.
func (r *WSJobReconciler) reconcileResources(
	ctx context.Context, wsjob *wsapisv1.WSJob) (jobResources, bool, error) {

	if e, ok := r.JobResourceCache.Get(wsjob.NamespacedName()); ok {
		return e.(jobResources), true, nil
	}

	jr, proceed, err := r.acquireResources(ctx, wsjob)
	if err != nil {
		return jr, false, err
	}
	if !proceed {
		return jr, false, nil
	}

	// Export info metric associating the job/pod/node/nic.
	for _, assignment := range r.getAssignedNics(&jr) {
		pod := core.GenGeneralName(wsjob.Name, assignment.replicaType, assignment.replicaIdx)
		deviceIp := r.Cfg.NodeMap[assignment.node].GetNicAddress(assignment.device)
		wscommon.JobNicAdded(wsjob.Namespace, wsjob.Name, pod, assignment.node, assignment.device, deviceIp)
	}

	r.EmitJobNetworkTopologyMetrics(&jr, wsjob.Name, wsjob.Namespace)
	r.JobResourceCache.Add(wsjob.NamespacedName(), jr)

	return jr, proceed, err
}

// acquireResources obtains a ResourceLock on the requested systems and nodes prior to returning the jobResources.
// The boolean return value indicates whether the resources are successfully locked and the job should proceed.
func (r *WSJobReconciler) acquireResources(
	ctx context.Context, wsjob *wsapisv1.WSJob) (jobResources, bool, error) {

	jr := jobResources{}
	lockCondState := getLockCondState(wsjob)
	if lockCondState == lockCondStateNew {
		if err := r.createLock(ctx, wsjob); err != nil {
			return jr, false, err
		}
		return jr, false, r.updateAwaitLockJobStatus(ctx, wsjob, wsapisv1.AwaitLock, "job queueing to be scheduled")
	}

	lock, err := r.getLock(ctx, wsjob.NamespacedName())
	if err != nil {
		return jr, false, err
	}
	if lock.IsGranted() {
		jr = newJobResources(r.Cfg, wsjob.NamespacedName(), lock)
		// new grant, update job's annotations to store system and nodegroup names
		if lockCondState == lockCondStateAwait {
			r.Log.Info("lock granted, start job bring up", "job", wsjob.Name)
			// best effort check on system version with redundancy pool
			r.updateAnnotations(jr, wsjob, lock)
			// update preferred address.
			if _, ok := wsjob.Annotations[wsapisv1.WsJobCrdAddrAnnoKey]; ok {
				crdNode := r.Cfg.NodeMap[jr.coordinator]
				if crdNode != nil && len(crdNode.NICs) > 0 {
					var nginxPodExists bool
					pods := &corev1.PodList{}
					err = r.ApiReader.List(context.Background(), pods, client.InNamespace(wscommon.NginxNameSpace),
						client.MatchingLabels{wscommon.NginxPodLabelKey: wscommon.NginxPodLabelVal})
					if err != nil {
						r.Log.Info("failed to list nginx pods", "err", err)
					} else {
						for _, po := range pods.Items {
							if (po.Status.HostIP == crdNode.HostIP || po.Spec.NodeName == crdNode.Name) &&
								podutils.IsPodReady(&po) {
								nginxPodExists = true
								break
							}
						}
					}
					// this check probably can be removed after client side all rolled out with curl check
					if nginxPodExists {
						wsjob.Annotations[wsapisv1.WsJobCrdAddrAnnoKey] = crdNode.NICs[0].Addr
					} else {
						r.Log.Info("no healthy/ready nginx pod on assigned crd node", "crd", crdNode.Name)
						delete(wsjob.Annotations, wsapisv1.WsJobCrdAddrAnnoKey)
					}
				} else if len(r.Cfg.GetCoordNodes()) > 0 {
					// configuration sanityCheck should have panic'd
					msg := fmt.Sprintf("invalid config: management node %s had no NIC but must have a NIC configured in multi-mgmt mode", jr.coordinator)
					r.Log.Error(nil, msg, "node", jr.coordinator)
					return jr, false, &CriticalError{msg}
				}
			}
			if err = r.Update(ctx, wsjob); err != nil {
				return jr, false, err
			}
			return jr, false, r.updateAwaitLockJobStatus(ctx, wsjob, wsapisv1.LockGranted, "job scheduled and waiting to be initialized")
		}
		// already granted
		return jr, true, nil
	} else if lock.HadFailed() {
		// lock failed
		return jr, false, r.updateAwaitLockJobStatus(ctx, wsjob, commonv1.JobFailed, string(lock.Status.Message))
	} else {
		// lock pending
		return jr, false, nil
	}
}

func (r *WSJobReconciler) updateAwaitLockJobStatus(_ context.Context, wsjob *wsapisv1.WSJob, condType commonv1.JobConditionType, message string) error {
	switch condType {
	case wsapisv1.LockGranted:
		if !commonutil.HasCondition(wsjob.Status, commonv1.JobQueueing) {
			return fmt.Errorf("stale wsjob not queueing, wait for Cache sync")
		}
		err := commonutil.UpdateJobConditions(&wsjob.Status, commonv1.JobScheduled, wsjobScheduledReason, message)
		if err != nil {
			return err
		}
	case wsapisv1.AwaitLock:
		err := commonutil.UpdateJobConditions(&wsjob.Status, commonv1.JobQueueing, wsjobQueueingReason, message)
		if err != nil {
			return err
		}
	case commonv1.JobFailed:
		err := commonutil.UpdateJobConditions(&wsjob.Status, commonv1.JobFailed, wsjobScheduleFailedReason, message)
		if err != nil {
			return err
		}
	}
	return r.UpdateJobStatusInApiServer(wsjob, &wsjob.Status)
}

func (r *WSJobReconciler) releaseResources(ctx context.Context, job *wsapisv1.WSJob) error {
	logger := log.FromContext(ctx)
	// release reserving lock if necessary
	if job.UnReserved() {
		err := r.deleteLockIfExist(ctx, job.Namespace, wscommon.ReservingLockName(job.Name))
		if err != nil {
			return err
		}
	}

	// skip check since job will be deleted and lock will be cleaned due to owner ref GC
	// we expect pods will be cleaned up in short time(<5m)
	if job.CreationTimestamp.IsZero() || job.DeletionTimestamp != nil {
		logger.Info("skip resource lock clean up for already deleted job")
		r.JobResourceCache.Remove(job.NamespacedName())
		delete(wscommon.JobResourcesTracker, job.NamespacedName())
		return nil
	}

	// release current lock if necessary
	lock := &rlv1.ResourceLock{}
	err := r.Client.Get(ctx, job.NamespacedName(), lock)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		} else {
			return fmt.Errorf("unable to get lock for cleanup: %w", err)
		}
	}

	// wait till pods cleanup before releasing
	if pods, err := r.getRunningPods(job); len(pods) > 0 {
		logger.Info("Detected pods still running when trying to release resource lock",
			"job", job.NamespacedName(), "runningPods", len(pods))
		// defer cleanup till pods are terminated to ensure cleanup properly like ip/netns release
		// only exception is when node is in unhealthy/unknown state causing pod stuck in terminating/running
		// in this case, ignore pods stuck on unhealthy nodes to avoid blocking resource release
		for _, pod := range pods {
			_, ok := r.Cfg.UnhealthyNodes[pod.Spec.NodeName]
			if !ok {
				// if pod running and node healthy, defer cleanup
				// return nil to unblock later kubeflow cleanup and rely on requeue with DefaultLockCleanupGracePeriod
				logger.Info("defer deleting resource-lock due to running pod",
					"job", job.NamespacedName(), "pod", pod.Name, "node", pod.Spec.NodeName)
				return nil
			} else {
				logger.Info("skip resource check for pods stuck on unhealthy node",
					"job", job.NamespacedName(), "pod", pod.Name, "node", pod.Spec.NodeName)
			}
		}
	} else if err != nil {
		return fmt.Errorf("unable to get running pod count: %w", err)
	}

	logger.Info("running pods check done, start lock cleanup", "job", job.Name)
	err = r.Client.Delete(ctx, lock)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("unable to cleanup lock: %w", err)
	}
	r.JobResourceCache.Remove(job.NamespacedName())
	delete(wscommon.JobResourcesTracker, job.NamespacedName())
	return nil
}

func (r *WSJobReconciler) deleteLockIfExist(ctx context.Context, namespace, name string) error {
	lock := &rlv1.ResourceLock{}
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, lock)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	r.Log.Info("release lock", "name", lock.NamespacedName())
	return r.Delete(ctx, lock)
}

func (r *WSJobReconciler) getLock(ctx context.Context, namespacedName client.ObjectKey) (*rlv1.ResourceLock, error) {
	lock := &rlv1.ResourceLock{}
	return lock, r.Client.Get(ctx, namespacedName, lock)
}

func (r *WSJobReconciler) getRunningPods(wsjob *wsapisv1.WSJob) ([]*corev1.Pod, error) {
	pods, err := r.Controller.GetPodsForJob(wsjob)
	if err != nil {
		return nil, err
	}
	var res []*corev1.Pod
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			res = append(res, pod)
		}
	}
	return res, nil
}

// createLock creates the lock for the given request. This is idempotent - if the lock already exists, then it returns
// the existing lock.
func (r *WSJobReconciler) createLock(ctx context.Context, wsjob *wsapisv1.WSJob) error {
	logger := log.FromContext(ctx)

	reqs := r.createReqs(wsjob)

	var ngReqs []rlv1.GroupResourceRequest
	if !wsjob.IsCompile() && !wscommon.DisableNodeGroupScheduling {
		ngReqs = r.createNodegroupReqs(wsjob)
	}

	var rqsUB []rlv1.ResourceUpperBound
	if len(ngReqs) > 0 {
		for rt, _ := range wsjob.Spec.WSReplicaSpecs {
			ub := r.getSubresourceUpperBound(wsjob, rt)
			if ub != nil {
				rqsUB = append(rqsUB, rlv1.ResourceUpperBound{Name: string(rt), Subresources: ub})
			}
		}
	}

	var queueName string
	if wsjob.IsCompile() {
		queueName = rlv1.CompileQueueName
		if wsjob.ParentJobName() != "" {
			queueName = rlv1.PrioritizedCompileQueueName
		}
	} else {
		queueName = rlv1.ExecuteQueueName
		if wsjob.ParentJobName() != "" {
			queueName = rlv1.PrioritizedExecuteQueueName
		}
	}

	annotations := map[string]string{}
	if wsjob.CanServerGenerateConfig() {
		clusterDetailsConfigMap, _, err := r.getClusterDetailsConfig(wsjob)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to get cluster details configmap")
				return err
			}
			if err = waitForServerGeneratedClusterDetails(logger, wsjob); err != nil {
				return err
			}
		} else {
			// If the configmap is not nil, set all the annotations required for the job.
			cdAnnos, err := buildAnnotationsFromClusterDetails(clusterDetailsConfigMap)
			if err != nil {
				return CriticalErrorf(err.Error())
			}
			maps.Copy(annotations, cdAnnos)
		}
	}

	if r.IsInferenceJobSharingEnabledForSession(wsjob) {
		annotations[wsapisv1.InferenceJobSharingAnnotKey] = "true"
	}

	// TODO: remove once SwDriver is scheduled on IX/SX
	if r.IsSwDriverMutualExclusionEnabledForSession(wsjob) {
		annotations[wsapisv1.SwDriverMutualExclusionAnnotKey] = "true"
	}

	lock := &rlv1.ResourceLock{
		ObjectMeta: ctrl.ObjectMeta{
			Name:        wsjob.Name,
			Namespace:   wsjob.Namespace,
			Labels:      map[string]string{apiv1.JobNameLabel: wsjob.Name},
			Annotations: annotations,
		},
		Spec: rlv1.ResourceLockSpec{
			QueueName:                       queueName,
			ResourceRequests:                reqs,
			GroupResourceRequests:           ngReqs,
			GroupResourceRequestUpperBounds: rqsUB,
			Priority:                        wsjob.GetPriorityPointer(),
		},
	}
	// append labels/annotation for some lock runtime config check
	// e.g. lock may have reservation
	maps.Copy(lock.Labels, wsjob.Labels)
	maps.Copy(lock.Annotations, wsjob.Annotations)
	// extra requirements from session/job
	// todo: delay session prop to scheduling time in case of session prop update
	r.applySessionProperty(lock)
	if err := r.applySchedulerHints(wsjob, lock); err != nil {
		return err
	}

	if err := controllerutil.SetControllerReference(wsjob, lock, r.Scheme); err != nil {
		return fmt.Errorf("set lock controller reference error: %w", err)
	}

	if err := r.Client.Create(ctx, lock); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("lock creation error: %w", err)
	}
	return nil
}

// create requests for individual sys/nodes
func (r *WSJobReconciler) createReqs(wsjob *wsapisv1.WSJob) []rlv1.ResourceRequest {
	var rv []rlv1.ResourceRequest

	if wsjob.Spec.NumWafers > 0 {
		port := resource.DefaultSystemPortRequest
		if wsjob.EnablePartialSystems() {
			port = 1
		}
		sysRes := resource.NewSystemResourceRequest(port)
		if wsjob.IsCompile() {
			// Compiles don't actually need the systems locked, so we don't acquire their subresources. Having the
			// system selected in the lock allows reuse of the property-selector logic and health check integration.
			sysRes[resource.SystemSubresource] = 0
			sysRes[resource.SystemPortSubresource] = 0
		}
		req := rlv1.ResourceRequest{
			Name:         rlv1.SystemsRequestName,
			Type:         rlv1.SystemResourceType,
			Count:        int(wsjob.Spec.NumWafers),
			Properties:   map[string][]string{},
			Annotations:  map[string]string{},
			Subresources: sysRes,
		}
		rv = append(rv, req)
	}

	if crd, crdExists := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator]; crdExists {
		annotations := map[string]string{}
		if wsjob.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] != "" {
			annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey] = wsjob.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey]
		}
		if wsjob.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] != "" {
			annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey] = wsjob.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey]
		}
		rv = append(rv, rlv1.ResourceRequest{
			Name:         string(wsapisv1.WSReplicaTypeCoordinator),
			Type:         rlv1.NodeResourceType,
			Count:        int(*crd.Replicas),
			Properties:   wscommon.RoleCoordinator.AsMatcherProperty(),
			Subresources: getSubresources(crd),
			Annotations:  annotations,
		})
	}

	return rv
}

// createNodegroupReqs creates the nodegroup requests for the given wsjob, including cpu/mem requests.
// This function assigns all weight servers to the first nodegroup matcher. It evenly distributes the remaining
// replica types servers across all the nodegroups, where there is a nodegroup for each system.
func (r *WSJobReconciler) createNodegroupReqs(wsjob *wsapisv1.WSJob) []rlv1.GroupResourceRequest {
	var grrq []rlv1.GroupResourceRequest
	primary, secondary, wgtCmd := allocateReplicasToGroups(wsjob.Spec.WSReplicaSpecs, int(wsjob.Spec.NumWafers), wsjob.UseAxOnlyForRuntime())
	nodeRoleMap := getJobNodeRoleSpecialization(wsjob)
	inferenceJobSharingEnabled := r.IsInferenceJobSharingEnabledForSession(wsjob)
	if len(primary) > 0 {
		primaryNg := rlv1.GroupResourceRequest{
			Name:             rlv1.PrimaryNgPrefix,
			Count:            1,
			ResourceRequests: r.resourceRqFromGroupAllotment(nodeRoleMap, wsjob.Spec.WSReplicaSpecs, primary, map[string]string{}),
		}
		if len(primaryNg.ResourceRequests) > 0 {
			grrq = append(grrq, primaryNg)
		}
	}

	if len(secondary) > 0 {
		// This assumes that every grouped resourceGroup after the first one is identical. This will usually be the case
		// since the secondary group matchers are usually just an activation, chief, and worker instance.
		secondaryNg := rlv1.GroupResourceRequest{
			Name:             rlv1.SecondaryNgName,
			Count:            int(wsjob.Spec.NumWafers) - 1,
			ResourceRequests: r.resourceRqFromGroupAllotment(nodeRoleMap, wsjob.Spec.WSReplicaSpecs, secondary, map[string]string{}),
		}
		if len(secondaryNg.ResourceRequests) > 0 {
			grrq = append(grrq, secondaryNg)
		}
	}

	rrAnnots := map[string]string{}
	if inferenceJobSharingEnabled {
		rrAnnots[wsapisv1.InferenceJobSharingAnnotKey] = strconv.FormatBool(inferenceJobSharingEnabled)
	}

	if wsapisv1.UseIxScheduling {
		if swdrSpec, swdrExists := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeSWDriver]; swdrExists {
			grrq = append(grrq, rlv1.GroupResourceRequest{
				Name:  rlv1.SWDriverRequestName,
				Count: 1,
				ResourceRequests: r.resourceRqFromGroupAllotment(
					nodeRoleMap,
					wsjob.Spec.WSReplicaSpecs,
					// Intended to set the replica count to 1 as further hydration happens in lock reconciler
					map[commonv1.ReplicaType]int{wsapisv1.WSReplicaTypeSWDriver: int(*swdrSpec.Replicas)},
					rrAnnots,
				),
			})
		}
	}

	if wsapisv1.UseAxScheduling {
		if cheSpec, chfExists := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeChief]; chfExists {
			grrq = append(grrq, rlv1.GroupResourceRequest{
				Name:  rlv1.ChiefRequestName,
				Count: 1,
				ResourceRequests: r.resourceRqFromGroupAllotment(
					nodeRoleMap,
					wsjob.Spec.WSReplicaSpecs,
					// Note that this is only a base group request which will be further split when hydration
					// happens in the lock reconciler
					map[commonv1.ReplicaType]int{wsapisv1.WSReplicaTypeChief: int(*cheSpec.Replicas)},
					rrAnnots,
				),
			})
		}
		if actSpec, actExists := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeActivation]; actExists {
			grrq = append(grrq, rlv1.GroupResourceRequest{
				Name:  rlv1.ActivationRequestName,
				Count: 1,
				ResourceRequests: r.resourceRqFromGroupAllotment(
					nodeRoleMap,
					wsjob.Spec.WSReplicaSpecs,
					// Intended to set the replica count to 1 as further hydration happens in lock reconciler
					// Note that this is only a base group request which will be further split when hydration
					// happens in the lock reconciler
					map[commonv1.ReplicaType]int{wsapisv1.WSReplicaTypeActivation: int(*actSpec.Replicas)},
					rrAnnots,
				),
			})
		}
		if kvssSpec, kvssExists := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeKVStorageServer]; kvssExists {
			grrq = append(grrq, rlv1.GroupResourceRequest{
				Name:  rlv1.KVStorageServerRequestName,
				Count: 1,
				ResourceRequests: r.resourceRqFromGroupAllotment(
					nodeRoleMap,
					wsjob.Spec.WSReplicaSpecs,
					// Intended to set the replica count to 1 as further hydration happens in lock reconciler
					// Note that this is only a base group request which will be further split when hydration
					// happens in the lock reconciler
					map[commonv1.ReplicaType]int{wsapisv1.WSReplicaTypeKVStorageServer: int(*kvssSpec.Replicas)},
					rrAnnots,
				),
			})
		}
		if len(wgtCmd) > 0 {
			grrq = append(grrq, rlv1.GroupResourceRequest{
				Name:  rlv1.WgtCmdGroupPrefix,
				Count: 1,
				ResourceRequests: r.resourceRqFromGroupAllotment(
					nodeRoleMap,
					wsjob.Spec.WSReplicaSpecs,
					wgtCmd,
					map[string]string{},
				),
			})
		}
	}

	return grrq
}

// resourceRqFromGroupAllotment creates a list of resource requests from the given replica specs (which define
// subresource requirements) and the allotment of replica instances to that particular group.
func (r *WSJobReconciler) resourceRqFromGroupAllotment(
	nodeRoleMap map[commonv1.ReplicaType]wscommon.NodeRole,
	specs map[commonv1.ReplicaType]*commonv1.ReplicaSpec,
	allotment map[commonv1.ReplicaType]int,
	annotations map[string]string) []rlv1.ResourceRequest {

	var rqs []rlv1.ResourceRequest
	for rt, count := range allotment {
		if count < 1 {
			continue
		}

		// props selects the node specialization for the given replica type e.g. weight => memory
		var props map[string][]string
		if nodeType, ok := nodeRoleMap[rt]; ok {
			props = nodeType.AsMatcherProperty()
		} else {
			props = map[string][]string{}
		}

		rqs = append(rqs, rlv1.ResourceRequest{
			Name:         string(rt),
			Type:         rlv1.NodeResourceType,
			Count:        count,
			Properties:   props,
			Subresources: getSubresources(specs[rt]),
			Annotations:  annotations,
		})
	}

	return rqs
}

func (r *WSJobReconciler) getSubresourceUpperBound(wsjob *wsapisv1.WSJob, rt commonv1.ReplicaType) map[string]int {
	annotations := wsjob.Spec.WSReplicaSpecs[rt].Template.ObjectMeta.Annotations
	if annotations == nil || annotations[wsapisv1.PodMemoryUpperBoundKey] == "" {
		return nil
	}
	srVal, _ := strconv.ParseInt(annotations[wsapisv1.PodMemoryUpperBoundKey], 10, 64)
	res := map[string]int{resource.MemSubresource: int(srVal >> 20)} // MiB

	maxVal, _ := strconv.ParseInt(annotations[wsapisv1.PodMemoryMaxLimitKey], 10, 64)
	// Ignore memory max limit passed in on inference job mode with job sharing feature enabled,
	// as due to the nature of job sharing, we don't conduct heterogeneous memory granting.
	if maxVal > 0 && !r.IsInferenceJobSharingEnabledForReplica(wsjob, rt) {
		res[resource.MemSubresource+resource.PodUpperBoundMaxLimitPostFix] = int(maxVal >> 20) // MiB
	}
	return res
}

// allocateReplicasToGroups distributes the job's replicas to the primary and secondary nodegroup. It's assumed that
// the secondary group will be repeated for each system besides the system associated with the primary group.
func allocateReplicasToGroups(
	replicas map[commonv1.ReplicaType]*commonv1.ReplicaSpec, numSystems int, axOnlyForRuntime bool) (
	map[commonv1.ReplicaType]int, map[commonv1.ReplicaType]int, map[commonv1.ReplicaType]int) {
	primary, secondary, wgtCmd := map[commonv1.ReplicaType]int{}, map[commonv1.ReplicaType]int{}, map[commonv1.ReplicaType]int{}
	for rt, rs := range replicas {
		if rt == wsapisv1.WSReplicaTypeCoordinator || rt == wsapisv1.WSReplicaTypeBroadcastReduce {
			continue
		}
		if wsapisv1.UseAxScheduling && (rt == wsapisv1.WSReplicaTypeActivation || rt == wsapisv1.WSReplicaTypeChief || rt == wsapisv1.WSReplicaTypeKVStorageServer) {
			continue
		}

		if wsapisv1.UseIxScheduling && (rt == wsapisv1.WSReplicaTypeSWDriver) {
			continue
		}
		if rt == wsapisv1.WSReplicaTypeWeight {
			// weights are all added to the primary group request, though could be further
			// split into multiple internal primary group requests in lock reconciler
			// For AX only for runtime server cases, weight/command allocation will be moved to wgtCmd map later.
			primary[rt] = int(*rs.Replicas)
			continue
		}

		replicasPerGroup := int(*rs.Replicas) / numSystems
		remainder := int(*rs.Replicas) % numSystems

		primaryAlloc := replicasPerGroup
		secondaryAlloc := replicasPerGroup
		if remainder > 0 {
			primaryAlloc++
			if remainder > 1 {
				secondaryAlloc++
			}
		}
		primary[rt] = primaryAlloc

		if numSystems > 1 && secondaryAlloc > 0 {
			secondary[rt] = secondaryAlloc
		}
	}

	if axOnlyForRuntime {
		for rt, alloc := range primary {
			if rt == wsapisv1.WSReplicaTypeWeight || rt == wsapisv1.WSReplicaTypeCommand {
				wgtCmd[rt] = alloc
			}
		}
		delete(primary, wsapisv1.WSReplicaTypeWeight)
		delete(primary, wsapisv1.WSReplicaTypeCommand)
	}

	return primary, secondary, wgtCmd
}

// getSubresources returns the subresources required by the given replica spec's ws container (the most resource heavy
// container in the pod). If the replica spec is nil, the default subresource is returned.
func getSubresources(rs *commonv1.ReplicaSpec) resource.Subresources {
	if rs == nil {
		return resource.Subresources{resource.PodSubresource: 1}
	}
	var containers []*corev1.Container
	for ic := range rs.Template.Spec.Containers {
		containers = append(containers, &rs.Template.Spec.Containers[ic])
	}
	return resource.SubresourceFromContainers(containers)
}

// applySchedulerHints modifies the lock request based on per-request configuration.
func (r *WSJobReconciler) applySchedulerHints(wsjob *wsapisv1.WSJob, lock *rlv1.ResourceLock) error {
	hints, err := r.schedulerHintsFromJob(wsjob)
	if err != nil {
		return err
	}

	hints.apply(lock)
	return nil
}

// getLockCondState determines the lock state by looking at the conditions met on the job
func getLockCondState(wsjob *wsapisv1.WSJob) lockCondState {
	if len(wsjob.Status.Conditions) > 0 {
		for i := len(wsjob.Status.Conditions) - 1; i >= 0; i-- {
			cond := wsjob.Status.Conditions[i]
			if cond.Type == commonv1.JobScheduled {
				return lockCondStateGranted
			} else if cond.Type == commonv1.JobQueueing {
				return lockCondStateAwait
			}
		}
	}
	return lockCondStateNew
}

// hydrateJobResources makes a map of replica type -> []res id assigned to this replica at this index + res id set
// This is amenable to the kubeflow controller's expectations for replica specification.
func hydrateJobResources(cfg *wscommon.ClusterConfig, lock *rlv1.ResourceLock) jobResources {
	status := lock.Status
	jr := jobResources{}
	// replicaToNodeMap[rType][idx] == nodeName
	replicaToNodeMap := make(map[string]map[int]string)
	// replica to res
	replicaToResourceMap := make(map[string]map[int]map[string]int)
	// replicaToNadMap[rType][idx] == nadName
	replicaToNadMap := make(map[string]map[int][]string)
	// replicaTypeToAnnoMap[rType][key] = val
	replicaTypeToAnnoMap := make(map[string]map[string]string)
	// replicaToNadMap[rType][idx][envkey] = envvalue
	replicaToEnvVarMap := make(map[string]map[int]map[string]string)
	// replicaToWioTargetMap[rType][idx][key] = val
	replicaToWioTargetMap := make(map[string]map[int]map[string]string)
	resGranted := map[string]bool{}
	// BR can be in partial mode where br id will be non-consecutive but pod id will be consecutive
	// so use array to keep mapping of pod id => br id, e.g. pod-0 => br-1 in case of skip group 0
	var replicaToBrNodeIdMap []int

	for _, rg := range status.ResourceGrants {
		for _, res := range rg.Resources {
			resGranted[res.Name] = true
			rType := rg.Name
			if rType != rlv1.CoordinatorRequestName {
				continue
			}

			if _, ok := replicaToNodeMap[rType]; !ok {
				replicaToNodeMap[rType] = map[int]string{}
				replicaToNadMap[rType] = map[int][]string{}
				replicaToEnvVarMap[rType] = map[int]map[string]string{}
			}
			id := len(replicaToNodeMap[rType])
			replicaToNodeMap[rType][id] = res.Name
			replicaToNadMap[rType][id] = []string{wscommon.GetNadByIndex(0)}
			if rg.Annotations[wsapisv1.NvmfUsableRangesEnvVarName] != "" {
				for _, nvmfGrant := range strings.Split(rg.Annotations[wsapisv1.NvmfUsableRangesEnvVarName], ",") {
					jr.nvmfGrants = append(jr.nvmfGrants, nvmfGrant)
				}
				replicaToEnvVarMap[rg.Name][id] = wscommon.EnsureMap(replicaToEnvVarMap[rg.Name][id])
				replicaToEnvVarMap[rg.Name][id][wsapisv1.NvmfUsableRangesEnvVarName] = rg.Annotations[wsapisv1.NvmfUsableRangesEnvVarName]
			}
		}
	}

	var primaryGGs []rlv1.GroupResourceGrant
	var secondaryGGs []rlv1.GroupResourceGrant
	var brGG rlv1.GroupResourceGrant
	var chfGGs []rlv1.GroupResourceGrant
	var actGGs []rlv1.GroupResourceGrant
	var wgtCmdGGs []rlv1.GroupResourceGrant
	var kvssGGs []rlv1.GroupResourceGrant
	var swdriverGGs []rlv1.GroupResourceGrant
	for _, gg := range status.GroupResourceGrants {
		if strings.HasPrefix(gg.Name, rlv1.PrimaryNgPrefix) {
			primaryGGs = append(primaryGGs, gg)
		} else if gg.Name == rlv1.SecondaryNgName {
			secondaryGGs = append(secondaryGGs, gg)
		} else if gg.Name == rlv1.BrRequestName {
			brGG = gg
		} else if strings.HasPrefix(gg.Name, rlv1.ChiefRequestName) {
			chfGGs = append(chfGGs, gg)
		} else if strings.HasPrefix(gg.Name, rlv1.ActivationRequestName) {
			actGGs = append(actGGs, gg)
		} else if strings.HasPrefix(gg.Name, rlv1.WgtCmdGroupPrefix) {
			wgtCmdGGs = append(wgtCmdGGs, gg)
		} else if strings.HasPrefix(gg.Name, rlv1.KVStorageServerRequestName) {
			kvssGGs = append(kvssGGs, gg)
		} else if strings.HasPrefix(gg.Name, rlv1.SWDriverRequestName) {
			swdriverGGs = append(swdriverGGs, gg)
		}
	}
	// sort group grants by group value for stability
	sort.Slice(primaryGGs, func(i, j int) bool {
		return primaryGGs[i].GroupingValue < primaryGGs[j].GroupingValue
	})
	sort.Slice(secondaryGGs, func(i, j int) bool {
		return secondaryGGs[i].GroupingValue < secondaryGGs[j].GroupingValue
	})
	sort.Slice(chfGGs, func(i, j int) bool {
		return chfGGs[i].Name < chfGGs[j].Name
	})
	sort.Slice(actGGs, func(i, j int) bool {
		return actGGs[i].Name < actGGs[j].Name
	})
	sort.Slice(wgtCmdGGs, func(i, j int) bool {
		return wgtCmdGGs[i].Name < wgtCmdGGs[j].Name
	})
	sort.Slice(kvssGGs, func(i, j int) bool {
		return kvssGGs[i].Name < kvssGGs[j].Name
	})
	sort.Slice(swdriverGGs, func(i, j int) bool {
		return swdriverGGs[i].Name < swdriverGGs[j].Name
	})

	updateResMap := func(res rlv1.Res, rType string, id int) {
		resGranted[res.Name] = true
		replicaToNadMap[rType] = wscommon.EnsureMap(replicaToNadMap[rType])
		replicaToNodeMap[rType] = wscommon.EnsureMap(replicaToNodeMap[rType])
		replicaToNodeMap[rType][id] = res.Name

		replicaToResourceMap[rType] = wscommon.EnsureMap(replicaToResourceMap[rType])
		res.Subresources = wscommon.EnsureMap(res.Subresources)

		// As of rel-2.3, the only multi-container pod is worker and there is no need to grant
		// upper bounds to worker pods. If this changed in the future, we will further redistribute
		// memory across containers within a pod.
		if rType == rlv1.WorkerRequestName {
			return
		}
		replicaToResourceMap[rType][id] = res.Subresources
		for _, bound := range status.GroupResourceGrantUpperBounds {
			if bound.Name == rType {
				for k := range bound.Subresources {
					if replicaToResourceMap[rType][id][k] < bound.Subresources[k] {
						replicaToResourceMap[rType][id][k] = bound.Subresources[k]
					}
				}
				break
			}
		}
	}

	for _, primaryGG := range primaryGGs {
		for _, rg := range primaryGG.ResourceGrants {
			for _, res := range rg.Resources {
				id := len(replicaToNodeMap[rg.Name])
				updateResMap(res, rg.Name, id)
				replicaToNadMap[rg.Name][id] = []string{wscommon.GetNadByIndex(0)}
				if len(jr.nvmfGrants) > 0 && rg.Name == rlv1.WeightRequestName {
					replicaToEnvVarMap[rg.Name] = wscommon.EnsureMap(replicaToEnvVarMap[rg.Name])
					replicaToEnvVarMap[rg.Name][id] = wscommon.EnsureMap(replicaToEnvVarMap[rg.Name][id])
					replicaToEnvVarMap[rg.Name][id][wsapisv1.NvmfUsableRangesEnvVarName] = jr.nvmfGrants[id]
				}
			}
		}
	}

	// for nodes that are in depop nodegroups and have the secondary nic enabled,
	// track how many activation pods landed on them respectively
	actDepopNodes := map[string]int{}
	for _, gg := range secondaryGGs {
		for _, rg := range gg.ResourceGrants {
			for _, res := range rg.Resources {
				rType := rg.Name
				id := len(replicaToNodeMap[rType])
				updateResMap(res, rType, id)

				replicaToNadMap[rType][id] = []string{wscommon.GetNadByIndex(0)}
				// Note that we won't exercise this branch when using ax scheduling
				if !wscommon.DisableSecondaryDataNic && rType == rlv1.ActivationRequestName {
					no, ok := cfg.NodeMap[res.Name]
					if !ok {
						continue
					}
					if g, ok := cfg.Resources.GetGroup(no.GetGroup()); !ok || g.IsPopulated() {
						continue
					}
					if len(no.NICs) > 1 && !no.NICs[1].HasError {
						actDepopNodes[res.Name] += 1
						if actDepopNodes[res.Name]%2 == 0 {
							replicaToNadMap[rType][id] = []string{wscommon.GetNadByIndex(1)}
						}
					}
				}
			}
		}
	}

	replicaTypeToAnnoMap[rlv1.BrRequestName] = brGG.Annotations
	for _, rg := range brGG.ResourceGrants {
		var brNodeId int
		parseId, err := strconv.Atoi(rg.Name)
		if err == nil {
			brNodeId = parseId
		} else {
			// this should not happen
			wscommon.Log.Warnf("Error parsing BR node id: %s", err)
		}

		replicaToWioTargetMap[rlv1.BrRequestName] = wscommon.EnsureMap(replicaToWioTargetMap[rlv1.BrRequestName])
		replicaToWioTargetMap[rlv1.BrRequestName][brNodeId] = wscommon.EnsureMap(replicaToWioTargetMap[rlv1.BrRequestName][brNodeId])
		for _, property := range []string{rlv1.TargetCsPortAnnotKey, rlv1.SystemIdAnnotKey, rlv1.TargetBrSetsAnnotKey, rlv1.BrSetIdAnnotKey} {
			replicaToWioTargetMap[rlv1.BrRequestName][brNodeId][property] = rg.Annotations[property]
		}

		// BR has only one resource for each id
		res := rg.Resources[0]
		updateResMap(res, rlv1.BrRequestName, brNodeId)

		replicaToBrNodeIdMap = append(replicaToBrNodeIdMap, brNodeId)
		if !wsapisv1.DisableMultusForBR {
			for _, nic := range getAssignedNicsForReplica(res.Subresources, true) {
				nad := cfg.NodeMap[res.Name].GetNadByNIC(nic)
				replicaToNadMap[rlv1.BrRequestName][brNodeId] = append(replicaToNadMap[rlv1.BrRequestName][brNodeId], nad)
			}
		}
	}

	hydrateCommonWioResources := func(replicaType string, rg *rlv1.ResourceGrant) {
		// wio requests has only one resource for each id
		res := rg.Resources[0]
		replicaToNodeMap[replicaType] = wscommon.EnsureMap(replicaToNodeMap[replicaType])
		podIdx, _ := strconv.Atoi(rg.Name)
		updateResMap(res, replicaType, podIdx)

		replicaToEnvVarMap[replicaType] = wscommon.EnsureMap(replicaToEnvVarMap[replicaType])
		replicaToEnvVarMap[replicaType][podIdx] = wscommon.EnsureMap(replicaToEnvVarMap[replicaType][podIdx])
		replicaToEnvVarMap[replicaType][podIdx][wsapisv1.RuntimeCsPortEnvVarName] = rg.Annotations[rlv1.TargetCsPortAnnotKey]
		replicaToEnvVarMap[replicaType][podIdx][wsapisv1.RuntimeCsPortAffinityEnvVarName] = rg.Annotations[rlv1.PreferredCsPortsAnnotKey]

		// We use nic scheduling for activations, which should have accounted for nic health.
		// We should skip the nic error check here.
		replicaToNadMap[replicaType] = wscommon.EnsureMap(replicaToNadMap[replicaType])
		for _, nic := range getAssignedNicsForReplica(res.Subresources, false) {
			nad := cfg.NodeMap[res.Name].GetNadByNIC(nic)
			replicaToNadMap[replicaType][podIdx] = append(replicaToNadMap[replicaType][podIdx], nad)
		}

		replicaToWioTargetMap[replicaType] = wscommon.EnsureMap(replicaToWioTargetMap[replicaType])
		replicaToWioTargetMap[replicaType][podIdx] = wscommon.EnsureMap(replicaToWioTargetMap[replicaType][podIdx])
		for _, property := range []string{rlv1.TargetCsPortAnnotKey, rlv1.PreferredCsPortsAnnotKey, rlv1.SystemIdAnnotKey} {
			replicaToWioTargetMap[replicaType][podIdx][property] = rg.Annotations[property]
		}
	}

	if wsapisv1.UseAxScheduling {
		// each group grant is stamp-specific
		for _, gg := range chfGGs {
			for _, rg := range gg.ResourceGrants {
				// Number of resources here should be the same as number of systems granted in the same stamp
				for _, res := range rg.Resources {
					id := len(replicaToNodeMap[rlv1.ChiefRequestName])
					updateResMap(res, rlv1.ChiefRequestName, id)

					no, ok := cfg.NodeMap[res.Name]
					if !ok {
						continue
					}
					// We don't do nic scheduling for chiefs. We by default choose the first nic unless it has error.
					if len(no.NICs) > 0 && !no.NICs[0].HasError {
						replicaToNadMap[rlv1.ChiefRequestName][id] = []string{wscommon.GetNadByIndex(0)}
					} else {
						replicaToNadMap[rlv1.ChiefRequestName][id] = []string{wscommon.GetNadByIndex(1)}
					}
				}
			}
		}

		// each group grant is stamp-specific
		for _, gg := range actGGs {
			for _, rg := range gg.ResourceGrants {
				hydrateCommonWioResources(rlv1.ActivationRequestName, &rg)
				podIdx, _ := strconv.Atoi(rg.Name)
				if gg.Annotations != nil && gg.Annotations[rlv1.LinearMemRangeXAnnotKey] != "" {
					replicaToEnvVarMap[rlv1.ActivationRequestName][podIdx][wsapisv1.RuntimeLinearMemRangeXEnvVarName] = gg.Annotations[rlv1.LinearMemRangeXAnnotKey]
				}
			}
		}

		// each group grant is stamp-specific
		for _, gg := range kvssGGs {
			for _, rg := range gg.ResourceGrants {
				hydrateCommonWioResources(rlv1.KVStorageServerRequestName, &rg)
				podIdx, _ := strconv.Atoi(rg.Name)
				if gg.Annotations != nil && gg.Annotations[rlv1.LinearMemRangeXAnnotKey] != "" {
					replicaToEnvVarMap[rlv1.KVStorageServerRequestName][podIdx][wsapisv1.RuntimeLinearMemRangeXEnvVarName] = gg.Annotations[rlv1.LinearMemRangeXAnnotKey]
				}
			}
		}
		if lock.IsInferenceJobSharingEnabled() {
			for _, replicaType := range wscommon.GetInferenceRelatedReplicaTypes() {
				requestType := string(replicaType)
				replicaTypeToAnnoMap[requestType] = wscommon.EnsureMap(replicaTypeToAnnoMap[requestType])
				replicaTypeToAnnoMap[requestType][wsapisv1.InferenceJobSharingAnnotKey] = strconv.FormatBool(true)
			}
		}

		for _, gg := range wgtCmdGGs {
			for _, rg := range gg.ResourceGrants {
				for _, res := range rg.Resources {
					if rg.Name == rlv1.CommandRequestName {
						updateResMap(res, rg.Name, 0)
						replicaToNadMap[rg.Name][0] = []string{wscommon.GetNadByIndex(0)}
					} else {
						id := len(replicaToNodeMap[rlv1.WeightRequestName])
						updateResMap(res, rlv1.WeightRequestName, id)
						replicaToNadMap[rlv1.WeightRequestName][id] = []string{wscommon.GetNadByIndex(0)}
					}
				}
			}
		}
	}

	// Add SWDriver scheduling logic similar to AX scheduling
	if wsapisv1.UseIxScheduling {
		// each group grant is stamp-specific
		for _, gg := range swdriverGGs {
			for _, rg := range gg.ResourceGrants {
				hydrateCommonWioResources(rlv1.SWDriverRequestName, &rg)
			}
		}
	}

	jr = jobResources{
		name:                  lock.NamespacedName(),
		lockType:              lock.Spec.QueueName,
		parentJobName:         lock.ParentLockName(),
		replicaToNodeMap:      replicaToNodeMap,
		replicaToResourceMap:  replicaToResourceMap,
		replicaToNadMap:       replicaToNadMap,
		resGranted:            resGranted,
		replicaToBrNodeIdMap:  replicaToBrNodeIdMap,
		replicaToEnvVarMap:    replicaToEnvVarMap,
		replicaToAnnoMap:      replicaTypeToAnnoMap,
		replicaToWioTargetMap: replicaToWioTargetMap,
		nvmfGrants:            jr.nvmfGrants,
	}
	return jr
}

func (jr jobResources) getAssignedInstanceId(rt commonv1.ReplicaType, rid int) string {
	for k := range jr.replicaToResourceMap[string(rt)][rid] {
		if strings.HasPrefix(k, resource.PodInstanceIdSubresource+"/") {
			return k[(len(resource.PodInstanceIdSubresource) + 1):]
		}
	}
	return ""
}

func (jr jobResources) getReplicaTypeMinAssign(rt commonv1.ReplicaType, resType string) string {
	minAssign := math.MaxInt
	for _, replica := range jr.replicaToResourceMap[string(rt)] {
		if val, ok := replica[resType]; ok && val < minAssign {
			minAssign = replica[resType]
		}
	}
	if minAssign != math.MaxInt {
		return strconv.Itoa(minAssign)
	}
	return ""
}

func (jr jobResources) getAssignedNicsForReplica(rt commonv1.ReplicaType, rid int) []string {
	return getAssignedNicsForReplica(jr.replicaToResourceMap[string(rt)][rid], true)
}

func (jr jobResources) getEnvVars(rt commonv1.ReplicaType, rid int) []corev1.EnvVar {
	var res []corev1.EnvVar
	if envVarsForReplicas, ok := jr.replicaToEnvVarMap[string(rt)]; ok {
		for k, v := range envVarsForReplicas[rid] {
			res = append(res, corev1.EnvVar{Name: k, Value: v})
		}
	}
	return res
}

func (jr *jobResources) IsInferenceJobSharingEnabled(rt commonv1.ReplicaType) bool {
	// SWD does not rely on the job sharing annotation
	// Instead it relies on the number of pods per node
	switch rt {
	case wsapisv1.WSReplicaTypeSWDriver:
		return resource_config.MaxSwDriverPodsPerIxNode > 1
	default:
		return jr.replicaToAnnoMap[string(rt)][wsapisv1.InferenceJobSharingAnnotKey] == strconv.FormatBool(true)
	}
}

func getAssignedNicsForReplica(res map[string]int, sortByBw bool) []string {
	var nics []string
	for k := range res {
		if strings.HasPrefix(k, resource.NodeNicSubresource) {
			// "cs-port/nic-0: 200"=>["nic-0", "nic-0"]
			nic := k[(len(resource.NodeNicSubresource) + 1):]
			for i := 0; i < res[k]; i += resource.DefaultNicBW {
				nics = append(nics, nic)
			}
		}
	}
	if !sortByBw {
		return wscommon.Sorted(nics)
	}
	// sort based on assigned BW then by name
	// e.g. eth0: 200, eth1: 100 => eth1, eth0
	// the BW sort is for BR ingress case to get ingress to be lower capacity NIC
	sort.Slice(nics, func(i, j int) bool {
		bwI := res[resource.NodeNicResName(nics[i])]
		bwJ := res[resource.NodeNicResName(nics[j])]
		if bwI == bwJ {
			return nics[i] < nics[j]
		}
		return bwI < bwJ
	})
	return nics
}

// Interfaces and nodes assigned to each pod.
func (r *WSJobReconciler) getAssignedNics(jr *jobResources) []NicAssignment {
	nics := map[NicAssignment]bool{}
	for rt := range jr.replicaToNodeMap {
		for rid, node := range jr.replicaToNodeMap[rt] {
			for _, nic := range getAssignedNicsForReplica(jr.replicaToResourceMap[rt][rid], true) {
				nics[NicAssignment{rt, strconv.Itoa(rid), node, nic}] = true
			}
			for _, nad := range jr.replicaToNadMap[rt][rid] {
				nic := r.Cfg.NodeMap[node].GetNICByNad(nad)
				nics[NicAssignment{rt, strconv.Itoa(rid), node, nic}] = true
			}
		}
	}
	return maps.Keys(nics)
}

// apply session specific property
func (r *WSJobReconciler) applySessionProperty(lock *rlv1.ResourceLock) {
	// skip single system/box
	if !wsapisv1.IsMultiBox {
		return
	}

	// add namespace anno to be backwards compatible with single box setup
	lock.Annotations[resource.NamespacePropertyKey] = lock.Namespace
	// node version filter
	if wscommon.EnablePlatformVersionCheck {
		if pv := r.Cfg.GetNsrProperty(lock.Namespace, wsapisv1.PlatformVersionKey); pv != "" {
			lock.Labels[resource.PlatformVersionPropertyKey] = pv
			r.Log.Info("applying node version filter from session prop",
				"job", lock.NamespacedName(), "version", pv)
		}
	}
	// for redundancy usage
	if !wscommon.DisableRedundantSession {
		redundancy := r.Cfg.GetNsrProperty(lock.Namespace, namespacev1.AssignedRedundancyKey)
		if redundancy != "" {
			lock.Labels[resource.AssignedRedundancyPropertyKey] = redundancy
		}
		if (!wscommon.DisableSystemVersionCheck && redundancy != "") || lock.RequireSystemVersionFilter() {
			// system version filter (only on redundancy usage to start with)
			// check on session prop first
			// fall back to a random system in session
			// this can be extend to check on existing cbcore tag
			if version := r.Cfg.GetNsrProperty(lock.Namespace, wsapisv1.SystemVersionKey); version != "" {
				lock.Labels[resource.SystemVersionPropertyKey] = version
				r.Log.Info("applying system version filter from session prop",
					"job", lock.NamespacedName(), "version", version)
			} else if systems := r.Cfg.GetAssignedSystemsList(lock.Namespace); systems != nil {
				for _, system := range systems {
					if version = r.Cfg.SystemMap[system].Properties[resource.SystemVersionPropertyKey]; version != "" {
						lock.Labels[resource.SystemVersionPropertyKey] = version
						r.Log.Info("applying system version filter from session system",
							"job", lock.NamespacedName(), "version", version, "system", system)
						return
					}
				}
			}
		}
	}
}

// This function emits job network topology related metrics
// E.g.: node/nics, leaf switch, target systems, system ports, preferred ports
func (r *WSJobReconciler) EmitJobNetworkTopologyMetrics(jr *jobResources, jobName, jobNamespace string) {
	for replicaType := range jr.replicaToNodeMap {
		for replicaIdx, node := range jr.replicaToNodeMap[replicaType] {
			wioTargets := jr.replicaToWioTargetMap[replicaType][replicaIdx]
			var nics []string
			for _, nic := range getAssignedNicsForReplica(jr.replicaToResourceMap[replicaType][replicaIdx], true) {
				nics = wscommon.AppendUnique(nics, nic)
			}

			networkTopology := wscommon.ReplicaNetworkTopology{
				NumOneHopTraffic:   0,
				NumMultiHopTraffic: 0,
				JobName:            jobName,
				JobNamespace:       jobNamespace,
				ReplicaType:        replicaType,
				NodeName:           node,
				NicNames:           strings.Join(nics, ","),
				PreferredPorts:     wioTargets[rlv1.PreferredCsPortsAnnotKey],
				TargetPorts:        wioTargets[rlv1.TargetCsPortAnnotKey],
				TargetSystems:      wioTargets[rlv1.SystemIdAnnotKey],
				BrSetId:            wioTargets[rlv1.BrSetIdAnnotKey],
				TargetBrSetId:      wioTargets[rlv1.TargetBrSetsAnnotKey],
				ReplicaIdx:         strconv.Itoa(replicaIdx),
			}
			networkTopology.EvaluateNetworkTopologyQuality(r.Cfg, jr.systems, true)
		}
	}
}

func getJobNodeRoleSpecialization(wsjob *wsapisv1.WSJob) map[commonv1.ReplicaType]wscommon.NodeRole {
	nodeRoleMap := wscommon.CopyMap(wscommon.ReplicaTypeNodeSpecialization)
	if wsapisv1.UseAxScheduling {
		nodeRoleMap[wsapisv1.WSReplicaTypeKVStorageServer] = wscommon.RoleActivation
		nodeRoleMap[wsapisv1.WSReplicaTypeChief] = wscommon.RoleActivation
		nodeRoleMap[wsapisv1.WSReplicaTypeActivation] = wscommon.RoleActivation
		if wsjob.UseAxOnlyForRuntime() {
			nodeRoleMap[wsapisv1.WSReplicaTypeWeight] = wscommon.RoleActivation
			nodeRoleMap[wsapisv1.WSReplicaTypeCommand] = wscommon.RoleActivation
		}
	}
	return nodeRoleMap
}

// This is only needed for job operator upgrade to rel-2.5, where the responsibility of the cluster details
// configmap generation shifted from job operator to cluster server.
// TODO: Remove in rel-2.7
func waitForServerGeneratedClusterDetails(logger logr.Logger, wsjob *wsapisv1.WSJob) error {
	if time.Now().Sub(wsjob.ObjectMeta.CreationTimestamp.Time) < wscommon.DefaultClusterDetailsGenerationTimeout {
		logger.Info("expected server side cluster details configmap not generated, wait till next cycle")
		// requeue with ttl to prevent exponential backoff as we expect the configmap
		// to be created immediately after the wsjob was created
		return &commonv1.RequeueAfterTTL{TTL: time.Second}
	}
	logger.Info(fmt.Sprintf("cluster details configmap was expected to be generated "+
		"by cluster server but does not exist %s after the job was created, "+
		"proceed to create the configmap regardless to unblock progress", wscommon.DefaultClusterDetailsGenerationTimeout))
	return nil
}

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
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	common_pb "cerebras/pb/workflow/appliance/common"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	commonctrlv1 "github.com/kubeflow/common/pkg/controller.v1/common"
)

func generateWseTaskInfo(res jobResources, systemMap map[string]*systemv1.SystemSpec) *common_pb.ClusterDetails_TaskInfo {
	taskMap := make([]*common_pb.ClusterDetails_TaskInfo_TaskMap, len(res.systems))
	for i := 0; i < len(res.systems); i++ {
		sys := systemMap[res.systems[i]]
		taskId := common_pb.ClusterDetails_TaskInfo_TaskMap_TaskId{
			TaskId: int32(i),
			WseId:  int32(i),
			WseIds: []int32{int32(i)},
		}
		addressBook := common_pb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
			WseIp:        sys.CmAddr,
			TaskIpPort:   sys.CmAddr,
			TaskNodeName: sys.Name,
		}
		taskMap[i] = &common_pb.ClusterDetails_TaskInfo_TaskMap{
			TaskId:      &taskId,
			AddressBook: &addressBook,
		}
	}
	return &common_pb.ClusterDetails_TaskInfo{
		TaskType: common_pb.ClusterDetails_TaskInfo_WSE,
		TaskMap:  taskMap,
	}
}

// Starting from rel-2.5, we fully respect the cluster details request sent by Framework by creating the cluster
// details configmap from the cluster server side.
// Routine below was introduced in very early days, trying to reconstruct the mappings between replicas to CSXs,
// which are already captured in Framework's input.
// TODO: Remove this cluster details reconstruction function entirely in rel-2.7
func generateClusterDetails(job *wsapisv1.WSJob, res jobResources, systemMap map[string]*systemv1.SystemSpec) (*common_pb.ClusterDetails, error) {
	// assign systems to replicas in a continuous way, e.g. id 0,1 to sys0, id 2,3 to sys1
	replicaToSystemId := func(jobName, rType string, replicaIndex, replicaCount, systems int) int {
		if replicaCount < systems {
			logrus.Warnf("Job %s rType %s rCount %d shouldn't be less than sys count %d, fall back to default id.",
				jobName, rType, replicaCount, systems)
			return replicaIndex
		}
		return replicaIndex / (replicaCount / systems)
	}

	var tasks []*common_pb.ClusterDetails_TaskInfo
	for replicaType, spec := range job.Spec.WSReplicaSpecs {
		// br has its own config
		if replicaType == wsapisv1.WSReplicaTypeBroadcastReduce {
			continue
		}
		rt := replicaType.Lower()
		taskType := wsapisv1.ClusterDetailsTaskTypeMapping[rt]
		if taskType == 0 {
			// Default to worker.
			taskType = common_pb.ClusterDetails_TaskInfo_WRK
		}

		taskMap := make([]*common_pb.ClusterDetails_TaskInfo_TaskMap, *spec.Replicas)
		for i := int32(0); i < *spec.Replicas; i++ {
			taskId := &common_pb.ClusterDetails_TaskInfo_TaskMap_TaskId{
				TaskId: i,
			}
			addressBook := &common_pb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
				TaskIpPort: job.GetDefaultTaskIpPort(replicaType, i),
			}

			if len(res.systems) > 0 &&
				(taskType == common_pb.ClusterDetails_TaskInfo_ACT ||
					taskType == common_pb.ClusterDetails_TaskInfo_CHF ||
					taskType == common_pb.ClusterDetails_TaskInfo_WRK) {
				taskId.WseId = int32(replicaToSystemId(job.NamespacedName().String(), taskType.String(),
					int(i), int(*spec.Replicas), len(res.systems)))
				addressBook.WseIp = systemMap[res.systems[taskId.WseId]].CmAddr

				// Hydrating the wse domains only for informational purposes
				if len(job.Spec.ActToCsxDomains) > 0 && taskType == common_pb.ClusterDetails_TaskInfo_ACT {
					numActsPerCsx := int(*spec.Replicas) / len(res.systems)
					kthActPerCsx := int(i) % numActsPerCsx
					// This condition should always be true
					// We use conditions to guard regardless given the hydration is not critical to the flow
					if kthActPerCsx < len(job.Spec.ActToCsxDomains) && len(job.Spec.ActToCsxDomains[kthActPerCsx]) > 0 {
						addressBook.WseDomains = []int32{int32(job.Spec.ActToCsxDomains[kthActPerCsx][0])}
					}
				}
			}

			taskMap[i] = &common_pb.ClusterDetails_TaskInfo_TaskMap{
				TaskId:      taskId,
				AddressBook: addressBook,
			}
		}

		taskInfo := common_pb.ClusterDetails_TaskInfo{
			TaskType: taskType,
			TaskMap:  taskMap,
		}
		tasks = append(tasks, &taskInfo)
	}

	if len(res.systems) > 0 {
		tasks = append(tasks, generateWseTaskInfo(res, systemMap))
	}

	return &common_pb.ClusterDetails{Tasks: tasks}, nil
}

// reconcileClusterDetailsConfig: generate cluster details config based brConfig.
// return proceed boolean to indicate whether it's ok to keep reconciling
func (jobController *WSJobController) reconcileClusterDetailsConfig(
	ctx context.Context, job *wsapisv1.WSJob, res jobResources) (*corev1.ConfigMap, bool, error) {
	logger := jobController.Log.WithValues(wsapisv1.Singular, job.Name)

	// idempotent logic for already generated cluster details config
	clusterDetailsConfigMap, configProtoMsg, err := jobController.getClusterDetailsConfig(job)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to get cluster details configmap")
		return nil, false, err
	} else if err == nil {
		err = jobController.updateClusterDetailsConfig(job, res, clusterDetailsConfigMap, configProtoMsg)
		if err != nil {
			logger.Error(err, "failed to update cluster details configmap")
			if wscommon.IsCriticalK8sError(err) {
				return nil, false, CriticalErrorf("unable to update cluster details: generated file is too large")
			}
			return nil, false, err
		}
		return clusterDetailsConfigMap, true, nil
	}

	logger.Info("start generating cluster details configmap")
	labels := jobController.GenLabels(job.Name)
	labels[wsapisv1.ConfigMapType] = wsapisv1.ConfigMapTypeClusterDetails

	clusterDetailsConfigMap = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job.Name),
			Namespace: job.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				wsapisv1.OperatorGeneratedConfigAnnot: strconv.FormatBool(true),
			},
		},
		Data: map[string]string{
			wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.InitSignalVal,
		},
	}

	var clusterDetails proto.Message
	clusterDetails, err = generateClusterDetails(job, res, jobController.Cfg.SystemMap)
	if err != nil {
		return nil, false, err
	}

	options := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	clusterDetailsData, err := options.Marshal(clusterDetails)
	if err != nil {
		logger.Error(err, "failed to convert cluster details protobuf to json")
		return nil, false, err
	}
	logger.Info("cluster details configmap generated", "clusterDetailsData", string(clusterDetailsData))

	compressed, err := wscommon.Gzip(clusterDetailsData)
	if err != nil {
		logger.Error(err, "failed to compress cluster details")
		return nil, false, err
	}
	clusterDetailsConfigMap.BinaryData = wscommon.EnsureMap(clusterDetailsConfigMap.BinaryData)
	clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName] = compressed

	if err = jobController.addResourceDetailsToCM(ctx, job, clusterDetailsConfigMap); err != nil {
		return nil, false, err
	}

	if err := controllerutil.SetControllerReference(job, clusterDetailsConfigMap, jobController.Scheme); err != nil {
		logger.Error(err, "set cluster details config controller reference error")
		return nil, false, err
	}
	err = jobController.Create(context.Background(), clusterDetailsConfigMap)
	if err != nil && !errors.IsAlreadyExists(err) {
		logger.Error(err, "failed to create cluster details configmap")
		if wscommon.IsCriticalK8sError(err) {
			return nil, false, CriticalErrorf("unable to create cluster details: generated file is too large")
		}
		return nil, false, err
	}
	logger.Info("generating cluster details configmap done")
	return clusterDetailsConfigMap, true, nil
}

func (jobController *WSJobController) getClusterDetailsConfig(job *wsapisv1.WSJob) (*corev1.ConfigMap, proto.Message, error) {
	clusterDetailsConfigMap := &corev1.ConfigMap{}
	var clusterDetailsConfig proto.Message
	if err := jobController.Get(context.Background(), types.NamespacedName{
		Namespace: job.GetNamespace(), Name: fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job.Name),
	}, clusterDetailsConfigMap); err != nil {
		return clusterDetailsConfigMap, clusterDetailsConfig, err
	}

	data := wscommon.GetClusterDetailsData(clusterDetailsConfigMap)
	options := protojson.UnmarshalOptions{
		AllowPartial:   true,
		DiscardUnknown: true,
	}
	clusterDetailsConfig = &common_pb.ClusterDetails{}
	if err := options.Unmarshal(data, clusterDetailsConfig); err != nil {
		return clusterDetailsConfigMap, clusterDetailsConfig, err
	}

	return clusterDetailsConfigMap, clusterDetailsConfig, nil
}

// updateClusterDetailsConfig: update cluster details config post scheduling.
func (jobController *WSJobController) updateClusterDetailsConfig(job *wsapisv1.WSJob, res jobResources,
	clusterDetailsConfigMap *corev1.ConfigMap, configProtoMsg proto.Message) error {
	logger := jobController.Log.WithValues(wsapisv1.Singular, job.Name)

	clusterDetailsConfig := configProtoMsg.(*common_pb.ClusterDetails)
	// idempotent logic for already updated cluster details config
	if clusterDetailsConfigMap.Data[wsapisv1.ClusterDetailsConfigUpdatedSignal] == wsapisv1.UpdatedSignalVal {
		return nil
	}

	// wait till all pods init
	if job.Status.ReplicaStatuses == nil {
		return nil
	}
	for rtype, spec := range job.Spec.WSReplicaSpecs {
		if spec == nil {
			return nil
		}
		status := job.Status.ReplicaStatuses[rtype]
		if status == nil || status.Init != *spec.Replicas {
			return nil
		}
	}

	logger.Info("start updating cluster details configmap")
	podToTaskMapMap := map[string]*common_pb.ClusterDetails_TaskInfo_TaskMap{}
	for _, taskInfo := range clusterDetailsConfig.Tasks {
		// br has its own config
		if taskInfo.TaskType == common_pb.ClusterDetails_TaskInfo_WSE || taskInfo.TaskType == common_pb.ClusterDetails_TaskInfo_BR {
			continue
		}
		for i, taskMap := range taskInfo.TaskMap {
			endpoint := taskMap.GetAddressBook().GetTaskIpPort()
			if len(endpoint) == 0 {
				err := fmt.Errorf("invalid cluster details: no taskIpPort for task %d, %+v", i, clusterDetailsConfig)
				logger.Error(err, "failed to parse cluster details configmap")
				return err
			}
			name := commonctrlv1.GenGeneralName(
				job.Name,
				wsapisv1.GetReplicaTypeFull(taskInfo.TaskType.String()).Lower(),
				strconv.Itoa(int(taskMap.TaskId.TaskId)))
			podToTaskMapMap[name] = taskMap
		}
	}

	pods, err := jobController.GetPodsForJob(job)
	if err != nil {
		logger.Error(err, "failed to list all pods")
		return err
	}
	var filteredPods []*corev1.Pod
	for _, pod := range pods {
		// br has its own config
		if pod.Labels[commonv1.ReplicaTypeLabel] == wsapisv1.WSReplicaTypeBroadcastReduce.Lower() {
			continue
		}
		filteredPods = append(filteredPods, pod)
	}

	if len(filteredPods) < len(podToTaskMapMap) {
		return fmt.Errorf("running pods length less than expected podToTaskMapMap, %d, %d", len(pods), len(podToTaskMapMap))
	}
	// workaround for backwards compatible
	for _, pod := range filteredPods {
		podName := pod.ObjectMeta.Name
		if taskMap, ok := podToTaskMapMap[podName]; ok {
			taskMap.AddressBook.TaskNodeName = pod.Spec.NodeName
			// update svc name with IP to skip dns query
			ips, err := jobController.GetPodDataIP(pod)
			if err != nil {
				return err
			}
			var taskCommPorts []string
			defaultPort := int32(wsapisv1.DefaultPort)
			taskMap.AddressBook.TaskDebugAddress = fmt.Sprintf("%s:%d", pod.Status.PodIP, wsapisv1.DefaultDebugPort)
			for _, container := range pod.Spec.Containers {
				if container.Name == wsapisv1.DefaultContainerName {
					for _, p := range container.Ports {
						if p.Name == wsapisv1.DefaultPortName {
							defaultPort = p.ContainerPort
						} else if strings.HasPrefix(p.Name, wsapisv1.DefaultWhostPortNamePrefix) {
							taskCommPorts = append(taskCommPorts, strconv.Itoa(int(p.ContainerPort)))
						}
					}
					taskMap.AddressBook.TaskIpPort = fmt.Sprintf("%s:%d", ips[0], defaultPort)
					if len(taskCommPorts) > 0 {
						taskMap.AddressBook.TaskCommPorts = taskCommPorts
					}
				} else if wsapisv1.IsUserSidecarContainer(&container) {
					taskMap.AddressBook.UserSidecarAddress = fmt.Sprintf("%s:%d", ips[0], wsapisv1.DefaultUserSidecarPort)
					// We used to use TaskDebugAddress for the streamer address in worker pods.
					// We no longer do that in rel-2.5 with the introduction of the generalized UserSidecarAddress.
					// TODO: Remove this block in rel-2.6.
					if pod.Labels["replica-type"] == wsapisv1.WSReplicaTypeWorker.Lower() && !job.IsSdk() {
						taskMap.AddressBook.TaskDebugAddress = fmt.Sprintf("%s:%d", ips[0], wsapisv1.DefaultUserSidecarPort)
					}
				}
			}
		} else {
			return fmt.Errorf("pod %s not found in cluster details %+v", podName, podToTaskMapMap)
		}
	}

	// Older cluster servers before 1.0.5 always delegate to operator for cluster details reconstruction
	// Note that the wse info and resource details would only get added to the cluster details configmap after pod init.
	// TODO: Make this block unconditional in rel-2.8
	if clusterDetailsConfigMap.Annotations[wsapisv1.OperatorGeneratedConfigAnnot] != strconv.FormatBool(true) {
		if err = jobController.addResourceDetailsToCM(context.Background(), job, clusterDetailsConfigMap); err != nil {
			return err
		}
		for i := range clusterDetailsConfig.Tasks {
			if clusterDetailsConfig.Tasks[i].TaskType == common_pb.ClusterDetails_TaskInfo_WSE {
				// inplace update WSE task info
				numSys := len(clusterDetailsConfig.Tasks[i].TaskMap)
				if numSys != len(res.systems) {
					return CriticalErrorf(
						"number of WSEs %d in cluster details config does not match number of systems granted %d",
						numSys, len(res.systems))
				}
				for j := 0; j < numSys; j++ {
					sys := jobController.Cfg.SystemMap[res.systems[j]]
					if clusterDetailsConfig.Tasks[i].TaskMap[j] == nil {
						// should not happen, but just in case
						clusterDetailsConfig.Tasks[i].TaskMap[j] = &common_pb.ClusterDetails_TaskInfo_TaskMap{
							TaskId: &common_pb.ClusterDetails_TaskInfo_TaskMap_TaskId{
								TaskId: int32(j),
								WseId:  int32(j),
								WseIds: []int32{int32(j)},
							},
						}
					}
					if clusterDetailsConfig.Tasks[i].TaskMap[j].AddressBook == nil {
						// AddressBook may not always been set, so create it if not set
						clusterDetailsConfig.Tasks[i].TaskMap[j].AddressBook = &common_pb.ClusterDetails_TaskInfo_TaskMap_AddressBook{}
					}
					clusterDetailsConfig.Tasks[i].TaskMap[j].AddressBook.WseIp = sys.CmAddr
					clusterDetailsConfig.Tasks[i].TaskMap[j].AddressBook.TaskIpPort = sys.CmAddr
					clusterDetailsConfig.Tasks[i].TaskMap[j].AddressBook.TaskNodeName = sys.Name
				}
			}
		}
	}
	options := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	configData, err := options.Marshal(clusterDetailsConfig)
	if err != nil {
		logger.Error(err, "failed to encode cluster details configmap")
		return err
	}
	if clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName] != nil {
		compressed, err := wscommon.Gzip(configData)
		if err != nil {
			logger.Error(err, "failed to compress cluster details")
			return err
		}
		clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName] = compressed
	} else {
		clusterDetailsConfigMap.Data[wsapisv1.ClusterDetailsConfigFileName] = string(configData)
	}
	clusterDetailsConfigMap.Data[wsapisv1.ClusterDetailsConfigUpdatedSignal] = wsapisv1.UpdatedSignalVal

	if err := jobController.Update(context.Background(), clusterDetailsConfigMap); err != nil {
		logger.Error(err, "failed to update cluster details configmap")
		return err
	}
	logger.Info("successfully updated cluster details configmap")

	return nil
}

func (jobController *WSJobController) updateWsjobByClusterDetails(ctx context.Context,
	wsjob *wsapisv1.WSJob, cm *corev1.ConfigMap) (bool, error) {
	// idempotent check
	if _, ok := wsjob.Annotations[wsapisv1.ClusterDetailsAnno]; ok {
		return true, nil
	}

	updatedJob := wsjob.DeepCopy()
	volume := corev1.Volume{
		Name: wsapisv1.ClusterDetailsConfigVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, updatedJob.Name),
				},
			},
		},
	}
	volumeMount := corev1.VolumeMount{
		Name:      wsapisv1.ClusterDetailsConfigVolume,
		MountPath: wsapisv1.ClusterDetailsConfigMountPath,
	}
	configFileName := wsapisv1.ClusterDetailsConfigCompressedFileName
	envs := []corev1.EnvVar{
		{
			Name: wsapisv1.ClusterDetailsConfigPathEnv,
			Value: fmt.Sprintf("%s/%s",
				wsapisv1.ClusterDetailsConfigMountPath, configFileName),
		},
		{
			Name: wsapisv1.ClusterDetailsConfigSignalEnv,
			Value: fmt.Sprintf("%s/%s",
				wsapisv1.ClusterDetailsConfigMountPath, wsapisv1.ClusterDetailsConfigUpdatedSignal),
		},
	}
	for rType, spec := range updatedJob.Spec.WSReplicaSpecs {
		UpdateVolumeMount(spec, volume, volumeMount, envs)

		// add resource details ENV to CRD if generated
		var resourceEnvs []corev1.EnvVar
		if rType == wsapisv1.WSReplicaTypeCoordinator {
			if jobController.canCreateResourceDetailsV2(ctx, wsjob) {
				resourceEnvs = append(resourceEnvs, corev1.EnvVar{
					Name:  wsapisv1.WsJobResourceDetailsV2FPEnv,
					Value: fmt.Sprintf("%s/%s", wsapisv1.ClusterDetailsConfigMountPath, wsapisv1.WsJobResourceDetailsV2Name),
				})
			}
			if jobController.canCreateResourceDetails(ctx, wsjob) {
				resourceEnvs = append(resourceEnvs, corev1.EnvVar{
					Name:  wsapisv1.WsJobResourceDetailsFPEnv,
					Value: fmt.Sprintf("%s/%s", wsapisv1.ClusterDetailsConfigMountPath, wsapisv1.WsJobResourceDetailsName),
				})
			}
		}

		// Add resource envs to all containers if any were created
		if len(resourceEnvs) > 0 {
			for i := range spec.Template.Spec.Containers {
				spec.Template.Spec.Containers[i].Env = append(spec.Template.Spec.Containers[i].Env, resourceEnvs...)
			}
		}
	}
	updatedJob.Annotations[wsapisv1.ClusterDetailsAnno] = ""
	jobController.Log.Info("start updating wsjob for new cluster details")
	return false, jobController.Update(ctx, updatedJob)
}

// get WSE group size from cluster details configmap.
// requires cluster details configmap to be generated by cluster server.
// return wseGroupSize as int, non-positive values indicate invalid (not set)
func (jobController *WSJobController) getWseGroupSize(wsjob *wsapisv1.WSJob) (int, error) {
	wseGroupSize := 0
	if wsjob.CanServerRecognizeWesGroupSize() {
		logger := jobController.Log.WithValues(wsapisv1.Singular, wsjob.Name)
		_, configProtoMsg, err := jobController.getClusterDetailsConfig(wsjob)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to get cluster details configmap")
				return wseGroupSize, err
			}
		}
		if configProtoMsg == nil {
			// if operator needs to step in and generate cluster details config,
			// then there won't be group size anyway, so proceed
			return wseGroupSize, nil
		}
		clusterDetailsConfig := configProtoMsg.(*common_pb.ClusterDetails)
		for _, task := range clusterDetailsConfig.GetTasks() {
			if task.TaskType != common_pb.ClusterDetails_TaskInfo_WSE {
				continue
			}
			hasAddressBookSet := false
			for i, taskMap := range task.TaskMap {
				if taskMap.AddressBook == nil {
					continue
				}
				// verify that WSE group size is consistent across all WSEs in AddressBook
				// when we see any value (including 0), should be the same for all WSEs
				if hasAddressBookSet && wseGroupSize != int(taskMap.AddressBook.WseGroupSize) {
					err := CriticalErrorf("inconsistent WSE group size: expected %d, got %d",
						wseGroupSize, int(taskMap.AddressBook.WseGroupSize))
					logger.Error(err, "inconsistent WSE group size found in WSE TaskInfo",
						"wseGroupSize", wseGroupSize, "taskMapIdx", i,
						"taskMapWseGroupSize", taskMap.AddressBook.WseGroupSize)
					return wseGroupSize, err
				}
				hasAddressBookSet = true
				wseGroupSize = int(taskMap.AddressBook.WseGroupSize)
			}
		}
	}
	return wseGroupSize, nil
}

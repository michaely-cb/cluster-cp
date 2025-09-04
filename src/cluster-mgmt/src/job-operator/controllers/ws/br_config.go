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
	"strconv"

	common_pb "cerebras/pb/workflow/appliance/common"

	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	corev1 "k8s.io/api/core/v1"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	wscommon "cerebras.com/job-operator/common"
)

// reconcileBrConfig: generate/update BR config based the granted system resources
// return proceed boolean to indicate whether it's ok to keep reconciling
func (jobController *WSJobController) reconcileBrConfig(ctx context.Context,
	job *wsapisv1.WSJob, res jobResources) (bool, error) {
	logger := jobController.Log.WithValues(wsapisv1.Singular, job.Name)
	// only generate br config when lock switch from lockCondStateAwait to lockCondStateGranted
	systems := res.systems
	if len(systems) == 0 && !job.IsCompile() {
		logger.Info("no systems granted and not a compile job, skip br config generation", "res", res)
		return true, nil
	}

	// idempotent logic for already generated br config
	brCM, brConfig, err := jobController.getBRConfig(ctx, job)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to get br configmap")
		return false, err
	} else if err == nil {
		proceed, err := jobController.updateBRConfig(ctx, job, brConfig, brCM)
		return proceed, err
	}

	wseGroupSize, err := jobController.getWseGroupSize(job)
	if err != nil {
		logger.Error(err, "failed to get wse group size during reconcileBrConfig")
		return false, err
	}

	logger.Info("start generating br configmap", "wseGroupSize", wseGroupSize)
	brConfig = jobController.buildBrConfig(
		res,
		getMockSysAddrIfSet(job),
		len(res.systems) <= 1 || job.SkipBR(),
		wseGroupSize,
	)

	configData, err := json.Marshal(brConfig)
	if err != nil {
		logger.Error(err, "failed to marshal br configmap")
		return false, err
	}

	labels := jobController.GenLabels(job.Name)
	labels[wsapisv1.ConfigMapType] = wsapisv1.ConfigMapTypeBRConfig

	signal := wsapisv1.InitSignalVal
	if wsapisv1.DisableMultusForBR {
		signal = wsapisv1.UpdatedSignalVal
	}
	brConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(wsapisv1.BrConfigName, job.Name),
			Namespace: job.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			wsapisv1.BrConfigUpdatedSignal: signal,
		},
	}
	compressed, err := wscommon.Gzip(configData)
	if err != nil {
		logger.Error(err, "failed to compress br configmap")
		return false, err
	}
	brConfigMap.BinaryData = wscommon.EnsureMap(brConfigMap.BinaryData)
	brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName] = compressed

	if err := controllerutil.SetControllerReference(job, brConfigMap, jobController.Scheme); err != nil {
		logger.Error(err, "set br config controller reference error")
		return false, err
	}

	err = jobController.Create(ctx, brConfigMap)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		logger.Error(err, "failed to create br configmap")
		if wscommon.IsCriticalK8sError(err) {
			return false, CriticalErrorf("unable to create br_config: generated file is too large")
		}
		return false, err
	}
	logger.Info("generating br configmap done", "cm", brConfigMap.Name)
	return true, nil
}

// scale BuildBrLogicalTree by available ports to build br config
// If skipBRNodes is true, only construct `csNodes` section. For compile
// and inference jobs, we only need `csNodes` section.
func (jobController *WSJobController) buildBrConfig(res jobResources, mockCsAddr string, skipBRNodes bool, wseGroupSize int) *wscommon.BRConfig {
	brConfig := &wscommon.BRConfig{
		CSNodes:  []wscommon.CSNode{},
		BRGroups: []wscommon.BRGroup{},
		BRNodes:  []wscommon.BRNode{},
	}
	for i, system := range res.systems {
		addr := ""
		mgmtAddr := ""
		sys := jobController.Cfg.SystemMap[system]
		if sys != nil {
			addr = sys.CmAddr
			mgmtAddr = sys.MgmtAddr
		}
		if mockCsAddr != "" {
			addr = mockCsAddr
		}
		// example: for wseGroupSize = 4, the wseGroupId for 6 systems will be [0, 1, 2, 3, 0, 1],
		// meaning in br config, system 0 and 4 will be forming group 0 and receiving the same weight
		wseGroupId := 0
		if wseGroupSize > 0 {
			// for wseGroupSize, only positive value is valid
			// when cbcore sees all systems have wseGroupId as 0, it should treat expert parallel on weight as disabled
			wseGroupId = i % wseGroupSize
		}
		brConfig.CSNodes = append(brConfig.CSNodes, wscommon.CSNode{
			NodeId:     i,
			CmAddr:     addr,
			SystemName: system,
			MgmtAddr:   mgmtAddr,
			WseGroupId: wseGroupId,
		})
	}
	if skipBRNodes {
		return brConfig
	}

	portCount := common.CSVersionSpec.NumPorts
	nodeGrants := res.replicaToNodeMap[rlv1.BrRequestName]
	treeVal := res.replicaToAnnoMap[rlv1.BrRequestName][rlv1.BrLogicalTreeKey]
	var brs wscommon.BrLogicalTree
	if err := json.Unmarshal([]byte(treeVal), &brs); err != nil {
		jobController.Log.Error(err, "fail to parse br logical tree",
			"job", res.name, "treeVal", treeVal)
	}

	skipGroup := -1
	for set, br := range brs {
		for port := 0; port < portCount; port++ {
			brNodeId := set*portCount + port
			ingressPort := wsapisv1.DefaultPort
			group := common.CSVersionSpec.Group(port)
			if wsapisv1.DisableMultusForBR {
				insId := res.getAssignedInstanceId(wsapisv1.WSReplicaTypeBroadcastReduce, brNodeId)
				insIdInt, _ := strconv.Atoi(insId)
				ingressPort += insIdInt
			}
			nics := res.getAssignedNicsForReplica(wsapisv1.WSReplicaTypeBroadcastReduce, brNodeId)
			brNode := wscommon.BRNode{
				Comment:  fmt.Sprintf("SwarmX node: %s, Assigned NICs: %s", nodeGrants[brNodeId], nics),
				NodeId:   brNodeId,
				GroupId:  group,
				DomainId: common.CSVersionSpec.Domain(port),
				Egress:   make([]wscommon.Egress, len(br.Egresses)),
			}
			_, ok := res.replicaToNodeMap[rlv1.BrRequestName][brNodeId]
			if !ok {
				// set skip nodes if exists for partial mode
				brNode.Comment = "Dummy node"
				brNode.ControlAddr = wsapisv1.DummyIpPort
				brNode.IngressAddr = wsapisv1.DummyIp
				for k := range brNode.Egress {
					brNode.Egress[k].ControlAddr = wsapisv1.DummyIpPort
					brNode.Egress[k].EgressAddr = wsapisv1.DummyIp
					brNode.Egress[k].IsBr = br.Egresses[k].IsBr
				}
				skipGroup = group
			} else {
				assignedNo := jobController.Cfg.NodeMap[nodeGrants[brNodeId]]
				var ingressIp string
				if wsapisv1.DisableMultusForBR {
					ingressIp = assignedNo.GetNicAddress(nics[0])
				}
				brNode.ControlAddr = fmt.Sprintf("%s:%d", ingressIp, ingressPort)
				brNode.IngressAddr = ingressIp
				for k, egress := range br.Egresses {
					ctrlAddr := ""
					if !egress.IsBr {
						ctrlAddr = jobController.Cfg.SystemMap[egress.SystemName].CmAddr
					} else {
						if wsapisv1.DisableMultusForBR {
							ctrlAddr = brConfig.BRNodes[egress.BrSetId*portCount+port].ControlAddr
						} else {
							// point to downstream br node id for update later
							ctrlAddr = strconv.Itoa(egress.BrSetId*portCount + port)
						}
					}
					var egAddr string
					if wsapisv1.DisableMultusForBR {
						egAddr = jobController.Cfg.NodeMap[nodeGrants[brNodeId]].GetNicAddress(nics[k+1])
					}
					brNode.Egress[k] = wscommon.Egress{
						ControlAddr: ctrlAddr,
						EgressAddr:  egAddr,
						IsBr:        egress.IsBr,
					}
				}
			}
			brConfig.BRNodes = append(brConfig.BRNodes, brNode)
		}
	}

	// last set will be the top layer for WGT/CMD connection
	numDomains := common.CSVersionSpec.NumPortDomains
	numGroups := portCount / numDomains
	if len(brs) > 0 {
		setId := brs[len(brs)-1].SetId
		for i := 0; i < numGroups; i++ {
			brGroup := wscommon.BRGroup{
				GroupId:     i,
				Connections: []wscommon.BRConnection{},
			}
			for j := 0; j < numDomains; j++ {
				connection := wscommon.BRConnection{
					DomainId: j,
					// node id pattern for one group is like: 0,3,6,9 / 1,4,7,10 / 2,5,8,11
					// point to top level
					ControlAddr: brConfig.BRNodes[setId*portCount+j*numGroups+i].ControlAddr,
				}
				if !wsapisv1.DisableMultusForBR {
					// assign br node id for later update stage if using multus
					connection.ControlAddr = strconv.Itoa(setId*portCount + j*numGroups + i)
				}

				// set skip group if exists
				if skipGroup == i {
					connection.HasError = true
					connection.ControlAddr = wsapisv1.DummyIpPort
				}
				brGroup.Connections = append(brGroup.Connections, connection)
			}
			brConfig.BRGroups = append(brConfig.BRGroups, brGroup)
		}
	}
	return brConfig
}

func (jobController *WSJobController) updateBRConfig(ctx context.Context, job *wsapisv1.WSJob,
	brConfig *wscommon.BRConfig, brConfigMap *corev1.ConfigMap) (bool, error) {
	logger := jobController.Log.WithValues(wsapisv1.Singular, job.Name)

	// idempotent logic for already updated br config
	if brConfigMap.Data[wsapisv1.BrConfigUpdatedSignal] == wsapisv1.UpdatedSignalVal || len(brConfig.BRNodes) == 0 {
		return true, nil
	}

	expect := *job.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce].Replicas
	if expect <= 1 || job.Status.ReplicaStatuses == nil ||
		job.Status.ReplicaStatuses[wsapisv1.WSReplicaTypeBroadcastReduce] == nil ||
		job.Status.ReplicaStatuses[wsapisv1.WSReplicaTypeBroadcastReduce].Init != expect {
		logger.Info("skip BR config update till all BR initialized", "expect", expect)
		return true, nil
	}

	logger.Info("start updating br configmap", "job", job.Name)

	pods, err := jobController.getBrPods(ctx, job.Namespace, job.Name)
	if err != nil {
		logger.Error(err, "failed to list BR pods")
		return false, err
	}
	idToPodMap := map[string]*corev1.Pod{}
	for _, pod := range pods {
		id := ""
		for _, c := range pod.Spec.Containers {
			if c.Name == wsapisv1.DefaultContainerName {
				for _, env := range c.Env {
					if env.Name == wsapisv1.ReplicaId {
						id = env.Value
						break
					}
				}
				break
			}
		}
		idToPodMap[id] = pod
	}

	for i, brLogicalNode := range brConfig.BRNodes {
		pod, ok := idToPodMap[strconv.Itoa(brLogicalNode.NodeId)]
		if !ok {
			// skip nodes in partial mode
			continue
		}
		ips, err := jobController.GetPodDataIP(pod)
		if err != nil {
			return false, err
		}
		if len(ips) < len(brLogicalNode.Egress)+1 {
			logger.Info("BR multus ip not fully assigned yet",
				"expect", len(brLogicalNode.Egress)+1,
				"actual", len(ips),
				"pod", pod.Name,
				"ns", pod.Namespace)
			return false, nil
		}

		brConfig.BRNodes[i].ControlAddr = fmt.Sprintf("%s:%d", ips[0], wsapisv1.DefaultPort)
		brConfig.BRNodes[i].IngressAddr = ips[0]
		for j, egress := range brLogicalNode.Egress {
			brConfig.BRNodes[i].Egress[j].EgressAddr = ips[j+1]
			if egress.IsBr {
				nodeId, err := strconv.Atoi(egress.ControlAddr)
				if err != nil {
					logger.Error(err, "failed to parse BR node id")
					return false, err
				}
				brConfig.BRNodes[i].Egress[j].ControlAddr = brConfig.BRNodes[nodeId].ControlAddr
			}
		}
	}
	for i, group := range brConfig.BRGroups {
		for j, con := range group.Connections {
			if con.HasError {
				continue
			}
			nodeId, err := strconv.Atoi(con.ControlAddr)
			if err != nil {
				logger.Error(err, "failed to parse BR node id")
				return false, err
			}
			brConfig.BRGroups[i].Connections[j].ControlAddr = brConfig.BRNodes[nodeId].ControlAddr
		}
	}

	configData, err := json.Marshal(brConfig)
	if err != nil {
		logger.Error(err, "failed to marshal br configmap")
		return false, err
	}
	brConfigMap = brConfigMap.DeepCopy()
	if brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName] != nil {
		compressed, err := wscommon.Gzip(configData)
		if err != nil {
			logger.Error(err, "failed to compress br configmap")
			return false, err
		}
		brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName] = compressed
	} else {
		brConfigMap.Data[wsapisv1.BrConfigFileName] = string(configData)
	}
	brConfigMap.Data[wsapisv1.BrConfigUpdatedSignal] = wsapisv1.UpdatedSignalVal
	if err := jobController.Update(ctx, brConfigMap); err != nil {
		logger.Error(err, "failed to update br configmap")
		if wscommon.IsCriticalK8sError(err) {
			return false, CriticalErrorf(err.Error())
		}
		return false, err
	}
	logger.Info("successfully updated br configmap")

	return true, nil
}

func (jobController *WSJobController) getBRConfig(ctx context.Context,
	job *wsapisv1.WSJob) (*corev1.ConfigMap, *wscommon.BRConfig, error) {
	brConfigMap := &corev1.ConfigMap{}
	brConfig := &wscommon.BRConfig{}
	if err := jobController.Get(ctx, types.NamespacedName{
		Namespace: job.GetNamespace(), Name: fmt.Sprintf(wsapisv1.BrConfigName, job.Name),
	}, brConfigMap); err != nil {
		return brConfigMap, brConfig, err
	}

	if err := json.Unmarshal(wscommon.GetBrConfigData(brConfigMap), brConfig); err != nil {
		return brConfigMap, brConfig, err
	}
	return brConfigMap, brConfig, nil
}

func (jobController *WSJobController) getBrPods(ctx context.Context, namespace, name string) ([]*corev1.Pod, error) {
	labels := jobController.GenLabels(name)
	labels[wsReplicaTypeLabel] = wsapisv1.WSReplicaTypeBroadcastReduce.Lower()
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: labels})
	if err != nil {
		return nil, err
	}

	podList := &corev1.PodList{}
	err = jobController.List(ctx, podList,
		client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(namespace))
	if err != nil {
		return nil, err
	}
	return wscommon.PointerList(podList.Items), nil
}

func (jobController *WSJobController) updateWsJobByBrConfig(ctx context.Context,
	wsjob *wsapisv1.WSJob, res jobResources) (bool, error) {
	// idempotent check
	if _, ok := wsjob.Annotations[wsapisv1.BrConfigAnno]; ok {
		return true, nil
	}

	updatedJob := wsjob.DeepCopy()
	volume := corev1.Volume{
		Name: wsapisv1.BrConfigVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: fmt.Sprintf(wsapisv1.BrConfigName, updatedJob.Name),
				},
			},
		},
	}
	volumeMount := corev1.VolumeMount{
		Name:      wsapisv1.BrConfigVolume,
		MountPath: wsapisv1.BrConfigMountPath,
	}

	configFileName := wsapisv1.BrConfigCompressedFileName
	envs := []corev1.EnvVar{
		{
			Name: wsapisv1.BrConfigPathEnv,
			Value: fmt.Sprintf("%s/%s",
				wsapisv1.BrConfigMountPath, configFileName),
		},
		{
			Name: wsapisv1.BrConfigSignalEnv,
			Value: fmt.Sprintf("%s/%s",
				wsapisv1.BrConfigMountPath, wsapisv1.BrConfigUpdatedSignal),
		},
	}
	for _, spec := range updatedJob.Spec.WSReplicaSpecs {
		UpdateVolumeMount(spec, volume, volumeMount, envs)
	}

	// update replica count
	count := int32(len(res.replicaToNodeMap[rlv1.BrRequestName]))
	if count != 0 &&
		updatedJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce] != nil &&
		*updatedJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce].Replicas != count {
		updatedJob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeBroadcastReduce].Replicas = &count
	}

	updatedJob.Annotations[wsapisv1.BrConfigAnno] = ""
	jobController.Log.Info("start updating wsjob for new br config")
	return false, jobController.Update(ctx, updatedJob)
}

func UpdateVolumeMount(spec *commonv1.ReplicaSpec, volume corev1.Volume, volumeMount corev1.VolumeMount, envs []corev1.EnvVar) {
	// check if volume exists and replace for backwards compatible
	alreadyExists := false
	for i, v := range spec.Template.Spec.Volumes {
		if v.Name == volume.Name {
			spec.Template.Spec.Volumes[i] = volume
			alreadyExists = true
			break
		}
	}
	if !alreadyExists {
		spec.Template.Spec.Volumes = append(spec.Template.Spec.Volumes, volume)
	}

	for i, container := range spec.Template.Spec.Containers {
		// append envs anyway since it's idempotent
		spec.Template.Spec.Containers[i].Env =
			append(spec.Template.Spec.Containers[i].Env, envs...)
		if !alreadyExists {
			spec.Template.Spec.Containers[i].VolumeMounts =
				append(spec.Template.Spec.Containers[i].VolumeMounts, volumeMount)
		} else {
			for j, mount := range container.VolumeMounts {
				if mount.Name == volumeMount.Name {
					spec.Template.Spec.Containers[i].VolumeMounts[j] = volumeMount
					break
				}
			}
		}
	}
}

// Check if the debug INI specifies mock_cs2 config
func getMockSysAddrIfSet(wsjob *wsapisv1.WSJob) string {
	mockCsAddr := wsjob.Annotations[wsapisv1.MockCsAddressAnnot]
	// In rel-2.5, we moved the mock cs address preparation from job operator to cluster server.
	// TODO: Remove the following routine in rel-2.7 and directly return the annotation instead.
	if mockCsAddr != "" {
		return mockCsAddr
	}

	for _, spec := range wsjob.Spec.WSReplicaSpecs {
		for _, env := range spec.Template.Spec.Containers[0].Env {
			if env.Name == "WS_DEBUG_ARGS" {
				debugArgs := &common_pb.DebugArgs{}
				options := protojson.UnmarshalOptions{
					AllowPartial:   true,
					DiscardUnknown: true,
				}
				if err := options.Unmarshal([]byte(env.Value), debugArgs); err != nil {
					logrus.Error("failed to unmarshal WS_DEBUG_ARGS, unable to check for mock_cs2 INIs")
				} else if debugArgs.Ini != nil {
					mockCsAddr = debugArgs.Ini.Strings["ws_rt_use_existing_mock_cs2_ip_port"]
					if debugArgs.Ini.Bools["ws_rt_using_mock_cs2"] && mockCsAddr != "" && wsjob.IsCompile() {
						// If it's a compile job, make sure we fall back to the offline_compile/ fabric json
						mockCsAddr = wsapisv1.CompileOnlyCmAddr
					}
				}

				// WS_DEBUG_ARGS env var has the same value for all replicas
				// Checking against one replica is sufficient
				return mockCsAddr
			}
		}
	}
	return mockCsAddr
}

package pkg

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	commonpb "cerebras/pb/workflow/appliance/common"

	"golang.org/x/exp/slices"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

const (
	ClusterEnvConfigMapName = "job-operator-cluster-env"
)

// generateClusterDetailsConfig generates the configmap which respects the Framework's
// cluster details input.
func GenerateClusterDetailsConfig(
	ctx context.Context,
	kubeClientSet kubernetes.Interface,
	wsjob *wsapisv1.WSJob,
	clusterDetails *commonpb.ClusterDetails,
) error {
	parallelExpertsDetected := false
	var actToCsxDomains []string
	var kvssToCsxDomains []string
	var swDriverToCsxDomains []string
	wseTaskIncluded := false
	for _, task := range clusterDetails.GetTasks() {
		if task.TaskType == commonpb.ClusterDetails_TaskInfo_BR ||
			task.TaskType == commonpb.ClusterDetails_TaskInfo_UNKNOWN {
			// br has its own config and we don't care about unknown task info
			continue
		} else if task.TaskType == commonpb.ClusterDetails_TaskInfo_WSE {
			wseTaskIncluded = true
		} else {
			// Not all requests have taskmap or addressbook populated
			numReplicas := wsjob.Spec.WSReplicaSpecs[wsapisv1.TaskTypeClusterDetailsMapping[task.TaskType]].Replicas
			for i := 0; i < int(*numReplicas); i++ {
				var taskMap *commonpb.ClusterDetails_TaskInfo_TaskMap
				if i >= len(task.TaskMap) {
					taskMap = &commonpb.ClusterDetails_TaskInfo_TaskMap{
						TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
							TaskId: int32(i),
							WseId:  int32(-1),
							WseIds: []int32{-1},
						},
					}
					task.TaskMap = append(task.TaskMap, taskMap)
				} else {
					taskMap = task.TaskMap[i]
				}
				taskMap.TaskId.TaskId = int32(i)
				if taskMap.AddressBook == nil {
					taskMap.AddressBook = &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{}
				}
				taskMap.AddressBook.TaskIpPort = wsjob.GetDefaultTaskIpPort(
					wsapisv1.TaskTypeClusterDetailsMapping[task.TaskType], task.TaskMap[i].TaskId.TaskId)

				if task.TaskType == commonpb.ClusterDetails_TaskInfo_ACT {
					if len(task.TaskMap[i].TaskId.GetWseIds()) > 1 {
						parallelExpertsDetected = true
					}
					// As of rel-2.5, appliance client may not necessarily set the wse domains for non ACT-0.
					// Cluster server will hydrate the missing fields for now because that's what the operator
					// had done in the past. Cluster details plumbing got updated in Stack as long term fix
					// when this gap was identified.
					// TODO: Remove this wse domains hydration in rel-2.7.
					if i > 0 && len(taskMap.AddressBook.WseDomains) == 0 && task.TaskMap[0].AddressBook != nil {
						taskMap.AddressBook.WseDomains = task.TaskMap[0].AddressBook.WseDomains
					}
					actToCsxDomains = append(actToCsxDomains, wscommon.SerializeWioTaskMap(taskMap))
				} else if task.TaskType == commonpb.ClusterDetails_TaskInfo_KVSS {
					kvssToCsxDomains = append(kvssToCsxDomains, wscommon.SerializeWioTaskMap(taskMap))
				} else if task.TaskType == commonpb.ClusterDetails_TaskInfo_SWD {
					swDriverToCsxDomains = append(swDriverToCsxDomains, wscommon.SerializeWioTaskMap(taskMap))
				} else if wsjob.IsSdk() && task.TaskType == commonpb.ClusterDetails_TaskInfo_WRK && slices.Equal(taskMap.TaskId.WseIds, []int32{-1}) {
					// As of rel-2.5, SDK appliance client does not set the WSE ID for workers.
					// Cluster server will hydrate the WSE ID for now because that's what the operator had done
					// in the past. Cluster details plumbing got updated in SDK appliance client as long term fix
					// when this gap was identified.
					// For context, SDK execute jobs only have one worker pod and will only use one wafer.
					// TODO: Remove this wse ids hydration in rel-2.7.
					taskMap.TaskId.TaskId = 0
					taskMap.TaskId.WseId = 0
					taskMap.TaskId.WseIds = []int32{0}
				}
			}
		}
	}

	// As of rel-2.5, SDK appliance client does not set WSE tasks.
	// Cluster server will hydrate the WSE task for now because that's what the operator had done in the past.
	// Cluster details plumbing got updated in SDK appliance client as long term fix when this gap was identified.
	// For context, SDK execute jobs only have one worker pod and will only use one wafer.
	// TODO: Remove this wse ids hydration in rel-2.7.
	if wsjob.IsSdk() && wsjob.IsExecute() && !wseTaskIncluded {
		clusterDetails.Tasks = append(clusterDetails.Tasks, &commonpb.ClusterDetails_TaskInfo{
			TaskType: commonpb.ClusterDetails_TaskInfo_WSE,
			TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{{
				TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
					TaskId: int32(0),
					WseId:  int32(0),
					WseIds: []int32{0},
				},
				AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{},
			}},
		})
	}

	operatorSemverStr := ""
	if wsjob.Annotations != nil {
		operatorSemverStr = wsjob.Annotations[wsapisv1.SemanticJobOperatorVersion]
	}
	err := operatorSupports(operatorSemverStr, wsapisv1.ParallelExperts)
	if err != nil {
		if parallelExpertsDetected {
			log.Warnf("Wsjob %s requested parallel experts feature but "+
				"job operator does not support it: %s", wsjob.Name, err)
			return err
		}
		// skip cm creation since old operator doesn't work well with server-side created cm
		log.Warnf("skip cm creation for wsjob %s since since old operator doesn't work well with server-side created cm: %s", wsjob.Name, err)
		return nil
	}

	emitUnpopulated := true
	// Linear memory range is added in 1.0.13
	// So we set emitUnpopulated to false for space conservation
	if err = operatorSupports(wsjob.Annotations[wsapisv1.SemanticJobOperatorVersion], wsapisv1.LinearMemoryRangeSupport); err != nil {
		emitUnpopulated = false
	}

	options := protojson.MarshalOptions{
		EmitUnpopulated: emitUnpopulated,
	}
	clusterDetailsData, err := options.Marshal(clusterDetails)
	if err != nil {
		log.Error(err, "failed to convert cluster details protobuf to json")
		return MapStatusError(err)
	}

	clusterDetailsConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, wsjob.Name),
			Namespace: wsjob.Namespace,
			Annotations: map[string]string{
				// Used to debug if the cluster details was created by server
				// TODO: Deprecate this annotation in rel-2.7
				"server-generated": strconv.FormatBool(true),
			},
			Labels: map[string]string{
				"job-name":             wsjob.Name,
				wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeClusterDetails,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "jobs.cerebras.com/v1",
				BlockOwnerDeletion: wscommon.Pointer(true),
				Controller:         wscommon.Pointer(true),
				Kind:               "WSJob",
				Name:               wsjob.Name,
				UID:                wsjob.ObjectMeta.UID,
			}},
		},
		Data: map[string]string{
			wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.InitSignalVal,
		},
	}
	compressed, err := wscommon.Gzip(clusterDetailsData)
	if err != nil {
		log.Error(err, "failed to compress cluster details")
		return MapStatusError(err)
	}
	clusterDetailsConfigMap.BinaryData = wscommon.EnsureMap(clusterDetailsConfigMap.BinaryData)
	clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName] = compressed

	if len(actToCsxDomains) > 0 {
		actToCsxDomainsStr := strings.Join(actToCsxDomains, ",")
		compressed, err = wscommon.Gzip([]byte(actToCsxDomainsStr))
		if err != nil {
			log.Error(err, "failed to compress activation to csx domains %s", actToCsxDomainsStr)
			return MapStatusError(err)
		}
		clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsActToCsxDomains] = compressed
	}

	if len(kvssToCsxDomains) > 0 {
		kvssToCsxDomainsStr := strings.Join(kvssToCsxDomains, ",")
		compressed, err = wscommon.Gzip([]byte(kvssToCsxDomainsStr))
		if err != nil {
			log.Error(err, "failed to compress kvss to csx domains %s", kvssToCsxDomainsStr)
			return MapStatusError(err)
		}
		clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsKvssToCsxDomains] = compressed
	}

	// As of Apr 2025, we will only have one software driver
	// The multi software driver support is mostly to future proof the codebase
	if len(swDriverToCsxDomains) > 0 {
		swDriverToCsxDomainsStr := strings.Join(swDriverToCsxDomains, ",")
		compressed, err = wscommon.Gzip([]byte(swDriverToCsxDomainsStr))
		if err != nil {
			log.Error(err, "failed to compress software driver to csx domains %s", swDriverToCsxDomainsStr)
			return MapStatusError(err)
		}
		clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsSWDriverToCsxDomains] = compressed
	}

	_, err = kubeClientSet.CoreV1().ConfigMaps(wsjob.Namespace).Create(ctx, clusterDetailsConfigMap, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("error creating cluster details configmap for %s: %v", wsjob.Name, err)
		return MapStatusError(err)
	}
	log.Infof("successfully generated cluster details configmap for %s", wsjob.Name)
	return nil
}

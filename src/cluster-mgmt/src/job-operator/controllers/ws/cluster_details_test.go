//go:build default

package ws

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	common_pb "cerebras/pb/workflow/appliance/common"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	v1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

func TestGenerateClusterDetails(t *testing.T) {
	t.Run("singlebox_cluster_details_generation", func(t *testing.T) {
		cmAddr := "localhost:30000"
		wsjob := wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name: "testjob0",
			},
			Spec: wsapisv1.WSJobSpec{
				NumWafers: 1,
				WSReplicaSpecs: map[v1.ReplicaType]*v1.ReplicaSpec{
					wsapisv1.WSReplicaTypeCoordinator: {Replicas: wscommon.Pointer(int32(1))},
					wsapisv1.WSReplicaTypeWorker:      {Replicas: wscommon.Pointer(int32(4))},
					wsapisv1.WSReplicaTypeActivation:  {Replicas: wscommon.Pointer(int32(2))},
					wsapisv1.WSReplicaTypeChief:       {Replicas: wscommon.Pointer(int32(1))},
					wsapisv1.WSReplicaTypeWeight:      {Replicas: wscommon.Pointer(int32(1))},
					wsapisv1.WSReplicaTypeCommand:     {Replicas: wscommon.Pointer(int32(1))},
				},
			},
		}
		fakeSysName := "fake-sys"
		systemMap := map[string]*systemv1.SystemSpec{
			fakeSysName: {
				Name:   fakeSysName,
				CmAddr: cmAddr,
			},
		}
		res := jobResources{
			systems: []string{fakeSysName},
		}
		clusterDetails, _ := generateClusterDetails(&wsjob, res, systemMap)

		for _, taskType := range []common_pb.ClusterDetails_TaskInfo_TaskType{
			common_pb.ClusterDetails_TaskInfo_ACT,
			common_pb.ClusterDetails_TaskInfo_CHF,
			common_pb.ClusterDetails_TaskInfo_WRK,
			common_pb.ClusterDetails_TaskInfo_WSE,
		} {
			tasks := getTasksByType(clusterDetails, taskType)
			require.Equal(t, 1, len(tasks))
			for _, taskMap := range tasks[0].TaskMap {
				assert.Equal(t, taskMap.AddressBook.WseIp, cmAddr)
			}
		}
	})

	t.Run("multibox_cluster_details_generation", func(t *testing.T) {
		fakeSysName0 := "fake-sys-0"
		fakeSysName1 := "fake-sys-1"
		cmAddr0 := "localhost:30000"
		cmAddr1 := "localhost:30001"
		numWafers := 2
		wsjob := wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name: "testjob2",
			},
			Spec: wsapisv1.WSJobSpec{
				NumWafers:       uint32(numWafers),
				ActToCsxDomains: defaultActToCsxDomains,
				WSReplicaSpecs: map[v1.ReplicaType]*v1.ReplicaSpec{
					wsapisv1.WSReplicaTypeCoordinator: {Replicas: wscommon.Pointer(int32(1))},
					wsapisv1.WSReplicaTypeWorker:      {Replicas: wscommon.Pointer(int32(numWafers * len(defaultActToCsxDomains) * 2))},
					wsapisv1.WSReplicaTypeActivation:  {Replicas: wscommon.Pointer(int32(numWafers * len(defaultActToCsxDomains)))},
					wsapisv1.WSReplicaTypeChief:       {Replicas: wscommon.Pointer(int32(numWafers))},
					wsapisv1.WSReplicaTypeWeight:      {Replicas: wscommon.Pointer(int32(numWafers))},
					wsapisv1.WSReplicaTypeCommand:     {Replicas: wscommon.Pointer(int32(1))},
				},
			},
		}
		systemMap := map[string]*systemv1.SystemSpec{
			fakeSysName0: {
				Name:   fakeSysName0,
				CmAddr: cmAddr0,
			},
			fakeSysName1: {
				Name:   fakeSysName1,
				CmAddr: cmAddr1,
			},
		}
		res := jobResources{
			systems: []string{fakeSysName0, fakeSysName1},
		}
		clusterDetails, _ := generateClusterDetails(&wsjob, res, systemMap)
		for _, taskType := range []common_pb.ClusterDetails_TaskInfo_TaskType{
			common_pb.ClusterDetails_TaskInfo_CRD,
			common_pb.ClusterDetails_TaskInfo_WRK,
			common_pb.ClusterDetails_TaskInfo_ACT,
			common_pb.ClusterDetails_TaskInfo_CHF,
			common_pb.ClusterDetails_TaskInfo_WGT,
			common_pb.ClusterDetails_TaskInfo_CMD,
		} {
			assert.Equal(t, 1, len(getTasksByType(clusterDetails, taskType)))
		}

		firstNg := int32(0)
		secondNg := int32(1)
		for _, taskType := range []common_pb.ClusterDetails_TaskInfo_TaskType{
			common_pb.ClusterDetails_TaskInfo_ACT,
			common_pb.ClusterDetails_TaskInfo_CHF,
			common_pb.ClusterDetails_TaskInfo_WRK,
			common_pb.ClusterDetails_TaskInfo_WSE,
		} {
			// The first half replicas should talk to the one system, and the second half replicas
			// should talk to the other system.
			tasks := getTasksByType(clusterDetails, taskType)
			for i, taskMap := range tasks[0].TaskMap {
				firstHalf := i < len(tasks[0].TaskMap)/2
				assert.Equal(t, wscommon.Ternary(firstHalf, firstNg, secondNg), taskMap.TaskId.WseId)
				assert.Equal(t, wscommon.Ternary(firstHalf, cmAddr0, cmAddr1), taskMap.AddressBook.WseIp)
				if taskType == common_pb.ClusterDetails_TaskInfo_ACT {
					assert.Equal(t, int32(i%len(defaultActToCsxDomains)), taskMap.AddressBook.WseDomains[0])
				} else if taskType == common_pb.ClusterDetails_TaskInfo_WSE {
					assert.Equal(t, wscommon.Ternary(firstHalf, fakeSysName0, fakeSysName1), taskMap.AddressBook.TaskNodeName)
				}
			}
		}
	})
}

func getTasksByType(
	clusterDetails *common_pb.ClusterDetails,
	taskType common_pb.ClusterDetails_TaskInfo_TaskType) []*common_pb.ClusterDetails_TaskInfo {
	tasks := wscommon.Filter(func(detail *common_pb.ClusterDetails_TaskInfo) bool {
		return detail.TaskType == taskType
	}, clusterDetails.Tasks)
	return tasks
}

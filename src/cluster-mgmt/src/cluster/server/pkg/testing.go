package pkg

import (
	"cerebras.com/job-operator/common"
	commonpb "cerebras/pb/workflow/appliance/common"
)

func MockJobMode(jobMode commonpb.JobMode_Job, parallelExperts bool) *commonpb.JobMode {
	mockActKvssTaskMap := []*commonpb.ClusterDetails_TaskInfo_TaskMap{
		{
			TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
				TaskId: 0,
				WseIds: common.Ternary(parallelExperts, []int32{0, 1}, []int32{0}),
			},
			AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
				WseIp:      "~ws_kapi_model~:0",
				WseDomains: []int32{2, 3},
			},
		},
		{
			TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
				TaskId: 1,
				WseIds: common.Ternary(parallelExperts, []int32{0, 1}, []int32{1}),
			},
			AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
				WseIp:      "~ws_kapi_model~:0",
				WseDomains: []int32{1, 4},
			},
		},
	}
	mockNonActTaskMap := []*commonpb.ClusterDetails_TaskInfo_TaskMap{
		{
			TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
				WseIds: []int32{0},
				TaskId: 0,
			},
			AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
				WseIp: "~ws_kapi_model~:0",
			},
		},
	}
	mapTaskType := func(tt commonpb.ClusterDetails_TaskInfo_TaskType) *commonpb.ClusterDetails_TaskInfo {
		return &commonpb.ClusterDetails_TaskInfo{
			TaskType: tt,
			TaskMap:  Ternary(tt == commonpb.ClusterDetails_TaskInfo_ACT || tt == commonpb.ClusterDetails_TaskInfo_KVSS, mockActKvssTaskMap, mockNonActTaskMap),
			ResourceInfo: &commonpb.ResourceInfo{
				MemoryBytes:         128 << 20,
				MemoryBytesMaxLimit: 512 << 20,
			},
		}
	}

	var taskInfos []commonpb.ClusterDetails_TaskInfo_TaskType
	if jobMode == commonpb.JobMode_COMPILE || jobMode == commonpb.JobMode_INFERENCE_COMPILE {
		taskInfos = []commonpb.ClusterDetails_TaskInfo_TaskType{
			commonpb.ClusterDetails_TaskInfo_CRD,
		}
	} else {
		taskInfos = []commonpb.ClusterDetails_TaskInfo_TaskType{
			commonpb.ClusterDetails_TaskInfo_ACT,
			commonpb.ClusterDetails_TaskInfo_CHF,
			commonpb.ClusterDetails_TaskInfo_CMD,
			commonpb.ClusterDetails_TaskInfo_CRD,
			commonpb.ClusterDetails_TaskInfo_WGT,
			commonpb.ClusterDetails_TaskInfo_WRK,
			commonpb.ClusterDetails_TaskInfo_KVSS,
			commonpb.ClusterDetails_TaskInfo_WSE,
		}
	}
	return &commonpb.JobMode{
		Job: jobMode,
		ClusterDetails: &commonpb.ClusterDetails{
			Tasks: common.Map(mapTaskType, taskInfos),
		},
	}
}

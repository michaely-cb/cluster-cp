//go:build default

/*
Copyright 2023 Cerebras Systems, Inc.

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

package common

import (
	"fmt"
	"math/rand"
	"testing"

	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestGetNodeHealth(t *testing.T) {
	node := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:    ClusterMgmtConditionPrefix + NodeErrorAlertType + "_false",
					Status:  corev1.ConditionFalse,
					Message: "",
				},
				{
					Type:    ClusterMgmtConditionPrefix + NodeErrorAlertType + "_test1",
					Status:  corev1.ConditionTrue,
					Message: "test1 error",
				},
				{
					Type:    ClusterMgmtConditionPrefix + NodeNICAlertType + "_eth0",
					Status:  corev1.ConditionTrue,
					Message: "nic error",
				},
				{
					Type:    ClusterMgmtConditionPrefix + NodeErrorAlertType + "_test2",
					Status:  corev1.ConditionTrue,
					Message: "test2 error",
				},
				{
					Type:    CsadmConditionPrefix + "_test_healthy",
					Status:  corev1.ConditionFalse,
					Message: "should still include message when condition is false",
				},
				{
					Type:    CsadmConditionPrefix + "_test_empty",
					Status:  corev1.ConditionFalse,
					Message: "", // should not contain empty condition messages
				},
			},
		},
	}

	cfgNode := &Node{
		NICs: []*NetworkInterface{},
	}
	healthy, _, msg := GetNodeHealth(node, cfgNode)
	require.False(t, healthy)
	require.Equal(t, "alert fired: test1 error; alert fired: nic error; alert fired: test2 error; should still include message when condition is false", msg)

	healthy, _, msg = GetNodeHealth(node, nil)
	require.False(t, healthy)
	require.Equal(t, "node does not exist in cluster config", msg)
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(length int) (string, error) {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	for i, b := range bytes {
		bytes[i] = charset[b%byte(len(charset))]
	}
	return string(bytes), nil
}

func TestCompress(t *testing.T) {
	length := 1024 * 1024
	str, err := randomString(length)
	require.NoError(t, err)
	randomBytes := []byte(str)

	compressed, err := Gzip(randomBytes)
	require.NoError(t, err)
	uncompressed, err := Gunzip(compressed)
	require.NoError(t, err)
	require.Equal(t, randomBytes, uncompressed)
	t.Log(len(randomBytes))
	t.Log(len(compressed))
}

func TestScaleAndRoundUp(t *testing.T) {
	for _, tc := range []struct {
		quantity int64
		scale    int64
		divisor  int64
		expected int64
		err      bool
	}{
		{1024, 1, 1, 1024, false},                                    // Base case
		{1024, 5, 10, 512, false},                                    // Small trivial integers
		{1024, 10, 100, 102, false},                                  // Rounding
		{1024, 10, 1000, 10, false},                                  // Rounding
		{1024, 1000, 0, 0, true},                                     // Divisor is 0
		{92233720368547, 50000000, 1000, 4611686018427350016, false}, // Large numbers -- correctly rounded when intermediate multiply is greater than 2^63
	} {
		t.Run(fmt.Sprintf("%d/%d/%d", tc.quantity, tc.scale, tc.divisor), func(t *testing.T) {
			actual, err := ScaleAndRoundUp(tc.quantity, tc.divisor, tc.scale)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, actual)
			}
		})
	}
}

func TestLogProtoAsJson(t *testing.T) {
	jobMode := commonpb.JobMode{
		ClusterDetails: &commonpb.ClusterDetails{
			Tasks: []*commonpb.ClusterDetails_TaskInfo{{
				ResourceInfo: nil,
				TaskType:     commonpb.ClusterDetails_TaskInfo_ACT,
				TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
					{
						TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
							WseId:  0,
							TaskId: 0,
							WseIds: []int32{0},
						},
						AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
							WseIp:      "~ws_kapi_model~:0",
							WseDomains: []int32{2, 3},
						},
					},
					{
						TaskId: &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{
							WseId:  0,
							TaskId: 1,
							WseIds: []int32{0},
						},
						AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{
							WseIp:      "~ws_kapi_model~:0",
							WseDomains: []int32{1, 4},
						},
					},
				},
				TaskCommand: "ws-srv -d",
			}},
		},
	}
	task0 := jobMode.GetClusterDetails().GetTasks()[0]
	actual := LogProtoAsJson(task0, false)
	expected := `{"taskCommand":"ws-srv -d","taskMap":[{"addressBook":{"wseDomains":[2,3],"wseIp":"~ws_kapi_model~:0"},"taskId":{"wseIds":[0]}},{"addressBook":{"wseDomains":[1,4],"wseIp":"~ws_kapi_model~:0"},"taskId":{"taskId":1,"wseIds":[0]}}],"taskType":"ACT"}`
	assert.Equal(t, expected, actual)

	actual = LogProtoAsJson(task0, true)
	expected = `{"resourceInfo":null,"taskCommand":"ws-srv -d","taskMap":[{"addressBook":{"taskCommPorts":[],"taskDebugAddress":"","taskIpPort":"","taskNodeName":"","userSidecarAddress":"","wseDomains":[2,3],"wseGroupSize":0,"wseIp":"~ws_kapi_model~:0","wseNstreams":[]},"taskId":{"taskId":0,"wseId":0,"wseIds":[0]}},{"addressBook":{"taskCommPorts":[],"taskDebugAddress":"","taskIpPort":"","taskNodeName":"","userSidecarAddress":"","wseDomains":[1,4],"wseGroupSize":0,"wseIp":"~ws_kapi_model~:0","wseNstreams":[]},"taskId":{"taskId":1,"wseId":0,"wseIds":[0]}}],"taskSidecarCommand":"","taskType":"ACT"}`
	assert.Equal(t, expected, actual)
}

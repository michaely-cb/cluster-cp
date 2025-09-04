// Copyright 2019 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"testing"

	apiv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

func TestGenGeneralName(t *testing.T) {
	tcs := []struct {
		index        string
		key          string
		replicaType  apiv1.ReplicaType
		expectedName string
	}{
		{
			index:        "1",
			key:          "1/2/3/4/5",
			replicaType:  "worker",
			expectedName: "1-2-3-4-5-worker-1",
		},
		{
			index:        "1",
			key:          "1/2/3/4/5",
			replicaType:  "WORKER",
			expectedName: "1-2-3-4-5-worker-1",
		},
	}

	for _, tc := range tcs {
		actual := GenGeneralName(tc.key, string(tc.replicaType), tc.index)
		if actual != tc.expectedName {
			t.Errorf("Expected name %s, got %s", tc.expectedName, actual)
		}
	}
}

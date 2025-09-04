//go:build default

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

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func TestMetric(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(jobPhaseMetric); err != nil {
		panic(fmt.Errorf("registering collector failed: %w", err))
	}

	JobPhaseMetricAdded("ns", "job1", "Initializing")
	JobPhaseMetricAdded("ns", "job1", "Running")
	got, err := reg.Gather()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, 2, len(got[0].Metric))
	metricStr := proto.MarshalTextString(got[0].Metric[0])
	//t.Log(metricStr)
	assert.Contains(t, metricStr, "Initializing")
	assert.Contains(t, metricStr, "value: 1")
	metricStr = proto.MarshalTextString(got[0].Metric[1])
	assert.Contains(t, metricStr, "Running")
	assert.Contains(t, metricStr, "value: 1")
}

func TestGetSystemVersions(t *testing.T) {
	jsonVersion, _ := json.Marshal(map[string]map[string]string{
		"product": {
			"version": "2.5.0",
			"buildid": "build1",
		},
	})

	mockData := model.Vector{
		&model.Sample{
			Metric: model.Metric{
				"system":   "system0",
				"software": "",
				"version":  "2.4.0",
				"buildid":  "build0",
			},
			Value: 1,
		},
		&model.Sample{
			Metric: model.Metric{
				"system":   "system1",
				"software": model.LabelValue(jsonVersion),
			},
			Value: 1,
		},
	}

	mockQuerier := &MockQuerier{Values: []model.Value{mockData}}
	systemVersions, releaseVersionMap := GetSystemVersions(context.Background(), mockQuerier, nil, true, true)
	assert.Equal(t, "2.4.0:build0", systemVersions["system0"])
	assert.Equal(t, "2.5.0:build1", systemVersions["system1"])
	assert.Contains(t, releaseVersionMap["2.4.0"], "system0")
	assert.Contains(t, releaseVersionMap["2.5.0"], "system1")

	mockQuerier = &MockQuerier{Values: []model.Value{mockData}}
	systemVersions, releaseVersionMap = GetSystemVersions(context.Background(), mockQuerier, nil, true, false)
	assert.Equal(t, "2.4.0", systemVersions["system0"])
	assert.Equal(t, "2.5.0", systemVersions["system1"])
	assert.Contains(t, releaseVersionMap["2.4.0"], "system0")
	assert.Contains(t, releaseVersionMap["2.5.0"], "system1")

	mockQuerier = &MockQuerier{Values: []model.Value{mockData}}
	systemVersions, releaseVersionMap = GetSystemVersions(context.Background(), mockQuerier, nil, false, false)
	assert.Equal(t, "{\"buildid\":\"build0\",\"execmode\":\"\",\"version\":\"2.4.0\"}", systemVersions["system0"])
	assert.Equal(t, string(jsonVersion), systemVersions["system1"])
}

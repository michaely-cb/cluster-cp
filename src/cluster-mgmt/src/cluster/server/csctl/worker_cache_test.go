//go:build !e2e

package csctl

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fakeclient "k8s.io/client-go/kubernetes/fake"

	wscommon "cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
)

func TestListWorkerCache(t *testing.T) {
	k8s := fakeclient.NewSimpleClientset()
	mocks := pkg.NewNodeStoreMocks().
		MockNodeNamedAddressed("worker-node-0", "", "10.254.36.48", "worker", "g0", 64<<10, 64).
		MockNodeNamedAddressed("worker-node-1", "", "10.254.12.96", "worker", "g0", 128<<10, 64)

	for _, testcase := range []struct {
		name                    string
		mockedPromResult        model.Value
		expectedWorkerCacheList *WorkerCacheList
	}{
		{
			"should return a list",
			model.Vector{
				&model.Sample{
					Value: model.SampleValue(65.65292502853409),
					Metric: map[model.LabelName]model.LabelValue{
						model.InstanceLabel: "10.254.12.96:9100",
					},
				},
				&model.Sample{
					Value: model.SampleValue(85.65292502853409),
					Metric: map[model.LabelName]model.LabelValue{
						model.InstanceLabel: "10.254.36.48:9100",
					},
				},
			},
			&WorkerCacheList{
				items: []*WorkerCache{
					{
						node:             "worker-node-0",
						diskUsagePercent: "85.65%",
					},
					{
						node:             "worker-node-1",
						diskUsagePercent: "65.65%",
					},
				},
			},
		},
		{
			"should show data unavailable in the event a node exporter did not return the reading",
			model.Vector{
				&model.Sample{
					Value: model.SampleValue(65.65292502853409),
					Metric: map[model.LabelName]model.LabelValue{
						model.InstanceLabel: "10.254.12.96:9100",
					},
				},
			},
			&WorkerCacheList{
				items: []*WorkerCache{
					{
						node:             "worker-node-0",
						diskUsagePercent: "unknown",
					},
					{
						node:             "worker-node-1",
						diskUsagePercent: "65.65%",
					},
				},
			},
		},
		{
			"should return only worker nodes in the cache and ignore readings from other nodes",
			model.Vector{
				&model.Sample{
					Value: model.SampleValue(1.08),
					Metric: map[model.LabelName]model.LabelValue{
						model.InstanceLabel: "10.254.0.1:9100",
					},
				},
				&model.Sample{
					Value: model.SampleValue(2.09),
					Metric: map[model.LabelName]model.LabelValue{
						model.InstanceLabel: "10.254.0.2:9100",
					},
				},
				&model.Sample{
					Value: model.SampleValue(85.65292502853409),
					Metric: map[model.LabelName]model.LabelValue{
						model.InstanceLabel: "10.254.36.48:9100",
					},
				},
			},
			&WorkerCacheList{
				items: []*WorkerCache{
					{
						node:             "worker-node-0",
						diskUsagePercent: "85.65%",
					},
					{
						node:             "worker-node-1",
						diskUsagePercent: "unknown",
					},
				},
			},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			wcs := &WorkerCacheStore{
				kubeClient:     k8s,
				metricsQuerier: &wscommon.MockQuerier{Values: []model.Value{testcase.mockedPromResult}},
				ipToWorkerCacheMap: map[string]*WorkerCache{
					"10.254.12.96": {node: "worker-node-1"},
					"10.254.36.48": {node: "worker-node-0"},
				},
				nodeLister: mocks,
			}
			res, err := wcs.List(context.TODO(), wscommon.DefaultNamespace, nil)
			require.NoError(t, err)
			actualList := res.(*WorkerCacheList)
			assert.ElementsMatch(t, testcase.expectedWorkerCacheList.items, actualList.items)
		})
	}
}

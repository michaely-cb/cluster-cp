//go:build default

package resource

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
)

func Test_ScoreMatchedResource(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		groupReqId                   string
		resource                     Resource
		request                      Request
		resourceAllocation           *SubresourceAllocation
		expectedRequestScoringOrder  []Request
		expectedResourceScoringOrder []Resource
	}{
		{
			name:       "CRD min packing/max left over",
			groupReqId: "",
			resource:   newResource("node/n0", "role-coordinator:", "cpu:128000,memory:128000,pod:32"),
			expectedRequestScoringOrder: []Request{
				newSRRequest("memory:120000", "pod:1"),
				newSRRequest("memory:8000", "pod:1"),
				newSRRequest("memory:2000", "pod:1"),
			},
		},
		{
			name:       "CRD memory given priority",
			groupReqId: "",
			resource:   newResource("node/n0", "role-coordinator:", "cpu:128000,memory:128000,pod:32"),
			expectedRequestScoringOrder: []Request{
				newSRRequest("cpu:8000", "memory:120000", "pod:1"),
				newSRRequest("cpu:120000", "memory:8000", "pod:1"),
			},
		},
		{
			name:       "unallocated - WGT minimal left over",
			groupReqId: "ng-primary-0",
			resource:   newResource("node/n0", "role-weight:", "cpu:128000,memory:128000,pod:32"),
			expectedRequestScoringOrder: []Request{
				newSRRequest("cpu:2000", "memory:2000", "pod:1"),
				newSRRequest("cpu:8000", "memory:8000", "pod:1"),
				newSRRequest("cpu:120000", "memory:120000", "pod:1"),
			},
		},
		{
			name:               "allocated - WGT minimal packing/max left over",
			groupReqId:         "ng-secondary",
			resource:           newResource("node/n0", "role-weight:", "cpu:128000,memory:128000,pod:32"),
			resourceAllocation: &SubresourceAllocation{Subresources: Subresources{"pod": 1}},
			expectedRequestScoringOrder: []Request{
				newSRRequest("cpu:1", "memory:120000", "pod:1"),
				newSRRequest("cpu:800", "memory:8000", "pod:1"),
				newSRRequest("cpu:2000", "memory:2000", "pod:1"),
			},
		},
		{
			name:       "1:2 mode max packing on BR",
			groupReqId: rlv1.BrRequestName,
			request:    newSRRequest("cs-port:300", "pod:1"),
			expectedResourceScoringOrder: []Resource{
				newResource("node/n0", "role-broadcastreduce:", "cs-port:500,pod:32"),
				newResource("node/n0", "role-broadcastreduce:", "cs-port:600,pod:32"),
				newResource("node/n0", "role-broadcastreduce:", "cs-port:300,pod:32"),
			},
		},
		{
			name:       "prefer healthy nodes for non-sharable modes, e.g. 1:3/1:4",
			groupReqId: rlv1.BrRequestName,
			request:    newSRRequest("cs-port:400", "pod:1"),
			expectedResourceScoringOrder: []Resource{
				newResource("node/n0", "role-broadcastreduce:,health:degraded", "cs-port:400,pod:32"),
				newResource("node/n0", "role-broadcastreduce:,health:degraded", "cs-port:500,pod:32"),
				newResource("node/n0", "role-broadcastreduce:", "cs-port:600,pod:32"),
			},
		},
		{
			name:       "max left over on system res",
			groupReqId: "",
			request:    newSRRequest("port:8"),
			expectedResourceScoringOrder: []Resource{
				newResource("system/s0", "", "port:11"),
				newResource("system/s0", "", "port:12"),
			},
		},
		{
			name:       "system with affinity",
			groupReqId: "",
			request:    newSRRequest("port:8"),
			expectedResourceScoringOrder: []Resource{
				newResource("system/s0", "", "port:11"),
				newResource("system/s0", "", "port:12"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.expectedResourceScoringOrder) <= 1 {
				if tc.resourceAllocation != nil {
					tc.resource.Allocate(nil, *tc.resourceAllocation)
				}
				previousScore := -1
				preReq := map[string]int{}
				for _, request := range tc.expectedRequestScoringOrder {
					score, _ := request.scoreMatchedResource(tc.groupReqId, tc.resource).getOnlyScore()
					assert.Lessf(t, previousScore, score,
						"Expected score %d (request = %v) to be less than %d (request = %v)",
						previousScore, preReq, score, request.subresources)
					previousScore = score
					preReq = request.subresources
				}
			} else {
				previousScore := -1
				preRes := map[string]int{}
				for _, res := range tc.expectedResourceScoringOrder {
					if tc.resourceAllocation != nil {
						res.Allocate(nil, *tc.resourceAllocation)
					}
					score, _ := tc.request.scoreMatchedResource(tc.groupReqId, res).getOnlyScore()
					assert.Lessf(t, previousScore, score,
						"Expected score %d (res = %v) to be less than %d (res = %v)",
						previousScore, preRes, score, res.subresources)
					previousScore = score
					preRes = res.subresources
				}
			}
		})
	}
}

func newSRRequest(subresources ...string) Request {
	filter := Filter{
		subresources: MapValues(
			func(a string) int { i, _ := strconv.Atoi(a); return i },
			MustSplitProps(subresources)),
	}
	return Request{Filter: filter}
}

//go:build default

package resource

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

// The following tests that resource filtering is working as expected
func TestCollection_Filter(t *testing.T) {
	c := Collection{resources: map[string]Resource{}}
	c.Add(newResource("node/n0", "rack:a,admin:x,group:g0", "cpu:4,memory:16"))
	c.Add(newResource("node/n1", "rack:a,admin:z,group:g0", "cpu:4,memory:16"))
	c.Add(newResource("node/n2", "rack:b,admin:x,group:g1", "cpu:8,memory:32"))
	c.Add(newResource("node/n3", "rack:b,admin:y,group:g1", "cpu:8,memory:32"))
	c.AddGroup(NewGroup("g0", map[string]string{"parent": "g0"}))
	c.AddGroup(NewGroup("g1", map[string]string{"parent": "g1"}))
	c.Add(newSystemResource("s0", 12, "rack:a,group:g0"))
	c.Add(newSystemResource("system1", 12, "rack:b,group:g1,admin:z"))
	n0, _ := c.Get("node/n0")
	n0.Allocate(&rlv1.ResourceLock{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lock0",
			Annotations: map[string]string{
				"foo": "bar0",
			},
		},
	}, SubresourceAllocation{Subresources: nil})
	n1, _ := c.Get("node/n1")
	n1.Allocate(&rlv1.ResourceLock{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lock1",
			Annotations: map[string]string{
				"foo": "bar1",
			},
		},
	}, SubresourceAllocation{Subresources: nil})

	for _, tc := range []struct {
		name   string
		filter Filter
		expect []string
	}{
		{
			name:   "filter by type",
			filter: NewFilter("node", nil, nil),
			expect: []string{"node/n0", "node/n1", "node/n2", "node/n3"},
		},
		{
			name:   "filter by type and prop one",
			filter: NewFilter("node", map[string][]string{"rack": {"a"}}, nil),
			expect: []string{"node/n0", "node/n1"},
		},
		{
			name:   "filter by type and prop many",
			filter: NewFilter("node", map[string][]string{"admin": {"x", "y"}}, nil),
			expect: []string{"node/n0", "node/n2", "node/n3"},
		},
		{
			name:   "filter by type and prop and available resource",
			filter: NewFilter("node", map[string][]string{"admin": {"x"}}, map[string]int{"cpu": 6}),
			expect: []string{"node/n2"},
		},
		{
			name:   "filter non-match",
			filter: NewFilter("node", map[string][]string{"admin": {"z"}}, map[string]int{"cpu": 8}),
		},
		{
			name:   "filter inherited prop",
			filter: NewFilter("node", map[string][]string{"group": {"g0"}}, map[string]int{}),
			expect: []string{"node/n0", "node/n1"},
		},
		{
			name:   "filter inherited prop multiple",
			filter: NewFilter("node", map[string][]string{"group": {"g0", "g1"}}, map[string]int{}),
			expect: []string{"node/n0", "node/n1", "node/n2", "node/n3"},
		},
		{
			name:   "filter inherited and non-inherited",
			filter: NewFilter("node", map[string][]string{"group": {"g0", "g1"}, "admin": {"z"}}, map[string]int{}),
			expect: []string{"node/n1"},
		},
		{
			name:   "filter free nodes only",
			filter: NewFreeNodesFilter(),
			expect: []string{"node/n2", "node/n3"},
		},
		{
			name:   "filter job anti-affinity key/val",
			filter: NewJobAntiAffinityFilter("foo", "bar1"),
			expect: []string{"node/n1", "node/n2", "node/n3"},
		},
	} {
		filterTestFn := func(cc *Collection) func(t *testing.T) {
			return func(t *testing.T) {
				result := cc.Filter(tc.filter)
				if result.Size() != len(tc.expect) {
					t.Errorf("Expected %d resources, got %d, %+v", len(tc.expect), result.Size(), *result)
				}
				for _, id := range tc.expect {
					if _, ok := result.Get(id); !ok {
						t.Errorf("Expected resource %s not found", id)
					}
				}
			}
		}
		t.Run(tc.name, filterTestFn(&c))
		t.Run(tc.name+" copied collection", filterTestFn(c.Copy()))
	}
}

func TestCollection_FindUngroupedScheduling(t *testing.T) {
	c := Collection{resources: map[string]Resource{}}
	c.Add(newResource("node/m0", "role-coordinator:,namespace:a", "cpu:4,memory:32,pod:4"))
	c.Add(newResource("node/m1", "role-coordinator:,namespace:a", "cpu:4,memory:32,pod:4"))
	c.Add(newResource("node/m2", "role-coordinator:,namespace:b", "cpu:4,memory:32,pod:4"))
	n, _ := c.Get("node/m2")
	n.Allocate(nil, SubresourceAllocation{Subresources: Subresources{MemSubresource: 16}})

	c.Add(newSystemResource("system0", 11, "namespace:a"))
	c.Add(newSystemResource("system1", 12, "namespace:a,stamp:g0"))
	c.Add(newSystemResource("system2", 11, "namespace:a,stamp:g1"))
	c.Add(newSystemResource("system3", 12, "namespace:a,stamp:g1"))
	c.Add(newSystemResource("system4", 12, "namespace:a,stamp:g1"))
	c.Add(newSystemResource("system5", 12, "namespace:b"))
	c.Add(newSystemResource("system6", 11, "namespace:bar"))
	c.Add(newSystemResource("system7", 12, "namespace:local"))

	for _, tc := range []struct {
		name              string
		requests          map[string]Request
		expectCount       map[string]int
		expectGrants      map[string]bool
		expectGrant       bool
		expectErrContains string
	}{
		{
			name: "system match and not coordinator match",
			requests: map[string]Request{
				rlv1.SystemsRequestName:     newRequest(rlv1.SystemsRequestName, 1, rlv1.SystemResourceType, "namespace:a", SystemSubresource+":1,"+SystemPortSubresource+":1"),
				rlv1.CoordinatorRequestName: newRequest(rlv1.CoordinatorRequestName, 1, rlv1.NodeResourceType, "namespace:a", CPUSubresource+":8"),
			},
			expectCount: map[string]int{
				rlv1.SystemsRequestName: 1,
			},
			expectGrant:       false,
			expectErrContains: "2 available with insufficient capacity {cpu:4m, mem:32Mi}",
		},
		{
			name: "system and coordinator match",
			requests: map[string]Request{
				rlv1.SystemsRequestName:     newRequest(rlv1.SystemsRequestName, 1, rlv1.SystemResourceType, "namespace:a", SystemSubresource+":1,"+SystemPortSubresource+":1"),
				rlv1.CoordinatorRequestName: newRequest(rlv1.CoordinatorRequestName, 1, rlv1.NodeResourceType, "namespace:a", CPUSubresource+":2"),
			},
			expectCount: map[string]int{
				rlv1.SystemsRequestName:     1,
				rlv1.CoordinatorRequestName: 1,
			},
			expectGrant: true,
		},
		{
			name: "system match with affinity",
			requests: map[string]Request{
				rlv1.SystemsRequestName: newRequest(rlv1.SystemsRequestName, 3,
					rlv1.SystemResourceType, "namespace:a", SystemSubresource+":1,"+SystemPortSubresource+":12").
					withAnnotation(map[string]string{
						wsapisv1.AffinityPrefix + "system3": "",
						wsapisv1.AffinityPrefix + "system4": "",
					}),
			},
			expectCount: map[string]int{
				rlv1.SystemsRequestName: 3,
			},
			expectGrant: true,
		},
		{
			name: "system not match",
			requests: map[string]Request{
				rlv1.SystemsRequestName: newRequest(rlv1.SystemsRequestName, 4, rlv1.SystemResourceType, "namespace:a", SystemSubresource+":1,"+SystemPortSubresource+":12"),
			},
			expectCount: map[string]int{
				rlv1.SystemsRequestName: 3,
			},
			expectGrant: false,
		},
		{
			name: "no system with requested capacity matches",
			requests: map[string]Request{
				rlv1.SystemsRequestName: newRequest(rlv1.SystemsRequestName, 1, rlv1.SystemResourceType, "namespace:bar", SystemSubresource+":1,"+SystemPortSubresource+":12"),
			},
			expectGrant:       false,
			expectErrContains: "requested 1 system[namespace:bar] but 0 available",
		},
		{
			name: "prefer full system",
			requests: map[string]Request{
				rlv1.SystemsRequestName: newRequest(rlv1.SystemsRequestName, 1, rlv1.SystemResourceType, "namespace:a", SystemSubresource+":1,"+SystemPortSubresource+":1"),
			},
			expectCount: map[string]int{
				rlv1.SystemsRequestName: 1,
			},
			expectGrants: map[string]bool{
				"system/system3": true,
			},
			expectGrant: true,
		},
		{
			name: "prefer group systems",
			requests: map[string]Request{
				rlv1.SystemsRequestName: newRequest(rlv1.SystemsRequestName, 2, rlv1.SystemResourceType, "namespace:a", SystemSubresource+":1,"+SystemPortSubresource+":1"),
			},
			expectCount: map[string]int{
				rlv1.SystemsRequestName: 2,
			},
			expectGrants: map[string]bool{
				"system/system3": true,
				"system/system4": true,
			},
			expectGrant: true,
		},
		{
			name: "prefer local session systems",
			requests: map[string]Request{
				rlv1.SystemsRequestName: newRequest(rlv1.SystemsRequestName, 1, rlv1.SystemResourceType,
					"namespace:a|b|local", SystemSubresource+":1,"+SystemPortSubresource+":12").
					withAnnotation(map[string]string{
						NamespacePropertyKey: "local",
					}),
			},
			expectCount: map[string]int{
				rlv1.SystemsRequestName: 1,
			},
			expectGrants: map[string]bool{
				"system/system7": true,
			},
			expectGrant: true,
		},
		{
			name: "no node with requested properties matches",
			requests: map[string]Request{
				rlv1.CoordinatorRequestName: newRequest(rlv1.CoordinatorRequestName, 1, rlv1.NodeResourceType, "namespace:foo", CPUSubresource+":8"),
			},
			expectGrant:       false,
			expectErrContains: "requested 1 node[namespace:foo] but only 0 available",
		},
		{
			name: "already allocated nodes no match",
			requests: map[string]Request{
				rlv1.CoordinatorRequestName: newRequest(rlv1.CoordinatorRequestName, 1, rlv1.NodeResourceType, "namespace:b", MemSubresource+":18"),
			},
			expectGrant:       false,
			expectErrContains: "requested 1 node[namespace:b]{mem:18Mi} but only 0 has enough available resources from 1 candidates",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grant, errs := c.FindUngroupedScheduling(tc.requests)
			ok := len(errs) == 0
			if ok != tc.expectGrant {
				t.Errorf("Expected grant: %t, got %t, %v", tc.expectGrant, ok, grant)
			} else {
				for id, expectedCount := range tc.expectCount {
					t.Log(grant[id].ResourceIds)
					if len(grant[id].ResourceIds) != expectedCount {
						t.Errorf("Expected %d resources for %s, got %d", expectedCount, id, len(grant[id].ResourceIds))
					}
					for _, rid := range grant[id].ResourceIds {
						if len(tc.expectGrants) > 0 && !tc.expectGrants[rid] {
							t.Errorf("Expected grant of %v but only got %s", tc.expectGrants, grant[id].ResourceIds)
						}
					}
				}
			}
			if tc.expectErrContains != "" {
				seenErr := false
				for _, err := range errs {
					if strings.Contains(err.Error(), tc.expectErrContains) {
						seenErr = true
						break
					}
				}
				assert.True(t, seenErr, "expected substring '%s' but not found in errs %v", tc.expectErrContains, errs)
			}
		})
	}

	// test ordering of resources - should be sorted by ratio of available subresources - more subresources available
	// should be lower index in the list.
	for _, tc := range []struct {
		name       string
		request    Request
		allocation map[string]SubresourceAllocation
		expectIds  []string
	}{
		{
			name:    "unconstrained coordinator prefers no-pod node",
			request: newRequest(rlv1.CoordinatorRequestName, c.Size(), rlv1.NodeResourceType, "role-coordinator:,namespace:a", PodSubresource+":1"),
			allocation: map[string]SubresourceAllocation{
				"node/m0": {
					Subresources: Subresources{"pod": 1},
				},
			},
			expectIds: []string{"node/m1", "node/m0"},
		},
		{
			name:    "constrained coordinator prefers least utilized node",
			request: newRequest(rlv1.CoordinatorRequestName, c.Size(), rlv1.NodeResourceType, "role-coordinator:,namespace:a", PodSubresource+":1,"+MemSubresource+":12"),
			allocation: map[string]SubresourceAllocation{
				"node/m1": {
					Subresources: Subresources{"memory": 8, "pod": 2},
				},
				"node/m0": {
					Subresources: Subresources{"memory": 4, "pod": 1},
				},
			},
			expectIds: []string{"node/m0", "node/m1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resources := c.Copy()
			resources.Allocate(nil, tc.allocation)

			grant, errs := resources.FindUngroupedScheduling(map[string]Request{rlv1.CoordinatorRequestName: tc.request})
			require.NotEmpty(t, errs)
			require.Equal(t, tc.expectIds, grant[rlv1.CoordinatorRequestName].ResourceIds)
		})
	}
}

type grantExpectation func(GroupGrant) (bool, string)
type placementExpectation struct {
	assertPlacement   func(GroupGrant, map[string]string) (bool, string)
	expectedPlacement map[string]string
}

func TestCollection_FindGroupScheduling(t *testing.T) {
	c := Collection{resources: map[string]Resource{}}
	c.AddGroup(Group{name: "0"})
	c.Add(newResource("node/m0", "role:memx,group:0", "pod:32,cpu:4,memory:32"))
	c.Add(newResource("node/m1", "role:memx,group:0", "pod:32,,cpu:4,memory:32"))
	c.Add(newResource("node/m2", "role:memx,group:0", "pod:32,cpu:4,memory:32"))
	c.Add(newResource("node/w0", "role:worker,group:0", "pod:32,cpu:4,memory:16"))
	c.Add(newResource("node/w1", "role:worker,group:0", "pod:32,cpu:4,memory:16"))
	c.Add(newResource("node/br-c-degraded", "role-broadcastreduce:,health:degraded", "pod:32,cs-port-0:5,instance-id:10"))
	c.Add(newResource("node/br-a-full", "role-broadcastreduce:", "pod:32,cs-port-0:6,instance-id:10"))
	c.Add(newResource("node/br-b-half", "role-broadcastreduce:", "pod:32,cs-port-0:3,cs-port-1:3,instance-id:10"))

	var expectActWgtEvenlyDistributed grantExpectation
	expectActWgtEvenlyDistributed = func(grant GroupGrant) (bool, string) {
		// the total number of act/wgt assigned to a node should be 0 or 1 distance from the highest allocated act/wgt node
		resourcePodCount := map[string]int{}
		for _, r := range c.Filter(NewNodeFilter("memx", 0, 0)).List() {
			resourcePodCount[r.id] = 0
		}
		largestAlloc := 0
		for replicaType, g := range grant.Grants {
			if replicaType != "Weight" && replicaType != "Activation" {
				continue
			}
			for _, r := range g.ResourceIds {
				resourcePodCount[r] += 1
				if resourcePodCount[r] > largestAlloc {
					largestAlloc = resourcePodCount[r]
				}
			}
		}
		for resource, count := range resourcePodCount {
			if count+1 < largestAlloc {
				return false, fmt.Sprintf(
					"resource %s has a total of %d wgt and act assigned but expected %d or %d",
					resource, count, largestAlloc-1, largestAlloc,
				)
			}
		}
		return true, ""
	}

	expectKvssPlacement := func(grant GroupGrant, expected map[string]string) (bool, string) {
		for k, v := range grant.Grants {
			if expected[k] != v.ResourceIds[0] {
				return false, fmt.Sprintf("expected request %v to be placed on %v, but got %v", k, expected[k], v.ResourceIds[0])
			}
		}
		return true, ""
	}

	for _, tc := range []struct {
		name              string
		preReq            GroupRequest
		req               GroupRequest
		expectMatch       bool
		expectNoOverlap   bool
		expectations      map[string]grantExpectation
		placements        []placementExpectation
		expectErrContains string
		extraRes          []Resource
	}{
		{
			name: "br - happy case",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withRequest("br0", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("br1", 1, "node", "", "cs-port-0:500,instance-id:1").
				withRequest("br2", 1, "node", "", "cs-port-0:500,instance-id:1").
				build(),
			expectMatch:     true,
			expectNoOverlap: true,
		},
		{
			name: "br - 1:2 mode will always take half mode nodes first",
			preReq: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withRequest("br0", 1, "node", "", "cs-port-1:300,instance-id:1").
				build(),
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withRequest("br0", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("br1", 1, "node", "", "cs-port-0:500,instance-id:1").
				withRequest("br2", 1, "node", "", "cs-port-0:300,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectMatch:     true,
			expectNoOverlap: true,
		},
		{
			name: "br - failure due to healthy node taken by 1:4 mode first ",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withRequest("br0", 1, "node", "", "cs-port-0:500,instance-id:1").
				withRequest("br1", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("br2", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("br3", 1, "node", "", "cs-port-0:300,instance-id:1").
				build(),
			expectMatch: false,
		},
		{
			name: "br - 1:2 mode will be scheduled to healthy nodes first",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withRequest("br0", 1, "node", "", "cs-port-0:500,instance-id:1").
				withRequest("br1", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("br2", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("br3", 1, "node", "", "cs-port-0:300,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectMatch:     true,
			expectNoOverlap: false,
		},
		{
			name: "br - no possible",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withRequest("br0", 1, "node", "", "cs-port-0:500,instance-id:1").
				withRequest("br1", 1, "node", "", "cs-port-0:500,instance-id:1").
				withRequest("br2", 1, "node", "", "cs-port-0:500,instance-id:1").
				build(),
			expectMatch: false,
		},
		{
			name: "br - test 1:2 mode prefer to be shared",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withRequest("br0", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("br1", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("br2", 1, "node", "", "cs-port-0:300,instance-id:1").
				build(),
			expectMatch:     true,
			expectNoOverlap: false,
		},
		{
			name: "greedy match - no overlap when possible - excess capacity",
			req: newGroupRequestBuilder("ng-primary-0", 1).
				withRequest("weight", 2, "node", "role:memx", "memory:8,cpu:2").
				withRequest("activation", 1, "node", "role:memx", "memory:8,cpu:2").
				withRequest("worker", 2, "node", "role:worker", "memory:8,cpu:2").
				build(),
			expectMatch:     true,
			expectNoOverlap: true,
		},
		{
			name: "greedy match - no overlap when possible - no excess capacity",
			req: newGroupRequestBuilder("ng-primary-0", 1).
				withRequest("weight", 2, "node", "role:memx", "memory:32,cpu:4").
				withRequest("activation", 1, "node", "role:memx", "memory:32,cpu:4").
				withRequest("worker", 2, "node", "role:worker", "memory:16,cpu:4").
				build(),
			expectMatch:     true,
			expectNoOverlap: true,
		},
		{
			name: "greedy match - overlap needed",
			req: newGroupRequestBuilder("ng-primary-0", 1).
				withRequest("weight", 10, "node", "role:memx", "memory:4,cpu:1").
				build(),
			expectMatch: true,
		},
		{
			name: "greedy match - insufficient weight subresources",
			req: newGroupRequestBuilder("ng-primary-0", 1).
				withRequest("Weight", 4, "node", "role:memx", "memory:20,cpu:4").
				build(),
			expectMatch:       false,
			expectErrContains: "3 nodes[role:memx]{cpu:4m, mem:32Mi} could only schedule 3 of 4 Weight{cpu:4m, mem:20Mi}",
		},
		{
			name: "greedy match - no matching weight properties",
			req: newGroupRequestBuilder("ng-primary-0", 1).
				withRequest("weight", 4, "node", "role:memx,populated:true", "memory:20,cpu:4").
				build(),
			expectMatch:       false,
			expectErrContains: "group request[ng-primary-0:weight], props[populated:true, role:memx], anti-affinity[map[]], matched no node resources",
		},
		{
			name: "greedy match - insufficient weight subresources after placing act",
			req: newGroupRequestBuilder("ng-primary-0", 1).
				withRequest("Activation", 1, "node", "role:memx", "memory:8,cpu:4").
				withRequest("Weight", 4, "node", "role:memx", "memory:26,cpu:4").
				build(),
			expectMatch:       false,
			expectErrContains: "3 nodes[role:memx]{cpu:4m, mem:32Mi} could only schedule 1 of 1 Activation{cpu:4m, mem:8Mi}, 2 of 4 Weight{cpu:4m, mem:26Mi}",
		},
		{
			name: "greedy match - evenly spread weight and act",
			req: newGroupRequestBuilder("ng-primary-0", 1).
				withRequest("Chief", 1, "node", "role:memx", "memory:4,cpu:1").
				withRequest("Activation", 5, "node", "role:memx", "memory:2,cpu:1").
				withRequest("Weight", 1, "node", "role:memx", "memory:2,cpu:1").
				build(),
			expectMatch: true,
			expectations: map[string]grantExpectation{
				"actWgtEven": expectActWgtEvenlyDistributed,
			},
		},
		{
			name: "job sharing - placement based on score priority",
			req: newGroupRequestBuilder("KVStorageServer-0", 3).
				// Request 0, has affinity to AX0/AX1, lands on AX0
				withAffinityRequest("0", 1, "node", "role:activation", map[string]string{},
					map[string]string{"system1-2": ""}, "memory:2,cpu:1,cs-port-0:1").
				// Request 1, has affinity to AX0/AX1, lands on AX1 because AX0 has no more resource capacity
				withAffinityRequest("1", 1, "node", "role:activation", map[string]string{},
					map[string]string{"system1-2": ""}, "memory:2,cpu:1,cs-port-1:1").
				// Request 2, has affinity to AX0/AX1, lands on AX2 because AX0/AX1 have no more resource capacity
				withAffinityRequest("2", 1, "node", "role:activation", map[string]string{},
					map[string]string{"system1-2": ""}, "memory:2,cpu:1,cs-port-1:1").
				// Request 3, , has affinity to AX2/AX3, lands on AX3 because AX2 has allocated some resources to Request 2
				withAffinityRequest("3", 1, "node", "role:activation", map[string]string{},
					map[string]string{"system2-4": ""}, "memory:2,cpu:1").
				withAnnotations(map[string]string{wsapisv1.InferenceJobSharingAnnotKey: strconv.FormatBool(true)}).
				build(),
			expectMatch:     true,
			expectNoOverlap: true,
			placements: []placementExpectation{{
				expectedPlacement: map[string]string{"0": "node/ax0", "1": "node/ax1", "2": "node/ax2", "3": "node/ax3"},
				assertPlacement:   expectKvssPlacement,
			}},
			extraRes: []Resource{
				newResource("node/ax0", "role:activation,group:0,system1-2:", "pod:32,cpu:1,memory:2,cs-port-0:5,cs-port-1:5,instance-id:10"),
				newResource("node/ax1", "role:activation,group:0,system1-2:", "pod:32,,cpu:1,memory:2,cs-port-0:5,cs-port-1:5,instance-id:10"),
				newResource("node/ax2", "role:activation,group:0,system2-3:", "pod:32,cpu:4,memory:32,cs-port-0:6,cs-port-1:6,instance-id:10"),
				newResource("node/ax3", "role:activation,group:0,system2-3:", "pod:32,cpu:4,memory:32,cs-port-0:6,cs-port-1:6,instance-id:10"),
			},
		},
		{
			// even if no mem specified, "pod" psuedo resource should be enough to force even spread over group
			name: "greedy match - evenly spread weight and act in no mem request case",
			req: newGroupRequestBuilder("ng-primary-0", 1).
				withRequest("Chief", 1, "node", "role:memx", "pod:1,memory:4,cpu:1").
				withRequest("Activation", 5, "node", "role:memx", "pod:1,memory:2,cpu:1").
				withRequest("Weight", 1, "node", "role:memx", "pod:1").
				build(),
			expectMatch: true,
			expectations: map[string]grantExpectation{
				"actWgtEven": expectActWgtEvenlyDistributed,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, extraRes := range tc.extraRes {
				if len(extraRes.subresources) > 0 {
					c.Add(extraRes)
				}
			}
			defer func() {
				for _, extraRes := range tc.extraRes {
					c.Remove(extraRes.Id())
				}
			}()
			if len(tc.preReq.Requests) > 0 {
				gg, err := c.FindGroupScheduling(tc.preReq)
				require.NoError(t, err)
				c.Allocate(nil, GatherAllocations(nil, map[string][]GroupGrant{gg.Id(): {gg}}))
			}
			result, err := c.FindGroupScheduling(tc.req)
			t.Log(result.Grants, err)
			if !tc.expectMatch && err == nil {
				t.Errorf("Expected no matches, got result %v", result.Grants)
			} else if tc.expectMatch {
				require.NotEmpty(t, result.Grants)
				require.NoError(t, err)
				seenResource := map[string]bool{}
				overlap := map[string]bool{}
				for _, grant := range result.Grants {
					for _, rid := range grant.ResourceIds {
						if seenResource[rid] {
							overlap[rid] = true
						}
						seenResource[rid] = true
					}
				}
				if tc.expectNoOverlap && len(overlap) > 0 {
					t.Errorf("Expected no overlap, but found overlap on resource %v", overlap)
				}
				if !tc.expectNoOverlap && len(overlap) == 0 {
					t.Errorf("Expected overlap, but no overlap")
				}
				for name, expectation := range tc.expectations {
					ok, msg := expectation(result)
					assert.True(t, ok, "expectation "+name+" failed: "+msg)
				}
				for _, expectation := range tc.placements {
					ok, msg := expectation.assertPlacement(result, expectation.expectedPlacement)
					assert.True(t, ok, fmt.Sprintf("placement assertion failed: %v", msg))
				}
			} else {
				if tc.expectErrContains != "" {
					assert.ErrorContains(t, err, tc.expectErrContains)
				}
			}
		})
	}
}

func TestCollection_GetFreeNodegroups(t *testing.T) {
	for _, tc := range []struct {
		name               string
		allocatedNodeNames []string
		popGroupTypes      []string
		expectedPopGroups  map[string][]string
		expectedAllGroups  map[string][]string
	}{
		{
			name: "getFreeNodegroups - no allocation - all pop group types allowed",
			popGroupTypes: []string{
				wsapisv1.PopNgTypeRegular,
				wsapisv1.PopNgTypeLarge,
				wsapisv1.PopNgTypeXLarge,
			},
			expectedPopGroups: map[string][]string{
				"nodegroup3": {"nodegroup3-node0", "nodegroup3-node1"},
				"nodegroup4": {"nodegroup4-node0", "nodegroup4-node1"},
				"nodegroup5": {"nodegroup5-node0", "nodegroup5-node1"}},
			expectedAllGroups: map[string][]string{
				"nodegroup0": {"nodegroup0-node0", "nodegroup0-node1"},
				"nodegroup1": {"nodegroup1-node0", "nodegroup1-node1"},
				"nodegroup2": {"nodegroup2-node0", "nodegroup2-node1"},
				"nodegroup3": {"nodegroup3-node0", "nodegroup3-node1"},
				"nodegroup4": {"nodegroup4-node0", "nodegroup4-node1"},
				"nodegroup5": {"nodegroup5-node0", "nodegroup5-node1"}},
		},
		{
			name: "getFreeNodegroups - no allocation - some pop group types allowed",
			popGroupTypes: []string{
				wsapisv1.PopNgTypeRegular,
				wsapisv1.PopNgTypeLarge,
			},
			expectedPopGroups: map[string][]string{
				"nodegroup3": {"nodegroup3-node0", "nodegroup3-node1"},
				"nodegroup4": {"nodegroup4-node0", "nodegroup4-node1"}},
			expectedAllGroups: map[string][]string{
				"nodegroup0": {"nodegroup0-node0", "nodegroup0-node1"},
				"nodegroup1": {"nodegroup1-node0", "nodegroup1-node1"},
				"nodegroup2": {"nodegroup2-node0", "nodegroup2-node1"},
				"nodegroup3": {"nodegroup3-node0", "nodegroup3-node1"},
				"nodegroup4": {"nodegroup4-node0", "nodegroup4-node1"},
				"nodegroup5": {"nodegroup5-node0", "nodegroup5-node1"}},
		},
		{
			name:          "getFreeNodegroups - no allocation - some other pop group types allowed",
			popGroupTypes: []string{wsapisv1.PopNgTypeXLarge},
			expectedPopGroups: map[string][]string{
				"nodegroup5": {"nodegroup5-node0", "nodegroup5-node1"}},
			expectedAllGroups: map[string][]string{
				"nodegroup0": {"nodegroup0-node0", "nodegroup0-node1"},
				"nodegroup1": {"nodegroup1-node0", "nodegroup1-node1"},
				"nodegroup2": {"nodegroup2-node0", "nodegroup2-node1"},
				"nodegroup3": {"nodegroup3-node0", "nodegroup3-node1"},
				"nodegroup4": {"nodegroup4-node0", "nodegroup4-node1"},
				"nodegroup5": {"nodegroup5-node0", "nodegroup5-node1"}},
		},
		{
			name:               "getFreeNodegroups - some allocation",
			popGroupTypes:      []string{wsapisv1.PopNgTypeRegular},
			allocatedNodeNames: []string{"nodegroup1-node1"},
			expectedPopGroups: map[string][]string{
				"nodegroup3": {"nodegroup3-node0", "nodegroup3-node1"}},
			expectedAllGroups: map[string][]string{
				"nodegroup0": {"nodegroup0-node0", "nodegroup0-node1"},
				"nodegroup2": {"nodegroup2-node0", "nodegroup2-node1"},
				"nodegroup3": {"nodegroup3-node0", "nodegroup3-node1"},
				"nodegroup4": {"nodegroup4-node0", "nodegroup4-node1"},
				"nodegroup5": {"nodegroup5-node0", "nodegroup5-node1"}},
		},
		{
			name: "getFreeNodegroups - all allocation",
			popGroupTypes: []string{
				wsapisv1.PopNgTypeRegular,
				wsapisv1.PopNgTypeLarge,
				wsapisv1.PopNgTypeXLarge,
			},
			allocatedNodeNames: []string{
				"nodegroup0-node0", "nodegroup0-node1",
				"nodegroup1-node0", "nodegroup1-node1",
				"nodegroup2-node0", "nodegroup2-node1",
				"nodegroup3-node0", "nodegroup3-node1",
				"nodegroup4-node0", "nodegroup4-node1",
				"nodegroup5-node0", "nodegroup5-node1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// create a collection of 6 groups, each with 2 nodes
			// nodegroup[0-2]: depop
			// nodegroup3: pop
			// nodegroup4: pop + large_memx
			// nodegroup5: pop + xlarge_memx
			c := &Collection{resources: map[string]Resource{}}
			for g := 0; g < 6; g++ {
				grpName := "nodegroup" + strconv.Itoa(g)
				props := map[string]string{}
				if g == 3 {
					props[wsapisv1.MemxPopNodegroupProp] = "true"
					props[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeRegular
				} else if g == 4 {
					props[wsapisv1.MemxPopNodegroupProp] = "true"
					props[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeLarge
				} else if g == 5 {
					props[wsapisv1.MemxPopNodegroupProp] = "true"
					props[wsapisv1.MemxMemClassProp] = wsapisv1.PopNgTypeXLarge
				}
				grp := NewGroup(grpName, props)
				c.AddGroup(grp)
				for n := 0; n < 2; n++ {
					res := newResource(
						"node/nodegroup"+strconv.Itoa(g)+"-node"+strconv.Itoa(n),
						"group:"+grpName,
						"cpu:4,memory:32")
					res.group = grp
					c.Add(res)
				}
			}
			for _, node := range tc.allocatedNodeNames {
				m2, _ := c.Get("node/" + node)
				m2.Allocate(nil, SubresourceAllocation{Subresources: Subresources{CPUSubresource: 2}})
			}

			popGroups, allGroups := c.GetFreeNodegroups(tc.popGroupTypes)

			assertGroups := func(expectedGroups map[string][]string, actualGroups map[string]*Collection) {
				if len(expectedGroups) != len(actualGroups) {
					t.Errorf("expected %d groups, got %d", len(expectedGroups), len(actualGroups))
				} else {
					for groupId, expectedNames := range expectedGroups {
						actualNames := Map(func(r Resource) string { return r.name }, actualGroups[groupId].List())
						assert.ElementsMatch(t, expectedNames, actualNames)
					}
				}
			}
			assertGroups(tc.expectedPopGroups, popGroups)
			assertGroups(tc.expectedAllGroups, allGroups)
		})
	}
}

func TestCollection_DeepCopy(t *testing.T) {
	c := &Collection{resources: map[string]Resource{}}
	c.Add(newResource("node/n0", "k:x,group:g0,role-memory:", "cpu:4,memory:16"))
	c.Add(newResource("node/n1", "k:x,group:g1,role-worker:", "cpu:8,memory:32"))
	c.Add(newResource("node/n2", "group:g0,role-memory:", "cpu:8,memory:32"))
	c.Add(newResource("node/n3", "group:g0,role-management:", "cpu:8,memory:32"))
	c.AddGroup(NewGroup("g0", map[string]string{"parent": "g0", "k": "z"}))
	c.AddGroup(NewGroup("g1", map[string]string{"parent": "g1"}))
	c.AddGroup(NewGroup("g2", map[string]string{"parent": "g2"}))

	assertCollection := func(c *Collection) {
		assert.Equal(t, 4, len(c.List()))
		assert.Equal(t, 3, len(c.ListGroups()))
		assert.Equal(t, 2, c.FilterByProps(map[string][]string{"k": {"x"}}, nil).Size())
		assert.Equal(t, 1, c.FilterByProps(map[string][]string{"k": {"x"}, "group": {"g0"}}, nil).Size())
		assert.Equal(t, 0, c.FilterByProps(map[string][]string{"k": {"y"}}, nil).Size())
		assert.Equal(t, 1, c.FilterByProps(map[string][]string{"k": {"z"}}, nil).Size())
	}

	assertCollection(c)
	assertCollection(c.DeepUnallocatedCopy())
}

func TestCollection_GetNodesWithMostAffinityCoverage(t *testing.T) {
	csSpec := systemv1.GetCSVersionSpec(systemv1.CSVersion2)
	portCount := csSpec.NumPorts         // 12 ports
	domainCount := csSpec.NumPortDomains // 4 domains

	newAxNodeCollection := func(stampSize, axToWseRatio int) *Collection {
		c := &Collection{resources: map[string]Resource{}}
		axNodeCount := stampSize * axToWseRatio

		for i := 0; i < axNodeCount; i++ {
			// 12 leaf switches per stamp
			// switch index also represents the affinity to that particular cs port
			// For example, leaf-sw-7 will have affinity to cs port 7 over all CSX's in the stamp
			connectedSwitches := []int{i % portCount, (i + domainCount - 1) % portCount}
			var affinityProps []string
			for j := 0; j < stampSize; j++ {
				affinityProps = append(affinityProps, fmt.Sprintf("%s/systemf%d-%d:", PortAffinityPropKeyPrefix, j, connectedSwitches[0]))
				affinityProps = append(affinityProps, fmt.Sprintf("%s/systemf%d-%d:", PortAffinityPropKeyPrefix, j, connectedSwitches[1]))
			}
			affinityPropsStr := strings.Join(affinityProps, ",")
			c.Add(
				newResource(
					fmt.Sprintf("node/ax-%d", i),
					fmt.Sprintf("role-activation:,%s:0,%s", StampPropertyKey, affinityPropsStr),
					"pod:32",
				),
			)

			if i%axToWseRatio == 0 {
				c.Add(
					newResource(
						fmt.Sprintf("system/systemf%d", i/axToWseRatio),
						fmt.Sprintf("%s:0", StampPropertyKey),
						"system:1",
					),
				)
			}
		}
		return c
	}

	for i, tc := range []struct {
		// "systemCountPerStamp" decides how many systems there are in a stamp. We do not consider selecting
		// AX nodes cross-stamp since that would incur spine load and unpredictable resource skew across stamps.
		systemCountPerStamp int
		// "preAllocatedAxsFromOtherNs" is the AX node indices which we will pre-allocate prior to running the select-K routine.
		// Ax nodes from other namespaces will not be part of the selection.
		preAllocatedAxsFromOtherNs []int
		// "preAllocatedAxsFromSameNs" is the AX node indices which we have pre-allocated earlier prior to running the select-K routine.
		// Ax nodes from the same namespace will be part of the selection.
		preAllocatedAxsFromSameNs []int
		// "selectKNodesPerSchedule" is a sequence of calls of the GetNodesWithMostAffinityCoverage (select-K) routine.
		// []int{1, 2} would mean first select 1, then select 2.
		// The following "expect*" assertions correspond to expectations against each select-K routine call.
		// For example, on performing "selectKNodesPerSchedule[0]", we expect to meet assertions (if defined) in:
		// - expectEdgeWeights[0]
		// - expectSelectedAxs[0]
		// - expectUnselectedAxs[0]
		// The sequence of calls of the select-K routine represents sequential requests to pick the best K AX nodes
		// in either job scheduling or in session management.
		selectKNodesPerSchedule []int

		// Among the selected set of nodes, if we had:
		// - 5 edges to systemf0-cs-port-0
		// - 5 edges to systemf0-cs-port-1
		// - 3 edges to systemf0-cs-port-2
		// then the map should look like map[5:2, 3:1].
		// Indices in the structure represent weights of edges and values represent amount of such edges.
		expectEdgeWeights []map[int]int
		// "expectSelectedAxs" represents the indices of the selected AX nodes per select-K routine call.
		expectSelectedAxs [][]int
		// "expectUnselectedAxs" represents the indices of the unselected AX nodes per select-K routine call.
		expectUnselectedAxs [][]int

		// "portAffinities" should encourage picking a node that meets the port affinities
		// for example, map[5: true]:
		// - port 5 is preferred
		portAffinities map[int]bool

		// "antiPortAffinities" should discourage picking a node that meets the port affinities
		// for example, map[5: true]:
		// - port 5 should be avoided
		antiPortAffinities map[int]bool
	}{
		{
			systemCountPerStamp:     32,
			selectKNodesPerSchedule: []int{1},

			expectEdgeWeights: []map[int]int{{1: 2}},
		},
		{
			systemCountPerStamp:     32,
			selectKNodesPerSchedule: []int{6},

			expectEdgeWeights: []map[int]int{{1: 12}},
		},
		{
			systemCountPerStamp:     32,
			selectKNodesPerSchedule: []int{7},

			expectEdgeWeights: []map[int]int{{2: 2, 1: 10}},
		},
		{
			systemCountPerStamp:     32,
			selectKNodesPerSchedule: []int{12},

			expectEdgeWeights: []map[int]int{{2: 12}},
		},
		{
			systemCountPerStamp:     16,
			selectKNodesPerSchedule: []int{6},

			expectEdgeWeights: []map[int]int{{1: 12}},
		},
		{
			systemCountPerStamp: 16,
			// 0, 1, 2, 6, 7, 8 should give the full coverage already
			preAllocatedAxsFromOtherNs: []int{3, 4, 5, 9, 10, 11, 12, 13, 14, 15},
			selectKNodesPerSchedule:    []int{6},

			expectEdgeWeights: []map[int]int{{1: 12}},
		},
		{
			// full affinity coverage is impossible for stamp size under 9
			systemCountPerStamp:     8,
			selectKNodesPerSchedule: []int{6},

			// order of node picking
			// ax-0: (0, 3)
			// ax-1: (1, 4)
			// ax-2: (2, 5)
			// ax-6: (6, 9)
			// ax-7: (7, 10)
			// only port 8 and port 11 missing now
			// given we don't have ax-8: (8, 11), the best would be ax-5: (5, 8)
			// other nodes ax-3: (3, 6), ax-4: (4, 7) would yield higher variance
			expectEdgeWeights:   []map[int]int{{2: 1, 1: 10}},
			expectUnselectedAxs: [][]int{{3, 4}},
		},
		{
			// Same spread pattern as above, but with a different node order
			// Test would fail if we didn't prioritize nodes with less overlapped affinity.
			systemCountPerStamp:        16,
			preAllocatedAxsFromOtherNs: []int{0, 1, 2, 8, 9, 10, 11, 15},
			selectKNodesPerSchedule:    []int{6},

			// order of node picking
			// ax-5: (5, 8)
			// ax-6: (6, 9)
			// ax-7: (7, 10)
			// ax-12: (0, 3)
			// ax-13: (1, 4)
			// ax-14: (2, 5)
			expectEdgeWeights:   []map[int]int{{2: 1, 1: 10}},
			expectUnselectedAxs: [][]int{{3, 4}},
		},
		{
			// Incremental build from existing collection
			systemCountPerStamp:        16,
			preAllocatedAxsFromOtherNs: []int{0, 1, 2, 8, 9, 10, 11, 15},
			preAllocatedAxsFromSameNs:  []int{5},
			selectKNodesPerSchedule:    []int{6},

			// order of node picking
			// ax-5: (5, 8)
			// ax-6: (6, 9)
			// ax-7: (7, 10)
			// ax-12: (0, 3)
			// ax-13: (1, 4)
			// ax-14: (2, 5)
			expectEdgeWeights:   []map[int]int{{2: 1, 1: 10}},
			expectUnselectedAxs: [][]int{{3, 4}},
		},
		{
			// Incremental build from existing collection
			systemCountPerStamp:        16,
			preAllocatedAxsFromOtherNs: []int{0, 1, 2, 8, 9, 10, 11, 15},
			preAllocatedAxsFromSameNs:  []int{13, 14},
			selectKNodesPerSchedule:    []int{6},

			// order of node picking
			// ax-13: (1, 4)
			// ax-14: (2, 5)
			// ax-6: (6, 9)
			// ax-7: (7, 10)
			// ax-12: (0, 3)
			// ax-5: (5, 8)
			expectEdgeWeights:   []map[int]int{{2: 1, 1: 10}},
			expectUnselectedAxs: [][]int{{3, 4}},
		},
		{
			systemCountPerStamp:     24,
			selectKNodesPerSchedule: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},

			// The node name sorting ensures we do not pick second half of the nodes
			// prior to picking the first half.
			expectSelectedAxs: [][]int{
				{0}, {1}, {2}, // (0, 3), (1, 4), (2, 5)
				{6}, {7}, {8}, // (6, 9), (7, 10), (8, 11)
				{3}, {4}, {5}, // (3, 6), (4, 7), (5, 8)
				{9}, {10}, {11}, // (9, 0), (10, 1), (11, 2)
				{12}, {13}, {14}, // (0, 3), (1, 4), (2, 5)
			},
		},
		{
			systemCountPerStamp:     12,
			selectKNodesPerSchedule: []int{1},
			expectSelectedAxs:       [][]int{{0}},
		},
		{
			systemCountPerStamp:     12,
			selectKNodesPerSchedule: []int{1},
			portAffinities:          map[int]bool{6: true},
			expectSelectedAxs:       [][]int{{3}},
		},
		{
			systemCountPerStamp:     12,
			selectKNodesPerSchedule: []int{1},
			portAffinities:          map[int]bool{2: true},
			expectSelectedAxs: [][]int{{
				2, // (2, 5) picked first due to preferred affinities
			}},
		},
		{
			systemCountPerStamp:       12,
			preAllocatedAxsFromSameNs: []int{2},
			selectKNodesPerSchedule:   []int{12},
			antiPortAffinities:        map[int]bool{0: true, 1: true, 2: true},
			expectSelectedAxs: [][]int{{
				2,       // (2, 5) picked first due to preferred affinities
				3, 4, 8, // (3, 6), (4, 7), (8, 11) nodes which do not contain 0, 1 or 2
				6, 7, 5, // (6, 9), (7, 10), (5, 8) nodes which do not contain 0, 1 or 2, though with overlapped ports
				// remaining nodes have no choice but to take the non-preferred ports
				// among which, the overall rule is still to minimize the variance increase in every step
				0, 1, 9, 10, 11, // (0, 3), (1, 4), (0, 9), (1, 10), (2, 11)
			}},
		},
	} {
		t.Run(fmt.Sprintf("test-%d-%d", i, tc.systemCountPerStamp), func(t *testing.T) {
			defaultAxNodesPerSystem := 1
			c := newAxNodeCollection(tc.systemCountPerStamp, defaultAxNodesPerSystem)
			if len(tc.preAllocatedAxsFromOtherNs) > 0 {
				subresAllocs := map[string]SubresourceAllocation{}
				for _, i := range tc.preAllocatedAxsFromOtherNs {
					subresAllocs[fmt.Sprintf("node/ax-%d", i)] = SubresourceAllocation{
						Subresources: Subresources{"pod": 1},
					}
				}
				c.Allocate(nil, subresAllocs)
			}

			allocatedCollection := Collection{}
			if len(tc.preAllocatedAxsFromSameNs) > 0 {
				for _, i := range tc.preAllocatedAxsFromSameNs {
					res, _ := c.Get(fmt.Sprintf("node/ax-%d", i))
					allocatedCollection.Add(res)
				}
			}

			systemCollection := c.Filter(
				NewFilter(rlv1.SystemResourceType, map[string][]string{StampPropertyKey: {"0"}}, nil))
			var systemsInStamp []string
			for _, res := range systemCollection.resources {
				systemsInStamp = append(systemsInStamp, res.Name())
			}

			for i, selectK := range tc.selectKNodesPerSchedule {
				axNodesCollection := c.Filter(
					NewFilter(rlv1.NodeResourceType, map[string][]string{StampPropertyKey: {"0"}}, nil))
				unallocated := axNodesCollection.GetFreeNodes()
				selection, _ := unallocated.GetNodesWithMostAffinityCoverage("test", "", systemsInStamp, &allocatedCollection, selectK,
					tc.portAffinities, tc.antiPortAffinities, portCount)

				assert.Equal(t, selectK, len(selection.List()))
				if len(tc.expectEdgeWeights) > 0 {
					sumEdges := 0
					for k, v := range tc.expectEdgeWeights[i] {
						sumEdges += k * v
					}
					assert.Equal(t, 2*selectK, sumEdges)

					edgeMap := map[string]int{}
					for _, res := range selection.List() {
						for k := range res.GetProps() {
							if strings.HasPrefix(k, PortAffinityPropKeyPrefix+"/systemf0-") {
								edgeMap[k]++
							}
						}
					}
					edgeWeightMap := map[int]int{}
					for _, edgeWeight := range edgeMap {
						edgeWeightMap[edgeWeight]++
					}
					assert.Equal(t, tc.expectEdgeWeights[i], edgeWeightMap)
				}

				if len(tc.expectUnselectedAxs) > 0 {
					expectUnselectedNodes := map[string]bool{}
					for _, idx := range tc.expectUnselectedAxs[i] {
						expectUnselectedNodes[fmt.Sprintf("node/ax-%d", idx)] = true
					}

					for rid := range selection.resources {
						assert.Equal(t, false, expectUnselectedNodes[rid])
					}
				}

				if len(tc.expectSelectedAxs) > 0 {
					expectSelectedNodes := map[string]bool{}
					for _, idx := range tc.expectSelectedAxs[i] {
						expectSelectedNodes[fmt.Sprintf("node/ax-%d", idx)] = true
					}

					for rid := range selection.resources {
						assert.Equal(t, true, expectSelectedNodes[rid])
					}
				}

				subresAllocs := map[string]SubresourceAllocation{}
				for res := range selection.resources {
					subresAllocs[res] = SubresourceAllocation{
						Subresources: Subresources{"pod": 1},
					}
				}
				c.Allocate(nil, subresAllocs)
			}
		})
	}
}

func TestCollection_BRGroupScheduling(t *testing.T) {
	c := Collection{resources: map[string]Resource{}}
	// BR in V1 networking relies on the "cs-port-k" subresource for scheduling
	// BR in V2 networking relies on the generic "cs-port" subresource and the affinity properties on switch level for scheduling
	c.Add(newResource("node/br-0-a", "role-broadcastreduce:", "pod:32,instance-id:12,cs-port-0:600"))
	c.Add(newResource("node/br-1-a", "role-broadcastreduce:", "pod:32,instance-id:12,cs-port-1:600"))
	c.Add(newResource("node/br-0-b", "role-broadcastreduce:", "pod:32,instance-id:12,cs-port-0:600"))
	c.Add(newResource("node/br-1-b", "role-broadcastreduce:", "pod:32,instance-id:12,cs-port-1:600"))
	c.Add(newResource("node/br-half", "role-broadcastreduce:", "pod:32,instance-id:12,cs-port-0:300,cs-port-1:300"))
	c.Add(newResource("node/br-2-g0", "system1-2:,system2-2:,role-broadcastreduce:", "pod:32,instance-id:12,cs-port:600"))
	c.Add(newResource("node/br-2-g00", "system1-2:,system2-3:,role-broadcastreduce:", "pod:32,instance-id:12,cs-port:600"))
	c.Add(newResource("node/br-3-g1", "system1-3:,system2-3:,role-broadcastreduce:", "pod:32,instance-id:12,cs-port:400"))
	c.Add(newResource("node/br-4-g2", "system1-4:,system2-4:,role-broadcastreduce:", "pod:32,instance-id:12,cs-port:600"))
	c.Add(newResource("node/br-5-g3", "system1-5:,system2-5:,role-broadcastreduce:,health:degraded", "pod:32,instance-id:12,cs-port:600"))

	for _, tc := range []struct {
		name             string
		req              GroupRequest
		extraRes         []Resource
		expectGrantNodes map[string]string
		expectGrantRes   map[string]Subresources
		expectErr        string
	}{
		{
			name: "simplest two 1:2 modes sharing same node",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withRequest("0", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("1", 1, "node", "", "cs-port-0:300,instance-id:1").
				withRequest("2", 1, "node", "", "cs-port-0:300,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectGrantNodes: map[string]string{
				"0": "node/br-half",
				"1": "node/br-0-a",
				"2": "node/br-0-a",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"1": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"2": {
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"cs-port/nic-5": DefaultNicBW,
					"instance-id/1": 1,
				},
			},
		},
		{
			name: "single layer br same mode",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withAffinityRequest("0", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-0:300,instance-id:1").
				withAffinityRequest("1", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-1:300,instance-id:1").
				withAffinityRequest("2", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-0:300,instance-id:1").
				withAffinityRequest("3", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-1:300,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectGrantNodes: map[string]string{
				"0": "node/br-half",
				"1": "node/br-half",
				"2": "node/br-0-a",
				"3": "node/br-1-a",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"1": {
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"cs-port/nic-5": DefaultNicBW,
					"instance-id/1": 1,
				},
				"2": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"3": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
			},
		},
		{
			name: "multi layer br same mode",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withAffinityRequest("0", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-0:300,instance-id:1").
				withAffinityRequest("2", 1, "node", "",
					map[string]string{"layer": "1"}, nil, "cs-port-0:300,instance-id:1").
				withAffinityRequest("1", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-1:300,instance-id:1").
				withAffinityRequest("3", 1, "node", "",
					map[string]string{"layer": "1"}, nil, "cs-port-1:300,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectGrantNodes: map[string]string{
				"0": "node/br-half",
				"1": "node/br-half",
				"2": "node/br-0-a",
				"3": "node/br-1-a",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"1": {
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"cs-port/nic-5": DefaultNicBW,
					"instance-id/1": 1,
				},
				"2": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"3": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
			},
		},
		{
			name: "multi layer different mode",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withAffinityRequest("0", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-0:500,instance-id:1").
				withAffinityRequest("1", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-1:500,instance-id:1").
				withAffinityRequest("2", 1, "node", "",
					map[string]string{"layer": "1"}, nil, "cs-port-0:300,instance-id:1").
				withAffinityRequest("3", 1, "node", "",
					map[string]string{"layer": "1"}, nil, "cs-port-1:300,instance-id:1").
				withAffinityRequest("4", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-0:500,instance-id:1").
				withAffinityRequest("5", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-1:500,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectGrantNodes: map[string]string{
				"0": "node/br-0-a",
				"1": "node/br-1-a",
				"2": "node/br-half",
				"3": "node/br-half",
				"4": "node/br-0-b",
				"5": "node/br-1-b",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
				"1": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
				"2": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"3": {
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"cs-port/nic-5": DefaultNicBW,
					"instance-id/1": 1,
				},
				"4": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
				"5": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
			},
		},
		{
			name: "multi layer anti-props+assign half mode first",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withAffinityRequest("0", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-0:500,instance-id:1").
				withAffinityRequest("1", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-1:300,instance-id:1").
				withAffinityRequest("2", 1, "node", "",
					map[string]string{"layer": "0"}, nil, "cs-port-1:300,instance-id:1").
				withAffinityRequest("3", 1, "node", "",
					map[string]string{"layer": "1"}, nil, "cs-port-0:300,instance-id:1").
				withAffinityRequest("4", 1, "node", "",
					map[string]string{"layer": "1"}, nil, "cs-port-1:500,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectGrantNodes: map[string]string{
				"0": "node/br-0-b",
				"1": "node/br-half",
				"2": "node/br-1-a",
				"3": "node/br-0-a",
				"4": "node/br-1-b",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
				"1": {
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"cs-port/nic-5": DefaultNicBW,
					"instance-id/0": 1,
				},
				"2": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"3": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"4": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
			},
		},
		{
			name: "affinity preferred than utilization+higher affinity wins",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withAffinityRequest("0", 1, "node", "",
					map[string]string{"layer": "0"},
					map[string]string{"system1-2": "", "system2-3": ""}, "cs-port:300,instance-id:1").
				withAffinityRequest("1", 1, "node", "",
					map[string]string{"layer": "0"},
					map[string]string{"system1-3": "", "system2-3": ""}, "cs-port:300,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectGrantNodes: map[string]string{
				"0": "node/br-2-g00",
				"1": "node/br-3-g1",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"1": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
			},
		},
		{
			name: "health preferred than affinity+utilization",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withAffinityRequest("0", 1, "node", "",
					map[string]string{"layer": "0"},
					map[string]string{"system1-3": "", "system2-3": ""}, "cs-port:300,instance-id:1").
				withAffinityRequest("1", 1, "node", "",
					map[string]string{"layer": "0"},
					map[string]string{"system1-5": "", "system2-5": ""}, "cs-port:500,instance-id:1").
				withAffinityRequest("2", 1, "node", "",
					map[string]string{"layer": "0"},
					map[string]string{"system1-5": "", "system2-5": ""}, "cs-port:600,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectGrantNodes: map[string]string{
				"0": "node/br-3-g1",
				"1": "node/br-2-g0",
				"2": "node/br-2-g00",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
				"1": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
				"2": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"cs-port/nic-5": DefaultNicBW,
					"instance-id/0": 1,
				},
			},
		},
		{
			name: "allow schedule without soft affinity",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withAffinityRequest("0", 1, "node", "",
					map[string]string{"layer": "0"},
					map[string]string{"system1-4": "", "system2-4": ""}, "cs-port:500,instance-id:1").
				withAffinityRequest("1", 1, "node", "",
					map[string]string{"layer": "0"},
					map[string]string{"system1-2": "", "system2-2": ""}, "cs-port:500,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			expectGrantNodes: map[string]string{
				"0": "node/br-4-g2",
				"1": "node/br-2-g0",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
				"1": {
					"cs-port/nic-0": DefaultNicBW,
					"cs-port/nic-1": DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"cs-port/nic-4": DefaultNicBW,
					"instance-id/0": 1,
				},
			},
		},
		{
			name: "br consolidate nic schedule",
			req: newGroupRequestBuilder(rlv1.BrRequestName, 1).
				withAffinityRequest("0", 1, "node", "",
					nil, nil, "cs-port:500,instance-id:1").
				withAffinityRequest("1", 1, "node", "",
					nil, nil, "cs-port:300,instance-id:1").
				withKeySort(BrRequestKeySort).
				build(),
			extraRes: []Resource{
				newResource("node/br-0-consolidate", "role-broadcastreduce:",
					"pod:32,instance-id:12,"+
						"cs-port/nic-0:400,cs-port/nic-1:400,cs-port/nic-2:100,cs-port/nic-3:100,cs-port/nic-4:300").
					WithIndex(SubresourceIndex{
						Subresources: map[string]map[string]bool{
							NodeNicSubresource: {
								"cs-port/nic-0": true,
								"cs-port/nic-1": true,
								"cs-port/nic-2": true,
								"cs-port/nic-3": true,
								"cs-port/nic-4": true,
							},
						},
					}),
			},
			expectGrantNodes: map[string]string{
				"0": "node/br-0-consolidate",
				"1": "node/br-0-consolidate",
			},
			expectGrantRes: map[string]Subresources{
				"0": {
					"cs-port/nic-1": 4 * DefaultNicBW,
					"cs-port/nic-3": DefaultNicBW,
					"instance-id/1": 1,
				},
				"1": {
					"cs-port/nic-0": 2 * DefaultNicBW,
					"cs-port/nic-2": DefaultNicBW,
					"instance-id/0": 1,
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, extraRes := range tc.extraRes {
				if len(extraRes.subresources) > 0 {
					c.Add(extraRes)
				}
			}
			defer func() {
				for _, extraRes := range tc.extraRes {
					c.Remove(extraRes.Id())
				}
			}()

			result, err := c.FindGroupScheduling(tc.req)
			//t.Log(c.List())
			t.Log(result.Grants, err)
			if tc.expectErr == "" {
				require.NoError(t, err)
			}
			for id, rid := range tc.expectGrantNodes {
				require.Equal(t, rid, result.Grants[id].ResourceIds[0])
			}
			for id, res := range tc.expectGrantRes {
				// expect specific "nic" grant instead of general "cs-port" grant
				require.Empty(t, result.Grants[id].Subresources)
				require.Equal(t, res, result.Grants[id].SubresourceOverride[result.Grants[id].ResourceIds[0]])
			}
		})
	}
}

func newResource(id string, props string, allocs string) Resource {
	rtype, rname := MustRIDToTypeName(id)
	p := MustSplitProps(strings.Split(props, ","))
	a := MapValues(
		func(a string) int { i, _ := strconv.Atoi(a); return i },
		MustSplitProps(strings.Split(allocs, ",")))
	return NewResource(rtype, rname, p, a, nil)
}

func newSystemResource(name string, portCount int, props string) Resource {
	return newResource("system/"+name, props,
		fmt.Sprintf("%s:1,%s:%d", SystemSubresource, SystemPortSubresource, portCount))
}

type groupRequestBuilder struct {
	GroupRequest
}

func newGroupRequestBuilder(id string, count int) *groupRequestBuilder {
	return &groupRequestBuilder{GroupRequest{Id: id, Count: count, Requests: map[string]Request{}, KeySort: ActWgtKeySort}}
}

func newRequest(id string, count int, type_ string, props string, subresources string) Request {
	filter := NewFilter(
		type_,
		MapValues(
			func(v string) []string { return strings.Split(v, "|") },
			MustSplitProps(strings.Split(props, ","))),
		MapValues(
			func(a string) int { i, _ := strconv.Atoi(a); return i },
			MustSplitProps(strings.Split(subresources, ","))),
	)
	return Request{Id: id, Count: count, Filter: filter}
}

func (r Request) withAnnotation(anno map[string]string) Request {
	r.Annotations = anno
	return r
}

func (b *groupRequestBuilder) withRequest(id string, count int, type_ string, props string, subresources string) *groupRequestBuilder {
	b.GroupRequest.Requests[id] = newRequest(id, count, type_, props, subresources)
	return b
}

func (b *groupRequestBuilder) withAnnotations(anno map[string]string) *groupRequestBuilder {
	for i, req := range b.GroupRequest.Requests {
		req.Annotations = anno
		b.GroupRequest.Requests[i] = req
	}
	return b
}

func (b *groupRequestBuilder) withAffinityRequest(id string, count int, type_ string,
	props string, antiAffinities map[string]string, affinity map[string]string, subresources string) *groupRequestBuilder {
	req := newRequest(id, count, type_, props, subresources)
	req.antiAffinities = antiAffinities
	req.allocateProps = antiAffinities
	req.affinities = affinity
	b.GroupRequest.Requests[id] = req
	return b
}

func (b *groupRequestBuilder) withKeySort(k func(map[string]Request) []string) *groupRequestBuilder {
	b.GroupRequest.KeySort = k
	return b
}

func (b *groupRequestBuilder) build() GroupRequest {
	return b.GroupRequest
}

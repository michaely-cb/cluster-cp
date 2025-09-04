//go:build default

package resource

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestSubresourceFromContainer(t *testing.T) {
	for _, testcase := range []struct {
		name     string
		c        corev1.Container
		expected Subresources
	}{
		{
			name:     "empty",
			c:        corev1.Container{},
			expected: Subresources{PodSubresource: 1},
		},
		{
			name: "requests",
			c: corev1.Container{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: *resource.NewQuantity(16<<30, resource.BinarySI),
						corev1.ResourceCPU:    *resource.NewMilliQuantity(64000, resource.DecimalSI),
					},
				},
			},
			expected: Subresources{
				PodSubresource: 1,
				MemSubresource: 16 << 10,
				CPUSubresource: 64000,
			},
		},
		{
			name: "limits and requests",
			c: corev1.Container{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: *resource.NewQuantity(8<<30, resource.BinarySI),
						corev1.ResourceCPU:    *resource.NewMilliQuantity(32000, resource.DecimalSI),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU: *resource.NewMilliQuantity(64000, resource.DecimalSI),
					},
				},
			},
			expected: Subresources{
				PodSubresource: 1,
				MemSubresource: 8 << 10,
				CPUSubresource: 64000,
			},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			actual := SubresourceFromContainers([]*corev1.Container{&testcase.c})
			if len(actual) != len(testcase.expected) {
				t.Errorf("expected %v, got %v", testcase.expected, actual)
			}
			for k, v := range testcase.expected {
				if actual[k] != v {
					t.Errorf("expected %v, got %v", testcase.expected, actual)
				}
			}
		})
	}
}

func TestGetHighestScoredIndexRes(t *testing.T) {
	// The affinity input should look like "system-0-2"
	applyPortAffinityPrefix := func(affinity string) string {
		return fmt.Sprintf("%s/%s", PortAffinityPropKeyPrefix, affinity)
	}
	applyNicSrPrefix := func(nicName string) string {
		return fmt.Sprintf("%s/%s", NodeNicSubresource, nicName)
	}
	nic0SrName := applyNicSrPrefix("nic-0")
	nic1SrName := applyNicSrPrefix("nic-1")

	res := NewResource(NodeType, "node-x", map[string]string{
		RoleAxPropKey: "",
		// nic-0 has affinity to cs port 2 and 3
		applyPortAffinityPrefix("system-0-2"): nic0SrName,
		applyPortAffinityPrefix("system-1-2"): nic0SrName,
		applyPortAffinityPrefix("system-0-3"): nic0SrName,
		applyPortAffinityPrefix("system-1-3"): nic0SrName,

		// nic-1 has affinity to cs port 4 and 5
		applyPortAffinityPrefix("system-0-4"): nic1SrName,
		applyPortAffinityPrefix("system-1-4"): nic1SrName,
		applyPortAffinityPrefix("system-0-5"): nic1SrName,
		applyPortAffinityPrefix("system-1-5"): nic1SrName,
	}, map[string]int{
		nic0SrName: 2,
		nic1SrName: 2,
	}, nil).WithIndex(SubresourceIndex{
		Subresources: map[string]map[string]bool{
			NodeNicSubresource: {
				nic0SrName: true,
				nic1SrName: true,
			},
		},
	})
	for _, tc := range []struct {
		name        string
		allocated   map[string]int
		affinityKey string

		expectScore  int
		expectOutput string
	}{
		{
			name:        "equal load and nics have no affinity, name wins",
			affinityKey: applyPortAffinityPrefix("system-0-7"),

			expectScore:  1,
			expectOutput: nic0SrName,
		},
		{
			name:        "equal load and request does not contain affinity key, name wins",
			affinityKey: "",

			expectScore:  1,
			expectOutput: nic0SrName,
		},
		{
			name:        "equal load, affinity wins",
			affinityKey: applyPortAffinityPrefix("system-0-4"),

			expectScore:  2000,
			expectOutput: nic1SrName,
		},
		{
			name:        "equal load, affinity wins - nic name ordering does not matter",
			affinityKey: applyPortAffinityPrefix("system-0-2"),

			expectScore:  2000,
			expectOutput: nic0SrName,
		},
		{
			name:        "unequal load, affinity wins if available",
			allocated:   map[string]int{nic1SrName: 1},
			affinityKey: applyPortAffinityPrefix("system-0-4"),

			expectScore:  1000,
			expectOutput: nic1SrName,
		},
		{
			name:        "unequal load, anti-affinity wins if affinity is unavailable",
			allocated:   map[string]int{nic1SrName: 2},
			affinityKey: applyPortAffinityPrefix("system-0-4"),

			expectScore:  1,
			expectOutput: nic0SrName,
		},
		{
			name:        "unequal load, least anti-affinity load wins",
			allocated:   map[string]int{nic1SrName: 1},
			affinityKey: applyPortAffinityPrefix("system-0-7"),

			expectScore:  1,
			expectOutput: nic0SrName,
		},
		{
			name:        "unequal load, anti-affinity wins if affinity is not available",
			allocated:   map[string]int{nic0SrName: 2},
			affinityKey: applyPortAffinityPrefix("system-0-2"),

			expectScore:  1,
			expectOutput: nic1SrName,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resCopy := res.UnallocatedCopy()
			resCopy.Allocate(nil, SubresourceAllocation{Subresources: tc.allocated})

			score, resName := resCopy.getHighestScoredIndexRes(NodeNicSubresource, tc.affinityKey, 1, false)
			assert.Equal(t, tc.expectScore, score)
			assert.Equal(t, tc.expectOutput, resName)
		})
	}
}

func TestRange_IsMergeableWith(t *testing.T) {
	for _, tc := range []struct {
		name            string
		range1          Range
		range2          Range
		expectMergeable bool
	}{
		{
			name:            "invalid ranges",
			range1:          Range{0, 0},
			range2:          Range{0, 0},
			expectMergeable: false,
		},
		{
			name:            "non-overlap 1",
			range1:          Range{0, 1},
			range2:          Range{1, 2},
			expectMergeable: true,
		},
		{
			name:            "non-overlap 2",
			range1:          Range{1, 2},
			range2:          Range{0, 1},
			expectMergeable: true,
		},
		{
			name:            "same-end",
			range1:          Range{0, 2},
			range2:          Range{1, 2},
			expectMergeable: true,
		},
		{
			name:            "same-start",
			range1:          Range{0, 2},
			range2:          Range{0, 1},
			expectMergeable: true,
		},
		{
			name:            "overlap",
			range1:          Range{0, 2},
			range2:          Range{1, 3},
			expectMergeable: true,
		},
		{
			name:            "negative numbers",
			range1:          Range{-1, 0},
			range2:          Range{0, 1},
			expectMergeable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.range1.IsMergeableWith(tc.range2)
			if tc.expectMergeable != actual {
				t.Errorf("testcase %s: expected %v, got %v", tc.name, tc.expectMergeable, actual)
				assert.Equal(t, tc.expectMergeable, actual)
			}
		})
	}
}

func TestSubresourceRanges_Canonicalize(t *testing.T) {
	dummyRes := "dummy"
	anotherDummyRes := "another"
	for _, tc := range []struct {
		name           string
		input          SubresourceRanges
		expectedOutput SubresourceRanges
	}{
		{
			name:           "same range",
			input:          SubresourceRanges{dummyRes: []Range{{0, 1}}},
			expectedOutput: SubresourceRanges{dummyRes: []Range{{0, 1}}},
		},
		{
			name:           "invalid ranges",
			input:          SubresourceRanges{dummyRes: []Range{{Start: 0, End: 0}}},
			expectedOutput: SubresourceRanges{dummyRes: nil},
		},
		{
			name:           "contiguous ranges",
			input:          SubresourceRanges{dummyRes: []Range{{0, 1}, {1, 2}}},
			expectedOutput: SubresourceRanges{dummyRes: []Range{{0, 2}}},
		},
		{
			name:           "overlapping ranges",
			input:          SubresourceRanges{dummyRes: []Range{{1, 3}, {0, 2}}},
			expectedOutput: SubresourceRanges{dummyRes: []Range{{0, 3}}},
		},
		{
			name:           "non-contiguous ranges",
			input:          SubresourceRanges{dummyRes: []Range{{2, 3}, {0, 1}}},
			expectedOutput: SubresourceRanges{dummyRes: []Range{{0, 1}, {2, 3}}},
		},
		{
			name: "multiple subresource ranges",
			input: SubresourceRanges{
				dummyRes:        []Range{{2, 3}, {0, 1}},
				anotherDummyRes: []Range{{1, 3}, {3, 5}},
			},
			expectedOutput: SubresourceRanges{
				dummyRes:        []Range{{0, 1}, {2, 3}},
				anotherDummyRes: []Range{{1, 5}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.input.Canonicalize()
			if !tc.expectedOutput.Equals(actual) {
				t.Errorf("testcase %s: expected %v, got %v", tc.name, tc.expectedOutput, actual)
				assert.True(t, tc.expectedOutput.Equals(actual))
			}
		})
	}
}

func TestSubresourceRanges_Subtract(t *testing.T) {
	dummyRes := "dummy"
	anotherDummyRes := "another"
	for _, tc := range []struct {
		name           string
		range1         SubresourceRanges
		range2         SubresourceRanges
		expectedOk     bool
		expectedOutput SubresourceRanges
	}{
		{
			name:           "subtract all",
			range1:         SubresourceRanges{dummyRes: []Range{{0, 1}}},
			range2:         SubresourceRanges{dummyRes: []Range{{0, 1}}},
			expectedOk:     true,
			expectedOutput: SubresourceRanges{},
		},
		{
			name: "subtract partial -1",
			range1: SubresourceRanges{
				dummyRes:        []Range{{0, 2}},
				anotherDummyRes: []Range{{0, 1}},
			},
			range2:     SubresourceRanges{dummyRes: []Range{{0, 1}}},
			expectedOk: true,
			expectedOutput: SubresourceRanges{
				dummyRes:        []Range{{1, 2}},
				anotherDummyRes: []Range{{0, 1}},
			},
		},
		{
			name: "subtract partial -2",
			range1: SubresourceRanges{
				dummyRes:        []Range{{0, 2}},
				anotherDummyRes: []Range{{0, 1}},
			},
			range2:         SubresourceRanges{dummyRes: []Range{{0, 2}}},
			expectedOk:     true,
			expectedOutput: SubresourceRanges{anotherDummyRes: []Range{{0, 1}}},
		},
		{
			name:           "subtract multiple ranges",
			range1:         SubresourceRanges{dummyRes: []Range{{0, 5}}},
			range2:         SubresourceRanges{dummyRes: []Range{{0, 1}, {2, 3}, {4, 5}}},
			expectedOk:     true,
			expectedOutput: SubresourceRanges{dummyRes: []Range{{1, 2}, {3, 4}}},
		},
		{
			name:           "not subtractable - 1",
			range1:         SubresourceRanges{dummyRes: []Range{{0, 5}}},
			range2:         SubresourceRanges{dummyRes: []Range{{4, 6}}},
			expectedOk:     false,
			expectedOutput: SubresourceRanges{},
		},
		{
			name:           "not subtractable - 2",
			range1:         SubresourceRanges{dummyRes: []Range{{0, 5}}},
			range2:         SubresourceRanges{dummyRes: []Range{{-1, 1}}},
			expectedOk:     false,
			expectedOutput: SubresourceRanges{},
		},
		{
			name:           "not subtractable - 3",
			range1:         SubresourceRanges{dummyRes: []Range{{0, 5}}},
			range2:         SubresourceRanges{dummyRes: []Range{{-1, 6}}},
			expectedOk:     false,
			expectedOutput: SubresourceRanges{},
		},
		{
			name:           "not subtractable - 4",
			range1:         SubresourceRanges{dummyRes: []Range{{0, 5}}},
			range2:         SubresourceRanges{dummyRes: []Range{{2, 3}, {5, 6}}},
			expectedOk:     false,
			expectedOutput: SubresourceRanges{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, ok := tc.range1.Subtract(tc.range2)
			if tc.expectedOk != ok || !tc.expectedOutput.Equals(actual) {
				t.Errorf("testcase %s: expected %v, got %v", tc.name, tc.expectedOutput, actual)
				assert.True(t, tc.expectedOutput.Equals(actual))
			}
		})
	}
}

func TestSubresourceRanges_Add(t *testing.T) {
	dummyRes := "dummy"
	anotherDummyRes := "another"
	for _, tc := range []struct {
		name           string
		range1         SubresourceRanges
		range2         SubresourceRanges
		expectedOutput SubresourceRanges
	}{
		{
			name:           "same range",
			range1:         SubresourceRanges{dummyRes: []Range{{0, 1}}},
			range2:         SubresourceRanges{dummyRes: []Range{{0, 1}}},
			expectedOutput: SubresourceRanges{dummyRes: []Range{{0, 1}}},
		},
		{
			name:   "multiple subresource ranges - merge",
			range1: SubresourceRanges{dummyRes: []Range{{0, 1}, {1, 2}}},
			range2: SubresourceRanges{anotherDummyRes: []Range{{0, 1}, {1, 2}}},
			expectedOutput: SubresourceRanges{
				dummyRes:        []Range{{0, 2}},
				anotherDummyRes: []Range{{0, 2}},
			},
		},
		{
			name:   "multiple subresource ranges - non-overlap",
			range1: SubresourceRanges{dummyRes: []Range{{0, 1}, {2, 3}}},
			range2: SubresourceRanges{anotherDummyRes: []Range{{0, 1}, {2, 3}}},
			expectedOutput: SubresourceRanges{
				dummyRes:        []Range{{0, 1}, {2, 3}},
				anotherDummyRes: []Range{{0, 1}, {2, 3}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.range1.Add(tc.range2)
			if !tc.expectedOutput.Equals(actual) {
				t.Errorf("testcase %s: expected %v, got %v", tc.name, tc.expectedOutput, actual)
				assert.True(t, tc.expectedOutput.Equals(actual))
			}
		})
	}
}

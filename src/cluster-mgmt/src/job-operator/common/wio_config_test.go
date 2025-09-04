//go:build default

package common

import (
	"reflect"
	"testing"
)

func TestGenerateSystemInterleavingOrder(t *testing.T) {
	tests := []struct {
		name     string
		targets  []WioTarget
		expected []int
	}{
		{
			name: "happy round-robin case",
			targets: []WioTarget{
				{AllSystemIndices: []int{0}}, // pod 0
				{AllSystemIndices: []int{0}}, // pod 1
				{AllSystemIndices: []int{1}}, // pod 2
				{AllSystemIndices: []int{1}}, // pod 3
				{AllSystemIndices: []int{2}}, // pod 4
				{AllSystemIndices: []int{2}}, // pod 5
			},
			// Groups: "0":[0,1], "1":[2,3], "2":[4,5]
			expected: []int{0, 2, 4, 1, 3, 5},
		},
		{
			name: "uneven distribution",
			targets: []WioTarget{
				{AllSystemIndices: []int{0}}, // pod 0
				{AllSystemIndices: []int{0}}, // pod 1
				{AllSystemIndices: []int{0}}, // pod 2
				{AllSystemIndices: []int{0}}, // pod 3
				{AllSystemIndices: []int{1}}, // pod 4
				{AllSystemIndices: []int{1}}, // pod 5
				{AllSystemIndices: []int{2}}, // pod 6
			},
			// Groups: "0":[0,1,2,3], "1":[4,5], "2":[6]
			expected: []int{0, 4, 6, 1, 5, 2, 3},
		},
		{
			name: "multi-system pods I",
			targets: []WioTarget{
				{AllSystemIndices: []int{0, 1}}, // pod 0
				{AllSystemIndices: []int{0, 1}}, // pod 1
				{AllSystemIndices: []int{2}},    // pod 2
				{AllSystemIndices: []int{2}},    // pod 3
				{AllSystemIndices: []int{3}},    // pod 4
				{AllSystemIndices: []int{3}},    // pod 5
			},
			// Groups: "0,1":[0,1], "2":[2,3], "3":[4,5]
			expected: []int{0, 2, 4, 1, 3, 5},
		},
		{
			name: "multi-system pods II",
			targets: []WioTarget{
				{AllSystemIndices: []int{0, 1}},    // pod 0
				{AllSystemIndices: []int{0, 1}},    // pod 1
				{AllSystemIndices: []int{2}},       // pod 2
				{AllSystemIndices: []int{0, 2}},    // pod 3
				{AllSystemIndices: []int{0, 1, 2}}, // pod 4
				{AllSystemIndices: []int{2}},       // pod 5
			},
			// Groups: "0,1":[0,1], "2":[2,5], "0,2":[3], "0,1,2":[4]
			expected: []int{0, 2, 3, 4, 1, 5},
		},
		{
			name: "single system",
			targets: []WioTarget{
				{AllSystemIndices: []int{5}}, // pod 0
				{AllSystemIndices: []int{5}}, // pod 1
				{AllSystemIndices: []int{5}}, // pod 2
				{AllSystemIndices: []int{5}}, // pod 3
				{AllSystemIndices: []int{5}}, // pod 4
			},
			// Sequential since only one group
			expected: []int{0, 1, 2, 3, 4},
		},
		{
			name: "single pod",
			targets: []WioTarget{
				{AllSystemIndices: []int{0}},
			},
			expected: []int{0},
		},
		{
			name:     "empty wio targets",
			targets:  []WioTarget{},
			expected: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateSystemInterleavingOrder(tt.targets)

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("GenerateSystemInterleavingOrder() = %v, expected %v", result, tt.expected)
			}

			// Verify all pods are included exactly once
			if len(result) != len(tt.targets) {
				t.Errorf("Result length = %d, expected %d", len(result), len(tt.targets))
			}

			// Verify no duplicates and all indices are valid
			seen := make(map[int]bool)
			for _, podIdx := range result {
				if podIdx < 0 || podIdx >= len(tt.targets) {
					t.Errorf("Invalid pod index %d, should be in range [0, %d)", podIdx, len(tt.targets))
				}
				if seen[podIdx] {
					t.Errorf("Duplicate pod index %d in result", podIdx)
				}
				seen[podIdx] = true
			}
		})
	}
}

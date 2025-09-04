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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

func TestBuildBrLogicalTreeSingleGroup(t *testing.T) {
	maxSystemCount := 1088
	unbalancedForValidation := map[int]bool{
		5:  true,
		6:  true,
		7:  true,
		9:  true,
		10: true,
		11: true,
		13: true,
		17: true,
		24: true,
	}
	var mu sync.Mutex
	var allUnbalancedCases []int
	type job struct {
		c        int
		balanced bool
	}
	worker := func(ch <-chan job, wg *sync.WaitGroup) {
		defer wg.Done()
		for tc := range ch {
			perStampSystems := generateSystemCombo(tc.c)
			var orderedSystems []string
			for stamp := range perStampSystems {
				for _, systemName := range perStampSystems[stamp] {
					orderedSystems = append(orderedSystems, systemName)
				}
			}
			expected := int(math.Ceil(math.Log(float64(tc.c)) / math.Log(4)))

			var brs []*BrInternalNode
			layers, ports, _, brs := BuildBrGroupTrees(perStampSystems, orderedSystems, tc.balanced, wsapisv1.BrTreePolicyPort)
			if layers != expected {
				t.Errorf("unexpected layer count for system size:`%d`: expected `%d` but got `%d`, ports: %d",
					tc.c, expected, layers, ports)
				for _, br := range brs {
					t.Errorf("%+v", *br)
				}
			}
			assert.Equal(t, perStampSystems["group0"], orderedSystems, fmt.Sprintf("see %v, expecting %v", orderedSystems, perStampSystems["group0"]))
			//t.Log("sys count:", tc.c, "ports:", ports)

			unbalanced := false
			for _, br := range brs {
				layersSeen := map[int]bool{}
				for _, egr := range br.Egresses {
					if !egr.IsBr {
						layersSeen[-1] = true
					} else {
						layersSeen[brs[egr.BrSetId].Layer] = true
					}
				}
				if tc.balanced {
					// assert consistent layer on all egresses
					assert.Equal(t, 1, len(layersSeen), fmt.Sprintf("%v, %v", perStampSystems, *br))
				}
				if len(Keys(layersSeen)) > 1 {
					unbalanced = true
				}
			}
			if unbalanced {
				mu.Lock()
				allUnbalancedCases = append(allUnbalancedCases, tc.c)
				mu.Unlock()
			}
			if !tc.balanced && unbalancedForValidation[tc.c] {
				assert.True(t, unbalanced, fmt.Sprintf("%d systems BR tree should be unbalanced", tc.c))
			}
		}
	}

	for _, tc := range []struct {
		name     string
		balanced bool
	}{
		{
			name:     "port policy",
			balanced: false,
		},
		{
			name:     "port policy balanced",
			balanced: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			numWorkers := 10
			jobs := make(chan job, maxSystemCount)
			// Create a wait group to wait for all workers to finish
			var wg sync.WaitGroup
			wg.Add(numWorkers)

			// Start workers
			for w := 1; w <= numWorkers; w++ {
				go worker(jobs, &wg)
			}

			// Add jobs to the jobs channel
			for c := 2; c <= maxSystemCount; c++ {
				jobs <- job{
					c:        c,
					balanced: tc.balanced,
				}
			}
			close(jobs)

			// Wait for all workers to finish
			wg.Wait()
		})
	}
}

func TestBuildBrLogicalTreeMultiGroups(t *testing.T) {
	sysCountToPortUsage := map[int]int{}
	for c := 2; c <= 1088; c++ {
		perStampSystems := generateSystemCombo(c)
		var orderedSystems []string
		for stamp := range perStampSystems {
			for _, systemName := range perStampSystems[stamp] {
				orderedSystems = append(orderedSystems, systemName)
			}
		}
		var brs []*BrInternalNode
		_, _, _, brs = BuildBrGroupTrees(perStampSystems, orderedSystems, false, wsapisv1.BrTreePolicyPort)
		portCount := 0
		for _, br := range brs {
			portCount += 1 + len(br.Egresses)
		}
		//t.Log("sys count:", c, "port count:", portCount)
		sysCountToPortUsage[c] = portCount
	}

	type job struct {
		c        int
		balanced bool
		policy   string
		groupSys map[string][]string
		group    int
	}
	var maxPortDiff atomic.Int32
	var totalExtraPorts atomic.Int32
	var maxLayerDiff atomic.Int32
	var totalExtraLayers atomic.Int32
	var totalCombinations atomic.Int32
	worker := func(ch <-chan job, wg *sync.WaitGroup) {
		defer wg.Done()
		for tc := range ch {
			policy := Ternary(tc.policy != "", tc.policy, wsapisv1.BrTreePolicySpine)
			var orderedSystems []string
			for stamp := range tc.groupSys {
				for _, systemName := range tc.groupSys[stamp] {
					orderedSystems = append(orderedSystems, systemName)
				}
			}
			layer, portCount, crossTraffic, brs := BuildBrGroupTrees(tc.groupSys, orderedSystems, tc.balanced, policy)
			if policy == wsapisv1.BrTreePolicySpine && !tc.balanced {
				require.True(t, crossTraffic <= tc.group-1, crossTraffic)
			}
			setIdSeen := map[int]bool{}
			for _, br := range brs {
				require.False(t, setIdSeen[br.SetId])
				setIdSeen[br.SetId] = true
				//t.Log("sysCount:", c, "left:", i, "right:", c-i, "br id:", br.SetId, "layer:", br.Layer)
			}

			totalPortWithoutSplit := sysCountToPortUsage[tc.c]
			portDiff := portCount - totalPortWithoutSplit
			if portDiff != 0 {
				//t.Log("sysCount:", c, "sys:", groupSys, "ports:", portCount, "portDiff:", portDiff, "brs", len(brs))
				if portDiff > int(maxPortDiff.Load()) {
					maxPortDiff.Store(int32(portCount - totalPortWithoutSplit))
				}
			}
			var expectPort, totalSys int
			for i, group := range SortedKeysByValSize(tc.groupSys, true) {
				sysCount := len(tc.groupSys[group])
				totalSys += sysCount
				expectPort += sysCountToPortUsage[sysCount]
				if i == len(tc.groupSys)-1 {
					break
				}
			}
			expectPort += sysCountToPortUsage[tc.c-totalSys+tc.group-1]
			if expectPort != portCount {
				//t.Log("sysCount:", c, "left:", i, "right:", c-i, "ports:", portCount, "expect:", expectPort)
			}

			expectLayer := int(math.Ceil(math.Log(float64(tc.c)) / math.Log(4)))
			layerDiff := layer - expectLayer
			if layer != expectLayer {
				//t.Log("sysCount:", c, "left:", i, "right:", c-i, "layer:", layer, "expect:", expectLayer)
				if layer-expectLayer > int(maxLayerDiff.Load()) {
					maxLayerDiff.Store(int32(layer - expectLayer))
				}
			}

			totalExtraLayers.Add(int32(layerDiff))
			totalExtraPorts.Add(int32(portDiff))
			totalCombinations.Inc()
		}
	}

	for _, tc := range []struct {
		name               string
		balanced           bool
		maxSys             int
		minSys             int
		group              int
		groups             []int
		policy             string
		expectCrossTraffic int
	}{
		{
			name:     "spine policy 2 group",
			balanced: false,
			maxSys:   192,
			minSys:   2,
			group:    2,
		},
		{
			name:     "spine policy 2 group + balanced",
			balanced: true,
			maxSys:   192,
			minSys:   2,
			group:    2,
		},
		{
			name:     "spine policy 3 group",
			balanced: false,
			maxSys:   128,
			minSys:   64,
			group:    3,
		},
		{
			name:     "spine policy 4 group",
			balanced: false,
			maxSys:   128,
			minSys:   120,
			group:    4,
		},
		{
			name:     "spine policy 3 group + balanced",
			balanced: true,
			maxSys:   128,
			minSys:   64,
			group:    3,
		},
		{
			name:     "spine policy 4 group + balanced",
			balanced: true,
			maxSys:   128,
			minSys:   120,
			group:    4,
		},
		{
			name:               "port policy cross traffic check",
			groups:             []int{5, 3},
			expectCrossTraffic: 2,
			policy:             wsapisv1.BrTreePolicyPort,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.groups) > 0 {
				combo := generateSystemCombo(tc.groups...)
				var orderedSystems []string
				for stamp := range combo {
					for _, systemName := range combo[stamp] {
						orderedSystems = append(orderedSystems, systemName)
					}
				}
				_, _, crossTraffic, tree := BuildBrGroupTrees(combo, orderedSystems, tc.balanced, tc.policy)
				require.Equal(t, tc.expectCrossTraffic, crossTraffic, tree.String(), combo)
				return
			}
			// reset
			maxPortDiff.Store(0)
			totalExtraPorts.Store(0)
			maxLayerDiff.Store(0)
			totalExtraLayers.Store(0)
			totalCombinations.Store(0)

			numWorkers := 10
			// Create a wait group to wait for all workers to finish
			var wg sync.WaitGroup
			wg.Add(numWorkers)
			jobs := make(chan job, (tc.maxSys-tc.minSys)*100)

			// Start workers
			for w := 1; w <= numWorkers; w++ {
				go worker(jobs, &wg)
			}

			for c := tc.minSys; c <= tc.maxSys; c++ {
				for _, groupSys := range generateFullSysCombo(tc.group, c) {
					jobs <- job{
						c:        c,
						balanced: tc.balanced,
						groupSys: groupSys,
						group:    tc.group,
						policy:   tc.policy,
					}
				}
			}
			close(jobs)

			// Wait for all workers to finish
			wg.Wait()

			avgExtraPorts := float32(totalExtraPorts.Load()) / (float32(totalCombinations.Load()))
			t.Log("maxLayerDiff:", maxLayerDiff.Load(), "avgExtraLayers:", float32(totalExtraLayers.Load())/(float32(totalCombinations.Load())))
			t.Log("maxPortDiff:", maxPortDiff.Load(), "avgExtraPorts:", avgExtraPorts)
			if !tc.balanced {
				switch tc.group {
				case 2:
					require.True(t, maxPortDiff.Load() <= 2, "expect", 2, "actual", maxPortDiff.Load())
				case 3, 4:
					require.True(t, maxPortDiff.Load() <= 4, "expect", 4, "actual", maxPortDiff.Load())
				}
			} else {
				require.True(t, maxLayerDiff.Load() <= 1, "expect", 1, "actual", maxLayerDiff.Load())
			}
		})
	}
}

// generate full sys combo
func generateFullSysCombo(group, total int) []map[string][]string {
	var res []map[string][]string
	for _, g := range generateFullGroupCombo(group, total) {
		res = append(res, generateSystemCombo(g...))
	}
	return res
}

// generate full group combo
func generateFullGroupCombo(group, total int) [][]int {
	current := make([]int, group)
	for i := group - 1; i >= 0; i-- {
		current[i] = Ternary(i != 0, 1, total-group+1)
	}
	combo := [][]int{current}
	for {
		next, ok := GetNextGroupCombo(current, nil)
		if next == nil {
			break
		}
		if ok {
			combo = append(combo, next)
		}
		current = next
	}
	return combo
}

// generate sys combo based on group distribution
func generateSystemCombo(groups ...int) map[string][]string {
	sysId := 0
	groupSys := map[string][]string{}
	for i := 0; i < len(groups); i++ {
		group := fmt.Sprintf("group%d", i)
		for j := 0; j < groups[i]; j++ {
			sys := fmt.Sprintf("sys%d", sysId)
			groupSys[group] = append(groupSys[group], sys)
			sysId++
		}
	}
	return groupSys
}

// generate random combo
func generateRandomSysCombo(group, total int) map[string][]string {
	var groups []int
	last := total
	for i := 0; i < group-1; i++ {
		count := rand.Intn(group) + 1
		groups = append(groups, count)
		last -= count
	}
	groups = append(groups, last)
	return generateSystemCombo(groups...)
}

//func TestComboGeneration(t *testing.T) {
//	t.Log(generateFullSysCombo(2, 6))
//	t.Log(generateFullGroupCombo(3, 9))
//}

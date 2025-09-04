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
	"encoding/json"
	"math"
	"strconv"
	"strings"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

var (
	// max fanout mode for BR, this will decide the preferred mode for BR
	MaxFanout = 4
)

// BRConfig is read by BR/WGT/ACT/CMD/COORDINATOR at start time
type BRConfig struct {
	CSNodes  []CSNode  `json:"cs_nodes"`
	BRGroups []BRGroup `json:"br_groups,omitempty"`
	BRNodes  []BRNode  `json:"br_nodes,omitempty"`
}

// CSNode is the CS nodes section in BRConfig needed by BR/ACT/COORDINATOR
type CSNode struct {
	NodeId     int    `json:"node_id"`
	CmAddr     string `json:"cmaddr"`
	SystemName string `json:"system_name"`
	MgmtAddr   string `json:"mgmt_addr,omitempty"`
	WseGroupId int    `json:"wse_group_id,omitempty"` // group id for expert parallel. system with the same group id will be receiving the same weights/gradients
}

// BRNode is the BR nodes section in BRConfig where each BR node will read its own info
type BRNode struct {
	Comment     string   `json:"_comment,omitempty"`
	NodeId      int      `json:"node_id"`
	GroupId     int      `json:"group_id"`
	DomainId    int      `json:"domain_id"`
	ControlAddr string   `json:"control_addr"`
	IngressAddr string   `json:"ingress_addr"`
	Egress      []Egress `json:"egress"`
}

// BRGroup is the BR groups section in BRConfig and WGT will connect to BR on the group level
// Each group will connect to 4 domains of FPGA in one CS2
type BRGroup struct {
	GroupId     int            `json:"group_id"`
	Connections []BRConnection `json:"connections"`
}

// Egress BR node will bind source to EgressAddr on one NIC and connect to ControlAddr
// isBr is multi-layer BR structure where BR could be connecting with both CS2/BR and different protocols needed
type Egress struct {
	ControlAddr string `json:"control_addr"`
	EgressAddr  string `json:"egress_addr"`
	IsBr        bool   `json:"is_br"`
}

// BRConnection represents a BR node where it has BR's ControlAddr and DomainId
type BRConnection struct {
	DomainId    int    `json:"domain_id"`
	HasError    bool   `json:"has_error"`
	ControlAddr string `json:"control_addr"`
}

type BrLogicalTree []*BrInternalNode

func (tree BrLogicalTree) String() string {
	treeVal, _ := json.Marshal(tree)
	return string(treeVal)
}

// BrInternalNode used for building config tree where it could be BR node or system Node
type BrInternalNode struct {
	SetId      int                 `json:"set_id"`
	Egresses   []*BrInternalEgress `json:"egresses"`
	SystemName string              `json:"system_name,omitempty"`
	IsBr       bool                `json:"is_br"`
	Layer      int                 `json:"layer"` // node's layer in tree, starting from 1 for br, 0 for system
	Stamp      string              `json:"-"`     // stamp of this node
}

// BrInternalEgress could be either connecting with BR node or CS2 node
type BrInternalEgress struct {
	SystemName string `json:"system_name,omitempty"`
	BrSetId    int    `json:"br_set_id"`
	IsBr       bool   `json:"is_br"`
	Stamp      string `json:"-"` // stamp of downstream node
}

// GetEgressTargets Return a set of system names & a list of BR sets that
// the BrInternalNode talks to as egress targets
func (br *BrInternalNode) GetEgressTargets() (targetSystemNames *Set[string], downstreamBrSets string) {
	targetSystemNames = NewSet[string]()
	downstreamBrSetIds := make([]string, 0)
	for _, e := range br.Egresses {
		if e.IsBr {
			downstreamBrSetIds = append(downstreamBrSetIds, strconv.Itoa(e.BrSetId))
		} else {
			targetSystemNames.Emplace(e.SystemName)
		}
	}
	return targetSystemNames, strings.Join(downstreamBrSetIds, ",")
}

// High level flow:
// 1. builds subtree for n-1 groups by sorted group count order and get n-1 top level BRs
// 2. build last subtree with most systems and pass in the n-1 BRs to merge together
// note: no limitation for group count but group>4 case is not validated yet
// reference: https://cerebras.atlassian.net/wiki/spaces/runtime/pages/3159588880/BR+V2+affinity
func BuildBrGroupTrees(groupSystems map[string][]string, orderedSystems []string, balancedTree bool, policy string) (totalLayer,
	totalPorts, totalCrossTraffic int, finalTree BrLogicalTree) {
	// global unique id for each logical node
	var setId int
	// top BRs of each subtree
	var topLayerNodes []*BrInternalNode
	// sort keys based on system count in group/stamp
	// always build the subtree with fewer systems first to ensure consistent result and layer count
	groups := SortedKeysByValSize(groupSystems, true)
	//  build index
	systemToGroupMap := make(map[string]string)
	for _, group := range groups {
		for _, system := range groupSystems[group] {
			systemToGroupMap[system] = group
		}
	}
	// flattened system list to single group for port policy
	if policy == wsapisv1.BrTreePolicyPort {
		groupSystems = map[string][]string{"": orderedSystems}
		groups = Keys(groupSystems)
	}
	// build one subtree per group
	for i, group := range groups {
		layerCount, portCount, crossTraffic, newBrs := BuildBrLogicalTree(
			setId,
			groupSystems[group],
			systemToGroupMap,
			// only for last subtree with most systems, pass in the top layer newBrs of all subtrees
			Ternary(i == len(groups)-1, topLayerNodes, nil),
			balancedTree)
		totalLayer = layerCount
		totalPorts += portCount
		totalCrossTraffic += crossTraffic
		topBr := newBrs[len(newBrs)-1]
		// set the top layer br to merge in last subtree
		topLayerNodes = append(topLayerNodes, topBr)
		// merge each subtree into final result
		// each subtree will only contain its own nodes in group/stamp
		if topBr.IsBr {
			finalTree = append(finalTree, newBrs...)
			setId = topBr.SetId + 1
		}
	}
	return totalLayer, totalPorts, totalCrossTraffic, finalTree
}

// BuildBrLogicalTree: based on assigned systems + nodes
// return layer count + port count(100gbps per port) + cross traffic + generated BR sets
// cross traffic is a logical number as indication only while actual cross traffic will depend on schedule allocation
// BR set is logical abstraction of one set of 12 or 24 pods/cs-ports as real BR pods request
//
// startSetId: first set id for this tree
// systems: downstream systems for building BRs
// nodes: optional BR nodes from other subtrees for merging purpose, must be sorted in order of smaller layer first
// balancedTree: if set, it's guaranteed to generate a balanced tree at the cost of more ports usage
// while by default the algorithm will prioritize on minimizing ports/layers usage
//
// High level flow:
// 1. keep two levels of nodes: upper/lower, initially lower will be systems and upper will be empty
// 2. greedily build nodes of 1:4 mode on top of lower nodes and add to upper layer until there are <4 lower nodes
// 3. try to meet layer requirement(<=4 nodes -> 1 layer, <=16 nodes -> 2 layers...) for the rest of nodes by
// either directly merge lower to upper or build new node with 1:2/1:3 mode
// 4. swap upper and lower so that lower will be the previous upper and upper will be empty
// 5. repeating 1-4 till only one node in lower
// https://cerebras.atlassian.net/wiki/spaces/runtime/pages/2155413534/BR+Config+Generation+Workflow
func BuildBrLogicalTree(startSetId int, systems []string, systemToGroupMap map[string]string,
	nodes []*BrInternalNode, balancedTree bool) (int, int, int, BrLogicalTree) {
	if len(systems) == 0 {
		// skip for no systems input
		return 0, 0, 0, nil
	} else if len(systems) == 1 && len(nodes) == 0 {
		// return 1 system as internal node for later subtree merge
		// e.g. 4 system with 3 + 1 on each stamp, the 1 system will need to pass to 3 system subtree for merge
		return 0, 0, 0, []*BrInternalNode{{
			SystemName: systems[0],
		}}
	}

	fanOut := MaxFanout
	var totalPorts int
	var crossGroupTraffic int
	var brs []*BrInternalNode
	var upper []*BrInternalNode
	var lower []*BrInternalNode
	// init lower layer
	for _, system := range systems {
		lower = append(lower, &BrInternalNode{
			SystemName: system,
			Stamp:      systemToGroupMap[system],
		})
	}
	// merge passed in nodes to lower
	// systems are always merged, brs merge can be deferred if force to be balanced tree
	if len(nodes) > 0 {
		lastMergedId := -1
		for i, no := range nodes {
			if balancedTree && no.IsBr {
				break
			}
			lower = append(lower, no)
			lastMergedId = i
		}
		// trim already merged nodes
		nodes = nodes[(lastMergedId + 1):]
	}

	for len(lower) > 1 {
		if balancedTree && len(nodes) > 0 {
			// merge passed in nodes to upper if forced to be balanced tree
			// this applies to both port+spine policy
			// idea is to only merge passed in brs at the same layer
			// e.g. for 9 system job with 7+2 combination
			// the left one will generate 3 layer while right side will generate 2 layer by deferring merging br0
			//               br3                   |                             br3
			//            /      \                 |                 /        /   |      \
			//       br1           br2             |            br1         br2   |      br0
			//   /  /  \  \     /  /  \  \         |        /  /  \  \     /  |   |     /   \
			// s0  s1  s2  s3  s4  s5  s6  br0     =>     s0  s1  s2  s3  s4  s5  s6   s7   s8
			//                            /  \     |
			//                          s7   s8    |
			lastMergedId := -1
			upperLayer := lower[0].Layer + 1
			for i, no := range nodes {
				// only merge passed in brs when layer match
				if no.Layer > upperLayer {
					break
				}
				upper = append(upper, no)
				lastMergedId = i
			}
			// trim already merged nodes
			nodes = nodes[(lastMergedId + 1):]
		}

		// prefer full fanout mode
		// right bound is the max count for full fanout mode only BRs in one layer
		rightBound := len(lower)
		if len(lower) > fanOut {
			rightBound = fanOut * (len(lower) / fanOut)
		}
		ports, newBrs := buildBrLayer(lower, fanOut, 0, rightBound, &startSetId, &crossGroupTraffic)
		totalPorts += ports
		upper = append(upper, newBrs...)
		brs = append(brs, newBrs...)

		// maxBrSetInLayer is the max node count in upper layer to meet layer requirement
		// which is the number of BR in a full tree in each layer
		// e.g. for job <= 16 systems, layer 0 will be max 4 nodes
		// recalculate each loop to allow flexibility to change fanout dynamically if needed
		expectTreeDepth := math.Ceil(math.Log(float64(len(lower))) / math.Log(float64(fanOut)))
		maxBrSetInLayer := int(math.Pow(float64(fanOut), expectTreeDepth-1))
		// if lower has remaining unconnected nodes after full mode
		// two options:
		// connect directly with partial mode for balanced mode Or to meet layer requirement
		// otherwise prefer to merge the rest of lower directly to upper for high utilization
		if remaining := len(lower) - rightBound; remaining > 0 && (balancedTree || remaining+len(upper) > maxBrSetInLayer) {
			// if only one node left for upper, connect directly with all the rest nodes to meet layer requirement
			if balancedTree || len(upper) == maxBrSetInLayer-1 {
				ports, newBrs = buildBrLayer(lower, fanOut, rightBound, len(lower), &startSetId, &crossGroupTraffic)
				totalPorts += ports
				upper = append(upper, newBrs...)
				brs = append(brs, newBrs...)
				rightBound = len(lower)
			} else if len(upper) < maxBrSetInLayer-1 {
				// if there are more than one node left, prefer 1:2 mode than 1:3
				// for higher node utilization for unconsolidated Swarmx(won't hurt for consolidated ones)
				// and merge the last unconnected one
				ports, newBrs = buildBrLayer(lower, fanOut, rightBound, rightBound+2, &startSetId, &crossGroupTraffic)
				totalPorts += ports
				upper = append(upper, newBrs...)
				brs = append(brs, newBrs...)
				rightBound += 2
			}
		}
		// merge the rest directly to upper for processing at next loop
		upper = append(upper, lower[rightBound:]...)
		lower = upper
		upper = []*BrInternalNode{}
	}
	return brs[len(brs)-1].Layer, totalPorts, crossGroupTraffic, brs
}

// build one layer of BR
func buildBrLayer(lower []*BrInternalNode, fanOut, left, right int, setId, crossGroupTraffic *int) (int, []*BrInternalNode) {
	var res []*BrInternalNode
	var br *BrInternalNode
	var egress *BrInternalEgress
	var ports int
	for i := left; i < right && i < len(lower); i++ {
		if i%fanOut == 0 {
			br = &BrInternalNode{
				SetId:    *setId,
				Egresses: []*BrInternalEgress{},
				IsBr:     true,
			}
			*setId++
			ports++
			res = append(res, br)
		}
		if br == nil {
			continue
		}
		if !lower[i].IsBr {
			egress = &BrInternalEgress{
				SystemName: lower[i].SystemName,
				Stamp:      lower[i].Stamp,
			}
		} else {
			egress = &BrInternalEgress{
				BrSetId: lower[i].SetId,
				IsBr:    true,
				Stamp:   lower[i].Stamp,
			}
		}
		// actual layer depends on downstream nodes
		if lower[i].Layer >= br.Layer {
			br.Layer = lower[i].Layer + 1
		}
		ports++
		br.Egresses = append(br.Egresses, egress)
	}
	for _, no := range res {
		var maxG string
		groupCount := map[string]int{}
		for _, eg := range no.Egresses {
			groupCount[eg.Stamp]++
			// note: in case of equal, this can be optimized to check overall group count
			if groupCount[eg.Stamp] > groupCount[maxG] {
				maxG = eg.Stamp
			}
		}
		no.Stamp = maxG
		*crossGroupTraffic += len(no.Egresses) - groupCount[maxG]
	}
	return ports, res
}

// average br port BW based on cluster redundancy, in unit of 100gbps per set
func AverageBrPortBwPerSet(sysCount int) int {
	bw := GetRequiredBrPortBw(sysCount)
	return int((float64(bw) + float64(sysCount)*wsapisv1.RedundantBrPortBwPerSystem) / float64(CSVersionSpec.NumPorts))
}

// required br port BW based on cluster redundancy, in unit of 100gbps
func GetRequiredBrPortBw(sysCount int) int {
	fakeSys := map[string][]string{"": make([]string, sysCount)}
	_, bw, _, _ := BuildBrGroupTrees(fakeSys, make([]string, sysCount), false, wsapisv1.BrTreePolicyPort)
	return bw * CSVersionSpec.NumPorts
}

// get next group combo from current combo
// e.g. for 9 sys with 3 groups, [7 1 1] [6 2 1] [5 3 1] [4 4 1] [5 2 2] [4 3 2] [3 3 3]
// current param is the current combo, e.g. [7 1 1] and next will be [6 2 1]
// limit param is to simulate cases the group count has max limit
// e.g. put limit of [5 3 3] and pick 9 systems, then invalid combo like [7 1 1] [6 2 1] will be skipped
func GetNextGroupCombo(current []int, limit []int) ([]int, bool) {
	next := make([]int, len(current))
	copy(next, current)

	increment := func(id int) bool {
		val := next[id] + 1
		for i := id; i > 0; i-- {
			diff := next[i] - val
			next[i] = val
			next[0] += diff
		}
		return next[0] >= next[1]
	}

	done := true
	for i := 1; i < len(next); i++ {
		if next[i] < next[i-1] && increment(i) {
			done = false
			break
		}
	}

	if !done {
		for i, v := range limit {
			if v < next[i] {
				return next, false
			}
		}
	} else {
		return nil, false
	}
	return next, true
}

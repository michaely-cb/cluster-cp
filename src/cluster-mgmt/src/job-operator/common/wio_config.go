package common

import (
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	"cerebras.com/job-operator/common/resource"
)

type WioTarget struct {
	// domains and AllSystemIndices are derived from cluster details directly
	Domains          []int
	AllSystemIndices []int

	// used for optimizing QoS
	// goal is to minimize the networking cost function going from
	// an activation pod to a predefined cs port over a set of systems
	StampIdx             int
	InStampSystemIndices []int
	TargetPorts          []int

	// unit of IO usage the pod would incur on the system
	WioBandwidth int
}

// GetV2WioPlacementQuality logs the v2 activation placement, including node-nic and sys-port scheduled
// for each activation pod. This is useful to measure the quality of the scheduler algorithm.
// First return value is number of one-hop traffic, second return value is number of multi-hop traffic.
func GetV2WioPlacementQuality(clusterCfg *ClusterConfig, lock *rlv1.ResourceLock) (int, int) {
	numOneHopTraffic := 0
	numMultiHopTraffic := 0

	var systemNames []string
	for _, rg := range lock.Status.ResourceGrants {
		if rg.Name == rlv1.SystemsRequestName {
			for _, res := range rg.Resources {
				systemNames = append(systemNames, res.Name)
			}
		}
	}

	for _, gg := range lock.Status.GroupResourceGrants {
		if !(strings.HasPrefix(gg.Name, rlv1.ActivationRequestName) || strings.HasPrefix(gg.Name, rlv1.KVStorageServerRequestName)) {
			continue
		}
		for _, rg := range gg.ResourceGrants {
			var nicName string
			for k := range rg.Resources[0].Subresources {
				if strings.HasPrefix(k, resource.NodeNicSubresource) {
					nicName = strings.TrimPrefix(k, resource.NodeNicSubresource+"/")
					break
				}
			}

			summary := ReplicaNetworkTopology{
				NumOneHopTraffic:   0,
				NumMultiHopTraffic: 0,
				JobName:            lock.Name,
				JobNamespace:       lock.Namespace,
				ReplicaType:        gg.Name,
				NodeName:           rg.Resources[0].Name,
				NicNames:           nicName,
				PreferredPorts:     rg.Annotations[rlv1.PreferredCsPortsAnnotKey],
				TargetPorts:        rg.Annotations[rlv1.TargetCsPortAnnotKey],
				TargetSystems:      rg.Annotations[rlv1.SystemIdAnnotKey],
				ReplicaIdx:         rg.Name,
			}

			summary.EvaluateNetworkTopologyQuality(clusterCfg, systemNames, false)
			numOneHopTraffic += summary.NumOneHopTraffic
			numMultiHopTraffic += summary.NumMultiHopTraffic
		}
	}
	logrus.Infof("Lock: %s, number of one-hop traffic: %d, number of multi-hop traffic: %d",
		lock.NamespacedName(), numOneHopTraffic, numMultiHopTraffic)
	return numOneHopTraffic, numMultiHopTraffic
}

// GenerateSystemInterleavingOrder creates a pod visiting order that alternates between
// different system groups to ensure fair connectivity distribution across systems.
//
// This function groups pods by their system assignments (AllSystemIndices) and then
// visits pods in round-robin fashion across these system groups. This prevents
// consecutive pods from being processed on the same system, improving load balancing.
//
// Example 1 - Simple case:
//
//	Input: 6 pods with system assignments:
//	  Pod 0: [0]    Pod 1: [0]    Pod 2: [1]
//	  Pod 3: [1]    Pod 4: [2]    Pod 5: [2]
//
//	System groups:
//	  "0": [0, 1]   "1": [2, 3]   "2": [4, 5]
//
//	Output: [0, 2, 4, 1, 3, 5]
//	(Round-robin: sys0→sys1→sys2→sys0→sys1→sys2)
//
// Example 2 - Complex case with multi-system pods:
//
//	Input: 8 pods with system assignments:
//	  Pod 0: [0,1]  Pod 1: [0,1]  Pod 2: [2]    Pod 3: [2]
//	  Pod 4: [3]    Pod 5: [3]    Pod 6: [0,1]  Pod 7: [2]
//
//	System groups:
//	  "0,1": [0, 1, 6]   "2": [2, 3, 7]   "3": [4, 5]
//
//	Output: [0, 2, 4, 1, 3, 5, 6, 7]
//	(Round-robin across the 3 unique system groups)
//
// Example 3 - Uneven distribution:
//
//	Input: 7 pods where system "0" has 4 pods, system "1" has 3 pods:
//	  System "0": [0, 1, 2, 3]   System "1": [4, 5, 6]
//
//	Output: [0, 4, 1, 5, 2, 6, 3]
//	(System "0" gets an extra pod at the end since it has more pods)
func GenerateSystemInterleavingOrder(wioTargets []WioTarget) []int {
	if len(wioTargets) == 0 {
		return []int{}
	}

	systemGroupToReplicaMapping := make(map[string][]int)
	var systemGroups []string
	maxPodsInAnyGroup := 0

	for podIdx, wioTarget := range wioTargets {
		systemStrs := make([]string, len(wioTarget.AllSystemIndices))
		for i, sysIdx := range wioTarget.AllSystemIndices {
			systemStrs[i] = strconv.Itoa(sysIdx)
		}
		systemGroupStr := strings.Join(systemStrs, ",")

		if _, exists := systemGroupToReplicaMapping[systemGroupStr]; !exists {
			systemGroups = append(systemGroups, systemGroupStr)
		}

		systemGroupToReplicaMapping[systemGroupStr] = append(systemGroupToReplicaMapping[systemGroupStr], podIdx)
		if curGroupLen := len(systemGroupToReplicaMapping[systemGroupStr]); curGroupLen > maxPodsInAnyGroup {
			maxPodsInAnyGroup = curGroupLen
		}
	}

	// No need to interleave if there's only 1 system group
	if len(systemGroups) == 1 {
		return systemGroupToReplicaMapping[systemGroups[0]]
	}

	var interleavingOrder []int

	for round := 0; round < maxPodsInAnyGroup; round++ {
		for _, systemGroupStr := range systemGroups {
			podIdxs := systemGroupToReplicaMapping[systemGroupStr]
			if round < len(podIdxs) {
				interleavingOrder = append(interleavingOrder, podIdxs[round])
			}
		}
	}

	logrus.Debugf("Generated interleaving order for %d pods across %d system groups: %v",
		len(wioTargets), len(systemGroups), interleavingOrder)
	return interleavingOrder
}

package lock

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource_config"
)

type nvmfGrant struct {
	deviceResName     string
	devicePath        string
	addressRange      resource.Range
	subresourceRanges resource.SubresourceRanges
}

func (r *Reconciler) scheduleForSystemAffinity(c *resource.Collection, l *lock, result *grantResult) {
	// do an ax node size check first
	axStampAgnosticFilter := resource.NewFilter(rlv1.NodeResourceType, common.RoleActivation.AsMatcherProperty(),
		nil).WithLockPropFilter(l.Obj)

	ixStampAgnosticFilter := resource.NewFilter(rlv1.NodeResourceType, common.RoleInferenceDriver.AsMatcherProperty(),
		nil).WithLockPropFilter(l.Obj)

	inferenceJobSharingEnabled := l.isInferenceJobSharingEnabled()
	// "allAxNodes" is used to find the next set of free nodes incrementally.
	allAxNodes := c.Filter(axStampAgnosticFilter).Filter(
		common.Ternary(!inferenceJobSharingEnabled,
			resource.NewFreeNodesFilter(),
			resource.NewJobAntiAffinityFilter(wsapisv1.InferenceJobSharingAnnotKey, wsapisv1.ConstTrue))).Copy()

	if !inferenceJobSharingEnabled && wsapisv1.UseAxScheduling && allAxNodes.Size() < l.Obj.GetAxNodesRequested() {
		// get shortage with ceiling operation
		result.axGrantShortage = (l.Obj.GetAxNodesRequested() - allAxNodes.Size() +
			wsapisv1.AxCountPerSystem - 1) / wsapisv1.AxCountPerSystem
		err := fmt.Errorf(fmt.Sprintf(
			"lock: %s, requested %d AX node(s) but only %d available across stamps: %v, filter: %v",
			l.Obj.NamespacedName(), l.Obj.GetAxNodesRequested(), allAxNodes.Size(),
			allAxNodes.ListResNames(), axStampAgnosticFilter))
		if len(l.ActTargets) > 0 {
			result.actGroupErrors = append(result.actGroupErrors, err)
		}
		if len(l.KvssTargets) > 0 {
			result.kvssGroupErrors = append(result.kvssGroupErrors, err)
		}
		result.chfGroupErrors = append(result.chfGroupErrors, err)
		logrus.Error(err)
	}

	// "allIxNodes" is used to find the next set of free nodes incrementally.
	allIxNodes := c.Filter(ixStampAgnosticFilter).Copy()
	if resource_config.MaxSwDriverPodsPerIxNode == 1 {
		allIxNodes = allIxNodes.GetFreeNodes()
	}

	// if we need any SW‑driver pods but there are no IX nodes at all, fail immediately
	if len(l.SWDriverTargets) > 0 && wsapisv1.UseIxScheduling && allIxNodes.Size() <= 0 {
		err := fmt.Errorf(
			"lock %s: requested IX nodes but none are available",
			l.Obj.NamespacedName())
		result.swDriverGroupErrors = append(result.swDriverGroupErrors, err)
		logrus.Error(err)
	}
	// skip scheduling if AX node shortage or system schedule not successful
	if !result.ok() {
		return
	}

	// Systems in system grants could appear where resources are not grouped by stamps
	// Preserving the order of system grants and partition systemNames into stamps is important
	// systemsOrderedInStamps is a 2D slice where each inner slice contains the systems for a stamp, ordered by stamp index.
	// Example: [][]string len: 2, cap: 2, [["system-2"],["system-0","system-1"]]
	var systemsOrderedInStamps [][]string

	// stampIdxMap:
	// Maps each stamp ID to its index in the ordered stamp list (systemsOrderedInStamps).
	// stampIdxMap["2"] = 0, means stampIdx "2" maps to the 0th slice in systemsOrderedInStamps
	// Example: {
	//   "0": 1,
	//   "1": 0,
	// }
	stampIdxMap := map[string]int{}

	// axNodeFiltersInStamps:
	// A slice of filters, one per stamp, to select AX (activation role) nodes within that stamp.
	// Each filter includes the stamp and role constraints, and is scoped to the current lock.
	var axNodeFiltersInStamps []resource.Filter

	// ixNodeFiltersInStamps:
	// Similar to axNodeFiltersInStamps, but for IX (inference-driver role) nodes.
	// One filter per stamp, constrained by role and stamp, and tied to the lock context.
	var ixNodeFiltersInStamps []resource.Filter

	// perStampSystems:
	// Maps each stamp ID to the list of systems within that stamp.
	// Example: {
	//   "1": ["system-2"],
	//   "0": ["system-0", "system-1"],
	// }
	perStampSystems := map[string][]string{}

	// l.orderedSystems:
	// A flat list of all systems in the lock, ordered by preference or requirement.
	// Example: []string len: 3, cap: 3, ["system-2","system-0","system-1"]
	l.OrderedSystems = make([]string, len(result.resourceGrants[rlv1.SystemsRequestName].ResourceIds))

	// This loop flattens the system grants into a list of systems ordered by stamp index.
	for i, rid := range result.resourceGrants[rlv1.SystemsRequestName].ResourceIds {
		sys, _ := c.Get(rid)
		systemName := sys.Name()
		l.OrderedSystems[i] = systemName
		g := sys.GetStampIndex()
		// compatible for v1 or single stamp case
		perStampSystems[g] = append(perStampSystems[g], systemName)
		var stampIdx string
		// If we had detected cross-stamp switches or no stamp, we will ignore picking stamp-aware AX nodes later on
		if common.StandardV2StampsDetected() && g != "" {
			stampIdx = g
		}
		if _, ok := stampIdxMap[stampIdx]; !ok {
			stampIdxMap[stampIdx] = len(stampIdxMap)
			systemsOrderedInStamps = append(systemsOrderedInStamps, []string{})

			if wsapisv1.UseAxScheduling {
				nodeProperties := common.RoleActivation.AsMatcherProperty()
				if common.StandardV2StampsDetected() {
					nodeProperties[resource.StampPropertyKey] = []string{stampIdx}
				}
				axNodeFiltersInStamps = append(axNodeFiltersInStamps,
					resource.NewFilter(rlv1.NodeResourceType, nodeProperties, nil).
						WithLockPropFilter(l.Obj))
			}

			if wsapisv1.UseIxScheduling {
				nodeProperties := common.RoleInferenceDriver.AsMatcherProperty()
				if common.StandardV2StampsDetected() {
					nodeProperties[resource.StampPropertyKey] = []string{stampIdx}
				}
				ixNodeFiltersInStamps = append(ixNodeFiltersInStamps,
					resource.NewFilter(rlv1.NodeResourceType, nodeProperties, nil).
						WithLockPropFilter(l.Obj))

			}
		}
		systemsOrderedInStamps[stampIdxMap[stampIdx]] = append(systemsOrderedInStamps[stampIdxMap[stampIdx]], systemName)
	}

	// schedule AX
	// We need to ensure the lock is a weight-streaming execute job with a non-empty wio allocation.
	// Simplifying the check to only check for wio allocation here.
	if (len(l.ActTargets) > 0 || len(l.KvssTargets) > 0) && wsapisv1.UseAxScheduling {
		if inferenceJobSharingEnabled {
			csxNeeded := len(l.OrderedSystems)
			csxNum := c.Filter(resource.NewSystemFilter().WithLockPropFilter(l.Obj)).Size()
			axTotalMem := allAxNodes.GetTotalAxMemory()
			aggrMemoryUpperboundOnFairShare := axTotalMem * csxNeeded / csxNum
			l.AggrAxMemLimit = aggrMemoryUpperboundOnFairShare
			logrus.Infof("kvss request requires %v systems out of %v total systems in the session", csxNeeded, csxNum)
			logrus.Infof("[%v] on AX nodes: totalMem=%v before allocation", l.Name, axTotalMem)
			logrus.Infof("[%v] aggrMemoryUpperbound=%v", l.Name, aggrMemoryUpperboundOnFairShare)
		}

		for groupId, groupReq := range l.GroupRequests {
			if groupId == rlv1.ChiefRequestName {
				l.ChfGroupRequests = hydrateChfRequests(systemsOrderedInStamps, groupReq)
			} else if groupId == rlv1.ActivationRequestName {
				// For inference job we have considerGlobalPortAllocation set to true.
				// In inference, ACT:System is 1:1 and ACT other than ACT-0 does not have a requirement on port affinity.
				// Hence we are relaxing the ACT-System logical mapping to simplify the port round-robin-ing effectiveness.
				// Cross-job ACT-0 port round-robin-ing is still implicitly guaranteed with this relaxation.
				//
				// For training job considerGlobalPortAllocation is set to false.
				// This is because ACT:System is n:1, and in MoE kn:k. There are enough ACTs per system so the port round-robin-ing
				// is already effective per-system, so we do not need special handling to consider global port allocation.
				r.buildPodToCsPortMapping(c, axNodeFiltersInStamps, systemsOrderedInStamps, l.ActTargets,
					l.LinearMemoryConfigs[wsapisv1.WSReplicaTypeActivation], inferenceJobSharingEnabled, l.isInferenceJobMode())
				l.ActGroupRequests = hydrateWioRequests(systemsOrderedInStamps, l.ActTargets, groupReq, rlv1.ActivationRequestName,
					common.GetLinearMemoryConfig(l.LinearMemoryConfigs, commonv1.ReplicaType(rlv1.ActivationRequestName)))
				// Hack for swdriver mutual exclusion during job sharing
				// TODO: remove once SwDriver is scheduled on IX/SX
				if l.isSwDriverMutualExclusionEnabled() {
					for i, gr := range l.ActGroupRequests {
						if _, exits := gr.Requests["0"]; exits {
							l.ActGroupRequests[i].Requests["0"].AddSubresource(resource.Act0SubResource, 1)
							break
						}
					}
				}
			} else if groupId == rlv1.WgtCmdGroupPrefix {
				l.WgtCmdGroupRequests = []resource.GroupRequest{{
					Id:       fmt.Sprintf("%s-group", rlv1.WgtCmdGroupPrefix),
					Count:    1,
					Requests: common.CopyMap(groupReq.Requests),
					KeySort:  resource.V2WgtCmdKeySort,
				}}
			}
		}

		if (len(l.KvssTargets) > 0) && wsapisv1.UseAxScheduling {
			for groupId, groupReq := range l.GroupRequests {
				if groupId == rlv1.KVStorageServerRequestName {
					r.buildPodToCsPortMapping(c, axNodeFiltersInStamps, systemsOrderedInStamps, l.KvssTargets,
						l.LinearMemoryConfigs[wsapisv1.WSReplicaTypeKVStorageServer], inferenceJobSharingEnabled, false)
					l.KvssGroupRequests = hydrateWioRequests(systemsOrderedInStamps, l.KvssTargets, groupReq, rlv1.KVStorageServerRequestName,
						common.GetLinearMemoryConfig(l.LinearMemoryConfigs, commonv1.ReplicaType(rlv1.KVStorageServerRequestName)))
				}
			}
		}

		axNodeFiltersInStamps = append(axNodeFiltersInStamps, axStampAgnosticFilter)
		r.scheduleOnAx(allAxNodes, l, systemsOrderedInStamps, axNodeFiltersInStamps, result)
	}

	// schedule software driver
	if len(l.SWDriverTargets) > 0 && wsapisv1.UseIxScheduling {
		for groupId, groupReq := range l.GroupRequests {
			if groupId == rlv1.SWDriverRequestName {
				swDriverJobSharing := false
				if resource_config.MaxSwDriverPodsPerIxNode > 1 {
					swDriverJobSharing = true
				}
				r.buildPodToCsPortMapping(c, ixNodeFiltersInStamps, systemsOrderedInStamps, l.SWDriverTargets,
					l.LinearMemoryConfigs[wsapisv1.WSReplicaTypeSWDriver], swDriverJobSharing, false)
				// Note: Currently only one SWDriver pod per job is expected, and LMR is not typically set for SWDriver
				// but we pass it for future compatibility if LMR support is added for other task types
				l.SWDriverGroupRequests = hydrateWioRequests(systemsOrderedInStamps, l.SWDriverTargets, groupReq, rlv1.SWDriverRequestName,
					common.GetLinearMemoryConfig(l.LinearMemoryConfigs, commonv1.ReplicaType(rlv1.SWDriverRequestName)))
			}
		}

		ixNodeFiltersInStamps = append(ixNodeFiltersInStamps, ixStampAgnosticFilter)
		// schedule software driver on IX nodes
		r.scheduleOnIx(allIxNodes, l, ixNodeFiltersInStamps, l.SWDriverTargets, result)
	}

	// schedule BR
	if l.ResourceRequests[rlv1.SystemsRequestName].Count > 1 && !l.SkipBR() {
		r.scheduleBRs(c, l, perStampSystems, result)
	}

}

func (r *Reconciler) annotatePreferredCsPorts(gg []resource.GroupGrant) {
	// add preferred cs port annotations
	for i := range gg {
		for idx, g := range gg[i].Grants {
			for nodeResName, subResources := range g.SubresourceOverride {
				res, ok := r.GetCollection().Get(nodeResName)
				if !ok {
					// this should not happen
					continue
				}
				for subResName := range subResources {
					if !strings.HasPrefix(subResName, resource.NodeNicSubresource) {
						continue
					}
					preferredCsPorts := map[string]bool{}
					for k, v := range res.GetProps() {
						if !strings.HasPrefix(k, resource.PortAffinityPropKeyPrefix) {
							continue
						}
						if v == subResName || v == "" {
							tokens := strings.Split(k, "-")
							preferredCsPorts[tokens[len(tokens)-1]] = true
						}
					}
					// Add annotations for the cs port in affinity
					gg[i].Grants[idx].Annotations[rlv1.PreferredCsPortsAnnotKey] =
						strings.Join(common.SortedKeys(preferredCsPorts), ",")
				}
			}
		}
	}
}

// todo: pick a few system combos for comparison
func (r *Reconciler) pickSystems(resourceIds []string, total int) [][]string {
	if total == 1 {
		return [][]string{{resourceIds[0]}}
	}
	var systemCombos [][]string
	// build group => systems index
	groupResMap := map[string][]string{}
	for _, rid := range resourceIds {
		sys, _ := r.resources.Get(rid)
		groupResMap[sys.GetStampIndex()] = append(groupResMap[sys.GetStampIndex()], rid)
	}
	groups := common.SortedKeysByValSize(groupResMap, false)
	hasCombo := false
	var groupCombo []int
	var groupLimit []int
	var groupSum int
	for _, group := range groups {
		groupLimit = append(groupLimit, len(groupResMap[group]))
		if len(groupResMap[group]) > total-groupSum {
			groupCombo = append(groupCombo, total-groupSum)
			hasCombo = true
			break
		} else {
			groupCombo = append(groupCombo, len(groupResMap[group]))
			groupSum += len(groupResMap[group])
		}
	}

	getSystemComboByGroup := func(group []int) []string {
		var names []string
		for i, count := range group {
			names = append(names, groupResMap[groups[i]][:count]...)
		}
		return names
	}
	systemCombos = append(systemCombos, getSystemComboByGroup(groupCombo))
	if hasCombo {
		next, ok := common.GetNextGroupCombo(groupCombo, groupLimit)
		if ok {
			systemCombos = append(systemCombos, getSystemComboByGroup(next))
		}
	}
	return systemCombos
}

func (r *Reconciler) scheduleBRs(c *resource.Collection, l *lock, groupSystems map[string][]string, result *grantResult) {
	filter := resource.NewFilter(rlv1.NodeResourceType,
		common.RoleBroadcastReduce.AsMatcherProperty(),
		nil).WithLockPropSkipSession(l.Obj)
	brNodes := c.Filter(filter)
	grantErr := map[string]error{}
	groupGrant := map[string]resource.GroupGrant{}
	groupReq := map[string]resource.GroupRequest{}
	logicalTree := map[string]common.BrLogicalTree{}
	layerCount, portCount, crossTrafficCount := map[string]int{}, map[string]int{}, map[string]int{}

	// default with hybrid
	policies := []string{wsapisv1.BrTreePolicyPort, wsapisv1.BrTreePolicySpine}
	if l.getSpecifiedBrTreePolicy() != "" {
		policies = []string{l.getSpecifiedBrTreePolicy()}
	} else if wsapisv1.BrTreePolicy != "" {
		policies = []string{wsapisv1.BrTreePolicy}
	} else if len(groupSystems) == 1 {
		policies = []string{wsapisv1.BrTreePolicyPort}
	}
	for _, policy := range policies {
		layer, ports, crossTraffic, tree, reqs, err := r.BuildBrRequest(groupSystems, l, policy)
		if err != nil {
			logrus.Warn(err) // err = set of granted systemNames do not have healthy ports that are inter-compatible
			grantErr[policy] = fmt.Errorf("unable to schedule systems due to %s", err.Error())
		} else {
			logicalTree[policy] = tree
			layerCount[policy] = layer
			portCount[policy] = ports
			crossTrafficCount[policy] = crossTraffic
			logrus.Infof("job %s constructed br logic tree with policy %s, layer %d, ports %d, crossTraffic %d",
				l.Obj.NamespacedName(), policy, layer, ports, crossTraffic)
			for _, req := range reqs {
				var e error
				groupReq[policy] = req
				// Use group scheduling because FindUngroupedScheduling disallow same node to be scheduled multiple times.
				groupGrant[policy], e = brNodes.FindGroupScheduling(req)
				if e != nil {
					msg := fmt.Sprintf("unable to construct %d BR tree with policy %s for %d system request %s: %v, "+
						"logical tree: %+v, available BR node count: %d, sample: %+v", len(req.Requests), policy,
						l.ResourceRequests[rlv1.SystemsRequestName].Count, l.Obj.NamespacedName(), e, tree, brNodes.Size(), brNodes)
					logrus.Warnf(msg) // err = could not grant - possibly could pass up info about which cs-port was lacking
					// simplified error to user side
					grantErr[policy] = fmt.Errorf("unable to construct %d BR tree "+
						"with policy %s on %d available SwarmX nodes with props(%v)",
						len(req.Requests), policy, brNodes.Size(), resource.PrettyProps(filter.Properties()))
				} else {
					grantErr[policy] = nil
					break
				}
			}
		}
	}

	pickedPolicy := policies[0]
	if len(policies) > 1 {
		avg := common.AverageBrPortBwPerSet(l.ResourceRequests[rlv1.SystemsRequestName].Count)
		if grantErr[wsapisv1.BrTreePolicySpine] == nil && grantErr[wsapisv1.BrTreePolicyPort] == nil {
			// prefer spine policy if meets port utilization requirement
			// note: this can be optimized to compare with cross spine traffic with actual allocation
			if portCount[wsapisv1.BrTreePolicySpine] <= avg {
				pickedPolicy = wsapisv1.BrTreePolicySpine
			} else {
				pickedPolicy = wsapisv1.BrTreePolicyPort
			}
		} else {
			pickedPolicy = common.Ternary(grantErr[wsapisv1.BrTreePolicySpine] == nil,
				wsapisv1.BrTreePolicySpine, wsapisv1.BrTreePolicyPort)
		}
		logrus.Infof("%s policy picked for job %s with port count: %v, average count: %d, other policy error: %s",
			pickedPolicy, l.Obj.NamespacedName(), portCount, avg, grantErr)
	}
	l.BrGroupRequest = groupReq[pickedPolicy]
	l.BrLogicalTree = logicalTree[pickedPolicy]
	l.BrLogicalTreePolicy = pickedPolicy
	result.brGroupGrant = groupGrant[pickedPolicy]
	result.brGroupError = grantErr[pickedPolicy]
}

// Only apply ACT-0 affinity logic for inference jobs, first stamp, and if IX scheduling is NOT enabled
func ACT0AffinityNeeded(l *lock, i int) bool {
	return l.Obj.IsInference() && len(l.ActGroupRequests) > 0 && i == 0 && !wsapisv1.UseIxScheduling
}

// Overall AX scheduling flow
//
// Problem: Given a set of granted systems and the number of activation pods which need talking to a particular domain
// on a system, we need to schedule pods to nodes to meet the following criteria:
// 1. CS port load within every CS domain on every granted system must be balanced.
// 2. The number of pods on the every scheduled AX node must be balanced.
// 3. NIC usage on every scheduled AX node must be balanced.
//
// Objective:
// Lower the network connectivity costs as much as possible.
//
// Main algorithm (includes but not limited to this function):
//  1. During request hydration time, buildPodToCsPortMapping assigns CS ports which the activation pods will talk to.
//  2. If we only need a subset of available nodes for a job, GetNodesWithMostAffinityCoverage finds a set of nodes with
//     optimal affinity spread or by respecting a specific affinity preference (see inference).
//  3. Performing pod scheduling and nic scheduling on the selected nodes.
//
// Note that all steps described in the algorithm are stamp-aware.
// https://cerebras.atlassian.net/wiki/spaces/runtime/pages/3105128493/Activation+scheduling
func (r *Reconciler) scheduleOnAx(allAxNodes *resource.Collection, l *lock, systemsOrderedInStamps [][]string,
	axNodeFiltersInStamps []resource.Filter, result *grantResult) {
	// "allSelectedAxNodes" grows incrementally as we grant resources stamp-by-stamp.
	// If we need to schedule for weights and command, we will use this collection to grant the group request.
	allSelectedAxNodes := &resource.Collection{}
	inferenceJobSharingEnabled := l.isInferenceJobSharingEnabled()
	systemsOrderedInStamps = r.useCrossStampAxIfNecessary(allAxNodes, l, systemsOrderedInStamps, axNodeFiltersInStamps)

	for i, ss := range systemsOrderedInStamps {
		numAxNodesRequested := wsapisv1.AxCountPerSystem * len(ss)
		axNodes := allAxNodes.Filter(axNodeFiltersInStamps[i]).Filter(
			common.Ternary(!inferenceJobSharingEnabled,
				resource.NewFreeNodesFilter(),
				resource.NewJobAntiAffinityFilter(wsapisv1.InferenceJobSharingAnnotKey, wsapisv1.ConstTrue))).Copy()

		// When job sharing is enabled, we evaluate all AX nodes in the stamp,
		// there's no need to pick the preferred AX nodes up front.
		if axNodes.Size() > numAxNodesRequested && !inferenceJobSharingEnabled {
			logrus.Infof("lock: %s, need to elect %d AX nodes from %v", l.Obj.NamespacedName(),
				numAxNodesRequested, common.Sorted(axNodes.ListResNames()))
			// In inference, it is important for ACT-0 to have locality to the targeted WSE domain.
			// The target CS port ACT-0 needs to talk to had already been determined during request hydration time.

			// If ACT-0 affinity is needed, we will select a node which has affinity to the targeted CS port as best effort.
			// If SWD is enable, we do not need to consider sepcial ACT-0 affinity logic, as SWD will be scheduled on IX nodes.
			if ACT0AffinityNeeded(l, i) {
				if req, ok := l.ActGroupRequests[i].Requests["0"]; ok {
					portAffinities := map[int]bool{}
					antiPortAffinities := map[int]bool{}
					targetedCsPortStr := req.Annotations[rlv1.TargetCsPortAnnotKey]
					if targetedCsPortInt, err := strconv.Atoi(targetedCsPortStr); err == nil {
						portAffinities[targetedCsPortInt] = true

						// if port 10 is preferred, we will select a node which has affinity to port 10 as best effort
						// for the remaining nodes, we will avoid choosing a node which has affinity to port 9, 10 or 11
						domain := common.CSVersionSpec.Domain(targetedCsPortInt)
						for _, port := range common.CSVersionSpec.PortsFromDomain(domain) {
							antiPortAffinities[port] = true
						}
					}
					preferredNode, _ := axNodes.GetNodesWithMostAffinityCoverage(l.Obj.Namespace, l.Obj.Name,
						ss, nil, 1, portAffinities, nil, common.CSVersionSpec.NumPorts)
					axNodes, _ = axNodes.GetNodesWithMostAffinityCoverage(l.Obj.Namespace, l.Obj.Name, ss, preferredNode,
						numAxNodesRequested, nil, antiPortAffinities, common.CSVersionSpec.NumPorts)
				}
			} else {
				axNodes, _ = axNodes.GetNodesWithMostAffinityCoverage(l.Obj.Namespace, l.Obj.Name, ss, nil,
					numAxNodesRequested, nil, nil, common.CSVersionSpec.NumPorts)
			}
		}

		sortedAxNodeNames := common.Sorted(axNodes.ListResNames())
		logrus.Infof("lock: %s, elected %d AX nodes: %v", l.Obj.NamespacedName(),
			len(sortedAxNodeNames), sortedAxNodeNames)

		for _, res := range axNodes.List() {
			if _, exist := allSelectedAxNodes.Get(res.Id()); exist && inferenceJobSharingEnabled {
				// When job sharing is enabled, we always consider all the axNodes, not just the ones that are free.
				// During cross stamp redistribution, we evaluate the nodes and add them to allSelectedAxNodes,
				// and there's a possibility that we add an existing node to it if job sharing is enabled.
				// We would like to skip the Add call as adding an existing element causes a panic.
				continue
			}
			allSelectedAxNodes.Add(res)
		}

		if !inferenceJobSharingEnabled {
			// In AX scheduling, besides the existing subresources, we also define the maximum number of pods a node could have ("AuxiliaryPodUsageLimit"),
			// as well as the maximum number of pods a node-nic could have ("AuxiliaryNicUsageLimit") to ensure both node-level and nic-level LB while
			// prioritizing port affinity during scheduling. These auxiliary limits are job-specific and set dynamically for each job.
			// Todo: further optimize to avoid invasive update on resource
			// one option is to refactor the wall idea to be non-invasive
			// or ideally, similar as k8s scheduling phase of filtering/scoring/...,
			// the scoring should be generic to allow both LB/affinity and be flexible to support node sharing across jobs
			// e.g. remainResScore(range 10-100) + affinityScore (range 0-9)
			// e.g. for node0(nic0-port0,nic1-port1), node1(nic0-port2,nic1-port3) if request to schedule 4 pods per system
			// the scheduling loop can be ordered by round robin system/ports:
			// sys0-port0, sys1-port2, sys0-port1, sys1-port3, sys0-port2, sys1-port0, sys0-port3, sys1-port1
			// this way the scheduling order will prefer LB and simplify scoring to priority on remaining score
			// more thoughts needed for degraded node that LB be at node level instead of NIC level but idea is similar
			// https://github.com/Cerebras/monolith/pull/100216#discussion_r1781763273
			totalNumActPods := len(l.ActTargets) * len(ss) / result.systemRequestCount()
			numActPodsPerNode := int(math.Ceil(float64(totalNumActPods) / float64(len(sortedAxNodeNames))))
			for _, n := range axNodes.List() {
				nics := n.GetNicRawNamesSorted()
				axNodes.UpdateSubresource(n.Id(), resource.AuxiliaryNicUsageLimit,
					int(math.Ceil(float64(numActPodsPerNode)/float64(len(nics)))))
				axNodes.UpdateSubresource(n.Id(), resource.AuxiliaryPodUsageLimit, numActPodsPerNode)
				logrus.Infof("lock: %s, updated node %s with pod usage limit and nic usage limit %v for activation scheduling",
					l.Obj.NamespacedName(), n.Name(), n.Subresources())
			}
		}

		// Grant chiefs in a stamp
		chfGroupGrant, e := axNodes.FindGroupScheduling(l.ChfGroupRequests[i])
		if e != nil {
			msg := fmt.Sprintf("lock: %s, error: %v, req: %+v, available AX nodes: %v",
				l.Obj.NamespacedName(), e, l.ChfGroupRequests[i], sortedAxNodeNames)
			logrus.Warnf(msg)
			result.chfGroupErrors = append(result.chfGroupErrors, fmt.Errorf("%v", e))
		} else {
			result.chfGroupGrants = append(result.chfGroupGrants, chfGroupGrant)
			srallocs := resource.GatherAllocations(
				map[string]resource.Grant{}, // empty resource grant
				map[string][]resource.GroupGrant{
					l.ChfGroupRequests[i].Id: {chfGroupGrant},
				})
			allSelectedAxNodes.Allocate(l.Obj, srallocs)
			allAxNodes.Allocate(l.Obj, srallocs)
		}

		if i < len(l.ActGroupRequests) {
			// Grant activations in a stamp
			// Note that we could in addition apply a sort on the requests to scatter requests targeting the same domains.
			// This could further lower the network traffic costs when requests to domains are not skewed.
			actGroupGrant, e := axNodes.FindGroupScheduling(l.ActGroupRequests[i])
			actGroupGrant.Annotations = common.EnsureMap(actGroupGrant.Annotations)

			if e != nil {
				msg := fmt.Sprintf("lock: %s, error: %v, req: %+v, available AX nodes: %v",
					l.Obj.NamespacedName(), e, l.ActGroupRequests[i], sortedAxNodeNames)
				logrus.Warnf(msg)
				result.actGroupErrors = append(result.actGroupErrors, fmt.Errorf("%v", e))
			} else {
				result.actGroupGrants = append(result.actGroupGrants, actGroupGrant)
				srallocs := resource.GatherAllocations(
					map[string]resource.Grant{}, // empty resource grant
					map[string][]resource.GroupGrant{
						l.ActGroupRequests[i].Id: {actGroupGrant},
					})
				allSelectedAxNodes.Allocate(l.Obj, srallocs)
				allAxNodes.Allocate(l.Obj, srallocs)
			}
		}

		if i < len(l.KvssGroupRequests) {
			if !inferenceJobSharingEnabled {
				// Update auxiliary usage limits, adding the new value of kvss pods required
				// Since KVSS can have uneven distribution over CSXs, we are using a rough estimation for totalNumKvssPods
				// in each stamp by taking the ceiling of kvss pods * portion of the systems in current stamp.
				// TODO: we could potentially use a better algorithm, such as greedy for more precise estimation of totalNumKvssPods
				totalNumKvssPods := int(math.Ceil(
					float64(len(l.KvssTargets)*len(ss)) / float64(result.systemRequestCount())))
				numKvssPodsPerNode := int(math.Ceil(float64(totalNumKvssPods) / float64(len(sortedAxNodeNames))))
				for _, n := range axNodes.List() {
					nics := n.GetNicRawNamesSorted()
					axNodes.UpdateSubresource(n.Id(), resource.AuxiliaryNicUsageLimit,
						int(math.Ceil(float64(numKvssPodsPerNode)/float64(len(nics)))))
					axNodes.UpdateSubresource(n.Id(), resource.AuxiliaryPodUsageLimit, numKvssPodsPerNode)
					logrus.Infof("lock: %s, updated node %s with pod usage limit and nic usage limit %v for kvss scheduling",
						l.Obj.NamespacedName(), n.Name(), n.Subresources())
				}
			}

			// Grant key-value cache in a stamp
			kvssGroupGrant, e := axNodes.FindGroupScheduling(l.KvssGroupRequests[i])
			// Make sure we have the map initialized here, as we will work with a slice of GroupGrants later for additional memory granting.
			// In GO, elements in slices are passed by copy, but elements in maps are passed by reference. Initializing the map here ensures
			// that we work with the same map later.
			kvssGroupGrant.Annotations = common.EnsureMap(kvssGroupGrant.Annotations)
			if e != nil {
				msg := fmt.Sprintf("lock: %s, error: %v, req: %+v, available AX nodes: %v",
					l.Obj.NamespacedName(), e, l.KvssGroupRequests[i], sortedAxNodeNames)
				logrus.Warnf(msg)
				result.kvssGroupErrors = append(result.kvssGroupErrors, fmt.Errorf("%v", e))
			} else {
				result.kvssGroupGrants = append(result.kvssGroupGrants, kvssGroupGrant)
				srallocs := resource.GatherAllocations(
					map[string]resource.Grant{}, // empty resource grant
					map[string][]resource.GroupGrant{
						l.KvssGroupRequests[i].Id: {kvssGroupGrant},
					})
				allSelectedAxNodes.Allocate(l.Obj, srallocs)
				allAxNodes.Allocate(l.Obj, srallocs)
			}
		}

		// reset auxiliary usage for sanity only - technically not needed
		for _, n := range axNodes.List() {
			for k := range n.Subresources() {
				if strings.HasPrefix(k, resource.AuxiliarySubresPrefix) {
					axNodes.UpdateSubresource(n.Id(), k, 0)
				}
			}
		}
	}

	// add preferred cs port annotations
	if len(result.actGroupErrors) == 0 {
		r.annotatePreferredCsPorts(result.actGroupGrants)
	}

	if len(result.kvssGroupErrors) == 0 {
		r.annotatePreferredCsPorts(result.kvssGroupGrants)
	}

	// Grant wgt/cmd across stamps
	if len(l.WgtCmdGroupRequests) > 0 {
		// Users could explicitly request additional ax nodes for bespoke workflows.
		// Only weights and command will use those additional ax nodes.
		pickAdditionalAxNodesIfRequired := func() {
			additionalAxNodesRequested := l.Obj.GetAxNodesRequested() - wsapisv1.AxCountPerSystem*l.Obj.GetSystemsRequested()
			remainingAxNodes := allAxNodes.GetFreeNodes()
			resByStamps := map[string][]resource.Resource{}
			for _, res := range remainingAxNodes.List() {
				stampIdx := res.GetProps()[resource.StampPropertyKey]
				resByStamps[stampIdx] = append(resByStamps[stampIdx], res)
				slices.SortFunc(resByStamps[stampIdx], func(a, b resource.Resource) bool {
					return a.Name() < b.Name()
				})
			}
			for i := 0; i < additionalAxNodesRequested; i++ {
				if len(resByStamps) == 0 {
					// this should never happen given we had done a total AX check at the very beginning
					continue
				}

				// We will pick the additional nodes by reducing further stamp fragmentation.
				// For example, assume we have 3 stamps with 1, 2, and 3 free AX nodes respectively.
				// If the request is for 3 AX nodes, we will pick 1 from each stamp instead of picking 3 from the last stamp.
				leastNodesInStamp := 0
				leastNodeStampIdx := ""
				for stampIdx, resources := range resByStamps {
					if leastNodesInStamp == 0 || len(resources) < leastNodesInStamp {
						leastNodesInStamp = len(resources)
						leastNodeStampIdx = stampIdx
					}
				}
				allSelectedAxNodes.Add(resByStamps[leastNodeStampIdx][0])
				resByStamps[leastNodeStampIdx] = resByStamps[leastNodeStampIdx][1:]
				if len(resByStamps[leastNodeStampIdx]) == 0 {
					delete(resByStamps, leastNodeStampIdx)
				}
			}
		}

		pickAdditionalAxNodesIfRequired()
		logrus.Infof("lock: %s, going to schedule weights and command across %d AX node(s): %v",
			l.Obj.NamespacedName(), allSelectedAxNodes.Size(), allSelectedAxNodes.ListResNames())

		for i := range l.WgtCmdGroupRequests {
			wgtCmdGroupGrant, e := allSelectedAxNodes.FindGroupScheduling(l.WgtCmdGroupRequests[i])
			if e != nil {
				msg := fmt.Sprintf("lock: %s, error: %v, req: %+v, available AX nodes: %v",
					l.Obj.NamespacedName(), e, l.WgtCmdGroupRequests[i], common.Sorted(allSelectedAxNodes.ListResNames()))
				logrus.Warnf(msg)
				result.wgtCmdGroupErrors = append(result.wgtCmdGroupErrors, fmt.Errorf("%v", e))
			} else {
				result.wgtCmdGroupGrants = append(result.wgtCmdGroupGrants, wgtCmdGroupGrant)
				srallocs := resource.GatherAllocations(
					map[string]resource.Grant{}, // empty resource grant
					map[string][]resource.GroupGrant{
						l.WgtCmdGroupRequests[i].Id: {wgtCmdGroupGrant},
					})
				allSelectedAxNodes.Allocate(l.Obj, srallocs)
				allAxNodes.Allocate(l.Obj, srallocs)
			}
		}
	}
}

// scheduleOnIx does a greedy, stamp-aware pack of SwDriver pods onto IX nodes:
//   - Allows multiple pods on the same node
//   - Honors the per-stamp CS-port & stamp affinity filters computed by buildPodToCsPortMapping
//   - Falls back to a cross-stamp bucket if any stamp runs out of nodes
//   - Currently supports only single WIO target for SW driver scheduling
func (r *Reconciler) scheduleOnIx(
	ixNodes *resource.Collection,
	l *lock,
	ixNodeFiltersInStamps []resource.Filter,
	wioTargets []common.WioTarget,
	result *grantResult,
) {

	logrus.Debugf("scheduleOnIx called with ixNodes size: %d, names: %v", ixNodes.Size(), ixNodes.ListResNames())

	wioTarget := wioTargets[0]
	targetStampIdx := wioTarget.StampIdx

	if targetStampIdx > len(ixNodeFiltersInStamps)-1 {
		result.swDriverGroupErrors = append(result.swDriverGroupErrors, fmt.Errorf(
			"stamp index %d is out of range for ixNodeFiltersInStamps", targetStampIdx))
		return
	}

	filteredIxNodes := ixNodes
	if targetStampIdx < len(ixNodeFiltersInStamps)-1 {
		filtered := ixNodes.Filter(ixNodeFiltersInStamps[targetStampIdx])
		if filtered.Size() == 0 {
			// Fall back to cross-stamp bucket
			filteredIxNodes = ixNodes.Filter(ixNodeFiltersInStamps[len(ixNodeFiltersInStamps)-1])
		} else {
			filteredIxNodes = filtered
		}
	}

	// Loop through SWDriverGroupRequests to find the first non-empty request since hydrateWioRequests
	// creates one group request per stamp but only populates the stamp that contains the SWDriver pod.
	// Example: For SWDriver in stamp 1, we get [empty_group_0, populated_group_1] and need the populated one.
	var swReq resource.GroupRequest
	for _, req := range l.SWDriverGroupRequests {
		if len(req.Requests) != 0 {
			swReq = req
			break
		}
	}
	if len(swReq.Requests) == 0 {
		result.swDriverGroupErrors = append(result.swDriverGroupErrors, fmt.Errorf("no non-empty SWDriverGroupRequest found"))
		return
	}

	// Attempt to schedule the group request
	gGrant, err := filteredIxNodes.FindGroupScheduling(swReq)
	if err != nil {
		result.swDriverGroupErrors = append(result.swDriverGroupErrors, fmt.Errorf(
			"scheduleOnIx req[%s]: %v", swReq.Id, err))
		return
	}

	// Record successful grant and allocate resources
	result.swDriverGroupGrants = append(result.swDriverGroupGrants, gGrant)

	// Annotate target CS‑ports back onto the Pods
	r.annotatePreferredCsPorts(result.swDriverGroupGrants)
}
func (r *Reconciler) scheduleNodegroups(c *resource.Collection, l *lock, result *grantResult) {
	popNgTypes := l.PopGroupTypes
	popGroupsRequired := l.PopGroupCount
	freePopGroups, freeAllGroups := c.GetFreeNodegroups(popNgTypes)

	totalGroupsRequired := 0
	var groupReqIds []string

	for _, groupReqId := range common.SortedKeys(l.GroupRequests) {
		groupReq := l.GroupRequests[groupReqId]
		// AX scheduling errors are explicitly shown in the error path
		// We filter out the AX group requests to get more accurate error messages for pop/depop nodegroup scheduling
		if groupReqId == rlv1.ActivationRequestName || groupReqId == rlv1.ChiefRequestName ||
			groupReqId == rlv1.WgtCmdGroupPrefix || groupReqId == rlv1.KVStorageServerRequestName || groupReqId == rlv1.SWDriverRequestName {
			continue
		}
		groupReqIds = append(groupReqIds, groupReqId)
		totalGroupsRequired += groupReq.Count
	}
	if totalGroupsRequired == 0 {
		return
	}

	// short-circuit group request matching if there's fewer groups than requested
	result.groupErrorType = NoGroupGrantError
	if len(freePopGroups) < popGroupsRequired {
		// If there are scheduling issues with both pop and depop nodegroups, we only show the pop nodegroup error to be simple
		result.groupErrorType = InsufficientPopGroups
		result.groupErrors = append(result.groupErrors, fmt.Sprintf("requested %d populated nodegroups of type %v but only %d available",
			popGroupsRequired, popNgTypes, len(freePopGroups)))
		result.groupGrantShortage = popGroupsRequired - len(freePopGroups)
		return
	} else if len(freeAllGroups) < totalGroupsRequired {
		result.groupErrorType = InsufficientTotalGroups
		result.groupErrors = append(result.groupErrors, fmt.Sprintf(
			"requested %d nodegroups in total but only %d available",
			totalGroupsRequired, len(freeAllGroups)))
		result.groupGrantShortage = totalGroupsRequired - len(freeAllGroups)
		return
	}

	// Find nodegroups which fit the grouped requests.
	// Outline:
	// Track:
	//   For each GroupRequest, for matched GroupVal -> GroupGrant
	// Then:
	//   Search to find a combination of groupRequest -> groupVal that's unique
	// Then:
	//   Produce a final map of groupRequest -> []groupGrants = length of request.Count.
	//   In the case of a non-match, return all the matched groups anyways for FIFO reservation (e.g. a group request
	//   with count 2 could match 2 groups, but if the second group request doesn't match any available groups, we still
	//   want to reserve the 2 groups that did match so they are not available to lower-priority requests).
	// NOTE: grouped resources are exclusively owned by a single Request so don't consider 2 requests fitting the same
	// group.
	// TODO: consider caching the known matched nodegroups in the lock
	grqGrants := map[string][]resource.GroupGrant{}
	for _, groupReqId := range groupReqIds {
		var grpError error
		mostFitSoFar := -1
		groupReq := l.GroupRequests[groupReqId]

		freeGroups := freeAllGroups
		// We only use the free pop groups collection to grant the primary nodegroup requests for weight streaming jobs
		// Note that SDK execute jobs do not require a populated nodegroup - they only need worker nodes
		if strings.HasPrefix(groupReqId, rlv1.PrimaryNgPrefix) && l.Obj.IsWeightStreaming() && !l.Obj.UseAxOnlyForRuntime() {
			freeGroups = freePopGroups
		}

		for groupName, resources := range freeGroups {
			groupCandidates, err := resources.FindGroupScheduling(groupReq)
			if err == nil {
				groupCandidates.GroupVal = groupName
				grqGrants[groupReqId] = append(grqGrants[groupReqId], groupCandidates)
			} else if mostFitSoFar < groupCandidates.GrantedCount() {
				grpError = err // track err associated with the largest candidate
				mostFitSoFar = groupCandidates.GrantedCount()
			}
		}

		// Skip search stage if impossible grant some group request but still continue to FindGroupScheduling for other
		// requests so that FIFO reservation is preserved. Reservation will eventually succeed as locks are released
		// (lock was validated to fit the cluster on creation).
		if len(grqGrants[groupReqId]) < groupReq.Count {
			// summarize, log group scheduling errors, reset errors
			additionalDetails := ""
			if grpError != nil {
				additionalDetails = fmt.Sprintf(" (largest candidate group failed with %s)", grpError.Error())
			}
			result.groupErrorType = PartiallyScheduled
			result.groupGrantShortage += groupReq.Count - len(grqGrants[groupReqId])
			result.groupErrors = append(result.groupErrors, fmt.Sprintf(
				"%s: %d of %d groups scheduled but required %d%s",
				groupReqId, len(grqGrants[groupReqId]), len(freeGroups), groupReq.Count, additionalDetails,
			))
		}
	}

	// check total candidates, skip search is not enough
	grqIdToGrants := map[string][]resource.GroupGrant{}
	totalCandidates := map[string]bool{}
	// flatten out, e.g. ng-secondary: [ng0, ng1] => ng-secondary-0: [ng0, ng1], ng-secondary-1: [ng0, ng1],
	for grqId, grpGrant := range grqGrants {
		for i := 0; i < l.GroupRequests[grqId].Count; i++ {
			grqIdToGrants[grqId+":"+strconv.Itoa(i)] = grpGrant
			for _, g := range grpGrant {
				totalCandidates[g.GroupVal] = true
			}
		}
	}
	if len(totalCandidates) < len(grqIdToGrants) {
		result.groupGrantShortage = len(grqIdToGrants) - len(totalCandidates)
		result.groupErrorType = PartiallyScheduled
		result.groupErrors = append(result.groupErrors,
			fmt.Sprintf("requested %d nodegroups in total but only %d match request(%v) ",
				len(grqIdToGrants), len(totalCandidates), common.SortedKeys(totalCandidates)))
	}
	if len(result.groupErrors) != 0 {
		return
	}

	// search: find a unique combination of groupRequest -> matched group
	result.groupGrants = map[string][]resource.GroupGrant{}
	if rqToGrant := search(grqIdToGrants); rqToGrant != nil {
		for _, grant := range rqToGrant {
			// Add whether a group grant uses a populated nodegroup as an annotation
			grant.Annotations = common.EnsureMap(grant.Annotations)

			// group was selected from a subset of the collection, hence no error handling needed
			group, _ := c.GetGroup(grant.GroupVal)
			grant.Annotations[wsapisv1.MemxPopNodegroupProp] = strconv.FormatBool(group.IsPopulated())
			result.groupGrants[grant.Request.Id] = append(result.groupGrants[grant.Request.Id], grant)
		}
	} else {
		// search failed, summarize the search
		var errs []string
		grpRqIds := common.Keys(grqGrants)
		sort.Strings(grpRqIds) // for deterministic err msg
		for _, grqId := range grpRqIds {
			grpGrant := grqGrants[grqId]
			var groupsGranted []string
			for _, gg := range grpGrant {
				groupsGranted = append(groupsGranted, gg.GroupVal)
			}
			sort.Strings(groupsGranted)
			errs = append(errs, fmt.Sprintf("%s requires %d of matched %v", grqId, l.GroupRequests[grqId].Count,
				groupsGranted))
		}
		result.groupErrorType = SearchFailed
		result.groupErrors = append(result.groupErrors, "group scheduling not possible: "+strings.Join(errs, ", "))
	}
}

// tryAllocateOptionalRequests allocates optional resources like subresourceOverride and subresourceRanges.
func (r *Reconciler) tryAllocateOptionalRequests(c *resource.Collection, l *lock, result *grantResult) {
	mergedGroupGrants := result.mergeGroupGrants()

	if len(l.GroupRequestUpperBounds) > 0 || len(l.LinearMemoryConfigs) > 0 {
		ubGrant, extraRes := r.tryAllocateRequestUpperBounds(*c, result.resourceGrants, mergedGroupGrants, l)
		for _, grgs := range mergedGroupGrants {
			for i := range grgs {
				ubGrantKey := ""
				if strings.HasPrefix(grgs[i].Request.Id, rlv1.ChiefRequestName) {
					// note that chief does not have upper bound grants
					ubGrantKey = rlv1.ChiefRequestName
				} else if strings.HasPrefix(grgs[i].Request.Id, rlv1.ActivationRequestName) {
					ubGrantKey = rlv1.ActivationRequestName
				} else if strings.HasPrefix(grgs[i].Request.Id, rlv1.KVStorageServerRequestName) {
					ubGrantKey = rlv1.KVStorageServerRequestName
				}

				linearMemoryUpperboundX, LmrXExists := ubGrant[ubGrantKey][resource.AuxiliaryLMRSubresource]
				if LmrXExists && linearMemoryUpperboundX >= 0 {
					grgs[i].Annotations[rlv1.LinearMemRangeXAnnotKey] = strconv.Itoa(linearMemoryUpperboundX)
				}

				for k, g := range grgs[i].Grants {
					requestId := common.Ternary(ubGrantKey != "", ubGrantKey, g.Request.Id)
					if strings.HasPrefix(grgs[i].Request.Id, rlv1.WgtCmdGroupPrefix) &&
						strings.HasPrefix(requestId, rlv1.WeightRequestName) {
						requestId = rlv1.WeightRequestName
					}

					for _, resId := range grgs[i].Grants[k].ResourceIds {
						// If upperbound for X exists, use linear memory granting to calculate memory upperbound grant
						if LmrXExists && linearMemoryUpperboundX >= 0 {
							g.SubresourceOverride = common.EnsureMap(g.SubresourceOverride)
							g.SubresourceOverride[resId] = common.EnsureMap(g.SubresourceOverride[resId])

							replicaId, _ := strconv.Atoi(g.Request.Annotations[rlv1.PodIdAnnotKey])
							overrideMemBytes := l.LinearMemoryConfigs[commonv1.ReplicaType(ubGrantKey)].MemFor(int64(linearMemoryUpperboundX), replicaId)
							// Use ceiling to prevent memory under-allocation in resource accounting.
							// The conversion may round up by at most 1 MiB, which is covered by the pre-allocated 1 MiB mem buffer per replica,
							// ensuring memory totals remain positive.
							overrideMem := int(math.Ceil(common.GetPreciseMiBFromBytes(overrideMemBytes)))
							if g.Subresources[resource.MemSubresource] < overrideMem {
								g.SubresourceOverride[resId][resource.MemSubresource] = overrideMem
							}
						}

						if len(ubGrant[requestId]) > 0 {
							g.SubresourceOverride = common.EnsureMap(g.SubresourceOverride)
							g.SubresourceOverride[resId] = common.EnsureMap(g.SubresourceOverride[resId])

							for t := range ubGrant[requestId] {
								overrideVal := ubGrant[requestId][t]
								if len(extraRes[resId]) > 0 {
									overrideVal = ubGrant[requestId][t] + extraRes[resId][t]
								}

								if g.Subresources[t] < overrideVal {
									g.SubresourceOverride[resId][t] = overrideVal
								}
							}
							grgs[i].Grants[k] = g
						}
					}
				}
			}
		}
		result.upperBounds = ubGrant
	}

	if result.nvmfEnabled(c) {
		for reqId, rg := range result.resourceGrants {
			if reqId != rlv1.CoordinatorRequestName {
				continue
			}

			// size of nvmfGrants will either be the same as the number of weight shards, or zero
			targetIdStr, nvmfGrants := r.tryAllocateNvmfDisks(c, l.Obj, rg, result)
			if len(nvmfGrants) == 0 {
				continue
			}

			var coordUsableRanges []string
			subresourceRangesGrant := resource.SubresourceRanges{}
			for _, g := range nvmfGrants {
				usableRange := fmt.Sprintf("%s:%d-%d",
					g.devicePath, g.addressRange.Start, g.addressRange.End)
				coordUsableRanges = append(coordUsableRanges, usableRange)
				subresourceRangesGrant = subresourceRangesGrant.Add(g.subresourceRanges)
			}

			// set annotations on the coordinator grant
			result.resourceGrants[reqId].Annotations[rlv1.NvmfTargetIdAnnotKey] = targetIdStr
			result.resourceGrants[reqId].Annotations[wsapisv1.NvmfUsableRangesEnvVarName] = strings.Join(
				coordUsableRanges, ",")

			// set subresource ranges on the coordinator grant
			for k, v := range subresourceRangesGrant {
				result.resourceGrants[reqId].SubresourceRanges[k] = v
			}
			break
		}
	}
}

// tryAllocateRequestUpperBounds tries to allocate extra resources on the node for pods with upper bound/limit request
// providing the default/low requests are already satisfied.
//
// There are two types of extra resources: uniform/heterogeneous
// uniform is the extra resources can be applied to all pods on all nodes (max is the upper bound)
// e.g. some node can have small memory or lots of pods, then this uniformed val can be small
// if we have 2 nodes with 10G+4G each, and we have 3 pods with default request of 3GB
// node-0 will have pod0-pod1, node-1 will have pod2
// if upper bound request is 5G, the actual upper bound assign will be 4G since pod2 can only get 4G at most.
// and pod0/pod1 will also have same 4G as upper bound since it's a uniform assign even though 10-4-4=2G left
//
// heterogeneous is the extra resource that differs from pod to pod (max is the upper limit)
// e.g. some node can have large memory or few pods, then pods on that node can have large extra assign
// in the same example above, if request upper limit is 5G, then pod0/pod1 will get the extra 2G shared
// so pod0/pod1 gets 5G while pod2 gets 4G
//
// nodeHeteroAssignReservedMemGB/Ratio are added later to give more buffer in the heterogeneous case only
// e.g. if we say nodeHeteroAssignReservedMemGB is 1G per node, ratio not defined
// then pod2 will still get 4G since it's a uniform assign
// but pod0/pod1 will be 4.5G each since we reserve 1G for this hetero assign
// if we say nodeHeteroAssignReservedMemGB is 3G per node, each pod still gets 4G as it's only for hetero
//
// Returns uniform assign + optional heterogeneous assign
func (r *Reconciler) tryAllocateRequestUpperBounds(
	resources resource.Collection,
	rsGrants map[string]resource.Grant,
	grpGrants map[string][]resource.GroupGrant,
	l *lock) (map[string]resource.Subresources, map[string]resource.Subresources) {

	ubReqs := l.GroupRequestUpperBounds
	nodeHeteroAssignReservedMemGB := l.RtNodeMaxLimitReservedMemGB
	nodeHeteroAssignReservedMemRatio := l.RtNodeMaxLimitReservedMemRatio
	aggrRemainingJobMemory := l.AggrAxMemLimit

	// returned result, type=>final granted bound
	replicaTypeToGrantBoundResult := map[string]resource.Subresources{}
	// returned result, extra resource with id=>res allowed for heterogeneous assignment
	extraResourceAllowed := map[string]resource.Subresources{}

	// index of node id -> granted pods with upper bound requests
	nodeAssignedGrantsAll := map[string][]resource.Grant{}
	// index of rtype => node -> grants for this replica type only
	nodeAssignedGrantsInType := map[string]map[string][]resource.Grant{}

	memSubResType := resource.MemSubresource
	// max limit allowed for heterogeneous extra per pod
	maxHeteroExtra := 0
	// granted max uniform extra per pod
	uniformExtra := math.MaxInt

	// index of node id -> allocated subresource.
	// used to track left over with updated assign
	// this accounts for the base allocation on the current job only
	currentAllocations := resource.GatherAllocations(rsGrants, grpGrants)

	// update aggrRemainingJobMemory by accounting the base allocation on current job
	for k, res := range currentAllocations {
		rs, ok := resources.Get(k)
		if !ok {
			continue
		}

		if rs.HasProp(resource.RoleAxPropKey) {
			aggrRemainingJobMemory -= res.Subresources.Mem()
		}
	}
	logrus.Debugf("aggrRemainingJobMemory: %d", aggrRemainingJobMemory)

	// update currentAllocations to take account for already-allocated memory on AX for job sharing scenario
	for res, allocation := range currentAllocations {
		if rs, ok := resources.Get(res); ok {
			if rs.HasProp(resource.RoleAxPropKey) {
				allocation.Subresources[resource.MemSubresource] += rs.GetAllocatedSubresources()[resource.MemSubresource]
			}
		}
	}

	// build index of replica type => granted pods
	replicaTypeGrantedList := map[string][]resource.Grant{}
	for _, ggList := range grpGrants {
		for _, gg := range ggList {
			// We do not need to consider chief and BR for upper bound granting
			if strings.HasPrefix(gg.Request.Id, rlv1.ChiefRequestName) ||
				strings.HasPrefix(gg.Request.Id, rlv1.BrRequestName) ||
				// Jul 2025: SWDriver upper bound granting needs to be discussed with framework
				// We will skip upper bound granting and only keep base memory allocation for now
				strings.HasPrefix(gg.Request.Id, rlv1.SWDriverRequestName) {
				continue
			}

			isActGroupGrant := false
			// When ax scheduling is enabled, activation grants are originated from activation group requests
			if strings.HasPrefix(gg.Request.Id, rlv1.ActivationRequestName) {
				isActGroupGrant = true
			}
			isKvssGroupGrant := false
			// When ax scheduling is enabled, off-wafer prompt caching (KVSS) grants are originated from activation group requests
			if strings.HasPrefix(gg.Request.Id, rlv1.KVStorageServerRequestName) {
				isKvssGroupGrant = true
			}
			isWgtCmdGroupGrant := false
			if strings.HasPrefix(gg.Request.Id, rlv1.WgtCmdGroupPrefix) {
				isWgtCmdGroupGrant = true
			}
			for _, g := range gg.Grants {
				requestId := common.Ternary(isActGroupGrant, rlv1.ActivationRequestName,
					common.Ternary(isKvssGroupGrant, rlv1.KVStorageServerRequestName, g.Request.Id))
				if isWgtCmdGroupGrant && strings.HasPrefix(g.Request.Id, rlv1.WeightRequestName) {
					requestId = rlv1.WeightRequestName
				}
				replicaTypeGrantedList[requestId] = append(replicaTypeGrantedList[requestId], g)
			}
		}
	}

	var rtRequiresLinearMemAllocation []commonv1.ReplicaType
	for replicaType, _ := range l.LinearMemoryConfigs {
		rtRequiresLinearMemAllocation = append(rtRequiresLinearMemAllocation, replicaType)
		// Add a dummy memory ubReq for the LMR replica types, it will ensure the propagation
		// of the nodeAssignedGrantsInType and nodeAssignedGrantsAll
		// this upperbound will not end up in any estimations.
		ubReqs[replicaType.String()] = common.EnsureMap(ubReqs[replicaType.String()])
		ubReqs[replicaType.String()][memSubResType] = -1
	}

	// build index of node id => granted pods
	replicaTypes := common.Keys(ubReqs)
	for _, replicaType := range replicaTypes {
		ubReq := ubReqs[replicaType]
		for srType := range ubReq {
			if strings.HasSuffix(srType, resource.PodUpperBoundMaxLimitPostFix) {
				continue
			}

			nodeAssignedGrantsInType[replicaType] = common.EnsureMap(nodeAssignedGrantsInType[replicaType])
			for _, g := range replicaTypeGrantedList[replicaType] {
				for _, resId := range g.ResourceIds {
					nodeAssignedGrantsInType[replicaType][resId] = append(nodeAssignedGrantsInType[replicaType][resId], g)
					nodeAssignedGrantsAll[resId] = append(nodeAssignedGrantsAll[resId], g)
				}
			}
		}
	}

	// Once allocated LMR upperbounds for certain resources (e.g. memory),
	// The same resource will not go though uniform/heterogeneous allocation.
	if len(rtRequiresLinearMemAllocation) > 0 {
		tryAllocateRequestLMRUpperbounds(
			aggrRemainingJobMemory,
			currentAllocations,
			l,
			nodeAssignedGrantsInType,
			replicaTypeGrantedList,
			resources,
			replicaTypes,
			replicaTypeToGrantBoundResult,
			rtRequiresLinearMemAllocation,
			ubReqs)
	}

	// calculate uniform extra by evenly assign left over to all grants
	for resId, grants := range nodeAssignedGrantsAll {
		leftOver := resources.GetLeftOverSubResource(resId, memSubResType, currentAllocations)
		perPodExtra := int(leftOver / float64(len(grants)))
		if perPodExtra < uniformExtra {
			uniformExtra = perPodExtra
		}
		if uniformExtra <= 0 {
			break
		}
	}

	// update resource grants/allocation with calculated uniform assign
	for _, replicaType := range replicaTypes {
		ubReq := ubReqs[replicaType]
		for srType, srUpperBound := range ubReq {
			// Memory grant is already complete by linear memory allocation
			if len(rtRequiresLinearMemAllocation) > 0 && srType == memSubResType {
				continue
			}

			if strings.HasSuffix(srType, resource.PodUpperBoundMaxLimitPostFix) {
				continue
			}
			srMinRequest := replicaTypeGrantedList[replicaType][0].Request.Filter.Subresources()[srType]
			extra := srUpperBound - srMinRequest
			if uniformExtra < extra {
				extra = uniformExtra
			}
			srMaxLimit := ubReq[srType+resource.PodUpperBoundMaxLimitPostFix]
			if srMaxLimit-srMinRequest-extra > maxHeteroExtra {
				maxHeteroExtra = srMaxLimit - srMinRequest - extra
			}

			replicaTypeToGrantBoundResult[replicaType] = common.EnsureMap(replicaTypeToGrantBoundResult[replicaType])
			replicaTypeToGrantBoundResult[replicaType][srType] = srMinRequest + extra
			for resId, grants := range nodeAssignedGrantsInType[replicaType] {
				currentAllocations[resId].Subresources[srType] += extra * len(grants)
			}
		}
	}
	logrus.Infof("granted uniform upper bound: %v", replicaTypeToGrantBoundResult)

	// todo: check limits for different replica types
	// https://cerebras.atlassian.net/browse/SW-115770
	if maxHeteroExtra > 0 {
		for resId, grants := range nodeAssignedGrantsAll {
			rs, _ := resources.Get(resId)
			maxAllowed := rs.GetMemForHeteroAssign(nodeHeteroAssignReservedMemGB, nodeHeteroAssignReservedMemRatio)
			leftOver := maxAllowed - currentAllocations[resId].Subresources[memSubResType]
			perPodExtra := int(float32(leftOver) / float32(len(grants)))
			if perPodExtra <= 0 {
				continue
			}
			if perPodExtra > maxHeteroExtra {
				perPodExtra = maxHeteroExtra
			}
			extraResourceAllowed[resId] = common.EnsureMap(extraResourceAllowed[resId])
			extraResourceAllowed[resId][memSubResType] = perPodExtra
			logrus.Infof("extra mem of %dMB assigned for each of %d pods on node %s, "+
				"total assigned w/o overhead %d, reserved mem gb %d / ratio %f / capacity %dMB",
				perPodExtra, len(grants), resId,
				maxAllowed, nodeHeteroAssignReservedMemGB, nodeHeteroAssignReservedMemRatio,
				rs.GetSubresourceCountCapacity(memSubResType))
		}
	}

	return replicaTypeToGrantBoundResult, extraResourceAllowed
}

// tryAllocateNvmfDisks tries to allocate the contiguous range of NVMe-oF disk space requested for each weight shard.
// In the event nothing or only partial grant was possible, we will not grant any disk space for any shard.
func (r *Reconciler) tryAllocateNvmfDisks(c *resource.Collection, lock *rlv1.ResourceLock,
	rg resource.Grant, result *grantResult) (string, []nvmfGrant) {
	res, _ := c.Get(rg.ResourceIds[0])
	targetIdStr, _ := res.GetProp(resource.NvmfTargetIdPropKey)
	targetId, _ := strconv.Atoi(targetIdStr)
	if targetId <= 0 {
		return "", nil
	}

	var perShardBytesRequests []int64
	perShardBytesUpperBound, _ := strconv.ParseInt(
		rg.Request.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesUbAnnotKey], 10, 64)
	if perShardBytesUpperBound > 0 {
		perShardBytesRequests = append(perShardBytesRequests, perShardBytesUpperBound)
	}
	perShardBytes, _ := strconv.ParseInt(rg.Request.Annotations[wsapisv1.WsJobPerWeightShardNvmfBytesAnnotKey],
		10, 64)
	if perShardBytes > 0 {
		perShardBytesRequests = append(perShardBytesRequests, perShardBytes)
	}

	// TODO: Use binary search to find the biggest integer k where the k*base amounts is between base and upper bound
	for _, bytesRequested := range perShardBytesRequests {
		// always start with a disk with lowest usage to avoid skews among disks across jobs
		nvmfDisksSortedByLoad := res.GetSubresourceRangesResNameSortedByLoad(resource.NvmfDiskSubresource)
		diskId := -1
		requestFailed := false
		var nvmfGrants []nvmfGrant

		resCopy := res.Copy()
		for groupReqId, ggs := range result.groupGrants {
			if !strings.HasPrefix(groupReqId, rlv1.PrimaryNgPrefix) {
				continue
			}
			for _, gg := range ggs {
				for _, g := range gg.Grants {
					if g.Request.Id != rlv1.WeightRequestName {
						continue
					}

					for i := 0; i < g.Request.Count; i++ {
						if requestFailed {
							break
						}
						// round-robin across nvme disks - do not encourage using disks with far less allocation
						// this is needed to load balance usage across shards
						for j := 0; j < len(nvmfDisksSortedByLoad); j++ {
							diskId = (diskId + 1) % len(nvmfDisksSortedByLoad)
							diskName := nvmfDisksSortedByLoad[diskId]

							srrCandidate := resCopy.GetSubresourceRangesCandidate(diskName, bytesRequested)
							if srrCandidate != nil && len((*srrCandidate)[diskName]) == 1 {
								devicePath := fmt.Sprintf("%s%d0%c", resource.NvmfDevicePathPrefix,
									targetId, diskName[len(diskName)-1])
								nvmfGrants = append(nvmfGrants, nvmfGrant{
									deviceResName: diskName,
									devicePath:    devicePath,
									addressRange:  (*srrCandidate)[diskName][0],
									subresourceRanges: resource.SubresourceRanges{
										diskName: (*srrCandidate)[diskName],
									},
								})

								resCopy.Allocate(lock, resource.SubresourceAllocation{SubresourceRanges: *srrCandidate})
								break
							} else if j == len(nvmfDisksSortedByLoad)-1 {
								requestFailed = true
							}
						}
					}
				}
			}
		}
		if !requestFailed {
			return targetIdStr, nvmfGrants
		}
	}
	return "", nil
}

// buildPodToCsPortMapping decides on which CS port each activation pod needs talking to, given the granted systems in order
// as well as the CS domain each activation pod needs talking to.
//
// There are a few goals:
// 1. Make sure activation pods targeted different systems have the same fairness in utilizing port affinities during scheduling.
// 2. Make sure on every system, port load within every CS domain is balanced - at most differed by 1.
// 3. For every CS domain in every stamp, make sure the ports with higher availability get considered prior to other ports.
//
// Inputs:
// - c, input collection which is used to find out ports with higher availability for every CS domain in every stamp
// - nodeFiltersInStamps, node filter to use for every stamp to find the AX nodes in order to evaluate on port availability
// - systemsOrderedInStamps, [][]string{{"sys-0","sys-1"},{"sys-5"}}, means two systems were granted in one stamp and another system granted in another stamp
// - wioTargets, slice of WioTarget structs containing pod system assignments and domains, gets updated with assigned target ports
// - inferenceSharingEnabled, whether job sharing is enabled
// - considerGlobalPortAllocation: if enabled, evenly distribute pods across ports in the requested domain over all systems in the same stamp;
// if disabled, do only local distribution on per-system basis.
func (r *Reconciler) buildPodToCsPortMapping(c *resource.Collection, nodeFiltersInStamps []resource.Filter,
	systemsOrderedInStamps [][]string, wioTargets []common.WioTarget, linearMemoryConfig common.LinearMemoryConfig,
	inferenceSharingEnabled bool, considerGlobalPortAllocation bool) {
	// for example: sysPortUsage[sys0][7] = 2 means, cs-port-7 on sys0 is used twice
	sysPortUsage := map[string]map[int]int{}
	// for example: affinityPortsPerDomainInStamps[0][3] = []int{10, 11, 9} means, in the first granted stamp
	// we should prioritize using port 10 for talking to domain 3
	var affinityPortsPerDomainInStamps []map[int][]int
	// for example: sysIdxStampIdxMap[0] = 0 means, the first system is in the first granted stamp
	// this is used to later on decide which is the dominating stamp for act traffic, which we would only prioritize for
	sysIdxStampIdxMap := map[int]int{}
	// for example: inStampSystemIndices[0] = []int{0, 1} means the first two granted systems are in the first granted stamp
	inStampSystemIndices := map[int][]int{}
	var systemNames []string
	for i, perStampSystemNames := range systemsOrderedInStamps {
		for _, systemName := range perStampSystemNames {
			if sys := r.Cfg.SystemMap[systemName]; sys != nil {
				sysPortUsage[systemName] = map[int]int{}
				for id, port := range sys.Ports {
					// If we reached here, it means the granted systemNames were schedulable,
					// which also means there were at least two healthy ports per domain per system.
					// Otherwise, the "SystemPortCompatibleCheck" function should have failed.
					if !port.HasError {
						sysPortUsage[systemName][id] = 0
					}
				}
			}
			sysIdxStampIdxMap[len(systemNames)] = i
			inStampSystemIndices[i] = append(inStampSystemIndices[i], len(systemNames))
			systemNames = append(systemNames, systemName)
		}

		// Among all AX nodes in a stamp, if job sharing is enabled we take all of them, else we only take the free ones.
		// We then sort the ports such that more preferred ports get used first.
		// This helps load balancing targeted ports across different jobs, especially when jobs use very few act pods.

		// TODO: When job sharing is enabled, we look at all nodes in a stamp, but this causes us to return the same affinity count for all ports.
		// This is not ideal, and we need a way to factor port allocations from other jobs when constructing the preferred sequence of ports for a given domain.
		selectedAxNodesInStamp := c.Filter(nodeFiltersInStamps[i]).Filter(
			common.Ternary(!inferenceSharingEnabled,
				resource.NewFreeNodesFilter(),
				resource.NewJobAntiAffinityFilter(wsapisv1.InferenceJobSharingAnnotKey, wsapisv1.ConstTrue))).Copy()

		localPortsInStamp := map[int]int{}
		for _, nodeRes := range selectedAxNodesInStamp.List() {
			if node := r.Cfg.NodeMap[nodeRes.Name()]; node != nil {
				for _, port := range node.GetCsPortsInAffinity(r.Cfg) {
					localPortsInStamp[port] += 1
				}
			}
		}
		affinityPortsPerDomain := map[int][]int{}
		for domain := 0; domain < common.CSVersionSpec.NumPortDomains; domain++ {
			ports := common.CSVersionSpec.PortsFromDomain(domain)
			sort.Slice(ports, func(i, j int) bool {
				if localPortsInStamp[ports[i]] != localPortsInStamp[ports[j]] {
					return localPortsInStamp[ports[i]] > localPortsInStamp[ports[j]]
				}
				return ports[i] < ports[j]
			})
			affinityPortsPerDomain[domain] = ports
		}
		affinityPortsPerDomainInStamps = append(affinityPortsPerDomainInStamps, affinityPortsPerDomain)
	}

	// Need a separate array for sorting wioTargets based on usage such that the original wioTarget order is preserved,
	// the original order is important as a lot of assumptions are being made based on that, e.g. activation-0 contains
	// SwDriver, etc.
	podIdxs := make([]int, len(wioTargets))
	var normalizedIoUsages []int64
	if !common.DisableWioBandwidthLB {
		normalizedIoUsages = linearMemoryConfig.GetNormalizedWioUsages()
	}

	for podIdx := range wioTargets {
		podUsage := int64(1)
		if podIdx < len(normalizedIoUsages) {
			podUsage = normalizedIoUsages[podIdx]
		}
		wioTargets[podIdx].WioBandwidth = int(podUsage)
		podIdxs[podIdx] = podIdx
	}

	if !common.DisableWioBandwidthLB {
		// Sort by usage descending for greedy port selection based min used port
		sort.Slice(podIdxs, func(i, j int) bool {
			return wioTargets[podIdxs[i]].WioBandwidth > wioTargets[podIdxs[j]].WioBandwidth
		})
	}

	// Interleaving order is not useful in KVSS where each pod talks to strictly 1 system/1 port,
	// the port selection balancing is limited to individual systems. Using an example:
	// ===========================================================================================================
	// System 0: pod0, pod1, pod2, pod3
	// System 1: pod4, pod5, pod6, pod7
	// Each pod has usage of 1
	//
	// Interleaving order for pod:
	// 0 (sys0 port0), 4 (sys1 port0), 1 (sys0 port1), 5 (sys1 port 1), 2 (sys0 port 2), 6 (sys1 port2), 3 (sys0 port0), 7 (sys1 port0)
	// Port usage would be sys0 [2, 1, 1] sys1 [2, 1, 1]
	//
	// Normal order:
	// 0 (sys0 port0), 1 (sys0 port1), 2 (sys0 port2), 3 (sys0 port0), 4 (sys1 port0), 5 (sys1 port1), 6 (sys1 port2), 7 (sys1 port0)
	// Port usage would be sys0 [2, 1, 1] sys1 [2, 1, 1], which is exactly the same as interleaving order
	// ===========================================================================================================
	//
	// Here we will use the descending order based on usage to apply greedy, to achieve better balancing. Again, an example:
	// ===========================================================================================================
	// Assume we have 5 pods talking to domain 0, we would like to split the 5 pods across 3 ports:
	// Usage without sorting [1, 3, 1, 4, 5]
	// Applying greedy on the 3 ports: [5 (1 + 4), 3, 6 (1 + 5)]
	//
	// Whereas if we sort in descending order: [5, 4, 3, 1, 1]
	// Applying greedy: [5, 5 (4 + 1), 4 (3 + 1)], this gives a more balanced result compared to non-sorting
	// ===========================================================================================================
	var visitingOrder []int
	if len(normalizedIoUsages) == 0 {
		// The interleaving order is probably not useful at all, even for MoE cases.
		// This is because the port allocation is deterministic, by balancing the usage on each port we already achieve the best allocation.
		// The greedy algorithm already take cover of that, thus the interleave order is not too useful.
		// TODO: evaluate and remove the interleaving order requirements
		visitingOrder = common.GenerateSystemInterleavingOrder(wioTargets)
	} else {
		visitingOrder = podIdxs
	}

	for _, podIdx := range visitingOrder {
		// For non-moe runs, an activation only needs to talk to one system.
		// For moe runs, an activation needs to talk to multiple systems, among which we could only
		// optimize traffic for the belonging stamp.
		systemIdx := wioTargets[podIdx].AllSystemIndices[0]
		if len(wioTargets[podIdx].AllSystemIndices) > 1 {
			// wioTargetsPerCsx: len(wioTargets) / len(systemNames)
			systemIdx = podIdx / (len(wioTargets) / len(systemNames))
		}

		stampIdx := sysIdxStampIdxMap[systemIdx]
		wioTargets[podIdx].StampIdx = stampIdx

		if considerGlobalPortAllocation {
			wioTargets[podIdx].InStampSystemIndices = inStampSystemIndices[stampIdx]
		} else {
			// Only consider systems in stamp that are assigned to the pod
			var targetSysIndicesInStamp []int
			for _, sysIdx := range wioTargets[podIdx].AllSystemIndices {
				if slices.Contains(inStampSystemIndices[stampIdx], sysIdx) {
					targetSysIndicesInStamp = append(targetSysIndicesInStamp, sysIdx)
				}
			}
			wioTargets[podIdx].InStampSystemIndices = targetSysIndicesInStamp
		}

		// Reset target ports as validateNewLock and grantQueue will go through the port allocation twice
		wioTargets[podIdx].TargetPorts = []int{}

		// In rel-2.3, each activation only talks to one domain
		// For KVSS, each pod could talk to multiple domains
		domains := wioTargets[podIdx].Domains
		for _, domain := range domains {
			// Load balance to always choose the port which served lowest aggregated traffic
			ports := affinityPortsPerDomainInStamps[stampIdx][domain]
			minUsedPort := -1
			minAggrUsage := math.MaxInt

			for _, port := range ports {
				aggrUsage := 0
				for _, sysIdx := range wioTargets[podIdx].InStampSystemIndices {
					aggrUsage += sysPortUsage[systemNames[sysIdx]][port]
				}
				if minUsedPort == -1 || aggrUsage < minAggrUsage {
					minUsedPort = port
					minAggrUsage = aggrUsage
				}
			}

			for _, sysIdx := range wioTargets[podIdx].InStampSystemIndices {
				sysPortUsage[systemNames[sysIdx]][minUsedPort] += wioTargets[podIdx].WioBandwidth
			}
			wioTargets[podIdx].TargetPorts = append(wioTargets[podIdx].TargetPorts, minUsedPort)
		}
	}

	// TODO: use metrics such as pod_port_affinity instead of logs
	logrus.Infof("normalized usages based on LMR: %v", normalizedIoUsages)
	logrus.Infof("final sys/port allocation with usage: %v", sysPortUsage)
}

// If we do not have sufficient AX nodes in stamps to fulfill the stamp-aware group requests, we will redistribute
// the requests to a set of cross-stamp group requests.
// The "systemsOrderedInStamps" input is stamp-aware. In case we could not find sufficient AX nodes in one or more stamps,
// we will relax the scheduling requirements by allowing chiefs and activations to talk to the "left-over" systems
// in different stamps. This is done by adding a new stamp-agnostic slice to "systemsOrderedInStamps", in which case,
// the last element in "axNodeFiltersInStamps" will be able to realize the relaxation with the lack of the stamp constraint.
//
// Note that the pod request redistribution is needed due to the current structure of using stamp-aware group requests.
// There is an opportunity to optimize by making pod requests stamp-aware, such that we could do one group scheduling
// instead of multiple.
//
// TODO: optimize cross stamp redistribution for job sharing: https://cerebras.atlassian.net/browse/SW-200355
func (r *Reconciler) useCrossStampAxIfNecessary(allAxNodes *resource.Collection, l *lock, systemsOrderedInStamps [][]string,
	axNodeFiltersInStamps []resource.Filter) [][]string {
	redistributedChfGroupReq := resource.GroupRequest{
		Id:       fmt.Sprintf("%s-group-redistributed", rlv1.ChiefRequestName),
		Count:    1,
		Requests: map[string]resource.Request{},
		KeySort:  resource.V2GenericAscKeySort,
	}
	redistributedActGroupReq := resource.GroupRequest{
		Id:       fmt.Sprintf("%s-group-redistributed", rlv1.ActivationRequestName),
		Count:    1,
		Requests: map[string]resource.Request{},
		KeySort:  resource.V2ActKvssKeySort,
	}
	redistributedKvssGroupReq := resource.GroupRequest{
		Id:       fmt.Sprintf("%s-group-redistributed", rlv1.KVStorageServerRequestName),
		Count:    1,
		Requests: map[string]resource.Request{},
		KeySort:  resource.V2ActKvssKeySort,
	}

	var redistributedSystems []string
	actPodsPerCsx := len(l.ActTargets) / l.ResourceRequests[rlv1.SystemsRequestName].Count
	systemIdx := 0
	for i, ss := range systemsOrderedInStamps {
		stampIdx := ""
		if val, ok := axNodeFiltersInStamps[i].Properties()[resource.StampPropertyKey]; ok && len(val) > 0 {
			stampIdx = val[0]
		}
		axNodes := allAxNodes.Filter(axNodeFiltersInStamps[i]).Filter(
			common.Ternary(!l.isInferenceJobSharingEnabled(),
				resource.NewFreeNodesFilter(),
				resource.NewJobAntiAffinityFilter(wsapisv1.InferenceJobSharingAnnotKey, wsapisv1.ConstTrue))).Copy()

		// We redistribute only when axNodes.Size() < wsapisv1.AxCountPerSystem * len(ss).
		if axNodes.Size() >= wsapisv1.AxCountPerSystem*len(ss) {
			systemIdx += len(ss)
		} else {
			numChiefsRetainedInStamp := axNodes.Size() / wsapisv1.AxCountPerSystem
			systemsOrderedInStamps[i] = ss[:numChiefsRetainedInStamp]
			redistributedSystems = append(redistributedSystems, ss[numChiefsRetainedInStamp:]...)

			// Redistribute chiefs accordingly
			redistributedChfReq, ok := redistributedChfGroupReq.Requests[rlv1.ChiefRequestName]
			if !ok {
				redistributedChfReq = l.ChfGroupRequests[i].Requests[rlv1.ChiefRequestName].Copy()
				redistributedChfReq.Count = 0
			}
			redistributedChfReq.Count += len(ss) - numChiefsRetainedInStamp
			redistributedChfGroupReq.Requests[rlv1.ChiefRequestName] = redistributedChfReq
			if axNodes.Size() == 0 {
				delete(l.ChfGroupRequests[i].Requests, rlv1.ChiefRequestName)
			} else {
				originalStampChfReq := l.ChfGroupRequests[i].Requests[rlv1.ChiefRequestName].Copy()
				originalStampChfReq.Count = numChiefsRetainedInStamp
				l.ChfGroupRequests[i].Requests[rlv1.ChiefRequestName] = originalStampChfReq
			}
			logrus.Infof("lock: %s, redistributing %d chief(s) to AX nodes that are not in stamp %s "+
				"where it does not contain sufficient AX nodes",
				l.Obj.NamespacedName(), len(ss)-numChiefsRetainedInStamp, stampIdx)

			// Redistribute activations accordingly
			numActRedistributed := 0
			numKvssRedistributed := 0
			systemIdx += numChiefsRetainedInStamp
			for j := numChiefsRetainedInStamp; j < len(ss); j++ {
				if i < len(l.ActGroupRequests) {
					for actSubReqId, actSubReq := range l.ActGroupRequests[i].Requests {
						actPodIdx, _ := strconv.Atoi(actSubReqId)
						if actPodIdx/actPodsPerCsx == systemIdx {
							redistributedActGroupReq.Requests[actSubReqId] = actSubReq.Copy()
							delete(l.ActGroupRequests[i].Requests, actSubReqId)
							numActRedistributed += 1
						}
					}
				}

				if i < len(l.KvssGroupRequests) {
					for kvssSubReqId, kvssSubReq := range l.KvssGroupRequests[i].Requests {
						kvssTargetSystemIdx, _ := strconv.Atoi(kvssSubReq.Annotations[rlv1.SystemIdAnnotKey])
						if kvssTargetSystemIdx == systemIdx {
							redistributedKvssGroupReq.Requests[kvssSubReqId] = kvssSubReq.Copy()
							delete(l.KvssGroupRequests[i].Requests, kvssSubReqId)
							numKvssRedistributed += 1
						}
					}
				}
				systemIdx += 1
			}
			logrus.Infof("lock: %s, redistributing %d activation(s) to AX nodes that are not in stamp %s "+
				"where it does not contain sufficient AX nodes",
				l.Obj.NamespacedName(), numActRedistributed, stampIdx)
		}
	}

	if len(redistributedSystems) > 0 {
		systemsOrderedInStamps = append(systemsOrderedInStamps, redistributedSystems)
		l.ChfGroupRequests = append(l.ChfGroupRequests, redistributedChfGroupReq)
		l.ActGroupRequests = append(l.ActGroupRequests, redistributedActGroupReq)
		l.KvssGroupRequests = append(l.KvssGroupRequests, redistributedKvssGroupReq)
	}

	return systemsOrderedInStamps
}

// We split Replica Types (RT) into ones that require Linear Memory Range (LMR) and ones that don't
// The sequence of placement is:
// 1. RT without LMR (non-LMR). Try allocate to upperbound for each replica, if not enough resource,
// grant uniform extra which is constrained by the node with least remaining resources.
// 2. RT with LMR. Calculate upperbound X for the entire replica group, and grant using the linear equation based on the remaining memory.
//
// Using an example, all memory numbers are in GiB:
// Assume 4 AX nodes, and 2 out of 4 systems requested
// Remaining memory on the 4 nodes are: 20, 20, 10, 10
// 4 non-LMR Act pods, Base: 1, UpperBound: 2
// 4 LMR Kvss pods, MinX: 1, MaxX: 4, Coef: {2,2,1,1}, Intercept: 1
// LMR model: memory = Coef[i] * X + Intercept
// Assume 1 Act + 1 Kvss on each AX
//
// AggregatedJobMemoryLimit is (20 + 20 + 10 + 10) * (2/4) = 30
// BaseActSum = BaseAct * NumActPods = 4
// BaseKvssSum = SUM(coef) * MinX + NumKvssPods * Intercept = 6 * 1 + 4 * 1 = 10
// AggregatedRemainingMemory = 30 - (4 + 10) = 16
// On each node: 4/20, 4/20, 3/10, 3/10, no overflow
//
// Placing Act (non-LMR) to upperbound: AggrRemainingMemory = 16,  ActExtraMem = UB - base = 2 - 1 = 1
// Each AX node gets 1 extra memory granting, AggrRemainingMemory = 16 - 4 = 12
// On each node: 5/20, 5/20, 4/10, 4/10, no overflow
// Each Act pod gets memory grating of 2 => ActTotal: 8
//
// Placing Kvss (LMR) to upperbound, this is done by evaluating X for LMR
// When MaxX is specified in LMR, we use MaxX - 1 as the calculated upperboundX, when MaxX is -1 we use the AggrRemainingMemory
// to calculate upperbound X
// CalculatedExtraX = AggrRemainingMemory / SUM(Coef) = 12 / 6 = 2
// UpperboundX = CalculatedExtraX + MinX = 2 + 1 = 3
// 3 is between MinX(1) and MaxX(4), so we are good
// Each Kvss gets total memory granting of EstimatedX * Coef + Intercept
// Kvss1: 7, Kvss2: 7, Kvss3: 4, Kvss4: 4 => KvssTotal: 22
//
// TotalMemoryAllocated: ActTotal + KvssTotal = 8 + 22 = 30, aligns with AggrJobMemoryLimit
// Memory allocated on each node: 9/20, 9/20, 6/10, 6/10, no overflow
//
// Note that the above example assumed no overflow for both non-LMR and LMR pod types.
// We would handle overflow by lowering the amount of extra Mem/X calculated,
// e.g. assume we calculated extraX_for_node1 * SUM(Coef_for_node1) >= remaining memory on the node,
// we will use a lower extraX that does not cause an overflow, and in the worst case scenario the MinX,
// which means only base memory is granted for the pods.
func tryAllocateRequestLMRUpperbounds(
	aggrRemainingJobMemory int,
	currentAllocations map[string]resource.SubresourceAllocation,
	l *lock,
	nodeAssignedGrantsInType map[string]map[string][]resource.Grant,
	replicaTypeGrantedList map[string][]resource.Grant,
	resources resource.Collection,
	replicaTypes []string,
	replicaTypeToGrantBoundResult map[string]resource.Subresources,
	rtRequiresLinearMemAllocation []commonv1.ReplicaType,
	ubReqs map[string]resource.Subresources) {

	memSubResType := resource.MemSubresource

	getReplicas := func(resourceGrants []resource.Grant) []int {
		var replica []int
		for _, grant := range resourceGrants {
			replicaId, _ := strconv.Atoi(grant.Request.Id)
			replica = append(replica, replicaId)
		}
		return replica
	}

	// Resource not requiring linear memory allocation
	// Upperbounds are calculated by comparing memory
	for _, replicaType := range replicaTypes {
		if !slices.Contains(rtRequiresLinearMemAllocation, commonv1.ReplicaType(replicaType)) {
			memUpperBound := ubReqs[replicaType][memSubResType]
			numPods := len(replicaTypeGrantedList[replicaType])
			memBase := replicaTypeGrantedList[replicaType][0].Request.Filter.Subresources()[memSubResType]
			memExtra := memUpperBound - memBase

			// Respect the per job-fair share against CSXs for non-LMR types
			if l.isInferenceJobSharingEnabledOnAx(commonv1.ReplicaType(replicaType)) && memExtra*numPods > aggrRemainingJobMemory {
				memExtra = aggrRemainingJobMemory / numPods
			}

			for resId, grants := range nodeAssignedGrantsInType[replicaType] {
				numPodsOnNode := len(grants)
				memLeftOver := resources.GetLeftOverSubResource(resId, memSubResType, currentAllocations)
				// For each node, if the sum of extraMem calculated out of upperbound request is greater than the leftover memory,
				// there's no way to grant all replicas to upperbound. The best we can do is to allow the replicas take an even share
				// of what's left on the node. And the smallest share across all nodes would become the uniform extra, that is
				// assigned to all replicas of this type.
				//
				// This is not optimal because in this case non-LMR replicas are prioritized when granting memory, and sequence matters:
				// the replica types up front are more likely to get higher memory granting when upperbound cannot be offered.
				// For now this is good enough because we only have ACT pods as non-LMR types, and there's a plan to move ACT out of AX.
				// TODO: Design a fair algorithm to balance the memory allocation on node between non-LMR and LMR types
				if float64(memExtra*numPodsOnNode) > memLeftOver {
					memExtra = int(math.Floor(memLeftOver / float64(numPodsOnNode)))
				}
			}
			replicaTypeToGrantBoundResult[replicaType] = common.EnsureMap(replicaTypeToGrantBoundResult[replicaType])
			replicaTypeToGrantBoundResult[replicaType][memSubResType] = memBase + memExtra
			if l.isInferenceJobSharingEnabledOnAx(commonv1.ReplicaType(replicaType)) {
				aggrRemainingJobMemory -= memExtra * numPods
			}
			for resId, grants := range nodeAssignedGrantsInType[replicaType] {
				currentAllocations[resId].Subresources[memSubResType] += memExtra * len(grants)
			}
		}
		logrus.Debugf("aggrRemainingJobMemory after allocating non-linear memory upperbounds %d", aggrRemainingJobMemory)
	}

	// Sort the rtRequiresLinearMemAllocation such that the LMR upperbounds are granted in correct order.
	// If a replica type does not exit in LMRGrantingSeq, it is granted first as it gets sequence value 0.
	sort.SliceStable(rtRequiresLinearMemAllocation, func(i, j int) bool {
		return common.LMRGrantingSeq[rtRequiresLinearMemAllocation[i]] < common.LMRGrantingSeq[rtRequiresLinearMemAllocation[j]]
	})

	// Each LMR RT should take a even share of the residual memory on the node.
	// E.g. when a node has 400Gi memory left, if 2 RT are scheduled on it, each of them can grant up to 200Gi;
	// if only 1 RT is scheduled on it, the 1 RT can grant up to 400Gi.
	lmrRTAllocatedCount := map[string]int{}
	for _, rtt := range rtRequiresLinearMemAllocation {
		for resId, _ := range nodeAssignedGrantsInType[rtt.String()] {
			lmrRTAllocatedCount[resId]++
		}
	}

	// Resource requiring linear memory allocation
	// Upperbounds are calculated by comparing X
	for _, rtt := range rtRequiresLinearMemAllocation {
		lmc := l.LinearMemoryConfigs[rtt]
		replicaType := rtt.String()
		extraX := lmc.Global.MaxX - lmc.Global.MinX
		// Reserve memory buffer to prevent over-allocation during MiB conversion.
		//
		// Problem: LMR_X values are calculated in bytes, but memory allocation uses MiB units.
		// When converting bytes to MiB, the system defaults to floor() which under-allocates
		// memory compared to the actual LMR_X requirements. Using ceil() would be more accurate
		// but risks over-allocation, potentially causing negative remaining memory on nodes.
		//
		// Solution: Pre-allocate a buffer (1 MiB per replica) before performing memory calculations.
		// This allows safe use of ceil() on each replica's MiB memory grant for more accurate allocation
		// without exceeding available node memory.
		memBuffer := wsapisv1.LMRReplicaMemoryBufferMiB * lmc.GetReplicaCount()
		// Use fair-share aggregated AX remaining memory for upperbound estimation is global MaxX is -1
		if lmc.Global.MaxX == -1 {
			extraX = lmc.CalculateUpperboundX(int64(aggrRemainingJobMemory-memBuffer)<<20, nil, false)
			logrus.Infof("Calculated extra X: %d with buffer room of %d MiB, with aggrRemainingJobMemory: %d", extraX, memBuffer, aggrRemainingJobMemory)
		}

		for resId, grants := range nodeAssignedGrantsInType[replicaType] {
			// resId should always exist in lmrRTAllocatedCount, and count should always > 0
			rtCount := lmrRTAllocatedCount[resId]
			if rtCount <= 0 {
				continue
			}
			memLeftOverPerRT := int64(resources.GetLeftOverSubResource(resId, memSubResType, currentAllocations) / float64(rtCount))
			replicas := getReplicas(grants)
			memBuffer = wsapisv1.LMRReplicaMemoryBufferMiB * len(replicas)
			calculatedX := lmc.CalculateUpperboundX((memLeftOverPerRT-int64(memBuffer))<<20, replicas, false)

			if extraX > calculatedX {
				extraX = calculatedX
			}
			lmrRTAllocatedCount[resId]--
		}

		if extraX > 0 {
			// When setting WS_RT_SET_LMR_X, we take estimate - 1
			extraX -= 1
		}
		upperBoundX := extraX + lmc.Global.MinX
		replicaTypeToGrantBoundResult[replicaType] = common.EnsureMap(replicaTypeToGrantBoundResult[replicaType])
		replicaTypeToGrantBoundResult[replicaType][resource.AuxiliaryLMRSubresource] = int(upperBoundX)
		logrus.Infof("Calculated extra X: %d, selected upperbound X: %d", extraX, upperBoundX)

		for resId, grants := range nodeAssignedGrantsInType[replicaType] {
			replicas := getReplicas(grants)
			// Calculate total extra memory allocation including replica buffer overhead
			memBuffer = wsapisv1.LMRReplicaMemoryBufferMiB * len(replicas)
			extraMemory := int(common.GetPreciseMiBFromBytes(lmc.CalculateMemSum(extraX, replicas, false))) + memBuffer
			aggrRemainingJobMemory -= extraMemory
			currentAllocations[resId].Subresources[memSubResType] += extraMemory
		}
	}
}

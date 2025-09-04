package ws

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
)

const PerfMayBeDegraded = "Performance may be degraded"
const AxSpineLoadMessage = "%d times from %d total AX->WSE connection(s) over %d node(s)"

// semanticVersionCheckAnnotation emits a log message and Kubernetes event if job/server version inconsistent.
// todo: add test coverage
func (r *WSJobReconciler) semanticVersionCheckAnnotation(wsjob *wsapisv1.WSJob) {
	jobImage := wscommon.GetWsjobImageVersion(wsjob)
	wsjob.Annotations[wscommon.JobOperatorVersion] = wscommon.CerebrasVersion
	// We moved the job operator semantic version setting to cluster server in cluster release 3.0.0.
	// TODO: Remove this in cluster release 3.2.0
	if wsjob.Annotations[wsapisv1.SemanticJobOperatorVersion] == "" {
		wsjob.Annotations[wsapisv1.SemanticJobOperatorVersion] = wscommon.SemanticVersion
	}
	clientSemver, _ := semver.NewVersion(wsjob.Annotations[wsapisv1.SemanticApplianceClientVersion])
	serverSemver, _ := semver.NewVersion(wsjob.Annotations[wsapisv1.SemanticClusterServerVersion])
	operatorSemver, _ := semver.NewVersion(wsjob.Annotations[wsapisv1.SemanticJobOperatorVersion])

	// client/server compatibility check
	if clientSemver != nil && serverSemver != nil {
		// TODO: Relax this check when we decide to discourage the usage of disabling version check across the board
		if !serverSemver.Equal(*clientSemver) {
			msg := fmt.Sprintf("Warning: client semantic version %s is inconsistent with cluster server semantic version %s, "+
				"there's a risk job could fail due to inconsistent setup.", clientSemver.String(), serverSemver.String())
			r.Log.Info(msg, "job", wsjob.NamespacedName())
			r.Recorder.Event(wsjob, corev1.EventTypeWarning, wscommon.InconsistentVersionEventReason, msg)
		}
	} else if serverImage, ok := wsjob.Annotations[wscommon.ClusterServerVersion]; ok {
		// TODO: Remove this in cluster release 3.2.0
		if !strings.Contains(
			// for release version, they should match exactly
			// for non-release, server version can be "0.0.0-user+7e5c99624e" while job version can be "build-7e5c99624e"
			strings.ReplaceAll(serverImage, "+", "-"),
			strings.ReplaceAll(jobImage, "build-", "")) {
			msg := fmt.Sprintf("Warning: job image version %s is inconsistent with cluster server version %s, "+
				"there's a risk job could fail due to inconsistent setup.", jobImage, serverImage)
			r.Log.Info(msg, "job", wsjob.NamespacedName())
			r.Recorder.Event(wsjob, corev1.EventTypeWarning, wscommon.InconsistentVersionEventReason, msg)
		}
	}

	// server/operator compatibility check
	if serverSemver != nil && operatorSemver != nil {
		if operatorSemver.LessThan(*serverSemver) {
			msg := fmt.Sprintf("Warning: job operator semantic version %s is behind cluster server semantic version %s in session '%s', "+
				"there's a risk job could fail due to job operator not upgraded.",
				operatorSemver.String(), serverSemver.String(), wsjob.Namespace)
			r.Log.Info(msg, "job", wsjob.NamespacedName())
		}
	}
}

// add annotation on br-system affinity/spine traffic info.
func (r *WSJobReconciler) addBRV2AffinityAnnotation(wsjob *wsapisv1.WSJob, j jobResources) {
	if !wsapisv1.IsV2Network || len(j.systems) <= 1 || wsjob.SkipBR() {
		return
	}

	// check if systems are within the same group
	checkSystemGroupAffinity := func(systems []string) {
		systemGroups := map[string]bool{}
		for _, name := range systems {
			if sys, ok := r.Cfg.SystemMap[name]; ok {
				systemGroups[sys.GetGroupName()] = true
			}
		}
		if len(systemGroups) != 1 {
			r.Log.Info("cross stamp systems found", "systems", systemGroups)
		}
	}

	// check if BRs are in affinity to systems/brs
	checkBrAffinity := func(portCount int, egressRes []string, egressBrIds []int, brId int, currentSx *wscommon.Node) int {
		crossSpine := 0
		for i, egress := range egressRes {
			if r.Cfg.SystemMap[egress] != nil {
				if _, ok := currentSx.Properties[systemv1.GetSysPortAffinityKey(egress, brId%portCount)]; !ok {
					r.Log.Info(fmt.Sprintf(
						"cross spine traffic incurred due to assigned br %d on node %s "+
							"does not have affinity with system %s port %d, wsjob %s",
						brId, currentSx.Name, egress, brId%wscommon.CSVersionSpec.NumPorts, wsjob.NamespacedName()))
					crossSpine++
				}
			} else if r.Cfg.NodeMap[egress] != nil {
				currentV2 := currentSx.GetV2Group()
				egressV2 := r.Cfg.NodeMap[egress].GetV2Group()
				if currentV2 != egressV2 {
					r.Log.Info(fmt.Sprintf(
						"cross spine traffic incurred due to assigned br %d on node %s v2Group %s "+
							"does not have same v2Group with br %d on node %s v2Group %s, wsjob %s",
						brId, currentSx.Name, currentV2, egressBrIds[i], egress, egressV2, wsjob.NamespacedName()))
					crossSpine++
				}
			}
		}
		return crossSpine
	}

	getAffinity := func() (int, int) {
		var crossSpineSystems int
		var crossSpineBrs int
		treeVal := j.replicaToAnnoMap[rlv1.BrRequestName][rlv1.BrLogicalTreeKey]
		var brs wscommon.BrLogicalTree
		if err := json.Unmarshal([]byte(treeVal), &brs); err != nil {
			r.Log.Error(err, "fail to parse br logical tree",
				"job", wsjob.NamespacedName(), "treeVal", treeVal)
			return crossSpineSystems, crossSpineBrs
		}

		// key is replica id, val is br node id in br config
		for _, id := range j.replicaToBrNodeIdMap {
			nodeName := j.replicaToNodeMap[rlv1.BrRequestName][id]
			node := r.Cfg.NodeMap[nodeName]
			if node == nil {
				r.Log.Info("unexpected nil node in affinity check", "node", nodeName)
				return crossSpineSystems, crossSpineBrs
			}

			portCount := wscommon.CSVersionSpec.NumPorts

			setId := id / portCount
			portId := id % portCount
			var egressRes []string
			var egressBrIds []int
			if brs[setId].Layer == 1 {
				for _, egr := range brs[setId].Egresses {
					egressRes = append(egressRes, egr.SystemName)
				}
				checkSystemGroupAffinity(egressRes)
				crossSpineSystems += checkBrAffinity(portCount, egressRes, egressBrIds, id, node)
			} else {
				for _, egr := range brs[setId].Egresses {
					egrBrId := egr.BrSetId + portId
					sxNodeName := j.replicaToNodeMap[rlv1.BrRequestName][egrBrId]
					egressRes = append(egressRes, sxNodeName)
					egressBrIds = append(egressBrIds, egrBrId)
				}
				crossSpineBrs += checkBrAffinity(portCount, egressRes, egressBrIds, id, node)
			}
		}
		return crossSpineSystems, crossSpineBrs
	}

	if crossSpineSystems, crossSpineBrs := getAffinity(); crossSpineSystems != 0 || crossSpineBrs != 0 {
		wscommon.SpineLoadMetricAdded(wsjob.Namespace, wsjob.Name, "brToSystem", crossSpineSystems)
		wscommon.SpineLoadMetricAdded(wsjob.Namespace, wsjob.Name, "brToBR", crossSpineBrs)
		msg := fmt.Sprintf(
			"%d times from BR to systems, %d times from BR to BR",
			crossSpineSystems, crossSpineBrs)
		wsjob.Annotations[wscommon.BrV2SpineLoad] = msg
		r.Log.Info(msg, "job", wsjob.NamespacedName())
		return
	}
	r.Log.Info("BR affinity met", "job", wsjob.NamespacedName())
}

func (r *WSJobReconciler) addWioV2AffinityAnnotation(wsjob *wsapisv1.WSJob, lock *rlv1.ResourceLock, j jobResources) {
	numOneHopTraffic, numMultiHopTraffic := wscommon.GetV2WioPlacementQuality(r.Cfg, lock)
	axNodes := wscommon.Unique(wscommon.Values(j.replicaToNodeMap[rlv1.ActivationRequestName]))
	if numMultiHopTraffic > 0 || wsjob.IsExecute() {
		wsjob.Annotations[wscommon.ActV2SpineLoad] = fmt.Sprintf(AxSpineLoadMessage, numMultiHopTraffic, numOneHopTraffic+numMultiHopTraffic, len(axNodes))
		wscommon.SpineLoadMetricAdded(wsjob.Namespace, wsjob.Name, "ax-wse-connection", numMultiHopTraffic)
		wscommon.NonSpineLoadMetricAdded(wsjob.Namespace, wsjob.Name, "ax-wse-connection", numOneHopTraffic)
	}
}

// add system port failure handling annotation if detected
func (r *WSJobReconciler) addPartialSystemsAnnotation(wsjob *wsapisv1.WSJob, j jobResources) {
	partialSystemAssigned := false
	// annotate as partial mode if pod id inconsistent with br id
	for id, v := range j.replicaToBrNodeIdMap {
		if id != v {
			partialSystemAssigned = true
			break
		}
	}
	var partialSystemList []string
	// get unhealthy system granted info
	// also required for single system job case without br
	for _, sys := range j.systems {
		if _, ok := r.Cfg.UnhealthySystems[sys]; ok {
			partialSystemAssigned = true
			partialSystemList = append(partialSystemList, sys)
		}
	}
	sort.Strings(partialSystemList)

	if partialSystemAssigned {
		wsjob.Annotations[wscommon.PartialSystemAnnotation] = fmt.Sprintf("%v", partialSystemList)
		msg := fmt.Sprintf("Assigned System/BR(s) %+v have ports down. %s.", partialSystemList, PerfMayBeDegraded)
		r.Log.Info(msg, "job", wsjob.NamespacedName())
		r.Recorder.Event(wsjob, corev1.EventTypeWarning, wscommon.PartialSystemAssignedEventReason, msg)
	}
}

// addSecondaryDataNicAnnotation emits a log message and Kubernetes event if there are nodes requiring but
// missing secondary data NICs.
func (r *WSJobReconciler) addSecondaryDataNicAnnotation(wsjob *wsapisv1.WSJob, j jobResources) {
	if _, ok := wsjob.Annotations[MemxSecondaryNicAnnotationKey]; ok {
		return
	}

	if wscommon.DisableSecondaryDataNic {
		wsjob.Annotations[MemxSecondaryNicAnnotationKey] = "disabled"
		logrus.Infof("Secondary nics not enabled in %s", wsjob.Name)
		return
	}

	// We only enable the secondary NIC for activations on depopulated nodegroups, and
	// we should at least see X ("nodesSeenThreshold") activations be scheduled to the same node.
	nodesSeenThreshold := 2
	nodesSeen := map[string]int{}
	var depopNodesQualified []string
	var degradedNodes []string
	for _, nodeName := range j.replicaToNodeMap[rlv1.ActivationRequestName] {
		no := r.Cfg.NodeMap[nodeName]
		groupName := no.GetGroup()
		if g, ok := r.Cfg.Resources.GetGroup(groupName); !ok || g.IsPopulated() {
			continue
		}

		nodesSeen[nodeName] += 1
		if nodesSeen[nodeName] < nodesSeenThreshold {
			continue
		}

		depopNodesQualified = append(depopNodesQualified, nodeName)
		if len(no.NICs) < 2 || no.NICs[1].HasError {
			degradedNodes = append(degradedNodes, nodeName)
		}
	}

	sort.Strings(depopNodesQualified)
	sort.Strings(degradedNodes)
	logrus.Infof("Node(s) requiring secondary data nics: %+v, degraded node(s): %+v", depopNodesQualified, degradedNodes)
	if len(depopNodesQualified) == len(degradedNodes) {
		if len(depopNodesQualified) == 0 {
			wsjob.Annotations[MemxSecondaryNicAnnotationKey] = "none-used"
		} else {
			wsjob.Annotations[MemxSecondaryNicAnnotationKey] = "degraded"
			msg := fmt.Sprintf("All %d assigned MemoryX node(s) in depopulated nodegroup(s) "+
				"preferred using second NIC but do not have it enabled. %s.",
				len(depopNodesQualified), PerfMayBeDegraded)
			r.Log.Info(msg, "job", wsjob.NamespacedName())
		}
		return
	}

	if len(degradedNodes) == 0 {
		wsjob.Annotations[MemxSecondaryNicAnnotationKey] = "fully-used"
	} else {
		wsjob.Annotations[MemxSecondaryNicAnnotationKey] = "degraded"
		msg := fmt.Sprintf("%d out of %d assigned MemoryX node(s) in depopulated nodegroup(s) "+
			"preferred using second NIC but do not have it enabled. %s.",
			len(degradedNodes), len(depopNodesQualified), PerfMayBeDegraded)
		r.Log.Info(msg, "job", wsjob.NamespacedName())
		r.Recorder.Event(wsjob, corev1.EventTypeWarning, wscommon.DegradedSchedulingJobEventReason, msg)
	}
}

func (r *WSJobReconciler) addNvmfGrantAnnotations(wsjob *wsapisv1.WSJob, j jobResources) {
	grants := map[string]int{}
	for _, g := range j.nvmfGrants {
		devicePath := strings.Split(g, ":")[0]
		grants[devicePath] += 1
	}
	if len(grants) > 0 {
		wsjob.Annotations[wscommon.NvmfGrants] = strings.Join(j.nvmfGrants, ",")
		var notes []string
		minLoad := len(j.nvmfGrants)
		maxLoad := 0
		for _, k := range wscommon.SortedKeys(grants) {
			notes = append(notes, fmt.Sprintf("%d grant(s) on %s", grants[k], k))
			minLoad = wscommon.Min(minLoad, grants[k])
			maxLoad = wscommon.Max(maxLoad, grants[k])
		}
		wsjob.Annotations[wscommon.NvmfGrantNotes] = strings.Join(notes, ", ")

		if len(grants) < wsresource.NvmfDiskCountPerCoordNode {
			msg := fmt.Sprintf("%d out of %d NVMe disks were used. %s.", len(grants), wsresource.NvmfDiskCountPerCoordNode, PerfMayBeDegraded)
			r.Log.Info(msg, "job", wsjob.NamespacedName())
			r.Recorder.Event(wsjob, corev1.EventTypeWarning, wscommon.DegradedSchedulingJobEventReason, msg)
		}

		// We do not alert users if loads are off-by-1
		// For example, if we had 4 weights scheduled to 3 disks - the load will be (2,1,1) and users won't see a warning event
		if minLoad+1 < maxLoad {
			msg := fmt.Sprintf("NVMe disk grants were unbalanced - %s. %s.", wsjob.Annotations[wscommon.NvmfGrantNotes], PerfMayBeDegraded)
			r.Log.Info(msg, "job", wsjob.NamespacedName())
			r.Recorder.Event(wsjob, corev1.EventTypeWarning, wscommon.DegradedSchedulingJobEventReason, msg)
		}
	}
}

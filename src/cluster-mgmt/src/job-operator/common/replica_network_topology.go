package common

import (
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	"cerebras.com/job-operator/common/resource"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

type ReplicaNetworkTopology struct {
	JobName        string
	JobNamespace   string
	ReplicaType    string
	NodeName       string
	NicNames       string
	PreferredPorts string
	TargetPorts    string
	TargetSystems  string
	ReplicaIdx     string

	// BR to BR peer connection properties
	BrSetId       string
	TargetBrSetId string

	NumOneHopTraffic   int
	NumMultiHopTraffic int
}

func (s *ReplicaNetworkTopology) canEvaluateConnectionQuality() bool {
	return s.TargetSystems != "" && s.TargetPorts != "" && s.NicNames != ""
}

func (s *ReplicaNetworkTopology) EvaluateNetworkTopologyQuality(clusterCfg *ClusterConfig, systemNames []string, emitMetrics bool) {
	no, ok := clusterCfg.NodeMap[s.NodeName]
	if !ok {
		// this should not happen
		return
	}

	replicaIdx, err := strconv.Atoi(s.ReplicaIdx)
	if err != nil {
		// this should not happen
		return
	}

	replicaName := GetReplicaName(s.ReplicaType, replicaIdx)
	v2Group := clusterCfg.NodeMap[s.NodeName].GetGroup()

	if !s.canEvaluateConnectionQuality() {
		if emitMetrics {
			// There's still point in emitting just the nic usage without the target resource/port/leafswitch
			PodNetworkTopologyMetricAdded(s.JobNamespace, s.JobName, replicaName, s.NodeName,
				s.NicNames, v2Group, s.PreferredPorts, "", "", "",
				s.BrSetId, s.TargetBrSetId, s.TargetPorts, false)
		}
		return
	}

	// For ACT/KVSS, there's only 1 target nic per pod, for BR, all nics have the same affinity
	// thus taking the first one as targetNic would work.
	targetNicName := strings.Split(s.NicNames, ",")[0]
	v2Group = clusterCfg.NodeMap[s.NodeName].GetV2GroupForNodeNic(targetNicName)

	var sysNames, targetPorts, targetV2Groups []string

	for _, systemIdStr := range strings.Split(s.TargetSystems, ",") {
		systemId, err := strconv.Atoi(systemIdStr)
		if err != nil || systemId < 0 || systemId >= len(systemNames) {
			// this should not happen
			continue
		}
		systemName := systemNames[systemId]
		sysNames = AppendUnique(sysNames, systemName)

		for _, csPortStr := range strings.Split(s.TargetPorts, ",") {
			csPort, err := strconv.Atoi(csPortStr)
			if err != nil || csPort < 0 || csPort >= 12 {
				// this should not happen
				continue
			}
			affinityKey := systemv1.GetSysPortAffinityKey(systemName, csPort)
			targetV2Group := clusterCfg.GetV2GroupFromAffinityKey(affinityKey)
			isOneHopTraffic := false
			systemPort := clusterCfg.GetSystemDataPortName(systemName, csPort)
			if systemPort == "" {
				systemPort = csPortStr
			}

			for _, nic := range no.NICs {
				if nic.Name != targetNicName {
					continue
				}
				val, ok := nic.Properties[affinityKey]
				val = strings.TrimPrefix(val, resource.NodeNicSubresource+"/")
				isOneHopTraffic = (targetV2Group == v2Group) || (ok && (val == "" || val == s.NicNames))
				if isOneHopTraffic {
					s.NumOneHopTraffic++
				} else {
					s.NumMultiHopTraffic++
				}

				logrus.Debugf("%s %s-%d (%s/%s -> %s:%d): affinity %s",
					s.JobName, s.ReplicaType, replicaIdx, s.NodeName, s.NicNames, systemNames[systemId], csPort,
					Ternary(isOneHopTraffic, "met", "not met"))
			}
			targetPorts = AppendUnique(targetPorts, systemPort)
			targetV2Groups = AppendUnique(targetV2Groups, targetV2Group)
		}
	}

	if emitMetrics {
		isOneHop := s.NumMultiHopTraffic == 0 && s.NumOneHopTraffic > 0
		PodNetworkTopologyMetricAdded(s.JobNamespace, s.JobName, replicaName, s.NodeName,
			s.NicNames, v2Group, s.PreferredPorts,
			strings.Join(sysNames, ","),
			strings.Join(targetPorts, ","),
			strings.Join(targetV2Groups, ","),
			s.BrSetId, s.TargetBrSetId, s.TargetPorts,
			isOneHop)
	}
}

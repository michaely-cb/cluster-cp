package csctl

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/common/model"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/server/pkg"

	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"

	log "github.com/sirupsen/logrus"
)

// GetClusterTopology returns information about the cluster network topology
func (s *Server) GetClusterTopology(ctx context.Context, req *csctlpb.GetClusterTopologyRequest) (*csctlpb.GetClusterTopologyResponse, error) {
	if s.configProvider == nil {
		return nil, grpcStatus.Errorf(codes.Internal, "cluster configuration provider is not available")
	}
	currentNamespace := pkg.GetNamespaceFromContext(ctx)

	cfg, err := s.configProvider.GetCfg(ctx)
	if err != nil {
		return nil, grpcStatus.Errorf(codes.Internal, "failed to get cluster configuration: %v", err)
	}
	if cfg == nil {
		return nil, grpcStatus.Errorf(codes.Internal, "cluster configuration is not available")
	}

	connections := generateSwitchPortConnections(cfg, currentNamespace, req.SwitchOnly)

	return &csctlpb.GetClusterTopologyResponse{
		Connections: connections,
	}, nil
}

func (s *Server) GetJobTopology(ctx context.Context, req *csctlpb.GetJobTopologyRequest) (*csctlpb.GetJobTopologyResponse, error) {
	// Validate request
	if req.JobId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "job id is required")
	}
	if req.JobNamespace == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "job namespace is required")
	}

	// Query Prometheus for pod_port_affinity metrics
	result, err := s.metricsQuerier.QueryPodPortAffinity(ctx, req.JobId, req.JobNamespace)
	if err != nil {
		log.Errorf("Failed to query pod_port_affinity metrics for job_id=%s, namespace=%s: %v", req.JobId, req.JobNamespace, err)
		return nil, grpcStatus.Error(codes.Internal, "failed to query metrics")
	}

	vector, ok := result.(model.Vector)
	if !ok {
		log.Errorf("Unexpected result type from pod_port_affinity query: %T", result)
		return nil, grpcStatus.Error(codes.Internal, "unexpected query result type")
	}

	// Get br_set_topology metrics
	brPodToSetMap, brSetToPodMap, brErr := s.getBrSetMaps(ctx, req.JobId, req.JobNamespace)
	if brErr != nil {
		// Do not fail the execution if unable to get brPodToSetMap, continue with the network affinity data
		log.Warnf("Failed to query br_set_topology metrics for job_id=%s, namespace=%s: %v", req.JobId, req.JobNamespace, err)
		brPodToSetMap = make(map[string]*model.Sample)
	}

	connections := make([]*csctlpb.JobTopologyConnection, 0, len(vector))

	for _, sample := range vector {
		connections = append(connections, metricToConnection(sample, brPodToSetMap, brSetToPodMap))
	}

	return &csctlpb.GetJobTopologyResponse{
		Connections: connections,
	}, nil
}

func (s *Server) getBrSetMaps(ctx context.Context, jobId, jobNamespace string) (brPodToSetMap map[string]*model.Sample,
	brSetToPodMap map[string]map[string][]string,
	err error) {

	result, err := s.metricsQuerier.QueryBrSetTopology(ctx, jobId, jobNamespace)
	if err != nil {
		return nil, nil, err
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return nil, nil, err
	}

	brPodToSetMap = make(map[string]*model.Sample)
	for _, sample := range vector {
		podId := string(sample.Metric["pod_id"])
		brPodToSetMap[podId] = sample
	}

	// build brSetToPod map for reverse lookups, this will be set -> port -> pod structure
	brSetToPodMap = make(map[string]map[string][]string)

	for podId, sample := range brPodToSetMap {
		brSetId := string(sample.Metric["br_set_id"])
		brPort := string(sample.Metric["target_port"])
		if brSetId != "" {
			brSetToPodMap[brSetId] = wscommon.EnsureMap(brSetToPodMap[brSetId])
			brSetToPodMap[brSetId][brPort] = append(brSetToPodMap[brSetId][brPort], podId)
		}
	}

	return brPodToSetMap, brSetToPodMap, nil
}

// generateSwitchPortConnections creates SwitchPortConnection objects from ClusterConfig
func generateSwitchPortConnections(cfg *wscommon.ClusterConfig,
	currentNamespace string,
	switchOnly bool) []*csctlpb.SwitchPortConnection {
	var switchOnlyConnections []*csctlpb.SwitchPortConnection
	var fullConnections []*csctlpb.SwitchPortConnection

	// for V2 network, naturally we use V2GroupMap for system-switch connection info.
	// for V1 network, we purposely populated V2Group info for V1, so get info from it as well.
	for switchName, switchInfo := range cfg.V2GroupMap {
		preferredPorts := getV2NetworkAffinedPorts(cfg, switchName)
		stampId := getStampId(switchInfo.Properties)

		if len(preferredPorts) > 0 {
			// hide switch with no affinity in switch-only view
			switchOnlyConnections = append(switchOnlyConnections, &csctlpb.SwitchPortConnection{
				StampId:          stampId,
				SwitchName:       switchName,
				PreferredCsPorts: preferredPorts,
			})
		}

		// From the same switch, we aggregate different ports to the same system to be in one line.
		// e.g. aggPorts["sys-1"] = []int{1, 5, 9}
		aggPorts := map[string][]int{}
		// Look for system connections in switch properties (format: port-affinity/systemName-portNumber)
		for propKey := range switchInfo.Properties {
			if sysName, portNum := parseSystemConnection(propKey); sysName != "" {
				if currentNamespace != wscommon.SystemNamespace {
					// if the targeted namespace is not system namespace,
					// we also only show systems assigned to user namespace
					if cfg.GetSystemAssignedNs(sysName) != currentNamespace {
						continue
					}
				}

				if _, exists := cfg.SystemMap[sysName]; exists {
					aggPorts[sysName] = append(aggPorts[sysName], portNum)
				} else {
					log.Warnf("sysName %s from propKey %v not found in systemMap: %v", sysName, propKey, cfg.SystemMap)
				}
			}
		}

		for sysName, ports := range aggPorts {
			sort.Slice(ports, func(i, j int) bool { return ports[i] < ports[j] })
			var peerPorts []string
			for _, port := range ports {
				peerPorts = append(peerPorts, fmt.Sprintf("n%.2d", port))
			}
			fullConnections = append(fullConnections, &csctlpb.SwitchPortConnection{
				StampId:          stampId,
				SwitchName:       switchName,
				PreferredCsPorts: preferredPorts,
				PeerName:         sysName,
				PeerPort:         strings.Join(peerPorts, ","),
			})
		}
	}

	isV2Network := cfg.Properties[resource.TopologyPropertyKey] == "v2"
	// Process nodes and their connections
	for nodeName, node := range cfg.NodeMap {
		if currentNamespace != wscommon.SystemNamespace {
			// if the targeted namespace is not system namespace,
			// we also only show nodes assigned to user namespace
			if !cfg.IsNodeInNamespace(currentNamespace, nodeName) {
				continue
			}
		}
		var stampId string
		if isV2Network {
			// Management, Coordinator, Memory, and Worker nodes are not stamp aware
			if node.Role == wscommon.RoleManagement ||
				node.Role == wscommon.RoleCoordinator ||
				node.Role == wscommon.RoleMemory ||
				node.Role == wscommon.RoleWorker {
				stampId = resource.NonApplicableField
			} else {
				stampId = getStampId(node.Properties)
			}
		} else {
			// Default stamp id "0" for all nodes in v1 networks
			stampId = getStampId(node.Properties)
		}

		// For a node with NICs, we aggregate different NICs in one line, if NICs connect to the same switch and
		// have the same port affinity.
		// e.g. aggNics["net001-lf-sw01/0,4,8"] = []string{"eth400g0", "eth400g1"}
		//      aggNics["net004-lf-sw02/-"] = []string{"ens2f0np0", "ens2f1np1"}
		aggNics := map[string][]string{}
		for _, nic := range node.NICs {
			var switchName string
			if isV2Network {
				switchName = getV2SwitchName(node, nic)
			} else {
				// For nodes in nodegroups (e.g. WRK, MEMX, MGMT), switch name can be acquired from nodegroup
				switchName = getV1SwitchName(cfg, node.GetGroup())
				if switchName == "" {
					// If not present, try getting it from v2Group. for SwarmX nodes, v2Group should be available at
					// NIC level; otherwise it should be available at node level.
					switchName = getV2SwitchName(node, nic)
				}
			}
			if switchName == "" {
				// Defensive check — this should never happen
				switchName = resource.NonApplicableField
			}

			var csPorts []string
			if isV2Network {
				csPorts = getV2NetworkAffinedPorts(cfg, switchName) // ports are already sorted
			} else {
				csPorts = getV1NetworkAffinedPorts(nic)
			}
			csPortsString := strings.Join(csPorts, ",")
			if csPortsString == "" {
				csPortsString = resource.NonApplicableField
			}
			aggKey := switchName + "/" + csPortsString
			aggNics[aggKey] = append(aggNics[aggKey], nic.Name)
		}

		for key, nics := range aggNics {
			parts := strings.Split(key, "/")
			if len(parts) != 2 {
				log.Warnf("internal error during aggregating connection for node %s, key %s", node.Name, key)
				continue
			}

			switchName := parts[0]
			csPorts := strings.Split(parts[1], ",") // if parts[1] == "-", then []string{"-"}
			nicsName := strings.Join(nics, ",")
			fullConnections = append(fullConnections, &csctlpb.SwitchPortConnection{
				StampId:          stampId,
				SwitchName:       switchName,
				PreferredCsPorts: csPorts,
				PeerName:         nodeName,
				PeerPort:         nicsName,
			})
		}
	}

	// Return the appropriate view based on switchOnly parameter
	if switchOnly {
		// lexicographic order {StampId, SwitchName}
		sort.Slice(switchOnlyConnections, func(i, j int) bool {
			if switchOnlyConnections[i].StampId != switchOnlyConnections[j].StampId {
				return switchOnlyConnections[i].StampId < switchOnlyConnections[j].StampId
			}
			return switchOnlyConnections[i].SwitchName < switchOnlyConnections[j].SwitchName
		})
		return switchOnlyConnections
	}
	// lexicographic order {StampId ("-" as last), SwitchName, (System before Node), PeerName}
	sort.Slice(fullConnections, func(i, j int) bool {
		a := fullConnections[i]
		b := fullConnections[j]

		// Special handling for "-" StampId
		if a.StampId == resource.NonApplicableField && b.StampId != resource.NonApplicableField {
			return false // a comes after
		}
		if a.StampId != resource.NonApplicableField && b.StampId == resource.NonApplicableField {
			return true // a comes before
		}
		if a.StampId != b.StampId {
			return a.StampId < b.StampId
		}

		// 2. If StampId same, sort by SwitchName
		if a.SwitchName != b.SwitchName {
			return a.SwitchName < b.SwitchName
		}

		_, aIsSystem := cfg.SystemMap[a.PeerName]
		_, bIsSystem := cfg.SystemMap[b.PeerName]
		// 3. systems comes before nodes
		if aIsSystem != bIsSystem {
			return aIsSystem
		}
		// 4. If both same, sort by PeerName
		return a.PeerName < b.PeerName
	})
	return fullConnections
}

// getStampId retrieves the stamp id from properties or returns the default (0)
func getStampId(properties map[string]string) string {
	if idx, ok := properties[resource.StampPropertyKey]; ok && idx != "" {
		return idx
	}
	return "0" // Default stamp id for v1 networks
}

// getV2SwitchName gets the V2Group name for v2 networks
func getV2SwitchName(node *wscommon.Node, nic *wscommon.NetworkInterface) string {
	if nic.V2Group != "" {
		return nic.V2Group
	}
	return node.Properties[resource.V2GroupPropertyKey]
}

// getV1SwitchName gets the switch name for v1 networks from the group
func getV1SwitchName(cfg *wscommon.ClusterConfig, groupName string) string {
	if group, ok := cfg.GroupMap[groupName]; ok {
		// In v1 networks, the switch name is in the switch property
		return group.Properties[resource.SwitchPropertyKey]
	}

	return ""
}

// getV1NetworkAffinedPorts gets the preferred CS ports for a NIC in v1 network
func getV1NetworkAffinedPorts(nic *wscommon.NetworkInterface) []string {
	// In v1 networks, just use the explicit CS port from the NIC
	if nic.CsPort != nil {
		return []string{strconv.Itoa(*nic.CsPort)}
	}
	return []string{}
}

// getV2NetworkAffinedPorts gets the preferred CS ports for a V2Group by name
func getV2NetworkAffinedPorts(cfg *wscommon.ClusterConfig, v2GroupName string) []string {
	if v2Group, ok := cfg.V2GroupMap[v2GroupName]; ok {
		if portsStr, ok := v2Group.Properties[resource.PreferredCsPortsPropKey]; ok && portsStr != "" {
			return strings.Split(portsStr, ",")
		}
	}
	return []string{}
}

// parseSystemConnection parses a system connection string in the format "port-affinity/systemName-portNumber"
func parseSystemConnection(propKey string) (string, int) {
	propKey = strings.TrimPrefix(propKey, resource.PortAffinityPropKeyPrefix+"/")
	parts := strings.Split(propKey, "-")
	if len(parts) >= 2 {
		sysName := strings.Join(parts[:len(parts)-1], "-")
		portNum, err := strconv.Atoi(parts[len(parts)-1])
		if err == nil {
			return sysName, portNum
		}
	}
	return "", 0
}

// metricToConnection converts metric sample to connection
func metricToConnection(sample *model.Sample,
	brPodToSetMap map[string]*model.Sample,
	brSetToPods map[string]map[string][]string) *csctlpb.JobTopologyConnection {

	conn := &csctlpb.JobTopologyConnection{
		PodId:                string(sample.Metric["pod_id"]),
		NodeName:             string(sample.Metric["node_name"]),
		Nics:                 string(sample.Metric["nic_names"]),
		NodeLeafSwitch:       string(sample.Metric["v2_groups"]),
		SystemPorts:          string(sample.Metric["target_ports"]),
		PodPreferredCsPorts:  string(sample.Metric["preferred_ports"]),
		SystemAffinitySwitch: string(sample.Metric["target_v2_groups"]),
	}

	var egressTargets []string
	if string(sample.Metric["target_resources"]) != "" {
		egressTargets = append(egressTargets, string(sample.Metric["target_resources"]))
	}

	// Add BR info if available
	if brSample, exists := brPodToSetMap[conn.PodId]; exists {
		egressTargets = append(egressTargets, resolveBrEgressTargets(brSample, brSetToPods)...)
	}
	conn.EgressTargets = strings.Join(egressTargets, ",")

	return conn
}

// resolveBrEgressTargets converts target BR set IDs to a comma-separated string of target pod IDs
func resolveBrEgressTargets(brSample *model.Sample, brSetToPods map[string]map[string][]string) []string {
	sourceBrSetId := string(brSample.Metric["br_set_id"])
	targetBrSetIds := string(brSample.Metric["target_br_set_ids"])
	brPort := string(brSample.Metric["target_port"])

	if targetBrSetIds == "" {
		return []string{}
	}
	brSetIds := strings.Split(targetBrSetIds, ",")
	targetPods := wscommon.NewSet[string]()

	for _, brSetId := range brSetIds {
		brSetId = strings.TrimSpace(brSetId)
		// Skip if this is the pod's own BR set ID
		if brSetId == sourceBrSetId {
			continue
		}

		if pods, ok := brSetToPods[brSetId][brPort]; ok {
			for _, podId := range pods {
				targetPods.Emplace(podId)
			}
		}
	}

	// Convert map keys to slice and sort for consistent output
	result := targetPods.ToSlice()
	sort.Slice(result, func(i, j int) bool {
		getNumericSuffix := func(podName string) int {
			parts := strings.Split(podName, "-")
			if len(parts) > 0 {
				if num, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
					return num
				}
			}
			return 0 // fallback for invalid format
		}
		return getNumericSuffix(result[i]) < getNumericSuffix(result[j])
	})
	return result
}

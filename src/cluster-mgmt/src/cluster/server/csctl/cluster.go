package csctl

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"

	"cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	rlapiv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	systemv1 "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
)

type NodeInfo struct {
	name        string
	session     string
	role        string
	version     string
	inUse       bool
	state       string
	notes       string
	cpuPctUse   float64
	memPctUse   float64
	cpuCapacity float64
	memCapacity int64
	cpuAlloc    float64
	memAlloc    int64
	jobIds      []string
	hostname    string
}

func (v *NodeInfo) Proto() proto.Message {
	return MapNodeInfo(v)
}

type NodeInfoList struct {
	items []*NodeInfo
}

func (v *NodeInfoList) Proto() proto.Message {
	return &csv1.NodeInfoList{Items: common.Map(MapNodeInfo, v.items)}
}

type ClusterStore struct {
	nodeLister     pkg.KubeNodeLister
	metricsQuerier common.MetricsQuerier
	sysClient      systemv1.SystemInterface
	lockInformer   rlinformer.GenericInformer
	wsJobInformer  wsinformer.GenericInformer
	cfgProvider    pkg.ClusterCfgProvider
}

func (v *ClusterStore) ListV2(ctx context.Context, namespace string, options *csctl.ListOptions) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "ListV2 not implemented for this type")
}

func (v *ClusterStore) Get(ctx context.Context, namespace, name string) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "Get not implemented for this type")
}

func (v *ClusterStore) List(ctx context.Context, namespace string, options *csctl.GetOptions) (Resource, error) {
	sysOnly := options != nil && options.SystemOnly
	nodeOnly := options != nil && options.NodeOnly
	errorOnly := options != nil && options.ErrorOnly

	// map of nodes/systems name
	nodeInfoMap := make(map[string]*NodeInfo)
	if !nodeOnly {
		listOption := metav1.ListOptions{}
		if namespace != common.SystemNamespace {
			listOption.LabelSelector = labels.Set{common.NamespaceKey: namespace}.String()
		}
		sysList, err := v.sysClient.List(ctx, listOption)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		for _, s := range sysList.Items {
			// filter sys name if needed
			if options != nil && options.System != "" && s.Name != options.System {
				continue
			}
			if options != nil && options.Role != "" && "system" != options.Role {
				continue
			}
			if options != nil && options.Version != "" && !platformVersionMatch(s.Labels[wsapisv1.PlatformVersionKey], options.Version) {
				continue
			}
			state := "ok"
			healthy, _, msg := common.GetSystemHealth(&s)
			if errorOnly && healthy {
				continue
			}
			if !healthy {
				state = "error"
			}

			hostname := s.Name
			if s.Spec.Hostname != "" {
				hostname = s.Spec.Hostname
			}

			nodeInfoMap[s.Name] = &NodeInfo{
				name:     s.Name,
				session:  s.Labels[common.NamespaceKey],
				role:     "system",
				version:  common.GetPlatformVersionAsString(s.Labels[wsapisv1.PlatformVersionKey]),
				inUse:    false,
				state:    state,
				notes:    msg,
				hostname: hostname,
			}
		}
		v.querySystemMetrics(ctx, nodeInfoMap)
	}

	if !sysOnly {
		cfg, err := v.cfgProvider.GetCfg(ctx)
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}

		var nodes []*v1.Node
		nodes, err = v.nodeLister.List()
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}

		nodeIpToNameMap := make(map[string]string)
		for _, n := range nodes {
			nodeNamespace := n.Labels[common.NamespaceLabelKey]
			if namespace != common.SystemNamespace {
				if !cfg.IsNodeInNamespace(namespace, n.Name) {
					continue
				}
			}
			if options != nil && options.Role != "" && getNodeMarketingRole(n) != options.Role {
				continue
			}
			if options != nil && options.Version != "" && !platformVersionMatch(n.Labels[wsapisv1.PlatformVersionKey], options.Version) {
				continue
			}
			var ip string
			if len(n.Status.Addresses) > 0 {
				ip = n.Status.Addresses[0].Address
			}
			// SW-137573, CGX-233, skip missing IP node
			if ip == "" {
				log.Warnf("node doesn't have ip assigned: %s, skip", n.Name)
				continue
			}

			healthy, _, msg := common.GetNodeHealth(n, cfg.NodeMap[n.Name])
			state := "ok"
			if !healthy {
				state = "error"
			}

			if errorOnly && healthy {
				continue
			}

			nodeIpToNameMap[ip] = n.Name
			memCap := int64(0)
			if m := n.Status.Capacity.Memory(); m != nil {
				memCap = m.Value()
			}
			cpuCap := float64(0)
			if c := n.Status.Capacity.Cpu(); c != nil {
				cpuCap = float64(c.MilliValue()) / 1000.0
			}
			nodeInfoMap[n.Name] = &NodeInfo{
				name:        n.Name,
				session:     nodeNamespace,
				role:        getNodeMarketingRole(n),
				memCapacity: memCap,
				cpuCapacity: cpuCap,
				inUse:       false,
				version:     common.GetPlatformVersionAsString(n.Labels[wsapisv1.PlatformVersionKey]),
				state:       state,
				notes:       msg,
				hostname:    n.Name,
			}
		}
		v.queryNodeMetrics(ctx, nodeInfoMap, nodeIpToNameMap)
	}

	// update node/system job id/labels
	if err := v.updateJobInfo(namespace, nodeInfoMap); err != nil {
		return nil, pkg.MapStatusError(err)
	}

	// Now make a final combined list
	nodeInfoList := common.Values(nodeInfoMap)
	sort.Slice(nodeInfoList, func(i, j int) bool {
		if nodeInfoList[i].role == "system" || nodeInfoList[j].role == "system" {
			if nodeInfoList[i].role == nodeInfoList[j].role {
				return nodeInfoList[i].name < nodeInfoList[j].name
			} else {
				return nodeInfoList[i].role == "system"
			}
		}
		if nodeInfoList[i].role != nodeInfoList[j].role {
			return nodeInfoList[i].role < nodeInfoList[j].role
		} else if nodeInfoList[i].session != nodeInfoList[j].session {
			return nodeInfoList[i].session < nodeInfoList[j].session
		} else if nodeInfoList[i].memPctUse != nodeInfoList[j].memPctUse {
			return nodeInfoList[i].memPctUse > nodeInfoList[j].memPctUse
		} else {
			return nodeInfoList[i].name < nodeInfoList[j].name
		}
	})
	return &NodeInfoList{items: nodeInfoList}, nil
}

func (v *ClusterStore) querySystemMetrics(ctx context.Context, nodeInfoMap map[string]*NodeInfo) {
	versions, _ := common.GetSystemVersions(ctx, v.metricsQuerier, common.Keys(nodeInfoMap), true, true)
	for s, version := range versions {
		if nodeInfoMap[s] == nil {
			continue
		}
		nodeInfoMap[s].version = version
	}
}

func (v *ClusterStore) queryNodeMetrics(ctx context.Context,
	nodeInfoMap map[string]*NodeInfo, nodeIpToNameMap map[string]string) {
	// Get stats from Prometheus
	wg := sync.WaitGroup{}
	wg.Add(4)
	var cpuPctUseRes, memPctUseRes model.Value
	var cpuAllocRes map[string]float64
	var memAllocRes map[string]int64
	go func() {
		defer wg.Done()
		var err error
		if cpuPctUseRes, err = v.metricsQuerier.QueryNodeCpuUsage(ctx); err != nil {
			log.Warnf("Unable to query prom cpu metrics: %s", err)
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if memPctUseRes, err = v.metricsQuerier.QueryNodeMemUsage(ctx); err != nil {
			log.Warnf("Unable to query prom mem metrics: %s", err)
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if cpuAllocRes, err = v.metricsQuerier.QueryNodeCpuAllocatable(ctx); err != nil {
			log.Warnf("Unable to query prom cpu allocatable metrics: %s", err)
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		if memAllocRes, err = v.metricsQuerier.QueryNodeMemAllocatable(ctx); err != nil {
			log.Warnf("Unable to query prom mem allocatable metrics: %s", err)
		}
	}()
	wg.Wait()
	if cpuPctUseRes != nil {
		for _, sample := range cpuPctUseRes.(model.Vector) {
			if sample == nil {
				continue
			}
			mIp, _, _ := net.SplitHostPort(string(sample.Metric["instance"]))
			mValue := float64(sample.Value)
			if name, ok := nodeIpToNameMap[mIp]; ok && nodeInfoMap[name] != nil {
				nodeInfoMap[name].cpuPctUse = mValue
			}
		}
	} else {
		for no := range nodeInfoMap {
			nodeInfoMap[no].cpuPctUse = -1
		}
	}
	if memPctUseRes != nil {
		for _, sample := range memPctUseRes.(model.Vector) {
			if sample == nil {
				continue
			}
			mIp, _, _ := net.SplitHostPort(string(sample.Metric["instance"]))
			mValue := float64(sample.Value)
			if name, ok := nodeIpToNameMap[mIp]; ok && nodeInfoMap[name] != nil {
				nodeInfoMap[name].memPctUse = mValue
			}
		}
	} else {
		for no := range nodeInfoMap {
			nodeInfoMap[no].memPctUse = -1
		}
	}
	if cpuAllocRes != nil {
		for node, cpuCores := range cpuAllocRes {
			if _, ok := nodeInfoMap[node]; ok {
				nodeInfoMap[node].cpuAlloc = cpuCores
			}
		}
	} else {
		for no := range nodeInfoMap {
			nodeInfoMap[no].cpuAlloc = -1
		}
	}
	if memAllocRes != nil {
		for node, memBytes := range memAllocRes {
			if _, ok := nodeInfoMap[node]; ok {
				nodeInfoMap[node].memAlloc = memBytes
			}
		}
	} else {
		for no := range nodeInfoMap {
			nodeInfoMap[no].memAlloc = -1
		}
	}
}

func (v *ClusterStore) updateJobInfo(namespace string, nodeInfoMap map[string]*NodeInfo) error {
	// get granted locks and systems
	var err error
	var locks []runtime.Object
	if namespace == common.SystemNamespace {
		locks, err = v.lockInformer.Lister().List(labels.NewSelector())
		if err != nil {
			return err
		}
	} else {
		locks, err = v.lockInformer.Lister().ByNamespace(namespace).List(labels.NewSelector())
		if err != nil {
			return err
		}
	}
	for _, l := range locks {
		lock := *l.(*rlapiv1.ResourceLock)
		if lock.IsGranted() {
			for _, g := range lock.Status.ResourceGrants {
				if err = v.getJobMetaFromGrants(lock, g, nodeInfoMap); err != nil {
					return err
				}
			}
			for _, gg := range lock.Status.GroupResourceGrants {
				for _, g := range gg.ResourceGrants {
					if err = v.getJobMetaFromGrants(lock, g, nodeInfoMap); err != nil {
						return err
					}
				}
			}
		}
	}
	// for session resource display, show SX with jobs running only since they are globally shared
	if namespace != common.SystemNamespace {
		for name, no := range nodeInfoMap {
			if len(no.jobIds) == 0 && no.role == "swarmx" {
				delete(nodeInfoMap, name)
			}
		}
	}
	return nil
}

func (v *ClusterStore) getJobMetaFromGrants(lock rlapiv1.ResourceLock,
	g rlapiv1.ResourceGrant, nodeInfoMap map[string]*NodeInfo) error {
	for _, res := range g.Resources {
		if _, ok := nodeInfoMap[res.Name]; ok {
			if g.Name == rlapiv1.SystemsRequestName {
				nodeInfoMap[res.Name].inUse = true
			}
			nodeInfoMap[res.Name].jobIds = append(nodeInfoMap[res.Name].jobIds, lock.NamespacedName())
		}
	}
	return nil
}

func (v *ClusterStore) PatchMerge(ctx context.Context, namespace, name string, patch []byte) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "patch not implemented for this type")
}

func (v *ClusterStore) AsTable(resource Resource) *csv1.Table {
	switch val := resource.(type) {
	case *NodeInfoList:
		table := emptyNodeInfoTablePb()
		table.Rows = common.Map(mapNodeInfoTableRow, common.Map(MapNodeInfo, val.items))
		return table
	default:
		panic(fmt.Sprintf("unhandled table conversion for type: %T", val))
	}
}

func emptyNodeInfoTablePb() *csv1.Table {
	return &csv1.Table{Columns: []*csv1.ColumnDefinition{
		{Name: "Name"},
		{Name: "Session"},
		{Name: "Hostname"},
		{Name: "Type"},
		{Name: "Version"},
		{Name: "CPU_ALLOCATABLE"},
		{Name: "MEM_ALLOCATABLE"},
		{Name: "CPU_USE"},
		{Name: "MEM_USE"},
		{Name: "JobIds"},
		{Name: "State"},
		{Name: "Notes"},
	}}
}

func MapNodeInfo(ni *NodeInfo) *csv1.NodeInfo {
	vol := &csv1.NodeInfo{
		Meta: &csv1.ObjectMeta{
			Type:       "cluster",
			Name:       ni.name,
			CreateTime: &timestamppb.Timestamp{},
			FieldPriority: map[string]int32{
				"meta.fieldPriority": 2,
				"meta.createTime":    2,
			},
		},
		Role:                ni.role,
		PlatformVersion:     ni.version,
		WseInUse:            ni.inUse,
		CpuLoadPercentage:   ni.cpuPctUse,
		MemoryUsePercentage: ni.memPctUse,
		CpuCapacityCores:    ni.cpuCapacity,
		MemoryCapacity:      fmtMemRound(ni.memCapacity),
		CpuAllocatableCores: ni.cpuAlloc,
		MemoryAllocatable:   fmtMemRound(ni.memAlloc),
		JobIds:              ni.jobIds,
		State:               ni.state,
		Notes:               ni.notes,
		Session:             ni.session,
		Hostname:            ni.hostname,
	}
	return vol
}

func mapNodeInfoTableRow(nodeInfo *csv1.NodeInfo) *csv1.RowData {
	var cpuCores, memBytes, cpuPct, memPct string
	if nodeInfo.Role == "system" {
		cpuCores = "n/a"
		memBytes = "n/a"
		cpuPct = "n/a"
		memPct = "n/a"
	} else {
		cpuCores = fmtCpuRound(nodeInfo.CpuAllocatableCores)
		memBytes = nodeInfo.MemoryAllocatable
		cpuPct = "unknown"
		if nodeInfo.CpuLoadPercentage >= 0 {
			cpuPct = fmt.Sprintf("%.2f", nodeInfo.CpuLoadPercentage) + "%"
		}
		memPct = "unknown"
		if nodeInfo.CpuLoadPercentage >= 0 {
			memPct = fmt.Sprintf("%.2f", nodeInfo.MemoryUsePercentage) + "%"
		}
	}
	ids := common.Unique(nodeInfo.JobIds)
	sort.Strings(ids)
	cells := []string{
		nodeInfo.Meta.Name,
		nodeInfo.Session,
		nodeInfo.Hostname,
		nodeInfo.Role,
		nodeInfo.PlatformVersion,
		cpuCores,
		memBytes,
		cpuPct,
		memPct,
		strings.Join(ids, ","),
		nodeInfo.State,
		nodeInfo.Notes,
	}
	return &csv1.RowData{Cells: cells}
}

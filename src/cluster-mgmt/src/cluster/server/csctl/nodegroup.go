package csctl

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	rlapiv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	sysv1 "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1"

	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
)

const (
	NodegroupType = "nodegroup"
	nodeRoleLabel = "k8s.cerebras.com/node-role-"
)

type nodegroup struct {
	name       string
	sampleNode string
	status     *csv1.NodeGroupStatus
}

func (n nodegroup) Proto() proto.Message {
	return &csv1.NodeGroup{
		Meta: &csv1.ObjectMeta{
			Type: NodegroupType,
			Name: n.name,
		},
		Status: n.status,
	}
}

type nodegroupList []nodegroup

func (n nodegroupList) Proto() proto.Message {
	ngs := make([]*csv1.NodeGroup, 0, len(n))
	for _, ng := range n {
		ngs = append(ngs, ng.Proto().(*csv1.NodeGroup))
	}
	return &csv1.NodeGroupList{
		Items: ngs,
	}
}

// kubeNodesToGroup aggregates nodes of a particular nodegroup by their marketing role
func kubeNodesToGroup(ngName, namespace, runningJob string,
	nodes []*corev1.Node, nodeCpuAlloc map[string]float64, nodeMemAlloc map[string]int64) nodegroup {
	roleStatus := map[string]*csv1.NodeGroupRoleStatus{}
	roleStatusMemCap := map[string]int64{}
	roleStatusMemAlloc := map[string]int64{}
	sampleNode := ""
	for _, n := range nodes {
		role := getNodeMarketingRole(n)
		if role == string(common.RoleManagement) {
			continue // mgmt scheduled separately of nodegroups, don't include
		}
		sampleNode = n.Name
		cpuCap := n.Status.Capacity[corev1.ResourceCPU]
		memCap := n.Status.Capacity[corev1.ResourceMemory]
		cpuAlloc := nodeCpuAlloc[n.Name]
		memAlloc := nodeMemAlloc[n.Name]
		if rs, ok := roleStatus[role]; ok {
			rs.Count += 1
			rs.CpuAllocatableCores = min(rs.CpuAllocatableCores, cpuAlloc)
			roleStatusMemAlloc[role] = min(roleStatusMemAlloc[role], memAlloc)
			rs.CpuCapacityCores = min(rs.CpuCapacityCores, float64(cpuCap.MilliValue()/1000.0))
			rs.Names = append(rs.Names, n.Name)
			roleStatusMemCap[role] = min(roleStatusMemCap[role], memCap.Value())
		} else {
			roleStatusMemCap[role] = memCap.Value()
			roleStatusMemAlloc[role] = memAlloc
			roleStatus[role] = &csv1.NodeGroupRoleStatus{
				Count:               1,
				CpuAllocatableCores: cpuAlloc,
				CpuCapacityCores:    float64(cpuCap.MilliValue() / 1000.0),
				Names:               []string{n.Name},
			}
		}
	}

	// pretty print the min mem vals
	for r, rs := range roleStatus {
		rs.MemoryCapacity = fmtMemRound(roleStatusMemCap[r])
		rs.MemoryAllocatable = fmtMemRound(roleStatusMemAlloc[r])
	}

	return nodegroup{
		name:       ngName,
		sampleNode: sampleNode,
		status: &csv1.NodeGroupStatus{
			NodeCount:  int32(len(nodes)),
			RoleStatus: roleStatus,
			RunningJob: runningJob,
			Namespace:  namespace,
		},
	}
}

type NodegroupStore struct {
	cfgProvider    pkg.ClusterCfgProvider
	sysClient      sysv1.SystemInterface
	lockInformer   rlinformer.GenericInformer
	nodeLister     pkg.KubeNodeLister
	metricsQuerier common.MetricsQuerier
}

func (n NodegroupStore) ListV2(ctx context.Context, namespace string, options *pb.ListOptions) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "ListV2 not implemented for this type")
}

// Get the nodes associated with a nodegroup, if there are none, then assume this nodegroup does not exist.
func (n NodegroupStore) Get(ctx context.Context, namespace, name string) (Resource, error) {
	cfg, err := n.cfgProvider.GetCfg(ctx)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	if cfg.ResourceVersion != "" && cfg.GroupMap[name] == nil {
		return nil, pkg.MapStatusError(fmt.Errorf("group %s not found in cluster config", name))
	}

	ns := cfg.GetGroupToNsMap()[name]
	if namespace != common.SystemNamespace && ns != namespace {
		return nil, grpcStatus.Errorf(codes.NotFound, "%s with name '%s' was not found", NodegroupType, name)
	}

	nodes, err := n.nodeLister.ListByGroup(name)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	if len(nodes) == 0 {
		return nil, grpcStatus.Errorf(codes.NotFound, "%s with name '%s' was not found", NodegroupType, name)
	}

	runningJob := ""
	if n.lockInformer != nil {
		res, err := n.lockInformer.Lister().List(labels.NewSelector())
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		for _, l := range res {
			lock := *l.(*rlapiv1.ResourceLock)
			if lock.IsGranted() {
				for _, g := range lock.Status.NodeGroupGrants {
					for _, gg := range g.NodeGroups {
						if gg == name {
							runningJob = lock.Name
						}
					}
				}
			}
		}
	}

	cpuAlloc, err := n.metricsQuerier.QueryNodeCpuAllocatable(ctx)
	if err != nil {
		logrus.Warnf("Unable to query prom cpu metrics: %s", err)
	}
	memAlloc, err := n.metricsQuerier.QueryNodeMemAllocatable(ctx)
	if err != nil {
		logrus.Warnf("Unable to query prom mem metrics: %s", err)
	}

	return kubeNodesToGroup(name, ns, runningJob, nodes, cpuAlloc, memAlloc), nil
}

func (n NodegroupStore) List(ctx context.Context, namespace string, options *pb.GetOptions) (Resource, error) {
	cfg, err := n.cfgProvider.GetCfg(ctx)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	var nodes []*corev1.Node
	if common.Namespace != common.SystemNamespace && n.nodeLister.AreNodesNamespaceLabeled() {
		labelSet := labels.Set{}
		labelSet[common.NamespaceLabelKey] = namespace
		nodes, err = n.nodeLister.ListByLabelSet(labelSet)
	} else {
		nodes, err = n.nodeLister.List()
	}
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	byGroup := map[string][]*corev1.Node{}
	for _, node := range nodes {
		ng := node.Labels[common.GroupLabelKey]
		if ng == "" || cfg.NodeMap[node.Name].GetRole() == string(common.RoleBroadcastReduce) {
			continue
		}
		if namespace == common.SystemNamespace || cfg.IsNodeInNamespace(namespace, node.Name) {
			byGroup[ng] = append(byGroup[ng], node)
		}
	}

	// get granted locks and systems
	runningJobs := map[string]string{}
	if n.lockInformer != nil {
		var res []runtime.Object
		if namespace == common.SystemNamespace {
			res, err = n.lockInformer.Lister().List(labels.NewSelector())
		} else {
			res, err = n.lockInformer.Lister().ByNamespace(namespace).List(labels.NewSelector())
		}
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		for _, l := range res {
			lock := *l.(*rlapiv1.ResourceLock)
			if lock.IsGranted() {
				for _, g := range lock.Status.NodeGroupGrants {
					for _, gg := range g.NodeGroups {
						if _, ok := byGroup[gg]; ok {
							runningJobs[gg] = lock.Name
						}
					}
				}
			}
		}
	}

	cpuAlloc, err := n.metricsQuerier.QueryNodeCpuAllocatable(ctx)
	if err != nil {
		logrus.Warnf("Unable to query prom cpu metrics: %s", err)
	}
	memAlloc, err := n.metricsQuerier.QueryNodeMemAllocatable(ctx)
	if err != nil {
		logrus.Warnf("Unable to query prom mem metrics: %s", err)
	}

	group2Ns := cfg.GetGroupToNsMap()
	var ngs []nodegroup
	for name, group := range byGroup {
		ngs = append(ngs, kubeNodesToGroup(name, group2Ns[name], runningJobs[name], group, cpuAlloc, memAlloc))
	}

	return nodegroupList(ngs), nil
}

func emptyNodegroupTablePb() *csv1.Table {
	return &csv1.Table{Columns: []*csv1.ColumnDefinition{
		{Name: "Name"},
		{Name: "Session"},
		{Name: "Sample-Node"},
		{Name: "Node-Count"},
		{Name: "Worker-Count"},
		{Name: "Worker-Mem"},
		{Name: "MemX-Count"},
		{Name: "MemX-Mem"},
		{Name: "Running-Executes"},
	}}
}

func mapNodeGroup(ng nodegroup) *csv1.RowData {
	// in non-POR (test) clusters, any = all roles, so add it to any missing vals
	anyCount := "0"
	anyMem := "0"
	if stat, ok := ng.status.GetRoleStatus()["any"]; ok {
		anyCount = strconv.Itoa(int(stat.Count))
		anyMem = stat.MemoryAllocatable
	}

	workerCount := anyCount
	workerMem := anyMem
	if stat, ok := ng.status.GetRoleStatus()["worker"]; ok {
		workerCount = strconv.Itoa(int(stat.Count))
		workerMem = stat.MemoryAllocatable
	}

	memxCount := anyCount
	memxMemCap := anyMem
	if stat, ok := ng.status.GetRoleStatus()[Memx]; ok {
		memxCount = strconv.Itoa(int(stat.Count))
		memxMemCap = stat.MemoryAllocatable
	}

	return &csv1.RowData{Cells: []string{
		ng.name,
		ng.status.Namespace,
		ng.sampleNode,
		strconv.Itoa(int(ng.status.NodeCount)),
		workerCount,
		workerMem,
		memxCount,
		memxMemCap,
		ng.status.RunningJob,
	}}
}

func (n NodegroupStore) AsTable(resource Resource) *csv1.Table {
	table := emptyNodegroupTablePb()
	switch val := resource.(type) {
	case nodegroup:
		table.Rows = []*csv1.RowData{mapNodeGroup(val)}
	case nodegroupList:
		sort.Slice(val, func(i, j int) bool {
			k := strings.Compare(val[i].status.Namespace, val[j].status.Namespace)
			if k == 0 {
				return strings.Compare(val[i].name, val[j].name) < 0
			}
			return k < 0
		})
		table.Rows = common.Map(mapNodeGroup, val)
	default:
		panic(fmt.Sprintf("unhandled nodegroup conversion for type: %T", val))
	}
	return table
}

func (n NodegroupStore) PatchMerge(ctx context.Context, namespace, name string, patch []byte) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "patch not implemented for this type")
}

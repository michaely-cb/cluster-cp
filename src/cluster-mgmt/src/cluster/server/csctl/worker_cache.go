package csctl

import (
	"context"
	"fmt"
	"net"
	"sort"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"

	"github.com/prometheus/common/model"
)

type WorkerCache struct {
	node             string
	diskUsagePercent string
}

func (v *WorkerCache) Proto() proto.Message {
	return MapWorkerCache(v)
}

type WorkerCacheList struct {
	items []*WorkerCache
}

func (v *WorkerCacheList) Proto() proto.Message {
	return &csv1.WorkerCacheList{Items: common.Map(MapWorkerCache, v.items)}
}

type WorkerCacheStore struct {
	kubeClient         kubernetes.Interface
	metricsQuerier     common.MetricsQuerier
	nodeLister         pkg.KubeNodeLister
	ipToWorkerCacheMap map[string]*WorkerCache
}

func (v WorkerCacheStore) ListV2(ctx context.Context, namespace string, options *csctl.ListOptions) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "ListV2 not implemented for this type")
}

func (v WorkerCacheStore) Get(ctx context.Context, namespace, name string) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "Get not implemented for this type")
}

func (v WorkerCacheStore) List(ctx context.Context, namespace string, options *csctl.GetOptions) (Resource, error) {
	if options != nil {
		log.WithField("options", options).Warn("options argument is ignored")
	}

	labelSet := labels.Set{"k8s.cerebras.com/node-role-worker": ""}
	if common.Namespace != common.SystemNamespace && v.nodeLister.AreNodesNamespaceLabeled() {
		labelSet[common.NamespaceLabelKey] = namespace
	}
	nodes, err := v.nodeLister.ListByLabelSet(labelSet)
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	v.ipToWorkerCacheMap = make(map[string]*WorkerCache, len(nodes))
	for _, n := range nodes {
		ip := n.Status.Addresses[0].Address
		v.ipToWorkerCacheMap[ip] = &WorkerCache{
			node: n.Name,
		}
	}

	result, err := v.metricsQuerier.QueryNodeDiskUsage(ctx)
	if err != nil {
		log.Warn("Could not query prometheus for worker cache status")
	} else {
		for _, sample := range result.(model.Vector) {
			if sample == nil {
				continue
			}
			ip, _, _ := net.SplitHostPort(string(sample.Metric["instance"]))
			if _, ok := v.ipToWorkerCacheMap[ip]; ok {
				v.ipToWorkerCacheMap[ip].diskUsagePercent = fmt.Sprintf("%.2f%%", sample.Value)
				log.Debugf("node name: %s, ip: %s, disk usage percent: %s",
					v.ipToWorkerCacheMap[ip].node, ip, v.ipToWorkerCacheMap[ip].diskUsagePercent)
			}
		}
	}

	var workerCacheList []*WorkerCache
	for ip, item := range v.ipToWorkerCacheMap {
		if item.diskUsagePercent != "" {
			workerCacheList = append(workerCacheList, v.ipToWorkerCacheMap[ip])
		} else {
			workerCacheList = append(workerCacheList, &WorkerCache{
				node:             item.node,
				diskUsagePercent: "unknown",
			})
		}
	}

	sort.Slice(workerCacheList, func(i, j int) bool {
		return workerCacheList[i].node < workerCacheList[j].node
	})

	return &WorkerCacheList{items: workerCacheList}, nil
}

func (v WorkerCacheStore) PatchMerge(ctx context.Context, namespace, name string, patch []byte) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "patch not implemented for this type")
}

func (v *WorkerCacheStore) AsTable(resource Resource) *csv1.Table {
	switch val := resource.(type) {
	case *WorkerCacheList:
		table := emptyWorkerCacheTablePb()
		table.Rows = common.Map(mapWorkerCacheTableRow, common.Map(MapWorkerCache, val.items))
		return table
	default:
		panic(fmt.Sprintf("unhandled table conversion for type: %T", val))
	}
}

func emptyWorkerCacheTablePb() *csv1.Table {
	return &csv1.Table{Columns: []*csv1.ColumnDefinition{
		{Name: "node"},
		{Name: "disk-usage"},
	}}
}

func MapWorkerCache(wc *WorkerCache) *csv1.WorkerCache {
	vol := &csv1.WorkerCache{
		Meta: &csv1.ObjectMeta{
			Type:       "worker-cache",
			Name:       wc.node,
			CreateTime: &timestamppb.Timestamp{},
			FieldPriority: map[string]int32{
				"meta.fieldPriority": 2,
				"meta.createTime":    2,
			},
		},
		Status: &csv1.WorkerCacheStatus{
			DiskUsagePercent: wc.diskUsagePercent,
		},
	}
	return vol
}

func mapWorkerCacheTableRow(workerCache *csv1.WorkerCache) *csv1.RowData {
	cells := []string{workerCache.Meta.Name, workerCache.Status.DiskUsagePercent}
	return &csv1.RowData{Cells: cells}
}

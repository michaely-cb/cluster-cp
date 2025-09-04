package csctl

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	wscommon "cerebras.com/job-operator/common"
	"cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"cerebras.com/cluster/server/pkg"
)

type VersionInfo struct {
	csctl               string
	csctlSemver         string
	clusterServer       string
	clusterServerSemver string
	jobOperator         string
	jobOperatorSemver   string
	cbcore              string
}

func (v *VersionInfo) Proto() proto.Message {
	return MapVersionInfo(v)
}

type VersionStore struct {
	kubeClient     kubernetes.Interface
	metricsQuerier wscommon.MetricsQuerier
}

func (v *VersionStore) ListV2(ctx context.Context, namespace string, options *csctl.ListOptions) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "ListV2 not implemented for this type")
}

func (v *VersionStore) Get(ctx context.Context, namespace, name string) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "Get not implemented for this type")
}

func (v *VersionStore) List(ctx context.Context, namespace string, options *csctl.GetOptions) (Resource, error) {
	var operatorNamespace string
	if wscommon.EnableClusterMode {
		operatorNamespace = wscommon.SystemNamespace
	} else {
		operatorNamespace = namespace
	}

	verData := &VersionInfo{}
	verData.jobOperator, verData.jobOperatorSemver, _ = v.metricsQuerier.QueryJobOperatorVersion(ctx, operatorNamespace)
	verData.clusterServer, verData.clusterServerSemver, _ = v.metricsQuerier.QueryClusterServerVersion(ctx, namespace)
	verData.csctl, verData.csctlSemver = pkg.GetClientVersionFromContext(ctx)
	verData.cbcore = "unknown"
	poList, err := v.kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/instance=cluster-server",
	})
	if err != nil {
		log.Errorf("Can't get cluster server pods: %s", err)
		return nil, pkg.MapStatusError(err)
	}
	if len(poList.Items) > 0 {
		for _, env := range poList.Items[0].Spec.Containers[0].Env {
			// We update from WSJOB_DEFAULT_COORDINATOR_IMAGE to WSJOB_DEFAULT_IMAGE in release 3.0.0
			// TODO: remove old environment variable in release 3.2.0
			if env.Name == "WSJOB_DEFAULT_IMAGE" || env.Name == "WSJOB_DEFAULT_COORDINATOR_IMAGE" {
				res := strings.Split(env.Value, ":")
				if strings.Contains(env.Value, "cbcore") && len(res) > 1 {
					verData.cbcore = res[1]
				}
				break
			}
		}
	}
	return verData, nil
}

func (v *VersionStore) PatchMerge(ctx context.Context, namespace, name string, patch []byte) (Resource, error) {
	return nil, grpcStatus.Error(codes.Unimplemented, "patch not implemented for this type")
}

func (v *VersionStore) AsTable(resource Resource) *csv1.Table {
	switch val := resource.(type) {
	case *VersionInfo:
		table := &csv1.Table{Columns: []*csv1.ColumnDefinition{
			{Name: "Package"},
			{Name: "Version"},
		}}
		table.Rows = []*csv1.RowData{
			{Cells: []string{"csctl", val.csctl}},
			{Cells: []string{"csctl-semantic-version", val.csctlSemver}},
			{Cells: []string{"cluster-server", val.clusterServer}},
			{Cells: []string{"cluster-server-semantic-version", val.clusterServerSemver}},
			{Cells: []string{"default-cbcore", val.cbcore}},
			{Cells: []string{"job-operator", val.jobOperator}},
			{Cells: []string{"job-operator-semantic-version", val.jobOperatorSemver}},
		}
		return table
	default:
		panic(fmt.Sprintf("unhandled table conversion for type: %T", val))
	}
}

func MapVersionInfo(vinfo *VersionInfo) *csv1.VersionInfo {
	vi := &csv1.VersionInfo{
		Meta: &csv1.ObjectMeta{
			Type:       "version",
			Name:       "version",
			CreateTime: &timestamppb.Timestamp{},
			FieldPriority: map[string]int32{
				"meta.fieldPriority": 2,
				"meta.createTime":    2,
			},
		},
		Csctl:         vinfo.csctl,
		Cbcore:        vinfo.cbcore,
		ClusterServer: vinfo.clusterServer,
		JobOperator:   vinfo.jobOperator,
	}
	return vi
}

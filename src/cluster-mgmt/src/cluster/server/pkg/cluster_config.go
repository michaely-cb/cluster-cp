package pkg

import (
	"context"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	nsv1 "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1"
	"cerebras.com/job-operator/common"
)

type ClusterCfgProvider interface {
	GetCfg(ctx context.Context) (*common.ClusterConfig, error)
}

type K8sClusterCfgProvider struct {
	k8sClient kubernetes.Interface
	nsClient  nsv1.NamespaceReservationInterface
}

func NewK8sClusterCfgProvider(k8sClient kubernetes.Interface, nsClient nsv1.NamespaceReservationInterface) ClusterCfgProvider {
	return K8sClusterCfgProvider{
		k8sClient: k8sClient,
		nsClient:  nsClient,
	}
}

func (k K8sClusterCfgProvider) GetCfg(ctx context.Context) (*common.ClusterConfig, error) {
	cm, err := k.k8sClient.CoreV1().ConfigMaps(common.SystemNamespace).Get(
		ctx,
		common.ClusterConfigMapName,
		metav1.GetOptions{},
	)
	if err != nil {
		logrus.Errorf("Failed to create cluster config: can't get configmap: %s", err)
		return nil, MapStatusError(err)
	}
	schema, err := common.NewClusterSchemaFromCM(cm)
	if err != nil {
		return nil, err
	}
	cfg := common.NewClusterConfig(schema)
	cfg.ResourceVersion = cm.ResourceVersion
	cfg.AdjustNodesByClusterSetup()
	cfg.CheckV2Network()

	nsrList, err := k.nsClient.List(ctx, metav1.ListOptions{})
	if err != nil {
		logrus.Errorf("Failed to create cluster config: can't list namespacereservations: %s", err)
		return nil, MapStatusError(err)
	}

	cfg.InitializeNsAssignment(*nsrList)
	return cfg, nil
}

type StaticCfgProvider common.ClusterConfig

func (m StaticCfgProvider) GetCfg(ctx context.Context) (*common.ClusterConfig, error) {
	cfg := common.ClusterConfig(m)
	return &cfg, nil
}

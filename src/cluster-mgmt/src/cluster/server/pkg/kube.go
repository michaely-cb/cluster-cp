package pkg

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreinformers "k8s.io/client-go/informers"
	v1 "k8s.io/client-go/informers/core/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"

	"cerebras.com/job-operator/common"
)

type KubeNodeLister interface {
	List() ([]*corev1.Node, error)
	ListByGroup(group string) ([]*corev1.Node, error)
	ListByLabelSet(labelSet labels.Set) ([]*corev1.Node, error)
	AreNodesNamespaceLabeled() bool
}

type KubeNodeInformerLister struct {
	Informer v1.NodeInformer
}

// Creates an Informer for core k8s objects
func NewNodeInformer(ctx context.Context) (v1.NodeInformer, error) {
	config := ctrl.GetConfigOrDie()
	kcs, err := kubeclientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	factory := coreinformers.NewSharedInformerFactory(kcs, 0)
	nodeInformer := factory.Core().V1().Nodes()
	go factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.Informer().HasSynced) {
		return nil, fmt.Errorf("failed to start node Informer cache")
	}
	return nodeInformer, nil
}

// List nodes
func (k KubeNodeInformerLister) List() ([]*corev1.Node, error) {
	lister := k.Informer.Lister()
	return lister.List(labels.Everything())
}

// ListByGroup lists nodes that are in a particular nodegroup
func (k KubeNodeInformerLister) ListByGroup(group string) ([]*corev1.Node, error) {
	selector := labels.SelectorFromSet(labels.Set{common.GroupLabelKey: group})
	return k.Informer.Lister().List(selector)
}

// ListByLabelSet lists nodes that are confined to the label set
func (k KubeNodeInformerLister) ListByLabelSet(labelSet labels.Set) ([]*corev1.Node, error) {
	return k.Informer.Lister().List(labels.SelectorFromSet(labelSet))
}

// AreNodesNamespaceLabeled indicates whether nodes are labeled to their belonging namespaces.
// If one node has the namespace label, it indicates the job operator is using
// rel-2.1 or above.
// todo: get by NSR instead of extra labelling
func (k KubeNodeInformerLister) AreNodesNamespaceLabeled() bool {
	nodes, err := k.List()
	if err != nil || len(nodes) == 0 {
		logrus.Warnf("Nodes are not namespace labeled: %v", err)
		return false
	}
	for _, no := range nodes {
		if _, ok := no.Labels[common.NamespaceLabelKey]; ok {
			return true
		}
	}
	return false
}

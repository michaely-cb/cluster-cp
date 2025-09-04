package resource_config

import (
	"context"
	"os"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/pager"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	v1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

// Define reserved memory/cpu for each node. These reserved value can be configured for each cluster,
// by setting envvars: NODE_MIN_RESERVED_MEM, NODE_MIN_RESERVED_CPU. The default values are specified
// in NodeReservedCpuDefault and nodeReservedMemDefault.
const (
	NodeReservedCpuEnvvar = "NODE_MIN_RESERVED_CPU"
	NodeReservedMemEnvvar = "NODE_MIN_RESERVED_MEM"

	CpReservedMemEnvvar = "CP_RESERVED_MEM"
	CpReservedCpuEnvvar = "CP_RESERVED_CPU"
	CpPodLabelKey       = "component"
	CpPodLabelVal       = "kube-apiserver"

	// Some nodes such as mgmt nodes in the cluster can use more memory/cpu than the reserved value.
	// In those cases, we will use the sum of the running pods + some buffer, as the reserved
	// values. The buffer is to account for other resources that are not covered by running pods.
	// The ratio defined here is used to compute the buffer.
	cpuBufferRatioDefault = 0.1
	memBufferRatioDefault = 0.1

	// default reserved values: 1cpu, 5GiB
	nodeReservedCpuDefaultValue = 1
	nodeReservedMemDefaultValue = 5 << 30
)

var (
	NodeReservedCpuDefault = *apiresource.NewQuantity(nodeReservedCpuDefaultValue, apiresource.DecimalSI)
	NodeReservedMemDefault = *apiresource.NewQuantity(nodeReservedMemDefaultValue, apiresource.BinarySI)

	ZeroQuantity = apiresource.Quantity{}

	cpReservedMem apiresource.Quantity
	cpReservedCpu apiresource.Quantity
)

type NodeCapacity interface {
	// Cpu is cpu available for wsjob scheduling - e.g. Node CPU capacity minus whatever daemon related overhead is present
	Cpu(node string) apiresource.Quantity

	// Mem is mem available for wsjob scheduling - e.g. Node mem capacity minus whatever daemon related overhead is present
	Mem(node string) apiresource.Quantity

	// Check if capacity changes
	Equals(n NodeCapacity) bool
}

// struct for tracking node mem/cpu overhead configuration
type nodeCapacity struct {
	// Overheads are resources used by non-wsjob related pods (daemons, prometheus, cluster-server, job-operator, etc...)
	nodeCpuOverhead    map[string]apiresource.Quantity
	nodeMemOverhead    map[string]apiresource.Quantity
	defaultCpuOverhead apiresource.Quantity
	defaultMemOverhead apiresource.Quantity

	// Capacity is node capacity observed on k8s Nodes or overridden by configs for tests
	nodeMemCapacity map[string]apiresource.Quantity
	nodeCpuCapacity map[string]apiresource.Quantity
}

func init() {
	memVal := os.Getenv(CpReservedMemEnvvar)
	if memVal != "" {
		cpReservedMem = apiresource.MustParse(memVal)
		logrus.Infof("CP_RESERVED_MEM: %s", memVal)
	}
	cpuVal := os.Getenv(CpReservedCpuEnvvar)
	if cpuVal != "" {
		cpReservedCpu = apiresource.MustParse(cpuVal)
		logrus.Infof("CP_RESERVED_CPU: %s", cpuVal)
	}
}

func (n nodeCapacity) Cpu(node string) apiresource.Quantity {
	capacity := n.nodeCpuCapacity[node]
	overhead, ok := n.nodeCpuOverhead[node]
	if !ok {
		overhead = n.defaultCpuOverhead
	}
	capacity.Sub(overhead)
	if capacity.Cmp(ZeroQuantity) == -1 {
		return ZeroQuantity
	}
	return capacity
}

func (n nodeCapacity) Mem(node string) apiresource.Quantity {
	capacity := n.nodeMemCapacity[node]
	overhead, ok := n.nodeMemOverhead[node]
	if !ok {
		overhead = n.defaultMemOverhead
	}
	logrus.Debugf("node: %s mem, capacity: %dMi, overhead: %dMi", node,
		capacity.Value()/1024/1024, overhead.Value()/1024/1024)
	capacity.Sub(overhead)
	if capacity.Cmp(ZeroQuantity) == -1 {
		return ZeroQuantity
	}
	return capacity
}

func (n nodeCapacity) Equals(m NodeCapacity) bool {
	equals := true
	for node := range n.nodeMemCapacity {
		if !n.Cpu(node).Equal(m.Cpu(node)) || !n.Mem(node).Equal(m.Mem(node)) {
			logrus.Infof("node capacity changes for node %s, "+
				"previous [cpu:%v, mem:%v], now [cpu:%v, mem:%v]", node,
				n.Cpu(node), n.Mem(node), m.Cpu(node), m.Mem(node))
			equals = false
		}
	}
	return equals
}

// actual mem capacity for user jobs = (memCapacity - non-user pods memOverhead - memOverheadBuffer)
// memOverheadBuffer = max(10% * existing non-user pods memOverhead, 5G))
// we need buffer for mainly two reason:
// 1. Critical pods like cilium/registry/nginx/... don't have limit set
// 2. CP pods like api-server/etcd/kube-proxy/.. don't have limit set
type NodeCapacityBuilder struct {
	// overhead with DS/deployment non-user pods
	cpuOverhead map[string]apiresource.Quantity
	memOverhead map[string]apiresource.Quantity
	// total capacity of node
	cpuCapacity map[string]apiresource.Quantity
	memCapacity map[string]apiresource.Quantity
}

func NewNodeCapacityBuilder() NodeCapacityBuilder {
	return NodeCapacityBuilder{
		cpuOverhead: make(map[string]apiresource.Quantity),
		memOverhead: make(map[string]apiresource.Quantity),
		cpuCapacity: make(map[string]apiresource.Quantity),
		memCapacity: make(map[string]apiresource.Quantity),
	}
}

func (b NodeCapacityBuilder) updateOverheadPod(pod *corev1.Pod) {
	if pod == nil || !isOverheadPod(pod) {
		return
	}
	nodeName := pod.Spec.NodeName
	cpu := &apiresource.Quantity{}
	cpu.Add(b.cpuOverhead[nodeName])
	mem := &apiresource.Quantity{}
	mem.Add(b.memOverhead[nodeName])
	for _, c := range pod.Spec.Containers {
		if !c.Resources.Limits.Cpu().IsZero() {
			cpu.Add(*c.Resources.Limits.Cpu())
		} else {
			cpu.Add(*c.Resources.Requests.Cpu())
		}
		if !c.Resources.Limits.Memory().IsZero() {
			mem.Add(*c.Resources.Limits.Memory())
		} else {
			mem.Add(*c.Resources.Requests.Memory())
		}
	}
	// add CP reserved overhead
	// do it once only since this var is supposed to include all CP pods
	if pod.Labels[CpPodLabelKey] == CpPodLabelVal {
		logrus.Infof("reserved CP overhead for node: %s, %dMi, %dm", nodeName,
			cpReservedMem.Value()/1024/1024, cpReservedMem.MilliValue())
		cpu.Add(cpReservedCpu)
		mem.Add(cpReservedMem)
	}
	b.cpuOverhead[nodeName] = *cpu
	b.memOverhead[nodeName] = *mem
}

// call apis for first time init when cache not synced and use informer afterwards
func (b NodeCapacityBuilder) UpdateOverheadPods(c client.Client, cfg *rest.Config, informerOnly bool) error {
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	ctx := context.Background()
	p := pager.New(pager.SimplePageFunc(func(opts metav1.ListOptions) (runtime.Object, error) {
		podList := &corev1.PodList{}
		if informerOnly {
			err = c.List(ctx, podList)
			return podList, err
		}
		return clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, opts)
	}))

	return p.EachListItem(ctx, metav1.ListOptions{}, func(obj runtime.Object) error {
		pod, ok := obj.(*corev1.Pod)
		if ok {
			b.updateOverheadPod(pod)
		}
		return nil
	})
}

func (b NodeCapacityBuilder) UpdateCapacityK8s(k8Node *corev1.Node) {
	if k8Node == nil {
		return
	}
	// FIXME: using Allocatable is more accurate but switching to it must also be accounted for
	// in resource_details.go when calculating the POR memory for MX and AX nodes
	b.cpuCapacity[k8Node.GetName()] = *k8Node.Status.Capacity.Cpu()
	b.memCapacity[k8Node.GetName()] = *k8Node.Status.Capacity.Memory()
}

func (b NodeCapacityBuilder) Build() NodeCapacity {
	cpuVal := os.Getenv(NodeReservedCpuEnvvar)
	reservedCpu := NodeReservedCpuDefault
	if cpuVal != "" {
		reservedCpu = apiresource.MustParse(cpuVal)
	}

	memVal := os.Getenv(NodeReservedMemEnvvar)
	reservedMem := NodeReservedMemDefault
	if memVal != "" {
		reservedMem = apiresource.MustParse(memVal)
	}

	cpu := make(map[string]apiresource.Quantity, len(b.cpuOverhead))
	mem := make(map[string]apiresource.Quantity, len(b.memOverhead))
	for n, c := range b.cpuOverhead {
		cpu[n] = applyCpuBuffer(c, cpuBufferRatioDefault, reservedCpu)
	}
	for n, m := range b.memOverhead {
		mem[n] = applyMemBuffer(m, memBufferRatioDefault, reservedMem)
	}

	return &nodeCapacity{
		nodeMemOverhead:    mem,
		nodeCpuOverhead:    cpu,
		defaultCpuOverhead: reservedCpu,
		defaultMemOverhead: reservedMem,
		nodeCpuCapacity:    b.cpuCapacity,
		nodeMemCapacity:    b.memCapacity,
	}
}

// apply whatever is larger as overhead
func applyMemBuffer(q apiresource.Quantity, ratio float64, reserved apiresource.Quantity) apiresource.Quantity {
	v := int64(float64(q.Value()) * ratio)
	q.Add(*apiresource.NewQuantity(v, apiresource.BinarySI))
	if q.Cmp(reserved) < 0 {
		q = reserved.DeepCopy()
	}
	return q
}

func applyCpuBuffer(q apiresource.Quantity, ratio float64, reserved apiresource.Quantity) apiresource.Quantity {
	v := int64(float64(q.MilliValue()) * ratio)
	q.Add(*apiresource.NewMilliQuantity(v, apiresource.DecimalSI))
	if q.Cmp(reserved) < 0 {
		q = reserved.DeepCopy()
	}
	return q
}

func isOverheadPod(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
		return false
	}
	if pod.Spec.NodeName == "" {
		return false
	}
	// todo: img/export job needs to go through scheduler and queued if too many
	if pod.Labels[wsapisv1.ImageBuilderAppLabelKey] == wsapisv1.ImageBuilderAppLabelValue {
		return false // image build job
	}
	if pod.Labels[wsapisv1.LogExportAppLabelKey] == wsapisv1.LogExportAppLabelValue {
		return false // log-export
	}
	if pod.Labels[v1.JobNameLabel] != "" {
		return false // wsjob
	}
	return true
}

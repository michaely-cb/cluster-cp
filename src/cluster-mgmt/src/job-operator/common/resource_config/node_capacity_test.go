//go:build default

package resource_config

import (
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	v1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

func TestNodeCapacity(t *testing.T) {
	pods := []*corev1.Pod{
		pod("n0-p0", corev1.PodRunning, "n0", 100, 500),
		pod("n0-p1", corev1.PodFailed, "n0", 100, 500),
		pod("wsjob-xxx", corev1.PodRunning, "n0", 1000, 8000, v1.JobNameLabel+"=xxx"),

		pod("n1-p0", corev1.PodPending, "n1", 100, 500),
		pod("n1-p1", corev1.PodRunning, "n1", 100, 500),
		pod("imgjob-xxx", corev1.PodRunning, "n1", 1000, 8000, wsapisv1.ImageBuilderAppLabelKey+"="+wsapisv1.ImageBuilderAppLabelValue),
		pod("xxxx", corev1.PodSucceeded, "n1", 1000, 8000),
	}
	b := NewNodeCapacityBuilder()
	for _, pod := range pods {
		b.updateOverheadPod(pod)
	}
	b.UpdateCapacityK8s(node("n0", 8_000, 32_000))
	b.UpdateCapacityK8s(node("n1", 8_000, 32_000))
	b.UpdateCapacityK8s(node("n2", 8_000, 32_000))
	b.UpdateCapacityK8s(nil)

	no := b.Build()

	for _, testcase := range []struct {
		node      string
		expectCpu apiresource.Quantity
		expectMem apiresource.Quantity
	}{
		{
			node:      "n0",
			expectCpu: *apiresource.NewMilliQuantity(8_000-NodeReservedCpuDefault.MilliValue(), apiresource.DecimalSI),
			expectMem: *apiresource.NewQuantity((32_000<<20)-NodeReservedMemDefault.Value(), apiresource.BinarySI),
		},
		{
			node:      "n1",
			expectCpu: *apiresource.NewMilliQuantity(8_000-NodeReservedCpuDefault.MilliValue(), apiresource.DecimalSI),
			expectMem: *apiresource.NewQuantity((32_000<<20)-NodeReservedMemDefault.Value(), apiresource.BinarySI),
		},
		{
			node:      "n2",
			expectCpu: *apiresource.NewMilliQuantity(8_000-NodeReservedCpuDefault.MilliValue(), apiresource.DecimalSI),
			expectMem: *apiresource.NewQuantity((32_000<<20)-NodeReservedMemDefault.Value(), apiresource.BinarySI),
		},
		{
			node:      "unknown",
			expectCpu: ZeroQuantity,
			expectMem: ZeroQuantity,
		},
	} {
		t.Run(testcase.node, func(t *testing.T) {
			cpu := no.Cpu(testcase.node)
			mem := no.Mem(testcase.node)
			if !testcase.expectCpu.Equal(cpu) {
				t.Errorf("expected cpu %v, got %v", testcase.expectCpu.String(), cpu.String())
			}
			if !testcase.expectMem.Equal(mem) {
				t.Errorf("expected mem %v, got %v", testcase.expectMem.String(), mem.String())
			}
		})
	}
}

func node(name string, cpu int, memory int) *corev1.Node {
	rl := corev1.ResourceList{
		corev1.ResourceCPU:    apiresource.MustParse(fmt.Sprintf("%dm", cpu)),
		corev1.ResourceMemory: apiresource.MustParse(fmt.Sprintf("%dMi", memory)),
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: corev1.NodeStatus{
			Capacity:    rl,
			Allocatable: rl,
		},
	}
}

func pod(name string, phase corev1.PodPhase, nodeName string, cpu int, memory int, labels ...string) *corev1.Pod {
	rl := corev1.ResourceList{
		corev1.ResourceCPU:    apiresource.MustParse(fmt.Sprintf("%dm", cpu)),
		corev1.ResourceMemory: apiresource.MustParse(fmt.Sprintf("%dMi", memory)),
	}
	l := map[string]string{}
	for _, v := range labels {
		kv := strings.SplitN(v, "=", 2)
		l[kv[0]] = kv[1]
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: l,
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: rl,
						Limits:   rl,
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
	}
}

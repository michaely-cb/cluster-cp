package ws

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

const (
	oomErrorMessage = "The pod was killed due to an out of memory (OOM) condition where " +
		"the current memory limit is %s."
)

type eventRecorderInterceptor struct {
	wrapped record.EventRecorder
}

func (e *eventRecorderInterceptor) Event(object runtime.Object, eventtype, reason, message string) {
	e.AnnotatedEventf(object, nil, eventtype, reason, message)
}

func (e *eventRecorderInterceptor) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	e.AnnotatedEventf(object, nil, eventtype, reason, messageFmt, args...)
}

func (e *eventRecorderInterceptor) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	// add memory limit to OOMKilled event
	if eventtype == corev1.EventTypeWarning && reason == "OOMKilled" {
		pod, ok := object.(*corev1.Pod)
		if ok {
			annotations = wscommon.EnsureMap(annotations)
			for _, container := range pod.Spec.Containers {
				if container.Name == wsapisv1.DefaultContainerName {
					if memLimit, ok := container.Resources.Limits[corev1.ResourceMemory]; ok {
						annotations["memory-limit"] = memLimit.String()
						errMsg := fmt.Sprintf(oomErrorMessage, fmtMemRound(memLimit.Value()))
						messageFmt = messageFmt + " " + errMsg
					}
				}
			}
		}
	}
	e.wrapped.AnnotatedEventf(object, annotations, eventtype, reason, messageFmt, args...)
}

// fmtMemRound is similar to the default String() method of Quantity, but it rounds down to the nearest Ki, Mi, Gi, etc.
// except Mi where values below 8Gi are rounded to the nearest Mi unless it's wouldn't lose precision, the idea being
// to preserve some precision where smaller values are more likely to make a difference.
func fmtMemRound(q int64) string {
	if q < (1 << 10) {
		return "0"
	} else if q < 1<<20 {
		return fmt.Sprintf("%dKi", q>>10)
	} else if q < 1<<33 && !(q&((1<<30)-1) == 0) {
		return fmt.Sprintf("%dMi", q>>20)
	} else {
		return fmt.Sprintf("%dGi", q>>30)
	}
}

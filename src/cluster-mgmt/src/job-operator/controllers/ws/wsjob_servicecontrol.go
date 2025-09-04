package ws

import (
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"github.com/kubeflow/common/pkg/controller.v1/control"
)

type WsjobServiceControl struct {
	control.RealServiceControl
	client client.Client
}

func (r WsjobServiceControl) DeleteService(service *corev1.Service, object runtime.Object) error {
	wsjob, isWsjob := object.(*wsapisv1.WSJob)
	if !isWsjob {
		return r.RealServiceControl.DeleteService(service, object)
	}

	if svcShouldExist(wsjob, service.Name) {
		return r.RealServiceControl.DeleteService(service, object)
	}
	return nil
}

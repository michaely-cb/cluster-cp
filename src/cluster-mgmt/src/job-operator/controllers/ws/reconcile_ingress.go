package ws

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

const (
	// SuccessfulCreateIngressReason is added in an event when ingress for a job is successfully created.
	SuccessfulCreateIngressReason = "SuccessfulCreateIngress"
)

// createIngress only runs when the job is in created state.
func (r *WSJobReconciler) createIngress(ctx context.Context, req ctrl.Request, wsjob *wsapisv1.WSJob) error {
	if wsapisv1.SkipIngressReconcile {
		return nil
	}
	ingress := &networkingv1.Ingress{}
	err := r.Client.Get(ctx, req.NamespacedName, ingress)
	if err == nil {
		return nil
	}

	clusterServerIngress := &networkingv1.Ingress{}
	err = r.Client.Get(ctx, types.NamespacedName{
		Namespace: req.Namespace,
		Name:      wsapisv1.ClusterServerIngressName,
	}, clusterServerIngress)
	if err != nil {
		return err
	}
	numTls := len(clusterServerIngress.Spec.TLS)
	if numTls != 1 {
		return fmt.Errorf("expected exactly one ingress TLS but saw %d", numTls)
	}
	clusterServerIngressSecretName := clusterServerIngress.Spec.TLS[0].SecretName

	svcName := wsjob.GetIngressSvcName()
	ingressClassName := "nginx"
	pathType := networkingv1.PathTypePrefix
	annotations := map[string]string{
		"nginx.ingress.kubernetes.io/server-snippet":        "grpc_next_upstream off; grpc_read_timeout 12h; grpc_send_timeout 12h; client_body_timeout 12h;",
		"nginx.ingress.kubernetes.io/proxy-connect-timeout": "60",
		"nginx.ingress.kubernetes.io/proxy-read-timeout":    "43200", // 12 hours
		"nginx.ingress.kubernetes.io/proxy-send-timeout":    "43200", // 12 hours
		"nginx.ingress.kubernetes.io/client-body-timeout":   "43200", // 12 hours
		"nginx.ingress.kubernetes.io/grpc-next-upstream":    "off",
		"nginx.ingress.kubernetes.io/proxy-body-size":       "0",
		"nginx.ingress.kubernetes.io/backend-protocol":      "GRPC",
		"nginx.ingress.kubernetes.io/ssl-redirect":          "true",
	}
	ingress = &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   req.Namespace,
			Name:        req.Name,
			Annotations: annotations,
			Labels:      r.GenLabels(wsjob.GetName()),
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &ingressClassName,
			Rules: []networkingv1.IngressRule{
				{
					Host: fmt.Sprintf("%s.%s", svcName, r.Cfg.GetClusterServerDomain()),
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathType,
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: svcName,
											Port: networkingv1.ServiceBackendPort{
												Number: wsapisv1.DefaultPort,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			TLS: []networkingv1.IngressTLS{
				{
					Hosts: []string{
						r.Cfg.GetClusterServerDomain(),
					},
					SecretName: clusterServerIngressSecretName,
				},
			},
		},
	}
	err = controllerutil.SetControllerReference(wsjob, ingress, r.Scheme)
	if err != nil {
		return fmt.Errorf("set controller reference error: %w", err)
	}
	err = r.Client.Create(ctx, ingress)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("ingress creation error: %w", err)
	}
	logrus.Infof("ingress has been created for %s", wsjob.Name)
	r.Recorder.Eventf(wsjob, v1.EventTypeNormal, SuccessfulCreateIngressReason,
		"Created ingress: %s", wsjob.Name)
	return nil
}

func (r *WSJobReconciler) cleanupIngress(ctx context.Context, namespace, name string) error {
	ingress := &networkingv1.Ingress{}
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, ingress)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get ingress: %w", err)
	}
	err = r.KubeClientSet.NetworkingV1().Ingresses(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("unable to cleanup ingress: %w", err)
	}
	return nil
}

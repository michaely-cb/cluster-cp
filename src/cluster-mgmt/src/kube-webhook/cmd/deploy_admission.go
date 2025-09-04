package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	crdv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const (
	systemUpdateAnnotation = "system-update"
	systemNamespaceLabel   = "system-namespace"
	deployRestrictedLabel  = "labels.k8s.cerebras.com/deploy-restricted"
)

func deployAdmission(w http.ResponseWriter, r *http.Request, clientset *kubernetes.Clientset, dynClient dynamic.Interface) {
	deserializer := codecs.UniversalDeserializer()
	admissionReviewRequest, err := admissionReviewFromRequest(r, deserializer)
	if err != nil {
		msg := fmt.Sprintf("error getting admission review from request: %v", err)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	rawRequest := admissionReviewRequest.Request.Object.Raw
	admissionResponse := &admissionv1.AdmissionResponse{Allowed: true}
	// Do server-side validation that we are only dealing with a deployment resource. This
	// should also be part of the MutatingWebhookConfiguration in the cluster, but
	// we should verify here before continuing.
	deployRes := metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	cmRes := metav1.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	crdRes := metav1.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	roleRes := metav1.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	clusterRoleRes := metav1.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}

	nsrRes := schema.GroupVersionResource{Group: "jobs.cerebras.com", Version: "v1", Resource: "namespacereservations"}

	ctx := context.TODO()

	systemNamespace := false
	nsrDeployRestricted := false

	namespace := admissionReviewRequest.Request.Namespace
	notAllowedMessage := "Please don't update or run jobs in default job-operator Namespace which is " +
		"intended for cluster mgmt team control only."

	if admissionReviewRequest.Request.Namespace != "" {
		logger.Printf("received namespaced message on DeployAdmission: %s", admissionReviewRequest.Request.Name)
		ns, err := clientset.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if err != nil {
			writeDecodeError(w, err)
		}
		_, systemNamespaceLabelExists := ns.Labels[systemNamespaceLabel]
		systemNamespace = systemNamespaceLabelExists

		nsr, err := dynClient.Resource(nsrRes).Get(ctx, namespace, metav1.GetOptions{})
		if err != nil {
			writeDecodeError(w, err)
		}
		_, deployRestrictedLabelExists := nsr.GetLabels()[deployRestrictedLabel]
		nsrDeployRestricted = deployRestrictedLabelExists
	} else {
		logger.Printf("received non namespaced message on DeployAdmission: %s", admissionReviewRequest.Request.Name)
	}

	if admissionReviewRequest.Request.Resource == deployRes {
		deploy := &appv1.Deployment{}
		if _, _, err = deserializer.Decode(rawRequest, nil, deploy); err != nil {
			writeDecodeError(w, err)
			return
		}
		if deploy.Labels["app.kubernetes.io/instance"] != "cluster-server" &&
			deploy.Labels["control-plane"] != "controller-manager" {
			logger.Printf("ignore non-cluster-mgmt deployment %s", deploy.Name)
		} else if strings.ToLower(deploy.Annotations[systemUpdateAnnotation]) == "true" {
			logger.Printf("deploy allowed: %s, image: %s since system update annotation is present",
				deploy.Name, deploy.Spec.Template.Spec.Containers[0].Image)
		} else if systemNamespace {
			logger.Printf("Annotation: %s, reject system deploy without system-update annotation: %s, image: %s",
				deploy.Annotations, deploy.Name, deploy.Spec.Template.Spec.Containers[0].Image)
			admissionResponse.Allowed = false
		} else if nsrDeployRestricted && deploy.Labels["app.kubernetes.io/instance"] == "cluster-server" {
			logger.Printf("Annotation: %s, reject nsr deploy restricted deployment without system-update annotation: %s, image: %s",
				deploy.Annotations, deploy.Name, deploy.Spec.Template.Spec.Containers[0].Image)
			admissionResponse.Allowed = false
			notAllowedMessage = "Please don't deploy to deploy-restricted namespaces, coordinate with the session owner if needed"
		}
	} else if admissionReviewRequest.Request.Resource == cmRes {
		if systemNamespace {
			res := corev1.ConfigMap{}
			if _, _, err = deserializer.Decode(rawRequest, nil, &res); err != nil {
				writeDecodeError(w, err)
				return
			}
			if strings.Contains(res.Name, "job-operator") && res.Annotations[systemUpdateAnnotation] != "true" {
				logger.Printf("denied cm request %s", res.Name)
				admissionResponse.Allowed = false
			}
		}
	} else if admissionReviewRequest.Request.Resource == crdRes {
		res := crdv1.CustomResourceDefinition{}
		if _, _, err := deserializer.Decode(rawRequest, nil, &res); err != nil {
			writeDecodeError(w, err)
			return
		}
		// including wsjob/resourcelock/systems
		if strings.Contains(res.Name, "cerebras") {
			if res.Annotations[systemUpdateAnnotation] != "true" {
				logger.Printf("denied crd request %s", res.Name)
				admissionResponse.Allowed = false
			}
		}
	} else if admissionReviewRequest.Request.Resource == roleRes {
		if systemNamespace {
			res := rbacv1.Role{}
			if _, _, err := deserializer.Decode(rawRequest, nil, &res); err != nil {
				writeDecodeError(w, err)
				return
			}
			if strings.Contains(res.Name, "job-operator") && res.Annotations[systemUpdateAnnotation] != "true" {
				logger.Printf("denied role request %s", res.Name)
				admissionResponse.Allowed = false
			}
		}
	} else if admissionReviewRequest.Request.Resource == clusterRoleRes {
		res := rbacv1.ClusterRole{}
		if _, _, err := deserializer.Decode(rawRequest, nil, &res); err != nil {
			writeDecodeError(w, err)
			return
		}
		if strings.Contains(res.Name, "job-operator") && res.Annotations[systemUpdateAnnotation] != "true" {
			logger.Printf("denied cluster role request %s", res.Name)
			admissionResponse.Allowed = false
		}
	} else {
		msg := fmt.Sprintf("unsupported resource types, got %v", admissionReviewRequest.Request.Resource)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	if !admissionResponse.Allowed {
		admissionResponse.Result = &metav1.Status{
			Status:  "Failure",
			Message: notAllowedMessage,
		}
	} else {
		logger.Printf("allowed request: %s", admissionReviewRequest.Request.Name)
	}
	// Construct the response, which is just another AdmissionReview.
	var admissionReviewResponse admissionv1.AdmissionReview
	admissionReviewResponse.Response = admissionResponse
	admissionReviewResponse.SetGroupVersionKind(admissionReviewRequest.GroupVersionKind())
	admissionReviewResponse.Response.UID = admissionReviewRequest.Request.UID

	resp, err := json.Marshal(admissionReviewResponse)
	if err != nil {
		msg := fmt.Sprintf("error marshalling response json: %v", err)
		logger.Printf(msg)
		w.WriteHeader(500)
		w.Write([]byte(msg))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(resp)
}

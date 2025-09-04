package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/admission/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func addIngressAnnotation(w http.ResponseWriter, r *http.Request) {
	logger.Printf("received message on addIngressAnnotation")

	deserializer := codecs.UniversalDeserializer()

	admissionReviewRequest, err := admissionReviewFromRequest(r, deserializer)
	if err != nil {
		msg := fmt.Sprintf("error getting admission review from request: %v", err)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	// Do server-side validation that we are only dealing with a deployment resource. This
	// should also be part of the MutatingWebhookConfiguration in the cluster, but
	// we should verify here before continuing.
	ingressRes := metav1.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"}
	if admissionReviewRequest.Request.Resource != ingressRes {
		msg := fmt.Sprintf("did not receive ingress, got %s", admissionReviewRequest.Request.Resource.Resource)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	admissionResponse := &admissionv1.AdmissionResponse{Allowed: true}
	rawRequest := admissionReviewRequest.Request.Object.Raw
	ingress := netv1.Ingress{}
	if _, _, err := deserializer.Decode(rawRequest, nil, &ingress); err != nil {
		msg := fmt.Sprintf("error decoding raw ingress: %v", err)
		logger.Printf(msg)
		w.WriteHeader(500)
		w.Write([]byte(msg))
		return
	}

	if strings.HasPrefix(ingress.Name, "wsjob") || strings.HasPrefix(ingress.Name, "cluster-server") {
		patchType := v1.PatchTypeJSONPatch
		patch := []map[string]interface{}{
			{
				"op":    "add",
				"path":  "/metadata/annotations/nginx.ingress.kubernetes.io~1proxy-connect-timeout",
				"value": "60",
			},
			{
				"op":    "add",
				"path":  "/metadata/annotations/nginx.ingress.kubernetes.io~1proxy-read-timeout",
				"value": "43200",
			},
			{
				"op":    "add",
				"path":  "/metadata/annotations/nginx.ingress.kubernetes.io~1proxy-send-timeout",
				"value": "43200",
			},
			{
				"op":    "add",
				"path":  "/metadata/annotations/nginx.ingress.kubernetes.io~1client-body-timeout",
				"value": "43200",
			},
			{
				"op":    "add",
				"path":  "/metadata/annotations/nginx.ingress.kubernetes.io~1grpc-next-upstream",
				"value": "off",
			},
		}
		patchBytes, _ := json.Marshal(patch)
		admissionResponse.PatchType = &patchType
		admissionResponse.Patch = patchBytes
		logger.Printf("Add grpc annotations to ingress %s", ingress.Name)
	}

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

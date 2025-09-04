package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var cephNetAttachOverride = "job-operator/multus-data-net"

const netAttachAnno = "k8s.v1.cni.cncf.io/networks"

func init() {
	cephNadOverrideEnv := os.Getenv("CEPH_NAD_OVERRIDE")
	if cephNadOverrideEnv != "" {
		cephNetAttachOverride = cephNadOverrideEnv
	}
}

// note: this ceph mutate can be removed after all clusters' ceph are going with hostnetwork
func mutateCephMultusPod(w http.ResponseWriter, r *http.Request) {
	logger.Printf("received message on mutateCephMultusPod")

	deserializer := codecs.UniversalDeserializer()

	admissionReviewRequest, err := admissionReviewFromRequest(r, deserializer)
	if err != nil {
		msg := fmt.Sprintf("error getting admission review from request: %v", err)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	// Do server-side validation that we are only dealing with a pod resource. This
	// should also be part of the MutatingWebhookConfiguration in the cluster, but
	// we should verify here before continuing.
	podResource := metav1.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	if admissionReviewRequest.Request.Resource != podResource {
		msg := fmt.Sprintf("did not receive pod, got %s", admissionReviewRequest.Request.Resource.Resource)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	admissionResponse := &admissionv1.AdmissionResponse{Allowed: true}
	var patch string
	patchType := v1.PatchTypeJSONPatch
	rawRequest := admissionReviewRequest.Request.Object.Raw
	pod := corev1.Pod{}
	if _, _, err := deserializer.Decode(rawRequest, nil, &pod); err != nil {
		msg := fmt.Sprintf("error decoding raw pod: %v", err)
		logger.Printf(msg)
		w.WriteHeader(500)
		w.Write([]byte(msg))
		return
	}

	if oldVal, ok := pod.Annotations[netAttachAnno]; ok && oldVal != cephNetAttachOverride {
		patch = fmt.Sprintf(
			`[{"op":"replace","path":"/metadata/annotations/%s","value": "%s"}]`,
			strings.ReplaceAll(netAttachAnno, "/", "~1"),
			cephNetAttachOverride)
		admissionResponse.PatchType = &patchType
		admissionResponse.Patch = []byte(patch)
		logger.Printf("replace net-attach-def for pod %s from %s to %s using %s", pod.Name, oldVal, cephNetAttachOverride, patch)
	} else {
		logger.Printf("ignore pod %s, no net-attach-def or net-attach-def already updated", pod.Name)
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

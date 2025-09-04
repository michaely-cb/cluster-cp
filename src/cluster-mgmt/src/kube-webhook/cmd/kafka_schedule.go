package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// In the event of multi-mgmt nodes, deleting and re-deploying the statefulset could cause the brokers to reschedule
// on a node that's different than where the metadata was first persisted. If we need to force redeployment the
// statefulset, we need a consistent way to schedule the brokers to the nodes where they were first brought up.
// As of the time of Kafka introduction, we do not see a need for such a force redeployment. Hence, this webhook serves
// more as a safety net and facilitates manual testing.
func scheduleKafkaStatefulSetPod(w http.ResponseWriter, r *http.Request, clientset *kubernetes.Clientset) {
	logger.Printf("received message on scheduleKafkaStatefulSetPod")

	deserializer := codecs.UniversalDeserializer()

	admissionReviewRequest, err := admissionReviewFromRequest(r, deserializer)
	if err != nil {
		msg := fmt.Sprintf("error getting admission review from request: %v", err)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	podResource := metav1.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	if admissionReviewRequest.Request.Resource != podResource {
		msg := fmt.Sprintf("did not receive pod, got %s", admissionReviewRequest.Request.Resource.Resource)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	admissionResponse := &admissionv1.AdmissionResponse{Allowed: true}
	rawRequest := admissionReviewRequest.Request.Object.Raw
	pod := corev1.Pod{}
	if _, _, err := deserializer.Decode(rawRequest, nil, &pod); err != nil {
		msg := fmt.Sprintf("error decoding raw pod: %v", err)
		logger.Printf(msg)
		w.WriteHeader(500)
		w.Write([]byte(msg))
		return
	}

	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: "kafka-node=",
	})

	if err != nil {
		msg := fmt.Sprintf("error fetching nodes: %v", err)
		logger.Printf(msg)
		w.WriteHeader(500)
		w.Write([]byte(msg))
		return
	}

	// Extract the node names
	nodeNames := make([]string, len(nodes.Items))
	for i, node := range nodes.Items {
		nodeNames[i] = node.Name
	}

	// Sort the node names alphabetically
	sort.Strings(nodeNames)

	// Create the replica to node mapping
	replicaToNode := make(map[string]string)
	for i, nodeName := range nodeNames {
		replicaToNode[strconv.Itoa(i)] = nodeName
	}

	// Add a dummy replica for testing purposes
	replicaToNode["webhook-validate"] = nodeNames[len(nodeNames)-1]

	// Check if the pod belongs to the specific StatefulSet
	statefulSetName := "kafka"
	if strings.HasPrefix(pod.Labels["statefulset.kubernetes.io/pod-name"], statefulSetName) {
		// Determine the replica number from the pod's name, which should have the format "kafka-X"
		replicaNumber := strings.TrimPrefix(pod.Name, statefulSetName+"-")

		nodeName, ok := replicaToNode[replicaNumber]
		if ok {
			patch := fmt.Sprintf(`[{"op":"replace","path":"/spec/nodeName","value":"%s"}]`, nodeName)
			patchType := v1.PatchTypeJSONPatch
			admissionResponse.PatchType = &patchType
			admissionResponse.Patch = []byte(patch)
			logger.Printf("scheduling pod %s to node %s", pod.Name, nodeName)
		} else {
			logger.Printf("ignore pod %s, no mapping found for replica number %s", pod.Name, replicaNumber)
		}
	} else {
		logger.Printf("ignore pod %s, does not belong to StatefulSet %s", pod.Name, statefulSetName)
	}

	admissionReviewResponse := admissionv1.AdmissionReview{
		Response: admissionResponse,
	}
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

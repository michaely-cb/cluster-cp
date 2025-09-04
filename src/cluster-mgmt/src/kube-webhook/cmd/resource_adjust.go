package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/admission/v1"
	appv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	cpuRes = "cpu"
	memRes = "memory"
)

var (
	memMiPerSystemForOperator = 50
	memMiPerSystemForServer   = 30
	isGenoaServer             bool
)

func init() {
	if val := strings.ToLower(os.Getenv("genoa_server")); val == "true" {
		logger.Println("genoa_server set to true")
		isGenoaServer = true
	}
	if val := os.Getenv("operator_mem_mi_per_system"); val != "" {
		memMiPerSystemForOperator, _ = strconv.Atoi(val)
	}
	if val := os.Getenv("server_mem_mi_per_system"); val != "" {
		memMiPerSystemForServer, _ = strconv.Atoi(val)
	}
}

// adjusts mem/cpu resources for non-wsjob resources
// for cluster-server/job-operator, adjust based on system count formula
// e.g. by default, operator is 50mb*sys, server is 30mb*sys
// for other components, allow env override
// e.g. "memory.prometheus.prometheus-grafana.grafana": "3Gi"
// e.g. "memory.prometheus.grafana": "3Gi"
// one more thing is genoa server cpu override, half the cpu if no override
func adjustResource(w http.ResponseWriter, r *http.Request) {
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
	admissionResponse := &admissionv1.AdmissionResponse{Allowed: true, UID: admissionReviewRequest.Request.UID}

	deployRes := metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	dsRes := metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}
	stsRes := metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	jobRes := metav1.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}
	patchType := v1.PatchTypeJSONPatch
	var patches []map[string]string
	logger.Printf("received create/update event on %s: %s/%s", admissionReviewRequest.Request.Kind.Kind,
		admissionReviewRequest.Request.Namespace, admissionReviewRequest.Request.Name)
	if admissionReviewRequest.Request.Resource == deployRes {
		deploy := &appv1.Deployment{}
		if _, _, err = deserializer.Decode(rawRequest, nil, deploy); err != nil {
			writeDecodeError(w, err)
			return
		}
		// for cluster-server/job-operator, scale by system count to account for incremental deploy + session update
		if deploy.Labels["app.kubernetes.io/instance"] == "cluster-server" ||
			deploy.Labels["control-plane"] == "controller-manager" {
			logger.Printf("received message on deploy create/update: %s", admissionReviewRequest.Request.Name)
			memPerSystem := memMiPerSystemForServer
			if deploy.Labels["control-plane"] == "controller-manager" {
				memPerSystem = memMiPerSystemForOperator
			}
			count, _ := strconv.Atoi(deploy.Annotations["cerebras/system-count"])
			// for now only do upsize for now to avoid edge case
			// e.g. session scale down but large jobs can still persist for 7d
			// e.g. cluster server in system NS don't own systems but needs watch all
			currentMi := deploy.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().Value() / 1024 / 1024
			expectMi := int64(count * memPerSystem)
			if currentMi > 0 && expectMi > currentMi {
				logger.Printf("deploy %s of session %s updated memory from %dMi to %dMi",
					deploy.Name, deploy.Namespace, currentMi, expectMi)
				patches = append(patches,
					map[string]string{
						"op":    "replace",
						"path":  "/spec/template/spec/containers/0/resources/limits/memory",
						"value": fmt.Sprintf("%dMi", expectMi),
					})
			}
		}
		// in case auto scaling by sys count doesn't work well
		// e.g. qa session can have 1 system only but have tons of jobs that requires larger mem
		for i, c := range deploy.Spec.Template.Spec.Containers {
			patches = append(patches, generateResPatches(admissionReviewRequest.Request, &c, i,
				isOnGenoaServer(deploy.Spec.Template.Spec.NodeSelector))...)
		}
	} else if admissionReviewRequest.Request.Resource == dsRes {
		ds := &appv1.DaemonSet{}
		if _, _, err = deserializer.Decode(rawRequest, nil, ds); err != nil {
			writeDecodeError(w, err)
			return
		}
		for i, c := range ds.Spec.Template.Spec.Containers {
			patches = append(patches, generateResPatches(admissionReviewRequest.Request, &c, i,
				isOnGenoaServer(ds.Spec.Template.Spec.NodeSelector))...)
		}
	} else if admissionReviewRequest.Request.Resource == stsRes {
		sts := &appv1.StatefulSet{}
		if _, _, err = deserializer.Decode(rawRequest, nil, sts); err != nil {
			writeDecodeError(w, err)
			return
		}
		for i, c := range sts.Spec.Template.Spec.Containers {
			patches = append(patches, generateResPatches(admissionReviewRequest.Request, &c, i,
				isOnGenoaServer(sts.Spec.Template.Spec.NodeSelector))...)
		}
	} else if admissionReviewRequest.Request.Resource == jobRes {
		job := &batchv1.Job{}
		if _, _, err = deserializer.Decode(rawRequest, nil, job); err != nil {
			writeDecodeError(w, err)
			return
		}
		for i, c := range job.Spec.Template.Spec.Containers {
			patches = append(patches, generateResPatches(admissionReviewRequest.Request, &c, i,
				isOnGenoaServer(job.Spec.Template.Spec.NodeSelector))...)
		}
	} else {
		msg := fmt.Sprintf("unsupported resource types, got %v", admissionReviewRequest.Request.Resource)
		logger.Printf(msg)
		w.WriteHeader(400)
		w.Write([]byte(msg))
		return
	}

	// Construct the response, which is just another AdmissionReview.
	var admissionReviewResponse admissionv1.AdmissionReview
	admissionReviewResponse.Response = admissionResponse
	admissionReviewResponse.SetGroupVersionKind(admissionReviewRequest.GroupVersionKind())
	if len(patches) > 0 {
		admissionResponse.PatchType = &patchType
		bytes, _ := json.Marshal(patches)
		admissionResponse.Patch = bytes
	}

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

func generateResPatches(request *v1.AdmissionRequest, c *corev1.Container, i int, applyHalfCpu bool) []map[string]string {
	var patches []map[string]string
	cpu := getResourceEnvVal(request, c, cpuRes)
	// only allow upsizing to account for incremental update
	// remove limits if found
	currentCpu := c.Resources.Limits.Cpu().MilliValue()
	hasLimit := true
	if currentCpu == 0 {
		currentCpu = c.Resources.Requests.Cpu().MilliValue()
		hasLimit = false
	}
	if cpu != "" {
		if convertRes(cpu).MilliValue() > currentCpu {
			logger.Printf("updated %s/%s/%s cpu from %dm to %s",
				request.Kind.Kind, request.Namespace, request.Name, currentCpu, cpu)
			patches = append(patches,
				map[string]string{
					"op":    "replace",
					"path":  fmt.Sprintf("/spec/template/spec/containers/%d/resources/requests/cpu", i),
					"value": fmt.Sprintf("%s", cpu),
				},
			)
			if hasLimit {
				patches = append(patches,
					map[string]string{
						"op":   "remove",
						"path": fmt.Sprintf("/spec/template/spec/containers/%d/resources/limits/cpu", i),
					},
				)
			}
		} else {
			logger.Printf("skip updating %s/%s/%s cpu with larger val %dm than %s",
				request.Kind.Kind, request.Namespace, request.Name, currentCpu, cpu)
		}
	} else if applyHalfCpu {
		// this can be optimized adding idempotent check with annotation
		// but not critical since spec won't be updated in normal case and this is request only
		if currentCpu != 0 {
			logger.Printf("for genoa server pods, cut cpu by half for %s/%s/%s from %dm to %dm",
				request.Kind.Kind, request.Namespace, request.Name, currentCpu, currentCpu/2)
			patches = append(patches,
				map[string]string{
					"op":    "replace",
					"path":  fmt.Sprintf("/spec/template/spec/containers/%d/resources/requests/cpu", i),
					"value": fmt.Sprintf("%dm", currentCpu/2),
				},
			)
			if hasLimit {
				patches = append(patches,
					map[string]string{
						"op":   "remove",
						"path": fmt.Sprintf("/spec/template/spec/containers/%d/resources/limits/cpu", i),
					},
				)
			}
		}
	}
	memory := getResourceEnvVal(request, c, memRes)
	// default to limits for memory, only allow upsizing to account for incremental update
	currentMem := c.Resources.Limits.Memory().Value()
	hasLimit = true
	if currentMem == 0 {
		currentMem = c.Resources.Requests.Memory().MilliValue()
		hasLimit = false
	}
	if memory != "" {
		if convertRes(memory).Value() > currentMem {
			logger.Printf("updated %s/%s/%s memory from %dMi to %s",
				request.Kind.Kind, request.Namespace, request.Name, currentMem/1024/1024, memory)
			if hasLimit {
				patches = append(patches,
					map[string]string{
						"op":    "replace",
						"path":  fmt.Sprintf("/spec/template/spec/containers/%d/resources/limits/memory", i),
						"value": fmt.Sprintf("%s", memory),
					})
			} else {
				patches = append(patches,
					map[string]string{
						"op":    "replace",
						"path":  fmt.Sprintf("/spec/template/spec/containers/%d/resources/requests/memory", i),
						"value": fmt.Sprintf("%s", memory),
					})
			}
		} else {
			logger.Printf("skip updating %s/%s/%s memory, current val %dMi >= %s",
				request.Kind.Kind, request.Namespace, request.Name, currentMem/1024/1024, memory)
		}
	}
	return patches
}

func isOnGenoaServer(nodeSelector map[string]string) bool {
	if isGenoaServer {
		for k := range nodeSelector {
			if k == "node-role.kubernetes.io/control-plane" ||
				k == "k8s.cerebras.com/node-role-management" ||
				k == "k8s.cerebras.com/node-role-coordinator" ||
				k == "ceph-csi" {
				return true
			}
		}
	}
	return false
}

// getResourceEnvVal gets the override env for resource
// e.g. "memory.prometheus.prometheus-grafana.grafana": "3Gi"
// e.g. "memory.prometheus.grafana": "3Gi"
// todo: after we persist pkg-props as CM, mount cm to keep one source of truth for override
// but note, CM doesn't auto update after mount, so will need some watch/reload mechanism
func getResourceEnvVal(request *v1.AdmissionRequest, container *corev1.Container, resType string) string {
	for _, env := range []string{
		fmt.Sprintf("%s.%s.%s.%s", resType, request.Namespace, request.Name, container.Name),
		// add a simplified version of env for cases of multiple deploy/sts having same resource
		// e.g. rook-ceph-osd-0,rook-ceph-osd-1...
		fmt.Sprintf("%s.%s.%s", resType, request.Namespace, container.Name),
	} {
		if val := os.Getenv(env); val != "" {
			logger.Printf("resource env found, %s:%s", env, val)
			return val
		}
	}
	return ""
}

// convertRes parses a CPU quantity string (e.g., "500m" or "0.5") or a memory quantity string (e.g., "500Mi" or "1Gi")
func convertRes(res string) *resource.Quantity {
	qty, err := resource.ParseQuantity(res)
	if err != nil {
		logger.Printf("error format of resource: %s, %s", res, err)
		return nil
	}
	return &qty
}

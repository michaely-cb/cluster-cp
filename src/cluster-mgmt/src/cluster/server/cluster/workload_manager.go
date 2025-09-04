package cluster

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonpb "cerebras/pb/workflow/appliance/common"

	"cerebras.com/cluster/server/csctl"
	"cerebras.com/cluster/server/pkg"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"

	"cerebras.com/job-operator/common"
	ws "cerebras.com/job-operator/controllers/ws"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var modelLabelKey = fmt.Sprintf("%s%s", csctl.K8sLabelPrefix, "model")
var reservedLabelKeys = map[string]struct{}{
	"app":         {},
	modelLabelKey: {},
}

var reservedFilteringLabelKeys = map[string]struct{}{
	"app": {},
}

const (
	workloadManagerIdentifier  = "workload-manager"
	workloadManagerLogBasePath = "/n0/cluster-mgmt/workload-manager"
)

// validateK8sName checks if the provided name complies with Kubernetes naming conventions
// See: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
func validateK8sName(name string) error {
	// Check length: max 63 characters (DNS requirement)
	if len(name) > 63 {
		return fmt.Errorf("name '%s' exceeds maximum length of 63 characters", name)
	}

	// Check that name follows DNS label standard:
	// - contain only lowercase alphanumeric characters, '-' or '.'
	// - start with an alphanumeric character
	// - end with an alphanumeric character
	validNameRegex := regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)
	if !validNameRegex.MatchString(name) {
		return fmt.Errorf("name '%s' must consist of lowercase alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character", name)
	}

	return nil
}

// getWorkloadManagerConfigMapName returns the standard name for a workload manager's config map
func getWorkloadManagerConfigMapName(name string) string {
	return fmt.Sprintf("%s-config", name)
}

// getServiceEndpoint returns a standard DNS name for accessing a service
// adds a trailing . to avoid extra DNS searching
// https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/?utm_source=chatgpt.com#namespaces-and-dns
func getServiceEndpoint(serviceName, namespace string, port int32) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local.:%d", serviceName, namespace, port)
}

// getNamespace retrieves namespace from context
func getNamespace(ctx context.Context) (string, error) {
	namespace := pkg.GetNamespaceFromContext(ctx)
	if namespace == "" {
		return "", fmt.Errorf("namespace not found in context")
	}
	return namespace, nil
}

// CreateWorkloadManager creates a new workload manager Kubernetes deployment
func (server ClusterServer) CreateWorkloadManager(ctx context.Context, req *pb.CreateWorkloadManagerRequest) (*pb.CreateWorkloadManagerResponse, error) {
	namespace, err := getNamespace(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	clientMeta, err := pkg.GetClientMetaFromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to get client metadata: %v", err)
	}

	// Validate the request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "workload manager name is required")
	}
	if req.WorkloadManagerConfig == nil {
		return nil, status.Error(codes.InvalidArgument, "workload manager config is required")
	}
	// Require original request JSON
	if req.OriginalRequestJson == "" {
		return nil, status.Error(codes.InvalidArgument, "original_request_json is required")
	}

	// Verify the req.Name, it needs to be a k8s name
	if err := validateK8sName(req.Name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid workload manager name: %v", err)
	}

	// Create the deployment specification
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: namespace,
			Labels: map[string]string{
				"app":         workloadManagerIdentifier,
				modelLabelKey: req.Name,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: common.Pointer(int32(req.WorkloadManagerConfig.Replicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":         workloadManagerIdentifier,
					modelLabelKey: req.Name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":         workloadManagerIdentifier,
						modelLabelKey: req.Name,
					},
					Annotations: map[string]string{
						common.NetworkAttachmentKey: common.DataNetAttachDef,
					},
				},
				Spec: corev1.PodSpec{
					// Set security context at pod level to preserve SupplementalGroups
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:          common.Pointer(clientMeta.UID), // Use requesting user's UID
						RunAsGroup:         common.Pointer(clientMeta.GID), // Use requesting user's GID
						SupplementalGroups: clientMeta.Groups,
					},

					Containers: []corev1.Container{
						{
							Name:  workloadManagerIdentifier,
							Image: req.WorkloadManagerConfig.Image,
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: req.WorkloadManagerConfig.ServicePort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "shared-bin",
									MountPath: "/usr/local/bin/get-cerebras-token",
									SubPath:   "get-cerebras-token",
									ReadOnly:  true,
								},
								{
									Name:      "server-log",
									MountPath: "/opt/cerebras/log",
								},
								{
									Name:      "user-auth-secret",
									MountPath: "/root/.cerebras",
									ReadOnly:  true,
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
					// Add pod anti-affinity to avoid scheduling on the same node
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"app":         workloadManagerIdentifier,
												modelLabelKey: req.Name,
											},
										},
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
					},
					// Node selector for coordinator role
					NodeSelector: map[string]string{
						common.NamespaceLabelKey: namespace,
						fmt.Sprintf(ws.RoleLabelKeyF, wsapisv1.WSReplicaTypeCoordinator.Lower()): "",
					},
					// Tolerations for control plane taint
					// Update tolerations to match WSJob controller's format
					Tolerations: []corev1.Toleration{
						{
							Key:      "node-role.kubernetes.io/control-plane",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
						{
							Key:      "node-role.kubernetes.io/master", // Add master taint toleration
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},

					// Add security context

					Volumes: []corev1.Volume{
						{
							Name: "shared-bin",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "server-log",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: workloadManagerLogBasePath,
									Type: common.Pointer(corev1.HostPathDirectoryOrCreate),
								},
							},
						},
						{
							Name: "user-auth-secret",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: "kube-user-auth",
									Items: []corev1.KeyToPath{
										{
											Key:  "secret",
											Path: "user-auth-secret",
										},
									},
									Optional: common.Pointer(false),
								},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  "copy-token",
							Image: wsapisv1.KubeUserAuthImage,
							Command: []string{
								"sh", "-c",
								"cp /usr/local/bin/get-cerebras-token /shared/bin/get-cerebras-token && chmod 4751 /shared/bin/get-cerebras-token",
							},
							// Override security context for init container to run as root
							SecurityContext: &corev1.SecurityContext{
								RunAsUser:  common.Pointer(int64(0)), // Run as root
								RunAsGroup: common.Pointer(int64(0)), // Run as root group
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "shared-bin",
									MountPath: "/shared/bin",
								},
							},
						},
					},
				},
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
			},
		},
	}

	// Apply entrypoint override if provided
	if len(req.WorkloadManagerConfig.EntrypointOverride) > 0 {
		deployment.Spec.Template.Spec.Containers[0].Command = req.WorkloadManagerConfig.EntrypointOverride
	}

	// Apply args override if provided
	if len(req.WorkloadManagerConfig.Args) > 0 {
		deployment.Spec.Template.Spec.Containers[0].Args = req.WorkloadManagerConfig.Args
	}

	// Apply resource requirements if provided
	if req.WorkloadManagerConfig.ResourceInfo != nil {
		resourceReqs, err := convertResourceInfo(req.WorkloadManagerConfig.ResourceInfo)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid resource requirements: %v", err)
		}
		deployment.Spec.Template.Spec.Containers[0].Resources = resourceReqs
	}

	// Configure probes
	configureProbes(req.WorkloadManagerConfig, &deployment.Spec.Template.Spec.Containers[0])

	// Set environment variables directly in pod spec
	if len(req.WorkloadManagerConfig.EnvVars) > 0 {
		envVars := []corev1.EnvVar{}
		for _, env := range req.WorkloadManagerConfig.EnvVars {
			envVars = append(envVars, corev1.EnvVar{
				Name:  env.Key,
				Value: env.Value,
			})
		}
		deployment.Spec.Template.Spec.Containers[0].Env = envVars
	}

	if req.WorkloadManagerConfig != nil && len(req.WorkloadManagerConfig.Labels) > 0 {
		deployment.ObjectMeta.Labels = common.EnsureMap(deployment.ObjectMeta.Labels)
		for k, v := range req.WorkloadManagerConfig.Labels {
			if _, exists := reservedLabelKeys[k]; exists {
				continue
			}
			deployment.ObjectMeta.Labels[k] = v
		}
	}

	// Create the deployment in Kubernetes
	result, err := server.kubeClientSet.AppsV1().Deployments(namespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	// Update the cleanup defer function
	var success bool
	defer func() {
		// Only delete if we didn't succeed
		if !success {
			logrus.Infof("Attempting to clean up deployment %s due to error", result.Name)
			deleteErr := server.kubeClientSet.AppsV1().Deployments(namespace).Delete(ctx, result.Name, metav1.DeleteOptions{})
			if deleteErr != nil {
				logrus.Errorf("Failed to clean up deployment %s: %v", result.Name, deleteErr)
				return
			}
			logrus.Infof("Successfully cleaned up deployment %s", result.Name)
		}
	}()

	// Create owner reference to use for dependent resources
	ownerRef := metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       result.Name,
		UID:        result.UID,
		Controller: common.Pointer(true),
	}

	// Create a ConfigMap to store the request params
	configRecord := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            getWorkloadManagerConfigMapName(req.Name),
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"app":         workloadManagerIdentifier,
				modelLabelKey: req.Name,
			},
		},
		Data: map[string]string{
			"request.json": req.OriginalRequestJson,
		},
	}

	_, err = server.kubeClientSet.CoreV1().ConfigMaps(namespace).Create(ctx, configRecord, metav1.CreateOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	// Create service with owner reference
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            req.Name,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"app":         workloadManagerIdentifier,
				modelLabelKey: req.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None", // Headless service
			Selector: map[string]string{
				"app":         workloadManagerIdentifier,
				modelLabelKey: req.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       req.WorkloadManagerConfig.ServicePort,
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	_, err = server.kubeClientSet.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	// Mark success at the end of the function before returning
	success = true

	// Create the service endpoint string in the DNS format for headless services
	serviceEndpoint := getServiceEndpoint(req.Name, namespace, req.WorkloadManagerConfig.ServicePort)
	return &pb.CreateWorkloadManagerResponse{
		Name:            req.Name,
		Namespace:       namespace,
		ServiceEndpoint: serviceEndpoint,
	}, nil
}

func (server ClusterServer) ListWorkloadManager(ctx context.Context, req *pb.ListWorkloadManagerRequest) (*pb.ListWorkloadManagerResponse, error) {

	namespace, err := getNamespace(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Set default page size if not specified
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 50 // Default page size
	}
	if pageSize > 100 {
		pageSize = 100 // Maximum page size
	}

	// Parse continue token if provided
	continueToken := req.PageToken

	// Build label selector: always include our base selector, then AND with any requested labels
	selectorParts := []string{fmt.Sprintf("app=%s", workloadManagerIdentifier)}
	if req.LabelSelector != nil && len(req.LabelSelector) > 0 {
		for k, v := range req.LabelSelector {
			// Skip attempts to override system-reserved labels
			if _, skip := reservedFilteringLabelKeys[k]; skip {
				continue
			}
			selectorParts = append(selectorParts, fmt.Sprintf("%s=%s", k, v))
		}
	}

	listOptions := metav1.ListOptions{
		LabelSelector: strings.Join(selectorParts, ","),
		Limit:         int64(pageSize),
		Continue:      continueToken,
	}

	deploymentList, err := server.kubeClientSet.AppsV1().Deployments(namespace).List(ctx, listOptions)
	if err != nil {
		logrus.Errorf("Failed to list deployments: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to list workload managers: %v", err)
	}

	// Convert deployments to workload manager responses
	var workloadManagers []*pb.GetWorkloadManagerResponse
	for _, deployment := range deploymentList.Items {
		wmResponse := convertDeploymentToWorkloadManagerResponseCore(&deployment)
		workloadManagers = append(workloadManagers, wmResponse)
	}

	response := &pb.ListWorkloadManagerResponse{
		WorkloadManagers: workloadManagers,
		NextPageToken:    deploymentList.Continue,
	}

	return response, nil
}

// Helper function to convert ResourceInfo to Kubernetes ResourceRequirements
func convertResourceInfo(info *commonpb.ResourceInfo) (corev1.ResourceRequirements, error) {
	requirements := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	// Set CPU requests/limits
	if info.CpuMillicore > 0 {
		// Create CPU quantity in millicores format
		cpu := resource.NewMilliQuantity(int64(info.CpuMillicore), resource.DecimalSI)
		requirements.Requests[corev1.ResourceCPU] = *cpu
		requirements.Limits[corev1.ResourceCPU] = *cpu
	}

	// Set memory requests/limits
	if info.MemoryBytes > 0 {
		memory := resource.NewQuantity(info.MemoryBytes, resource.BinarySI)
		requirements.Requests[corev1.ResourceMemory] = *memory
		requirements.Limits[corev1.ResourceMemory] = *memory
	}

	return requirements, nil
}

// Configure container probes based on the workload manager config
func configureProbes(config *pb.WorkloadManagerConfig, container *corev1.Container) {
	// Set default HTTP health endpoint path if not specified
	healthPath := "/v1/health"

	// Configure liveness probe
	if config.LivenessProbe != nil {
		container.LivenessProbe = convertProbeConfig(config.LivenessProbe, healthPath)
	} else {
		// Default liveness probe
		container.LivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   healthPath,
					Port:   intstr.FromInt(int(config.ServicePort)),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 30,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
			FailureThreshold:    3,
		}
	}

	// Configure readiness probe
	if config.ReadinessProbe != nil {
		container.ReadinessProbe = convertProbeConfig(config.ReadinessProbe, healthPath)
	} else {
		// Default readiness probe
		container.ReadinessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   healthPath,
					Port:   intstr.FromInt(int(config.ServicePort)),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       5,
			TimeoutSeconds:      3,
			FailureThreshold:    3,
		}
	}

	// Configure startup probe if specified
	if config.StartupProbe != nil {
		container.StartupProbe = convertProbeConfig(config.StartupProbe, healthPath)
	}
}

// Convert ProbeConfig to Kubernetes Probe
func convertProbeConfig(probeConfig *commonpb.ProbeConfig, defaultPath string) *corev1.Probe {
	probe := &corev1.Probe{
		InitialDelaySeconds: probeConfig.InitialDelaySeconds,
		PeriodSeconds:       probeConfig.PeriodSeconds,
		TimeoutSeconds:      probeConfig.TimeoutSeconds,
		SuccessThreshold:    probeConfig.SuccessThreshold,
		FailureThreshold:    probeConfig.FailureThreshold,
	}

	// Configure HTTP handler
	if httpGet := probeConfig.GetHttpGet(); httpGet != nil {
		scheme := corev1.URISchemeHTTP
		if httpGet.Scheme == commonpb.HTTPGetAction_HTTPS {
			scheme = corev1.URISchemeHTTPS
		}

		path := httpGet.Path
		if path == "" {
			path = defaultPath
		}

		probe.ProbeHandler = corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   path,
				Port:   intstr.FromInt(int(httpGet.Port)),
				Host:   httpGet.Host,
				Scheme: scheme,
			},
		}
	}

	return probe
}

// UpdateWorkloadManager updates specific fields of an existing workload manager
func (server ClusterServer) UpdateWorkloadManager(ctx context.Context, req *pb.UpdateWorkloadManagerRequest) (*pb.UpdateWorkloadManagerResponse, error) {
	namespace, err := getNamespace(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate the request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "workload manager name is required")
	}
	if req.WorkloadManagerConfig == nil {
		return nil, status.Error(codes.InvalidArgument, "workload manager config is required")
	}

	// Get the existing deployment
	deployment, err := server.kubeClientSet.AppsV1().Deployments(namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	if req.UpdateMask == nil || len(req.UpdateMask.Paths) == 0 {
		return nil, status.Error(codes.InvalidArgument, "update_mask is required")
	}

	// Build reserved set: system labels + any selector keys (deployment and service)
	reserved := make(map[string]struct{}, len(reservedLabelKeys))
	for k := range reservedLabelKeys {
		reserved[k] = struct{}{}
	}
	for k := range deployment.Spec.Selector.MatchLabels {
		reserved[k] = struct{}{}
	}

	for _, path := range req.UpdateMask.GetPaths() {
		switch path {
		case "workload_manager_config.replicas":
			replicas := int32(req.WorkloadManagerConfig.Replicas)
			deployment.Spec.Replicas = &replicas

		case "workload_manager_config.labels":
			newLabels := req.WorkloadManagerConfig.GetLabels()
			if newLabels == nil {
				newLabels = map[string]string{} // clearing all non-reserved labels
			}

			// Update ONLY Deployment metadata labels.
			deployment.ObjectMeta.Labels = keepOnlyReservedAndNewLabels(
				deployment.ObjectMeta.GetLabels(), newLabels, reserved,
			)

		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported update field: %s", path)
		}
	}

	// Persist deployment changes based on UpdateMask
	switch {
	// If only replicas need to be updated, optimize scale-only updates
	case len(req.UpdateMask.Paths) == 1 && req.UpdateMask.Paths[0] == "workload_manager_config.replicas":
		_, err = server.kubeClientSet.AppsV1().Deployments(namespace).UpdateScale(
			ctx,
			deployment.Name,
			&v1.Scale{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deployment.Name,
					Namespace: namespace,
				},
				Spec: v1.ScaleSpec{
					Replicas: int32(req.WorkloadManagerConfig.Replicas),
				},
			},
			metav1.UpdateOptions{},
		)
	default:
		_, err = server.kubeClientSet.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	}
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	// Get the service to include in the response
	service, err := server.kubeClientSet.CoreV1().Services(namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, pkg.MapStatusError(err)
	}

	// Create service endpoint for response
	serviceEndpoint := ""
	if service != nil && len(service.Spec.Ports) > 0 {
		serviceEndpoint = getServiceEndpoint(service.Name, service.Namespace, service.Spec.Ports[0].Port)
	}

	return &pb.UpdateWorkloadManagerResponse{
		Name:            deployment.Name,
		Namespace:       namespace,
		ServiceEndpoint: serviceEndpoint,
	}, nil
}

// keepOnlyReservedAndNewLabels preserves reserved keys from existing (with their values)
// and replaces all other keys with newLabels.
func keepOnlyReservedAndNewLabels(existing map[string]string, newLabels map[string]string, reserved map[string]struct{}) map[string]string {
	out := map[string]string{}
	// keep reserved from existing
	for k, v := range existing {
		if _, ok := reserved[k]; ok {
			out[k] = v
		}
	}
	// add/overwrite with new (skip attempts to set reserved keys)
	for k, v := range newLabels {
		if _, isReserved := reserved[k]; isReserved {
			logrus.Warnf("ignoring attempt to set reserved label %q (value=%q); keeping existing reserved value", k, v)
			continue
		}
		out[k] = v
	}
	return out
}

func (server ClusterServer) DeleteWorkloadManager(ctx context.Context, req *pb.DeleteWorkloadManagerRequest) (*pb.DeleteWorkloadManagerResponse, error) {
	namespace, err := getNamespace(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate the request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "workload manager name is required")
	}

	// Check if the workload manager exists
	_, err = server.kubeClientSet.AppsV1().Deployments(namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	// Delete the deployment (will cascade delete any resources with owner references)
	deletePolicy := metav1.DeletePropagationForeground
	deleteOptions := metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}

	err = server.kubeClientSet.AppsV1().Deployments(namespace).Delete(ctx, req.Name, deleteOptions)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Already deleted
			return &pb.DeleteWorkloadManagerResponse{
				Name:      req.Name,
				Namespace: namespace,
			}, nil
		}
		return nil, pkg.MapStatusError(err)
	}

	return &pb.DeleteWorkloadManagerResponse{
		Name:      req.Name,
		Namespace: namespace,
	}, nil
}

// GetWorkloadManager retrieves status and metadata for an existing workload manager
func (server ClusterServer) GetWorkloadManager(ctx context.Context, req *pb.GetWorkloadManagerRequest) (*pb.GetWorkloadManagerResponse, error) {
	namespace, err := getNamespace(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate the request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "workload manager name is required")
	}

	// Get the deployment
	deployment, err := server.kubeClientSet.AppsV1().Deployments(namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}

	resp, err := server.convertDeploymentToWorkloadManagerResponseForGet(ctx, deployment)

	if err != nil {
		logrus.Errorf("Failed to convert deployment %s to workload manager response: %v", deployment.Name, err)
		return nil, status.Errorf(codes.Internal, "failed to get workload manager: %v", err)
	}
	return resp, nil
}

// extractProbeConfig converts a Kubernetes Probe to a ProbeConfig proto
func extractProbeConfig(probe *corev1.Probe) *commonpb.ProbeConfig {
	config := &commonpb.ProbeConfig{
		InitialDelaySeconds: probe.InitialDelaySeconds,
		PeriodSeconds:       probe.PeriodSeconds,
		TimeoutSeconds:      probe.TimeoutSeconds,
		SuccessThreshold:    probe.SuccessThreshold,
		FailureThreshold:    probe.FailureThreshold,
	}

	// Extract HTTP probe if present
	if probe.HTTPGet != nil {
		scheme := commonpb.HTTPGetAction_HTTP
		if probe.HTTPGet.Scheme == corev1.URISchemeHTTPS {
			scheme = commonpb.HTTPGetAction_HTTPS
		}

		config.Handler = &commonpb.ProbeConfig_HttpGet{
			HttpGet: &commonpb.HTTPGetAction{
				Path:   probe.HTTPGet.Path,
				Port:   probe.HTTPGet.Port.IntVal,
				Host:   probe.HTTPGet.Host,
				Scheme: scheme,
			},
		}
	}

	return config
}

func convertDeploymentStatus(deployment *appsv1.Deployment) *pb.DeploymentStatus {
	deploymentStatus := &pb.DeploymentStatus{
		Replicas:           deployment.Status.Replicas,
		AvailableReplicas:  deployment.Status.AvailableReplicas,
		ReadyReplicas:      deployment.Status.ReadyReplicas,
		UpdatedReplicas:    deployment.Status.UpdatedReplicas,
		ObservedGeneration: deployment.Status.ObservedGeneration,
	}

	// Add conditions
	conditions := make([]*pb.DeploymentCondition, 0, len(deployment.Status.Conditions))
	for _, condition := range deployment.Status.Conditions {
		conditions = append(conditions, &pb.DeploymentCondition{
			Type:               string(condition.Type),
			Status:             string(condition.Status),
			Reason:             condition.Reason,
			Message:            condition.Message,
			LastUpdateTime:     condition.LastUpdateTime.String(),
			LastTransitionTime: condition.LastTransitionTime.String(),
		})
	}
	deploymentStatus.Conditions = conditions

	return deploymentStatus
}

// convertDeploymentToWorkloadManagerResponseCore converts deployment to WorkloadManagerResponse (core conversion logic)
func convertDeploymentToWorkloadManagerResponseCore(deployment *appsv1.Deployment) *pb.GetWorkloadManagerResponse {
	// Extract workload manager name from deployment
	wmName := deployment.Name

	// Extract workload manager config from deployment
	wmc := &pb.WorkloadManagerConfig{
		Replicas: int32(*deployment.Spec.Replicas),
	}

	// If we have containers, extract the image and other container config
	if len(deployment.Spec.Template.Spec.Containers) > 0 {
		container := deployment.Spec.Template.Spec.Containers[0]
		wmc.Image = container.Image
		wmc.EntrypointOverride = container.Command
		wmc.Args = container.Args

		// Extract resource requirements if present
		if !container.Resources.Requests.Cpu().IsZero() || !container.Resources.Requests.Memory().IsZero() {
			wmc.ResourceInfo = &commonpb.ResourceInfo{
				CpuMillicore: container.Resources.Requests.Cpu().MilliValue(),
				MemoryBytes:  container.Resources.Requests.Memory().Value(),
			}
		}

		// Extract probes if present
		if container.LivenessProbe != nil {
			wmc.LivenessProbe = extractProbeConfig(container.LivenessProbe)
		}
		if container.ReadinessProbe != nil {
			wmc.ReadinessProbe = extractProbeConfig(container.ReadinessProbe)
		}
		if container.StartupProbe != nil {
			wmc.StartupProbe = extractProbeConfig(container.StartupProbe)
		}

		// Get port from container
		if len(container.Ports) > 0 {
			wmc.ServicePort = container.Ports[0].ContainerPort
		}

		// Extract environment variables from container spec
		envVars := make([]*pb.WorkloadManagerConfig_EnvVar, 0, len(container.Env))
		for _, env := range container.Env {
			envVars = append(envVars, &pb.WorkloadManagerConfig_EnvVar{
				Key:   env.Name,
				Value: env.Value,
			})
		}
		wmc.EnvVars = envVars
	}

	// Populate labels from deployment (exclude reserved keys used by selectors)
	if deployment.ObjectMeta.Labels != nil {
		labels := make(map[string]string, len(deployment.ObjectMeta.Labels))
		for k, v := range deployment.ObjectMeta.Labels {
			if _, reserved := reservedLabelKeys[k]; reserved {
				continue
			}
			labels[k] = v
		}
		wmc.Labels = labels
	}

	// Convert deployment status
	deploymentStatus := convertDeploymentStatus(deployment)

	// Construct service endpoint without API call (since service name = deployment name)
	serviceEndpoint := getServiceEndpoint(wmName, deployment.Namespace, wmc.ServicePort)

	return &pb.GetWorkloadManagerResponse{
		Name:                  wmName,
		Namespace:             deployment.Namespace,
		ServiceEndpoint:       serviceEndpoint,
		WorkloadManagerConfig: wmc,
		DeploymentStatus:      deploymentStatus,
		OriginalRequestJson:   "", // Will be populated by calling function if needed
	}
}

// Helper method for GetWorkloadManager - includes original request JSON
func (server *ClusterServer) convertDeploymentToWorkloadManagerResponseForGet(ctx context.Context, deployment *appsv1.Deployment) (*pb.GetWorkloadManagerResponse, error) {
	// Get the core response
	response := convertDeploymentToWorkloadManagerResponseCore(deployment) // Note: no server prefix

	// Get the ConfigMap to retrieve original request JSON
	configMapName := getWorkloadManagerConfigMapName(deployment.Name)
	configMap, err := server.kubeClientSet.CoreV1().ConfigMaps(deployment.Namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, pkg.MapStatusError(err)
		}
		// ConfigMap not found, but we can continue without it
		logrus.Warnf("ConfigMap for workload manager %s not found", deployment.Name)
	} else {
		// Extract original request JSON from ConfigMap
		if requestJson, exists := configMap.Data["request.json"]; exists {
			response.OriginalRequestJson = requestJson
		} else {
			logrus.Warnf("request.json not found in ConfigMap for workload manager %s", deployment.Name)
		}
	}

	return response, nil
}

//go:build e2e

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	ws "cerebras.com/job-operator/controllers/ws"
)

const (
	// Timeouts and intervals for resource operations
	resourceTimeout  = 30 * time.Second
	resourceInterval = 1 * time.Second

	// Timeouts and intervals for pod readiness
	podReadyTimeout  = 60 * time.Second
	podReadyInterval = 2 * time.Second

	// Timeouts and intervals for connectivity checks
	connectTimeout  = 60 * time.Second
	connectInterval = 3 * time.Second
)

// setupTestEnvironment creates all required test components
func setupTestEnvironment(t *testing.T) (context.Context, func(), kubernetes.Interface, pb.ClusterManagementClient) {
	mgr, testEnv, ctx, cancel := common.EnvSetup(true, t)

	wsjobClient := getWsJobClient(t, mgr)
	k8sClientSet := wsjobClient.Clientset

	ctx, cleanup, _, clusterClient := setup(
		false, // with non-root user (testuser, UID 1000, GID 1001)
		wsjobClient.Clientset,
		nil,
		nil,
		false, // not multi-mgmt nodes
	)

	// Create required secrets for testing
	setupRequiredSecrets(t, ctx, k8sClientSet, common.DefaultNamespace)

	cleanupAll := func() {
		// Clean up test secrets
		cleanupTestSecrets(t, ctx, k8sClientSet, common.DefaultNamespace)
		cleanup()
		cancel()
		require.NoError(t, testEnv.Stop())
	}

	return ctx, cleanupAll, k8sClientSet, clusterClient
}

// setupRequiredSecrets creates the necessary secrets for workload manager testing
func setupRequiredSecrets(t *testing.T, ctx context.Context, k8sClient kubernetes.Interface, namespace string) {
	// Create kube-user-auth secret
	kubeUserAuthSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-user-auth",
			Namespace: namespace,
			Labels: map[string]string{
				"created-by": "test-fixture",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"secret": []byte("test-user-auth-secret-key-for-testing-purposes-only"),
		},
	}

	_, err := k8sClient.CoreV1().Secrets(namespace).Create(ctx, kubeUserAuthSecret, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err, "Failed to create kube-user-auth secret")
	}

	t.Logf("Created test secret: kube-user-auth")
}

// cleanupTestSecrets removes the test secrets created by setupRequiredSecrets
func cleanupTestSecrets(t *testing.T, ctx context.Context, k8sClient kubernetes.Interface, namespace string) {
	t.Log("Cleaning up test secrets...")

	// Delete kube-user-auth secret with retry
	require.Eventually(t, func() bool {
		err := k8sClient.CoreV1().Secrets(namespace).Delete(ctx, "kube-user-auth", metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Logf("Retry: Failed to delete kube-user-auth secret: %v", err)
			return false
		}
		return true
	}, 30*time.Second, 1*time.Second, "Failed to delete kube-user-auth secret after retries")

	// Verify secret is actually deleted
	require.Eventually(t, func() bool {
		_, err := k8sClient.CoreV1().Secrets(namespace).Get(ctx, "kube-user-auth", metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 30*time.Second, 1*time.Second, "kube-user-auth secret was not fully deleted")

	t.Log("Test secrets cleanup completed")
}

// setupWorkloadManagerForTesting creates a workload manager deployment, service, and ConfigMap
// directly using the Kubernetes client rather than through the service method
func setupWorkloadManagerForTesting(ctx context.Context, t *testing.T, k8sClient kubernetes.Interface, name string, namespace string) {
	// Default user-style labels
	defaultUserLabels := map[string]string{
		"wm.color":      "blue",
		"wm.deployment": "abc",
	}
	setupWorkloadManagerForTestingWithUserLabels(ctx, t, k8sClient, name, namespace, defaultUserLabels)
}

// setupWorkloadManagerForTestingWithUserLabels is identical to the above, but lets tests provide user labels.
// Reserved labels (app, model) and selector keys are preserved and cannot be overridden by userLabels.
func setupWorkloadManagerForTestingWithUserLabels(
	ctx context.Context,
	t *testing.T,
	k8sClient kubernetes.Interface,
	name string,
	namespace string,
	userLabels map[string]string,
) {
	// Build metadata labels for Deployment
	metaLabels := map[string]string{
		"app":                           workloadManagerIdentifier,
		"component":                     "workload-manager",
		"labels.k8s.cerebras.com/model": name,
	}
	// Merge user labels, skipping reserved keys
	for k, v := range userLabels {
		if k == "app" || k == modelLabelKey {
			continue
		}
		metaLabels[k] = v
	}

	// Build pod template labels (must match selector)
	tmplLabels := map[string]string{
		"app":                           name,
		"component":                     "workload-manager",
		"labels.k8s.cerebras.com/model": name,
	}
	for k, v := range userLabels {
		if k == "app" || k == modelLabelKey {
			continue
		}
		tmplLabels[k] = v
	}

	// Create the deployment (same as before, but use metaLabels/tmplLabels)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    metaLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: common.Pointer(int32(2)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":       name,
					"component": "workload-manager",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: tmplLabels,
					Annotations: map[string]string{
						common.NetworkAttachmentKey: common.DataNetAttachDef,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "workload-manager",
							Image: common.DefaultImage,
							Env: []corev1.EnvVar{
								{
									Name:  "ENV_TEST",
									Value: "test-value",
								},
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: 8000,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Command: []string{
								"sh", "-c",
								`while true; do echo -e 'HTTP/1.1 200 OK\r\n\r\nok' | nc -l -p 8000; done`,
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path:   "/v1/health",
										Port:   intstr.FromInt(8000),
										Scheme: corev1.URISchemeHTTP,
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path:   "/v1/health",
										Port:   intstr.FromInt(8000),
										Scheme: corev1.URISchemeHTTP,
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       5,
							},
						},
					},
					NodeSelector: map[string]string{
						common.NamespaceLabelKey: namespace,
						fmt.Sprintf(ws.RoleLabelKeyF, wsapisv1.WSReplicaTypeCoordinator.Lower()): "",
					},
				},
			},
		},
	}

	// Create the deployment
	createdDeployment, err := k8sClient.AppsV1().Deployments(namespace).Create(ctx, deployment, metav1.CreateOptions{})
	require.NoError(t, err, "Failed to create test deployment")

	// Create owner reference for dependent resources
	ownerRef := metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       createdDeployment.Name,
		UID:        createdDeployment.UID,
		Controller: common.Pointer(true),
	}

	// Create original request JSON for testing
	// TODO: use a more dynamic approach to generate this JSON from the deployment spec above
	originalRequestJSON := fmt.Sprintf(`{
		"name": "%s",
		"workload_manager_config": {
			"image": "%s",
			"replicas": 2,
			"service_port": 8000,
			"resource_info": {
				"cpu_millicore": 500,
				"memory_bytes": 536870912
			},
			"env_vars": [
				{
					"key": "ENV_TEST",
					"value": "test-value"
				}
			]
		}
	}`, name, common.DefaultImage)

	// Create ConfigMap with original request JSON (using the correct naming convention)
	configMapRequestRecord := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            getWorkloadManagerConfigMapName(name), // Use the correct naming function
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"app":                           workloadManagerIdentifier,
				"labels.k8s.cerebras.com/model": name,
			},
		},
		Data: map[string]string{
			"request.json": originalRequestJSON, // Store the original request JSON
		},
	}
	_, err = k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, configMapRequestRecord, metav1.CreateOptions{})
	require.NoError(t, err, "Failed to create test ConfigMap for request record")

	// Create ConfigMap for environment variables (separate from request record)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-env", name),
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"app":        name,
				"component":  "workload-manager",
				"created-by": "test-helper",
			},
		},
		Data: map[string]string{},
	}
	_, err = k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, configMap, metav1.CreateOptions{})
	require.NoError(t, err, "Failed to create test ConfigMap")

	// Create service
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"app":        name,
				"component":  "workload-manager",
				"created-by": "test-helper",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app":       name,
				"component": "workload-manager",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       8000,
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	_, err = k8sClient.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
	require.NoError(t, err, "Failed to create test service")

	// Update deployment to use the ConfigMap
	require.Eventually(t, func() bool {
		// Get the latest version of the deployment to avoid conflicts
		latestDeployment, err := k8sClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Logf("Failed to get latest deployment: %v", err)
			return false
		}

		// Apply our changes to the latest version
		latestDeployment.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{
			{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMap.Name,
					},
				},
			},
		}

		// Attempt the update
		_, err = k8sClient.AppsV1().Deployments(namespace).Update(ctx, latestDeployment, metav1.UpdateOptions{})
		if err != nil {
			t.Logf("Deployment update attempt failed: %v", err)
			return false
		}

		return true
	}, 30*time.Second, 1*time.Second, "Failed to update deployment with ConfigMap reference after multiple attempts")

	// Wait for deployment to be available
	require.Eventually(t, func() bool {
		d, err := k8sClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		for _, condition := range d.Status.Conditions {
			if condition.Type == appsv1.DeploymentAvailable && condition.Status == corev1.ConditionTrue {
				return true
			}
		}
		return false
	}, 60*time.Second, 2*time.Second, "Deployment never became available")
}

// createWorkloadManagerRequestHelper creates a workload manager request with configurable options
func createWorkloadManagerRequestHelper(name string, customizations ...func(*pb.CreateWorkloadManagerRequest)) *pb.CreateWorkloadManagerRequest {
	// Create a default JSON representation for all requests
	defaultJSON := fmt.Sprintf(`{
		"name": "%s",
		"workload_manager_config": {
			"image": "%s",
			"replicas": 2,
			"service_port": 8000,
			"resource_info": {
				"cpu_millicore": 500,
				"memory_bytes": 536870912
			},
			"env_vars": [
				{
					"key": "ENV_TEST",
					"value": "test-value"
				}
			]
		}
	}`, name, common.DefaultImage)

	// Parse JSON into request object
	request := &pb.CreateWorkloadManagerRequest{}
	err := protojson.Unmarshal([]byte(defaultJSON), request)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal default JSON: %v", err))
	}

	// Store the original JSON
	request.OriginalRequestJson = defaultJSON

	// Add default entrypoint override
	request.WorkloadManagerConfig.EntrypointOverride = []string{
		"sh", "-c",
		`while true; do echo -e 'HTTP/1.1 200 OK\r\n\r\nok' | nc -l -p 8000; done`,
	}

	// Apply any customizations
	for _, customize := range customizations {
		customize(request)
	}

	return request
}

// cleanupResources deletes test resources
func cleanupResources(t *testing.T, ctx context.Context, k8sClient kubernetes.Interface, namespace, name string) {
	t.Log("Cleaning up test resources...")
	// Delete deployment
	err := k8sClient.AppsV1().Deployments(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		t.Logf("Warning: Failed to delete deployment %s: %v", name, err)
	}

	// Since we use OwnerReferences, deleting the deployment should also delete the Service and ConfigMap
	// Verify resources are actually gone
	assert.Eventually(t, func() bool {
		_, err := k8sClient.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		return err != nil && strings.Contains(err.Error(), "not found")
	}, 30*time.Second, 1*time.Second, "Deployment was not deleted")

	assert.Eventually(t, func() bool {
		_, err := k8sClient.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
		return err != nil && strings.Contains(err.Error(), "not found")
	}, 30*time.Second, 1*time.Second, "Service was not deleted")

	assert.Eventually(t, func() bool {
		_, err := k8sClient.CoreV1().ConfigMaps(namespace).Get(ctx, getWorkloadManagerConfigMapName(name), metav1.GetOptions{})
		return err != nil && strings.Contains(err.Error(), "not found")
	}, 30*time.Second, 1*time.Second, "ConfigMap was not deleted")
}

func TestCreateWorkloadManager_HappyPath(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Define test parameters
	testName := "test-vm-create"

	// Add labels via customization (instead of hardcoding inside helper)
	request := createWorkloadManagerRequestHelper(testName, func(req *pb.CreateWorkloadManagerRequest) {
		if req.WorkloadManagerConfig.Labels == nil {
			req.WorkloadManagerConfig.Labels = map[string]string{}
		}
		req.WorkloadManagerConfig.Labels["wm.color"] = "blue"
		req.WorkloadManagerConfig.Labels["wm.deployment"] = "abc"
	})

	response, err := clusterClient.CreateWorkloadManager(ctx, request)
	require.NoError(t, err)

	// Get deployment and namespace info
	namespace := response.Namespace

	// Add deferred cleanup to ensure resources are deleted even if test fails
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	// Verify basic response fields
	assert.Equal(t, testName, response.Name)
	assert.Equal(t, common.DefaultNamespace, response.Namespace)

	// Verify the deployment exists using require.Eventually
	var deployment *appsv1.Deployment
	require.Eventually(t, func() bool {
		deployment, err = k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
		return err == nil && deployment != nil
	}, resourceTimeout, resourceInterval, "Deployment was not created")

	// Run assertions on the deployment
	t.Run("Verify deployment properties", func(t *testing.T) {
		// Verify replicas
		assert.Equal(t, int32(2), *deployment.Spec.Replicas, "Incorrect replica count")

		// Verify containers exist
		require.NotEmpty(t, deployment.Spec.Template.Spec.Containers, "No containers found in deployment")

		container := deployment.Spec.Template.Spec.Containers[0]

		// Verify image
		assert.Equal(t, common.DefaultImage, container.Image, "Incorrect container image")

		// Verify ports
		require.NotEmpty(t, container.Ports, "No ports configured")
		assert.Equal(t, int32(8000), container.Ports[0].ContainerPort, "Incorrect port configuration")

		// Assert custom labels are present on Deployment
		assert.Equal(t, "blue", deployment.ObjectMeta.Labels["wm.color"], "deployment label wm.color missing/incorrect")
		assert.Equal(t, "abc", deployment.ObjectMeta.Labels["wm.deployment"], "deployment label wm.deployment missing/incorrect")

		// Verify environment variables
		envFound := false
		for _, env := range container.Env {
			if env.Name == "ENV_TEST" {
				assert.Equal(t, "test-value", env.Value, "Incorrect environment variable value")
				envFound = true
				break
			}
		}
		assert.True(t, envFound, "Environment variable not found in container spec")

		// Verify resource requirements
		cpu := container.Resources.Requests.Cpu()
		mem := container.Resources.Requests.Memory()
		assert.Equal(t, int64(500), cpu.MilliValue(), "Incorrect CPU request")
		assert.Equal(t, int64(512*1024*1024), mem.Value(), "Incorrect memory request")

		// Verify node selector and tolerations
		assert.Contains(t, deployment.Spec.Template.Spec.NodeSelector, fmt.Sprintf(ws.RoleLabelKeyF, wsapisv1.WSReplicaTypeCoordinator.Lower()), "Missing coordinator node selector")

		// Verify pod security context
		t.Run("Verify pod security context", func(t *testing.T) {
			podSecurityContext := deployment.Spec.Template.Spec.SecurityContext
			require.NotNil(t, podSecurityContext, "Pod security context is missing")

			// Check user and group settings at pod level
			require.NotNil(t, podSecurityContext.RunAsUser, "RunAsUser is not set")
			assert.Equal(t, int64(1000), *podSecurityContext.RunAsUser, "Incorrect RunAsUser value")

			require.NotNil(t, podSecurityContext.RunAsGroup, "RunAsGroup is not set")
			assert.Equal(t, int64(1001), *podSecurityContext.RunAsGroup, "Incorrect RunAsGroup value")

			// Verify main container doesn't override pod security context
			containerSecurityContext := deployment.Spec.Template.Spec.Containers[0].SecurityContext
			if containerSecurityContext != nil {
				assert.Nil(t, containerSecurityContext.RunAsUser, "Main container should not override RunAsUser")
				assert.Nil(t, containerSecurityContext.RunAsGroup, "Main container should not override RunAsGroup")
			}

			// Verify init container has root override
			require.NotEmpty(t, deployment.Spec.Template.Spec.InitContainers, "Init containers should exist")
			initContainerSecurityContext := deployment.Spec.Template.Spec.InitContainers[0].SecurityContext
			require.NotNil(t, initContainerSecurityContext, "Init container security context should be set")
			require.NotNil(t, initContainerSecurityContext.RunAsUser, "Init container RunAsUser should be set")
			assert.Equal(t, int64(0), *initContainerSecurityContext.RunAsUser, "Init container should run as root")
			require.NotNil(t, initContainerSecurityContext.RunAsGroup, "Init container RunAsGroup should be set")
			assert.Equal(t, int64(0), *initContainerSecurityContext.RunAsGroup, "Init container should run as root group")
		})

		// Verify network attachment annotation
		t.Run("Verify network attachment annotation", func(t *testing.T) {
			// Verify the network attachment annotation exists in pod template
			podAnnotations := deployment.Spec.Template.ObjectMeta.Annotations
			require.NotNil(t, podAnnotations, "Pod template annotations should not be nil")

			networkAttachment, _ := podAnnotations[common.NetworkAttachmentKey]
			assert.Equal(t, common.DataNetAttachDef, networkAttachment, "Network attachment should use correct value")

			// Log the annotation for verification
			t.Logf("Network attachment annotation: %s=%s", common.NetworkAttachmentKey, networkAttachment)
		})
	})

	// Verify the service exists using require.Eventually
	var service *corev1.Service
	require.Eventually(t, func() bool {
		service, err = k8sClientSet.CoreV1().Services(namespace).Get(ctx, testName, metav1.GetOptions{})
		return err == nil && service != nil
	}, resourceTimeout, resourceInterval, "Service was not created")

	// Run assertions on the service
	t.Run("Verify service properties", func(t *testing.T) {
		// Verify ports
		require.NotEmpty(t, service.Spec.Ports, "No service ports configured")
		assert.Equal(t, int32(8000), service.Spec.Ports[0].Port, "Incorrect service port")

		// Verify selector
		assert.Equal(t, workloadManagerIdentifier, service.Spec.Selector["app"], "Incorrect service selector app")
	})

	// Verify the config record ConfigMap
	t.Run("Verify config record ConfigMap", func(t *testing.T) {
		configRecordName := getWorkloadManagerConfigMapName(testName)
		var configRecord *corev1.ConfigMap

		require.Eventually(t, func() bool {
			var err error
			configRecord, err = k8sClientSet.CoreV1().ConfigMaps(namespace).Get(ctx, configRecordName, metav1.GetOptions{})
			return err == nil && configRecord != nil
		}, resourceTimeout, resourceInterval, "Config record ConfigMap was not created")

		// Verify ConfigMap has the original request JSON only
		assert.Contains(t, configRecord.Data, "request.json", "Request JSON not found in config record")

		// The stored JSON should be exactly what was provided in the request
		expectedJSON := request.OriginalRequestJson
		assert.Equal(t, expectedJSON, configRecord.Data["request.json"],
			"Stored JSON doesn't match original request JSON")

		// Verify it contains the deployment name
		assert.Contains(t, configRecord.Data["request.json"], testName, "Config record doesn't contain deployment name")

		// Verify ConfigMap labels
		assert.Equal(t, workloadManagerIdentifier, configRecord.Labels["app"], "Incorrect app label in config record")

		// Verify it has owner reference to the deployment
		require.NotEmpty(t, configRecord.OwnerReferences, "Config record missing owner references")
		assert.Equal(t, "Deployment", configRecord.OwnerReferences[0].Kind, "Config record has incorrect owner kind")
		assert.Equal(t, deployment.Name, configRecord.OwnerReferences[0].Name, "Config record has incorrect owner name")
	})

	t.Run("Verify pod readiness", func(t *testing.T) {
		require.Eventually(t, func() bool {
			pods, err := k8sClientSet.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=%s", workloadManagerIdentifier),
			})
			if err != nil || len(pods.Items) == 0 {
				return false
			}
			for _, pod := range pods.Items {
				for _, cond := range pod.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						return true
					}
				}
			}
			return false
		}, podReadyTimeout, podReadyInterval, "Pod never became ready")
	})

	t.Run("Verify service endpoint connectivity", func(t *testing.T) {
		// Build the fully qualified service endpoint (with trailing dot)
		serviceEndpoint := getServiceEndpoint(testName, namespace, 8000)
		// Validate that the service endpoint matches what the API returned
		assert.Equal(t, serviceEndpoint, response.ServiceEndpoint,
			"Service endpoint in response doesn't match expected format")

		// Create a debug pod using common.DefaultImage
		debugPodName := "debug-pod"
		debugPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      debugPodName,
				Namespace: namespace,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:    "debug",
						Image:   common.DefaultImage,
						Command: []string{"sleep", "3600"},
					},
				},
				RestartPolicy: corev1.RestartPolicyNever,
			},
		}
		_, err := k8sClientSet.CoreV1().Pods(namespace).Create(ctx, debugPod, metav1.CreateOptions{})
		// Ensure we clean up the debug pod when done
		defer func() {
			err := k8sClientSet.CoreV1().Pods(namespace).Delete(ctx, debugPodName, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				require.NoError(t, err, "failed to delete debug pod")
			}
		}()
		require.NoError(t, err, "failed to create debug pod")

		// Wait until the debug pod is running
		require.Eventually(t, func() bool {
			pod, err := k8sClientSet.CoreV1().Pods(namespace).Get(ctx, debugPodName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			return pod.Status.Phase == corev1.PodRunning
		}, podReadyTimeout, podReadyInterval, "debug pod did not become Running")

		require.Eventually(t, func() bool {
			cmd := exec.Command("kubectl", "exec", "-n", namespace, debugPodName,
				"--", "wget", "-q", "-O", "-", "-T", "5", serviceEndpoint)
			output, err := cmd.CombinedOutput()
			trimmedOutput := strings.TrimSpace(string(output))

			// Log each attempt
			t.Logf("Service response: %s (err: %v)", string(output), err)

			// Return true if we got the expected response
			return err == nil && trimmedOutput == "ok"
		}, connectTimeout, connectInterval, "Failed to get expected response from service endpoint")
	})
}

func TestCreateWorkloadManager_InvalidName(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Test with invalid K8s name (uppercase not allowed)
	invalidName := "Invalid-Name"
	request := createWorkloadManagerRequestHelper(invalidName)

	// Get the namespace from context to use for cleanup
	namespace := common.DefaultNamespace

	// Add deferred cleanup to ensure resources are deleted if somehow creation succeeds
	defer cleanupResources(t, ctx, k8sClientSet, namespace, invalidName)

	_, err := clusterClient.CreateWorkloadManager(ctx, request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid workload manager name")
}

func TestCreateWorkloadManager_MissingImage(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	testName := "test-no-image"
	namespace := common.DefaultNamespace

	request := createWorkloadManagerRequestHelper(testName, func(req *pb.CreateWorkloadManagerRequest) {
		req.WorkloadManagerConfig.Image = ""
	})

	// Add deferred cleanup in case a deployment is created by a bug
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	_, err := clusterClient.CreateWorkloadManager(ctx, request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image: Required value")
}

func TestCreateWorkloadManager_InvalidPort(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	testName := "test-bad-port"
	namespace := common.DefaultNamespace
	badPort := 999999 // Invalid port number

	// Test with invalid port number
	request := createWorkloadManagerRequestHelper(testName, func(req *pb.CreateWorkloadManagerRequest) {
		req.WorkloadManagerConfig.ServicePort = int32(badPort) // Port too high
	})

	// Add deferred cleanup in case a deployment is created by a bug
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	_, err := clusterClient.CreateWorkloadManager(ctx, request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("Invalid value: %d", badPort))
}

func TestWorkloadManagerLogsExistence(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Define test parameters with unique identifiable name
	testName := "log-verify"
	uniqueLogMarker := fmt.Sprintf("test-marker-%d", time.Now().Unix())

	// Create a workload manager that writes identifiable log content
	request := createWorkloadManagerRequestHelper(testName, func(req *pb.CreateWorkloadManagerRequest) {
		req.WorkloadManagerConfig.Replicas = 2
		req.WorkloadManagerConfig.EntrypointOverride = []string{
			"sh", "-c",
			fmt.Sprintf(`echo "STARTUP_MARKER: %s" > /opt/cerebras/log/startup.log && 
				echo "CONFIG_TEST: %s" > /opt/cerebras/log/config-test.log && 
				echo "Initial heartbeat: %s at $(date)" > /opt/cerebras/log/workload-manager.log &&
				echo "Log files created successfully" &&
				ls -la /opt/cerebras/log/ &&
				# Keep the container running by serving HTTP
				while true; do 
					echo "HEARTBEAT: %s at $(date)" >> /opt/cerebras/log/workload-manager.log; 
					echo -e 'HTTP/1.1 200 OK\r\n\r\nok' | nc -l -p 8000; 
					sleep 2; 
				done`, uniqueLogMarker, uniqueLogMarker, uniqueLogMarker, uniqueLogMarker),
		}
	})

	// Call the API to create the workload manager
	response, err := clusterClient.CreateWorkloadManager(ctx, request)
	require.NoError(t, err)
	namespace := response.Namespace

	// Add deferred cleanup
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	// Wait for the deployment to be created
	var deployment *appsv1.Deployment
	require.Eventually(t, func() bool {
		deployment, err = k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
		return err == nil && deployment != nil
	}, resourceTimeout, resourceInterval, "Deployment was not created")

	var workloadManagerNodeName string
	require.Eventually(t, func() bool {
		pods, err := k8sClientSet.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", workloadManagerIdentifier),
		})
		if err != nil {
			t.Logf("Error listing pods: %v", err)
			return false
		}

		if len(pods.Items) == 0 {
			t.Logf("No pods found with selector app=%s", workloadManagerIdentifier)
			return false
		}

		t.Logf("Found %d pods", len(pods.Items))
		for i, pod := range pods.Items {
			t.Logf("Pod %d: %s, Phase: %s, Node: %s", i, pod.Name, pod.Status.Phase, pod.Spec.NodeName)

			// Log any error conditions
			for _, cond := range pod.Status.Conditions {
				if cond.Status != corev1.ConditionTrue {
					t.Logf("Pod %s condition %s: %s - %s", pod.Name, cond.Type, cond.Status, cond.Message)
				}
			}

			if pod.Status.Phase == corev1.PodRunning && pod.Spec.NodeName != "" {
				for _, cond := range pod.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						workloadManagerNodeName = pod.Spec.NodeName
						t.Logf("Found ready pod %s on node %s", pod.Name, pod.Spec.NodeName)
						return true
					}
				}
			}
		}
		return false
	}, podReadyTimeout, podReadyInterval, "Pod never became ready")

	// Test that logs are written to the host path
	t.Run("Verify logs exist on host path", func(t *testing.T) {
		expectedLogPath := workloadManagerLogBasePath
		// Create log checker pod that runs on the same node as the workload manager
		logCheckerPodName := "log-check-pod"
		logCheckerPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      logCheckerPodName,
				Namespace: namespace,
			},
			Spec: corev1.PodSpec{
				//Ensure this pod runs on the same node as the workload manager
				NodeName: workloadManagerNodeName,
				Containers: []corev1.Container{
					{
						Name:  "log-checker",
						Image: common.DefaultImage,
						Command: []string{
							"sh", "-c",
							fmt.Sprintf(`
							echo "=== Log Checker Starting ==="
							echo "Node: $(hostname)"
							echo "Checking host path: %[1]s"
							echo "Target marker: %[2]s"
							echo "Current time: $(date)"
							echo "================================"
							
							# Check if the mount point exists
							if [ ! -d "%[1]s" ]; then
								echo "ERROR: Mount point %[1]s does not exist"
								exit 1
							fi
							
							echo "✓ Host mount point exists"
							
							# Retry loop: check every 1s for up to 60s
							MAX_ATTEMPTS=60
							ATTEMPT=0
							
							while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
								ATTEMPT=$((ATTEMPT + 1))
								echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Checking for startup.log with marker..."
								
								# Check for startup.log with our marker
								if [ -f "%[1]s/startup.log" ] && grep -q "%[2]s" %[1]s/startup.log; then
									echo "✓ Found expected marker in startup.log on attempt $ATTEMPT"
									exit 0
								fi
								
								if [ $ATTEMPT -lt $MAX_ATTEMPTS ]; then
									echo "Marker not found yet, waiting 1 second before retry..."
									sleep 1
								fi
							done
							
							echo "✗ startup.log not found or marker missing after $MAX_ATTEMPTS attempts"
							echo "Final check - listing directory contents:"
							ls -la %[1]s/ || echo "Directory listing failed"
							echo "Checking if startup.log exists:"
							[ -f "%[1]s/startup.log" ] && echo "startup.log exists" || echo "startup.log does not exist"
							if [ -f "%[1]s/startup.log" ]; then
								echo "startup.log contents:"
								cat %[1]s/startup.log
							fi
							exit 1
                        `, expectedLogPath, uniqueLogMarker),
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "host-log-dir",
								MountPath: expectedLogPath,
							},
						},
					},
				},
				Volumes: []corev1.Volume{
					{
						Name: "host-log-dir",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: workloadManagerLogBasePath,
							},
						},
					},
				},
				RestartPolicy: corev1.RestartPolicyNever,
			},
		}

		_, err := k8sClientSet.CoreV1().Pods(namespace).Create(ctx, logCheckerPod, metav1.CreateOptions{})
		require.NoError(t, err, "failed to create log checker pod")

		// Ensure we clean up the log checker pod
		defer func() {
			err := k8sClientSet.CoreV1().Pods(namespace).Delete(ctx, logCheckerPodName, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				require.NoError(t, err, "failed to delete debug pod")
			}
		}()

		// Wait for the log checker pod to complete successfully
		require.Eventually(t, func() bool {
			pod, err := k8sClientSet.CoreV1().Pods(namespace).Get(ctx, logCheckerPodName, metav1.GetOptions{})
			if err != nil {
				t.Logf("Error getting log checker pod: %v", err)
				return false
			}

			t.Logf("Log checker pod status: %s (node: %s)", pod.Status.Phase, pod.Spec.NodeName)

			// Get logs if pod has completed
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				logBytes, logErr := k8sClientSet.CoreV1().Pods(namespace).GetLogs(logCheckerPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
				if logErr == nil {
					t.Logf("=== %s LOGS (phase=%s) ===\n%s\n=== END LOGS ===", logCheckerPodName, pod.Status.Phase, string(logBytes))
				}
			}

			if pod.Status.Phase == corev1.PodFailed {
				t.Logf("Log checker pod failed, checking conditions:")
				for _, cond := range pod.Status.Conditions {
					t.Logf("Condition %s: %s - %s", cond.Type, cond.Status, cond.Message)
				}
				return false
			}

			return pod.Status.Phase == corev1.PodSucceeded
		}, 90*time.Second, 3*time.Second, "Log checking pod did not complete successfully")

		t.Logf("✓ Successfully verified that workload manager logs are written to host path")
	})
}

func TestCreateWorkloadManager_MissingOriginalJSON(t *testing.T) {
	ctx, cleanup, _, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	testName := "test-missing-json"

	// Create request with nil original JSON
	request := createWorkloadManagerRequestHelper(testName, func(req *pb.CreateWorkloadManagerRequest) {
		req.OriginalRequestJson = "" // Empty the required field
	})

	// Call the API, expect error
	_, err := clusterClient.CreateWorkloadManager(ctx, request)
	require.Error(t, err, "Should return error when original_request_json is missing")
	assert.Contains(t, err.Error(), "original_request_json is required",
		"Error should indicate original_request_json is required")

	// Verify gRPC status code
	statusErr, ok := status.FromError(err)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.InvalidArgument, statusErr.Code(),
		"Should return InvalidArgument status code")
}

func TestGetWorkloadManager(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Define test parameters
	testName := "test-vm-get"
	namespace := common.DefaultNamespace

	// Create the workload manager resources directly using k8s client
	setupWorkloadManagerForTesting(ctx, t, k8sClientSet, testName, namespace)

	// Add deferred cleanup
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	// Wait for the deployment to be created
	require.Eventually(t, func() bool {
		deployment, err := k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
		return err == nil && deployment != nil
	}, 30*time.Second, 1*time.Second, "Deployment was not created")

	// Wait for pod to be ready
	require.Eventually(t, func() bool {
		pods, err := k8sClientSet.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", testName),
		})
		if err != nil || len(pods.Items) == 0 {
			return false
		}

		for _, pod := range pods.Items {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return true
				}
			}
		}
		return false
	}, 60*time.Second, 2*time.Second, "Pod never became ready")

	// Give the container a moment to write logs
	time.Sleep(5 * time.Second)

	// Execute docker command to check if log files exist in the control plane container
	// Call the GetWorkloadManager API
	response, err := clusterClient.GetWorkloadManager(ctx, &pb.GetWorkloadManagerRequest{Name: testName})
	require.NoError(t, err, "Failed to get workload manager")

	// Verify basic response fields
	assert.Equal(t, testName, response.Name, "Incorrect name in response")
	assert.Equal(t, namespace, response.Namespace, "Incorrect namespace in response")
	assert.Contains(t, response.ServiceEndpoint, testName, "Service endpoint should contain workload manager name")
	assert.Contains(t, response.ServiceEndpoint, fmt.Sprintf(":%d", 8000), "Service endpoint should contain port")

	// Verify workload manager config
	assert.NotNil(t, response.WorkloadManagerConfig, "WorkloadManagerConfig should not be nil")
	assert.Equal(t, int32(2), response.WorkloadManagerConfig.Replicas, "Incorrect replica count")
	assert.Equal(t, common.DefaultImage, response.WorkloadManagerConfig.Image, "Incorrect image")
	assert.Equal(t, int32(8000), response.WorkloadManagerConfig.ServicePort, "Incorrect service port")

	// Verify entrypoint command
	assert.NotEmpty(t, response.WorkloadManagerConfig.EntrypointOverride, "Entrypoint override should not be empty")
	assert.Equal(t, "sh", response.WorkloadManagerConfig.EntrypointOverride[0], "Incorrect entrypoint command")

	// Verify resource requirements
	assert.NotNil(t, response.WorkloadManagerConfig.ResourceInfo, "ResourceInfo should not be nil")
	assert.Equal(t, int64(500), response.WorkloadManagerConfig.ResourceInfo.CpuMillicore, "Incorrect CPU request")
	assert.Equal(t, int64(512*1024*1024), response.WorkloadManagerConfig.ResourceInfo.MemoryBytes, "Incorrect memory request")

	// Verify probes
	assert.NotNil(t, response.WorkloadManagerConfig.LivenessProbe, "LivenessProbe should not be nil")
	assert.NotNil(t, response.WorkloadManagerConfig.ReadinessProbe, "ReadinessProbe should not be nil")
	assert.Equal(t, int32(30), response.WorkloadManagerConfig.LivenessProbe.InitialDelaySeconds, "Incorrect liveness probe initial delay")
	assert.Equal(t, int32(10), response.WorkloadManagerConfig.LivenessProbe.PeriodSeconds, "Incorrect liveness probe period")

	// Verify environment variables
	assert.NotEmpty(t, response.WorkloadManagerConfig.EnvVars, "Environment variables should not be empty")
	envMap := make(map[string]string)
	for _, env := range response.WorkloadManagerConfig.EnvVars {
		envMap[env.Key] = env.Value
	}
	assert.Equal(t, "test-value", envMap["ENV_TEST"], "Missing or incorrect ENV_TEST environment variable")

	// Verify deployment status
	assert.NotNil(t, response.DeploymentStatus, "DeploymentStatus should not be nil")
	assert.NotZero(t, response.DeploymentStatus.ObservedGeneration, "ObservedGeneration should not be zero")
	assert.NotEmpty(t, response.DeploymentStatus.Conditions, "Deployment conditions should not be empty")

	// Verify at least one deployment condition
	foundAvailableCondition := false
	for _, condition := range response.DeploymentStatus.Conditions {
		if condition.Type == string(appsv1.DeploymentAvailable) && condition.Status == string(corev1.ConditionTrue) {
			foundAvailableCondition = true
			break
		}
	}
	assert.True(t, foundAvailableCondition, "Should have an Available condition with status True")

	// Verify original request JSON is returned
	assert.NotEmpty(t, response.OriginalRequestJson, "OriginalRequestJson should not be empty")
	assert.Contains(t, response.OriginalRequestJson, testName, "OriginalRequestJson should contain the workload manager name")
	assert.Contains(t, response.OriginalRequestJson, common.DefaultImage, "OriginalRequestJson should contain the image name")
	assert.Contains(t, response.OriginalRequestJson, "ENV_TEST", "OriginalRequestJson should contain environment variable")

	// Verify it's valid JSON
	var parsedJSON map[string]interface{}
	err = json.Unmarshal([]byte(response.OriginalRequestJson), &parsedJSON)
	assert.NoError(t, err, "OriginalRequestJson should be valid JSON")

	// Verify labels are returned from Get
	require.NotNil(t, response.WorkloadManagerConfig.Labels, "Labels map should not be nil")
	labels := response.WorkloadManagerConfig.Labels

	// Custom/user labels should be present
	assert.Equal(t, "blue", labels["wm.color"], "label wm.color missing/incorrect")
	assert.Equal(t, "abc", labels["wm.deployment"], "label wm.deployment missing/incorrect")
	assert.Equal(t, "workload-manager", labels["component"], "label component missing/incorrect")

	// Reserved labels should be excluded from the map
	assert.NotContains(t, labels, "app", "reserved label 'app' should not be returned")
	assert.NotContains(t, labels, modelLabelKey, "reserved model label should not be returned")
}

func TestDeleteWorkloadManager_HappyPath(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Define test parameters
	testName := "test-vm-delete"
	namespace := common.DefaultNamespace

	// Create the workload manager resources directly using k8s client
	setupWorkloadManagerForTesting(ctx, t, k8sClientSet, testName, namespace)

	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	// Verify the resources exist before deletion
	deployment, err := k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
	require.NoError(t, err, "Deployment should exist before deletion")
	require.NotNil(t, deployment, "Deployment should not be nil")

	service, err := k8sClientSet.CoreV1().Services(namespace).Get(ctx, testName, metav1.GetOptions{})
	require.NoError(t, err, "Service should exist before deletion")
	require.NotNil(t, service, "Service should not be nil")

	configMapName := fmt.Sprintf("%s-env", testName)
	configMap, err := k8sClientSet.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})
	require.NoError(t, err, "ConfigMap should exist before deletion")
	require.NotNil(t, configMap, "ConfigMap should not be nil")

	// Call the DeleteWorkloadManager API
	response, err := clusterClient.DeleteWorkloadManager(ctx, &pb.DeleteWorkloadManagerRequest{Name: testName})
	require.NoError(t, err, "Failed to delete workload manager")

	// Verify response fields
	assert.Equal(t, testName, response.Name, "Incorrect name in response")
	assert.Equal(t, namespace, response.Namespace, "Incorrect namespace in response")

	// Verify that resources are deleted (with foreground propagation, this might take a moment)
	require.Eventually(t, func() bool {
		_, err := k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 60*time.Second, 1*time.Second, "Deployment was not deleted")

	require.Eventually(t, func() bool {
		_, err := k8sClientSet.CoreV1().Services(namespace).Get(ctx, testName, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 30*time.Second, 1*time.Second, "Service was not deleted")

	require.Eventually(t, func() bool {
		_, err := k8sClientSet.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 30*time.Second, 1*time.Second, "ConfigMap was not deleted")
}

func TestDeleteWorkloadManager_NotFound(t *testing.T) {
	ctx, cleanup, _, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Call delete on a non-existent workload manager
	nonExistentName := "non-existent-workload-manager"
	_, err := clusterClient.DeleteWorkloadManager(ctx, &pb.DeleteWorkloadManagerRequest{Name: nonExistentName})

	// Verify the error
	require.Error(t, err, "Should return error when deleting non-existent workload manager")
	assert.Contains(t, err.Error(), "not found", "Error should indicate resource not found")

	// Verify the gRPC status code
	statusErr, ok := status.FromError(err)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.NotFound, statusErr.Code(), "Should return NotFound status code")
}

func TestDeleteWorkloadManager_MissingName(t *testing.T) {
	ctx, cleanup, _, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Call delete with empty name
	_, err := clusterClient.DeleteWorkloadManager(ctx, &pb.DeleteWorkloadManagerRequest{Name: ""})

	// Verify the error
	require.Error(t, err, "Should return error when name is empty")
	assert.Contains(t, err.Error(), "required", "Error should indicate name is required")

	// Verify the gRPC status code
	statusErr, ok := status.FromError(err)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.InvalidArgument, statusErr.Code(), "Should return InvalidArgument status code")
}

func TestUpdateWorkloadManager_HappyPath(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Define test parameters
	testName := "test-vm-update"
	namespace := common.DefaultNamespace
	originalReplicas := int32(2)
	updatedReplicas := int32(3)

	// Create a workload manager for testing
	setupWorkloadManagerForTesting(ctx, t, k8sClientSet, testName, namespace)

	// Add deferred cleanup
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	// Wait for deployment to be created and stable
	var deployment *appsv1.Deployment
	require.Eventually(t, func() bool {
		var err error
		deployment, err = k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
		if err != nil || deployment == nil {
			return false
		}

		// Check for deployment readiness
		for _, condition := range deployment.Status.Conditions {
			if condition.Type == appsv1.DeploymentAvailable &&
				condition.Status == corev1.ConditionTrue {
				return true
			}
		}
		return false
	}, 60*time.Second, 2*time.Second, "Deployment did not become available")

	// Verify original replica count
	assert.Equal(t, originalReplicas, *deployment.Spec.Replicas, "Initial replica count should match")

	// Create update request - currently only replicas is supported
	updateRequest := &pb.UpdateWorkloadManagerRequest{
		Name: testName,
		WorkloadManagerConfig: &pb.WorkloadManagerConfig{
			Replicas: updatedReplicas,
			// Set other fields that might be supported in the future
			Image: "updated-image:latest",
			ResourceInfo: &commonpb.ResourceInfo{
				CpuMillicore: 1000,
				MemoryBytes:  1024 * 1024 * 1024, // 1GB
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"workload_manager_config.replicas"},
		},
	}

	// Call the UpdateWorkloadManager API
	response, err := clusterClient.UpdateWorkloadManager(ctx, updateRequest)
	require.NoError(t, err, "Failed to update workload manager")

	// Verify response fields
	assert.Equal(t, testName, response.Name, "Name in response should match request")
	assert.Equal(t, namespace, response.Namespace, "Namespace in response should match expected")
	assert.Contains(t, response.ServiceEndpoint, testName, "Service endpoint should contain the workload manager name")

	// Verify that the deployment was updated
	updatedDeployment, err := k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
	require.NoError(t, err, "Failed to get updated deployment")

	// Verify that the replicas field was updated
	assert.Equal(t, updatedReplicas, *updatedDeployment.Spec.Replicas, "Replicas should be updated")

	// Verify that other fields were NOT updated (currently only replicas is supported)
	assert.NotEqual(t, "updated-image:latest", updatedDeployment.Spec.Template.Spec.Containers[0].Image,
		"Image should not be updated as it's not in the update mask")

	// Get CPU resources and verify they're unchanged
	cpuMillis := updatedDeployment.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
	assert.Equal(t, int64(500), cpuMillis, "CPU resources should remain unchanged")
}

func TestUpdateWorkloadManager_LabelsAllOrNothing(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	testName := "test-vm-update-labels"
	namespace := common.DefaultNamespace

	// Create initial WM with default labels from setup helper
	setupWorkloadManagerForTesting(ctx, t, k8sClientSet, testName, namespace)
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	// Wait for deployment to exist
	require.Eventually(t, func() bool {
		_, err := k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
		return err == nil
	}, 30*time.Second, 1*time.Second)

	// New labels set (four labels), “all-or-nothing” replacement of non-reserved labels
	newLabels := map[string]string{
		"wm.color":      "green",
		"wm.deployment": "blue-green",
		"env":           "prod",
		"phase":         "beta",
	}

	updateReq := &pb.UpdateWorkloadManagerRequest{
		Name: testName,
		WorkloadManagerConfig: &pb.WorkloadManagerConfig{
			Labels: newLabels,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"workload_manager_config.labels"},
		},
	}

	_, err := clusterClient.UpdateWorkloadManager(ctx, updateReq)
	require.NoError(t, err, "Failed to update labels")

	// Fetch updated deployment and service
	deployment, err := k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, testName, metav1.GetOptions{})
	require.NoError(t, err)

	// Build reserved set dynamically: system + selector keys (deployment + service)
	reserved := map[string]struct{}{
		"app":         {},
		modelLabelKey: {},
	}
	for k := range deployment.Spec.Selector.MatchLabels {
		reserved[k] = struct{}{}
	}

	t.Run("deployment labels replaced", func(t *testing.T) {
		// Reserved keys must be present and untouched
		require.Equal(t, workloadManagerIdentifier, deployment.ObjectMeta.Labels["app"], "reserved label app should remain unchanged")
		require.Equal(t, testName, deployment.ObjectMeta.Labels[modelLabelKey], "reserved model label should remain unchanged")

		// Build trimmed map without reserved (system + selector keys)
		trim := map[string]string{}
		for k, v := range deployment.ObjectMeta.Labels {
			if _, isReserved := reserved[k]; isReserved {
				continue
			}
			trim[k] = v
		}
		// Expect exactly newLabels
		require.Equal(t, newLabels, trim, "non-reserved labels should match exactly the provided set")
	})

}

func TestUpdateWorkloadManager_MissingUpdateMask(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Define test parameters
	testName := "test-vm-update-nomask"
	namespace := common.DefaultNamespace

	// Create a workload manager for testing
	setupWorkloadManagerForTesting(ctx, t, k8sClientSet, testName, namespace)
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	// Prepare update request with no update mask
	updateRequest := &pb.UpdateWorkloadManagerRequest{
		Name: testName,
		WorkloadManagerConfig: &pb.WorkloadManagerConfig{
			Replicas: 4,
		},
		// No UpdateMask
	}

	// Call the UpdateWorkloadManager API
	_, err := clusterClient.UpdateWorkloadManager(ctx, updateRequest)

	// Verify error response
	require.Error(t, err, "Should return error when update_mask is missing")
	assert.Contains(t, err.Error(), "update_mask is required", "Error should indicate update_mask is required")

	// Verify gRPC status code
	statusErr, ok := status.FromError(err)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.InvalidArgument, statusErr.Code(), "Should return InvalidArgument status code")
}

func TestUpdateWorkloadManager_NotFound(t *testing.T) {
	ctx, cleanup, _, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Call update on a non-existent workload manager
	nonExistentName := "non-existent-workload-manager"
	updateRequest := &pb.UpdateWorkloadManagerRequest{
		Name: nonExistentName,
		WorkloadManagerConfig: &pb.WorkloadManagerConfig{
			Replicas: 3,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"workload_manager_config.replicas"},
		},
	}

	// Call the UpdateWorkloadManager API
	_, err := clusterClient.UpdateWorkloadManager(ctx, updateRequest)

	// Verify error
	require.Error(t, err, "Should return error when updating non-existent workload manager")
	assert.Contains(t, err.Error(), "not found", "Error should indicate resource not found")

	// Verify gRPC status code
	statusErr, ok := status.FromError(err)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.NotFound, statusErr.Code(), "Should return NotFound status code")
}

func TestUpdateWorkloadManager_InvalidField(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Define test parameters
	testName := "test-vm-update-invalid"
	namespace := common.DefaultNamespace

	// Create a workload manager for testing
	setupWorkloadManagerForTesting(ctx, t, k8sClientSet, testName, namespace)
	defer cleanupResources(t, ctx, k8sClientSet, namespace, testName)

	// Prepare update request with invalid field
	updateRequest := &pb.UpdateWorkloadManagerRequest{
		Name: testName,
		WorkloadManagerConfig: &pb.WorkloadManagerConfig{
			Replicas: 4,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"workload_manager_config.invalid_field"},
		},
	}

	// Call the UpdateWorkloadManager API
	_, err := clusterClient.UpdateWorkloadManager(ctx, updateRequest)

	// Verify error
	require.Error(t, err, "Should return error when update_mask contains invalid field")
	assert.Contains(t, err.Error(), "unsupported update field", "Error should indicate invalid field")

	// Verify gRPC status code
	statusErr, ok := status.FromError(err)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.InvalidArgument, statusErr.Code(), "Should return InvalidArgument status code")
}

func TestListWorkloadManager_HappyPath(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	namespace := common.DefaultNamespace

	// Create multiple workload managers for testing
	testNames := []string{"test-wm-list-1", "test-wm-list-2", "test-wm-list-3"}

	// Create workload managers
	for _, name := range testNames {
		setupWorkloadManagerForTesting(ctx, t, k8sClientSet, name, namespace)
		defer cleanupResources(t, ctx, k8sClientSet, namespace, name)
	}

	// Wait for all deployments to be available
	// Wait for deployments to be available concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, len(testNames))

	for _, name := range testNames {
		wg.Add(1)
		go func(deploymentName string) {
			defer wg.Done()

			// Use a separate context with timeout for each goroutine
			deployCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			success := false
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					deployment, err := k8sClientSet.AppsV1().Deployments(namespace).Get(deployCtx, deploymentName, metav1.GetOptions{})
					if err != nil {
						continue // Keep trying
					}

					for _, condition := range deployment.Status.Conditions {
						if condition.Type == appsv1.DeploymentAvailable && condition.Status == corev1.ConditionTrue {
							t.Logf("Deployment %s became available", deploymentName)
							success = true
							return
						}
					}

				case <-deployCtx.Done():
					if !success {
						errChan <- fmt.Errorf("deployment %s did not become available within timeout", deploymentName)
					}
					return
				}
			}
		}(name)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check if any deployments failed to become available
	var deploymentErrors []string
	for err := range errChan {
		deploymentErrors = append(deploymentErrors, err.Error())
	}

	if len(deploymentErrors) > 0 {
		t.Fatalf("Some deployments failed to become available:\n%s", strings.Join(deploymentErrors, "\n"))
	}

	t.Logf("All %d deployments became available", len(testNames))

	// Test listing workload managers
	listRequest := &pb.ListWorkloadManagerRequest{
		PageSize: 10,
	}

	response, err := clusterClient.ListWorkloadManager(ctx, listRequest)
	require.NoError(t, err, "Failed to list workload managers")

	// Verify response
	assert.NotNil(t, response, "Response should not be nil")
	assert.GreaterOrEqual(t, len(response.WorkloadManagers), len(testNames),
		"Should return at least the workload managers we created")

	// Build a set of the names we created so we only assert labels on those
	expectedNames := map[string]bool{}
	for _, n := range testNames {
		expectedNames[n] = true
	}

	// Verify each workload manager in the response
	foundNames := make(map[string]bool)
	for _, wm := range response.WorkloadManagers {
		foundNames[wm.Name] = true

		// Basic validations
		assert.NotEmpty(t, wm.Name, "Workload manager name should not be empty")
		assert.Equal(t, namespace, wm.Namespace, "Namespace should match")
		assert.NotEmpty(t, wm.ServiceEndpoint, "Service endpoint should not be empty")
		assert.NotNil(t, wm.WorkloadManagerConfig, "Config should not be nil")
		assert.NotNil(t, wm.DeploymentStatus, "Deployment status should not be nil")

		// Verify service endpoint format
		assert.Contains(t, wm.ServiceEndpoint, wm.Name, "Service endpoint should contain workload manager name")
		assert.Contains(t, wm.ServiceEndpoint, ":8000", "Service endpoint should contain port")

		// Verify config details
		assert.Equal(t, int32(2), wm.WorkloadManagerConfig.Replicas, "Should have 2 replicas")
		assert.Equal(t, common.DefaultImage, wm.WorkloadManagerConfig.Image, "Should have correct image")
		assert.Equal(t, int32(8000), wm.WorkloadManagerConfig.ServicePort, "Should have correct port")

		// Verify environment variables from ConfigMap
		assert.NotEmpty(t, wm.WorkloadManagerConfig.EnvVars, "Should have environment variables")
		envMap := make(map[string]string)
		for _, env := range wm.WorkloadManagerConfig.EnvVars {
			envMap[env.Key] = env.Value
		}
		assert.Equal(t, "test-value", envMap["ENV_TEST"], "Should have ENV_TEST environment variable")

		// Verify labels: only assert for the ones created by this test
		if expectedNames[wm.Name] {
			require.NotNil(t, wm.WorkloadManagerConfig.Labels, "Labels map should not be nil")
			labels := wm.WorkloadManagerConfig.Labels

			// Custom/user labels from setupWorkloadManagerForTesting
			assert.Equal(t, "blue", labels["wm.color"], "label wm.color missing/incorrect")
			assert.Equal(t, "abc", labels["wm.deployment"], "label wm.deployment missing/incorrect")
			assert.Equal(t, "workload-manager", labels["component"], "label component missing/incorrect")

			// Reserved labels should be excluded
			assert.NotContains(t, labels, "app", "reserved label 'app' should not be returned")
			assert.NotContains(t, labels, modelLabelKey, "reserved model label should not be returned")
		}

		// Original request JSON is not included in List
		assert.Empty(t, wm.OriginalRequestJson, "OriginalRequestJson should be empty")
	}

	// Verify all our test workload managers were found
	for _, expectedName := range testNames {
		assert.True(t, foundNames[expectedName],
			fmt.Sprintf("Expected workload manager %s not found in list", expectedName))
	}
}

func TestListWorkloadManager_EmptyNamespace(t *testing.T) {
	ctx, cleanup, _, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// Create a new context WITHOUT the resource-namespace header
	// This simulates a request that doesn't include the namespace metadata
	md := metadata.New(map[string]string{
		// Intentionally omit ResourceNamespaceHeader
		"x-forwarded-host": "test.cluster-server.mbx.cerebrassc.local", // Required by interceptor
	})
	ctxWithoutNamespace := metadata.NewOutgoingContext(ctx, md)

	// Test with empty namespace header
	request := &pb.ListWorkloadManagerRequest{
		PageSize: 10,
	}

	_, err := clusterClient.ListWorkloadManager(ctxWithoutNamespace, request)
	require.Error(t, err, "Should return error when namespace is empty")
	assert.Contains(t, err.Error(), "Request is not allowed to access resources in the", "Error should indicate namespace is required")

	// Verify gRPC status code
	statusErr, ok := status.FromError(err)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.PermissionDenied, statusErr.Code(), "Should return InvalidArgument status code")
}

func TestListWorkloadManager_NoWorkloadManagers(t *testing.T) {
	ctx, cleanup, _, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	// List workload managers in empty namespace
	request := &pb.ListWorkloadManagerRequest{
		PageSize: 10,
	}

	response, err := clusterClient.ListWorkloadManager(ctx, request)
	require.NoError(t, err, "Should not return error for empty namespace")

	// Should return empty list
	assert.NotNil(t, response, "Response should not be nil")
	assert.Empty(t, response.WorkloadManagers, "Should return empty list when no workload managers exist")
	assert.Empty(t, response.NextPageToken, "Should not have next page token when list is empty")
}

func TestListWorkloadManager_Pagination(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	namespace := common.DefaultNamespace

	// Create multiple workload managers for testing pagination
	testNames := []string{"test-page-1", "test-page-2", "test-page-3", "test-page-4", "test-page-5"}

	// Set up cleanup for all workload managers BEFORE creating them
	for _, name := range testNames {
		defer cleanupResources(t, ctx, k8sClientSet, namespace, name)
	}

	// Create workload managers concurrently
	var createWg sync.WaitGroup
	createErrChan := make(chan error, len(testNames))

	t.Logf("Creating %d workload managers concurrently...", len(testNames))
	for _, name := range testNames {
		createWg.Add(1)
		go func(wmName string) {
			defer createWg.Done()
			// NO defer cleanup here - it's handled outside the goroutines

			// Create deployment concurrently
			func() {
				defer func() {
					if r := recover(); r != nil {
						createErrChan <- fmt.Errorf("panic creating %s: %v", wmName, r)
					}
				}()

				setupWorkloadManagerForTesting(ctx, t, k8sClientSet, wmName, namespace)
				t.Logf("Successfully created workload manager %s", wmName)
			}()
		}(name)
	}

	// Wait for all creations to complete
	createWg.Wait()
	close(createErrChan)

	// Check if any creations failed
	var createErrors []string
	for err := range createErrChan {
		createErrors = append(createErrors, err.Error())
	}

	if len(createErrors) > 0 {
		t.Fatalf("Some workload managers failed to create:\n%s", strings.Join(createErrors, "\n"))
	}

	t.Logf("All %d workload managers created successfully", len(testNames))

	// Test pagination with page size of 2 - collect all results
	allWorkloadManagers := []*pb.GetWorkloadManagerResponse{}
	pageNumber := 1
	nextPageToken := ""

	t.Logf("Starting pagination test with page size 2")

	// Page through all results
	for {
		t.Logf("Requesting page %d (token: %s)", pageNumber, nextPageToken)

		pageRequest := &pb.ListWorkloadManagerRequest{
			PageSize:  2,
			PageToken: nextPageToken,
		}

		pageResponse, err := clusterClient.ListWorkloadManager(ctx, pageRequest)
		require.NoError(t, err, fmt.Sprintf("Failed to get page %d", pageNumber))

		t.Logf("Page %d returned %d workload managers", pageNumber, len(pageResponse.WorkloadManagers))

		// Collect results from this page
		for _, wm := range pageResponse.WorkloadManagers {
			allWorkloadManagers = append(allWorkloadManagers, wm)
		}

		// Check if we're done
		if pageResponse.NextPageToken == "" {
			t.Logf("Reached end of pagination at page %d", pageNumber)
			break
		}

		// Move to next page
		nextPageToken = pageResponse.NextPageToken
		pageNumber++

		// Safety check to prevent infinite loops
		if pageNumber > 10 {
			t.Fatalf("Too many pages - possible infinite loop")
		}
	}

	// Verify we got the expected number of pages (3 pages for 5 items with page size 2)
	assert.Equal(t, 3, pageNumber, "Should have exactly 3 pages for 5 workload managers with page size 2")

	// Verify total count matches what we created
	assert.Equal(t, len(testNames), len(allWorkloadManagers),
		"Total workload managers across all pages should match created count")

	// Verify no duplicates across pages
	seenNames := make(map[string]bool)
	duplicates := []string{}
	for _, wm := range allWorkloadManagers {
		if seenNames[wm.Name] {
			duplicates = append(duplicates, wm.Name)
		}
		seenNames[wm.Name] = true
	}
	assert.Empty(t, duplicates, "Should not have duplicate workload managers across pages")

	// Verify all expected workload managers were found
	foundNames := make(map[string]bool)
	for _, wm := range allWorkloadManagers {
		foundNames[wm.Name] = true
	}

	missingNames := []string{}
	for _, expectedName := range testNames {
		if !foundNames[expectedName] {
			missingNames = append(missingNames, expectedName)
		}
	}
	assert.Empty(t, missingNames, "All created workload managers should be found across all pages")

	// Log the complete results for verification
	allNames := []string{}
	for _, wm := range allWorkloadManagers {
		allNames = append(allNames, wm.Name)
	}
	t.Logf("Complete pagination results: %v", allNames)

	// Verify each workload manager has correct properties
	for _, wm := range allWorkloadManagers {
		// Basic validations
		assert.NotEmpty(t, wm.Name, "Workload manager name should not be empty")
		assert.Equal(t, namespace, wm.Namespace, "Namespace should match")
		assert.NotEmpty(t, wm.ServiceEndpoint, "Service endpoint should not be empty")
		assert.NotNil(t, wm.WorkloadManagerConfig, "Config should not be nil")
		assert.NotNil(t, wm.DeploymentStatus, "Deployment status should not be nil")

		// Verify service endpoint format
		assert.Contains(t, wm.ServiceEndpoint, wm.Name, "Service endpoint should contain workload manager name")
		assert.Contains(t, wm.ServiceEndpoint, ":8000", "Service endpoint should contain port")

		// Verify config details
		assert.Equal(t, int32(2), wm.WorkloadManagerConfig.Replicas, "Should have 2 replicas")
		assert.Equal(t, common.DefaultImage, wm.WorkloadManagerConfig.Image, "Should have correct image")
		assert.Equal(t, int32(8000), wm.WorkloadManagerConfig.ServicePort, "Should have correct port")

		// Verify original request JSON is returned
		assert.Empty(t, wm.OriginalRequestJson, "OriginalRequestJson should be empty")
	}
}

func TestListWorkloadManager_FilterByLabels(t *testing.T) {
	ctx, cleanup, k8sClientSet, clusterClient := setupTestEnvironment(t)
	defer cleanup()

	namespace := common.DefaultNamespace

	// Create two workload managers via helper (initial labels: wm.color=blue, wm.deployment=abc)
	nameBlue := "wm-filter-blue"
	nameGreen := "wm-filter-green"
	setupWorkloadManagerForTesting(ctx, t, k8sClientSet, nameBlue, namespace)
	defer cleanupResources(t, ctx, k8sClientSet, namespace, nameBlue)
	setupWorkloadManagerForTesting(ctx, t, k8sClientSet, nameGreen, namespace)
	defer cleanupResources(t, ctx, k8sClientSet, namespace, nameGreen)

	// Wait until both deployments exist
	require.Eventually(t, func() bool {
		_, err1 := k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, nameBlue, metav1.GetOptions{})
		_, err2 := k8sClientSet.AppsV1().Deployments(namespace).Get(ctx, nameGreen, metav1.GetOptions{})
		return err1 == nil && err2 == nil
	}, 30*time.Second, 1*time.Second, "deployments not created")

	// Update one WM to different labels
	newLabels := map[string]string{
		"wm.color":      "green",
		"wm.deployment": "xyz",
		"env":           "qa",
		"phase":         "alpha",
	}
	_, err := clusterClient.UpdateWorkloadManager(ctx, &pb.UpdateWorkloadManagerRequest{
		Name: nameGreen,
		WorkloadManagerConfig: &pb.WorkloadManagerConfig{
			Labels: newLabels,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"workload_manager_config.labels"}},
	})
	require.NoError(t, err, "failed to update labels on wm-filter-green")

	// Filter for the blue one
	respBlue, err := clusterClient.ListWorkloadManager(ctx, &pb.ListWorkloadManagerRequest{
		PageSize: 10,
		LabelSelector: map[string]string{
			"wm.color":      "blue",
			"wm.deployment": "abc",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, respBlue)
	require.NotEmpty(t, respBlue.WorkloadManagers)

	gotNames := map[string]struct{}{}
	for _, wm := range respBlue.WorkloadManagers {
		gotNames[wm.Name] = struct{}{}
		lbls := wm.WorkloadManagerConfig.GetLabels()
		require.Equal(t, "blue", lbls["wm.color"])
		require.Equal(t, "abc", lbls["wm.deployment"])
	}
	_, hasBlue := gotNames[nameBlue]
	_, hasGreen := gotNames[nameGreen]
	assert.True(t, hasBlue, "expected wm-filter-blue in filtered results")
	assert.False(t, hasGreen, "did not expect wm-filter-green in filtered results")

	// Filter for the green one
	respGreen, err := clusterClient.ListWorkloadManager(ctx, &pb.ListWorkloadManagerRequest{
		PageSize: 10,
		LabelSelector: map[string]string{
			"wm.color":      "green",
			"wm.deployment": "xyz",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, respGreen)

	gotNames2 := map[string]struct{}{}
	for _, wm := range respGreen.WorkloadManagers {
		gotNames2[wm.Name] = struct{}{}
	}
	_, hasBlue2 := gotNames2[nameBlue]
	_, hasGreen2 := gotNames2[nameGreen]
	assert.False(t, hasBlue2, "did not expect wm-filter-blue in green-filtered results")
	assert.True(t, hasGreen2, "expected wm-filter-green in filtered results")
}

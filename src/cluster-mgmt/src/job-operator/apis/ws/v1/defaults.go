/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"

	corev1 "k8s.io/api/core/v1"

	commonapisv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

// Int32 is a helper routine that allocates a new int32 value
// to store v and returns a pointer to it.
func Int32(v int32) *int32 {
	return &v
}

// addDefaultingFuncs is used to register default funcs
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

func getDefaultInitContainer(spec *corev1.PodSpec) int {
	for i, container := range spec.InitContainers {
		if container.Name == DefaultInitContainerName {
			return i
		}
	}
	return -1
}

// setDefaultInitContainer sets the init containers.
func setDefaultInitContainer(wsjob *WSJob, spec *corev1.PodSpec) {
	// for test purpose
	if wsjob.SkipDefaultInitContainer() {
		return
	}

	defaultInitContainer := corev1.Container{
		Image: KubectlImage,
		Name:  DefaultInitContainerName,
		Command: []string{
			"/bin/bash",
			"-c",
			InitContainerScript,
		},
		Env: []corev1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			{
				Name:  "WSJOB_ID",
				Value: wsjob.Name,
			},
			{
				Name:  "NAMESPACE",
				Value: wsjob.Namespace,
			},
			{
				Name:  "CFG_UPDATE_SIGNAL",
				Value: ClusterDetailsConfigUpdatedSignal,
			},
			{
				Name:  "UPDATED_VALUE",
				Value: UpdatedSignalVal,
			},
		},
	}
	// override existing containers to ensure init container is controlled at operator side only
	if i := getDefaultInitContainer(spec); i >= 0 {
		spec.InitContainers[i] = defaultInitContainer
	} else {
		spec.InitContainers = append(spec.InitContainers, defaultInitContainer)
	}
	for i, container := range spec.InitContainers {
		if strings.Contains(container.Image, KubectlImageName) {
			// override to keep consistent version
			spec.InitContainers[i].Image = KubectlImage
		}
	}
	// for test purpose, if one wsjob e2e test is started manually without version info from makefile
	for i, container := range spec.Containers {
		if strings.Contains(container.Image, KubectlImageName) {
			// override to keep consistent version
			spec.Containers[i].Image = KubectlImage
		}
	}
}

// setDefaultPort sets the default ports for ws container.
func setDefaultPort(rt commonapisv1.ReplicaType, spec *corev1.PodSpec) {
	if rt == WSReplicaTypeBroadcastReduce && DisableMultusForBR {
		spec.HostNetwork = true
		spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
		return
	}
	index := 0
	for i, container := range spec.Containers {
		if container.Name == DefaultContainerName {
			index = i
			break
		}
	}

	hasWSJobPort := false
	for _, port := range spec.Containers[index].Ports {
		if port.Name == DefaultPortName {
			hasWSJobPort = true
			break
		}
	}

	if !hasWSJobPort {
		defaultPortCount := int64(0)
		if rt == WSReplicaTypeSWDriver {
			defaultPortCount = DefaultSwdrPortCount
		} else if rt == WSReplicaTypeWeight {
			defaultPortCount = DefaultWgtPortCount
		} else if IsReplicaTypeActKvss(rt) {
			defaultPortCount = DefaultActKvssPortCount
		}
		spec.Containers[index].Ports = append(
			spec.Containers[index].Ports,
			GenerateDefaultPorts(rt, defaultPortCount)...,
		)
	}
}

func GenerateDefaultPorts(rt commonapisv1.ReplicaType, portCount int64) []corev1.ContainerPort {
	if rt == WSReplicaTypeBroadcastReduce {
		return nil
	}

	ports := []corev1.ContainerPort{
		{
			Name:          DefaultPortName,
			ContainerPort: DefaultPort,
		},
		{
			Name:          DefaultDebugPortName,
			ContainerPort: DefaultDebugPort,
		},
	}

	if rt == WSReplicaTypeWeight || IsReplicaTypeActKvss(rt) || rt == WSReplicaTypeSWDriver {
		for portIdx := 0; int64(portIdx) < portCount; portIdx++ {
			ports = append(ports,
				corev1.ContainerPort{
					Name:          DefaultWhostPortNamePrefix + strconv.Itoa(portIdx),
					ContainerPort: int32(DefaultWhostPortStart + portIdx),
				},
			)
		}
	}
	return ports
}

func setDefaultReplicas(spec *commonapisv1.ReplicaSpec) {
	if spec.Replicas == nil {
		spec.Replicas = Int32(1)
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = DefaultRestartPolicy
	}
}

func setDefaultProbe(wsjob *WSJob, rt commonapisv1.ReplicaType, spec *corev1.PodSpec) {
	if !wsjob.StartupProbeAllEnabled() {
		return
	}
	cmd := "python $WSJOB_SCRIPT_ROOT/$WSJOB_RUN_SCRIPT_NAME %s %s"
	startupProbeCmd := fmt.Sprintf(cmd, GetReplicaTypeShort(rt).Lower(), "startup_probe")
	// We disable startup probe when tests use different commands.
	hasDefaultWsjobEntrypoint := func(command []string) bool {
		for _, c := range command {
			if strings.Contains(c, "WSJOB_INIT_SCRIPT_NAME") {
				return true
			}
		}
		return false
	}
	for i := range spec.Containers {
		container := &spec.Containers[i]
		if IsWsjobContainer(container) {
			if container.StartupProbe == nil && hasDefaultWsjobEntrypoint(container.Command) {
				container.StartupProbe = &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						Exec: &corev1.ExecAction{
							Command: []string{
								"sh", "-c", startupProbeCmd,
							},
						},
					},
					InitialDelaySeconds: int32(5),
					PeriodSeconds:       int32(5),
					FailureThreshold:    int32(80),
					TimeoutSeconds:      int32(3),
				}
			}
		}
	}
}

// setDefaultResources sets the resource request/limit wsjob pod containers
// for now only CRD needed, override if spec mem smaller than env crdCompileMinMemoryGi/crdExecuteMinMemoryGi.
func setDefaultResources(wsjob *WSJob) {
	if wsjob.Annotations[AppliedDefaultLimitsKey] != "" {
		return
	}
	minCrdMem := CrdExecuteMinMemoryBytes
	if wsjob.IsCompile() {
		minCrdMem = CrdCompileMinMemoryBytes
	}
	for rt, s := range wsjob.Spec.WSReplicaSpecs {
		if rt != WSReplicaTypeCoordinator {
			continue
		}
		spec := s.Template.Spec
		for i := range spec.Containers {
			c := &spec.Containers[i]
			if c.Resources.Limits == nil {
				c.Resources.Limits = make(corev1.ResourceList)
			}
			if c.Resources.Requests == nil {
				c.Resources.Requests = make(corev1.ResourceList)
			}
			if c.Name == DefaultContainerName {
				specMem := c.Resources.Limits.Memory()
				// ensure memory limit >= provided min only if already set
				if minCrdMem > 0 && specMem != nil && !specMem.IsZero() {
					if specMem.Value() < int64(minCrdMem) {
						logrus.Infof("override CRD limits/requests to larger value, job %s, before: %d, after: %d",
							wsjob.NamespacedName(), specMem.Value(), minCrdMem)
						mem, _ := resource.ParseQuantity(strconv.Itoa(minCrdMem))
						c.Resources.Requests[corev1.ResourceMemory] = mem
						c.Resources.Limits[corev1.ResourceMemory] = mem
					}
				}
			}
			if specCpu := c.Resources.Requests[corev1.ResourceCPU]; IsGenoaServer && !specCpu.IsZero() {
				newCpu := resource.NewMilliQuantity(specCpu.MilliValue()/2, resource.DecimalSI)
				c.Resources.Requests[corev1.ResourceCPU] = *newCpu
				logrus.Infof("override cpu requests to half, job %s, %s/%s, before: %dm, after: %dm",
					wsjob.NamespacedName(), rt, c.Name, specCpu.MilliValue(), newCpu.MilliValue())
			}
		}
	}
	// for idempotence check
	if wsjob.Annotations == nil {
		wsjob.Annotations = map[string]string{}
	}
	wsjob.Annotations[AppliedDefaultLimitsKey] = "true"
}

// setTypeNamesToCamelCase sets the name of all replica types from any case to correct case.
func setTypeNamesToCamelCase(wsjob *WSJob) {
	setTypeNameToCamelCase(wsjob, WSReplicaTypeWeight)
	setTypeNameToCamelCase(wsjob, WSReplicaTypeCommand)
	setTypeNameToCamelCase(wsjob, WSReplicaTypeActivation)
	setTypeNameToCamelCase(wsjob, WSReplicaTypeBroadcastReduce)
	setTypeNameToCamelCase(wsjob, WSReplicaTypeChief)
	setTypeNameToCamelCase(wsjob, WSReplicaTypeCoordinator)
	setTypeNameToCamelCase(wsjob, WSReplicaTypeWorker)
	setTypeNameToCamelCase(wsjob, WSReplicaTypeKVStorageServer)
}

// setTypeNameToCamelCase sets the name of the replica type from any case to correct case.
func setTypeNameToCamelCase(wsjob *WSJob, typ commonapisv1.ReplicaType) {
	for t := range wsjob.Spec.WSReplicaSpecs {
		if strings.EqualFold(string(t), string(typ)) && t != typ {
			spec := wsjob.Spec.WSReplicaSpecs[t]
			delete(wsjob.Spec.WSReplicaSpecs, t)
			wsjob.Spec.WSReplicaSpecs[typ] = spec
			return
		}
	}
}

// setDefaultJobPriority sets the default job priority in case not set.
func setDefaultJobPriority(wsjob *WSJob) {
	if wsjob.Spec.Priority == nil {
		wsjob.SetPriority(DefaultJobPriorityValue)
	}
}

// setWsjobDomainAnno applies job business knowledge of act/system domain/br replica.
func setWsjobDomainAnno(wsjob *WSJob) {
	if len(wsjob.Annotations) == 0 {
		wsjob.Annotations = map[string]string{}
	}
	if wsjob.SkipBR() {
		wsjob.SetSkipBrAnno()
	}
	// if job < 1.0.5, TODO: Remove the following block in rel-2.7
	if len(wsjob.Spec.ActToCsxDomains) == 0 ||
		wsjob.Annotations[RuntimeTargetedDomainsKey] != "" {
		return
	}
	// input: [][]int{{0}, {1}, {2}}
	// output: "0,1,2"
	// input (future support): [][]int{{0,1}, {1,2}, {2,3}}
	// output (future support): "0-1,1-2,2-3"
	var domains []string
	for _, perPodDomains := range wsjob.Spec.ActToCsxDomains {
		slices.Sort(perPodDomains)
		var perPodDomainsStrs []string
		for _, domain := range perPodDomains {
			perPodDomainsStrs = append(perPodDomainsStrs, strconv.Itoa(domain))
		}
		domains = append(domains, strings.Join(perPodDomainsStrs, "-"))
	}
	serializedDomains := strings.Join(domains, ",")
	wsjob.Annotations[RuntimeTargetedDomainsKey] = serializedDomains
}

// SetDefaults_WSJob sets any unspecified values to defaults.
func SetDefaults_WSJob(wsjob *WSJob) {
	if wsjob.Spec.RunPolicy.CleanPodPolicy == nil {
		policy := commonapisv1.CleanPodPolicyRunning
		wsjob.Spec.RunPolicy.CleanPodPolicy = &policy
	}

	if wsjob.Spec.RunPolicy.TTLSecondsAfterFinished == nil {
		ttl := int32(WsjobCleanupTTLSeconds)
		wsjob.Spec.RunPolicy.TTLSecondsAfterFinished = &ttl
	}

	if wsjob.Spec.SuccessPolicy == nil {
		defaultPolicy := SuccessPolicyDefault
		wsjob.Spec.SuccessPolicy = &defaultPolicy
	}

	setTypeNamesToCamelCase(wsjob)
	setDefaultJobPriority(wsjob)
	setWsjobDomainAnno(wsjob)
	setDefaultResources(wsjob)

	for rt, spec := range wsjob.Spec.WSReplicaSpecs {
		setDefaultInitContainer(wsjob, &spec.Template.Spec)
		setDefaultReplicas(spec)
		setDefaultPort(rt, &spec.Template.Spec)
		setDefaultProbe(wsjob, rt, &spec.Template.Spec)
	}
}

func IsWsjobContainer(c *corev1.Container) bool {
	return c.Name == DefaultContainerName || IsUserSidecarContainer(c)
}

func IsUserSidecarContainer(c *corev1.Container) bool {
	return c.Name == UserSidecarContainerName || c.Name == StreamerContainerName
}

func IsReplicaTypeActKvss(rt commonapisv1.ReplicaType) bool {
	return rt == WSReplicaTypeActivation || rt == WSReplicaTypeKVStorageServer
}

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
	"os"
	"strconv"
	"strings"

	"github.com/coreos/go-semver/semver"
	"sigs.k8s.io/controller-runtime/pkg/client"

	common_pb "cerebras/pb/workflow/appliance/common"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonctrlv1 "github.com/kubeflow/common/pkg/controller.v1/common"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=true
type JobUser struct {
	Uid int64 `json:"uid,omitempty"`
	Gid int64 `json:"gid,omitempty"`
	// Username is optional username scraped from the client image. It should only be used as metadata not for security.
	Username string  `json:"username,omitempty"`
	Groups   []int64 `json:"groups,omitempty"`
}

type NotificationType string

const (
	NotificationTypeEmail     NotificationType = "email"
	NotificationTypeSlack     NotificationType = "slack"
	NotificationTypePagerduty NotificationType = "pagerduty"
)

// UserNotification defines notification preferences
type UserNotification struct {
	NotificationType NotificationType `json:"notificationType"`
	Target           string           `json:"target"`
	// Severity threshold for notifications
	// If not set, notifications will be sent for all severities
	SeverityThreshold *int32 `json:"severityThreshold,omitempty"`
}

// WSJobSpec defines the desired state of WSJob
type WSJobSpec struct {
	// RunPolicy encapsulates various runtime policies of the distributed training
	// job, for example how to clean up resources and how long the job can stay
	// active.
	//+kubebuilder:validation:Optional
	RunPolicy commonv1.RunPolicy `json:"runPolicy"`

	// SuccessPolicy defines the policy to mark the WSJob as succeeded.
	// Default to "", using the default rules.
	// +optional
	SuccessPolicy *SuccessPolicy `json:"successPolicy,omitempty"`

	// A map of WSReplicaType (type) to ReplicaSpec (value). Specifies the cluster configuration.
	// For example,
	//   {
	//     "Weight": ReplicaSpec,
	//     "Command": ReplicaSpec,
	//     "Activation": ReplicaSpec,
	//     "BroadcastReduce": ReplicaSpec,
	//     "Chief": ReplicaSpec,
	//     "Coordinator": ReplicaSpec,
	//     "Worker": ReplicaSpec,
	//     "KVStorageServer": ReplicaSpec,
	//     "SWDriver": ReplicaSpec,
	//   }
	WSReplicaSpecs map[commonv1.ReplicaType]*commonv1.ReplicaSpec `json:"wsReplicaSpecs"`

	// Number of wafers in this job
	NumWafers uint32 `json:"numWafers"`

	// User that owns this job.
	User *JobUser `json:"user,omitempty"`

	// Priority of this job
	Priority *int `json:"priority,omitempty"`

	// Activations are required to talk to specific domains in AX scheduling (V2 networking specific).
	// Indices of "ActToCsxDomains" represent the pod index per-CSX.
	// Values of "ActToCsxDomains[i]" represent the CSX domains that the i-th pod is required to talk to.
	// In rel-2.3, each activation pod only talks to one CSX domain. The plural form is only for forward
	// compatibility.
	ActToCsxDomains [][]int `json:"actToCsxDomains,omitempty"`

	// Notification preferences for this job
	Notifications []*UserNotification `json:"notifications,omitempty"`
}

// SuccessPolicy is the success policy.
type SuccessPolicy string

// WSReplicaType is the type for WSReplica for servers in weight streaming execution model.
// Can be one of: "Weight", "Command", "Activation", "BroadcastReduce", "Coordinator",
// "Worker" or "XlaService."

const (
	// WSReplicaTypeWeight is the type for weight servers.
	WSReplicaTypeWeight      commonv1.ReplicaType = "Weight"
	WSReplicaTypeWeightShort commonv1.ReplicaType = "WGT"

	// WSReplicaTypeCommand is the type for command servers.
	WSReplicaTypeCommand      commonv1.ReplicaType = "Command"
	WSReplicaTypeCommandShort commonv1.ReplicaType = "CMD"

	// WSReplicaTypeActivation is the type for activation servers.
	WSReplicaTypeActivation      commonv1.ReplicaType = "Activation"
	WSReplicaTypeActivationShort commonv1.ReplicaType = "ACT"

	WSReplicaTypeSWDriver      commonv1.ReplicaType = "SWDriver"
	WSReplicaTypeSWDriverShort commonv1.ReplicaType = "SWD"

	// WSReplicaTypeBroadcastReduce is the type for broadcast-reduce servers.
	// This is hidden from the user.
	WSReplicaTypeBroadcastReduce      commonv1.ReplicaType = "BroadcastReduce"
	WSReplicaTypeBroadcastReduceShort commonv1.ReplicaType = "BR"

	// WSReplicaTypeCoordinator is the type for coordinator servers.
	WSReplicaTypeCoordinator      commonv1.ReplicaType = "Coordinator"
	WSReplicaTypeCoordinatorShort commonv1.ReplicaType = "CRD"

	// WSReplicaTypeChief is the type for chief servers.
	WSReplicaTypeChief      commonv1.ReplicaType = "Chief"
	WSReplicaTypeChiefShort commonv1.ReplicaType = "CHF"

	// WSReplicaTypeKVStorageServer is the type for key-value storage servers.
	WSReplicaTypeKVStorageServer      commonv1.ReplicaType = "KVStorageServer"
	WSReplicaTypeKVStorageServerShort commonv1.ReplicaType = "KVSS"

	// WSReplicaTypeWorker is the type for input worker servers.
	WSReplicaTypeWorker      commonv1.ReplicaType = "Worker"
	WSReplicaTypeWorkerShort commonv1.ReplicaType = "WRK"

	WSReplicaTypeInvalid commonv1.ReplicaType = "Invalid"

	// Legacy jobs are deprecated. Keep this for backward compatibility.
	ReplicaTypeLegacy commonv1.ReplicaType = "Pack"

	SuccessPolicyDefault     SuccessPolicy = ""
	SuccessPolicyAllServices SuccessPolicy = "AllServices"

	AwaitLock   commonv1.JobConditionType = "AwaitLock"
	LockGranted commonv1.JobConditionType = "LockGranted"
)

var RootUser = &JobUser{
	Uid:      int64(0),
	Gid:      int64(0),
	Username: "root",
}

var ClusterDetailsTaskTypeMapping = map[string]common_pb.ClusterDetails_TaskInfo_TaskType{
	WSReplicaTypeCoordinator.Lower():     common_pb.ClusterDetails_TaskInfo_CRD,
	WSReplicaTypeChief.Lower():           common_pb.ClusterDetails_TaskInfo_CHF,
	WSReplicaTypeWorker.Lower():          common_pb.ClusterDetails_TaskInfo_WRK,
	WSReplicaTypeActivation.Lower():      common_pb.ClusterDetails_TaskInfo_ACT,
	WSReplicaTypeWeight.Lower():          common_pb.ClusterDetails_TaskInfo_WGT,
	WSReplicaTypeCommand.Lower():         common_pb.ClusterDetails_TaskInfo_CMD,
	WSReplicaTypeBroadcastReduce.Lower(): common_pb.ClusterDetails_TaskInfo_BR,
	WSReplicaTypeKVStorageServer.Lower(): common_pb.ClusterDetails_TaskInfo_KVSS,
	WSReplicaTypeSWDriver.Lower():        common_pb.ClusterDetails_TaskInfo_SWD,
	ReplicaTypeLegacy.Lower():            common_pb.ClusterDetails_TaskInfo_WRK,
}

var TaskTypeClusterDetailsMapping = map[common_pb.ClusterDetails_TaskInfo_TaskType]commonv1.ReplicaType{
	common_pb.ClusterDetails_TaskInfo_CRD:  WSReplicaTypeCoordinator,
	common_pb.ClusterDetails_TaskInfo_CHF:  WSReplicaTypeChief,
	common_pb.ClusterDetails_TaskInfo_WRK:  WSReplicaTypeWorker,
	common_pb.ClusterDetails_TaskInfo_ACT:  WSReplicaTypeActivation,
	common_pb.ClusterDetails_TaskInfo_WGT:  WSReplicaTypeWeight,
	common_pb.ClusterDetails_TaskInfo_CMD:  WSReplicaTypeCommand,
	common_pb.ClusterDetails_TaskInfo_BR:   WSReplicaTypeBroadcastReduce,
	common_pb.ClusterDetails_TaskInfo_KVSS: WSReplicaTypeKVStorageServer,
	common_pb.ClusterDetails_TaskInfo_SWD:  WSReplicaTypeSWDriver,
}

func GetReplicaTypeShort(r any) commonv1.ReplicaType {
	if val, ok := r.(commonv1.ReplicaType); ok {
		return RoleFullToShortMap[val]
	}
	return RoleFullToShortMap[commonv1.ReplicaType(r.(string))]
}

func GetReplicaTypeFull(r any) commonv1.ReplicaType {
	if val, ok := r.(commonv1.ReplicaType); ok {
		return RoleShortToFullMap[val]
	}
	return RoleShortToFullMap[commonv1.ReplicaType(r.(string))]
}

var RoleShortToFullMap = map[commonv1.ReplicaType]commonv1.ReplicaType{
	WSReplicaTypeWorkerShort:          WSReplicaTypeWorker,
	WSReplicaTypeChiefShort:           WSReplicaTypeChief,
	WSReplicaTypeCoordinatorShort:     WSReplicaTypeCoordinator,
	WSReplicaTypeWeightShort:          WSReplicaTypeWeight,
	WSReplicaTypeCommandShort:         WSReplicaTypeCommand,
	WSReplicaTypeActivationShort:      WSReplicaTypeActivation,
	WSReplicaTypeBroadcastReduceShort: WSReplicaTypeBroadcastReduce,
	WSReplicaTypeKVStorageServerShort: WSReplicaTypeKVStorageServer,
	WSReplicaTypeSWDriverShort:        WSReplicaTypeSWDriver,
}

var RoleFullToShortMap = map[commonv1.ReplicaType]commonv1.ReplicaType{
	WSReplicaTypeWorker:          WSReplicaTypeWorkerShort,
	WSReplicaTypeChief:           WSReplicaTypeChiefShort,
	WSReplicaTypeCoordinator:     WSReplicaTypeCoordinatorShort,
	WSReplicaTypeWeight:          WSReplicaTypeWeightShort,
	WSReplicaTypeCommand:         WSReplicaTypeCommandShort,
	WSReplicaTypeActivation:      WSReplicaTypeActivationShort,
	WSReplicaTypeBroadcastReduce: WSReplicaTypeBroadcastReduceShort,
	WSReplicaTypeKVStorageServer: WSReplicaTypeKVStorageServerShort,
	WSReplicaTypeSWDriver:        WSReplicaTypeSWDriverShort,
}

//+genclient
//+k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
//+resource:path=wsjob
//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.conditions[-1:].type`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
//+kubebuilder:printcolumn:name="Start",type=string,JSONPath=`.status.startTime`
//+kubebuilder:printcolumn:name="End",type=string,JSONPath=`.status.completionTime`

// WSJob is the Schema for the wsjobs API
type WSJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WSJobSpec          `json:"spec,omitempty"`
	Status commonv1.JobStatus `json:"status,omitempty"`
}

func (in *WSJob) IsCompile() bool {
	return in.Labels[JobTypeLabelKey] == JobTypeCompile
}

func (in *WSJob) IsExecute() bool {
	return in.Labels[JobTypeLabelKey] == JobTypeExecute
}

func (in *WSJob) IsPrioritizedCompile() bool {
	return in.Annotations[PendingLockGracePeriodAnnotKey] != ""
}

func (in *WSJob) IsWeightStreaming() bool {
	return in.Labels[JobModeLabelKey] == JobModeWeightStreaming
}

func (in *WSJob) IsSdk() bool {
	return in.Labels[JobModeLabelKey] == JobModeSdk
}

func (in *WSJob) IsInference() bool {
	return in.Labels[JobModeLabelKey] == JobModeInference
}

func (in *WSJob) SkipBR() bool {
	return in.Spec.WSReplicaSpecs[WSReplicaTypeBroadcastReduce] == nil ||
		*in.Spec.WSReplicaSpecs[WSReplicaTypeBroadcastReduce].Replicas == 0
}

func (in *WSJob) SetSkipBrAnno() {
	in.Annotations[JobSkipBRKey] = "true"
}

func (in *WSJob) SpecifiedBrTreePolicy() string {
	return in.Annotations[BrTreePolicyKey]
}

func (in *WSJob) UseAxOnlyForRuntime() bool {
	return in.Annotations[UseAxOnlyForRuntimeKey] == UseAxOnlyForRuntimeValue && UseAxScheduling
}

func (in *WSJob) HasJobType() bool {
	return in.Labels[JobTypeLabelKey] != ""
}

func (in *WSJob) GetJobType() string {
	return in.Labels[JobTypeLabelKey]
}

func (in *WSJob) EnablePartialSystems() bool {
	if !EnablePartialSystemMode {
		return false
	}
	return in.Annotations[EnablePartialSystemKey] == strconv.FormatBool(true)
}

func (in *WSJob) EnableBalancedTree() bool {
	return in.Annotations[BuildBalancedBrTreeKey] == strconv.FormatBool(true)
}

func (in *WSJob) SemanticVersionLessThan(version string) bool {
	if v, err := semver.NewVersion(in.Annotations[SemanticClusterServerVersion]); err != nil ||
		v.LessThan(*semver.New(version)) {
		return true
	}
	return false
}

func (in *WSJob) SkipDefaultInitContainer() bool {
	return in.Labels[InitContainerOverrideKey] == "true"
}

func (in *WSJob) GetIngressSvcName() string {
	// In most cases, coordinator requires to be brought up in the job creation process.
	// The only exception is in SDK execute where the coordinator (treated as worker) is placed on a
	// worker node.
	// We currently only need the ingress to talk to the only entrypoint (coordinator/worker).
	// If this changes in the future, we may need to update the configuration accordingly.
	if in.IsSdk() && !in.IsCompile() {
		return commonctrlv1.GenSvcName(in.Name, "worker", "0")
	}
	return commonctrlv1.GenSvcName(in.Name, "coordinator", "0")
}

func (in *WSJob) IsIngressReplicaType(rType commonv1.ReplicaType) bool {
	if in.IsSdk() && !in.IsCompile() {
		return rType == WSReplicaTypeWorker
	}
	return rType == WSReplicaTypeCoordinator
}

func (in *WSJob) GetCrdAddress() string {
	// client will have the option to use this address if they set WsJobCrdAddrAnnoKey to true
	// in DebugArgs.DebugMGR
	// https://cerebras.atlassian.net/browse/SW-90480
	url := in.Annotations[WsJobCrdAddrAnnoKey]
	if url != "" {
		url = fmt.Sprintf("%s:443", url)
	}
	return url
}

func (in *WSJob) StartupProbeAllEnabled() bool {
	return strings.ToLower(in.Annotations[EnableStartupProbeAll]) != "false"
}

func (in *WSJob) NamespacedName() client.ObjectKey {
	return client.ObjectKey{Namespace: in.Namespace, Name: in.Name}
}

func (in *WSJob) CancelRequested() bool {
	return in.Annotations[CancelWithStatusKey] != ""
}

func (in *WSJob) GetPriority() int {
	// Existing wsjobs does not have priority assigned when created.
	// We will use the default job priority value for those wsjobs.
	if in.Spec.Priority == nil {
		return DefaultJobPriorityValue
	}
	return *in.Spec.Priority
}

func (in *WSJob) GetPriorityPointer() *int {
	return in.Spec.Priority
}

func (in *WSJob) SetPriority(priority int) {
	in.Spec.Priority = &priority
}

// return true if it's the parent job which is reserved by client api
func (in *WSJob) IsReserved() bool {
	return in.Annotations[commonv1.ResourceReservationStatus] == commonv1.ResourceReservedValue
}

// return true if it's the parent job which is released by client api
func (in *WSJob) UnReserved() bool {
	return in.Annotations[commonv1.ResourceReservationStatus] == commonv1.ResourceReleasedValue
}

// return true if it's in active reserving status
// i.e. reserving itself or inheriting reservation
func (in *WSJob) InReservingWorkflow() bool {
	return in.IsReserved() || in.HasReservation()
}

// return true if it's the child job that going to inherit parent job reserved resources
func (in *WSJob) HasReservation() bool {
	return in.Annotations[HasReservationKey] != ""
}

func (in *WSJob) ParentJobName() string {
	return in.Annotations[ParentJobAnnotKey]
}

func (in *WSJob) HasWorkflowMeta() bool {
	_, ok1 := in.Annotations[ParentJobAnnotKey]
	_, ok2 := in.Annotations[commonv1.ResourceReservationStatus]
	return ok1 || ok2
}

func (in *WSJob) SetWorkflowMeta(reserved bool, parent, subsidiary string) {
	if parent != "" {
		// set parent job name
		in.Annotations[ParentJobAnnotKey] = parent
		// parent job is reserved
		if reserved {
			in.Annotations[HasReservationKey] = "true"
		}
	} else if reserved {
		// current job needs reservation
		in.Annotations[commonv1.ResourceReservationStatus] = commonv1.ResourceReservedValue
		// add subsidiary
		if subsidiary != "" {
			in.Annotations[SubsidiaryReservationKey] = subsidiary
		}
	}
}

func (in *WSJob) GetWorkflowId() string {
	return in.Labels[WorkflowIdLabelKey]
}

func (in *WSJob) GetNamespacedWorkflowId() string {
	return in.Namespace + "/" + in.GetWorkflowId()
}

func (in *WSJob) GetPopulatedNgCount() int {
	if !in.IsExecute() {
		return 0
	}
	popNgCount, err := strconv.Atoi(in.Annotations[WsJobPopulatedNodegroupCount])
	if err != nil || popNgCount == 0 {
		popNgCount = 1
	}
	return popNgCount
}

func (in *WSJob) GetTotalNodegroupCount() int {
	if !in.IsExecute() {
		return 0
	}
	populatedNgCount := in.GetPopulatedNgCount()
	// if requested 2 systems and 1 pop group, then total groups required should be 2 (pop + depop)
	// if requested 2 systems and 2 pop groups, then total groups required should be 2
	// if requested 2 systems and 3 pop groups, then total groups required should be 3
	return max(int(in.Spec.NumWafers), populatedNgCount)
}

func (jl *WSJobList) NamespacedNames() []string {
	var nsNames []string
	for _, j := range jl.Items {
		nsNames = append(nsNames, j.NamespacedName().String())
	}
	return nsNames
}

func (in *WSJob) AllowUnhealthySystems() bool {
	return in.Annotations[EnableAllowUnhealthySystems] == strconv.FormatBool(true)
}

// When operator is old (<= 1.0.4), the configmap creation responsibility was fulfilled by operator.
// When both cluster server and operator is greater than 1.0.5, the responsibility is shifted to cluster server.
// One special case is that, at the job request time, if cluster server saw that operator was old, the server
// would skip creating the configmap. If operator gets upgraded while the job is in queue, then the new operator
// should still fulfill the responsibility if configmap does not exist after some time. If the cause of the
// configmap not being created was due to a server-side error when creating the configmap, the client should have
// exited already and the job should eventually fail with client being disconnected.
// TODO: Retire this check in rel-2.7
func (in *WSJob) CanServerGenerateConfig() bool {
	clusterServerSemver, err := semver.NewVersion(in.Annotations[SemanticClusterServerVersion])
	return err == nil && !clusterServerSemver.LessThan(SoftwareFeatureSemverMap[ParallelExperts])
}

func (in *WSJob) CanServerRecognizeWesGroupSize() bool {
	clusterServerSemver, err := semver.NewVersion(in.Annotations[SemanticClusterServerVersion])
	return err == nil && !clusterServerSemver.LessThan(SoftwareFeatureSemverMap[ExpertParallelOnBr])
}

func (in *WSJob) RequireInfraSetup() bool {
	_, ok := in.Annotations[InfraSetupAnnot]
	return ok
}

// Gets the default port of the WS container or the default port value if no such container exists.
func (in *WSJob) GetDefaultPort(rtype commonv1.ReplicaType) int32 {
	containers := in.Spec.WSReplicaSpecs[rtype].Template.Spec.Containers
	for _, container := range containers {
		if container.Name == DefaultContainerName {
			ports := container.Ports
			for _, port := range ports {
				if port.Name == DefaultPortName {
					return port.ContainerPort
				}
			}
		}
	}
	return DefaultPort
}

// As described here: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#a-records.
// Headless services are assigned a DNS A record for a name of the form
// "my-svc.my-namespace.svc.cluster.local". And the last part "cluster.local" is
// called cluster domain which maybe different between kubernetes clusters.
func (in *WSJob) GetDefaultTaskIpPort(replicaType commonv1.ReplicaType, taskId int32) string {
	serviceAddr := commonctrlv1.GenGeneralName(in.Name, string(replicaType), fmt.Sprintf("%d", taskId))
	clusterDomain := os.Getenv(EnvCustomClusterDomain)
	if len(clusterDomain) > 0 {
		serviceAddr += "." + clusterDomain
	}
	defaultPort := in.GetDefaultPort(replicaType)
	return fmt.Sprintf("%s:%d", serviceAddr, defaultPort)
}

func (in *WSJob) GetAppClientSemver() string {
	return in.Annotations[SemanticApplianceClientVersion]
}

func (in *WSJob) GetClusterServerSemver() string {
	return in.Annotations[SemanticClusterServerVersion]
}

func (in *WSJob) GetJobOperatorSemver() string {
	return in.Annotations[SemanticJobOperatorVersion]
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=wsjobs
//+kubebuilder:object:root=true

// WSJobList contains a list of WSJob
type WSJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WSJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WSJob{}, &WSJobList{})
	SchemeBuilder.SchemeBuilder.Register(addDefaultingFuncs)
}

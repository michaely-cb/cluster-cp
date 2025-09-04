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
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

type SystemType string

const (
	SystemCS2   = SystemType("cs2")
	SystemOther = SystemType("other")
	SystemUnset = SystemType("")
)

type LockState string

const (
	LockPending = LockState("Pending")
	LockGranted = LockState("Granted")
	LockError   = LockState("Error")
)

const (
	// PodIdAnnotKey is key for annotating the pod index of a replica type.
	PodIdAnnotKey = "pod-id"

	// PodIdPerCsxAnnotKey is key for annotating the pod index in its task with respect to the CSX it talks to.
	// For example, in a 2-system job where each system has 4 activations, the first 4 activations talk to the first
	// system and the second 4 activations talk to the second system.
	// "activation-7" is the fourth pod for the second system and "4" is the value for this annotation in this example.
	PodIdPerCsxAnnotKey = "pod-id-per-csx"

	// SystemIdAnnotKey is key for annotating the system index to the order a system had been granted.
	// For example, if a job got granted sys-x, sys-y, sys-z, then system id for sys-z is 2.
	SystemIdAnnotKey = "system-id"

	// TargetCsPortAnnotKey is key for annotating the cs port a pod should talk to.
	TargetCsPortAnnotKey = "cs-port"

	// PreferredCsPortsAnnotKey is key annotating the cs port(s) where the pod has lower costs talking to.
	PreferredCsPortsAnnotKey = "preferred-cs-ports"

	// BrSetIdAnnotKey is key annotating the belonging BR set of the pod.
	BrSetIdAnnotKey = "br-set-id"

	// TargetBrSetsAnnotKey is key annotating the target BR set(s) where the BR pod has egress to.
	TargetBrSetsAnnotKey = "target-br-sets"

	// NvmfTargetIdAnnotKey is the key for annotating the coordinator nvmf target id. It is used for identifying
	// block device paths for the coordinator and weight pods.
	NvmfTargetIdAnnotKey = "nvme-of-target-id"

	// LinearMemRangeXAnnotKey is the key for annotating the estimated X value for linear memory granting.
	LinearMemRangeXAnnotKey = "linear-mem-range-x"

	// WioBandwidthAnnotKey for annotating the io stream usages of the pod
	WioBandwidthAnnotKey = "wio-bandwidth"
)

type LockStatusMessage string

type ResourceRequest struct {
	// The name of the request. Must be unique within the request group since it will be used in the response to indicate
	// which request was satisfied.
	//+kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Annotations is a map that contains miscellaneous information about the request.
	// For example, readable Id, targeted system and cs port.
	// Information from the annotations can be plumbed through all the way to pod spec in the end.
	Annotations map[string]string `json:"annotations,omitempty"`

	// Type is the type of resource requested. Currently only "system" and "node" are supported.
	//+kubebuilder:validation:Enum=system;node
	//+kubebuilder:validation:Required
	Type string `json:"type"`

	// Properties are key values pairs where the requested resources must have at least one of the values listed in the
	// array. Properties cannot end in _ as these are reserved for internal use.
	//TODO: kubebuilder:validation:Pattern=^[^_]*$ would be nice to have but it doesn't work
	Properties map[string][]string `json:"properties,omitempty"`

	// Subresources are key value pairs which the requested resource must have. Subresources cannot end in _ as these
	// are reserved for internal use. Subresources are required.
	//TODO: kubebuilder:validation:Pattern=^[^_]*$ would be nice to have but it doesn't work
	//+kubebuilder:validation:MinProperties=1
	Subresources map[string]int `json:"subresources"`

	// Count is the number of resources requested which match the given properties and subresources.
	//+kubebuilder:validation:Minimum=1
	Count int `json:"count"`
}

type ResourceUpperBound struct {
	// The name of the request.
	//+kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Subresources are key value pairs which the requested resource must have. Subresources cannot end in _ as these
	// are reserved for internal use. Subresources are required.
	//+kubebuilder:validation:MinProperties=1
	Subresources map[string]int `json:"subresources"`
}

// GroupResourceRequest is a group of ResourceRequests that must be matched against a set of resources belonging to the
// same group. Groups are defined by the resource's property "group" which implements the concept of NodeGroup, e.g.
// node.properties["group"] = "nodegroup0". Today, this is only implemented for nodes as they are the only resources
// which have the group property, but any resource could potentially be grouped such as systems in the case that
// racks of nodes should also be associated with a group of systems.
// As an example, if you want to request a NodeGroup with 2 memx nodes and 1 worker node where all 3 are in the same
// nodegroup, you'd add 2 ResourceRequests, one with type=node and properties=role-memory:true, count=2 and one with
// type=node and properties=role-worker:true, count=1. The response appears in the groupResourceGrants field of the
// LockStatus.
type GroupResourceRequest struct {
	// Name of the request. This must be unique amongst all other GroupResourceRequest.
	//+kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Count is the number of GroupResourceGrant responses to create for this GroupResourceRequest. Each response will
	// contain a list of resources which match the given requests.
	//+kubebuilder:validation:Minimum=1
	Count int `json:"count"`

	//+kubebuilder:validation:MinItems=1
	ResourceRequests []ResourceRequest `json:"resourceRequests"`
}

const (
	NodeResourceType   = "node"
	SystemResourceType = "system"

	SystemsRequestName         = "System"
	CoordinatorRequestName     = string(wsapisv1.WSReplicaTypeCoordinator)
	ActivationRequestName      = string(wsapisv1.WSReplicaTypeActivation)
	ChiefRequestName           = string(wsapisv1.WSReplicaTypeChief)
	BrRequestName              = string(wsapisv1.WSReplicaTypeBroadcastReduce)
	WeightRequestName          = string(wsapisv1.WSReplicaTypeWeight)
	WorkerRequestName          = string(wsapisv1.WSReplicaTypeWorker)
	CommandRequestName         = string(wsapisv1.WSReplicaTypeCommand)
	KVStorageServerRequestName = string(wsapisv1.WSReplicaTypeKVStorageServer)
	SWDriverRequestName        = string(wsapisv1.WSReplicaTypeSWDriver)

	WgtCmdGroupPrefix = "WeightCommand"

	PrimaryNgPrefix        = "ng-primary"
	SecondaryNgName        = "ng-secondaries"
	NodegroupRequestPrefix = "ng-"

	NamespaceKey                  = "namespace"
	SystemVersionRequired         = "require-system-version-filter" // for testing purpose
	SystemVersionPropertyKey      = "system-version"
	PlatformVersionPropertyKey    = "platform-version"
	AssignedRedundancyPropertyKey = "assigned-redundancy"

	CompileQueueName            = "compile"
	ExecuteQueueName            = "execute"
	PrioritizedCompileQueueName = "prioritized-compile"
	PrioritizedExecuteQueueName = "prioritized-execute"
	ReservationQueueName        = "reservation"

	BrLogicalTreeKey    = "BR_LOGICAL_TREE"
	BrLogicalTreePolicy = "BR_LOGICAL_TREE_POLICY"
)

var OrderedQueues = []string{
	ReservationQueueName,
	PrioritizedExecuteQueueName,
	PrioritizedCompileQueueName,
	ExecuteQueueName,
	CompileQueueName,
}

type Range struct {
	Start int64 `json:"start,omitempty"`
	End   int64 `json:"end,omitempty"`
}

type Res struct {
	// Type of the resource. Currently only `system` and `node` are supported.
	//+kubebuilder:validation:Enum=system;node
	Type string `json:"type"`

	// Name of the resource. Example: systemf99
	//+kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Granted subresources
	Subresources map[string]int `json:"subresources,omitempty"`

	// Granted subresourceRanges
	SubresourceRanges map[string][]Range `json:"subresourceRanges,omitempty"`
}

func (r Res) Id() string {
	return r.Type + "/" + r.Name
}

func ResourceFromId(id string) Res {
	i := strings.Index(id, "/")
	if i == -1 {
		return Res{Type: "", Name: id} // shouldn't happen
	}
	return Res{Type: id[0:i], Name: id[i+1:]}
}

type ResourceGrant struct {
	// Name of the corresponding ResourceRequest.
	//+kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Annotations is a map that contains metadata information about the request.
	// For example, readable Id, targeted system and cs port.
	// Information from the annotations can be plumbed through all the way to pod spec in the end.
	Annotations map[string]string `json:"annotations,omitempty"`

	// Resources which matched the corresponding ResourceRequest.
	//+kubebuilder:validation:MinItems=1
	Resources []Res `json:"resources"`
}

// GroupResourceGrant is a group of resources which belong to the same group. Each GroupResourceGrant corresponds to a
// GroupResourceRequest where there are Count GroupResourceGrant responses for each GroupResourceRequest, each with a
// unique groupingValue.
type GroupResourceGrant struct {
	// Name of the GroupResourceRequest corresponding to this MatchList.
	//+kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// GroupingValue is the value of the property "group" which all resources in this GroupResourceGrant have in common.
	// E.g. "nodegroup0" in the case where all the ResourceGrants share the group=nodegroup0 property.
	GroupingValue string `json:"groupingValue"`

	ResourceGrants []ResourceGrant `json:"resourceGrants"`

	// Extra metadata info on the grant result
	Annotations map[string]string `json:"annotations,omitempty"`
}

type SystemRequest struct {
	// The type of system requested. Allows `cs2` and `other`. `other` is for test purposes.
	// +kubebuilder:default=cs2
	// +kubebuilder:validation:Enum=cs2;other
	Type SystemType `json:"type,omitempty"`

	// Replicas is the number of systems of the given type requested.
	// +kubebuilder:validation:Minimum=1
	Replicas int `json:"replicas,omitempty"`

	// Properties are automatically set to characteristics of the System. Currently,
	// "name" is automatically set so to allow matching like name=systemf0,systemf1
	// which means "select where name is one of systemf0 or systemf1".
	Properties map[string]string `json:"properties,omitempty"`
}

type SystemGrant struct {
	// Name of the granted system.
	Name string `json:"name,omitempty"`
}

type NodeGroupRequest struct {
	// Name of the request. This must be unique amongst all other NodeGroupRequests.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`([a-zA-Z0-9][a-zA-Z0-9-]{1,62})?[a-zA-Z0-9]`
	Name string `json:"name,omitempty"`

	// Replicas is the requested number of Groups with the target properties
	// +kubebuilder:validation:Minimum=1
	Replicas int `json:"replicas,omitempty"`

	// Properties are some admin-defined characteristic of the NodeGroup such as
	// 'memoryxMemory:1TB' or 'GPT3:true'.
	Properties map[string]string `json:"properties,omitempty"`
}

type NodeGroupGrant struct {
	// Name of the node group request.
	Name string `json:"name"`

	// Names of the granted nodeGroups.
	NodeGroups []string `json:"nodeGroups,omitempty"`
}

// ResourceLockSpec defines the desired state of ResourceLock
type ResourceLockSpec struct {
	// QueueName is the name of the queue to which the client wishes to reserve resources. Empty string means the
	// default queue.
	// +kubebuilder:default="execute"
	// +kubebuilder:validation:Enum=compile;execute;prioritized-compile;prioritized-execute;reservation
	QueueName string `json:"queueName,omitempty"`

	// ResourceRequests is the list of resources which the client wishes to reserve.
	ResourceRequests []ResourceRequest `json:"resourceRequests,omitempty"`

	// GroupResourceRequests is the list of resources, grouped by properties.group, which the client wishes to reserve.
	GroupResourceRequests []GroupResourceRequest `json:"groupResourceRequests,omitempty"`

	// ResourceRequestUpperBounds is the list of resources which the client wishes to reserve over the ResourceRequests.
	// Note: the logic here is optimized for Runtime services so it sacrifices some flexibility for simplicity.
	// Specifically, for each Replica type (corresponding to a ResourceRequest.Name), the desire of the client is to
	// reserve subresources >= the request for that replica type and that reservation should be the same across all
	// node groups, hence this field is not placed in the ResourceRequest.
	GroupResourceRequestUpperBounds []ResourceUpperBound `json:"groupResourceRequestUpperBounds,omitempty"`

	// Deprecated: Use ResourceRequests instead.
	// SystemRequests is the list of types of systems the client wishes to reserve for exclusive use. Currently, this
	// list may only have a single member of type cs2 with replicas > 0.
	SystemRequests []SystemRequest `json:"systemRequests,omitempty"`

	// Deprecated: Use GroupResourceRequests instead.
	NodeGroupRequests []NodeGroupRequest `json:"nodeGroupRequests,omitempty"`

	// Priority of the resource lock
	Priority *int `json:"priority,omitempty"`
}

// ResourceLockStatus defines the observed state of ResourceLock
type ResourceLockStatus struct {
	// +kubebuilder:default=Pending
	// +kubebuilder:validation:Enum=Pending;Granted;Error
	State LockState `json:"state,omitempty"`

	// +kubebuilder:default=""
	Message LockStatusMessage `json:"message,omitempty"`

	// Grants is the list of resources which were granted to the client's corresponding ResourceRequests.
	ResourceGrants []ResourceGrant `json:"resourceGrants,omitempty"`

	// Granted resources for the requested resources. This is only set when the state is Granted. There are Count
	// entries for each GroupResourceRequest in the spec.
	GroupResourceGrants []GroupResourceGrant `json:"groupResourceGrants,omitempty"`

	// ResourceGrant upper bound for each GroupResourceRequestUpperBound in the spec.
	GroupResourceGrantUpperBounds []ResourceUpperBound `json:"groupResourceGrantUpperBounds,omitempty"`

	// Deprecated: refer to Grants instead.
	SystemGrants []SystemGrant `json:"systemGrants,omitempty"`

	// Deprecated: refer to Grants instead.
	NodeGroupGrants []NodeGroupGrant `json:"nodeGroupGrants,omitempty"`
}

// ResourceLock is the Schema for the resourcelock API
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=rl
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`
type ResourceLock struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ResourceLockSpec `json:"spec,omitempty"`

	// +kubebuilder:default={state: "Pending", message: ""}
	Status ResourceLockStatus `json:"status,omitempty"`
}

// ResourceLockList contains a list of ResourceLock
// +kubebuilder:object:root=true
type ResourceLockList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ResourceLock `json:"items"`
}

func (rl *ResourceLock) NamespacedName() string {
	return rl.Namespace + "/" + rl.Name
}

func (rl *ResourceLock) IsWeightStreaming() bool {
	return rl.Labels[wsapisv1.JobModeLabelKey] == wsapisv1.JobModeWeightStreaming
}

func (rl *ResourceLock) IsInference() bool {
	return rl.Labels[wsapisv1.JobModeLabelKey] == wsapisv1.JobModeInference
}

func (rl *ResourceLock) UseAxOnlyForRuntime() bool {
	return rl.Annotations[wsapisv1.UseAxOnlyForRuntimeKey] == wsapisv1.UseAxOnlyForRuntimeValue
}

func (rl *ResourceLock) IsPending() bool {
	return rl.Status.State == LockPending
}

func (rl *ResourceLock) IsGranted() bool {
	return rl.Status.State == LockGranted
}

func (rl *ResourceLock) HadFailed() bool {
	return rl.Status.State == LockError
}

func (rl *ResourceLock) ReservingOnly() bool {
	return rl.Spec.QueueName == ReservationQueueName
}

// child lock is the lock current inheriting reservation
func (rl *ResourceLock) ChildLockName() string {
	return rl.Annotations[wsapisv1.ChildJobAnnotKey]
}

func (rl *ResourceLock) SetChildLock(job string) {
	rl.Annotations[wsapisv1.ChildJobAnnotKey] = job
}

func (rl *ResourceLock) UnsetChildLock() {
	rl.Annotations[wsapisv1.ChildJobAnnotKey] = ""
}

func (rl *ResourceLock) ParentLockName() string {
	return rl.Annotations[wsapisv1.ParentJobAnnotKey]
}

func (rl *ResourceLock) SetParentLockName(lock string) {
	rl.Annotations[wsapisv1.ParentJobAnnotKey] = lock
}

// if subsidiary(compile), require an execute reservation to enable and can't reserve by itself
func (rl *ResourceLock) SetAsSubsidiaryReservation() {
	rl.Annotations[wsapisv1.SubsidiaryReservationKey] = "true"
}

func (rl *ResourceLock) IsSubsidiaryReservation() bool {
	return rl.Annotations[wsapisv1.SubsidiaryReservationKey] == "true"
}

// get subsidiary(compile) lock, only applies to first execute job
func (rl *ResourceLock) GetSubsidiaryReservationName() string {
	return rl.Annotations[wsapisv1.SubsidiaryReservationKey]
}

func (rl *ResourceLock) SetSkipBr() {
	rl.Annotations[wsapisv1.JobSkipBRKey] = "true"
}

func (rl *ResourceLock) IsSkipBr() bool {
	return rl.Annotations[wsapisv1.JobSkipBRKey] == "true"
}

// indicating it's a child job inheriting reservation
// parent job is not enough since compile-eval flow can also have parent job
func (rl *ResourceLock) HasReservation() bool {
	return rl.Annotations[wsapisv1.HasReservationKey] != ""
}

// annotate to trigger reservation query
func (rl *ResourceLock) QueryReservation() {
	rl.Annotations[wsapisv1.QueryReservationKey] = "true"
}

// return true if queried and status not set
func (rl *ResourceLock) ReservationQueried() bool {
	return rl.Annotations[wsapisv1.QueryReservationKey] == "true"
}

// set reservation system count
func (rl *ResourceLock) SetReservedSystems(count int, context string) {
	rl.Annotations[wsapisv1.QueryReservationKey] = strconv.Itoa(count)
	rl.Annotations[wsapisv1.QueryReservationContextKey] = context
}

// return reservation system count
func (rl *ResourceLock) GetReservedSystems() (int, string, error) {
	res, err := strconv.Atoi(rl.Annotations[wsapisv1.QueryReservationKey])
	if err != nil {
		return 0, "", err
	}
	return res, rl.Annotations[wsapisv1.QueryReservationContextKey], nil
}

func (rl *ResourceLock) GetSessionFilter() string {
	if ns := rl.Annotations[NamespaceKey]; ns != "" {
		return ns
	}
	// for backwards compatible check, todo: remove at rel-2.8
	if len(rl.Spec.ResourceRequests) > 0 &&
		len(rl.Spec.ResourceRequests[0].Properties) > 0 &&
		len(rl.Spec.ResourceRequests[0].Properties[NamespaceKey]) > 0 {
		return rl.Spec.ResourceRequests[0].Properties[NamespaceKey][0]
	}
	return ""
}

func (rl *ResourceLock) GetPlatformVersionFilter() string {
	return rl.Labels[PlatformVersionPropertyKey]
}

func (rl *ResourceLock) GetSystemVersionFilter() string {
	return rl.Labels[SystemVersionPropertyKey]
}

func (rl *ResourceLock) RequireSystemVersionFilter() bool {
	return rl.Labels[SystemVersionPropertyKey] != "" || rl.Labels[SystemVersionRequired] != ""
}

// return list of redundant sessions assigned
func (rl *ResourceLock) GetAssignedRedundancy() []string {
	if val := rl.Labels[AssignedRedundancyPropertyKey]; val != "" {
		return strings.Split(val, wsapisv1.LabelSeparator)
	}
	return nil
}

func (rl *ResourceLock) GetPriority() int {
	// Existing wsjobs does not have priority assigned when created.
	// We will use the default job priority value for those wsjobs.
	if rl.Spec.Priority == nil {
		return wsapisv1.DefaultJobPriorityValue
	}
	return *rl.Spec.Priority
}

func (rl *ResourceLock) GetRtMaxLimitOverhead() (int, float64) {
	overheadGb := wsapisv1.RtNodeMaxLimitReservedMemGB
	overheadRatio := wsapisv1.RtNodeMaxLimitReservedMemRatio
	if val, err := strconv.Atoi(rl.Annotations[wsapisv1.RtNodeMaxLimitReservedMemGbKey]); err == nil && val > 0 {
		overheadGb = val
	}
	if val, err := strconv.ParseFloat(rl.Annotations[wsapisv1.RtNodeMaxLimitReservedMemRatioKey], 64); err == nil && val > 0 && val < 1 {
		overheadRatio = val
	}
	return overheadGb, overheadRatio
}

func (rl *ResourceLock) SetPopulatedNgTypes(popTypes []string) {
	rl.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = strings.Join(popTypes, ",")
}

func (rl *ResourceLock) GetPopulatedNgTypes() []string {
	popNgTypes := []string{wsapisv1.PopNgTypeRegular, wsapisv1.PopNgTypeLarge, wsapisv1.PopNgTypeXLarge}
	if rl.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] != "" {
		popNgTypes = strings.Split(rl.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes], ",")
	}
	return popNgTypes
}

func (rl *ResourceLock) GetNonXLPopulatedNgTypes() []string {
	var popNgTypes []string
	for _, ngType := range rl.GetPopulatedNgTypes() {
		if ngType == wsapisv1.PopNgTypeRegular || ngType == wsapisv1.PopNgTypeLarge {
			popNgTypes = append(popNgTypes, ngType)
		}
	}
	return popNgTypes
}

func (rl *ResourceLock) GetXLPopulatedNgTypes() []string {
	if slices.Contains(rl.GetPopulatedNgTypes(), wsapisv1.PopNgTypeXLarge) {
		return []string{wsapisv1.PopNgTypeXLarge}
	}
	return []string{}
}

func (rl *ResourceLock) IsPopulatedNgCountDefined() bool {
	_, ok := rl.Annotations[wsapisv1.WsJobPopulatedNodegroupCount]
	return ok
}

func (rl *ResourceLock) GetSystemsRequested() int {
	count := 0
	for _, rr := range rl.Spec.ResourceRequests {
		if rr.Name == SystemsRequestName {
			count += rr.Count
		}
	}
	return count
}

func (rl *ResourceLock) GetSystemsAcquired() int {
	count := 0
	if !rl.IsGranted() {
		return count
	}
	for _, rg := range rl.Status.ResourceGrants {
		if rg.Name == SystemsRequestName {
			count += len(rg.Resources)
		}
	}
	return count
}

func (rl *ResourceLock) GetAxNodesRequested() int {
	defaultAxNodesRequested := rl.GetSystemsRequested() * wsapisv1.AxCountPerSystem
	val, _ := strconv.Atoi(rl.Annotations[wsapisv1.WsJobActivationNodeCount])
	if val < defaultAxNodesRequested {
		return defaultAxNodesRequested
	}
	return val
}

func (rl *ResourceLock) GetNgCountAcquired() (int, int) {
	popGroupCount, depopGroupCount := 0, 0
	if !rl.IsGranted() {
		return popGroupCount, depopGroupCount
	}
	for _, grg := range rl.Status.GroupResourceGrants {
		if !strings.HasPrefix(grg.Name, NodegroupRequestPrefix) {
			continue
		}
		if val, ok := grg.Annotations[wsapisv1.MemxPopNodegroupProp]; ok {
			if val == strconv.FormatBool(true) {
				popGroupCount += 1
			} else {
				depopGroupCount += 1
			}
		}
	}
	return popGroupCount, depopGroupCount
}

func (rl *ResourceLock) GetMaxAllowedPopulatedNgs(popGroupTypes []string) int {
	// As of rel-2.4, we only allow at most 2 pop groups if the pop groups are xlarge,
	// or unless the number of pop groups was explicitly specified in the annotations.
	maxPopNgCount := 2
	defaultPopNgCount := 1
	valStr, ok := rl.Annotations[wsapisv1.WsJobPopulatedNodegroupCount]
	if ok {
		val, err := strconv.Atoi(valStr)
		if err != nil {
			return defaultPopNgCount
		}
		return val
	}
	if slices.Contains(popGroupTypes, wsapisv1.PopNgTypeXLarge) {
		return maxPopNgCount
	}
	return defaultPopNgCount
}

func (rl *ResourceLock) SetPriority(priority int) {
	rl.Spec.Priority = &priority
}

func (rl *ResourceLock) AllowUnhealthySystems() bool {
	return rl.Annotations[wsapisv1.EnableAllowUnhealthySystems] == "true"
}

func (rl *ResourceLockList) NamespacedNames() []string {
	var nsNames []string
	for _, l := range rl.Items {
		nsNames = append(nsNames, l.NamespacedName())
	}
	return nsNames
}

func (rl *ResourceLock) IsInferenceJobSharingEnabled() bool {
	return rl.Annotations[wsapisv1.InferenceJobSharingAnnotKey] == strconv.FormatBool(true)
}

func init() {
	SchemeBuilder.Register(&ResourceLock{}, &ResourceLockList{})
}

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
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	LabelSeparator               = "__"
	IsRedundantModeKey           = "cerebras/redundant-mode"        // label on session indicating resources used as redundancy by other sessions
	AssignedRedundancyKey        = "cerebras/assigned-redundancy"   // label on session indicating allowed to use redundant sessions as redundancy
	NSLabel                      = "cerebras/namespace"             // label on cluster-scoped resources (PV) that should cascade delete
	UserNSLabel                  = "user-namespace"                 // label on NS object for user-owned namespaces
	SystemNSLabel                = "system-namespace"               // lable on NS object own by system-namespace
	SessionWorkloadTypeKey       = "cerebras/session-workload-type" // label on workload type of the session
	UnassignedResourcesCM        = "unassigned-resources"
	UnassignedResourcesCMDataKey = "nsr.yaml"
	UnassignedNamespace          = "*unassigned"

	SessionWarningAnnoKeyPrefix      = "cerebras/session-warning"       // annotation of session warning
	SessionPortAffinityAnnoKeyPrefix = "cerebras/session-port-affinity" // annotation of session CS port affinity

	maxRequestHistory = 5

	// MaxCsPortAffinityDifference - the value is only experimental, and subject to change on user feedback.
	// --suppress-affinity-error flag in csctl will suppress the error from failing the NSR.
	MaxCsPortAffinityDifference = 3
)

type SessionWorkloadType string

const (
	WorkloadTypeNotSet    = SessionWorkloadType("")
	InferenceWorkloadType = SessionWorkloadType("inference")
	TrainingWorkloadType  = SessionWorkloadType("training")
	SDKWorkloadType       = SessionWorkloadType("sdk")
)

func (p SessionWorkloadType) String() string {
	return string(p)
}

func StringToSessionWorkloadType(s string) SessionWorkloadType {
	m := map[string]SessionWorkloadType{
		WorkloadTypeNotSet.String():    WorkloadTypeNotSet,
		InferenceWorkloadType.String(): InferenceWorkloadType,
		TrainingWorkloadType.String():  TrainingWorkloadType,
		SDKWorkloadType.String():       SDKWorkloadType,
	}
	if workloadType, in := m[s]; in {
		return workloadType
	}
	return WorkloadTypeNotSet
}

type NSRUpdateMode string

const (
	ModeNotSet                = NSRUpdateMode("")
	SetResourceSpecMode       = NSRUpdateMode("SetResourceSpec")
	SetResourceSpecDryRunMode = NSRUpdateMode("SetResourceSpecDryRun")
	DebugRemoveMode           = NSRUpdateMode("DebugRemove")
	DebugRemoveAllMode        = NSRUpdateMode("DebugRemoveAll")
	DebugAppendMode           = NSRUpdateMode("DebugAppend")
	DebugSetMode              = NSRUpdateMode("DebugSet")
)

// RequestParams describes details of the user's request which the system translates into additional Nodegroup/Nodes
// requests of the SystemRequest.
type RequestParams struct {
	// UpdateMode defines how the RequestParams will be reconciled. SetResourceSpecMode replaces the previous RequestParams
	// and re-evaluates the given '*Count' Params to derive a new resource allocation meeting the Count criterias.
	// SetResourceSpecDryRun evaluates the '*Count' params same as SetResourceSpecMode but does not apply them and
	// only updates the UpdateHistory with a record containing *Count params representing the resource capabilities
	// of the session after the proposed change and the *Name params with the final names if the Dryrun change was effective.
	// Debug modes add/remove/set specific resources reference in the *Name params. *Name params are
	// ignored in SetCapabilities mode and *Count params are ignored in debug modes.
	// +kubebuilder:default=SetResourceSpec
	// +kubebuilder:validation:Enum="";SetResourceSpec;SetResourceSpecDryRun;DebugRemove;DebugRemoveAll;DebugAppend;DebugSet;
	UpdateMode NSRUpdateMode `json:"updateMode"`

	// SystemCount specifies number of systems to allocate to the session.
	SystemCount *int `json:"systemCount,omitempty"`

	// ParallelCompileCount is an estimate of the number of parallel compiles which can run in parallel using default
	// memory settings. Implies some number of management nodes reserved.
	//+kubebuilder:validation:Minimum=0
	ParallelCompileCount *int `json:"parallelCompileCount,omitempty"`

	// ParallelExecuteCount is an estimate for the number of a parallel trains which can run in parallel using default
	// memory settings. Implies some number of management nodes reserved and some number of fully-populated memx racks.
	//+kubebuilder:validation:Minimum=0
	ParallelExecuteCount *int `json:"parallelExecuteCount,omitempty"`

	// LargeMemoryRackCount are the number of fully-populated large (1TB) memx racks for the system to reserve.
	//+kubebuilder:validation:Minimum=0
	LargeMemoryRackCount *int `json:"largeMemoryRackCount,omitempty"`

	// XLargeMemoryRackCount are the number of fully-populated xlarge (2.3TB) memx racks for the system to reserve.
	//+kubebuilder:validation:Minimum=0
	XLargeMemoryRackCount *int `json:"xlargeMemoryRackCount,omitempty"`

	// SystemNames specifies systems to add/remove/set in debug mode
	SystemNames []string `json:"systemNames,omitempty"`

	// NodegroupNames specifies nodegroups to add/remove/set in debug mode
	NodegroupNames []string `json:"nodegroupNames,omitempty"`

	// ActivationNodeNames specifies activation nodes to add/remove/set in debug mode
	ActivationNodeNames []string `json:"activationNodeNames,omitempty"`

	// CoordinatorNodeNames specifies coordinator nodes to add/remove/set in debug mode
	CoordinatorNodeNames []string `json:"coordinatorNodeNames,omitempty"`

	// ManagementNodeNames specifies management nodes to add/remove/set in debug mode
	ManagementNodeNames []string `json:"managementNodeNames,omitempty"`

	// InferenceDriverNodeNames specifies inference driver nodes to add/remove/set in debug mode
	InferenceDriverNodeNames []string `json:"inferenceDriverNodeNames,omitempty"`

	// AutoSelectActivationNodes automatically selects activation nodes to match systems
	// with best affinity coverage in respective resource stamps
	AutoSelectActivationNodes bool `json:"autoSelectActivationNodes,omitempty"`

	// SuppressAffinityErrors ignore errors from CS port affinity requirements
	SuppressAffinityErrors bool `json:"suppressAffinityErrors,omitempty"`

	// AutoSelectInferenceDriverNodes automatically selects inference driver node in the possible
	// resource stamp with the most systems
	AutoSelectInferenceDriverNodes bool `json:"autoSelectInferenceDriverNodes,omitempty"`
}

type ResourceSpec struct {
	// ParallelCompileCount is an estimate of the number of parallel compiles which can run in parallel using default
	// memory settings
	ParallelCompileCount int `json:"parallelCompileCount,omitempty"`

	// ParallelExecuteCount is an estimate for the number of a parallel trains which can run in parallel using default
	// memory settings.
	ParallelExecuteCount int `json:"parallelExecuteCount,omitempty"`

	// LargeMemoryRackCount are the number of fully-populated large (1TB) memx racks for the system to reserve.
	LargeMemoryRackCount int `json:"largeMemoryRackCount,omitempty"`

	// XLargeMemoryRackCount are the number of fully-populated xlarge (2.3TB) memx racks for the system to reserve.
	XLargeMemoryRackCount int `json:"xlargeMemoryRackCount,omitempty"`
}

func (in *RequestParams) GetSystemCount() int {
	if in.SystemCount == nil {
		return 0
	}
	return *in.SystemCount
}

func (in *RequestParams) GetParallelCompileCount() int {
	if in.ParallelCompileCount == nil {
		return 0
	}
	return *in.ParallelCompileCount
}

func (in *RequestParams) GetParallelExecuteCount() int {
	if in.ParallelExecuteCount == nil {
		return 0
	}
	return *in.ParallelExecuteCount
}

func (in *RequestParams) GetLargeMemoryRackCount() int {
	if in.LargeMemoryRackCount == nil {
		return 0
	}
	return *in.LargeMemoryRackCount
}

func (in *RequestParams) GetXLargeMemoryRackCount() int {
	if in.XLargeMemoryRackCount == nil {
		return 0
	}
	return *in.XLargeMemoryRackCount
}

func (in *RequestParams) IsReleaseAllResources() bool {
	return in.UpdateMode == SetResourceSpecMode &&
		in.GetSystemCount() == 0 &&
		in.GetParallelCompileCount() == 0 &&
		in.GetParallelExecuteCount() == 0 &&
		in.GetLargeMemoryRackCount() == 0 &&
		in.GetXLargeMemoryRackCount() == 0
}

type ReservationRequest struct {
	// RequestParams are user-friendly request details. If submitted, the reconciler will update the System request with
	// the translated RequestParams request.
	RequestParams RequestParams `json:"requestParameters,omitempty"`

	// RequestUid is a random id used for k8s client to track the progress of their request. Clients should not update
	// an NSR.spec with a new request until the spec.RequestUid == status.requestHistory[0].requestId so as not to overwrite
	// another client's reservation progress.
	RequestUid string `json:"requestUid,omitempty"`
}

type NamespaceReservationSpec struct {
	ReservationRequest `json:",inline"`
}

type RequestStatus struct {
	ReconcileTimestamp metav1.Time `json:"reconcileTimestamp,omitempty"`

	// Succeeded is true if the request was granted
	Succeeded bool `json:"succeeded"`

	// Message is populated with details of the request attempt failure.
	Message string `json:"message,omitempty"`

	Request ReservationRequest `json:"request"`
}

type NamespaceReservationStatus struct {
	/* Reserved resources */

	Systems    []string `json:"systems,omitempty"`
	Nodegroups []string `json:"nodegroups,omitempty"`
	Nodes      []string `json:"nodes,omitempty"`

	// Current estimate of resources
	CurrentResourceSpec ResourceSpec `json:"currentResourceSpec,omitempty"`

	/* Reconciler state management */

	// GrantedRequest is a copy of the last granted ReservationRequest reflected in the reserved resources.
	GrantedRequest ReservationRequest `json:"grantedRequest,omitempty"`

	// UpdateHistory is the last few requests submitted to the NSR.spec
	UpdateHistory []RequestStatus `json:"updateHistory,omitempty"`
}

func (s *NamespaceReservationStatus) GetLastAttemptedRequest() RequestStatus {
	if len(s.UpdateHistory) > 0 {
		return s.UpdateHistory[0]
	}
	return RequestStatus{}
}

func (s *NamespaceReservationStatus) getResources(resourceType string) []string {
	switch resourceType {
	case "system":
		return s.Systems
	case "nodegroup":
		return s.Nodegroups
	case "node":
		return s.Nodes
	default:
		panic("invalid resourceType")
	}
}

func (s *NamespaceReservationStatus) setResources(resourceType string, resources []string) {
	sort.Strings(resources)
	switch resourceType {
	case "system":
		s.Systems = resources
	case "nodegroup":
		s.Nodegroups = resources
	case "node":
		s.Nodes = resources
	default:
		panic("invalid resourceType")
	}
}

func (s *NamespaceReservationStatus) RemoveAllocated(resourceType string, name string) {
	resources := s.getResources(resourceType)
	s.setResources(resourceType, RemoveElement(resources, name))
}

func (s *NamespaceReservationStatus) AddAllocated(resourceType, name string) {
	resources := s.getResources(resourceType)
	s.setResources(resourceType, append(resources, name))
}

func (s *NamespaceReservationStatus) GetAllocatedRIDs() []string {
	var rids []string
	for _, name := range s.Systems {
		rids = append(rids, "system/"+name)
	}
	for _, name := range s.Nodegroups {
		rids = append(rids, "nodegroup/"+name)
	}
	for _, name := range s.Nodes {
		rids = append(rids, "node/"+name)
	}
	return rids
}

func (s *NamespaceReservationStatus) HasResources() bool {
	return len(s.Systems)+len(s.Nodegroups)+len(s.Nodes) > 0
}

func (in *NamespaceReservationStatus) SetSuccessfulRequest(request ReservationRequest) {
	if request.RequestParams.UpdateMode != SetResourceSpecDryRunMode {
		in.GrantedRequest = request
	}
	in.addHistory(RequestStatus{
		ReconcileTimestamp: metav1.Now(),
		Succeeded:          true,
		Message:            "success",
		Request:            request,
	})
}

func (in *NamespaceReservationStatus) SetFailedRequest(request ReservationRequest, msg string) {
	in.addHistory(RequestStatus{
		ReconcileTimestamp: metav1.Now(),
		Message:            msg,
		Request:            request,
	})

}

func (in *NamespaceReservationStatus) addHistory(status RequestStatus) {
	history := []RequestStatus{status}
	keepHistory := maxRequestHistory
	if status.Request.RequestParams.UpdateMode == SetResourceSpecDryRunMode {
		keepHistory += 1
	}
	// discard previous dry runs, keeping a constant maxRequestHistory of non-dryruns
	for _, s := range in.UpdateHistory {
		if s.Request.RequestParams.UpdateMode != SetResourceSpecDryRunMode {
			history = append(history, s)
		}
		if len(history) == keepHistory {
			break
		}
	}
	in.UpdateHistory = history
}

func (in *NamespaceReservationStatus) SetGrantedIds(ids []string) {
	resources := map[string][]string{
		"system":    {},
		"nodegroup": {},
		"node":      {},
	}
	for _, id := range ids {
		typeName := strings.SplitN(id, "/", 2)
		resources[typeName[0]] = append(resources[typeName[0]], typeName[1])
	}
	in.setResources("system", resources["system"])
	in.setResources("nodegroup", resources["nodegroup"])
	in.setResources("node", resources["node"])
}

func RemoveElement(slice []string, v string) []string {
	vindex := -1
	for i := range slice {
		if slice[i] == v {
			vindex = i
			break
		}
	}
	if vindex > -1 {
		rv := make([]string, len(slice)-1)
		copy(rv, slice[:vindex])
		copy(rv[vindex:], slice[vindex+1:])
		return rv
	}
	return slice
}

//+genclient
//+genclient:nonNamespaced
//+resource:path=namespace
//+kubebuilder:object:root=true
//+kubebuilder:resource:shortName=nsr
//+kubebuilder:resource:scope=Cluster
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="systems",type=string,JSONPath=`.status.systems`
//+kubebuilder:printcolumn:name="nodegroups",type=string,priority=1,JSONPath=`.status.nodegroups`
//+kubebuilder:printcolumn:name="nodes",type=string,priority=1,JSONPath=`.status.nodes`
//+kubebuilder:printcolumn:name="executes",type=string,JSONPath=`.status.currentResourceSpec.parallelExecuteCount`
//+kubebuilder:printcolumn:name="compiles",type=string,JSONPath=`.status.currentResourceSpec.parallelCompileCount`
//+kubebuilder:printcolumn:name="largeMemRacks",type=string,JSONPath=`.status.currentResourceSpec.largeMemoryRackCount`
//+kubebuilder:printcolumn:name="xlargeMemRacks",type=string,JSONPath=`.status.currentResourceSpec.xlargeMemoryRackCount`

// NamespaceReservation is the Schema for the Namespace API
type NamespaceReservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NamespaceReservationSpec   `json:"spec,omitempty"`
	Status NamespaceReservationStatus `json:"status,omitempty"`
}

func (in *NamespaceReservation) GetLastUpdateTime() metav1.Time {
	if len(in.Status.UpdateHistory) > 0 {
		return in.Status.GetLastAttemptedRequest().ReconcileTimestamp
	}
	return in.CreationTimestamp
}

// return list of redundant sessions assigned
func (in *NamespaceReservation) GetAssignedRedundancy() []string {
	if val := in.Labels[AssignedRedundancyKey]; val != "" {
		return strings.Split(val, LabelSeparator)
	}
	return nil
}

func (in *NamespaceReservation) GetWorkloadType() SessionWorkloadType {
	// if SessionWorkloadTypeKey label not set, return value will be WorkloadTypeNotSet
	val := in.Labels[SessionWorkloadTypeKey]
	return StringToSessionWorkloadType(val)
}

//+kubebuilder:object:root=true

// NamespaceReservationList contains a list of NamespaceReservation
type NamespaceReservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NamespaceReservation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NamespaceReservation{}, &NamespaceReservationList{})
}

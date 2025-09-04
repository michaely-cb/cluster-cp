package lock

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	rtutil "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
	"cerebras.com/job-operator/common/resource/helpers"
)

// -- Event Recorder --

type fakeEvent struct {
	object    runtime.Object
	eventtype string
	reason    string
	message   string
}
type fakeEventRecorder struct {
	events []fakeEvent

	grantCount   int
	invalidCount int
}

func (f *fakeEventRecorder) getEvents() []fakeEvent {
	return f.events
}

func (f *fakeEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	f.events = append(f.events, fakeEvent{object, eventtype, reason, message})
	if reason == EventSchedulingSucceeded {
		f.grantCount += 1
	} else if reason == EventSchedulingFailed {
		f.invalidCount += 1
	} else {
		panic("unexpected event reason")
	}
}

func (f *fakeEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	f.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (f *fakeEventRecorder) AnnotatedEventf(object runtime.Object, _ map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	f.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (f *fakeEventRecorder) dropEvents() {
	f.events = []fakeEvent{}
}

// === ResourceLockBuilder ===
// structs for building ResourceLocks to be able to make locks using chaining.

type LockBuilder struct {
	name                    string
	createTime              int64
	queueName               string
	enablePartialSystem     bool
	enableBalanced          bool
	skipBR                  bool
	useAxScheduling         bool
	useAxOnlyForRuntime     bool
	enableActKvssJobSharing bool
	annotations             map[string]string

	resources               []rlv1.ResourceRequest
	groups                  []rlv1.GroupResourceRequest
	groupUpperBoundsRequest map[string]map[string]int
	popGroupTypes           map[string]bool
	actToCsxDomains         [][]int
	kvssToCsxDomains        [][]int
	swDriverToCsxDomains    [][]int
	swDriverSysIndex        int
	priority                int
	workflowId              string
	assignedRedundancy      []string
	parentLock              string
	childLock               string
	subsidiaryLock          string
	isSubsidiary            bool
	reservationStatus       string
	gracePeriodSeconds      int
	numSystems              int
	isInferenceJob          bool
}

type GroupRequestBuilder struct {
	parent *LockBuilder

	name     string
	count    int
	requests []rlv1.ResourceRequest
}

type groupedResourceRequestBuilder struct {
	parent *GroupRequestBuilder

	request rlv1.ResourceRequest
}

type GrantBuilder struct {
	lock *rlv1.ResourceLock
}

type GroupGrantBuilder struct {
	parent     *GrantBuilder
	name       string
	groupValue string
	grants     []rlv1.ResourceGrant
}

func NewLockBuilder(name string) *LockBuilder {
	return &LockBuilder{name: name, annotations: map[string]string{}, groupUpperBoundsRequest: map[string]map[string]int{}}
}

func (b *LockBuilder) WithCreateTime(t int64) *LockBuilder {
	b.createTime = t * 1000
	return b
}

func (b *LockBuilder) WithGracePendingPeriod(seconds int) *LockBuilder {
	b.gracePeriodSeconds = seconds
	return b
}

func (b *LockBuilder) WithSkipBR(skipBR bool) *LockBuilder {
	b.skipBR = skipBR
	return b
}

func (b *LockBuilder) EnablePartialMode() *LockBuilder {
	b.enablePartialSystem = true
	return b
}

func (b *LockBuilder) EnableBalanced() *LockBuilder {
	b.enableBalanced = true
	return b
}

func (b *LockBuilder) EnableJobSharing() *LockBuilder {
	b.enableActKvssJobSharing = true
	return b
}

func (b *LockBuilder) WithAxScheduling() *LockBuilder {
	b.useAxScheduling = true
	return b
}

func (b *LockBuilder) WithAxOnlyForRuntime() *LockBuilder {
	b.useAxOnlyForRuntime = true
	return b
}

func (b *LockBuilder) WithGroupRequest(name string, count int) *GroupRequestBuilder {
	return &GroupRequestBuilder{parent: b, name: name, count: count}
}

func (b *LockBuilder) WithUpperBoundsAndLimitRequest(name string, upper, limit int) *LockBuilder {
	b.groupUpperBoundsRequest[name] = wscommon.EnsureMap(b.groupUpperBoundsRequest[name])
	b.groupUpperBoundsRequest[name]["memory"] = upper
	b.groupUpperBoundsRequest[name]["memory-limit"] = limit
	return b
}

func (b *LockBuilder) WithUpperBoundsRequest(name string, mem int) *LockBuilder {
	b.groupUpperBoundsRequest[name] = wscommon.EnsureMap(b.groupUpperBoundsRequest[name])
	b.groupUpperBoundsRequest[name]["memory"] = mem
	return b
}

func (b *LockBuilder) WithActToCsxDomainsMap(m [][]int) *LockBuilder {
	b.actToCsxDomains = m
	return b
}

func (b *LockBuilder) WithKvssToCsxDomainsMap(m [][]int) *LockBuilder {
	b.kvssToCsxDomains = m
	return b
}

func (b *LockBuilder) WithSWDriverToCsxDomainsMap(m [][]int) *LockBuilder {
	b.swDriverToCsxDomains = m
	return b
}

func (b *LockBuilder) WithSWDriverSysIndex(sysIndex int) *LockBuilder {
	b.swDriverSysIndex = sysIndex
	return b
}

func (b *LockBuilder) WithNumSystems(n int) *LockBuilder {
	b.numSystems = n
	return b
}

func (b *LockBuilder) WithPriority(priority int) *LockBuilder {
	b.priority = priority
	return b
}

func (b *LockBuilder) WithWorkflowId(workflowId string) *LockBuilder {
	b.workflowId = workflowId
	return b
}

func (b *LockBuilder) WithAssignedRedundancy(sessions []string) *LockBuilder {
	b.assignedRedundancy = sessions
	return b
}

func (b *LockBuilder) WithParentLock(lock string) *LockBuilder {
	b.parentLock = lock
	return b
}

func (b *LockBuilder) WithChildLock(lock string) *LockBuilder {
	b.childLock = lock
	return b
}

func (b *LockBuilder) SetIsSubsidiary() *LockBuilder {
	b.isSubsidiary = true
	return b
}

func (b *LockBuilder) SetSubsidiaryLock(lock string) *LockBuilder {
	b.subsidiaryLock = lock
	return b
}

func (b *LockBuilder) WithReservationStatus(status string) *LockBuilder {
	b.reservationStatus = status
	return b
}

func (b *LockBuilder) IsInference() *LockBuilder {
	b.isInferenceJob = true
	return b
}

func (b *LockBuilder) BuildPendingForNS(ns string) *rlv1.ResourceLock {
	var upperBounds []rlv1.ResourceUpperBound
	for k, r := range b.groupUpperBoundsRequest {
		upperBounds = append(upperBounds, rlv1.ResourceUpperBound{
			Name:         k,
			Subresources: resource.Subresources(r).Copy(),
		})
	}
	annotations := wscommon.CopyMap(b.annotations)
	annotations[wsapisv1.EnablePartialSystemKey] = strconv.FormatBool(b.enablePartialSystem)
	annotations[wsapisv1.BuildBalancedBrTreeKey] = strconv.FormatBool(b.enableBalanced)
	if b.skipBR {
		annotations[wsapisv1.JobSkipBRKey] = "true"
	}
	if b.useAxOnlyForRuntime {
		annotations[wsapisv1.UseAxOnlyForRuntimeKey] = wsapisv1.UseAxOnlyForRuntimeValue
	}
	if b.parentLock != "" {
		annotations[wsapisv1.ParentJobAnnotKey] = b.parentLock
	}
	if b.childLock != "" {
		annotations[wsapisv1.ChildJobAnnotKey] = b.childLock
	}
	if b.isSubsidiary {
		annotations[wsapisv1.SubsidiaryReservationKey] = "true"
	} else if b.subsidiaryLock != "" {
		annotations[wsapisv1.SubsidiaryReservationKey] = b.subsidiaryLock
	}
	if b.reservationStatus != "" {
		annotations[commonv1.ResourceReservationStatus] = b.reservationStatus
	}
	if b.enableActKvssJobSharing {
		annotations[wsapisv1.InferenceJobSharingAnnotKey] = "true"
	}
	labels := map[string]string{
		wsapisv1.WorkflowIdLabelKey: b.workflowId,
	}
	if b.isInferenceJob {
		labels[wsapisv1.JobModeLabelKey] = wsapisv1.JobModeInference
	}
	rl := &rlv1.ResourceLock{
		ObjectMeta: metav1.ObjectMeta{
			Name:              b.name,
			Namespace:         ns,
			CreationTimestamp: metav1.Unix(b.createTime, 0),
			Labels:            labels,
			Annotations:       annotations,
		},
		Spec: rlv1.ResourceLockSpec{
			QueueName:                       b.queueName,
			ResourceRequests:                b.resources,
			GroupResourceRequests:           b.groups,
			GroupResourceRequestUpperBounds: upperBounds,
			Priority:                        &b.priority,
		},
		Status: rlv1.ResourceLockStatus{
			State: rlv1.LockPending,
		},
	}

	for _, groupResourceRequest := range rl.Spec.GroupResourceRequests {
		switch {
		case strings.HasPrefix(groupResourceRequest.Name, rlv1.ActivationRequestName):
			populateActAnnotations(b.actToCsxDomains, rl)
		case strings.HasPrefix(groupResourceRequest.Name, rlv1.KVStorageServerRequestName):
			populateKvssAnnotations(b.numSystems, b.kvssToCsxDomains, rl)
		case strings.HasPrefix(groupResourceRequest.Name, rlv1.SWDriverRequestName):
			populateSwDriverAnnotations(b.swDriverSysIndex, b.swDriverToCsxDomains, rl)
		default:
			continue
		}
	}

	if b.gracePeriodSeconds != 0 {
		rl.ObjectMeta.Annotations[wsapisv1.PendingLockGracePeriodAnnotKey] = strconv.Itoa(b.gracePeriodSeconds)
	}

	if len(b.popGroupTypes) > 0 {
		rl.ObjectMeta.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] = strings.Join(wscommon.Keys(b.popGroupTypes), ",")
	}

	if ns == "" {
		rl.Namespace = wscommon.Namespace
		return rl
	}
	rl.Annotations[resource.NamespacePropertyKey] = ns
	if b.assignedRedundancy != nil {
		rl.Labels[resource.AssignedRedundancyPropertyKey] = strings.Join(b.assignedRedundancy, ",")
	}
	return rl
}

func populateActAnnotations(toCsxDomains [][]int, rl *rlv1.ResourceLock) {
	var domains []string
	for _, perPodDomains := range toCsxDomains {
		var perPodDomainsStrs []string
		for _, domain := range wscommon.Sorted(perPodDomains) {
			perPodDomainsStrs = append(perPodDomainsStrs, strconv.Itoa(domain))
		}
		domains = append(domains, strings.Join(perPodDomainsStrs, "-"))
	}
	serializedDomains := strings.Join(domains, ",")
	rl.ObjectMeta.Annotations = wscommon.EnsureMap(rl.Annotations)
	rl.ObjectMeta.Annotations[wsapisv1.RuntimeTargetedDomainsKey] = serializedDomains
}

func populateSwDriverAnnotations(systemIndex int, toCsxDomains [][]int, rl *rlv1.ResourceLock) {
	var domains []string
	for _, perPodDomains := range toCsxDomains {
		var perPodDomainsStrs []string
		for _, domain := range wscommon.Sorted(perPodDomains) {
			perPodDomainsStrs = append(perPodDomainsStrs, strconv.Itoa(domain))
		}
		domains = append(domains, strings.Join(perPodDomainsStrs, "-")+"@"+strconv.Itoa(systemIndex))
	}
	serializedDomains := strings.Join(domains, ",")
	rl.ObjectMeta.Annotations = wscommon.EnsureMap(rl.Annotations)
	compressed, _ := wscommon.Gzip([]byte(serializedDomains))
	rl.ObjectMeta.Annotations[wsapisv1.AllSwdrTargetedDomainsKey] = base64.StdEncoding.EncodeToString(compressed)
}

func populateKvssAnnotations(numSystems int, toCsxDomains [][]int, rl *rlv1.ResourceLock) {
	var domains []string
	for _, perPodDomains := range toCsxDomains {
		var perPodDomainsStrs []string
		for _, domain := range wscommon.Sorted(perPodDomains) {
			perPodDomainsStrs = append(perPodDomainsStrs, strconv.Itoa(domain))
		}
		for i := 0; i < numSystems; i++ {
			domains = append(domains, strings.Join(perPodDomainsStrs, "-")+"@"+strings.Join([]string{strconv.Itoa(i), strconv.Itoa((i + 1) % numSystems)}, "-"))
		}
	}
	serializedDomains := strings.Join(domains, ",")
	rl.ObjectMeta.Annotations = wscommon.EnsureMap(rl.Annotations)
	compressed, _ := wscommon.Gzip([]byte(serializedDomains))
	rl.ObjectMeta.Annotations[wsapisv1.AllPromptCachingTargetedDomainsKey] = base64.StdEncoding.EncodeToString(compressed)
}

func (b *LockBuilder) BuildPending() *rlv1.ResourceLock {
	return b.BuildPendingForNS("")
}

func (b *LockBuilder) BuildGranted() *GrantBuilder {
	rl := b.BuildPending()
	rl.Status.State = rlv1.LockGranted
	return &GrantBuilder{lock: rl}
}

func (b *GroupRequestBuilder) BuildGroupRequest() *LockBuilder {
	b.parent.groups = append(b.parent.groups, rlv1.GroupResourceRequest{
		Name:             b.name,
		Count:            b.count,
		ResourceRequests: b.requests,
	})
	return b.parent
}

func (b *GroupRequestBuilder) WithRequest(name string, count int) *groupedResourceRequestBuilder {
	return &groupedResourceRequestBuilder{
		parent:  b,
		request: rlv1.ResourceRequest{Name: name, Count: count, Properties: map[string][]string{}, Subresources: map[string]int{}},
	}
}

func (b *GroupRequestBuilder) WithResourceRequest(name string, count int, props ...string) *groupedResourceRequestBuilder {
	return &groupedResourceRequestBuilder{
		parent:  b,
		request: rlv1.ResourceRequest{Name: name, Count: count, Properties: wscommon.MustSplitPropsVals(props)},
	}
}

func (b *GroupRequestBuilder) WithNodeRequest(name string, count int, props ...string) *groupedResourceRequestBuilder {
	m := b.WithRequest(name, count)
	m.request.Properties = wscommon.MustSplitPropsVals(props)
	m.request.Type = rlv1.NodeResourceType
	return m
}

func (b *LockBuilder) WithQueue(name string) *LockBuilder {
	b.queueName = name
	return b
}

func (b *LockBuilder) WithSystemRequest(count int, props ...string) *LockBuilder {
	port := resource.DefaultSystemPortRequest
	if b.enablePartialSystem {
		port = 1
	}
	b.resources = append(b.resources,
		rlv1.ResourceRequest{
			Name:       rlv1.SystemsRequestName,
			Type:       rlv1.SystemResourceType,
			Properties: wscommon.MustSplitPropsVals(props),
			Subresources: map[string]int{resource.SystemSubresource: 1,
				resource.SystemPortSubresource: port},
			Count: count,
		})
	return b
}

func (b *LockBuilder) WithCompileSystemRequest(props ...string) *LockBuilder {
	b.resources = append(b.resources,
		rlv1.ResourceRequest{
			Name:         rlv1.SystemsRequestName,
			Type:         rlv1.SystemResourceType,
			Properties:   wscommon.MustSplitPropsVals(props),
			Subresources: map[string]int{resource.SystemSubresource: 0},
			Count:        1,
		})
	return b
}

func (b *LockBuilder) WithCoordinatorRequest(cpu, mem int, annotations map[string]string, props ...string) *LockBuilder {
	p := wscommon.MustSplitPropsVals(props)
	p["role-"+string(wscommon.RoleCoordinator)] = []string{""}
	b.resources = append(b.resources,
		rlv1.ResourceRequest{
			Name:         rlv1.CoordinatorRequestName,
			Type:         rlv1.NodeResourceType,
			Properties:   p,
			Subresources: map[string]int{resource.PodSubresource: 1, resource.CPUSubresource: cpu, resource.MemSubresource: mem},
			Count:        1,
			Annotations:  annotations,
		})
	return b
}

func (b *groupedResourceRequestBuilder) Role(role string) *groupedResourceRequestBuilder {
	b.request.Properties["role-"+role] = []string{""}
	return b
}

func (b *groupedResourceRequestBuilder) JobSharing() *groupedResourceRequestBuilder {
	if b.parent.parent.enableActKvssJobSharing {
		b.request.Annotations = map[string]string{wsapisv1.InferenceJobSharingAnnotKey: "true"}
	}
	return b
}

func (b *groupedResourceRequestBuilder) CpuMem(cpu, mem int) *GroupRequestBuilder {
	b.request.Subresources[resource.CPUSubresource] = cpu
	b.request.Subresources[resource.MemSubresource] = mem
	b.parent.requests = append(b.parent.requests, b.request)
	return b.parent
}

func (b *GrantBuilder) WithResourceGrant(requestName string, resIds ...string) *GrantBuilder {
	var rv []rlv1.Res
	for _, rid := range resIds {
		rtype, rname := resource.MustRIDToTypeName(rid)
		rv = append(rv, rlv1.Res{Type: rtype, Name: rname})
	}
	b.lock.Status.ResourceGrants = append(b.lock.Status.ResourceGrants, rlv1.ResourceGrant{Name: requestName, Resources: rv})
	return b
}

func (b *GrantBuilder) WithSystemsGrant(resIds ...string) *GrantBuilder {
	return b.WithResourceGrant(rlv1.SystemsRequestName, resIds...)
}

func (b *GrantBuilder) WithCoordinatorGrant(resId string) *GrantBuilder {
	return b.WithResourceGrant(rlv1.CoordinatorRequestName, resId)
}

func (b *GrantBuilder) AppendGroupGrant(name, groupValue string) *GroupGrantBuilder {
	return &GroupGrantBuilder{parent: b, name: name, groupValue: groupValue}
}

func (b *GroupGrantBuilder) WithGrant(requestName string, resIds ...string) *GroupGrantBuilder {
	var rv []rlv1.Res
	for _, rid := range resIds {
		tn := strings.Split(rid, "/")
		rv = append(rv, rlv1.Res{Type: tn[0], Name: tn[1]})
	}
	b.grants = append(b.grants, rlv1.ResourceGrant{Name: requestName, Resources: rv})
	return b
}

func (b *GroupGrantBuilder) Build() *GrantBuilder {
	b.parent.lock.Status.GroupResourceGrants = append(b.parent.lock.Status.GroupResourceGrants, rlv1.GroupResourceGrant{
		Name:           b.name,
		GroupingValue:  b.groupValue,
		ResourceGrants: b.grants,
	})
	return b.parent
}

func (b *GrantBuilder) Build() *rlv1.ResourceLock {
	return b.lock
}

// NewSystemCoordinatorLockBuilder creates a lock builder containing a primary request group with a system request as well as a node
// request for the coordinator
func NewSystemCoordinatorLockBuilder(name string, lockSys, totalSys, createTime int) *LockBuilder {
	return NewSystemLockBuilder(name, lockSys, createTime).
		WithCoordinatorRequest((helpers.DefaultMgtNodeCpu/totalSys)*lockSys, (helpers.DefaultMgtNodeMem/totalSys)*lockSys, nil)
}

func NewCompileLockBuilder(name string, resUsed, resTotal int) *LockBuilder {
	return NewLockBuilder(name).
		WithQueue(rlv1.CompileQueueName).
		WithCoordinatorRequest((helpers.DefaultMgtNodeCpu/resTotal)*resUsed, (helpers.DefaultMgtNodeMem/resTotal)*resUsed, nil).
		WithCompileSystemRequest()
}

func NewPrioritizedCompileLockBuilder(name string, resUsed, resTotal int) *LockBuilder {
	return NewLockBuilder(name).
		WithCreateTime(time.Now().Unix()/1000).
		WithGracePendingPeriod(3600).
		WithQueue(rlv1.PrioritizedCompileQueueName).
		WithCoordinatorRequest((helpers.DefaultMgtNodeCpu/resTotal)*resUsed, (helpers.DefaultMgtNodeMem/resTotal)*resUsed, nil).
		WithCompileSystemRequest()
}

func NewSystemLockBuilder(name string, lockSys, createTime int) *LockBuilder {
	return NewLockBuilder(name).
		WithCreateTime(int64(createTime)).
		WithSystemRequest(lockSys).
		WithQueue(rlv1.ExecuteQueueName)
}

// WithPrimaryNodeGroup adds a node group with a nodegroup for replica types typically added to the primary nodegroup
func (b *LockBuilder) WithPrimaryNodeGroup(props ...string) *LockBuilder {
	return b.WithPrimaryNodeGroupWithNumWeights(helpers.DefaultMemNodePerSys-1, props...)
}

func (b *LockBuilder) WithPrimaryNodeGroupWithNumWeights(numWeights int, props ...string) *LockBuilder {
	ngName := rlv1.PrimaryNgPrefix
	groupReqBuilder := b.WithGroupRequest(ngName, 1).
		WithNodeRequest(rlv1.WeightRequestName, numWeights, props...).Role("memory").CpuMem(helpers.DefaultMemNodeCpu/2, helpers.DefaultMemNodeMem/2).
		WithNodeRequest(rlv1.WorkerRequestName, helpers.DefaultWrkNodePerSys*2, props...).Role("worker").CpuMem(helpers.DefaultWrkNodeCpu/4, helpers.DefaultWrkNodeMem/4).
		WithNodeRequest(rlv1.CommandRequestName, 1, props...).Role("memory").CpuMem(0, 0)
	if !b.useAxScheduling {
		groupReqBuilder.WithNodeRequest(rlv1.ActivationRequestName, 1, props...).Role("memory").CpuMem(helpers.DefaultMemNodeCpu/4, helpers.DefaultMemNodeMem/4).
			WithNodeRequest(rlv1.ChiefRequestName, 1, props...).Role("memory").CpuMem(helpers.DefaultMemNodeCpu/4, helpers.DefaultMemNodeMem/4)
	}
	for _, prop := range props {
		tokens := strings.Split(prop, ":")
		if tokens[0] == wsapisv1.MemxMemClassProp {
			b.popGroupTypes = wscommon.EnsureMap(b.popGroupTypes)
			b.popGroupTypes[tokens[1]] = true
			break
		}
	}
	return groupReqBuilder.BuildGroupRequest()
}

func (b *LockBuilder) WithNumPopGroupAnnotation(numPopGroup int) *LockBuilder {
	b.annotations[wsapisv1.WsJobPopulatedNodegroupCount] = strconv.Itoa(numPopGroup)
	return b
}

// WithAxOnlyPrimaryNodeGroup adds a node group with a nodegroup for replica types typically added to the primary nodegroup
// in Ax nodes only use case
func (b *LockBuilder) WithAxOnlyPrimaryNodeGroup() *LockBuilder {
	primaryNgIdx := 0
	for _, grp := range b.groups {
		if strings.HasPrefix(grp.Name, rlv1.PrimaryNgPrefix) {
			primaryNgIdx += 1
		}
	}
	ngName := fmt.Sprintf("%s-%d", rlv1.PrimaryNgPrefix, primaryNgIdx)
	groupReqBuilder := b.WithGroupRequest(ngName, 1).
		WithNodeRequest(string(wsapisv1.WSReplicaTypeWorker), helpers.DefaultWrkNodePerSys*2).Role("worker").CpuMem(helpers.DefaultWrkNodeCpu/4, helpers.DefaultWrkNodeMem/4)
	return groupReqBuilder.BuildGroupRequest()
}

// WithSecondaryNodeGroup adds a node group with a nodegroup for replica types typically added to the secondary nodegroup
// where count = number of systems - 1
func (b *LockBuilder) WithSecondaryNodeGroup(count int, props ...string) *LockBuilder {
	groupReqBuilder := b.WithGroupRequest(rlv1.SecondaryNgName, count).
		WithNodeRequest(rlv1.WorkerRequestName, helpers.DefaultWrkNodePerSys*2, props...).Role("worker").CpuMem(helpers.DefaultWrkNodeCpu/4, helpers.DefaultWrkNodeMem/4)
	if !b.useAxScheduling {
		groupReqBuilder.WithNodeRequest(rlv1.ActivationRequestName, 1, props...).Role("memory").CpuMem(helpers.DefaultMemNodeCpu/4, helpers.DefaultMemNodeMem/4).
			WithNodeRequest(rlv1.ChiefRequestName, 1, props...).Role("memory").CpuMem(helpers.DefaultMemNodeCpu/4, helpers.DefaultMemNodeMem/4)
	}
	return groupReqBuilder.BuildGroupRequest()
}

func (b *LockBuilder) WithChfGroupRequest(cpu, mem int) *LockBuilder {
	return b.WithGroupRequest(rlv1.ChiefRequestName, 1).
		WithNodeRequest("Chief", 1).Role("activation").CpuMem(cpu, mem).
		BuildGroupRequest()
}

func (b *LockBuilder) WithActGroupRequest(numActPerSystem, cpu, mem int) *LockBuilder {
	var perCsxDomains [][]int
	for i := 0; i < numActPerSystem; i++ {
		perCsxDomains = append(perCsxDomains, []int{i % 4})
	}

	return b.WithGroupRequest(rlv1.ActivationRequestName, 1).
		WithNodeRequest("Activation", 1).Role("activation").JobSharing().CpuMem(cpu, mem).
		BuildGroupRequest().WithActToCsxDomainsMap(perCsxDomains)
}

func (b *LockBuilder) WithKvssGroupRequest(numKvssPerSystem, numSystems, cpu, mem int) *LockBuilder {
	var perCsxDomains [][]int
	for i := 0; i < numKvssPerSystem; i++ {
		// ex: [0,1], [1,2], [2,3], [3,0], [0,1], [1,2], [2,3]
		perCsxDomains = append(perCsxDomains, []int{i % 4, (i + 1) % 4})
	}

	// Eventually KVSS instances will be put on KVUX nodes, so the role will not always be activation
	return b.WithNumSystems(numSystems).WithGroupRequest(rlv1.KVStorageServerRequestName, 1).
		WithNodeRequest("KVStorageServer", 1).Role("activation").JobSharing().CpuMem(cpu, mem).
		BuildGroupRequest().WithKvssToCsxDomainsMap(perCsxDomains)
}

func (b *LockBuilder) WithSWDriverGroupRequest(cpu, mem, domain, sysIndex int) *LockBuilder {
	var perCsxDomains [][]int

	// SWDriver should only talk to a single domain, hence we pass a single domain
	perCsxDomains = append(perCsxDomains, []int{domain})

	return b.WithGroupRequest(rlv1.SWDriverRequestName, 1).
		WithNodeRequest("SWDriver", 1).Role("inferencedriver").CpuMem(cpu, mem).
		BuildGroupRequest().WithSWDriverToCsxDomainsMap(perCsxDomains).WithSWDriverSysIndex(sysIndex)
}

func (b *LockBuilder) WithWgtCmdGroupRequest(numWgts, wgtCpu, wgtMem, cmdCpu, cmdMem int) *LockBuilder {
	return b.WithGroupRequest(rlv1.WgtCmdGroupPrefix, 1).
		WithNodeRequest("Weight", numWgts).Role("activation").CpuMem(wgtCpu, wgtMem).
		WithNodeRequest("Command", 1).Role("activation").CpuMem(cmdCpu, cmdMem).
		BuildGroupRequest()
}

//===client builder===

var rlscheme = runtime.NewScheme()

type clientBuilder struct {
	objs             []client.Object
	interceptorFuncs *interceptor.Funcs
}

func newClientBuilder(lockLists ...[]*rlv1.ResourceLock) *clientBuilder {
	var o clientBuilder
	for _, ll := range lockLists {
		for _, l := range ll {
			o.objs = append(o.objs, l)
		}
	}
	rand.Shuffle(len(o.objs), func(i, j int) {
		o.objs[i], o.objs[j] = o.objs[j], o.objs[i]
	})
	return &o
}

func (o *clientBuilder) add(obj ...client.Object) *clientBuilder {
	o.objs = append(o.objs, obj...)
	return o
}

func (o *clientBuilder) remove(name string) *clientBuilder {
	var newLocks []client.Object
	for _, obj := range o.objs {
		if obj.GetName() != name {
			newLocks = append(newLocks, obj)
		}
	}
	o.objs = newLocks
	return o
}

func (o *clientBuilder) updateLockPriority(name string, priority int) *clientBuilder {
	for _, obj := range o.objs {
		if obj.GetName() == name {
			l := obj.(*rlv1.ResourceLock)
			l.SetPriority(priority)
			break
		}
	}
	return o
}

func (o *clientBuilder) updateGracePeriod(name string, sec int) *clientBuilder {
	for _, obj := range o.objs {
		if obj.GetName() == name {
			l := obj.(*rlv1.ResourceLock)
			l.Annotations[wsapisv1.PendingLockGracePeriodAnnotKey] = strconv.Itoa(sec)
			break
		}
	}
	return o
}

func (o *clientBuilder) withInterceptorFuncs(interceptorFuncs *interceptor.Funcs) *clientBuilder {
	o.interceptorFuncs = interceptorFuncs
	return o
}

func (o *clientBuilder) build() client.Client {
	b := fake.NewClientBuilder().
		WithScheme(rlscheme).
		WithObjects(o.objs...).
		WithStatusSubresource(o.objs...)
	if o.interceptorFuncs != nil {
		b.WithInterceptorFuncs(*o.interceptorFuncs)
	}
	a := b.Build()

	return a
}

func init() {
	rtutil.Must(clientgoscheme.AddToScheme(rlscheme))
	rtutil.Must(rlv1.AddToScheme(rlscheme))
}

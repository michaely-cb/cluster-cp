package resource

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

const (
	typeProperty  = "type_"
	maxRatioScore = 100_000
)

type Filter struct {
	// resource type
	type_ string

	// Must have at least one of the property values (target[KEY] in VALUES)
	properties map[string][]string

	// Optional request allocate props for affinity/anti-affinity from job
	// when doing affinity/anti-affinity, we check on resource props which can be static from cluster yaml or adhoc from assigned pods
	// e.g. if a node has assigned one pod has "layer":"0" set as allocateProps, it will have this temporary prop
	// later pods within same job can use it for do affinity or anti-affinity
	// Be careful to only add this for job level props and avoid override existing node level props.
	allocateProps map[string]string

	// resources that match antiAffinities will be excluded
	// supports Exists or NotEqual
	// e.g. "role-br":"" => excludes resource has "role-br" key
	// e.g. "layer":"0" => excludes resource has "layer" key && "layer"!="0"
	antiAffinities map[string]string

	// resources with same property affinities will get higher score
	affinities map[string]string

	// Must have at least these subresources
	subresources map[string]int

	// customFilter is used to filter resources according to arbitrary criteria
	// If customFilter is set, it will override the properties filter
	// Prefer to compose the properties above, and restrict its use cases that really require custom criteria.
	customFilter func(Resource) bool
}

func NewFilter(type_ string, properties map[string][]string, subresources map[string]int) Filter {
	f := Filter{
		type_:          type_,
		properties:     CopyMap(EnsureMap(properties)),
		allocateProps:  map[string]string{},
		affinities:     map[string]string{},
		antiAffinities: map[string]string{},
		subresources:   EnsureMap(subresources),
	}
	f.properties[typeProperty] = []string{type_}
	return f
}

func NewSystemFilter(props ...string) Filter {
	return NewFilter(rlv1.SystemResourceType, SplitValues(MustSplitProps(props)), nil)
}

func NewNodeFilter(role string, cpu, mem int, props ...string) Filter {
	p := SplitValues(MustSplitProps(props))
	p["role-"+role] = []string{""}
	return NewFilter(rlv1.NodeResourceType, p, NewCpuMemPodSubresources(cpu, mem))
}

// NewSchedulableFilter returns all schedulable resources
func NewSchedulableFilter() Filter {
	return Filter{
		properties: map[string][]string{HealthProp: {HealthOk, HealthDegraded}},
	}
}

func NewSessionFilter(session string) Filter {
	return Filter{
		customFilter: func(r Resource) bool {
			// BR is global
			return r.IsBrNode() || r.GetNamespace() == session
		},
	}
}

func NewFreeNodesFilter() Filter {
	return Filter{
		customFilter: func(r Resource) bool {
			if r.type_ != NodeType {
				return false
			}
			if r.HasAllocation() {
				return false
			}
			return true
		},
	}
}

// anti-affinity filter across jobs with NOT-EQUAL semantics simulating behavior of NotIn operator
// https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#operators
//
// e.g.
// node-0 has allocated lock-0 with annotation of foo:bar0
// node-1 has allocated lock-1 with annotation of foo:bar1
// if trying to schedule lock-2 by calling
// NewJobAntiAffinityFilter(key=foo, val=bar1)
// then node-1 will be candidate while node-0 will be excluded
func NewJobAntiAffinityFilter(key, val string) Filter {
	return Filter{
		customFilter: func(r Resource) bool {
			if r.type_ != NodeType {
				return false
			}
			if r.NonMatchingPropInAllocatedLocks(key, val) {
				return false
			}
			return true
		},
	}
}

func (f Filter) Copy() Filter {
	return Filter{
		type_:          f.type_,
		properties:     CopyMap(EnsureMap(f.properties)),
		antiAffinities: CopyMap(EnsureMap(f.antiAffinities)),
		affinities:     CopyMap(EnsureMap(f.affinities)),
		subresources:   CopyMap(EnsureMap(f.subresources)),
	}
}

func (f Filter) Type() string {
	return f.type_
}

func (f Filter) Subresources() Subresources {
	return f.subresources
}

func (f Filter) AddSubresource(k string, v int) {
	f.subresources[k] = v
}

func (f Filter) Properties() map[string][]string {
	return f.properties
}

func (f Filter) SetProperty(k, v string) {
	f.properties[k] = []string{v}
}

func (f Filter) SetPropertyList(k string, v []string) {
	f.properties[k] = v
}

// default filter inherited from lock
func (f Filter) WithLockPropFilter(lock *rlv1.ResourceLock) Filter {
	f.WithHealthFilter()
	f.WithSessionFilter(lock)
	if f.type_ == rlv1.NodeResourceType {
		f.WithPlatformVersionFilter(lock)
	}
	if f.type_ == rlv1.SystemResourceType {
		f.WithSystemVersionFilter(lock)
	}
	return f
}

// skip session filter for BR request
func (f Filter) WithLockPropSkipSession(lock *rlv1.ResourceLock) Filter {
	f.WithHealthFilter()
	f.WithPlatformVersionFilter(lock)
	return f
}

func (f Filter) WithLockProps(lock *rlv1.ResourceLock,
	skipHealthFilter, skipSessionFilter, skipPlatformVersionFilter, skipSystemVersionFilter bool) Filter {
	if !skipHealthFilter {
		f.WithHealthFilter()
	}
	if !skipSessionFilter {
		f.WithSessionFilter(lock)
	}
	if !skipPlatformVersionFilter {
		f.WithPlatformVersionFilter(lock)
	}
	if !skipSystemVersionFilter {
		f.WithSystemVersionFilter(lock)
	}
	return f
}

func (f Filter) WithHealthFilter() Filter {
	f.properties[HealthProp] = []string{HealthOk, HealthDegraded}
	return f
}

func (f Filter) WithSessionFilter(lock *rlv1.ResourceLock) Filter {
	if ns := lock.GetSessionFilter(); ns != "" {
		f.properties[NamespacePropertyKey] = []string{ns}
	}
	if shares := lock.GetAssignedRedundancy(); len(shares) > 0 {
		f.properties[NamespacePropertyKey] = append(f.properties[NamespacePropertyKey], shares...)
	}
	return f
}

func (f Filter) WithPlatformVersionFilter(lock *rlv1.ResourceLock) Filter {
	if pv := lock.GetPlatformVersionFilter(); pv != "" {
		f.properties[PlatformVersionResKey(pv)] = []string{""}
	}
	return f
}

func (f Filter) WithSystemVersionFilter(lock *rlv1.ResourceLock) Filter {
	if v := lock.GetSystemVersionFilter(); v != "" {
		f.properties[SystemVersionPropertyKey] = []string{v}
	}
	return f
}

// isJobAllocateProps indicates it's a job prop needs to be allocated to node for evaluation later
func (f Filter) AddAntiAffinity(k, v string, isJobAllocateProps bool) {
	f.antiAffinities[k] = v
	if isJobAllocateProps {
		f.allocateProps[k] = v
	}
}

func (f Filter) AntiAffinities() map[string]string {
	return f.antiAffinities
}

func (f Filter) AddAffinity(k, v string, isJobAllocateProps bool) {
	f.affinities = EnsureMap(f.affinities)
	f.affinities[k] = v
	if isJobAllocateProps {
		f.allocateProps[k] = v
	}
}

func (f Filter) GetAffinities() map[string]string {
	return f.affinities
}

// PrettyProps returns a string for end-user error messages: filter _underscore props and make 'role-memory: ""' into 'role:memory'
func PrettyProps(p map[string][]string) string {
	pp := strings.Builder{}
	pp.WriteString("[")
	keys := Keys(p)
	sort.Strings(keys)
	for _, k := range keys {
		if k == HealthProp {
			continue
		}
		v := p[k]
		if strings.HasPrefix(k, "role-") {
			if pp.Len() != 1 {
				pp.WriteString(", ")
			}
			pp.WriteString("role:")
			pp.WriteString(k[len("role-"):])
		} else if !strings.HasSuffix(k, "_") {
			if pp.Len() != 1 {
				pp.WriteString(", ")
			}
			pp.WriteString(k)
			pp.WriteString(":")
			pp.WriteString(strings.Join(v, ","))
		}
	}
	if pp.Len() == 1 {
		return ""
	}
	pp.WriteString("]")
	return pp.String()
}

type Request struct {
	// Id is the unique id of this matcher.
	Id string

	// Annotations is a map that contains metadata information about the request.
	// For example, readable Id, targeted system and cs port.
	// Information from the annotations can be plumbed through all the way to pod spec in the end.
	Annotations map[string]string

	// Count is the number of resources being requested by this matcher
	Count int

	Filter
}

func (r Request) GetSession() string {
	return r.Annotations[NamespacePropertyKey]
}

func (r Request) Copy() Request {
	newReq := Request{
		Id:    r.Id,
		Count: r.Count,
	}

	newReq.type_ = r.type_
	newReq.properties = map[string][]string{}
	maps.Copy(newReq.properties, r.properties)

	newReq.antiAffinities = map[string]string{}
	maps.Copy(newReq.antiAffinities, r.antiAffinities)

	newReq.affinities = map[string]string{}
	maps.Copy(newReq.affinities, r.affinities)

	newReq.subresources = map[string]int{}
	maps.Copy(newReq.subresources, r.subresources)

	newReq.Annotations = map[string]string{}
	maps.Copy(newReq.Annotations, r.Annotations)

	return newReq
}

func (r Request) String() string {
	return fmt.Sprintf("id=%s, count=%d, %v", r.Id, r.Count, r.Filter)
}

func (r Request) IsInferenceJobSharingEnabled() bool {
	return r.Annotations[wsapisv1.InferenceJobSharingAnnotKey] == strconv.FormatBool(true)
}

// scoreMatchedResource returns a score that is meaningful only when there is a valid match. The score gives a soft
// metric for how well the candidate fits the Request where
// < 0 is a fit but resource does not have enough available subresources to match the request
// Higher score indicates a better fit. The score tries to prefer packing
// Additionally, we expect memory to be the bottleneck resource, so we give it a higher weight (mem=MiB, cpu=millicore)
func (r Request) scoreMatchedResource(groupReqId string, resource Resource) resourceScore {
	remainRatioScore := r.scoreByRemainRatio(resource)
	if remainRatioScore < 0 {
		return newResourceScore(resource, nil, remainRatioScore)
	}
	affinityScore := r.scoreByAffinity(resource)
	// for ungrouped request, score for most remaining+affinity
	if _, ok := resource.properties[RoleCrdPropKey]; ok || resource.Type() == rlv1.SystemResourceType {
		return newResourceScore(resource, nil, remainRatioScore+affinityScore)
	}
	// for grouped request
	isBrRequest := strings.HasPrefix(groupReqId, rlv1.BrRequestName)
	isActKvssRequest := strings.HasPrefix(groupReqId, rlv1.ActivationRequestName) || strings.HasPrefix(groupReqId, rlv1.KVStorageServerRequestName)
	indexedResScore, indexedResOverride := resource.ScoreIndexedReq(groupReqId, r)

	// for BR, score by utilization+affinity
	if isBrRequest {
		return newResourceScore(resource, indexedResOverride, r.scoreForBR(remainRatioScore, affinityScore, resource))
	}
	// for activation/off prompt caching in AX scheduling, where the quality of a placement is solely determined by scoring
	// the preferred nic on a node
	// note that chief request won't specify affinities
	if isActKvssRequest {
		if r.IsInferenceJobSharingEnabled() {
			// Sort the list of resource scoring structs (rss) based on priority criteria when job sharing is enabled.
			// Priority order:
			//   1. affinityScore — prefer resources with higher affinity to the domain requested
			//   2. indexedResScore — when same affinity score, prefer resources with less usage of indexed NICs
			//   3. remainRatioScore — when same NIC usage score, prefer resources with more remaining resource available
			return newResourceScore(resource, indexedResOverride, affinityScore, indexedResScore, remainRatioScore)
		}
		return newResourceScore(resource, indexedResOverride, indexedResScore)
	}
	// for WGT/WRK/ACT/CHF/CMD, score by packing preference
	// Note that ACT scheduling in v2 network will be performed earlier
	return newResourceScore(resource, indexedResOverride, r.defaultPackingScore(remainRatioScore, resource))
}

// with the following priorities, highest to lowest:
// 1. Packing an unallocated resource with minimal left-over subresources. (prefer not sharing with efficient packing)
// 2. Packing an allocated resource with maximal left-over subresources. (prefer minimal packing in shared case)
func (r Request) defaultPackingScore(remainingRatio int, resource Resource) int {
	if !resource.HasAllocation() {
		return maxRatioScore + (maxRatioScore - remainingRatio)
	}
	return remainingRatio
}

// score branch for BR based on utilization+affinity+healthiness
// utilization: prefer nodes with higher remainScore
// except for the case that's a perfect fit
// e.g. 6-nic br has 3 nic free for 1:2 mode OR 12-nic br has 4 nic free for 1:3 mode
//
// affinity: prefer nodes with most affinity match
// v2 has enough redundant nodes and spine traffic cost is higher
// larger weight than utilization (only v2 has affinity)
//
// healthiness: prefer healthy nodes
// adds penalty to nodes if degraded
// larger weight than utilization+affinity
func (r Request) scoreForBR(remainRatioScore, affinityScore int, resource Resource) int {
	utilizationWeight := 1
	affinityWeight := 10
	penalty := 1

	// todo: add env to allow prefer packing
	utilizationScore := remainRatioScore
	if resource.properties[HealthProp] == HealthDegraded {
		// add penalty if it's not fully healthy
		penalty = 100
	} else if remainRatioScore == 0 {
		if wsapisv1.IsV2Network && r.Annotations[wsapisv1.BrPreferIdleSx] != "" {
			// for session pickup job job to prefer idle nodes in v2
			utilizationScore = 0
		} else {
			// perfect fit to reduce fragmentation by default
			utilizationScore = maxRatioScore
		}
	}
	return (utilizationScore*utilizationWeight + affinityScore*affinityWeight) / penalty
}

// scoreByRemainRatio calculates the ratio of remaining unallocated subresources to total available
// subresources after this request is allocated to the given resource. Returns -1 if there is not enough available.
func (r Request) scoreByRemainRatio(resource Resource) int {
	totalAvailable, remainingUnallocated := 0, 0
	for k, rqVal := range r.subresources {
		ava := resource.GetSubresourceCountAvailable(k)
		if ava < rqVal {
			return -1 // not enough free subresources for a fit
		}
		weight := 1
		if k == MemSubresource {
			weight = MemSubresourceScoringWeight
		} else if (k == PodSubresource || k == PodInstanceIdSubresource) && len(r.subresources) > 1 {
			// skip pod virtual resource for scoring unless it's the only request in case of unlimited pod
			continue
		}
		totalAvailable += ava * weight
		remainingUnallocated += (ava - rqVal) * weight
	}
	// use percent of targeted resources * 100_000 for integer score with 3
	// decimal points of precision
	return int(float64(remainingUnallocated) * maxRatioScore / float64(totalAvailable))
}

// scoreByAffinity calculates the affinities met by resource
func (r Request) scoreByAffinity(resource Resource) int {
	var affinityScore int
	if len(r.Filter.affinities) > 0 {
		// prioritize affinity than utilization for v2
		affinityMetCount := 0
		for k, v := range r.Filter.affinities {
			if val, ok := resource.GetProp(k); ok && val == v {
				affinityMetCount++
			}
		}
		affinityScore = maxRatioScore * affinityMetCount / len(r.Filter.affinities)
	}
	return affinityScore
}

// get total requesst count which contains key
func (r Request) getRequestedCount(key string) int {
	count := 0
	for k, rqVal := range r.subresources {
		if strings.HasPrefix(k, key) {
			count += rqVal
		}
	}
	return count
}

func (r Request) WithLockProps(lock *rlv1.ResourceLock) Request {
	r.Filter.WithLockProps(lock,
		// skip health filter if allow unhealthy system
		r.Id == rlv1.SystemsRequestName && lock.AllowUnhealthySystems(),
		// skip session filter for br which is global
		len(r.properties[RoleBRPropKey]) > 0,
		// skip platform version filter for systems
		r.Id == rlv1.SystemsRequestName,
		// skip system version filter for non-systems
		r.Id != rlv1.SystemsRequestName)
	// plumb session to request for affinity check later
	if ns := lock.GetSessionFilter(); ns != "" {
		r.Annotations = EnsureMap(r.Annotations)
		r.Annotations[NamespacePropertyKey] = ns
	}
	return r
}

type GroupRequest struct {
	// Id is the unique id of this group request.
	Id string

	// The number of groups matching this GroupRequest to select.
	Count int

	// RequestId -> Request
	Requests map[string]Request

	// key sort function. Determines order requests are processed when determining if requests fit a candidate group.
	KeySort func(map[string]Request) []string
}

func (g GroupRequest) getSortedRequestIds() []string {
	return g.KeySort(g.Requests)
}

func (g GroupRequest) Copy() GroupRequest {
	res := GroupRequest{
		Id:       g.Id,
		Count:    g.Count,
		Requests: make(map[string]Request),
		KeySort:  g.KeySort,
	}
	for k, v := range g.Requests {
		res.Requests[k] = v.Copy()
	}
	return res
}

func (g GroupRequest) AggregatedRequestedRes(subResName string) int {
	aggrRequestedRes := 0
	for _, subReq := range g.Requests {
		aggrRequestedRes += subReq.Filter.Subresources()[subResName] * subReq.Count
	}
	return aggrRequestedRes
}

func (g GroupRequest) IsGroupScheduledForAx() bool {
	for _, subReq := range g.Requests {
		_, roleAxKeyExists := subReq.Filter.Properties()[RoleAxPropKey]
		if !roleAxKeyExists {
			return false
		}
	}
	return true
}

type Grant struct {
	Request Request

	// List of matched resource ids
	ResourceIds []string

	// Subresources allocated to this request. May be more than requested in the case of SubresourceUpperBounds.
	// This is uniformed across the request ids
	Subresources Subresources

	// SubresourceOverride resource id => sub-resources
	// for heterogeneous assign override or named sub-resources that are different with the general request
	// e.g. "node0" => {"nic0": 100, "nic1": 100, "mem": 100}
	// note: this will only contain override resources only
	// i.e. if "cpu" is not overridden, it will still be in the uniformed Subresources field above.
	// so eventual grant should be a merged result of Subresources + SubresourceOverride
	SubresourceOverride map[string]Subresources

	// SubresourceRanges allocated to this request.
	// As of rel-2.4, this is used only in optional NVMe disk address range granting for coordinators.
	SubresourceRanges SubresourceRanges

	// Annotations with get applied on the grant status
	// One example usage is to leverage the annotations to tell if a coordinator had been configured as a NVMe-oF target.
	Annotations map[string]string
}

func (g Grant) SystemCount() int { return len(g.ResourceIds) }
func (g Grant) Size() int        { return len(g.ResourceIds) }

type GroupGrant struct {
	// Corresponding request
	Request GroupRequest

	// GroupVal stores the grouping bucket value so that we can re-derive the old-school
	// nodegroup each entry in groupVals corresponds to the Grants[] entry. An example of this
	// value is "nodegroup0"
	GroupVal string

	// Each value is a Grant where the key is the requestId and the value is the Grant.
	Grants map[string]Grant

	// Annotations will get applied on the group grant status.
	// One example usage is to leverage the annotations to tell if a certain group grant acquired a populated nodegroup.
	Annotations map[string]string

	// Cost score of the current group grant for searcher to evaluate across candidates
	// Idea is to pick the group with minimal resources left + prefer local session's resources
	cost int
}

// Id + Cost implements search.go::searchable
func (g GroupGrant) Id() string { return g.GroupVal }
func (g GroupGrant) Cost() int  { return g.cost }

func (g GroupGrant) GrantedCount() int {
	count := 0
	for _, g := range g.Grants {
		count += len(g.ResourceIds)
	}
	return count
}

func (g GroupGrant) MatchGroupGrantType(ggType string) bool {
	return strings.HasPrefix(g.Request.Id, ggType) ||
		(strings.HasPrefix(g.Request.Id, rlv1.WgtCmdGroupPrefix) && ggType == rlv1.CommandRequestName)
}

func (g GroupGrant) IsActKvssGrantType() bool {
	return strings.HasPrefix(g.Request.Id, rlv1.ActivationRequestName) || strings.HasPrefix(g.Request.Id, rlv1.KVStorageServerRequestName)
}

// BrRequestKeySort is to sort requests by CS port count
// i.e. 1:2 mode will be scheduled prior to 1:4 mode
// this is to ensure 1:2 mode will be assigned to healthy nodes for sharing purpose
// degraded node can't be used for 2 * 1:2 modes
// current group scheduling depends on candidates order
func BrRequestKeySort(m map[string]Request) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		// for v1, schedule 1:2 mode first for higher utilization
		if !wsapisv1.IsV2Network {
			count := func(i int) int {
				for k, v := range m[keys[i]].Filter.Subresources() {
					if strings.HasPrefix(k, NodeNicSubresource) {
						return v
					}
				}
				return 0
			}
			countI := count(i)
			countJ := count(j)
			if countI != countJ {
				return countI < countJ
			}
		}

		// use id to ensure one BR set on same layer to be scheduled together to help anti-affinity schedule
		id := func(i int) int {
			ret, _ := strconv.Atoi(m[keys[i]].Id)
			return ret
		}
		return id(i) < id(j)
	})
	return keys
}

// ActWgtKeySort sorts requests for act/wgt server groups first by act, wgt replica type, and then alphabetical. This
// ensures an even spread of act/wgt servers in the presence of chf, cmd servers.
func ActWgtKeySort(m map[string]Request) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i] == "Activation" || keys[j] == "Activation" {
			return keys[i] == "Activation"
		} else if keys[i] == "Weight" || keys[j] == "Weight" {
			return keys[i] == "Weight"
		}
		return strings.Compare(keys[i], keys[j]) < 0
	})
	return keys
}

// For test only
func V2ActKeySortForGrant(m map[string]rlv1.ResourceGrant) []string {
	keys := Keys(m)
	sort.Slice(keys, func(i, j int) bool {
		// For example, 4th pod on 2nd system goes before 5th pod on 2nd system, which goes before 5th pod on the 3rd system.
		podI, _ := strconv.Atoi(m[keys[i]].Annotations[rlv1.PodIdPerCsxAnnotKey])
		sysI, _ := strconv.Atoi(m[keys[i]].Annotations[rlv1.SystemIdAnnotKey])
		podJ, _ := strconv.Atoi(m[keys[j]].Annotations[rlv1.PodIdPerCsxAnnotKey])
		sysJ, _ := strconv.Atoi(m[keys[j]].Annotations[rlv1.SystemIdAnnotKey])
		if podI != podJ {
			return podI < podJ
		} else {
			return sysI < sysJ
		}
	})
	return keys
}

func V2ActKvssKeySort(m map[string]Request) []string {
	keys := Keys(m)
	sort.Slice(keys, func(i, j int) bool {
		// For example, 4th pod on 2nd system goes before 5th pod on 2nd system, which goes before 5th pod on the 3rd system.
		podI, _ := strconv.Atoi(m[keys[i]].Annotations[rlv1.PodIdPerCsxAnnotKey])
		sysI, _ := strconv.Atoi(m[keys[i]].Annotations[rlv1.SystemIdAnnotKey])
		podJ, _ := strconv.Atoi(m[keys[j]].Annotations[rlv1.PodIdPerCsxAnnotKey])
		sysJ, _ := strconv.Atoi(m[keys[j]].Annotations[rlv1.SystemIdAnnotKey])
		if podI != podJ {
			return podI < podJ
		} else {
			return sysI < sysJ
		}
	})
	return keys
}

func V2GenericAscKeySort(m map[string]Request) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// V2WgtCmdKeySort sorts requests for wgt/cmd server groups first by wgt, cmd replica type, and then alphabetical. This
// ensures an even spread of wgt servers in the presence of cmd server.
func V2WgtCmdKeySort(m map[string]Request) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.Compare(keys[i], keys[j]) > 0
	})
	return keys
}

// GatherAllocations aggregates granted resources' subresources and returns a map of resourceId -> alloc.
func GatherAllocations(
	grants map[string]Grant,
	groupGrants map[string][]GroupGrant,
) map[string]SubresourceAllocation {
	allocation := make(map[string]SubresourceAllocation)

	allocResources := func(g Grant) {
		for _, rid := range g.ResourceIds {
			subRes := g.Subresources.Copy()
			if len(g.SubresourceOverride[rid]) > 0 {
				maps.Copy(subRes, g.SubresourceOverride[rid])
			}

			if _, ok := allocation[rid]; !ok {
				allocation[rid] = SubresourceAllocation{
					Subresources:      Subresources{},
					SubresourceRanges: g.SubresourceRanges.Copy(),
				}
			}
			allocation[rid].Subresources.Add(subRes)
		}
	}

	for _, resourceGrant := range grants {
		allocResources(resourceGrant)
	}
	for _, ggList := range groupGrants {
		for _, gg := range ggList {
			for _, g := range gg.Grants {
				allocResources(g)
			}
		}
	}

	return allocation
}

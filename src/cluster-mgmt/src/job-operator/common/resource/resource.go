package resource

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

const (
	NodeType      = "node"
	NodegroupType = "nodegroup"
	SystemType    = "system"

	CPUSubresource    = "cpu"
	MemSubresource    = "memory"
	PodSubresource    = "pod"
	SystemSubresource = "system"
	// calling as cs-port for legacy reasons that br has this cs-port-0/1/... resource from existing v1
	// todo: rename to "node-nic" + "system-port" and ensure backwards compatible in case of upgrade when old jobs running
	NodeNicSubresource    = "cs-port"
	SystemPortSubresource = "port"
	// virtual resource indicating the order of pod brought up on a particular node (only for br now)
	// e.g. if node-0 got a few brs running, first br will get ins-id: 0, next will be id:1, next will be 2, etc.
	// if first br terminated while others still running then next br assigned to node-0, it will get the released id: 0
	// it's a per node res, so if each node has only one pod, then each pod will always get id: 0
	PodInstanceIdSubresource = "instance-id"
	NvmfDiskSubresource      = "nvmf-disk"
	AuxiliaryLMRSubresource  = AuxiliarySubresPrefix + "lmr-x"

	// Job-specific auxiliary limits
	AuxiliarySubresPrefix  = "auxiliary-"
	AuxiliaryPodUsageLimit = AuxiliarySubresPrefix + "pod-usage-limit"
	AuxiliaryNicUsageLimit = AuxiliarySubresPrefix + "nic-usage-limit"

	// default data NIC BW val(100Gb/s)
	DefaultNicBW   = 100
	DefaultAxNicBW = 400
	DefaultIxNicBW = 400

	// default max br pods count per node, for CS4 this is now 24 (was previously 12);
	DefaultMaxBrPodsPerNode = 24

	// max limit specified for upper bound
	PodUpperBoundMaxLimitPostFix = "-limit"

	// MemSubresourceScoringWeight is the weight of a subresource when considered for scoring during the node matching process.
	// This tweaks the scoring to punish pod -> node matches which leave more memory resources available on the node.
	MemSubresourceScoringWeight = 10

	// HealthProp health=ok|degraded|err
	HealthProp     = "health"
	HealthOk       = "ok"
	HealthDegraded = "degraded"
	HealthErr      = "err"

	// BrLayerProp indicates which layer this pod/request resides in the tree
	BrLayerProp = "layer"

	NvmfTargetIdPropKey       = "nvme-of-target-id"
	NvmfInitiatorPropKey      = "nvme-of-initiator"
	NvmfDiskCountPerCoordNode = 3
	NvmfDevicePathPrefix      = "/dev/datanvme"

	// Subresource limit hack for SwDriver mutual exclusion
	// TODO: remove once SwDriver is scheduled on IX/SX
	Act0SubResource = "act0"
)

var DefaultSystemPortRequest = 12

var IndexedSubresourceNamePrefixList = map[string]bool{
	NodeNicSubresource:       true,
	PodInstanceIdSubresource: true,
}

// Subresources represents quantizable component resources of a resource
// such as CPU/mem of a node resource.
//
// "memory" has unit Mibibytes (2^20 bytes)
// "cpu" has unit MilliCPUs (1/1000 of a CPU)
//
// all jobs are specifying coordinator mem/cpu limits.
//
// "pod" has unit pod and represents the placement of a pod on a node. Useful for tracking allocations where no cpu/mem was given
// "system" has unit 1 and represents an exclusive lock on the entire system
type Subresources map[string]int

func NodeNicResName(nic string) string {
	return fmt.Sprintf("%s/%s", NodeNicSubresource, nic)
}

func InstIdResName(id int) string {
	return fmt.Sprintf("%s/%d", PodInstanceIdSubresource, id)
}

func NvmfDiskResName(id int) string {
	return fmt.Sprintf("%s/%d", NvmfDiskSubresource, id)
}

func NewSystemResourceRequest(portCount int) Subresources {
	m := map[string]int{SystemSubresource: 1}
	if portCount > 0 {
		m[SystemPortSubresource] = portCount
	}
	return m
}

func NewCpuMemPodSubresources(cpu int, mem int) Subresources {
	return map[string]int{
		CPUSubresource: cpu,
		MemSubresource: mem,
		PodSubresource: 1,
	}
}

func (s Subresources) Cpu() int {
	return s[CPUSubresource]
}

func (s Subresources) Mem() int {
	return s[MemSubresource]
}

func (s Subresources) String() string {
	var rv []string
	ks := Keys(s)
	sort.Strings(ks)
	for _, k := range ks {
		rv = append(rv, k+":"+strconv.Itoa(s[k]))
	}
	return "{" + strings.Join(rv, ",") + "}"
}

// SubresourceFromContainers creates a subresource from a container resource limits for cpu, mem. Always request 1 Pod
// subresource for the purpose of denoting the count of pods occupying the resource.
func SubresourceFromContainers(containers []*v1.Container) Subresources {
	sr := Subresources{PodSubresource: 1}
	for _, c := range containers {
		memBytes := c.Resources.Limits.Memory().Value()
		if memBytes == 0 {
			memBytes = c.Resources.Requests.Memory().Value()
		}
		memMB := int(memBytes >> 20)
		if memMB > 0 {
			sr[MemSubresource] += memMB // Mebibytes
		}

		cpu := c.Resources.Limits.Cpu().MilliValue()
		if cpu == 0 {
			cpu = c.Resources.Requests.Cpu().MilliValue()
		}
		if cpu > 0 {
			sr[CPUSubresource] += int(cpu)
		}
	}

	return sr
}

func (s Subresources) ToQuantities() v1.ResourceList {
	rv := v1.ResourceList{}
	for k, v := range s {
		switch k {
		case CPUSubresource:
			rv[v1.ResourceCPU] = *resource.NewMilliQuantity(int64(v), resource.DecimalSI)
		case MemSubresource:
			rv[v1.ResourceMemory] = *resource.NewQuantity(int64(v)<<20, resource.BinarySI)
		}
	}
	return rv
}

func (s Subresources) Copy() Subresources {
	return CopyMap(EnsureMap(s))
}

// Add returns a new Subresources object that is the sum of the two. This mutates the receiver and returns the
// receiver for chaining.
func (s Subresources) Add(sr Subresources) Subresources {
	for k, v := range sr {
		s[k] += v
	}
	return s
}

// Pretty is intended for end-user error messages: show cpu, memory, cs-port since that's only likely to be limiting for end user
func (s Subresources) Pretty() string {
	pp := strings.Builder{}
	pp.WriteString("{")
	keys := Keys(s)
	sort.Strings(keys)
	for _, k := range keys {
		v := s[k]
		if k == MemSubresource {
			if pp.Len() != 1 {
				pp.WriteString(", ")
			}
			if v > (1 << 10) {
				pp.WriteString(fmt.Sprintf("mem:%dGi", v>>10))
			} else {
				pp.WriteString(fmt.Sprintf("mem:%dMi", v))
			}
		} else if k == CPUSubresource {
			if pp.Len() != 1 {
				pp.WriteString(", ")
			}
			if v%1000 == 0 {
				pp.WriteString(fmt.Sprintf("cpu:%d", int(float64(v)/1000)))
			} else {
				pp.WriteString(fmt.Sprintf("cpu:%dm", v))
			}
		} else if strings.HasPrefix(k, NodeNicSubresource) || k == "count" {
			if pp.Len() != 1 {
				pp.WriteString(", ")
			}
			pp.WriteString(fmt.Sprintf("%s:%d", k, v))
		}
	}
	if pp.Len() == 1 {
		return ""
	}
	pp.WriteString("}")
	return pp.String()
}

// Range represents a contiguous range from Start (inclusive) to End (exclusive).
// For example, we can use it to represent disk space from the k-th byte to the m-th byte.
type Range struct {
	Start int64
	End   int64
}

func (r Range) Size() int64 {
	return r.End - r.Start
}

func (r Range) Equals(other Range) bool {
	return r.Start == other.Start && r.End == other.End
}

func (r Range) Copy() Range {
	return Range{r.Start, r.End}
}

func (r Range) IsValid() bool {
	return r.Start < r.End
}

func (r Range) IsMergeableWith(other Range) bool {
	// invalid range - should never happen
	if !r.IsValid() || !other.IsValid() {
		return false
	}
	if r.End == other.Start || r.Start == other.End {
		return true
	}
	return r.End > other.Start && r.Start < other.End
}

// SubresourceRanges represents contiguous blocks of resources
// such as a range of disk space on a node resource.
// Ranges should always be non-contiguous after Canonicalize():
// - SubresourceRanges[k][m].End < SubresourceRanges[k][m+1].Start
//
// "nvmf-disk" has unit bytes
type SubresourceRanges map[string][]Range

func (sr SubresourceRanges) Copy() SubresourceRanges {
	return CopyMap(EnsureMap(sr))
}

func (sr SubresourceRanges) Equals(other SubresourceRanges) bool {
	if len(sr) != len(other) {
		return false
	}
	for k := range sr {
		if len(sr[k]) != len(other[k]) {
			return false
		}
		for i := range sr[k] {
			if !sr[k][i].Equals(other[k][i]) {
				return false
			}
		}
	}
	return true
}

func (sr SubresourceRanges) Size(k string) int64 {
	var result int64
	for _, v := range sr[k] {
		result += v.Size()
	}
	return result
}

// Canonicalize sorts and merges ranges in the SubresourceRanges instance.
func (sr SubresourceRanges) Canonicalize() SubresourceRanges {
	for k := range sr {
		ranges := sr[k]
		slices.SortFunc(ranges, func(a, b Range) bool {
			return a.Start < b.Start
		})

		// Merge contiguous ranges
		var mergedRanges []Range
		for _, r := range ranges {
			if !r.IsValid() {
				continue
			} else if len(mergedRanges) == 0 {
				mergedRanges = append(mergedRanges, r)
				continue
			}

			lastRange := &mergedRanges[len(mergedRanges)-1]
			if lastRange.IsMergeableWith(r) {
				// Merge the current range with the last range in mergedRanges
				lastRange.End = Max(lastRange.End, r.End)
			} else {
				// Not contiguous, so add the current range as a new entry
				mergedRanges = append(mergedRanges, r)
			}
		}
		sr[k] = mergedRanges
	}
	return sr
}

// Return new subresource ranges after subtraction if one is a superset of the other.
func (sr SubresourceRanges) Subtract(other SubresourceRanges) (SubresourceRanges, bool) {
	sr.Canonicalize()
	other.Canonicalize()

	result := sr.Copy()
	// Example "other":
	// map["nvmf-disk/0"] = []Range{{Start: 0, End: 9}, {Start:20, End: 24}}
	for key, otherRanges := range other {
		ranges, ok := result[key]
		// return false if "other" has a key which "result" does not
		if !ok {
			return SubresourceRanges{}, false
		}

		var newRanges []Range
		var rangesCopy []Range
		for _, r := range ranges {
			rangesCopy = append(rangesCopy, r.Copy())
		}

		i := 0 // index for "otherRanges"
		j := 0 // index for "rangesCopy"
		for j < len(rangesCopy) {
			// every "otherRanges[i]" needs to be a subset of one "rangesCopy[j]"
			// find that "r" before proceeding
			if i >= len(otherRanges) || otherRanges[i].Start >= rangesCopy[j].End {
				newRanges = append(newRanges, rangesCopy[j])
				j += 1
				continue
			}

			// if the start of range in "other" is less than the start of range in "sr", then "other" is not subtractable
			// if there are left-overs in the front, add the non-overlapping range
			if otherRanges[i].Start < rangesCopy[j].Start {
				return SubresourceRanges{}, false
			} else if otherRanges[i].Start > rangesCopy[j].Start {
				newRanges = append(newRanges, Range{Start: rangesCopy[j].Start, End: otherRanges[i].Start})
			}

			// if the end of range in "other" is greater than the end of range in "sr", then "other" is not subtractable
			// if there are left-overs in the end, shrink the current range and re-evaluate
			// otherwise, skip this range as it's fully subtracted
			if otherRanges[i].End > rangesCopy[j].End {
				return SubresourceRanges{}, false
			} else if otherRanges[i].End < rangesCopy[j].End {
				rangesCopy[j].Start = otherRanges[i].End
			} else {
				j += 1
			}
			i += 1
		}

		// there are left-over ranges in "otherRanges" that are not subset of "rangesCopy"
		if i < len(otherRanges) {
			return SubresourceRanges{}, false
		} else if len(newRanges) == 0 {
			delete(result, key)
		} else {
			result[key] = newRanges
		}
	}

	return result, true
}

// Return new subresource ranges after addition.
func (sr SubresourceRanges) Add(other SubresourceRanges) SubresourceRanges {
	result := sr.Copy()
	for key, otherRanges := range other {
		ranges := result[key]
		ranges = append(ranges, otherRanges...)
		result[key] = ranges
	}
	return result.Canonicalize()
}

type SubresourceAllocation struct {
	Subresources      Subresources
	SubresourceRanges SubresourceRanges
}

// Subresources represents quantizable component resources of a resource
// such as CPU/mem of a node resource.
//
// "memory" has unit Mibibytes (2^20 bytes)
// "cpu" has unit MilliCPUs (1/1000 of a CPU)
//
// all jobs are specifying coordinator mem/cpu limits.
//
// "pod" has unit pod and represents the placement of a pod on a node. Useful for tracking allocations where no cpu/mem was given
// "system" has unit 1 and represents an exclusive lock on the entire system

type SubresourceIndex struct {
	// An inverted index of sub-resource type -> sub-resource names.
	// This is used for sub-resources of a specific type but with individual names.
	// E.g. BR/ACT NIC resource, index["cs-port"] => cs-port/nic0, cs-port/nic1...
	// subresources["nic0"]=100 (Gb/s), subresources["nic1"]=100 (Gb/s)
	Subresources map[string]map[string]bool

	// An inverted index of sub-resource type -> sub-resource range names.
	// This is used for sub-resource ranges of a specific type but with individual names.
	// E.g. Coordinator nvmf disk resource, index["nvmf-disk"] => nvmf-disk/0, nvmf-disk/1...
	// subresourceRanges["nvmf-disk/0"]=[[0,100]], subresourceRanges["nvmf-disk/1"]=[[50,150]]
	SubresourceRanges map[string]map[string]bool
}

func (si SubresourceIndex) Copy() SubresourceIndex {
	index := SubresourceIndex{
		Subresources:      CopyMap(EnsureMap(si.Subresources)),
		SubresourceRanges: CopyMap(EnsureMap(si.SubresourceRanges)),
	}
	return index
}

type Resource struct {
	type_      string
	name       string
	id         string // usually "type/name", cached since commonly used. type_ is included to avoid collisions between names
	properties map[string]string
	group      Group // possibly empty, will be the same as properties.group if set

	subresources   Subresources
	allocated      Subresources
	allocatedProps map[string]string
	allocatedLocks map[string]*rlv1.ResourceLock

	subresourceRanges SubresourceRanges
	allocatedRanges   SubresourceRanges

	index SubresourceIndex
}

func NewResource(
	type_, name string,
	properties map[string]string,
	subresources Subresources,
	subresourceRanges SubresourceRanges) Resource {
	res := Resource{
		type_:             type_,
		name:              name,
		id:                type_ + "/" + name,
		properties:        properties,
		subresources:      subresources.Copy(),
		subresourceRanges: subresourceRanges.Copy(),
		allocated:         map[string]int{},
		allocatedRanges:   SubresourceRanges{},
		allocatedProps:    map[string]string{},
		allocatedLocks:    map[string]*rlv1.ResourceLock{},
		index: SubresourceIndex{
			Subresources:      map[string]map[string]bool{},
			SubresourceRanges: map[string]map[string]bool{}},
	}
	// test only
	// todo: move to test code side, not doing it for now due to v1 legacy test code
	return res.BuildTestIndex(subresources)
}

func (r Resource) BuildTestIndex(subresources Subresources) Resource {
	// check if it's test setup
	if _, ok := subresources[PodInstanceIdSubresource]; !ok {
		return r
	}

	subresourceIndex := map[string]map[string]bool{}
	subresourceNames := Keys(subresources)
	sort.Strings(subresourceNames)
	nicId := 0
	for _, subresourceName := range subresourceNames {
		v := subresources[subresourceName]
		if strings.Contains(subresourceName, "/") {
			// skip indexed subresource
			continue
		}
		// for test purpose only generating mock nics/instance-ids only
		// these res will only be in index for real case by call WithIndex()
		if strings.HasPrefix(subresourceName, NodeNicSubresource) {
			subresourceIndex[subresourceName] = EnsureMap(subresourceIndex[subresourceName])
			if v >= DefaultNicBW {
				v = v / DefaultNicBW
			}
			for i := 0; i < v; i++ {
				nic := fmt.Sprintf("%s/nic-%d", NodeNicSubresource, nicId)
				subresources[nic] = DefaultNicBW
				subresourceIndex[subresourceName][nic] = true
				// use separate var to ensure id increment in case of different cs ports
				nicId++
			}
			delete(subresources, subresourceName)
		} else if subresourceName == PodInstanceIdSubresource {
			subresourceIndex[subresourceName] = EnsureMap(subresourceIndex[subresourceName])
			for i := 0; i < v; i++ {
				insId := fmt.Sprintf("%s/%d", PodInstanceIdSubresource, i)
				subresources[insId] = 1
				subresourceIndex[subresourceName][insId] = true
			}
			delete(subresources, subresourceName)
		}
	}
	r.subresources = subresources.Copy()
	r.index.Subresources = subresourceIndex
	return r
}

func (r Resource) WithIndex(index SubresourceIndex) Resource {
	r.index.Subresources = EnsureMap(r.index.Subresources)
	r.index.SubresourceRanges = EnsureMap(r.index.SubresourceRanges)
	maps.Copy(r.index.Subresources, index.Subresources)
	maps.Copy(r.index.SubresourceRanges, index.SubresourceRanges)
	return r
}

func (r Resource) Id() string {
	return r.id
}

func (r Resource) Name() string {
	return r.name
}

func (r Resource) Type() string {
	return r.type_
}

func (r Resource) Group() Group {
	return r.group
}

func (r Resource) GetGroupName() string {
	return r.properties[GroupPropertyKey]
}

func (r Resource) GetStampIndex() string {
	return r.properties[StampPropertyKey]
}

func (r Resource) GetRoleCount() int {
	res := 0
	for k := range r.properties {
		if strings.HasPrefix(k, "role-") {
			res++
		}
	}
	return res
}

func (r Resource) HasProp(k string) bool {
	_, ok := r.properties[k]
	return ok
}

// get prop from resource or belonged group or runtime allocated prop
func (r Resource) GetProp(k string) (string, bool) {
	v, ok := r.properties[k]
	if !ok {
		v, ok = r.group.GetProp(k)
		if !ok {
			v, ok = r.allocatedProps[k]
		}
	}
	return v, ok
}

func (r Resource) IsFullyHealthy() bool {
	if v, ok := r.properties[HealthProp]; ok {
		return v == "ok"
	}
	return true
}

func (r Resource) GetNamespace() string {
	ns, _ := r.GetProp(NamespacePropertyKey)
	return ns
}

func (r Resource) InUnassignedPool() bool {
	if r.IsBrNode() {
		return false
	}
	if wsapisv1.ManagementSeparationEnforced && r.IsMgmtNode() {
		return false
	}
	return r.GetNamespace() == ""
}

func (r Resource) IsInNodegroup() bool {
	isResInNodegroup := false
	if _, ok := r.properties[RoleMemoryPropKey]; ok {
		isResInNodegroup = true
	} else if _, ok := r.properties[RoleWorkerPropKey]; ok {
		isResInNodegroup = true
	}
	return isResInNodegroup
}

func (r Resource) IsMgmtNode() bool {
	_, ok := r.GetProp(RoleMgmtPropKey)
	return ok
}

func (r Resource) IsCrdNode() bool {
	_, ok := r.GetProp(RoleCrdPropKey)
	return ok
}

func (r Resource) IsMemxNode() bool {
	_, ok := r.GetProp(RoleMemoryPropKey)
	return ok
}

func (r Resource) IsWrkNode() bool {
	_, ok := r.GetProp(RoleWorkerPropKey)
	return ok
}

func (r Resource) IsAxNode() bool {
	_, ok := r.GetProp(RoleAxPropKey)
	return ok
}

func (r Resource) IsIxNode() bool {
	_, ok := r.GetProp(RoleIxPropKey)
	return ok
}

func (r Resource) IsBrNode() bool {
	_, ok := r.GetProp(RoleBRPropKey)
	return ok
}

func (r Resource) GetProps() map[string]string {
	return CopyMap(r.properties)
}

func (r Resource) NonMatchingPropInAllocatedLocks(k, v string) bool {
	for _, rlock := range r.allocatedLocks {
		if rlock != nil && rlock.Annotations[k] != v {
			return true
		}
	}
	return false
}

func (r Resource) Subresources() Subresources {
	if len(r.subresources) == 0 {
		return nil
	}
	return CopyMap(r.subresources)
}

func (r Resource) GetAllocatedSubresources() Subresources {
	return CopyMap(r.allocated)
}

func (r Resource) SubresourceRanges() SubresourceRanges {
	if len(r.subresourceRanges) == 0 {
		return nil
	}
	return CopyMap(r.subresourceRanges.Canonicalize())
}

func (r Resource) GetAllocatedSubresourceRanges() SubresourceRanges {
	return CopyMap(r.allocatedRanges)
}

func (r Resource) GetSubresourceRangesResNameSortedByLoad(resType string) []string {
	var result []string
	remainingSubresourceRanges, ok := r.SubresourceRanges().Subtract(r.allocatedRanges)
	if !ok {
		return result
	}
	result = Keys(r.index.SubresourceRanges[resType])
	// subresources with lower usage or smaller name go first
	slices.SortFunc(result, func(a, b string) bool {
		if remainingSubresourceRanges.Size(a) != remainingSubresourceRanges.Size(b) {
			return remainingSubresourceRanges.Size(a) > remainingSubresourceRanges.Size(b)
		}
		return a < b
	})
	return result
}

func (r Resource) GetIndex() SubresourceIndex {
	return r.index
}

func (r Resource) GetNicRawNamesSorted() []string {
	var nics []string
	if nicResMap, ok := r.index.Subresources[NodeNicSubresource]; ok {
		for nicResName := range nicResMap {
			nics = append(nics, nicResName[(len(NodeNicSubresource)+1):])
		}
	}
	slices.Sort(nics)
	return nics
}

func (r Resource) GetNicResNamesSorted() []string {
	var nics []string
	if nicResMap, ok := r.index.Subresources[NodeNicSubresource]; ok {
		nics = append(nics, Keys(nicResMap)...)
	}
	slices.Sort(nics)
	return nics
}

func (r Resource) IsNvmfReady() bool {
	_, isTarget := r.GetProp(NvmfTargetIdPropKey)
	_, isInitiator := r.GetProp(NvmfInitiatorPropKey)
	return isTarget || isInitiator
}

// HasCapacity returns true if the resource has enough potential capacity to satisfy the request.
func (r Resource) HasCapacity(request Subresources, ignoreRes map[string]bool) bool {
	for k, v := range request {
		if ignoreRes[IgnoreSubresPrefix+k] {
			continue
		}
		if r.GetSubresourceCountCapacity(k) < v {
			return false
		}
	}
	return true
}

// HasAvailable returns true if the resource has enough available subresources to satisfy the request.
func (r Resource) HasAvailable(request Subresources, ignoreRes map[string]bool) bool {
	for k, v := range request {
		if ignoreRes[IgnoreSubresPrefix+k] {
			continue
		}
		if r.GetSubresourceCountAvailable(k) < v {
			return false
		}
	}
	return true
}

// HasSubresourceRangesCapacity returns true if the resource has enough resource ranges capacity to satisfy the request.
func (r Resource) HasSubresourceRangesCapacity(name string, amount int64) bool {
	for _, _range := range r.subresourceRanges[name] {
		if _range.Size() >= amount {
			return true
		}
	}
	return false
}

// HasSubresourceRangesAvailable returns true if the resource has enough subresource ranges available to satisfy the request.
func (r Resource) HasSubresourceRangesAvailable(name string, amount int64) bool {
	remainingSrr, ok := r.subresourceRanges.Subtract(r.allocatedRanges)
	if !ok {
		return false
	}
	for _, _range := range remainingSrr[name] {
		if _range.Size() >= amount {
			return true
		}
	}
	return false
}

func (r Resource) HasAllocation() bool {
	return len(r.allocatedLocks) > 0 || len(r.allocated) > 0 || len(r.allocatedRanges) > 0
}

// GetSubresourceRangesCandidate attempts to get a range for the amount requested.
// This method currently looks for the first range available from the beginning of the SubresourceRanges.
// TODO: optimize to search by scoring to reduce fragmentation
func (r Resource) GetSubresourceRangesCandidate(name string, amount int64) *SubresourceRanges {
	remainingSrr, ok := r.subresourceRanges.Subtract(r.allocatedRanges)
	if !ok {
		return nil
	}
	for _, _range := range remainingSrr[name] {
		if _range.Size() >= amount {
			return &SubresourceRanges{
				name: []Range{{Start: _range.Start, End: _range.Start + amount}},
			}
		}
	}
	return nil
}

// AvailableSubresources returns a new Subresource object containing the subresources available for allocation.
func (r Resource) AvailableSubresources() Subresources {
	asr := r.Subresources()
	for k, v := range r.allocated {
		asr[k] -= v
	}
	return asr
}

// GetSubresourceCountAvailable returns a new Subresource available count of specific type.
// in case of indexed resource, get aggregated count.
func (r Resource) GetSubresourceCountAvailable(k string) int {
	availableK := 0
	if _, ok := r.index.Subresources[k]; ok {
		for res := range r.index.Subresources[k] {
			availableK += r.subresources[res] - r.allocated[res]
		}
	} else {
		availableK = r.subresources[k] - r.allocated[k]
	}
	return availableK
}

// GetMemForHeteroAssign returns mem resource - max(overheadGB, overheadRatio*totalMem).
func (r Resource) GetMemForHeteroAssign(overheadGB int, overheadRatio float64) int {
	overheadMB := overheadGB << 10
	if val := int(float64(r.subresources[MemSubresource]) * overheadRatio); val > overheadMB {
		overheadMB = val
	}
	return r.subresources[MemSubresource] - overheadMB
}

// GetSubresourceCountCapacity returns a new Subresource total count of specific type.
// in case of indexed resource, get aggregated count.
func (r Resource) GetSubresourceCountCapacity(k string) int {
	s := 0
	if v, ok := r.index.Subresources[k]; ok {
		for rs := range v {
			s += r.subresources[rs]
		}
		return s
	}
	return r.subresources[k]
}

// GetHighestScoredIndexRes returns highest scored indexed subresource
// indexKey example: "cs-port" => "nic-0"
// affinityKey example: "system0-0"
// minUnit is to keep compatible with legacy format of cs-port:1 => cs-port:100gbps
// preferPacking will prefer min remaining resource with lowest capacity (e.g. br ingress)
func (r Resource) getHighestScoredIndexRes(indexKey string,
	affinityKey string, minUnit int, preferPacking bool) (int, string) {
	resNames := Keys(r.index.Subresources[indexKey])

	sort.Slice(resNames, func(i, j int) bool {
		scoreI := r.scoreIndexRes(affinityKey, resNames[i], minUnit, preferPacking)
		scoreJ := r.scoreIndexRes(affinityKey, resNames[j], minUnit, preferPacking)
		if scoreI != scoreJ {
			return scoreI > scoreJ
		}
		// try sorting res by id if possible
		idsI := strings.Split(resNames[i], "/")
		intI, err := strconv.Atoi(idsI[len(idsI)-1])
		if err == nil {
			idJ := strings.Split(resNames[j], "/")
			intJ, _ := strconv.Atoi(idJ[len(idJ)-1])
			return intI < intJ
		}
		return resNames[i] < resNames[j]
	})

	if len(resNames) != 0 {
		return r.scoreIndexRes(affinityKey, resNames[0], minUnit, preferPacking), resNames[0]
	}
	return -1, ""
}

// scoreIndexRes scores indexed res by affinity+remaining res, affinity has higher weight
// preferPacking will prefer lower remaining
func (r Resource) scoreIndexRes(affinityKey, name string, minUnit int, preferPacking bool) int {
	score := 0
	srAmount := r.GetSubresourceCountAvailable(name)
	if srAmount <= 0 {
		return -1
	} else if r.Subresources()[AuxiliaryNicUsageLimit] > 0 &&
		r.allocated[fmt.Sprintf("%s-%s", AuxiliaryNicUsageLimit, name)] >= r.Subresources()[AuxiliaryNicUsageLimit] {
		return -1
	}

	v, ok := r.properties[affinityKey]
	// this simulates an affinity check by val exists in list of ["$name", ""] or val == resName
	hasAffinity := affinityKey == name || (ok && (v == name || v == ""))
	affinityWeight := 1
	if hasAffinity {
		affinityWeight = 1000
		// res with allocated affinity will be scored higher
		score += affinityWeight
	}

	remain := srAmount - minUnit
	if !preferPacking {
		// res with more remaining will be scored higher
		score += remain * affinityWeight
	} else {
		capacity := r.GetSubresourceCountCapacity(name)
		// upper limit for nic BW in theory, 100*100Gbps(minUnit)
		maxCapacity := 100 * minUnit
		// res with less remaining and less capacity will be scored higher
		score += (maxCapacity - remain - capacity) * affinityWeight
	}
	return score
}

// WeightedRemaining returns weighted remaining score.
func (r Resource) WeightedRemaining() int {
	weightedCount := func(sr string, c int) int {
		s := 0
		if sr == MemSubresource {
			s += c * MemSubresourceScoringWeight
		} else {
			s += c
		}
		return s
	}

	s := 0
	for sr, srVal := range r.AvailableSubresources() {
		s += weightedCount(sr, srVal)
	}
	return s
}

// score and convert indexed req to named ones, e.g. cs-port:1 => nic0:1
//
// for now, we expect minimal request unit can always be satisfied by any named resource
// e.g. the following case won't happen in reality
// nic0=>100, nic1=>100, request 1 * "cs-port"=150,
// the current logic allows grant with nic0+nic1 since the aggregated value 200 > 150
// but this is actually wrong since no single nic can satisfy the request
// since this won't happen in reality, we keep this simple for now
func (r Resource) ScoreIndexedReq(groupReqId string, req Request) (int, map[string]Subresources) {
	isBrRequest := strings.HasPrefix(groupReqId, rlv1.BrRequestName)
	isActKvssRequest := strings.HasPrefix(groupReqId, rlv1.ActivationRequestName) || strings.HasPrefix(groupReqId, rlv1.KVStorageServerRequestName)
	isSWDriverRequest := strings.HasPrefix(groupReqId, rlv1.SWDriverRequestName)
	if !isBrRequest && !isActKvssRequest && !isSWDriverRequest {
		// As of rel-2.3, we only expect BR, ACT and SWDR to use nic scheduling.
		return -1, nil
	}

	rcopy := r.Copy()
	indexedRes := map[string]Subresources{}
	score := 0

	// For nic scheduling, "indexKey" below is "cs-port".
	for indexKey, srAmount := range req.subresources {
		if _, ok := r.index.Subresources[indexKey]; !ok {
			continue
		}

		// In AX scheduling for activations, we predefine the capacity of node nics and min unit is 1
		minUnit := 1
		// If subresource is nic, and not an activation or SW driver request, use default nic BW
		if strings.HasPrefix(indexKey, NodeNicSubresource) &&
			!isActKvssRequest &&
			!isSWDriverRequest {
			minUnit = DefaultNicBW
		}

		var lastRes string
		var id int
		// repeatedly assign to highest scored subresource assuming minimal unit can always be satisfied
		for srAmount > 0 {
			var affinityKey string
			if isActKvssRequest || isSWDriverRequest {
				// currently only handle one or zero affinity key
				for k := range req.affinities {
					affinityKey = k
					break
				}
			} else if isBrRequest && lastRes != "" {
				// br prefers to pack egress on same NIC
				affinityKey = lastRes
			}
			// br prefer to assign ingress on lowest capacity/remaining NIC
			resScore, resName := rcopy.getHighestScoredIndexRes(indexKey, affinityKey, minUnit, isBrRequest && id == 0)
			if resScore < 0 {
				return resScore, indexedRes
			}
			if id > 0 {
				lastRes = resName
			}
			rcopy.allocated[resName] += minUnit
			indexedRes[indexKey] = EnsureMap(indexedRes[indexKey])
			indexedRes[indexKey][resName] += minUnit
			indexedRes[AuxiliaryNicUsageLimit] = EnsureMap(indexedRes[AuxiliaryNicUsageLimit])
			indexedRes[AuxiliaryNicUsageLimit][fmt.Sprintf("%s-%s", AuxiliaryNicUsageLimit, resName)] = req.Filter.Subresources()[AuxiliaryNicUsageLimit]
			srAmount -= minUnit
			score += resScore
			id++
		}
	}

	return score, indexedRes
}

// Allocate adds the given subresources to the resource's allocated subresources. It is assumed that the caller has
// already checked that the resource has enough available subresources to allocate. It also assumes that there is
// no concurrent modifications to the resource.
// return optional indexed resources override, e.g. cs-port: 200  =>  cs-port: {nic0: 100, nic1: 100}
func (r Resource) Allocate(lock *rlv1.ResourceLock, srallocs SubresourceAllocation) {
	// for test case simplicity only
	if lock != nil {
		r.allocatedLocks[lock.NamespacedName()] = lock
	}
	for k, v := range srallocs.Subresources {
		r.allocated[k] += v
	}
	for k, v := range srallocs.SubresourceRanges {
		r.allocatedRanges[k] = append(r.allocatedRanges[k], v...)
	}
	r.allocatedRanges = r.allocatedRanges.Canonicalize()
}

// Allocate requested subresources + anti-props
func (r Resource) AllocateReq(req Request, indexOverride map[string]Subresources) {
	// assign allocateProps for following sub-requests filtering
	// only assign within the current group grant cycle on the copied resources
	for k, v := range req.allocateProps {
		r.allocatedProps[k] = v
	}
	rs := req.Subresources().Copy()
	for index, res := range indexOverride {
		delete(rs, index)
		rs.Add(res)
	}
	r.Allocate(nil, SubresourceAllocation{Subresources: rs})
}

func (r Resource) Deallocate(lockNamespacedName string, srAllocs SubresourceAllocation) {
	delete(r.allocatedLocks, lockNamespacedName)
	for k, v := range srAllocs.Subresources {
		r.allocated[k] -= v
		if r.allocated[k] <= 0 {
			delete(r.allocated, k)
		}
	}

	allocatedRanges, _ := r.allocatedRanges.Subtract(srAllocs.SubresourceRanges)
	for k := range r.allocatedRanges {
		if len(allocatedRanges[k]) == 0 {
			delete(r.allocatedRanges, k)
		} else {
			r.allocatedRanges[k] = allocatedRanges[k]
		}
	}
}

func (r Resource) Copy() Resource {
	return Resource{
		type_:             r.type_,
		name:              r.name,
		id:                r.id,
		properties:        r.properties,
		group:             r.group,
		subresources:      r.subresources,
		subresourceRanges: r.subresourceRanges,
		index:             r.index,
		// avoid updating allocated in try scheduling cycle
		allocated:       CopyMap(r.allocated),
		allocatedRanges: CopyMap(r.allocatedRanges),
		// allocatedProps props are temporary/local to each schedule cycle
		allocatedProps: CopyMap(r.allocatedProps),
		allocatedLocks: CopyMap(r.allocatedLocks),
	}
}

// UnallocatedCopy returns a copy of the resource with no allocated subresources. Useful for determining if a group of
// requests can fit a resource
func (r Resource) UnallocatedCopy() Resource {
	return Resource{
		type_:             r.type_,
		name:              r.name,
		id:                r.id,
		properties:        r.properties,
		group:             r.group,
		subresources:      r.subresources,
		subresourceRanges: r.subresourceRanges,
		index:             r.index,
		allocated:         Subresources{},
		allocatedRanges:   SubresourceRanges{},
		allocatedProps:    map[string]string{},
		allocatedLocks:    map[string]*rlv1.ResourceLock{},
	}
}

// DeepUnallocatedCopy returns a deep copy of the resource with no allocated subresources
// and allows modification without impact original res.
func (r Resource) DeepUnallocatedCopy() Resource {
	return Resource{
		type_:             r.type_,
		name:              r.name,
		id:                r.id,
		group:             r.group.DeepCopy(),
		properties:        CopyMap(r.properties),
		subresources:      CopyMap(r.subresources),
		subresourceRanges: CopyMap(r.subresourceRanges),
		index:             r.index.Copy(),
		allocated:         Subresources{},
		allocatedRanges:   SubresourceRanges{},
		allocatedProps:    map[string]string{},
		allocatedLocks:    map[string]*rlv1.ResourceLock{},
	}
}

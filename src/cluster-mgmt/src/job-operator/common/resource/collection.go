package resource

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	v1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

const (
	NamespacePropertyKey          = "namespace"
	TopologyPropertyKey           = "topology"
	GroupPropertyKey              = "group"
	V2GroupPropertyKey            = "v2Group"
	StampPropertyKey              = "stamp"
	AssignedRedundancyPropertyKey = "assigned-redundancy"
	PlatformVersionPropertyKey    = "platform-version"
	SystemVersionPropertyKey      = "system-version"
	PortAffinityPropKeyPrefix     = "port-affinity"
	PreferredCsPortsPropKey       = "preferred-cs-ports"
	NvmfDiskBytesPropertyKey      = "nvme-per-disk-bytes"
	SwitchPropertyKey             = "switch"

	IgnorePropPrefix   = "prop/"
	IgnoreSubresPrefix = "subres/"

	RoleMgmtPropKey   = "role-management"
	RoleCrdPropKey    = "role-coordinator"
	RoleBRPropKey     = "role-broadcastreduce"
	RoleMemoryPropKey = "role-memory"
	RoleWorkerPropKey = "role-worker"
	RoleAxPropKey     = "role-activation"
	RoleIxPropKey     = "role-inferencedriver"

	// penalty on shared pool resource to prefer local session's resources
	NonLocalSessionPenalty = 1_000_000_000_000

	NonApplicableField = "-"
)

// Collection is a collection of resources. It is used to filter over subsets of resources during scheduling.
// Collection is used both to represent the entire set of resources in the cluster and collections are used to contain
// subsets of nodes for node-group fit calculations.
// Collection is not thread safe. Additionally, properties are assumed to be immutable and should therefore
// not be modified after adding to the collection.
type Collection struct {
	resources map[string]Resource
	groups    map[string]Group

	// An inverted index of property "k:v" -> resourceIds where resourceIds is a map of resource id to true if the
	// resource has the property key/value pair. E.g. find all system resources would be keys(index["type_:system"])
	// This can be discarded and rebuilt when resources changes.
	index map[string]map[string]bool

	// Map of property/subresource keys which will be ignored during filtering.
	// Used for cases when request's filter is tricky to alter.
	ignoreKeys map[string]bool
}

// Copy is a copy of the Collection, allowing for modifications of resource allocations without modifying the original collection.
func (c *Collection) Copy() *Collection {
	resources := map[string]Resource{}
	for id, r := range c.resources {
		resources[id] = r.Copy()
	}

	return &Collection{
		resources:  resources,
		groups:     c.groups,
		ignoreKeys: c.ignoreKeys,

		// new Collection rebuilds index when needed as we don't share state to avoid bugs
		// could be shared for optimization
	}
}

// DeepUnallocatedCopy is a deep copy of the Collection. Resource allocation is discarded during a deep copy. A deep copy also
// allows modifications of subresources without modifying the original collection.
func (c *Collection) DeepUnallocatedCopy() *Collection {
	groups := map[string]Group{}
	resources := map[string]Resource{}
	for id, r := range c.resources {
		rcopy := r.DeepUnallocatedCopy()
		if r.group.name != "" {
			// point to the same group property if possible
			if g, ok := groups[r.group.name]; ok {
				rcopy.group = g
			}
			groups[r.group.name] = rcopy.group
		}
		resources[id] = rcopy
	}
	for id, g := range c.groups {
		if _, ok := groups[id]; !ok {
			groups[id] = g.DeepCopy()
		}
	}
	return &Collection{
		resources:  resources,
		groups:     groups,
		ignoreKeys: c.ignoreKeys,
	}
}

// UnallocatedCopy returns a deep copy of the Collection where every resource's allocations have been cleared. Useful
// for determining if the Collection can fit a lock when the collection is completely free.
func (c *Collection) UnallocatedCopy() *Collection {
	resources := map[string]Resource{}
	for id, r := range c.resources {
		resources[id] = r.UnallocatedCopy()
	}

	return &Collection{
		resources:  resources,
		groups:     c.groups,
		ignoreKeys: c.ignoreKeys,
	}
}

func (c *Collection) AddGroup(g Group) {
	if c.groups == nil {
		c.groups = map[string]Group{}
	}
	if existing, ok := c.groups[g.name]; ok {
		for k, v := range g.properties {
			existing.properties[k] = v
		}
	} else {
		c.groups[g.name] = g
	}
	c.index = nil
}

// Add a Resource. If the Resource is a member of a group, panics if the group was not added
func (c *Collection) Add(resource Resource) string {
	if c.resources == nil {
		c.resources = map[string]Resource{}
	}
	if _, ok := c.resources[resource.Id()]; ok {
		panic("resource with id already exists, " + resource.Id())
	}

	grpName := resource.properties[GroupPropertyKey]
	if grpName != "" && resource.IsInNodegroup() {
		if c.groups == nil {
			c.groups = map[string]Group{}
		}
		if _, ok := c.groups[grpName]; !ok {
			c.groups[grpName] = Group{name: grpName, properties: map[string]string{}}
		}
		resource.group = c.groups[grpName]
	}

	c.resources[resource.Id()] = resource
	c.index = nil
	return resource.Id()
}

func (c *Collection) UpdateSubresource(resourceId string, k string, v int) {
	_, ok := c.Get(resourceId)
	if !ok {
		return
	}
	if v == 0 {
		delete(c.resources[resourceId].subresources, k)
		for _, keys := range c.resources[resourceId].index.Subresources {
			if keys[k] {
				delete(keys, k)
			}
		}
	} else {
		c.resources[resourceId].subresources[k] = v
	}
}

func (c *Collection) UpdateSubresourceRanges(resourceId string, k string, v []Range) {
	_, ok := c.Get(resourceId)
	if !ok {
		return
	}
	if len(v) == 0 {
		delete(c.resources[resourceId].subresourceRanges, k)
		for _, keys := range c.resources[resourceId].index.SubresourceRanges {
			if keys[k] {
				delete(keys, k)
			}
		}
	} else {
		c.resources[resourceId].subresourceRanges[k] = v
	}
}

func (c *Collection) Remove(resourceId string) {
	delete(c.resources, resourceId)
	c.index = nil
}

func (c *Collection) Get(id string) (Resource, bool) {
	rv, ok := c.resources[id]
	return rv, ok
}

func (c *Collection) GetGroup(id string) (Group, bool) {
	g, ok := c.groups[id]
	return g, ok
}

func (c *Collection) ListGroups() []Group {
	rv := make([]Group, 0, len(c.groups))
	for i := range c.groups {
		rv = append(rv, c.groups[i])
	}
	return rv
}

func (c *Collection) AddIgnorePropKey(key string) {
	c.ignoreKeys = EnsureMap(c.ignoreKeys)
	c.ignoreKeys[IgnorePropPrefix+key] = true
}

func (c *Collection) AddIgnoreSubresKey(key string) {
	c.ignoreKeys = EnsureMap(c.ignoreKeys)
	c.ignoreKeys[IgnoreSubresPrefix+key] = true
}

func (c *Collection) ClearIgnoreKeys() {
	c.ignoreKeys = nil
}

func (c *Collection) GetStampSystemCount(resourceIds []string) map[string]int {
	stampSystemCount := map[string]int{}
	for _, id := range resourceIds {
		sys, _ := c.Get(id)
		stampSystemCount[sys.GetStampIndex()]++
	}
	return stampSystemCount
}

func (c *Collection) List() []Resource {
	var resources []Resource
	for _, r := range c.resources {
		resources = append(resources, r)
	}
	return resources
}

func (c *Collection) Sample() Resource {
	for _, r := range c.resources {
		return r
	}
	return Resource{}
}

func (c *Collection) ListRids() []string {
	return Keys(c.resources)
}

func (c *Collection) ListSystems() []string {
	var resNames []string
	for _, r := range c.resources {
		if r.type_ != SystemType {
			continue
		}
		resNames = append(resNames, r.Name())
	}
	return resNames
}

func (c *Collection) ListResNames() []string {
	var resNames []string
	for _, r := range c.resources {
		resNames = append(resNames, r.Name())
	}
	return resNames
}

func (c *Collection) Size() int {
	return len(c.resources)
}

// IsUnallocated returns true if all resources in the collection are unallocated.
func (c *Collection) IsUnallocated() bool {
	for _, rs := range c.resources {
		if len(rs.allocated) > 0 {
			return false
		}
	}
	return true
}

// Allocate takes a map of resourceId -> subresource allocation and checks that the collection can fit the request,
// then does the allocation. The check is done to avoid a partial allocation where an error could be returned after
// some but not all resources have been allocated.
func (c *Collection) Allocate(lock *v1.ResourceLock, subresAllocs map[string]SubresourceAllocation) {
	for rid, subresAlloc := range subresAllocs {
		res, ok := c.Get(rid)
		if ok {
			if !res.HasAvailable(subresAlloc.Subresources, nil) {
				// this could happen if resources(e.g. NICs) are removed as error hardware and controller restart
				// TODO: this can be optimized to track status in resource collection instead of removal directly
				logrus.Warnf("resource %v cannot be fit to requested allocation: %s during assignment", res, subresAlloc.Subresources)
			}
			res.Allocate(lock, subresAlloc)
		} else {
			// this could happen if resources are removed as error hardware and controller restart
			logrus.Warnf("resource %s not found during assignment", rid)
		}
	}
}

// AllocateForced takes a map of resourceId -> subresource and forces allocation of the lock over the collection,
// even if the lock requests more subresources than is in the resource. Useful for forcing a FIFO reservation.
func (c *Collection) AllocateForced(lock *v1.ResourceLock, targetSession string, subresAllocs map[string]SubresourceAllocation,
	fullAllocation, enableClusterMode bool) {
	for rid, subresAlloc := range subresAllocs {
		if res, ok := c.Get(rid); ok {
			// for fully granted or reserving-only locks, allocation should fully respect grant result
			// for only partially granted locks, only reserve local session resources
			// skip reserve global/sharable resources which can be starved by user session with long queue
			if !fullAllocation && enableClusterMode && targetSession != "" {
				if res.GetNamespace() == "" || res.GetNamespace() != targetSession {
					continue
				}
			}
			res.Allocate(lock, subresAlloc)
		} else {
			// this should not happen
			logrus.Warnf("resource %s not found during reservation", rid)
		}
	}
}

// Deallocate takes a map of resourceId -> subresource and deallocates the given subresources them from the collection.
// This ignores errors if the resource is not found in the collection.
func (c *Collection) Deallocate(lockNamespacedName string, subresAllocs map[string]SubresourceAllocation) {
	for rid, subresAlloc := range subresAllocs {
		if res, ok := c.Get(rid); ok {
			res.Deallocate(lockNamespacedName, subresAlloc)
		} else {
			// this could happen if resources are removed as error hardware and job completes
			logrus.Warnf("resource %s not found during unassignment", rid)
		}
	}
}

// reindex rebuilds the inverted index of properties. This needs to be rebuilt any time that resources are added or
// removed from the collection. It assumes that properties are immutable.
func (c *Collection) reindex() {
	c.index = map[string]map[string]bool{}
	for _, rs := range c.resources {
		for k, v := range rs.properties {
			prop := k + ":" + v
			if ids, ok := c.index[prop]; !ok {
				c.index[prop] = map[string]bool{rs.Id(): true}
			} else {
				ids[rs.Id()] = true
			}
		}
		// Inherit group's properties if resource did not override
		for k, v := range rs.group.properties {
			if _, ok := rs.properties[k]; ok {
				continue
			}
			prop := k + ":" + v
			if ids, ok := c.index[prop]; !ok {
				c.index[prop] = map[string]bool{rs.Id(): true}
			} else {
				ids[rs.Id()] = true
			}
		}
	}

	// type is also a kind of property
	for _, rs := range c.resources {
		typeKey := typeProperty + ":" + rs.Type()
		if ids, ok := c.index[typeKey]; !ok {
			c.index[typeKey] = map[string]bool{rs.Id(): true}
		} else {
			ids[rs.Id()] = true
		}
	}
}

// findByProperties returns a map of resourceIds that match one of the values of the given k/v propKey -> propValues.
// + also filter out resource based on antiAffinities
// If props+antiAffinities are both nil or empty, all resources are returned
// e.g. props = {"a": ["1", "2"], "b": ["3"]} + antiAffinities = {"c": "2", "d":""}
// resources will be returned if:
// has prop "a"=="1" or "a"=="2"
// && has prop "b"=="3"
// && has prop "c"=="2" or has no prop "c"
// && has no prop "d"
func (c *Collection) findByProperties(props map[string][]string, antiAffinities map[string]string) map[string]bool {
	var finalCandidates map[string]bool
	if len(props) == 0 {
		allIds := map[string]bool{}
		for id := range c.resources {
			allIds[id] = true
		}
		finalCandidates = allIds
	}

	// use inverted index to find resources matching properties
	for k, vals := range props {
		if c.ignoreKeys[IgnorePropPrefix+k] {
			continue
		}

		roundCandidates := map[string]bool{}
		for _, v := range vals {
			prop := k + ":" + v
			matchedIds, ok := c.index[prop]
			if !ok {
				continue
			}
			for matchedId := range matchedIds {
				roundCandidates[matchedId] = true
			}
		}

		if finalCandidates == nil {
			finalCandidates = roundCandidates
		} else {
			for id := range finalCandidates {
				if _, ok := roundCandidates[id]; !ok {
					delete(finalCandidates, id)
				}
			}
		}

		if len(finalCandidates) == 0 {
			break
		}
	}

	// safe to delete during iteration in go!
	// https://go.dev/doc/effective_go#for
	// https://stackoverflow.com/questions/23229975
	for k, v := range antiAffinities {
		for id := range finalCandidates {
			// skip only if key exists and val diff
			if rv, ok := c.resources[id].GetProp(k); ok {
				if v == "" || rv != v {
					delete(finalCandidates, id)
				}
			}
		}
	}

	return finalCandidates
}

// findByCustomFilter returns a map of resourceIds that match the custom filter.
func (c *Collection) findByCustomFilter(filter Filter) map[string]bool {
	rv := map[string]bool{}
	for id, res := range c.resources {
		if filter.customFilter(res) {
			rv[id] = true
		}
	}
	return rv
}

// Filter returns a list of resources that match the given matcher.
func (c *Collection) Filter(filter Filter) *Collection {
	if c.index == nil {
		c.reindex()
	}

	var candidateIds map[string]bool
	if filter.customFilter == nil {
		candidateIds = c.findByProperties(filter.properties, filter.antiAffinities)
	} else {
		candidateIds = c.findByCustomFilter(filter)
	}

	// filter by subresources, producing the final list
	rv := map[string]Resource{}
	for id := range candidateIds {
		resource, _ := c.Get(id)
		if resource.HasCapacity(filter.subresources, c.ignoreKeys) {
			rv[id] = resource
		}
	}

	return &Collection{resources: rv, groups: c.groups, ignoreKeys: c.ignoreKeys}
}

// FilterByProps return resources with each property k having one of the values v specified in props[k] = [v0, v1...vN]
// and not have any kv in antiAffinities
func (c *Collection) FilterByProps(props map[string][]string, antiAffinities map[string]string) *Collection {
	if c.index == nil {
		c.reindex()
	}
	candidateIds := c.findByProperties(props, antiAffinities)
	rv := map[string]Resource{}
	for id := range candidateIds {
		rv[id] = c.resources[id]
	}
	return &Collection{resources: rv, groups: c.groups, ignoreKeys: c.ignoreKeys}
}

// FilterByCapacity return resources with at least capacity specified in sr
func (c *Collection) FilterByCapacity(sr Subresources) *Collection {
	rv := map[string]Resource{}
	for id, rs := range c.resources {
		if rs.HasCapacity(sr, c.ignoreKeys) {
			rv[id] = c.resources[id]
		}
	}
	return &Collection{resources: rv, groups: c.groups, ignoreKeys: c.ignoreKeys}
}

// GetFreeNodegroups groups resources by their "group" property. It only returns groups if the group has no allocations
// on any of its resources which imposes the rule that NodeGroups are exclusively owned by a lock. Any existing
// allocation would imply that the nodeGroup is occupied already. This function also exclude populated nodegroups whose
// types do not match the pop group types passed in and returns the resources of populated nodegroups that match the search.
// TODO: this could be optimized by adding an index
func (c *Collection) GetFreeNodegroups(popGroupTypes []string) (map[string]*Collection, map[string]*Collection) {
	ignoreGroupVal := map[string]bool{}
	popGroups := map[string]*Collection{}
	allGroups := map[string]*Collection{}
	for _, rs := range c.resources {
		groupName, ok := rs.properties[GroupPropertyKey]
		if !ok {
			continue
		}

		if ignoreGroupVal[groupName] {
			continue
		}
		if len(rs.allocated) > 0 {
			ignoreGroupVal[groupName] = true
			delete(allGroups, groupName)
			delete(popGroups, groupName)
			continue
		}

		rdb := allGroups[groupName]
		if rdb == nil {
			allGroups[groupName] = &Collection{
				resources:  map[string]Resource{rs.Id(): rs},
				groups:     map[string]Group{rs.group.name: rs.group},
				ignoreKeys: c.ignoreKeys,
			}
			rdb = allGroups[groupName]
		} else {
			rdb.Add(rs)
		}

		// todo: refactor to remove explicit filter to keep scheduling msg readable
		if rs.group.IsPopulated() && rs.group.HasProps(map[string][]string{wsapisv1.MemxMemClassProp: popGroupTypes}) {
			popGroups[groupName] = rdb
		}
	}
	return popGroups, allGroups
}

// GetFreeNodes returns a collection of unallocated nodes.
func (c *Collection) GetFreeNodes() *Collection {
	unallocated := c.Copy()
	for rid, rs := range unallocated.resources {
		if rs.Type() != NodeType || len(rs.allocated) > 0 {
			unallocated.Remove(rid)
		}
	}
	return unallocated
}

// GetFreeSystems returns a collection of unallocated systems.
func (c *Collection) GetFreeSystems() *Collection {
	unallocated := c.Copy()
	for rid, rs := range unallocated.resources {
		if rs.Type() != SystemType || len(rs.allocated) > 0 {
			unallocated.Remove(rid)
		}
	}
	return unallocated
}

func (c *Collection) GetLeftOverSubResource(resId, subResType string, currentAllocations map[string]SubresourceAllocation) float64 {
	rs, ok := c.Get(resId)
	if !ok {
		// If failed to get resource, return 0 to indicate that this resource does not have any more leftovers
		return 0
	}
	return float64(rs.Subresources()[subResType]) - float64(currentAllocations[resId].Subresources[subResType])
}

// GetNodesWithMostAffinityCoverage finds the best set of nodes that have most even affinity spread from a collection.
// The function assumes the input systems and nodes in the same stamp in v2 network.
// An "edge" represents a one-hop connectivity from a node to a CSX.
// Among the selected set of nodes, if we had:
// - 5 edges to systemf0-cs-port-0
// - 5 edges to systemf0-cs-port-1
// - 3 edges to systemf0-cs-port-2
// then the map should look like map[5:2, 3:1].
// Indices in the structure represent weights of edges and values represent amount of such edges.
// The goal is to minimize the amount of distinct edge weights and maximize edge amounts per weight by measuring variance.
//
// Both job scheduling and session management leverage this function, though in different ways.
// For job scheduling, we are interested to find a set of unallocated (not in use by jobs) nodes from the same namespace.
// For session management (with auto ax selection only):
// - when we need to create a new session, or grow a session, we are interested to find a set of additional nodes that do not belong to any namespace.
// - when we need to shrink a session, we are interested to find a subset of nodes within the current selection.
// TODO: There may be opportunities to refactor so the code behaves more similar to ungroup scheduling.
func (c *Collection) GetNodesWithMostAffinityCoverage(namespace, name string,
	systemsInStamp []string, allocated *Collection, totalNodesNeeded int, portAffinities map[int]bool,
	antiPortAffinities map[int]bool, portCount int) (*Collection, map[int]int) {
	type sourceNode struct {
		res                   Resource
		systemPortAffinities  []int
		sumAffinityScore      int
		minAffinityScore      int
		portAffinityScore     int
		antiPortAffinityScore int
		portsInAffinity       []int // for logging only
	}
	var sourceNodes []sourceNode

	sysIndexMap := map[string]int{}
	for i, v := range systemsInStamp {
		sysIndexMap[v] = i
	}

	cCopy := c.Copy()
	if allocated != nil {
		// Add allocated resources if not already added
		for name, res := range allocated.resources {
			if _, ok := cCopy.resources[name]; !ok {
				cCopy.Add(res)
			}
		}
	}

	requestName := namespace
	if name != "" {
		requestName = namespace + "/" + name
	}
	reqPreamble := fmt.Sprintf("Request: %s, ", requestName)
	if len(portAffinities) > 0 || len(antiPortAffinities) > 0 {
		logrus.Infof("%sport affinities: %v, anti port affinities: %v", reqPreamble, portAffinities, antiPortAffinities)
	}

	aggregatedAffinities := make([]int, len(systemsInStamp)*portCount)
	for _, res := range cCopy.resources {
		systemPortAffinities := make([]int, len(systemsInStamp)*portCount)
		portsInAffinities := map[int]bool{}
		for k := range res.GetProps() {
			if !strings.HasPrefix(k, PortAffinityPropKeyPrefix+"/") {
				continue
			}
			sysPortStr := strings.TrimPrefix(k, PortAffinityPropKeyPrefix+"/")
			id := strings.LastIndex(sysPortStr, "-")
			if id < 0 {
				continue
			}

			system := sysPortStr[:id]
			sysIdx, ok := sysIndexMap[system]
			if !ok {
				// A node could have port affinity to all systems, but "sysIndexMap" only has the granted systems
				// It's normal in this case to not find the system.
				continue
			} else {
				portStr := sysPortStr[id+1:]
				port, err := strconv.Atoi(portStr)
				if err != nil || port < 0 || port >= portCount {
					// this should not happen
					logrus.Errorf("%sfailed to parse port %s (affinity: %s)", reqPreamble, portStr, k)
					continue
				}

				systemPortAffinities[sysIdx*portCount+port] = 1
				aggregatedAffinities[sysIdx*portCount+port] += 1
				portsInAffinities[port] = true
			}

		}
		sourceNodes = append(sourceNodes, sourceNode{
			res:                  res,
			systemPortAffinities: systemPortAffinities,
			portsInAffinity:      SortedKeys(portsInAffinities),
		})
	}

	slices.SortFunc(sourceNodes, func(a, b sourceNode) bool {
		// health check is for session mgmt to prioritize picking healthy nodes first
		if a.res.IsFullyHealthy() != b.res.IsFullyHealthy() {
			return a.res.IsFullyHealthy()
		}
		// prioritize on local resources if redundancy assigned
		aMatches := a.res.GetNamespace() == namespace
		bMatches := b.res.GetNamespace() == namespace
		if aMatches != bMatches {
			return aMatches
		}
		// The length sort is not technically necessary, but makes it easier to understand test cases
		// ax-1 < ax-2 < ax-11
		if len(a.res.Name()) != len(b.res.Name()) {
			return len(a.res.Name()) < len(b.res.Name())
		}
		return a.res.Name() < b.res.Name()
	})

	// Calculates the mean and variance of the current distribution of one-hop connections
	calculateVariance := func(distribution []int) (variance float64) {
		n := float64(len(distribution))
		sum := float64(0)
		for _, value := range distribution {
			sum += float64(value)
		}
		mean := sum / n

		var sumOfSquares float64
		for _, value := range distribution {
			sumOfSquares += math.Pow(float64(value)-mean, 2)
		}
		variance = sumOfSquares / n
		return
	}

	// Calculate the variance on node change
	calculateVarianceOnNodeChange := func(distribution []int, node sourceNode, isAdd bool) float64 {
		newDistribution := make([]int, len(distribution))
		copy(newDistribution, distribution)
		for i, conn := range node.systemPortAffinities {
			if isAdd {
				newDistribution[i] += conn
			} else {
				newDistribution[i] -= conn
			}
		}
		return calculateVariance(newDistribution)
	}

	// Score nodes such that the nodes with weaker aggregated affinity get picked first.
	// If we had picked a node with stronger aggregated affinity first, we'd lose some potential
	// to find a lower variance. Between two nodes, if one has a smaller sum, we pick that one.
	// If both has the same sum, we pick the one with lower min.
	// For example:
	// - ax-0: (port0, port1)
	// - ax-1: (port1, port2)
	// - ax-2: (port2, port3)
	// To schedule a 2-CSX job, we want to pick (ax-0 + ax-2), instead of (ax-1 + any)
	scoreNodeAffinity := func(node sourceNode) (int, int, int, int) {
		sum := 0
		min := math.MaxInt
		preferredAffinityScore := 0
		nonPreferredAffinityScore := 0
		for idx, affinity := range node.systemPortAffinities {
			if affinity > 0 {
				sum += aggregatedAffinities[idx]
				if aggregatedAffinities[idx] < min {
					min = aggregatedAffinities[idx]
				}
			}
		}
		for _, port := range node.portsInAffinity {
			if portAffinities[port] {
				preferredAffinityScore += 1
			} else if antiPortAffinities[port] {
				nonPreferredAffinityScore += 1
			}
		}
		return sum, min, preferredAffinityScore, nonPreferredAffinityScore
	}

	selection := &Collection{}
	distribution := make([]int, len(systemsInStamp)*portCount)

	// Pre-allocate nodes that were already in the selection
	if allocated != nil {
		for _, allocatedRes := range allocated.resources {
			for j, node := range sourceNodes {
				if node.res.Name() == allocatedRes.Name() {
					// Updates the distribution with the best node's affinities
					for i, conn := range node.systemPortAffinities {
						distribution[i] += conn
						aggregatedAffinities[i] -= conn
					}
					selection.Add(node.res)
					// Removes the selected node from sourceNodes to prevent reselection
					sourceNodes = append(sourceNodes[:j], sourceNodes[j+1:]...)
					break
				}
			}
		}
	}

	// Greedy algorithm to select source nodes aiming to minimize the variance increase
	selectionAffinities := map[int]int{}
	for len(selection.resources) < totalNodesNeeded {
		bestSelectedVariance := math.MaxFloat64
		bestUnselectedVariance := math.MaxFloat64
		bestIndex := -1
		var bestNode sourceNode
		isLastRound := (totalNodesNeeded - len(selection.resources)) == 1

		for i, node := range sourceNodes {
			selectedVariance := calculateVarianceOnNodeChange(distribution, node, true)
			unselectedVariance := calculateVarianceOnNodeChange(aggregatedAffinities, node, false)
			node.sumAffinityScore, node.minAffinityScore, node.portAffinityScore, node.antiPortAffinityScore = scoreNodeAffinity(node)

			// Floating point values that are mathematically identical might end up
			// not equal when doing comparison. Best practice is to add a tolerance.
			tolerance := 1e-6
			selectedVarianceImproved := selectedVariance < bestSelectedVariance-tolerance
			unselectedVarianceImproved := unselectedVariance < bestUnselectedVariance-tolerance
			selectedVarianceDegraded := math.Abs(selectedVariance-bestSelectedVariance) > tolerance

			isBetterChoice := false
			reasonPreamble := fmt.Sprintf("%sbest node so far due to ", reqPreamble)
			if i == 0 {
				// We pick the first source node no matter what
				isBetterChoice = true
			} else if bestNode.portAffinityScore > node.portAffinityScore ||
				bestNode.antiPortAffinityScore < node.antiPortAffinityScore {
				// We would never pick a node with less preferred affinities, or with higher non-preferred affinities
				continue
			} else if bestNode.portAffinityScore < node.portAffinityScore {
				// We prioritize picking a nodes with max preferred port affinities first
				isBetterChoice = true
				logrus.Debugf("%sport affinities are preferred: %s", reasonPreamble, node.res.Name())
			} else if bestNode.antiPortAffinityScore > node.antiPortAffinityScore {
				isBetterChoice = true
				logrus.Debugf("%savoiding non-preferred port affinities: %s", reasonPreamble, node.res.Name())
			} else if selectedVarianceImproved {
				// The node is a better choice since it brings a smaller variance increase to the selected set
				isBetterChoice = true
				logrus.Debugf("%ssmallest variance in the selected set: %s", reasonPreamble, node.res.Name())
			} else if selectedVarianceDegraded {
				// At this point, we no longer take a node that would degrade the variance score
				continue
			} else if !isLastRound && (node.sumAffinityScore < bestNode.sumAffinityScore || node.minAffinityScore < bestNode.minAffinityScore) {
				// By now, there is no change to the selected variance score
				// If it's not the last round, we pick nodes with weaker aggregated affinity first,
				// which is more hopeful in reducing the final variance in the end.
				isBetterChoice = true
				logrus.Debugf("%ssum and min affinity score decrease: %s", reasonPreamble, node.res.Name())
			} else if isLastRound && unselectedVarianceImproved {
				// This is to prevent the algorithm from always selecting nodes with smaller affinity scores,
				// which could lead to undesired increase of variance in the unselected nodes.
				isBetterChoice = true
				logrus.Debugf("%ssmallest variance in the unselected set: %s", reasonPreamble, node.res.Name())
			}

			if isBetterChoice {
				bestSelectedVariance = selectedVariance
				bestUnselectedVariance = unselectedVariance
				bestNode = node
				bestIndex = i
			}
		}

		// Updates the distribution with the best node's affinities
		for i, conn := range bestNode.systemPortAffinities {
			distribution[i] += conn
			aggregatedAffinities[i] -= conn
		}
		for _, port := range bestNode.portsInAffinity {
			selectionAffinities[port] += 1
		}
		logrus.Infof("%swhen searching for the %d-th node, the best node is %v (ports in affinity: %v)",
			reqPreamble, len(selection.resources), bestNode.res.Name(), bestNode.portsInAffinity)
		selection.Add(bestNode.res)
		// Removes the selected node from sourceNodes to prevent reselection
		sourceNodes = append(sourceNodes[:bestIndex], sourceNodes[bestIndex+1:]...)
	}
	logrus.Infof("%saffinity summary: %v", reqPreamble, selectionAffinities)
	return selection, selectionAffinities
}

// FindUngroupedScheduling finds candidate resources for each Request in the requests map. This assumes that Request's
// results are distinct subsets of resources of the given collection and that the requests will not grant the same
// resource twice (e.g. if 2 coordinator node requests were assigned to the same node). This method is intended
// for system and coordinator node matching and is distinct from the "grouped" GroupRequests which are used to search
// over node groups.
//
// This method returns candidates in the case where there are not enough resources to satisfy the request so
// that the caller may reserve the partially available resources if it chooses. In the case of a match, an exact match
// will be returned.
// returns non-empty errors if not all the requests could be granted
func (c *Collection) FindUngroupedScheduling(requests map[string]Request) (map[string]Grant, []error) {
	cndGrants := map[string]Grant{}
	var errors []error
	for _, rq := range requests {
		var resourceIds []string
		var hasError bool

		// filter by properties
		resourceCnds0 := c.FilterByProps(rq.Filter.properties, rq.Filter.antiAffinities)
		if resourceCnds0.Size() < rq.Count {
			errors = append(errors, fmt.Errorf(
				"requested %d %s%s but only %d available with matching properties",
				rq.Count, rq.type_, PrettyProps(rq.Filter.Properties()), resourceCnds0.Size()))
			hasError = true
		}
		// filter by capacity
		resourceCnds1 := resourceCnds0.FilterByCapacity(rq.subresources)
		if resourceCnds1.Size() < rq.Count && !hasError {
			remainingSubresourceSummary := fmtMaxCpuMem(resourceCnds0, rq)
			if remainingSubresourceSummary != "" {
				remainingSubresourceSummary = fmt.Sprintf("%d available with insufficient capacity %s",
					resourceCnds0.Size()-resourceCnds1.Size(), remainingSubresourceSummary)
			} else {
				remainingSubresourceSummary = fmt.Sprintf("%d available with enough capacity", resourceCnds1.Size())
			}
			errors = append(errors, fmt.Errorf(
				"requested %d %s%s%s but %s",
				rq.Count, rq.type_, PrettyProps(rq.Filter.Properties()),
				rq.Filter.Subresources().Pretty(), remainingSubresourceSummary))
			hasError = true
		}

		// of the resources matching the properties + capacity, check which have enough available subresources
		remainingSR := map[string]int{}
		for _, r := range resourceCnds1.resources {
			if r.HasAvailable(rq.Subresources(), resourceCnds0.ignoreKeys) {
				resourceIds = append(resourceIds, r.id)
				// Group request Id (ng-primary-0, Activation, etc) is passed in to help identifying request type.
				// Passing an empty string given not applicable here. Can use refactoring to make this more clear.
				rs, err := rq.scoreMatchedResource("", r).getOnlyScore()
				if err != nil {
					errors = append(errors, fmt.Errorf("error getting resource score for resource %s: %s", r.id, err))
					hasError = true
				}
				remainingSR[r.id] = rs
			}
		}
		if len(resourceIds) < rq.Count && !hasError {
			errors = append(errors, fmt.Errorf(
				"requested %d %s%s%s but only %d has enough available resources from %d candidates",
				rq.Count, rq.type_, PrettyProps(rq.Filter.Properties()), rq.Subresources().Pretty(),
				len(resourceIds), resourceCnds1.Size(),
			))
			hasError = true
		}

		stampSystemCount := c.GetStampSystemCount(resourceIds)
		sort.Slice(resourceIds, func(i, j int) bool {
			res1, _ := c.Get(resourceIds[i])
			res2, _ := c.Get(resourceIds[j])
			// prefer local session's resource
			if ns := rq.GetSession(); ns != "" {
				iMatches := res1.GetNamespace() == ns
				jMatches := res2.GetNamespace() == ns
				if iMatches != jMatches {
					return iMatches
				}
			}
			// prefer resources with higher remaining subresource ratio
			if remainingSR[resourceIds[i]] != remainingSR[resourceIds[j]] {
				return remainingSR[resourceIds[i]] > remainingSR[resourceIds[j]]
			}
			health1 := res1.IsFullyHealthy()
			health2 := res2.IsFullyHealthy()
			g1 := res1.GetStampIndex()
			g2 := res2.GetStampIndex()
			// for different groups, sort by group size/group name
			// for same group, sort by health/res name
			if g1 != g2 {
				if stampSystemCount[g1] != stampSystemCount[g2] {
					return stampSystemCount[g1] > stampSystemCount[g2]
				}
				return g1 < g2
			} else if health1 != health2 {
				return health1
			}
			return res1.Name() < res2.Name()
		})
		if len(resourceIds) >= rq.Count {
			resourceIds = resourceIds[:rq.Count]

			perStampSystems := map[string][]string{}
			for _, resourceId := range resourceIds {
				res, _ := c.Get(resourceId)
				perStampSystems[res.GetStampIndex()] = append(perStampSystems[res.GetStampIndex()], resourceId)
			}

			// sort keys based on system count in stamp
			// To facilitate BR scheduling to always build the subtree with fewer systems first to
			// ensure consistent result and layer count, sort the resourceIds with smaller group size first.
			stamps := SortedKeysByValSize(perStampSystems, true)
			resourceIds = nil
			for _, stamp := range stamps {
				for _, systemName := range perStampSystems[stamp] {
					resourceIds = append(resourceIds, systemName)
				}
			}
		}
		cndGrants[rq.Id] = Grant{
			Request:           rq,
			ResourceIds:       resourceIds,
			Subresources:      rq.subresources,
			SubresourceRanges: EnsureMap(SubresourceRanges{}),
			Annotations:       EnsureMap(map[string]string{}),
		}
	}

	return cndGrants, errors
}

type resourceScore struct {
	Resource
	scores             []int
	indexedResOverride map[string]Subresources
}

func newResourceScore(r Resource, indexedResOverride map[string]Subresources, scores ...int) resourceScore {
	return resourceScore{
		Resource:           r,
		scores:             scores,
		indexedResOverride: indexedResOverride,
	}
}

func (rs resourceScore) isValidScore() bool {
	if len(rs.scores) == 0 {
		return false
	}

	for _, score := range rs.scores {
		if score < 0 {
			return false
		}
	}
	return true
}

// There are discussions that we will be refactoring the scoring & sorting algorithm for resources into pod-type and job-mode agnostic
// After which is done the getOnlyScore function will be discarded, so TODO: remove this function, cleanup its usages when refactoring is complete
func (rs resourceScore) getOnlyScore() (int, error) {
	if len(rs.scores) != 1 {
		return -1, fmt.Errorf("expected only 1 score, getting %d. Resource: %s, scores: %v", len(rs.scores), rs.id, rs.scores)
	}

	return rs.scores[0], nil
}

func (rs resourceScore) Compare(other resourceScore) (bool, error) {
	if len(rs.scores) != len(other.scores) {
		return false, fmt.Errorf("unexpected length of scores. Resource %s: %v,\nResource %s: %v", rs.id, rs.scores, other.id, other.scores)
	}
	for i := 0; i < len(rs.scores); i++ {
		if rs.scores[i] != other.scores[i] {
			return rs.scores[i] > other.scores[i], nil
		}
	}
	return strings.Compare(rs.id, other.id) < 0, nil
}

// Find the best resource grant for a given Request indexed by rqIndex in a groupRequest.
// using a greedy algorithm. Returns the exact match and the score of the match.
// For a given request, ResourceIds may be included more than once
// indicating the Request is allocated multiple times to the same resource.
//
// In addition to the grant, a list of allocated resources that are used in the grant will be returned.
// An error is returned if the request cannot be satisfied.
// Pass in groupReq+index only to help err msg
func grantForGroupSubRequest(
	rcopy *Collection,
	rq Request,
	groupRq *GroupRequest,
	rqIndex int,
) (*Grant, error) {
	candidates := rcopy.FilterByProps(rq.Filter.properties, rq.Filter.antiAffinities)
	if candidates.Size() == 0 {
		return nil, fmt.Errorf(
			"group request[%s:%s], props%s, anti-affinity[%v], matched no %s resources",
			groupRq.Id, rq.Id, PrettyProps(rq.Properties()), rq.antiAffinities, rq.type_,
		)
	}
	requestedSub := rq.Subresources().Copy()
	// greedily allocate by calculating the score of each candidate and choosing the highest positive scores
	// in each loop, iterate through every candidate once only to spread out the allocation
	var resourceIds []string
	resourceOverride := map[string]Subresources{}
	remaining := rq.Count
	groupRqId := groupRq.Id

	for remaining > 0 {
		var rss []resourceScore
		for _, r := range candidates.resources {
			rs := rq.scoreMatchedResource(groupRqId, r)
			// only consider the node if score is positive
			// - a negative score in remaining ratio means the node does not have enough resource for scheduling the base need of the request
			// - a negative score in indexed res score means the node does not have NIC (cs-port) registered as subresource
			// - affinity score cannot be negative
			if rs.isValidScore() {
				rss = append(rss, rs)
			}
		}

		if len(rss) == 0 {
			// add the partial grant for reporting purposes
			grant := Grant{Request: rq, ResourceIds: resourceIds, Subresources: rq.Subresources().Copy()}
			// include previously placed resources in error message
			grpRqPlacedStr := groupSchedulingMsg(groupRq, rqIndex)
			// include max size of subresource capacity in error message
			grpMaxCpuMemStr := fmtMaxCpuMem(candidates, rq)
			if grpMaxCpuMemStr != "" {
				grpMaxCpuMemStr = fmt.Sprintf("%d nodes%s%s ",
					candidates.Size(), PrettyProps(rq.properties), grpMaxCpuMemStr)
			}
			id := rq.Id
			return &grant, fmt.Errorf(
				"%scould only schedule %s%d of %d %s%s",
				grpMaxCpuMemStr,
				grpRqPlacedStr,
				rq.Count-remaining, rq.Count,
				id, rq.Subresources().Pretty(),
			)
		}

		var sortingErr error
		sort.Slice(rss, func(i, j int) bool {
			res, err := rss[i].Compare(rss[j])
			if err != nil {
				sortingErr = err
				return false
			}
			return res
		})

		if sortingErr != nil {
			return nil, fmt.Errorf("unable to sort candidate resources by score: %s", sortingErr.Error())
		}

		for _, rs := range rss {
			// reserve requested resource + optional anti-affinities
			rs.AllocateReq(rq, rs.indexedResOverride)
			// assign indexed rs override if exists
			if len(rs.indexedResOverride) > 0 {
				resourceOverride[rs.id] = EnsureMap(resourceOverride[rs.id])
				for indexedRs := range rs.indexedResOverride {
					delete(requestedSub, indexedRs)
					for k, v := range rs.indexedResOverride[indexedRs] {
						resourceOverride[rs.id][k] = v
					}
				}
			}
			resourceIds = append(resourceIds, rs.id)
			remaining--
			if remaining == 0 {
				break
			}
		}
	}
	return &Grant{
		Request:             rq,
		ResourceIds:         resourceIds,
		Subresources:        requestedSub,
		SubresourceOverride: resourceOverride,
		Annotations:         EnsureMap(map[string]string{}),
	}, nil
}

// FindGroupScheduling matches each Request in the GroupRequest, find the best resource grant for the Request
// using a greedy algorithm. Returns the exact match and the score of the match. For a given request, ResourceIds may be
// included more than once indicating the Request is allocated multiple times to the same resource.
// This is the core algorithm for determining if a node group matches a group request.
// An error is returned if the request cannot be satisfied.
func (c *Collection) FindGroupScheduling(groupRq GroupRequest) (GroupGrant, error) {
	// greedy match, with determinism via ordering of the request keys.
	requestIds := groupRq.getSortedRequestIds()
	grants := map[string]Grant{}
	rcopy := c.Copy() // copy required since algorithm allocates over the copy during greedy matching
	var rq Request
	for index, rqId := range requestIds {
		rq = groupRq.Requests[rqId]
		g, err := grantForGroupSubRequest(rcopy, rq, &groupRq, index)
		if g != nil {
			grants[rq.Id] = *g
		}
		if err != nil {
			return GroupGrant{Grants: grants}, err
		}
	}
	// return cost score for searching purpose
	costScore := 0
	for _, r := range rcopy.List() {
		costScore += r.WeightedRemaining()
		if ns := rq.GetSession(); ns != "" && r.GetNamespace() != ns {
			costScore += NonLocalSessionPenalty
		}
	}

	for k := range grants {
		// clean up job-specific auxiliary usage limits
		for subresK := range grants[k].Subresources {
			if strings.HasPrefix(subresK, AuxiliarySubresPrefix) {
				delete(grants[k].Subresources, subresK)
			}
		}

		// Clean up auxiliary usage limits for each node such that the next pod types are not blocked
		// for evaluation on the same node. For example, we allow kvss and act pod types to co-exist on
		// an AX node.
		for nodeK := range grants[k].SubresourceOverride {
			for subresOverrideK := range grants[k].SubresourceOverride[nodeK] {
				if strings.HasPrefix(subresOverrideK, AuxiliarySubresPrefix) {
					delete(grants[k].SubresourceOverride[nodeK], subresOverrideK)
				}
			}
		}
	}
	return GroupGrant{Request: groupRq, Grants: grants, cost: costScore}, nil
}

func groupSchedulingMsg(g *GroupRequest, index int) string {
	if index <= 0 {
		return ""
	}
	requestIds := g.getSortedRequestIds()
	sb := strings.Builder{}
	for j := 0; j < index; j++ {
		placedRqId := requestIds[j]
		placedRq := g.Requests[placedRqId]
		sb.WriteString(strconv.Itoa(placedRq.Count))
		sb.WriteString(" of ")
		sb.WriteString(strconv.Itoa(placedRq.Count))
		sb.WriteString(" ")
		sb.WriteString(placedRqId)
		sb.WriteString(placedRq.Subresources().Pretty())
		sb.WriteString(", ")
	}
	return sb.String()
}

func (c *Collection) UpdateProp(rid string, k, v string) {
	r, ok := c.Get(rid)
	if !ok {
		return
	}
	if oldV, hasK := r.properties[k]; !hasK || oldV != v {
		r.properties[k] = v
		c.index = nil
	}
}

func (c *Collection) UpdateGroupProp(group, k, v string) {
	g, ok := c.groups[group]
	if !ok {
		return
	}
	if oldV, hasK := g.properties[k]; !hasK || oldV != v {
		g.properties[k] = v
		c.index = nil
	}
}

func (c *Collection) ResetAllocations() {
	for _, r := range c.resources {
		for _, k := range Keys(r.allocated) {
			delete(r.allocated, k)
		}
	}
}

func (c *Collection) GetTotalAxMemory() int {
	aggrRemainingMem := 0

	for _, res := range c.resources {
		aggrRemainingMem += res.GetSubresourceCountCapacity(MemSubresource)
	}
	return aggrRemainingMem
}

// Print the current remaining memory for AX nodes given a list of node ids
// Only log for inference job sharing enabled jobs
func (c *Collection) LogRemainingMemoryForAxNodes(label string, nodeList []string, isInferenceJobWithSharing bool) {
	if !isInferenceJobWithSharing || len(nodeList) == 0 {
		return
	}

	var remainingMemoryMessage string

	for _, nodeName := range nodeList {
		if res, ok := c.Get(nodeName); ok && res.IsAxNode() {
			remainingMemoryMessage += fmt.Sprintf(" [Node: %s, Memory Remaining: %d MiB] ",
				res.Name(), res.GetSubresourceCountAvailable(MemSubresource))
		}
	}
	logrus.Infof("[%s] Remaining memory for ax nodes: %s", label, remainingMemoryMessage)
}

// Summarizes the max CPU, MEM capacity of resources in `candidates` for human-readable error message
func fmtMaxCpuMem(candidates *Collection, rq Request) string {
	if rq.type_ != v1.NodeResourceType ||
		(rq.Subresources().Cpu() == 0 && rq.Subresources().Mem() == 0) {
		return ""
	}
	srMax := Subresources{}
	for _, r := range candidates.List() {
		if srMax.Cpu() < r.Subresources().Cpu() {
			srMax[CPUSubresource] = r.Subresources().Cpu()
		}
		if srMax.Mem() < r.Subresources().Mem() {
			srMax[MemSubresource] = r.Subresources().Mem()
		}
	}
	return srMax.Pretty()
}

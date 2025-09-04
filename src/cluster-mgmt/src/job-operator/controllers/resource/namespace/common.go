package namespace

import (
	"fmt"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

var mgmtNodeFilter = resource.NewFilter(resource.NodeType, map[string][]string{resource.RoleMgmtPropKey: {""}}, map[string]int{})
var crdNodeFilter = resource.NewFilter(resource.NodeType, map[string][]string{resource.RoleCrdPropKey: {""}}, map[string]int{})
var axNodeFilter = resource.NewFilter(resource.NodeType, map[string][]string{resource.RoleAxPropKey: {""}}, map[string]int{})
var ixNodeFilter = resource.NewFilter(resource.NodeType, map[string][]string{resource.RoleIxPropKey: {""}}, map[string]int{})

// newCollectionForNsMgmt takes the job-operator's global Collection object and filters for system/nodegroup/node
// resources, converting Nodegroups from a Group to a Resource to facilitate filtering on groups
func newCollectionForNsMgmt(c *resource.Collection) *resource.Collection {
	rv := resource.Collection{}

	for _, ng := range c.ListGroups() {
		rv.Add(resource.NewResource(resource.NodegroupType, ng.Name(), removeNsProp(ng.Props()), nil, nil))
	}

	for _, sys := range c.Filter(resource.NewFilter(resource.SystemType, nil, nil)).List() {
		rv.Add(resource.NewResource(resource.SystemType, sys.Name(), removeNsProp(sys.GetProps()), sys.Subresources(), nil)) // Deep copy
	}

	filters := []resource.Filter{crdNodeFilter, axNodeFilter, ixNodeFilter}
	// In the event where mgmt/coord separation is enforced, we add the management nodes to the collection so we can
	// provide better error messages when users explicitly provide the management node names through debug-update.
	if wsapisv1.ManagementSeparationEnforced {
		filters = []resource.Filter{mgmtNodeFilter, crdNodeFilter, axNodeFilter, ixNodeFilter}
	}
	for _, f := range filters {
		nodes := c.Filter(f).List()
		for _, n := range nodes {
			// remove prop inheritance, squash group props
			props := common.EnsureMap(common.CopyMap(n.Group().Props()))
			for k, v := range n.GetProps() {
				props[k] = v
			}
			res := resource.NewResource(resource.NodeType, n.Name(), removeNsProp(props), n.Subresources(), n.SubresourceRanges()) // Deep copy
			rv.Add(res)
		}
	}

	return &rv
}

// MapToMetricResourceType converts internal resource types to metric-friendly names.
// For nodes, it determines the specific node type (coordinator, activation, etc.)
func MapToMetricResourceType(rid string, collection *resource.Collection, resourceType string) string {
	if resourceType == resource.NodeType {
		// Return specific node types
		if res, ok := collection.Get(rid); ok {
			return GetNodeMetricType(res)
		}
	}
	// For non-node resources (systems, nodegroups), return the type as-is
	return resourceType
}

func GetNodeMetricType(res resource.Resource) string {
	if res.IsCrdNode() || res.IsMgmtNode() {
		return resource.RoleCrdPropKey
	} else if res.IsAxNode() {
		return resource.RoleAxPropKey
	} else if res.IsIxNode() {
		return resource.RoleIxPropKey
	} else if res.IsBrNode() {
		return resource.RoleBRPropKey
	}
	return "unknown-node"
}

func removeNsProp(m map[string]string) map[string]string {
	delete(m, common.NamespaceKey)
	return m
}

func fmtMem(mem int) string {
	memUnit := "Mi"
	memGi := mem >> 10
	if memGi > 0 {
		mem = memGi
		memUnit = "Gi"
	}
	return fmt.Sprintf("%d%s", mem, memUnit)
}

// getDiff returns lists of elements removed, added from before->after. Assumes before/after lists have unique elements
func getDiff(before, after []string) ([]string, []string) {
	removed, added := common.MapBool(before), common.MapBool(after)
	for _, val := range after {
		if removed[val] {
			delete(removed, val)
		}
	}
	for _, val := range before {
		if added[val] {
			delete(added, val)
		}
	}
	return common.Keys(removed), common.Keys(added)
}

func getDuplicates(vals []string) []string {
	seen := map[string]bool{}
	duplicates := map[string]bool{}
	for _, v := range vals {
		if seen[v] {
			duplicates[v] = true
		}
		seen[v] = true
	}
	return common.Keys(duplicates)
}

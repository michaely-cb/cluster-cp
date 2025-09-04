package namespace

import "cerebras.com/job-operator/common/resource"

// TODO: return a list of the running job ids rather than yes/no allocated
type ResourceStatus interface {
	// IsInUse returns true if a job is using this resource
	IsInUse(resourceId string) bool
}

type placeholderResourceStatus struct{}

func (placeholderResourceStatus) IsInUse(resourceId string) bool {
	return false
}

type resourceStatus struct {
	col *resource.Collection
}

func NewResourceStatus(c *resource.Collection) ResourceStatus {
	return resourceStatus{col: c}
}

func (c resourceStatus) IsInUse(resourceId string) bool {
	rtype, rname := resource.MustRIDToTypeName(resourceId)
	if rtype == resource.NodegroupType {
		filter := resource.NewFilter(resource.NodeType, map[string][]string{resource.GroupPropertyKey: {rname}}, nil)
		return !c.col.Filter(filter).IsUnallocated()
	}
	r, ok := c.col.Get(resourceId)
	return ok && r.HasAllocation()
}

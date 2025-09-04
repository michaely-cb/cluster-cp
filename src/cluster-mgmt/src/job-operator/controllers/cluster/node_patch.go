package cluster

import (
	"fmt"
	"strconv"
	"strings"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common/resource"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"

	"cerebras.com/job-operator/common"
)

type jsonPatchStr struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value,omitempty"`
}

func (j jsonPatchStr) String() string {
	return fmt.Sprintf("%s:%s:%v", j.Op, j.Path, j.Value)
}

type jsonPatchInt struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value *int32 `json:"value,omitempty"`
}

func (j jsonPatchInt) String() string {
	v := ""
	if j.Value != nil {
		v = strconv.Itoa(int(*j.Value))
	}
	return fmt.Sprintf("%s:%s:%s", j.Op, j.Path, v)
}

type nodePatches struct {
	node   []jsonPatchStr
	status []jsonPatchInt
}

func (n nodePatches) String() string {
	return fmt.Sprintf("node[%s],status[%s]",
		strings.Join(common.Map(func(j jsonPatchStr) string { return j.String() }, n.node), ","),
		strings.Join(common.Map(func(j jsonPatchInt) string { return j.String() }, n.status), ","))
}

func (n nodePatches) empty() bool {
	return len(n.node) == len(n.status) && len(n.status) == 0
}

type nodePatcher struct {
	logger logr.Logger
}

func newNodePatcher(logger logr.Logger) nodePatcher {
	np := nodePatcher{
		logger: logger,
	}
	return np
}

// getUpdatePatches returns patches to move k8sNode to state described in node w.r.t role labels and CSPort allocation/cap
func (n nodePatcher) getUpdatePatches(cfgNode *common.Node, k8sNode *v1.Node, namespace string) nodePatches {
	var patches nodePatches
	requireLabels := map[string]string{
		common.NamespaceLabelKey: namespace,
	}
	for _, k := range cfgNode.ExpandedRoles {
		requireLabels[k.AsNodeLabel()] = ""
	}
	group, hasGroup := cfgNode.Properties[resource.GroupPropertyKey]
	originGroup, hasOriginGroup := cfgNode.Properties[common.OriginGroupKey]
	if !hasGroup && hasOriginGroup {
		group = originGroup
		hasGroup = true
	}
	if hasGroup && cfgNode.GetRole() != string(common.RoleBroadcastReduce) {
		requireLabels[common.GroupLabelKey] = group
	}
	if wsapisv1.IsMultiMgmt &&
		(cfgNode.GetRole() == string(common.RoleManagement) || cfgNode.GetRole() == string(common.RoleCoordinator)) {
		requireLabels[common.CephAccessibleKey] = ""
	}

	for k, v := range k8sNode.Labels {
		if strings.HasPrefix(k, "k8s.cerebras.com/node-role-") {
			if _, hasRequired := requireLabels[k]; hasRequired {
				delete(requireLabels, k)
			} else {
				patches.node = append(patches.node, jsonPatchStr{
					Op:   "remove",
					Path: fmt.Sprintf("/metadata/labels/%s", strings.Replace(k, "/", "~1", -1)),
				})
			}
		} else if k == common.GroupLabelKey {
			requireGroupVal, requireGroup := requireLabels[k]
			if requireGroup && requireGroupVal != v {
				patches.node = append(patches.node, jsonPatchStr{
					Op:    "replace",
					Path:  fmt.Sprintf("/metadata/labels/%s", strings.Replace(k, "/", "~1", -1)),
					Value: requireGroupVal,
				})
				delete(requireLabels, k)
			} else if !requireGroup {
				patches.node = append(patches.node, jsonPatchStr{
					Op:   "remove",
					Path: fmt.Sprintf("/metadata/labels/%s", strings.Replace(k, "/", "~1", -1)),
				})
			} else {
				delete(requireLabels, k)
			}
		} else if k == common.NamespaceLabelKey {
			requireGroupVal, requireGroup := requireLabels[k]
			if requireGroup && requireGroupVal != v {
				patches.node = append(patches.node, jsonPatchStr{
					Op:    "replace",
					Path:  fmt.Sprintf("/metadata/labels/%s", strings.Replace(k, "/", "~1", -1)),
					Value: requireGroupVal,
				})
			}
			delete(requireLabels, k)
		}
	}

	for k, v := range requireLabels {
		patches.node = append(patches.node, jsonPatchStr{
			Op:    "add",
			Path:  fmt.Sprintf("/metadata/labels/%s", strings.Replace(k, "/", "~1", -1)),
			Value: v,
		})
	}

	patches.status = append(patches.status, n.getNodeResourcePatches("capacity", cfgNode, k8sNode.Status.Capacity)...)
	patches.status = append(patches.status, n.getNodeResourcePatches("allocatable", cfgNode, k8sNode.Status.Allocatable)...)

	return patches
}

// getNodeResourcePatches gets patches to add csPort resources to BR and any type nodes. This is done on the node's
// status subresource and is therefore PATCH'ed at a different endpoint than the label updates.
func (n nodePatcher) getNodeResourcePatches(capOrAlloc string, cfg *common.Node, rl v1.ResourceList) []jsonPatchInt {
	required := map[string]int64{}

	var patches []jsonPatchInt
	for resource, quantity := range rl {
		if strings.HasPrefix(resource.String(), "cerebras.com/") {
			requiredQuantity, isRequired := required[resource.String()]
			if !isRequired {
				patches = append(patches, jsonPatchInt{
					Op:   "remove",
					Path: fmt.Sprintf("/status/%s/%s", capOrAlloc, strings.Replace(resource.String(), "/", "~1", -1)),
				})
			} else if isRequired && requiredQuantity != quantity.Value() {
				patches = append(patches, jsonPatchInt{
					Op:    "replace",
					Path:  fmt.Sprintf("/status/%s/%s", capOrAlloc, strings.Replace(resource.String(), "/", "~1", -1)),
					Value: common.Pointer(int32(requiredQuantity)),
				})
			}
			if isRequired {
				delete(required, resource.String())
			}
		}
	}
	for resource, count := range required {
		patches = append(patches, jsonPatchInt{
			Op:    "add",
			Path:  fmt.Sprintf("/status/%s/%s", capOrAlloc, strings.Replace(resource, "/", "~1", -1)),
			Value: common.Pointer(int32(count)),
		})
	}
	return patches
}

// getRemovePatches returns patches to remove node labels and resources
func getRemovePatches(k8sNode *v1.Node) nodePatches {
	var patches nodePatches
	for k, _ := range k8sNode.Labels {
		if strings.HasPrefix(k, "k8s.cerebras.com/node-") {
			patches.node = append(patches.node, jsonPatchStr{
				Op:   "remove",
				Path: fmt.Sprintf("/metadata/labels/%s", strings.Replace(k, "/", "~1", -1)),
			})
		}
	}

	for resource := range k8sNode.Status.Capacity {
		if strings.HasPrefix(resource.String(), "cerebras.com/") {
			patches.status = append(patches.status, jsonPatchInt{
				Op:   "remove",
				Path: fmt.Sprintf("/status/capacity/%s", strings.Replace(resource.String(), "/", "~1", -1)),
			})
		}
	}

	for resource := range k8sNode.Status.Allocatable {
		if strings.HasPrefix(resource.String(), "cerebras.com/") {
			patches.status = append(patches.status, jsonPatchInt{
				Op:   "remove",
				Path: fmt.Sprintf("/status/allocatable/%s", strings.Replace(resource.String(), "/", "~1", -1)),
			})
		}
	}

	return patches
}

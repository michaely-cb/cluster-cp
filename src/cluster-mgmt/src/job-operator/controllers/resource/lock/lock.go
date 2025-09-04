package lock

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/maps"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"

	"cerebras.com/job-operator/common/resource"
)

const (
	brSkipGroupNotSet = -1
)

// lock is cached view of rlv1.ResourceLock
type lock struct {
	// Name is in format of namespace/name
	Name string
	// CreateTime is epoch millis and only used for ordering
	CreateTime int64
	// QueueName is the name of the queue that the lock is in
	QueueName string

	// ResourceRequests contains ungrouped resource requests. Key is Request.Id
	ResourceRequests map[string]resource.Request

	// ResourceGrants contains ungrouped resource grants. Key is Request.Id
	ResourceGrants map[string]resource.Grant

	// GroupRequests are all the group requests where GroupRequestId -> GroupRequest
	GroupRequests map[string]resource.GroupRequest

	// User specified BR tree policy to construct the BR tree.
	SpecifiedBrTreePolicy string

	// SkipBr to skip br requests
	SkipBr bool
	// BrLogicalTree for logical tree of BR
	BrLogicalTree common.BrLogicalTree
	// BrLogicalTree for logical tree policy that is used to construct the final BR tree for the lock.
	BrLogicalTreePolicy string
	// BrGroupRequest for requests of BR
	BrGroupRequest resource.GroupRequest
	// BrGroupGrant -> BrGroupRequest
	BrGroupGrant resource.GroupGrant

	// ChfGroupRequests for requests of chiefs
	ChfGroupRequests []resource.GroupRequest
	// ChfGroupGrants -> ChfGroupRequests
	ChfGroupGrants []resource.GroupGrant

	// ActGroupRequests for requests of activations
	ActGroupRequests []resource.GroupRequest
	// ActGroupGrants -> ActGroupRequests
	ActGroupGrants []resource.GroupGrant

	// KvssGroupRequests for request of key-value caches
	KvssGroupRequests []resource.GroupRequest
	// KvssGroupGrants -> KvssGroupRequests
	KvssGroupGrants []resource.GroupGrant

	// SWDriverGroupRequests for request of SWDriver
	SWDriverGroupRequests []resource.GroupRequest
	// SWDriverGroupGrants -> SWDriverGroupRequests
	SWDriverGroupGrants []resource.GroupGrant

	// WgtCmdGroupRequests for requests of weight and command
	// This is used in a special case when only AX nodes are used for used from runtime servers.
	WgtCmdGroupRequests []resource.GroupRequest
	// WgtCmdGroupGrants -> WgtCmdGroupRequests
	WgtCmdGroupGrants []resource.GroupGrant

	// GroupRequestLowerBounds are the upper bounds for each group request. Key is request id.
	GroupRequestUpperBounds map[string]resource.Subresources

	// GroupRequestId -> GroupGrants
	GroupGrants map[string][]resource.GroupGrant

	// PopGroupTypes contains the supported populated nodegroup types which are allowed to use
	PopGroupTypes []string

	// PopGroupCount is the number of populated nodegroup requests of a lock
	PopGroupCount int

	// Reqeust type -> res, e.g. Activation => mem: 5G
	GroupGrantUpperBounds map[string]resource.Subresources
	// Reserved mem overhead for node hetero assign / RT max limit only
	RtNodeMaxLimitReservedMemGB    int
	RtNodeMaxLimitReservedMemRatio float64

	// Activations are required to talk to specific domains in AX scheduling (V2 networking specific).
	// Indices of "ActTargets" represent the activation pod index.
	// Values of "ActTargets[i]" represent the CSX and domains that the i-th pod is required to talk to.
	// We will get this information from the lock annotation, which was inherited from wsjob annotation.
	// In rel-2.3, each activation pod only talks to one CSX domain. The plural form is only for forward
	// compatibility.
	ActTargets      []common.WioTarget
	KvssTargets     []common.WioTarget
	SWDriverTargets []common.WioTarget

	// Priority of the lock
	Priority int

	// WorkflowId of the lock
	WorkflowId string

	// ParentLockNsName is the parent granted/reserving lock in the same workflow
	ParentLockNsName     string
	ParentLockCreateTime int64
	// ChildLockNsName is the lock that where the reservation is created from
	ChildLockNsName string
	// SubsidiaryLockNsName is the lock that requires to be granted together
	SubsidiaryLockNsName string

	// Allocations from last grant result
	Allocations map[string]resource.SubresourceAllocation
	// Grant status from last grant result
	granted bool

	// If this is not nil and lock could not be granted after this deadline, the lock
	// will be rejected.
	GrantDeadline *time.Time

	Obj *rlv1.ResourceLock

	// System names that are granted for this lock, stored in the order as they appear in the leaf level of
	// the BR tree. The order of the system names are important to make sure the systems in the BR tree and
	// the systems used by ACT servers have the same mapping. This allows different runs with different sets
	// of systems to produce the same loss value.
	OrderedSystems []string

	// LinearMemory holds both the global range and any per-replica overrides
	LinearMemoryConfigs map[commonv1.ReplicaType]common.LinearMemoryConfig

	// AggrAxMemLimit represents the aggregated AX memory that can be utilized for this job
	// This is utilized in context of job sharing, where each job gets an aggregated memory limit
	// based on the portion of systems requested.
	AggrAxMemLimit int
}

type PopGroupOptions struct {
	popNgTypes []string
	popNgCount int
}

// Less returns true if l is less than other using CreateTime (when job priority scheduling is enabled)
// and Name as tie breakers
func (l *lock) Less(other *lock) bool {
	// compare by Priority first
	if l.Priority != other.Priority {
		return l.Priority < other.Priority
	}

	// If both locks have the same priority, then prioritize the workflow which has a longer age
	if l.ParentLockNsName != "" && other.ParentLockNsName != "" && l.ParentLockCreateTime != other.ParentLockCreateTime {
		return l.ParentLockCreateTime < other.ParentLockCreateTime
	}

	// If priorities are equal, compare by CreateTime
	if l.CreateTime != other.CreateTime {
		return l.CreateTime < other.CreateTime
	}

	// If CreateTime is also equal, compare by Name
	return l.Name < other.Name
}

// copy request only for the purpose of not overriding grant result
func (l *lock) Copy() *lock {
	return &lock{
		Name:                           l.Name,
		CreateTime:                     l.CreateTime,
		QueueName:                      l.QueueName,
		ResourceRequests:               l.ResourceRequests,
		GroupRequests:                  l.GroupRequests,
		SkipBr:                         l.SkipBr,
		SpecifiedBrTreePolicy:          l.SpecifiedBrTreePolicy,
		BrGroupRequest:                 l.BrGroupRequest,
		GroupRequestUpperBounds:        l.GroupRequestUpperBounds,
		PopGroupTypes:                  l.PopGroupTypes,
		PopGroupCount:                  l.PopGroupCount,
		RtNodeMaxLimitReservedMemGB:    l.RtNodeMaxLimitReservedMemGB,
		RtNodeMaxLimitReservedMemRatio: l.RtNodeMaxLimitReservedMemRatio,
		ActTargets:                     l.ActTargets,
		KvssTargets:                    l.KvssTargets,
		SWDriverTargets:                l.SWDriverTargets,
		Priority:                       l.Priority,
		WorkflowId:                     l.WorkflowId,
		ParentLockNsName:               l.ParentLockNsName,
		ParentLockCreateTime:           l.ParentLockCreateTime,
		ChildLockNsName:                l.ChildLockNsName,
		GrantDeadline:                  l.GrantDeadline,
		Obj:                            l.Obj,
	}
}

// granted lock or reserving only lock should be fully allocated
func (l *lock) RequireFullAllocation() bool {
	return l.IsGranted() || l.ReservingOnly()
}

// lock grant status
func (l *lock) IsGranted() bool {
	if l.Obj != nil && l.Obj.IsGranted() {
		return true
	}
	if l.ReservingOnly() {
		return false
	}
	return l.granted
}

func (l *lock) HadFailed() bool {
	return l.Obj.HadFailed()
}

func (l *lock) HasReservation() bool {
	return l.Obj.HasReservation()
}

func (l *lock) ReservingOnly() bool {
	return l.QueueName == rlv1.ReservationQueueName
}

func (l *lock) IsPrioritized() bool {
	return l.QueueName == rlv1.PrioritizedCompileQueueName || l.QueueName == rlv1.PrioritizedExecuteQueueName
}

func (l *lock) SkipBR() bool {
	return l.SkipBr || l.Obj.Annotations[wsapisv1.JobSkipBRKey] == "true"
}

func (l *lock) getAnnotations() map[string]string {
	return l.Obj.Annotations
}

func (l *lock) getLabels() map[string]string {
	return l.Obj.Labels
}

func (l *lock) getSpecifiedBrTreePolicy() string {
	return l.Obj.Annotations[wsapisv1.BrTreePolicyKey]
}

func (l *lock) enablePartialSystems() bool {
	if !wsapisv1.EnablePartialSystemMode {
		return false
	}
	return l.Obj.Labels[wsapisv1.EnablePartialSystemKey] == strconv.FormatBool(true) ||
		l.Obj.Annotations[wsapisv1.EnablePartialSystemKey] == strconv.FormatBool(true)
}

func (l *lock) enableBalancedTree() bool {
	return l.Obj.Labels[wsapisv1.BuildBalancedBrTreeKey] == strconv.FormatBool(true) ||
		l.Obj.Annotations[wsapisv1.BuildBalancedBrTreeKey] == strconv.FormatBool(true)
}

func (l *lock) clearAllocations() {
	l.Allocations = nil
	// Also clear internal grant state to prevent inconsistency
	// when allocation clearing happens due to K8s update failures
	l.granted = false
}

func (l *lock) getAllocations() map[string]resource.SubresourceAllocation {
	if l.Allocations != nil {
		return l.Allocations
	}
	if !l.IsGranted() {
		return nil
	}
	ggResult := grantResult{
		name:              l.Obj.NamespacedName(),
		groupGrants:       l.GroupGrants,
		brGroupGrant:      l.BrGroupGrant,
		chfGroupGrants:    l.ChfGroupGrants,
		actGroupGrants:    l.ActGroupGrants,
		wgtCmdGroupGrants: l.WgtCmdGroupGrants,
		kvssGroupGrants:   l.KvssGroupGrants,
	}
	l.Allocations = resource.GatherAllocations(l.ResourceGrants, ggResult.mergeGroupGrants())
	return l.Allocations
}

// Retrieve the resource ids allocated by the lock
func (l *lock) getResIdsAllocated() []string {
	var allocatedNodes []string
	for nodeName, _ := range l.getAllocations() {
		allocatedNodes = append(allocatedNodes, nodeName)
	}
	return allocatedNodes
}

func (l *lock) getPopNgTypes() []string {
	return l.Obj.GetPopulatedNgTypes()
}

func (l *lock) isInferenceJobSharingEnabled() bool {
	return l.getAnnotations()[wsapisv1.InferenceJobSharingAnnotKey] == strconv.FormatBool(true)
}

// TODO: remove once SwDriver is scheduled on IX/SX
func (l *lock) isSwDriverMutualExclusionEnabled() bool {
	return l.getAnnotations()[wsapisv1.SwDriverMutualExclusionAnnotKey] == strconv.FormatBool(true)
}

func (l *lock) isInferenceJobMode() bool {
	if l.getLabels() == nil {
		return false
	}
	return l.getLabels()[wsapisv1.JobModeLabelKey] == wsapisv1.JobModeInference
}

// Identify if JobSharing is enabled for replicaType rt
// Here job sharing refers specifically to inference job sharing feature on AX nodes
//
// returns true for replica types in a sharing enabled inference job that's targeting AX nodes
func (l *lock) isInferenceJobSharingEnabledOnAx(rt commonv1.ReplicaType) bool {
	gr, ok := l.GroupRequests[rt.String()]
	if !ok {
		return false
	}
	return l.isInferenceJobSharingEnabled() && gr.IsGroupScheduledForAx()
}

func (l *lock) EmitInfJobSharingMemLimitMetrics() {
	if !l.isInferenceJobSharingEnabled() {
		return
	}

	jobName := l.Obj.Name
	jobNamespace := l.Obj.Namespace
	common.InfJobSharingMemoryLimitMetricAdded(jobNamespace, jobName, l.AggrAxMemLimit)
}

// toK8s converts the lock to a ResourceLock object by updating the .Status field to match the grant held in the
// internal lock representation. This is used to update the ResourceLock object in the API server.
// It is also used to convert the lock to a ResourceLock object for the old-school system and nodegroup but in the
// long term, this should only use the GroupGrants and not the NodeGroupGrants and SystemGrants.
func (l *lock) toK8s() *rlv1.ResourceLock {
	rl := l.Obj.DeepCopy()
	rl.Status.ResourceGrants = nil
	rl.Status.GroupResourceGrants = nil

	if !l.IsGranted() {
		rl.Status.State = rlv1.LockPending
		return rl
	}
	rl.Status.State = rlv1.LockGranted

	// Translate ungrouped and group grants as is.
	for requestId, resourceGrant := range l.ResourceGrants {
		rg := rlv1.ResourceGrant{
			Name:        requestId,
			Annotations: common.Merge(resourceGrant.Request.Annotations, resourceGrant.Annotations),
			Resources: common.Map(func(id string) rlv1.Res {
				res := rlv1.ResourceFromId(id)
				// Add detailed granted subresources or subresource ranges for coordinator
				if resourceGrant.Request.Id != rlv1.CoordinatorRequestName {
					return res
				}

				res.Subresources = common.EnsureMap(res.Subresources)
				if len(resourceGrant.Subresources) > 0 {
					maps.Copy(res.Subresources, resourceGrant.Subresources.Copy())
				}
				if len(resourceGrant.SubresourceOverride[id]) > 0 {
					maps.Copy(res.Subresources, resourceGrant.SubresourceOverride[id].Copy())
				}

				res.SubresourceRanges = common.EnsureMap(res.SubresourceRanges)
				if len(resourceGrant.SubresourceRanges) > 0 {
					for k, ranges := range resourceGrant.SubresourceRanges.Copy() {
						for _, r := range ranges {
							res.SubresourceRanges[k] = append(res.SubresourceRanges[k], rlv1.Range{
								Start: r.Start, End: r.End,
							})
						}
					}
				}
				return res
			}, resourceGrant.ResourceIds),
		}
		rl.Status.ResourceGrants = append(rl.Status.ResourceGrants, rg)
	}

	// copy to avoid updating on original object
	groupGrantsCopy := map[string][]resource.GroupGrant{}
	maps.Copy(groupGrantsCopy, l.GroupGrants)
	if len(l.BrGroupGrant.Grants) > 0 {
		groupGrantsCopy[l.BrGroupRequest.Id] = []resource.GroupGrant{l.BrGroupGrant}
	}
	for i, gg := range l.ChfGroupGrants {
		if len(gg.Grants) > 0 {
			groupGrantsCopy[l.ChfGroupRequests[i].Id] = []resource.GroupGrant{gg}
		}
	}
	for i, gg := range l.ActGroupGrants {
		if len(gg.Grants) > 0 {
			groupGrantsCopy[l.ActGroupRequests[i].Id] = []resource.GroupGrant{gg}
		}
	}
	for i, gg := range l.KvssGroupGrants {
		if len(gg.Grants) > 0 {
			groupGrantsCopy[l.KvssGroupRequests[i].Id] = []resource.GroupGrant{gg}
		}
	}
	for i, gg := range l.SWDriverGroupGrants {
		if len(gg.Grants) > 0 {
			groupGrantsCopy[l.SWDriverGroupRequests[i].Id] = []resource.GroupGrant{gg}
		}
	}
	for i, gg := range l.WgtCmdGroupGrants {
		if len(gg.Grants) > 0 {
			groupGrantsCopy[l.WgtCmdGroupRequests[i].Id] = []resource.GroupGrant{gg}
		}
	}
	for groupRequestId, groupResourceGrants := range groupGrantsCopy {
		for _, grg := range groupResourceGrants {
			gGrant := rlv1.GroupResourceGrant{
				Name:          groupRequestId,
				GroupingValue: grg.GroupVal,
			}
			k8rg := make([]rlv1.ResourceGrant, 0, len(grg.Grants))
			keys := common.Keys(grg.Grants)
			if groupRequestId == rlv1.BrRequestName {
				treeVal, _ := json.Marshal(l.BrLogicalTree)
				gGrant.Annotations = common.EnsureMap(gGrant.Annotations)
				gGrant.Annotations[rlv1.BrLogicalTreeKey] = string(treeVal)
				gGrant.Annotations[rlv1.BrLogicalTreePolicy] = l.BrLogicalTreePolicy
				sort.Slice(keys, func(i, j int) bool {
					k1, _ := strconv.Atoi(keys[i])
					k2, _ := strconv.Atoi(keys[j])
					return k1 < k2
				})
			}
			for _, k := range keys {
				g := grg.Grants[k]
				k8rg = append(k8rg,
					rlv1.ResourceGrant{
						Name:        g.Request.Id,
						Annotations: common.Merge(g.Request.Annotations, g.Annotations),
						Resources: common.Map(func(id string) rlv1.Res {
							res := rlv1.ResourceFromId(id)
							res.Subresources = common.EnsureMap(res.Subresources)
							// TODO: Refactor to consolidate "g.Subresources", "g.SubresourceOverride[id]" and corresponding upper bound
							if len(g.Subresources) > 0 {
								maps.Copy(res.Subresources, g.Subresources.Copy())
							}
							if len(g.SubresourceOverride[id]) > 0 {
								maps.Copy(res.Subresources, g.SubresourceOverride[id].Copy())
							}
							res.SubresourceRanges = common.EnsureMap(res.SubresourceRanges)
							if len(g.SubresourceRanges) > 0 {
								for k, ranges := range g.SubresourceRanges.Copy() {
									for _, r := range ranges {
										res.SubresourceRanges[k] = append(res.SubresourceRanges[k], rlv1.Range{
											Start: r.Start, End: r.End,
										})
									}
								}
							}
							return res
						}, g.ResourceIds),
					},
				)
			}
			gGrant.ResourceGrants = k8rg
			gGrant.Annotations = common.Merge(gGrant.Annotations, grg.Annotations)
			rl.Status.GroupResourceGrants = append(rl.Status.GroupResourceGrants, gGrant)
		}
	}
	for name, sr := range l.GroupGrantUpperBounds {
		rl.Status.GroupResourceGrantUpperBounds = append(rl.Status.GroupResourceGrantUpperBounds, rlv1.ResourceUpperBound{
			Name:         name,
			Subresources: sr,
		})
	}

	// Hack back in the old-school system and nodegroup grants. Makes it easier to migrate. Systems are always
	// ungrouped and any grouped requests with ng- prefix are nodegroups.
	for id, g := range l.ResourceGrants {
		if id == rlv1.SystemsRequestName {
			rl.Status.SystemGrants = common.Map(func(id string) rlv1.SystemGrant {
				return rlv1.SystemGrant{Name: rlv1.ResourceFromId(id).Name}
			}, g.ResourceIds)
			break
		}
	}

	for gid, grants := range groupGrantsCopy {
		if !strings.HasPrefix(gid, rlv1.NodegroupRequestPrefix) {
			continue
		}
		rl.Status.NodeGroupGrants = append(rl.Status.NodeGroupGrants, rlv1.NodeGroupGrant{
			Name:       gid,
			NodeGroups: common.Map(func(gg resource.GroupGrant) string { return gg.GroupVal }, grants),
		})
	}
	return rl
}

// As of rel-2.4, we only allow the following 3 patterns for populated nodegroups if all pop group types are allowed:
// - 1 regular/large pop group
// - 1 xlarge pop group
// - 2 xlarge pop groups
//
// Rules:
// Iterate 3 options and stop early if any option works.
// 1. If a job can fit in the first bucket, then xlarge group(s) should not be used.
// 2. If a job can fit in the second bucket, then multiple xlarge group(s) should not be used.
// Note: xlarge nodegroups will have a lower performance for regular jobs so regular/large NGs are always preferred.
//
// If the number of pop groups was not explicitly defined through annotations, we incrementally grant the buckets
// in order and stop when there's a grant. If there's no grant after examining all pop group options, the corresponding
// job will fail. If the number of pop groups was explicitly defined, we will attempt the request with less incremental search.
//
// When multiple pop groups are used, we will spread the WGTs evenly across those pop groups.
func (l *lock) GetAllowedPopGroupOptions() []PopGroupOptions {
	includesPrimaryNgRequest := false
	legacyPrimaryNgRequests := 0
	if !l.Obj.UseAxOnlyForRuntime() {
		for groupReqId := range l.GroupRequests {
			// During rel-2.4, there was a short period of time when the primary nodegroup request splits occurred
			// at lock creation time. The nodegroup request names would look like "ng-primary-0", "ng-primary-1",
			// instead of the "ng-primary" name. After that feature landed, we decided to push down the request
			// splitting into the lock reconciler, such that we could enable auto incremental grantable checks.
			// For old locks (request splits already happened at lock creation time) that had been created but still
			// in queue, we should not split the requests again on those locks inside lock reconciler.
			// TODO: Remove the legacy handling in rel-2.5
			if groupReqId == rlv1.PrimaryNgPrefix {
				includesPrimaryNgRequest = true
				break
			} else if strings.HasPrefix(groupReqId, rlv1.PrimaryNgPrefix) {
				legacyPrimaryNgRequests += 1
			}
		}
	}

	var popGroupOptions []PopGroupOptions
	if includesPrimaryNgRequest {
		if len(l.Obj.GetNonXLPopulatedNgTypes()) > 0 {
			popNgCount := l.Obj.GetMaxAllowedPopulatedNgs(l.Obj.GetNonXLPopulatedNgTypes())
			popGroupOptions = append(popGroupOptions, PopGroupOptions{popNgTypes: l.Obj.GetNonXLPopulatedNgTypes(), popNgCount: popNgCount})
		}
		if len(l.Obj.GetXLPopulatedNgTypes()) > 0 {
			popNgCount := 1
			if l.Obj.IsPopulatedNgCountDefined() {
				popNgCount = l.Obj.GetMaxAllowedPopulatedNgs(l.Obj.GetXLPopulatedNgTypes())
			}
			for ; popNgCount <= l.Obj.GetMaxAllowedPopulatedNgs(l.Obj.GetXLPopulatedNgTypes()); popNgCount++ {
				popGroupOptions = append(popGroupOptions, PopGroupOptions{popNgTypes: l.Obj.GetXLPopulatedNgTypes(), popNgCount: popNgCount})
			}
		}
	} else if legacyPrimaryNgRequests > 0 {
		popNgTypes := []string{wsapisv1.PopNgTypeRegular, wsapisv1.PopNgTypeLarge}
		if l.Obj.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes] != "" {
			popNgTypes = strings.Split(l.Obj.Annotations[wsapisv1.WsJobPopulatedNodegroupTypes], ",")
		}
		popGroupOptions = append(popGroupOptions, PopGroupOptions{popNgTypes: popNgTypes, popNgCount: legacyPrimaryNgRequests})
	} else {
		popGroupOptions = append(popGroupOptions, PopGroupOptions{popNgTypes: []string{}, popNgCount: 0})
	}

	return popGroupOptions
}

// In the event where multiple pop groups are used, we will:
// - spread all weights evenly across all pop groups
// - redistribute workers from secondary group requests to primary group requests
func (l *lock) redistributeTasks() {
	originalPopGroupReqId := ""
	originalPopGroupReq := resource.GroupRequest{}
	wgtReq := resource.Request{}
	wrkReq := resource.Request{}
	numCsx := l.ResourceRequests[rlv1.SystemsRequestName].Count
	for groupReqId, groupReq := range l.GroupRequests {
		// AX scheduling errors are explicitly shown in the error path
		// We filter out the AX group requests to get more accurate error messages for pop/depop nodegroup scheduling
		if groupReqId == rlv1.ActivationRequestName || groupReqId == rlv1.ChiefRequestName ||
			groupReqId == rlv1.WgtCmdGroupPrefix || groupReqId == rlv1.KVStorageServerRequestName {
			continue
		}

		if groupReqId == rlv1.PrimaryNgPrefix {
			originalPopGroupReq = groupReq
			originalPopGroupReqId = groupReqId
			// no error handling as weight and worker should always be in the primary group request
			if _req, ok := groupReq.Requests[rlv1.WeightRequestName]; ok {
				wgtReq = _req
			}
			if _req, ok := groupReq.Requests[rlv1.WorkerRequestName]; ok {
				wrkReq = _req
			}
		}
	}

	totalWgtsRemaining := wgtReq.Count
	totalWrksRemaining := numCsx * wrkReq.Count
	for i := 0; i < l.PopGroupCount; i++ {
		popGroupReq := resource.GroupRequest{
			Id:       fmt.Sprintf("%s-%d", rlv1.PrimaryNgPrefix, i),
			Count:    originalPopGroupReq.Count,
			Requests: common.CopyMap(originalPopGroupReq.Requests),
			KeySort:  originalPopGroupReq.KeySort,
		}
		if i != 0 {
			// subsequent pop group request(s) should only contain weight and worker requests
			for _, k := range common.Keys(popGroupReq.Requests) {
				if k != rlv1.WeightRequestName && k != rlv1.WorkerRequestName {
					delete(popGroupReq.Requests, k)
				}
			}
		}

		if totalWgtsRemaining > 0 {
			wgtsPerPopGroup := totalWgtsRemaining / (l.PopGroupCount - i)
			totalWgtsRemaining -= wgtsPerPopGroup
			wgtReq.Count = wgtsPerPopGroup
			popGroupReq.Requests[rlv1.WeightRequestName] = wgtReq
		}
		if totalWrksRemaining > 0 {
			popGroupReq.Requests[rlv1.WorkerRequestName] = wrkReq
			totalWrksRemaining -= wrkReq.Count
		}
		l.GroupRequests[popGroupReq.Id] = popGroupReq
	}
	delete(l.GroupRequests, originalPopGroupReqId)

	// If secondary group exists, reduce the number of secondary group requests needed
	// as we have included more workers in the primary group requests
	if val, ok := l.GroupRequests[rlv1.SecondaryNgName]; ok {
		remainingSecondaryNgCount := numCsx - l.PopGroupCount
		if remainingSecondaryNgCount <= 0 {
			delete(l.GroupRequests, rlv1.SecondaryNgName)
		} else {
			secondaryGrpReqCopy := val.Copy()
			secondaryGrpReqCopy.Count = remainingSecondaryNgCount
			l.GroupRequests[rlv1.SecondaryNgName] = secondaryGrpReqCopy
		}
	}
}

// NewLockFromK8s creates a lock from a ResourceLock object. If the ResourceLock is in an error state, nil is returned.
// Some validation is performed to ensure that the ResourceLock's spec is valid and the spec and status are consistent.
func NewLockFromK8s(rl *rlv1.ResourceLock) (*lock, error) {
	if rl.Status.State == rlv1.LockError {
		return nil, nil
	}

	// convert ResourceLock spec into internal representation
	l := &lock{
		Name:       rl.NamespacedName(),
		CreateTime: createTime(rl).UnixMilli(),
		QueueName:  rl.Spec.QueueName,
		Priority:   rl.GetPriority(),
		WorkflowId: rl.Labels[wsapisv1.WorkflowIdLabelKey],
		Obj:        rl.DeepCopy(),

		ResourceRequests:      map[string]resource.Request{},
		ResourceGrants:        map[string]resource.Grant{},
		GroupRequests:         map[string]resource.GroupRequest{},
		GroupGrants:           map[string][]resource.GroupGrant{},
		BrGroupRequest:        resource.GroupRequest{},
		BrGroupGrant:          resource.GroupGrant{},
		GroupGrantUpperBounds: map[string]resource.Subresources{},
		SpecifiedBrTreePolicy: rl.Annotations[wsapisv1.BrTreePolicyKey],
		SkipBr:                rl.IsSkipBr(),
		LinearMemoryConfigs:   make(map[commonv1.ReplicaType]common.LinearMemoryConfig),
	}

	// We already call and verify ParseLinearMemoryConfigs in reconcile_resources in the wsjob reconciler,
	// so we do not need to check for the error here.
	l.LinearMemoryConfigs, _ = common.ParseLinearMemoryConfigs(rl.Annotations)

	var gracePeriodSeconds int
	// grant deadline if prioritized but without reservation
	if l.IsPrioritized() && !l.HasReservation() {
		gracePeriodSeconds = wsapisv1.PendingLockGracePeriodValue
	}
	// for override case
	if l.Obj.Annotations[wsapisv1.PendingLockGracePeriodAnnotKey] != "" {
		val, _ := strconv.Atoi(l.Obj.Annotations[wsapisv1.PendingLockGracePeriodAnnotKey])
		if val > 0 {
			gracePeriodSeconds = val
		}
	}
	if gracePeriodSeconds > 0 {
		deadline := l.Obj.CreationTimestamp.Add(time.Duration(gracePeriodSeconds) * time.Second)
		l.GrantDeadline = &deadline
	}

	// ungrouped
	numSysRequested := 0
	for _, resourceRequest := range rl.Spec.ResourceRequests {
		l.ResourceRequests[resourceRequest.Name] = resource.Request{
			Id:          resourceRequest.Name,
			Annotations: resourceRequest.Annotations,
			Count:       resourceRequest.Count,
			Filter: resource.NewFilter(
				resourceRequest.Type,
				resourceRequest.Properties,
				resourceRequest.Subresources),
		}.WithLockProps(l.Obj)
		if resourceRequest.Name == rlv1.SystemsRequestName {
			numSysRequested = l.ResourceRequests[resourceRequest.Name].Count
		}
		kvs, isJobProp := common.GetAffinityRequest(resourceRequest.Annotations, false)
		for k, v := range kvs {
			l.ResourceRequests[resourceRequest.Name].Filter.AddAffinity(k, v, isJobProp[k])
		}
	}

	if len(l.ResourceRequests) != len(rl.Spec.ResourceRequests) {
		return nil, fmt.Errorf("duplicate request name in resource requests")
	}

	// grouped
	groupedRequestUpperBounds := map[string]resource.Subresources{}
	for _, ub := range rl.Spec.GroupResourceRequestUpperBounds {
		groupedRequestUpperBounds[ub.Name] = ub.Subresources
	}
	l.GroupRequestUpperBounds = groupedRequestUpperBounds
	l.RtNodeMaxLimitReservedMemGB, l.RtNodeMaxLimitReservedMemRatio = rl.GetRtMaxLimitOverhead()

	for _, groupResourceRequest := range rl.Spec.GroupResourceRequests {
		gr := resource.GroupRequest{
			Id:       groupResourceRequest.Name,
			Count:    groupResourceRequest.Count,
			Requests: map[string]resource.Request{},
			// When AX scheduling is enabled, activation and chief group requests will override the key sort function
			// during request hydration at a later point
			KeySort: resource.ActWgtKeySort,
		}

		for _, rr := range groupResourceRequest.ResourceRequests {
			gr.Requests[rr.Name] = resource.Request{
				Id:          rr.Name,
				Annotations: rr.Annotations,
				Count:       rr.Count,
				Filter:      resource.NewFilter(rr.Type, rr.Properties, rr.Subresources),
			}.WithLockProps(l.Obj)
			if _, ok := groupedRequestUpperBounds[rr.Name]; ok {
				for k, ubSr := range groupedRequestUpperBounds[rr.Name] {
					if srReq, ok := rr.Subresources[k]; !ok && !strings.HasSuffix(k, resource.PodUpperBoundMaxLimitPostFix) {
						return nil, fmt.Errorf(
							"invalid groupedRequestUpperBounds: %s subresource '%s' request was not set", rr.Name, k)
					} else if ubSr < srReq {
						return nil, fmt.Errorf(
							"invalid groupedRequestUpperBounds: %s subresource '%s' upper bound of %d is less than %d",
							rr.Name, k, ubSr, srReq)
					}
				}
			}

			if wsapisv1.UseAxScheduling {
				// "RuntimeTargetedDomainsKey" is legacy annotation and deprecated in rel-2.5
				// TODO: Remove the "RuntimeTargetedDomainsKey" block below in rel-2.7
				// This is present only for backward compatibility
				if strings.HasPrefix(groupResourceRequest.Name, rlv1.ActivationRequestName) {
					var actTargets []common.WioTarget
					if val, ok := l.Obj.Annotations[wsapisv1.AllRuntimeTargetedDomainsKey]; ok {
						// error was already checked on the lock creation side
						actTargets, _ = common.ParseTargetedDomainsAnnot(val)
					} else if val, ok = l.Obj.Annotations[wsapisv1.RuntimeTargetedDomainsKey]; ok {
						for i := 0; i < numSysRequested; i++ {
							// input: "0,1,2"
							// output: [][]int{{0}, {1}, {2}}
							// input (future support): "0-1,1-2,2-3"
							// output (future support): [][]int{{0,1}, {1,2}, {2,3}}
							for _, perPodDomains := range strings.Split(val, ",") {
								var domains []int
								for _, domainStr := range strings.Split(perPodDomains, "-") {
									if domain, err := strconv.Atoi(domainStr); err != nil {
										// We should not hit here, but handling it just to be safe
										return nil, fmt.Errorf("received an unrecognized activation to CSX domains input: %s", val)
									} else {
										domains = append(domains, domain)
									}
								}
								actTargets = append(actTargets, common.WioTarget{
									AllSystemIndices: []int{i},
									Domains:          domains,
								})
							}
						}
					}
					l.ActTargets = actTargets

					if len(l.ActTargets) == 0 {
						// this should not happen
						return nil, fmt.Errorf("activation domains annotation should not be empty when activation group request was detected")
					}
				}

				// KVSS and ACT should never be in the same group resource request
				if strings.HasPrefix(groupResourceRequest.Name, rlv1.KVStorageServerRequestName) {
					var kvssTargets []common.WioTarget
					if val, ok := l.Obj.Annotations[wsapisv1.AllPromptCachingTargetedDomainsKey]; ok {
						// error was already checked on the lock creation side
						kvssTargets, _ = common.ParseTargetedDomainsAnnot(val)
					}
					l.KvssTargets = kvssTargets

					if len(l.KvssTargets) == 0 {
						// this should not happen
						return nil, fmt.Errorf("kvss domains annotation should not be empty when kvss group request was detected")
					}
				}
			}

			if wsapisv1.UseIxScheduling {
				if strings.HasPrefix(groupResourceRequest.Name, rlv1.SWDriverRequestName) {
					var swDriverTargets []common.WioTarget
					if val, ok := l.Obj.Annotations[wsapisv1.AllSwdrTargetedDomainsKey]; ok {
						// error was already checked on the lock creation side
						swDriverTargets, _ = common.ParseTargetedDomainsAnnot(val)
					}
					if len(swDriverTargets) == 0 {
						return nil, fmt.Errorf("sw driver domains annotation should not be empty when sw driver group request was detected")
					}
					if len(swDriverTargets) > 1 {
						return nil, fmt.Errorf("sw driver group request should not have more than one target, got %d", len(swDriverTargets))
					}
					l.SWDriverTargets = swDriverTargets
				}
			}
		}

		if len(gr.Requests) != len(groupResourceRequest.ResourceRequests) {
			return nil, fmt.Errorf("duplicate request name in group %s", groupResourceRequest.Name)
		}

		l.GroupRequests[groupResourceRequest.Name] = gr
	}
	if len(l.GroupRequests) != len(rl.Spec.GroupResourceRequests) {
		return nil, fmt.Errorf("duplicate GroupRequest name in spec")
	}

	// convert ResourceLock status into internal representation checking that the grant was valid
	for _, rg := range rl.Status.ResourceGrants {
		l.ResourceGrants[rg.Name] = resource.Grant{
			Request:           l.ResourceRequests[rg.Name],
			ResourceIds:       common.Map(func(r rlv1.Res) string { return r.Id() }, rg.Resources),
			Subresources:      l.ResourceRequests[rg.Name].Subresources().Copy(),
			SubresourceRanges: common.EnsureMap(resource.SubresourceRanges{}),
			Annotations:       rg.Annotations,
		}
		for _, res := range rg.Resources {
			for k, ranges := range res.SubresourceRanges {
				for _, r := range ranges {
					l.ResourceGrants[rg.Name].SubresourceRanges[k] = append(l.ResourceGrants[rg.Name].SubresourceRanges[k],
						resource.Range{Start: r.Start, End: r.End})
				}
			}
		}
	}

	for _, ggub := range rl.Status.GroupResourceGrantUpperBounds {
		l.GroupGrantUpperBounds[ggub.Name] = ggub.Subresources
	}

	for _, grg := range rl.Status.GroupResourceGrants {
		grpReq, groupRequestExists := l.GroupRequests[grg.Name]
		isBrGroupGrant := grg.Name == rlv1.BrRequestName
		isChfGroupGrant := strings.HasPrefix(grg.Name, rlv1.ChiefRequestName)
		isActGroupGrant := strings.HasPrefix(grg.Name, rlv1.ActivationRequestName)
		isPrimaryGroupGrant := strings.HasPrefix(grg.Name, rlv1.PrimaryNgPrefix)
		isWgtCmdGroupGrant := strings.HasPrefix(grg.Name, rlv1.WgtCmdGroupPrefix)
		isKvssGroupGrant := strings.HasPrefix(grg.Name, rlv1.KVStorageServerRequestName)
		isSWDriverGroupGrant := strings.HasPrefix(grg.Name, rlv1.SWDriverRequestName)
		if !groupRequestExists {
			groupNotFoundErr := fmt.Errorf("invalid grant: group %s was not in the request", grg.Name)
			if isChfGroupGrant {
				if originalReq, ok := l.GroupRequests[rlv1.ChiefRequestName]; !ok {
					return nil, groupNotFoundErr
				} else {
					grpReq = originalReq
				}
			} else if isActGroupGrant {
				if originalReq, ok := l.GroupRequests[rlv1.ActivationRequestName]; !ok {
					return nil, groupNotFoundErr
				} else {
					grpReq = originalReq
				}
			} else if isPrimaryGroupGrant {
				if originalReq, ok := l.GroupRequests[rlv1.PrimaryNgPrefix]; !ok {
					return nil, groupNotFoundErr
				} else {
					grpReq = originalReq
				}
			} else if isWgtCmdGroupGrant {
				if originalReq, ok := l.GroupRequests[rlv1.WgtCmdGroupPrefix]; !ok {
					return nil, groupNotFoundErr
				} else {
					grpReq = originalReq
				}
			} else if isKvssGroupGrant {
				if originalReq, ok := l.GroupRequests[rlv1.KVStorageServerRequestName]; !ok {
					return nil, groupNotFoundErr
				} else {
					grpReq = originalReq
				}
			} else if isSWDriverGroupGrant {
				if originalReq, ok := l.GroupRequests[rlv1.SWDriverRequestName]; !ok {
					return nil, groupNotFoundErr
				} else {
					grpReq = originalReq
				}
			} else if !isBrGroupGrant {
				return nil, groupNotFoundErr
			}
		}

		gg := resource.GroupGrant{
			Request:     grpReq,
			GroupVal:    grg.GroupingValue,
			Grants:      map[string]resource.Grant{},
			Annotations: grg.Annotations,
		}

		for _, rg := range grg.ResourceGrants {
			// grant name, e.g ACT, WGT..., or BR pod id: 0, 1, 2...
			grantName := rg.Name
			subgroupReq, ok := gg.Request.Requests[grantName]
			if !ok {
				if isWgtCmdGroupGrant {
					if strings.HasPrefix(grantName, rlv1.WeightRequestName) {
						subgroupReq, ok = gg.Request.Requests[rlv1.WeightRequestName]
						if !ok {
							return nil, fmt.Errorf("missing group request for grant %s", grantName)
						}
					} else {
						subgroupReq, ok = gg.Request.Requests[rlv1.CommandRequestName]
						if !ok {
							return nil, fmt.Errorf("missing group request for grant %s", grantName)
						}
					}
				} else if len(grpReq.Requests) > 0 {
					// special branch for act/chf
					subgroupReq = common.Values(grpReq.Requests)[0]
				}
			}

			// assign default from request
			grant := resource.Grant{
				Request:             subgroupReq,
				ResourceIds:         common.Map(func(r rlv1.Res) string { return r.Id() }, rg.Resources),
				Subresources:        resource.Subresources{},
				SubresourceOverride: map[string]resource.Subresources{},
				SubresourceRanges:   resource.SubresourceRanges{},
			}
			// assign default request
			for k, v := range subgroupReq.Subresources() {
				isIndexed := false
				for indexPre := range resource.IndexedSubresourceNamePrefixList {
					// skip indexed sub-res
					// e.g. "cs-port":1, skip it and use the "nic-0" from grant override field
					if strings.HasPrefix(k, indexPre) {
						isIndexed = true
					}
				}
				if !isIndexed {
					grant.Subresources[k] = v
				}
			}

			// override by upper bound grant
			upperBoundKey := grantName
			if isActGroupGrant {
				upperBoundKey = rlv1.ActivationRequestName
			}
			if isKvssGroupGrant {
				upperBoundKey = rlv1.KVStorageServerRequestName
			}
			if isWgtCmdGroupGrant && strings.HasPrefix(grantName, rlv1.WeightRequestName) {
				upperBoundKey = rlv1.WeightRequestName
			}
			if ubSr, ok := l.GroupGrantUpperBounds[upperBoundKey]; ok {
				for srName, srValue := range ubSr {
					grant.Subresources[srName] = srValue
				}
			}

			// assign overrides if exists
			for _, re := range rg.Resources {
				grant.SubresourceOverride[re.Id()] = common.CopyMap(re.Subresources)
			}

			// assign subresource ranges if exists
			res := rg.Resources[0]
			for k, ranges := range res.SubresourceRanges {
				for _, r := range ranges {
					grant.SubresourceRanges[k] = append(grant.SubresourceRanges[k],
						resource.Range{Start: r.Start, End: r.End})
				}
			}
			gg.Grants[grantName] = grant
		}
		if isBrGroupGrant {
			l.BrGroupGrant = gg
		} else if isChfGroupGrant {
			l.ChfGroupGrants = append(l.ChfGroupGrants, gg)
		} else if isActGroupGrant {
			l.ActGroupGrants = append(l.ActGroupGrants, gg)
		} else if isWgtCmdGroupGrant {
			l.WgtCmdGroupGrants = append(l.WgtCmdGroupGrants, gg)
		} else if isKvssGroupGrant {
			l.KvssGroupGrants = append(l.KvssGroupGrants, gg)
		} else {
			l.GroupGrants[grg.Name] = append(l.GroupGrants[grg.Name], gg)
		}
	}

	return l, nil
}

// hydrateChfRequests splits the original chief group request into multiple stamp-aware group requests.
func hydrateChfRequests(perStampSystems [][]string, groupReq resource.GroupRequest) []resource.GroupRequest {
	groupSubReq := resource.Request{}
	for _, subReq := range groupReq.Requests {
		// original group request only contains a base subrequest for this global hydration routine
		groupSubReq = subReq
		groupSubReq.Id = rlv1.ChiefRequestName
		groupSubReq.AddSubresource(resource.PodSubresource, 1)
		break
	}

	var newGroupReqs []resource.GroupRequest
	for i := 0; i < len(perStampSystems); i++ {
		groupSubReq.Count = len(perStampSystems[i])
		newGroupReqs = append(newGroupReqs, resource.GroupRequest{
			Id:    fmt.Sprintf("%s-group-%d", rlv1.ChiefRequestName, i),
			Count: 1,
			Requests: map[string]resource.Request{
				rlv1.ChiefRequestName: groupSubReq,
			},
			KeySort: resource.V2GenericAscKeySort,
		})
	}
	return newGroupReqs
}

// hydrateWioRequests splits the original activation/kvss group requests into multiple stamp-aware group requests.
// This function also annotates which CS port a pod should be talking to and updates the affinity filter accordingly.
func hydrateWioRequests(
	perStampSystems [][]string,
	wioTargets []common.WioTarget,
	groupReq resource.GroupRequest,
	requestType string,
	linearMemoryConfig *common.LinearMemoryConfig,
) []resource.GroupRequest {
	var systemNames []string
	systemStampIdxMap := map[string]int{}
	for i, ss := range perStampSystems {
		for _, s := range ss {
			systemStampIdxMap[s] = i
			systemNames = append(systemNames, s)
		}
	}

	groupSubReq := resource.Request{}
	inferenceJobSharingEnabled := false
	for _, subReq := range groupReq.Requests {
		// original group request only contains a base subrequest for this global hydration routine
		groupSubReq = subReq
		inferenceJobSharingEnabled = subReq.IsInferenceJobSharingEnabled()
		groupSubReq.AddSubresource(resource.PodSubresource, 1)
		groupSubReq.AddSubresource(resource.NodeNicSubresource, 1)
		if !inferenceJobSharingEnabled {
			// In AX scheduling, we define the maximum number of pods a node could have ("AuxiliaryPodUsageLimit"),
			// and maximum number of pods a node-nic could have ("AuxiliaryNicUsageLimit") to ensure both node-level and nic-level LB.
			// Here AuxiliaryPod/NicUsageLimit in the subresource represents the basic unit of capacity consumed when pod is placed on the node.
			// Without job sharing we try to balance the pod number/nic usage, where the difference between usages are at most 1.
			// With job sharing turned on we don't aim to retain such balancing.
			// SWDriver does not have a PodPerCSX concept, so we do not add AuxiliaryPodUsageLimit and AuxiliaryNicUsageLimit
			// subresources for it.
			if requestType == rlv1.ActivationRequestName || requestType == rlv1.KVStorageServerRequestName {
				groupSubReq.AddSubresource(resource.AuxiliaryPodUsageLimit, 1)
				groupSubReq.AddSubresource(resource.AuxiliaryNicUsageLimit, 1)
			}
		}
		break
	}

	var newGroupReqs []resource.GroupRequest
	for i := 0; i < len(perStampSystems); i++ {
		newGroupReqs = append(newGroupReqs, resource.GroupRequest{
			Id:       fmt.Sprintf("%s-group-%d", requestType, i),
			Count:    1,
			Requests: map[string]resource.Request{},
			KeySort:  resource.V2ActKvssKeySort,
		})
	}

	for podId := 0; podId < len(wioTargets); podId++ {
		newGroupSubReq := groupSubReq.Copy()

		newGroupSubReq.Id = strconv.Itoa(podId)
		var sysIndices []string
		for _, sysIdx := range wioTargets[podId].AllSystemIndices {
			sysIndices = append(sysIndices, strconv.Itoa(sysIdx))
		}

		targetPorts := make([]string, len(wioTargets[podId].TargetPorts))
		for i, port := range wioTargets[podId].TargetPorts {
			targetPorts[i] = strconv.Itoa(port)
		}

		newGroupSubReq.Annotations = map[string]string{
			rlv1.PodIdAnnotKey:        strconv.Itoa(podId),
			rlv1.TargetCsPortAnnotKey: strings.Join(targetPorts, ","),
			// used for KeySort and QoS measurement only
			rlv1.SystemIdAnnotKey:     strings.Join(sysIndices, ","),
			rlv1.WioBandwidthAnnotKey: strconv.Itoa(wioTargets[podId].WioBandwidth),
		}

		// SWDriver does not have a PodPerCSX concept
		if wsapisv1.IsReplicaTypeActKvss(commonv1.ReplicaType(requestType)) {
			newGroupSubReq.Annotations[rlv1.PodIdPerCsxAnnotKey] = strconv.Itoa(podId % (len(wioTargets) / len(systemNames)))
		}

		if inferenceJobSharingEnabled {
			newGroupSubReq.Annotations[wsapisv1.InferenceJobSharingAnnotKey] = strconv.FormatBool(inferenceJobSharingEnabled)
		}

		newGroupSubReq.Count = 1
		for _, sysIdx := range wioTargets[podId].InStampSystemIndices {
			for _, port := range wioTargets[podId].TargetPorts {
				newGroupSubReq.AddAffinity(systemv1.GetSysPortAffinityKey(systemNames[sysIdx], port), "", false)
			}
		}

		//  inject MemSubresource via global or per-replica coef
		if linearMemoryConfig != nil {
			lower := linearMemoryConfig.LowerFor(podId)
			// Set memory request (convert to MiB)
			newGroupSubReq.AddSubresource(resource.MemSubresource, int(math.Ceil(common.GetPreciseMiBFromBytes(lower))))
		}

		newGroupReqs[wioTargets[podId].StampIdx].Requests[newGroupSubReq.Id] = newGroupSubReq
	}
	return newGroupReqs
}

// BuildBrRequest returns []GroupRequest where only one of the requests is expected to be granted.
// Multiple requests represent different possible valid BR allocations
func (r *Reconciler) BuildBrRequest(groupSystems map[string][]string, l *lock, policy string) (int, int, int,
	common.BrLogicalTree, []resource.GroupRequest, error) {

	var brs common.BrLogicalTree
	var layer, ports, crossTraffic int
	layer, ports, crossTraffic, brs = common.BuildBrGroupTrees(groupSystems,
		l.OrderedSystems, l.enableBalancedTree(), policy)

	skipGroup := brSkipGroupNotSet
	if l.enablePartialSystems() {
		var sysList []*systemv1.SystemSpec
		if r.Cfg != nil && r.Cfg.SystemMap != nil {
			for _, group := range groupSystems {
				for _, system := range group {
					if sys, ok := r.Cfg.SystemMap[system]; ok && sys != nil {
						sysList = append(sysList, sys)
					}
				}
			}
		}
		id, err := systemv1.SystemPortCompatibleCheck(sysList)
		if err != nil {
			return -1, -1, -1, nil, nil, err
		}
		skipGroup = id
	}

	var reqs []resource.GroupRequest
	if skipGroup == brSkipGroupNotSet {
		req := buildFullBrRequest(brs, l)
		reqs = append(reqs, req)
	}
	if l.enablePartialSystems() {
		// if skip group defined
		if skipGroup >= 0 {
			req := buildBrRequestWithOptions(brs, skipGroup, l)
			reqs = append(reqs, req)
		} else {
			// Try all available port groups (was hardcoded to 3; now derived from CS version spec).
			groupCount := common.CSVersionSpec.NumGroups()
			// in V2 network, BR ports are decoupled from system port so try one skip group is enough
			if wsapisv1.IsV2Network {
				groupCount = 1
			}
			for group := 0; group < groupCount; group++ {
				req := buildBrRequestWithOptions(brs, group, l)
				reqs = append(reqs, req)
			}
		}
	}
	return layer, ports, crossTraffic, brs, reqs, nil
}

func buildFullBrRequest(brs common.BrLogicalTree, l *lock) resource.GroupRequest {
	return buildBrRequestWithOptions(brs, brSkipGroupNotSet, l)
}

// actual build request based on provided options
// todo: BR schedule is slow for large system job due to too many sub group requests
// try to group request id from per pod (set*systemv1.PortCount + port) to per logical pod (set*systemv1.PortCount)
func buildBrRequestWithOptions(brs common.BrLogicalTree, skipGroup int, l *lock) resource.GroupRequest {
	groupReq := resource.GroupRequest{
		Id:       rlv1.BrRequestName,
		Count:    1,
		Requests: map[string]resource.Request{},
		KeySort:  resource.BrRequestKeySort,
	}

	portCount := common.CSVersionSpec.NumPorts
	for set, br := range brs {
		targetSystemNames, downstreamBrSets := br.GetEgressTargets()
		for port := 0; port < portCount; port++ {
			group := common.CSVersionSpec.Group(port)
			if group == skipGroup {
				continue
			}
			filter := resource.NewFilter(
				rlv1.NodeResourceType,
				common.RoleBroadcastReduce.AsMatcherProperty(),
				map[string]int{
					// br requires instance id on node to split cpu in a flexible way
					resource.PodInstanceIdSubresource: 1,
				})
			kvs, isJobProp := common.GetAffinityRequest(l.getAnnotations(), false)
			for k, v := range kvs {
				filter.AddAffinity(k, v, isJobProp[k])
			}
			kvs, isJobProp = common.GetAffinityRequest(l.getAnnotations(), true)
			for k, v := range kvs {
				filter.AddAntiAffinity(k, v, isJobProp[k])
			}
			portBwRequestCount := (len(br.Egresses) + 1) * resource.DefaultNicBW
			if wsapisv1.IsV2Network {
				// add affinity for all connected systems/ports in the bottom layer
				addBrPortAffinityProps(&filter, br, brs, port)
				// add request of port aggregated BW
				filter.AddSubresource(resource.NodeNicSubresource, portBwRequestCount)
			} else {
				// add request of port id + aggregated BW
				filter.AddSubresource(fmt.Sprintf("%s-%d", resource.NodeNicSubresource, port), portBwRequestCount)
			}
			// As of 2.2.x, HostIO code can't handle the case when 2 BR pods that are from different levels share the same BR node.
			// So we try not to reuse the same BR node for pods from different BR tree levels.
			// Note: this may need revisit since it may lower utilization in edge cases of 2 * 1:2 mode in different layers
			// layer prop has to be allocated since it's a job level prop
			filter.AddAntiAffinity(resource.BrLayerProp, strconv.Itoa(br.Layer), true)
			id := strconv.Itoa(set*portCount + port)
			groupReq.Requests[id] = resource.Request{
				Id:          id,
				Count:       1,
				Filter:      filter,
				Annotations: l.buildBrRequestAnnotations(targetSystemNames, downstreamBrSets, port, set),
			}.WithLockProps(l.Obj)
		}
	}
	return groupReq
}

// build BR affinity props
func addBrPortAffinityProps(filter *resource.Filter, br *common.BrInternalNode, brs []*common.BrInternalNode, port int) {
	for _, eg := range br.Egresses {
		egr := eg
		if !egr.IsBr {
			filter.AddAffinity(systemv1.GetSysPortAffinityKey(egr.SystemName, port), "", false)
		} else {
			addBrPortAffinityProps(filter, brs[egr.BrSetId], brs, port)
		}
	}
}

// build annotations for BR replica request
func (l *lock) buildBrRequestAnnotations(targetSystemNames *common.Set[string], targetBrSets string, port, set int) map[string]string {
	annotations := maps.Clone(l.getAnnotations())
	annotations = common.EnsureMap(annotations)
	annotations[rlv1.TargetCsPortAnnotKey] = strconv.Itoa(port)
	annotations[rlv1.BrSetIdAnnotKey] = strconv.Itoa(set)

	if targetBrSets != "" {
		annotations[rlv1.TargetBrSetsAnnotKey] = targetBrSets
	}

	if !targetSystemNames.IsEmpty() {
		var targetSystemIdx []string
		for i, systemName := range l.OrderedSystems {
			if targetSystemNames.Contains(systemName) {
				targetSystemIdx = append(targetSystemIdx, strconv.Itoa(i))
			}
		}
		annotations[rlv1.SystemIdAnnotKey] = strings.Join(targetSystemIdx, ",")
	}

	return annotations
}

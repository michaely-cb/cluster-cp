package namespace

import (
	"fmt"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

// nsrResourcesReq temp struct for holding nsr assignments while the nsr scheduling progresses
type nsrResourcesReq struct {
	name          string
	append        bool
	systemChanges []string
	ngChanges     []string
	nodeChanges   map[string][]string

	// systems, nodegroups, nodes assigned to namespace name
	haveSystems map[string]bool
	haveNGs     map[string]bool
	haveNodes   map[string]map[string]bool
	finalRIDs   []string

	// note that ax nodes have an advanced option to have operator automatically finds the best ax nodes given session capacity
	// this advanced option by default is turned off
}

// reconcileNSRDebugMode appends/removes/sets systems/nodegroups/nodes from current NSR. No resource-capacity validation
// is performed, only in-use checks on release and unassigned checks on append
func (r *Reconciler) reconcileNSRDebugMode(ctx reconcileCtx, nsr *namespacev1.NamespaceReservation) error {
	var add nsrResourcesReq
	var rm nsrResourcesReq
	var err error

	add, rm, err = r.newNsrPatchReq(nsr, r.collection)
	if err != nil {
		return r.failNsrRequest(ctx, nsr, err)
	}
	// add phase
	if err = add.assignResources(r); err != nil {
		return r.failNsrRequest(ctx, nsr, err)
	}
	// remove phase
	rm.haveSystems = add.haveSystems
	rm.haveNGs = add.haveNGs
	rm.haveNodes = add.haveNodes
	if err = rm.assignResources(r); err != nil {
		return r.failNsrRequest(ctx, nsr, err)
	}
	req := rm

	prevRIDs := nsr.Status.GetAllocatedRIDs()
	nsr.Status.Systems = common.SortedKeys(req.haveSystems)
	nsr.Status.Nodegroups = common.SortedKeys(req.haveNGs)
	// display mgmt nodes first
	nsr.Status.Nodes = []string{}
	for _, role := range []string{resource.RoleCrdPropKey, resource.RoleAxPropKey, resource.RoleIxPropKey} {
		if nodes, ok := req.haveNodes[role]; ok {
			nsr.Status.Nodes = append(nsr.Status.Nodes, common.SortedKeys(nodes)...)
		}
	}
	nsr.Status.SetSuccessfulRequest(nsr.Spec.ReservationRequest)
	nsr.Status.CurrentResourceSpec = estimateResourceSpec(r.collection, nsr.Status, nsr.GetWorkloadType())
	if err = r.client.Status().Update(ctx, nsr); err != nil {
		return err
	}
	removed, added := getDiff(prevRIDs, req.finalRIDs)
	ctx.modifiedRID.addUnassigned(removed)
	ctx.modifiedRID.addAssigned(added, nsr.Name)
	return nil
}

func (r *Reconciler) newNsrPatchReq(nsr *namespacev1.NamespaceReservation, c *resource.Collection) (nsrResourcesReq, nsrResourcesReq, error) {
	currentParams := nsr.Spec.RequestParams
	// On system resChanges, AX will need to be recalculated all together, possibly involving append + remove.
	// We treat append/remove same as set for simplicity
	newNsr := nsr.DeepCopy()
	newParams := newNsr.Spec.RequestParams
	newParams.UpdateMode = namespacev1.DebugSetMode
	newParams.SystemNames = common.CopySlice(nsr.Status.Systems)
	newParams.NodegroupNames = common.CopySlice(nsr.Status.Nodegroups)

	_, haveNodes := parseNsrStatusNodes(c, nsr.Status.Nodes)
	newParams.CoordinatorNodeNames = common.CopySlice(common.Keys(haveNodes[resource.RoleCrdPropKey]))
	newParams.ActivationNodeNames = common.CopySlice(common.Keys(haveNodes[resource.RoleAxPropKey]))
	newParams.InferenceDriverNodeNames = common.CopySlice(common.Keys(haveNodes[resource.RoleIxPropKey]))

	// first construct the desired systems/nodegroups/coord nodes in the updated nsr
	if currentParams.UpdateMode == namespacev1.DebugAppendMode {
		for _, systemName := range currentParams.SystemNames {
			if !slices.Contains(newParams.SystemNames, systemName) {
				newParams.SystemNames = append(newParams.SystemNames, systemName)
			}
		}
		for _, ngName := range currentParams.NodegroupNames {
			if !slices.Contains(newParams.NodegroupNames, ngName) {
				newParams.NodegroupNames = append(newParams.NodegroupNames, ngName)
			}
		}
		for _, nodeName := range currentParams.CoordinatorNodeNames {
			if !slices.Contains(newParams.CoordinatorNodeNames, nodeName) {
				newParams.CoordinatorNodeNames = append(newParams.CoordinatorNodeNames, nodeName)
			}
		}
		for _, nodeName := range currentParams.ActivationNodeNames {
			if !slices.Contains(newParams.ActivationNodeNames, nodeName) {
				newParams.ActivationNodeNames = append(newParams.ActivationNodeNames, nodeName)
			}
		}
		for _, nodeName := range currentParams.InferenceDriverNodeNames {
			if !slices.Contains(newParams.InferenceDriverNodeNames, nodeName) {
				newParams.InferenceDriverNodeNames = append(newParams.InferenceDriverNodeNames, nodeName)
			}
		}
	} else if currentParams.UpdateMode == namespacev1.DebugRemoveMode {
		for _, systemName := range currentParams.SystemNames {
			newParams.SystemNames = common.Filter(
				func(v string) bool { return v != systemName }, newParams.SystemNames)
		}
		for _, ngName := range currentParams.NodegroupNames {
			newParams.NodegroupNames = common.Filter(
				func(v string) bool { return v != ngName }, newParams.NodegroupNames)
		}
		for _, nodeName := range currentParams.CoordinatorNodeNames {
			newParams.CoordinatorNodeNames = common.Filter(
				func(v string) bool { return v != nodeName }, newParams.CoordinatorNodeNames)
		}
		for _, nodeName := range currentParams.ActivationNodeNames {
			newParams.ActivationNodeNames = common.Filter(
				func(v string) bool { return v != nodeName }, newParams.ActivationNodeNames)
		}
		for _, nodeName := range currentParams.InferenceDriverNodeNames {
			newParams.InferenceDriverNodeNames = common.Filter(
				func(v string) bool { return v != nodeName }, newParams.InferenceDriverNodeNames)
		}
	} else if currentParams.UpdateMode == namespacev1.DebugSetMode {
		shouldSet := func(val []string) bool {
			return len(val) > 0 && !slices.Equal(val, []string{""})
		}
		shouldRemove := func(val []string) bool {
			return slices.Equal(val, []string{""})
		}
		if shouldRemove(currentParams.SystemNames) {
			newParams.SystemNames = []string{}
		} else if shouldSet(currentParams.SystemNames) {
			newParams.SystemNames = common.CopySlice(currentParams.SystemNames)
		}
		if shouldRemove(currentParams.NodegroupNames) {
			newParams.NodegroupNames = []string{}
		} else if shouldSet(currentParams.NodegroupNames) {
			newParams.NodegroupNames = common.CopySlice(currentParams.NodegroupNames)
		}
		if shouldRemove(currentParams.CoordinatorNodeNames) {
			newParams.CoordinatorNodeNames = []string{}
		} else if shouldSet(currentParams.CoordinatorNodeNames) {
			newParams.CoordinatorNodeNames = common.CopySlice(currentParams.CoordinatorNodeNames)
		}
		if shouldRemove(currentParams.ActivationNodeNames) {
			newParams.ActivationNodeNames = []string{}
		} else if shouldSet(currentParams.ActivationNodeNames) {
			newParams.ActivationNodeNames = common.CopySlice(currentParams.ActivationNodeNames)
		}
		if shouldRemove(currentParams.InferenceDriverNodeNames) {
			newParams.InferenceDriverNodeNames = []string{}
		} else if shouldSet(currentParams.InferenceDriverNodeNames) {
			newParams.InferenceDriverNodeNames = common.CopySlice(currentParams.InferenceDriverNodeNames)
		}
	} else if currentParams.UpdateMode == namespacev1.DebugRemoveAllMode {
		newParams.SystemNames = []string{}
		newParams.NodegroupNames = []string{}
		newParams.CoordinatorNodeNames = []string{}
		newParams.ActivationNodeNames = []string{}
		newParams.InferenceDriverNodeNames = []string{}
	}

	systems := common.MapBool(newNsr.Status.Systems)
	nodegroups := common.MapBool(newNsr.Status.Nodegroups)
	if wsapisv1.UseAxScheduling {
		perStampPortAffinity := make(map[string]map[int]int)
		if currentParams.AutoSelectActivationNodes {
			// ax nodes can be automatically determined to encourage node health and wide affinity coverage
			replaceReq := nsrReplaceReq{
				name:        newNsr.Name,
				haveSystems: newParams.SystemNames,
				haveNodes:   map[string][]string{resource.RoleAxPropKey: {}},
			}
			var err error
			if perStampPortAffinity, err = r.addAxNodes(&replaceReq, common.Keys(haveNodes[resource.RoleAxPropKey])); err != nil {
				return nsrResourcesReq{}, nsrResourcesReq{}, err
			}
			newParams.ActivationNodeNames = replaceReq.haveNodes[resource.RoleAxPropKey]
		} else {
			// AX node names are provided. Use GetNodesWithMostAffinityCoverage() (stamp by stamp) to get
			// perStampPortAffinity.
			perStampSystems := map[string][]string{}
			for _, systemName := range newParams.SystemNames {
				rid := resource.SystemType + "/" + systemName
				var stampIdx string
				sys, _ := c.Get(rid)
				if val, ok := sys.GetProp(resource.StampPropertyKey); ok {
					stampIdx = val
				}
				perStampSystems[stampIdx] = append(perStampSystems[stampIdx], systemName)
			}

			perStampAxNodes := map[string][]resource.Resource{}
			for _, nodeName := range newParams.ActivationNodeNames {
				rid := resource.NodeType + "/" + nodeName
				var stampIdx string
				res, _ := c.Get(rid)
				if val, ok := res.GetProp(resource.StampPropertyKey); ok {
					stampIdx = val
				}
				perStampAxNodes[stampIdx] = append(perStampAxNodes[stampIdx], res)
			}

			for stamp, systemsInStamp := range perStampSystems {
				axAllocated := &resource.Collection{}
				for _, node := range perStampAxNodes[stamp] {
					axAllocated.Add(node)
				}

				// not allocating more nodes because totalNodesNeeded == len(perStampAxNodes[stamp])
				_, inStampPortAffinity := r.collection.GetNodesWithMostAffinityCoverage(
					newNsr.Name, "", systemsInStamp, axAllocated, len(perStampAxNodes[stamp]),
					nil, nil, common.CSVersionSpec.NumPorts)
				perStampPortAffinity[stamp] = inStampPortAffinity
			}
		}

		// check AX affinity, fail if necessary
		if err := r.checkAXPortAffinity(perStampPortAffinity, nsr); err != nil {
			return nsrResourcesReq{}, nsrResourcesReq{}, err
		}
	}

	if wsapisv1.UseIxScheduling && currentParams.AutoSelectInferenceDriverNodes {
		replaceReq := nsrReplaceReq{
			name:        newNsr.Name,
			haveSystems: newParams.SystemNames,
			haveNodes:   map[string][]string{resource.RoleIxPropKey: {}},
		}
		if err := r.addIxNodes(&replaceReq, common.Keys(haveNodes[resource.RoleIxPropKey]), nsr.GetWorkloadType()); err != nil {
			return nsrResourcesReq{}, nsrResourcesReq{}, err
		}
		newParams.InferenceDriverNodeNames = replaceReq.haveNodes[resource.RoleIxPropKey]
	}

	// creates 2 resource reqs for purpose of set: first append new resources, then remove unspecified resources
	appendReq := nsrResourcesReq{
		name:        newNsr.Name,
		append:      true,
		haveSystems: systems,
		haveNGs:     nodegroups,
		haveNodes:   haveNodes,
	}
	rmReq := nsrResourcesReq{
		name:   newNsr.Name,
		append: false,
	}
	appendReq.systemChanges, rmReq.systemChanges = getAppendRemoveNames(newParams.SystemNames, systems)
	appendReq.ngChanges, rmReq.ngChanges = getAppendRemoveNames(newParams.NodegroupNames, nodegroups)
	coordNodeAppends, coordNodeRemoves := getAppendRemoveNames(newParams.CoordinatorNodeNames, haveNodes[resource.RoleCrdPropKey])
	axNodeAppends, axNodeRemoves := getAppendRemoveNames(newParams.ActivationNodeNames, haveNodes[resource.RoleAxPropKey])
	ixNodeAppends, ixNodeRemoves := getAppendRemoveNames(newParams.InferenceDriverNodeNames, haveNodes[resource.RoleIxPropKey])

	appendReq.nodeChanges = map[string][]string{
		resource.RoleCrdPropKey: coordNodeAppends,
		resource.RoleAxPropKey:  axNodeAppends,
		resource.RoleIxPropKey:  ixNodeAppends,
	}
	rmReq.nodeChanges = map[string][]string{
		resource.RoleCrdPropKey: coordNodeRemoves,
		resource.RoleAxPropKey:  axNodeRemoves,
		resource.RoleIxPropKey:  ixNodeRemoves,
	}
	return appendReq, rmReq, nil
}

func parseNsrStatusNodes(c *resource.Collection, nodeNames []string) (map[string][]string, map[string]map[string]bool) {
	nodesByRole := map[string][]string{
		resource.RoleCrdPropKey: {},
		resource.RoleAxPropKey:  {},
		resource.RoleIxPropKey:  {},
	}
	haveNodes := map[string]map[string]bool{
		resource.RoleCrdPropKey: {},
		resource.RoleAxPropKey:  {},
		resource.RoleIxPropKey:  {},
	}
	for _, nodeName := range nodeNames {
		if res, ok := c.Get(resource.NodeType + "/" + nodeName); ok {
			if _, ok = res.GetProp(resource.RoleCrdPropKey); ok {
				nodesByRole[resource.RoleCrdPropKey] = append(nodesByRole[resource.RoleCrdPropKey], nodeName)
			} else if _, ok = res.GetProp(resource.RoleAxPropKey); ok {
				nodesByRole[resource.RoleAxPropKey] = append(nodesByRole[resource.RoleAxPropKey], nodeName)
			} else if _, ok = res.GetProp(resource.RoleIxPropKey); ok {
				nodesByRole[resource.RoleIxPropKey] = append(nodesByRole[resource.RoleIxPropKey], nodeName)
			}
		}
	}
	for k, v := range nodesByRole {
		haveNodes[k] = common.MapBool(v)
	}
	return nodesByRole, haveNodes
}

// appendNames returns the names that are requested but not part of the currentNames
// rmNames returns currentNames minus the intersection between requestedNames and currentNames
func getAppendRemoveNames(requestedNames []string, currentNames map[string]bool) ([]string, []string) {
	currentNamesCopy := map[string]bool{}
	maps.Copy(currentNamesCopy, currentNames)

	// skip update if nothing specified to avoid implicit erasing the existing assignment
	if len(requestedNames) == 0 {
		return []string{}, common.Keys(currentNamesCopy)
	}

	var appendNames []string
	var rmNames []string
	for _, name := range requestedNames {
		if name != "" && currentNamesCopy[name] == false {
			appendNames = append(appendNames, name)
		}
		delete(currentNamesCopy, name)
	}
	rmNames = common.Keys(currentNamesCopy)
	return appendNames, rmNames
}

func (req *nsrResourcesReq) assignResources(r *Reconciler) error {
	update := func(rtype string, changes []string, assignment map[string]bool) error {
		for _, name := range changes {
			rid := rtype + "/" + name
			rs, rsExists := r.collection.Get(rid)
			if req.append {
				if !rsExists {
					return fmt.Errorf("%s requested %s '%s' was not found", reqErrPreamble, rtype, name)
				} else if currentNs, _ := rs.GetProp(common.NamespaceKey); currentNs != "" && currentNs != req.name {
					return fmt.Errorf("%s requested %s %s is already assigned to session %s. Remove this resource from %s before retrying", reqErrPreamble, rtype, name, currentNs, currentNs)
				} else if wsapisv1.ManagementSeparationEnforced && rs.IsMgmtNode() {
					return fmt.Errorf("%s requested %s '%s' is a dedicated management node and cannot be assigned for sessions/jobs. "+
						"Please check with 'csctl get node --role=coordinator' and use a dedicated coordinator node instead", reqErrPreamble, rtype, name)
				}
				assignment[name] = true
			} else {
				if !assignment[name] {
					continue
				}
				if r.resourceStatus.IsInUse(rid) {
					return fmt.Errorf("%s %s %s has jobs running and cannot be released. %s", reqErrPreamble, rtype, name, resourcesInUseMsg)
				}
				delete(assignment, name)
			}
		}
		for name := range assignment {
			req.finalRIDs = append(req.finalRIDs, rtype+"/"+name)
		}
		return nil
	}

	if err := update(resource.SystemType, req.systemChanges, req.haveSystems); err != nil {
		return err
	}
	if err := update(resource.NodegroupType, req.ngChanges, req.haveNGs); err != nil {
		return err
	}
	if err := update(resource.NodeType, req.nodeChanges[resource.RoleCrdPropKey], req.haveNodes[resource.RoleCrdPropKey]); err != nil {
		return err
	}
	if err := update(resource.NodeType, req.nodeChanges[resource.RoleAxPropKey], req.haveNodes[resource.RoleAxPropKey]); err != nil {
		return err
	}
	if err := update(resource.NodeType, req.nodeChanges[resource.RoleIxPropKey], req.haveNodes[resource.RoleIxPropKey]); err != nil {
		return err
	}
	return nil
}

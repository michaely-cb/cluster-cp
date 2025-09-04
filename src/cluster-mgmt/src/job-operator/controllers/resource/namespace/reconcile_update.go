package namespace

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"

	"cerebras.com/job-operator/common"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common/resource"
)

// nsrReplaceReq temp struct for holding nsr assignments while the nsr scheduling progresses
type nsrReplaceReq struct {
	name             string
	parallelCompiles int // defaults to 1 if systemCount > 0
	parallelExecutes int // defaults to 1 if parallelExecutes > 0

	// require* params are derived from the NSR.spec's params
	requireSystemCount   int
	requirePopNGCount    int
	requireLargeNGCount  int
	requireXLargeNGCount int
	requireNGCount       int
	requireCoordMemMi    int
	requireAxCount       int

	// accounting for currently held resources - reflect nsrProps of entries in nodegroups, nodes slices
	haveLargeMemNGCount  int
	haveXLargeMemNGCount int
	havePopNGCount       int
	haveCoordMemMi       int

	// systems, nodegroups, nodes assigned to namespace name
	haveSystems []string
	haveNGs     []string
	haveNodes   map[string][]string
}

func (n nsrReplaceReq) getRIDs() []string {
	var nsrStatusNodes []string
	for _, role := range []string{resource.RoleCrdPropKey, resource.RoleAxPropKey, resource.RoleIxPropKey} {
		if nodes, ok := n.haveNodes[role]; ok {
			nsrStatusNodes = append(nsrStatusNodes, common.Sorted(nodes)...)
		}
	}
	s := &namespacev1.NamespaceReservationStatus{Nodes: nsrStatusNodes, Systems: n.haveSystems, Nodegroups: n.haveNGs}
	return s.GetAllocatedRIDs()
}

// checkNGGrantableAfterInUseSelected validates that given the preselected resources, the request can still
// achieve the minimum parallelExecutes and largeMemoryNodegroups requested without adding more NG than systems requested
func (n nsrReplaceReq) checkNGGrantableAfterInUseSelected() error {
	if len(n.haveNGs) > n.requireSystemCount {
		return fmt.Errorf(
			"%s update was forced to select %d in-use nodegroups which exceeds requested system count %d. %s",
			reqErrPreamble, len(n.haveNGs), n.requireSystemCount, resourcesInUseMsg)
	}

	needPopNg := floor0(n.requirePopNGCount - n.havePopNGCount)
	if len(n.haveNGs)+needPopNg > n.requireSystemCount {
		haveDepopNg := len(n.haveNGs) - n.havePopNGCount
		return fmt.Errorf(
			"%s update was forced to select %d in-use nodegroups that do not support execute jobs but need "+
				"%d more to meet parallelExecutes requirement which would surpass %d systems requested. Consider cancelling "+
				"jobs, decreasing requested parallelExecutes count, or increasing requested systems count",
			reqErrPreamble, haveDepopNg, needPopNg, n.requireSystemCount)
	}

	needLargeMem := floor0(n.requireLargeNGCount - n.haveLargeMemNGCount)
	needXLargeMem := floor0(n.requireXLargeNGCount - n.haveXLargeMemNGCount)
	if len(n.haveNGs)+needLargeMem+needXLargeMem > n.requireSystemCount {
		haveRegular := len(n.haveNGs) - n.haveLargeMemNGCount - n.haveXLargeMemNGCount

		needMemMsg := ""
		reduceMemAction := ""
		if needLargeMem > 0 {
			needMemMsg = fmt.Sprintf("%d more to meet large memory rack count requirement", needLargeMem)
			reduceMemAction = fmt.Sprintf("decreasing requested large memeory rack count")
		}
		if needXLargeMem > 0 {
			if needMemMsg != "" {
				needMemMsg += " and "
			}
			if reduceMemAction != "" {
				reduceMemAction += " or "
			}
			needMemMsg += fmt.Sprintf("%d more to meet xlarge memory rack count requirement", needXLargeMem)
			reduceMemAction += fmt.Sprintf("decreasing requested xlarge memeory rack count")
		}

		return fmt.Errorf(
			"%s update was forced to select %d in-use nodegroups that have regular memory but need "+
				"%s which would surpass %d systems requested. Consider cancelling jobs, %s, "+
				"or increasing requested systems count",
			reqErrPreamble, haveRegular, needMemMsg, n.requireSystemCount, reduceMemAction)
	}

	return nil
}

func (n nsrReplaceReq) checkCoordGrantableAfterInUseSelected(coordNodeMemsMi map[string]int) error {
	// check that we don't use more nodes than is needed to fulfill the coord request. Sort order takes into account
	// that we assign higher-memory coord nodes first
	var selectedCoordNodeMems []int
	for _, nodeName := range n.haveNodes[resource.RoleCrdPropKey] {
		selectedCoordNodeMems = append(selectedCoordNodeMems, coordNodeMemsMi[nodeName])
	}
	sort.Slice(selectedCoordNodeMems, func(i, j int) bool {
		return selectedCoordNodeMems[i] > selectedCoordNodeMems[j]
	})

	requireMem := n.requireCoordMemMi
	selectNode := 0
	for ; selectNode < len(selectedCoordNodeMems); selectNode++ {
		requireMem -= selectedCoordNodeMems[selectNode]
		if requireMem <= 0 {
			break
		}
	}
	if selectNode < len(selectedCoordNodeMems)-1 {
		return fmt.Errorf(
			"%s update was forced to select %d in-use coordinator nodes with %s memory which exceeds requested %s memory from "+
				"requested %d compiles and %d executes by more than a complete node. Consider cancelling jobs, or increasing parallel compiles",
			reqErrPreamble,
			len(selectedCoordNodeMems),
			fmtMem(n.haveCoordMemMi),
			fmtMem(n.requireCoordMemMi),
			n.parallelCompiles,
			n.parallelExecutes)
	}
	return nil
}

func (r *Reconciler) reconcileNSRResourceSpecMode(ctx reconcileCtx, nsr *namespacev1.NamespaceReservation) error {
	req, err := r.createNsrReq(nsr.Name, nsr.Spec.ReservationRequest, nsr.GetWorkloadType())
	if err != nil {
		return r.failNsrRequest(ctx, nsr, err)
	}

	// search for required resources
	if err = r.addSystems(req, nsr.Status.Systems); err != nil {
		return r.failNsrRequest(ctx, nsr, err)
	}
	if err = r.addNGs(req, nsr.Status.Nodegroups, nsr.GetWorkloadType()); err != nil {
		return r.failNsrRequest(ctx, nsr, err)
	}
	_, haveNodes := parseNsrStatusNodes(r.collection, nsr.Status.Nodes)
	if err = r.addCoordNodes(req, common.Keys(haveNodes[resource.RoleCrdPropKey])); err != nil {
		return r.failNsrRequest(ctx, nsr, err)
	}
	if wsapisv1.UseAxScheduling {
		var perStampPortAffinity map[string]map[int]int
		if perStampPortAffinity, err = r.addAxNodes(req, common.Keys(haveNodes[resource.RoleAxPropKey])); err != nil {
			return r.failNsrRequest(ctx, nsr, err)
		}

		if err = r.checkAXPortAffinity(perStampPortAffinity, nsr); err != nil {
			return r.failNsrRequest(ctx, nsr, err)
		}
	}
	if wsapisv1.UseIxScheduling {
		if err = r.addIxNodes(req, common.Keys(haveNodes[resource.RoleIxPropKey]), nsr.GetWorkloadType()); err != nil {
			return r.failNsrRequest(ctx, nsr, err)
		}
	}
	requestedNodes := append(req.haveNodes[resource.RoleCrdPropKey], req.haveNodes[resource.RoleAxPropKey]...)
	requestedNodes = append(requestedNodes, req.haveNodes[resource.RoleIxPropKey]...)
	if nsr.Spec.RequestParams.UpdateMode == namespacev1.SetResourceSpecDryRunMode {
		resp := nsr.Spec.ReservationRequest.DeepCopy()
		resp.RequestParams.SystemNames = req.haveSystems
		resp.RequestParams.NodegroupNames = req.haveNGs
		resp.RequestParams.ActivationNodeNames = req.haveNodes[resource.RoleAxPropKey]
		resp.RequestParams.InferenceDriverNodeNames = req.haveNodes[resource.RoleIxPropKey]
		resp.RequestParams.CoordinatorNodeNames = req.haveNodes[resource.RoleCrdPropKey]
		resp.RequestParams.ManagementNodeNames = req.haveNodes[resource.RoleCrdPropKey]
		sort.Strings(resp.RequestParams.SystemNames)
		sort.Strings(resp.RequestParams.NodegroupNames)
		sort.Strings(resp.RequestParams.ActivationNodeNames)
		sort.Strings(resp.RequestParams.InferenceDriverNodeNames)
		sort.Strings(resp.RequestParams.CoordinatorNodeNames)
		sort.Strings(resp.RequestParams.ManagementNodeNames)
		specEstimate := estimateResourceSpec(r.collection, namespacev1.NamespaceReservationStatus{
			Nodes:      requestedNodes,
			Nodegroups: req.haveNGs,
			Systems:    req.haveSystems,
		}, nsr.GetWorkloadType())
		resp.RequestParams.ParallelCompileCount = common.Pointer(specEstimate.ParallelCompileCount)
		resp.RequestParams.ParallelExecuteCount = common.Pointer(specEstimate.ParallelExecuteCount)
		resp.RequestParams.LargeMemoryRackCount = common.Pointer(specEstimate.LargeMemoryRackCount)
		resp.RequestParams.XLargeMemoryRackCount = common.Pointer(specEstimate.XLargeMemoryRackCount)
		nsr.Status.SetSuccessfulRequest(*resp)
		if err = r.client.Status().Update(ctx, nsr); err != nil {
			return err
		}
	} else {
		prevRIDs := nsr.Status.GetAllocatedRIDs()
		nsr.Status.Nodes = requestedNodes
		nsr.Status.Systems = req.haveSystems
		nsr.Status.Nodegroups = req.haveNGs
		nsr.Status.SetSuccessfulRequest(nsr.Spec.ReservationRequest)
		nsr.Status.CurrentResourceSpec = estimateResourceSpec(r.collection, nsr.Status, nsr.GetWorkloadType())
		if err := r.client.Status().Update(ctx, nsr); err != nil {
			return err
		}
		removed, added := getDiff(prevRIDs, req.getRIDs())
		ctx.modifiedRID.addUnassigned(removed)
		ctx.modifiedRID.addAssigned(added, nsr.Name)
	}

	return nil
}

func (r *Reconciler) createNsrReq(name string, resreq namespacev1.ReservationRequest, workloadType namespacev1.SessionWorkloadType) (*nsrReplaceReq, error) {
	params := resreq.RequestParams
	req := &nsrReplaceReq{
		name: name,
		haveNodes: map[string][]string{
			resource.RoleCrdPropKey: {},
			resource.RoleAxPropKey:  {},
			resource.RoleIxPropKey:  {},
		},
	}

	req.requireSystemCount = params.GetSystemCount()

	req.parallelExecutes = params.GetParallelExecuteCount()
	if req.parallelExecutes == 0 && req.requireSystemCount > 0 {
		req.parallelExecutes = 1
	}
	if req.parallelExecutes > req.requireSystemCount {
		return req, fmt.Errorf("%s requested %d parallel executes which is greater than %d requested systems",
			invalidRequestErr, req.parallelExecutes, req.requireSystemCount)
	}

	req.parallelCompiles = params.GetParallelCompileCount()
	if req.parallelCompiles == 0 && req.requireSystemCount > 0 {
		req.parallelCompiles = 1
	}
	if workloadType != namespacev1.InferenceWorkloadType {
		// inference session does not need node groups
		req.requirePopNGCount = req.parallelExecutes
	}
	req.requireLargeNGCount = params.GetLargeMemoryRackCount()
	req.requireXLargeNGCount = params.GetXLargeMemoryRackCount()
	if req.requireLargeNGCount+req.requireXLargeNGCount > req.requireSystemCount {
		requestedResourceMsg := ""
		if req.requireLargeNGCount > 0 {
			requestedResourceMsg += fmt.Sprintf("%d large memory nodegroups", req.requireLargeNGCount)
		}
		if req.requireXLargeNGCount > 0 {
			if requestedResourceMsg != "" {
				requestedResourceMsg += " and "
			}
			requestedResourceMsg += fmt.Sprintf("%d xlarge memory nodegroups", req.requireXLargeNGCount)
		}
		return req, fmt.Errorf("%s requested %s which is greater than %d requested systems",
			invalidRequestErr, requestedResourceMsg, req.requireSystemCount)
	}

	req.requireCoordMemMi = req.parallelExecutes*perExecuteCrdMemEstimateMi + req.parallelCompiles*perCompileCrdMemEstimateMi
	if duplicates := getDuplicates(params.CoordinatorNodeNames); len(duplicates) > 0 {
		return req, fmt.Errorf("%s requested coordinator node names '%s' more than once", invalidRequestErr, strings.Join(duplicates, ", "))
	}

	if wsapisv1.UseAxScheduling {
		req.requireAxCount = req.requireSystemCount
		if duplicates := getDuplicates(params.ActivationNodeNames); len(duplicates) > 0 {
			// this is only a preemptive message - this should not happen given users won't be able to set ax nodes
			logrus.Errorf("%s requested activation node names '%s' more than once", invalidRequestErr, strings.Join(duplicates, ", "))
		}
	}

	return req, nil
}

type nsrSys struct {
	name           string
	isCurrentOwner bool
	isHealthy      bool
}

func (r *Reconciler) addSystems(req *nsrReplaceReq, currentSystemNames []string) error {
	for _, sys := range currentSystemNames {
		if r.resourceStatus.IsInUse("system/" + sys) {
			req.haveSystems = append(req.haveSystems, sys)
		}
	}
	if len(req.haveSystems) > req.requireSystemCount {
		return fmt.Errorf(
			"%s session requested change from %d to %d systems but %d systems have jobs running and cannot be released from the current session. %s",
			reqErrPreamble, len(currentSystemNames), req.requireSystemCount, len(req.haveSystems), resourcesInUseMsg)
	}

	freeSystems := map[string]nsrSys{}
	systems := r.collection.Filter(resource.NewSystemFilter())
	stampSysCount := map[string]int{}
	sysStampMap := map[string]string{}
	for _, s := range systems.List() {
		ns, _ := s.GetProp(common.NamespaceKey)
		sysName := s.Name()
		if ns != "" && ns != req.name {
			continue
		}
		// calculate v2 group system count for both existing/free systems
		if s.GetStampIndex() != "" {
			stampSysCount[s.GetStampIndex()]++
			sysStampMap[sysName] = s.GetStampIndex()
		}
		if common.Contains(req.haveSystems, sysName) {
			continue
		}
		freeSystems[sysName] = nsrSys{
			name:           sysName,
			isCurrentOwner: ns == req.name,
			isHealthy:      s.IsFullyHealthy(),
		}
	}

	if len(freeSystems) < req.requireSystemCount-len(req.haveSystems) {
		return fmt.Errorf(
			"%s cluster only has %d available of %d total systems but %d requested",
			reqErrPreamble, len(freeSystems), systems.Size(), req.requireSystemCount)
	}

	// minor optimization: assign in order system health -> stamp size -> stamp name -> already owned -> others
	freeSystemsList := common.Values(freeSystems)
	sort.Slice(freeSystemsList, func(i, j int) bool {
		isys, jsys := freeSystemsList[i], freeSystemsList[j]
		if isys.isHealthy != jsys.isHealthy {
			return isys.isHealthy
		}

		// prioritize systems that have a non-empty stamp property for testing purposes
		stampI, stampJ := sysStampMap[isys.name], sysStampMap[jsys.name]
		stampSysCountI, stampSysCountJ := stampSysCount[stampI], stampSysCount[stampJ]
		if stampSysCountI != stampSysCountJ {
			// prioritize the system where its belonging group has more systems
			return stampSysCountI > stampSysCountJ
		} else if stampI != stampJ {
			// prioritize stamp name that is "smaller" in sort function
			return stampI < stampJ
		}
		if isys.isCurrentOwner != jsys.isCurrentOwner {
			return isys.isCurrentOwner
		}
		return isys.name < jsys.name
	})

	remainingSys := req.requireSystemCount - len(req.haveSystems)
	for i := 0; i < remainingSys; i++ {
		req.haveSystems = append(req.haveSystems, freeSystemsList[i].name)
	}
	return nil
}

type nsrNG struct {
	name           string
	ns             string
	isPop          bool
	isLargeMem     bool
	isXLargeMem    bool
	isCurrentOwner bool
}

func (n nsrNG) addNGToNsrReq(req *nsrReplaceReq) {
	req.haveNGs = append(req.haveNGs, n.name)
	if n.isPop {
		req.havePopNGCount += 1
	}
	if n.isLargeMem {
		req.haveLargeMemNGCount += 1
	}
	if n.isXLargeMem {
		req.haveXLargeMemNGCount += 1
	}
}

func nsrNGFromResource(r resource.Resource, currNS string) nsrNG {
	ns, _ := r.GetProp(common.NamespaceKey)
	memClass, _ := r.GetProp(wsapisv1.MemxMemClassProp)
	isPop, _ := r.GetProp(wsapisv1.MemxPopNodegroupProp)
	return nsrNG{
		name:           r.Name(),
		ns:             ns,
		isPop:          isPop == "true",
		isLargeMem:     memClass == wsapisv1.PopNgTypeLarge && isPop == "true",
		isXLargeMem:    memClass == wsapisv1.PopNgTypeXLarge && isPop == "true",
		isCurrentOwner: ns == currNS,
	}
}

func (r *Reconciler) addNGs(req *nsrReplaceReq, currentNgNames []string, workloadType namespacev1.SessionWorkloadType) error {
	// add NG by in-use
	for _, ngName := range currentNgNames {
		rid := resource.NodegroupType + "/" + ngName
		if r.resourceStatus.IsInUse(rid) {
			if rs, ok := r.collection.Get(rid); ok {
				ng := nsrNGFromResource(rs, req.name)
				ng.addNGToNsrReq(req)
			}
		}
	}
	if err := req.checkNGGrantableAfterInUseSelected(); err != nil {
		return err
	}

	if wsapisv1.IsInferenceCluster || workloadType == namespacev1.InferenceWorkloadType {
		// inference clusters do not have node groups; inference session does not need node groups
		logrus.Infof("skip adding nodegroup for inference session %s", req.name)
		return nil
	}

	// add additional NG required by request
	freeNgs := map[string]nsrNG{}
	totalXLargeMem, totalLargeMem, totalPopulated, totalNG, totalFreeNG := 0, 0, 0, 0, 0
	for _, ng := range r.collection.Filter(resource.NewFilter(resource.NodegroupType, nil, nil)).List() {
		n := nsrNGFromResource(ng, req.name)
		isSelectable := n.ns == "" || n.isCurrentOwner
		if isSelectable && !common.Contains(req.haveNGs, ng.Name()) {
			freeNgs[ng.Name()] = n
		}
		if isSelectable {
			totalFreeNG += 1
		}
		if n.isXLargeMem {
			totalXLargeMem += 1
		}
		if n.isLargeMem {
			totalLargeMem += 1
		}
		if n.isPop {
			totalPopulated += 1
		}
		totalNG += 1
	}

	// TODO consider overall group health in allocation order

	needXLargeNG := req.requireXLargeNGCount - req.haveXLargeMemNGCount
	if needXLargeNG > 0 {
		var xlargeMemNgs []nsrNG
		for _, ng := range freeNgs {
			if ng.isXLargeMem {
				xlargeMemNgs = append(xlargeMemNgs, ng)
			}
		}

		if len(xlargeMemNgs) < needXLargeNG {
			return fmt.Errorf(
				"%s cluster only has %d available of %d total xlarge memory racks but %d requested",
				reqErrPreamble, len(xlargeMemNgs), totalLargeMem, needXLargeNG)
		}

		sort.Slice(xlargeMemNgs, func(i, j int) bool {
			ing, jng := xlargeMemNgs[i], xlargeMemNgs[j]
			if ing.isCurrentOwner != jng.isCurrentOwner {
				return ing.isCurrentOwner
			}
			return ing.name < jng.name
		})
		for i := 0; i < needXLargeNG; i++ {
			req.haveNGs = append(req.haveNGs, xlargeMemNgs[i].name)
			delete(freeNgs, xlargeMemNgs[i].name)
			req.havePopNGCount += 1
			req.haveXLargeMemNGCount += 1
		}
	}

	needLargeNG := req.requireLargeNGCount - req.haveLargeMemNGCount
	if needLargeNG > 0 {
		var largeMemNgs []nsrNG
		for _, ng := range freeNgs {
			if ng.isLargeMem {
				largeMemNgs = append(largeMemNgs, ng)
			}
		}

		if len(largeMemNgs) < needLargeNG {
			return fmt.Errorf(
				"%s cluster only has %d available of %d total large memory racks but %d requested",
				reqErrPreamble, len(largeMemNgs), totalLargeMem, needLargeNG)
		}

		sort.Slice(largeMemNgs, func(i, j int) bool {
			ing, jng := largeMemNgs[i], largeMemNgs[j]
			if ing.isCurrentOwner != jng.isCurrentOwner {
				return ing.isCurrentOwner
			}
			return ing.name < jng.name
		})
		for i := 0; i < needLargeNG; i++ {
			req.haveNGs = append(req.haveNGs, largeMemNgs[i].name)
			delete(freeNgs, largeMemNgs[i].name)
			req.havePopNGCount += 1
			req.haveLargeMemNGCount += 1
		}
	}

	needPopNG := req.requirePopNGCount - req.havePopNGCount
	if needPopNG > 0 {
		var popNg []nsrNG
		for _, ng := range freeNgs {
			if ng.isPop {
				popNg = append(popNg, ng)
			}
		}

		if len(popNg) < needPopNG {
			return fmt.Errorf(
				"%s cluster only has %d available of %d total nodegroups capable of running execute jobs but %d requested",
				reqErrPreamble, len(popNg), totalPopulated, needPopNG)
		}

		sort.Slice(popNg, func(i, j int) bool {
			ing, jng := popNg[i], popNg[j]
			if ing.isXLargeMem != jng.isXLargeMem {
				return !ing.isXLargeMem
			}
			if ing.isLargeMem != jng.isLargeMem {
				return !ing.isLargeMem
			}
			if ing.isCurrentOwner != jng.isCurrentOwner {
				return ing.isCurrentOwner
			}
			return ing.name < jng.name
		})

		for i := 0; i < needPopNG; i++ {
			req.haveNGs = append(req.haveNGs, popNg[i].name)
			delete(freeNgs, popNg[i].name)
			req.havePopNGCount += 1
		}
	}

	var needNG int
	if totalFreeNG < req.requireSystemCount {
		needNG = totalFreeNG - len(req.haveNGs) // allow assigning redundant systems in case of more sys than NG
	} else {
		needNG = req.requireSystemCount - len(req.haveNGs)
	}
	if needNG > 0 {
		if len(freeNgs) < needNG {
			return fmt.Errorf(
				"%s cluster only has %d available of %d total nodegroups but %d required",
				reqErrPreamble, len(freeNgs), totalNG, needNG)
		}

		ngs := common.Values(freeNgs)

		// prefer depop -> pop -> largeMem -> xlargeMem
		sort.Slice(ngs, func(i, j int) bool {
			ing, jng := ngs[i], ngs[j]
			if ing.isXLargeMem != jng.isXLargeMem {
				return !ing.isXLargeMem
			}
			if ing.isLargeMem != jng.isLargeMem {
				return !ing.isLargeMem
			}
			if ing.isPop != jng.isPop {
				return !ing.isPop
			}
			if ing.isCurrentOwner != jng.isCurrentOwner {
				return ing.isCurrentOwner
			}
			return ing.name < jng.name
		})

		for i := 0; i < needNG; i++ {
			req.haveNGs = append(req.haveNGs, ngs[i].name)
			delete(freeNgs, ngs[i].name)
		}
	}

	return nil
}

type nsrNode struct {
	name           string
	memMi          int
	isCurrentOwner bool
	isHealthy      bool
}

func (r *Reconciler) addCoordNodes(req *nsrReplaceReq, currentNodeNames []string) error {
	coordNodeMemsMi := map[string]int{} // memory available for compiles/executes on each coord node
	for _, rs := range r.collection.Filter(crdNodeFilter).List() {
		coordNodeMemsMi[rs.Name()] = rs.Subresources().Mem()
	}

	// pre-select coord nodes running jobs
	for _, nodeName := range currentNodeNames {
		rid := resource.NodeType + "/" + nodeName
		if r.resourceStatus.IsInUse(rid) {
			req.haveNodes[resource.RoleCrdPropKey] = append(req.haveNodes[resource.RoleCrdPropKey], nodeName)
			req.haveCoordMemMi += coordNodeMemsMi[nodeName]
		}
	}
	if err := req.checkCoordGrantableAfterInUseSelected(coordNodeMemsMi); err != nil {
		return err
	}

	// add additional coord nodes to meet minimums required by selection criteria
	var freeNodes []nsrNode
	totalMemMi := 0
	for _, node := range r.collection.Filter(crdNodeFilter).List() {
		ns, _ := node.GetProp(common.NamespaceKey)
		if (ns == "" || ns == req.name) && !common.Contains(req.haveNodes[resource.RoleCrdPropKey], node.Name()) {
			freeNodes = append(freeNodes, nsrNode{
				name:           node.Name(),
				memMi:          node.Subresources().Mem(),
				isCurrentOwner: ns == req.name,
				isHealthy:      node.IsFullyHealthy(),
			})
			totalMemMi += node.Subresources().Mem()
		}
	}

	if req.requireCoordMemMi-req.haveCoordMemMi > totalMemMi {
		alreadyAssignedMsg := ""
		if len(req.haveNodes[resource.RoleCrdPropKey]) > 0 {
			alreadyAssignedMsg = fmt.Sprintf(" and %d currently assigned nodes", len(req.haveNodes[resource.RoleCrdPropKey]))
		}
		return fmt.Errorf("%s requested a total of %s coordinator memory capacity but could only allocate %s from %d available nodes%s",
			reqErrPreamble, fmtMem(req.requireCoordMemMi), fmtMem(totalMemMi), len(freeNodes), alreadyAssignedMsg)
	}

	// sort order: healthy, isCurrentOwner, memSize, name
	sort.Slice(freeNodes, func(i, j int) bool {
		inode, jnode := freeNodes[i], freeNodes[j]
		if inode.isHealthy != jnode.isHealthy {
			return inode.isHealthy
		}
		if inode.isCurrentOwner != jnode.isCurrentOwner {
			return inode.isCurrentOwner
		}
		if inode.memMi != jnode.memMi {
			return inode.memMi > jnode.memMi
		}
		return inode.name < jnode.name
	})

	i := 0
	for req.requireCoordMemMi-req.haveCoordMemMi > 0 {
		req.haveNodes[resource.RoleCrdPropKey] = append(req.haveNodes[resource.RoleCrdPropKey], freeNodes[i].name)
		req.haveCoordMemMi += freeNodes[i].memMi
		i += 1
	}

	return nil
}

func (r *Reconciler) addAxNodes(req *nsrReplaceReq, currentNodeNames []string) (map[string]map[int]int, error) {
	// Systems in nsr status could appear where resources are not grouped by stamps
	// When adding ax nodes, we should find the ax nodes that are in the same stamps as the systems
	// for example: perStampSystems["stamp-0"] = []string{"sys-0", "sys-1"}
	perStampSystems := map[string][]string{}
	for _, systemName := range req.haveSystems {
		rid := resource.SystemType + "/" + systemName
		// If we had detected cross-stamp switches or no stamp, we will ignore picking stamp-aware AX nodes later on
		var stampIdx string
		sys, _ := r.collection.Get(rid)
		if val, ok := sys.GetProp(resource.StampPropertyKey); ok {
			stampIdx = val
		}
		perStampSystems[stampIdx] = append(perStampSystems[stampIdx], systemName)
	}

	inUseNodes := map[string][]resource.Resource{}
	for _, nodeName := range currentNodeNames {
		rid := resource.NodeType + "/" + nodeName
		if !r.resourceStatus.IsInUse(rid) {
			continue
		} else if res, ok := r.collection.Get(rid); ok {
			if _, ok = res.GetProp(resource.RoleAxPropKey); !ok {
				continue
			} else if stampIdx, ok := res.GetProp(resource.StampPropertyKey); ok {
				// this is an AX node in stamp which is currently in use
				inUseNodes[stampIdx] = append(inUseNodes[stampIdx], res)
			}
		}
	}

	// Stores the per-stamp AX affinity to CSX ports.
	// for example: perStampPortAffinity["0"] = {1:1, 4:1}, meaning in stamp 0, AX nodes have affinity of 1 to ports 1 and 4.
	perStampPortAffinity := map[string]map[int]int{}

	inUseConflictErrorf := "%s update was forced to select %d in-use activation node(s) in stamp %s which exceeds requested count %d. " +
		"Consider cancelling jobs, or increasing system count"
	for _, stampIdx := range common.SortedKeys(inUseNodes) {
		nodesInStamp := inUseNodes[stampIdx]
		systemsInStamp, ok := perStampSystems[stampIdx]
		numAxNodesNeeded := len(systemsInStamp) * wsapisv1.AxCountPerSystem
		if !ok || len(nodesInStamp) > numAxNodesNeeded {
			// this is only a preemptive error - this should not happen
			return perStampPortAffinity, fmt.Errorf(inUseConflictErrorf, reqErrPreamble, len(nodesInStamp), stampIdx, len(systemsInStamp))
		}
	}
	for _, stampIdx := range common.SortedKeys(perStampSystems) {
		systemsInStamp := perStampSystems[stampIdx]
		numAxNodesNeeded := len(systemsInStamp) * wsapisv1.AxCountPerSystem
		allocated := &resource.Collection{}
		var selectedAxNodes []string

		// pre-select ax nodes in the same stamp which are running jobs
		for _, nodeRes := range inUseNodes[stampIdx] {
			selectedAxNodes = append(selectedAxNodes, nodeRes.Name())
			allocated.Add(nodeRes)
		}

		if len(selectedAxNodes) == numAxNodesNeeded {
			req.haveNodes[resource.RoleAxPropKey] = append(req.haveNodes[resource.RoleAxPropKey], selectedAxNodes...)
			// move on to the next stamp
			continue
		}

		// add additional ax nodes to meet minimums required by selection criteria
		stampFilter := axNodeFilter.Copy()
		// if stamp information is not available (due to no systems installed), we will skip the stamp index filter.
		if common.StandardV2StampsDetected() {
			stampFilter.SetProperty(resource.StampPropertyKey, stampIdx)
		}
		candidates := &resource.Collection{}
		axNodes := r.collection.Filter(stampFilter).List()
		for _, node := range axNodes {
			ns, _ := node.GetProp(common.NamespaceKey)
			if (ns == "" || ns == req.name) && !common.Contains(selectedAxNodes, node.Name()) {
				candidates.Add(node)
			}
		}

		if len(selectedAxNodes)+candidates.Size() < numAxNodesNeeded {
			alreadyAssignedMsg := ""
			if len(selectedAxNodes) > 0 {
				alreadyAssignedMsg = fmt.Sprintf(" and %d currently assigned nodes", len(selectedAxNodes))
			}
			return perStampPortAffinity, fmt.Errorf("%s requested a total of %d activation node(s) in stamp %s "+
				"but could only allocate %d available node(s)%s",
				reqErrPreamble, numAxNodesNeeded, stampIdx, candidates.Size(), alreadyAssignedMsg)
		}

		bestNodes, affinityInStamp := candidates.GetNodesWithMostAffinityCoverage(req.name, "",
			systemsInStamp, allocated, numAxNodesNeeded, nil, nil, common.CSVersionSpec.NumPorts)
		perStampPortAffinity[stampIdx] = affinityInStamp

		for _, res := range bestNodes.List() {
			req.haveNodes[resource.RoleAxPropKey] = append(req.haveNodes[resource.RoleAxPropKey], res.Name())
		}
	}

	return perStampPortAffinity, nil
}

// Check port affinity based on the selected AX nodes.
func (r *Reconciler) checkAXPortAffinity(perStampPortAffinity map[string]map[int]int, nsr *namespacev1.NamespaceReservation) error {
	failNsrBaseOnAffinity := false // if false, we write down suggestions in annotation instead
	if nsr.GetWorkloadType() == namespacev1.InferenceWorkloadType && !nsr.Spec.RequestParams.SuppressAffinityErrors {
		failNsrBaseOnAffinity = true
	}

	portCount := common.CSVersionSpec.NumPorts

	var perStampDetailedAffinity []string // Write detailed affinity as NSR annotation
	var affinitySuggestions []string
	for _, stamp := range common.SortedKeys(perStampPortAffinity) {
		var minAffinity = math.MaxInt
		var maxAffinity = 0
		affinityPairs := make([]string, 0, portCount)
		zeroAffinityPorts := make([]int, 0, portCount)
		for port := 0; port < portCount; port++ {
			affinity := perStampPortAffinity[stamp][port]
			affinityPairs = append(affinityPairs, fmt.Sprintf("port%d:%d", port, affinity))
			if affinity < minAffinity {
				minAffinity = affinity
			}
			if affinity > maxAffinity {
				maxAffinity = affinity
			}
			if affinity == 0 {
				zeroAffinityPorts = append(zeroAffinityPorts, port)
			}
		}
		if len(zeroAffinityPorts) != 0 {
			// Criteria 1: All AX nodes in session need to cover all 12 CS ports
			str := fmt.Sprintf("- Session %s: AX nodes allocated in stamp %s do not have affinity to port(s) %v. "+
				"Please consider adding AX node(s) to cover all ports.", nsr.Name, stamp, zeroAffinityPorts)
			if failNsrBaseOnAffinity {
				return fmt.Errorf(str)
			} else {
				affinitySuggestions = append(affinitySuggestions, str)
			}
		}

		inStampAffinity := "stamp " + stamp + ": [" + strings.Join(affinityPairs, ",") + "]"
		perStampDetailedAffinity = append(perStampDetailedAffinity, inStampAffinity)

		// Criteria 2: The aggregated affinity difference should not be greater than
		// namespacev1.MaxCsPortAffinityDifference
		if maxAffinity-minAffinity > namespacev1.MaxCsPortAffinityDifference {
			str := fmt.Sprintf("- Session %s: AX nodes allocated in stamp %s have aggregated port affinity "+
				"difference of %d, which is greater than threshold %d. Please consider choosing AX nodes with "+
				"balanced affinity.",
				nsr.Name, stamp, maxAffinity-minAffinity, namespacev1.MaxCsPortAffinityDifference)
			if failNsrBaseOnAffinity {
				return fmt.Errorf(str)
			} else {
				affinitySuggestions = append(affinitySuggestions, str)
			}
		}
	}

	// Write to NSR annotation
	annotations := common.EnsureMap(nsr.GetAnnotations())
	for idx, inStampAffinity := range perStampDetailedAffinity {
		annoKey := fmt.Sprintf("%s-%d", namespacev1.SessionPortAffinityAnnoKeyPrefix, idx)
		annotations[annoKey] = inStampAffinity
	}
	for idx, suggestion := range affinitySuggestions {
		annoKey := fmt.Sprintf("%s-%d", namespacev1.SessionWarningAnnoKeyPrefix, idx)
		annotations[annoKey] = suggestion
	}
	nsr.SetAnnotations(annotations)

	return nil
}

// addIxNodes tries to place IX node in the stamp with the most systems;
// if no IX available in the best stamp, try the next best stamp.
func (r *Reconciler) addIxNodes(req *nsrReplaceReq, currentNodeNames []string, workloadType namespacev1.SessionWorkloadType) error {
	// this slice serves as the output, and should always stay empty when passed in
	req.haveNodes[resource.RoleIxPropKey] = []string{}

	if workloadType != namespacev1.InferenceWorkloadType || len(req.haveSystems) == 0 {
		// For non-inference session, IX nodes are not needed; for inference, remove IX node(s) if removing all systems
		return nil
	}

	// currently each inference session only requires one IX node
	numIxNodesNeeded := 1

	var selectedIxNodes []string
	// IX nodes currently in use need to stay chosen, we should not release them
	for _, nodeName := range currentNodeNames {
		rid := resource.NodeType + "/" + nodeName
		if !r.resourceStatus.IsInUse(rid) {
			continue
		} else if res, ok := r.collection.Get(rid); ok {
			if _, ok = res.GetProp(resource.RoleIxPropKey); !ok {
				continue
			} else {
				selectedIxNodes = append(selectedIxNodes, res.Name())
			}
		}
	}
	numIxNodesInUse := len(selectedIxNodes) // for logging purpose

	if len(selectedIxNodes) >= numIxNodesNeeded {
		// in-use IX node count already satisfied the requirement, return directly
		req.haveNodes[resource.RoleIxPropKey] = append(req.haveNodes[resource.RoleIxPropKey], selectedIxNodes...)
		return nil
	}

	// count systems in each stamp
	perStampSystemCount := map[string]int{}
	for _, systemName := range req.haveSystems {
		rid := resource.SystemType + "/" + systemName
		// If we had detected cross-stamp switches or no stamp, we will ignore picking stamp-aware IX nodes later on
		var stampName string
		sys, _ := r.collection.Get(rid)
		if val, ok := sys.GetProp(resource.StampPropertyKey); ok {
			stampName = val
		}
		perStampSystemCount[stampName]++
	}

	// order stamp names based on number of systems in the stamp, descending order
	type stampCountPair struct {
		stampName string
		count     int
	}
	var stampCountPairs []stampCountPair
	for stampName, count := range perStampSystemCount {
		stampCountPairs = append(stampCountPairs, stampCountPair{stampName, count})
	}
	sort.Slice(stampCountPairs, func(i, j int) bool {
		if stampCountPairs[i].count != stampCountPairs[j].count {
			return stampCountPairs[i].count > stampCountPairs[j].count
		} else {
			// tie-breaker using stamp name
			return stampCountPairs[i].stampName < stampCountPairs[j].stampName
		}
	})
	var stampsOrderedBySystemCount []string
	for _, pair := range stampCountPairs {
		stampsOrderedBySystemCount = append(stampsOrderedBySystemCount, pair.stampName)
	}

	// select IX - always try in the best stamp (with the most systems). If not possible, try the next best stamp.
	for _, stampName := range stampsOrderedBySystemCount {
		if len(selectedIxNodes) >= numIxNodesNeeded {
			break
		}
		stampFilter := ixNodeFilter.Copy()
		// if stamp information is not available (due to no systems installed), we will skip the stamp index filter.
		if common.StandardV2StampsDetected() {
			stampFilter.SetProperty(resource.StampPropertyKey, stampName)
		}
		var candidates []string
		ixNodes := r.collection.Filter(stampFilter).List()
		for _, node := range ixNodes {
			ns, _ := node.GetProp(common.NamespaceKey)
			if (ns == "" || ns == req.name) && !common.Contains(selectedIxNodes, node.Name()) {
				candidates = append(candidates, node.Name())
			}
		}
		// tie-breaker using IX node name
		sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })

		for _, ixNode := range candidates {
			selectedIxNodes = append(selectedIxNodes, ixNode)
			if len(selectedIxNodes) >= numIxNodesNeeded {
				break
			}
		}
	}

	if len(selectedIxNodes) < numIxNodesNeeded {
		alreadyAssignedMsg := ""
		if numIxNodesInUse > 0 {
			alreadyAssignedMsg = fmt.Sprintf(" and %d currently assigned nodes", numIxNodesInUse)
		}
		return fmt.Errorf("%s requested a total of %d inference driver node(s) in cluster "+
			"but could only allocate %d available node(s)%s",
			reqErrPreamble, numIxNodesNeeded, len(selectedIxNodes)-numIxNodesInUse, alreadyAssignedMsg)
	}

	req.haveNodes[resource.RoleIxPropKey] = append(req.haveNodes[resource.RoleIxPropKey], selectedIxNodes...)
	return nil
}

func (r *Reconciler) failNsrRequest(ctx reconcileCtx, nsr *namespacev1.NamespaceReservation, err error) error {
	nsr.Status.SetFailedRequest(nsr.Spec.ReservationRequest, err.Error())
	return r.client.Status().Update(ctx, nsr)
}

func floor0(i int) int {
	if i < 0 {
		return 0
	}
	return i
}

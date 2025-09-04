package csadm

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/duration"
	"sigs.k8s.io/yaml"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"

	nsapisv1 "cerebras.com/job-operator/apis/namespace/v1"
	rlapiv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/csctl"
	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

var (
	clientOutOfDateError = grpcStatus.Error(
		codes.InvalidArgument,
		"argument updateMode was not set. Likely csctl client is out of date. Please upgrade csctl to latest")

	updateModePbTok8 = map[pb.SessionUpdateMode]nsapisv1.NSRUpdateMode{
		pb.SessionUpdateMode_SET_RESOURCE_SPEC:        nsapisv1.SetResourceSpecMode,
		pb.SessionUpdateMode_SET_RESOURCE_SPEC_DRYRUN: nsapisv1.SetResourceSpecDryRunMode,
		pb.SessionUpdateMode_DEBUG_APPEND:             nsapisv1.DebugAppendMode,
		pb.SessionUpdateMode_DEBUG_REMOVE:             nsapisv1.DebugRemoveMode,
		pb.SessionUpdateMode_DEBUG_REMOVE_ALL:         nsapisv1.DebugRemoveAllMode,
		pb.SessionUpdateMode_DEBUG_SET:                nsapisv1.DebugSetMode,
		pb.SessionUpdateMode_UNSPECIFIED:              nsapisv1.ModeNotSet,
	}
	updateModek8ToPb = map[nsapisv1.NSRUpdateMode]pb.SessionUpdateMode{
		nsapisv1.SetResourceSpecMode:       pb.SessionUpdateMode_SET_RESOURCE_SPEC,
		nsapisv1.SetResourceSpecDryRunMode: pb.SessionUpdateMode_SET_RESOURCE_SPEC_DRYRUN,
		nsapisv1.DebugRemoveMode:           pb.SessionUpdateMode_DEBUG_REMOVE,
		nsapisv1.DebugRemoveAllMode:        pb.SessionUpdateMode_DEBUG_REMOVE_ALL,
		nsapisv1.DebugAppendMode:           pb.SessionUpdateMode_DEBUG_APPEND,
		nsapisv1.DebugSetMode:              pb.SessionUpdateMode_DEBUG_SET,
		nsapisv1.ModeNotSet:                pb.SessionUpdateMode_UNSPECIFIED,
	}
)

// validateSessionName checks if namespace already exists and is not a user namespace. Prevents ns like 'kube-system'
// from having a session associated with it
func (s *Server) validateSessionName(ctx context.Context, name string) error {
	ns, err := s.k8s.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return pkg.MapStatusError(err)
	}

	_, isUserNs := ns.Labels[nsapisv1.UserNSLabel]
	_, isSysNs := ns.Labels[nsapisv1.SystemNSLabel]
	if !isUserNs && !isSysNs {
		return grpcStatus.Errorf(codes.InvalidArgument,
			fmt.Sprintf("invalid session name: '%s' is reserved for system use", name))
	}
	return nil
}

func (s *Server) CreateSession(ctx context.Context, req *pb.CreateSessionRequest) (*pb.Session, error) {
	if err := s.validateSessionName(ctx, req.Name); err != nil {
		return nil, err
	}

	requestUid := wsclient.GenUUID()
	requestParams, err := extractParams(ctx, req.Spec)
	if err != nil {
		return nil, err
	}
	if requestParams.UpdateMode == nsapisv1.ModeNotSet {
		return nil, clientOutOfDateError
	}

	labels := mapLabels(map[string]string{}, req.UserLabelOperations)
	createReq := &nsapisv1.NamespaceReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:   req.Name,
			Labels: labels,
		},
		Spec: nsapisv1.NamespaceReservationSpec{
			ReservationRequest: nsapisv1.ReservationRequest{
				RequestParams: requestParams,
				RequestUid:    requestUid,
			},
		},
	}
	if req.Properties[nsapisv1.IsRedundantModeKey] == "true" {
		createReq.Labels[nsapisv1.IsRedundantModeKey] = "true"
	}
	if req.Properties[nsapisv1.SessionWorkloadTypeKey] != "" {
		createReq.Labels[nsapisv1.SessionWorkloadTypeKey] = req.Properties[nsapisv1.SessionWorkloadTypeKey]
	}

	res, err := s.nsrCli.Create(ctx, createReq, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			return nil, grpcStatus.Errorf(codes.AlreadyExists, "session already exists")
		}
		log.Errorf("create session error %v, %v", createReq, err)
		return nil, pkg.MapStatusError(err)
	}

	return s.watchUpdate(ctx, requestUid, res)
}

func mapLabels(existingLabels map[string]string, labelOperations []*pb.UserLabelOperation) map[string]string {
	ret := map[string]string{}
	// Since these are existing labels, we shouldn't apply any key changes
	for key, val := range existingLabels {
		ret[key] = val
	}
	for _, operation := range labelOperations {
		newKey := csctl.K8sLabelPrefix + operation.Key
		switch operation.Op {
		case pb.UserLabelOperation_UPSERT:
			ret[newKey] = operation.Value
		case pb.UserLabelOperation_REMOVE:
			delete(ret, newKey)
		}
	}

	return ret
}

func (s *Server) UpdateSession(ctx context.Context, req *pb.UpdateSessionRequest) (*pb.Session, error) {
	res, err := s.nsrCli.Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		if req.Spec.UpdateMode != pb.SessionUpdateMode_SET_RESOURCE_SPEC && k8serrors.IsNotFound(err) {
			// allow debug mode commands to create-if-not-exists
			return s.CreateSession(ctx, &pb.CreateSessionRequest{Name: req.Name, Spec: req.Spec})
		} else {
			return nil, handleSessionErr(err, req.Name)
		}
	}
	if _, ok := req.Properties[nsapisv1.AssignedRedundancyKey]; ok {
		return s.sessionRedundancyUpdate(ctx, res, req.Properties[nsapisv1.AssignedRedundancyKey])
	}

	requestUid := wsclient.GenUUID()
	// fail if another req is in progress
	if res.Spec.RequestUid != res.Status.GetLastAttemptedRequest().Request.RequestUid {
		return nil, grpcStatus.Errorf(codes.FailedPrecondition,
			"session is being updated by another request %s, please retry later", res.Spec.RequestUid)
	}
	updateReq := res.DeepCopy()
	updateReq.Spec.RequestParams, err = extractParams(ctx, req.Spec)
	if err != nil {
		return nil, err
	}
	if updateReq.Spec.RequestParams.UpdateMode == nsapisv1.ModeNotSet {
		return nil, clientOutOfDateError
	}
	if updateReq.Spec.RequestParams.IsReleaseAllResources() {
		if err = s.redundancyReferenceCheck(ctx, res); err != nil {
			return nil, err
		}
	}

	updateReq.Spec.RequestUid = requestUid
	updateReq.Labels = mapLabels(updateReq.Labels, req.UserLabelOperations)
	if res, err = s.nsrCli.Update(ctx, updateReq, metav1.UpdateOptions{}); err != nil {
		if k8serrors.IsConflict(err) {
			return nil, grpcStatus.Errorf(codes.FailedPrecondition,
				"session is being updated by another request, please retry later")
		}
		log.Errorf("update session error %v, %v", updateReq, err)
		return nil, err
	}

	return s.watchUpdate(ctx, requestUid, res)
}

func (s *Server) GetSession(ctx context.Context, req *pb.GetSessionRequest) (*pb.Session, error) {
	// Get namespace from the context
	currentNamespace := pkg.GetNamespaceFromContext(ctx)

	// Permission rules:
	// 1. Allow if requested session is the system namespace (anyone can access system namespace)
	// 2. Allow if requested session matches the user's namespace (users can access their own namespace)
	// 3. Deny all other cases (users cannot access other users' namespaces)
	if wscommon.Namespace != wscommon.SystemNamespace && req.Name != currentNamespace {
		return nil, grpcStatus.Errorf(codes.PermissionDenied,
			"access denied: you are not allowed to access sessions other than '%s'", currentNamespace)
	}

	res, err := s.nsrCli.Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return nil, handleSessionErr(err, req.Name)
	}
	return s.nsrToSession(*res)
}

func (s *Server) ListSession(ctx context.Context, req *pb.ListSessionRequest) (*pb.ListSessionResponse, error) {
	var res, resUnassigned, resRegular *nsapisv1.NamespaceReservationList
	var err error
	listOpt := metav1.ListOptions{}
	if req.Redundant {
		listOpt.LabelSelector = fmt.Sprintf("%s=true", nsapisv1.IsRedundantModeKey)
	}
	if resRegular, err = s.nsrCli.List(ctx, listOpt); err != nil {
		return nil, pkg.MapStatusError(err)
	}

	res = resRegular
	// skip unassigned res if list by redundant only
	if !req.Redundant {
		if resUnassigned, err = s.fetchUnassignedNSR(ctx); err != nil {
			return nil, err
		}
		// Based on the presence of the "--unassigned-only" flag, either (1) ONLY show unassigned, or (2) concatenate unassigned and regular NSRs
		if req.ShowUnassigned {
			res = resUnassigned
		} else {
			res = &nsapisv1.NamespaceReservationList{Items: append(res.Items, resUnassigned.Items...)}
		}
	}
	sessions, err := s.NsrToSessionList(res)
	if err != nil {
		return nil, err
	}
	if err = s.appendJobQueueStatuses(sessions); err != nil {
		return nil, err
	}
	if err = s.appendLastActiveStatuses(sessions); err != nil {
		return nil, err
	}
	return &pb.ListSessionResponse{Items: sessions}, nil
}

func (s *Server) sessionRedundancyUpdate(ctx context.Context,
	session *nsapisv1.NamespaceReservation, sessionNames string) (*pb.Session, error) {
	if session.Labels[nsapisv1.IsRedundantModeKey] == "true" {
		return nil, grpcStatus.Errorf(codes.FailedPrecondition,
			"session %s is in redundant mode itself, can't add redundancy", session.Name)
	}
	assignSessionMap := map[string]bool{}
	for _, name := range strings.Split(sessionNames, ",") {
		if name == "" {
			continue
		}
		assignSessionMap[name] = true
		ss, err := s.nsrCli.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, handleSessionErr(err, name)
		}
		if ss.Labels[nsapisv1.IsRedundantModeKey] != "true" {
			return nil, grpcStatus.Errorf(codes.FailedPrecondition,
				"session %s is not in redundant mode and can't be assigned, "+
					"please run `csctl session list --redundant` to check available redundant sessions", name)
		}
	}
	// fail redundancy removal if session have running/queueing jobs
	for _, existing := range session.GetAssignedRedundancy() {
		if !assignSessionMap[existing] && s.lockInformer != nil {
			locks, err := s.lockInformer.Lister().ByNamespace(session.Name).List(labels.NewSelector())
			if err != nil {
				return nil, pkg.MapStatusError(err)
			}
			if len(locks) == 0 {
				break
			}
			return nil, grpcStatus.Errorf(codes.FailedPrecondition,
				"remove redundancy '%s' is not supported when session '%s' has active jobs,"+
					"please cancel jobs or wait till jobs complete", existing, session.Name)
		}
	}
	nsr, err := s.nsrCli.Patch(ctx,
		session.Name, types.MergePatchType,
		wscommon.GetLabelPatch(map[string]string{
			nsapisv1.AssignedRedundancyKey: strings.Join(wscommon.SortedKeys(assignSessionMap), wsapisv1.LabelSeparator)}),
		metav1.PatchOptions{})
	if err != nil {
		return nil, err
	}
	return s.nsrToSession(*nsr)
}

func (s *Server) fetchUnassignedNSR(ctx context.Context) (*nsapisv1.NamespaceReservationList, error) {
	cm, err := s.k8s.CoreV1().ConfigMaps(wscommon.SystemNamespace).Get(ctx, nsapisv1.UnassignedResourcesCM, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, grpcStatus.Errorf(codes.Internal, "unexpected state: missing unassigned resources configmap")
		} else {
			return nil, pkg.MapStatusError(err)
		}
	}

	cmData := cm.Data[nsapisv1.UnassignedResourcesCMDataKey]
	nsr := nsapisv1.NamespaceReservation{}
	err = yaml.Unmarshal([]byte(cmData), &nsr)
	if err != nil {
		log.Errorf("failed to unmarshal contents of CM %s: %s", nsapisv1.UnassignedResourcesCM, err.Error())
		return nil, grpcStatus.Errorf(codes.Internal, "unexpected state: unable to parse unassigned resources configmap")
	}
	return &nsapisv1.NamespaceReservationList{Items: []nsapisv1.NamespaceReservation{nsr}}, nil
}

func (s *Server) DeleteSession(ctx context.Context, req *pb.DeleteSessionRequest) (*pb.DeleteSessionResponse, error) {
	if req.Name == wscommon.SystemNamespace {
		return nil, grpcStatus.Errorf(
			codes.InvalidArgument, "Session '%s' is the default session and cannot be deleted", wscommon.SystemNamespace)
	}

	res, err := s.nsrCli.Get(ctx, req.Name, metav1.GetOptions{})
	if err != nil {
		return nil, handleSessionErr(err, req.Name)
	}
	if err = s.redundancyReferenceCheck(ctx, res); err != nil {
		return nil, err
	}

	if s.lockInformer != nil {
		rl, err := s.lockInformer.Lister().ByNamespace(req.Name).List(labels.Everything())
		if err != nil {
			return nil, pkg.MapStatusError(err)
		}
		for _, l := range rl {
			lock := *l.(*rlapiv1.ResourceLock)
			if lock.Status.State == rlapiv1.LockPending || lock.Status.State == rlapiv1.LockGranted {
				return nil, grpcStatus.Error(
					codes.FailedPrecondition,
					"session has running jobs and cannot be deleted. Cancel jobs or await job completion")
			}
		}
	}
	err = s.nsrCli.Delete(ctx, req.Name, metav1.DeleteOptions{})
	if err != nil {
		return nil, handleSessionErr(err, req.Name)
	}
	return &pb.DeleteSessionResponse{Message: fmt.Sprintf("Successfully deleted session '%s'.", req.Name)}, nil
}

// ensure redundant session only cleaned after no reference
func (s *Server) redundancyReferenceCheck(ctx context.Context, req *nsapisv1.NamespaceReservation) error {
	if req.Labels[nsapisv1.IsRedundantModeKey] != "true" {
		return nil
	}
	nsrList, err := s.nsrCli.List(ctx, metav1.ListOptions{})
	if err != nil {
		return pkg.MapStatusError(err)
	}
	var assignedSessions []string
	for _, nsr := range nsrList.Items {
		if slices.Contains(nsr.GetAssignedRedundancy(), req.Name) {
			assignedSessions = append(assignedSessions, nsr.Name)
		}
	}
	sort.Strings(assignedSessions)
	if len(assignedSessions) > 0 {
		return grpcStatus.Errorf(
			codes.FailedPrecondition,
			"Session '%s' is assigned as redundancy to sessions:%s, "+
				"please unassign first before release all resources or delete",
			req.Name,
			assignedSessions)
	}
	return nil
}

func (s *Server) watchUpdate(ctx context.Context, requestUid string, res *nsapisv1.NamespaceReservation) (*pb.Session, error) {
	w, err := s.nsrCli.Watch(ctx, metav1.ListOptions{
		Watch:           true,
		ResourceVersion: res.ResourceVersion,
		FieldSelector:   "metadata.name=" + res.Name,
	})
	if err != nil {
		return nil, pkg.MapStatusError(err)
	}
	log.Infof("watch on NamespaceReservation %s (rv=%s) started.", res.Name, res.ResourceVersion)
	defer w.Stop()
	timer := time.NewTimer(time.Duration(s.serverOpts.SessionUpdateTimeoutSecs) * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-w.ResultChan():
			if !ok {
				log.Errorf("watch on NamespaceReservation %s (rv=%s) closed.", res.Name, res.ResourceVersion)
				return nil, grpcStatus.Errorf(codes.Internal,
					"failed to get session's latest status: %s. %s", res.Name, pkg.SysAdminSupportMsg)
			}
			nsr, ok := event.Object.(*nsapisv1.NamespaceReservation)
			if !ok {
				continue
			}
			res = nsr
			lastAttempt := res.Status.GetLastAttemptedRequest()
			log.Infof("watched NamespaceReservation %s server update, expected requestUid=%s, attempt=%v", res.Name, requestUid, lastAttempt)
			if res.Spec.RequestUid == lastAttempt.Request.RequestUid {
				op := pkg.Ternary(len(res.Status.UpdateHistory) == 1, "create", "update")
				if lastAttempt.Succeeded {
					if res.Spec.ReservationRequest.RequestParams.UpdateMode == nsapisv1.SetResourceSpecDryRunMode {
						return s.nsrToSessionDryRun(*res)
					} else {
						return s.nsrToSession(*res)
					}
				} else {
					return nil, grpcStatus.Errorf(codes.FailedPrecondition, "session %s failed: %s", op, lastAttempt.Message)
				}
			}
		case <-timer.C:
			return nil, grpcStatus.Errorf(codes.Internal,
				"timed out waiting for session to complete create/update. %s", pkg.SysAdminSupportMsg)
		}
	}
}

func (s *Server) appendJobQueueStatuses(sessions []*pb.Session) error {
	if s.lockInformer == nil {
		return nil
	}
	var locks []runtime.Object
	var err error
	if len(sessions) == 1 {
		locks, err = s.lockInformer.Lister().ByNamespace(sessions[0].Name).List(labels.NewSelector())
	} else {
		locks, err = s.lockInformer.Lister().List(labels.NewSelector())
	}
	if err != nil {
		return err
	}

	m := map[string]*pb.Session{}
	for _, session := range sessions {
		if session.Status == nil {
			session.Status = &pb.RuntimeStatus{}
		}
		m[session.Name] = session
	}

	for _, l := range locks {
		lock := *l.(*rlapiv1.ResourceLock)
		if m[lock.Namespace] == nil {
			continue
		}

		if lock.Status.State == rlapiv1.LockGranted {
			m[lock.Namespace].Status.RunningJobCount++
		} else if lock.Status.State == rlapiv1.LockPending {
			m[lock.Namespace].Status.QueueingJobCount++
		}
		m[lock.Namespace].Status.InactiveDuration = "active"
	}
	return nil
}

func (s *Server) appendLastActiveStatuses(sessions []*pb.Session) error {
	if s.wsJobInformer == nil {
		return nil
	}

	defaultDuration := fmt.Sprintf(">%d days", wsapisv1.DefaultWsjobTTLDays)
	for _, session := range sessions {
		if session.Status == nil {
			session.Status = &pb.RuntimeStatus{}
		}
		if session.Status.RunningJobCount > 0 || session.Status.QueueingJobCount > 0 {
			session.Status.InactiveDuration = "active" // job started
			continue
		}
		jobs, err := s.wsJobInformer.Lister().ByNamespace(session.Name).List(labels.NewSelector())
		if err != nil {
			return err
		}
		session.Status.InactiveDuration = defaultDuration
		var latestCompletedTime *metav1.Time
		for _, job := range jobs {
			j := *job.(*wsapisv1.WSJob)
			if j.Status.CompletionTime == nil {
				session.Status.InactiveDuration = "active" // job started
				break
			}
			if latestCompletedTime == nil || latestCompletedTime.Before(j.Status.CompletionTime) {
				latestCompletedTime = j.Status.CompletionTime
			}
		}
		if session.Status.InactiveDuration != "active" && latestCompletedTime != nil {
			session.Status.InactiveDuration = duration.HumanDuration(time.Now().Sub(latestCompletedTime.Time))
		}
	}
	return nil
}

func (s *Server) nsrToSession(nsr nsapisv1.NamespaceReservation) (*pb.Session, error) {
	nodeMap, err := s.getNodeMap()
	if err != nil {
		return nil, err
	}
	session := s.NsrToSession(nsr, nodeMap)
	if err = s.appendJobQueueStatuses([]*pb.Session{session}); err != nil {
		return nil, err
	}
	if err = s.appendLastActiveStatuses([]*pb.Session{session}); err != nil {
		return nil, err
	}
	return session, nil
}

// dryrun session responses are meant for parsing by client for creation of a user-confirm dialogue.
// return 2 pseudo histories containing the dry-run response information:
// history[0]: resources which the session would have if dry run were effective
// history[1]: capabilities which the session would have if dry run were effective
func (s *Server) nsrToSessionDryRun(nsr nsapisv1.NamespaceReservation) (*pb.Session, error) {
	if len(nsr.Status.UpdateHistory) == 0 {
		return nil, grpcStatus.Error(codes.Internal, "invalid response returned from job-operator")
	}

	dryRunUpdate := nsr.Status.UpdateHistory[0]
	uint32p := func(i *int) uint32 {
		if i == nil || *i < 0 {
			return 0
		}
		return uint32(*i)
	}

	nodeMap, err := s.getNodeMap()
	if err != nil {
		return nil, err
	}
	sessionResource := getSessionResources(nsr, nodeMap)

	sessionDryRun := &pb.Session{
		Name: nsr.Name,
		State: &pb.SessionState{
			Resources: sessionResource,
			CurrentResourceSpec: &pb.SessionResourceSpec{
				SystemCount:           uint32(len(nsr.Status.Systems)),
				ParallelExecuteCount:  uint32(nsr.Status.CurrentResourceSpec.ParallelExecuteCount),
				ParallelCompileCount:  uint32(nsr.Status.CurrentResourceSpec.ParallelCompileCount),
				LargeMemoryRackCount:  uint32(nsr.Status.CurrentResourceSpec.LargeMemoryRackCount),
				XlargeMemoryRackCount: uint32(nsr.Status.CurrentResourceSpec.XLargeMemoryRackCount),
			},
			UpdateHistory: []*pb.RequestStatus{
				{
					Spec: &pb.SessionSpec{
						UpdateMode: pb.SessionUpdateMode_DEBUG_SET,
						UpdateParams: &pb.SessionSpec_Resources{
							Resources: &pb.SessionResources{
								Systems:              dryRunUpdate.Request.RequestParams.SystemNames,
								Nodegroups:           dryRunUpdate.Request.RequestParams.NodegroupNames,
								CoordinatorNodes:     dryRunUpdate.Request.RequestParams.CoordinatorNodeNames,
								ActivationNodes:      dryRunUpdate.Request.RequestParams.ActivationNodeNames,
								InferenceDriverNodes: dryRunUpdate.Request.RequestParams.InferenceDriverNodeNames,
								ManagementNodes:      dryRunUpdate.Request.RequestParams.ManagementNodeNames,
							},
						},
					},
				},
				{
					Spec: &pb.SessionSpec{
						UpdateMode: pb.SessionUpdateMode_SET_RESOURCE_SPEC,
						UpdateParams: &pb.SessionSpec_ResourceSpec{
							ResourceSpec: &pb.SessionResourceSpec{
								SystemCount:           uint32(len(dryRunUpdate.Request.RequestParams.SystemNames)),
								LargeMemoryRackCount:  uint32p(dryRunUpdate.Request.RequestParams.LargeMemoryRackCount),
								XlargeMemoryRackCount: uint32p(dryRunUpdate.Request.RequestParams.XLargeMemoryRackCount),
								ParallelCompileCount:  uint32p(dryRunUpdate.Request.RequestParams.ParallelCompileCount),
								ParallelExecuteCount:  uint32p(dryRunUpdate.Request.RequestParams.ParallelExecuteCount),
							},
						},
					},
				},
			},
		},
	}
	return sessionDryRun, nil
}

func (s *Server) NsrToSession(nsr nsapisv1.NamespaceReservation, nodeMap map[string]*wscommon.Node) *pb.Session {
	props := make(map[string]string)
	if nsr.Labels != nil {
		for k, v := range nsr.Labels {
			props[k] = v
		}
	}
	if nsr.Annotations != nil {
		for k, v := range nsr.Annotations {
			props[k] = v
		}
	}

	session := &pb.Session{
		Name: nsr.Name,
		State: &pb.SessionState{
			Resources: getSessionResources(nsr, nodeMap),
			CurrentResourceSpec: &pb.SessionResourceSpec{
				SystemCount:           uint32(len(nsr.Status.Systems)),
				LargeMemoryRackCount:  uint32(nsr.Status.CurrentResourceSpec.LargeMemoryRackCount),
				XlargeMemoryRackCount: uint32(nsr.Status.CurrentResourceSpec.XLargeMemoryRackCount),
				ParallelCompileCount:  uint32(nsr.Status.CurrentResourceSpec.ParallelCompileCount),
				ParallelExecuteCount:  uint32(nsr.Status.CurrentResourceSpec.ParallelExecuteCount),
			},
		},
		Properties: props,
	}

	for _, res := range nsr.Status.UpdateHistory {
		if res.Request.RequestParams.UpdateMode == nsapisv1.SetResourceSpecDryRunMode {
			continue // filter dry runs
		}
		session.State.UpdateHistory = append(session.State.UpdateHistory, &pb.RequestStatus{
			Succeeded:     res.Succeeded,
			Message:       res.Message,
			Spec:          ParamsToSessionSpec(&res.Request.RequestParams),
			RequestUid:    res.Request.RequestUid,
			ReconcileTime: timestamppb.New(res.ReconcileTimestamp.Time),
		})
	}
	return session
}

func (s *Server) NsrToSessionList(nsrList *nsapisv1.NamespaceReservationList) ([]*pb.Session, error) {
	nodeMap, err := s.getNodeMap()
	if err != nil {
		return nil, err
	}
	var sessionList []*pb.Session
	for _, nsr := range nsrList.Items {
		session := s.NsrToSession(nsr, nodeMap)
		sessionList = append(sessionList, session)
	}
	// sort by syscount/ngcount/nodecount desc, name asc
	sort.Slice(sessionList, func(i, j int) bool {
		si, sj := sessionList[i], sessionList[j]
		// Sort unassigned sessions at the end always
		if si.Name == nsapisv1.UnassignedNamespace {
			return false
		}
		isys, jsys := len(si.State.Resources.Systems), len(sj.State.Resources.Systems)
		if isys != jsys {
			return isys > jsys
		}
		ing, jng := len(si.State.Resources.Nodegroups), len(sj.State.Resources.Nodegroups)
		if ing != jng {
			return ing > jng
		}
		in, jn := len(si.State.Resources.CoordinatorNodes), len(sj.State.Resources.CoordinatorNodes)
		if in != jn {
			return in > jn
		}
		in, jn = len(si.State.Resources.ManagementNodes), len(sj.State.Resources.ManagementNodes)
		if in != jn {
			return in > jn
		}
		in, jn = len(si.State.Resources.ActivationNodes), len(sj.State.Resources.ActivationNodes)
		if in != jn {
			return in > jn
		}
		in, jn = len(si.State.Resources.InferenceDriverNodes), len(sj.State.Resources.InferenceDriverNodes)
		if in != jn {
			return in > jn
		}
		return si.Name < sj.Name
	})
	return sessionList, nil
}

func (s *Server) getNodeMap() (map[string]*wscommon.Node, error) {
	cfg, err := s.cfgProvider.GetCfg(context.Background())
	if err != nil {
		return nil, err
	}
	return cfg.NodeMap, nil
}

func getSessionResources(nsr nsapisv1.NamespaceReservation, nodeMap map[string]*wscommon.Node) *pb.SessionResources {
	coordNodes := []string{}
	axNodes := []string{}
	ixNodes := []string{}
	for _, nodeName := range nsr.Status.Nodes {
		if no, ok := nodeMap[nodeName]; ok {
			if no.HasRole(wscommon.RoleCoordinator) {
				coordNodes = append(coordNodes, nodeName)
			} else if no.Role == wscommon.RoleActivation {
				axNodes = append(axNodes, nodeName)
			} else if no.Role == wscommon.RoleInferenceDriver {
				ixNodes = append(ixNodes, nodeName)
			}
		}
	}
	return &pb.SessionResources{
		Systems:              nsr.Status.Systems,
		Nodegroups:           nsr.Status.Nodegroups,
		CoordinatorNodes:     coordNodes,
		ActivationNodes:      axNodes,
		InferenceDriverNodes: ixNodes,
		ManagementNodes:      coordNodes,
	}
}

func ParamsToSessionSpec(params *nsapisv1.RequestParams) *pb.SessionSpec {
	var updateMode pb.SessionUpdateMode
	if m, ok := updateModek8ToPb[params.UpdateMode]; ok {
		updateMode = m
	} else {
		m = pb.SessionUpdateMode_UNKNOWN
	}

	sessionSpec := &pb.SessionSpec{
		UpdateMode:             updateMode,
		SuppressAffinityErrors: params.SuppressAffinityErrors,
	}
	if updateMode == pb.SessionUpdateMode_SET_RESOURCE_SPEC || updateMode == pb.SessionUpdateMode_UNSPECIFIED {
		sessionSpec.UpdateParams = &pb.SessionSpec_ResourceSpec{
			ResourceSpec: &pb.SessionResourceSpec{
				SystemCount:           uint32(params.GetSystemCount()),
				ParallelExecuteCount:  uint32(params.GetParallelExecuteCount()),
				ParallelCompileCount:  uint32(params.GetParallelCompileCount()),
				LargeMemoryRackCount:  uint32(params.GetLargeMemoryRackCount()),
				XlargeMemoryRackCount: uint32(params.GetXLargeMemoryRackCount()),
			},
		}
	} else if updateMode == pb.SessionUpdateMode_DEBUG_APPEND || updateMode == pb.SessionUpdateMode_DEBUG_REMOVE ||
		updateMode == pb.SessionUpdateMode_DEBUG_REMOVE_ALL || updateMode == pb.SessionUpdateMode_DEBUG_SET {
		coordNodes := wscommon.Ternary(len(params.CoordinatorNodeNames) > 0, params.CoordinatorNodeNames, params.ManagementNodeNames)
		sessionSpec.UpdateParams = &pb.SessionSpec_Resources{
			Resources: &pb.SessionResources{
				Systems:                        params.SystemNames,
				Nodegroups:                     params.NodegroupNames,
				ActivationNodes:                params.ActivationNodeNames,
				AutoSelectActivationNodes:      params.AutoSelectActivationNodes,
				InferenceDriverNodes:           params.InferenceDriverNodeNames,
				AutoSelectInferenceDriverNodes: params.AutoSelectInferenceDriverNodes,
				CoordinatorNodes:               coordNodes,
				ManagementNodes:                coordNodes,
			},
		}
	}
	return sessionSpec
}

func SessionSpecToParams(spec *pb.SessionSpec) (nsapisv1.RequestParams, error) {
	_convert := func(in uint32) *int {
		return pkg.Ternary(in == 0, nil, pointer.Int(int(in)))
	}
	var mode nsapisv1.NSRUpdateMode
	if m, ok := updateModePbTok8[spec.UpdateMode]; ok {
		mode = m
	} else {
		return nsapisv1.RequestParams{}, fmt.Errorf("unsupported updateMode value '%s'", spec.UpdateMode.String())
	}

	requestParams := nsapisv1.RequestParams{
		UpdateMode:             mode,
		SuppressAffinityErrors: spec.SuppressAffinityErrors,
	}
	if s := spec.GetResourceSpec(); s != nil {
		requestParams.SystemCount = _convert(s.SystemCount)
		requestParams.ParallelExecuteCount = _convert(s.ParallelExecuteCount)
		requestParams.ParallelCompileCount = _convert(s.ParallelCompileCount)
		requestParams.LargeMemoryRackCount = _convert(s.LargeMemoryRackCount)
		requestParams.XLargeMemoryRackCount = _convert(s.XlargeMemoryRackCount)
	} else if rs := spec.GetResources(); rs != nil {
		requestParams.SystemNames = rs.Systems
		requestParams.NodegroupNames = rs.Nodegroups
		requestParams.ActivationNodeNames = rs.ActivationNodes
		requestParams.AutoSelectActivationNodes = rs.AutoSelectActivationNodes
		requestParams.InferenceDriverNodeNames = rs.InferenceDriverNodes
		requestParams.AutoSelectInferenceDriverNodes = rs.AutoSelectInferenceDriverNodes

		coordNodes := wscommon.Ternary(len(rs.CoordinatorNodes) > 0, rs.CoordinatorNodes, rs.ManagementNodes)
		requestParams.CoordinatorNodeNames = coordNodes
		requestParams.ManagementNodeNames = coordNodes
	}
	return requestParams, nil
}

func extractParams(ctx context.Context, req *pb.SessionSpec) (nsapisv1.RequestParams, error) {
	requestParams, err := SessionSpecToParams(req)
	if err != nil {
		clientBuildVersion, clientSemanticVersion := pkg.GetClientVersionFromContext(ctx)
		return nsapisv1.RequestParams{}, grpcStatus.Errorf(codes.Unimplemented,
			"%s. Likely cluster-server (version=%s) is older than csctl (version=%s). Please upgrade cluster-server to latest",
			err.Error(),
			wscommon.Ternary(clientSemanticVersion != "", wsclient.SemanticVersion, pkg.CerebrasVersion),
			wscommon.Ternary(clientSemanticVersion != "", clientSemanticVersion, clientBuildVersion))
	}
	return requestParams, nil
}

// user friendly check on session name typo
func handleSessionErr(err error, name string) error {
	if k8serrors.IsNotFound(err) {
		// hides k8 resource name
		return grpcStatus.Errorf(codes.NotFound, "session '%s' not found", name)
	}
	return pkg.MapStatusError(err)
}

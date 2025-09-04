// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"

	"cerebras.com/job-operator/common/resource"
	commonutil "github.com/kubeflow/common/pkg/util"

	"cerebras.com/cluster/server/csctl"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/kubeflow/common/pkg/util"
)

var (
	// IngressPollInterval poll interval for ingress status
	IngressPollInterval = 10 * time.Second
	// IngressPollMaxIdleInterval max interval for client side not getting any message
	IngressPollMaxIdleInterval = 30 * time.Minute
)

func init() {
	duration, err := strconv.Atoi(os.Getenv("INGRESS_POLL_MAX_IDLE_INTERVAL"))
	if err == nil && duration > 0 {
		IngressPollMaxIdleInterval = time.Duration(duration) * time.Second
	}
	duration, err = strconv.Atoi(os.Getenv("INGRESS_POLL_INTERVAL"))
	if err == nil && duration > 0 {
		IngressPollInterval = time.Duration(duration) * time.Second
	}
}

// Not supported anymore. Use PollIngressV2
func (server ClusterServer) PollIngress(request *pb.GetIngressRequest,
	stream pb.ClusterManagement_PollIngressServer) (err error) {
	log.Infof("poll ingress request: %s", request.JobId)
	return grpcStatus.Errorf(codes.Unimplemented, "Unsupported api, please upgrade client and use PollIngressV2")
}

// When the client uses this method to poll ingress, we expect the client liveness is handled
// by the leasing mechanism.
// Deprecated: HeartbeatV2 will have all the info
func (server ClusterServer) PollIngressV2(request *pb.GetIngressRequest,
	stream pb.ClusterManagement_PollIngressV2Server) (err error) {
	return server.pollIngress(request, stream, true)
}

func (server ClusterServer) pollIngress(request *pb.GetIngressRequest,
	stream pb.ClusterManagement_PollIngressServer, leaseEnabled bool) (err error) {
	log.Infof("start poll ingress for %s (lease enabled: %t)", request.JobId, leaseEnabled)
	ctx := stream.Context()

	var lastResponse *pb.GetIngressResponse
	var lastMsgTime time.Time

	defer func() {
		if err != nil {
			log.Errorf("error returned by poll ingress for %s: %s", request.JobId, err.Error())
			err = grpcStatus.Error(codes.Internal, err.Error())
		} else {
			log.Infof("success poll ingress for %s", request.JobId)
		}
	}()

	for {
		newResponse := &pb.GetIngressResponse{}
		var message string
		var job *wsapisv1.WSJob
		var ingress *networkingv1.Ingress
		select {
		case <-ctx.Done():
			log.Infof("client side stopped poll ingress for %s", request.JobId)
			return nil
		case <-time.Tick(server.ingressPollInterval):
			message, job, ingress, err = server.checkJobReady(ctx, request.JobId)
			if err != nil {
				log.Warnf("check job readiness failed: %s, %v", request.JobId, err)
				return err
			}
			break
		}
		if lastResponse != nil && lastResponse.Message == message &&
			lastMsgTime.Add(IngressPollMaxIdleInterval).After(time.Now()) {
		} else {
			newResponse.Message = message
			newResponse.IsScheduled = commonutil.HasScheduled(job.Status)
			newResponse.JobFailed = commonutil.EndingOrEnded(job.Status)
			// ingress only returned when fully ready
			if ingress != nil {
				newResponse.IsReady = true
				newResponse.ServiceAuthority = wscommon.GetIngressAuthority(ingress)
				newResponse.ServiceUrl = job.GetCrdAddress()
			}
			if err = stream.Send(newResponse); err != nil {
				log.Warnf("send lastResponse error: %s, %v", request.JobId, err)
				return err
			}
			if newResponse.IsReady || newResponse.JobFailed {
				return nil
			}
			lastMsgTime = time.Now()
			lastResponse = newResponse
		}
	}
}

func (server ClusterServer) checkJobReady(ctx context.Context, jobId string) (string, *wsapisv1.WSJob, *networkingv1.Ingress, error) {
	ctx, cancel := context.WithTimeout(ctx, IngressPollInterval)
	defer cancel()

	var err error
	var message string
	var status commonv1.JobConditionType
	var wsjob *wsapisv1.WSJob
	var ingress *networkingv1.Ingress
	wsjob, err = server.wsJobClient.Get(wscommon.Namespace, jobId)
	if err != nil {
		message = fmt.Sprintf("Error get wsjob for %v: %v", jobId, err)
		log.Warnf(message)
		return message, wsjob, ingress, err
	}

	if conditions := wsjob.Status.Conditions; len(conditions) > 0 {
		message = conditions[len(conditions)-1].Message
		status = conditions[len(conditions)-1].Type
	}
	if commonutil.EndingOrEnded(wsjob.Status) {
		message = fmt.Sprintf("Job %s failed due to: %s", wsjob.Name, message)
		if util.IsSucceeded(wsjob.Status) {
			// job should not finish before ingress handled
			message =
				fmt.Sprintf("Job %s exited without error unexpectedly before ingress became ready. "+
					"This typically means the container command exited too quickly.", wsjob.Name)
		}
		return message, wsjob, ingress, nil
	} else if !commonutil.HasScheduled(wsjob.Status) {
		message = fmt.Sprintf("Waiting for job to be scheduled, current job status: %s, msg: %s. %s",
			status, message, server.gatherQueueingInfo(wsjob.Namespace, wsjob.Name))
		return message, wsjob, ingress, nil
	}

	// job start running, wait till all pods ready
	// this is requirement of CRD and replacement logic of previous connection test in CRD
	for rtype, replica := range wsjob.Spec.WSReplicaSpecs {
		rs, ok := wsjob.Status.ReplicaStatuses[rtype]
		readyCount := int32(0)
		if ok {
			readyCount = rs.Ready
		}
		if *replica.Replicas != readyCount {
			message = fmt.Sprintf("Waiting for all %s pods to be running, current running: %d/%d.",
				rtype, readyCount, *replica.Replicas)
			return message, wsjob, ingress, nil
		}
	}

	// check ingress readiness
	message, ingress, err = server.checkIngressReadiness(ctx, wsjob)
	if err != nil {
		log.Warnf("check ingress readiness failed: %s, %v", jobId, err)
	}
	return message, wsjob, ingress, nil
}

// Check backend svc readiness before returning ingress handler
// This is necessary since ingress/svc can be ready before backend endpoints ready
func (server ClusterServer) checkIngressReadiness(ctx context.Context, job *wsapisv1.WSJob) (string, *networkingv1.Ingress, error) {
	var message string
	ingressSvc := job.GetIngressSvcName()
	svcReady, err := wscommon.IsServiceReady(server.kubeClientSet, job.Namespace, ingressSvc)
	if err != nil && !apierrors.IsNotFound(err) {
		message = fmt.Sprintf("Error get job ingress endpoint for %v: %v", job.Name, err)
		log.Warnf(message)
		return message, nil, err
	}
	if !svcReady {
		return "Waiting for job service readiness.", nil, nil
	}

	ingress, err := server.kubeClientSet.NetworkingV1().
		Ingresses(wscommon.Namespace).
		Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			message = fmt.Sprintf("Error get ingress for %v: %v", job.Name, err)
			log.Warnf(message)
			return message, nil, err
		}
		message = "Waiting for ingress to be created."
		return message, nil, nil
	}

	// check that nginx has updated load balancer status on the ingress
	lbReady := false
	for _, ing := range ingress.Status.LoadBalancer.Ingress {
		if ing.IP != "" {
			lbReady = true
			break
		}
	}
	if !lbReady {
		return "Waiting for job ingress readiness.", nil, nil
	}

	message = fmt.Sprintf("Job ingress ready, "+
		"dashboard: https://%s/d/WebHNShVz/wsjob-dashboard?orgId=1&var-wsjob=%s&from=%d&to=now",
		csctl.GrafanaUrl,
		job.Name,
		job.Status.StartTime.UnixMilli()-csctl.JobDashboardWindowBufferMs)

	return message, ingress, nil
}

// sort jobs to get queueing info
// Todo: get info from scheduler, https://cerebras.atlassian.net/browse/SW-102822
func (server ClusterServer) gatherQueueingInfo(namespace, name string) string {
	if server.lockInformer == nil || server.wsJobInformer == nil {
		return ""
	}

	var queueingJobs, effectiveCompiles, effectiveExecutes, sysInUse,
		popGroupsInUse, depopGroupsInUse, unconstrainedCompiles, compileMemInUse int
	selector := labels.NewSelector()
	locks, err := server.lockInformer.Lister().ByNamespace(namespace).List(selector)
	if err != nil {
		log.Warnf("list locks failed: %s", err)
		return ""
	}
	wsjobs, err := server.wsJobInformer.Lister().ByNamespace(namespace).List(selector)
	if err != nil {
		log.Warnf("list jobs failed: %s", err)
		return ""
	}

	jobs, jobQueueMap, lockMap := wscommon.SortJobs(
		wscommon.Map(func(a runtime.Object) *wsapisv1.WSJob {
			return a.(*wsapisv1.WSJob)
		}, wsjobs),
		wscommon.Map(func(a runtime.Object) *rlv1.ResourceLock {
			return a.(*rlv1.ResourceLock)
		}, locks),
		wscommon.Descending, wscommon.Priority, false, nil, 0)

	for _, j := range jobs {
		// stop checking for jobs queued afterwards
		if j.Name == name {
			break
		}
		isExecute := false
		if !commonutil.HasScheduled(j.Status) {
			queueingJobs++
		} else {
			if strings.Contains(jobQueueMap[j.Name], rlv1.ExecuteQueueName) {
				effectiveExecutes++
				isExecute = true
			} else {
				effectiveCompiles++
			}
			lock := lockMap[j.Name]
			if lock != nil {
				if isExecute {
					sysInUse += lock.GetSystemsAcquired()
					popCount, depopCount := lock.GetNgCountAcquired()
					popGroupsInUse += popCount
					depopGroupsInUse += depopCount
					continue
				}
				for _, req := range lock.Spec.ResourceRequests {
					if req.Name == string(wsapisv1.WSReplicaTypeCoordinator) {
						mem := req.Subresources[resource.MemSubresource]
						// unconstrained compiles don't have mem limit set(internal case only)
						if mem == 0 {
							unconstrainedCompiles++
						} else {
							compileMemInUse += mem
						}
						break
					}
				}
			}
		}
	}
	// build msg to return
	var queueInfo string
	nodegroupUsageMsg := fmt.Sprintf(" and %d nodegroup(s)[%d pop and %d depop]",
		popGroupsInUse+depopGroupsInUse, popGroupsInUse, depopGroupsInUse)
	if queueingJobs > 0 {
		queueInfo = fmt.Sprintf("Job queue status: %d %s job(s) queued before current job",
			queueingJobs, jobQueueMap[name])
	} else {
		queueInfo = "Job queue status: current job is top of queue but likely blocked by running/reserved jobs"
		if effectiveExecutes > 0 {
			queueInfo = fmt.Sprintf("%s, %d execute job(s) running/reserved using %d system(s)%s",
				queueInfo, effectiveExecutes, sysInUse, nodegroupUsageMsg)
		}
		if effectiveCompiles > 0 {
			queueInfo = fmt.Sprintf("%s, %d compile job(s) running/reserved", queueInfo, effectiveCompiles)
			// only display mem usage if all compiles have mem limits set
			if unconstrainedCompiles == 0 && compileMemInUse > 0 {
				queueInfo = fmt.Sprintf("%s using %dGi memory", queueInfo, compileMemInUse>>10)
			}
		}
	}
	queueInfo = fmt.Sprintf("%s. For more information, please run 'csctl get jobs'.", queueInfo)
	return queueInfo
}

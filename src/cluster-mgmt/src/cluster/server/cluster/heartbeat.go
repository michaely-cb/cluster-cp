// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	coordv1 "k8s.io/api/coordination/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	commonutil "github.com/kubeflow/common/pkg/util"
)

const MaxLeaseUpdateAttempts = 3

// lease update cycle
var leaseUpdateInterval = 10 * time.Second

// max deadline in case of disconnect not detected
var heartbeatMissDeadline = 10 * time.Minute

// renew abort deadline in case of k8s CP down
var renewAbortDeadline = 12 * 60 * time.Minute

func init() {
	duration, err := strconv.Atoi(os.Getenv("LEASE_UPDATE_INTERVAL_SECONDS"))
	if err == nil && duration > 0 {
		leaseUpdateInterval = time.Duration(duration) * time.Second
	}
	missDeadline, err := strconv.Atoi(os.Getenv("HB_MISS_DEADLINE_MINUTES"))
	if err == nil && missDeadline > 0 {
		heartbeatMissDeadline = time.Duration(missDeadline) * time.Minute
	}
	rewDeadline, err := strconv.Atoi(os.Getenv("LEASE_RENEW_DEADLINE_MINUTES"))
	if err == nil && rewDeadline > 0 {
		renewAbortDeadline = time.Duration(rewDeadline) * time.Minute
	}
}

// Heartbeat stream call
// starts a new lease update routine after first connection and stop routine if disconnection detected
// connection close can be detected even if user process forced killed with -9 by OS scheduled RST packet cleanup
// if in extreme edge case RST packet lost, we rely on keepalive config to detect it after 5min
// use as last resort to be conservative to avoid terminating job prematurely
// idea is we can accept slow resource release in edge cases instead of prematurely terminating jobs
// note:
// https://github.com/nginxinc/kubernetes-ingress/discussions/2356#discussioncomment-2053385
// due to grpc keepalive doesn't work well with nginx
// we will rely on nginx side keepalive configs to cancel grpc stream after idle(no calls) timeout
// https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/configmap/#keep-alive
func (server ClusterServer) HeartbeatV2(stream pb.ClusterManagement_HeartbeatV2Server) error {
	var leaseId string
	var leaseRenewStarted bool
	stopCh := make(chan struct{})
	resCh := make(chan error)
	defer close(stopCh)
	ctx := stream.Context()

	// heartbeat routine
	go func() {
		for {
			request, err := stream.Recv()
			if err != nil {
				log.Warnf("client side heartbeat stream disconnected for %s, %s", leaseId, err)
				close(resCh)
				return
			}
			leaseId = request.LeaseId
			if leaseId == "" {
				leaseId = request.JobId
			}
			if !leaseRenewStarted {
				server.leaseRenewLoop(leaseId, request.LeaseDurationSecondsOverride, stopCh)
				leaseRenewStarted = true
			}
			HeartbeatToUserNodeMetrics(request)
			newResponse := &pb.HeartbeatResponse{
				Message: fmt.Sprintf("server timestamp at: %s", time.Now().Format(time.RFC3339)),
			}

			if request.StatusPoll {
				var message string
				var job *wsapisv1.WSJob
				var ingress *networkingv1.Ingress
				message, job, ingress, err = server.checkJobReady(ctx, request.JobId)
				if err != nil {
					log.Warnf("check job readiness failed: %s, %v", request.JobId, err)
					resCh <- err
					return
				}
				newResponse.Message = message
				newResponse.IsScheduled = commonutil.HasScheduled(job.Status)
				newResponse.JobFailed = commonutil.EndingOrEnded(job.Status)
				// ingress only returned when fully ready
				if ingress != nil {
					newResponse.IsReady = true
					newResponse.ServiceAuthority = wscommon.GetIngressAuthority(ingress)
					newResponse.ServiceUrl = job.GetCrdAddress()
				}
			}

			if err = stream.Send(newResponse); err != nil {
				log.Warnf("error sending message to client: %s, %s", leaseId, err)
				close(resCh)
				return
			}
			resCh <- nil
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Warnf("client side disconnected %s", leaseId)
			return nil
		case <-time.After(heartbeatMissDeadline):
			log.Warnf("client still connected but heartbeat missed for longer than %s, "+
				"stop heartbeat for %s", heartbeatMissDeadline, leaseId)
			return nil
		case err, ok := <-resCh:
			if !ok {
				log.Warnf("response channel closed, %s", leaseId)
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
}

// lease renew routine
func (server ClusterServer) leaseRenewLoop(leaseId string, leaseDuration int32, stop chan struct{}) {
	go func() {
		// trigger first call immediately to pickup duration update quicker mostly for test purpose
		_ = server.renewLease(leaseId, leaseDuration)
		failures := 0

		ticker := time.NewTicker(leaseUpdateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				log.Infof("signal received, stop updating lease for %s", leaseId)
				return
			case <-ticker.C:
				if err := server.renewLease(leaseId, leaseDuration); err != nil {
					log.Errorf("renew lease failed error for %s, %s", leaseId, err)
					failures++
					if time.Duration(failures)*leaseUpdateInterval > renewAbortDeadline {
						log.Errorf("%s lease renew failed %d times consecutively, abort", leaseId, failures)
						return
					}
				} else {
					failures = 0
				}
			}
		}
	}()
}

// Heartbeat will trigger lease new while operator will reconcile on leases to ensure client liveness.
// Job will be cancelled for unresponsive client after deadline.
func (server ClusterServer) renewLease(leaseId string, leaseDurationOverride int32) error {
	var lease *coordv1.Lease
	var err error
	for i := 1; i <= MaxLeaseUpdateAttempts; i++ {
		lease, err = server.kubeClientSet.CoordinationV1().
			Leases(wscommon.Namespace).Get(context.Background(), leaseId, metav1.GetOptions{})
		if err != nil {
			// could be due to lease not created by operator yet
			if apierrors.IsNotFound(err) {
				log.Warnf("lease not found for %s, skip renew", leaseId)
				return nil
			}
			log.Warnf("failed to retrieve lease for %s: %v", leaseId, err)
			return err
		}

		lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
		if leaseDurationOverride > 0 {
			lease.Spec.LeaseDurationSeconds = &leaseDurationOverride
		}
		_, err = server.kubeClientSet.CoordinationV1().
			Leases(wscommon.Namespace).Update(context.Background(), lease, metav1.UpdateOptions{})
		if err == nil {
			log.Infof("successfully renewed lease for %s", leaseId)
			return nil
		}

		log.Errorf("failed to renew lease for %s: %v", leaseId, err)
		if apierrors.IsConflict(err) && i < MaxLeaseUpdateAttempts {
			time.Sleep(time.Duration(math.Pow(2, float64(i-1))) * time.Second)
		} else {
			return err
		}
	}
	return nil
}

// Deprecated: use HeartbeatV2
func (server ClusterServer) SendClientHeartbeat(
	_ context.Context,
	request *pb.HeartbeatRequest,
) (*pb.HeartbeatResponse, error) {
	if err := server.renewLease(request.JobId, request.LeaseDurationSecondsOverride); err != nil {
		return nil, err
	}

	return &pb.HeartbeatResponse{Message: "Lease has been successfully renewed"}, nil
}

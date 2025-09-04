//go:build !e2e

// Copyright 2024 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/keepalive"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

func TestHeartbeatV2(t *testing.T) {
	log.SetOutput(os.Stdout)
	leaseUpdateInterval = 500 * time.Millisecond
	defer func() {
		leaseUpdateInterval = 10 * time.Second
		heartbeatMissDeadline = 60 * time.Minute
	}()

	type testCase struct {
		name       string
		pollError  error
		cancelled  bool
		disconnect bool
		reconnect  bool
		miss       bool
		keepalive  *keepalive.ServerParameters
	}

	tests := []testCase{
		{
			name:      "status-poll-job-not-found",
			pollError: errors.New("job not found"),
		},
		{
			name:      "status-poll-job-canceled",
			cancelled: true,
		},
		{
			name:       "client-disconnect",
			disconnect: true,
		},
		{
			name:       "client-reconnect",
			disconnect: true,
			reconnect:  true,
		},
		{
			name: "connected-missing-heartbeats",
			miss: true,
		},
		// this can be flaky since 1ns is very short but won't guarantee timeout
		//{
		//	name: "keepalive", // safeguard to catch case connection closed but not detected due to RST loss
		//	keepalive: &keepalive.ServerParameters{
		//		Time:    2 * time.Second,     // start detection after 2s no data
		//		Timeout: 1 * time.Nanosecond, // simulate no response case
		//	},
		//},
	}

	run := func(test testCase, t *testing.T) {
		ctx, cleanup, clusterServer, client := setupWithKeepalive(
			false,
			fakeKubeClient,
			mockedWsJobClient,
			nil,
			nil,
			false,
			test.keepalive,
		)
		defer cleanup()
		fakeK8sClient := fake.NewSimpleClientset()
		clusterServer.kubeClientSet = fakeK8sClient
		mockedWsJobClient.MockedGet = func(namespace, jobName string) (*wsapisv1.WSJob, error) {
			job := heartbeatWsjob.DeepCopy()
			if test.cancelled {
				job.Status.Conditions = append(job.Status.Conditions, commonv1.JobCondition{
					Type:    commonv1.JobCancelled,
					Message: "canceled-by-csctl",
					Status:  corev1.ConditionTrue,
				})
			}
			return job, test.pollError
		}

		now := &metav1.MicroTime{Time: time.Now()}
		lastRenew := now
		leaseDurationSeconds := int32(wscommon.ClientLeaseDuration.Seconds())
		holderIdentity := test.name
		lease := &coordv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: wscommon.Namespace,
				Name:      test.name,
			},
			Spec: coordv1.LeaseSpec{
				AcquireTime:          now,
				RenewTime:            now,
				HolderIdentity:       &holderIdentity,
				LeaseDurationSeconds: &leaseDurationSeconds,
			},
		}
		_, err := fakeK8sClient.CoordinationV1().Leases(wscommon.DefaultNamespace).
			Create(context.TODO(), lease, metav1.CreateOptions{})
		require.NoError(t, err)

		ct, cancel := context.WithCancel(ctx)
		defer cancel()
		stream, err := client.HeartbeatV2(ct)
		if err != nil {
			t.Fatalf("hb request failed: %v", err)
		}
		i := 0
		for {
			err = stream.Send(&pb.HeartbeatRequest{JobId: test.name, StatusPoll: true})
			if err != nil {
				t.Logf("hb request failed: %v", err)
				break
			}
			res, err := stream.Recv()
			if test.pollError != nil {
				t.Logf("poll error expected: %v", err)
				require.NotNil(t, err)
				return
			}
			if err != nil {
				t.Logf("Iterate hb response failed: %v", err)
				break
			}
			if test.cancelled {
				require.Contains(t, res.Message, "canceled-by-csctl")
				require.True(t, res.JobFailed)
				require.True(t, res.IsScheduled)
				require.False(t, res.IsReady)
				return
			}

			require.Contains(t, res.Message, "Waiting for all Weight pods to be running")
			require.True(t, res.IsScheduled)
			require.False(t, res.IsReady)
			require.False(t, res.JobFailed)
			// extra sleep to avoid race condition on lease update time
			time.Sleep(2*leaseUpdateInterval + 1*time.Second)
			lease, err = fakeK8sClient.CoordinationV1().Leases(wscommon.DefaultNamespace).
				Get(context.TODO(), test.name, metav1.GetOptions{})
			require.NoError(t, err)
			// check renew is happening
			require.NotEqual(t, lease.Spec.RenewTime, lastRenew)
			i++
			lastRenew = lease.Spec.RenewTime
			// reaching here means lease update is continuing working fine
			if i >= 5 {
				if test.reconnect {
					return
				}
				if test.miss {
					// expect heartbeat canceled on server side
					heartbeatMissDeadline = 0
					time.Sleep(2*leaseUpdateInterval + 1*time.Second)
					continue
				}
			}
			// simulate client disconnected
			if test.disconnect {
				if i == 3 {
					cancel()
					time.Sleep(2*leaseUpdateInterval + 1*time.Second)
					// simulate client retried
					if test.reconnect {
						stream, err = client.HeartbeatV2(ctx)
						if err != nil {
							t.Fatalf("hb request failed: %v", err)
						}
					}
				}
			} else if test.miss { // simulate client thread starving case
				time.Sleep(2*leaseUpdateInterval + 1*time.Second)
				lease, err = fakeK8sClient.CoordinationV1().Leases(wscommon.DefaultNamespace).
					Get(context.TODO(), test.name, metav1.GetOptions{})
				require.NoError(t, err)
				// check renew is still happening
				require.NotEqual(t, lease.Spec.RenewTime, lastRenew)
				lastRenew = lease.Spec.RenewTime
			} else if test.keepalive != nil { // simulate client died but disconnection status unknown
				for j := 0; j < 5; j++ {
					time.Sleep(2*leaseUpdateInterval + 1*time.Second)
					lease, err = fakeK8sClient.CoordinationV1().Leases(wscommon.DefaultNamespace).
						Get(context.TODO(), test.name, metav1.GetOptions{})
					require.NoError(t, err)
					t.Log(lease.Spec.RenewTime)
					// expect keepalive check will detect and close connection
					if lease.Spec.RenewTime.Equal(lastRenew) {
						err = stream.Send(&pb.HeartbeatRequest{JobId: test.name})
						require.Error(t, err)
						return
					}
					lastRenew = lease.Spec.RenewTime
				}
				t.Fatal("keepalive check didn't detect client side liveness")
			}
		}

		// expect lease renew stopped after disconnect
		// extra sleep to avoid race condition
		time.Sleep(2*leaseUpdateInterval + 1*time.Second)
		lease, err = fakeK8sClient.CoordinationV1().Leases(wscommon.DefaultNamespace).
			Get(context.TODO(), test.name, metav1.GetOptions{})
		require.NoError(t, err)
		lastRenew = lease.Spec.RenewTime
		time.Sleep(2*leaseUpdateInterval + 1*time.Second)
		lease, err = fakeK8sClient.CoordinationV1().Leases(wscommon.DefaultNamespace).
			Get(context.TODO(), test.name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, lease.Spec.RenewTime, lastRenew)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run(test, t)
		})
	}
}

var heartbeatWsjob = &wsapisv1.WSJob{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "hb",
		Namespace: wscommon.DefaultNamespace,
	},
	Spec: wsapisv1.WSJobSpec{
		Priority: wscommon.Pointer(100),
		WSReplicaSpecs: map[commonv1.ReplicaType]*commonv1.ReplicaSpec{
			wsapisv1.WSReplicaTypeWeight: {
				Replicas: &weightCount,
			},
		},
	},
	Status: commonv1.JobStatus{
		Conditions: []commonv1.JobCondition{
			{
				Type:    commonv1.JobScheduled,
				Message: "job in scheduled",
				Status:  corev1.ConditionTrue,
			},
		},
	},
}

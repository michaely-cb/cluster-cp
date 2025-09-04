//go:build !e2e

// Copyright 2022 Cerebras Systems, Inc.

package cluster

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpcStatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsfake "cerebras.com/job-operator/client/clientset/versioned/fake"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	"cerebras.com/job-operator/common/resource"
	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonctrlv1 "github.com/kubeflow/common/pkg/controller.v1/common"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	rlfake "cerebras.com/job-operator/client-resourcelock/clientset/versioned/fake"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	wscommon "cerebras.com/job-operator/common"
)

const fakeNginxLBIP = "10.0.0.10"

var weightCount = int32(12)
var testWsjob = &wsapisv1.WSJob{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test",
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
				Type:    commonv1.JobCreated,
				Message: "job created",
			},
		},
		ReplicaStatuses: map[commonv1.ReplicaType]*commonv1.ReplicaStatus{
			wsapisv1.WSReplicaTypeWeight: {
				Ready: weightCount,
			},
		},
		StartTime: &metav1.Time{Time: time.Date(2024, 11, 1, 12, 0, 0, 0, time.UTC)},
	},
}

var testIngress = &networkingv1.Ingress{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test",
		Namespace: wscommon.DefaultNamespace,
	},
	Spec: networkingv1.IngressSpec{
		TLS: []networkingv1.IngressTLS{
			{
				Hosts: []string{
					"test",
				},
			},
		},
		Rules: []networkingv1.IngressRule{
			{
				Host: "test",
			},
		},
	},
}

var testEps = &corev1.Endpoints{
	ObjectMeta: metav1.ObjectMeta{
		Name:      commonctrlv1.GenSvcName("test", "coordinator", "0"),
		Namespace: wscommon.DefaultNamespace,
	},
	Subsets: []corev1.EndpointSubset{
		{
			Addresses: []corev1.EndpointAddress{
				{
					IP: "test",
				},
			},
		},
	},
}

func TestPollIngress(t *testing.T) {
	ctx, cleanup, clusterServer, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()
	clusterServer.ingressPollInterval = 100 * time.Millisecond

	tests := []struct {
		name          string
		wsJob         *wsapisv1.WSJob
		ingress       *networkingv1.Ingress
		eps           *corev1.Endpoints
		svcReady      bool
		disconnect    bool
		reconnect     bool
		expectSuccess bool
	}{
		{
			name:          "disconnect",
			wsJob:         testWsjob,
			ingress:       testIngress,
			eps:           testEps,
			svcReady:      false,
			disconnect:    true,
			reconnect:     false,
			expectSuccess: false,
		},
		{
			name:          "reconnect",
			wsJob:         testWsjob,
			ingress:       testIngress,
			eps:           testEps,
			svcReady:      false,
			disconnect:    true,
			reconnect:     true,
			expectSuccess: true,
		},
		{
			name:          "success",
			wsJob:         testWsjob,
			ingress:       testIngress,
			eps:           testEps,
			svcReady:      true,
			disconnect:    false,
			reconnect:     false,
			expectSuccess: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockedWsJobClient.MockedGet = func(namespace, jobName string) (*wsapisv1.WSJob, error) {
				test.wsJob.Status.Conditions[0].Type = commonv1.JobScheduled
				test.wsJob.Status.Conditions[0].Message = "job is scheduled"
				test.wsJob.Status.Conditions[0].Status = corev1.ConditionTrue
				return test.wsJob, nil
			}
			mockedWsJobClient.MockedUpdate = func(job *wsapisv1.WSJob) (*wsapisv1.WSJob, error) {
				return nil, nil
			}
			fakeK8sClient := fake.NewSimpleClientset()
			clusterServer.kubeClientSet = fakeK8sClient

			ingress, err := fakeK8sClient.NetworkingV1().Ingresses(wscommon.DefaultNamespace).
				Create(context.TODO(), test.ingress, metav1.CreateOptions{})
			require.NoError(t, err)
			ingress.Status.LoadBalancer.Ingress = []networkingv1.IngressLoadBalancerIngress{{
				IP: fakeNginxLBIP,
			}}
			ingress, err = fakeK8sClient.NetworkingV1().Ingresses(wscommon.DefaultNamespace).
				UpdateStatus(ctx, ingress, metav1.UpdateOptions{})
			require.NoError(t, err)

			if test.svcReady {
				_, err = fakeK8sClient.CoreV1().Endpoints(wscommon.DefaultNamespace).
					Create(context.TODO(), test.eps, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			ct, cancel := context.WithCancel(ctx)
			defer cancel()
			stream, err := client.PollIngressV2(ct, &pb.GetIngressRequest{JobId: test.wsJob.Name})
			if err != nil {
				t.Fatalf("PollIngress request failed: %v", err)
			}
			success := false
			disconnected := false
			for {
				res, err := stream.Recv()
				if err == io.EOF {
					t.Logf("Iterate PollIngress done")
					break
				}
				if err != nil {
					t.Logf("Iterate PollIngress response failed: %v", err)
					if test.reconnect {
						_, ok := grpcStatus.FromError(err)
						if ok {
							time.Sleep(1 * time.Second)
							stream, err = client.PollIngressV2(ctx, &pb.GetIngressRequest{JobId: test.wsJob.Name})
							if err != nil {
								t.Fatalf("PollIngress request failed: %v", err)
							}
							_, err = fakeK8sClient.CoreV1().Endpoints(wscommon.DefaultNamespace).
								Create(context.TODO(), test.eps, metav1.CreateOptions{})
							require.NoError(t, err)
							continue
						}
					}
					break
				}
				success = res.IsReady
				t.Log(res.Message)
				if !disconnected && test.disconnect {
					cancel()
					disconnected = true
				}
			}
			assert.Equal(t, test.expectSuccess, success)
		})
	}
}

func TestPollIngressQueueInfo(t *testing.T) {
	ctx, cleanup, clusterServer, client := setup(false, nil, mockedWsJobClient, nil, false)
	defer cleanup()
	fakeK8sClient := fake.NewSimpleClientset()
	clusterServer.kubeClientSet = fakeK8sClient

	// init job
	testWsjob.Status.Conditions[0].Type = commonv1.JobQueueing
	testWsjob.Status.Conditions[0].Message = "job is queueing"
	mockedWsJobClient.MockedGet = func(namespace, jobName string) (*wsapisv1.WSJob, error) { return testWsjob, nil }
	mockedWsJobClient.MockedUpdate = func(job *wsapisv1.WSJob) (*wsapisv1.WSJob, error) { return nil, nil }

	var wsjobList = []runtime.Object{
		&wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-1",
				Namespace: wscommon.DefaultNamespace,
			},
			Spec: wsapisv1.WSJobSpec{
				Priority: wscommon.Pointer(10),
			},
		},
		&wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-2",
				Namespace: wscommon.DefaultNamespace,
			},
			Spec: wsapisv1.WSJobSpec{
				Priority: wscommon.Pointer(20),
			},
			Status: commonv1.JobStatus{
				Conditions: []commonv1.JobCondition{
					{
						Type:   commonv1.JobScheduled,
						Status: corev1.ConditionTrue,
					},
				},
			},
		},
		&wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-3",
				Namespace: wscommon.DefaultNamespace,
			},
			Spec: wsapisv1.WSJobSpec{
				Priority: wscommon.Pointer(30),
			},
			Status: commonv1.JobStatus{
				Conditions: []commonv1.JobCondition{
					{
						Type:   commonv1.JobScheduled,
						Status: corev1.ConditionTrue,
					},
				},
			},
		},
		testWsjob,
	}

	var lockList = []runtime.Object{
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: wscommon.DefaultNamespace,
			},
			Spec: rlv1.ResourceLockSpec{
				QueueName: rlv1.ExecuteQueueName,
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockPending,
			},
		},
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-1",
				Namespace: wscommon.DefaultNamespace,
			},
			Spec: rlv1.ResourceLockSpec{
				QueueName: rlv1.ExecuteQueueName,
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockPending,
			},
		},
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-2",
				Namespace: wscommon.DefaultNamespace,
			},
			Spec: rlv1.ResourceLockSpec{
				QueueName: rlv1.ExecuteQueueName,
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
				ResourceGrants: []rlv1.ResourceGrant{
					{
						Name:      rlv1.SystemsRequestName,
						Resources: []rlv1.Res{{Name: "sys-0"}, {Name: "sys-1"}},
					},
				},
				GroupResourceGrants: []rlv1.GroupResourceGrant{
					{
						Name:        fmt.Sprintf("%s-0", rlv1.PrimaryNgPrefix),
						Annotations: map[string]string{wsapisv1.MemxPopNodegroupProp: "true"},
					},
					{
						Name:        fmt.Sprintf("%s-1", rlv1.PrimaryNgPrefix),
						Annotations: map[string]string{wsapisv1.MemxPopNodegroupProp: "true"},
					},
					{
						Name:        rlv1.SecondaryNgName,
						Annotations: map[string]string{wsapisv1.MemxPopNodegroupProp: "false"},
					},
				},
			},
		},
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lock-3",
				Namespace: wscommon.DefaultNamespace,
			},
			Spec: rlv1.ResourceLockSpec{
				QueueName: rlv1.CompileQueueName,
				ResourceRequests: []rlv1.ResourceRequest{
					{
						Name: string(wsapisv1.WSReplicaTypeCoordinator),
						Subresources: map[string]int{
							resource.MemSubresource: 32 << 10,
						},
					},
				},
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
			},
		},
	}
	// init jobs
	jobClient := wsfake.NewSimpleClientset(wsjobList...)
	jobFactory := wsinformer.NewSharedInformerFactoryWithOptions(jobClient, 0)
	jobInformer, _ := jobFactory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("wsjobs"))
	jobFactory.Start(context.Background().Done())
	jobFactory.WaitForCacheSync(context.Background().Done())
	clusterServer.wsJobInformer = jobInformer
	// init locks
	rlClient := rlfake.NewSimpleClientset(lockList...)
	factory := rlinformer.NewSharedInformerFactoryWithOptions(rlClient, 0)
	rlInformer, _ := factory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("resourcelocks"))
	factory.Start(context.Background().Done())
	factory.WaitForCacheSync(context.Background().Done())
	clusterServer.lockInformer = rlInformer

	t.Run("poll job msg", func(t *testing.T) {
		clusterServer.ingressPollInterval = 100 * time.Millisecond
		ct, cancel := context.WithCancel(ctx)
		defer cancel()
		stream, err := client.PollIngressV2(ct, &pb.GetIngressRequest{JobId: testWsjob.Name})
		if err != nil {
			t.Fatalf("PollIngress request failed: %v", err)
		}

		var ingress *networkingv1.Ingress
		i := 0
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				t.Logf("Iterate PollIngress done")
				break
			}
			if err != nil {
				t.Logf("Iterate PollIngress response failed: %v", err)
				break
			}
			t.Log(res.Message)
			switch i {
			case 0:
				require.Equal(t, "Waiting for job to be scheduled, "+
					"current job status: Queueing, msg: job is queueing. "+
					"Job queue status: 1 execute job(s) queued before current job. "+
					"For more information, please run 'csctl get jobs'.", res.Message)
				require.NoError(t, rlClient.ResourcelockV1().ResourceLocks(wscommon.DefaultNamespace).
					Delete(context.TODO(), "lock-1", metav1.DeleteOptions{}))
				require.NoError(t, jobClient.WsV1().WSJobs(wscommon.DefaultNamespace).
					Delete(context.TODO(), "lock-1", metav1.DeleteOptions{}))
				break
			case 1:
				require.Equal(t, "Waiting for job to be scheduled, "+
					"current job status: Queueing, msg: job is queueing. "+
					"Job queue status: current job is top of queue but likely blocked by running/reserved jobs, "+
					"1 execute job(s) running/reserved using 2 system(s) and 3 nodegroup(s)[2 pop and 1 depop], "+
					"1 compile job(s) running/reserved using 32Gi memory. "+
					"For more information, please run 'csctl get jobs'.", res.Message)
				testWsjob.Status.Conditions[0].Type = commonv1.JobScheduled
				testWsjob.Status.Conditions[0].Message = "job scheduled"
				testWsjob.Status.Conditions[0].Status = corev1.ConditionTrue
				testWsjob.Status.ReplicaStatuses[wsapisv1.WSReplicaTypeWeight] = &commonv1.ReplicaStatus{
					Ready: int32(3),
				}
				break
			case 2:
				require.Equal(t, "Waiting for all Weight pods to be running, "+
					"current running: 3/12.", res.Message)
				testWsjob.Status.ReplicaStatuses[wsapisv1.WSReplicaTypeWeight] = &commonv1.ReplicaStatus{
					Ready: weightCount,
				}
				break
			case 3:
				require.Equal(t, "Waiting for job service readiness.", res.Message)
				_, err = fakeK8sClient.CoreV1().Endpoints(wscommon.DefaultNamespace).
					Create(context.TODO(), testEps, metav1.CreateOptions{})
				require.NoError(t, err)
				break
			case 4:
				require.Equal(t, "Waiting for ingress to be created.", res.Message)
				ingress, err = fakeK8sClient.NetworkingV1().Ingresses(wscommon.DefaultNamespace).
					Create(context.TODO(), testIngress, metav1.CreateOptions{})
				require.NoError(t, err)
				break
			case 5:
				require.Equal(t, "Waiting for job ingress readiness.", res.Message)
				ingress.Status.LoadBalancer.Ingress = []networkingv1.IngressLoadBalancerIngress{{IP: fakeNginxLBIP}}
				ingress, err = fakeK8sClient.NetworkingV1().Ingresses(wscommon.DefaultNamespace).
					UpdateStatus(context.TODO(), ingress, metav1.UpdateOptions{})
				require.NoError(t, err)
				break
			default:
				require.True(t, res.IsReady)
				require.Equal(t, "Job ingress ready, "+
					"dashboard: https:///d/WebHNShVz/wsjob-dashboard?orgId=1&var-wsjob=test&from=1730461800000&to=now",
					res.Message)
			}
			i++
		}
	})
}

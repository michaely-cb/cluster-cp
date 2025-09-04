package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	joboperatorpb "cerebras/pb/workflow/appliance/cluster_mgmt/job_operator"

	wscommon "cerebras.com/job-operator/common"
)

// +kubebuilder:rbac:groups="discovery.k8s.io",resources=endpointslices,verbs=get;create;list;watch;patch

type GRPCServer struct {
	server *grpc.Server
	port   int

	k8sClient client.Client
	logger    logr.Logger

	joboperatorpb.UnimplementedJobOperatorServiceServer
	grpc_health_v1.UnimplementedHealthServer
}

func NewGRPCServer(port int, k8sClient client.Client, logger logr.Logger) *GRPCServer {
	kp := keepalive.ServerParameters{
		Time:    5 * time.Minute,  // Send keepalive ping after 5 minutes idle
		Timeout: 30 * time.Second, // Close connection after 30s not pingable
	}
	grpcServer := grpc.NewServer(grpc.KeepaliveParams(kp))
	server := &GRPCServer{
		server:    grpcServer,
		port:      port,
		k8sClient: k8sClient,
		logger:    logger,
	}
	joboperatorpb.RegisterJobOperatorServiceServer(grpcServer, server)
	grpc_health_v1.RegisterHealthServer(grpcServer, server)
	reflection.Register(grpcServer)
	return server
}

// === Functions related to starting the server on leaders only ===
func (s *GRPCServer) NeedLeaderElection() bool {
	return true
}

func (s *GRPCServer) upsertEndpointSlice(ctx context.Context) error {
	podIp := wscommon.PodIp
	if podIp == "" {
		return fmt.Errorf("pod IP is not set, cannot upsert EndpointSlice")
	}

	endpointSlice := &discoveryv1.EndpointSlice{}
	err := s.k8sClient.Get(ctx, client.ObjectKey{
		Name:      wscommon.GRPCServerEndpointSliceName,
		Namespace: wscommon.Namespace,
	}, endpointSlice)

	// If it already exists, patch it
	if err == nil {
		patch := map[string]interface{}{
			"endpoints": []map[string]interface{}{
				{
					"addresses": []string{wscommon.PodIp},
				},
			},
		}
		patchBytes, err := json.Marshal(patch)
		if err != nil {
			return fmt.Errorf("failed to marshal patch: %w", err)
		}

		return wscommon.PatchWithRetryAndTimeout(ctx, s.k8sClient, &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      wscommon.GRPCServerEndpointSliceName,
				Namespace: wscommon.Namespace,
			}}, client.RawPatch(types.MergePatchType, patchBytes))
	} else {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get EndpointSlice: %w", err)
		} else {
			portName := wscommon.GRPCServerPortName
			port := int32(s.port)
			protocol := corev1.ProtocolTCP
			return wscommon.CreateWithRetryAndTimeout(ctx, s.k8sClient, &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wscommon.GRPCServerEndpointSliceName,
					Namespace: wscommon.Namespace,
					Labels: map[string]string{
						wscommon.K8sServiceNameLabelKey:         wscommon.K8sServiceNameLabelValue,
						wscommon.EndpointSliceManagedByLabelKey: wscommon.EndpointSliceManagedByLabelValue,
					},
				},
				AddressType: discoveryv1.AddressTypeIPv4,
				Endpoints: []discoveryv1.Endpoint{
					{
						Addresses: []string{podIp},
					},
				},
				Ports: []discoveryv1.EndpointPort{
					{
						Name:     &portName,
						Protocol: &protocol,
						Port:     &port,
					},
				},
			})
		}
	}
}

func (s *GRPCServer) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	errCh := make(chan error, 1)

	go func() {
		s.logger.Info("Starting gRPC server", "port", s.port)
		if err := s.server.Serve(listener); err != nil {
			s.logger.Error(err, "Failed to serve gRPC")
			errCh <- err
		}
	}()

	if err := s.upsertEndpointSlice(ctx); err != nil {
		s.logger.Error(err, "Failed to upsert endpoint slice")
		return err
	}

	select {
	case <-ctx.Done():
		s.logger.Info("shutting down gRPC server, trying graceful stop...")
		s.server.GracefulStop()
		return nil
	case err := <-errCh:
		s.logger.Error(err, "gRPC setup server encountered an error")
		return err
	}
}

// === API endpoints ===
func (s *GRPCServer) Test(ctx context.Context, req *joboperatorpb.TestRequest) (*joboperatorpb.TestResponse, error) {
	return &joboperatorpb.TestResponse{
		Response: "Test successful",
	}, nil
}

package csadm

import (
	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"k8s.io/client-go/kubernetes"
	controllerruntime "sigs.k8s.io/controller-runtime"

	networkingv1 "k8s.io/api/networking/v1"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	nsv1 "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	sysv1 "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	"cerebras.com/job-operator/common"
	wscommon "cerebras.com/job-operator/common"
)

var clientIngressNamespace = ""
var testUser = pkg.ClientMeta{
	UID:      1000,
	GID:      1001,
	Username: "testuser",
}
var k8sClient client.Client

var rootUser = pkg.ClientMeta{
	Username: "root",
}

func getClusterServerIngress(namespace string) *networkingv1.Ingress {
	return &networkingv1.Ingress{
		ObjectMeta: ctrl.ObjectMeta{
			Name:      wsapisv1.ClusterServerIngressName,
			Namespace: namespace,
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{
					Host: "wsjob",
				},
			},
			TLS: []networkingv1.IngressTLS{
				{
					SecretName: "grpc-secret",
				},
			},
		},
	}
}

func setup(
	withRoot bool,
	kubeClientSet kubernetes.Interface,
	wsjobClient wsclient.WSJobClientProvider,
	multiMgmtNodes bool,
) (context.Context, func(), *Server, pb.CsAdmV1Client) {
	return setupWithKeepalive(
		withRoot,
		kubeClientSet,
		wsjobClient,
		multiMgmtNodes,
		nil,
	)
}

// setupWithKeepalive creates a new Server, registers it with a gRPC server,
// and returns a context, cleanup function, the Server instance, and a CsAdmV1Client.
func setupWithKeepalive(
	withRoot bool,
	kubeClientSet kubernetes.Interface,
	wsjobClient wsclient.WSJobClientProvider,
	multiMgmtNodes bool,
	ka *keepalive.ServerParameters,
) (context.Context, func(), *Server, pb.CsAdmV1Client) {
	// Prepare job path configuration.
	jpConfig := wsclient.JobPathConfig{
		LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
		CachedCompileRootPath:    wsclient.CachedCompileRootPath,
		TensorStorageRootPath:    wsclient.TensorStorageRootHostPath,
	}
	// Create an in-memory listener.
	listener := bufconn.Listen(1024 * 1024)

	// Setup gRPC server options.
	grpcOpts := []grpc.ServerOption{
		pkg.NewUnaryInterceptors(true),
		pkg.NewStreamInterceptors(),
	}
	if ka != nil {
		grpcOpts = append(grpcOpts, grpc.KeepaliveParams(*ka))
	}
	grpcServer := grpc.NewServer(grpcOpts...)

	// Define Server options.
	opts := wsclient.ServerOptions{
		HasMultiMgmtNodes:                   multiMgmtNodes,
		DisableUserAuth:                     true,
		CompileSystemType:                   "cs2",
		MaxTrainFabricFreqMhz:               "900",
		JobPathConfig:                       jpConfig,
		SkipSystemMaintenanceResourceLimits: true,
	}

	// For this setup, use nil for interfaces not needed in your tests.
	var sysCli sysv1.SystemInterface = nil
	var nsCli nsv1.NamespaceReservationInterface = nil
	var lockInf rlinformer.GenericInformer = nil
	var jobInf wsinformer.GenericInformer = nil
	var metricsQuerier wscommon.MetricsQuerier = nil

	// Create the Server (from csadm package) using our NewServer constructor.
	csadmServer := NewServer(kubeClientSet, sysCli, nsCli, lockInf, jobInf, metricsQuerier, opts, wsjobClient)

	// Register the server with its gRPC service.
	pb.RegisterCsAdmV1Server(grpcServer, csadmServer)

	// Start the gRPC server.
	go grpcServer.Serve(listener)

	// Define a gRPC service configuration with retry policy.
	grpcServiceConfig := `{
        "methodConfig": [{
            "name": [
                {"service": "cluster.cluster_mgmt_pb.csadm.CsAdmV1"}
            ],
            "retryPolicy": {
                "maxAttempts": 4,
                "initialBackoff": "3s",
                "maxBackoff": "10s",
                "backoffMultiplier": 2,
                "retryableStatusCodes": ["UNAVAILABLE", "UNKNOWN", "RESOURCE_EXHAUSTED"]
            }
        }]
    }`

	ctx := context.Background()
	conn, err := grpc.DialContext(
		ctx,
		"",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithDefaultServiceConfig(grpcServiceConfig),
		grpc.WithChainUnaryInterceptor(
			AuthClientInterceptor(pkg.Ternary(withRoot, rootUser, testUser)),
			NamespaceInterceptor(),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create a CsAdmV1Client.
	client := pb.NewCsAdmV1Client(conn)

	// Cleanup closes the connection and stops the server.
	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
	}

	return ctx, cleanup, csadmServer, client
}

func AuthClientInterceptor(user pkg.ClientMeta) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		newMD := metadata.Pairs(
			"auth-uid", strconv.Itoa(int(user.UID)),
			"auth-gid", strconv.Itoa(int(user.GID)),
			"auth-username", user.Username,
		)
		ctx = metadata.NewOutgoingContext(ctx, metadata.Join(md, newMD))
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func NamespaceInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)

		ingressDomain := fmt.Sprintf("%s.local", common.Namespace)
		if clientIngressNamespace != "" {
			ingressDomain = fmt.Sprintf("%s.local", clientIngressNamespace)
		}

		newMD := metadata.Pairs(pkg.XForwardedHostHeader, ingressDomain)
		ctx = metadata.NewOutgoingContext(ctx, metadata.Join(md, newMD))
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func getWsJobClient(t *testing.T, mgr controllerruntime.Manager) *wsclient.WSJobClient {
	wsjobClient, err := wsclient.NewWSJobClient(common.DefaultNamespace, mgr.GetConfig())
	require.NoError(t, err)
	return wsjobClient
}

package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreinformers "k8s.io/client-go/informers"
	fakeclient "k8s.io/client-go/kubernetes/fake"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"k8s.io/client-go/kubernetes"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	rlCliv1 "cerebras.com/job-operator/client-resourcelock/clientset/versioned/typed/resourcelock/v1"
	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

var k8sClient client.Client
var lockCli rlCliv1.ResourceLockInterface
var clientIngressNamespace = ""
var defaultJpConfig = wsclient.JobPathConfig{
	LocalWorkdirLogsRootPath: wsclient.WorkdirLogsRootPath,
	CachedCompileRootPath:    wsclient.CachedCompileRootPath,
	LogExportRootPath:        wsclient.LogExportRootPath,
	DebugArtifactRootPath:    wsclient.DebugArtifactRootPath,
	TensorStorageRootPath:    wsclient.TensorStorageRootHostPath,
}

var mockContainerImage = "cerebras/mock-image:latest"
var mockContainerCommand = []string{"sleep", "infinity"}

func setup(
	withRoot bool,
	kubeClientSet kubernetes.Interface,
	wsjobClient wsclient.WSJobClientProvider,
	metricsQuerier common.MetricsQuerier,
	multiMgmtNodes bool,
) (context.Context, func(), *ClusterServer, pb.ClusterManagementClient) {
	return setupWithKeepalive(
		withRoot,
		kubeClientSet,
		wsjobClient,
		metricsQuerier,
		nil,
		multiMgmtNodes,
		nil,
	)
}

func setupWithKeepalive(
	withRoot bool,
	kubeClientSet kubernetes.Interface,
	wsjobClient wsclient.WSJobClientProvider,
	metricsQuerier common.MetricsQuerier,
	metricsPublisher MetricsPublisher,
	multiMgmtNodes bool,
	ka *keepalive.ServerParameters) (context.Context, func(), *ClusterServer, pb.ClusterManagementClient) {
	listener = bufconn.Listen(1024 * 1024)
	grpcOpts := []grpc.ServerOption{
		pkg.NewUnaryInterceptors(true), pkg.NewStreamInterceptors(),
	}
	if ka != nil {
		grpcOpts = append(grpcOpts, grpc.KeepaliveParams(*ka))
	}
	server := grpc.NewServer(grpcOpts...)
	opts := wsclient.ServerOptions{
		HasMultiMgmtNodes:     multiMgmtNodes,
		DisableUserAuth:       true,
		CompileSystemType:     "cs2",
		MaxTrainFabricFreqMhz: "900",
		WsjobDefaultImage:     mockContainerImage,
		WsjobDefaultCommand:   strings.Join(mockContainerCommand, " "),
		JobPathConfig:         defaultJpConfig,
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
			Labels: map[string]string{
				fmt.Sprintf("%s-%s", common.RoleLabelKeyPrefix, common.RoleActivation.String()): "",
			},
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceMemory: *resource.NewQuantity(16<<20, resource.BinarySI),
				corev1.ResourceCPU:    *resource.NewMilliQuantity(64000, resource.DecimalSI),
			},
		},
	}

	k8s := fakeclient.NewSimpleClientset()
	nodeFactory := coreinformers.NewSharedInformerFactory(k8s, 0)
	nodeInformer := nodeFactory.Core().V1().Nodes()
	nodeFactory.Start(context.Background().Done())
	nodeFactory.WaitForCacheSync(context.Background().Done())

	store := nodeInformer.Informer().GetIndexer()
	if _, exists, _ := store.Get(node); !exists {
		err := store.Add(node)
		if err != nil {
			log.Fatal(err)
		}
	}

	clusterServer := NewClusterServer(
		kubeClientSet,
		wsjobClient,
		metricsQuerier,
		metricsPublisher,
		lockCli,
		nil,
		nil,
		nil,
		nodeInformer,
		nil,
		nil,
		opts,
	)
	pb.RegisterClusterManagementServer(server, clusterServer)
	go server.Serve(listener)
	grpcServiceConfig := `{
		"methodConfig": [{
			"name": [
				{"service": "cluster.cluster_mgmt_pb.csctl.CsCtlV1"},
				{"service": "cluster.cluster_mgmt_pb.csadm.CsAdmV1"},
				{"service": "cluster.cluster_mgmt_pb.ClusterManagement"}
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

	client := pb.NewClusterManagementClient(conn)
	cleanup := func() {
		conn.Close()
		server.Stop()
	}

	return ctx, cleanup, clusterServer, client
}

func getWsJobClient(t *testing.T, mgr controllerruntime.Manager) *wsclient.WSJobClient {
	wsjobClient, err := wsclient.NewWSJobClient(common.DefaultNamespace, mgr.GetConfig())
	require.NoError(t, err)
	return wsjobClient
}

var listener *bufconn.Listener

var testUser = pkg.ClientMeta{
	UID:      1000,
	GID:      1001,
	Username: "testuser",
}

var rootUser = pkg.ClientMeta{
	Username: "root",
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

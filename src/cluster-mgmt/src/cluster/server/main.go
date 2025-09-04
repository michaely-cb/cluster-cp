// Copyright 2022 Cerebras Systems, Inc.

package main

// Cluster Server is a gRPC service that handles the requests to the Cerebras appliance.

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	ctrl "sigs.k8s.io/controller-runtime"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	csadmpb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
	csctlpb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	nsv1 "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1"
	rlCliv1 "cerebras.com/job-operator/client-resourcelock/clientset/versioned/typed/resourcelock/v1"
	sysv1 "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1"
	wscommon "cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/cluster"
	"cerebras.com/cluster/server/csadm"
	"cerebras.com/cluster/server/csctl"
	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
)

const maxSendMsgSize = 10 * 1024 * 1024

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
}

func main() {
	var port int
	var promPort int
	flag.IntVar(&port, "port", wsclient.DefaultPort, "The grpcServer port.")
	flag.IntVar(&promPort, "metrics-port", wsclient.MetricsPort, "The prometheus metrics port.")
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.WithField("CEREBRAS_VERSION", pkg.CerebrasVersion).Info("cluster-server version")
	if pkg.CerebrasVersion == "" {
		log.Fatalf("Cluster server version is not set")
	}
	log.WithField("SEMANTIC_VERSION", wsclient.SemanticVersion).Info("cluster-server semantic version")
	cluster.ServerSemanticVersionAdded(wsclient.SemanticVersion, pkg.CerebrasVersion)

	opts := wsclient.ServerOptionsFromEnv()

	log.WithField("port", port).
		WithField("serverOptions", fmt.Sprintf("%+v", opts)).
		Info("server config")

	wsJobInformer, err := wsclient.NewWSJobInformer(ctx,
		wscommon.EnableClusterMode && wscommon.Namespace == wscommon.SystemNamespace)
	if err != nil {
		log.Fatalf("Failed to instantiate a job informer: %s", err)
	}
	nsCli, err := nsv1.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		log.Fatalf("Failed to instantiate a namespace reservation client: %s", err)
	}
	rlCli, err := rlCliv1.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		log.Fatalf("Failed to instantiate a resource lock client: %s", err)
	}
	sysCli, err := sysv1.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		log.Fatalf("Failed to instantiate a system client: %s", err)
	}
	lockInformer, err := wsclient.NewLockInformer(ctx,
		wscommon.EnableClusterMode && wscommon.Namespace == wscommon.SystemNamespace)
	if err != nil {
		log.Fatalf("Failed to instantiate a resource lock informer: %s", err)
	}
	nodeInformer, err := pkg.NewNodeInformer(ctx)
	if err != nil {
		log.Fatalf("Failed to instantiate a node informer: %s", err)
	}

	wsJobClient, err := wsclient.NewWSJobClient(wscommon.Namespace, nil)
	if err != nil {
		log.Fatalf("Failed to instantiate a job client: %s", err)
	}

	promUrl := fmt.Sprintf("http://%s.%s.svc:9090",
		pkg.Ternary(opts.HasMultiMgmtNodes, wsclient.ThanosQuerySvc, wsclient.PrometheusOperatedSvc), wsclient.PrometheusNs)
	promMetricsQuerier := wscommon.NewPromMetricsQuerier(promUrl)
	logMetricsPublisher := cluster.NewLogMetricsPublisher()
	csctl.GrafanaUrl = os.Getenv("GRAFANA_URL")

	// Courtesy error messaging only, due to the following environment variables would only come from rel-2.4 old charts.
	staleBranchMsg := "Your local repo is likely falling behind with the deployed server version. " +
		"Please pull/rebase to ensure your local repo on same version of target server and then redeploy."
	if os.Getenv("WSJOB_DEFAULT_COMMAND") == "" {
		log.Fatalf("Cluster server failed to start due to stale deployment chart was used. " + staleBranchMsg)
	}

	if os.Getenv("ALPINE_KUBE_USER_AUTH_TAG") == "" {
		log.Fatalf("Cluster server failed to start due to ALPINE_KUBE_USER_AUTH_TAG missing. " + staleBranchMsg)
	}

	log.WithField("wsjobClient", wsJobClient.String()).Info("client config")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %s", err)
	}

	// periodic keepalive pings while no data or no calls
	kp := keepalive.ServerParameters{
		Time:    5 * time.Minute,  // Send keepalive ping after 5 minutes idle
		Timeout: 30 * time.Second, // Close connection after 30s not pingable
	}
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(kp),
		grpc.MaxSendMsgSize(maxSendMsgSize),
		pkg.NewUnaryInterceptors(opts.DisableUserAuth),
		pkg.NewStreamInterceptors())

	clusterServer := cluster.NewClusterServer(
		wsJobClient.Clientset,
		wsJobClient,
		promMetricsQuerier,
		logMetricsPublisher,
		rlCli.ResourceLocks(wscommon.Namespace),
		sysCli.Systems(),
		wsJobInformer,
		lockInformer,
		nodeInformer,
		nsCli.NamespaceReservations(),
		nil,
		opts,
	)
	pb.RegisterClusterManagementServer(grpcServer, *clusterServer)
	hv1.RegisterHealthServer(grpcServer, *clusterServer)

	csCtlServer := csctl.NewServer(
		wsJobInformer,
		wsJobClient,
		promMetricsQuerier,
		sysCli.Systems(),
		nsCli.NamespaceReservations(),
		lockInformer,
		nodeInformer,
		opts,
	)
	csctlpb.RegisterCsCtlV1Server(grpcServer, csCtlServer)

	csAdmServer := csadm.NewServer(
		wsJobClient.Clientset,
		sysCli.Systems(),
		nsCli.NamespaceReservations(),
		lockInformer,
		wsJobInformer,
		promMetricsQuerier,
		opts,
		wsJobClient,
	)
	csadmpb.RegisterCsAdmV1Server(grpcServer, csAdmServer)

	reflection.Register(grpcServer)
	go func() {
		log.Infof("starting on port %d", port)
		if err = grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to start grpc server: %v", err)
		}
	}()

	// Expose the metrics on an HTTP endpoint
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", promPort), nil); err != nil {
			panic(err)
		}
	}()

	// Listen for SIGINT and SIGTERM signals and gracefully shut down the server when they are received
	// graceful shutdown allows some time for inflight requests to complete while rejecting new requests
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh

	stopCh := make(chan struct{})
	go func() {
		log.Infof("shutting down, try graceful stop...")
		grpcServer.GracefulStop()
		close(stopCh)
	}()

	select {
	case <-stopCh:
		log.Infof("gracefully stopped grpc server")
	case <-time.After(10 * time.Second):
		log.Infof("force stopping grpc server")
		grpcServer.Stop()
	}
}

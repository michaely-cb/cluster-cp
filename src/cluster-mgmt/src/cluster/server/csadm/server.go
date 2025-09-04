package csadm

import (
	"k8s.io/client-go/kubernetes"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"

	"cerebras.com/cluster/server/pkg"
	"cerebras.com/cluster/server/pkg/wsclient"
	nsv1 "cerebras.com/job-operator/client-namespace/clientset/versioned/typed/namespace/v1"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	sysv1 "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	wscommon "cerebras.com/job-operator/common"
)

type Server struct {
	pb.UnimplementedCsAdmV1Server
	k8s            kubernetes.Interface
	sysCli         sysv1.SystemInterface
	nsrCli         nsv1.NamespaceReservationInterface
	lockInformer   rlinformer.GenericInformer
	wsJobInformer  wsinformer.GenericInformer
	metricsQuerier wscommon.MetricsQuerier
	serverOpts     wsclient.ServerOptions
	wsJobClient    wsclient.WSJobClientProvider
	cfgProvider    pkg.ClusterCfgProvider
}

func NewServer(k8s kubernetes.Interface, sysCli sysv1.SystemInterface,
	nsCli nsv1.NamespaceReservationInterface, lockInf rlinformer.GenericInformer,
	jobInf wsinformer.GenericInformer, metricsQuerier wscommon.MetricsQuerier,
	serverOpts wsclient.ServerOptions, wsJobClient wsclient.WSJobClientProvider) *Server {
	return &Server{
		k8s:            k8s,
		sysCli:         sysCli,
		nsrCli:         nsCli,
		lockInformer:   lockInf,
		wsJobInformer:  jobInf,
		metricsQuerier: metricsQuerier,
		serverOpts:     serverOpts,
		wsJobClient:    wsJobClient,
		cfgProvider:    pkg.NewK8sClusterCfgProvider(k8s, nsCli),
	}
}

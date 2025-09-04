/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"errors"
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	"cerebras.com/job-operator/controllers/cluster"
	"cerebras.com/job-operator/controllers/health"
	"cerebras.com/job-operator/controllers/resource"
	wsreconcilers "cerebras.com/job-operator/controllers/ws"
	"cerebras.com/job-operator/server"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(wsapisv1.AddToScheme(scheme))
	utilruntime.Must(rlv1.AddToScheme(scheme))
	utilruntime.Must(systemv1.AddToScheme(scheme))
	utilruntime.Must(namespacev1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var grpcServerPort int
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&wscommon.ClusterConfigMapName, "cluster-configmap-name", wscommon.DefaultClusterConfigMapName, "The name of cluster configmap name.")
	flag.BoolVar(&wscommon.IsDev, "dev", false,
		"Dev mode disables scheduling features that would otherwise not be supported on 1 or 2 node clusters. Should be only used for limited local testing purpose")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.IntVar(&grpcServerPort, "grpc-server-port", wsapisv1.DefaultPort, "The gRPC server port.")

	flag.Parse()
	logger := wscommon.InitLogger()
	ctrl.SetLogger(logger.Logr())

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		WebhookServer: &webhook.DefaultServer{
			Options: webhook.Options{
				Port: 9443,
			},
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       wscommon.LeaseName,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	setupLog.Info("job-operator version", "CEREBRAS_VERSION", wscommon.CerebrasVersion)
	if wscommon.CerebrasVersion == "" {
		setupLog.Error(errors.New("job-operator version not set"), "Job operator version is not set")
		os.Exit(1)
	}
	setupLog.Info("job-operator semantic version", "SEMANTIC_VERSION", wscommon.SemanticVersion)
	wscommon.OperatorSemanticVersionAdded(wscommon.SemanticVersion, wscommon.CerebrasVersion)

	setupLog.Info("alpine-kubectl image tag", "ALPINE_KUBECTL_TAG", wsapisv1.KubectlImageTag)
	setupLog.Info("alpine-containerd image tag", "ALPINE_CONTAINERD_TAG", wsapisv1.ContainerdImageTag)
	if wsapisv1.KubectlImageTag == "" || wsapisv1.ContainerdImageTag == "" {
		setupLog.Error(errors.New("invalid common images tags"), "Common images tags are not set")
		os.Exit(1)
	}

	if wscommon.EnableClusterMode && wscommon.Namespace != wscommon.SystemNamespace {
		setupLog.Error(errors.New("non-system namespace can't be in cluster mode"),
			"non-system namespace can't be in cluster mode")
		os.Exit(1)
	}

	if wscommon.IsDev {
		setupLog.Info("Warning: DEV mode is on, will overwrite cluster flags")
	}

	cfgMgr, err := wscommon.InitClusterConfigMgr(mgr.GetAPIReader(), mgr.GetClient(), mgr.GetConfig())
	if err != nil {
		setupLog.Error(err,
			"unable to generate cluster config/flags",
			"name", wscommon.ClusterConfigMapName)
		os.Exit(1)
	}
	utilruntime.Must(mgr.Add(cfgMgr))

	nodeLabeler := cluster.NewNodeLabeler(mgr.GetClient(), mgr.GetLogger())
	cfgMgr.AddWatcher(nodeLabeler.GetCfgCh())
	utilruntime.Must(mgr.Add(nodeLabeler))

	systemUpdater := cluster.NewSystemUpdater(mgr.GetClient(), mgr.GetLogger())
	cfgMgr.AddWatcher(systemUpdater.GetCfgCh())
	utilruntime.Must(mgr.Add(systemUpdater))

	utilruntime.Must(mgr.Add(server.NewGRPCServer(grpcServerPort, mgr.GetClient(), mgr.GetLogger())))

	resourceController := resource.NewController(cfgMgr)
	if err = resourceController.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ResourceController")
		os.Exit(1)
	}
	cfgMgr.AddWatcher(resourceController.CfgChan)

	jobReconciler := wsreconcilers.NewJobOperatorReconciler(cfgMgr.Cfg, mgr, false)
	if err = jobReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "WSJob")
		os.Exit(1)
	}
	cfgMgr.AddWatcher(jobReconciler.CfgChan)

	workflowReconciler := wsreconcilers.NewWorkflowReconciler(jobReconciler.WorkflowEventChan, jobReconciler.WorkflowCache)
	if err = workflowReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Workflow")
		os.Exit(1)
	}

	leaseReconciler := wsreconcilers.NewLeaseReconciler(jobReconciler.WorkflowEventChan, false)
	if err = leaseReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Lease")
		os.Exit(1)
	}

	if _, err = cluster.NewConfigReconciler(cfgMgr, mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ConfigReconciler")
		os.Exit(1)
	}

	if err, _, _ = health.NewHealthReconciler(mgr, false); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ClusterHealth")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

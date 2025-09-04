package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wscommon "cerebras.com/job-operator/common"
)

const (
	nodeEventPrefix   = "_node_"
	systemEventPrefix = "_system_"
	deployEventPrefix = "_deploy_"
)

// ConfigReconciler watches Manager's cache informer for updates on the cluster configmap.
type ConfigReconciler struct {
	Logger   logr.Logger
	recorder record.EventRecorder

	CfgMgr     *wscommon.ClusterConfigMgr
	initCfgMgr bool

	client client.Client
	reader client.Reader
	config *rest.Config
	ctx    context.Context
}

func NewConfigReconciler(
	cfgMgr *wscommon.ClusterConfigMgr,
	mgr manager.Manager,
) (*ConfigReconciler, error) {
	configReconciler := &ConfigReconciler{
		Logger: mgr.GetLogger().WithValues("controller", "configReconciler"),
		reader: mgr.GetAPIReader(),
		client: mgr.GetClient(),
		config: mgr.GetConfig(),
		CfgMgr: cfgMgr,
		ctx:    context.Background(),
	}
	if err := configReconciler.setupWithManager(mgr); err != nil {
		return nil, err
	}
	return configReconciler, nil
}

func (c *ConfigReconciler) setupWithManager(mgr ctrl.Manager) error {
	c.recorder = mgr.GetEventRecorderFor("configReconciler")
	ctr, err := controller.New("configReconciler", mgr, controller.Options{
		Reconciler: c,
	})
	if err != nil {
		return err
	}
	if err = ctr.Watch(source.Kind(mgr.GetCache(), &corev1.Node{}),
		handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, a client.Object) []reconcile.Request {
			return []reconcile.Request{
				{NamespacedName: types.NamespacedName{
					Name: nodeEventPrefix + a.GetName(),
				}},
			}
		}),
		predicate.Funcs{
			DeleteFunc: func(e event.DeleteEvent) bool {
				return false
			},
		},
	); err != nil {
		return err
	}
	if err = ctr.Watch(source.Kind(mgr.GetCache(), &systemv1.System{}),
		handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, a client.Object) []reconcile.Request {
			return []reconcile.Request{
				{NamespacedName: types.NamespacedName{
					Name: systemEventPrefix + a.GetName(),
				}},
			}
		}),
		predicate.Funcs{
			// only config update will trigger system deletion
			// watch for creation in case of operator restart
			DeleteFunc: func(e event.DeleteEvent) bool {
				return false
			},
		},
	); err != nil {
		return err
	}
	if err = ctr.Watch(source.Kind(mgr.GetCache(), &corev1.ConfigMap{}), &handler.EnqueueRequestForObject{},
		wscommon.SystemNamespacePredicate(), predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				if e.Object.GetName() == wscommon.ClusterConfigMapName {
					c.Logger.Info("new cluster config observed", "name", e.Object.GetName())
					return true
				}
				return false
			},
			// update is only for internal test convenience, should not happen in Prod
			UpdateFunc: func(e event.UpdateEvent) bool {
				if e.ObjectNew.GetName() == wscommon.ClusterConfigMapName {
					c.Logger.Info("updated cluster config observed", "name", e.ObjectNew.GetName())
					return true
				}
				return false
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				return false
			},
		},
	); err != nil {
		return err
	}
	if err = ctr.Watch(source.Kind(mgr.GetCache(), &appv1.Deployment{}),
		handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, a client.Object) []reconcile.Request {
			return []reconcile.Request{
				{NamespacedName: types.NamespacedName{
					Name:      deployEventPrefix + a.GetName(),
					Namespace: a.GetNamespace(),
				}},
			}
		}),
		predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				// skip watching on self deployment
				if !wscommon.EnableClusterMode || e.Object.GetNamespace() == wscommon.Namespace ||
					e.Object.GetName() != fmt.Sprintf("%s-controller-manager", e.Object.GetNamespace()) {
					return false
				}
				c.Logger.Info("new controller-manager observed", "deploy", e.Object.GetName())
				return true
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				// skip watching on self deployment
				if !wscommon.EnableClusterMode || e.ObjectNew.GetNamespace() == wscommon.Namespace ||
					!strings.Contains(e.ObjectNew.GetName(), "controller-manager") {
					return false
				}
				c.Logger.Info("updated controller-manager observed", "deploy", e.ObjectNew.GetName())
				return true
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				// skip watching on self deployment
				if !wscommon.EnableClusterMode || e.Object.GetNamespace() == wscommon.Namespace ||
					!strings.Contains(e.Object.GetName(), "controller-manager") {
					return false
				}
				c.Logger.Info("deleted controller-manager observed", "deploy", e.Object.GetName())
				return true
			},
		},
	); err != nil {
		return err
	}
	return nil
}

func (c *ConfigReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	if strings.HasPrefix(request.Name, deployEventPrefix) {
		return c.ReconcileUserDeploy(ctx, request)
	}

	// defer node/system event processing until cfgMgr has been initialized with the latest config
	isNodeEvent := strings.HasPrefix(request.Name, nodeEventPrefix)
	isSystemEvent := strings.HasPrefix(request.Name, systemEventPrefix)
	if !c.initCfgMgr && (isNodeEvent || isSystemEvent) {
		return wscommon.ConfigInitRequeue, nil
	}

	if isNodeEvent {
		return c.ReconcileNode(ctx, request)
	} else if isSystemEvent {
		return c.ReconcileSystem(ctx, request)
	}

	// config update observed
	cm := &corev1.ConfigMap{}
	err := c.client.Get(ctx, request.NamespacedName, cm)
	if err != nil {
		c.Logger.Error(err, "could not get cluster config")
		return reconcile.Result{}, err
	}
	clusterSchema, err := wscommon.NewClusterSchemaFromCM(cm)
	if err != nil {
		c.Logger.Error(err, "NewClusterSchemaFromCM parse failed")
		return reconcile.Result{}, err
	}

	currentRV := ""
	if c.CfgMgr != nil && c.CfgMgr.Cfg != nil {
		currentRV = c.CfgMgr.Cfg.ResourceVersion
	}
	resourceVersionChanged := !c.initCfgMgr || currentRV != cm.ResourceVersion

	c.Logger.Info("cluster config observed",
		"new-rv", cm.ResourceVersion,
		"current-rv", currentRV)
	// build cfg from configmap
	if resourceVersionChanged {
		err = c.CfgMgr.Initialize(clusterSchema)
		if err != nil {
			c.Logger.Error(err, "NewClusterConfig build failed")
			return reconcile.Result{}, err
		}
		c.initCfgMgr = true
	}
	// for create event or test
	if c.CfgMgr.Cfg != nil && c.CfgMgr.Cfg.ResourceVersion == "" {
		c.CfgMgr.Cfg.ResourceVersion = cm.ResourceVersion
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

// reconcile on node event
func (c *ConfigReconciler) ReconcileNode(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	node := &corev1.Node{}
	request.NamespacedName.Name = strings.TrimPrefix(request.NamespacedName.Name, nodeEventPrefix)
	err := c.client.Get(ctx, request.NamespacedName, node)
	if err != nil {
		c.Logger.Error(err, "could not get node")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	c.CfgMgr.UpdateNode(node)
	return reconcile.Result{}, nil
}

// reconcile on system event
func (c *ConfigReconciler) ReconcileSystem(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	system := &systemv1.System{}
	request.NamespacedName.Name = strings.TrimPrefix(request.NamespacedName.Name, systemEventPrefix)
	err := c.client.Get(ctx, request.NamespacedName, system)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		c.Logger.Error(err, "could not get system")
		return reconcile.Result{}, err
	}
	c.CfgMgr.UpdateSystem(system)
	return reconcile.Result{}, nil
}

//+kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;update;delete

// reconcile on user namespace deploy event for tracking cluster mode + scale down old deploys
func (c *ConfigReconciler) ReconcileUserDeploy(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	deploy := &appv1.Deployment{}
	request.NamespacedName.Name = strings.TrimPrefix(request.NamespacedName.Name, deployEventPrefix)
	err := c.client.Get(ctx, request.NamespacedName, deploy)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			delete(wscommon.ClusterModeDisabledNamespaceMap, request.Namespace)
			return reconcile.Result{}, nil
		}
		c.Logger.Error(err, "could not get deployment", "deploy", request.NamespacedName)
		return reconcile.Result{}, err
	}
	if deploy.Labels["control-plane"] != "controller-manager" {
		return reconcile.Result{}, nil
	}
	clusterModeDisabled, _ := c.CfgMgr.IsClusterModeDisabled(deploy.Namespace)
	// scale down user deploy in cluster mode
	if !clusterModeDisabled && *deploy.Spec.Replicas != 0 {
		c.Logger.Info("scale down deploy for cluster mode", "deploy", request.NamespacedName)
		downScale := int32(0)
		deploy.Spec.Replicas = &downScale
		err = c.client.Update(ctx, deploy)
		if err != nil {
			c.Logger.Error(err, "could not scale down deployment")
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

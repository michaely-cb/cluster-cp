package health

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"cerebras.com/job-operator/common"
)

const (
	alertTypeLabel          = "type"
	alertSummaryAnnotation  = "summary"
	statusTrue              = "True"
	statusFalse             = "False"
	statusUnknown           = "Unknown"
	alertManagerNamespace   = "prometheus"
	alertManagerServiceName = "alertmanager-operated"
)

var (
	// frequency on reconciling nodes/systems/alerts
	syncInterval = 10 * time.Second
	// frequency on syncing on prometheus metrics
	metricSyncInterval = 1 * time.Minute
	// how long should condition persists with false status after alerts gone
	conditionTTL = 24 * time.Hour
)

// Reconciler returns Reconciler that will update nodes/systems
// with updates to NodeConditions from alerts specific to the node. As alerts
// are not known ahead, the NodeConditionType is prefixed with "ClusterMgmt_" to allow the reconciler
// to distinguish NodeConditions it "owns" from those it does not. It will not modify non-"owned"
// NodeConditions.
//
// NodeConditions created from a given alert have the provided structure:
//
//     	NodeCondition{
//		    Type:               "ClusterMgmt" + $labels.type
//		    Status:             True - if firing,
//		                        False if not firing,
//		                        Unknown if alerts are unavailable
//		    LastHeartbeatTime:  currentTime,
//		    LastTransitionTime: currentTime if status changed,
//		    Reason:             One of "AlertIsFiring", "AlertIsNotFiring", "AlertsUnavailable"
//		    Message:            $annotations.summary if present
//	    }

type reconciler struct {
	log         logr.Logger
	recorder    record.EventRecorder
	client      client.Client
	AlertCache  AlertCache
	MetricCache MetricCache
	Disabled    bool
}

func NewHealthReconciler(
	mgr manager.Manager, disable bool,
) (error, *NodeReconciler, *SystemReconciler) {
	// skip health reconcile for non-system deploy
	if common.Namespace != common.SystemNamespace {
		return nil, nil, nil
	}
	alertSync, err := NewAlertSyncer(mgr.GetLogger())
	if err != nil {
		return err, nil, nil
	}

	var monitorEnabled bool
	err = mgr.GetAPIReader().Get(context.Background(),
		client.ObjectKey{Namespace: alertManagerNamespace, Name: alertManagerServiceName}, &corev1.Service{})
	// don't fail operator since this health reconciler can be optional in some envs without prometheus
	// if alert-manager is not available, keep reconciling to clean up stale alarms
	if err != nil {
		mgr.GetLogger().Error(err, "Alert manager not deployed, skip health reconciling")
	} else {
		monitorEnabled = true
		alertSync.SyncOnce()
		err = mgr.Add(alertSync)
		if err != nil {
			return err, nil, nil
		}
	}

	metricSync := NewMetricSyncer(mgr.GetLogger(), common.NewPromMetricsQuerier(""))
	if monitorEnabled {
		metricSync.SyncOnce()
		err = mgr.Add(metricSync)
		if err != nil {
			return err, nil, nil
		}
	}
	r := reconciler{
		log:         mgr.GetLogger(),
		client:      mgr.GetClient(),
		AlertCache:  alertSync,
		MetricCache: metricSync,
		Disabled:    disable,
	}
	nR := NewNodeReconciler(r)
	if err = nR.SetupWithManager(mgr); err != nil {
		return err, nil, nil
	}
	sR := NewSystemReconciler(r)
	if err = sR.SetupWithManager(mgr); err != nil {
		return err, nil, nil
	}
	return nil, nR, sR
}

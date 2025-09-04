package cluster

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	systemv1 "cerebras.com/job-operator/apis/system/v1"
	"cerebras.com/job-operator/common"
)

// SystemUpdater creates/updates/deletes system CRD based on cluster config
type SystemUpdater struct {
	CfgChangeWatcher

	client client.Client
}

func NewSystemUpdater(
	client client.Client,
	logger logr.Logger,
) *SystemUpdater {

	updater := &SystemUpdater{client: client}
	updater.CfgChangeWatcher = newCfgChangeWatcher(
		logger.WithValues("component", "SystemUpdater"),
		updater.tryApplyCfgChange,
		[]common.ConfigUpdateReason{common.ConfigUpdateReload},
	)
	return updater
}

// tryApplyCfgChange creates or updates systems in the cfg, otherwise deletes any systems in k8s which are not in cfg
func (su *SystemUpdater) tryApplyCfgChange(ctx context.Context, cfg *common.ClusterConfig) error {
	// only apply updates by system deployment
	if common.SystemNamespace != common.Namespace {
		return nil
	}

	sl := &systemv1.SystemList{}
	if err := su.client.List(ctx, sl); err != nil {
		su.logger.Error(err, "unable to list system CRDs")
		return err
	}
	k8sSys := map[string]systemv1.System{}
	for _, sys := range sl.Items {
		k8sSys[sys.Name] = sys
	}

	for _, cfgSpec := range cfg.Systems {
		k8Sys, ok := k8sSys[cfgSpec.Name]
		if !ok {
			su.logger.Info("system crd create detected", "new", cfgSpec)
			if err := su.createSystem(cfgSpec); err != nil {
				return err
			}
		} else if ok && !k8Sys.Spec.Equals(cfgSpec) {
			// update system crd if cfgSpec diff, exclude unschedulable which is managed by health reconciler/cli
			// skip creation which will be done by cluster config init time
			su.logger.Info("system crd update detected", "old", k8Sys.Spec, "new", cfgSpec)
			k8Sys.Spec.Copy(cfgSpec)
			if err := su.client.Update(ctx, &k8Sys); err != nil {
				su.logger.Error(err, "unable to update system CRD", "spec", cfgSpec)
				return err
			}
		}
		delete(k8sSys, cfgSpec.Name)
	}

	// remove stale systems
	for _, sys := range k8sSys {
		su.logger.Info("stale system crd detected", "sys", sys)
		if err := su.client.Delete(ctx, &sys); err != nil {
			su.logger.Error(err, "unable to delete system CRD", "cfgSpec", sys.Spec)
			return err
		}
	}
	return nil
}

func (su *SystemUpdater) createSystem(cfgSpec *systemv1.SystemSpec) error {
	s := &systemv1.System{
		ObjectMeta: ctrl.ObjectMeta{
			Name: cfgSpec.Name,
			Labels: map[string]string{
				common.NamespaceKey: "", // signal to NS reconciler that this is a new system
			},
		},
		Spec: *cfgSpec,
	}

	if err := su.client.Create(context.Background(), s); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		su.logger.Error(err, "unable to create system CRD", "name", s.Name)
		return err
	}
	return nil
}

package cluster

import (
	"context"
	"encoding/json"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"cerebras.com/job-operator/common"
)

type NodeLabeler struct {
	CfgChangeWatcher

	client      client.Client
	nodePatcher nodePatcher
}

// disable for integration tests to keep log clean
var DisableLabeler = false

func NewNodeLabeler(
	client client.Client,
	logger logr.Logger) *NodeLabeler {

	labeler := &NodeLabeler{
		client:      client,
		nodePatcher: newNodePatcher(logger),
	}
	labeler.CfgChangeWatcher = newCfgChangeWatcher(
		logger.WithValues("component", "NodeLabeler"),
		labeler.tryApplyCfgChange,
		[]common.ConfigUpdateReason{common.ConfigUpdateReload, common.ConfigUpdateNamespace},
	)

	return labeler
}

// TODO: A prod-worthy solution would wait until the cluster was running no jobs prior to applying changes to nodes since
// there's potential for scheduling interleaving where BRs are scheduled to a node which gets reassigned CS Ports
// an oversubscribes that node
// E.g.
//
//	T0: BRNode0{csPort0:6, Pod{csPort0:6}} + Unscheduled[Pod{csPort1:6}]
//	T1: NodeLabeling Occurs, BRNode0 reassigned csPort1
//	T2: BRNode0{csPort1:6, Pod{csPort0:6}, Pod{csPort1:6}} << broken state
//
// Additionally, because the nodes may not be labeled before the first job is reconciled, there could be a time period
// when wsjob Pods are created but unschedulable due to missing labels.
// todo: move this to cluster config build time
func (n *NodeLabeler) tryApplyCfgChange(ctx context.Context, cfg *common.ClusterConfig) error {
	if common.SystemNamespace != common.Namespace || DisableLabeler {
		return nil
	}

	nodePatch, err := n.findPatches(cfg)
	if err != nil {
		return err
	}
	// ideally as the above comment, apply should be done only when no running jobs
	if len(nodePatch) > 0 {
		if err = n.applyPatches(ctx, nodePatch); err != nil {
			return err
		}
	}
	return nil
}

func (n *NodeLabeler) findPatches(cfg *common.ClusterConfig) (map[*v1.Node]nodePatches, error) {
	patches := map[*v1.Node]nodePatches{}
	matchNodes := 0

	for _, k8sNode := range cfg.K8sNodeMap {
		if cfgNode, ok := cfg.NodeMap[k8sNode.Name]; ok {
			ns := ""
			if !cfg.IsSharableAcrossNs(k8sNode.Name) {
				ns = cfg.GetNodeNamespace(k8sNode.Name)
			}
			if p := n.nodePatcher.getUpdatePatches(cfgNode, k8sNode, ns); !p.empty() {
				patches[k8sNode] = p
			}
			matchNodes++
		} else {
			n.logger.WithValues("node", k8sNode.Name).
				Info("node not found in configmap, removing any labels if present")
			if p := getRemovePatches(k8sNode); !p.empty() {
				patches[k8sNode] = p
			}
		}
	}

	if len(cfg.NodeMap)-matchNodes > 0 {
		n.logger.WithValues("orphanNodes", common.Keys(cfg.NodeMap)).
			Info("warning: nodes existed in config which did not exist in k8s")
	}
	return patches, nil
}

func (n *NodeLabeler) applyPatches(ctx context.Context, patches map[*v1.Node]nodePatches) error {
	n.logger.WithValues("nodes", common.Map(func(n *v1.Node) string { return n.Name }, common.Keys(patches))).
		Info("changes between config and cluster detected, applying labeling changes")

	var jpatch []byte
	var err error
	for k8sNode, p := range patches {
		if len(p.node) > 0 {
			if jpatch, err = json.Marshal(p.node); err != nil {
				return err
			}
			n.logger.WithValues("node", k8sNode.Name, "patch", p.node).Info("patch node")
			if err = n.client.Patch(ctx, k8sNode, client.RawPatch(types.JSONPatchType, jpatch)); err != nil {
				return err
			}
		}

		if len(p.status) > 0 {
			if jpatch, err = json.Marshal(p.status); err != nil {
				return err
			}
			n.logger.WithValues("node", k8sNode.Name, "patch", p.status).Info("patch node status")
			if err = n.client.Status().Patch(ctx, k8sNode, client.RawPatch(types.JSONPatchType, jpatch)); err != nil {
				return err
			}
		}
	}

	n.logger.Info("update complete")
	return nil
}

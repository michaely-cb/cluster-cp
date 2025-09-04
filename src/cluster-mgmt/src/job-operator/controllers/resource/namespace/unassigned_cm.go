package namespace

import (
	nsv1 "cerebras.com/job-operator/apis/namespace/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"

	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

func (r *Reconciler) updateUnassignedResourcesCM(ctx reconcileCtx) error {
	freeNSR := &nsv1.NamespaceReservation{ObjectMeta: metav1.ObjectMeta{Name: nsv1.UnassignedNamespace}}

	for _, rs := range r.collection.List() {
		if !rs.InUnassignedPool() {
			continue
		}
		switch rs.Type() {
		case resource.SystemType:
			freeNSR.Status.Systems = append(freeNSR.Status.Systems, rs.Name())
		case resource.NodegroupType:
			freeNSR.Status.Nodegroups = append(freeNSR.Status.Nodegroups, rs.Name())
		case resource.NodeType:
			freeNSR.Status.Nodes = append(freeNSR.Status.Nodes, rs.Name())
		}
	}

	resourceSpec := estimateResourceSpec(r.collection, freeNSR.Status, freeNSR.GetWorkloadType())
	freeNSR.Status.CurrentResourceSpec = resourceSpec
	freeNSR.Status.GrantedRequest.RequestParams = nsv1.RequestParams{
		SystemCount:           common.Pointer(len(freeNSR.Status.Systems)),
		ParallelCompileCount:  common.Pointer(resourceSpec.ParallelCompileCount),
		ParallelExecuteCount:  common.Pointer(resourceSpec.ParallelExecuteCount),
		LargeMemoryRackCount:  common.Pointer(resourceSpec.LargeMemoryRackCount),
		XLargeMemoryRackCount: common.Pointer(resourceSpec.XLargeMemoryRackCount),
	}

	// sort for consistent equality check when deciding to patch or not
	sort.Strings(freeNSR.Status.Systems)
	sort.Strings(freeNSR.Status.Nodegroups)
	sort.Strings(freeNSR.Status.Nodes)

	return r.createOrUpdateFreeNsrCM(ctx, freeNSR)
}

func (r *Reconciler) createOrUpdateFreeNsrCM(ctx reconcileCtx, freeNSR *nsv1.NamespaceReservation) error {
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: nsv1.UnassignedResourcesCM, Namespace: common.Namespace},
	}
	cmData := map[string]string{}
	if b, err := yaml.Marshal(freeNSR); err != nil {
		ctx.log.Error(err, "failed to marshal freeNSR yaml")
		return err
	} else {
		cmData[nsv1.UnassignedResourcesCMDataKey] = string(b)
	}

	// create if not exists
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(&cm), &cm); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		cm.Data = cmData
		return r.client.Create(ctx, &cm)
	}

	if cmData[nsv1.UnassignedResourcesCMDataKey] == cm.Data[nsv1.UnassignedResourcesCMDataKey] {
		ctx.log.V(1).Info("skip patch unassigned resources CM, data not changed")
		return nil
	}

	// patch otherwise
	patchBytes, err := json.Marshal(map[string]interface{}{"data": cmData})
	if err != nil {
		return err
	}
	return r.client.Patch(ctx, &cm, client.RawPatch(types.StrategicMergePatchType, patchBytes))
}

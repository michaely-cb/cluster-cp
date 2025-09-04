package namespace

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

const (
	clusterSvr                = "cluster-server"
	jobOperator               = "job-operator-controller-manager"
	ClusterSvrRedirectSvc     = "cluster-server-redirect"
	NSRScaleDownNowAnnotation = "cerebras/scale-down-now" // for test trigger
	ContainsCrdNodesAnno      = "cerebras/session-contains-crd-nodes"
)

var (
	// Number of scaled up sessions over the number of populated nodegroups that are allowed to be scaled up
	// prior to being included in job-operator auto-scale down candidate selection
	MaxAllowedUpSessionsOverPopNGThreshold = 2

	// Min time between now and last update when a session can be considered for scale down
	SessionIdleThresholdForScaleDown = time.Hour * 24 * 7

	// Min time between now and last update when a session can be considered for cleanup
	SessionIdleThresholdForCleanup = time.Hour * 24 * 30

	// ClusterServers created within this min age will not be considered for scale down
	ClusterServerScaleDownMinAge = time.Hour
)

// Finds idle user namespaces without resource assignments for recycle
func (r *Reconciler) findNSRecycleCandidates() ([]*namespacev1.NamespaceReservation, []*namespacev1.NamespaceReservation) {
	ngFilter := resource.NewFilter(
		resource.NodegroupType,
		map[string][]string{wsapisv1.MemxPopNodegroupProp: {"true"}},
		nil,
	)

	var scaleDowns []*namespacev1.NamespaceReservation
	var cleanups []*namespacev1.NamespaceReservation
	for _, nsr := range r.nsState.reservations {
		if nsr.Status.HasResources() || nsr.Name == common.SystemNamespace {
			continue
		}
		if nsr.GetLastUpdateTime().Add(SessionIdleThresholdForCleanup).Before(time.Now()) {
			cleanups = append(cleanups, nsr)
		} else if nsr.GetLastUpdateTime().Add(SessionIdleThresholdForScaleDown).Before(time.Now()) {
			scaleDowns = append(scaleDowns, nsr)
		}
	}
	sort.Slice(scaleDowns, func(i, j int) bool {
		ti := scaleDowns[i].GetLastUpdateTime().Time
		tj := scaleDowns[j].GetLastUpdateTime().Time
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return scaleDowns[i].Name < scaleDowns[j].Name
	})

	// conservatively scale down
	maxAllowedUpSessions := r.collection.Filter(ngFilter).Size() + MaxAllowedUpSessionsOverPopNGThreshold
	minScaleDownRequired := len(r.nsState.reservations) - maxAllowedUpSessions - len(cleanups)
	if minScaleDownRequired < 0 {
		minScaleDownRequired = 0
	}
	if minScaleDownRequired > len(scaleDowns) {
		minScaleDownRequired = len(scaleDowns)
	}
	return scaleDowns[:minScaleDownRequired], cleanups
}

// Delete NSR
func (r *Reconciler) cleanupNS(ctx reconcileCtx, nsr *namespacev1.NamespaceReservation) error {
	ctx.log.Info("deleting idle session after threshold reached", "namespace", nsr.Name)
	if err := r.client.Delete(ctx, nsr); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// Scales down a user namespace by creating an external name service that points to the system namespace cluster-server,
// updating the ingress to point to this new external name service, and finally scaling down the cluster-server.
// From an end-user perspective, their csctl calls will continue to work with their existing certs, but be serviced by
// the system-namespace cluster server (cluster-server filters on resourceNS header value set by csctl so resource/job
// view is consistent as with their scaled up csctl view).
func (r *Reconciler) scaleDownNS(ctx reconcileCtx, nsr *namespacev1.NamespaceReservation) error {
	deploy := &appv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: clusterSvr, Namespace: nsr.Name}}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(deploy), deploy); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas == 0 {
		ctx.log.V(1).Info("skip scale down cluster-server: already set to 0 replicas", "namespace", nsr.Name)
		return nil
	}

	_, force := nsr.Annotations[NSRScaleDownNowAnnotation]
	if !force && deploy.CreationTimestamp.After(time.Now().Add(-ClusterServerScaleDownMinAge)) {
		ctx.log.Info(
			"skip scale down cluster-server: cluster server was created too recently",
			"namespace", nsr.Name, "createTime", deploy.CreationTimestamp)
		return nil
	}

	ctx.log.Info("creating cluster-server system namespace redirection service", "namespace", nsr.Name)
	sysNsSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: ClusterSvrRedirectSvc, Namespace: nsr.Name},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: fmt.Sprintf("cluster-server.%s.svc.cluster.local", common.SystemNamespace),
		},
	}
	if err := r.client.Create(ctx, sysNsSvc); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			ctx.log.Info("failed to create redirect service on scale down", "error", err.Error())
			return err
		}
	}

	ctx.log.Info("updating cluster-server ingress to point to system namespace cluster-server", "namespace", nsr.Name)
	nsIngress := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: clusterSvr, Namespace: nsr.Name}}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(nsIngress), nsIngress); err != nil {
		ctx.log.Info("failed to get ingress on scale down", "error", err.Error())
		return err
	}
	updateIngressBackendSvc(nsIngress, ClusterSvrRedirectSvc)
	if err := r.client.Update(ctx, nsIngress); err != nil {
		ctx.log.Info("failed to update ingress on scale down", "error", err.Error())
		return err
	}

	ctx.log.Info("scaling down cluster-server", "namespace", nsr.Name)
	deploy.Annotations = common.EnsureMap(deploy.Annotations)
	deploy.Annotations[originalReplicaCountAnno] = strconv.Itoa(int(*deploy.Spec.Replicas))
	deploy.Spec.Replicas = common.Pointer(int32(0))
	if err := r.client.Update(ctx, deploy); err != nil {
		ctx.log.Info("failed to update cluster-server on scale down", "error", err.Error())
		return err
	}
	return nil
}

// Reverses the operations performed by scaleDownNS. If deploy exists and replicaCount is 0, scale back to the
// original replica count. Update the cluster-server ingress to point to the original cluster-server namespace.
// Finally, remove the system namespace redirection service.
func (r *Reconciler) scaleUpNS(ctx reconcileCtx, nsr *namespacev1.NamespaceReservation) error {
	deploy := &appv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: clusterSvr, Namespace: nsr.Name}}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(deploy), deploy); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	replicaCount := *deploy.Spec.Replicas
	if replicaCount == 0 {
		ctx.log.Info("scaling up cluster-server", "namespace", nsr.Name)
		count := 1
		if countStr, ok := deploy.Annotations[originalReplicaCountAnno]; ok {
			if c, err := strconv.Atoi(countStr); err == nil && c > 0 {
				count = c
			}
		}
		delete(deploy.Annotations, originalReplicaCountAnno)
		deploy.Spec.Replicas = common.Pointer(int32(count))
		deploy.Annotations[systemCountAnno] = strconv.Itoa(len(nsr.Status.Systems))
		if err := r.client.Update(ctx, deploy); err != nil {
			return err
		}
		ctx.log.V(1).Info("scaled up cluster-server", "namespace", nsr.Name)
	}

	ctx.log.Info("updating cluster-server ingress to point to user namespace cluster-server", "namespace", nsr.Name)
	nsIngress := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: clusterSvr, Namespace: nsr.Name}}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(nsIngress), nsIngress); err != nil {
		return err
	}
	updateIngressBackendSvc(nsIngress, clusterSvr)
	if err := r.client.Update(ctx, nsIngress); err != nil {
		return err
	}

	ctx.log.Info("delete cluster-server system namespace redirection service", "namespace", nsr.Name)
	sysNsSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: ClusterSvrRedirectSvc, Namespace: nsr.Name}}
	if err := r.client.Delete(ctx, sysNsSvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// annotate system count to trigger webhook adjust memory / pod template to trigger a redeploy
func (r *Reconciler) annotateDeploy(ctx reconcileCtx, nsr *namespacev1.NamespaceReservation, clusterSysCount int) error {
	ctx.log.V(1).Info("update annotation with system count for mem adjust start", "namespace", nsr.Name)
	containsCrdNodes := r.containsCrdNodes(nsr.Status.Nodes)

	// update session level server
	if nsr.Name != common.SystemNamespace {
		deploy := &appv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: clusterSvr, Namespace: nsr.Name}}
		if err := r.updateDeploymentAnnotationsAndApply(
			ctx,
			deploy,
			len(nsr.Status.Systems),
			containsCrdNodes,
			true,
		); err != nil {
			return err
		}
	}

	// update system level server if necessary
	deploy := &appv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: clusterSvr, Namespace: common.SystemNamespace}}

	if err := r.updateDeploymentAnnotationsAndApply(
		ctx,
		deploy,
		clusterSysCount,
		containsCrdNodes,
		false,
	); err != nil {
		return err
	}

	// update operator if necessary
	deploy = &appv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: jobOperator, Namespace: common.SystemNamespace}}
	if err := r.updateDeploymentAnnotationsAndApply(
		ctx,
		deploy,
		clusterSysCount,
		containsCrdNodes,
		false,
	); err != nil {
		return err
	}

	return nil
}

func updateIngressBackendSvc(ingress *networkingv1.Ingress, svcName string) {
	for ruleIndex := range ingress.Spec.Rules {
		rule := &ingress.Spec.Rules[ruleIndex]
		httpRule := rule.HTTP
		if httpRule == nil {
			continue
		}
		for pathIndex := range httpRule.Paths {
			svc := httpRule.Paths[pathIndex].Backend.Service
			if svc == nil {
				continue
			}
			svc.Name = svcName
		}
	}
}

func (r *Reconciler) containsCrdNodes(assignedNodes []string) bool {
	crdNodes := []string{}
	for _, nodeName := range assignedNodes {
		if node, exists := r.collection.Get(resource.NodeType + "/" + nodeName); exists {
			if _, exists = node.GetProp(resource.RoleCrdPropKey); exists {
				crdNodes = append(crdNodes, nodeName)
			}
		}
	}
	return len(crdNodes) > 0
}

// updates the system count and coordinator node annotations on the given deployment if necessary
func (r *Reconciler) updateDeploymentAnnotationsAndApply(
	ctx reconcileCtx,
	deploy *appv1.Deployment,
	systemCount int,
	containsCrdNodes bool,
	shouldCheckCrdNodesAnnotation bool,
) error {
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(deploy), deploy); err == nil {
		updated := false

		countStr := strconv.Itoa(systemCount)
		if deploy.Annotations[systemCountAnno] != countStr {
			deploy.Annotations = common.EnsureMap(deploy.Annotations)
			deploy.Annotations[systemCountAnno] = countStr
			updated = true
		}

		if shouldCheckCrdNodesAnnotation {
			// Check if we either removed the last coordinator node or added the first one
			// If so we need to update the annotation at the pod template level rather than the deployment level to trigger a rollout
			deploy.Spec.Template.Annotations = common.EnsureMap(deploy.Spec.Template.Annotations)
			current, exists := deploy.Spec.Template.Annotations[ContainsCrdNodesAnno]
			desired := strconv.FormatBool(containsCrdNodes)
			// Old cluster-server deployments may not have this annotation set, so we need to check if it exists
			if exists && current != desired {
				deploy.Spec.Template.Annotations[ContainsCrdNodesAnno] = desired
				updated = true
			}
		}

		if updated {
			if err := r.client.Update(ctx, deploy); err != nil {
				return err
			}
		}
	}

	return nil
}

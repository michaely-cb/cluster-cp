//go:build integration && monitoring

package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubeclientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
)

func (t *TestSuite) TestClusterMonitorIntegration() {
	ctx := context.Background()
	t.Run("resource_version_monitor/schedule", func() {
		wscommon.EnablePlatformVersionCheck = true
		t.NodeReconciler.Disabled = false
		t.SystemReconciler.Disabled = false
		defer func() {
			wscommon.EnablePlatformVersionCheck = false
			t.NodeReconciler.Disabled = true
			t.SystemReconciler.Disabled = true
		}()
		version := "rel-2.5.0"
		t.updateSessionLabel(wscommon.Namespace, wsapisv1.PlatformVersionKey, version)
		t.updateSessionLabel(wscommon.Namespace, wsapisv1.SystemVersionKey, version)

		// job failed due to inconsistent version
		jobName := "schedule-inconsistent-version"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, 2)
		wsjob.Labels[rlv1.SystemVersionRequired] = "true"
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertWsJobFailEventually(t.T(), wsjob.Namespace, wsjob.Name)
		cond := getConditions(t.T(), wsjob.Namespace, wsjob.Name)[commonv1.JobFailed]
		assert.Contains(t.T(), cond.Message,
			"requested 2 system[namespace:job-operator, system-version:rel-2.5.0] but only 0 available")
		assert.Contains(t.T(), cond.Message,
			"requested 1 node[namespace:job-operator, platform-version-rel-2.5.0:, role:coordinator] but only 0 available")
		assert.Contains(t.T(), cond.Message,
			"ng-secondaries: 0 of 8 groups scheduled "+
				"but required 1 (largest candidate group failed with group request[ng-secondaries:Activation], "+
				"props[namespace:job-operator, platform-version-rel-2.5.0:, role:memory], anti-affinity[map[]], matched no node resources)")
		assert.Contains(t.T(), cond.Message,
			"ng-primary: 0 of 8 groups scheduled "+
				"but required 1 (largest candidate group failed with group request[ng-primary:Activation], "+
				"props[namespace:job-operator, platform-version-rel-2.5.0:, role:memory], anti-affinity[map[]], matched no node resources)")
		assertWsjobCleanedUp(t.T(), wsjob)

		// update nodes version
		sxPorts := map[int]bool{}
		for _, n := range t.CfgMgr.Cfg.Nodes {
			if n.GetGroup() == "group0" || n.GetGroup() == "group1" ||
				n.GetRole() == wscommon.RoleCoordinator.String() ||
				n.GetRole() == wscommon.RoleBroadcastReduce.String() {
				// only need 12 ports for SX
				if n.GetRole() == wscommon.RoleBroadcastReduce.String() && len(sxPorts) < 12 {
					if sxPorts[*n.NICs[0].CsPort] || sxPorts[*n.NICs[len(n.NICs)-1].CsPort] {
						continue
					}
					sxPorts[*n.NICs[0].CsPort] = true
					sxPorts[*n.NICs[len(n.NICs)-1].CsPort] = true
				}
				node := &corev1.Node{}
				require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: n.Name}, node))
				node.Labels[wsapisv1.PlatformVersionKey] = version
				require.NoError(t.T(), k8sClient.Update(ctx, node))
			}
		}
		// remove session version prop and test on the code path of filter by local system version
		t.updateSessionLabel(wscommon.Namespace, wsapisv1.SystemVersionKey, "")
		cache := &mockMetricCache{
			Cache: map[string]string{
				"system-1": version,
				"system-2": version,
				"system-3": version,
			},
		}
		t.SystemReconciler.MetricCache = cache

		alert := &mockAlertCache{
			Cache: map[string]models.GettableAlerts{},
		}
		t.SystemReconciler.AlertCache = alert
		// update one system to unhealthy to validate label/status patch together
		msg := "System system-1 is in error state for 5min"
		alert.Cache["system-1"] = []*models.GettableAlert{
			{
				Annotations: map[string]string{
					"summary": msg,
				},
				Alert: models.Alert{
					Labels: map[string]string{
						"type":     "UnknownError",
						"instance": "system-1",
					},
				},
			},
		}

		// trigger reconcile
		sys := &systemv1.System{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: "system-1"}, sys))
		sys.Status.Message = "test"
		require.NoError(t.T(), k8sClient.Status().Update(ctx, sys))
		sys = &systemv1.System{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: "system-2"}, sys))
		sys.Status.Message = "test"
		require.NoError(t.T(), k8sClient.Status().Update(ctx, sys))
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: "system-3"}, sys))
		sys.Status.Message = "test"
		require.NoError(t.T(), k8sClient.Status().Update(ctx, sys))
		require.Eventually(t.T(), func() bool {
			_, nOk := t.ResourceController.Cfg.GetCoordNodes()[0].Properties[wsresource.PlatformVersionResKey(version)]
			s1Ok := t.ResourceController.Cfg.Systems[1].Properties[wsresource.SystemVersionPropertyKey] == version
			s2Ok := t.ResourceController.Cfg.Systems[2].Properties[wsresource.SystemVersionPropertyKey] == version
			s3Ok := t.ResourceController.Cfg.Systems[3].Properties[wsresource.SystemVersionPropertyKey] == version
			return nOk && s1Ok && s2Ok && s3Ok && len(t.ResourceController.Cfg.UnhealthySystems) == 1
		}, eventuallyTimeout, eventuallyTick)
		defer func() {
			cache.Cache = map[string]string{}
			alert.Cache = map[string]models.GettableAlerts{}
			s := &systemv1.System{}
			require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: "system-1"}, s))
			s.Status.Message = "done"
			require.NoError(t.T(), k8sClient.Status().Update(ctx, s))
			require.Eventually(t.T(), func() bool {
				return len(t.ResourceController.Cfg.UnhealthySystems) == 0
			},
				eventuallyTimeout, eventuallyTick)
		}()

		// job success with version filter on node/system
		jobName = "schedule-success-version"
		wsjob = newWsExecuteJob(wscommon.Namespace, jobName, 2)
		wsjob.Labels[rlv1.SystemVersionRequired] = "true"
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertWsJobScheduledEventually(t.T(), wsjob.Namespace, wsjob.Name)
		lock := assertLockExists(t.T(), wsjob.Namespace, wsjob.Name)
		require.Equal(t.T(), "system-2", lock.Status.SystemGrants[0].Name)
		require.Equal(t.T(), "system-3", lock.Status.SystemGrants[1].Name)
		assertWsjobCleanedUp(t.T(), wsjob)
	})

	t.Run("hardware_error_msg_populated", func() {
		// lock success with all systems
		jobName := "hardware-events"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		defer assertWsjobCleanedUp(t.T(), wsjob)

		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		wsjob = assertWsJobScheduledEventually(t.T(), wsjob.Namespace, jobName)
		// simulate job in Running state
		pod := assertPodCountEventually(t.T(), wsjob.Namespace, jobName, rlv1.CoordinatorRequestName, 1).Items[0]
		pod.Status.Phase = corev1.PodRunning
		require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		assertWsJobRunningEventually(t.T(), wsjob.Namespace, jobName)

		t.SystemReconciler.Disabled = false
		t.NodeReconciler.Disabled = false
		defer func() {
			t.SystemReconciler.Disabled = true
			t.NodeReconciler.Disabled = true
		}()
		cache := &mockAlertCache{
			Cache: map[string]models.GettableAlerts{},
		}
		t.SystemReconciler.AlertCache = cache
		t.NodeReconciler.AlertCache = cache

		// update one system to unhealthy during job runtime
		msg := "System system-0 is in error state for 5min"
		cache.Cache[t.JobReconciler.Cfg.Systems[0].Name] = []*models.GettableAlert{
			{
				Annotations: map[string]string{
					"summary": msg,
				},
				Alert: models.Alert{
					Labels: map[string]string{
						"type":     wscommon.SystemErrorAlertType,
						"instance": "system-0",
					},
				},
			},
		}
		// trigger event explicitly
		patchNodeCondition(t, wsjob.Annotations[CoordinatorNodeNameAnnotationKey],
			[]corev1.NodeCondition{{
				Type:   corev1.NodeDiskPressure,
				Status: corev1.ConditionTrue,
			}})
		s := &systemv1.System{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: "system-0"}, s))
		s.Status.Message = "test"
		require.NoError(t.T(), k8sClient.Status().Update(ctx, s))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 1
		}, eventuallyTimeout, eventuallyTick)
		defer func() {
			// update system/node back to healthy
			require.NoError(t.T(), patchNode(t, wsjob.Annotations[CoordinatorNodeNameAnnotationKey], nil))
			cache.Cache = map[string]models.GettableAlerts{}
			s = &systemv1.System{}
			require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: "system-0"}, s))
			s.Status.Message = "done"
			require.NoError(t.T(), k8sClient.Status().Update(ctx, s))
			require.Eventually(t.T(), func() bool {
				return len(t.ResourceController.Cfg.UnhealthySystems) == 0
			}, eventuallyTimeout, eventuallyTick)
		}()

		// expect error msg populated
		events := wscommon.GetJobFailureEvents(kubeclientset.NewForConfigOrDie(t.Manager.GetConfig()), wsjob, map[string]bool{})
		require.True(t.T(), strings.Contains(events[0].Message, msg) || strings.Contains(events[1].Message, msg))
		require.True(t.T(), strings.Contains(events[0].Message, string(corev1.NodeDiskPressure)) ||
			strings.Contains(events[1].Message, string(corev1.NodeDiskPressure)))
	})

	t.Run("unhealthy_system_state_persisted_during_restart", func() {
		s := &systemv1.System{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: t.CfgMgr.Cfg.Systems[0].Name}, s))
		s.Spec.Unschedulable = true
		require.NoError(t.T(), k8sClient.Update(ctx, s))
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 1
		}, eventuallyTimeout, eventuallyTick)
		node := &corev1.Node{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: t.CfgMgr.Cfg.Nodes[0].Name}, node))
		node.Spec.Unschedulable = true
		require.NoError(t.T(), k8sClient.Update(ctx, node))
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 1
		}, eventuallyTimeout, eventuallyTick)

		// update config to trigger reload
		updateClusterConfig(t.T(), t.CfgMgr, t.DefaultCfgSchema)
		require.Eventually(t.T(), func() bool {
			return len(t.CfgMgr.Cfg.UnhealthySystems) == 1 && len(t.ResourceController.Cfg.UnhealthySystems) == 1
		}, eventuallyTimeout, eventuallyTick)
		require.Eventually(t.T(), func() bool {
			return len(t.CfgMgr.Cfg.UnhealthyNodes) == 1 && len(t.ResourceController.Cfg.UnhealthyNodes) == 1
		}, eventuallyTimeout, eventuallyTick)

		// update back to healthy
		s.Spec.Unschedulable = false
		require.NoError(t.T(), k8sClient.Update(ctx, s))
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 0
		},
			eventuallyTimeout, eventuallyTick)
		node.Spec.Unschedulable = false
		require.NoError(t.T(), k8sClient.Update(ctx, node))
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 0
		},
			eventuallyTimeout, eventuallyTick)
	})

	t.Run("resource-lock_failed_due_to_unhealthy_system", func() {
		// update system to unhealthy
		s := &systemv1.System{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: t.CfgMgr.Cfg.Systems[0].Name}, s))
		s.Spec.Unschedulable = true
		require.NoError(t.T(), k8sClient.Update(ctx, s))
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 1
		},
			eventuallyTimeout, eventuallyTick)

		// lock failed due to not enough systems
		jobName := "schedule-fail-unhealthy-system"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertWsJobFailEventually(t.T(), wsjob.Namespace, wsjob.Name)
		cond := getConditions(t.T(), wsjob.Namespace, wsjob.Name)[commonv1.JobFailed]
		assert.Contains(t.T(), cond.Message,
			"cluster lacks requested capacity possibly due to unhealthy resources: "+
				"requested 8 system[namespace:job-operator] but only 7")
		assert.Contains(t.T(), cond.Message, " 1 system unhealthy: system-0")
		assertWsjobCleanedUp(t.T(), wsjob)

		// update system back to healthy
		s.Spec.Unschedulable = false
		require.NoError(t.T(), k8sClient.Update(ctx, s))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 0
		},
			eventuallyTimeout, eventuallyTick)

		// lock success after recovery
		jobName = "schedule-success-system-recovery"
		wsjob = newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertLockCountEventually(t.T(), 1)
		t.Require().Eventually(func() bool {
			condStatus, ok := getConditions(t.T(), wsjob.Namespace, jobName)[commonv1.JobScheduled]
			return ok && condStatus.Status == corev1.ConditionTrue
		}, eventuallyTimeout, eventuallyTick)
		assertWsjobCleanedUp(t.T(), wsjob)
	})

	t.Run("resource-lock_failed_due_to_unhealthy_coordinator_node", func() {
		// update coord node to unhealthy
		coordNodes := t.CfgMgr.Cfg.GetCoordNodes()
		require.Len(t.T(), coordNodes, 1)
		coordNode := &corev1.Node{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: coordNodes[0].Name}, coordNode))
		coordNode.Status.Conditions = []corev1.NodeCondition{
			{
				Type:   wscommon.CsadmConditionPrefix + wscommon.NodeErrorAlertType,
				Status: corev1.ConditionTrue,
			},
		}
		require.NoError(t.T(), k8sClient.Status().Update(ctx, coordNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 1
		},
			eventuallyTimeout, eventuallyTick)

		// lock failed due to not enough coord node
		jobName := "schedule-fail-unhealthy-coord"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, 1)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertWsJobFailEventually(t.T(), wsjob.Namespace, wsjob.Name)
		cond := getConditions(t.T(), wsjob.Namespace, wsjob.Name)[commonv1.JobFailed]
		assert.Contains(t.T(), cond.Message,
			"possibly due to unhealthy resources: requested 1 node[namespace:job-operator, role:coordinator] "+
				"but only 0 available with matching properties. 1 node unhealthy: 0-coord-0")
		assertWsjobCleanedUp(t.T(), wsjob)

		// update coord node back to healthy
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: coordNodes[0].Name}, coordNode))
		coordNode.Status.Conditions = nil
		require.NoError(t.T(), k8sClient.Status().Update(ctx, coordNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 0
		}, eventuallyTimeout, eventuallyTick)

		// lock success after recovery
		jobName = "schedule-success-coord-recovery"
		wsjob = newWsExecuteJob(wscommon.Namespace, jobName, 1)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertWsJobScheduledEventually(t.T(), wscommon.Namespace, jobName)
		assertLockCountEventually(t.T(), 1)
		assertWsjobCleanedUp(t.T(), wsjob)
	})

	t.Run("resource-lock_failed_due_to_unhealthy_BR_node", func() {
		// create k8s node
		brNode := &corev1.Node{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: "br-0"}, brNode))
		brNode.Status.Conditions = []corev1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "UnknownError",
				Status: corev1.ConditionTrue,
			},
		}
		require.NoError(t.T(), k8sClient.Status().Update(ctx, brNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 1
		},
			eventuallyTimeout, eventuallyTick)

		// lock failed due to not enough BR node
		jobName := "schedule-fail-unhealthy-br"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertWsJobFailEventually(t.T(), wsjob.Namespace, wsjob.Name)
		cond := getConditions(t.T(), wsjob.Namespace, wsjob.Name)[commonv1.JobFailed]
		assert.Contains(t.T(), cond.Message,
			"possibly due to unhealthy resources: "+
				"unable to construct 36 BR tree with policy PORT on 29 available SwarmX nodes "+
				"with props([role:broadcastreduce]). 1 node unhealthy: br-0")
		assertWsjobCleanedUp(t.T(), wsjob)

		// update br node back to healthy
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: "br-0"}, brNode))
		brNode.Status.Conditions = nil
		require.NoError(t.T(), k8sClient.Status().Update(ctx, brNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 0
		},
			eventuallyTimeout, eventuallyTick)

		// lock success after recovery
		jobName = "schedule-success-br-recovery"
		wsjob = newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertLockCountEventually(t.T(), 1)
		condStatus, ok := getConditions(t.T(), wsjob.Namespace, jobName)[commonv1.JobScheduled]
		assert.True(t.T(), ok && condStatus.Status == corev1.ConditionTrue)
		assertWsjobCleanedUp(t.T(), wsjob)
	})

	t.Run("fail_job_with_not_enough_systems_due_to_multiple_group_failures", func() {
		wsapisv1.EnablePartialSystemMode = true
		defer func() {
			wsapisv1.EnablePartialSystemMode = false
		}()
		// update system ports to unhealthy
		s := &systemv1.System{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: t.JobReconciler.Cfg.Systems[0].Name}, s))
		s.Status.Conditions = []corev1.NodeCondition{
			{
				Type:               wscommon.ClusterMgmtConditionPrefix + wscommon.SystemPortAlertType + "_n00",
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				LastHeartbeatTime:  metav1.Now(),
			},
			{
				Type:               wscommon.ClusterMgmtConditionPrefix + wscommon.SystemPortAlertType + "_n01",
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				LastHeartbeatTime:  metav1.Now(),
			}}

		require.NoError(t.T(), k8sClient.Status().Update(ctx, s))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 1
		},
			eventuallyTimeout, eventuallyTick)

		failJob := newWsExecuteJobWithPartialMode(wscommon.Namespace, "fail-partial-system", systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, failJob))
		defer assertWsjobCleanedUp(t.T(), failJob)
		assertWsJobFailEventually(t.T(), failJob.Namespace, failJob.Name)
		cond := getConditions(t.T(), failJob.Namespace, failJob.Name)[commonv1.JobFailed]
		assert.Contains(t.T(), cond.Message,
			"possibly due to unhealthy resources: requested 8 system[namespace:job-operator] "+
				"but only 7 available with matching properties. 1 system unhealthy: system-0{errorPorts=n00,n01}")

		// update system back to healthy
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: t.JobReconciler.Cfg.Systems[0].Name}, s))
		s.Status.Conditions = nil
		require.NoError(t.T(), k8sClient.Status().Update(ctx, s))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthySystems) == 0
		},
			eventuallyTimeout, eventuallyTick)
	})

	t.Run("support_partial_BR/system_for_multibox", func() {
		wsapisv1.EnablePartialSystemMode = true
		defer func() {
			wsapisv1.EnablePartialSystemMode = false
		}()
		// create k8s node
		br := t.JobReconciler.Cfg.NodeMap["br-0"]
		brNode := &corev1.Node{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, brNode))
		brNode.Status.Conditions = []corev1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic1",
				Status: corev1.ConditionTrue,
			},
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
				Status: corev1.ConditionTrue,
			},
		}
		require.NoError(t.T(), k8sClient.Status().Update(ctx, brNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			if t.ResourceController.Cfg == nil {
				return false
			}
			//t.T().Log(t.ResourceController.Cfg.UnhealthyNodes)
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 1 &&
				len(t.ResourceController.Cfg.UnhealthyNodes[br.Name].GetErrorNICs()) == 2
		}, eventuallyTimeout, eventuallyTick)

		// lock success with partial mode
		jobName := "br-success-partial-mode"
		wsjob := newWsExecuteJobWithPartialMode(wscommon.Namespace, jobName, systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertLockCountEventually(t.T(), 1)
		lock := assertLockExists(t.T(), wscommon.Namespace, jobName)
		assertLocked(t.T(), lock)
		condStatus, ok := getConditions(t.T(), wsjob.Namespace, jobName)[commonv1.JobScheduled]
		assert.True(t.T(), ok && condStatus.Status == corev1.ConditionTrue)
		require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKeyFromObject(wsjob), wsjob))
		_, ok = wsjob.Annotations[wscommon.PartialSystemAnnotation]
		assert.True(t.T(), ok)

		brConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.BrConfigName, wsjob.Name),
				Namespace: wsjob.Namespace,
			},
		}
		// br config generated
		require.Eventually(t.T(), func() bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(brConfigMap), brConfigMap)
			if err != nil && apierrors.IsNotFound(err) {
				return false
			}
			require.NoError(t.T(), err)
			return true
		}, eventuallyTimeout, eventuallyTick)
		brConfig := wscommon.BRConfig{}
		data, err := wscommon.Gunzip(brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName])
		require.NoError(t.T(), err)
		err = json.Unmarshal(data, &brConfig)
		require.NoError(t.T(), err)
		//t.T().Logf("%+v", brConfig.BRNodes)
		//t.T().Logf("%+v", brConfig.BRGroups)

		// 24 BR pods partial mode for 8 CS job + non-br pods
		pods := assertPodCountEventually(t.T(), wsjob.Namespace, wsjob.Name, "", 24+countNonBrPods(wsjob))
		setPodsInit(t.T(), t.CfgMgr.Cfg.NodeMap, pods)
		for _, po := range pods.Items {
			if po.Labels["replica-type"] != strings.ToLower(rlv1.BrRequestName) {
				continue
			}
			for _, container := range po.Spec.Containers {
				if container.Name == wsapisv1.DefaultContainerName {
					id := ""
					for _, env := range container.Env {
						if env.Name == wsapisv1.ReplicaId {
							id = env.Value
							break
						}
					}
					idInt, _ := strconv.Atoi(id)
					brNo := brConfig.BRNodes[idInt]
					assert.Equal(t.T(), brNo.NodeId, idInt)
				}
			}
		}
		// br config updated
		brConfigMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.BrConfigName, wsjob.Name),
				Namespace: wsjob.Namespace,
			},
		}
		brConfig = wscommon.BRConfig{}
		require.Eventually(t.T(), func() bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(brConfigMap), brConfigMap)
			if err != nil {
				return false
			}
			return brConfigMap.Data[wsapisv1.BrConfigUpdatedSignal] == wsapisv1.UpdatedSignalVal
		}, eventuallyTimeout, eventuallyTick)
		data, err = wscommon.Gunzip(brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName])
		require.NoError(t.T(), err)
		err = json.Unmarshal(data, &brConfig)
		require.NoError(t.T(), err)
		for id, group := range brConfig.BRGroups {
			for _, con := range group.Connections {
				if id == 0 {
					require.True(t.T(), con.HasError)
				} else {
					require.False(t.T(), con.HasError)
				}
			}
		}
		for id, node := range brConfig.BRNodes {
			if wscommon.CSVersionSpec.Group(id%wscommon.CSVersionSpec.NumPorts) == 0 {
				require.Equal(t.T(), node.ControlAddr, wsapisv1.DummyIpPort)
				require.Equal(t.T(), node.IngressAddr, wsapisv1.DummyIp)
				for _, e := range node.Egress {
					require.Equal(t.T(), e.ControlAddr, wsapisv1.DummyIpPort)
					require.Equal(t.T(), e.EgressAddr, wsapisv1.DummyIp)
				}
			} else {
				require.NotEmpty(t.T(), node.ControlAddr, wsapisv1.DummyIpPort)
				require.NotEmpty(t.T(), node.IngressAddr, wsapisv1.DummyIp)
				require.NotEqual(t.T(), node.ControlAddr, wsapisv1.DummyIpPort)
				require.NotEqual(t.T(), node.IngressAddr, wsapisv1.DummyIp)
				for _, e := range node.Egress {
					require.NotEmpty(t.T(), e.ControlAddr, wsapisv1.DummyIpPort)
					require.NotEmpty(t.T(), e.EgressAddr, wsapisv1.DummyIp)
					require.NotEqual(t.T(), e.ControlAddr, wsapisv1.DummyIpPort)
					require.NotEqual(t.T(), e.EgressAddr, wsapisv1.DummyIp)
				}
			}
		}

		// update br node back to healthy
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, brNode))
		brNode.Status.Conditions = nil
		require.NoError(t.T(), k8sClient.Status().Update(ctx, brNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 0
		}, eventuallyTimeout, eventuallyTick)
		assertWsjobCleanedUp(t.T(), wsjob)
	})

	t.Run("support_BR_NIC_failure", func() {
		// create k8s node
		br := t.JobReconciler.Cfg.NodeMap["br-0"]
		brNode := &corev1.Node{}
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, brNode))

		// update br nic to unhealthy
		require.Eventually(t.T(), func() bool {
			return k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, brNode) == nil
		}, eventuallyTimeout, eventuallyTick)
		brNode.Status.Conditions = []corev1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
				Status: corev1.ConditionTrue,
			},
		}
		require.NoError(t.T(), k8sClient.Status().Update(ctx, brNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			t.T().Log(t.ResourceController.Cfg.UnhealthyNodes)
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 1 &&
				len(t.ResourceController.Cfg.UnhealthyNodes[br.Name].GetErrorNICs()) == 1
		}, eventuallyTimeout, eventuallyTick)

		// lock success with one NIC error
		jobName := "br-success-one-link-down"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertLockCountEventually(t.T(), 1)
		assertWsJobScheduledEventually(t.T(), wsjob.Namespace, wsjob.Name)

		crd := assertPodCountEventually(t.T(), wsjob.Namespace, jobName, rlv1.CoordinatorRequestName, 1).Items[0]
		status := []netv1.NetworkStatus{
			{
				Name:      "cilium",
				Interface: "eth0",
				IPs:       []string{""},
			},
			{
				Name:      wscommon.DataNetAttachDef,
				Interface: "net1",
				IPs: []string{
					"10.10.10.10",
				},
			},
		}
		netStatus, _ := json.Marshal(status)
		crd.Annotations = map[string]string{}
		crd.Annotations[netv1.NetworkStatusAnnot] = string(netStatus)
		require.NoError(t.T(), k8sClient.Update(ctx, &crd))

		// 36 BR pods for 8 CS job + non-BR pods
		pods := assertPodCountEventually(t.T(), wsjob.Namespace, wsjob.Name, "", 36+countNonBrPods(wsjob))
		setPodsInit(t.T(), t.CfgMgr.Cfg.NodeMap, pods)
		id := ""
		for _, po := range pods.Items {
			nodeName := po.Spec.NodeSelector[corev1.LabelHostname]
			if nodeName == br.Name {
				id = po.Labels["replica-index"]
				break
			}
		}
		// br config updated
		brConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.BrConfigName, wsjob.Name),
				Namespace: wsjob.Namespace,
			},
		}
		brConfig := wscommon.BRConfig{}
		require.Eventually(t.T(), func() bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(brConfigMap), brConfigMap)
			if err != nil {
				return false
			}
			return brConfigMap.Data[wsapisv1.BrConfigUpdatedSignal] == wsapisv1.UpdatedSignalVal
		}, eventuallyTimeout, eventuallyTick)
		data, err := wscommon.Gunzip(brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName])
		require.NoError(t.T(), err)
		err = json.Unmarshal(data, &brConfig)
		require.NoError(t.T(), err)
		for _, b := range brConfig.BRNodes {
			// assert NIC 0 skipped due to error
			if strconv.Itoa(b.NodeId) == id {
				require.Equal(t.T(), b.IngressAddr, br.NICs[1].Addr)
				require.Equal(t.T(), b.Egress[0].EgressAddr, br.NICs[2].Addr)
				require.Equal(t.T(), b.Egress[1].EgressAddr, br.NICs[3].Addr)
				require.Equal(t.T(), b.Egress[2].EgressAddr, br.NICs[4].Addr)
				require.Equal(t.T(), b.Egress[3].EgressAddr, br.NICs[5].Addr)
				break
			}
		}
		assertWsjobCleanedUp(t.T(), wsjob)

		// two link error will fail a full systems job
		require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, brNode))
		brNode.Status.Conditions = []corev1.NodeCondition{
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic0",
				Status: corev1.ConditionTrue,
			},
			{
				Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic1",
				Status: corev1.ConditionTrue,
			},
		}
		require.NoError(t.T(), k8sClient.Status().Update(ctx, brNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			t.T().Log(t.ResourceController.Cfg.UnhealthyNodes)
			t.T().Log(t.ResourceController.Cfg.UnhealthyNodes[br.Name].GetErrorNICs())
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 1 &&
				len(t.ResourceController.Cfg.UnhealthyNodes[br.Name].GetErrorNICs()) == 2
		}, eventuallyTimeout, eventuallyTick)

		// lock failed due to not enough BR node
		jobName = "br-fail-two-links"
		wsjob = newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertWsJobFailEventually(t.T(), wsjob.Namespace, wsjob.Name)
		cond := getConditions(t.T(), wsjob.Namespace, wsjob.Name)[commonv1.JobFailed]
		assert.Contains(t.T(), cond.Message,
			"possibly due to unhealthy resources: "+
				"unable to construct 36 BR tree with policy PORT on 30 available SwarmX nodes "+
				"with props([role:broadcastreduce]). 1 node unhealthy: br-0{errorNics=nic0,nic1}")
		assertWsjobCleanedUp(t.T(), wsjob)

		// update br node back to healthy
		brNode.Status.Conditions = nil
		require.NoError(t.T(), k8sClient.Status().Update(ctx, brNode))
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return len(t.ResourceController.Cfg.UnhealthyNodes) == 0
		}, eventuallyTimeout, eventuallyTick)

		// lock success after recovery
		jobName = "br-success-after-recovery"
		wsjob = newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertLockCountEventually(t.T(), 1)
		assertWsJobScheduledEventually(t.T(), wsjob.Namespace, jobName)
		assertWsjobCleanedUp(t.T(), wsjob)
	})

	t.Run("fail_job_switch_inter-link_error", func() {
		group := "group1" // avoid the mgmt node group (group0) since then also fails bc of mgmt node unavailability
		t.NodeReconciler.Disabled = false
		wscommon.EnableSwitchAlertDetection = true
		defer func() {
			t.NodeReconciler.Disabled = true
			wscommon.EnableSwitchAlertDetection = false
		}()
		cache := &mockAlertCache{
			Cache: map[string]models.GettableAlerts{
				group: []*models.GettableAlert{
					{
						Annotations: map[string]string{
							"summary": "Node switch has inter-switch link error for 5min",
						},
						Alert: models.Alert{
							Labels: map[string]string{
								"type":     wscommon.NodeSwitchPortAlertType,
								"instance": group,
							},
						},
					},
				},
			},
		}
		t.NodeReconciler.AlertCache = cache

		count := 0
		for _, no := range t.JobReconciler.Cfg.NodeMap {
			if no.GetGroup() == group {
				count++
				// triggers status update
				t.NodeReconciler.Reconcile(context.TODO(), reconcile.Request{
					NamespacedName: types.NamespacedName{Name: no.Name},
				})
			}
		}
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			numUnhealthyNodes := -1
			if t.ResourceController.Cfg != nil {
				numUnhealthyNodes = len(t.ResourceController.Cfg.UnhealthyNodes)
			}
			t.T().Log(t.ResourceController.Cfg.UnhealthyNodes)
			t.T().Logf("expect %d unhealthy nodes, current: %d", count, numUnhealthyNodes)
			return numUnhealthyNodes == count
		}, eventuallyTimeout, eventuallyTick)

		// lock failed due to not enough racks
		jobName := "schedule-fail-to-switch-error"
		wsjob := newWsExecuteJob(wscommon.Namespace, jobName, systemCount)
		wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWeight].Replicas = wscommon.Pointer(int32(10))
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		assertWsJobFailEventually(t.T(), wsjob.Namespace, wsjob.Name)
		cond := getConditions(t.T(), wsjob.Namespace, wsjob.Name)[commonv1.JobFailed]
		assert.Contains(t.T(), cond.Message, "possibly due to unhealthy resources")
		assert.Contains(t.T(), cond.Message, "group request requested 8 nodegroups in total but only 7")
		assert.Contains(t.T(), cond.Message, fmt.Sprintf("%d nodes unhealthy: group1-", memoryXPerGroup+workersPerGroup))
		assertWsjobCleanedUp(t.T(), wsjob)

		// update back to healthy
		cache.Cache = map[string]models.GettableAlerts{}
		for _, no := range t.JobReconciler.Cfg.NodeMap {
			if no.GetGroup() == group {
				n := &corev1.Node{}
				require.NoError(t.T(), k8sClient.Get(ctx, client.ObjectKey{Name: no.Name}, n))
				n.Spec.ProviderID = "test"
				require.NoError(t.T(), k8sClient.Update(ctx, n))
			}
		}
		// not populated to actual cfg till next loop
		require.Eventually(t.T(), func() bool {
			return t.ResourceController.Cfg != nil && len(t.ResourceController.Cfg.UnhealthyNodes) == 0
		}, eventuallyTimeout, eventuallyTick)
	})

	t.Run("nic_error_handling_for_memx_nodes", func() {
		var nodes []string
		// get all memx nodes in the first nodegroup
		for _, no := range t.CfgMgr.Cfg.Nodes {
			if !no.HasRole(wscommon.RoleMemory) || no.GetGroup() != t.CfgMgr.Cfg.Groups[0].Name {
				continue
			}
			nodes = append(nodes, no.Name)
		}

		assertHealthStates := func(unhealthyNodeCount, errorNicsPerNode int) {
			require.Eventually(t.T(), func() bool {
				for _, nodeName := range nodes {
					no := t.CfgMgr.Cfg.NodeMap[nodeName]
					errorNics := no.GetErrorNICs()
					if len(errorNics) != errorNicsPerNode {
						return false
					}
				}
				return len(t.CfgMgr.Cfg.UnhealthyNodes) == unhealthyNodeCount
			}, eventuallyTimeout, eventuallyTick)
		}

		// mark primary nics on all memx nodes in a nodegroup to have errors and expect lock to fail
		runJobWithPrimaryNicError := func() {
			for _, no := range nodes {
				n := &corev1.Node{}
				require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKey{Name: no}, n))
				n.Status.Conditions = []corev1.NodeCondition{
					{
						Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic1",
						Status: corev1.ConditionTrue,
					},
				}
				require.NoError(t.T(), k8sClient.Status().Update(context.Background(), n))
			}
			assertHealthStates(len(nodes), 1)

			failedJob := newWsExecuteJob(wscommon.Namespace, "memx-primary-nics-failure", uint32(len(t.CfgMgr.Cfg.Systems)))
			require.NoError(t.T(), k8sClient.Create(ctx, failedJob))
			assertWsJobFailEventually(t.T(), failedJob.Namespace, failedJob.Name)
			cond := getConditions(t.T(), failedJob.Namespace, failedJob.Name)[commonv1.JobFailed]
			assert.Contains(t.T(), cond.Message, "cluster lacks requested capacity possibly due to unhealthy resources")
			assertWsjobCleanedUp(t.T(), failedJob)
		}

		// clear the primary nic error and add secondary nic errors
		runJobWithSecondaryNicError := func() {
			for _, no := range nodes {
				n := &corev1.Node{}
				require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKey{Name: no}, n))
				n.Status.Conditions = []corev1.NodeCondition{
					{
						Type:   wscommon.ClusterMgmtConditionPrefix + "NodeNICError_nic2",
						Status: corev1.ConditionTrue,
					},
				}
				require.NoError(t.T(), k8sClient.Status().Update(context.Background(), n))
			}
			assertHealthStates(0, 1)

			degradedJob := newWsExecuteJob(wscommon.Namespace, "memx-secondary-nics-failure", uint32(len(t.CfgMgr.Cfg.Systems)))
			require.NoError(t.T(), k8sClient.Create(ctx, degradedJob))
			lock := assertLockExists(t.T(), degradedJob.Namespace, degradedJob.Name)
			require.Eventually(t.T(), func() bool {
				return lock.Status.State == rlv1.LockGranted
			}, eventuallyTimeout, eventuallyTick)
			assertWsjobCleanedUp(t.T(), degradedJob)
		}

		// clear nic errors and rerun jobs
		runJobWithoutNicError := func() {
			for _, no := range nodes {
				n := &corev1.Node{}
				require.NoError(t.T(), k8sClient.Get(context.Background(), client.ObjectKey{Name: no}, n))
				n.Status.Conditions = nil
				require.NoError(t.T(), k8sClient.Status().Update(context.Background(), n))
			}
			assertHealthStates(0, 0)

			recoveredJob := newWsExecuteJob(wscommon.Namespace, "memx-nics-recovered", uint32(len(t.CfgMgr.Cfg.Systems)))
			require.NoError(t.T(), k8sClient.Create(ctx, recoveredJob))
			lock := assertLockExists(t.T(), recoveredJob.Namespace, recoveredJob.Name)
			require.Eventually(t.T(), func() bool {
				return lock.Status.State == rlv1.LockGranted
			}, eventuallyTimeout, eventuallyTick)
			assertWsjobCleanedUp(t.T(), recoveredJob)
		}

		// transition from no nic errors to primary nic errors
		runJobWithPrimaryNicError()
		// transition from primary nic errors to secondary nic errors
		runJobWithSecondaryNicError()
		// transition from secondary nic errors to no nic errors
		runJobWithoutNicError()
		// transition from no nic errors to secondary nic errors
		runJobWithSecondaryNicError()
		// transition from secondary nic errors to primary nic errors
		runJobWithPrimaryNicError()
		// transition from primary nic errors to no nic errors
		runJobWithoutNicError()
	})
}

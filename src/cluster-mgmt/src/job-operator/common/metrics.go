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

package common

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Define all the prometheus counters for all jobs
var (
	operatorSemanticVersionMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_version",
			Help: "Version of operator",
		},
		[]string{"semantic_version", "cerebras_version"},
	)
	jobsCreatedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "job_operator_jobs_created_total",
			Help: "Counts number of jobs created",
		},
		[]string{"job_namespace", "job_kind"},
	)
	jobsDeletedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "job_operator_jobs_deleted_total",
			Help: "Counts number of jobs deleted",
		},
		[]string{"job_namespace", "job_kind"},
	)
	jobsSuccessfulCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "job_operator_jobs_successful_total",
			Help: "Counts number of jobs successful",
		},
		[]string{"job_namespace", "job_kind"},
	)
	jobsFailedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "job_operator_jobs_failed_total",
			Help: "Counts number of jobs failed",
		},
		[]string{"job_namespace", "job_kind"},
	)
	jobsRestartedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "job_operator_jobs_restarted_total",
			Help: "Counts number of jobs restarted",
		},
		[]string{"job_namespace", "job_kind"},
	)
	jobsCancelledCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "job_operator_jobs_cancelled_total",
			Help: "Counts number of jobs cancelled",
		},
		[]string{"job_namespace", "job_kind"},
	)
	jobPhaseMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_wsjob_phase",
			Help: "Current phase of wsjob",
		},
		[]string{"job_namespace", "job_name", "phase"},
	)
	semanticVersionMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_wsjob_semantic_version",
			Help: "Semantic versions of wsjob",
		},
		[]string{"job_namespace", "job_name", "appliance_client", "cluster_server", "job_operator"},
	)
	spineLoadMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_spine_load",
			Help: "Spine load incurred in specific wsjob",
		},
		[]string{"job_namespace", "job_name", "load_type"},
	)
	nonSpineLoadMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_non_spine_load",
			Help: "None spine load incurred in specific wsjob",
		},
		[]string{"job_namespace", "job_name", "load_type"},
	)
	lockEventCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "job_operator_resource_lock_events_total",
			Help: "Records events from resource lock reconciler. type='error' is a user error, type='fatal' is a system error",
		},
		[]string{"event_type"},
	)
	lockGrantMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_wsjob_lock_grant",
			Help: "Record lock grants for wsjob",
		},
		[]string{"wsjob_id"},
	)
	dataNetworkInterface = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_data_network_interface",
			Help: "In use data network interface name for jobs.",
		},
		[]string{"node", "device", "group"},
	)
	jobNetworkInterfaces = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_wsjob_network_interface",
			Help: "Network interface (device) assigned to job on node.",
		},
		[]string{"job_namespace", "job_name", "pod_name", "node", "device", "device_ip"},
	)
	nodeRole = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_node_role",
			Help: "Role label of cluster nodes.",
		},
		[]string{"node", "group", "role"},
	)
	groupSwitchInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_group_switch_info",
			Help: "Switch group association.",
		},
		[]string{"group", "switch_id"},
	)
	NodeAvailableResources = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_operator_node_status_allocatable",
			Help: "Node resources after subtracting cluster-mgmt overheads",
		},
		[]string{"node", "resource", "unit"},
	)
	podNetworkAffinityRT = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pod_port_affinity",
			Help: "The network topology & connection quality for a pod of a specific replica type in a wsjob",
		},
		[]string{"job_namespace", "job_name", "pod_id", "node_name", "nic_names", "v2_groups",
			"preferred_ports", "target_resources", "target_ports", "target_v2_groups", "is_one_hop"},
	)
	brSetTopology = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "br_set_topology",
			Help: "The network connection topology for BR pods in a wsjob",
		},
		[]string{"job_namespace", "job_name", "pod_id", "br_set_id", "target_br_set_ids", "target_port"},
	)
	// kvss & job sharing related metrics
	jobAxMemoryLimit = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "job_ax_memory_limit",
			Help: "The aggregated memory upperbound can be on AXs for a specific wsjob",
		},
		[]string{"job_namespace", "job_name"},
	)
	resourceMovement = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "session_resource_movement_total",
			Help: "Total resource movement events by type, resource, operation, and session",
		},
		[]string{
			"resource_type",
			"resource_name",
			"operation",
			"session",
		},
	)
	sessionResourceCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "session_resource_count",
			Help: "Current count of resources assigned to a session",
		},
		[]string{
			"resource_type",
			"session",
		},
	)
)

func init() {
	// Register custom metrics with the global prometheus registry
	metrics.Registry.MustRegister(
		operatorSemanticVersionMetric,
		jobsCreatedCount,
		jobsDeletedCount,
		jobsSuccessfulCount,
		jobsFailedCount,
		jobsRestartedCount,
		jobPhaseMetric,
		semanticVersionMetric,
		spineLoadMetric,
		nonSpineLoadMetric,
		lockEventCounter,
		lockGrantMetric,
		dataNetworkInterface,
		jobNetworkInterfaces,
		nodeRole,
		groupSwitchInfo,
		NodeAvailableResources,
		podNetworkAffinityRT,
		brSetTopology,
		jobAxMemoryLimit,
		resourceMovement,
		sessionResourceCount,
	)
}

func OperatorSemanticVersionAdded(sv, cv string) {
	operatorSemanticVersionMetric.WithLabelValues(sv, cv).Set(1)
}

func DataNetworkInterfaceMetricAdded(node, nicInterface, group string) {
	dataNetworkInterface.WithLabelValues(node, nicInterface, group).Set(1)
}

func NodeRoleMetricAdded(node, group, role string) {
	nodeRole.DeletePartialMatch(map[string]string{"node": node})
	nodeRole.WithLabelValues(node, group, role).Set(1)
}

func GroupSwitchInfoMetricAdded(group, switchId string) {
	groupSwitchInfo.WithLabelValues(group, switchId).Set(1)
}

func SpineLoadMetricAdded(jobNamespace, wsjobId, loadType string, load int) {
	spineLoadMetric.WithLabelValues(jobNamespace, wsjobId, loadType).Set(float64(load))
}

func SpineLoadMetricRemoved(jobNamespace, wsjobId string) {
	spineLoadMetric.DeletePartialMatch(map[string]string{"job_namespace": jobNamespace, "job_name": wsjobId})
}

func NonSpineLoadMetricAdded(jobNamespace, wsjobId, loadType string, load int) {
	nonSpineLoadMetric.WithLabelValues(jobNamespace, wsjobId, loadType).Set(float64(load))
}

func NonSpineLoadMetricRemoved(jobNamespace, wsjobId string) {
	nonSpineLoadMetric.DeletePartialMatch(map[string]string{"job_namespace": jobNamespace, "job_name": wsjobId})
}

func JobNicAdded(jobNamespace, wsjobId string, podName string, node string, dev string, devIp string) {
	jobNetworkInterfaces.WithLabelValues(jobNamespace, wsjobId, podName, node, dev, devIp).Set(1)
}

func JobNicsAllRemoved(jobNamespace, wsjobId string) {
	jobNetworkInterfaces.DeletePartialMatch(map[string]string{"job_namespace": jobNamespace, "job_name": wsjobId})
}

func SemanticVersionMetricAdded(jobNamespace, wsjobId, clientSemver, serverSemver, operatorSemver string) {
	semanticVersionMetric.WithLabelValues(jobNamespace, wsjobId, clientSemver, serverSemver, operatorSemver).Set(1)
}

func SemanticVersionMetricRemoved(jobNamespace, wsjobId string) {
	semanticVersionMetric.DeletePartialMatch(map[string]string{"job_namespace": jobNamespace, "job_name": wsjobId})
}

// For testing purposes, check whether there's an info metric for this pod/node/nic.
func JobHasNicForTesting(jobNamespace, wsjobId string, podName string, node string, dev string, devIp string) bool {
	return testutil.ToFloat64(
		jobNetworkInterfaces.WithLabelValues(jobNamespace, wsjobId, podName, node, dev, devIp)) == 1
}

func LockGrantMetricAdded(wsjobId string) {
	lockGrantMetric.WithLabelValues(wsjobId).Set(1)
}

// PodNetworkTopologyMetricAdded records a network topology metric for replica-to-resource communication.
//
// Parameters:
//   - jobNamespace: namespace/session where the job is running
//   - jobName: name of the job
//   - replicaName: replica/pod name
//   - nodeName: node (AX, SX, ...) where the pod is scheduled
//   - nicNames: node's NIC names used by the pod
//   - v2Groups: v2 group or switch the source pod is connected to
//   - preferredPorts: comma-separated list of preferred system ports of the connected switch
//   - targetResources: comma-separated list of targeted resource names (e.g. systems or nodes)
//   - targetPorts: comma-separated list of target system ports
//   - targetV2Groups: comma-separated list of v2 network groups that the target resources are connected to
//   - brSetId: the BR set the pod belongs to
//   - targetBrSetId: comma-separated list of BR sets that the pod has egress traffic to
//   - brPort: the numeric port that the br pod is targeting
//   - isOneHop: whether the communication path requires only one network hop (true) or multiple hops (false)
func PodNetworkTopologyMetricAdded(jobNamespace, jobName, replicaName, nodeName, nicNames, v2Groups, preferredPorts, targetResources, targetPorts, targetV2Groups, brSetId, targetBrSetIds, brPort string, isOneHop bool) {
	oneHopTraffic := Ternary(isOneHop, "1", "0")
	podNetworkAffinityRT.WithLabelValues(jobNamespace, jobName, replicaName, nodeName, nicNames, v2Groups, preferredPorts, targetResources, targetPorts, targetV2Groups, oneHopTraffic).Set(1)
	if brSetId != "" || targetBrSetIds != "" {
		brSetTopology.WithLabelValues(jobNamespace, jobName, replicaName, brSetId, targetBrSetIds, brPort).Set(1)
	}
}

func PodNetworkTopologyMetricRemoved(jobNamespace, jobName string) {
	podNetworkAffinityRT.DeletePartialMatch(map[string]string{"job_namespace": jobNamespace, "job_name": jobName})
	brSetTopology.DeletePartialMatch(map[string]string{"job_namespace": jobName, "job_name": jobName})
}

// For testing purposes, check whether there's an info metric for this pod/node/nic.
func PodNetworkTopologyForTesting(jobNamespace, jobName, replicaName, nodeName, nicNames, v2Groups, preferredPorts, targetResources, targetPorts, targetV2Groups, brSetId, targetBrSetIds, brPort string, isOneHop bool) bool {
	oneHopTraffic := Ternary(isOneHop, "1", "0")
	networkTopologyMetricCheck := testutil.ToFloat64(
		podNetworkAffinityRT.WithLabelValues(jobNamespace, jobName, replicaName, nodeName, nicNames, v2Groups, preferredPorts, targetResources, targetPorts, targetV2Groups, oneHopTraffic)) == 1
	brSetMetricCheck := testutil.ToFloat64(brSetTopology.WithLabelValues(jobNamespace, jobName, replicaName, brSetId, targetBrSetIds, brPort)) == Ternary[float64](brSetId != "" || targetBrSetIds != "", 1, 0)
	return networkTopologyMetricCheck && brSetMetricCheck
}

// aggrMemoryLimit in MiB
func InfJobSharingMemoryLimitMetricAdded(jobNamespace, jobName string, aggrMemoryLimit int) {
	jobAxMemoryLimit.WithLabelValues(jobNamespace, jobName).Set(float64(aggrMemoryLimit))
}

func InfJobSharingMemoryLimitMetricRemoved(jobNamespace, jobName string) {
	jobAxMemoryLimit.DeletePartialMatch(map[string]string{"job_namespace": jobNamespace, "job_name": jobName})
}

// choose not to remove existing phase because phase metrics are not well organized
// and can be updated in multiple places, e.g. api server status update and internal reconcile loop
// ideally it should be cleaned and only one phase should be active
func JobPhaseMetricAdded(jobNamespace, wsjobId string, phase string) {
	jobPhaseMetric.WithLabelValues(jobNamespace, wsjobId, phase).Set(1)
}

func JobPhaseMetricRemoved(jobNamespace, wsjobId string) {
	jobPhaseMetric.DeletePartialMatch(map[string]string{"job_namespace": jobNamespace, "job_name": wsjobId})
}

// Job metric to be removed immediately when job enters terminal state
func JobMetricRemoved(jobNamespace, wsjobId string) {
	JobPhaseMetricRemoved(jobNamespace, wsjobId)
	JobNicsAllRemoved(jobNamespace, wsjobId)
	SpineLoadMetricRemoved(jobNamespace, wsjobId)
	NonSpineLoadMetricRemoved(jobNamespace, wsjobId)
	SemanticVersionMetricRemoved(jobNamespace, wsjobId)
	InfJobSharingMemoryLimitMetricRemoved(jobNamespace, wsjobId)
}

// Job metric to be removed after 7 days at metadata cleanup
func PersistentJobMetricRemoved(jobNamespace, wsjobId string) {
	PodNetworkTopologyMetricRemoved(jobNamespace, wsjobId)
}

func CreatedJobsCounterInc(jobNamespace, jobKind string) {
	jobsCreatedCount.WithLabelValues(jobNamespace, jobKind).Inc()
}

func DeletedJobsCounterInc(jobNamespace, jobKind string) {
	jobsDeletedCount.WithLabelValues(jobNamespace, jobKind).Inc()
}

func SuccessfulJobsCounterInc(jobNamespace, jobKind string) {
	jobsSuccessfulCount.WithLabelValues(jobNamespace, jobKind).Inc()
}

func FailedJobsCounterInc(jobNamespace, jobKind string) {
	jobsFailedCount.WithLabelValues(jobNamespace, jobKind).Inc()
}

func RestartedJobsCounterInc(jobNamespace, jobKind string) {
	jobsRestartedCount.WithLabelValues(jobNamespace, jobKind).Inc()
}

func CancelledJobsCounterInc(jobNamespace, jobKind string) {
	jobsCancelledCount.WithLabelValues(jobNamespace, jobKind).Inc()
}

func RecordResourceMovement(resourceType, resourceName, operation, session string) {
	resourceMovement.WithLabelValues(resourceType, resourceName, operation, session).Inc()
}

func RecordResourceMovementMetricRemoved(session string) {
	resourceMovement.DeletePartialMatch(map[string]string{"session": session})
}

func UpdateSessionResourceCount(resourceType, session string, count float64) {
	sessionResourceCount.WithLabelValues(resourceType, session).Set(count)
}

func SessionResourceCountMetricRemoved(session string) {
	sessionResourceCount.DeletePartialMatch(map[string]string{"session": session})
}

func RemoveSessionResourceMetric(session string) {
	RecordResourceMovementMetricRemoved(session)
	SessionResourceCountMetricRemoved(session)
}

// Helper function to check if a resource movement metric exists
func SessionHasResourceMovementForTesting(session, resourceType, resourceName, operation string) bool {
	return testutil.ToFloat64(
		resourceMovement.WithLabelValues(resourceType, resourceName, operation, session)) > 0
}

// Helper function to check if a session resource count metric exists with expected value
func SessionHasResourceCountForTesting(session, resourceType string, expectedCount float64) bool {
	return testutil.ToFloat64(
		sessionResourceCount.WithLabelValues(resourceType, session)) == expectedCount
}

/*
Copyright 2024 Cerebras Systems, Inc..

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

package cluster

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	wscommon "cerebras.com/job-operator/common"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
)

var (
	serverSemanticVersion = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cluster_server_version",
			Help: "version of cluster server",
		},
		[]string{"semantic_version", "cerebras_version"},
	)
	userNodeMemTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "user_node_mem_total_bytes",
			Help: "user node total mem",
		},
		[]string{"node"},
	)
	userNodeMemUsed = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "user_node_mem_used_bytes",
			Help: "user node used mem",
		},
		[]string{"node"},
	)
	userNodeCpuUsedPercent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "user_node_cpu_used_percent",
			Help: "user node used cpu percent",
		},
		[]string{"node"},
	)
	userNodeProcessRssMem = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "user_node_process_mem_rss_bytes",
			Help: "user node process used mem",
		},
		[]string{"node", "namespace", "job_id", "process_id"},
	)
	userNodeProcessCpuUsedPercent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "user_node_process_cpu_used_percent",
			Help: "user node process used cpu percent",
		},
		[]string{"node", "namespace", "job_id", "process_id"},
	)
)

func init() {
	// Register custom metrics with the global prometheus registry
	metrics.Registry.MustRegister(
		serverSemanticVersion,
		userNodeMemTotal,
		userNodeMemUsed,
		userNodeCpuUsedPercent,
		userNodeProcessRssMem,
		userNodeProcessCpuUsedPercent,
	)
}

func ServerSemanticVersionAdded(sv, cv string) {
	serverSemanticVersion.WithLabelValues(sv, cv).Set(1)
}

func UserNodeTotalMemAdded(node string, val float64) {
	userNodeMemTotal.DeletePartialMatch(map[string]string{"node": node})
	userNodeMemTotal.WithLabelValues(node).Set(val)
}

func UserNodeUsedMemAdded(node string, val float64) {
	userNodeMemUsed.DeletePartialMatch(map[string]string{"node": node})
	userNodeMemUsed.WithLabelValues(node).Set(val)
}

func UserNodeUsedCpuAdded(node string, val float64) {
	userNodeCpuUsedPercent.DeletePartialMatch(map[string]string{"node": node})
	userNodeCpuUsedPercent.WithLabelValues(node).Set(val)
}

func UserNodeProcessMemRssAdded(node, namespace, jobId string, processId int32, val float64) {
	userNodeProcessRssMem.DeletePartialMatch(map[string]string{"node": node, "job_id": jobId})
	userNodeProcessRssMem.WithLabelValues(node, namespace, jobId, strconv.Itoa(int(processId))).Set(val)
}

func UserNodeProcessUsedCpuAdded(node, namespace, jobId string, processId int32, val float64) {
	userNodeProcessCpuUsedPercent.DeletePartialMatch(map[string]string{"node": node, "job_id": jobId})
	userNodeProcessCpuUsedPercent.WithLabelValues(node, namespace, jobId, strconv.Itoa(int(processId))).Set(val)
}

func HeartbeatToUserNodeMetrics(request *pb.HeartbeatRequest) {
	logrus.Debug(request.String())
	UserNodeTotalMemAdded(request.HostName, request.HostMemTotalBytes)
	UserNodeUsedMemAdded(request.HostName, request.HostMemUsedBytes)
	UserNodeUsedCpuAdded(request.HostName, request.HostCpuUsedPercent)
	UserNodeProcessMemRssAdded(request.HostName, wscommon.Namespace, request.JobId,
		request.ProcessId, request.ProcessMemRssBytes)
	UserNodeProcessUsedCpuAdded(request.HostName, wscommon.Namespace, request.JobId,
		request.ProcessId, request.ProcessCpuUsedPercent)
}

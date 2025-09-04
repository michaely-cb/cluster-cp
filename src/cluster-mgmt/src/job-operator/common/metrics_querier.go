package common

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

const (
	// Warning: for new queries, please ensure metrics don't require global view from multiple shards
	// e.g. metrics calculations/joins from different shards
	// consider switch to thanos client/use new recording rule if unable to avoid
	// for node_load5: https://groups.google.com/g/prometheus-developers/c/3Q4AF1fYRxA
	nodeCpuUtilizationQuery   = `100 * node_load5 / count(node_cpu_seconds_total{mode="idle"}) without (cpu,mode)`
	nodeMemUtilizationQuery   = `100 - (node_memory_MemAvailable_bytes * 100 / node_memory_MemTotal_bytes)`
	nodeCpuAllocatableQuery   = `job_operator_node_status_allocatable{resource="cpu", unit="core"}`
	nodeMemAllocatableQuery   = `job_operator_node_status_allocatable{resource="memory", unit="bytes"}`
	nodeDiskUtilizationQuery  = `100 - (node_filesystem_avail_bytes{mountpoint="%s"} * 100 / node_filesystem_size_bytes)`
	systemSoftwareInfoQuery   = `group by (system, software, version, buildid, execmode) (system_software_info{system=~"%s"})`
	podContainerInfoQuery     = `kube_pod_container_info{container="%s", namespace="%s"} * on(pod, namespace) group_left() (kube_pod_status_phase{phase="Running"} == 1)`
	systemMaintenanceQuery    = `system_maintenance_mode{system=~"%s"}`
	jobOperatorVersionQuery   = `job_operator_version{namespace="%s"}`
	clusterServerVersionQuery = `cluster_server_version{namespace="%s"}`
	podPortAffinityQuery      = `pod_port_affinity{job_name="%s",job_namespace="%s"}`
	brSetTopologyQuery        = `br_set_topology{job_name="%s",job_namespace="%s"}`

	WorkerCachePartition = "/n0"
)

// TODO: Update return type from "model.Value" to actual values of interest.
type MetricsQuerier interface {
	// query node disk utilization
	QueryNodeDiskUsage(ctx context.Context) (model.Value, error)
	// query node cpu utilization for last 5m
	QueryNodeCpuUsage(ctx context.Context) (model.Value, error)
	// query node mem utilization
	QueryNodeMemUsage(ctx context.Context) (model.Value, error)
	// nodeName -> (total cpu - overhead) in millicores available for wsjob scheduling
	QueryNodeCpuAllocatable(ctx context.Context) (map[string]float64, error)
	// nodeName -> (total mem - overhead) in bytes available for wsjob scheduling
	QueryNodeMemAllocatable(ctx context.Context) (map[string]int64, error)
	// query system software version
	QuerySystemVersion(ctx context.Context, systemNames []string) (model.Value, error)
	// query general pod/container info
	QueryPodContainerInfo(ctx context.Context, container string, namespace string) (model.Value, error)
	// query whether system maintenance mode can be detected
	QuerySystemInMaintenanceMode(ctx context.Context, systemNames []string) (model.Value, error)
	// query job operator build and semantic version
	QueryJobOperatorVersion(ctx context.Context, namespace string) (string, string, error)
	// query cluster server build and semantic version
	QueryClusterServerVersion(ctx context.Context, namespace string) (string, string, error)
	// query job pod network topology
	QueryPodPortAffinity(ctx context.Context, jobId, namespace string) (model.Value, error)
	// query br set topology
	QueryBrSetTopology(ctx context.Context, jobId, namespace string) (model.Value, error)
}

type PromMetricsQuerier struct {
	PromUrl string
}

func NewPromMetricsQuerier(promUrl string) MetricsQuerier {
	return &PromMetricsQuerier{
		PromUrl: promUrl,
	}
}

func (p PromMetricsQuerier) QueryNodeDiskUsage(ctx context.Context) (model.Value, error) {
	query := fmt.Sprintf(nodeDiskUtilizationQuery, WorkerCachePartition)
	return p.execQueryNow(ctx, query)
}

func (p PromMetricsQuerier) QueryNodeCpuUsage(ctx context.Context) (model.Value, error) {
	return p.execQueryNow(ctx, nodeCpuUtilizationQuery)
}

func (p PromMetricsQuerier) QueryNodeMemUsage(ctx context.Context) (model.Value, error) {
	return p.execQueryNow(ctx, nodeMemUtilizationQuery)
}

func (p PromMetricsQuerier) QuerySystemVersion(ctx context.Context, systemNames []string) (model.Value, error) {
	sort.Strings(systemNames)
	regex := ".*"
	if len(systemNames) != 0 {
		regex = strings.Join(systemNames, "|")
	}
	query := fmt.Sprintf(systemSoftwareInfoQuery, regex)
	return p.execQueryNow(ctx, query)
}

func (p PromMetricsQuerier) QueryPodPortAffinity(ctx context.Context, jobId, namespace string) (model.Value, error) {
	return p.execQueryNow(ctx, fmt.Sprintf(podPortAffinityQuery, jobId, namespace))
}

func (p PromMetricsQuerier) QueryBrSetTopology(ctx context.Context, jobId, namespace string) (model.Value, error) {
	return p.execQueryNow(ctx, fmt.Sprintf(brSetTopologyQuery, jobId, namespace))
}

func (p PromMetricsQuerier) QueryNodeCpuAllocatable(ctx context.Context) (map[string]float64, error) {
	r, err := p.execQueryNow(ctx, nodeCpuAllocatableQuery)
	if err != nil {
		return nil, err
	}
	rv := make(map[string]float64)
	for _, sample := range r.(model.Vector) {
		if sample == nil {
			continue
		}
		node := sample.Metric["node"]
		if node == "" {
			continue
		}
		rv[string(node)] = float64(sample.Value)
	}
	return rv, nil
}

func (p PromMetricsQuerier) QueryNodeMemAllocatable(ctx context.Context) (map[string]int64, error) {
	r, err := p.execQueryNow(ctx, nodeMemAllocatableQuery)
	if err != nil {
		return nil, err
	}
	rv := make(map[string]int64)
	for _, sample := range r.(model.Vector) {
		if sample == nil {
			continue
		}
		node := sample.Metric["node"]
		if node == "" {
			continue
		}
		rv[string(node)] = int64(sample.Value)
	}
	return rv, nil
}

func (p PromMetricsQuerier) QueryPodContainerInfo(ctx context.Context, container string, namespace string) (model.Value, error) {
	return p.execQueryNow(ctx, fmt.Sprintf(podContainerInfoQuery, container, namespace))
}

func (p PromMetricsQuerier) QuerySystemInMaintenanceMode(ctx context.Context, systemNames []string) (model.Value, error) {
	sort.Strings(systemNames)
	regex := ".*"
	if len(systemNames) != 0 {
		regex = strings.Join(systemNames, "|")
	}
	query := fmt.Sprintf(systemMaintenanceQuery, regex)
	return p.execQueryNow(ctx, query)
}

func (p PromMetricsQuerier) QueryJobOperatorVersion(ctx context.Context, namespace string) (string, string, error) {
	return p.getOrchestrationVersion(ctx, fmt.Sprintf(jobOperatorVersionQuery, namespace))
}

func (p PromMetricsQuerier) QueryClusterServerVersion(ctx context.Context, namespace string) (string, string, error) {
	return p.getOrchestrationVersion(ctx, fmt.Sprintf(clusterServerVersionQuery, namespace))
}

// execQueryNow issues a prometheus query at the current timestamp
func (p PromMetricsQuerier) execQueryNow(ctx context.Context, q string) (model.Value, error) {
	promUrl := p.PromUrl
	if promUrl == "" {
		promUrl = wsapisv1.PromUrl
	}
	client, _ := promapi.NewClient(promapi.Config{
		Address: promUrl,
	})
	var result model.Vector
	res, warns, err := promv1.NewAPI(client).Query(ctx, q, time.Now(), promv1.WithTimeout(10*time.Second))
	if err != nil {
		log.Warnf("Prom query failed query='%s', address=%s, err=%v", q, promUrl, err)
	}
	if warns != nil {
		log.Warnf("Prom query execd with warnings query='%s', address=%s, warn=%v", q, promUrl, warns)
	}
	if res != nil {
		result = append(result, res.(model.Vector)...)
	}
	return result, err
}

func (p PromMetricsQuerier) getOrchestrationVersion(ctx context.Context, query string) (string, string, error) {
	buildVersion, semanticVersion := "unknown", "unknown"
	r, err := p.execQueryNow(ctx, query)
	if err != nil {
		return buildVersion, semanticVersion, err
	}
	for _, sample := range r.(model.Vector) {
		if sample == nil {
			continue
		}
		buildVersion = string(sample.Metric["cerebras_version"])
		semanticVersion = string(sample.Metric["semantic_version"])
		break
	}
	return buildVersion, semanticVersion, nil
}

var GetSystemVersions = func(ctx context.Context, metricsQuerier MetricsQuerier, systemNames []string,
	requireParsing, requireBuildId bool) (map[string]string, map[string][]string) {
	if metricsQuerier == nil {
		return nil, nil
	}
	systemVersions := map[string]string{}
	releaseVersionMap := map[string][]string{}
	result, err := metricsQuerier.QuerySystemVersion(ctx, systemNames)
	if err != nil {
		// best effort only
		log.Warnf("failed to retrieve prom version metrics for systems:%s, err:%v",
			Ternary(systemNames == nil, "ALL", strings.Join(systemNames, "|")), err)
		return nil, nil
	}
	for _, sample := range result.(model.Vector) {
		if sample == nil {
			continue
		}
		systemName, ok := sample.Metric["system"]
		if !ok {
			continue
		}

		var version, buildID, execMode string
		if v, ok := sample.Metric["version"]; ok {
			version = string(v)
			if b, ok := sample.Metric["buildid"]; ok {
				buildID = string(b)
			}
			if e, ok := sample.Metric["execmode"]; ok {
				execMode = string(e)
			}
			if !requireParsing {
				software := map[string]string{
					"version":  version,
					"buildid":  buildID,
					"execmode": execMode,
				}
				softwareStr, _ := json.Marshal(software)
				systemVersions[string(systemName)] = string(softwareStr)
				continue
			}
		} else {
			if !requireParsing {
				systemVersions[string(systemName)] = string(sample.Metric["software"])
				continue
			}
			// fallback to legacy field
			var metric map[string]interface{}
			err = json.Unmarshal([]byte(sample.Metric["software"]), &metric)
			if err != nil {
				log.Warnf("malformated metric: %s", sample.Metric["software"])
				continue
			}
			product := metric["product"].(map[string]interface{})
			version = product["version"].(string)
			buildID = product["buildid"].(string)
		}

		if version == "" || version == "unavailable" {
			continue
		}
		if requireBuildId {
			systemVersions[string(systemName)] = version + ":" + buildID
		} else {
			systemVersions[string(systemName)] = version
		}
		releaseVersionMap[version] = append(releaseVersionMap[version], string(systemName))
	}
	return systemVersions, releaseVersionMap
}

type MockQuerier struct {
	Values []model.Value
}

func (m *MockQuerier) QueryNodeDiskUsage(ctx context.Context) (model.Value, error) {
	return m.exec()
}

func (m *MockQuerier) QueryNodeCpuUsage(ctx context.Context) (model.Value, error) {
	return m.exec()
}

func (m *MockQuerier) QueryNodeMemUsage(ctx context.Context) (model.Value, error) {
	return m.exec()
}

func (m *MockQuerier) QuerySystemVersion(ctx context.Context, systemNames []string) (model.Value, error) {
	return m.exec()
}

func (m *MockQuerier) QueryNodeCpuAllocatable(ctx context.Context) (map[string]float64, error) {
	return nil, nil
}

func (m *MockQuerier) QueryNodeMemAllocatable(ctx context.Context) (map[string]int64, error) {
	return nil, nil
}

func (m *MockQuerier) QueryPodContainerInfo(ctx context.Context, container string, namespace string) (model.Value, error) {
	return nil, nil
}

func (m *MockQuerier) QuerySystemInMaintenanceMode(ctx context.Context, systemNames []string) (model.Value, error) {
	return nil, nil
}

func (p MockQuerier) QueryJobOperatorVersion(ctx context.Context, namespace string) (string, string, error) {
	return "unknown", "unknown", nil
}

func (p MockQuerier) QueryClusterServerVersion(ctx context.Context, namespace string) (string, string, error) {
	return "unknown", "unknown", nil
}

func (m *MockQuerier) QueryPodPortAffinity(ctx context.Context, jobId, namespace string) (model.Value, error) {
	return m.exec()
}

func (m *MockQuerier) QueryBrSetTopology(ctx context.Context, jobId, namespace string) (model.Value, error) {
	return m.exec()
}

func (m *MockQuerier) exec() (model.Value, error) {
	if len(m.Values) == 0 {
		return nil, fmt.Errorf("no results")
	}
	rv := m.Values[0]
	m.Values = m.Values[1:]
	return rv, nil
}

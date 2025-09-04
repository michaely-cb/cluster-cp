local g = import '../g.libsonnet';
local prometheusQuery = g.query.prometheus;

local variables = import './variables.libsonnet';

{
  tableQuery: {
    new(queryText, legendFormat):
      prometheusQuery.new(
        '$' + variables.datasource.name,
        queryText,
      )
      + {
        format: 'table',
        instant: true,
        legendFormat: legendFormat,
      },

    healthQuery(podFilterText):
      self.new(
        'min(kube_pod_container_status_ready{pod=~"' + podFilterText + '"}) by (pod) and group by (pod) (kube_pod_status_phase{phase="Running"} == 1)',
        'Status',
      ),

    restartsQuery(podFilterText):
      self.new(
        'max(increase(kube_pod_container_status_restarts_total{pod=~"' + podFilterText + '"}[$__range])) by (pod) and group by (pod) (kube_pod_status_phase{phase="Running"} == 1)',
        'Restarts',
      ),
  },

  timeSeriesQuery: {
    new(queryText, legendFormat):
      prometheusQuery.new(
        '$' + variables.datasource.name,
        queryText,
      )
      + {
        legendFormat: legendFormat,
      },
  },

  alertsOnlyControlPlane:
    self.timeSeriesQuery.new(
      'avg without (alertstate) (ALERTS{control_plane_oncall="true", alertstate="pending"} or (ALERTS{control_plane_oncall="true", alertstate="firing"} + 1))',
      '{{alertname}} {{instance}}',
    ),

  alertsExceptControlPlane:
    self.timeSeriesQuery.new(
      'avg without (alertstate) (ALERTS{control_plane_oncall!="true", alertstate="pending", alertname!="Watchdog"} or (ALERTS{control_plane_oncall!="true", alertstate="firing", alertname!="Watchdog"} + 1))',
      '{{alertname}} {{instance}}',
    ),

  apiServerAvail:
    self.timeSeriesQuery.new(
      'apiserver_request:availability30d{verb="all"}',
      'Verbose',
    ),

  apiServerRequestsLatencyByVerb:
    self.timeSeriesQuery.new(
      |||
        sum(rate(apiserver_request_duration_seconds_sum{job="apiserver"}[$__rate_interval])) by (verb)
        /
        sum(rate(apiserver_request_duration_seconds_count{job="apiserver"}[$__rate_interval])) by (verb)
      |||,
      '{{ verb }}',
    ),

  apiServerRequestsByCode:
    self.timeSeriesQuery.new(
      'sum by (code) (rate(apiserver_request_total[$__rate_interval]))',
      '{{ code }}',
    ),

  apiServerCpu:
    self.timeSeriesQuery.new(
      |||
        rate(process_cpu_seconds_total{job="apiserver"}[$__rate_interval]) * on(instance) group_left(node)
          max by (node, instance) (label_replace(kube_node_info * on(node, instance) group_left(role)
          (kube_node_role{role="control-plane"}) , "instance", "$1:6443", "internal_ip", "(.*)"))
      |||,
      '{{ node }}',
    ),

  apiServerMem:
    self.timeSeriesQuery.new(
      |||
        process_resident_memory_bytes{job="apiserver"} * on(instance) group_left(node)
          max by (node, instance) (label_replace(kube_node_info * on(node) group_left(role)
          (kube_node_role{role="control-plane"}) , "instance", "$1:6443", "internal_ip", "(.*)"))
      |||,
      '{{ node }}',
    ),

  apiServerNetworkTx:
    self.timeSeriesQuery.new(
      |||
        (rate(node_network_transmit_bytes_total{device!~"lo|veth.*|docker.*|br.*|cilium.*|lxc.*"}[$__rate_interval]) * 8 and
          (node_network_speed_bytes == 1250000000 or node_network_speed_bytes == 125000000)) * on(instance) group_left(node)
          max by (node, instance) (label_replace(kube_node_info * on(node) group_left(role)
          (kube_node_role{role="control-plane"}) , "instance", "$1:9100", "internal_ip", "(.*)"))
      |||,
      '{{ node }}',
    ),

  apiServerNetworkRx:
    self.timeSeriesQuery.new(
      |||
        (rate(node_network_receive_bytes_total{device!~"lo|veth.*|docker.*|br.*|cilium.*|lxc.*"}[$__rate_interval]) * 8 and
          (node_network_speed_bytes == 1250000000 or node_network_speed_bytes == 125000000)) * on(instance) group_left(node)
          max by (node, instance) (label_replace(kube_node_info * on(node) group_left(role)
          (kube_node_role{role="control-plane"}) , "instance", "$1:9100", "internal_ip", "(.*)"))
      |||,
      '{{ node }}',
    ),


  ciliumReady:
    self.timeSeriesQuery.new(
      'count(kube_pod_container_status_ready{container="cilium-agent"})',
      'Ready',
    ),

  ciliumNotReady:
    self.timeSeriesQuery.new(
      'count(kube_node_info) - count(kube_pod_container_status_ready{container="cilium-agent"})',
      'NotReady',
    ),

  multusReady:
    self.timeSeriesQuery.new(
      'count(kube_pod_container_status_ready{container="kube-multus"})',
      'Ready',
    ),

  multusNotReady:
    self.timeSeriesQuery.new(
      |||
        count(kube_pod_status_phase{phase="Pending", pod=~"multus-multus-ds-.*"}) - 
          count(kube_pod_container_status_ready{container="kube-multus"})
      |||,
      'NotReady',
    ),

  whereaboutsReady:
    self.timeSeriesQuery.new(
      'count(kube_pod_container_status_ready{container="whereabouts", pod!~"whereabouts-operator-.*"})',
      'Ready',
    ),

  whereaboutsNotReady:
    self.timeSeriesQuery.new(
      |||
        count(kube_node_info) -
          count(kube_pod_container_status_ready{container="whereabouts", pod!~"whereabouts-operator-.*"})
      |||,
      'NotReady',
    ),

  rdmaReady:
    self.timeSeriesQuery.new(
      'count(kube_pod_container_status_ready{container="rdma-device-plugin"})',
      'Ready',
    ),

  rdmaNotReady:
    self.timeSeriesQuery.new(
      |||
        count(kube_pod_container_state_started{container="rdma-device-plugin"}) -
          count(kube_pod_container_status_ready{container="rdma-device-plugin"})
      |||,
      'NotReady',
    ),

  ciliumDropCount:
    self.timeSeriesQuery.new(
      |||
        sum(increase(cilium_drop_count_total{reason="Missed tail call"}[$__range])) by (node, direction, reason) > 0
      |||,
      '{{node}}-{{direction}}-{{reason}}',
    ),

  zeroRDMA:
    self.tableQuery.new(
      'kube_node_status_capacity{resource="rdma_hca_devices"} == 0',
      '{{node}}',
    ),

  cephHealth:
    self.tableQuery.new(
      'ceph_health_status{}',
      'Verbose',
    ),

  cephOsdsIn:
    self.tableQuery.new(
      'sum(ceph_osd_in{})',
      'Verbose',
    ),

  cephOsdsOut:
    self.tableQuery.new(
      'sum(ceph_osd_out{})',
      'Verbose',
    ),

  cephOsdsUp:
    self.tableQuery.new(
      'sum(ceph_osd_up{})',
      'Verbose',
    ),

  cephOsdsDown:
    self.tableQuery.new(
      'sum(ceph_osd_down{})',
      'Verbose',
    ),

  etcdUsedBytes:
    self.tableQuery.new(
      'etcd_mvcc_db_total_size_in_bytes{job="kube-etcd"}',
      'Used Bytes',
    ),

  etcdCapacityBytes:
    self.tableQuery.new(
      'etcd_server_quota_backend_bytes{job="kube-etcd"}',
      'Capacity Bytes',
    ),

  rootDiskUsage:
    self.timeSeriesQuery.new(
      |||
        max(label_replace(
          (((node_filesystem_size_bytes{mountpoint="/"}-node_filesystem_avail_bytes{mountpoint="/"})/node_filesystem_size_bytes{mountpoint="/"}) > .85),
          "internal_ip", "$1", "instance", "([^:]+):.*"
        ) * on (internal_ip) group_left (node) group(kube_node_info) by (node, internal_ip)) by (node)
      |||,
      '{{node}}',
    ),

  pvcUsage:
    self.tableQuery.new(
      |||
        sum by (persistentvolumeclaim) (
          kubelet_volume_stats_used_bytes{job="kubelet", persistentvolumeclaim!=""}
        )
      |||,
      '{{persistentvolumeclaim}}'
    ),

  pvcUsagePercent:
    self.tableQuery.new(
      |||
        100 * (
          kubelet_volume_stats_capacity_bytes{job="kubelet", persistentvolumeclaim!=""}
          -
          kubelet_volume_stats_available_bytes{job="kubelet", persistentvolumeclaim!=""}
        )
        / kubelet_volume_stats_capacity_bytes{job="kubelet", persistentvolumeclaim!=""}
      |||,
      '{{persistentvolumeclaim}}'
    ),

  cephPoolStoredByName:
    self.tableQuery.new(
      |||
        sum((ceph_pool_stored) *on (pool_id) group_left(name)
          (ceph_pool_metadata{name=~".*"})) by (name)
      |||,
      '{{name}}',
    ),

  cephPoolUsagePercent:
    self.tableQuery.new(
      '100 * sum((ceph_pool_stored) *on (pool_id) group_left(name) (ceph_pool_metadata{name=~".*"})) by (name) / (sum((ceph_pool_stored) *on (pool_id) group_left(name) (ceph_pool_metadata{name=~".*"})) by (name) + sum((ceph_pool_max_avail) *on (pool_id) group_left(name) (ceph_pool_metadata{name=~".*"})) by (name))',
      '{{name}}',
    ),
}

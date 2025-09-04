local conf = import '../config.libsonnet';
local g = import '../g.libsonnet';

local panels = import './panels.libsonnet';
local queries = import './queries.libsonnet';
local variables = import './variables.libsonnet';

g.dashboard.new('Cluster Control Plane Health')
+ g.dashboard.withDescription(|||
  Dashboard for cluster-mgmt core components
|||)
+ g.dashboard.graphTooltip.withSharedCrosshair()
+ g.dashboard.withUid(conf.grafanaDashboardIDs['cluster-control-plane-health.json'])
+ g.dashboard.withVariables([
  variables.datasource,
])
+ g.dashboard.withPanels(
  g.util.grid.wrapPanels(
    //
    // Row: Service Pod Status
    //
    [
      g.panel.row.new('Alerts')
      + g.panel.row.withCollapsed()
      + g.panel.row.withPanels([
        panels.stateTimeline.alerts(
          'Control Plane Oncall Alerts',
          queries.alertsOnlyControlPlane,
          description='Alerts with `control_plane_oncall="true"`, colored according to alertstate: <font color="#FF9840">orange</font> for pending; <font color="#F2495C">red</font> for firing.',
          linkTitle='Alerting Rules',
          linkUrl='/alerting/list',
          h=8,
          w=24
        ),
        panels.stateTimeline.alerts(
          'All Other Alerts',
          queries.alertsExceptControlPlane,
          description='Alerts with `control_plane_oncall!="true"`, colored according to alertstate: <font color="#FF9840">orange</font> for pending; <font color="#F2495C">red</font> for firing.',
          linkTitle='Alerting Rules',
          linkUrl='/alerting/list',
          h=8,
          w=24
        ),
      ]),
    ] +

    //
    // Row: Service Pod Status
    //
    [
      g.panel.row.new('Service Pod Status'),
      panels.table.podStatus(
        'API Server - Pod Status',
        [
          queries.tableQuery.healthQuery('kube-apiserver.*'),
          queries.tableQuery.restartsQuery('kube-apiserver.*'),
        ],
        description='API server pods status and restarts count',
        linkTitle='API Server Dashboard',
        linkUrl='/d/09ec8aa1e996d6ffcd6817bbaff4db1b/kubernetes-api-server',
        h=6,
        w=6,
      ),
      panels.table.podStatus(
        'Kubevip 1G - Pod Status',
        [
          queries.tableQuery.healthQuery('kube-vip-[a-zA-Z0-9]*-.*'),
          queries.tableQuery.restartsQuery('kube-vip-[a-zA-Z0-9]*-.*'),
        ],
        description='1G kube-vip pods status and restarts count',
        h=6,
        w=6,
      ),
      panels.table.podStatus(
        'Private Registry - Pod Status',
        [
          queries.tableQuery.healthQuery('private-registry-.*'),
          queries.tableQuery.restartsQuery('private-registry-.*'),
        ],
        description='Private registry pods status and restarts count',
        linkTitle='Private Registry Dashboard',
        linkUrl='/d/CoBSgj8iz/docker-registry',
        h=6,
        w=6,
      ),
      panels.table.podStatus(
        'Nginx - Pod Status',
        [
          queries.tableQuery.healthQuery('ingress-nginx-controller-.*'),
          queries.tableQuery.restartsQuery('ingress-nginx-controller-.*'),
        ],
        description='Nginx pods status and restarts count',
        linkTitle='Nginx Dashboard',
        linkUrl='/d/nginx/nginx-ingress-controller',
        h=12,
        w=6,
      ),
      panels.table.podStatus(
        'Etcd - Pod Status',
        [
          queries.tableQuery.healthQuery('etcd.*'),
          queries.tableQuery.restartsQuery('etcd.*'),
        ],
        description='Etcd pods status and restarts count',
        linkTitle='Etcd Overview',
        linkUrl='/d/hzhXdzznZn/etcd-cluster-overview',
        h=6,
        w=6,
      ),
      panels.table.podStatus(
        'Kubevip 100G - Pod Status',
        [
          queries.tableQuery.healthQuery('kube-vip-[a-zA-Z0-9]*'),
          queries.tableQuery.restartsQuery('kube-vip-[a-zA-Z0-9]*'),
        ],
        description='100G kube-vip pods status and restarts count',
        h=6,
        w=6,
      ),
      panels.table.podStatus(
        'Coredns - Pod Status',
        [
          queries.tableQuery.healthQuery('coredns-.*'),
          queries.tableQuery.restartsQuery('coredns-.*'),
        ],
        description='Coredns pods status and restarts count',
        linkTitle='Coredns Overview',
        linkUrl='/d/vkQ0UHxik/coredns',
        h=6,
        w=6,
      ),
    ] +

    //
    // Row: Monitoring
    //
    [g.panel.row.new('Monitoring Pod Status')] +
    [
      panels.table.podStatus(
        'System Exporter - Pod Status',
        [
          queries.tableQuery.healthQuery('cluster-mgmt-system-exporter-.*'),
          queries.tableQuery.restartsQuery('cluster-mgmt-system-exporter-.*'),
        ],
        description='System exporter pods status and restarts count',
        h=8,
        w=8,
      ),
      panels.table.podStatus(
        'SNMP Exporter - Pod Status',
        [
          queries.tableQuery.healthQuery('prometheus-snmp-exporter-.*'),
          queries.tableQuery.restartsQuery('prometheus-snmp-exporter-.*'),
        ],
        description='SNMP exporter pods status and restarts count',
        h=8,
        w=8,
      ),
      panels.table.podStatus(
        'Prometheus - Pod Status',
        [
          queries.tableQuery.healthQuery('prometheus-prometheus-prometheus-.*'),
          queries.tableQuery.restartsQuery('prometheus-prometheus-prometheus-.*'),
        ],
        description='Prometheus pods status and restarts count',
        linkTitle='Prometheus Overview',
        linkUrl='/d/aecwqw2b0njswe/prometheus-overview',
        h=12,
        w=8,
      ),
      panels.table.podStatus(
        'Alertmanager - Pod Status',
        [
          queries.tableQuery.healthQuery('alertmanager-prometheus-alertmanager-.*'),
          queries.tableQuery.restartsQuery('alertmanager-prometheus-alertmanager-.*'),
        ],
        description='Alertmanager pods status and restarts count',
        linkTitle='Alertmanager Overview',
        linkUrl='/d/alertmanager-overview',
        h=12,
        w=8,
      ),
      panels.table.podStatus(
        'Alertrouter - Pod Status',
        [
          queries.tableQuery.healthQuery('alert-router-.*'),
          queries.tableQuery.restartsQuery('alert-router-.*'),
        ],
        description='Alertrouter pods status and restarts count',
        h=12,
        w=8,
      ),
      panels.table.podStatus(
        'Grafana - Pod Status',
        [
          queries.tableQuery.healthQuery('prometheus-grafana-.*'),
          queries.tableQuery.restartsQuery('prometheus-grafana-.*'),
        ],
        description='Grafana pods status and restarts count',
        linkTitle='Grafana Overview',
        linkUrl='/d/6be0s85Mk/grafana-overview',
        h=4,
        w=8,
      ),
      panels.table.podStatus(
        'Linkmon - Pod Status',
        [
          queries.tableQuery.healthQuery('cluster-mgmt-linkmon-switch-.*'),
          queries.tableQuery.restartsQuery('cluster-mgmt-linkmon-switch-.*'),
        ],
        description='Linkmon pods status and restarts count',
        h=4,
        w=8,
      ),
      panels.table.podStatus(
        'Loki - Pod Status',
        [
          queries.tableQuery.healthQuery('loki-.*'),
          queries.tableQuery.restartsQuery('loki-.*'),
        ],
        description='Loki pods status and restarts count',
        linkTitle='Loki Overview',
        linkUrl='/d/eeiwbf963045cb/loki-detailed',
        h=4,
        w=8,
      ),
      panels.table.podStatus(
        'Thanos Query - Pod Status',
        [
          queries.tableQuery.healthQuery('thanos-query-.*'),
          queries.tableQuery.restartsQuery('thanos-query-.*'),
        ],
        description='Thanos Query pods status and restarts count',
        linkTitle='Thanos Query',
        linkUrl='/d/af36c91291a603f1d9fbdabdd127ac4a/thanos-query',
        h=8,
        w=8,
      ),
      panels.table.podStatus(
        'Thanos Ruler - Pod Status',
        [
          queries.tableQuery.healthQuery('thanos-ruler-prometheus-thanos-ruler-.*'),
          queries.tableQuery.restartsQuery('thanos-ruler-prometheus-thanos-ruler-.*'),
        ],
        description='Thanos Ruler pods status and restart counts',
        linkTitle='Thanos Ruler',
        linkUrl='/d/35da848f5f92b2dc612e0c3a0577b8a1/thanos-rule',
        h=4,
        w=8,
      ),
      panels.table.podStatus(
        'Fluentbit - Pod Status',
        [
          queries.tableQuery.healthQuery('fluentbit-.*'),
          queries.tableQuery.restartsQuery('fluentbit-.*'),
        ],
        description='Fluentbit pods status and restarts count',
        linkTitle='Fluentbit Overview',
        linkUrl='/d/d557c8f6-cac1-445f-8ade-4c351a9076b1/fluent-bit',
        h=4,
        w=8,
      ),
    ] +
    //
    // Row: API Server Overview
    //
    [
      g.panel.row.new('API Server Overview'),
      panels.stat.apiServerAvail(
        'API Server - Availability (30d) > 99.000%',
        queries.apiServerAvail,
        description='How many percent of requests (both read and write) in 30 days have been answered successfully and fast enough?'
      ),
      panels.timeSeries.latency('API Server - HTTP Requests Latency by verb', queries.apiServerRequestsLatencyByVerb),
      panels.timeSeries.base('API Server - HTTP Requests by code', queries.apiServerRequestsByCode),
      panels.timeSeries.cpu('API Server - CPU Usage by instance', queries.apiServerCpu, h=8, w=12),
      panels.timeSeries.mem('API Server - Memory Usage by instance', queries.apiServerMem, h=8, w=12),
      panels.timeSeries.network('API Server - Network Transmitted', queries.apiServerNetworkTx, h=8, w=12),
      panels.timeSeries.network('API Server - Network Received', queries.apiServerNetworkRx, h=8, w=12),
    ] +

    //
    // Row: Networking - cilium + Multus + Whereabouts + RDMA
    //
    [
      g.panel.row.new('Networking - cilium + Multus + Whereabouts + RDMA'),
      panels.stat.readyOrNot(
        'Cilium',
        [queries.ciliumReady, queries.ciliumNotReady],
        description='Cilium pods Ready/NotReady count',
        linkTitle='Cilium Metrics',
        linkUrl='/d/vtuWtdumz/cilium-metrics',
        h=4,
        w=6
      ),
      panels.stat.readyOrNot(
        'Multus',
        [queries.multusReady, queries.multusNotReady],
        description='Multus pods Ready/NotReady count',
        h=4,
        w=6
      ),
      panels.stat.readyOrNot(
        'Whereabouts',
        [queries.whereaboutsReady, queries.whereaboutsNotReady],
        description='Whereabouts pods Ready/NotReady count',
        h=4,
        w=6
      ),
      panels.stat.readyOrNot(
        'RDMA',
        [queries.rdmaReady, queries.rdmaNotReady],
        description='Rdma device pods Ready/NotReady count',
        h=4,
        w=6
      ),
      panels.timeSeries.base('Cilium Drop Count', queries.ciliumDropCount, h=4, w=18),
      panels.table.zeroRdma(
        'Unhealthy Nodes with 0 RDMA device',
        queries.zeroRDMA,
        description='Nodes with 0 RDMA device',
        h=4,
        w=6
      ),
    ] +

    //
    // Row: Storage
    //
    [
      g.panel.row.new('Storage'),
      panels.stat.cephHealth(
        'Ceph - Health Status',
        queries.cephHealth,
        description='Ceph cluster health status',
        linkTitle='Ceph Cluster',
        linkUrl='d/tb19LAiZK/ceph-cluster',
        h=3,
        w=4
      ),
      panels.stat.cephHealth('OSDs IN', queries.cephOsdsIn, h=3, w=2),
      panels.stat.cephHealth('OSDs OUT', queries.cephOsdsOut, h=3, w=2),
      panels.stat.cephHealth('OSDs UP', queries.cephOsdsUp, h=3, w=2),
      panels.stat.cephHealth('OSDs DOWN', queries.cephOsdsDown, h=3, w=2),
      panels.timeSeries.rootDiskUsage('Root Disk Usage > 85%', queries.rootDiskUsage, h=9, w=12),
      panels.table.storageUsage(
        'Etcd Storage Usage',
        [
          queries.etcdUsedBytes,
          queries.etcdCapacityBytes,
        ],
        h=6,
        w=12,
      ),
      panels.pieChart.pvcUsageGrouped(
        title='PVC usage by component',
        targets=[queries.pvcUsage],
        h=8,
        w=12
      ),
      panels.barChart.pvcUsagePercentBar(
        title='PVC usage % by component',
        targets=[queries.pvcUsagePercent],
        h=8,
        w=12,
      ),
      panels.pieChart.cephPoolStoredPie(
        title='Ceph Pool Storage by Name',
        targets=[queries.cephPoolStoredByName],
        h=8,
        w=12,
      ),
      panels.barChart.cephPoolUsagePercentBar(
        title='Ceph Pool Usage % by Name',
        targets=[queries.cephPoolUsagePercent],
        h=8,
        w=12,
      ),
    ],

  )
)

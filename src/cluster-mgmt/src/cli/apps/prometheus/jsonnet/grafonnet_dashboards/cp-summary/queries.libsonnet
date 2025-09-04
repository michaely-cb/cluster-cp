local g = import '../g.libsonnet';
local utils = import './utils.libsonnet';
local prometheusQuery = g.query.prometheus;

local tableQuery = {
  new(datasource, query, legendFormat):
    prometheusQuery.new(datasource, query)
    + {
      format: 'table',
      instant: true,
      legendFormat: legendFormat,
    },
};

local services = [
  { name: 'API Server', regex: 'kube-apiserver-.*', ns: 'kube-system' },
  { name: 'etcd', regex: 'etcd-.*', ns: 'kube-system' },
  { name: 'CoreDNS', regex: 'coredns-.*', ns: 'kube-system' },
  { name: 'Rook Ceph (MON)', regex: 'rook-ceph-mon-.*', ns: 'rook-ceph' },
  { name: 'Rook Ceph (MDS)', regex: 'rook-ceph-mds-.*', ns: 'rook-ceph' },
  { name: 'Rook Ceph (OSD)', regex: 'rook-ceph-osd-.*', ns: 'rook-ceph' },
  { name: 'System Exporter', regex: 'cluster-mgmt-system-exporter-*', ns: 'prometheus' },
  { name: 'SNMP Exporter', regex: 'prometheus-snmp-exporter-*', ns: 'prometheus' },
  { name: 'Kube State Metrics', regex: 'prometheus-kube-state-metrics-*', ns: 'prometheus' },
  { name: 'Prometheus', regex: 'prometheus-prometheus-prometheus-.*', ns: 'prometheus' },
  { name: 'Grafana', regex: 'prometheus-grafana-.*', ns: 'prometheus' },
  { name: 'Loki', regex: 'loki-.*', ns: 'loki' },
  { name: 'Fluent-Bit', regex: 'fluent-bit-.*', ns: 'fluent-bit' },
  { name: 'Registry', regex: 'private-registry-.*', ns: 'kube-system' },
  { name: 'Kube-Vip (1G)', regex: 'kube-vip-[^-]+-.+', ns: 'kube-system' },
  { name: 'Kube-Vip (100G)', regex: 'kube-vip-[a-zA-Z0-9]+', ns: 'kube-system' },
  { name: 'Nginx', regex: 'ingress-nginx-controller-.*', ns: 'kube-system' },
  { name: 'Linkmon', regex: 'cluster-mgmt-linkmon-switch-.*', ns: 'prometheus' },
  { name: 'Cilium', regex: 'cilium-.*', ns: 'kube-system' },
  { name: 'AlertRouter', regex: 'alert-router-.*', ns: 'prometheus' },
  { name: 'AlertManager', regex: 'alertmanager-.*', ns: 'prometheus' },
  { name: 'Multus', regex: 'multus-multus-.*', ns: 'kube-system' },
  { name: 'Whereabouts', regex: 'whereabouts-.*', ns: 'kube-system' },
];

{
  apiServerSuccess:
    prometheusQuery.new(
      '$PROMETHEUS_DS',
      'sum(rate(apiserver_request_total{job="apiserver", code=~"2..|3..|4.."}[5m])) / sum(rate(apiserver_request_total{job="apiserver"}[5m])) * 100'
    )
    + {
      format: 'percent',  // this is key for Grafana to format it properly
      legendFormat: 'API Server Success Rate',
    },

  degradedServices: [utils.mkDegraded(svc, tableQuery, '$PROMETHEUS_DS') for svc in services],

  downServices: [utils.mkDown(svc, tableQuery, '$PROMETHEUS_DS') for svc in services],

  restartsByService: [utils.mkRestarts(svc, tableQuery, '$PROMETHEUS_DS') for svc in services],
}

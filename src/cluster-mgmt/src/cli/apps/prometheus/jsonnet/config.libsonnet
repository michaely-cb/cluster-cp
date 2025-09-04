{
  // Grafana dashboard IDs are necessary for stable links for dashboards
  grafanaDashboardIDs: {
    'wsjob-dashboard.json': 'WebHNShVz',
    'ml-admin-dashboard.json': 'IsU0ZxuVz',
    'cluster-control-plane-health.json': 'aec3a3n17e2o0c',
    'cp-summary.json': 'dej8p35xpawhse',
  },
  globalConfig: {
    editable: false,
    graphTooltip: 1,
    refresh: '',
    timezone: '',
    time: {
      from: 'now-24h',
      to: 'now',
    },
    tags: ['cluster-mgmt'],
  },
  _config+:: {
    wsjob: '$wsjob',
  },
}

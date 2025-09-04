local alerts = (import 'alert.libsonnet');

{
  additionalPrometheusRules: {
    name: 'cluster-mgmt',
    groups: alerts.prometheusAlerts.groups,
  },
}

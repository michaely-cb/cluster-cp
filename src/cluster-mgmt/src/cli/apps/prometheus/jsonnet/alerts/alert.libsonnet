local query = (import '../queries/query.libsonnet');
local conf = import '../config.libsonnet';

{
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'cluster-mgmt-alerts',
        rules: [
          {
            expr: query.pod_errors.expr % $._config,
            labels: {
              severity: 'warning',
            },
            annotations: {
              description: '{{ $labels.pod }} error: {{$labels.reason}}.',
              summary: 'Wsjob failed.',
            },
            'for': '5m',
            alert: 'WsjobPodError',
          },
        ],
      },
    ],
  },
} + conf

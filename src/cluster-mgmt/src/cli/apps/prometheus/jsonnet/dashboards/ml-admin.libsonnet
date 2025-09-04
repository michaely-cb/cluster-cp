local conf = import '../config.libsonnet';
local dashboard = import 'dashboard.libsonnet';
local pannel = import 'pannel.libsonnet';
local template = import 'template.libsonnet';

{
  grafanaDashboards+:: {
    local ds = template.datasource(),
    local namespace =
      template.new(
        name='namespace',
        query='label_values(namespace_reservation, namespace)',
        includeAll=true,
      ),

    'ml-admin-dashboard.json':
      dashboard.newDashboard(
        'ML Admin Dashboard',
        uid=conf.grafanaDashboardIDs['ml-admin-dashboard.json'],
      )
      + {
        panels+:
          pannel.newRow()
          .addPanel(
            pannel.newStatePanel('CS2 Overall Status')
            + pannel.addDescription('Systems status at the last timestamp of the selected range.')
            + pannel.queryPanel('sum((max(system_overall_health) by (system)) * on (system) group_left group by (system) (namespace_reserved_system{namespace=~"$namespace"}) == bool 1) + sum((max(system_overall_health) by (system)) * on (system) group_left group by (system) (namespace_reserved_system{namespace=~"$namespace"}) == bool 2)', 'Available')
            + pannel.queryPanel('clamp_max(avg_over_time((sum((group(wsjob_system_grant{namespace=~"$namespace"}) by (system)) or (sum(0*system_label) by (system))) / count(group(namespace_reserved_system{namespace=~"$namespace"}) by (system)))[$__range:$__rate_interval]) *100, 100)', 'Utilized Percent')
            + pannel.queryPanel('avg_over_time((sum(group(wsjob_system_grant{namespace=~"$namespace"}) by (system)) or sum(sum(system_label*0) by (system)))[$__range:$__rate_interval]) * (${__to:date:seconds}-${__from:date:seconds})/3600', 'Utilized Hours')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('CS2 Jobs Histogram', unit='none', decimals=0, stacking=true, lineInterpolation='stepBefore', cals=['max'], h=11, fillOpacity=100, lineWidth=0)
            + pannel.addDescription('(Max) Number of systems used by jobs')
            + pannel.queryPanel('count by (name, labels_k8s_cerebras_com_Model) (group by (name, system) (wsjob_system_grant{namespace=~"$namespace"})) * on (name) group_left(labels_k8s_cerebras_com_Model) group by(name, labels_k8s_cerebras_com_Model) (wsjob_annotation{job_type!="compile", namespace=~"$namespace"})', '{{labels_k8s_cerebras_com_Model}} / {{name}}')
          ).panels,
      }
      + {
        templating+: {
          list: [ds, namespace],
        },
      } + conf.globalConfig,
  },
}

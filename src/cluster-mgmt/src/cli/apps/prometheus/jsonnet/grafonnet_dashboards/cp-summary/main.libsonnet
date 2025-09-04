local conf = import '../../config.libsonnet';
local g = import '../g.libsonnet';
local panels = import './panels.libsonnet';
local variables = import './variables.libsonnet';

g.dashboard.new('Cluster Control Plane Summary')
+ g.dashboard.withUid(conf.grafanaDashboardIDs['cp-summary.json'])
+ g.dashboard.withDescription('Summary dashboard for cluster control plane health signals')
+ g.dashboard.graphTooltip.withSharedCrosshair()
+ g.dashboard.withVariables([variables.datasource])
+ g.dashboard.withPanels(
  g.util.grid.wrapPanels([
    g.panel.row.new('API Summary'),
    panels.apiServerSuccess(),
    g.panel.row.new('Action Required'),
    panels.servicesDown(),
    g.panel.row.new('Warnings'),
    panels.failedPods(),
    g.panel.row.new('Error History'),
    panels.recentRestarts(),
  ])
)
+ conf.globalConfig

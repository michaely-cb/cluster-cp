local dashboards = (import 'dashboards/dashboards.libsonnet').grafanaDashboards;

{
  [name]: dashboards[name]
  for name in std.objectFields(dashboards)
}

local g = import 'github.com/grafana/jsonnet-libs/grafana-builder/grafana.libsonnet';

{
  // init new dashboard
  newDashboard(title, uid=''):: g.dashboard(title, uid),
}

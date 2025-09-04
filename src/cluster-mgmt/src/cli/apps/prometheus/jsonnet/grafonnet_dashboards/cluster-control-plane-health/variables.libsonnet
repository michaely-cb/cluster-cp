local g = import '../g.libsonnet';
local var = g.dashboard.variable;

{
  datasource:
    var.datasource.new('datasource', 'prometheus')
    + var.datasource.generalOptions.showOnDashboard.withNothing()
    + var.datasource.generalOptions.withCurrent('default')
    + var.datasource.generalOptions.withLabel('Data Source'),
}

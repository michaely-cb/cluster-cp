local g = import '../g.libsonnet';
local queries = import './queries.libsonnet';
local variables = import './variables.libsonnet';

{
  apiServerSuccess(): g.panel.stat.new('API Server Success Rate')
                      + { datasource: '$' + variables.datasource.name }
                      + g.panel.stat.queryOptions.withTargets([queries.apiServerSuccess])
                      + g.panel.stat.options.withGraphMode('none')
                      + g.panel.stat.standardOptions.withUnit('percent')
                      + g.panel.stat.standardOptions.thresholds.withSteps([
                        { value: null, color: 'red' },
                        { value: 90, color: 'orange' },
                        { value: 95, color: '#EAB839' },
                        { value: 99.9, color: 'green' },
                      ])
                      + g.panel.stat.panelOptions.withLinks([
                        {
                          title: 'View Components Dashboard',
                          url: '/d/aec3a3n17e2o0c/clustermgmt-core-components-dashboard?orgId=1&${__url_time_range}&var-datasource=${datasource}',
                        },
                      ])
                      + g.panel.stat.panelOptions.withGridPos(h=6, w=24),

  servicesDown(): g.panel.table.new('Services That Are Down')
                  + { datasource: '$' + variables.datasource.name }
                  + g.panel.table.queryOptions.withTargets(queries.downServices)
                  + g.panel.table.queryOptions.withTransformations([
                    g.panel.table.queryOptions.transformation.withId('labelsToFields'),
                    g.panel.table.queryOptions.transformation.withId('organize')
                    + g.panel.table.queryOptions.transformation.withOptions({
                      includeByName: { Series: true },
                      renameByName: { Series: 'Service' },
                    }),
                  ])
                  + g.panel.table.standardOptions.withNoValue('All services healthy')
                  + g.panel.table.panelOptions.withGridPos(h=4, w=24),

  failedPods(): g.panel.table.new('Failed Pods by Service')
                + { datasource: '$' + variables.datasource.name }
                + g.panel.table.queryOptions.withTargets(queries.degradedServices)
                + g.panel.table.queryOptions.withTransformations([
                  g.panel.table.queryOptions.transformation.withId('labelsToFields'),
                  g.panel.table.queryOptions.transformation.withId('organize')
                  + g.panel.table.queryOptions.transformation.withOptions({
                    includeByName: { Series: true },
                    renameByName: { Series: 'Service' },
                  }),
                ])
                + g.panel.table.standardOptions.withNoValue('No failed pods')
                + g.panel.table.panelOptions.withGridPos(h=4, w=24),

  recentRestarts(): g.panel.table.new('Recent Restarts by Service (15m)')
                    + { datasource: '$' + variables.datasource.name }
                    + g.panel.table.queryOptions.withTargets(queries.restartsByService)
                    + g.panel.table.queryOptions.withTransformations([
                      g.panel.table.queryOptions.transformation.withId('labelsToFields'),
                      g.panel.table.queryOptions.transformation.withId('organize')
                      + g.panel.table.queryOptions.transformation.withOptions({
                        includeByName: { Series: true },
                        renameByName: { Series: 'Service' },
                      }),
                    ])
                    + g.panel.table.standardOptions.withNoValue('No recent restarts')
                    + g.panel.table.panelOptions.withGridPos(h=4, w=24),
}

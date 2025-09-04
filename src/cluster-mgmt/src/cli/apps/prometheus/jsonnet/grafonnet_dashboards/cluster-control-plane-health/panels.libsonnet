local g = import '../g.libsonnet';

local variables = import './variables.libsonnet';

{
  stat: {
    local stat = g.panel.stat,
    local queryOptions = stat.queryOptions,
    local options = stat.options,
    local standardOptions = stat.standardOptions,
    local panelOptions = stat.panelOptions,

    base(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      stat.new(title)
      + (
        if description == '' then {} else
          panelOptions.withDescription(description)
      )
      + (
        if h == 0 && w == 0 then {} else
          panelOptions.withGridPos(h=h, w=w)
      )
      + (
        if linkTitle == '' && linkUrl == '' then {} else
          panelOptions.withLinks(
            panelOptions.link.withTitle(linkTitle)
            + panelOptions.link.withUrl(linkUrl)
          )
      )
      + queryOptions.withTargets(targets)
      + queryOptions.withDatasource('$' + variables.datasource.type, '$' + variables.datasource.type)
      + options.withOrientation('horizontal')
      + options.withGraphMode('none'),

    readyOrNot(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withOverrides(
        standardOptions.override.byName.new('NotReady')
        + standardOptions.override.byName.withProperty(
          'color',
          {
            fixedColor: 'semi-dark-red',
            mode: 'thresholds',
          },
        )
      )
      + standardOptions.withOverridesMixin(
        standardOptions.override.byName.new('Ready')
        + standardOptions.override.byName.withProperty(
          'color',
          {
            fixedColor: 'semi-dark-green',
            mode: 'fixed',
          },
        ),
      )
      + standardOptions.thresholds.withSteps([
        { color: 'green', value: 0 },
        { color: 'red', value: 1 },
      ])
      + options.withColorMode('background_solid'),

    cephHealth(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withMappings([
        {
          type: 'value',
          options: {
            '0': { text: 'Healthy', color: 'green' },
            '1': { text: 'Warning', color: 'yellow' },
            '2': { text: 'Error', color: 'red' },
          },
        },
        {
          type: 'special',
          options: {
            result: { text: 'N/A', color: 'gray' },
          },
        },
      ]),

    apiServerAvail(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + options.withGraphMode('area')
      + standardOptions.withUnit('percentunit')
      + standardOptions.withDecimals(3),
  },

  timeSeries: {
    local timeSeries = g.panel.timeSeries,
    local queryOptions = timeSeries.queryOptions,
    local standardOptions = timeSeries.standardOptions,
    local options = timeSeries.options,
    local panelOptions = timeSeries.panelOptions,

    base(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      timeSeries.new(title)
      + queryOptions.withTargets(targets)
      + queryOptions.withDatasource('$' + variables.datasource.type, '$' + variables.datasource.type)
      + (
        if description == '' then {} else
          panelOptions.withDescription(description)
      )
      + (
        if h == 0 && w == 0 then {} else
          timeSeries.panelOptions.withGridPos(h=h, w=w)
      )
      + (
        if linkTitle == '' && linkUrl == '' then {} else
          panelOptions.withLinks(
            panelOptions.link.withTitle(linkTitle)
            + panelOptions.link.withUrl(linkUrl)
          )
      ),

    rootDiskUsage(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.thresholds.withSteps([
        { color: 'green', value: null },
        { color: 'red', value: 0.85 },
      ])
      + standardOptions.withUnit('percentunit')
      + standardOptions.withNoValue('Healthy')
      + options.withLegend({
        calcs: ['lastNotNull'],
        displayMode: 'table',
        placement: 'right',
      })
      + timeSeries.fieldConfig.defaults.custom.withDrawStyle('bars'),

    latency(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withUnit('milliseconds (ms)'),

    cpu(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withUnit('percentunit'),

    mem(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withUnit('decbytes'),

    network(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withUnit('bps'),
  },

  stateTimeline: {
    local stateTimeline = g.panel.stateTimeline,
    local queryOptions = stateTimeline.queryOptions,
    local standardOptions = stateTimeline.standardOptions,
    local options = stateTimeline.options,
    local panelOptions = stateTimeline.panelOptions,

    base(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      stateTimeline.new(title)
      + queryOptions.withTargets(targets)
      + queryOptions.withDatasource('$' + variables.datasource.type, '$' + variables.datasource.type)
      + (
        if description == '' then {} else
          panelOptions.withDescription(description)
      )
      + (
        if h == 0 && w == 0 then {} else
          stateTimeline.panelOptions.withGridPos(h=h, w=w)
      )
      + (
        if linkTitle == '' && linkUrl == '' then {} else
          panelOptions.withLinks(
            panelOptions.link.withTitle(linkTitle)
            + panelOptions.link.withUrl(linkUrl)
          )
      ),

    alerts(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withNoValue('No alerts')
      + options.withShowValue('never')
      + options.withPerPage(10)
      + options.withTooltip({ mode: 'multi', sort: 'desc' })
      + standardOptions.withMappings([
        {
          type: 'value',
          options: {
            '1': { text: 'Pending', color: 'orange' },
            '2': { text: 'Firing', color: 'red' },
          },
        },
      ])
      + standardOptions.withUnit('milliseconds (ms)'),
  },

  barChart: {
    local barChart = g.panel.barChart,
    local queryOptions = barChart.queryOptions,
    local standardOptions = barChart.standardOptions,
    local options = barChart.options,
    local panelOptions = barChart.panelOptions,

    base(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      barChart.new(title)
      + queryOptions.withTargets(targets)
      + queryOptions.withDatasource('$' + variables.datasource.type, '$' + variables.datasource.type)
      + (
        if description == '' then {} else
          panelOptions.withDescription(description)
      )
      + (
        if h == 0 && w == 0 then {} else
          panelOptions.withGridPos(h=h, w=w)
      )
      + (
        if linkTitle == '' && linkUrl == '' then {} else
          panelOptions.withLinks(
            panelOptions.link.withTitle(linkTitle)
            + panelOptions.link.withUrl(linkUrl)
          )
      ),

    pvcUsagePercentBar(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withUnit('percent')
      + standardOptions.thresholds.withSteps([
        { color: 'green', value: 0 },
        { color: 'orange', value: 80 },
        { color: 'red', value: 90 },
      ])
      + options.withLegend({
        displayMode: 'table',
        placement: 'right',
        showLegend: true,
      })
      + options.withOrientation('horizontal')
      + options.withShowValue(true)
      + options.withText({
        valueField: 'Usage %',
      })
      + queryOptions.withTransformations([
        {
          id: 'renameByRegex',
          options: {
            fieldName: 'persistentvolumeclaim',
            regex: '^(.*)-shard-\\d+-\\d+$',
            renamePattern: '$1',
          },
        },
        {
          id: 'renameByRegex',
          options: {
            fieldName: 'persistentvolumeclaim',
            regex: '^(.*)-shard-\\d+$',
            renamePattern: '$1',
          },
        },
        {
          id: 'renameByRegex',
          options: {
            fieldName: 'persistentvolumeclaim',
            regex: '^(.*)-\\d+$',
            renamePattern: '$1',
          },
        },
        {
          id: 'groupBy',
          options: {
            fields: {
              Value: {
                aggregations: ['last'],
                operation: 'aggregate',
              },
              persistentvolumeclaim: {
                aggregations: [],
                operation: 'groupby',
              },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {},
            indexByName: {
              persistentvolumeclaim: 0,
              'Value (last)': 1,
            },
            renameByName: {
              'Value (last)': 'Usage %',
            },
          },
        },
        {
          id: 'convertFieldType',
          options: {
            conversions: [
              {
                destinationType: 'number',
                targetField: 'Usage %',
              },
            ],
          },
        },
      ]),

    cephPoolUsagePercentBar(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.withUnit('percent')
      + standardOptions.thresholds.withSteps([
        { color: 'green', value: 0 },
        { color: 'orange', value: 80 },
        { color: 'red', value: 90 },
      ])
      + options.withLegend({
        displayMode: 'table',
        placement: 'right',
        showLegend: true,
      })
      + options.withOrientation('horizontal')
      + options.withShowValue(true)
      + options.withText({
        valueField: 'Usage %',
      })
      + queryOptions.withTransformations([
        {
          id: 'groupBy',
          options: {
            fields: {
              Value: {
                aggregations: ['last'],
                operation: 'aggregate',
              },
              name: {
                aggregations: [],
                operation: 'groupby',
              },
            },
          },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {},
            indexByName: {
              name: 0,
              'Value (last)': 1,
            },
            renameByName: {
              'Value (last)': 'Usage %',
            },
          },
        },
        {
          id: 'convertFieldType',
          options: {
            conversions: [
              {
                destinationType: 'number',
                targetField: 'Usage %',
              },
            ],
          },
        },
      ]),
  },

  table: {
    local table = g.panel.table,
    local queryOptions = table.queryOptions,
    local transformation = queryOptions.transformation,
    local standardOptions = table.standardOptions,
    local panelOptions = table.panelOptions,

    base(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      table.new(title)
      + queryOptions.withTargets(targets)
      + queryOptions.withDatasource('$' + variables.datasource.type, '$' + variables.datasource.type)
      + (
        if description == '' then {} else
          panelOptions.withDescription(description)
      )
      + (
        if linkTitle == '' && linkUrl == '' then {} else
          panelOptions.withLinks(
            panelOptions.link.withTitle(linkTitle)
            + panelOptions.link.withUrl(linkUrl)
          )
      )
      + (
        if h == 0 || w == 0 then {} else
          panelOptions.withGridPos(h=h, w=w)
      ),

    podStatus(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + table.fieldConfig.defaults.custom.withCellOptions({
        mode: 'gradient',
        type: 'color-background',
      })
      + standardOptions.thresholds.withSteps([
        { color: 'text', value: null },
        { color: 'red', value: 0 },
        { color: 'green', value: 1 },
      ])
      + queryOptions.withTransformations(
        transformation.withId('joinByField')
        + transformation.withOptions({ byField: 'pod' })
        + transformation.withOptionsMixin({ mode: 'outerTabular' })
        + transformation.withTopic('series')
      )
      + queryOptions.withTransformationsMixin(
        transformation.withId('organize')
        + transformation.withOptions({
          includeByName: {
            'Value #A': true,
            'Value #B': true,
            pod: true,
          },
        })
        + transformation.withOptionsMixin({
          renameByName: {
            'Value #A': 'Status',
            'Value #B': 'Restarts',
          },
        })
      )
      + queryOptions.withTransformationsMixin(
        transformation.withId('sortBy')
        + transformation.withOptions({
          sort: [
            {
              desc: true,
              field: 'Restarts',
            },
          ],
        })
      )
      + standardOptions.withMappings([
        {
          type: 'value',
          options: {
            '1': { text: 'Up', color: 'green' },
            '0': { text: 'Down', color: 'red' },
          },
        },
      ])
      + standardOptions.withOverrides(
        standardOptions.override.byName.new('Status')
        + standardOptions.override.byName.withProperty(
          'color',
          '',
        )
        + standardOptions.override.byName.withProperty(
          'custom.width',
          80,
        )
      )
      + standardOptions.withOverridesMixin(
        standardOptions.override.byName.new('Restarts')
        + standardOptions.override.byName.withProperty(
          'mappings',
          [
            {
              options: {
                '0': {
                  color: 'semi-dark-green',
                  index: 0,
                  text: '0',
                },
              },
              type: 'value',
            },
            {
              options: {
                from: 1,
                result: {
                  color: 'semi-dark-yellow',
                  index: 1,
                },
                to: 10000,
              },
              type: 'range',
            },
          ],
        )
        + standardOptions.override.byName.withProperty(
          'custom.width',
          80,
        ),
      ),

    zeroRdma(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + table.fieldConfig.defaults.custom.withCellOptions({
        mode: 'gradient',
        type: 'color-background',
      })
      + standardOptions.thresholds.withSteps([
        { color: 'red', value: 0 },
      ])
      + standardOptions.withNoValue('Healthy')
      + queryOptions.withTransformationsMixin(
        transformation.withId('organize')
        + transformation.withOptions({
          includeByName: {
            node: true,
          },
        })
      ),

    storageUsage(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      self.base(title, targets, description, linkTitle, linkUrl, h, w)
      + standardOptions.thresholds.withSteps([
        { color: 'green', value: 0 },
      ])
      + queryOptions.withTransformations(
        transformation.withId('joinByField')
        + transformation.withOptions({ byField: 'pod' })
        + transformation.withOptionsMixin({ mode: 'outerTabular' })
        + transformation.withTopic('series')
      )
      + queryOptions.withTransformationsMixin(
        transformation.withId('organize')
        + transformation.withOptions({
          includeByName: {
            'Value #A': true,
            'Value #B': true,
            pod: true,
          },
        })
        + transformation.withOptionsMixin({
          renameByName: {
            'Value #A': 'Used Bytes',
            'Value #B': 'Capacity Bytes',
          },
        })
      )
      + standardOptions.withOverrides(
        standardOptions.override.byName.new('Used Bytes')
        + standardOptions.override.byName.withProperty(
          'unit',
          'decbytes',
        )
        + standardOptions.override.byName.withProperty(
          'custom.width',
          111,
        )
      )
      + standardOptions.withOverridesMixin(
        standardOptions.override.byName.new('Capacity Bytes')
        + standardOptions.override.byName.withProperty(
          'unit',
          'decbytes',
        )
        + standardOptions.override.byName.withProperty(
          'custom.width',
          111
        )
      ),

    pvcUsage(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      local panel = self.base(title, targets, description, linkTitle, linkUrl, h, w);

      panel
      + queryOptions.withTransformations([
        transformation.withId('organize')
        + transformation.withOptions({
          indexByName: {
            Time: 0,
            Value: 1,
            namespace: 2,
            persistentvolumeclaim: 3,
          },
          renameByName: {
            Value: 'Usage',
            persistentvolumeclaim: 'PVC',
            namespace: 'Namespace',
          },
        }),
      ])
      + standardOptions.withOverrides([
        standardOptions.override.byName.new('Usage')
        + standardOptions.override.byName.withProperty('unit', 'bytes')
        + standardOptions.override.byName.withProperty('custom.width', 120)
        + standardOptions.override.byName.withProperty('custom.displayMode', 'auto'),
      ])
      + standardOptions.withOverridesMixin([
        standardOptions.override.byName.new('PVC')
        + standardOptions.override.byName.withProperty('custom.width', 350),
      ])
      + standardOptions.withOverridesMixin([
        standardOptions.override.byName.new('Namespace')
        + standardOptions.override.byName.withProperty('custom.width', 200),
      ]),
  },

  pieChart: {
    local pieChart = g.panel.pieChart,
    local queryOptions = pieChart.queryOptions,
    local standardOptions = pieChart.standardOptions,
    local panelOptions = pieChart.panelOptions,
    local options = pieChart.options,

    base(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      pieChart.new(title)
      + queryOptions.withTargets(targets)
      + queryOptions.withDatasource('$' + variables.datasource.type, '$' + variables.datasource.type)
      + (if h == 0 || w == 0 then {} else panelOptions.withGridPos(h=h, w=w))
      + (if description == '' then {} else panelOptions.withDescription(description))
      + options.withReduceOptions({ calcs: ['allValues'] })
      + standardOptions.withUnit('bytes')
      + options.withPieType('donut')
      + options.withLegend({ displayMode: 'list', placement: 'bottom', show: true })
      + options.withDisplayLabels(['name', 'percent']),

    pvcUsageGrouped(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      local panel = self.base(title, targets, description, linkTitle, linkUrl, h, w);
      panel
      + queryOptions.withTransformations([
        {
          id: 'renameByRegex',
          options: {
            fieldName: 'persistentvolumeclaim',
            regex: '^(.*)-shard-\\d+-\\d+$',
            renamePattern: '$1',
          },
        },
        {
          id: 'renameByRegex',
          options: {
            fieldName: 'persistentvolumeclaim',
            regex: '^(.*)-shard-\\d+$',
            renamePattern: '$1',
          },
        },
        {
          id: 'renameByRegex',
          options: {
            fieldName: 'persistentvolumeclaim',
            regex: '^(.*)-\\d+$',
            renamePattern: '$1',
          },
        },
        {
          id: 'groupBy',
          options: {
            groupBy: ['persistentvolumeclaim'],
            aggregations: [
              { field: 'Value', type: 'sum' },
            ],
            fields: {
              Value: {
                aggregations: ['sum'],
                operation: 'aggregate',
              },
              persistentvolumeclaim: {
                aggregations: [],
                operation: 'groupby',
              },
            },
          },
        },
      ])
      + options.withPieType('donut')
      + options.withReduceOptions({
        calcs: ['allValues'],
        fields: '',
        values: true,
      })
      + options.withDisplayLabels([])
      + options.withLegend({
        displayMode: 'list',
        placement: 'bottom',
        show: true,
      })
      + standardOptions.withUnit('decbytes'),

    cephPoolStoredPie(title, targets, description='', linkTitle='', linkUrl='', h=0, w=0):
      local panel = self.base(title, targets, description, linkTitle, linkUrl, h, w);
      panel
      + queryOptions.withTransformations([
        {
          id: 'organize',
          options: {
            renameByName: {
              sum: 'Value',
            },
          },
        },
        {
          id: 'groupBy',
          options: {
            groupBy: ['name'],
            aggregations: [
              { field: 'Value', type: 'sum' },
            ],
            fields: {
              Value: {
                aggregations: ['sum'],
                operation: 'aggregate',
              },
              name: {
                aggregations: [],
                operation: 'groupby',
              },
            },
          },
        },
      ])
      + options.withPieType('donut')
      + options.withReduceOptions({
        calcs: ['allValues'],
        fields: '',
        values: true,
      })
      + options.withDisplayLabels([])
      + options.withLegend({
        displayMode: 'list',
        placement: 'bottom',
        show: true,
      })
      + standardOptions.withUnit('decbytes'),
  },

}

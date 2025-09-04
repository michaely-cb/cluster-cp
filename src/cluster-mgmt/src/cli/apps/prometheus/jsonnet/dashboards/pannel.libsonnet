local g = import 'github.com/grafana/jsonnet-libs/grafana-builder/grafana.libsonnet';

{
  // init new row
  newRow(name='', collapsed=false):: {
    _panels:: if collapsed == false then [{
      type: 'row',
      collapsed: collapsed,
      gridPos: {
        h: 1,
        w: 24,
        x: 0,
        y: 0,
      },
    }] else [],
    addPanel(panel):: self {
      _panels+: [panel],
    },
    panels+: self._panels,
    collapsed: collapsed,
    title: name,
    type: 'row',
  },

  // init a new table pannel with default format
  newTablePanel(title, h=6)::
    { type: 'table', title: title }
    + $.setPannelSize(h),

  // init a new time series pannel with default format
  newTimeSeriesPanel(title, unit, decimals=2, repeat='', stacking=false, highLightLegend='', cals=['max', 'mean'], sort='Max', h=6, fillOpacity=5, lineWidth=3, lineInterpolation='')::
    g.timeseriesPanel(title)
    + $.addLegends(cals, sort)
    + $.setLineWidth(fillOpacity, lineWidth)
    + $.setUnit(unit, decimals)
    + $.setPannelSize(h)
    + $.setColorScheme()
    + (if lineInterpolation == '' then {} else $.setLineInterpolation(lineInterpolation))
    + (if stacking == true then $.setStacking() else {})
    + (if repeat == '' then {} else $.setRepeat(repeat))
    + (if highLightLegend != '' then $.setDashLine(highLightLegend) else {}),


  // init a new time state pannel with default format
  newTimeStatePanel(title, valueMapping='')::
    g.timeseriesPanel(title)
    + { type: 'state-timeline' }
    + $.addLegends()
    + $.setLineWidth(fillOpacity=100, lineWidth=0)
    + $.setUnit('none')
    + $.setPannelSize()
    + $.setColorScheme()
    + if valueMapping == '' then $.disableValueShow() else $.setValueMapping(valueMapping),

  // init a new state pannel with default format
  newStatePanel(title)::
    g.timeseriesPanel(title)
    + { type: 'stat' }
    + $.setUnit('short', 'auto')
    + $.setPannelSize(9, 24)
    + $.statDisplay()
    + $.setPercent('Utilized Percent')
    + $.setColorScheme(color='green'),

  newLogPanel(title, repeat='')::
    g.timeseriesPanel(title)
    + { type: 'logs' }
    + {
      options: {
        showTime: true,
        sortOrder: 'Descending',
        wrapLogMessage: true,
        dedupStrategy: 'none',
        enableLogDetails: false,
        prettifyLogMessage: false,
        showCommonLabels: false,
        showLabels: false,
      },
    }
    + $.setPannelSize(15)
    + if repeat == '' then {} else $.setRepeat(repeat),

  // init a query target
  queryPanel(queries, legends, legendLink=null):: g.queryPanel(queries, legends, legendLink),

  // init a query target for a table
  queryTablePanel(query):: {
    targets+: [
      {
        expr: query,
        format: 'table',
        instant: true,
        legendFormat: '',
      },
    ],
  },

  // add description
  addDescription(des):: {
    description: des,
  },

  // add legends
  addLegends(cals=['max', 'mean'], sort='Max'):: {
    options+: {
      legend+: {
        calcs: cals,
        sortBy: sort,
        sortDesc: true,
      },
    } + $.legendOnRight().options,
  },

  // add transformation
  addTransformations(t):: {
    transformations: t,
  },

  // add overrides
  addOverrides(ovrds):: {
    fieldConfig+: {
      overrides+: ovrds,
    },
  },

  // stat display style
  statDisplay():: {
    options+: {
      graphMode: 'none',
      justifyMode: 'center',
    },
  },

  // display legends on right
  legendOnRight():: {
    options+: {
      legend+: {
        displayMode: 'table',
        placement: 'right',
      },
      tooltip: {
        mode: 'multi',
        sort: 'desc',
      },
    },
  },

  // set line width
  setLineWidth(fillOpacity=5, lineWidth=3):: {
    fieldConfig+: {
      defaults+: {
        custom+: {
          fillOpacity: fillOpacity,
          lineWidth: lineWidth,
        },
      },
    },
  },

  // set line width
  setSpanNulls(spanNulls=3600000):: {
    fieldConfig+: {
      defaults+: {
        custom+: {
          spanNulls: spanNulls,
        },
      },
    },
  },

  // set default color
  setColorScheme(color=''):: {
    fieldConfig+: {
      defaults+: {
        color: {
          mode: 'palette-classic',
        } + if color != '' then {
          fixedColor: color,
          mode: 'fixed',
        } else {},
      },
    },
  },

  // set percent override
  setPercent(name=''):: {
    fieldConfig+: {
      overrides: [
        {
          matcher: {
            id: 'byName',
            options: name,
          },
          properties: [
            {
              id: 'unit',
              value: 'percent',
            },
          ],
        },
      ],
    },
  },

  // set unit
  setUnit(unit, decimals=2):: {
    local min = if unit == 'percent' then { min: 0 } else {},
    fieldConfig+: {
      defaults+: {
        unit: unit,
        decimals: decimals,
      } + min,
    },
  },

  // set repeat option
  setRepeat(var):: {
    repeat: var,
  },

  // set datasource override
  setDataSource(ds):: {
    datasource: ds,
  },

  // use dash line for highlight legend. this is useful to display node capacity/pod limits/...
  // also set to stack view since it's meant to compare with the highlight legend
  setDashLine(regex):: {
    fieldConfig+: {
      overrides+: [
        {
          matcher: {
            id: 'byRegexp',
            options: regex,
          },
          properties: [
            {
              id: 'color',
              value: {
                fixedColor: 'red',
                mode: 'fixed',
              },
            },
            {
              id: 'custom.lineStyle',
              value: {
                dash: [
                  10,
                  10,
                ],
                fill: 'dash',
              },
            },
            {
              id: 'custom.stacking',
              value: {
                group: 'A',
                mode: 'none',
              },
            },
          ],
        },
      ],
    },
  },

  // set stacking
  setStacking():: {
    fieldConfig+: {
      defaults+: {
        min: 0,
        custom+: {
          stacking+: {
            group: 'A',
            mode: 'normal',
          },
        },
      },
    },
  },

  // set line interpolation
  setLineInterpolation(lineInterpolation='linear'):: {
    fieldConfig+: {
      defaults+: {
        custom+: {
          lineInterpolation: lineInterpolation,
        },
      },
    },
  },

  // set pannel size
  setPannelSize(h=6, w=24):: {
    gridPos: {
      h: h,
      w: w,
    },
  },

  // set value mapping
  setValueMapping(mapping):: {
    fieldConfig+: {
      defaults+: {
        mappings+: mapping,
      },
    },
  },

  // set default no value text
  setNoValue(text):: {
    fieldConfig+: {
      defaults+: {
        noValue: text,
      },
    },
  },

  // disable value show in case of no vale mapping to display
  disableValueShow():: {
    options+: {
      showValue: 'never',
      legend: {
        displayMode: 'hidden',
      },
    },
  },

  // error value mapping
  errorValMapping: [
    {
      options: {
        '1': {
          text: 'Error',
        },
      },
      type: 'value',
    },
  ],

  // system port value mapping
  systemNetworkMapping: [
    {
      options: {
        '1': {
          color: 'orange',
          index: 0,
          text: 'Flapping',
        },
      },
      type: 'value',
    },
    {
      options: {
        from: 2,
        result: {
          color: 'red',
          index: 1,
          text: 'Error',
        },
        to: 3,
      },
      type: 'range',
    },
  ],

  // system status value mapping
  systemStatusMapping: [
    {
      options: {
        '0': {
          color: '#bf1aef',
          index: 2,
          text: 'UNKNOWN',
        },
        '1': {
          color: 'green',
          index: 3,
          text: 'OK',
        },
        '2': {
          color: 'dark-yellow',
          index: 4,
          text: 'DEGRADED',
        },
        '3': {
          color: 'red',
          index: 5,
          text: 'CRITICAL',
        },
        '4': {
          color: '#18dbd680',
          index: 6,
          text: 'CRITICAL_EMERGENCY_SHUTDOWN',
        },
        '5': {
          color: 'super-light-blue',
          index: 7,
          text: 'STANDBY',
        },
        '6': {
          color: '#0d7cea',
          index: 8,
          text: 'PENDING_STANDBY',
        },
        '7': {
          color: '#12e280',
          index: 9,
          text: 'PENDING_ACTIVE',
        },
        '8': {
          color: '#dbaa12',
          index: 10,
          text: 'MISSING',
        },
        '9': {
          color: '#410be3',
          index: 11,
          text: 'NO_POWER',
        },
        '10': {
          color: '#f1750a',
          index: 12,
          text: 'HARDWARE_ERROR',
        },
        '11': {
          color: 'super-light-orange',
          index: 13,
          text: 'DISABLED',
        },
        '12': {
          color: 'super-light-red',
          index: 14,
          text: 'UNAVAILABLE',
        },
        '13': {
          color: '#be2b2b',
          index: 15,
          text: 'UPDATING_SW',
        },
        '14': {
          color: 'purple',
          index: 16,
          text: 'PUMP_BLEEDING',
        },
        '15': {
          color: 'blue',
          index: 17,
          text: 'SERVICES_NOT_READY',
        },
        '16': {
          color: 'light-blue',
          index: 18,
          text: 'FPGA_UPDATE',
        },
        '17': {
          color: '#2eddcf',
          index: 19,
          text: 'PUMP_LIMPING',
        },
        '18': {
          color: '#8ced0e',
          index: 20,
          text: 'DOOR_OPEN',
        },
        '19': {
          color: '#0ecef8',
          index: 21,
          text: 'DOOR_CLOSED',
        },
        '-1': {
          color: '#11469a',
          index: 1,
          text: 'ADMIN_SVC_UNKNOWN',
        },
        '-2': {
          color: '#ed0a0a',
          index: 0,
          text: 'SYSTEM_NOT_REACHABLE',
        },
      },
      type: 'value',
    },
  ],
  jobStatusMapping: [
    {
      options: {
        '1': {
          color: 'super-light-green',
          index: 0,
          text: 'CKPT_IN_PROGRESS',
        },
        '2': {
          color: 'yellow',
          index: 1,
          text: 'IDLE',
        },
        '3': {
          color: 'red',
          index: 2,
          text: 'INPUT_STARVATION',
        },
        '4': {
          color: 'red',
          index: 3,
          text: 'DEADLOCKED',
        },
        '5': {
          color: 'dark-green',
          index: 4,
          text: 'COMPLETED',
        },
        '6': {
          color: 'dark-green',
          index: 5,
          text: 'TERMINATED',
        },
        '7': {
          color: 'green',
          index: 6,
          text: 'PROGRESSING',
        },
      },
      type: 'value',
    },
  ],
  jobStatusTransformations: [
    {
      id: 'extractFields',
      options: {
        source: 'labels',
      },
    },
    {
      id: 'organize',
      options: {
        excludeByName: {
          Line: true,
          id: true,
          iteration: true,
          labels: true,
          metric: true,
          metric_extracted: true,
          namespace: true,
          namespace_extracted: true,
          replica_id: true,
          replica_id_extracted: true,
          replica_type: true,
          replica_type_extracted: true,
          tsNs: true,
          wsjob: true,
          wsjob_extracted: true,
        },
        indexByName: {},
        renameByName: {
          labels: '',
        },
      },
    },
  ],
  deadlockStatusTransformations: [
    {
      id: 'extractFields',
      options: {
        source: 'Line',
      },
    },
    {
      id: 'concatenate',
      options: {},
    },
    {
      id: 'filterFieldsByName',
      options: {
        include: {
          pattern: '(fault)|(summary)|(link)',
        },
      },
    },
    {
      id: 'organize',
      options: {
        excludeByName: {},
        includeByName: {},
        indexByName: {
          fault: 0,
          link: 2,
          summary: 1,
        },
        renameByName: {
          fault: 'Fault',
          link: 'Debug Viz Link',
          summary: 'Summary',
        },
      },
    },
  ],
  deadlockStatusOverrides: [
    {
      matcher: {
        id: 'byName',
        options: 'Fault',
      },
      properties: [
        {
          id: 'custom.width',
          value: 112,
        },
      ],
    },
    {
      matcher: {
        id: 'byName',
        options: 'Summary',
      },
      properties: [
        {
          id: 'custom.width',
          value: 861,
        },
      ],
    },
    {
      matcher: {
        id: 'byName',
        options: 'Debug Viz Link',
      },
      properties: [
        {
          id: 'custom.width',
          value: 383,
        },
        {
          id: 'links',
          value: [
            {
              targetBlank: true,
              title: 'Debug Viz: ${__data.fields["Debug Viz Link"]}',
              url: '${__data.fields["Debug Viz Link"]}',
            },
          ],
        },
      ],
    },
  ],
}

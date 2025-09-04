{
  /**
   * Creates a [template](https://grafana.com/docs/grafana/latest/variables/#templates) that can be added to a dashboard.
   *
   * @name template.new
   *
   * @param name Name of variable.
   * @param datasource Template [datasource](https://grafana.com/docs/grafana/latest/variables/variable-types/add-data-source-variable/)
   * @param query [Query expression](https://grafana.com/docs/grafana/latest/variables/variable-types/add-query-variable/) for the datasource.
   * @param label (optional) Display name of the variable dropdown. If null, then the dropdown label will be the variable name.
   * @param allValues (optional) Formatting for [multi-value variables](https://grafana.com/docs/grafana/latest/variables/formatting-multi-value-variables/#formatting-multi-value-variables)
   * @param tagValuesQuery (default `''`) Group values into [selectable tags](https://grafana.com/docs/grafana/latest/variables/variable-value-tags/)
   * @param current (default `null`) Can be `null`, `'all'` for all, or any other custom text value.
   * @param hide (default `''`) `''`: the variable dropdown displays the variable Name or Label value. `'label'`: the variable dropdown only displays the selected variable value and a down arrow. Any other value: no variable dropdown is displayed on the dashboard.
   * @param regex (default `''`) Regex expression to filter or capture specific parts of the names returned by your data source query. To see examples, refer to [Filter variables with regex](https://grafana.com/docs/grafana/latest/variables/filter-variables-with-regex/).
   * @param refresh (default `'never'`) `'never'`: variables queries are cached and values are not updated. This is fine if the values never change, but problematic if they are dynamic and change a lot. `'load'`: Queries the data source every time the dashboard loads. This slows down dashboard loading, because the variable query needs to be completed before dashboard can be initialized. `'time'`: Queries the data source when the dashboard time range changes. Only use this option if your variable options query contains a time range filter or is dependent on the dashboard time range.
   * @param includeAll (default `false`) Whether all value option is available or not.
   * @param multi (default `false`) Whether multiple values can be selected or not from variable value list.
   * @param sort (default `0`) `0`: Without Sort, `1`: Alphabetical (asc), `2`: Alphabetical (desc), `3`: Numerical (asc), `4`: Numerical (desc).
   *
   * @return A [template](https://grafana.com/docs/grafana/latest/variables/#templates)
   */
  new(
    name,
    datasource='$datasource',
    query,
    label=null,
    allValues='.*',
    tagValuesQuery='',
    current=null,
    hide='',
    regex='',
    refresh='time',
    includeAll=false,
    multi=false,
    sort=1,
  )::
    {
      allValue: allValues,
      current: $.current(current),
      datasource: datasource,
      includeAll: includeAll,
      hide: $.hide(hide),
      label: label,
      multi: multi,
      name: name,
      options: [],
      query: query,
      refresh: $.refresh(refresh),
      regex: regex,
      sort: sort,
      tagValuesQuery: tagValuesQuery,
      tags: [],
      tagsQuery: '',
      type: 'query',
      useTags: false,
    },

  // default to include all+allow multi
  newMulti(
    name,
    datasource='$datasource',
    query,
    label=null,
    allValues='.*',
    tagValuesQuery='',
    current=null,
    hide='',
    regex='',
    refresh='time',
    includeAll=true,
    multi=true,
    sort=1,
  ):: $.new(name,
            datasource,
            query,
            label,
            allValues,
            tagValuesQuery,
            current,
            hide,
            regex,
            refresh,
            includeAll,
            multi,
            sort),

  /**
   * Data [source variables](https://grafana.com/docs/grafana/latest/variables/variable-types/add-data-source-variable/)
   * allow you to quickly change the data source for an entire dashboard.
   * They are useful if you have multiple instances of a data source, perhaps in different environments.
   *
   * @name template.datasource
   *
   * @param name Data source variable name. Ex: `'PROMETHEUS_DS'`.
   * @param query Type of data source. Ex: `'prometheus'`.
   * @param current Ex: `'Prometheus'`.
   * @param hide (default `''`) `''`: the variable dropdown displays the variable Name or Label value. `'label'`: the variable dropdown only displays the selected variable value and a down arrow. Any other value: no variable dropdown is displayed on the dashboard.
   * @param refresh (default `'load'`) `'never'`: Variables queries are cached and values are not updated. This is fine if the values never change, but problematic if they are dynamic and change a lot. `'load'`: Queries the data source every time the dashboard loads. This slows down dashboard loading, because the variable query needs to be completed before dashboard can be initialized. `'time'`: Queries the data source when the dashboard time range changes. Only use this option if your variable options query contains a time range filter or is dependent on the dashboard time range.
   *
   * @return A [data source variable](https://grafana.com/docs/grafana/latest/variables/variable-types/add-data-source-variable/).
   */
  datasource(
    name='datasource',
    current='default',
    hide='all',
    refresh='load',
  ):: {
    current: {
      text: current,
      value: current,
    },
    hide: $.hide(hide),
    name: name,
    query: 'prometheus',
    refresh: $.refresh(refresh),
    type: 'datasource',
    label: 'Data Source',
  },

  refresh(refresh)::
    if refresh == 'never'
    then 0
    else if refresh == 'load'
    then 1
    else if refresh == 'time'
    then 2
    else refresh,

  hide(hide)::
    if hide == '' then 0 else if hide == 'label' then 1 else 2,

  current(current):: {
    [if current != null then 'text']: current,
    [if current != null then 'value']: if current == 'auto' then
      '$__auto_interval'
    else if current == 'all' then
      '$__all'
    else
      current,
  },
}

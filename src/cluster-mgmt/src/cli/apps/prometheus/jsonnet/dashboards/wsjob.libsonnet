local conf = import '../config.libsonnet';
local query = import '../queries/query.libsonnet';
local dashboard = import 'dashboard.libsonnet';
local link = import 'link.libsonnet';
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
    local workflow =
      template.new(
        name='workflow',
        query='label_values(wsjob_annotation{namespace=~"$namespace"}, cerebras_workflow_id)',
        includeAll=true,
      ),
    local model =
      template.newMulti(
        name='model',
        query='label_values(wsjob_annotation, labels_k8s_cerebras_com_Model)',
      ),
    local job_type =
      template.new(
        name='job_type',
        query='label_values(wsjob_annotation, job_type)',
        includeAll=true,
      ),
    local wsjob =
      template.new(
        name='wsjob',
        query='label_values(wsjob_annotation{cerebras_workflow_id=~"$workflow", labels_k8s_cerebras_com_Model=~"$model", job_type=~"$job_type", namespace=~"$namespace", start!="", end=""}, name)',
      ),
    local replica_type =
      template.new(
        name='replica_type',
        query='label_values(kube_pod_info{created_by_name="$wsjob", pod=~"$wsjob-.*", pod_ip!=""}, pod)',
        regex='.*-(.*)-.*',
      ),
    local replica_id =
      template.newMulti(
        name='replica_id',
        query='label_values(kube_pod_info{created_by_name="$wsjob", pod=~"$wsjob-$replica_type.*", pod_ip!=""}, pod)',
        regex='$wsjob-$replica_type-(.*)',
        current='0',
        includeAll=false,
        sort=3,
      ),
    local utc_start =
      template.new(
        name='utc_start',
        query='label_values(wsjob_annotation{name="$wsjob"}, start)',
        refresh='load',
      ),
    local utc_end =
      template.new(
        name='utc_end',
        query='label_values(wsjob_annotation{name="$wsjob"}, end)',
        refresh='load',
      ),
    local system_count =
      template.new(
        name='system_count',
        query='label_values(wsjob_annotation{name="$wsjob"}, system_count)',
        refresh='load',
      ),
    local system_list =
      template.newMulti(
        name='system_list',
        query='label_values(wsjob_system_grant{name="$wsjob"}, system)',
        refresh='load',
        includeAll=false,
      ),
    local system_metrics_category =
      template.new(
        name='system_metrics_category',
        query='label_values(system_dev_stats_labels, category)',
        refresh='load',
      ),
    local system_metrics_type =
      template.new(
        name='system_metrics_type',
        query='label_values(system_dev_stats_labels{category="$system_metrics_category"}, type)',
        refresh='load',
      ),
    local system_metrics_component =
      template.newMulti(
        name='system_metrics_component',
        query='label_values(system_dev_stats_labels{category="$system_metrics_category", type="$system_metrics_type"}, component)',
        refresh='load',
        includeAll=false,
      ),
    local system_wafter_die =
      template.newMulti(
        name='system_wafter_die',
        query='label_values(system_dev_stats_labels{system=~"$system_list", component=~"GLTCH_EXCUR_.*"}, component)',
        refresh='load',
        regex='.*_(..)',
      ),

    'wsjob-dashboard.json':
      dashboard.newDashboard(
        'WsJob Dashboard',
        uid=conf.grafanaDashboardIDs['wsjob-dashboard.json'],
      )
      + {
        links+: [
          link.dashboards(title='WsJob Resources Dashboards', tags=['wsjob-resources'], keepTime=true, includeVars=true, asDropdown=false),
        ],
      }
      + {
        panels+: [
          pannel.newRow('overview', collapsed=true)
          .addPanel(
            pannel.newTimeSeriesPanel('Memory Usage (WSS)', unit='bytes')
            + pannel.queryPanel('sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", pod=~"$wsjob-.*", container!="", image!=""}) by (pod, node)', '{{node}}/{{pod}}')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Memory Usage (Pool allocator)', unit='megabytes')
            + pannel.queryPanel('max(max_over_time({metric="rt_iter_perf", wsjob="$wsjob"} |= `` | json | unwrap pa [$__interval])) by (replica_type, replica_id)', '{{replica_type}}-{{replica_id}}')
            + pannel.setSpanNulls(3600000)
            + pannel.setDataSource('Loki')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Peak Memory Usage (Pool allocator)', unit='megabytes')
            + pannel.queryPanel('max(max_over_time({metric="rt_iter_perf", wsjob="$wsjob"} |= `` | json | unwrap ppa [$__interval])) by (replica_type, replica_id)', '{{replica_type}}-{{replica_id}}')
            + pannel.setSpanNulls(3600000)
            + pannel.setDataSource('Loki')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('CPU Usage', unit='short')
            + pannel.queryPanel('sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{pod=~"$wsjob-.*", container=~".+"}) by (pod, node)', '{{node}}/{{pod}}')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Egress Bandwidth', unit='Bps')
            + pannel.queryPanel('sum(irate(container_network_transmit_bytes_total{pod=~"$wsjob-.*", interface=~"e.*|net.*"}[$__rate_interval])) by (pod, interface)', '{{pod}}/{{interface}}')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Ingress Bandwidth', unit='Bps')
            + pannel.queryPanel('sum(irate(container_network_receive_bytes_total{pod=~"$wsjob-.*", interface=~"e.*|net.*"}[$__rate_interval])) by (pod, interface)', '{{pod}}/{{interface}}')
          ),
          pannel.newRow('errors and debugging', collapsed=true)
          .addPanel(
            pannel.newTimeStatePanel('Failed Pods Errors')
            + pannel.addDescription('Display info about failed pod errors. No data means no error.')
            + pannel.queryPanel(query.pod_errors.expr % conf._config, query.pod_errors.legend)
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Systems Internal Errors', valueMapping=pannel.systemStatusMapping)
            + pannel.addDescription('Display info about whether assigned Systems had internal errors. No data means no error.')
            + pannel.queryPanel('max(max_over_time(system_overall_health[$__rate_interval]) != 1) by (system) * on (system) group_left group(wsjob_system_grant{name="$wsjob"}) by (name, system)', '{{system}}')
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Systems Network Errors', valueMapping=pannel.systemNetworkMapping)
            + pannel.addDescription('Display info about whether assigned Systems had network errors. No data means no error.')
            + pannel.queryPanel('max(((increase(system_network_flap_count[$__rate_interval]) >= bool 2) + 2 * (max_over_time(system_network_health[$__rate_interval]) != bool 1)) != 0) by (system) * on (system) group_left group(wsjob_system_grant{name="$wsjob"}) by (name, system)', '{{system}}')
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Systems - Network Error Counters - Fatal')
            + pannel.addDescription('Network Counters that indicate job-fatal networking problems, increase over time. No data means no errors.')
            + pannel.queryPanel('(increase(system_network_err_qps[$__rate_interval]) > 0) * on (system) group_left group(wsjob_system_grant{name=~"$wsjob"}) by (name, system)', '{{system}}/{{port}} QP error state counters')
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Systems - Network Error Counters - Debug')
            + pannel.addDescription('Network Counters that may indicate the root cause of networking problems or performance issues, increase over time. No data means no errors. If the job succeeded, no action is needed for these.')
            + pannel.queryPanel('(increase(system_network_rx_pkt_bad_fcs[$__rate_interval]) > 0) * on (system) group_left group(wsjob_system_grant{name=~"$wsjob"}) by (name, system)', '{{system}}/{{port}} CRC errors')
            + pannel.queryPanel('(increase(system_network_ack_timeouts[$__rate_interval]) > 0) * on (system) group_left group(wsjob_system_grant{name=~"$wsjob"}) by (name, system)', '{{system}}/{{port}} Local ACK Timeouts')
            + pannel.queryPanel('(increase(system_network_rx_fec_uncorrected_pkts[$__rate_interval]) > 0) * on (system) group_left group(wsjob_system_grant{name=~"$wsjob"}) by (name, system)', '{{system}}/{{port}} FEC Uncorrected Errors')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Assigned Systems - Glitch Counter Errors On Selected Die', unit='short')
            + pannel.queryPanel('system_dev_stats{system=~"$system_list", component=~"^GLTCH_(EXCUR|TRANS).*($system_wafter_die)"} * on (system) group_left wsjob_system_grant{name="$wsjob"} > 0', '{{system}}/{{component}}')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Assigned Systems - VDDC_IOUT On Selected Die', unit='Amperes')
            + pannel.queryPanel('system_dev_stats{system=~"$system_list", component=~"^VDDC.*($system_wafter_die)_IOUT"} * on (system) group_left wsjob_system_grant{name="$wsjob"}', '{{system}}/{{component}}')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Assigned Systems - VDDC_ASIC On Selected Die', unit='Volts')
            + pannel.queryPanel('system_dev_stats{system=~"$system_list", component=~"^VDDC_ASIC.*($system_wafter_die).*"} * on (system) group_left wsjob_system_grant{name="$wsjob"}', '{{system}}/{{component}}')
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Systems Warning/Error Events')
            + pannel.addDescription('Display info about whether assigned Systems had warning/error events. No data means no warning/error.')
            + pannel.queryPanel('(group by (system, severity, type, component) (system_events{severity=~"CRITICAL|ERROR|WARN"})) * on (system) group_left group(wsjob_system_grant{name="$wsjob"}) by (name, system)', '{{system}}/{{severity}}/{{type}}/{{component}}')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Assigned Systems Internal Metrics', unit='short')
            + pannel.addDescription('System dev metrics for debuging purpose. System name, metric category/type can be changed from the top menu.')
            + pannel.queryPanel('system_dev_stats{system=~"$system_list", category="$system_metrics_category", type="$system_metrics_type", component=~"$system_metrics_component"}', '{{system}}/{{category}}/{{type}}/{{component}}')
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Nodes Internal Errors')
            + pannel.addDescription('Display info about whether assigned nodes had internal errors. No data means no error.')
            + pannel.queryPanel('(max_over_time(kube_node_status_condition{condition=~"ClusterMgmt.*|ClusterDeploy.*|Csadm.*", status="true"}[$__rate_interval]) > 0) * on (node) group_left group by(node) (kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""})', '{{node}}/{{condition}}')
            + pannel.queryPanel('((min_over_time(node_health_overall[$__rate_interval]) == bool 0) > 0) * on (node) group_left group by(node) (kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""})', '{{node}}/IPMIUnhealthy')
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Nodes Network Errors')
            + pannel.addDescription('Display info about whether assigned nodes had network errors. No data means no error.')
            + pannel.queryPanel('((increase(interface_watch_list:status_changes_total[$__rate_interval]) >= bool 2) > 0) * on (instance) group_left label_replace(group(kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""}) by (node, host_ip), "instance", "$1:9100", "host_ip", "(.*)")', '{{node}}/{{device}} flapping')
            + pannel.queryPanel(
              '((min_over_time(interface_watch_list:status[$__rate_interval]) == bool 0) > 0) * on (instance) group_left group by(node, instance) (label_replace(kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""}, "instance", "$1:9100", "host_ip", "(.*)"))', '{{node}}/{{device}} down'
            )
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Nodes - Network Error Counters - Fatal')
            + pannel.addDescription('Network Counters that indicate job-fatal networking problems, increase over time. No data means no errors.')
            + pannel.queryPanel('(increase(node_nic_stats_resp_cqe_error[$__rate_interval]) > 0) * on (node) group_left group by(node) (kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""})', '{{node}}/{{interface}} RX CQE Errors')
            + pannel.queryPanel('(increase(node_nic_stats_req_cqe_error[$__rate_interval]) > 0) * on (node) group_left group by(node) (kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""})', '{{node}}/{{interface}} TX CQE Errors')
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Nodes - Network Error Counters - Warning')
            + pannel.addDescription('Network Counters that indicate networking congestion or errors, or problems with nodes, increase over time. No data means no errors. If errors occur, notify the cluster operator to check for defective links.')
            + pannel.queryPanel('(increase(node_nic_stats_local_ack_timeout_err[$__rate_interval]) > 0) * on (node) group_left group by(node) (kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""})', '{{node}}/{{interface}} Local ACK Timeouts')
          )
          .addPanel(
            pannel.newTimeStatePanel('Assigned Nodes - Network Error Counters - Debug')
            + pannel.addDescription('Network Counters that may indicate the root cause of networking problems or performance issues, increase over time. No data means no errors. If the job succeeded, no action is needed for these.')
            + pannel.queryPanel('(increase(node_nic_stats_rx_crc_errors_phy[$__rate_interval]) > 0) * on (node) group_left group by(node) (kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""})', '{{node}}/{{interface}} RX CRC Errors')
            + pannel.queryPanel('(increase(node_nic_stats_rx_pcs_symbol_err_phy[$__rate_interval]) > 0) * on (node) group_left group by(node) (kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""})', '{{node}}/{{interface}} RX FEC Uncorrected Errors')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('Assigned Nodes - Network PFC Pause Duration Increase', unit='µs')
            + pannel.addDescription('Network congestion. If the job succeeded, no action is needed for these.')
            + { fieldConfig: { defaults: { custom: { thresholdsStyle: { mode: 'dashed' } }, thresholds: { mode: 'absolute', steps: [{ color: 'red', value: null }, { color: 'red', value: 600000 }] } } } }
            + { options: { tooltip: { mode: 'multi', sort: 'desc' }, legend: { showLegend: false, displayMode: 'list', placement: 'bottom', calcs: [] } } }
            + pannel.queryPanel('(increase(node_nic_stats_rx_pfc_duration[$__rate_interval]) > 0) * on (node) group_left group by(node) (kube_pod_info{pod=~"$wsjob-.*", pod_ip!=""})', '{{node}}/{{interface}} PFC Pause Duration increase')
          )
          .addPanel(
            pannel.newTimeStatePanel('Training Status', valueMapping=pannel.jobStatusMapping)
            + pannel.addDescription('Job status from coordinator logs. Possible states: UNKNOWN, CKPT_IN_PROGRESS, IDLE, INPUT_STARVATION, DEADLOCKED, COMPLETED, TERMINATED, PROGRESSING.')
            + pannel.queryPanel('{wsjob="$wsjob", metric="stall_detect"} | json status', 'state')
            + pannel.setDataSource('Loki')
            + pannel.addTransformations(pannel.jobStatusTransformations)
          )
          .addPanel(
            pannel.newTablePanel('Deadlock Status', h=4)
            + pannel.addDescription('Summary of a deadlock, if detected.')
            + pannel.setDataSource('Loki')
            + pannel.queryTablePanel('{wsjob=~"$wsjob", metric="deadlock"}')
            + pannel.queryTablePanel('{wsjob=~"$wsjob", metric="debugviz"}')
            + pannel.addTransformations(pannel.deadlockStatusTransformations)
            + pannel.addOverrides(pannel.deadlockStatusOverrides)
            + pannel.setNoValue('Deadlock not detected')
          ),
          pannel.newRow('$replica_type type view', collapsed=true)
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id Memory Usage', unit='bytes', repeat='replica_id', highLightLegend='request|limit')
            + pannel.queryPanel('sum(container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", pod=~"$wsjob-$replica_type-$replica_id", container!="", image!=""})', 'usage (RSS)')
            + pannel.queryPanel('sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", pod=~"$wsjob-$replica_type-$replica_id", container!="", image!=""})', 'usage (WSS)')
            + pannel.queryPanel('max(kube_pod_container_resource_limits{job="kube-state-metrics", pod=~"$wsjob-$replica_type-$replica_id", resource="memory"})', 'limit')
            + pannel.queryPanel('max(kube_pod_container_resource_requests{job="kube-state-metrics", pod=~"$wsjob-$replica_type-$replica_id", resource="memory"})', 'request')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id CPU Usage (Cores)', unit='short', repeat='replica_id', highLightLegend='request|limit')
            + pannel.queryPanel('sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{pod=~"$wsjob-$replica_type-$replica_id"})', 'usage')
            + pannel.queryPanel('max(kube_pod_container_resource_limits{job="kube-state-metrics", pod=~"$wsjob-$replica_type-$replica_id", resource="cpu"})', 'limit')
            + pannel.queryPanel('max(kube_pod_container_resource_requests{job="kube-state-metrics", pod=~"$wsjob-$replica_type-$replica_id", resource="cpu"})', 'request')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id Network Usage', unit='Bps', repeat='replica_id')
            + pannel.queryPanel('sum(irate(container_network_transmit_bytes_total{pod=~"$wsjob-$replica_type-$replica_id", interface=~"e.*|net.*"}[$__rate_interval])) by (pod, interface)', '{{interface}} egress')
            + pannel.queryPanel('sum(irate(container_network_receive_bytes_total{pod=~"$wsjob-$replica_type-$replica_id", interface=~"e.*|net.*"}[$__rate_interval])) by (pod, interface)', '{{interface}} ingress')
          ),
          pannel.newRow('$replica_type assigned nodes', collapsed=true)
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id node Memory Usage', unit='bytes', repeat='replica_id', highLightLegend='.*total.*', stacking=true)
            + pannel.queryPanel('max(kube_node_status_capacity{resource="memory"} * on (node) group_left group by (node) (kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""})) by (node)', '{{node}} total capactiy')
            + pannel.queryPanel('max(label_replace(node_memory_Cached_bytes,"host_ip", "$1", "instance", "([^:]+):.*") * on (host_ip) group_left(node) (kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""})) by (node)', '{{node}} cache used')
            + pannel.queryPanel('sum(node_namespace_pod_container:container_memory_cache{container!=""} * on (node) group_left group by (node) (kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""})) by (node)', '{{node}} total container cache')
            + pannel.queryPanel('sum(node_namespace_pod_container:container_memory_working_set_bytes{container!=""} * on (node) group_left group by (node) (kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""})) by (pod, node)', '{{pod}}')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id node CPU Usage', unit='short', repeat='replica_id', highLightLegend='.*total.*', stacking=true)
            + pannel.queryPanel('max(kube_node_status_capacity{resource="cpu"} * on (node) group_left group by (node) (kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""})) by (node)', '{{node}} total capacity')
            + pannel.queryPanel('sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate * on (node) group_left group by (node) (kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""})) by (pod, node)', '{{pod}}')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id node Network Usage', unit='Bps', repeat='replica_id')
            + pannel.queryPanel('rate(node_network_transmit_bytes_total{device=~"e.*"}[$__rate_interval]) * on (instance) group_left (node) label_replace(group by (node, host_ip) (kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""}), "instance", "$1:9100", "host_ip", "(.*)")', '{{node}}/{{device}} egress')
            + pannel.queryPanel('rate(node_network_receive_bytes_total{device=~"e.*"}[$__rate_interval]) * on (instance) group_left (node) label_replace(group by (node, host_ip) (kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""}), "instance", "$1:9100", "host_ip", "(.*)")', '{{node}}/{{device}} ingress')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id node Disk Usage', unit='percent', repeat='replica_id')
            + pannel.queryPanel('max(label_replace((((node_filesystem_size_bytes{mountpoint="/n0"}-node_filesystem_avail_bytes{mountpoint="/n0"})/node_filesystem_size_bytes{mountpoint="/n0"})*100), "host_ip", "$1", "instance", "([^:]+):.*") * on (host_ip) group_right kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""}) by (node)', '{{node}}:/n0')
            + pannel.queryPanel('max(label_replace((((node_filesystem_size_bytes{mountpoint="/"}-node_filesystem_avail_bytes{mountpoint="/"})/node_filesystem_size_bytes{mountpoint="/"})*100), "host_ip", "$1", "instance", "([^:]+):.*") * on (host_ip) group_right kube_pod_info{pod=~"$wsjob-$replica_type-$replica_id", pod_ip!=""}) by (node)', '{{node}}:/')
          ),
          pannel.newRow('$replica_type iteration performance', collapsed=true)
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id iteration performance', unit='µs', repeat='replica_id')
            + pannel.queryPanel('max(max_over_time({metric="rt_iter_perf", replica_type="$replica_type", replica_id="$replica_id", wsjob="$wsjob"} |= `` | json | unwrap it [$__interval]))', 'iteration time')
            + pannel.queryPanel('max(max_over_time({metric="rt_iter_perf", replica_type="$replica_type", replica_id="$replica_id", wsjob="$wsjob"} |= `` | json | unwrap ct [$__interval]))', 'cross-iteration time')
            + pannel.queryPanel('max(max_over_time({metric="rt_iter_perf", replica_type="$replica_type", replica_id="$replica_id", wsjob="$wsjob"} |= `` | json | unwrap ft [$__interval]))', 'forward iteration time')
            + pannel.queryPanel('max(max_over_time({metric="rt_iter_perf", replica_type="$replica_type", replica_id="$replica_id", wsjob="$wsjob"} |= `` | json | unwrap bt [$__interval]))', 'backward iteration time')
            + pannel.setDataSource('Loki')
          )
          .addPanel(
            pannel.newTimeSeriesPanel('$replica_type-$replica_id input starvation', unit='µs', repeat='replica_id')
            + pannel.queryPanel('max by(metric) (max_over_time({metric="rt_iter_perf", replica_type="$replica_type", replica_id="$replica_id", wsjob="$wsjob"} |= `` | json | unwrap is [$__interval]))', 'input starvation')
            + pannel.setDataSource('Loki')
          ),
          pannel.newRow('$replica_type logs', collapsed=true)
          .addPanel(
            pannel.newLogPanel('$replica_type-$replica_id logs', repeat='replica_id')
            + pannel.queryPanel('{replica_type="$replica_type", replica_id="$replica_id", wsjob="$wsjob"} | json | line_format "{{.stream}} {{.message}}"', 'logs')
            + pannel.setDataSource('Loki')
          ),
        ],
      }
      + {
        templating+: {
          list: [
            ds,
            namespace,
            workflow,
            model,
            job_type,
            wsjob,
            replica_type,
            replica_id,
            utc_start,
            utc_end,
            system_count,
            system_list,
            system_metrics_category,
            system_metrics_type,
            system_metrics_component,
            system_wafter_die,
          ],
        },
      } + conf.globalConfig,
  },
}

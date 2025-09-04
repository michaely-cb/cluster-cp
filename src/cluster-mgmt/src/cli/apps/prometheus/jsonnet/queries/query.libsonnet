{
  pod_errors: {
    // add 5min offset, can be removed later after csctl links have 10min buffer
    expr: |||
      sum by (pod, reason)
      (
        kube_pod_container_status_terminated_reason{pod=~"%(wsjob)s-.*", reason!="Completed"} offset -5m
      ) > 0
    |||,
    legend: '{{pod}}/{{reason}}',
  },
}

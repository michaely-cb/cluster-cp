{
  mkDegraded(svc, tableQuery, ds):
    tableQuery.new(
      ds,
      'count(kube_pod_container_status_ready{namespace="' + svc.ns + '",pod=~"' + svc.regex + '"} == 0) > 0',
      svc.name
    ),

  mkDown(svc, tableQuery, ds):
    tableQuery.new(
      ds,
      'count(kube_pod_container_status_ready{namespace="' + svc.ns + '",pod=~"' + svc.regex + '"} == 1) < 1',
      svc.name
    ),

  mkRestarts(svc, tableQuery, ds):
    tableQuery.new(
      ds,
      'increase(kube_pod_container_status_restarts_total{namespace="' + svc.ns + '",pod=~"' + svc.regex + '"}[$__range]) > 0',
      svc.name
    ),
}

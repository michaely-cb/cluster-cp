# Dashboards using Grafonnet

This directory contains dashboard jsonnet code which is constructed using [Grafonnet library](https://grafana.github.io/grafonnet/index.html).

Each dashboard has its own folder. We will exact common code from different dashboards when
we migrate more dashboards to this model.

## Main constructs

Each dashboard has 4 main constructs:

* `main.libsonnet`: this glues all relevant pieces of dashboard together.

* `variables.libsonnet`: this defines the variables used in the dashboard.

* `queries.libsonnet`: this defines all queries used in the dashboard.

* `panels.libsonnet`: this defines all panels used in the dashboard.
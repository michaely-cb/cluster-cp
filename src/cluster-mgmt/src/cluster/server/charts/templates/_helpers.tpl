{{/*
Expand the name of the chart.
*/}}
{{- define "cluster-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "cluster-server.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "cluster-server.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cluster-server.labels" -}}
helm.sh/chart: {{ include "cluster-server.chart" . }}
{{ include "cluster-server.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if eq .Values.namespace .Values.system_namespace }}
system-namespace: ""
{{- else }}
user-namespace: {{ .Values.namespace }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cluster-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cluster-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "cluster-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "cluster-server.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- $isSystemNamespace := eq .Values.namespace .Values.system_namespace -}}
{{- $rbacPrefix := printf "%s-" .Values.namespace -}}

{{- define "cluster-server-viewer-cluster-role.rules" -}}
- apiGroups: [ "jobs.cerebras.com" ]
  resources: [ "namespacereservations", "systems" ]
  verbs: [ "list", "get", "watch" ]
- apiGroups: [ "jobs.cerebras.com" ]
  resources: [ "systems/status" ]
  verbs: [ "update", "patch"]
- apiGroups: [ "" ]
  resources: [ "nodes", "namespaces" ]
  verbs: [ "list", "get", "watch" ]
- apiGroups: [ "" ]
  resources: [ "configmaps" ]
  resourceNames:   [ "cluster", "job-operator-cluster-env" ]
  verbs: [ "get" ]
# The following permissions are only needed in Kafka integration
# TODO: Consider refactoring so this is less coupled
- apiGroups: [ "" ]
  resources: [ "endpoints", "secrets" ]
  verbs: [ "get" ]
{{- end -}}

{{- define "cluster-server-editor-cluster-role.rules" -}}
- apiGroups: [ "jobs.cerebras.com" ]
  resources: [ "namespacereservations" ]
  verbs: [ "create", "update", "patch", "delete" ]
- apiGroups: [ "jobs.cerebras.com" ]
  resources: [ "systems" ]
  verbs: [ "update", "patch"]
{{- end -}}

{{- define "cluster-server-editor-ns-role.rules" -}}
- apiGroups: [ "jobs.cerebras.com" ]
  resources: [ "wsjobs" ]
  verbs: [ "list", "get", "watch", "create", "update", "patch", "delete" ]
- apiGroups: [ "jobs.cerebras.com" ]
  resources: [ "wsjobs/status" ]
  verbs: [ "get" ]
- apiGroups: [ "jobs.cerebras.com" ]
  resources: [ "resourcelocks" ]
  verbs: [ "list", "get", "watch", "update", "patch" ]
- apiGroups: [ "networking.k8s.io" ]
  resources: [ "ingresses" ]
  verbs: [ "list", "get" ]
- apiGroups: [ "batch" ]
  resources: [ "jobs" ]
  verbs: [ "list", "get", "watch", "create", "delete" ]
- apiGroups: [ "" ]
  resources: [ "events", "endpoints" ]
  verbs: [ "list", "get", "watch" ]
- apiGroups: [ "" ]
  resources: [ "pods" ]
  verbs: [ "list", "get", "watch", "deletecollection" ]
- apiGroups: [ "" ]
  resources: [ "pods/log", "secrets" ]
  verbs: [ "get" ]
- apiGroups: [ "" ]
  resources: [ "configmaps" ]
  verbs: [ "list", "get", "watch", "create", "delete" ]
- apiGroups: [ "coordination.k8s.io" ]
  resources: [ "leases" ]
  verbs: [ "get", "create", "update", "patch" ]
- apiGroups: [ "" ]
  resources: [ "services" ]
  verbs: [ "list", "get", "watch", "create", "delete" ]
- apiGroups: [ "apps" ]
  resources: [ "deployments" ]
  verbs: [ "list", "get", "watch", "create", "update", "delete" ]
- apiGroups: [ "apps" ]
  resources: [ "deployments/scale" ]
  verbs: [ "get", "update" ]
{{- end -}}

{{- define "wsjob-viewer-role.rules" -}}
- apiGroups: [ "" ]
  resources: [ "configmaps" ]
  verbs: [ "list", "get", "watch" ]
{{- end -}}

{{- define "log-export-viewer-cluster-role.rules" -}}
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "list", "watch"]
{{- end -}}

{{- define "log-export-viewer-ns-role.rules" -}}
- apiGroups: [""]
  resources: ["configmaps", "pods", "pods/log", "services"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["jobs.cerebras.com"]
  resources: ["wsjobs"]
  verbs: ["get", "list", "watch"]
- apiGroups: [ "batch" ]
  resources: [ "jobs" ]
  verbs: [ "list", "get", "watch" ]
{{- end -}}



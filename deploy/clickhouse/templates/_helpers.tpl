{{/*
Expand the name of the chart.
*/}}
{{- define "clickhouse.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "clickhouse.fullname" -}}
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
Chart label helpers.
*/}}
{{- define "clickhouse.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "clickhouse.labels" -}}
helm.sh/chart: {{ include "clickhouse.chart" . }}
{{ include "clickhouse.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: clickhouse
{{- end }}

{{- define "clickhouse.selectorLabels" -}}
app.kubernetes.io/name: {{ include "clickhouse.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "clickhouse.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "clickhouse.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
ConfigMap names.
*/}}
{{- define "clickhouse.serverConfigMapName" -}}
{{- printf "%s-server" (include "clickhouse.fullname" .) }}
{{- end }}

{{- define "clickhouse.usersConfigMapName" -}}
{{- printf "%s-users" (include "clickhouse.fullname" .) }}
{{- end }}

{{/*
Architecture mode validation (cluster hook is reserved, not implemented).
*/}}
{{- define "clickhouse.architecture" -}}
{{- $mode := .Values.architecture.mode | default "standalone" }}
{{- if not (has $mode (list "standalone" "cluster")) }}
{{- fail (printf "architecture.mode must be standalone or cluster (got %q)" $mode) }}
{{- end }}
{{- if and (eq $mode "cluster") (not .Values.cluster.enabled) }}
{{- /* allow values.cluster.enabled to gate future templates */ -}}
{{- end }}
{{- $mode }}
{{- end }}

{{/*
True when auth.secret.existingSecret is set (password injected via env + from_env).
*/}}
{{- define "clickhouse.auth.secretEnabled" -}}
{{- if .Values.auth.secret.existingSecret }}true{{- end }}
{{- end }}

{{- define "clickhouse.auth.passwordEnvVar" -}}
{{- .Values.auth.secret.passwordEnvVar | default "CLICKHOUSE_DEFAULT_PASSWORD" }}
{{- end }}

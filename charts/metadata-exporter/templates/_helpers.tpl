{{/*
Expand the name of the chart.
*/}}
{{- define "metadata-exporter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "metadata-exporter.fullname" -}}
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
Chart label string.
*/}}
{{- define "metadata-exporter.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to all resources.
*/}}
{{- define "metadata-exporter.labels" -}}
helm.sh/chart: {{ include "metadata-exporter.chart" . }}
{{ include "metadata-exporter.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for Deployment and Service.
*/}}
{{- define "metadata-exporter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "metadata-exporter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Target namespace for namespaced resources.
*/}}
{{- define "metadata-exporter.namespace" -}}
{{- default .Release.Namespace .Values.namespace.name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "metadata-exporter.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "metadata-exporter.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Container image reference.
*/}}
{{- define "metadata-exporter.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Whether to grant Kubernetes discovery API access in RBAC.
*/}}
{{- define "metadata-exporter.rbacDiscovery" -}}
{{- or .Values.rbac.discovery (and .Values.config.discovery .Values.config.discovery.enabled) }}
{{- end }}

{{/*
Metrics port derived from args.metricsAddr (accepts ":8080" or "0.0.0.0:8080").
*/}}
{{- define "metadata-exporter.metricsPort" -}}
{{- regexReplaceAll "^.*:" (.Values.args.metricsAddr | toString) "" }}
{{- end }}

{{/*
Expand the name of the chart.
*/}}
{{- define "chalert.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "chalert.fullname" -}}
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
{{- define "chalert.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "chalert.labels" -}}
helm.sh/chart: {{ include "chalert.chart" . }}
{{ include "chalert.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "chalert.selectorLabels" -}}
app.kubernetes.io/name: {{ include "chalert.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "chalert.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "chalert.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Build the ClickHouse DSN from auth fields.
*/}}
{{- define "chalert.clickhouseDSN" -}}
{{- if .Values.config.auth.enabled -}}
  {{- $scheme := ternary "clickhouses" "clickhouse" .Values.config.auth.tls.enabled -}}
  {{- $userpass := .Values.config.auth.username -}}
  {{- if .Values.config.auth.password -}}
    {{- $userpass = printf "%s:%s" .Values.config.auth.username .Values.config.auth.password -}}
  {{- end -}}
  {{- $params := list -}}
  {{- if and .Values.config.auth.tls.enabled .Values.config.auth.tls.insecureSkipVerify -}}
    {{- $params = append $params "secure=true&skip_verify=true" -}}
  {{- end -}}
  {{- $query := join "&" $params -}}
  {{- if $query -}}
    {{- printf "%s://%s@%s/%s?%s" $scheme $userpass .Values.config.auth.endpoint .Values.config.clickhouse.database $query -}}
  {{- else -}}
    {{- printf "%s://%s@%s/%s" $scheme $userpass .Values.config.auth.endpoint .Values.config.clickhouse.database -}}
  {{- end -}}
{{- else -}}
  {{- .Values.config.clickhouse.dsn -}}
{{- end -}}
{{- end }}

{{/*
Extension hook for extra pod-template annotations. Default: empty.

Parent charts consuming chalert as a dependency may override this named
template (parent-chart definitions take precedence over subchart ones) to
inject computed annotations — most importantly a checksum of rule
ConfigMaps the parent renders itself and wires in via
`existingRuleConfigMaps`. The built-in `checksum/rules` annotation only
covers this chart's own configmap.yaml, so externally-rendered rule
changes do not roll the Deployment without such an override; chalert
loads rules once at startup and does not (yet) hot-reload them.

The template is rendered with the subchart's context: parent values are
NOT visible here except under `.Values.global`, so overrides must read
any data they need from global values.

Must emit zero or more `key: value` annotation lines.
*/}}
{{- define "chalert.extraPodAnnotations" -}}
{{- end -}}

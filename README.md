Very WIP, but think `vmalert` but for ClickHouse.

# chalert

ClickHouse-native alerting engine. Evaluates SQL expressions against ClickHouse on a schedule, manages alert state transitions, and sends notifications to Alertmanager.

Structurally based on [vmalert](https://docs.victoriametrics.com/vmalert/) — same Group/Rule hierarchy, same alert state machine — but with ClickHouse SQL instead of PromQL and alert state persisted to ClickHouse tables instead of remote-written as time series.

## Quick Start

```sh
chalert \
  -clickhouse.dsn="clickhouse://default:@localhost:9000/default" \
  -rule="rules/*.yaml" \
  -notifier.url="http://localhost:9093" \
  -evaluationInterval=1m
```

## How It Works

1. Parse YAML rule files into groups of alerting rules
2. Each group runs an evaluation loop on its configured interval
3. Each evaluation executes the rule's SQL expression against ClickHouse
4. Query results drive the alert state machine: **Inactive → Pending → Firing**
5. Firing and resolved alerts are sent to Alertmanager via the v2 API
6. Alert state is persisted to ClickHouse tables for restart recovery

```
Inactive ──(expr matches)──→ Pending ──(for elapsed)──→ Firing
    ↑                            │                         │
    └──(expr stops matching)─────┘                         │
    ↑                                                      │
    └──(keep_firing_for elapsed)───────────────────────────┘
```

## Rule Configuration

Rules use the same YAML format as vmalert. The key difference is that `expr` contains ClickHouse SQL instead of PromQL.

```yaml
groups:
  - name: http-errors
    interval: 30s
    rules:
      - alert: HighErrorRate
        expr: |
          SELECT
              service,
              countIf(status >= 500) / count() AS value
          FROM http_requests
          WHERE timestamp > now() - INTERVAL 5 MINUTE
          GROUP BY service
          HAVING value > 0.05
        for: 3m
        keep_firing_for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Error rate {{ .Value }} on {{ .Labels.service }}"
```

### Query Contract

Alert expressions must return columns matching this shape:

| Column type | Role | Notes |
|---|---|---|
| String, LowCardinality(String) | Dimension labels | Column name becomes label key |
| String listed in `annotation_columns` | Annotation data | Not a label; read in templates as `.Columns.<name>` |
| Numeric (Float64, UInt64, etc.) | Alert value | Column named `value` preferred; first numeric otherwise |
| DateTime, DateTime64 | Timestamp | Optional; defaults to evaluation time |

Each result row produces one alert instance with the string columns as labels.
The labels are the instance's identity: a row whose labels change is a new
instance, and the old one resolves. Rows must be unique on their labels.

### Annotation-only Columns

Some columns describe an alert without identifying it: a contact, a link, a
location. As labels, a change in their value while the alert fires resolves
the alert and fires a new one, which downstream (for example a ticketing
receiver) can look like two incidents. List such columns in
`annotation_columns`:

```yaml
- alert: LinkDown
  expr: |
    SELECT l.device AS device, l.port AS port,
           any(c.carrier_noc_phone) AS carrier_noc_phone,
           1 AS value
    FROM link_state AS l
    LEFT JOIN circuits AS c USING (device, port)
    WHERE l.oper_state != 'up'
    GROUP BY device, port
  annotation_columns: [carrier_noc_phone]
  annotations:
    summary: "{{ .Labels.port }} on {{ .Labels.device }} is down"
    contact: "{{ if .Columns.carrier_noc_phone }}Carrier NOC: {{ .Columns.carrier_noc_phone }}{{ end }}"
```

The `LEFT JOIN` keeps ports that have no circuit. The `GROUP BY` with `any()`
keeps one row per label set: rows must stay unique on their labels, and a
duplicate fails the whole rule's evaluation, not just one alert.

A listed column is not a label: it is not part of the alert's identity, not
sent to Alertmanager as a label, and not stored as a dimension. Templates read
it as `.Columns.<name>`, or `$columns.<name>` (the `$columns` shorthand works
only with a name after the dot; use `.Columns` with `index` or `range`). An
absent or empty value reads as an empty string.

When its value changes, the same alert stays firing and its annotations
update. Alertmanager sends a notification when the set of firing or resolved
alerts in a group changes, or when `repeat_interval` passes; an annotation
change alone does not trigger one. Receivers see the new text at the group's
next notification.

Notes:

- Only columns that would otherwise be labels can be annotation columns:
  String-like types (String, Nullable(String), FixedString, UUID, Enum, IPv4,
  IPv6), not numeric or DateTime. Cast others with `toString()`.
- Names must match the query's column names exactly (ClickHouse names are
  case-sensitive). A name that matches no column is ignored without an error:
  the column stays a label and `.Columns.<name>` reads as empty.
- A listed name cannot be `value`, `ts` or `alertname`, or a label set on the
  rule or its group. External labels (`-external.label`) are not checked; they
  are added to the labels and never collide with an annotation column.
- Recording rules cannot have `annotation_columns`.
- Adding or removing annotation columns does not change the rule ID, so a
  reload keeps the rule's state. It does change the label set of its alerts:
  alerts already firing resolve once and fire again with the new label set.

### Evaluation Timestamp

The evaluation timestamp is available as a named ClickHouse parameter `{chalert_eval_ts:DateTime64(3)}` for use in queries:

```sql
SELECT service, count() AS value
FROM http_requests
WHERE timestamp > {chalert_eval_ts:DateTime64(3)} - INTERVAL 5 MINUTE
  AND timestamp <= {chalert_eval_ts:DateTime64(3)}
GROUP BY service
HAVING value > 100
```

### Annotation Templates

Annotations support Go `text/template` syntax with `.Labels`, `.Columns` (see [Annotation-only Columns](#annotation-only-columns)), `.Value`, and `.Expr` fields. Legacy vmalert-style `$labels` and `$value` variables are also supported, and `$columns` the same way.

```yaml
annotations:
  summary: "{{ .Labels.service }} error rate is {{ printf \"%.2f\" .Value }}"
  description: "Generated by: {{ .Expr }}"
```

### Environment Variable Substitution

Use `%{ENV_VAR}` syntax in rule files (same as vmalert):

```yaml
expr: SELECT 1 AS value FROM %{MY_TABLE}
```

### Group-Level Options

```yaml
groups:
  - name: my-group
    interval: 30s           # Evaluation interval (default: -evaluationInterval flag)
    concurrency: 4          # Parallel rule evaluation within group (default: 1)
    limit: 1000             # Max alert instances per rule (default: -rule.defaultLimit)
    eval_delay: 30s         # Compensate for ingestion lag
    connection: "..."       # Override ClickHouse connection for this group
    labels:                 # Extra labels applied to all rules in group
      env: production
```

## Architecture

```
cmd/chalert/       Main entry point, flag parsing, signal handling
config/            YAML rule parsing, validation, rule identity hashing
rule/              Alert state machine, group evaluation loop
datasource/        ClickHouse query execution, column-to-metric mapping
notifier/          Alertmanager v2 HTTP client
statestore/        Alert state persistence to ClickHouse tables
chclient/          ClickHouse connection pool (read/write separation)
integration/       Integration tests (testcontainers)
```

### Rule Identity

Rules are identified by a hash of their expression, name, and labels. At startup, chalert computes canonical rule IDs using ClickHouse's [`normalizedQueryHashKeepNames`](https://clickhouse.com/docs/sql-reference/functions/other-functions#normalizedqueryhashkeepnames) function, which normalizes whitespace, comments, and literal values. This means cosmetic edits to SQL expressions (reformatting, changing comments) don't change rule identity and won't disrupt in-flight alert state.

A Go-side fallback (`HashRule` with whitespace normalization) is used for `--dryRun` validation when no ClickHouse connection is available.

### State Persistence

Alert state is stored in two ClickHouse tables (auto-created on startup):

- **`alert_state`** — `ReplacingMergeTree` keyed by `(rule_id, alert_hash)`. Current state for restart recovery. Read only on startup with `FINAL`.
- **`alert_history`** — `MergeTree` append-only audit log. Every state transition gets a row. TTL'd at 90 days.

### Hot Reload

Rule files are hot-reloaded without restarting. Existing alert state is preserved for rules whose identity hasn't changed. New rules start fresh; removed rules are stopped gracefully. Reload failures (unreadable or invalid files) keep the previous rules running and are surfaced via `chalert_config_reloads_total{result="error"}` and `chalert_config_last_reload_successful`.

Three triggers:

- **Periodic check** — with `-rule.configCheckInterval` set, chalert re-reads the rule files on that interval and reloads only when their content changed. This is how Kubernetes ConfigMap updates apply automatically: the kubelet syncs the mounted volume, and the next check picks it up. Cheap when nothing changed — a content fingerprint is compared before any parsing or ClickHouse round-trips happen.
- **`POST /-/reload`** — schedules an immediate reload.
- **`SIGHUP`** — same as before.

### ClickHouse Guard Rails

Per-connection settings prevent runaway alert queries:

- `-clickhouse.maxQueryTime` — max execution time per query (default: 30s)
- `-clickhouse.maxRowsToRead` — max rows ClickHouse may scan per query
- `-clickhouse.maxThreads` — max threads per query on ClickHouse

A separate read connection can be configured with `-clickhouse.read-dsn` to isolate alert evaluation from ingestion load.

## Flags

| Flag | Default | Description |
|---|---|---|
| `-rule` | (required) | Path to rule files (supports globs, `;`-separated) |
| `-clickhouse.dsn` | (required) | ClickHouse connection DSN |
| `-clickhouse.read-dsn` | | Optional read replica DSN |
| `-clickhouse.database` | `default` | Database for alert state tables |
| `-clickhouse.maxQueryTime` | `30s` | Max query execution time |
| `-clickhouse.maxRowsToRead` | `0` | Max rows per query (0 = unlimited) |
| `-clickhouse.maxThreads` | `0` | Max threads per query (0 = CH default) |
| `-notifier.url` | | Alertmanager URL(s), comma-separated |
| `-evaluationInterval` | `1m` | Default group evaluation interval |
| `-rule.defaultLimit` | `10000` | Default max alert instances per rule |
| `-rule.configCheckInterval` | `0` | Interval for re-reading rule files and hot-reloading on changes (0 = disabled) |
| `-external.url` | | Base URL for alert source links |
| `-external.label` | | External labels (`Name=value`, repeatable) |
| `-httpListenAddr` | `:8880` | HTTP API address |
| `-dryRun` | `false` | Validate rules without starting evaluation |

## Differences from vmalert

| Area | vmalert | chalert |
|---|---|---|
| Query language | PromQL / MetricsQL | ClickHouse SQL |
| State storage | Remote-written as ALERTS time series | ClickHouse tables (alert_state, alert_history) |
| Rule identity | Go-side FNV hash | ClickHouse `normalizedQueryHashKeepNames` |
| Recording rules | Time series output | Not yet supported (use CH materialized views) |
| Dependencies | prompb, remote write | clickhouse-go v2 native protocol |
| Notification | Alertmanager v2 API | Alertmanager v2 API (same) |

## Observability

chalert exposes Prometheus metrics on the HTTP server (`-httpListenAddr`, default `:8880`):

| Endpoint | Description |
|---|---|
| `/health` | Liveness probe (always 200 once started) |
| `/ready` | Readiness probe (200 after all groups are running) |
| `/metrics` | Prometheus metrics |
| `/version` | Build version JSON |
| `/-/reload` | POST triggers a rule reload |

Key metrics:

| Metric | Type | Description |
|---|---|---|
| `chalert_rule_eval_duration_seconds` | histogram | Rule evaluation duration |
| `chalert_rule_eval_errors_total` | counter | Rule evaluation errors |
| `chalert_alerts_active` | gauge | Active alert instances per rule/state |
| `chalert_notifier_sends_total` | counter | Notification send attempts by URL/result |
| `chalert_config_reloads_total` | counter | Config reload attempts by result |
| `chalert_config_last_reload_successful` | gauge | Whether the last reload attempt succeeded (1) or failed (0) |

## Development

### Prerequisites

- Go 1.25+
- ClickHouse (for integration tests)
- [colima](https://github.com/abiosoft/colima) or Docker (for testcontainers)

### Unit Tests

```sh
go test ./config/ ./rule/ ./notifier/ -v
```

### Integration Tests

Integration tests use [testcontainers](https://golang.testcontainers.org/) to spin up ClickHouse, Alertmanager, and sshd containers. They require Docker — on macOS with colima:

```sh
# Ensure colima is running
colima status  # or: colima start

# Run integration tests
DOCKER_HOST="unix://$HOME/.colima/default/docker.sock" \
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="/var/run/docker.sock" \
go test ./integration/ -tags integration -v -timeout 120s
```

### All Tests

```sh
DOCKER_HOST="unix://$HOME/.colima/default/docker.sock" \
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="/var/run/docker.sock" \
go test ./... -tags integration -v -timeout 180s
```

### Dry Run

Validate rule files without connecting to ClickHouse:

```sh
chalert -rule="rules/*.yaml" -dryRun
```

## Contributing

### CI

Every push and pull request to `main` runs the CI workflow:

- `go vet` and `golangci-lint`
- Unit tests with race detector
- `go build`
- `helm lint`

### Releases

Releases are automated with [release-please](https://github.com/googleapis/release-please). The workflow:

1. Push commits to `main` using [Conventional Commits](https://www.conventionalcommits.org/) messages:
   - `feat: add new feature` — bumps minor version
   - `fix: fix a bug` — bumps patch version
   - `feat!: breaking change` or `BREAKING CHANGE:` footer — bumps major version
   - `chore:`, `docs:`, `ci:`, `refactor:` etc. — no version bump
2. release-please automatically creates/updates a **Release PR** with a changelog and version bumps
3. When you merge the Release PR, release-please creates a git tag and GitHub Release
4. That triggers the build pipeline: cross-compiled binaries, multi-arch Docker image pushed to GHCR, and Helm chart pushed to the OCI registry

No manual tagging needed. Just write good commit messages and merge the Release PR when you're ready to ship.

### Helm Chart

The chart is published to `oci://ghcr.io/garbett1/charts/chalert` on each release. Install with:

```sh
helm install chalert oci://ghcr.io/garbett1/charts/chalert --version <version>
```

## License

Apache License 2.0. See [LICENSE](LICENSE).

This project is derived from [VictoriaMetrics vmalert](https://github.com/VictoriaMetrics/VictoriaMetrics) (Apache 2.0). See [NOTICE](NOTICE) for attribution details.

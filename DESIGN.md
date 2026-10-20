# chalert Design Document

## What is this?

chalert is a ClickHouse-native alerting engine inspired by VictoriaMetrics vmalert.
It evaluates SQL-based alerting rules against ClickHouse and sends notifications
via Alertmanager.

## Architecture

```
┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  YAML Rules  │───→│    Rule Engine   │───→│  Alertmanager    │
│  (config/)   │    │  (rule/, main)   │    │  (notifier/)     │
└──────────────┘    └────────┬─────────┘    └──────────────────┘
                             │
                    ┌────────┴─────────┐
                    │   ClickHouse     │
                    │  ┌─────────────┐ │
                    │  │ User tables │◄├── Alert queries (read)
                    │  └─────────────┘ │
                    │  ┌─────────────┐ │
                    │  │ alert_state │◄├── State persistence (write)
                    │  └─────────────┘ │
                    │  ┌──────────────┐│
                    │  │alert_history │◄├── Audit log (write)
                    │  └──────────────┘│
                    └──────────────────┘
```

## Key Assumptions

### Query Contract

Alert rule SQL expressions MUST follow this contract:

1. **String columns** → become alert dimensions (labels)
2. **Exactly one numeric column** → the metric value (prefer naming it `value`)
3. **Optional DateTime column** → evaluation timestamp (prefer naming it `ts`)
4. **HAVING clause** → the threshold filter; only violating rows are returned

The engine does NOT inject time filters. The query author controls the time
window via explicit WHERE clauses. This is deliberate because:
- ClickHouse tables have diverse schemas with different time column names
- The user knows their data model better than we do
- It allows complex time comparisons (current vs baseline) without fighting the engine

### Why No prompb

vmalert uses `prompb.Label` (a protobuf type) throughout as a key-value pair.
VLogs continued this pattern by making its stats API return Prometheus-compatible JSON.

We break away because:
- ClickHouse query results are rows and columns, not metric series
- We don't need protobuf serialization (no remote write)
- `map[string]string` is simpler and sufficient
- Our `datasource.Label{Name, Value string}` is structurally identical anyway

### Why Not Materialized Views for Alerting

MVs trigger on INSERT, not on a clock. Alert evaluation needs periodic assessment
of the current state ("has error rate exceeded 5% over the last 5 minutes?").
An MV only sees rows in the current INSERT block — it has no window over
historical data.

ClickHouse's `REFRESH EVERY` syntax for refreshable MVs is essentially a
scheduled query — which is exactly what our polling loop does, but with
alert state management, notification, and lifecycle management built in.

MVs remain useful as a **pre-aggregation layer** that alert queries read from.
The examples demonstrate this pattern.

### Alert State Persistence

- In-memory state (rule package) is authoritative during runtime
- ClickHouse `alert_state` table is written to periodically for crash recovery
- `alert_history` table is append-only audit log with 90-day TTL
- On restart, active alerts are loaded from `alert_state FINAL`

ReplacingMergeTree's eventual consistency is acceptable because:
- We only read on startup (not during evaluation)
- FINAL keyword ensures we see the latest version
- The table is write-heavy, read-rarely — ideal for RMT

### Connection Topology

- Alert evaluation queries should target read replicas (via `-clickhouse.read-dsn`)
- State writes go to the primary (via `-clickhouse.dsn`)
- Max query time is enforced to prevent runaway queries from impacting the cluster
- Each query runs in its own connection from the pool; no long-lived transactions

## What's Implemented (v0.1)

- [x] YAML rule parsing with validation (config/)
- [x] ClickHouse querier with column-type-based mapping (datasource/)
- [x] Alert state machine: Inactive → Pending → Firing (rule/)
- [x] `for` duration, `keep_firing_for`, `eval_delay`
- [x] Label merging with `exported_` prefix on conflicts
- [x] Cardinality limit per rule
- [x] Concurrent rule evaluation within groups
- [x] Hot config reload via SIGHUP
- [x] Alertmanager v2 notification with HA fan-out (notifier/)
- [x] ClickHouse state persistence and restart recovery (statestore/)
- [x] Separate read/write connection pools (chclient/)
- [x] Environment variable substitution in rules
- [x] Multi-document YAML support
- [x] Dry-run validation mode

## Refreshable Materialized Views as Pre-Aggregation

ClickHouse Refreshable MVs (`REFRESH EVERY`) run a query on a fixed schedule and
atomically swap the results into a target table. They're production-ready since
CH 24.10 and are a natural complement to chalert.

### Recommended Pattern

Create RMVs yourself for expensive aggregations, then point alert rules at the
pre-computed target tables:

```
┌──────────────┐    REFRESH EVERY 1m    ┌─────────────────┐
│ Raw tables   │───────────────────────→│ RMV target table│
│ (payments,   │                        │ (payment_stats) │
│  http_reqs)  │                        └────────┬────────┘
└──────────────┘                                 │
                                        chalert reads FROM
                                                 │
                                        ┌────────┴────────┐
                                        │  Alert rules    │
                                        │  (trivial SELECT│
                                        │   on small table│
                                        └─────────────────┘
```

Benefits:
- Alert queries become trivial SELECTs (fast, low resource usage)
- Multiple alert rules can share the same base aggregation
- The target table is queryable for debugging ("what does the alert see?")
- RMV refresh is independent of chalert — survives chalert restarts

### Why chalert Doesn't Manage RMV Lifecycle

We considered having chalert auto-create RMVs per rule, but the downsides outweigh
the benefits:

- **Timing mismatch**: RMVs refresh on their own clock; the Go eval loop can't
  control exactly when data was computed. `eval_delay` becomes meaningless.
- **Stale reads**: If an RMV refresh fails, Go silently reads stale data. Detecting
  this requires polling `system.view_refreshes`, adding complexity.
- **DDL management**: CREATE/DROP/ALTER for each rule change, orphan cleanup on
  restart, DDL storms on SIGHUP reload with many rules.
- **Debugging**: The Go loop logs exact SQL + results per evaluation. RMV execution
  happens inside ClickHouse's scheduler — harder to correlate with alert state.

The Go polling loop is simpler, gives precise eval timestamps, and handles errors
in a single place. RMVs work best as a user-managed pre-aggregation layer.

### RMV Best Practices

- **Align intervals**: Set the RMV `REFRESH EVERY` to match or be faster than the
  alert group's `interval`. A 1m RMV with a 1m alert interval is fine.
- **Stagger refreshes**: Use `OFFSET` and `RANDOMIZE FOR` to spread load:
  ```sql
  REFRESH EVERY 1 MINUTE OFFSET 10 SECOND RANDOMIZE FOR 5 SECOND
  ```
- **Monitor freshness**: Alert on stale RMVs using `system.view_refreshes`:
  ```sql
  SELECT view, dateDiff('second', last_success_time, now()) AS staleness
  FROM system.view_refreshes
  WHERE staleness > 300
  ```
- **Include a timestamp column**: Add `now() AS refreshed_at` to the RMV query
  so alert rules can verify data freshness.

See `examples/rmv_preaggregation.yaml` for complete examples.

## What's NOT Implemented Yet

- [ ] HTTP API and web UI (port from vmalert's web.go)
- [ ] Full Go template engine for annotations (currently simple substitution)
- [ ] Recording rules / MV lifecycle management
- [ ] Notifier service discovery (Consul SD, DNS SD)
- [ ] Alert relabeling
- [ ] Replay/backfill mode
- [ ] Per-group ClickHouse connection overrides
- [ ] Metrics exposition (Prometheus /metrics endpoint)
- [ ] Grafana-compatible /api/v1/rules endpoint

## Edge Cases and Mitigations

### Too Many Alert Instances

A query like `GROUP BY user_id` on a table with 1M users could produce 1M alerts.

Mitigations:
- `limit` per rule and per group (implemented)
- Future: pre-flight cardinality check before full evaluation
- Future: circuit breaker that disables rules producing excessive instances

### Runaway Queries

A poorly written alert query could scan the entire table.

Mitigations:
- `clickhouse.maxQueryTime` flag (default 30s)
- ClickHouse-level `max_execution_time` setting on the connection
- Read replica isolation so evaluation doesn't impact ingestion

### ClickHouse Downtime

If ClickHouse is unavailable:
- Alert evaluation fails gracefully (error logged, state preserved in memory)
- Alerts that were already firing continue to fire (state machine is in-memory)
- State persistence failures are logged but don't crash the process
- On reconnection, evaluation resumes normally

### Clock Skew / Ingestion Lag

If data arrives in ClickHouse with a delay:
- `eval_delay` compensates by shifting the query timestamp backward
- Example: with `eval_delay: 30s`, a query at T=60s actually evaluates at T=30s
- This matches vmalert's `-rule.evalDelay` behavior

## File Structure

```
chalert/
├── cmd/chalert/main.go      # Entry point, flag parsing, lifecycle
├── config/                   # YAML rule parsing and validation
│   ├── config.go
│   └── config_test.go
├── datasource/               # ClickHouse querier abstraction
│   ├── datasource.go         # Interfaces and types
│   └── clickhouse.go         # ClickHouse implementation
├── rule/                     # Alert state machine and group scheduler
│   ├── rule.go               # AlertingRule, AlertInstance, state machine
│   ├── rule_test.go          # Unit tests with fake querier
│   └── group.go              # Group evaluation loop
├── notifier/                 # Alertmanager integration
│   └── alertmanager.go
├── statestore/               # ClickHouse state persistence
│   └── clickhouse.go
├── chclient/                 # ClickHouse connection pool
│   └── client.go
└── examples/                 # Example rule files
    ├── http_alerts.yaml
    ├── infrastructure_alerts.yaml
    └── business_alerts.yaml
```

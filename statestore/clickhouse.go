// Package statestore provides ClickHouse-backed persistence for alert state.
//
// # Design Decisions
//
// Alert state is persisted in two tables:
//
//  1. alert_state — ReplacingMergeTree keyed by (rule_id, alert_hash).
//     This is the "current state" table used for restart recovery.
//     Written to periodically (not on every evaluation) to avoid excessive writes.
//
//  2. alert_history — MergeTree append-only audit log.
//     Every state transition gets a row. TTL'd at 90 days by default.
//     Used for debugging, postmortems, and compliance.
//
//  3. rule_evaluations — MergeTree append-only evaluation log.
//     One row per rule per evaluation, success or failure, the same facts
//     the rule_eval_* metrics carry. A rule that matched nothing and a rule
//     whose query failed both write no alert_history row; this table tells
//     them apart. TTL'd at 30 days.
//
// # Assumptions
//
//   - The in-memory state machine (in the rule package) is authoritative during runtime.
//     The state tables are secondary — used only for restart recovery and audit.
//   - alert_state is read only on startup (LoadActive), never during normal evaluation.
//     This means ReplacingMergeTree's eventual consistency is fine — we read with FINAL.
//   - Write batching: state is flushed in batches, not per-alert. The caller controls
//     when Save() and RecordHistory() are called.
//
// # Table DDL
//
// The package can auto-create tables via EnsureTables(). See the DDL constants below.
package statestore

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/garbett1/chalert/rule"
)

const ddlAlertState = `
CREATE TABLE IF NOT EXISTS %s.alert_state (
    rule_id       UInt64,
    alert_hash    UInt64,
    group_name    LowCardinality(String),
    alert_name    LowCardinality(String),
    state         Enum8('inactive' = 0, 'pending' = 1, 'firing' = 2),
    dimensions    Map(String, String),
    value         Float64,
    expr          String,
    active_at     DateTime64(3),
    fired_at      Nullable(DateTime64(3)),
    resolved_at   Nullable(DateTime64(3)),
    annotations        Map(String, String),
    keep_firing_since  Nullable(DateTime64(3)),
    updated_at         DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (rule_id, alert_hash)
SETTINGS index_granularity = 256
`

const ddlRuleEvaluations = `
CREATE TABLE IF NOT EXISTS %s.rule_evaluations (
    rule_id       UInt64,
    group_name    LowCardinality(String),
    alert_name    LowCardinality(String),
    evaluated_at  DateTime64(3),
    duration_ms   UInt32,
    ok            Bool,
    samples       UInt32,
    error         String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(evaluated_at)
ORDER BY (rule_id, evaluated_at)
TTL evaluated_at + INTERVAL 30 DAY
`

// maxErrorLen bounds the error text one evaluation row stores.
const maxErrorLen = 1024

const ddlAlertHistory = `
CREATE TABLE IF NOT EXISTS %s.alert_history (
    rule_id       UInt64,
    alert_hash    UInt64,
    group_name    LowCardinality(String),
    alert_name    LowCardinality(String),
    state         Enum8('inactive' = 0, 'pending' = 1, 'firing' = 2),
    dimensions    Map(String, String),
    value         Float64,
    expr          String,
    annotations   Map(String, String),
    evaluated_at  DateTime64(3),
    event_time    DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (rule_id, alert_hash, event_time)
TTL event_time + INTERVAL 90 DAY
`

// Store implements rule.StateStore backed by ClickHouse.
type Store struct {
	conn     driver.Conn
	database string
}

// New creates a new ClickHouse state store.
func New(conn driver.Conn, database string) *Store {
	return &Store{conn: conn, database: database}
}

// EnsureTables creates the alert_state, alert_history and rule_evaluations
// tables if they don't exist.
func (s *Store) EnsureTables(ctx context.Context) error {
	stmts := []string{
		fmt.Sprintf(ddlAlertState, s.database),
		fmt.Sprintf(ddlAlertHistory, s.database),
		fmt.Sprintf(ddlRuleEvaluations, s.database),
		// Migration: add keep_firing_since column if missing (for upgrades from earlier schema).
		fmt.Sprintf("ALTER TABLE %s.alert_state ADD COLUMN IF NOT EXISTS keep_firing_since Nullable(DateTime64(3)) AFTER annotations", s.database),
	}
	for _, stmt := range stmts {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	slog.Info("chalert state tables ensured", "database", s.database)
	return nil
}

// Save persists the current state of active alert instances.
func (s *Store) Save(ctx context.Context, instances []rule.AlertInstance) error {
	if len(instances) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s.alert_state", s.database))
	if err != nil {
		return fmt.Errorf("preparing batch: %w", err)
	}

	now := time.Now()
	for _, inst := range instances {
		var firedAt, resolvedAt, keepFiringSince *time.Time
		if !inst.FiredAt.IsZero() {
			t := inst.FiredAt
			firedAt = &t
		}
		if !inst.ResolvedAt.IsZero() {
			t := inst.ResolvedAt
			resolvedAt = &t
		}
		if !inst.KeepFiringSince.IsZero() {
			t := inst.KeepFiringSince
			keepFiringSince = &t
		}

		if err := batch.Append(
			inst.RuleID,
			inst.ID,
			inst.GroupName,
			inst.AlertName,
			stateToEnum(inst.State),
			inst.Labels,
			inst.Value,
			inst.Expr,
			inst.ActiveAt,
			firedAt,
			resolvedAt,
			inst.Annotations,
			keepFiringSince,
			now,
		); err != nil {
			return fmt.Errorf("appending to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}
	slog.Info("chalert state saved", "count", len(instances), "database", s.database)
	return nil
}

// LoadActive loads all Pending and Firing alert instances for restart recovery.
func (s *Store) LoadActive(ctx context.Context) ([]rule.AlertInstance, error) {
	query := fmt.Sprintf(`
		SELECT
			rule_id, alert_hash, group_name, alert_name,
			state, dimensions, value, expr,
			active_at, fired_at, resolved_at, annotations,
			keep_firing_since
		FROM %s.alert_state FINAL
		WHERE state IN ('pending', 'firing')
	`, s.database)

	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying active alerts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var instances []rule.AlertInstance
	for rows.Next() {
		var (
			inst            rule.AlertInstance
			stateStr        string
			firedAt         *time.Time
			resolvedAt      *time.Time
			keepFiringSince *time.Time
		)
		if err := rows.Scan(
			&inst.RuleID, &inst.ID, &inst.GroupName, &inst.AlertName,
			&stateStr, &inst.Labels, &inst.Value, &inst.Expr,
			&inst.ActiveAt, &firedAt, &resolvedAt, &inst.Annotations,
			&keepFiringSince,
		); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		inst.State = enumToState(stateStr)
		if firedAt != nil {
			inst.FiredAt = *firedAt
		}
		if resolvedAt != nil {
			inst.ResolvedAt = *resolvedAt
		}
		if keepFiringSince != nil {
			inst.KeepFiringSince = *keepFiringSince
		}
		instances = append(instances, inst)
	}

	slog.Info("chalert loaded active alerts", "count", len(instances))
	return instances, rows.Err()
}

// RecordHistory writes state transition events to the audit log.
func (s *Store) RecordHistory(ctx context.Context, instances []rule.AlertInstance) error {
	if len(instances) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s.alert_history", s.database))
	if err != nil {
		return fmt.Errorf("preparing history batch: %w", err)
	}

	now := time.Now()
	for _, inst := range instances {
		if err := batch.Append(
			inst.RuleID,
			inst.ID,
			inst.GroupName,
			inst.AlertName,
			stateToEnum(inst.State),
			inst.Labels,
			inst.Value,
			inst.Expr,
			inst.Annotations,
			now, // evaluated_at
			now, // event_time (DEFAULT now64(3) but explicit for batch)
		); err != nil {
			return fmt.Errorf("appending to history batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}
	slog.Info("chalert history recorded", "count", len(instances), "database", s.database)
	return nil
}

// RecordEvaluations writes one row per rule evaluation to the evaluation log.
func (s *Store) RecordEvaluations(ctx context.Context, evals []rule.Evaluation) error {
	if len(evals) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s.rule_evaluations", s.database))
	if err != nil {
		return fmt.Errorf("preparing evaluation batch: %w", err)
	}

	for _, e := range evals {
		msg := e.Error
		if len(msg) > maxErrorLen {
			msg = msg[:maxErrorLen]
		}
		if err := batch.Append(
			e.RuleID,
			e.GroupName,
			e.AlertName,
			e.EvaluatedAt,
			uint32(min(e.Duration.Milliseconds(), math.MaxUint32)),
			e.OK,
			uint32(min(e.Samples, math.MaxUint32)),
			msg,
		); err != nil {
			return fmt.Errorf("appending to evaluation batch: %w", err)
		}
	}

	return batch.Send()
}

func stateToEnum(s rule.AlertState) string {
	switch s {
	case rule.StateFiring:
		return "firing"
	case rule.StatePending:
		return "pending"
	default:
		return "inactive"
	}
}

func enumToState(s string) rule.AlertState {
	switch s {
	case "firing":
		return rule.StateFiring
	case "pending":
		return rule.StatePending
	default:
		return rule.StateInactive
	}
}

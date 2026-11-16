// Package datasource provides the query abstraction for evaluating alert and recording rules.
//
// # Design Decisions
//
// We deliberately stay close to VictoriaMetrics vmalert's data model so that the rule engine,
// notifier, and web API can be reused with minimal changes. The core type is Metric, which
// represents a single result row from a ClickHouse query mapped into the vmalert-compatible
// shape: a set of string dimensions (labels) and a numeric value at a point in time.
//
// # Query Contract
//
// Alert/recording rule SQL queries MUST return columns matching this shape:
//
//   - Any number of String or LowCardinality(String) columns become dimensions (labels)
//   - Exactly ONE numeric column named "value" becomes the metric value
//   - An optional DateTime/DateTime64 column named "ts" provides the evaluation timestamp
//     (defaults to the evaluation time if absent)
//
// Example:
//
//	SELECT
//	    service,
//	    endpoint,
//	    countIf(status >= 500) / count() AS value
//	FROM http_requests
//	WHERE timestamp > now() - INTERVAL 5 MINUTE
//	GROUP BY service, endpoint
//	HAVING value > 0.05
//
// This query returns rows like:
//
//	| service   | endpoint | value |
//	|-----------|----------|-------|
//	| api       | /users   | 0.08  |
//	| payments  | /charge  | 0.12  |
//
// Each row becomes one Metric with Labels=[{service,api},{endpoint,/users}] and Values=[0.08].
package datasource

import (
	"context"
	"time"
)

// Label is a key-value pair representing a dimension from a ClickHouse query result.
// Structurally identical to prompb.Label, kept as our own type to avoid the dependency
// while remaining compatible with the vmalert rule engine patterns.
type Label struct {
	Name  string
	Value string
}

// Metric represents a single query result row, mapped into dimension labels + numeric value.
type Metric struct {
	Labels     []Label
	Timestamps []int64
	Values     []float64
}

// SetLabel adds or updates a label by key.
func (m *Metric) SetLabel(key, value string) {
	for i, l := range m.Labels {
		if l.Name == key {
			m.Labels[i].Value = value
			return
		}
	}
	m.Labels = append(m.Labels, Label{Name: key, Value: value})
}

// GetLabel returns the value for the given label key, or "" if not found.
func (m *Metric) GetLabel(key string) string {
	for _, l := range m.Labels {
		if l.Name == key {
			return l.Value
		}
	}
	return ""
}

// DelLabel removes a label by key.
func (m *Metric) DelLabel(key string) {
	for i, l := range m.Labels {
		if l.Name == key {
			m.Labels = append(m.Labels[:i], m.Labels[i+1:]...)
			return
		}
	}
}

// Result represents the response from a datasource query.
type Result struct {
	Data []Metric

	// RowsExamined is the number of rows scanned by ClickHouse.
	// Exposed for observability / debugging. May be nil if not available.
	RowsExamined *uint64
}

// Querier executes queries against the datasource and returns results.
type Querier interface {
	// Query executes an instant query at the given timestamp.
	// The expr is a SQL statement following the query contract above.
	Query(ctx context.Context, expr string, ts time.Time) (Result, error)

	// QueryRange executes a range query between start and end.
	// Used for replay/backfill functionality.
	QueryRange(ctx context.Context, expr string, start, end time.Time) (Result, error)
}

// QuerierBuilder constructs Querier instances with per-group configuration.
type QuerierBuilder interface {
	BuildWithParams(params QuerierParams) Querier
}

// QuerierParams holds per-group configuration for building a Querier.
type QuerierParams struct {
	// EvaluationInterval is the group's evaluation interval, used to
	// construct default time ranges for queries that don't specify one.
	EvaluationInterval time.Duration

	// QueryParams holds additional key-value parameters passed to the query.
	// For ClickHouse this could include settings like max_execution_time.
	QueryParams map[string]string

	// Debug enables verbose logging for this querier.
	Debug bool
}

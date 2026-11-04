// Package datasource provides a ClickHouse implementation of the Querier interface.
//
// # Column Mapping
//
// When a SQL query returns results, columns are mapped as follows:
//
//   - String, LowCardinality(String), FixedString → Label (dimension)
//   - Float32, Float64, UInt8-UInt64, Int8-Int64, Decimal → Value (the numeric result)
//   - DateTime, DateTime64 → Timestamp
//   - Nullable(T) → as T; a NULL label is left out, a NULL value skips the row
//
// The first numeric column found is used as the value. If the query uses an alias
// like "AS value", that column is preferred. All string columns become labels
// with the column name as the label key.
//
// # Assumptions
//
//   - Queries are expected to be self-contained SELECT statements.
//   - Time windowing is the query author's responsibility — the engine does NOT
//     automatically inject WHERE timestamp > now() - INTERVAL clauses. This is
//     deliberate: ClickHouse tables have diverse schemas and the user knows best
//     what their time column is called and how to filter it.
//   - The evaluation timestamp (ts) is available as a ClickHouse parameter
//     {chalert_eval_ts:DateTime64(3)} that can be referenced in queries.
package datasource

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseQuerier implements Querier by executing SQL against ClickHouse.
type ClickHouseQuerier struct {
	conn  driver.Conn
	debug bool

	evaluationInterval time.Duration
	queryParams        map[string]string
}

// ClickHouseQuerierBuilder creates ClickHouseQuerier instances from a shared connection.
type ClickHouseQuerierBuilder struct {
	conn driver.Conn
}

func NewQuerierBuilder(conn driver.Conn) *ClickHouseQuerierBuilder {
	return &ClickHouseQuerierBuilder{conn: conn}
}

func (b *ClickHouseQuerierBuilder) BuildWithParams(params QuerierParams) Querier {
	return &ClickHouseQuerier{
		conn:               b.conn,
		debug:              params.Debug,
		evaluationInterval: params.EvaluationInterval,
		queryParams:        params.QueryParams,
	}
}

func (q *ClickHouseQuerier) Query(ctx context.Context, expr string, ts time.Time) (Result, error) {
	slog.Info("chalert query", "expr", truncate(expr, 200), "ts", ts)

	start := time.Now()

	// Pass the evaluation timestamp as a named parameter so queries can reference it.
	ctx = withEvalTimestamp(ctx, ts)

	rows, err := q.conn.Query(ctx, expr)
	if err != nil {
		return Result{}, fmt.Errorf("query execution failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	metrics, err := mapRows(rows, ts)
	if err != nil {
		return Result{}, fmt.Errorf("mapping query results: %w", err)
	}

	slog.Info("chalert query complete",
		"results", len(metrics),
		"duration", time.Since(start))

	return Result{Data: metrics}, nil
}

func (q *ClickHouseQuerier) QueryRange(ctx context.Context, expr string, start, end time.Time) (Result, error) {
	// For range queries (replay/backfill), we execute the same SQL but let the
	// query author handle the time range via {chalert_eval_ts} parameter.
	// This is a simplification — vmalert's range query adds step-based iteration,
	// but for ClickHouse we expect the query to handle the range natively.
	return q.Query(ctx, expr, end)
}

// mapRows converts ClickHouse result rows into Metric objects.
//
// Column classification:
//   - String types → labels
//   - Numeric types → value candidates (first one wins, or column named "value")
//   - DateTime types → timestamp candidates (first one wins, or column named "ts")
func mapRows(rows driver.Rows, evalTS time.Time) ([]Metric, error) {
	colTypes := rows.ColumnTypes()
	colNames := rows.Columns()

	if len(colTypes) == 0 {
		return nil, nil
	}

	// Classify columns by type
	type colRole int
	const (
		roleLabel colRole = iota
		roleValue
		roleTimestamp
	)

	roles := make([]colRole, len(colTypes))
	valueIdx := -1
	tsIdx := -1

	for i, ct := range colTypes {
		typeName := baseType(ct.DatabaseTypeName())
		switch {
		case isStringType(typeName):
			roles[i] = roleLabel
		case isNumericType(typeName):
			roles[i] = roleValue
			// Prefer column explicitly named "value"
			if colNames[i] == "value" || valueIdx == -1 {
				valueIdx = i
			}
		case isDateTimeType(typeName):
			roles[i] = roleTimestamp
			if colNames[i] == "ts" || tsIdx == -1 {
				tsIdx = i
			}
		default:
			// Treat unknown types as labels (will be converted to string)
			roles[i] = roleLabel
		}
	}

	if valueIdx == -1 {
		return nil, fmt.Errorf("query must return at least one numeric column for the value; got columns: %v", colNames)
	}

	var metrics []Metric
	for rows.Next() {
		// Allocate scan targets
		scanDest := make([]any, len(colTypes))
		for i, ct := range colTypes {
			scanDest[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := rows.Scan(scanDest...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		m := Metric{}

		// Extract labels from string columns
		for i, role := range roles {
			if role != roleLabel {
				continue
			}
			val := derefToString(scanDest[i])
			if val == "" {
				continue
			}
			m.Labels = append(m.Labels, Label{
				Name:  colNames[i],
				Value: val,
			})
		}

		// Extract value; a NULL is no sample, and skips the row
		if isNull(scanDest[valueIdx]) {
			continue
		}
		v, err := derefToFloat64(scanDest[valueIdx])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", colNames[valueIdx], err)
		}
		m.Values = []float64{v}

		// Extract timestamp
		ts := evalTS
		if tsIdx >= 0 {
			if t, ok := derefToTime(scanDest[tsIdx]); ok {
				ts = t
			}
		}
		m.Timestamps = []int64{ts.Unix()}

		metrics = append(metrics, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return metrics, nil
}

// baseType is a column's type without its Nullable wrapper: a Nullable(T)
// column is mapped as T.
func baseType(typeName string) string {
	if inner, ok := strings.CutPrefix(typeName, "Nullable("); ok {
		return strings.TrimSuffix(inner, ")")
	}
	return typeName
}

// isStringType returns true for ClickHouse string-family types.
func isStringType(typeName string) bool {
	switch {
	case typeName == "String", typeName == "FixedString",
		typeName == "UUID", typeName == "IPv4", typeName == "IPv6",
		typeName == "Enum8", typeName == "Enum16":
		return true
	case len(typeName) > 15 && typeName[:15] == "LowCardinality(":
		// LowCardinality(String), LowCardinality(FixedString(...))
		return true
	}
	return false
}

// isNumericType returns true for ClickHouse numeric types.
func isNumericType(typeName string) bool {
	switch {
	case typeName == "Float32", typeName == "Float64":
		return true
	case len(typeName) >= 4 && typeName[:4] == "UInt":
		return true
	case len(typeName) >= 3 && typeName[:3] == "Int":
		return true
	case len(typeName) >= 7 && typeName[:7] == "Decimal":
		return true
	}
	return false
}

// isDateTimeType returns true for ClickHouse date/time types.
func isDateTimeType(typeName string) bool {
	switch {
	case typeName == "DateTime", typeName == "Date", typeName == "Date32":
		return true
	case len(typeName) >= 10 && typeName[:10] == "DateTime64":
		return true
	}
	return false
}

// deref follows the pointers of a scan target to its value. It stops at a
// nil pointer, a Nullable column's NULL.
func deref(v any) reflect.Value {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer && !rv.IsNil() {
		rv = rv.Elem()
	}
	return rv
}

// isNull reports whether a scan target holds a Nullable column's NULL.
func isNull(v any) bool {
	return deref(v).Kind() == reflect.Pointer
}

// derefToString converts a scanned value to string; a NULL is "".
func derefToString(v any) string {
	rv := deref(v)
	if rv.Kind() == reflect.Pointer {
		return ""
	}
	if s, ok := rv.Interface().(string); ok {
		return s
	}
	return fmt.Sprintf("%v", rv.Interface())
}

// derefToFloat64 converts a scanned numeric value to float64.
func derefToFloat64(v any) (float64, error) {
	rv := deref(v)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

// derefToTime converts a scanned date/time value to time.Time.
func derefToTime(v any) (time.Time, bool) {
	rv := deref(v)
	if t, ok := rv.Interface().(time.Time); ok {
		return t, true
	}
	return time.Time{}, false
}

func withEvalTimestamp(ctx context.Context, ts time.Time) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{
		"chalert_eval_ts": ts.Format("2006-01-02 15:04:05.000"),
	}))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

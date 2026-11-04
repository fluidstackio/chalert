package datasource

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeColumn is a result column of one ClickHouse type.
type fakeColumn struct {
	name, chType string
	scan         reflect.Type
}

func (c fakeColumn) Name() string             { return c.name }
func (c fakeColumn) Nullable() bool           { return strings.HasPrefix(c.chType, "Nullable(") }
func (c fakeColumn) ScanType() reflect.Type   { return c.scan }
func (c fakeColumn) DatabaseTypeName() string { return c.chType }

// fakeRows serves rows of scanned values; a nil value is NULL.
type fakeRows struct {
	cols []fakeColumn
	rows [][]any
	i    int
}

func (r *fakeRows) Next() bool { r.i++; return r.i <= len(r.rows) }

func (r *fakeRows) Scan(dest ...any) error {
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if v := r.rows[r.i-1][i]; v != nil {
			target.Set(reflect.ValueOf(v))
		} else {
			target.Set(reflect.Zero(target.Type()))
		}
	}
	return nil
}

func (r *fakeRows) ColumnTypes() []driver.ColumnType {
	out := make([]driver.ColumnType, 0, len(r.cols))
	for _, c := range r.cols {
		out = append(out, c)
	}
	return out
}

func (r *fakeRows) Columns() []string {
	out := make([]string, 0, len(r.cols))
	for _, c := range r.cols {
		out = append(out, c.name)
	}
	return out
}

func (r *fakeRows) ScanStruct(any) error { return nil }
func (r *fakeRows) Totals(...any) error  { return nil }
func (r *fakeRows) Close() error         { return nil }
func (r *fakeRows) Err() error           { return nil }

func ptr[T any](v T) *T { return &v }

var (
	tFloat   = reflect.TypeOf(float64(0))
	tString  = reflect.TypeOf("")
	tNFloat  = reflect.PointerTo(tFloat)
	tNString = reflect.PointerTo(tString)
)

// A Nullable(T) column is mapped as T. A NULL value is no sample and skips
// the row; a NULL label is left out.
func TestMapRowsNullable(t *testing.T) {
	rows := &fakeRows{
		cols: []fakeColumn{{"host", "String", tString}, {"region", "Nullable(String)", tNString}, {"value", "Nullable(Float64)", tNFloat}},
		rows: [][]any{
			{"a", ptr("eu"), ptr(1.5)},
			{"b", nil, ptr(2.5)},
			{"c", ptr("us"), nil},
		},
	}
	got, err := mapRows(rows, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	want := []Metric{
		{Labels: []Label{{"host", "a"}, {"region", "eu"}}, Values: []float64{1.5}, Timestamps: []int64{0}},
		{Labels: []Label{{"host", "b"}}, Values: []float64{2.5}, Timestamps: []int64{0}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("mapRows = %+v, want %+v", got, want)
	}
}

// A Nullable numeric column beside the value is no label either: as one,
// it held the address of the pointer it was scanned into.
func TestMapRowsNullableNumericIsNotALabel(t *testing.T) {
	rows := &fakeRows{
		cols: []fakeColumn{{"host", "String", tString}, {"p50", "Nullable(Float64)", tNFloat}, {"value", "Float64", tFloat}},
		rows: [][]any{{"a", ptr(0.5), 1.0}},
	}
	got, err := mapRows(rows, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0].Labels, []Label{{"host", "a"}}) {
		t.Errorf("mapRows = %+v, want one row labelled host=a only", got)
	}
}

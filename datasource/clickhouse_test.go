package datasource

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type fakeColumn struct {
	name, dbType string
	scan         reflect.Type
}

func (c fakeColumn) Name() string             { return c.name }
func (c fakeColumn) Nullable() bool           { return false }
func (c fakeColumn) ScanType() reflect.Type   { return c.scan }
func (c fakeColumn) DatabaseTypeName() string { return c.dbType }

func columns(cols ...fakeColumn) ([]driver.ColumnType, []string) {
	types := make([]driver.ColumnType, len(cols))
	names := make([]string, len(cols))
	for i, c := range cols {
		types[i] = c
		names[i] = c.name
	}
	return types, names
}

var (
	stringType = reflect.TypeFor[string]()
	floatType  = reflect.TypeFor[float64]()
	uintType   = reflect.TypeFor[uint64]()
	timeType   = reflect.TypeFor[time.Time]()
)

func TestClassifyColumns(t *testing.T) {
	tests := []struct {
		name      string
		cols      []fakeColumn
		wantRoles []colRole
		wantValue int
		wantTS    int
		wantErr   bool
	}{
		{
			name:      "labels, value and timestamp",
			cols:      []fakeColumn{{"host", "String", stringType}, {"value", "Float64", floatType}, {"ts", "DateTime", timeType}},
			wantRoles: []colRole{roleLabel, roleValue, roleTimestamp},
			wantValue: 1,
			wantTS:    2,
		},
		{
			name:      "column named value wins over an earlier numeric",
			cols:      []fakeColumn{{"count", "UInt64", uintType}, {"value", "Float64", floatType}},
			wantRoles: []colRole{roleValue, roleValue},
			wantValue: 1,
			wantTS:    -1,
		},
		{
			name:      "column named ts wins over an earlier timestamp",
			cols:      []fakeColumn{{"value", "Float64", floatType}, {"day", "Date", timeType}, {"ts", "DateTime64(3)", timeType}},
			wantRoles: []colRole{roleValue, roleTimestamp, roleTimestamp},
			wantValue: 0,
			wantTS:    2,
		},
		{
			name:      "unknown types are labels",
			cols:      []fakeColumn{{"tags", "Array(String)", stringType}, {"value", "Int32", floatType}},
			wantRoles: []colRole{roleLabel, roleValue},
			wantValue: 1,
			wantTS:    -1,
		},
		{
			name:    "no numeric column",
			cols:    []fakeColumn{{"host", "LowCardinality(String)", stringType}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := classifyColumns(columns(tt.cols...))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(l.roles, tt.wantRoles) {
				t.Errorf("roles = %v, want %v", l.roles, tt.wantRoles)
			}
			if l.valueIdx != tt.wantValue || l.tsIdx != tt.wantTS {
				t.Errorf("valueIdx, tsIdx = %d, %d, want %d, %d", l.valueIdx, l.tsIdx, tt.wantValue, tt.wantTS)
			}
		})
	}
}

func TestColumnLayoutMetric(t *testing.T) {
	evalTS := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	rowTS := evalTS.Add(-time.Minute)

	t.Run("labels, value and row timestamp", func(t *testing.T) {
		l, err := classifyColumns(columns(
			fakeColumn{"host", "String", stringType},
			fakeColumn{"empty", "String", stringType},
			fakeColumn{"value", "UInt64", uintType},
			fakeColumn{"ts", "DateTime", timeType},
		))
		if err != nil {
			t.Fatal(err)
		}
		host, empty, value := "a", "", uint64(3)
		m, err := l.metric([]any{&host, &empty, &value, &rowTS}, evalTS)
		if err != nil {
			t.Fatal(err)
		}
		if want := []Label{{Name: "host", Value: "a"}}; !slices.Equal(m.Labels, want) {
			t.Errorf("labels = %v, want %v (empty labels dropped)", m.Labels, want)
		}
		if !slices.Equal(m.Values, []float64{3}) {
			t.Errorf("values = %v, want [3]", m.Values)
		}
		if !slices.Equal(m.Timestamps, []int64{rowTS.Unix()}) {
			t.Errorf("timestamps = %v, want [%d]", m.Timestamps, rowTS.Unix())
		}
	})

	t.Run("no timestamp column stamps the eval time", func(t *testing.T) {
		l, err := classifyColumns(columns(fakeColumn{"value", "Float64", floatType}))
		if err != nil {
			t.Fatal(err)
		}
		value := 1.5
		m, err := l.metric([]any{&value}, evalTS)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(m.Timestamps, []int64{evalTS.Unix()}) {
			t.Errorf("timestamps = %v, want [%d]", m.Timestamps, evalTS.Unix())
		}
	})

	t.Run("non-numeric value", func(t *testing.T) {
		l := columnLayout{names: []string{"value"}, roles: []colRole{roleValue}, valueIdx: 0, tsIdx: -1}
		s := "x"
		if _, err := l.metric([]any{&s}, evalTS); err == nil {
			t.Fatal("expected an error")
		}
	})
}

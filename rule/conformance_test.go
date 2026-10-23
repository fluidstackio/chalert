package rule

// Conformance tests ported from VictoriaMetrics vmalert.
// Source: github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/rule/alerting_test.go
// Function: TestAlertingRule_Exec (lines 210-580)
//
// These tests validate that chalert's alert state machine matches vmalert's
// behavior for the same sequence of metric inputs.
//
// Differences from vmalert (documented, not bugs):
//   - chalert doesn't emit ALERTS/ALERTS_FOR_STATE time series
//   - chalert has its own resendDelay logic in alertsToSend()
//   - chalert's Exec() returns alerts-to-send, vmalert's exec() returns time series

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
)

type expectedAlert struct {
	labels map[string]string
	state  AlertState
}

// execSteps runs a multi-step state machine test matching vmalert's pattern.
// For each step: sets querier results, calls Exec(), advances timestamp by step,
// then compares alert states against expected.
func execSteps(t *testing.T, ar *AlertingRule, step time.Duration,
	steps [][]datasource.Metric, expected map[int][]expectedAlert) {
	t.Helper()

	q := ar.querier.(*fakeQuerier)
	ts := time.Date(2024, 10, 29, 0, 0, 0, 0, time.UTC)

	for i, metrics := range steps {
		q.results = datasource.Result{Data: metrics}
		_, err := ar.Exec(context.Background(), ts, 0)
		if err != nil {
			t.Fatalf("step %d: unexpected error: %v", i, err)
		}

		if exp, ok := expected[i]; ok {
			got := ar.GetAlerts()

			// Filter out stale inactive alerts for comparison (vmalert deletes
			// pending alerts that disappear, and inactive alerts are retained
			// for resolvedRetention — only compare non-deleted alerts).
			active := append([]AlertInstance{}, got...)

			if len(active) != len(exp) {
				t.Fatalf("step %d: expected %d alerts, got %d\n  expected: %v\n  got: %v",
					i, len(exp), len(active), fmtExpected(exp), fmtAlerts(active))
			}

			// Build expected lookup by label hash
			expByHash := make(map[uint64]expectedAlert)
			for _, e := range exp {
				labels := make(map[string]string)
				for k, v := range e.labels {
					labels[k] = v
				}
				labels["alertname"] = ar.name
				expByHash[hashLabels(labels)] = e
			}

			for _, a := range active {
				e, ok := expByHash[a.ID]
				if !ok {
					t.Fatalf("step %d: unexpected alert with labels %v", i, a.Labels)
				}
				if a.State != e.state {
					t.Fatalf("step %d: alert %v: expected state %s, got %s",
						i, a.Labels, e.state, a.State)
				}
			}
		}

		ts = ts.Add(step)
	}
}

func fmtExpected(exp []expectedAlert) string {
	var parts []string
	for _, e := range exp {
		parts = append(parts, fmt.Sprintf("{labels:%v state:%s}", e.labels, e.state))
	}
	return fmt.Sprintf("[%s]", join(parts, ", "))
}

func fmtAlerts(alerts []AlertInstance) string {
	var parts []string
	for _, a := range alerts {
		parts = append(parts, fmt.Sprintf("{labels:%v state:%s}", a.Labels, a.State))
	}
	return fmt.Sprintf("[%s]", join(parts, ", "))
}

func join(s []string, sep string) string {
	sort.Strings(s)
	result := ""
	for i, v := range s {
		if i > 0 {
			result += sep
		}
		result += v
	}
	return result
}

// metric creates a datasource.Metric with the given label key-value pairs.
// Labels are provided as alternating key, value strings.
func metric(labels ...string) datasource.Metric {
	m := datasource.Metric{
		Values:     []float64{1},
		Timestamps: []int64{1},
	}
	for i := 0; i < len(labels); i += 2 {
		m.Labels = append(m.Labels, datasource.Label{
			Name:  labels[i],
			Value: labels[i+1],
		})
	}
	return m
}

func newConformanceRule(name string, forDur, keepFiringFor time.Duration) *AlertingRule {
	q := &fakeQuerier{}
	qb := &fakeQuerierBuilder{q: q}
	cfg := config.Rule{
		Alert:         name,
		Expr:          "SELECT 1 AS value",
		For:           config.Duration{D: forDur},
		KeepFiringFor: config.Duration{D: keepFiringFor},
		ID:            config.HashRule(config.Rule{Alert: name, Expr: "SELECT 1 AS value"}),
	}
	return NewAlertingRule(qb, "conformance", time.Minute, cfg)
}

// ---------------------------------------------------------------------------
// Tests ported from vmalert TestAlertingRule_Exec
// ---------------------------------------------------------------------------

func TestConformance_Empty(t *testing.T) {
	// vmalert: "empty" — no metrics, no alerts
	ar := newConformanceRule("empty", 0, 0)
	execSteps(t, ar, 5*time.Millisecond,
		[][]datasource.Metric{},
		nil)
}

func TestConformance_EmptyLabels(t *testing.T) {
	// vmalert: "empty_labels" — metric with no labels, immediate firing (for=0)
	ar := newConformanceRule("empty_labels", 0, 0)
	execSteps(t, ar, 5*time.Millisecond,
		[][]datasource.Metric{
			{datasource.Metric{Values: []float64{1}, Timestamps: []int64{1}}},
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{}, state: StateFiring}},
		})
}

func TestConformance_SingleFiringInactiveCycle(t *testing.T) {
	// vmalert: "single-firing=>inactive=>firing=>inactive=>inactive"
	ar := newConformanceRule("single-firing-cycle", 0, 0)
	execSteps(t, ar, 5*time.Millisecond,
		[][]datasource.Metric{
			{metric("name", "foo")},
			{},
			{metric("name", "foo")},
			{},
			{},
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			1: {{labels: map[string]string{"name": "foo"}, state: StateInactive}},
			2: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			3: {{labels: map[string]string{"name": "foo"}, state: StateInactive}},
			4: {{labels: map[string]string{"name": "foo"}, state: StateInactive}},
		})
}

func TestConformance_SingleFiringInactiveFiringAgain(t *testing.T) {
	// vmalert: "single-firing=>inactive=>firing=>inactive=>inactive=>firing"
	ar := newConformanceRule("single-firing-refiring", 0, 0)
	execSteps(t, ar, 5*time.Millisecond,
		[][]datasource.Metric{
			{metric("name", "foo")},
			{},
			{metric("name", "foo")},
			{},
			{},
			{metric("name", "foo")},
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			1: {{labels: map[string]string{"name": "foo"}, state: StateInactive}},
			2: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			3: {{labels: map[string]string{"name": "foo"}, state: StateInactive}},
			4: {{labels: map[string]string{"name": "foo"}, state: StateInactive}},
			5: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
		})
}

func TestConformance_MultipleFiring(t *testing.T) {
	// vmalert: "multiple-firing" — 3 alerts fire in same step
	ar := newConformanceRule("multiple-firing", 0, 0)
	execSteps(t, ar, 5*time.Millisecond,
		[][]datasource.Metric{
			{
				metric("name", "foo"),
				metric("name", "foo1"),
				metric("name", "foo2"),
			},
		},
		map[int][]expectedAlert{
			0: {
				{labels: map[string]string{"name": "foo"}, state: StateFiring},
				{labels: map[string]string{"name": "foo1"}, state: StateFiring},
				{labels: map[string]string{"name": "foo2"}, state: StateFiring},
			},
		})
}

func TestConformance_MultipleStepsFiring(t *testing.T) {
	// vmalert: "multiple-steps-firing" — different alerts rotate across steps
	ar := newConformanceRule("multiple-steps-firing", 0, 0)
	execSteps(t, ar, 5*time.Millisecond,
		[][]datasource.Metric{
			{metric("name", "foo")},
			{metric("name", "foo1")},
			{metric("name", "foo2")},
		},
		map[int][]expectedAlert{
			0: {
				{labels: map[string]string{"name": "foo"}, state: StateFiring},
			},
			1: {
				{labels: map[string]string{"name": "foo"}, state: StateInactive},
				{labels: map[string]string{"name": "foo1"}, state: StateFiring},
			},
			2: {
				{labels: map[string]string{"name": "foo"}, state: StateInactive},
				{labels: map[string]string{"name": "foo1"}, state: StateInactive},
				{labels: map[string]string{"name": "foo2"}, state: StateFiring},
			},
		})
}

func TestConformance_ForPending(t *testing.T) {
	// vmalert: "for-pending" — alert stays pending (for > elapsed)
	ar := newConformanceRule("for-pending", time.Minute, 0)
	execSteps(t, ar, 5*time.Millisecond,
		[][]datasource.Metric{
			{metric("name", "foo")},
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
		})
}

func TestConformance_ForFired(t *testing.T) {
	// vmalert: "for-fired" — Pending→Firing transition when for elapses
	step := 5 * time.Millisecond
	ar := newConformanceRule("for-fired", step, 0)
	execSteps(t, ar, step,
		[][]datasource.Metric{
			{metric("name", "foo")},
			{metric("name", "foo")},
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
			1: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
		})
}

func TestConformance_ForPendingThenEmpty(t *testing.T) {
	// vmalert: "for-pending=>empty" — pending alert deleted when metrics disappear
	ar := newConformanceRule("for-pending-empty", time.Second, 0)
	execSteps(t, ar, 5*time.Millisecond,
		[][]datasource.Metric{
			{metric("name", "foo")},
			{metric("name", "foo")},
			{}, // empty — pending alert should be deleted
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
			1: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
			2: {}, // deleted, not inactive
		})
}

func TestConformance_ForPendingFiringInactivePendingFiring(t *testing.T) {
	// vmalert: "for-pending=>firing=>inactive=>pending=>firing"
	step := 5 * time.Millisecond
	ar := newConformanceRule("for-full-lifecycle", step, 0)
	execSteps(t, ar, step,
		[][]datasource.Metric{
			{metric("name", "foo")},
			{metric("name", "foo")},
			{}, // empty → firing becomes inactive
			{metric("name", "foo")},
			{metric("name", "foo")},
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
			1: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			2: {{labels: map[string]string{"name": "foo"}, state: StateInactive}},
			3: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
			4: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
		})
}

func TestConformance_KeepFiringForDataReturns(t *testing.T) {
	// vmalert: "for-pending=>firing=>keepfiring=>firing"
	// keep_firing_for = 1 step, data returns before expiry
	step := 5 * time.Millisecond
	ar := newConformanceRule("keepfiring-return", step, step)
	execSteps(t, ar, step,
		[][]datasource.Metric{
			{metric("name", "foo")},
			{metric("name", "foo")},
			{},                      // empty — keep firing for 1 step
			{metric("name", "foo")}, // data returns
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
			1: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			2: {{labels: map[string]string{"name": "foo"}, state: StateFiring}}, // kept firing
			3: {{labels: map[string]string{"name": "foo"}, state: StateFiring}}, // back to normal firing
		})
}

func TestConformance_KeepFiringForFullExpiry(t *testing.T) {
	// vmalert: "for-pending=>firing=>keepfiring=>keepfiring=>inactive=>pending=>firing"
	// keep_firing_for = 2 steps, expires after 2 empty steps, then re-fires
	step := 5 * time.Millisecond
	ar := newConformanceRule("keepfiring-expiry", step, 2*step)
	execSteps(t, ar, step,
		[][]datasource.Metric{
			{metric("name", "foo")},
			{metric("name", "foo")},
			{}, // empty — keep firing (1/2)
			{}, // empty — keep firing (2/2)
			{}, // empty — expires now
			{metric("name", "foo")},
			{metric("name", "foo")},
		},
		map[int][]expectedAlert{
			0: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
			1: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			2: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			3: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
			4: {{labels: map[string]string{"name": "foo"}, state: StateInactive}},
			5: {{labels: map[string]string{"name": "foo"}, state: StatePending}},
			6: {{labels: map[string]string{"name": "foo"}, state: StateFiring}},
		})
}

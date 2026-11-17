package rule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
	"github.com/garbett1/chalert/metrics"
)

const (
	holdExpr   = "SELECT device, 1 AS value FROM faults"
	silentExpr = "SELECT device, 1 AS value FROM silence"
)

// holdQuerier answers the rule's expr and its silent query separately and
// counts how often each runs.
type holdQuerier struct {
	results map[string]datasource.Result
	errs    map[string]error
	calls   map[string]int
}

func newHoldQuerier() *holdQuerier {
	return &holdQuerier{
		results: map[string]datasource.Result{},
		errs:    map[string]error{},
		calls:   map[string]int{},
	}
}

func (q *holdQuerier) Query(_ context.Context, expr string, _ time.Time) (datasource.Result, error) {
	q.calls[expr]++
	return q.results[expr], q.errs[expr]
}

func (q *holdQuerier) QueryRange(ctx context.Context, expr string, _, ts time.Time) (datasource.Result, error) {
	return q.Query(ctx, expr, ts)
}

func deviceRows(label string, devices ...string) datasource.Result {
	var res datasource.Result
	for _, d := range devices {
		res.Data = append(res.Data, datasource.Metric{
			Labels: []datasource.Label{{Name: label, Value: d}},
			Values: []float64{1},
		})
	}
	return res
}

func holdRule(q datasource.Querier, hold *config.HoldWhileSilent) *AlertingRule {
	return NewAlertingRule(&fakeQuerierBuilder{q: q}, "g", time.Minute, config.Rule{
		Alert:           "LinkDown",
		Expr:            holdExpr,
		HoldWhileSilent: hold,
		ID:              777,
	})
}

func defaultHold() *config.HoldWhileSilent {
	return &config.HoldWhileSilent{
		DeviceLabel: "device",
		SilentQuery: silentExpr,
		Max:         config.Duration{D: time.Hour},
	}
}

func exec(t *testing.T, r *AlertingRule, ts time.Time) {
	t.Helper()
	if _, err := r.Exec(context.Background(), ts, 0); err != nil {
		t.Fatal(err)
	}
}

// onlyAlert returns the rule's single instance.
func onlyAlert(t *testing.T, r *AlertingRule) AlertInstance {
	t.Helper()
	all := r.GetAlerts()
	if len(all) != 1 {
		t.Fatalf("expected 1 alert instance, got %d", len(all))
	}
	return all[0]
}

// firingThenAbsent fires one instance for device sw1 at now and empties the
// rule's result, so the next evaluation finds it absent.
func firingThenAbsent(t *testing.T, q *holdQuerier, r *AlertingRule, now time.Time) {
	t.Helper()
	q.results[holdExpr] = deviceRows("device", "sw1")
	exec(t, r, now)
	if a := onlyAlert(t, r); a.State != StateFiring {
		t.Fatalf("expected Firing, got %s", a.State)
	}
	q.results[holdExpr] = datasource.Result{}
}

func TestHoldWhileSilentHoldsAnAbsentInstanceWhoseSourceIsSilent(t *testing.T) {
	q := newHoldQuerier()
	r := holdRule(q, defaultHold())
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	firingThenAbsent(t, q, r, now)
	q.results[silentExpr] = deviceRows("device", "sw1", "sw9")

	held := now.Add(time.Minute)
	exec(t, r, held)
	exec(t, r, held.Add(time.Minute))

	a := onlyAlert(t, r)
	if a.State != StateFiring {
		t.Fatalf("expected Firing while the source is silent, got %s", a.State)
	}
	if !a.KeepFiringSince.Equal(held) {
		t.Errorf("hold start = %v, want %v", a.KeepFiringSince, held)
	}
	if got, want := a.Annotations[HeldAnnotation], "source silent since "+held.Format(time.RFC3339); got != want {
		t.Errorf("%s = %q, want %q", HeldAnnotation, got, want)
	}
	if _, ok := a.Labels[HeldAnnotation]; ok {
		t.Errorf("a held instance must not gain a label")
	}
	if a.ID != hashLabels(a.Labels) {
		t.Errorf("a held instance's identity changed")
	}
	if got := testutil.ToFloat64(metrics.AlertsHeld.WithLabelValues("g", "LinkDown")); got != 1 {
		t.Errorf("held gauge = %v, want 1", got)
	}
}

func TestHoldWhileSilentResolvesWhenTheSourceIsNotSilent(t *testing.T) {
	q := newHoldQuerier()
	r := holdRule(q, defaultHold())
	now := time.Now()
	firingThenAbsent(t, q, r, now)
	q.results[silentExpr] = deviceRows("device", "sw9")

	exec(t, r, now.Add(time.Minute))

	a := onlyAlert(t, r)
	if a.State != StateInactive {
		t.Fatalf("expected Inactive, got %s", a.State)
	}
	if _, ok := a.Annotations[HeldAnnotation]; ok {
		t.Errorf("a resolved instance must not carry %s", HeldAnnotation)
	}
	if got := testutil.ToFloat64(metrics.AlertsHeld.WithLabelValues("g", "LinkDown")); got != 0 {
		t.Errorf("held gauge = %v, want 0", got)
	}
}

func TestHoldWhileSilentHoldsWhenTheSilentQueryFails(t *testing.T) {
	q := newHoldQuerier()
	r := holdRule(q, defaultHold())
	now := time.Now()
	firingThenAbsent(t, q, r, now)
	q.errs[silentExpr] = errors.New("UNKNOWN_TABLE")

	if _, err := r.Exec(context.Background(), now.Add(time.Minute), 0); err != nil {
		t.Fatalf("a failed silent query must not fail the rule: %v", err)
	}

	if a := onlyAlert(t, r); a.State != StateFiring {
		t.Fatalf("expected Firing while the source is unknown, got %s", a.State)
	}
}

func TestHoldWhileSilentResolvesOnceMaxElapses(t *testing.T) {
	q := newHoldQuerier()
	r := holdRule(q, defaultHold())
	now := time.Now()
	firingThenAbsent(t, q, r, now)
	q.results[silentExpr] = deviceRows("device", "sw1")

	start := now.Add(time.Minute)
	exec(t, r, start)
	exec(t, r, start.Add(59*time.Minute))
	if a := onlyAlert(t, r); a.State != StateFiring {
		t.Fatalf("expected Firing inside max, got %s", a.State)
	}

	exec(t, r, start.Add(time.Hour))
	a := onlyAlert(t, r)
	if a.State != StateInactive {
		t.Fatalf("expected Inactive once max elapsed, got %s", a.State)
	}
	if _, ok := a.Annotations[HeldAnnotation]; ok {
		t.Errorf("a resolved instance must not carry %s", HeldAnnotation)
	}
}

func TestHoldWhileSilentReappearingInstanceClearsTheHold(t *testing.T) {
	q := newHoldQuerier()
	r := holdRule(q, defaultHold())
	now := time.Now()
	firingThenAbsent(t, q, r, now)
	q.results[silentExpr] = deviceRows("device", "sw1")
	exec(t, r, now.Add(time.Minute))

	q.results[holdExpr] = deviceRows("device", "sw1")
	exec(t, r, now.Add(2*time.Minute))

	a := onlyAlert(t, r)
	if a.State != StateFiring {
		t.Fatalf("expected Firing, got %s", a.State)
	}
	if !a.KeepFiringSince.IsZero() {
		t.Errorf("expected the hold start cleared, got %v", a.KeepFiringSince)
	}
	if _, ok := a.Annotations[HeldAnnotation]; ok {
		t.Errorf("a matching instance must not carry %s", HeldAnnotation)
	}

	// A fresh absence starts a fresh hold, measured from its own start.
	q.results[holdExpr] = datasource.Result{}
	second := now.Add(90 * time.Minute)
	exec(t, r, second)
	a = onlyAlert(t, r)
	if a.State != StateFiring || !a.KeepFiringSince.Equal(second) {
		t.Errorf("expected a new hold from %v, got state %s since %v", second, a.State, a.KeepFiringSince)
	}
}

func TestHoldWhileSilentResolvesAnInstanceWithoutTheDeviceLabel(t *testing.T) {
	q := newHoldQuerier()
	r := holdRule(q, defaultHold())
	now := time.Now()
	q.results[holdExpr] = deviceRows("host", "sw1")
	exec(t, r, now)
	q.results[holdExpr] = datasource.Result{}
	q.results[silentExpr] = deviceRows("device", "sw1")

	exec(t, r, now.Add(time.Minute))

	if a := onlyAlert(t, r); a.State != StateInactive {
		t.Fatalf("expected Inactive, got %s", a.State)
	}
}

func TestRuleWithoutHoldWhileSilentResolvesAnAbsentInstance(t *testing.T) {
	q := newHoldQuerier()
	r := holdRule(q, nil)
	now := time.Now()
	firingThenAbsent(t, q, r, now)
	q.results[silentExpr] = deviceRows("device", "sw1")

	exec(t, r, now.Add(time.Minute))

	if a := onlyAlert(t, r); a.State != StateInactive {
		t.Fatalf("expected Inactive, got %s", a.State)
	}
	if q.calls[silentExpr] != 0 {
		t.Errorf("silent query ran %d times for a rule without the block", q.calls[silentExpr])
	}
}

func TestHoldWhileSilentQueriesOncePerEvaluationAndOnlyWhenNeeded(t *testing.T) {
	q := newHoldQuerier()
	r := holdRule(q, defaultHold())
	now := time.Now()

	q.results[holdExpr] = deviceRows("device", "sw1", "sw2", "sw3")
	exec(t, r, now)
	exec(t, r, now.Add(time.Minute))
	if q.calls[silentExpr] != 0 {
		t.Fatalf("silent query ran %d times with no absent firing instance", q.calls[silentExpr])
	}

	q.results[holdExpr] = datasource.Result{}
	q.results[silentExpr] = deviceRows("device", "sw1", "sw2", "sw3")
	exec(t, r, now.Add(2*time.Minute))
	if q.calls[silentExpr] != 1 {
		t.Fatalf("silent query ran %d times for three absent instances, want 1", q.calls[silentExpr])
	}
	exec(t, r, now.Add(3*time.Minute))
	if q.calls[silentExpr] != 2 {
		t.Fatalf("silent query ran %d times over two evaluations, want 2", q.calls[silentExpr])
	}
}

func TestHoldWhileSilentSurvivesARestore(t *testing.T) {
	q := newHoldQuerier()
	r1 := holdRule(q, defaultHold())
	now := time.Now()
	firingThenAbsent(t, q, r1, now)
	q.results[silentExpr] = deviceRows("device", "sw1")
	start := now.Add(time.Minute)
	exec(t, r1, start)

	r2 := holdRule(q, defaultHold())
	r2.Restore(r1.GetAlerts())

	exec(t, r2, start.Add(30*time.Minute))
	a := onlyAlert(t, r2)
	if a.State != StateFiring || !a.KeepFiringSince.Equal(start) {
		t.Fatalf("expected the restored hold from %v, got state %s since %v", start, a.State, a.KeepFiringSince)
	}

	exec(t, r2, start.Add(time.Hour))
	if a := onlyAlert(t, r2); a.State != StateInactive {
		t.Fatalf("expected max measured from the restored start, got %s", a.State)
	}
}

// A rule that adopts hold_while_silent on reload keeps its ID, so its firing
// instance is held by the running rule rather than resolved.
func TestReloadAdoptingHoldWhileSilentHoldsTheRunningRulesInstance(t *testing.T) {
	q := newHoldQuerier()
	cfg := config.Rule{Alert: "LinkDown", Expr: holdExpr}
	cfg.ID = config.HashRule(cfg)
	g := NewGroup(config.Group{Name: "g", Rules: []config.Rule{cfg}}, &fakeQuerierBuilder{q: q}, GroupOptions{DefaultInterval: time.Minute})
	now := time.Now()
	firingThenAbsent(t, q, g.Rules[0], now)

	held := cfg
	held.HoldWhileSilent = defaultHold()
	if config.HashRule(held) != cfg.ID {
		t.Fatalf("adopting hold_while_silent changed the rule ID")
	}
	ng := NewGroup(config.Group{Name: "g", Rules: []config.Rule{held}}, &fakeQuerierBuilder{q: q}, GroupOptions{DefaultInterval: time.Minute})
	g.mu.Lock()
	dropped := g.updateWith(ng)
	g.mu.Unlock()
	if len(dropped) != 0 {
		t.Fatalf("reload dropped %d rules", len(dropped))
	}

	q.results[silentExpr] = deviceRows("device", "sw1")
	exec(t, g.Rules[0], now.Add(time.Minute))
	if a := onlyAlert(t, g.Rules[0]); a.State != StateFiring {
		t.Fatalf("expected Firing after the reload, got %s", a.State)
	}
}

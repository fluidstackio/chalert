package rule

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
	"github.com/garbett1/chalert/metrics"
)

// activeAlerts returns one rule's alerts_active series, by state.
func activeAlerts(t *testing.T, group, rule string) map[string]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric)
	go func() { metrics.AlertsActive.Collect(ch); close(ch) }()
	out := map[string]float64{}
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatal(err)
		}
		l := map[string]string{}
		for _, p := range d.GetLabel() {
			l[p.GetName()] = p.GetValue()
		}
		if l["group"] == group && l["rule"] == rule {
			out[l["state"]] = d.GetGauge().GetValue()
		}
	}
	return out
}

// Each evaluation sets the rule's alerts_active series to the instances it
// holds pending and firing.
func TestExecSetsAlertsActive(t *testing.T) {
	row := func(svc string) datasource.Metric {
		return datasource.Metric{Labels: []datasource.Label{{Name: "service", Value: svc}}, Values: []float64{1}, Timestamps: []int64{time.Now().Unix()}}
	}
	q := &fakeQuerier{results: datasource.Result{Data: []datasource.Metric{row("a"), row("b")}}}
	r := NewAlertingRule(&fakeQuerierBuilder{q: q}, "active", time.Minute, config.Rule{
		Alert: "Active",
		Expr:  "SELECT service, 1 AS value",
		For:   config.Duration{D: 2 * time.Minute},
		ID:    1,
	})
	ctx, now := context.Background(), time.Now()
	check := func(when string, at time.Duration, pending, firing float64) {
		t.Helper()
		if _, err := r.Exec(ctx, now.Add(at), 0); err != nil {
			t.Fatal(err)
		}
		got := activeAlerts(t, "active", "Active")
		if len(got) != 2 || got["pending"] != pending || got["firing"] != firing {
			t.Errorf("%s: alerts_active = %v, want pending %v and firing %v", when, got, pending, firing)
		}
	}
	check("first match", 0, 2, 0)
	check("after for", 2*time.Minute, 0, 2)
	q.results = datasource.Result{}
	check("resolved", 3*time.Minute, 0, 0)
}

// A reload that drops a rule removes its alerts_active series, so the
// gauge does not report the removed rule's instances as firing forever.
func TestUpdateForgetsADroppedRulesAlertsActive(t *testing.T) {
	store := newRecordingStore()
	g := evalGroup(1, config.Rule{Alert: "Dropped", Expr: exprTwoRows, ID: 1})
	startGroup(t, g, store, nil)
	if got := activeAlerts(t, "g", "Dropped"); got["firing"] != 2 {
		t.Fatalf("before the reload: alerts_active = %v, want firing 2", got)
	}
	g.UpdateWith(evalGroup(1, config.Rule{Alert: "None", Expr: exprNoRows, ID: 2}))
	store.waitFor(t, ofRule(1, StateInactive))
	if got := activeAlerts(t, "g", "Dropped"); len(got) != 0 {
		t.Errorf("after the reload: alerts_active = %v, want no series", got)
	}
}

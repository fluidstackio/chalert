package rule

import (
	"context"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
)

func paymentsQuerier() *fakeQuerier {
	return &fakeQuerier{results: datasource.Result{Data: []datasource.Metric{{
		Labels: []datasource.Label{{Name: "service", Value: "payments"}},
		Values: []float64{2},
	}}}}
}

func execAnnotations(t *testing.T, r *AlertingRule, ts time.Time) map[string]string {
	t.Helper()
	alerts, err := r.Exec(context.Background(), ts, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}
	return alerts[0].Annotations
}

// An annotation that does not parse renders as written, on every
// evaluation, and the rule's other annotations still render.
func TestAnnotationThatDoesNotParseRendersAsWritten(t *testing.T) {
	broken := "{{ .Labels.service "
	r := NewAlertingRule(&fakeQuerierBuilder{q: paymentsQuerier()}, "g", time.Minute, config.Rule{
		Alert: "Broken",
		Expr:  "SELECT 1",
		Annotations: map[string]string{
			"broken":  broken,
			"summary": "{{ .Labels.service }} at {{ .Value }}",
		},
		ID: 1,
	})
	now := time.Now()
	for i := range 2 {
		got := execAnnotations(t, r, now.Add(time.Duration(i)*time.Minute))
		if got["broken"] != broken {
			t.Errorf("evaluation %d: broken annotation: want %q, got %q", i, broken, got["broken"])
		}
		if want := "payments at 2"; got["summary"] != want {
			t.Errorf("evaluation %d: summary: want %q, got %q", i, want, got["summary"])
		}
	}
}

// A reload that edits only a rule's annotations keeps the rule's ID, so the
// running rule is updated in place; its alerts render the new annotations
// from the next evaluation on.
func TestReloadRendersEditedAnnotations(t *testing.T) {
	qb := &fakeQuerierBuilder{q: paymentsQuerier()}
	group := func(summary string) *Group {
		return NewGroup(config.Group{
			Name:     "g",
			Interval: config.Duration{D: time.Minute},
			Rules: []config.Rule{{
				Alert:       "Edited",
				Expr:        "SELECT 1",
				Annotations: map[string]string{"summary": summary},
				ID:          1,
			}},
		}, qb, GroupOptions{DefaultInterval: time.Minute})
	}
	g := group("old {{ .Labels.service }}")
	r := g.Rules[0]
	now := time.Now()
	if got := execAnnotations(t, r, now)["summary"]; got != "old payments" {
		t.Fatalf("before reload: want %q, got %q", "old payments", got)
	}

	g.mu.Lock()
	dropped := g.updateWith(group("new {{ .Labels.service }}"))
	g.mu.Unlock()
	if len(dropped) != 0 || len(g.Rules) != 1 || g.Rules[0] != r {
		t.Fatalf("an annotation edit replaced the rule: dropped %d, rules %d", len(dropped), len(g.Rules))
	}
	if got := execAnnotations(t, r, now.Add(time.Minute))["summary"]; got != "new payments" {
		t.Errorf("after reload: want %q, got %q", "new payments", got)
	}
}

func BenchmarkRenderAnnotations(b *testing.B) {
	r := NewAlertingRule(&fakeQuerierBuilder{q: paymentsQuerier()}, "g", time.Minute, config.Rule{
		Alert: "Bench",
		Expr:  "SELECT 1",
		Annotations: map[string]string{
			"summary":     "{{ $labels.service }} has value {{ $value }}",
			"description": "{{ .Labels.service }}: {{ printf \"%.2f\" .Value }} for {{ .Value | humanizeDuration }}",
		},
		ID: 1,
	})
	m := paymentsQuerier().results.Data[0]
	b.ReportAllocs()
	for b.Loop() {
		r.renderAnnotations(m, nil)
	}
}

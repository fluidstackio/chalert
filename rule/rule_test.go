package rule

import (
	"context"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
)

// fakeQuerier returns predefined results for testing.
type fakeQuerier struct {
	results datasource.Result
	err     error
}

func (f *fakeQuerier) Query(_ context.Context, _ string, _ time.Time) (datasource.Result, error) {
	return f.results, f.err
}

func (f *fakeQuerier) QueryRange(_ context.Context, _ string, _, _ time.Time) (datasource.Result, error) {
	return f.results, f.err
}

type fakeQuerierBuilder struct {
	q datasource.Querier
}

func (f *fakeQuerierBuilder) BuildWithParams(_ datasource.QuerierParams) datasource.Querier {
	return f.q
}

func TestAlertingRule_PendingToFiring(t *testing.T) {
	q := &fakeQuerier{
		results: datasource.Result{
			Data: []datasource.Metric{
				{
					Labels:     []datasource.Label{{Name: "service", Value: "api"}},
					Values:     []float64{0.1},
					Timestamps: []int64{time.Now().Unix()},
				},
			},
		},
	}
	qb := &fakeQuerierBuilder{q: q}

	rule := NewAlertingRule(qb, "test-group", time.Minute, config.Rule{
		Alert: "TestAlert",
		Expr:  "SELECT 'api' AS service, 0.1 AS value",
		For:   config.Duration{D: 2 * time.Minute},
		ID:    12345,
	})

	now := time.Now()

	// First eval: should create Pending alert
	alerts, err := rule.Exec(context.Background(), now, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts to send (pending), got %d", len(alerts))
	}

	allAlerts := rule.GetAlerts()
	if len(allAlerts) != 1 {
		t.Fatalf("expected 1 alert instance, got %d", len(allAlerts))
	}
	if allAlerts[0].State != StatePending {
		t.Errorf("expected Pending, got %s", allAlerts[0].State)
	}

	// Second eval at now+1m: still pending (for=2m)
	alerts, err = rule.Exec(context.Background(), now.Add(time.Minute), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("expected 0 alerts (still pending), got %d", len(alerts))
	}

	// Third eval at now+2m: should transition to Firing
	alerts, err = rule.Exec(context.Background(), now.Add(2*time.Minute), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("expected 1 firing alert, got %d", len(alerts))
	}
	if alerts[0].State != StateFiring {
		t.Errorf("expected Firing, got %s", alerts[0].State)
	}
	if alerts[0].Labels["alertname"] != "TestAlert" {
		t.Errorf("expected alertname=TestAlert, got %q", alerts[0].Labels["alertname"])
	}
	if alerts[0].Labels["service"] != "api" {
		t.Errorf("expected service=api, got %q", alerts[0].Labels["service"])
	}
}

func TestAlertingRule_ForZero_ImmediateFiring(t *testing.T) {
	q := &fakeQuerier{
		results: datasource.Result{
			Data: []datasource.Metric{
				{
					Labels:     []datasource.Label{{Name: "host", Value: "node1"}},
					Values:     []float64{42},
					Timestamps: []int64{time.Now().Unix()},
				},
			},
		},
	}
	qb := &fakeQuerierBuilder{q: q}

	rule := NewAlertingRule(qb, "test-group", time.Minute, config.Rule{
		Alert: "InstantAlert",
		Expr:  "SELECT 'node1' AS host, 42 AS value",
		// No "for" duration — fires immediately
		ID: 99999,
	})

	alerts, err := rule.Exec(context.Background(), time.Now(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert (immediate fire), got %d", len(alerts))
	}
	if alerts[0].State != StateFiring {
		t.Errorf("expected Firing, got %s", alerts[0].State)
	}
}

func TestAlertingRule_Resolution(t *testing.T) {
	q := &fakeQuerier{
		results: datasource.Result{
			Data: []datasource.Metric{
				{
					Labels:     []datasource.Label{{Name: "svc", Value: "a"}},
					Values:     []float64{1},
					Timestamps: []int64{time.Now().Unix()},
				},
			},
		},
	}
	qb := &fakeQuerierBuilder{q: q}

	rule := NewAlertingRule(qb, "g", time.Minute, config.Rule{
		Alert: "ResTest",
		Expr:  "SELECT 'a' AS svc, 1 AS value",
		ID:    111,
	})

	now := time.Now()

	// Fire the alert
	alerts, err := rule.Exec(context.Background(), now, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 || alerts[0].State != StateFiring {
		t.Fatal("expected 1 firing alert")
	}

	// Expression stops matching (empty results)
	q.results = datasource.Result{Data: nil}

	alerts, err = rule.Exec(context.Background(), now.Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}

	// Should get a resolved alert notification
	found := false
	for _, a := range alerts {
		if a.State == StateInactive {
			found = true
		}
	}
	if !found {
		t.Error("expected resolved (inactive) alert to be sent")
	}
}

func TestAlertingRule_LabelConflict(t *testing.T) {
	q := &fakeQuerier{
		results: datasource.Result{
			Data: []datasource.Metric{
				{
					Labels:     []datasource.Label{{Name: "env", Value: "from-query"}},
					Values:     []float64{1},
					Timestamps: []int64{time.Now().Unix()},
				},
			},
		},
	}
	qb := &fakeQuerierBuilder{q: q}

	rule := NewAlertingRule(qb, "g", time.Minute, config.Rule{
		Alert:  "LabelTest",
		Expr:   "SELECT 'from-query' AS env, 1 AS value",
		Labels: map[string]string{"env": "from-config"},
		ID:     222,
	})

	alerts, err := rule.Exec(context.Background(), time.Now(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}

	// Config label should win, original gets "exported_" prefix
	if alerts[0].Labels["env"] != "from-config" {
		t.Errorf("expected env=from-config, got %q", alerts[0].Labels["env"])
	}
	if alerts[0].Labels["exported_env"] != "from-query" {
		t.Errorf("expected exported_env=from-query, got %q", alerts[0].Labels["exported_env"])
	}
}

func TestAlertingRule_Limit(t *testing.T) {
	metrics := make([]datasource.Metric, 10)
	for i := range metrics {
		metrics[i] = datasource.Metric{
			Labels:     []datasource.Label{{Name: "id", Value: string(rune('a' + i))}},
			Values:     []float64{float64(i)},
			Timestamps: []int64{time.Now().Unix()},
		}
	}

	q := &fakeQuerier{
		results: datasource.Result{Data: metrics},
	}
	qb := &fakeQuerierBuilder{q: q}

	rule := NewAlertingRule(qb, "g", time.Minute, config.Rule{
		Alert: "LimitTest",
		Expr:  "SELECT ...",
		ID:    333,
	})

	// Should fail with limit=5
	_, err := rule.Exec(context.Background(), time.Now(), 5)
	if err == nil {
		t.Fatal("expected limit exceeded error")
	}
	t.Logf("got expected error: %s", err)
}

func TestAlertingRule_KeepFiringFor(t *testing.T) {
	q := &fakeQuerier{
		results: datasource.Result{
			Data: []datasource.Metric{
				{
					Labels:     []datasource.Label{{Name: "x", Value: "1"}},
					Values:     []float64{1},
					Timestamps: []int64{time.Now().Unix()},
				},
			},
		},
	}
	qb := &fakeQuerierBuilder{q: q}

	rule := NewAlertingRule(qb, "g", time.Minute, config.Rule{
		Alert:         "KeepFireTest",
		Expr:          "SELECT '1' AS x, 1 AS value",
		KeepFiringFor: config.Duration{D: 5 * time.Minute},
		ID:            444,
	})

	now := time.Now()

	// Fire the alert
	_, err := rule.Exec(context.Background(), now, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Expression stops matching
	q.results = datasource.Result{Data: nil}

	// At now+1m: should still be firing (keep_firing_for=5m)
	alerts, err := rule.Exec(context.Background(), now.Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}

	firing := false
	for _, a := range alerts {
		if a.State == StateFiring {
			firing = true
		}
	}
	if !firing {
		t.Error("expected alert to keep firing")
	}

	// At now+6m: should resolve
	alerts, err = rule.Exec(context.Background(), now.Add(6*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}

	stillFiring := false
	for _, a := range alerts {
		if a.State == StateFiring {
			stillFiring = true
		}
	}
	if stillFiring {
		t.Error("expected alert to stop firing after keep_firing_for elapsed")
	}
}

func TestKeepFiringFor_ReMatchResetsTimer(t *testing.T) {
	matchData := datasource.Result{
		Data: []datasource.Metric{
			{
				Labels:     []datasource.Label{{Name: "x", Value: "1"}},
				Values:     []float64{1},
				Timestamps: []int64{time.Now().Unix()},
			},
		},
	}
	noData := datasource.Result{Data: nil}

	q := &fakeQuerier{results: matchData}
	qb := &fakeQuerierBuilder{q: q}

	r := NewAlertingRule(qb, "g", time.Minute, config.Rule{
		Alert:         "ReMatchTest",
		Expr:          "SELECT '1' AS x, 1 AS value",
		KeepFiringFor: config.Duration{D: 5 * time.Minute},
		ID:            555,
	})

	now := time.Now()

	// Fire the alert
	_, err := r.Exec(context.Background(), now, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Remove data — starts keep_firing_for
	q.results = noData
	_, err = r.Exec(context.Background(), now.Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify KeepFiringSince is set
	allAlerts := r.GetAlerts()
	if len(allAlerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(allAlerts))
	}
	if allAlerts[0].KeepFiringSince.IsZero() {
		t.Fatal("expected KeepFiringSince to be set")
	}

	// Re-match before keep_firing_for expires
	q.results = matchData
	_, err = r.Exec(context.Background(), now.Add(2*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}

	// KeepFiringSince should be reset to zero
	allAlerts = r.GetAlerts()
	if len(allAlerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(allAlerts))
	}
	if !allAlerts[0].KeepFiringSince.IsZero() {
		t.Errorf("expected KeepFiringSince to be reset to zero after re-match, got %v", allAlerts[0].KeepFiringSince)
	}
	if allAlerts[0].State != StateFiring {
		t.Errorf("expected Firing state, got %s", allAlerts[0].State)
	}

	// Remove data again — new keep_firing_for window
	q.results = noData
	_, err = r.Exec(context.Background(), now.Add(3*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}

	allAlerts = r.GetAlerts()
	if len(allAlerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(allAlerts))
	}
	if allAlerts[0].KeepFiringSince.IsZero() {
		t.Fatal("expected new KeepFiringSince to be set")
	}

	// Should still fire at +7m (only 4m into new keep_firing_for window)
	alerts, err := r.Exec(context.Background(), now.Add(7*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	firing := false
	for _, a := range alerts {
		if a.State == StateFiring {
			firing = true
		}
	}
	if !firing {
		t.Error("expected alert to still be firing within new keep_firing_for window")
	}

	// Should resolve at +9m (6m into new keep_firing_for window > 5m)
	alerts, err = r.Exec(context.Background(), now.Add(9*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	stillFiring := false
	for _, a := range alerts {
		if a.State == StateFiring {
			stillFiring = true
		}
	}
	if stillFiring {
		t.Error("expected alert to resolve after new keep_firing_for window elapsed")
	}
}

func TestKeepFiringFor_StatePreservedAcrossRestore(t *testing.T) {
	matchData := datasource.Result{
		Data: []datasource.Metric{
			{
				Labels:     []datasource.Label{{Name: "x", Value: "1"}},
				Values:     []float64{1},
				Timestamps: []int64{time.Now().Unix()},
			},
		},
	}

	q := &fakeQuerier{results: matchData}
	qb := &fakeQuerierBuilder{q: q}

	cfg := config.Rule{
		Alert:         "RestoreTest",
		Expr:          "SELECT '1' AS x, 1 AS value",
		KeepFiringFor: config.Duration{D: 5 * time.Minute},
		ID:            666,
	}

	r1 := NewAlertingRule(qb, "g", time.Minute, cfg)
	now := time.Now()

	// Fire the alert
	_, err := r1.Exec(context.Background(), now, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Remove data — enter keep_firing_for
	q.results = datasource.Result{Data: nil}
	_, err = r1.Exec(context.Background(), now.Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot the instances
	instances := r1.GetAlerts()
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	if instances[0].KeepFiringSince.IsZero() {
		t.Fatal("expected KeepFiringSince to be set before restore")
	}

	// Create a new rule and restore instances
	r2 := NewAlertingRule(qb, "g", time.Minute, cfg)
	r2.Restore(instances)

	// Verify the restored alert still has KeepFiringSince
	restored := r2.GetAlerts()
	if len(restored) != 1 {
		t.Fatalf("expected 1 restored alert, got %d", len(restored))
	}
	if restored[0].KeepFiringSince.IsZero() {
		t.Error("expected KeepFiringSince to be preserved after restore")
	}
	if restored[0].State != StateFiring {
		t.Errorf("expected Firing state after restore, got %s", restored[0].State)
	}

	// Exec with no data — should still be in keep_firing_for window
	alerts, err := r2.Exec(context.Background(), now.Add(2*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	firing := false
	for _, a := range alerts {
		if a.State == StateFiring {
			firing = true
		}
	}
	if !firing {
		t.Error("expected alert to keep firing after restore within keep_firing_for window")
	}
}

func TestAnnotationTemplates_FullGoTemplate(t *testing.T) {
	q := &fakeQuerier{
		results: datasource.Result{
			Data: []datasource.Metric{
				{
					Labels:     []datasource.Label{{Name: "service", Value: "payments"}},
					Values:     []float64{0.123456},
					Timestamps: []int64{time.Now().Unix()},
				},
			},
		},
	}
	qb := &fakeQuerierBuilder{q: q}

	r := NewAlertingRule(qb, "g", time.Minute, config.Rule{
		Alert: "TemplateTest",
		Expr:  "SELECT 'payments' AS service, 0.123456 AS value",
		Annotations: map[string]string{
			"label_access": "{{ .Labels.service }}",
			"formatted":    `{{ printf "%.2f" .Value }}`,
			"expr":         "{{ .Expr }}",
			"legacy":       "{{ $labels.service }} has value {{ $value }}",
		},
		ID: 777,
	})

	alerts, err := r.Exec(context.Background(), time.Now(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(alerts))
	}

	tests := map[string]string{
		"label_access": "payments",
		"formatted":    "0.12",
		"expr":         "SELECT 'payments' AS service, 0.123456 AS value",
		"legacy":       "payments has value 0.123456",
	}

	for key, want := range tests {
		got := alerts[0].Annotations[key]
		if got != want {
			t.Errorf("annotation %q: want %q, got %q", key, want, got)
		}
	}
}

func TestLabelTemplates(t *testing.T) {
	tests := []struct {
		name      string
		rowLabels []datasource.Label
		cfgLabels map[string]string
		want      map[string]string
	}{
		{
			name:      "legacy syntax renders from row labels",
			rowLabels: []datasource.Label{{Name: "team", Value: "platform"}},
			cfgLabels: map[string]string{"grouping_key": "Foo | {{ $labels.team }}"},
			want:      map[string]string{"grouping_key": "Foo | platform"},
		},
		{
			name:      "go template syntax renders from row labels",
			rowLabels: []datasource.Label{{Name: "team", Value: "platform"}},
			cfgLabels: map[string]string{"grouping_key": "{{ .Labels.team }}"},
			want:      map[string]string{"grouping_key": "platform"},
		},
		{
			name:      "missing label renders empty",
			rowLabels: []datasource.Label{{Name: "team", Value: "platform"}},
			cfgLabels: map[string]string{"grouping_key": "x{{ $labels.nope }}y"},
			want:      map[string]string{"grouping_key": "xy"},
		},
		{
			name:      "parse error falls back to raw string",
			rowLabels: []datasource.Label{{Name: "team", Value: "platform"}},
			cfgLabels: map[string]string{"grouping_key": "{{ $labels.team"},
			want:      map[string]string{"grouping_key": "{{ $labels.team"},
		},
		{
			name:      "exec error falls back to raw string",
			rowLabels: []datasource.Label{{Name: "team", Value: "platform"}},
			cfgLabels: map[string]string{"grouping_key": "{{ .Nope }}"},
			want:      map[string]string{"grouping_key": "{{ .Nope }}"},
		},
		{
			name:      "non-templated labels pass through verbatim",
			rowLabels: []datasource.Label{{Name: "team", Value: "platform"}},
			cfgLabels: map[string]string{"severity": "critical"},
			want:      map[string]string{"severity": "critical", "team": "platform"},
		},
		{
			name:      "template sees row labels before configured override",
			rowLabels: []datasource.Label{{Name: "env", Value: "prod"}},
			cfgLabels: map[string]string{"env": "{{ $labels.env }}-override"},
			want:      map[string]string{"env": "prod-override", "exported_env": "prod"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := &fakeQuerier{
				results: datasource.Result{
					Data: []datasource.Metric{
						{
							Labels:     tc.rowLabels,
							Values:     []float64{1},
							Timestamps: []int64{time.Now().Unix()},
						},
					},
				},
			}
			qb := &fakeQuerierBuilder{q: q}

			r := NewAlertingRule(qb, "g", time.Minute, config.Rule{
				Alert:  "LabelTplTest",
				Expr:   "SELECT 1 AS value",
				Labels: tc.cfgLabels,
				ID:     888,
			})

			alerts, err := r.Exec(context.Background(), time.Now(), 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(alerts) != 1 {
				t.Fatalf("expected 1 alert, got %d", len(alerts))
			}
			for k, want := range tc.want {
				if got := alerts[0].Labels[k]; got != want {
					t.Errorf("label %q: want %q, got %q", k, want, got)
				}
			}
		})
	}
}

// TestLabelTemplates_AlertIdentity verifies that label templates are rendered
// before the alert ID is computed: a templated label set must hash identically
// to the equivalent literal label set.
func TestLabelTemplates_AlertIdentity(t *testing.T) {
	newRule := func(labels map[string]string) *AlertingRule {
		q := &fakeQuerier{
			results: datasource.Result{
				Data: []datasource.Metric{
					{
						Labels:     []datasource.Label{{Name: "team", Value: "platform"}},
						Values:     []float64{1},
						Timestamps: []int64{time.Now().Unix()},
					},
				},
			},
		}
		return NewAlertingRule(&fakeQuerierBuilder{q: q}, "g", time.Minute, config.Rule{
			Alert:  "IdentityTest",
			Expr:   "SELECT 1 AS value",
			Labels: labels,
			ID:     999,
		})
	}

	templated := newRule(map[string]string{"grouping_key": "Foo | {{ $labels.team }}"})
	literal := newRule(map[string]string{"grouping_key": "Foo | platform"})

	ta, err := templated.Exec(context.Background(), time.Now(), 0)
	if err != nil {
		t.Fatal(err)
	}
	la, err := literal.Exec(context.Background(), time.Now(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ta) != 1 || len(la) != 1 {
		t.Fatalf("expected 1 alert each, got %d and %d", len(ta), len(la))
	}

	if ta[0].ID != la[0].ID {
		t.Errorf("templated and literal label sets must produce the same alert ID: %d != %d", ta[0].ID, la[0].ID)
	}
	if ta[0].ID != hashLabels(ta[0].Labels) {
		t.Errorf("alert ID must be the hash of the rendered labels: id=%d hash=%d", ta[0].ID, hashLabels(ta[0].Labels))
	}
}

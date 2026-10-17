package rule

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
)

// exprQuerier answers by expression: a result, an error, or a panic.
type exprQuerier struct {
	results map[string]datasource.Result
	errs    map[string]error
	panics  map[string]bool
}

func (q *exprQuerier) Query(_ context.Context, expr string, _ time.Time) (datasource.Result, error) {
	if q.panics[expr] {
		panic("query blew up")
	}
	return q.results[expr], q.errs[expr]
}

func (q *exprQuerier) QueryRange(ctx context.Context, expr string, _, ts time.Time) (datasource.Result, error) {
	return q.Query(ctx, expr, ts)
}

const (
	exprTwoRows = "SELECT two"
	exprNoRows  = "SELECT none"
	exprFails   = "SELECT fails"
	exprPanics  = "SELECT panics"
)

func evalQuerier() *exprQuerier {
	row := func(svc string) datasource.Metric {
		return datasource.Metric{Labels: []datasource.Label{{Name: "service", Value: svc}}, Values: []float64{1}, Timestamps: []int64{time.Now().Unix()}}
	}
	return &exprQuerier{
		results: map[string]datasource.Result{exprTwoRows: {Data: []datasource.Metric{row("a"), row("b")}}},
		errs:    map[string]error{exprFails: errors.New("UNKNOWN_TABLE")},
		panics:  map[string]bool{exprPanics: true},
	}
}

func evalGroup(concurrency int, rules ...config.Rule) *Group {
	return NewGroup(config.Group{Name: "g", Interval: config.Duration{D: time.Minute}, Concurrency: concurrency, Rules: rules},
		&fakeQuerierBuilder{q: evalQuerier()}, GroupOptions{DefaultInterval: time.Minute})
}

func byRule(evals []Evaluation) map[uint64]Evaluation {
	out := map[uint64]Evaluation{}
	for _, e := range evals {
		out[e.RuleID] = e
	}
	return out
}

func TestExecRulesRecordsOneEvaluationPerRule(t *testing.T) {
	rules := []config.Rule{
		{Alert: "Two", Expr: exprTwoRows, ID: 1},
		{Alert: "None", Expr: exprNoRows, ID: 2},
		{Alert: "Fails", Expr: exprFails, ID: 3},
	}
	for name, concurrency := range map[string]int{"sequential": 1, "concurrent": 3} {
		t.Run(name, func(t *testing.T) {
			g := evalGroup(concurrency, rules...)
			before := time.Now()
			_, evals := g.execRules(context.Background(), g.Rules, time.Now(), concurrency, 0)
			if len(evals) != 3 {
				t.Fatalf("got %d evaluations, want 3: %+v", len(evals), evals)
			}
			got := byRule(evals)
			if e := got[1]; !e.OK || e.Samples != 2 || e.Error != "" || e.AlertName != "Two" || e.GroupName != "g" {
				t.Errorf("rule with two rows: %+v", e)
			}
			if e := got[2]; !e.OK || e.Samples != 0 || e.Error != "" {
				t.Errorf("rule with no rows: %+v", e)
			}
			if e := got[3]; e.OK || e.Samples != 0 || e.Error == "" {
				t.Errorf("failing rule: %+v", e)
			}
			for _, e := range evals {
				if e.EvaluatedAt.Before(before) || e.EvaluatedAt.After(time.Now()) {
					t.Errorf("rule %d evaluated at %v, outside the call", e.RuleID, e.EvaluatedAt)
				}
			}
		})
	}
}

// A rule whose evaluation panics is recorded as failed, beside the rules
// that ran, on the sequential path as on the concurrent one.
func TestExecRulesRecordsAPanicAsFailed(t *testing.T) {
	for name, concurrency := range map[string]int{"sequential": 1, "concurrent": 2} {
		t.Run(name, func(t *testing.T) {
			g := evalGroup(concurrency, config.Rule{Alert: "Panics", Expr: exprPanics, ID: 1}, config.Rule{Alert: "Two", Expr: exprTwoRows, ID: 2})
			alerts, evals := g.execRules(context.Background(), g.Rules, time.Now(), concurrency, 0)
			got := byRule(evals)
			if len(evals) != 2 {
				t.Fatalf("got %d evaluations, want 2: %+v", len(evals), evals)
			}
			if e := got[1]; e.OK || e.Error == "" {
				t.Errorf("panicking rule: %+v", e)
			}
			if e := got[2]; !e.OK || e.Samples != 2 || len(alerts) != 2 {
				t.Errorf("the other rule: %+v, alerts %d", e, len(alerts))
			}
		})
	}
}

// Samples counts the query's matches, not the alerts sent to notifiers: two
// rows still pending under a `for` window send nothing and are two samples.
func TestExecRulesCountsPendingMatches(t *testing.T) {
	for name, concurrency := range map[string]int{"sequential": 1, "concurrent": 2} {
		t.Run(name, func(t *testing.T) {
			g := evalGroup(concurrency, config.Rule{Alert: "Pending", Expr: exprTwoRows, ID: 1, For: config.Duration{D: 5 * time.Minute}})
			alerts, evals := g.execRules(context.Background(), g.Rules, time.Now(), concurrency, 0)
			if len(alerts) != 0 {
				t.Fatalf("pending rule sent %d alerts, want 0", len(alerts))
			}
			if e := byRule(evals)[1]; !e.OK || e.Samples != 2 {
				t.Errorf("pending rule: %+v, want ok with 2 samples", e)
			}
		})
	}
}

type recordingStore struct {
	mu    sync.Mutex
	evals [][]Evaluation
	got   chan struct{}
}

func (s *recordingStore) Save(context.Context, []AlertInstance) error { return nil }
func (s *recordingStore) LoadActive(context.Context) ([]AlertInstance, error) {
	return nil, nil
}
func (s *recordingStore) RecordHistory(context.Context, []AlertInstance) error { return nil }
func (s *recordingStore) RecordEvaluations(_ context.Context, evals []Evaluation) error {
	s.mu.Lock()
	s.evals = append(s.evals, evals)
	s.mu.Unlock()
	select {
	case s.got <- struct{}{}:
	default:
	}
	return nil
}

// An evaluation that produces no alerts still reaches the evaluation log:
// that is what tells a quiet rule from one that did not run.
func TestStartRecordsEvaluationsWithoutAlerts(t *testing.T) {
	g := evalGroup(1, config.Rule{Alert: "None", Expr: exprNoRows, ID: 7})
	store := &recordingStore{got: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.Start(ctx, nil, store); close(done) }()
	select {
	case <-store.got:
	case <-time.After(5 * time.Second):
		t.Fatal("no evaluation reached the store")
	}
	cancel()
	<-done
	store.mu.Lock()
	defer store.mu.Unlock()
	if e := store.evals[0]; len(e) != 1 || e[0].RuleID != 7 || !e[0].OK || e[0].Samples != 0 {
		t.Errorf("recorded %+v", e)
	}
}

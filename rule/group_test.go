package rule

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
)

// tsQuerier records the timestamp each query is evaluated at.
type tsQuerier struct {
	mu  sync.Mutex
	ts  []time.Time
	got chan struct{}
}

func (q *tsQuerier) Query(_ context.Context, _ string, ts time.Time) (datasource.Result, error) {
	q.mu.Lock()
	q.ts = append(q.ts, ts)
	q.mu.Unlock()
	select {
	case q.got <- struct{}{}:
	default:
	}
	return datasource.Result{}, nil
}

func (q *tsQuerier) QueryRange(ctx context.Context, expr string, _, ts time.Time) (datasource.Result, error) {
	return q.Query(ctx, expr, ts)
}

func (q *tsQuerier) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ts)
}

// waitForQueries returns the latest query's timestamp once more than n
// queries have run, failing the test after a few seconds.
func (q *tsQuerier) waitForQueries(t *testing.T, n int) time.Time {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for q.count() <= n {
		select {
		case <-q.got:
		case <-deadline:
			t.Fatal("no query ran")
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ts[len(q.ts)-1]
}

func delayedGroup(q datasource.Querier, delay time.Duration) *Group {
	cfg := config.Group{Name: "g", Interval: config.Duration{D: 20 * time.Millisecond}, Rules: []config.Rule{{Alert: "None", Expr: exprNoRows, ID: 1}}}
	if delay > 0 {
		cfg.EvalDelay = &config.Duration{D: delay}
	}
	return NewGroup(cfg, &fakeQuerierBuilder{q: q}, GroupOptions{DefaultInterval: time.Minute})
}

// assertDelay checks that a query ran about delay behind the clock.
func assertDelay(t *testing.T, ts time.Time, delay time.Duration) {
	t.Helper()
	if d := time.Since(ts) - delay; d < -5*time.Second || d > 5*time.Second {
		t.Errorf("query evaluated at %s, %s ago; want about %s ago", ts, time.Since(ts), delay)
	}
}

// A reload that changes a group's eval_delay changes the timestamp its rules
// are evaluated at. The group's ID does not change with its delay, so the
// running group is updated in place rather than replaced.
func TestUpdateAppliesAChangedEvalDelay(t *testing.T) {
	q := &tsQuerier{got: make(chan struct{}, 1)}
	g := delayedGroup(q, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.Start(ctx, nil, nil, nil); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	assertDelay(t, q.waitForQueries(t, 0), 30*time.Second)

	for _, delay := range []time.Duration{2 * time.Minute, 0} {
		ng := delayedGroup(q, delay)
		if ng.ID() != g.ID() {
			t.Fatalf("group ID changed with eval_delay: %d != %d", ng.ID(), g.ID())
		}
		g.UpdateWith(ng)
		// UpdateWith returns once the loop has taken the update, so every
		// later query runs with the new delay.
		assertDelay(t, q.waitForQueries(t, q.count()), delay)
	}
}

func TestNextEvalTS(t *testing.T) {
	prev := time.Date(2026, 11, 3, 17, 19, 14, 552_000_000, time.UTC)
	for _, tc := range []struct {
		name       string
		now        time.Time
		wantTS     time.Time
		wantMissed int64
		wantReset  bool
	}{
		{"tick lands late", prev.Add(time.Minute + 3*time.Microsecond), prev.Add(time.Minute), 0, false},
		{"tick lands on time", prev.Add(time.Minute), prev.Add(time.Minute), 0, false},
		{"evaluation overran two intervals", prev.Add(3*time.Minute + time.Second), prev.Add(3 * time.Minute), 2, false},
		{"wall clock moved backwards", prev.Add(-time.Second), prev.Add(-time.Second), 0, true},
		{"less than an interval since prev", prev.Add(59 * time.Second), prev.Add(59 * time.Second), 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, missed, reset := nextEvalTS(prev, tc.now, time.Minute)
			if !ts.Equal(tc.wantTS) || missed != tc.wantMissed || reset != tc.wantReset {
				t.Errorf("nextEvalTS = (%s, %d, %t), want (%s, %d, %t)",
					ts, missed, reset, tc.wantTS, tc.wantMissed, tc.wantReset)
			}
		})
	}
}

// A rule still matching once its for duration has passed fires on that
// evaluation, however late the ticks that woke the evaluations landed. Using
// ticker times as evaluation timestamps left the gap microseconds short of
// for whenever the last tick landed less late than the first, holding the
// alert pending for one more interval.
func TestNextEvalTSFiresForOnTime(t *testing.T) {
	ar := newConformanceRule("jitter", 2*time.Minute, 0)
	ar.querier.(*fakeQuerier).results = datasource.Result{Data: []datasource.Metric{metric("rack", "r1")}}

	start := time.Date(2026, 11, 3, 17, 19, 14, 552_000_000, time.UTC)
	ts := start
	for i, lag := range []time.Duration{0, 5 * time.Microsecond, time.Microsecond} {
		if i > 0 {
			ts, _, _ = nextEvalTS(ts, start.Add(time.Duration(i)*time.Minute+lag), time.Minute)
		}
		if _, err := ar.Exec(context.Background(), ts, 0); err != nil {
			t.Fatal(err)
		}
	}

	alerts := ar.GetAlerts()
	if len(alerts) != 1 || alerts[0].State != StateFiring {
		t.Fatalf("after evaluations spanning for: got %v, want one firing alert", fmtAlerts(alerts))
	}
}

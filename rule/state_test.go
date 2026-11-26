package rule

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
)

// stateStore keeps the latest saved row per instance, as alert_state's
// ReplacingMergeTree does, and loads the pending and firing ones.
type stateStore struct {
	mu   sync.Mutex
	rows map[[2]uint64]AlertInstance
}

func newStateStore() *stateStore { return &stateStore{rows: map[[2]uint64]AlertInstance{}} }

func (s *stateStore) Save(_ context.Context, instances []AlertInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, inst := range instances {
		s.rows[[2]uint64{inst.RuleID, inst.ID}] = inst
	}
	return nil
}

func (s *stateStore) LoadActive(context.Context) ([]AlertInstance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []AlertInstance
	for _, inst := range s.rows {
		if inst.State == StatePending || inst.State == StateFiring {
			out = append(out, inst)
		}
	}
	return out, nil
}

func (s *stateStore) RecordHistory(context.Context, time.Time, []AlertInstance) error { return nil }

func (s *stateStore) loadActive(t *testing.T) []AlertInstance {
	t.Helper()
	active, err := s.LoadActive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return active
}

func oneRow() datasource.Result {
	return datasource.Result{Data: []datasource.Metric{{
		Labels: []datasource.Label{{Name: "port", Value: "Ethernet1/1"}},
		Values: []float64{1},
	}}}
}

func stateGroup(q datasource.Querier, forDur time.Duration) *Group {
	return NewGroup(config.Group{Name: "g", Rules: []config.Rule{{
		Alert: "PortDown", Expr: "SELECT port", For: config.Duration{D: forDur}, ID: 1,
	}}}, &fakeQuerierBuilder{q: q}, GroupOptions{DefaultInterval: time.Minute})
}

// saveAll stands in for the state chalert saves at shutdown.
func saveAll(t *testing.T, g *Group, store StateStore) {
	t.Helper()
	if err := store.Save(context.Background(), g.Rules[0].GetAlerts()); err != nil {
		t.Fatal(err)
	}
}

// A firing alert that resolves is saved inactive when it resolves, so a
// restart that follows without a shutdown save does not load it again.
func TestResolvedAlertIsNotRestoredAfterAKill(t *testing.T) {
	ctx, store := context.Background(), newStateStore()
	q := &fakeQuerier{results: oneRow()}
	g := stateGroup(q, 0)
	g.evaluate(ctx, time.Now(), nil, store, nil)
	saveAll(t, g, store)
	if got := store.loadActive(t); len(got) != 1 || got[0].State != StateFiring {
		t.Fatalf("after the first shutdown, active = %+v, want the firing instance", got)
	}

	q.results = datasource.Result{}
	g.evaluate(ctx, time.Now(), nil, store, nil)

	if got := store.loadActive(t); len(got) != 0 {
		t.Errorf("after the resolve and a kill, active = %+v, want none", got)
	}
}

// A pending alert the query stops returning is saved inactive when it is
// dropped.
func TestDroppedPendingAlertIsNotRestoredAfterAKill(t *testing.T) {
	ctx, store := context.Background(), newStateStore()
	q := &fakeQuerier{results: oneRow()}
	g := stateGroup(q, time.Hour)
	g.evaluate(ctx, time.Now(), nil, store, nil)
	saveAll(t, g, store)
	if got := store.loadActive(t); len(got) != 1 || got[0].State != StatePending {
		t.Fatalf("after the first shutdown, active = %+v, want the pending instance", got)
	}

	q.results = datasource.Result{}
	g.evaluate(ctx, time.Now(), nil, store, nil)

	if got := store.loadActive(t); len(got) != 0 {
		t.Errorf("after the drop and a kill, active = %+v, want none", got)
	}
}

// An alert the query still returns stays active in the saved state.
func TestFiringAlertStaysRestorable(t *testing.T) {
	ctx, store := context.Background(), newStateStore()
	g := stateGroup(&fakeQuerier{results: oneRow()}, 0)
	g.evaluate(ctx, time.Now(), nil, store, nil)
	saveAll(t, g, store)
	g.evaluate(ctx, time.Now(), nil, store, nil)

	if got := store.loadActive(t); len(got) != 1 || got[0].State != StateFiring {
		t.Errorf("active = %+v, want the firing instance", got)
	}
}

// A restored pending instance whose rule no longer exists is saved
// inactive, so the next restart does not load it again.
func TestUnclaimedPendingAlertIsRetired(t *testing.T) {
	ctx, store := context.Background(), newStateStore()
	if err := store.Save(ctx, []AlertInstance{{ID: 7, RuleID: 99, State: StatePending}}); err != nil {
		t.Fatal(err)
	}
	g := stateGroup(&fakeQuerier{}, 0)
	restored := store.loadActive(t)
	g.RestoreState(restored)

	NewRetirements(store).Resolve(ctx, time.Now(), Unclaimed(restored, map[uint64]*Group{g.ID(): g}))

	if got := store.loadActive(t); len(got) != 0 {
		t.Errorf("active = %+v, want none", got)
	}
}

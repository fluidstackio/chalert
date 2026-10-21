package rule

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
)

// recordingStore captures each RecordHistory call.
type recordingStore struct {
	mu    sync.Mutex
	calls []historyCall
	got   chan struct{}
}

type historyCall struct {
	at        time.Time
	instances []AlertInstance
}

func newRecordingStore() *recordingStore { return &recordingStore{got: make(chan struct{}, 16)} }

func (s *recordingStore) Save(context.Context, []AlertInstance) error { return nil }

func (s *recordingStore) LoadActive(context.Context) ([]AlertInstance, error) { return nil, nil }

func (s *recordingStore) RecordHistory(_ context.Context, at time.Time, instances []AlertInstance) error {
	s.mu.Lock()
	s.calls = append(s.calls, historyCall{at: at, instances: append([]AlertInstance(nil), instances...)})
	s.mu.Unlock()
	select {
	case s.got <- struct{}{}:
	default:
	}
	return nil
}

// waitFor returns the first call that match accepts, failing the test after
// a few seconds.
func (s *recordingStore) waitFor(t *testing.T, match func(historyCall) bool) historyCall {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		s.mu.Lock()
		for _, c := range s.calls {
			if match(c) {
				s.mu.Unlock()
				return c
			}
		}
		s.mu.Unlock()
		select {
		case <-s.got:
		case <-deadline:
			t.Fatal("no matching history write")
		}
	}
}

// ofRule reports whether every instance of a call belongs to rule id and has
// state st, and there is at least one.
func ofRule(id uint64, st AlertState) func(historyCall) bool {
	return func(c historyCall) bool {
		if len(c.instances) == 0 {
			return false
		}
		for _, a := range c.instances {
			if a.RuleID != id || a.State != st {
				return false
			}
		}
		return true
	}
}

func firingRule(id uint64) config.Rule { return config.Rule{Alert: "Two", Expr: exprTwoRows, ID: id} }

// startGroup runs g until the test ends and waits for its first evaluation's
// firing rows.
func startGroup(t *testing.T, g *Group, store *recordingStore, log EvaluationRecorder) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.Start(ctx, nil, store, log); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	store.waitFor(t, ofRule(g.Rules[0].ID(), StateFiring))
}

// One clock: an evaluation's history rows carry the time its log line
// carries, the start of the group's evaluation, so a reader bounding a
// window by that evaluation's time holds its rows.
func TestHistoryRowsCarryTheEvaluationTime(t *testing.T) {
	store, log := newRecordingStore(), &recordingLog{got: make(chan struct{}, 1)}
	g := evalGroup(1, firingRule(1), config.Rule{Alert: "None", Expr: exprNoRows, ID: 2})
	startGroup(t, g, store, log)
	<-log.got
	call := store.waitFor(t, ofRule(1, StateFiring))
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, e := range log.evals[0] {
		if !e.EvaluatedAt.Equal(call.at) {
			t.Errorf("rule %d evaluated at %s, its history rows at %s", e.RuleID, e.EvaluatedAt, call.at)
		}
	}
}

// A reload that changes a rule's ID drops the old rule: each instance it
// held firing gets a resolved row, so its history does not stay firing.
func TestUpdateResolvesADroppedRulesFiringInstances(t *testing.T) {
	store := newRecordingStore()
	g := evalGroup(1, firingRule(1))
	startGroup(t, g, store, nil)
	g.UpdateWith(evalGroup(1, firingRule(2)))
	call := store.waitFor(t, ofRule(1, StateInactive))
	if len(call.instances) != 2 {
		t.Errorf("resolved %d instances of the dropped rule, want 2", len(call.instances))
	}
}

// A kept rule's instances are not resolved by a reload.
func TestUpdateResolvesNothingForAKeptRule(t *testing.T) {
	store := newRecordingStore()
	g := evalGroup(1, firingRule(1))
	startGroup(t, g, store, nil)
	g.UpdateWith(evalGroup(1, firingRule(1), config.Rule{Alert: "None", Expr: exprNoRows, ID: 3}))
	time.Sleep(200 * time.Millisecond)
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, c := range store.calls {
		for _, a := range c.instances {
			if a.State == StateInactive {
				t.Errorf("reload resolved %+v of a kept rule", a)
			}
		}
	}
}

// A group a reload removes is closed, then retired: each instance its rules
// held firing gets a resolved row.
func TestRetireResolvesARemovedGroupsFiringInstances(t *testing.T) {
	store := newRecordingStore()
	g := evalGroup(1, firingRule(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go g.Start(ctx, nil, store, nil)
	store.waitFor(t, ofRule(1, StateFiring))
	g.Close()
	g.Retire(ctx)
	call := store.waitFor(t, ofRule(1, StateInactive))
	if len(call.instances) != 2 {
		t.Errorf("resolved %d instances, want 2", len(call.instances))
	}
}

// At startup, restored firing instances whose rule ID no rule carries are
// unclaimed; pending ones wrote no firing row and are left out.
func TestUnclaimedIsTheFiringStateOfNoRule(t *testing.T) {
	g := evalGroup(1, firingRule(1))
	got := Unclaimed([]AlertInstance{
		{ID: 10, RuleID: 1, State: StateFiring},
		{ID: 11, RuleID: 9, State: StateFiring},
		{ID: 12, RuleID: 9, State: StatePending},
	}, map[uint64]*Group{g.ID(): g})
	if len(got) != 1 || got[0].ID != 11 {
		t.Errorf("unclaimed = %+v, want only instance 11", got)
	}
}

// flakyStore fails its first fail writes of resolved rows; it records only
// writes that succeed.
type flakyStore struct {
	*recordingStore
	mu   sync.Mutex
	fail int
}

func (s *flakyStore) RecordHistory(ctx context.Context, at time.Time, instances []AlertInstance) error {
	s.mu.Lock()
	if s.fail > 0 && len(instances) > 0 && instances[0].State == StateInactive {
		s.fail--
		s.mu.Unlock()
		return errors.New("insert failed")
	}
	s.mu.Unlock()
	return s.recordingStore.RecordHistory(ctx, at, instances)
}

func fastGroup(name string, opts GroupOptions, rules ...config.Rule) *Group {
	opts.DefaultInterval = time.Minute
	return NewGroup(config.Group{Name: name, Interval: config.Duration{D: 20 * time.Millisecond}, Concurrency: 1, Rules: rules},
		&fakeQuerierBuilder{q: evalQuerier()}, opts)
}

// EV-42: a retirement whose write fails is kept, counted, and written by a
// later flush; a pending instance needs no row.
func TestRetirementsKeepAFailedWriteUntilAFlushLandsIt(t *testing.T) {
	store := &flakyStore{recordingStore: newRecordingStore(), fail: 1}
	r := NewRetirements(store)
	at := time.Now()
	r.Resolve(context.Background(), at, []AlertInstance{
		{ID: 1, RuleID: 9, State: StateFiring}, {ID: 2, RuleID: 9, State: StateFiring}, {ID: 3, RuleID: 9, State: StatePending},
	})
	if got := r.Pending(); got != 2 {
		t.Fatalf("pending = %d after a failed write, want 2", got)
	}
	r.Flush(context.Background())
	call := store.waitFor(t, ofRule(9, StateInactive))
	if len(call.instances) != 2 || !call.at.Equal(at) || r.Pending() != 0 {
		t.Errorf("flush wrote %d rows at %s, pending %d; want 2 at %s, 0 pending", len(call.instances), call.at, r.Pending(), at)
	}
}

// EV-42: a dropped rule's resolution that fails is written by the group's
// next evaluation.
func TestADroppedRulesFailedResolutionIsRetried(t *testing.T) {
	store := &flakyStore{recordingStore: newRecordingStore(), fail: 1}
	g := fastGroup("g", GroupOptions{}, firingRule(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go g.Start(ctx, nil, store, nil)
	store.waitFor(t, ofRule(1, StateFiring))
	g.UpdateWith(fastGroup("g", GroupOptions{}, firingRule(2)))
	store.waitFor(t, ofRule(1, StateInactive))
	g.Close()
}

// EV-42: a removed group's retirement that fails is written by another
// group's evaluation, since the removed group runs no more.
func TestARemovedGroupsFailedResolutionIsRetriedByAnotherGroup(t *testing.T) {
	store := &flakyStore{recordingStore: newRecordingStore(), fail: 1}
	opts := GroupOptions{Retirements: NewRetirements(store)}
	removed, other := fastGroup("removed", opts, firingRule(1)), fastGroup("other", opts, config.Rule{Alert: "None", Expr: exprNoRows, ID: 5})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go removed.Start(ctx, nil, store, nil)
	store.waitFor(t, ofRule(1, StateFiring))
	removed.Close()
	removed.Retire(ctx)
	go other.Start(ctx, nil, store, nil)
	store.waitFor(t, ofRule(1, StateInactive))
	other.Close()
}

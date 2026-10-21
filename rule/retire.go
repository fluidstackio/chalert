package rule

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/garbett1/chalert/metrics"
)

// Retirements writes the resolved history rows of retired rules: a rule an
// edit dropped on reload, the rules of a group a reload removed, and
// restored instances no rule claims after a restart. A rule that stopped
// running leaves no evaluation to resolve its instances, and without these
// rows each instance's latest history row stays firing for good. Nothing is
// sent to the notifiers: an edited rule usually fires the same label set
// under its new ID, so a resolve sent for the old one would end the alert
// the new one keeps.
//
// A write that fails is kept and retried by Flush, which every group's
// evaluation calls, so a removed group's rows are written by the groups
// that remain. The queue is held in memory: rows still pending when chalert
// stops are lost, and only the instances that the alert state saved at
// shutdown still holds are resolved after the restart (Unclaimed).
type Retirements struct {
	store StateStore

	mu      sync.Mutex
	pending []retirement
}

// retirement is one write of resolved rows, stamped at.
type retirement struct {
	at   time.Time
	rows []AlertInstance
}

// NewRetirements writes retirements to store; a nil store writes nothing.
func NewRetirements(store StateStore) *Retirements { return &Retirements{store: store} }

// Resolve writes a resolved row, stamped at, for each firing instance, and
// keeps the write for Flush when it fails. Pending instances wrote no
// firing row and need none.
func (r *Retirements) Resolve(ctx context.Context, at time.Time, instances []AlertInstance) {
	var rows []AlertInstance
	for _, inst := range instances {
		if inst.State != StateFiring {
			continue
		}
		inst.State = StateInactive
		inst.ResolvedAt = at
		rows = append(rows, inst)
	}
	if r.store == nil || len(rows) == 0 {
		return
	}
	r.mu.Lock()
	r.pending = append(r.pending, retirement{at: at, rows: rows})
	r.mu.Unlock()
	r.Flush(ctx)
}

// Flush writes the pending retirements in order, keeping each that fails.
func (r *Retirements) Flush(ctx context.Context) {
	if r == nil || r.store == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 {
		return
	}
	var kept []retirement
	for _, p := range r.pending {
		if err := r.store.RecordHistory(ctx, p.at, p.rows); err != nil {
			slog.Error("chalert resolved rows for retired rules failed; kept for retry", "count", len(p.rows), "error", err)
			kept = append(kept, p)
			continue
		}
		slog.Info("chalert resolved the firing instances of retired rules", "count", len(p.rows))
	}
	r.pending = kept
	metrics.RetiredRowsPending.Set(float64(countRows(kept)))
}

// Pending is the number of resolved rows not yet written.
func (r *Retirements) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return countRows(r.pending)
}

func countRows(ps []retirement) int {
	n := 0
	for _, p := range ps {
		n += len(p.rows)
	}
	return n
}

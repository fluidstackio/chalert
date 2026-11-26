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
// that remain. Rows still unwritten at shutdown go into the alert state
// saved then, as the instances they were (Unwritten), so the restart finds
// them unclaimed and resolves them. Once a write lands, the instances' state
// is saved inactive, so no later restart resolves them again. A pending
// instance wrote no firing row and gets no history row, but its state is
// saved inactive too, or every restart would load it again.
type Retirements struct {
	store StateStore

	mu      sync.Mutex
	pending []retirement
}

// retirement is one write, stamped at: the resolved history rows, the
// inactive state of every retired instance, and the instances as they were.
type retirement struct {
	at       time.Time
	rows     []AlertInstance
	inactive []AlertInstance
	was      []AlertInstance
}

// NewRetirements writes retirements to store; a nil store writes nothing.
func NewRetirements(store StateStore) *Retirements { return &Retirements{store: store} }

// Resolve writes a resolved row, stamped at, for each firing instance and
// saves every pending or firing instance inactive, keeping the write for
// Flush when it fails. Pending instances wrote no firing row and get no
// resolved row.
func (r *Retirements) Resolve(ctx context.Context, at time.Time, instances []AlertInstance) {
	var rows, inactive, was []AlertInstance
	for _, inst := range instances {
		if inst.State != StateFiring && inst.State != StatePending {
			continue
		}
		was = append(was, inst)
		firing := inst.State == StateFiring
		inst.State = StateInactive
		inst.ResolvedAt = at
		inactive = append(inactive, inst)
		if firing {
			rows = append(rows, inst)
		}
	}
	if r.store == nil || len(inactive) == 0 {
		return
	}
	r.mu.Lock()
	r.pending = append(r.pending, retirement{at: at, rows: rows, inactive: inactive, was: was})
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
		if len(p.rows) > 0 {
			if err := r.store.RecordHistory(ctx, p.at, p.rows); err != nil {
				slog.Error("chalert resolved rows for retired rules failed; kept for retry", "count", len(p.rows), "error", err)
				kept = append(kept, p)
				continue
			}
			slog.Info("chalert resolved the firing instances of retired rules", "count", len(p.rows))
		}
		// The saved state holds the retired instances as pending or firing
		// until it is told otherwise; a failure here only resolves them
		// again at the next restart.
		if err := r.store.Save(ctx, p.inactive); err != nil {
			slog.Error("chalert saving retired instances as inactive failed", "count", len(p.inactive), "error", err)
		}
	}
	r.pending = kept
	metrics.RetiredRowsPending.Set(float64(countRows(kept)))
}

// Unwritten returns, as they were, the instances whose retirement is not
// yet written.
func (r *Retirements) Unwritten() []AlertInstance {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []AlertInstance
	for _, p := range r.pending {
		out = append(out, p.was...)
	}
	return out
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

package rule

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
	"github.com/garbett1/chalert/metrics"
)

// Group is a collection of alerting rules that share an evaluation interval.
//
// The group runs a ticker that evaluates all rules concurrently (up to Concurrency),
// sends notifications for firing/resolved alerts, and persists state changes.
//
// # Lifecycle
//
//	NewGroup() → Init() → Start(ctx) → [running] → Close()
//
// The group can be live-updated via UpdateWith() without stopping the evaluation loop.
type Group struct {
	mu sync.RWMutex

	id          uint64
	Name        string
	File        string
	Rules       []*AlertingRule
	Interval    time.Duration
	Concurrency int
	Limit       int
	EvalDelay   *time.Duration
	Labels      map[string]string
	checksum    string

	doneCh     chan struct{}
	finishedCh chan struct{}
	updateCh   chan *Group
}

// GroupOptions holds parameters for constructing a Group that come from
// global flags rather than per-group YAML configuration.
type GroupOptions struct {
	// DefaultInterval is the fallback evaluation interval when the group
	// config does not specify one.
	DefaultInterval time.Duration

	// ExternalLabels are applied to every alert with lowest priority.
	// Group labels override external labels; rule labels override group labels.
	ExternalLabels map[string]string

	// DefaultLimit caps alert instances per rule when the group config
	// does not specify a limit. 0 means unlimited.
	DefaultLimit int
}

// NewGroup creates a new group from config.
func NewGroup(cfg config.Group, qb datasource.QuerierBuilder, opts GroupOptions) *Group {
	interval := cfg.Interval.Duration()
	if interval == 0 {
		interval = opts.DefaultInterval
	}

	g := &Group{
		Name:        cfg.Name,
		File:        cfg.File,
		Interval:    interval,
		Concurrency: cfg.Concurrency,
		Labels:      cfg.Labels,
		checksum:    cfg.Checksum,
		doneCh:      make(chan struct{}),
		finishedCh:  make(chan struct{}),
		updateCh:    make(chan *Group),
	}
	if g.Concurrency < 1 {
		g.Concurrency = 1
	}
	if cfg.Limit != nil {
		g.Limit = *cfg.Limit
	} else if opts.DefaultLimit > 0 {
		g.Limit = opts.DefaultLimit
	}
	if cfg.EvalDelay != nil {
		d := cfg.EvalDelay.Duration()
		g.EvalDelay = &d
	}

	// Merge external labels with group labels (group labels take precedence)
	mergedLabels := make(map[string]string)
	for k, v := range opts.ExternalLabels {
		mergedLabels[k] = v
	}
	for k, v := range cfg.Labels {
		mergedLabels[k] = v
	}

	rules := make([]*AlertingRule, 0, len(cfg.Rules))
	for _, r := range cfg.Rules {
		if !r.IsAlerting() {
			// TODO: recording rules will be handled by MV manager
			slog.Warn("chalert: recording rules not yet supported, skipping",
				"rule", r.Name(), "group", cfg.Name)
			continue
		}
		// Apply merged labels to rule config
		if len(mergedLabels) > 0 {
			if r.Labels == nil {
				r.Labels = make(map[string]string)
			}
			for k, v := range mergedLabels {
				if _, ok := r.Labels[k]; !ok {
					r.Labels[k] = v
				}
			}
		}
		rules = append(rules, NewAlertingRule(qb, cfg.Name, interval, r))
	}
	g.Rules = rules

	// Compute group ID from file + name + interval
	h := fnv.New64a()
	h.Write([]byte(g.File))
	h.Write([]byte("\xff"))
	h.Write([]byte(g.Name))
	h.Write([]byte(g.Interval.String()))
	g.id = h.Sum64()

	return g
}

// ID returns the unique group identifier.
func (g *Group) ID() uint64 { return g.id }

// Checksum returns the config checksum for change detection.
func (g *Group) Checksum() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.checksum
}

// Start begins the evaluation loop. Blocks until ctx is cancelled or Close() is called.
func (g *Group) Start(ctx context.Context, notifier Notifier, store StateStore, evalLog EvaluationRecorder) {
	defer close(g.finishedCh)

	slog.Info("chalert group started",
		"group", g.Name,
		"interval", g.Interval,
		"rules", len(g.Rules),
		"concurrency", g.Concurrency)

	eval := func(ctx context.Context, ts time.Time) {
		g.mu.RLock()
		rules := g.Rules
		concurrency := g.Concurrency
		limit := g.Limit
		g.mu.RUnlock()

		if len(rules) == 0 {
			return
		}

		// Adjust timestamp for eval delay
		evalTS := ts
		if g.EvalDelay != nil {
			evalTS = ts.Add(-*g.EvalDelay)
		}

		evalStart := time.Now()
		slog.Info("chalert group eval started",
			"group", g.Name,
			"rules", len(rules),
			"eval_ts", evalTS)

		allAlerts, evals := g.execRules(ctx, rules, evalTS, evalStart, concurrency, limit)

		var firing, resolved int
		for _, a := range allAlerts {
			switch a.State {
			case StateFiring:
				firing++
			case StateInactive:
				resolved++
			}
		}
		slog.Info("chalert group eval complete",
			"group", g.Name,
			"duration", time.Since(evalStart),
			"alerts_to_send", len(allAlerts),
			"firing", firing,
			"resolved", resolved)

		// Send notifications
		if notifier != nil && len(allAlerts) > 0 {
			slog.Info("chalert sending notifications",
				"group", g.Name,
				"firing", firing,
				"resolved", resolved)
			if err := notifier.Send(ctx, allAlerts); err != nil {
				slog.Error("chalert notification failed",
					"group", g.Name, "error", err)
			} else {
				slog.Info("chalert notifications sent successfully",
					"group", g.Name,
					"firing", firing,
					"resolved", resolved)
			}
		}

		// Persist state
		if store != nil && len(allAlerts) > 0 {
			if err := store.RecordHistory(ctx, evalStart, allAlerts); err != nil {
				slog.Error("chalert state persistence failed",
					"group", g.Name, "error", err)
			}
		}
		// Every evaluation is recorded, whether or not it produced alerts.
		if evalLog != nil && len(evals) > 0 {
			evalLog.RecordEvaluations(evals)
		}
	}

	// First evaluation immediately
	eval(ctx, time.Now())

	ticker := time.NewTicker(g.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.doneCh:
			return
		case ng := <-g.updateCh:
			g.mu.Lock()
			dropped := g.updateWith(ng)
			g.mu.Unlock()
			slog.Info("chalert group updated", "group", g.Name)
			recordRetired(ctx, store, g.Name, dropped)
		case ts := <-ticker.C:
			eval(ctx, ts)
		}
	}
}

// execRules evaluates rules with the given concurrency. It returns the
// alerts to send and one Evaluation per rule evaluated, each stamped at, the
// start of the group's evaluation, which its history rows carry too.
func (g *Group) execRules(ctx context.Context, rules []*AlertingRule, ts, at time.Time, concurrency, limit int) ([]AlertInstance, []Evaluation) {
	if concurrency <= 1 {
		var all []AlertInstance
		evals := make([]Evaluation, 0, len(rules))
		for _, r := range rules {
			start := time.Now()
			alerts, panicked, err := g.execRecovered(ctx, r, ts, limit)
			duration := time.Since(start)
			evals = append(evals, g.evaluation(r, at, duration, r.lastMatches(), err))
			if panicked {
				continue
			}
			metrics.RuleEvalDuration.WithLabelValues(g.Name, r.Name()).Observe(duration.Seconds())
			if err != nil {
				metrics.RuleEvalErrors.WithLabelValues(g.Name, r.Name()).Inc()
				slog.Error("chalert rule exec failed",
					"rule", r.Name(), "group", g.Name, "error", err)
				continue
			}
			metrics.RuleEvalSamples.WithLabelValues(g.Name, r.Name()).Set(float64(len(alerts)))
			all = append(all, alerts...)
		}
		return all, evals
	}

	// Concurrent evaluation
	type result struct {
		alerts []AlertInstance
		// samples is the query's match count, pending or firing.
		samples  int
		err      error
		r        *AlertingRule
		duration time.Duration
		// panicked marks a result sent from the recover path, whose error
		// metric the recover path already counted.
		panicked bool
	}
	results := make(chan result, len(rules))
	sem := make(chan struct{}, concurrency)

	var wg sync.WaitGroup
	for _, r := range rules {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("chalert panic in rule exec",
						"rule", r.Name(), "group", g.Name, "panic", rec)
					metrics.RuleEvalErrors.WithLabelValues(g.Name, r.Name()).Inc()
					results <- result{err: fmt.Errorf("panic: %v", rec), r: r, duration: time.Since(start), panicked: true}
				}
			}()
			sem <- struct{}{}
			defer func() { <-sem }()

			start = time.Now()
			alerts, err := r.Exec(ctx, ts, limit)
			results <- result{alerts: alerts, samples: r.lastMatches(), err: err, r: r, duration: time.Since(start)}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var all []AlertInstance
	evals := make([]Evaluation, 0, len(rules))
	for res := range results {
		evals = append(evals, g.evaluation(res.r, at, res.duration, res.samples, res.err))
		if res.panicked {
			continue
		}
		metrics.RuleEvalDuration.WithLabelValues(g.Name, res.r.Name()).Observe(res.duration.Seconds())
		if res.err != nil {
			metrics.RuleEvalErrors.WithLabelValues(g.Name, res.r.Name()).Inc()
			slog.Error("chalert rule exec failed",
				"rule", res.r.Name(), "group", g.Name, "error", res.err)
			continue
		}
		metrics.RuleEvalSamples.WithLabelValues(g.Name, res.r.Name()).Set(float64(len(res.alerts)))
		all = append(all, res.alerts...)
	}
	return all, evals
}

// execRecovered runs one rule's Exec on the sequential path and turns a
// panic into a failed result, as the concurrent path's recover does, so the
// panicking rule's evaluation is still recorded and the other rules run.
func (g *Group) execRecovered(ctx context.Context, r *AlertingRule, ts time.Time, limit int) (alerts []AlertInstance, panicked bool, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("chalert panic in rule exec",
				"rule", r.Name(), "group", g.Name, "panic", rec)
			metrics.RuleEvalErrors.WithLabelValues(g.Name, r.Name()).Inc()
			alerts, panicked, err = nil, true, fmt.Errorf("panic: %v", rec)
		}
	}()
	alerts, err = r.Exec(ctx, ts, limit)
	return alerts, false, err
}

// evaluation builds one rule's Evaluation from its Exec outcome.
func (g *Group) evaluation(r *AlertingRule, at time.Time, duration time.Duration, samples int, err error) Evaluation {
	e := Evaluation{
		RuleID: r.ID(), GroupName: g.Name, AlertName: r.Name(),
		EvaluatedAt: at, Duration: duration, OK: err == nil,
	}
	if err != nil {
		e.Error = err.Error()
	} else {
		e.Samples = samples
	}
	return e
}

// updateWith replaces the group's rules with those from newGroup and returns
// the rules it dropped: those whose ID no new rule carries, since an edit to
// a rule's query, name or labels changes its ID. Must be called with mu held.
func (g *Group) updateWith(ng *Group) []*AlertingRule {
	// Build registry of new rules by ID
	newRules := make(map[uint64]*AlertingRule)
	for _, r := range ng.Rules {
		newRules[r.ID()] = r
	}

	// Update existing rules or mark for removal
	var kept, dropped []*AlertingRule
	for _, old := range g.Rules {
		if nr, ok := newRules[old.ID()]; ok {
			// Rule still exists — preserve alert state, update expression
			old.mu.Lock()
			old.expr = nr.expr
			old.forDur = nr.forDur
			old.keepFire = nr.keepFire
			old.labels = nr.labels
			old.annTpls = nr.annTpls
			old.debug = nr.debug
			old.querier = nr.querier
			old.mu.Unlock()
			delete(newRules, nr.ID())
			kept = append(kept, old)
			continue
		}
		dropped = append(dropped, old)
	}

	// Add genuinely new rules
	for _, nr := range newRules {
		kept = append(kept, nr)
	}

	g.Rules = kept
	g.Concurrency = ng.Concurrency
	g.Labels = ng.Labels
	g.Limit = ng.Limit
	g.checksum = ng.checksum
	return dropped
}

// Retire records a resolved history row for each instance the group's rules
// hold firing. Called after Close on a group a reload removed; a shutdown
// closes groups without retiring them, since their rules run again on start.
func (g *Group) Retire(ctx context.Context, store StateStore) {
	g.mu.RLock()
	rules := g.Rules
	g.mu.RUnlock()
	recordRetired(ctx, store, g.Name, rules)
}

// Unclaimed returns the firing instances among a restart's restored state
// whose rule ID matches no rule of the groups: a rule edited or removed
// while chalert was down. Pending instances wrote no firing row and are
// left out.
func Unclaimed(instances []AlertInstance, groups map[uint64]*Group) []AlertInstance {
	ids := map[uint64]bool{}
	for _, g := range groups {
		for _, r := range g.Rules {
			ids[r.ID()] = true
		}
	}
	var out []AlertInstance
	for _, inst := range instances {
		if inst.State == StateFiring && !ids[inst.RuleID] {
			out = append(out, inst)
		}
	}
	return out
}

// RecordResolved writes a resolved history row, stamped at, for each firing
// instance: a rule that stopped running leaves no evaluation to resolve its
// instances, and without the row each one's latest history row stays firing
// for good. Nothing is sent to the notifiers: an edited rule usually fires
// the same label set under its new ID, so a resolve sent for the old one
// would end the alert the new one keeps.
func RecordResolved(ctx context.Context, store StateStore, at time.Time, instances []AlertInstance) {
	var resolved []AlertInstance
	for _, inst := range instances {
		if inst.State != StateFiring {
			continue
		}
		inst.State = StateInactive
		inst.ResolvedAt = at
		resolved = append(resolved, inst)
	}
	if store == nil || len(resolved) == 0 {
		return
	}
	if err := store.RecordHistory(ctx, at, resolved); err != nil {
		slog.Error("chalert resolved rows for retired rules failed", "count", len(resolved), "error", err)
		return
	}
	slog.Info("chalert resolved the firing instances of retired rules", "count", len(resolved))
}

// recordRetired resolves every firing instance of rules that stopped running.
func recordRetired(ctx context.Context, store StateStore, group string, rules []*AlertingRule) {
	var all []AlertInstance
	for _, r := range rules {
		all = append(all, r.GetAlerts()...)
	}
	if len(all) > 0 {
		slog.Info("chalert retiring rules", "group", group, "rules", len(rules))
	}
	RecordResolved(ctx, store, time.Now(), all)
}

// Close stops the evaluation loop.
func (g *Group) Close() {
	close(g.doneCh)
	<-g.finishedCh
}

// UpdateWith sends a new group definition to the running evaluation loop.
func (g *Group) UpdateWith(ng *Group) {
	g.updateCh <- ng
}

// RestoreState loads previously persisted alert state for all rules.
func (g *Group) RestoreState(instances []AlertInstance) {
	for _, r := range g.Rules {
		r.Restore(instances)
	}
}

func (g *Group) String() string {
	return fmt.Sprintf("group %q (%d rules, interval=%s)", g.Name, len(g.Rules), g.Interval)
}

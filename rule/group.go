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
func (g *Group) Start(ctx context.Context, notifier Notifier, store StateStore) {
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

		allAlerts, evals := g.execRules(ctx, rules, evalTS, concurrency, limit)

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
			if err := store.RecordHistory(ctx, allAlerts); err != nil {
				slog.Error("chalert state persistence failed",
					"group", g.Name, "error", err)
			}
		}
		// Every evaluation is recorded, whether or not it produced alerts.
		if store != nil && len(evals) > 0 {
			if err := store.RecordEvaluations(ctx, evals); err != nil {
				slog.Error("chalert evaluation log write failed",
					"group", g.Name, "error", err)
			}
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
			g.updateWith(ng)
			g.mu.Unlock()
			slog.Info("chalert group updated", "group", g.Name)
		case ts := <-ticker.C:
			eval(ctx, ts)
		}
	}
}

// execRules evaluates rules with the given concurrency. It returns the
// alerts to send and one Evaluation per rule evaluated.
func (g *Group) execRules(ctx context.Context, rules []*AlertingRule, ts time.Time, concurrency, limit int) ([]AlertInstance, []Evaluation) {
	if concurrency <= 1 {
		var all []AlertInstance
		evals := make([]Evaluation, 0, len(rules))
		for _, r := range rules {
			start := time.Now()
			alerts, err := r.Exec(ctx, ts, limit)
			duration := time.Since(start)
			metrics.RuleEvalDuration.WithLabelValues(g.Name, r.Name()).Observe(duration.Seconds())
			evals = append(evals, g.evaluation(r, start, duration, len(alerts), err))
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
		alerts   []AlertInstance
		err      error
		r        *AlertingRule
		start    time.Time
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
					results <- result{err: fmt.Errorf("panic: %v", rec), r: r, start: start, duration: time.Since(start), panicked: true}
				}
			}()
			sem <- struct{}{}
			defer func() { <-sem }()

			start = time.Now()
			alerts, err := r.Exec(ctx, ts, limit)
			results <- result{alerts: alerts, err: err, r: r, start: start, duration: time.Since(start)}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var all []AlertInstance
	evals := make([]Evaluation, 0, len(rules))
	for res := range results {
		evals = append(evals, g.evaluation(res.r, res.start, res.duration, len(res.alerts), res.err))
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

// evaluation builds one rule's Evaluation from its Exec outcome.
func (g *Group) evaluation(r *AlertingRule, start time.Time, duration time.Duration, samples int, err error) Evaluation {
	e := Evaluation{
		RuleID: r.ID(), GroupName: g.Name, AlertName: r.Name(),
		EvaluatedAt: start, Duration: duration, OK: err == nil,
	}
	if err != nil {
		e.Error = err.Error()
	} else {
		e.Samples = samples
	}
	return e
}

// updateWith replaces the group's rules with those from newGroup.
// Must be called with mu held.
func (g *Group) updateWith(ng *Group) {
	// Build registry of new rules by ID
	newRules := make(map[uint64]*AlertingRule)
	for _, r := range ng.Rules {
		newRules[r.ID()] = r
	}

	// Update existing rules or mark for removal
	var kept []*AlertingRule
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
		}
		// If not in newRules, old rule is dropped (its alerts will naturally expire)
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

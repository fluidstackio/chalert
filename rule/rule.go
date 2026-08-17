// Package rule implements the alert evaluation engine.
//
// # Design Decisions
//
// The rule engine is structurally based on vmalert's rule package. We preserve the
// same Group → Rule hierarchy and the same alert state machine (Inactive → Pending → Firing)
// because these are well-tested patterns covering many edge cases.
//
// Key differences from vmalert:
//   - No prompb dependency. We use our own datasource.Label and AlertInstance types.
//   - No remote write of ALERTS/ALERTS_FOR_STATE time series. Alert state is persisted
//     to ClickHouse via the StateStore interface instead.
//   - No recording rule time series output. Recording rules will be implemented as
//     optional ClickHouse MV management (future work).
//
// # Alert State Machine
//
//	Inactive ──(expr matches)──→ Pending ──(for elapsed)──→ Firing
//	    ↑                            │                         │
//	    │                            │                         │
//	    └──(expr stops matching)─────┘                         │
//	    ↑                                                      │
//	    └──(keep_firing_for elapsed)───────────────────────────┘
//
// The state machine is evaluated on every tick of the group's interval.
// An alert instance is identified by the hash of its dimension labels.
package rule

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
)

// AlertState represents the state of an alert instance.
type AlertState int

const (
	StateInactive AlertState = iota
	StatePending
	StateFiring
)

func (s AlertState) String() string {
	switch s {
	case StateFiring:
		return "firing"
	case StatePending:
		return "pending"
	}
	return "inactive"
}

// AlertInstance represents a single instance of a firing/pending alert.
// An alert rule can produce many instances — one per unique set of dimension labels.
//
// For example, an alert on "error rate by service" with 3 services in violation
// produces 3 AlertInstances, each with different Labels.
type AlertInstance struct {
	// ID is a hash of the alert's labels, used for deduplication across evaluations.
	ID uint64

	// RuleID links back to the parent rule.
	RuleID uint64

	// GroupName and AlertName identify the parent group and rule.
	GroupName string
	AlertName string

	// Labels are the dimension key-value pairs from the query result that
	// identify this particular alert instance.
	Labels map[string]string

	// Annotations are the rendered annotation templates for this instance.
	Annotations map[string]string

	// State is the current state of this alert instance.
	State AlertState

	// Value is the numeric value from the most recent evaluation.
	Value float64

	// Expr is the SQL expression that generated this alert.
	Expr string

	// ActiveAt is when this alert instance first entered Pending state.
	ActiveAt time.Time

	// FiredAt is when this alert instance transitioned to Firing.
	FiredAt time.Time

	// ResolvedAt is when this alert instance was resolved (expression stopped matching).
	ResolvedAt time.Time

	// LastSent is when this alert was last sent to the notifier.
	LastSent time.Time

	// KeepFiringSince tracks when the expression stopped matching but the alert
	// is kept firing due to keep_firing_for.
	KeepFiringSince time.Time

	// For is the configured pending duration before firing.
	For time.Duration

	// EvaluationInterval is the group's evaluation interval, used by
	// notifiers to compute EndsAt (4 × interval, matching vmalert).
	EvaluationInterval time.Duration
}

// Notifier is the interface for sending alert notifications.
// Kept here to avoid circular imports between rule and notifier packages.
type Notifier interface {
	Send(ctx context.Context, alerts []AlertInstance) error
}

// StateStore persists alert state for restart recovery and audit.
type StateStore interface {
	// Save persists the current state of all active alert instances.
	Save(ctx context.Context, instances []AlertInstance) error

	// LoadActive loads all Pending/Firing alert instances on startup.
	LoadActive(ctx context.Context) ([]AlertInstance, error)

	// RecordHistory writes a state transition event to the audit log.
	RecordHistory(ctx context.Context, instances []AlertInstance) error
}

// AlertingRule evaluates a SQL expression and manages alert state transitions.
type AlertingRule struct {
	mu sync.RWMutex

	ruleID    uint64
	name      string
	expr      string
	forDur    time.Duration
	keepFire  time.Duration
	labels    map[string]string // extra labels from config
	annTpls   map[string]string // annotation templates
	groupName string
	debug     bool

	querier       datasource.Querier
	groupInterval time.Duration
	alerts        map[uint64]*AlertInstance

	// lastEvaluation tracks recent evaluation state for debugging.
	lastEval     time.Time
	lastDuration time.Duration
	lastSamples  int
	lastErr      error
}

// NewAlertingRule creates a new alerting rule from config.
func NewAlertingRule(qb datasource.QuerierBuilder, groupName string, groupInterval time.Duration, cfg config.Rule) *AlertingRule {
	debug := false
	if cfg.Debug != nil {
		debug = *cfg.Debug
	}
	return &AlertingRule{
		ruleID:        cfg.ID,
		name:          cfg.Alert,
		expr:          cfg.Expr,
		forDur:        cfg.For.Duration(),
		keepFire:      cfg.KeepFiringFor.Duration(),
		labels:        cfg.Labels,
		annTpls:       cfg.Annotations,
		groupName:     groupName,
		groupInterval: groupInterval,
		debug:         debug,
		querier: qb.BuildWithParams(datasource.QuerierParams{
			EvaluationInterval: groupInterval,
			Debug:              debug,
		}),
		alerts: make(map[uint64]*AlertInstance),
	}
}

// resolvedRetention is how long resolved alerts are kept in memory
// so they can be re-sent to notifiers as resolved.
const resolvedRetention = 15 * time.Minute

// ResendDelay is the minimum interval between re-sending a firing alert
// to the notifier. This avoids flooding Alertmanager on every evaluation tick.
// Alertmanager deduplicates anyway, but reducing sends saves network/CPU.
// Set to the group interval so alerts are sent at most once per evaluation
// after the initial send, plus one immediate send on state transitions.
var ResendDelay = time.Minute

// Exec evaluates the rule expression and updates alert state.
// Returns the list of alerts that should be sent to notifiers.
func (ar *AlertingRule) Exec(ctx context.Context, ts time.Time, limit int) ([]AlertInstance, error) {
	start := time.Now()
	res, err := ar.querier.Query(ctx, ar.expr, ts)
	duration := time.Since(start)

	ar.mu.Lock()
	defer ar.mu.Unlock()

	ar.lastEval = start
	ar.lastDuration = duration
	ar.lastSamples = len(res.Data)
	ar.lastErr = err

	if err != nil {
		return nil, fmt.Errorf("rule %q: query failed: %w", ar.name, err)
	}

	// Early bail-out: if the query returned more rows than the limit allows,
	// reject before processing state transitions to avoid memory explosion.
	if limit > 0 && len(res.Data) > limit {
		return nil, fmt.Errorf("rule %q: query returned %d results, exceeding limit of %d", ar.name, len(res.Data), limit)
	}

	slog.Info("chalert rule eval",
		"rule", ar.name,
		"group", ar.groupName,
		"samples", len(res.Data),
		"duration", duration)

	// Clean up resolved alerts past retention
	for id, a := range ar.alerts {
		if a.State == StateInactive && ts.Sub(a.ResolvedAt) > resolvedRetention {
			delete(ar.alerts, id)
		}
	}

	// Track which alerts were seen this evaluation
	seen := make(map[uint64]struct{})

	for _, m := range res.Data {
		labels := ar.buildLabels(m)
		alertID := hashLabels(labels)

		if _, ok := seen[alertID]; ok {
			return nil, fmt.Errorf("rule %q: duplicate label set %v", ar.name, labels)
		}
		seen[alertID] = struct{}{}

		if a, ok := ar.alerts[alertID]; ok {
			// Existing alert — update value
			if a.State == StateInactive {
				// Was resolved, re-activate
				a.State = StatePending
				a.ActiveAt = ts
				a.ResolvedAt = time.Time{}
				ar.logDebug(ts, a, "INACTIVE => PENDING")
			}
			a.Value = m.Values[0]
			a.Annotations = ar.renderAnnotations(m, a)
			a.KeepFiringSince = time.Time{}
		} else {
			// New alert
			a := &AlertInstance{
				ID:          alertID,
				RuleID:      ar.ruleID,
				GroupName:   ar.groupName,
				AlertName:   ar.name,
				Labels:      labels,
				Annotations: ar.renderAnnotations(m, nil),
				State:       StatePending,
				Value:       m.Values[0],
				Expr:        ar.expr,
				ActiveAt:    ts,
				For:         ar.forDur,
			}
			ar.alerts[alertID] = a
			ar.logDebug(ts, a, "created in PENDING")
		}
	}

	// Process alerts not seen this evaluation
	var numActive int
	for id, a := range ar.alerts {
		if _, ok := seen[id]; !ok {
			switch a.State {
			case StatePending:
				// Pending alert disappeared — just delete it
				delete(ar.alerts, id)
				ar.logDebug(ts, a, "PENDING => DELETED (absent)")
				continue
			case StateFiring:
				if ar.keepFire > 0 {
					if a.KeepFiringSince.IsZero() {
						a.KeepFiringSince = ts
					}
					if ts.Sub(a.KeepFiringSince) < ar.keepFire {
						ar.logDebug(ts, a, "KEEP_FIRING for %s since %v", ar.keepFire, a.KeepFiringSince)
						numActive++
						continue
					}
				}
				a.State = StateInactive
				a.ResolvedAt = ts
				ar.logDebug(ts, a, "FIRING => INACTIVE (absent)")
				continue
			}
		} else {
			numActive++
		}

		// Transition Pending → Firing when for duration elapsed
		if a.State == StatePending && ts.Sub(a.ActiveAt) >= ar.forDur {
			a.State = StateFiring
			a.FiredAt = ts
			ar.logDebug(ts, a, "PENDING => FIRING after %s", ts.Sub(a.ActiveAt))
		}
	}

	if limit > 0 && numActive > limit {
		// Match vmalert: clear all alerts when limit is exceeded.
		ar.alerts = make(map[uint64]*AlertInstance)
		return nil, fmt.Errorf("rule %q: exceeded limit of %d with %d active alerts", ar.name, limit, numActive)
	}

	toSend := ar.alertsToSend(ts)
	if len(toSend) > 0 {
		var firing, resolved int
		for _, a := range toSend {
			switch a.State {
			case StateFiring:
				firing++
			case StateInactive:
				resolved++
			}
		}
		slog.Info("chalert rule alerts to send",
			"rule", ar.name,
			"group", ar.groupName,
			"total", len(toSend),
			"firing", firing,
			"resolved", resolved)
	}
	return toSend, nil
}

// alertsToSend returns alerts that need to be sent to notifiers.
// Firing alerts are resent only if ResendDelay has elapsed since the last send,
// or if the alert just transitioned to firing (LastSent is zero or before FiredAt).
// Resolved alerts are always sent (once).
func (ar *AlertingRule) alertsToSend(ts time.Time) []AlertInstance {
	var out []AlertInstance
	for _, a := range ar.alerts {
		if a.State == StatePending {
			continue
		}
		if a.State == StateFiring {
			// Always send on first fire; otherwise respect resend delay.
			if !a.LastSent.IsZero() && a.LastSent.After(a.FiredAt) && ts.Sub(a.LastSent) < ResendDelay {
				continue
			}
			a.LastSent = ts
			inst := *a
			inst.EvaluationInterval = ar.groupInterval
			out = append(out, inst)
		} else if a.State == StateInactive && !a.ResolvedAt.IsZero() {
			a.LastSent = ts
			inst := *a
			inst.EvaluationInterval = ar.groupInterval
			out = append(out, inst)
		}
	}
	return out
}

// buildLabels merges query result dimensions with configured extra labels.
// Extra labels take precedence; conflicting original labels get "exported_" prefix.
//
// Configured label values containing "{{" are rendered with the same template
// pipeline as annotations, seeing the row's labels (before configured
// overrides) as .Labels, matching vmalert. This runs before hashLabels():
// the rendered value is the alert identity.
func (ar *AlertingRule) buildLabels(m datasource.Metric) map[string]string {
	labels := make(map[string]string, len(m.Labels)+len(ar.labels))
	for _, l := range m.Labels {
		labels[l.Name] = l.Value
	}
	var data annotationData
	haveData := false
	for k, v := range ar.labels {
		if v == "" {
			continue
		}
		if strings.Contains(v, "{{") {
			if !haveData {
				data = ar.templateData(m)
				haveData = true
			}
			v = ar.renderTemplate("label", k, v, data)
		}
		if orig, exists := labels[k]; exists && orig != v {
			labels["exported_"+k] = orig
		}
		labels[k] = v
	}
	// Always set alertname
	if ar.name != "" {
		labels["alertname"] = ar.name
	}
	return labels
}

// annotationData is the template context available in label and annotation
// templates. Supports both Go template syntax (.Labels.X, .Value, .Expr) and
// legacy vmalert-compatible $labels/$value variables.
type annotationData struct {
	Labels map[string]string
	Value  float64
	Expr   string
}

// templateData builds the template context from a query result row.
// Labels are the row's dimension labels, before configured labels override them.
func (ar *AlertingRule) templateData(m datasource.Metric) annotationData {
	labels := make(map[string]string, len(m.Labels))
	for _, l := range m.Labels {
		labels[l.Name] = l.Value
	}

	var value float64
	if len(m.Values) > 0 {
		value = m.Values[0]
	}

	return annotationData{
		Labels: labels,
		Value:  value,
		Expr:   ar.expr,
	}
}

// renderTemplate renders a single template string with the given data.
// On parse or exec error the raw string is returned and a warning is logged;
// templating failures must never drop a label/annotation or fail evaluation.
// kind distinguishes label and annotation templates in warning logs.
func (ar *AlertingRule) renderTemplate(kind, key, tpl string, data annotationData) string {
	// Rewrite legacy {{ $labels.X }} and {{ $value }} to Go template syntax.
	normalized := normalizeLegacyTemplate(tpl)

	t, err := template.New(key).Option("missingkey=zero").Parse(normalized)
	if err != nil {
		slog.Warn("chalert "+kind+" template parse error",
			"rule", ar.name, "key", key, "error", err)
		return tpl
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		slog.Warn("chalert "+kind+" template exec error",
			"rule", ar.name, "key", key, "error", err)
		return tpl
	}
	return buf.String()
}

// renderAnnotations renders annotation templates for an alert using text/template.
// Templates can use .Labels.key, .Value, .Expr, and the legacy {{ $labels.key }} / {{ $value }}.
func (ar *AlertingRule) renderAnnotations(m datasource.Metric, _ *AlertInstance) map[string]string {
	if len(ar.annTpls) == 0 {
		return nil
	}

	data := ar.templateData(m)

	out := make(map[string]string, len(ar.annTpls))
	for k, tpl := range ar.annTpls {
		out[k] = ar.renderTemplate("annotation", k, tpl, data)
	}
	return out
}

// normalizeLegacyTemplate rewrites vmalert-compatible {{ $labels.X }} and {{ $value }}
// to Go template equivalents {{ .Labels.X }} and {{ .Value }}.
func normalizeLegacyTemplate(s string) string {
	s = strings.ReplaceAll(s, "$labels.", ".Labels.")
	s = strings.ReplaceAll(s, "$value", ".Value")
	return s
}

func hashLabels(labels map[string]string) uint64 {
	h := fnv.New64a()
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(labels[k]))
		h.Write([]byte("\xff"))
	}
	return h.Sum64()
}

func (ar *AlertingRule) logDebug(ts time.Time, a *AlertInstance, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	slog.Info("chalert rule state",
		"rule", ar.name,
		"group", ar.groupName,
		"alert_id", a.ID,
		"labels", a.Labels,
		"msg", msg,
		"ts", ts)
}

// GetAlerts returns a snapshot of all active alert instances.
func (ar *AlertingRule) GetAlerts() []AlertInstance {
	ar.mu.RLock()
	defer ar.mu.RUnlock()
	out := make([]AlertInstance, 0, len(ar.alerts))
	for _, a := range ar.alerts {
		out = append(out, *a)
	}
	return out
}

// Name returns the rule name.
func (ar *AlertingRule) Name() string { return ar.name }

// ID returns the rule ID.
func (ar *AlertingRule) ID() uint64 { return ar.ruleID }

// Restore loads alert state from a previous run.
func (ar *AlertingRule) Restore(instances []AlertInstance) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	for i := range instances {
		inst := instances[i]
		if inst.RuleID != ar.ruleID {
			continue
		}
		if inst.State == StatePending || inst.State == StateFiring {
			ar.alerts[inst.ID] = &inst
			slog.Info("chalert restored alert",
				"rule", ar.name,
				"alert_id", inst.ID,
				"state", inst.State,
				"active_at", inst.ActiveAt)
		}
	}
}

package rule

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"time"
)

// APIGroup is a rule group in the Prometheus /api/v1/rules format, which
// Grafana reads.
type APIGroup struct {
	Name     string    `json:"name"`
	File     string    `json:"file"`
	Interval float64   `json:"interval"`
	Rules    []APIRule `json:"rules"`
}

// APIRule is an alerting rule in the Prometheus /api/v1/rules format.
type APIRule struct {
	Type           string            `json:"type"`
	Name           string            `json:"name"`
	Query          string            `json:"query"`
	Duration       float64           `json:"duration"`
	KeepFiringFor  float64           `json:"keepFiringFor"`
	Labels         map[string]string `json:"labels"`
	Annotations    map[string]string `json:"annotations"`
	Alerts         []APIAlert        `json:"alerts"`
	Health         string            `json:"health"`
	LastError      string            `json:"lastError"`
	EvaluationTime float64           `json:"evaluationTime"`
	LastEvaluation time.Time         `json:"lastEvaluation"`
	State          string            `json:"state"`
}

// APIAlert is a pending or firing alert instance of an APIRule.
type APIAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	State       string            `json:"state"`
	ActiveAt    time.Time         `json:"activeAt"`
	Value       string            `json:"value"`
}

// APIGroups returns groups in the /api/v1/rules format, ordered by file
// and name.
func APIGroups(groups []*Group) []APIGroup {
	out := make([]APIGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, g.api())
	}
	slices.SortFunc(out, func(a, b APIGroup) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Name, b.Name))
	})
	return out
}

func (g *Group) api() APIGroup {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := APIGroup{Name: g.Name, File: g.File, Interval: g.Interval.Seconds(), Rules: make([]APIRule, 0, len(g.Rules))}
	for _, r := range g.Rules {
		out.Rules = append(out.Rules, r.api())
	}
	return out
}

func (ar *AlertingRule) api() APIRule {
	ar.mu.RLock()
	defer ar.mu.RUnlock()
	out := APIRule{
		Type:           "alerting",
		Name:           ar.name,
		Query:          ar.expr,
		Duration:       ar.forDur.Seconds(),
		KeepFiringFor:  ar.keepFire.Seconds(),
		Labels:         copyMap(ar.labels),
		Annotations:    copyMap(ar.annTpls),
		Alerts:         []APIAlert{},
		Health:         "ok",
		EvaluationTime: ar.lastDuration.Seconds(),
		LastEvaluation: ar.lastEval,
	}
	switch {
	case ar.lastEval.IsZero():
		out.Health = "unknown"
	case ar.lastErr != nil:
		out.Health, out.LastError = "err", ar.lastErr.Error()
	}
	state := StateInactive
	for _, a := range ar.alerts {
		if a.State == StateInactive {
			continue
		}
		state = max(state, a.State)
		out.Alerts = append(out.Alerts, APIAlert{
			Labels:      copyMap(a.Labels),
			Annotations: copyMap(a.Annotations),
			State:       a.State.String(),
			ActiveAt:    a.ActiveAt,
			Value:       strconv.FormatFloat(a.Value, 'e', -1, 64),
		})
	}
	out.State = state.String()
	return out
}

// copyMap copies m, as {} when empty, to encode outside the rule's lock.
func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	maps.Copy(out, m)
	return out
}

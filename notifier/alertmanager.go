// Package notifier sends alert notifications to Alertmanager.
//
// # Design Decisions
//
// We implement the Alertmanager v2 API (/api/v2/alerts) which accepts JSON payloads.
// This is the same API that vmalert uses, so alerts from chalert appear identically
// in Alertmanager and downstream receivers (PagerDuty, Slack, etc.).
//
// The notifier is intentionally simple compared to vmalert's — we don't implement
// service discovery (Consul SD, DNS SD) or alert relabeling in v1. These can be
// added later following the same patterns.
//
// # Assumptions
//
//   - Alertmanager is reachable via HTTP(S).
//   - The /api/v2/alerts endpoint accepts POST with Content-Type: application/json.
//   - Multiple Alertmanager URLs can be specified for HA — alerts are sent to all.
package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/garbett1/chalert/metrics"
	"github.com/garbett1/chalert/rule"
)

// Config holds notifier configuration.
type Config struct {
	// URLs is the list of Alertmanager endpoints.
	// Alerts are sent to ALL endpoints (fan-out for HA).
	URLs []string

	// Timeout for each HTTP request. Default: 10s.
	Timeout time.Duration

	// ExternalURL is the base URL of the chalert instance, used to generate
	// alert source links.
	ExternalURL string

	// BasicAuth credentials (optional).
	Username string
	Password string

	// BearerToken for auth (optional, mutually exclusive with BasicAuth).
	BearerToken string

	// DropLabels keeps an alert out of every Alertmanager when one of its
	// labels is present and equals the value given here; one match is
	// enough. chalert still evaluates the alert, keeps its state and writes
	// its alert_history rows. Use it for alerts that exist only for their
	// history and that Alertmanager would route to no receiver anyway: it
	// holds every alert it is sent in memory, routed or not.
	DropLabels map[string]string
}

// AlertManager sends alerts to Prometheus Alertmanager instances.
type AlertManager struct {
	urls        []string
	client      *http.Client
	externalURL string
	username    string
	password    string
	bearerToken string
	dropLabels  map[string]string
}

// New returns a notifier that pushes to every Alertmanager in cfg.
func New(cfg Config) *AlertManager {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &AlertManager{
		urls:        cfg.URLs,
		client:      &http.Client{Timeout: timeout},
		externalURL: cfg.ExternalURL,
		username:    cfg.Username,
		password:    cfg.Password,
		bearerToken: cfg.BearerToken,
		dropLabels:  cfg.DropLabels,
	}
}

// Send implements rule.Notifier. Sends alerts to all configured Alertmanager instances.
func (am *AlertManager) Send(ctx context.Context, alerts []rule.AlertInstance) error {
	if len(am.urls) == 0 {
		return nil
	}
	offered := len(alerts)
	alerts = am.keep(alerts)
	if dropped := offered - len(alerts); dropped > 0 {
		slog.Info("chalert notifier: dropped by label",
			"dropped", dropped,
			"kept", len(alerts))
	}
	if len(alerts) == 0 {
		return nil
	}

	var firing, resolved int
	for _, a := range alerts {
		switch a.State {
		case rule.StateFiring:
			firing++
			slog.Info("chalert notifier: sending firing alert",
				"alert", a.AlertName,
				"group", a.GroupName,
				"labels", a.Labels)
		case rule.StateInactive:
			resolved++
			slog.Info("chalert notifier: sending resolution",
				"alert", a.AlertName,
				"group", a.GroupName,
				"labels", a.Labels,
				"resolved_at", a.ResolvedAt)
		default:
		}
	}
	slog.Info("chalert notifier: pushing to alertmanager",
		"total", len(alerts),
		"firing", firing,
		"resolved", resolved,
		"targets", len(am.urls))

	payload, err := am.buildPayload(alerts)
	if err != nil {
		return fmt.Errorf("building alertmanager payload: %w", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(am.urls))

	for _, u := range am.urls {
		u := u
		wg.Go(func() {
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("chalert panic in notifier", "url", u, "panic", rec)
					metrics.NotifierErrors.Inc()
				}
			}()
			if err := am.sendTo(ctx, u, payload); err != nil {
				metrics.NotifierSends.WithLabelValues(u, "error").Inc()
				errCh <- fmt.Errorf("alertmanager %s: %w", u, err)
			} else {
				metrics.NotifierSends.WithLabelValues(u, "success").Inc()
			}
		})
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("notification errors: %v", errs)
	}
	return nil
}

// keep returns the alerts that match none of the drop labels. Each alert it
// leaves out adds one to chalert_notifier_dropped_total, so a firing alert
// counts again on every resend.
func (am *AlertManager) keep(alerts []rule.AlertInstance) []rule.AlertInstance {
	if len(am.dropLabels) == 0 {
		return alerts
	}
	kept := make([]rule.AlertInstance, 0, len(alerts))
	for _, a := range alerts {
		if am.drops(a.Labels) {
			metrics.NotifierDropped.Inc()
			continue
		}
		kept = append(kept, a)
	}
	return kept
}

// drops reports whether labels holds one of the drop labels with its value.
func (am *AlertManager) drops(labels map[string]string) bool {
	for name, value := range am.dropLabels {
		if v, ok := labels[name]; ok && v == value {
			return true
		}
	}
	return false
}

func (am *AlertManager) sendTo(ctx context.Context, url string, payload []byte) error {
	endpoint := url + "/api/v2/alerts"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	if am.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+am.bearerToken)
	} else if am.username != "" {
		req.SetBasicAuth(am.username, am.password)
	}

	resp, err := am.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	slog.Info("chalert alerts delivered", "url", url, "payload_bytes", len(payload))
	return nil
}

// amAlert is the Alertmanager v2 alert format.
// See: https://prometheus.io/docs/alerting/latest/clients/
type amAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt,omitempty"`
	GeneratorURL string            `json:"generatorURL,omitempty"`
}

func (am *AlertManager) buildPayload(alerts []rule.AlertInstance) ([]byte, error) {
	amAlerts := make([]amAlert, 0, len(alerts))
	for _, a := range alerts {
		aa := amAlert{
			Labels:      a.Labels,
			Annotations: a.Annotations,
			StartsAt:    a.ActiveAt,
		}

		switch a.State {
		case rule.StateFiring:
			aa.Status = "firing"
			// EndsAt is set to a time in the future. Alertmanager uses this to
			// auto-resolve if we stop sending. Convention: 4x evaluation interval.
			delta := 4 * time.Minute
			if a.EvaluationInterval > 0 {
				delta = 4 * a.EvaluationInterval
			}
			aa.EndsAt = time.Now().Add(delta)
		case rule.StateInactive:
			aa.Status = "resolved"
			aa.EndsAt = a.ResolvedAt
		default:
			continue // Don't send pending alerts
		}

		if am.externalURL != "" {
			aa.GeneratorURL = fmt.Sprintf("%s/api/v1/alerts?group=%s&alert=%s",
				am.externalURL, a.GroupName, a.AlertName)
		}

		amAlerts = append(amAlerts, aa)
	}

	return json.Marshal(amAlerts)
}

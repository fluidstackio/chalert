package notifier

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/garbett1/chalert/metrics"
	"github.com/garbett1/chalert/rule"
)

func TestBuildPayload_EndsAtFromInterval(t *testing.T) {
	am := &AlertManager{externalURL: "http://localhost"}

	now := time.Now()
	alerts := []rule.AlertInstance{
		{
			State:              rule.StateFiring,
			Labels:             map[string]string{"alertname": "Test"},
			ActiveAt:           now.Add(-time.Minute),
			EvaluationInterval: 10 * time.Second,
		},
	}

	payload, err := am.buildPayload(alerts)
	if err != nil {
		t.Fatalf("buildPayload: %v", err)
	}

	var parsed []amAlert
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(parsed) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(parsed))
	}

	// EndsAt should be ~40s from now (4 * 10s), not ~4min
	diff := parsed[0].EndsAt.Sub(now)
	if diff < 30*time.Second || diff > 50*time.Second {
		t.Errorf("expected EndsAt ~40s from now, got diff=%s", diff)
	}
}

func TestBuildPayload_EndsAtFallback(t *testing.T) {
	am := &AlertManager{}

	now := time.Now()
	alerts := []rule.AlertInstance{
		{
			State:    rule.StateFiring,
			Labels:   map[string]string{"alertname": "Test"},
			ActiveAt: now.Add(-time.Minute),
		},
	}

	payload, err := am.buildPayload(alerts)
	if err != nil {
		t.Fatal(err)
	}

	var parsed []amAlert
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatal(err)
	}

	if len(parsed) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(parsed))
	}

	diff := parsed[0].EndsAt.Sub(now)
	if diff < 3*time.Minute || diff > 5*time.Minute {
		t.Errorf("expected EndsAt ~4min from now (fallback), got diff=%s", diff)
	}
}

func TestBuildPayload_ResolvedEndsAt(t *testing.T) {
	am := &AlertManager{}

	resolvedAt := time.Now().Add(-30 * time.Second)
	alerts := []rule.AlertInstance{
		{
			State:      rule.StateInactive,
			Labels:     map[string]string{"alertname": "Test"},
			ActiveAt:   time.Now().Add(-5 * time.Minute),
			ResolvedAt: resolvedAt,
		},
	}

	payload, err := am.buildPayload(alerts)
	if err != nil {
		t.Fatal(err)
	}

	var parsed []amAlert
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatal(err)
	}

	if len(parsed) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(parsed))
	}

	if parsed[0].Status != "resolved" {
		t.Errorf("expected resolved status, got %q", parsed[0].Status)
	}
	if !parsed[0].EndsAt.Equal(resolvedAt) {
		t.Errorf("expected EndsAt=%v, got %v", resolvedAt, parsed[0].EndsAt)
	}
}

// receiver is a fake Alertmanager that records the alerts each POST carries.
type receiver struct {
	mu     sync.Mutex
	posts  int
	alerts []amAlert
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var got []amAlert
	if err := json.NewDecoder(req.Body).Decode(&got); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.posts++
	r.alerts = append(r.alerts, got...)
	r.mu.Unlock()
}

// sent returns the posts made and the alerts they carried.
func (r *receiver) sent() (int, []amAlert) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.posts, append([]amAlert(nil), r.alerts...)
}

func TestSend_DropLabels(t *testing.T) {
	rec := &receiver{}
	srv := httptest.NewServer(rec)
	defer srv.Close()

	am := New(Config{
		URLs:       []string{srv.URL},
		DropLabels: map[string]string{"notify": "false"},
	})
	now := time.Now()
	alerts := []rule.AlertInstance{
		{State: rule.StateFiring, ActiveAt: now, Labels: map[string]string{"device": "a", "notify": "false"}},
		{State: rule.StateFiring, ActiveAt: now, Labels: map[string]string{"device": "b", "notify": "true"}},
		{State: rule.StateFiring, ActiveAt: now, Labels: map[string]string{"device": "c"}},
		{State: rule.StateInactive, ActiveAt: now, ResolvedAt: now, Labels: map[string]string{"device": "d", "notify": "false"}},
	}
	before := droppedTotal(t)
	if err := am.Send(context.Background(), alerts); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := droppedTotal(t) - before; got != 2 {
		t.Errorf("chalert_notifier_dropped_total rose by %v, want 2", got)
	}

	_, sent := rec.sent()
	var devices []string
	for _, a := range sent {
		devices = append(devices, a.Labels["device"])
	}
	if len(devices) != 2 || devices[0] != "b" || devices[1] != "c" {
		t.Errorf("sent devices = %v, want [b c]", devices)
	}
}

func TestSend_AllDroppedSendsNothing(t *testing.T) {
	rec := &receiver{}
	srv := httptest.NewServer(rec)
	defer srv.Close()

	am := New(Config{
		URLs:       []string{srv.URL},
		DropLabels: map[string]string{"notify": "false"},
	})
	alerts := []rule.AlertInstance{
		{State: rule.StateFiring, ActiveAt: time.Now(), Labels: map[string]string{"notify": "false"}},
	}
	if err := am.Send(context.Background(), alerts); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if posts, _ := rec.sent(); posts != 0 {
		t.Errorf("posts = %d, want 0", posts)
	}
}

func TestSend_NoDropLabelsSendsAll(t *testing.T) {
	rec := &receiver{}
	srv := httptest.NewServer(rec)
	defer srv.Close()

	am := New(Config{URLs: []string{srv.URL}})
	alerts := []rule.AlertInstance{
		{State: rule.StateFiring, ActiveAt: time.Now(), Labels: map[string]string{"notify": "false"}},
	}
	if err := am.Send(context.Background(), alerts); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, sent := rec.sent(); len(sent) != 1 {
		t.Errorf("sent %d alerts, want 1", len(sent))
	}
}

// droppedTotal reads chalert_notifier_dropped_total.
func droppedTotal(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.NotifierDropped.Write(&m); err != nil {
		t.Fatalf("reading chalert_notifier_dropped_total: %v", err)
	}
	return m.GetCounter().GetValue()
}

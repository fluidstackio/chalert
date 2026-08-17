//go:build integration

package integration

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/testcontainers/testcontainers-go"
	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/garbett1/chalert/chclient"
	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
	"github.com/garbett1/chalert/notifier"
	"github.com/garbett1/chalert/rule"
	"github.com/garbett1/chalert/statestore"
)

var (
	// Package-level state set by TestMain.
	chDSN         string
	chConn        driver.Conn
	amURL         string
	webhook       *webhookCapture
	webhookServer *httptest.Server
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// 1. Start webhook capture server (in-process).
	webhook = &webhookCapture{}
	mux := http.NewServeMux()
	mux.Handle("/webhook", webhook)
	webhookServer = httptest.NewServer(mux)
	defer webhookServer.Close()

	// 2. Start ClickHouse container.
	chContainer, err := chmodule.Run(ctx,
		"clickhouse/clickhouse-server:25.12-alpine",
	)
	if err != nil {
		log.Fatalf("failed to start ClickHouse container (is Docker running?): %s", err)
	}
	defer func() { _ = chContainer.Terminate(ctx) }()

	chDSN, err = chContainer.ConnectionString(ctx, "")
	if err != nil {
		log.Fatalf("failed to get ClickHouse DSN: %s", err)
	}

	chConn, err = openTestConn(chDSN)
	if err != nil {
		log.Fatalf("failed to connect to ClickHouse: %s", err)
	}
	defer chConn.Close()

	// Create test tables.
	if err := createHTTPRequestsTable(ctx, chConn); err != nil {
		log.Fatalf("failed to create http_requests table: %s", err)
	}
	store := statestore.New(chConn, "default")
	if err := store.EnsureTables(ctx); err != nil {
		log.Fatalf("failed to create state tables: %s", err)
	}

	// 3. Start AlertManager container with webhook config.
	amContainer, cleanup, err := startAlertManager(ctx, webhookServer.URL)
	if err != nil {
		log.Fatalf("failed to start AlertManager: %s", err)
	}
	defer func() { _ = amContainer.Terminate(ctx) }()
	defer cleanup()

	amEndpoint, err := amContainer.Endpoint(ctx, "http")
	if err != nil {
		log.Fatalf("failed to get AlertManager endpoint: %s", err)
	}
	amURL = amEndpoint

	os.Exit(m.Run())
}

// startAlertManager creates an AlertManager container configured to forward alerts to webhookURL.
func startAlertManager(ctx context.Context, webhookURL string) (testcontainers.Container, func(), error) {
	// Resolve webhook URL for container access.
	// Replace localhost with host.docker.internal for container→host networking.
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(strings.TrimPrefix(webhookURL, "http://"), "https://"))
	containerWebhookURL := fmt.Sprintf("http://host.docker.internal:%s/webhook", port)

	// Write alertmanager config with resolved webhook URL.
	tmpDir, err := os.MkdirTemp("", "chalert-am-*")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { os.RemoveAll(tmpDir) }

	amCfg := fmt.Sprintf(`route:
  receiver: test-webhook
  group_wait: 1s
  group_interval: 1s
  repeat_interval: 5s
  group_by: ['alertname', 'service']
receivers:
  - name: test-webhook
    webhook_configs:
      - url: '%s'
        send_resolved: true
`, containerWebhookURL)

	cfgPath := filepath.Join(tmpDir, "alertmanager.yml")
	if err := os.WriteFile(cfgPath, []byte(amCfg), 0644); err != nil {
		cleanup()
		return nil, nil, err
	}

	// Expose webhook port so container can reach the host.
	portNum, _ := net.LookupPort("tcp", port)

	req := testcontainers.ContainerRequest{
		Image:           "prom/alertmanager:v0.28.0",
		ExposedPorts:    []string{"9093/tcp"},
		HostAccessPorts: []int{portNum},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      cfgPath,
				ContainerFilePath: "/etc/alertmanager/alertmanager.yml",
				FileMode:          0644,
			},
		},
		WaitingFor: wait.ForListeningPort("9093/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("alertmanager container: %w", err)
	}
	return container, cleanup, nil
}

func openTestConn(dsn string) (driver.Conn, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// ---------------------------------------------------------------------------
// Test: Two HTTP services, one recovers, the other keeps firing
// ---------------------------------------------------------------------------

func TestTwoServiceRecoveryScenario(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Clear any leftover data.
	if err := chConn.Exec(ctx, "TRUNCATE TABLE http_requests"); err != nil {
		t.Fatalf("truncate: %s", err)
	}
	webhook.reset()

	// Phase 1: Seed both services with errors (50% error rate).
	for _, svc := range []string{"service-a", "service-b"} {
		if err := insertHTTPRequests(ctx, chConn, svc, 500, 100); err != nil {
			t.Fatalf("insert errors for %s: %s", svc, err)
		}
		if err := insertHTTPRequests(ctx, chConn, svc, 200, 100); err != nil {
			t.Fatalf("insert ok for %s: %s", svc, err)
		}
	}

	// Parse rules and start evaluation.
	rulesPath, _ := filepath.Abs("testdata/two_services_rules.yaml")
	groups, err := config.Parse([]string{rulesPath})
	if err != nil {
		t.Fatalf("parse rules: %s", err)
	}

	ch, err := chclient.New(chclient.Config{
		DSN:          chDSN,
		MaxQueryTime: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("chclient: %s", err)
	}
	defer ch.Close()

	qb := datasource.NewQuerierBuilder(ch.ReadConn())
	am := notifier.New(notifier.Config{URLs: []string{amURL}})
	store := statestore.New(ch.WriteConn(), "default")

	g := rule.NewGroup(groups[0], qb, rule.GroupOptions{
		DefaultInterval: 5 * time.Second,
		ExternalLabels:  map[string]string{"env": "integration-test"},
	})
	go g.Start(ctx, am, store)
	defer func() {
		cancel()
		g.Close()
	}()

	// Phase 2: Wait for both services to appear as firing in webhook.
	t.Log("waiting for both services to fire...")
	waitFor(t, 60*time.Second, time.Second, "both services firing", func() bool {
		alerts := webhook.getAlerts()
		firingA, firingB := false, false
		for _, a := range alerts {
			if a.Status == "firing" {
				switch a.Labels["service"] {
				case "service-a":
					firingA = true
				case "service-b":
					firingB = true
				}
			}
		}
		return firingA && firingB
	})

	// Verify labels on the fired alerts. The Alertmanager container is
	// shared across tests, so only inspect this test's own alerts —
	// other tests' alerts can still be repeating in it.
	for _, a := range webhook.getAlerts() {
		if a.Status != "firing" || a.Labels["alertname"] != "HighErrorRate" {
			continue
		}
		if a.Labels["severity"] != "critical" {
			t.Errorf("expected severity=critical, got %q", a.Labels["severity"])
		}
		if a.Labels["env"] != "integration-test" {
			t.Errorf("expected env=integration-test, got %q", a.Labels["env"])
		}
	}

	// Phase 3: service-a recovers — dilute error rate well below 5%.
	t.Log("service-a recovering...")
	if err := insertHTTPRequests(ctx, chConn, "service-a", 200, 5000); err != nil {
		t.Fatalf("insert recovery for service-a: %s", err)
	}
	// Keep service-b erroring.
	if err := insertHTTPRequests(ctx, chConn, "service-b", 500, 100); err != nil {
		t.Fatalf("insert more errors for service-b: %s", err)
	}
	if err := insertHTTPRequests(ctx, chConn, "service-b", 200, 100); err != nil {
		t.Fatalf("insert ok for service-b: %s", err)
	}

	// Phase 4: Wait for service-a to resolve.
	t.Log("waiting for service-a to resolve...")
	waitFor(t, 60*time.Second, time.Second, "service-a resolved", func() bool {
		alerts := webhook.getAlerts()
		for _, a := range alerts {
			if a.Labels["service"] == "service-a" && a.Status == "resolved" {
				return true
			}
		}
		return false
	})

	// Verify service-b is still firing (check the most recent alert for service-b).
	alerts := webhook.getAlerts()
	var latestB *capturedAlert
	for i := len(alerts) - 1; i >= 0; i-- {
		if alerts[i].Labels["service"] == "service-b" {
			latestB = &alerts[i]
			break
		}
	}
	if latestB == nil {
		t.Fatal("no alert found for service-b")
	}
	if latestB.Status != "firing" {
		t.Errorf("expected service-b to still be firing, got %q", latestB.Status)
	}

	// Phase 5: Verify alert_history has state transitions.
	rows, err := chConn.Query(ctx, `
		SELECT alert_name, dimensions['service'] AS service, state
		FROM default.alert_history
		ORDER BY event_time
	`)
	if err != nil {
		t.Fatalf("query alert_history: %s", err)
	}
	defer rows.Close()

	type historyRow struct {
		alertName, service, state string
	}
	var history []historyRow
	for rows.Next() {
		var r historyRow
		if err := rows.Scan(&r.alertName, &r.service, &r.state); err != nil {
			t.Fatalf("scan: %s", err)
		}
		history = append(history, r)
	}

	if len(history) == 0 {
		t.Fatal("alert_history is empty")
	}

	// Verify service-a eventually has an inactive (resolved) entry.
	hasResolvedA := false
	for _, r := range history {
		if r.service == "service-a" && r.state == "inactive" {
			hasResolvedA = true
		}
	}
	if !hasResolvedA {
		t.Error("expected service-a to have an 'inactive' entry in alert_history")
	}

	t.Logf("alert_history has %d rows", len(history))
}

// ---------------------------------------------------------------------------
// Test: Guard rail — max_rows_to_read
// ---------------------------------------------------------------------------

func TestGuardRailMaxRowsToRead(t *testing.T) {
	ctx := context.Background()

	// Insert a large dataset.
	if err := chConn.Exec(ctx, "TRUNCATE TABLE http_requests"); err != nil {
		t.Fatalf("truncate: %s", err)
	}
	if err := insertHTTPRequests(ctx, chConn, "big-service", 200, 10000); err != nil {
		t.Fatalf("insert: %s", err)
	}

	// Create a client with a very low max_rows_to_read.
	ch, err := chclient.New(chclient.Config{
		DSN:           chDSN,
		MaxQueryTime:  10 * time.Second,
		MaxRowsToRead: 100,
	})
	if err != nil {
		t.Fatalf("chclient: %s", err)
	}
	defer ch.Close()

	qb := datasource.NewQuerierBuilder(ch.ReadConn())
	querier := qb.BuildWithParams(datasource.QuerierParams{})

	// This query scans all 10k rows — should fail.
	_, err = querier.Query(ctx, "SELECT service, count() AS value FROM http_requests GROUP BY service", time.Now())
	if err == nil {
		t.Fatal("expected query to fail due to max_rows_to_read, but it succeeded")
	}
	t.Logf("got expected error: %v", err)
}

// ---------------------------------------------------------------------------
// Test: External labels appear on alerts via webhook
// ---------------------------------------------------------------------------

func TestExternalLabelsInAlerts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := chConn.Exec(ctx, "TRUNCATE TABLE http_requests"); err != nil {
		t.Fatalf("truncate: %s", err)
	}
	webhook.reset()

	// Insert error data.
	if err := insertHTTPRequests(ctx, chConn, "label-test-svc", 500, 100); err != nil {
		t.Fatal(err)
	}
	if err := insertHTTPRequests(ctx, chConn, "label-test-svc", 200, 100); err != nil {
		t.Fatal(err)
	}

	rulesPath, _ := filepath.Abs("testdata/two_services_rules.yaml")
	groups, err := config.Parse([]string{rulesPath})
	if err != nil {
		t.Fatalf("parse: %s", err)
	}

	ch, err := chclient.New(chclient.Config{DSN: chDSN, MaxQueryTime: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	qb := datasource.NewQuerierBuilder(ch.ReadConn())
	am := notifier.New(notifier.Config{URLs: []string{amURL}})
	store := statestore.New(ch.WriteConn(), "default")

	g := rule.NewGroup(groups[0], qb, rule.GroupOptions{
		DefaultInterval: 5 * time.Second,
		ExternalLabels: map[string]string{
			"env":     "test-env",
			"cluster": "test-cluster",
		},
	})
	go g.Start(ctx, am, store)
	defer func() {
		cancel()
		g.Close()
	}()

	waitFor(t, 60*time.Second, time.Second, "alert with external labels", func() bool {
		for _, a := range webhook.getAlerts() {
			if a.Status == "firing" && a.Labels["env"] == "test-env" && a.Labels["cluster"] == "test-cluster" {
				return true
			}
		}
		return false
	})
}

// ---------------------------------------------------------------------------
// Test: Default limit enforced
// ---------------------------------------------------------------------------

func TestDefaultLimitEnforced(t *testing.T) {
	ctx := context.Background()

	if err := chConn.Exec(ctx, "TRUNCATE TABLE http_requests"); err != nil {
		t.Fatalf("truncate: %s", err)
	}

	// Insert data that will produce many distinct services (> limit).
	for i := 0; i < 20; i++ {
		svc := fmt.Sprintf("svc-%03d", i)
		if err := insertHTTPRequests(ctx, chConn, svc, 500, 10); err != nil {
			t.Fatal(err)
		}
		if err := insertHTTPRequests(ctx, chConn, svc, 200, 10); err != nil {
			t.Fatal(err)
		}
	}

	rulesPath, _ := filepath.Abs("testdata/two_services_rules.yaml")
	groups, err := config.Parse([]string{rulesPath})
	if err != nil {
		t.Fatal(err)
	}

	ch, err := chclient.New(chclient.Config{DSN: chDSN, MaxQueryTime: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	qb := datasource.NewQuerierBuilder(ch.ReadConn())

	// Set a low limit so 20 services exceeds it.
	g := rule.NewGroup(groups[0], qb, rule.GroupOptions{
		DefaultInterval: 5 * time.Second,
		DefaultLimit:    5,
	})

	// Run a single evaluation manually via the rule.
	for _, r := range g.Rules {
		_, err := r.Exec(ctx, time.Now(), g.Limit)
		if err == nil {
			t.Fatal("expected limit exceeded error, got nil")
		}
		t.Logf("got expected error: %s", err)
	}
}

// ---------------------------------------------------------------------------
// Test: keep_firing_for with flapping data
// ---------------------------------------------------------------------------

func TestKeepFiringForFlappingData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := chConn.Exec(ctx, "TRUNCATE TABLE http_requests"); err != nil {
		t.Fatalf("truncate: %s", err)
	}
	webhook.reset()

	// Phase 1: Seed error data to trigger alert.
	if err := insertHTTPRequests(ctx, chConn, "flapper", 500, 100); err != nil {
		t.Fatal(err)
	}
	if err := insertHTTPRequests(ctx, chConn, "flapper", 200, 100); err != nil {
		t.Fatal(err)
	}

	rulesPath, _ := filepath.Abs("testdata/keep_firing_for_rules.yaml")
	groups, err := config.Parse([]string{rulesPath})
	if err != nil {
		t.Fatalf("parse: %s", err)
	}

	ch, err := chclient.New(chclient.Config{DSN: chDSN, MaxQueryTime: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	qb := datasource.NewQuerierBuilder(ch.ReadConn())
	am := notifier.New(notifier.Config{URLs: []string{amURL}})
	store := statestore.New(ch.WriteConn(), "default")

	g := rule.NewGroup(groups[0], qb, rule.GroupOptions{
		DefaultInterval: 5 * time.Second,
	})
	go g.Start(ctx, am, store)
	defer func() {
		cancel()
		g.Close()
	}()

	// Phase 2: Wait for alert to fire.
	t.Log("waiting for flapper to fire...")
	waitFor(t, 60*time.Second, time.Second, "flapper firing", func() bool {
		for _, a := range webhook.getAlerts() {
			if a.Status == "firing" && a.Labels["service"] == "flapper" {
				return true
			}
		}
		return false
	})

	// Phase 3: Remove error data (dilute below threshold).
	t.Log("flapper recovering — diluting errors...")
	if err := insertHTTPRequests(ctx, chConn, "flapper", 200, 5000); err != nil {
		t.Fatal(err)
	}

	// Alert should still fire for ~30s (keep_firing_for).
	// Wait 10s and confirm still firing.
	time.Sleep(10 * time.Second)
	latestFiring := false
	for _, a := range webhook.getAlerts() {
		if a.Labels["service"] == "flapper" && a.Status == "firing" {
			latestFiring = true
		}
	}
	if !latestFiring {
		t.Error("expected alert to still be firing within keep_firing_for window")
	}

	// Phase 4: Wait for resolution after keep_firing_for expires.
	t.Log("waiting for flapper to resolve after keep_firing_for...")
	waitFor(t, 60*time.Second, time.Second, "flapper resolved", func() bool {
		alerts := webhook.getAlerts()
		for _, a := range alerts {
			if a.Labels["service"] == "flapper" && a.Status == "resolved" {
				return true
			}
		}
		return false
	})
}

// ---------------------------------------------------------------------------
// Test: Eval timestamp parameter
// ---------------------------------------------------------------------------

func TestEvalTimestampParameter(t *testing.T) {
	ctx := context.Background()

	ch, err := chclient.New(chclient.Config{DSN: chDSN, MaxQueryTime: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	qb := datasource.NewQuerierBuilder(ch.ReadConn())
	querier := qb.BuildWithParams(datasource.QuerierParams{})

	evalTS := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	res, err := querier.Query(ctx,
		"SELECT {chalert_eval_ts:DateTime64(3)} AS ts, 1 AS value",
		evalTS,
	)
	if err != nil {
		t.Fatalf("query with eval timestamp parameter failed: %v", err)
	}

	if len(res.Data) != 1 {
		t.Fatalf("expected 1 result, got %d", len(res.Data))
	}

	// The timestamp in the result should match our eval timestamp.
	gotTS := time.Unix(res.Data[0].Timestamps[0], 0).UTC()
	if gotTS.Year() != 2025 || gotTS.Month() != 6 || gotTS.Day() != 15 {
		t.Errorf("expected eval timestamp 2025-06-15, got %v", gotTS)
	}
}

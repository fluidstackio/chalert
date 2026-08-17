//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/garbett1/chalert/internal/testutil"
)

// These tests exercise hot reloading against the real chalert binary: rules
// are laid out (and atomically swapped) exactly the way the kubelet's
// AtomicWriter maintains a ConfigMap volume, and assertions go through the
// binary's own /metrics endpoint and the captured Alertmanager webhook.

var (
	buildOnce sync.Once
	buildPath string
	buildErr  error
)

func chalertBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "chalert-bin-*")
		if err != nil {
			buildErr = err
			return
		}
		buildPath = filepath.Join(dir, "chalert")
		cmd := exec.Command("go", "build", "-o", buildPath, "github.com/garbett1/chalert/cmd/chalert")
		cmd.Dir = ".."
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build: %s: %s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return buildPath
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

type chalertProc struct {
	cmd     *exec.Cmd
	baseURL string
	output  *bytes.Buffer
	mu      *sync.Mutex
}

func (p *chalertProc) logs() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.output.String()
}

// syncedWriter serializes subprocess output into a buffer.
type syncedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w syncedWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(b)
}

func startChalert(t *testing.T, extraArgs ...string) *chalertProc {
	t.Helper()
	port := freePort(t)
	args := append([]string{
		"-clickhouse.dsn=" + chDSN,
		"-notifier.url=" + amURL,
		"-httpListenAddr=127.0.0.1:" + strconv.Itoa(port),
		"-evaluationInterval=1s",
		"-rule.resendDelay=2s",
	}, extraArgs...)

	cmd := exec.Command(chalertBinary(t), args...)
	var mu sync.Mutex
	buf := &bytes.Buffer{}
	cmd.Stdout = syncedWriter{&mu, buf}
	cmd.Stderr = syncedWriter{&mu, buf}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	p := &chalertProc{
		cmd:     cmd,
		baseURL: "http://127.0.0.1:" + strconv.Itoa(port),
		output:  buf,
		mu:      &mu,
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		if t.Failed() {
			t.Logf("chalert output:\n%s", p.logs())
		}
	})

	waitFor(t, 30*time.Second, 200*time.Millisecond, "chalert ready", func() bool {
		resp, err := http.Get(p.baseURL + "/ready")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	})
	return p
}

// metricValue scrapes /metrics and returns the value of the first sample
// whose name+labels prefix matches. An absent series is a genuine 0 (counter
// series only exist once incremented); a failed scrape fails the test so
// assertions on 0 can't pass vacuously.
func (p *chalertProc) metricValue(t *testing.T, prefix string) float64 {
	t.Helper()
	resp, err := http.Get(p.baseURL + "/metrics")
	if err != nil {
		t.Fatalf("scrape /metrics: %s", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape /metrics: status %d", resp.StatusCode)
	}
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatalf("scrape /metrics: read body: %s", err)
	}
	for _, line := range strings.Split(body.String(), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		return v
	}
	return 0
}

func (p *chalertProc) reloadCount(t *testing.T, result string) float64 {
	return p.metricValue(t, fmt.Sprintf(`chalert_config_reloads_total{result="%s"}`, result))
}

func alertingRule(alert, expr string) string {
	return fmt.Sprintf(`      - alert: %s
        expr: %s
        labels:
          severity: test
`, alert, expr)
}

func rulesFile(rules ...string) string {
	return "groups:\n  - name: hot-reload\n    interval: 1s\n    rules:\n" + strings.Join(rules, "")
}

const (
	ruleAlwaysB = `SELECT 'hotreload-b' AS service, 1 AS value`
	ruleAlwaysC = `SELECT 'hotreload-c' AS service, 1 AS value`
)

func countingRule(threshold int) string {
	return fmt.Sprintf(
		`SELECT service, count() AS value FROM http_requests WHERE service = 'hotreload-a' AND status >= 500 AND timestamp > now() - INTERVAL 10 MINUTE GROUP BY service HAVING value > %d`,
		threshold)
}

func firingAlerts(alertname string) []capturedAlert {
	var out []capturedAlert
	for _, a := range webhook.getAlerts() {
		if a.Labels["alertname"] == alertname && a.Status == "firing" {
			out = append(out, a)
		}
	}
	return out
}

func hasResolved(alertname string) bool {
	for _, a := range webhook.getAlerts() {
		if a.Labels["alertname"] == alertname && a.Status == "resolved" {
			return true
		}
	}
	return false
}

func TestHotReloadConfigMapSwap(t *testing.T) {
	ctx := t.Context()
	if err := chConn.Exec(ctx, "TRUNCATE TABLE http_requests"); err != nil {
		t.Fatalf("truncate: %s", err)
	}
	webhook.reset()

	if err := insertHTTPRequests(ctx, chConn, "hotreload-a", 500, 50); err != nil {
		t.Fatalf("insert: %s", err)
	}

	dir := t.TempDir()
	testutil.WriteConfigMapVolume(t, dir, map[string]string{
		"rules.yaml": rulesFile(alertingRule("HotReloadA", countingRule(10))),
	})

	p := startChalert(t,
		"-rule="+filepath.Join(dir, "*"),
		"-rule.configCheckInterval=500ms",
	)

	t.Log("phase 1: initial rule fires")
	waitFor(t, 60*time.Second, 500*time.Millisecond, "HotReloadA firing", func() bool {
		return len(firingAlerts("HotReloadA")) > 0
	})
	startsAtBefore := firingAlerts("HotReloadA")[0].StartsAt

	t.Log("phase 2: unchanged files cause no reloads")
	time.Sleep(2 * time.Second)
	if got := p.reloadCount(t, "success"); got != 0 {
		t.Errorf("expected 0 reloads while content unchanged, got %v", got)
	}

	t.Log("phase 3: ConfigMap swap adds a rule, picked up without restart")
	testutil.WriteConfigMapVolume(t, dir, map[string]string{
		"rules.yaml": rulesFile(
			alertingRule("HotReloadA", countingRule(10)),
			alertingRule("HotReloadB", ruleAlwaysB),
		),
	})
	waitFor(t, 30*time.Second, 200*time.Millisecond, "reload success", func() bool {
		return p.reloadCount(t, "success") >= 1
	})
	waitFor(t, 60*time.Second, 500*time.Millisecond, "HotReloadB firing", func() bool {
		return len(firingAlerts("HotReloadB")) > 0
	})

	t.Log("phase 4: HotReloadA state survived the reload")
	if hasResolved("HotReloadA") {
		t.Error("HotReloadA should not have resolved across the reload")
	}
	webhook.reset()
	waitFor(t, 60*time.Second, 500*time.Millisecond, "HotReloadA re-sent after reload", func() bool {
		return len(firingAlerts("HotReloadA")) > 0
	})
	startsAtAfter := firingAlerts("HotReloadA")[0].StartsAt
	if !startsAtAfter.Equal(startsAtBefore) {
		t.Errorf("HotReloadA startsAt changed across reload: %s -> %s (alert state lost)",
			startsAtBefore, startsAtAfter)
	}

	t.Log("phase 5: broken config is rejected, old rules keep evaluating")
	errorsBefore := p.reloadCount(t, "error")
	testutil.WriteConfigMapVolume(t, dir, map[string]string{
		"rules.yaml": "groups: [ this is not valid yaml",
	})
	waitFor(t, 30*time.Second, 200*time.Millisecond, "reload error counted", func() bool {
		return p.reloadCount(t, "error") > errorsBefore
	})
	if got := p.metricValue(t, "chalert_config_last_reload_successful"); got != 0 {
		t.Errorf("expected last_reload_successful=0 after broken config, got %v", got)
	}
	webhook.reset()
	waitFor(t, 60*time.Second, 500*time.Millisecond, "old rules still evaluating", func() bool {
		return len(firingAlerts("HotReloadB")) > 0
	})

	t.Log("phase 6: fixed config raises HotReloadA threshold, alert resolves in place")
	testutil.WriteConfigMapVolume(t, dir, map[string]string{
		"rules.yaml": rulesFile(
			alertingRule("HotReloadA", countingRule(1000000)),
			alertingRule("HotReloadB", ruleAlwaysB),
		),
	})
	waitFor(t, 30*time.Second, 200*time.Millisecond, "second reload success", func() bool {
		return p.reloadCount(t, "success") >= 2
	})
	if got := p.metricValue(t, "chalert_config_last_reload_successful"); got != 1 {
		t.Errorf("expected last_reload_successful=1 after recovery, got %v", got)
	}
	waitFor(t, 60*time.Second, 500*time.Millisecond, "HotReloadA resolved", func() bool {
		return hasResolved("HotReloadA")
	})
}

func TestHotReloadHTTPEndpointAndSIGHUP(t *testing.T) {
	ctx := t.Context()
	if err := chConn.Exec(ctx, "TRUNCATE TABLE http_requests"); err != nil {
		t.Fatalf("truncate: %s", err)
	}
	webhook.reset()

	dir := t.TempDir()
	rulesPath := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(rulesPath, []byte(rulesFile(alertingRule("HotReloadB", ruleAlwaysB))), 0644); err != nil {
		t.Fatal(err)
	}

	p := startChalert(t, "-rule="+rulesPath)

	waitFor(t, 60*time.Second, 500*time.Millisecond, "HotReloadB firing", func() bool {
		return len(firingAlerts("HotReloadB")) > 0
	})

	t.Log("phase 1: no periodic checks when -rule.configCheckInterval is unset")
	if err := os.WriteFile(rulesPath, []byte(rulesFile(
		alertingRule("HotReloadB", ruleAlwaysB),
		alertingRule("HotReloadC", ruleAlwaysC),
	)), 0644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if got := p.reloadCount(t, "success"); got != 0 {
		t.Errorf("expected no automatic reload with checks disabled, got %v", got)
	}

	t.Log("phase 2: POST /-/reload applies the pending change")
	resp, err := http.Post(p.baseURL+"/-/reload", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 from /-/reload, got %d", resp.StatusCode)
	}
	waitFor(t, 30*time.Second, 200*time.Millisecond, "reload via endpoint", func() bool {
		return p.reloadCount(t, "success") >= 1
	})
	waitFor(t, 60*time.Second, 500*time.Millisecond, "HotReloadC firing", func() bool {
		return len(firingAlerts("HotReloadC")) > 0
	})

	t.Log("phase 3: SIGHUP still reloads")
	if err := os.WriteFile(rulesPath, []byte(rulesFile(
		alertingRule("HotReloadB", ruleAlwaysB),
		alertingRule("HotReloadC", ruleAlwaysC),
		alertingRule("HotReloadD", `SELECT 'hotreload-d' AS service, 1 AS value`),
	)), 0644); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, 200*time.Millisecond, "reload via SIGHUP", func() bool {
		return p.reloadCount(t, "success") >= 2
	})
	waitFor(t, 60*time.Second, 500*time.Millisecond, "HotReloadD firing", func() bool {
		return len(firingAlerts("HotReloadD")) > 0
	})
}

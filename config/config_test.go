package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseValidConfig(t *testing.T) {
	yaml := `
groups:
  - name: test-group
    interval: 30s
    rules:
      - alert: HighErrorRate
        expr: |
          SELECT service, countIf(status >= 500) / count() AS value
          FROM http_requests
          WHERE timestamp > now() - INTERVAL 5 MINUTE
          GROUP BY service
          HAVING value > 0.05
        for: 3m
        labels:
          severity: critical
        annotations:
          summary: "Error rate {{ $value }} on {{ $labels.service }}"

      - alert: SimpleAlert
        expr: SELECT 'test' AS scope, 1 AS value
`

	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	groups, err := Parse([]string{path})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}

	g := groups[0]
	if g.Name != "test-group" {
		t.Errorf("expected name 'test-group', got %q", g.Name)
	}
	if g.Interval.Duration() != 30*time.Second {
		t.Errorf("expected interval 30s, got %s", g.Interval.Duration())
	}
	if len(g.Rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(g.Rules))
	}

	r := g.Rules[0]
	if r.Alert != "HighErrorRate" {
		t.Errorf("expected alert 'HighErrorRate', got %q", r.Alert)
	}
	if r.For.Duration() != 3*time.Minute {
		t.Errorf("expected for 3m, got %s", r.For.Duration())
	}
	if r.Labels["severity"] != "critical" {
		t.Errorf("expected severity=critical, got %q", r.Labels["severity"])
	}
	if r.ID == 0 {
		t.Error("expected non-zero rule ID")
	}

	// Verify IDs are different
	if g.Rules[0].ID == g.Rules[1].ID {
		t.Error("expected different rule IDs")
	}
}

func TestParseInvalidRule(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "missing alert and record",
			yaml: `
groups:
  - name: g
    rules:
      - expr: SELECT 1 AS value
`,
		},
		{
			name: "both alert and record",
			yaml: `
groups:
  - name: g
    rules:
      - alert: foo
        record: bar
        expr: SELECT 1 AS value
`,
		},
		{
			name: "empty expression",
			yaml: `
groups:
  - name: g
    rules:
      - alert: foo
        expr: ""
`,
		},
		{
			name: "non-SELECT expression",
			yaml: `
groups:
  - name: g
    rules:
      - alert: foo
        expr: INSERT INTO bad VALUES (1)
`,
		},
		{
			name: "recording rule with annotations",
			yaml: `
groups:
  - name: g
    rules:
      - record: foo
        expr: SELECT 1 AS value
        annotations:
          summary: "nope"
`,
		},
		{
			name: "recording rule with for",
			yaml: `
groups:
  - name: g
    rules:
      - record: foo
        expr: SELECT 1 AS value
        for: 5m
`,
		},
		{
			name: "empty group name",
			yaml: `
groups:
  - name: ""
    rules:
      - alert: foo
        expr: SELECT 1 AS value
`,
		},
		{
			name: "duplicate rules",
			yaml: `
groups:
  - name: g
    rules:
      - alert: foo
        expr: SELECT 1 AS value
      - alert: foo
        expr: SELECT 1 AS value
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "rules.yaml")
			if err := os.WriteFile(path, []byte(tt.yaml), 0644); err != nil {
				t.Fatal(err)
			}
			_, err := Parse([]string{path})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			t.Logf("got expected error: %s", err)
		})
	}
}

func TestEnvSubstitution(t *testing.T) {
	t.Setenv("CHALERT_TEST_TABLE", "my_table")

	yaml := `
groups:
  - name: env-test
    rules:
      - alert: EnvAlert
        expr: SELECT 1 AS value FROM %{CHALERT_TEST_TABLE}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	groups, err := Parse([]string{path})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expr := groups[0].Rules[0].Expr
	if expected := "SELECT 1 AS value FROM my_table"; expr != expected {
		t.Errorf("expected %q, got %q", expected, expr)
	}
}

func TestHashRuleDeterminism(t *testing.T) {
	r := Rule{
		Alert: "TestAlert",
		Expr:  "SELECT 1 AS value",
		Labels: map[string]string{
			"b": "2",
			"a": "1",
		},
	}

	h1 := HashRule(r)
	h2 := HashRule(r)
	if h1 != h2 {
		t.Errorf("hash not deterministic: %d != %d", h1, h2)
	}

	// Different expression = different hash
	r2 := r
	r2.Expr = "SELECT 2 AS value"
	if HashRule(r2) == h1 {
		t.Error("different expressions should produce different hashes")
	}
}

func TestNormalizeRuleIDs(t *testing.T) {
	// Simulate a CH-based query hash that normalizes whitespace and literals.
	fakeQueryHash := func(expr string) (uint64, error) {
		// Simple simulation: hash only the non-whitespace tokens
		// (real CH also normalizes literals, but this suffices for testing)
		normalized := strings.Join(strings.Fields(expr), " ")
		h := uint64(0)
		for _, b := range []byte(normalized) {
			h = h*31 + uint64(b)
		}
		return h, nil
	}

	groups := []Group{
		{
			Name: "g1",
			Rules: []Rule{
				{Alert: "A", Expr: "SELECT 1 AS value", ID: 1},
				{Alert: "B", Expr: "SELECT 2 AS value", ID: 2},
			},
		},
	}

	err := NormalizeRuleIDs(groups, fakeQueryHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// IDs should have been recomputed (different from the originals)
	if groups[0].Rules[0].ID == 1 {
		t.Error("expected rule A ID to be recomputed")
	}
	if groups[0].Rules[1].ID == 2 {
		t.Error("expected rule B ID to be recomputed")
	}
	// IDs should be different from each other
	if groups[0].Rules[0].ID == groups[0].Rules[1].ID {
		t.Error("expected different IDs for different rules")
	}
}

func TestNormalizeRuleIDs_DuplicateDetection(t *testing.T) {
	// A query hash that always returns the same value (simulates
	// normalizedQueryHashKeepNames merging two exprs that differ
	// only in literal values).
	constantHash := func(expr string) (uint64, error) {
		return 42, nil
	}

	groups := []Group{
		{
			Name: "g1",
			Rules: []Rule{
				{Alert: "SameName", Expr: "SELECT 1 AS value", ID: 1},
				{Alert: "SameName", Expr: "SELECT 2 AS value", ID: 2},
			},
		},
	}

	err := NormalizeRuleIDs(groups, constantHash)
	if err == nil {
		t.Fatal("expected duplicate detection error, got nil")
	}
	if !strings.Contains(err.Error(), "same normalized identity") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeRuleIDs_DifferentNamesSameExprOK(t *testing.T) {
	// Same expr hash but different names should produce different rule IDs.
	constantHash := func(expr string) (uint64, error) {
		return 42, nil
	}

	groups := []Group{
		{
			Name: "g1",
			Rules: []Rule{
				{Alert: "AlertA", Expr: "SELECT 1 AS value"},
				{Alert: "AlertB", Expr: "SELECT 1 AS value"},
			},
		},
	}

	err := NormalizeRuleIDs(groups, constantHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if groups[0].Rules[0].ID == groups[0].Rules[1].ID {
		t.Error("different alert names should produce different IDs even with same expr hash")
	}
}

func TestHashRuleNormalization(t *testing.T) {
	// Same query with different whitespace should produce the same hash.
	r1 := Rule{
		Alert: "TestAlert",
		Expr:  "SELECT service, count() AS value FROM http_requests GROUP BY service",
	}
	r2 := Rule{
		Alert: "TestAlert",
		Expr: `SELECT   service,
			count() AS   value
		FROM http_requests
		GROUP BY service`,
	}
	r3 := Rule{
		Alert: "TestAlert",
		Expr:  "  SELECT service, count() AS value FROM http_requests GROUP BY service  \n",
	}

	h1 := HashRule(r1)
	h2 := HashRule(r2)
	h3 := HashRule(r3)

	if h1 != h2 {
		t.Errorf("multiline whitespace changed hash: %d != %d", h1, h2)
	}
	if h1 != h3 {
		t.Errorf("leading/trailing whitespace changed hash: %d != %d", h1, h3)
	}

	// Semantically different query should still produce different hash.
	r4 := Rule{
		Alert: "TestAlert",
		Expr:  "SELECT service, count() AS value FROM http_requests GROUP BY service HAVING value > 0.1",
	}
	if HashRule(r4) == h1 {
		t.Error("different queries should produce different hashes")
	}
}

func TestParseValidCTEExpression(t *testing.T) {
	yaml := `
groups:
  - name: cte-test
    rules:
      - alert: CTEAlert
        expr: |
          WITH cte AS (
              SELECT service, count() AS cnt
              FROM http_requests
              GROUP BY service
          )
          SELECT service, cnt AS value FROM cte WHERE cnt > 100
`
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	groups, err := Parse([]string{path})
	if err != nil {
		t.Fatalf("CTE expression should be valid, got error: %v", err)
	}
	if len(groups) != 1 || len(groups[0].Rules) != 1 {
		t.Fatalf("expected 1 group with 1 rule, got %d groups", len(groups))
	}
}

func TestReadFilesSkipsDirectories(t *testing.T) {
	// Simulate Kubernetes ConfigMap mount: the glob matches both the real
	// YAML file and hidden metadata directories (..data, ..2026_...).
	dir := t.TempDir()

	// Create a valid rule file
	yaml := `
groups:
  - name: test
    rules:
      - alert: TestAlert
        expr: SELECT 1 AS value
`
	if err := os.WriteFile(filepath.Join(dir, "rules.yaml"), []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	// Create directories that mimic ConfigMap metadata (..data, ..timestamp)
	for _, d := range []string{"..data", "..2026_03_24_12_00_00.123456"} {
		if err := os.Mkdir(filepath.Join(dir, d), 0755); err != nil {
			t.Fatal(err)
		}
	}

	groups, err := Parse([]string{filepath.Join(dir, "*")})
	if err != nil {
		t.Fatalf("expected directories to be skipped, got error: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
}

func TestMultiDocumentYAML(t *testing.T) {
	yaml := `
groups:
  - name: group-a
    rules:
      - alert: AlertA
        expr: SELECT 1 AS value
---
groups:
  - name: group-b
    rules:
      - alert: AlertB
        expr: SELECT 2 AS value
`
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	groups, err := Parse([]string{path})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].Name != "group-a" || groups[1].Name != "group-b" {
		t.Errorf("unexpected group names: %q, %q", groups[0].Name, groups[1].Name)
	}
}

func TestFingerprint(t *testing.T) {
	write := func(t *testing.T, dir, name, content string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		return path
	}

	t.Run("stable for unchanged content", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "a.yaml", "groups: []")
		write(t, dir, "b.yaml", "groups: []")

		fp1, err := Fingerprint([]string{filepath.Join(dir, "*")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fp2, err := Fingerprint([]string{filepath.Join(dir, "*")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fp1 != fp2 {
			t.Errorf("fingerprint changed without content change: %d != %d", fp1, fp2)
		}
	})

	t.Run("changes when content changes", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "a.yaml", "groups: []")

		fp1, err := Fingerprint([]string{filepath.Join(dir, "*")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		write(t, dir, "a.yaml", "groups: [{name: g, rules: []}]")
		fp2, err := Fingerprint([]string{filepath.Join(dir, "*")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fp1 == fp2 {
			t.Error("fingerprint did not change with content change")
		}
	})

	t.Run("changes when a file is added or removed", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "a.yaml", "groups: []")

		fp1, err := Fingerprint([]string{filepath.Join(dir, "*")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		added := write(t, dir, "b.yaml", "groups: []")
		fp2, err := Fingerprint([]string{filepath.Join(dir, "*")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fp1 == fp2 {
			t.Error("fingerprint did not change when a file was added")
		}
		if err := os.Remove(added); err != nil {
			t.Fatal(err)
		}
		fp3, err := Fingerprint([]string{filepath.Join(dir, "*")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fp3 != fp1 {
			t.Errorf("fingerprint after removal should match original: %d != %d", fp3, fp1)
		}
	})

	t.Run("errors when no files match", func(t *testing.T) {
		dir := t.TempDir()
		if _, err := Fingerprint([]string{filepath.Join(dir, "*")}); err == nil {
			t.Error("expected error for empty glob")
		}
	})

	t.Run("multiple patterns are order independent in content", func(t *testing.T) {
		dir := t.TempDir()
		a := write(t, dir, "a.yaml", "groups: []")
		b := write(t, dir, "b.yaml", "groups: []")

		fp1, err := Fingerprint([]string{a, b})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fp2, err := Fingerprint([]string{b, a})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fp1 != fp2 {
			t.Errorf("fingerprint depends on pattern order: %d != %d", fp1, fp2)
		}
	})
}

// writeConfigMapDir lays files out the way the kubelet's AtomicWriter does for
// a ConfigMap volume: a timestamped payload directory, a ..data symlink to it,
// and one top-level symlink per key pointing through ..data.
func writeConfigMapDir(t *testing.T, dir, payloadDir string, files map[string]string) {
	t.Helper()
	payload := filepath.Join(dir, payloadDir)
	if err := os.Mkdir(payload, 0755); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(payload, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	dataLink := filepath.Join(dir, "..data")
	tmpLink := filepath.Join(dir, "..data_tmp")
	if err := os.Symlink(payloadDir, tmpLink); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpLink, dataLink); err != nil {
		t.Fatal(err)
	}

	for name := range files {
		link := filepath.Join(dir, name)
		if _, err := os.Lstat(link); os.IsNotExist(err) {
			if err := os.Symlink(filepath.Join("..data", name), link); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestFingerprintConfigMapSymlinkSwap(t *testing.T) {
	dir := t.TempDir()
	rules := "groups: [{name: g, rules: [{alert: A, expr: SELECT 1 AS value}]}]"
	writeConfigMapDir(t, dir, "..2026_08_11_00_00_00.001", map[string]string{"rules.yaml": rules})

	pattern := []string{filepath.Join(dir, "*")}
	fp1, err := Fingerprint(pattern)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Rotating the payload directory without changing content must not
	// change the fingerprint — kubelet does this on every volume sync.
	writeConfigMapDir(t, dir, "..2026_08_11_00_01_00.002", map[string]string{"rules.yaml": rules})
	fp2, err := Fingerprint(pattern)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fp1 != fp2 {
		t.Errorf("payload dir rotation with identical content changed fingerprint: %d != %d", fp1, fp2)
	}

	// A content change through the same swap mechanism must change it.
	changed := "groups: [{name: g, rules: [{alert: B, expr: SELECT 2 AS value}]}]"
	writeConfigMapDir(t, dir, "..2026_08_11_00_02_00.003", map[string]string{"rules.yaml": changed})
	fp3, err := Fingerprint(pattern)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fp3 == fp1 {
		t.Error("content change via ..data swap did not change fingerprint")
	}

	// Parse must also read the swapped content correctly.
	groups, err := Parse(pattern)
	if err != nil {
		t.Fatalf("parse after swap: %v", err)
	}
	if len(groups) != 1 || len(groups[0].Rules) != 1 || groups[0].Rules[0].Alert != "B" {
		t.Errorf("parse did not see swapped content: %+v", groups)
	}
}

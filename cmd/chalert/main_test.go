package main

import (
	"context"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
	"github.com/garbett1/chalert/rule"
)

type fakeQuerier struct{}

func (fakeQuerier) Query(context.Context, string, time.Time) (datasource.Result, error) {
	return datasource.Result{}, nil
}

func (fakeQuerier) QueryRange(context.Context, string, time.Time, time.Time) (datasource.Result, error) {
	return datasource.Result{}, nil
}

type fakeQuerierBuilder struct{}

func (fakeQuerierBuilder) BuildWithParams(datasource.QuerierParams) datasource.Querier {
	return fakeQuerier{}
}

// parseGroups writes the YAML to a fixed-name file and parses it with
// deterministic Go-side rule IDs. A fixed file name keeps group identity
// stable across "reloads", matching a real rule file changing in place.
func parseGroups(t *testing.T, dir, yaml string) []config.Group {
	t.Helper()
	path := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	groups, err := config.Parse([]string{path})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	err = config.NormalizeRuleIDs(groups, func(expr string) (uint64, error) {
		h := fnv.New64a()
		h.Write([]byte(strings.Join(strings.Fields(expr), " ")))
		return h.Sum64(), nil
	})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	return groups
}

func TestReloadGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	qb := fakeQuerierBuilder{}
	opts := rule.GroupOptions{DefaultInterval: time.Hour}
	var wg sync.WaitGroup

	initial := parseGroups(t, dir, `
groups:
  - name: group-a
    rules:
      - alert: AlertA
        expr: SELECT 'a' AS scope, 1 AS value
  - name: group-b
    rules:
      - alert: AlertB
        expr: SELECT 'b' AS scope, 1 AS value
`)

	registry := make(map[uint64]*rule.Group)
	for _, cfg := range initial {
		g := rule.NewGroup(cfg, qb, opts)
		registry[g.ID()] = g
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Start(ctx, nil, nil)
		}()
	}
	defer func() {
		cancel()
		for _, g := range registry {
			g.Close()
		}
		wg.Wait()
	}()

	var groupA *rule.Group
	for _, g := range registry {
		if g.Name == "group-a" {
			groupA = g
		}
	}
	if groupA == nil {
		t.Fatal("group-a not found in registry")
	}
	checksumBefore := groupA.Checksum()

	updated := parseGroups(t, dir, `
groups:
  - name: group-a
    rules:
      - alert: AlertA
        expr: SELECT 'a' AS scope, 2 AS value
  - name: group-c
    rules:
      - alert: AlertC
        expr: SELECT 'c' AS scope, 1 AS value
`)

	reloadGroups(ctx, registry, updated, qb, nil, nil, &wg, opts)

	if len(registry) != 2 {
		t.Fatalf("expected 2 groups after reload, got %d", len(registry))
	}
	names := make(map[string]*rule.Group)
	for _, g := range registry {
		names[g.Name] = g
	}
	if _, ok := names["group-b"]; ok {
		t.Error("group-b should have been removed")
	}
	if _, ok := names["group-c"]; !ok {
		t.Error("group-c should have been added")
	}

	got, ok := names["group-a"]
	if !ok {
		t.Fatal("group-a should still exist")
	}
	if got != groupA {
		t.Error("group-a should be updated in place, not replaced")
	}

	// The update is applied asynchronously by the group's own loop.
	deadline := time.Now().Add(5 * time.Second)
	for groupA.Checksum() == checksumBefore {
		if time.Now().After(deadline) {
			t.Fatal("group-a checksum never changed after reload")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReloadGroupsNoChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	qb := fakeQuerierBuilder{}
	opts := rule.GroupOptions{DefaultInterval: time.Hour}
	var wg sync.WaitGroup

	yaml := `
groups:
  - name: group-a
    rules:
      - alert: AlertA
        expr: SELECT 'a' AS scope, 1 AS value
`
	initial := parseGroups(t, dir, yaml)

	registry := make(map[uint64]*rule.Group)
	for _, cfg := range initial {
		g := rule.NewGroup(cfg, qb, opts)
		registry[g.ID()] = g
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Start(ctx, nil, nil)
		}()
	}
	defer func() {
		cancel()
		for _, g := range registry {
			g.Close()
		}
		wg.Wait()
	}()

	var before *rule.Group
	for _, g := range registry {
		before = g
	}

	reloadGroups(ctx, registry, parseGroups(t, dir, yaml), qb, nil, nil, &wg, opts)

	if len(registry) != 1 {
		t.Fatalf("expected 1 group, got %d", len(registry))
	}
	for _, g := range registry {
		if g != before {
			t.Error("unchanged group should be left untouched")
		}
	}
}

func TestParseExternalLabels(t *testing.T) {
	labels, err := parseExternalLabels([]string{"env=prod", "cluster=buf101"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if labels["env"] != "prod" || labels["cluster"] != "buf101" {
		t.Errorf("unexpected labels: %v", labels)
	}

	if _, err := parseExternalLabels([]string{"missing-value"}); err == nil {
		t.Error("expected error for malformed label")
	}
	if _, err := parseExternalLabels([]string{"=value"}); err == nil {
		t.Error("expected error for empty name")
	}
	labels, err = parseExternalLabels(nil)
	if err != nil || labels != nil {
		t.Errorf("expected nil map for no labels, got %v, %v", labels, err)
	}
}

func TestRedactDSN(t *testing.T) {
	got := redactDSN("clickhouse://user:secret@host:9000/db")
	if strings.Contains(got, "secret") {
		t.Errorf("password not redacted: %s", got)
	}
	if got != "clickhouse://***@host:9000/db" {
		t.Errorf("unexpected redaction: %s", got)
	}
	if redactDSN("clickhouse://host:9000/db") != "clickhouse://host:9000/db" {
		t.Error("DSN without credentials should be unchanged")
	}
}

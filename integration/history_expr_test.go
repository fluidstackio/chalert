//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/garbett1/chalert/rule"
	"github.com/garbett1/chalert/statestore"
)

// exprs returns the expr of every row of table ("alert_history" or
// "alert_state FINAL") written for ruleID.
func exprs(t *testing.T, ctx context.Context, table string, ruleID uint64) []string {
	t.Helper()
	rows, err := chConn.Query(ctx, "SELECT expr FROM default."+table+" WHERE rule_id = ?", ruleID)
	if err != nil {
		t.Fatalf("query %s: %s", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var e string
		if err := rows.Scan(&e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

// -history.expr=none empties expr in alert_history only: alert_state keeps
// the full query, which restart recovery reads back. The default keeps it in
// both.
func TestHistoryExpr(t *testing.T) {
	ctx := context.Background()
	const query = "SELECT service, count() AS value FROM http_requests WHERE status >= 500 GROUP BY service"
	for _, tc := range []struct {
		name        string
		opts        []statestore.Option
		ruleID      uint64
		wantHistory string
	}{
		{name: "default", ruleID: 9101, wantHistory: query},
		{name: "full", opts: []statestore.Option{statestore.WithHistoryExpr(statestore.HistoryExprFull)}, ruleID: 9102, wantHistory: query},
		{name: "none", opts: []statestore.Option{statestore.WithHistoryExpr(statestore.HistoryExprNone)}, ruleID: 9103, wantHistory: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := statestore.New(chConn, "default", tc.opts...)
			now := time.Now()
			inst := []rule.AlertInstance{{
				ID: 1, RuleID: tc.ruleID, GroupName: "g", AlertName: "HistoryExpr",
				Labels: map[string]string{"service": "a"}, State: rule.StateFiring,
				Value: 3, Expr: query, ActiveAt: now, FiredAt: now,
			}}
			if err := store.RecordHistory(ctx, now, inst); err != nil {
				t.Fatal(err)
			}
			if err := store.Save(ctx, inst); err != nil {
				t.Fatal(err)
			}
			if got := exprs(t, ctx, "alert_history", tc.ruleID); len(got) != 1 || got[0] != tc.wantHistory {
				t.Errorf("alert_history expr: want [%q], got %q", tc.wantHistory, got)
			}
			if got := exprs(t, ctx, "alert_state FINAL", tc.ruleID); len(got) != 1 || got[0] != query {
				t.Errorf("alert_state expr: want [%q], got %q", query, got)
			}
		})
	}
}

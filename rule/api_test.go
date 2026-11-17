package rule

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
)

// The API reports each rule's health and state, and its pending and firing
// instances, as Prometheus serves them at /api/v1/rules.
func TestAPIGroups(t *testing.T) {
	g := evalGroup(1, firingRule(1), config.Rule{Alert: "Fails", Expr: exprFails, ID: 2})
	if got := APIGroups([]*Group{g})[0].Rules[0].Health; got != "unknown" {
		t.Errorf("before an evaluation: health %q, want unknown", got)
	}
	now := time.Now()
	g.execRules(context.Background(), g.Rules, now, now, 1, 0)

	groups := APIGroups([]*Group{g})
	if len(groups) != 1 || groups[0].Name != "g" || groups[0].Interval != 60 || len(groups[0].Rules) != 2 {
		t.Fatalf("groups = %+v, want group g, interval 60, 2 rules", groups)
	}
	firing, failed := groups[0].Rules[0], groups[0].Rules[1]
	if firing.Type != "alerting" || firing.Health != "ok" || firing.State != "firing" || len(firing.Alerts) != 2 {
		t.Errorf("firing rule = %+v, want alerting, ok, firing, 2 alerts", firing)
	}
	for _, a := range firing.Alerts {
		if a.State != "firing" || a.Value != "1e+00" || a.Labels["alertname"] != "Two" || !a.ActiveAt.Equal(now) {
			t.Errorf("alert = %+v, want firing, value 1e+00, alertname Two, active at %s", a, now)
		}
	}
	if failed.Health != "err" || !strings.Contains(failed.LastError, "UNKNOWN_TABLE") || failed.State != "inactive" || failed.Alerts == nil {
		t.Errorf("failed rule = %+v, want err with UNKNOWN_TABLE, inactive, empty alerts", failed)
	}
}

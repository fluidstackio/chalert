package rule

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Each evaluation is one JSON line with the fields the rule_evaluations view
// reads, under the fixed message it selects on. A uint64 rule ID survives as
// a decimal string, and a long error is cut to the bound.
func TestEvaluationLogWritesOneJSONLinePerEvaluation(t *testing.T) {
	var buf bytes.Buffer
	at := time.Date(2026, 10, 20, 12, 0, 0, 123000000, time.UTC)
	NewEvaluationLog(&buf).RecordEvaluations([]Evaluation{
		{RuleID: 18446744073709551615, GroupName: "g", AlertName: "Ok", EvaluatedAt: at, Duration: 120 * time.Millisecond, OK: true, Samples: 3},
		{RuleID: 9002, GroupName: "g", AlertName: "Broken", EvaluatedAt: at, Duration: 5 * time.Millisecond, Error: strings.Repeat("x", 5000)},
	})
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2:\n%s", len(lines), buf.String())
	}
	type line struct {
		Msg         string `json:"msg"`
		RuleID      string `json:"rule_id"`
		Group       string `json:"group"`
		Alert       string `json:"alert"`
		EvaluatedAt string `json:"evaluated_at"`
		DurationMS  int64  `json:"duration_ms"`
		OK          bool   `json:"ok"`
		Samples     int    `json:"samples"`
		Error       string `json:"error"`
	}
	var got [2]line
	for i, l := range lines {
		if err := json.Unmarshal([]byte(l), &got[i]); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", i, err, l)
		}
	}
	want0 := line{Msg: EvaluationLogMessage, RuleID: "18446744073709551615", Group: "g", Alert: "Ok", EvaluatedAt: "2026-10-20T12:00:00.123Z", DurationMS: 120, OK: true, Samples: 3}
	if got[0] != want0 {
		t.Errorf("ok line = %+v, want %+v", got[0], want0)
	}
	if g := got[1]; g.Msg != EvaluationLogMessage || g.RuleID != "9002" || g.OK || g.Samples != 0 || len(g.Error) != maxEvaluationErrorLen {
		t.Errorf("failed line = %+v (error length %d)", g, len(g.Error))
	}
}

package rule

import (
	"io"
	"log/slog"
	"strconv"
	"time"
)

// EvaluationLogMessage is the message of the evaluation log's lines. The
// ClickHouse materialized view that fills rule_evaluations selects
// chalert's log lines by it, so it never changes.
const EvaluationLogMessage = "chalert rule evaluation"

// maxEvaluationErrorLen bounds the error text one line carries.
const maxEvaluationErrorLen = 1024

// EvaluationRecorder takes one Evaluation per rule evaluated.
type EvaluationRecorder interface {
	RecordEvaluations(evals []Evaluation)
}

// EvaluationLog is the evaluation log: one JSON line per rule evaluation,
// success or failure, on chalert's log stream. The log collector ships the
// lines to ClickHouse, where a materialized view turns them into the
// rule_evaluations table. A rule that matched nothing and a rule whose query
// failed both write no alert_history row; these lines tell them apart.
type EvaluationLog struct {
	logger *slog.Logger
}

// NewEvaluationLog writes the evaluation log to w.
func NewEvaluationLog(w io.Writer) *EvaluationLog {
	return &EvaluationLog{logger: slog.New(slog.NewJSONHandler(w, nil))}
}

// RecordEvaluations writes one line per evaluation. rule_id is a decimal
// string, since a uint64 does not survive a JSON reader that parses numbers
// as floats.
func (l *EvaluationLog) RecordEvaluations(evals []Evaluation) {
	for _, e := range evals {
		msg := e.Error
		if len(msg) > maxEvaluationErrorLen {
			msg = msg[:maxEvaluationErrorLen]
		}
		l.logger.Info(EvaluationLogMessage,
			"rule_id", strconv.FormatUint(e.RuleID, 10),
			"group", e.GroupName,
			"alert", e.AlertName,
			"evaluated_at", e.EvaluatedAt.UTC().Format(time.RFC3339Nano),
			"duration_ms", e.Duration.Milliseconds(),
			"ok", e.OK,
			"samples", e.Samples,
			"error", msg)
	}
}

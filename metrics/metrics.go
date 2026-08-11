// Package metrics defines Prometheus metrics for chalert.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// RuleEvalDuration is a histogram of rule evaluation durations.
	RuleEvalDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "chalert",
		Name:      "rule_eval_duration_seconds",
		Help:      "Duration of rule evaluation in seconds.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"group", "rule"})

	// RuleEvalErrors counts rule evaluation errors.
	RuleEvalErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "chalert",
		Name:      "rule_eval_errors_total",
		Help:      "Total number of rule evaluation errors.",
	}, []string{"group", "rule"})

	// RuleEvalSamples tracks the number of samples returned by rule evaluation.
	RuleEvalSamples = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "chalert",
		Name:      "rule_eval_samples",
		Help:      "Number of samples returned by the last rule evaluation.",
	}, []string{"group", "rule"})

	// AlertsActive tracks active (pending + firing) alert instances per rule.
	AlertsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "chalert",
		Name:      "alerts_active",
		Help:      "Number of active alert instances per rule.",
	}, []string{"group", "rule", "state"})

	// NotifierSends counts alert notification sends.
	NotifierSends = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "chalert",
		Name:      "notifier_sends_total",
		Help:      "Total number of notification send attempts.",
	}, []string{"url", "result"})

	// NotifierErrors counts notification errors.
	NotifierErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "chalert",
		Name:      "notifier_errors_total",
		Help:      "Total number of notification errors.",
	})

	// ConfigReloads counts config reload attempts.
	ConfigReloads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "chalert",
		Name:      "config_reloads_total",
		Help:      "Total number of config reload attempts.",
	}, []string{"result"})

	// ConfigLastReloadSuccess tracks the last successful config reload timestamp.
	ConfigLastReloadSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "chalert",
		Name:      "config_last_reload_success_timestamp_seconds",
		Help:      "Timestamp of the last successful config reload.",
	})

	// ConfigLastReloadSuccessful reports whether the last config reload attempt succeeded (1) or failed (0).
	ConfigLastReloadSuccessful = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "chalert",
		Name:      "config_last_reload_successful",
		Help:      "Whether the last config reload attempt succeeded (1) or failed (0).",
	})
)

func init() {
	prometheus.MustRegister(
		RuleEvalDuration,
		RuleEvalErrors,
		RuleEvalSamples,
		AlertsActive,
		NotifierSends,
		NotifierErrors,
		ConfigReloads,
		ConfigLastReloadSuccess,
		ConfigLastReloadSuccessful,
	)
}

package rule

import "github.com/garbett1/chalert/metrics"

// recordActive sets the rule's alerts_active series to the instances it
// holds pending and firing. Called with ar.mu held.
func (ar *AlertingRule) recordActive() {
	var pending, firing int
	for _, a := range ar.alerts {
		switch a.State {
		case StatePending:
			pending++
		case StateFiring:
			firing++
		}
	}
	metrics.AlertsActive.WithLabelValues(ar.groupName, ar.name, StatePending.String()).Set(float64(pending))
	metrics.AlertsActive.WithLabelValues(ar.groupName, ar.name, StateFiring.String()).Set(float64(firing))
}

// forgetActive removes the rule's alerts_active series once it stops
// running, so a removed rule does not report its last counts forever.
func (ar *AlertingRule) forgetActive() {
	for _, s := range []AlertState{StatePending, StateFiring} {
		metrics.AlertsActive.DeleteLabelValues(ar.groupName, ar.name, s.String())
	}
}

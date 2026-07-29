package source

import "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"

// sourceMetrics collects the Kafka source's metric updates behind a small
// helper so the rest of the source never touches the global metric registry
// directly. The underlying Prometheus collectors are unchanged — only the
// call sites move here.
type sourceMetrics struct{}

// recordDeserFailure increments the counter tracking records that failed
// deserialization (dropped, DLQ'd or failed depending on policy).
func (sourceMetrics) recordDeserFailure() {
	metrics.RecordsFailedTotal.Inc()
}

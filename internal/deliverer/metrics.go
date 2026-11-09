package deliverer

import (
	"strconv"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
)

// Metrics is what the pool records. A nil *Metrics records nothing.
//
// Series are labelled by endpoint id, so their number grows with the
// number of endpoints, not with traffic. That is what makes per-endpoint
// success rates and latencies possible; a gateway with tens of thousands
// of endpoints would aggregate these before export instead.
type Metrics struct {
	claims    *metrics.CounterVec
	attempts  *metrics.CounterVec
	duration  *metrics.HistogramVec
	inFlight  *metrics.GaugeVec
	lost      *metrics.CounterVec
	reportErr *metrics.CounterVec
	wakeups   *metrics.CounterVec
	ended     *metrics.CounterVec
}

// attemptBuckets span a fast receiver (milliseconds) to the sender's
// timeout (10s by default) and a bit beyond, for slow TLS handshakes.
var attemptBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// NewMetrics registers the pool's series on reg.
func NewMetrics(reg *metrics.Registry) *Metrics {
	return &Metrics{
		claims: reg.NewCounterVec("webhook_claims_total",
			"Claim calls by result: deliveries (took at least one), empty (none due), error.", "result"),
		attempts: reg.NewCounterVec("webhook_attempts_total",
			"Finished attempts by endpoint and outcome: delivered, retry, rejected, blocked. "+
				"rate(outcome=\"delivered\") over the total is an endpoint's success rate.",
			"endpoint", "outcome"),
		duration: reg.NewHistogramVec("webhook_attempt_duration_seconds",
			"Time from the start of the request to the end of the response, per endpoint.",
			attemptBuckets, "endpoint"),
		inFlight: reg.NewGaugeVec("webhook_in_flight_requests",
			"Requests to receivers running right now on this instance."),
		lost: reg.NewCounterVec("webhook_lease_lost_total",
			"Attempts whose lease another deliverer or the reaper took before the outcome was reported."),
		reportErr: reg.NewCounterVec("webhook_report_errors_total",
			"Outcomes that could not be written; the reaper retries those deliveries when their lease runs out."),
		ended: reg.NewCounterVec("webhook_deliveries_ended_total",
			"Deliveries that reached a final state after an attempt, by endpoint and state: delivered, failed "+
				"(attempts spent: the dead-letter queue), rejected, blocked.",
			"endpoint", "state"),
		wakeups: reg.NewCounterVec("webhook_claim_wakeups_total",
			"Claims made at once, without waiting for the poll interval, because an ordered endpoint's delivery finished."),
	}
}

func (m *Metrics) claimed(n int, err error) {
	if m == nil {
		return
	}
	switch {
	case err != nil:
		m.claims.Inc("error")
	case n == 0:
		m.claims.Inc("empty")
	default:
		m.claims.Inc("deliveries")
	}
}

func (m *Metrics) started() {
	if m != nil {
		m.inFlight.Add(1)
	}
}

func (m *Metrics) sent(endpointID int64, r event.Result, took time.Duration) {
	if m == nil {
		return
	}
	ep := strconv.FormatInt(endpointID, 10)
	m.inFlight.Add(-1)
	m.attempts.Inc(ep, r.Outcome.String())
	m.duration.Observe(took.Seconds(), ep)
}

func (m *Metrics) leaseLost() {
	if m != nil {
		m.lost.Inc()
	}
}

func (m *Metrics) reportFailed() {
	if m != nil {
		m.reportErr.Inc()
	}
}

func (m *Metrics) wokeUp() {
	if m != nil {
		m.wakeups.Inc()
	}
}

func (m *Metrics) finished(d event.Delivery) {
	if m == nil {
		return
	}
	switch d.State {
	case event.StateDelivered, event.StateFailed, event.StateRejected, event.StateBlocked:
		m.ended.Inc(strconv.FormatInt(d.EndpointID, 10), string(d.State))
	}
}

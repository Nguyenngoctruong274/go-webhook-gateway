package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/breaker"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
)

// Snapshot is the state of the queue and the endpoints at one moment,
// as the metrics show it.
type Snapshot struct {
	// DueNow counts pending deliveries whose next attempt is due, to
	// endpoints whose breaker is closed: the backlog deliverers should be
	// taking right now.
	DueNow int
	// HeldBack counts due deliveries to endpoints whose breaker is open
	// or half-open: waiting for the receiver, not for the gateway.
	HeldBack int
	// Scheduled counts pending deliveries waiting for their backoff.
	Scheduled int
	InFlight  int
	// OldestDue is how long the longest-waiting delivery counted in
	// DueNow has been due; 0 when none is. It rises when deliverers fall
	// behind. Deliveries held back by a breaker are left out, so a
	// customer's outage does not page the gateway's operators; a rate
	// limit or ordering does hold deliveries in DueNow, by design.
	OldestDue time.Duration
	// Endpoints maps each endpoint to its breaker status and whether it
	// is enabled.
	Endpoints []EndpointHealth
}

// EndpointHealth is one endpoint's line in a Snapshot.
type EndpointHealth struct {
	ID      int64
	Enabled bool
	Breaker breaker.Status
}

// Snapshot reads the queue counts and every endpoint's breaker. The
// counts come from the open deliveries only (pending and in flight),
// through the partial index on them, so the cost does not grow with the
// history of delivered events.
func (s *Store) Snapshot(ctx context.Context) (Snapshot, error) {
	var (
		snap   Snapshot
		oldest float64
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE d.state = 'pending' AND d.next_attempt_at <= now() AND p.breaker_status = 'closed'),
		       count(*) FILTER (WHERE d.state = 'pending' AND d.next_attempt_at <= now() AND p.breaker_status <> 'closed'),
		       count(*) FILTER (WHERE d.state = 'pending' AND d.next_attempt_at > now()),
		       count(*) FILTER (WHERE d.state = 'in_flight'),
		       COALESCE(extract(epoch FROM now() - min(d.next_attempt_at)
		                FILTER (WHERE d.state = 'pending' AND d.next_attempt_at <= now() AND p.breaker_status = 'closed')), 0)
		FROM deliveries AS d JOIN endpoints AS p ON p.id = d.endpoint_id
		WHERE d.state IN ('pending', 'in_flight')`).
		Scan(&snap.DueNow, &snap.HeldBack, &snap.Scheduled, &snap.InFlight, &oldest)
	if err != nil {
		return snap, fmt.Errorf("snapshot: %w", err)
	}
	snap.OldestDue = time.Duration(oldest * float64(time.Second))

	rows, err := s.db.QueryContext(ctx, `SELECT id, enabled, breaker_status FROM endpoints ORDER BY id`)
	if err != nil {
		return snap, fmt.Errorf("snapshot: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			e      EndpointHealth
			status string
		)
		if err := rows.Scan(&e.ID, &e.Enabled, &status); err != nil {
			return snap, fmt.Errorf("snapshot: %w", err)
		}
		if e.Breaker, err = breaker.ParseStatus(status); err != nil {
			return snap, fmt.Errorf("snapshot: endpoint %d: %w", e.ID, err)
		}
		snap.Endpoints = append(snap.Endpoints, e)
	}
	if err := rows.Err(); err != nil {
		return snap, fmt.Errorf("snapshot: %w", err)
	}
	return snap, nil
}

var breakerStatuses = []breaker.Status{breaker.Closed, breaker.Open, breaker.HalfOpen}

// RegisterQueueMetrics exposes a Snapshot as gauges computed when
// /metrics is scraped: the backlog by kind, the age of the oldest due
// delivery, breakers by status in total and per endpoint, and endpoints
// by enabled. The snapshot is cached for cacheFor so that several
// scrapers or a tight scrape interval cost one read; a failed read keeps
// the last snapshot, logs the error and counts it, so a database outage
// shows up as stale gauges plus a rising error counter rather than as a
// scrape failure.
func (s *Store) RegisterQueueMetrics(reg *metrics.Registry, logger *slog.Logger, cacheFor time.Duration) {
	if logger == nil {
		logger = slog.Default()
	}
	readErrors := reg.NewCounterVec("webhook_metrics_read_errors_total",
		"Failed reads of the queue snapshot behind the webhook_backlog and breaker gauges; while it rises those gauges are stale.")
	var (
		mu     sync.Mutex
		last   Snapshot
		readAt time.Time
	)
	snapshot := func() Snapshot {
		mu.Lock()
		defer mu.Unlock()
		if !readAt.IsZero() && time.Since(readAt) < cacheFor {
			return last
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		snap, err := s.Snapshot(ctx)
		if err != nil {
			readErrors.Inc()
			logger.ErrorContext(ctx, "queue metrics", slog.Any("error", err))
			return last
		}
		last, readAt = snap, time.Now()
		return last
	}

	reg.NewGaugeVecFunc("webhook_backlog",
		"Open deliveries by kind: due (should be claimed now), held (due, behind an open or half-open breaker), "+
			"scheduled (waiting for a retry backoff), in_flight.",
		[]string{"kind"}, func(set func(float64, ...string)) {
			snap := snapshot()
			set(float64(snap.DueNow), "due")
			set(float64(snap.HeldBack), "held")
			set(float64(snap.Scheduled), "scheduled")
			set(float64(snap.InFlight), "in_flight")
		})
	reg.NewGaugeFunc("webhook_oldest_due_seconds",
		"How long the longest-waiting due delivery to an endpoint with a closed breaker has been due; 0 when none is.",
		func() float64 { return snapshot().OldestDue.Seconds() })
	reg.NewGaugeVecFunc("webhook_breakers",
		"Endpoints by circuit-breaker status.", []string{"status"}, func(set func(float64, ...string)) {
			counts := map[breaker.Status]int{}
			for _, e := range snapshot().Endpoints {
				counts[e.Breaker]++
			}
			for _, st := range breakerStatuses {
				set(float64(counts[st]), string(st))
			}
		})
	reg.NewGaugeVecFunc("webhook_endpoint_breaker",
		"1 for the status each endpoint's circuit breaker is in, 0 for the others.",
		[]string{"endpoint", "status"}, func(set func(float64, ...string)) {
			for _, e := range snapshot().Endpoints {
				id := strconv.FormatInt(e.ID, 10)
				for _, st := range breakerStatuses {
					v := 0.0
					if e.Breaker == st {
						v = 1
					}
					set(v, id, string(st))
				}
			}
		})
	reg.NewGaugeVecFunc("webhook_endpoints", "Endpoints by whether they are enabled (true) or paused (false).",
		[]string{"enabled"}, func(set func(float64, ...string)) {
			var on, off float64
			for _, e := range snapshot().Endpoints {
				if e.Enabled {
					on++
				} else {
					off++
				}
			}
			set(on, "true")
			set(off, "false")
		})
}

// RegisterPoolMetrics exposes the connection pool: a saturated pool shows
// up as waits before it shows up as latency.
func RegisterPoolMetrics(reg *metrics.Registry, db *sql.DB) {
	reg.NewGaugeFunc("db_pool_open_connections", "Open connections, in use or idle.",
		func() float64 { return float64(db.Stats().OpenConnections) })
	reg.NewGaugeFunc("db_pool_in_use_connections", "Connections currently handed out.",
		func() float64 { return float64(db.Stats().InUse) })
	reg.NewGaugeFunc("db_pool_max_open_connections", "Configured connection limit.",
		func() float64 { return float64(db.Stats().MaxOpenConnections) })
	reg.NewCounterFunc("db_pool_wait_total", "Times a caller had to wait for a connection.",
		func() float64 { return float64(db.Stats().WaitCount) })
	reg.NewCounterFunc("db_pool_wait_seconds_total", "Total time callers waited for a connection.",
		func() float64 { return db.Stats().WaitDuration.Seconds() })
}

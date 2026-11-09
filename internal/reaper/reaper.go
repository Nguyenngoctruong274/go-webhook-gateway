// Package reaper returns deliveries whose deliverer went silent to the
// queue. A deliverer that crashes, is killed or loses its network stops
// renewing its lease; once the lease runs out the reaper ends the attempt
// as a transient failure, so the delivery retries or fails by the same
// rules as any other failure.
package reaper

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
)

// Store is what the reaper needs from the job store.
type Store interface {
	// Reap expires up to limit run-out leases and returns the ids it expired.
	Reap(ctx context.Context, limit int) ([]int64, error)
}

// Options tunes a Reaper. Zero values take the defaults.
type Options struct {
	// Interval is the pause between sweeps when the last one found
	// nothing. Default 5s. A sweep that filled its batch is followed by
	// another at once, so a backlog clears quickly.
	Interval time.Duration
	// BatchSize is the most leases one sweep expires. Default 100.
	BatchSize int
	// Metrics, when set, receives the reaper's counters.
	Metrics *metrics.Registry
}

// Reaper sweeps expired leases on a timer.
type Reaper struct {
	store  Store
	logger *slog.Logger
	opts   Options
	// reaped counts leases expired; sweeps counts sweeps by result.
	reaped *metrics.CounterVec
	sweeps *metrics.CounterVec
}

// New returns a Reaper over store.
func New(store Store, logger *slog.Logger, opts Options) *Reaper {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if logger == nil {
		logger = slog.Default()
	}
	reg := opts.Metrics
	if reg == nil {
		reg = metrics.NewRegistry() // count anyway; nothing serves it
	}
	return &Reaper{store: store, logger: logger, opts: opts,
		reaped: reg.NewCounterVec("webhook_reaped_leases_total",
			"Leases that ran out without a report: each is a deliverer that crashed, stalled or lost its network."),
		sweeps: reg.NewCounterVec("webhook_reaper_sweeps_total", "Reaper sweeps by result: ok or error.", "result"),
	}
}

// Run sweeps until ctx ends. Errors are logged and retried on the next
// sweep: a database blip must not stop the reaper for good, because then
// crashed deliverers' deliveries would stay in flight forever. It returns nil when
// ctx ends.
func (r *Reaper) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		n, err := r.Sweep(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			r.logger.ErrorContext(ctx, "reap failed", slog.Any("error", err))
			timer.Reset(r.opts.Interval)
		case n == r.opts.BatchSize:
			timer.Reset(0) // more may be waiting
		default:
			timer.Reset(r.opts.Interval)
		}
	}
}

// Sweep expires one batch and returns how many leases it ended.
func (r *Reaper) Sweep(ctx context.Context) (int, error) {
	ids, err := r.store.Reap(ctx, r.opts.BatchSize)
	if len(ids) > 0 {
		r.logger.WarnContext(ctx, "expired leases of silent deliverers",
			slog.Int("deliveries", len(ids)), slog.Any("ids", ids))
	}
	r.reaped.Add(float64(len(ids)))
	if err != nil && !errors.Is(err, context.Canceled) {
		r.sweeps.Inc("error")
		return len(ids), err
	}
	r.sweeps.Inc("ok")
	return len(ids), nil
}

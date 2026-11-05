package deliverer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/postgres"
)

// Store is what the pool needs from the delivery store. Every method
// that takes an attempt uses it as the fencing token Claim handed out.
type Store interface {
	Claim(ctx context.Context, limit int, lease time.Duration) ([]postgres.Claimed, error)
	Finish(ctx context.Context, id int64, attempt int, r event.Result) (event.Delivery, error)
	Heartbeat(ctx context.Context, id int64, attempt int, lease time.Duration) error
}

// Transport sends one attempt; *Sender is the real one, tests fake it.
type Transport interface {
	Send(ctx context.Context, r Request) event.Result
}

// MaxConcurrency bounds Options.Concurrency.
const MaxConcurrency = 1000

// reportTimeout bounds each claim and each report of an outcome. Both run
// detached from the pool's context, so that shutdown cannot abort a claim
// that already took rows, or drop the result of a finished request.
const reportTimeout = 10 * time.Second

// Options tunes a Pool. Zero values take the defaults.
type Options struct {
	// Concurrency is the most requests in flight at once. Default 16.
	Concurrency int
	// Lease is how long a claimed delivery belongs to this pool without a
	// heartbeat. It must exceed the sender's timeout, or a slow receiver
	// would get the same request twice. Default 60s.
	Lease time.Duration
	// Heartbeat is how often an in-flight delivery's lease is renewed;
	// shorter than Lease. Default Lease/3.
	Heartbeat time.Duration
	// PollInterval is the pause before claiming again when nothing was
	// due. Default 1s.
	PollInterval time.Duration
	// DrainTimeout is how long shutdown waits for in-flight requests
	// before cancelling them. Default 15s.
	DrainTimeout time.Duration
}

// Pool claims due deliveries and sends them.
type Pool struct {
	store  Store
	send   Transport
	logger *slog.Logger
	opts   Options
}

// New returns a Pool.
func New(store Store, send Transport, logger *slog.Logger, opts Options) (*Pool, error) {
	if opts.Concurrency == 0 {
		opts.Concurrency = 16
	}
	if opts.Lease == 0 {
		opts.Lease = 60 * time.Second
	}
	if opts.Heartbeat == 0 {
		opts.Heartbeat = opts.Lease / 3
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	if opts.DrainTimeout == 0 {
		opts.DrainTimeout = 15 * time.Second
	}
	var errs []error
	if store == nil || send == nil {
		errs = append(errs, errors.New("store and transport are required"))
	}
	if opts.Concurrency < 1 || opts.Concurrency > MaxConcurrency {
		errs = append(errs, fmt.Errorf("concurrency must be between 1 and %d", MaxConcurrency))
	}
	if opts.Lease < time.Millisecond {
		errs = append(errs, errors.New("lease must be at least 1ms"))
	}
	if opts.Heartbeat <= 0 || opts.Heartbeat >= opts.Lease {
		errs = append(errs, errors.New("heartbeat must be positive and shorter than the lease"))
	}
	if opts.PollInterval < 0 || opts.DrainTimeout < 0 {
		errs = append(errs, errors.New("poll interval and drain timeout must not be negative"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("deliverer: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{store: store, send: send, logger: logger, opts: opts}, nil
}

// Run claims and sends until ctx ends, then drains: it claims nothing
// more, waits up to DrainTimeout for in-flight requests, cancels the
// rest and waits for their outcomes to be reported. A cancelled request
// is reported as a transient failure, so the delivery retries elsewhere.
func (p *Pool) Run(ctx context.Context) error {
	sendCtx, cancelSends := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSends()

	var inFlight sync.WaitGroup
	slots := make(chan struct{}, p.opts.Concurrency)
	p.logger.InfoContext(ctx, "deliverer started",
		slog.Int("concurrency", p.opts.Concurrency), slog.Duration("lease", p.opts.Lease))

	p.claimLoop(ctx, sendCtx, slots, &inFlight)

	done := make(chan struct{})
	go func() {
		inFlight.Wait()
		close(done)
	}()
	p.logger.Info("deliverer draining", slog.Int("in_flight", len(slots)))
	timer := time.NewTimer(p.opts.DrainTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		p.logger.Warn("drain timed out; cancelling in-flight requests", slog.Int("in_flight", len(slots)))
		cancelSends()
		<-done
	}
	p.logger.Info("deliverer stopped")
	return nil
}

// claimLoop holds one slot per in-flight delivery and claims only as
// many as there are free slots, so concurrency never exceeds the bound
// and no claimed delivery waits for a slot while its lease runs down.
func (p *Pool) claimLoop(ctx, sendCtx context.Context, slots chan struct{}, inFlight *sync.WaitGroup) {
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		free := 1
	fill:
		for free < cap(slots) {
			select {
			case slots <- struct{}{}:
				free++
			default:
				break fill
			}
		}
		if ctx.Err() != nil {
			for range free {
				<-slots
			}
			return
		}

		claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
		claimed, err := p.store.Claim(claimCtx, free, p.opts.Lease)
		cancel()
		for range free - len(claimed) {
			<-slots
		}
		for _, c := range claimed {
			inFlight.Add(1)
			go func() {
				defer inFlight.Done()
				defer func() { <-slots }()
				p.process(sendCtx, c)
			}()
		}
		if err != nil {
			p.logger.ErrorContext(ctx, "claim failed", slog.Any("error", err))
		}
		if len(claimed) < free {
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.opts.PollInterval):
			}
		}
	}
}

// process sends one claimed delivery and reports the outcome.
func (p *Pool) process(sendCtx context.Context, c postgres.Claimed) {
	d := c.Delivery
	log := p.logger.With(slog.Int64("delivery_id", d.ID), slog.Int64("endpoint_id", d.EndpointID),
		slog.String("type", c.Type), slog.Int("attempt", d.Attempt))
	ctx, cancel := context.WithCancel(sendCtx)
	defer cancel()

	// Heartbeat while the request is in flight; stop the request if the
	// lease is found to be lost, since its outcome could not be reported.
	beating := make(chan struct{})
	go func() {
		defer close(beating)
		p.heartbeat(ctx, d, cancel, log)
	}()

	start := time.Now()
	res := p.send.Send(ctx, Request{
		DeliveryID: d.ID, EventType: c.Type, Body: c.Payload, URL: c.URL,
		Secrets: c.Secrets.Active(time.Now()),
	})
	cancel()
	<-beating

	reportCtx, cancelReport := context.WithTimeout(context.WithoutCancel(sendCtx), reportTimeout)
	defer cancelReport()
	after, err := p.store.Finish(reportCtx, d.ID, d.Attempt, res)
	switch {
	case errors.Is(err, event.ErrLeaseLost):
		log.Warn("lease lost before the outcome was reported; another deliverer owns it", slog.Any("result", res.Outcome))
		return
	case err != nil:
		// The delivery stays in flight; the reaper retries it once the
		// lease runs out.
		log.Error("reporting the outcome failed", slog.Any("error", err))
		return
	}
	attrs := []any{slog.String("outcome", res.Outcome.String()), slog.Int("status", res.Status),
		slog.String("state", string(after.State)), slog.Duration("took", time.Since(start))}
	switch after.State {
	case event.StateDelivered:
		log.Info("delivered", attrs...)
	case event.StatePending:
		log.Warn("attempt failed; will retry", append(attrs, slog.Time("next_attempt_at", after.NextAttemptAt), slog.String("error", res.Error))...)
	default:
		log.Warn("delivery ended", append(attrs, slog.String("error", res.Error))...)
	}
}

// heartbeat renews the lease until ctx ends. If the store says the lease
// is gone, it cancels the request: nothing this attempt does can be
// reported any more.
func (p *Pool) heartbeat(ctx context.Context, d event.Delivery, cancel context.CancelFunc, log *slog.Logger) {
	ticker := time.NewTicker(p.opts.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		beatCtx, cancelBeat := context.WithTimeout(ctx, p.opts.Heartbeat)
		err := p.store.Heartbeat(beatCtx, d.ID, d.Attempt, p.opts.Lease)
		cancelBeat()
		switch {
		case err == nil:
		case errors.Is(err, event.ErrLeaseLost), errors.Is(err, event.ErrNotFound):
			log.Warn("lease lost mid-request; cancelling", slog.Any("error", err))
			cancel()
			return
		case ctx.Err() != nil:
			return
		default:
			log.Warn("heartbeat failed; will retry", slog.Any("error", err))
		}
	}
}

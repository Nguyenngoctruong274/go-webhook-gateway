package event

import (
	"fmt"
	"time"
)

// State is where a delivery is in its lifecycle.
type State string

// The states of a delivery. Pending deliveries are waiting for their
// next attempt; in_flight ones are with a deliverer under a lease; the
// rest are final until an operator replays them.
const (
	StatePending   State = "pending"
	StateInFlight  State = "in_flight"
	StateDelivered State = "delivered"
	// StateFailed: attempts exhausted on transient failures. The
	// dead-letter queue.
	StateFailed State = "failed"
	// StateRejected: the receiver gave a final answer (most 4xx).
	StateRejected State = "rejected"
	// StateBlocked: the gateway refused to send to this endpoint.
	StateBlocked State = "blocked"
	// StateCancelled: an operator or the endpoint being deleted stopped
	// a pending delivery.
	StateCancelled State = "cancelled"
)

// ParseState converts a stored state back to a State.
func ParseState(s string) (State, error) {
	switch st := State(s); st {
	case StatePending, StateInFlight, StateDelivered, StateFailed, StateRejected, StateBlocked, StateCancelled:
		return st, nil
	}
	return "", fmt.Errorf("unknown delivery state %q", s)
}

// Replayable reports whether an operator may replay a delivery in this
// state. Delivered ones can be replayed too (a customer lost the data);
// in-flight and pending ones cannot, since they are still going to run.
func (s State) Replayable() bool {
	switch s {
	case StateDelivered, StateFailed, StateRejected, StateBlocked, StateCancelled:
		return true
	}
	return false
}

// Delivery is one event bound for one endpoint and the bookkeeping of
// its attempts. The attempt number is a fencing token: Claim returns it
// and every report must present it, so a deliverer that lost its lease
// cannot overwrite the attempt that replaced it.
type Delivery struct {
	ID         int64
	EventID    int64
	EndpointID int64
	State      State
	Attempt    int
	// MaxAttempts is copied from the endpoint at fan-out time, so
	// changing an endpoint does not change deliveries already queued.
	MaxAttempts int
	// NextAttemptAt is the earliest time a pending delivery may be claimed.
	NextAttemptAt time.Time
	// LeaseUntil is when an in-flight delivery's lease ends; zero otherwise.
	LeaseUntil time.Time
	// LastStatus is the HTTP status of the latest attempt, 0 if none or
	// no response.
	LastStatus int
	// LastError describes the latest failed attempt (see Describe).
	LastError string
	// DeliveredAt is set when the delivery succeeded.
	DeliveredAt time.Time
	CreatedAt   time.Time
}

// Claim starts the next attempt of a due pending delivery under a lease
// that ends at now+lease and returns the attempt's fencing token.
func (d *Delivery) Claim(now time.Time, lease time.Duration) (int, error) {
	if lease <= 0 {
		return 0, fmt.Errorf("delivery %d: claim: lease must be positive, got %v", d.ID, lease)
	}
	if d.State != StatePending {
		return 0, d.transitionErr("claim", ErrInvalidTransition)
	}
	if now.Before(d.NextAttemptAt) {
		return 0, fmt.Errorf("delivery %d: claim: due at %s: %w", d.ID, d.NextAttemptAt.Format(time.RFC3339Nano), ErrNotDue)
	}
	if d.Attempt >= d.MaxAttempts {
		return 0, fmt.Errorf("delivery %d: claim: %d of %d attempts used: %w", d.ID, d.Attempt, d.MaxAttempts, ErrInvalidTransition)
	}
	d.Attempt++
	d.State = StateInFlight
	d.LeaseUntil = now.Add(lease)
	return d.Attempt, nil
}

// Heartbeat extends the lease of an in-flight attempt to now+lease.
func (d *Delivery) Heartbeat(attempt int, now time.Time, lease time.Duration) error {
	if lease <= 0 {
		return fmt.Errorf("delivery %d: heartbeat: lease must be positive, got %v", d.ID, lease)
	}
	if err := d.checkOwner("heartbeat", attempt); err != nil {
		return err
	}
	d.LeaseUntil = now.Add(lease)
	return nil
}

// Result is what a deliverer learned from one attempt.
type Result struct {
	Outcome Outcome
	// Status is the HTTP status code, 0 when no response arrived.
	Status int
	// Error describes a failure; see Describe. Empty on success.
	Error string
}

// Finish records the result of the attempt a deliverer holds.
//
// Delivered, Rejected and Blocked are final. Retry schedules the next
// attempt at now plus the backoff for the number of failures so far, or
// fails the delivery for good when the attempts are spent. A lease that
// ran out is not checked here: until the reaper moves the delivery on,
// the attempt still owns it, and a result that arrived late beats
// sending the event again.
func (d *Delivery) Finish(attempt int, now time.Time, r Result, b Backoff, rnd Rand) error {
	if err := d.checkOwner("finish", attempt); err != nil {
		return err
	}
	d.LeaseUntil = time.Time{}
	d.LastStatus = r.Status
	d.LastError = r.Error
	switch r.Outcome {
	case Delivered:
		d.State = StateDelivered
		d.LastError = ""
		d.DeliveredAt = now
	case Rejected:
		d.State = StateRejected
	case Blocked:
		d.State = StateBlocked
	case Retry:
		d.scheduleRetry(now, b, rnd)
	default:
		return fmt.Errorf("delivery %d: finish: unknown outcome %v", d.ID, r.Outcome)
	}
	return nil
}

// Expire ends an in-flight attempt whose lease ran out, as the reaper
// does for deliverers that crashed. It counts as a transient failure.
func (d *Delivery) Expire(now time.Time, b Backoff, rnd Rand) error {
	if d.State != StateInFlight {
		return d.transitionErr("expire", ErrInvalidTransition)
	}
	if now.Before(d.LeaseUntil) {
		return fmt.Errorf("delivery %d: expire: lease until %s: %w", d.ID, d.LeaseUntil.Format(time.RFC3339Nano), ErrLeaseActive)
	}
	d.LeaseUntil = time.Time{}
	d.LastStatus = 0
	d.LastError = "lease expired before the attempt finished"
	d.scheduleRetry(now, b, rnd)
	return nil
}

// Cancel stops a pending delivery. An in-flight one cannot be cancelled:
// the request may already be on the wire.
func (d *Delivery) Cancel() error {
	if d.State != StatePending {
		return d.transitionErr("cancel", ErrInvalidTransition)
	}
	d.State = StateCancelled
	return nil
}

// Replay queues a finished delivery again with a fresh set of attempts,
// due now. The previous status and error stay visible until the next
// attempt overwrites them.
func (d *Delivery) Replay(now time.Time) error {
	if !d.State.Replayable() {
		return d.transitionErr("replay", ErrInvalidTransition)
	}
	d.State = StatePending
	d.Attempt = 0
	d.NextAttemptAt = now
	d.DeliveredAt = time.Time{}
	return nil
}

func (d *Delivery) scheduleRetry(now time.Time, b Backoff, rnd Rand) {
	if d.Attempt >= d.MaxAttempts {
		d.State = StateFailed
		return
	}
	d.State = StatePending
	d.NextAttemptAt = now.Add(b.Delay(d.Attempt, rnd))
}

func (d *Delivery) checkOwner(op string, attempt int) error {
	if d.State != StateInFlight || d.Attempt != attempt {
		return fmt.Errorf("delivery %d: %s: attempt %d, delivery is %s at attempt %d: %w",
			d.ID, op, attempt, d.State, d.Attempt, ErrLeaseLost)
	}
	return nil
}

func (d *Delivery) transitionErr(op string, err error) error {
	return fmt.Errorf("delivery %d: cannot %s a %s delivery: %w", d.ID, op, d.State, err)
}

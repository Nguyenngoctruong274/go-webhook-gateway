package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lib/pq"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/breaker"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

// MaxClaimBatch caps how many deliveries one Claim may take, so a single
// deliverer cannot hoard the queue or hold row locks across a huge update.
const MaxClaimBatch = 1000

// Claimed is a delivery a deliverer holds, with everything needed to send
// it: the request body and type from the event, the URL and the signing
// secrets from the endpoint. One claim is one round trip.
type Claimed struct {
	Delivery event.Delivery
	Type     string
	Payload  []byte
	URL      string
	Secrets  signing.Rotation
}

// Claim starts the next attempt of up to limit due pending deliveries, in
// due order (earliest next_attempt_at, then id), under a lease that ends
// at the database's now() plus lease. No due delivery is not an error:
// Claim returns an empty slice.
//
// FOR UPDATE SKIP LOCKED lets many deliverers claim at once without
// waiting on each other: a row another transaction is claiming is
// skipped, not waited for, and once that transaction commits the row is
// in flight and no longer matches. Each attempt therefore goes to exactly
// one deliverer, which is what the fencing token (the attempt number)
// relies on.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration) ([]Claimed, error) {
	if limit < 1 || limit > MaxClaimBatch {
		return nil, fmt.Errorf("claim: limit must be between 1 and %d, got %d", MaxClaimBatch, limit)
	}
	micros := lease.Microseconds()
	if micros < 1 {
		return nil, fmt.Errorf("claim: lease must be at least 1µs, got %v", lease)
	}

	var claimed []Claimed
	err := s.inTx(ctx, "claim", func(tx *sql.Tx) error {
		var err error
		claimed, err = s.claimTx(ctx, tx, limit, micros)
		return err
	})
	if err != nil {
		return nil, err
	}
	// RETURNING does not promise an order; put the batch in claim order.
	sortClaimed(claimed)
	return claimed, nil
}

// claimTx is Claim inside its transaction.
//
// The breaker of each endpoint gates the claim: closed endpoints are
// claimed freely; open ones are skipped until their period elapses;
// after that, and when a half-open probe has timed out, exactly one
// delivery is let through as the probe, chosen here by a SELECT on the
// endpoint rows under FOR UPDATE SKIP LOCKED so that two instances
// claiming at once cannot both probe. Deliveries locked but not admitted
// are left untouched; they are not updated, so the lock is released at
// commit and they stay pending.
func (s *Store) claimTx(ctx context.Context, tx *sql.Tx, limit int, micros int64) ([]Claimed, error) {
	// Candidate deliveries: due, pending, and not behind a breaker that
	// is plainly shut. The breaker columns come along so that probes can
	// be decided without a second read.
	rows, err := tx.QueryContext(ctx, `
		SELECT d.id, d.endpoint_id, p.breaker_status, p.breaker_opened_until, p.breaker_probe_until, now()
		FROM deliveries AS d JOIN endpoints AS p ON p.id = d.endpoint_id
		WHERE d.state = 'pending' AND d.next_attempt_at <= now()
		  AND (p.breaker_status = 'closed'
		       OR (p.breaker_status = 'open' AND p.breaker_opened_until <= now())
		       OR (p.breaker_status = 'half_open' AND p.breaker_probe_until <= now()))
		ORDER BY d.next_attempt_at, d.id
		LIMIT $1
		FOR UPDATE OF d SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	type candidate struct {
		id, endpointID int64
		st             breaker.State
	}
	var (
		cands []candidate
		now   time.Time
	)
	for rows.Next() {
		var (
			c      candidate
			status string
			opened sql.NullTime
			probe  sql.NullTime
		)
		if err := rows.Scan(&c.id, &c.endpointID, &status, &opened, &probe, &now); err != nil {
			rows.Close()
			return nil, fmt.Errorf("claim: %w", err)
		}
		c.st = breaker.State{OpenedUntil: opened.Time, ProbeUntil: probe.Time}
		if c.st.Status, err = breaker.ParseStatus(status); err != nil {
			rows.Close()
			return nil, fmt.Errorf("claim: endpoint %d: %w", c.endpointID, err)
		}
		cands = append(cands, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}

	// Admit: closed endpoints pass; others get at most one probe each,
	// and only if this transaction can lock the endpoint row.
	ids := make([]int64, 0, len(cands))
	probed := map[int64]bool{}
	for _, c := range cands {
		if c.st.Status == breaker.Closed {
			ids = append(ids, c.id)
			continue
		}
		if probed[c.endpointID] {
			continue
		}
		st, err := lockBreakerSkipLocked(ctx, tx, c.endpointID)
		if err != nil {
			return nil, fmt.Errorf("claim: %w", err)
		}
		if st == nil {
			continue // another instance is deciding this endpoint's probe
		}
		switch st.Admit(now, s.breaker) {
		case breaker.Send:
			ids = append(ids, c.id) // closed meanwhile by a late success
		case breaker.Probe:
			if err := writeBreaker(ctx, tx, c.endpointID, *st); err != nil {
				return nil, fmt.Errorf("claim: %w", err)
			}
			ids = append(ids, c.id)
			probed[c.endpointID] = true
		case breaker.Skip:
			probed[c.endpointID] = true // someone else's probe is in flight
		}
	}
	if len(ids) == 0 {
		return []Claimed{}, nil
	}

	// The UPDATE applies event.Delivery.Claim's transition to the admitted
	// rows and joins what the deliverer needs.
	rows, err = tx.QueryContext(ctx, `
		UPDATE deliveries AS d SET
			state       = 'in_flight',
			attempt     = d.attempt + 1,
			lease_until = now() + $2 * interval '1 microsecond'
		FROM events AS e, endpoints AS p
		WHERE d.id = ANY($1) AND e.id = d.event_id AND p.id = d.endpoint_id
		RETURNING d.id, d.event_id, d.endpoint_id, d.state, d.attempt, d.max_attempts,
			d.next_attempt_at, d.lease_until, d.last_status, d.last_error, d.delivered_at, d.created_at,
			e.type, e.payload, p.url, p.secret, p.previous_secret, p.previous_secret_until`,
		pq.Array(ids), micros)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()

	claimed := make([]Claimed, 0, len(ids))
	for rows.Next() {
		var (
			c     Claimed
			until sql.NullTime
		)
		c.Delivery, err = scanDelivery(rows, &c.Type, &c.Payload, &c.URL,
			&c.Secrets.Current, &c.Secrets.Previous, &until)
		if err != nil {
			return nil, fmt.Errorf("claim: %w", err)
		}
		c.Secrets.PreviousUntil = until.Time
		claimed = append(claimed, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return claimed, nil
}

// lockBreakerSkipLocked loads an endpoint's breaker under FOR UPDATE SKIP
// LOCKED; nil means another transaction holds the row.
func lockBreakerSkipLocked(ctx context.Context, tx *sql.Tx, endpointID int64) (*breaker.State, error) {
	st, err := scanBreaker(tx.QueryRowContext(ctx,
		`SELECT `+breakerColumns+` FROM endpoints WHERE id = $1 FOR UPDATE SKIP LOCKED`, endpointID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock breaker of endpoint %d: %w", endpointID, err)
	}
	return &st, nil
}

// Finish records the result of the attempt a deliverer holds, as
// event.Delivery.Finish does: Delivered, Rejected and Blocked are final,
// Retry schedules the next attempt after the backoff or fails the
// delivery when the attempts are spent. The attempt number is the
// fencing token Claim returned; a stale one is refused with
// event.ErrLeaseLost and the delivery is left as it is. It returns the
// delivery as stored.
func (s *Store) Finish(ctx context.Context, id int64, attempt int, r event.Result) (event.Delivery, error) {
	return s.transitionTx(ctx, "finish", id, func(tx *sql.Tx, d *event.Delivery, now time.Time) error {
		if err := d.Finish(attempt, now, r, s.backoff, s.rnd); err != nil {
			return err
		}
		return s.recordOutcome(ctx, tx, d.EndpointID, r, now)
	})
}

// Heartbeat extends the lease of the attempt a deliverer holds to
// now+lease, for a receiver that is slow but alive. A stale attempt is
// refused with event.ErrLeaseLost.
func (s *Store) Heartbeat(ctx context.Context, id int64, attempt int, lease time.Duration) error {
	_, err := s.transition(ctx, "heartbeat", id, func(d *event.Delivery, now time.Time) error {
		return d.Heartbeat(attempt, now, lease)
	})
	return err
}

// GetDelivery returns the delivery with the given id.
func (s *Store) GetDelivery(ctx context.Context, id int64) (event.Delivery, error) {
	d, err := scanDelivery(s.db.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return event.Delivery{}, fmt.Errorf("get delivery %d: %w", id, event.ErrNotFound)
	}
	if err != nil {
		return event.Delivery{}, fmt.Errorf("get delivery %d: %w", id, err)
	}
	return d, nil
}

// DeliveriesOfEvent returns the deliveries fanned out for an event, by
// endpoint id.
func (s *Store) DeliveriesOfEvent(ctx context.Context, eventID int64) ([]event.Delivery, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE event_id = $1 ORDER BY endpoint_id`, eventID)
	if err != nil {
		return nil, fmt.Errorf("deliveries of event %d: %w", eventID, err)
	}
	defer rows.Close()
	var out []event.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("deliveries of event %d: %w", eventID, err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("deliveries of event %d: %w", eventID, err)
	}
	return out, nil
}

// transition locks a delivery, applies a domain method to it at the
// database's now(), and writes the result back, all in one transaction.
// If the method refuses, nothing is written and its error is returned
// with the delivery as it was.
func (s *Store) transition(ctx context.Context, op string, id int64,
	apply func(d *event.Delivery, now time.Time) error) (event.Delivery, error) {
	return s.transitionTx(ctx, op, id, func(_ *sql.Tx, d *event.Delivery, now time.Time) error {
		return apply(d, now)
	})
}

// transitionTx is transition for callers that need the transaction too,
// to change other rows together with the delivery.
func (s *Store) transitionTx(ctx context.Context, op string, id int64,
	apply func(tx *sql.Tx, d *event.Delivery, now time.Time) error) (event.Delivery, error) {
	var d event.Delivery
	err := s.inTx(ctx, op, func(tx *sql.Tx) error {
		var now time.Time
		var err error
		d, err = scanDelivery(tx.QueryRowContext(ctx,
			`SELECT `+deliveryColumns+`, now() FROM deliveries WHERE id = $1 FOR UPDATE`, id), &now)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%s delivery %d: %w", op, id, event.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("%s delivery %d: %w", op, id, err)
		}
		if err := apply(tx, &d, now); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE deliveries SET
				state = $2, attempt = $3, next_attempt_at = $4, lease_until = $5,
				last_status = $6, last_error = $7, delivered_at = $8
			WHERE id = $1`,
			d.ID, string(d.State), d.Attempt, d.NextAttemptAt, nullTime(d.LeaseUntil),
			d.LastStatus, d.LastError, nullTime(d.DeliveredAt)); err != nil {
			return fmt.Errorf("%s delivery %d: %w", op, id, err)
		}
		return nil
	})
	return d, err
}

// sortClaimed orders a batch by next_attempt_at, then id: the order the
// claim query selected them in.
func sortClaimed(cs []Claimed) {
	slices.SortFunc(cs, func(a, b Claimed) int {
		return cmp.Or(a.Delivery.NextAttemptAt.Compare(b.Delivery.NextAttemptAt), cmp.Compare(a.Delivery.ID, b.Delivery.ID))
	})
}

package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

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

	// The CTE walks the deliveries_due index in order and locks the first
	// rows nobody else holds; the UPDATE applies event.Delivery.Claim's
	// transition to exactly those rows and joins what the deliverer needs.
	rows, err := s.db.QueryContext(ctx, `
		WITH due AS (
			SELECT id FROM deliveries
			WHERE state = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE deliveries AS d SET
			state       = 'in_flight',
			attempt     = d.attempt + 1,
			lease_until = now() + $2 * interval '1 microsecond'
		FROM events AS e, endpoints AS p
		WHERE d.id IN (SELECT id FROM due) AND e.id = d.event_id AND p.id = d.endpoint_id
		RETURNING d.id, d.event_id, d.endpoint_id, d.state, d.attempt, d.max_attempts,
			d.next_attempt_at, d.lease_until, d.last_status, d.last_error, d.delivered_at, d.created_at,
			e.type, e.payload, p.url, p.secret, p.previous_secret, p.previous_secret_until`,
		limit, micros)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()

	claimed := make([]Claimed, 0, limit)
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
	// RETURNING does not promise an order; put the batch in claim order.
	sortClaimed(claimed)
	return claimed, nil
}

// Finish records the result of the attempt a deliverer holds, as
// event.Delivery.Finish does: Delivered, Rejected and Blocked are final,
// Retry schedules the next attempt after the backoff or fails the
// delivery when the attempts are spent. The attempt number is the
// fencing token Claim returned; a stale one is refused with
// event.ErrLeaseLost and the delivery is left as it is. It returns the
// delivery as stored.
func (s *Store) Finish(ctx context.Context, id int64, attempt int, r event.Result) (event.Delivery, error) {
	return s.transition(ctx, "finish", id, func(d *event.Delivery, now time.Time) error {
		return d.Finish(attempt, now, r, s.backoff, s.rnd)
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
		if err := apply(&d, now); err != nil {
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

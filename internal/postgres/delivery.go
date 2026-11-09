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
	// Ordered says the endpoint takes one delivery at a time: the next
	// one becomes claimable when this one finishes, so the deliverer
	// should claim again then rather than wait for its poll interval.
	Ordered bool
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
// The endpoint row gates the claim three ways, all from state every
// instance shares: the circuit breaker (closed endpoints flow; open ones
// are skipped until their period elapses, then exactly one probe goes
// through), the rate limit (a cursor that each admitted delivery moves
// forward by 1/rate) and ordering (one delivery at a time, the oldest
// first). An endpoint that needs none of these is claimed freely; one
// that does is locked with FOR UPDATE SKIP LOCKED so that two instances
// claiming at once cannot both probe, both spend the same rate credit or
// both take the head of an ordered queue. Deliveries locked but not
// admitted are left untouched: they are not updated, so the lock is
// released at commit and they stay pending.
func (s *Store) claimTx(ctx context.Context, tx *sql.Tx, limit int, micros int64) ([]Claimed, error) {
	// Candidate deliveries: due, pending, and not behind a gate that is
	// plainly shut (a breaker in its open period, a rate cursor in the
	// future, an older open delivery to an ordered endpoint). The gate
	// columns come along so that most endpoints need no second read.
	rows, err := tx.QueryContext(ctx, `
		SELECT d.id, d.endpoint_id, p.breaker_status, p.rate_limit, p.ordered, now()
		FROM deliveries AS d JOIN endpoints AS p ON p.id = d.endpoint_id
		WHERE d.state = 'pending' AND d.next_attempt_at <= now()
		  AND (p.breaker_status = 'closed'
		       OR (p.breaker_status = 'open' AND p.breaker_opened_until <= now())
		       OR (p.breaker_status = 'half_open' AND p.breaker_probe_until <= now()))
		  AND (p.rate_limit = 0 OR p.rate_cursor < now())
		  AND (NOT p.ordered OR NOT EXISTS (
		       SELECT 1 FROM deliveries AS older
		       WHERE older.endpoint_id = d.endpoint_id
		         AND (older.state = 'in_flight' OR (older.state = 'pending' AND older.id < d.id))))
		ORDER BY d.next_attempt_at, d.id
		LIMIT $1
		FOR UPDATE OF d SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	type candidate struct {
		id, endpointID int64
		gated          bool
	}
	var (
		cands []candidate
		now   time.Time
	)
	for rows.Next() {
		var (
			c       candidate
			status  string
			rate    int
			ordered bool
		)
		if err := rows.Scan(&c.id, &c.endpointID, &status, &rate, &ordered, &now); err != nil {
			rows.Close()
			return nil, fmt.Errorf("claim: %w", err)
		}
		c.gated = status != string(breaker.Closed) || rate > 0 || ordered
		cands = append(cands, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}

	// Admit. Ungated endpoints pass; the rest are decided by their gate,
	// loaded once per endpoint under the row lock and written back once.
	ids := make([]int64, 0, len(cands))
	gates := map[int64]*gate{}
	for _, c := range cands {
		if !c.gated {
			ids = append(ids, c.id)
			continue
		}
		g, ok := gates[c.endpointID]
		if !ok {
			g, err = lockGate(ctx, tx, c.endpointID)
			if err != nil {
				return nil, fmt.Errorf("claim: %w", err)
			}
			gates[c.endpointID] = g // nil when another instance holds the row
		}
		if g != nil && g.admit(now, s.breaker) {
			ids = append(ids, c.id)
		}
	}
	for id, g := range gates {
		if g == nil || !g.changed {
			continue
		}
		if err := writeGate(ctx, tx, id, g); err != nil {
			return nil, fmt.Errorf("claim: %w", err)
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
			e.type, e.payload, p.url, p.secret, p.previous_secret, p.previous_secret_until, p.ordered`,
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
			&c.Secrets.Current, &c.Secrets.Previous, &until, &c.Ordered)
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

// gate is the admission state of one endpoint during a claim: its
// breaker, its rate cursor and whether it is ordered.
type gate struct {
	breaker breaker.State
	rate    int
	cursor  time.Time
	ordered bool
	// admitted counts deliveries let through in this claim; an ordered
	// endpoint allows one, and a breaker that is not closed allows one
	// probe.
	admitted int
	changed  bool
}

// RateBurst is how much credit an idle rate-limited endpoint keeps: the
// cursor never starts further back than this, so after a quiet spell at
// most one second's worth of deliveries goes out at once.
const RateBurst = time.Second

// admit decides one delivery and moves the gate when it says yes.
func (g *gate) admit(now time.Time, p breaker.Policy) bool {
	if g.ordered && g.admitted > 0 {
		return false
	}
	cursor := g.cursor
	if g.rate > 0 {
		if floor := now.Add(-RateBurst); cursor.Before(floor) {
			cursor = floor
		}
		if !cursor.Before(now) {
			return false // the credit is spent until the cursor
		}
		cursor = cursor.Add(time.Second / time.Duration(g.rate))
	}
	st := g.breaker
	if st.Admit(now, p) == breaker.Skip {
		return false // open, or a probe (perhaps this claim's) is in flight
	}
	if st != g.breaker || !cursor.Equal(g.cursor) {
		g.changed = true
	}
	g.breaker, g.cursor = st, cursor
	g.admitted++
	return true
}

// lockGate loads an endpoint's gate under FOR UPDATE SKIP LOCKED; nil
// means another transaction holds the row and is deciding for it.
func lockGate(ctx context.Context, tx *sql.Tx, endpointID int64) (*gate, error) {
	g := &gate{}
	var err error
	g.breaker, err = scanBreaker(tx.QueryRowContext(ctx,
		`SELECT `+breakerColumns+`, rate_limit, rate_cursor, ordered FROM endpoints WHERE id = $1 FOR UPDATE SKIP LOCKED`,
		endpointID), &g.rate, &g.cursor, &g.ordered)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock endpoint %d: %w", endpointID, err)
	}
	return g, nil
}

func writeGate(ctx context.Context, tx *sql.Tx, endpointID int64, g *gate) error {
	st := g.breaker
	if _, err := tx.ExecContext(ctx, `
		UPDATE endpoints SET breaker_status = $2, breaker_failures = $3, breaker_opens = $4,
			breaker_opened_until = $5, breaker_probe_until = $6, rate_cursor = $7
		WHERE id = $1`,
		endpointID, string(st.Status), st.Failures, st.Opens, nullTime(st.OpenedUntil), nullTime(st.ProbeUntil), g.cursor); err != nil {
		return fmt.Errorf("write gate of endpoint %d: %w", endpointID, err)
	}
	return nil
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

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/breaker"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// WithBreaker sets the circuit-breaker policy. The default is
// breaker.DefaultPolicy.
func WithBreaker(p breaker.Policy) Option {
	return func(s *Store) { s.breaker = p }
}

const breakerColumns = `breaker_status, breaker_failures, breaker_opens, breaker_opened_until, breaker_probe_until`

// scanBreaker reads breakerColumns from row.
func scanBreaker(row scanner, extra ...any) (breaker.State, error) {
	var (
		st          breaker.State
		status      string
		openedUntil sql.NullTime
		probeUntil  sql.NullTime
	)
	dest := append([]any{&status, &st.Failures, &st.Opens, &openedUntil, &probeUntil}, extra...)
	if err := row.Scan(dest...); err != nil {
		return st, err
	}
	parsed, err := breaker.ParseStatus(status)
	if err != nil {
		return st, err
	}
	st.Status = parsed
	st.OpenedUntil = openedUntil.Time
	st.ProbeUntil = probeUntil.Time
	return st, nil
}

// Breaker returns the breaker of an endpoint.
func (s *Store) Breaker(ctx context.Context, endpointID int64) (breaker.State, error) {
	st, err := scanBreaker(s.db.QueryRowContext(ctx,
		`SELECT `+breakerColumns+` FROM endpoints WHERE id = $1`, endpointID))
	if err == sql.ErrNoRows { //nolint:errorlint // database/sql returns the sentinel itself
		return st, fmt.Errorf("breaker of endpoint %d: %w", endpointID, event.ErrNotFound)
	}
	if err != nil {
		return st, fmt.Errorf("breaker of endpoint %d: %w", endpointID, err)
	}
	return st, nil
}

// ResetBreaker closes an endpoint's breaker, as an operator does after
// the receiver is fixed. Pending deliveries to it become claimable at
// once.
func (s *Store) ResetBreaker(ctx context.Context, endpointID int64) error {
	return s.inTx(ctx, "reset breaker", func(tx *sql.Tx) error {
		st, err := lockBreaker(ctx, tx, endpointID)
		if err != nil {
			return fmt.Errorf("reset breaker of endpoint %d: %w", endpointID, err)
		}
		st.Reset()
		return writeBreaker(ctx, tx, endpointID, st)
	})
}

// lockBreaker loads an endpoint's breaker under FOR UPDATE.
func lockBreaker(ctx context.Context, tx *sql.Tx, endpointID int64) (breaker.State, error) {
	st, err := scanBreaker(tx.QueryRowContext(ctx,
		`SELECT `+breakerColumns+` FROM endpoints WHERE id = $1 FOR UPDATE`, endpointID))
	if err == sql.ErrNoRows { //nolint:errorlint // database/sql returns the sentinel itself
		return st, event.ErrNotFound
	}
	return st, err
}

func writeBreaker(ctx context.Context, tx *sql.Tx, endpointID int64, st breaker.State) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE endpoints SET breaker_status = $2, breaker_failures = $3, breaker_opens = $4,
			breaker_opened_until = $5, breaker_probe_until = $6, updated_at = now()
		WHERE id = $1`,
		endpointID, string(st.Status), st.Failures, st.Opens, nullTime(st.OpenedUntil), nullTime(st.ProbeUntil)); err != nil {
		return fmt.Errorf("write breaker of endpoint %d: %w", endpointID, err)
	}
	return nil
}

// recordOutcome feeds an attempt's result to the endpoint's breaker
// inside the delivery's transaction, so the breaker and the delivery
// change together or not at all. The breaker row is locked after the
// delivery row, always in that order, so deliverers finishing against
// the same endpoint queue instead of deadlocking.
func (s *Store) recordOutcome(ctx context.Context, tx *sql.Tx, endpointID int64, r event.Result, now time.Time) error {
	st, err := lockBreaker(ctx, tx, endpointID)
	if err != nil {
		return fmt.Errorf("breaker of endpoint %d: %w", endpointID, err)
	}
	before := st
	st.Record(r.Outcome == event.Delivered, r.Outcome == event.Retry, now, s.breaker)
	if st == before {
		return nil
	}
	return writeBreaker(ctx, tx, endpointID, st)
}

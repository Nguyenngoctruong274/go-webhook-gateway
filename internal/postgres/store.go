package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// Store keeps endpoints, events and deliveries in PostgreSQL. It applies
// the rules of internal/event: every transition loads the row, runs the
// domain method on it and writes the result back in one transaction, so
// what the tests of the domain prove holds for the stored rows too.
type Store struct {
	db      *sql.DB
	backoff event.Backoff
	rnd     event.Rand // nil: the shared math/rand source
}

// Option configures a Store.
type Option func(*Store)

// WithBackoff sets the retry policy Finish uses. The default is
// event.DefaultBackoff.
func WithBackoff(b event.Backoff) Option {
	return func(s *Store) { s.backoff = b }
}

// WithRand sets the jitter source, for tests that need exact delays.
func WithRand(r event.Rand) Option {
	return func(s *Store) { s.rnd = r }
}

// NewStore returns a Store using db. The caller owns db and closes it.
func NewStore(db *sql.DB, opts ...Option) (*Store, error) {
	s := &Store{db: db, backoff: event.DefaultBackoff()}
	for _, o := range opts {
		o(s)
	}
	if err := s.backoff.Validate(); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	return s, nil
}

// scanner is satisfied by *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

const endpointColumns = `id, url, description, types, enabled, max_attempts, created_at, updated_at`

// scanEndpoint reads endpointColumns, then any extra destinations, from row.
func scanEndpoint(row scanner, extra ...any) (event.Endpoint, error) {
	var (
		e     event.Endpoint
		types pq.StringArray
	)
	dest := append([]any{
		&e.ID, &e.URL, &e.Description, &types, &e.Enabled, &e.MaxAttempts, &e.CreatedAt, &e.UpdatedAt,
	}, extra...)
	if err := row.Scan(dest...); err != nil {
		return event.Endpoint{}, err
	}
	e.Types = []string(types)
	return e, nil
}

const eventColumns = `id, type, payload, idempotency_key, created_at`

// scanEvent reads eventColumns, then any extra destinations, from row.
func scanEvent(row scanner, extra ...any) (event.Event, error) {
	var (
		ev  event.Event
		key sql.NullString
	)
	dest := append([]any{&ev.ID, &ev.Type, &ev.Payload, &key, &ev.CreatedAt}, extra...)
	if err := row.Scan(dest...); err != nil {
		return event.Event{}, err
	}
	ev.IdempotencyKey = key.String
	return ev, nil
}

const deliveryColumns = `id, event_id, endpoint_id, state, attempt, max_attempts,
	next_attempt_at, lease_until, last_status, last_error, delivered_at, created_at`

// scanDelivery reads deliveryColumns, then any extra destinations, from row.
func scanDelivery(row scanner, extra ...any) (event.Delivery, error) {
	var (
		d           event.Delivery
		state       string
		leaseUntil  sql.NullTime
		deliveredAt sql.NullTime
	)
	dest := append([]any{
		&d.ID, &d.EventID, &d.EndpointID, &state, &d.Attempt, &d.MaxAttempts,
		&d.NextAttemptAt, &leaseUntil, &d.LastStatus, &d.LastError, &deliveredAt, &d.CreatedAt,
	}, extra...)
	if err := row.Scan(dest...); err != nil {
		return event.Delivery{}, err
	}
	st, err := event.ParseState(state)
	if err != nil {
		return event.Delivery{}, fmt.Errorf("delivery %d: %w", d.ID, err)
	}
	d.State = st
	d.LeaseUntil = leaseUntil.Time
	d.DeliveredAt = deliveredAt.Time
	return d, nil
}

// textArray makes a []string safe to bind to a NOT NULL text[] column:
// lib/pq sends a nil slice as NULL.
func textArray(ss []string) pq.StringArray {
	if ss == nil {
		return pq.StringArray{}
	}
	return pq.StringArray(ss)
}

// nullTime binds a zero time as NULL.
func nullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}

// inTx runs fn in a transaction and commits if it returns nil. The
// transaction is rolled back on error, and a rollback failure after the
// caller's error is dropped: the caller's error is the one that matters.
func (s *Store) inTx(ctx context.Context, op string, fn func(tx *sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: begin: %w", op, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("%s: commit: %w", op, err)
	}
	return nil
}

// mapPayloadError turns PostgreSQL refusing a payload that encoding/json
// accepted, such as a string holding bytes that are not UTF-8, into
// event.ErrInvalid.
func mapPayloadError(err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch pqErr.Code {
		// character_not_in_repertoire, invalid_text_representation,
		// untranslatable_character
		case "22021", "22P02", "22P05":
			return fmt.Errorf("%w: payload: %s", event.ErrInvalid, pqErr.Message)
		}
	}
	return err
}

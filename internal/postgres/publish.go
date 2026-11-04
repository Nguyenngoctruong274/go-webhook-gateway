package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// Published is what Publish reports: the event as stored and, when it was
// created by this call, how many deliveries were fanned out for it.
type Published struct {
	Event event.Event
	// Created is false when the idempotency key was already taken and
	// the existing event is returned instead.
	Created bool
	// Deliveries is the number of enabled, subscribed endpoints the event
	// was queued for; 0 when Created is false.
	Deliveries int
}

// keyRaceRetries bounds how often Publish goes back and forth between
// inserting and reading a keyed event. Each extra round needs the event
// holding the key to be deleted in between, so 3 is plenty.
const keyRaceRetries = 3

// Publish stores an event and, in the same transaction, one pending
// delivery for every enabled endpoint subscribed to its type: the event
// and its fan-out exist together or not at all. A producer that retries
// after a timeout and sends the same idempotency key gets the event it
// already published, with no second fan-out; if that key was used for an
// event of another type or payload, Publish returns
// event.ErrIdempotencyConflict. An invalid spec is refused with
// event.ErrInvalid before anything is written.
func (s *Store) Publish(ctx context.Context, spec event.Spec) (Published, error) {
	spec, err := spec.Normalize()
	if err != nil {
		return Published{}, fmt.Errorf("publish: %w", err)
	}
	var key any
	if spec.IdempotencyKey != "" {
		key = spec.IdempotencyKey
	}
	// A string, not []byte: lib/pq would send []byte as bytea.
	payload := string(spec.Payload)

	var out Published
	for range keyRaceRetries {
		err := s.inTx(ctx, "publish", func(tx *sql.Tx) error {
			ev, err := scanEvent(tx.QueryRowContext(ctx, `
				INSERT INTO events (type, payload, idempotency_key)
				VALUES ($1, $2, $3)
				ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
				RETURNING `+eventColumns,
				spec.Type, payload, key))
			if errors.Is(err, sql.ErrNoRows) {
				out, err = existingEvent(ctx, tx, spec, payload)
				return err
			}
			if err != nil {
				return fmt.Errorf("publish %s: %w", spec.Type, mapPayloadError(err))
			}
			n, err := fanOut(ctx, tx, ev)
			if err != nil {
				return err
			}
			out = Published{Event: ev, Created: true, Deliveries: n}
			return nil
		})
		if errors.Is(err, errKeyVanished) {
			continue // the holder was deleted in between: insert again
		}
		return out, err
	}
	return Published{}, fmt.Errorf("publish %s: key %q kept changing hands", spec.Type, spec.IdempotencyKey)
}

var errKeyVanished = errors.New("event holding the key vanished")

// existingEvent reads the event that holds spec's idempotency key. The
// insert's ON CONFLICT waited for the holder to commit, but that
// statement's snapshot predates it, so this new statement is what sees it.
func existingEvent(ctx context.Context, tx *sql.Tx, spec event.Spec, payload string) (Published, error) {
	var same bool
	ev, err := scanEvent(tx.QueryRowContext(ctx, `
		SELECT `+eventColumns+`, type = $2 AND payload::text = $3
		FROM events WHERE idempotency_key = $1`,
		spec.IdempotencyKey, spec.Type, payload), &same)
	if errors.Is(err, sql.ErrNoRows) {
		return Published{}, errKeyVanished
	}
	if err != nil {
		return Published{}, fmt.Errorf("publish %s: read event holding the key: %w", spec.Type, err)
	}
	if !same {
		return Published{}, fmt.Errorf("publish %s: key %q held by event %d: %w",
			spec.Type, spec.IdempotencyKey, ev.ID, event.ErrIdempotencyConflict)
	}
	return Published{Event: ev}, nil
}

// fanOut queues ev for every enabled endpoint subscribed to its type,
// copying each endpoint's attempt limit into its delivery. It returns
// how many deliveries it created.
func fanOut(ctx context.Context, tx *sql.Tx, ev event.Event) (int, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO deliveries (event_id, endpoint_id, max_attempts)
		SELECT $1, id, max_attempts FROM endpoints
		WHERE enabled AND (cardinality(types) = 0 OR $2 = ANY (types))
		ORDER BY id`, ev.ID, ev.Type)
	if err != nil {
		return 0, fmt.Errorf("publish %s: fan out event %d: %w", ev.Type, ev.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("publish %s: fan out event %d: %w", ev.Type, ev.ID, err)
	}
	return int(n), nil
}

// GetEvent returns the event with the given id.
func (s *Store) GetEvent(ctx context.Context, id int64) (event.Event, error) {
	ev, err := scanEvent(s.db.QueryRowContext(ctx, `SELECT `+eventColumns+` FROM events WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return event.Event{}, fmt.Errorf("get event %d: %w", id, event.ErrNotFound)
	}
	if err != nil {
		return event.Event{}, fmt.Errorf("get event %d: %w", id, err)
	}
	return ev, nil
}

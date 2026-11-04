package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

// CreateEndpoint registers a customer's receiver with a fresh signing
// secret and returns both. The secret is shown once, here; afterwards it
// can only be rotated. An invalid spec is refused with event.ErrInvalid
// before anything is written.
func (s *Store) CreateEndpoint(ctx context.Context, spec event.EndpointSpec) (ep event.Endpoint, secret string, err error) {
	spec, err = spec.Normalize()
	if err != nil {
		return event.Endpoint{}, "", fmt.Errorf("create endpoint: %w", err)
	}
	secret, err = signing.NewSecret()
	if err != nil {
		return event.Endpoint{}, "", fmt.Errorf("create endpoint: %w", err)
	}
	ep, err = scanEndpoint(s.db.QueryRowContext(ctx, `
		INSERT INTO endpoints (url, description, types, max_attempts, secret)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING `+endpointColumns,
		spec.URL, spec.Description, textArray(spec.Types), spec.MaxAttempts, secret))
	if err != nil {
		return event.Endpoint{}, "", fmt.Errorf("create endpoint: %w", err)
	}
	return ep, secret, nil
}

// GetEndpoint returns the endpoint with the given id.
func (s *Store) GetEndpoint(ctx context.Context, id int64) (event.Endpoint, error) {
	ep, err := scanEndpoint(s.db.QueryRowContext(ctx, `SELECT `+endpointColumns+` FROM endpoints WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return event.Endpoint{}, fmt.Errorf("get endpoint %d: %w", id, event.ErrNotFound)
	}
	if err != nil {
		return event.Endpoint{}, fmt.Errorf("get endpoint %d: %w", id, err)
	}
	return ep, nil
}

// ListEndpoints returns every endpoint, enabled or not, oldest first.
func (s *Store) ListEndpoints(ctx context.Context) ([]event.Endpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+endpointColumns+` FROM endpoints ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}
	defer rows.Close()
	var out []event.Endpoint
	for rows.Next() {
		ep, err := scanEndpoint(rows)
		if err != nil {
			return nil, fmt.Errorf("list endpoints: %w", err)
		}
		out = append(out, ep)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}
	return out, nil
}

// UpdateEndpoint replaces the URL, description, subscribed types and
// attempt limit of an endpoint. Deliveries already queued keep the
// attempt limit they were created with; the next fan-out uses the new one.
func (s *Store) UpdateEndpoint(ctx context.Context, id int64, spec event.EndpointSpec) (event.Endpoint, error) {
	spec, err := spec.Normalize()
	if err != nil {
		return event.Endpoint{}, fmt.Errorf("update endpoint %d: %w", id, err)
	}
	ep, err := scanEndpoint(s.db.QueryRowContext(ctx, `
		UPDATE endpoints
		SET url = $2, description = $3, types = $4, max_attempts = $5, updated_at = now()
		WHERE id = $1
		RETURNING `+endpointColumns,
		id, spec.URL, spec.Description, textArray(spec.Types), spec.MaxAttempts))
	if errors.Is(err, sql.ErrNoRows) {
		return event.Endpoint{}, fmt.Errorf("update endpoint %d: %w", id, event.ErrNotFound)
	}
	if err != nil {
		return event.Endpoint{}, fmt.Errorf("update endpoint %d: %w", id, err)
	}
	return ep, nil
}

// SetEndpointEnabled pauses (false) or resumes (true) an endpoint. A
// paused endpoint takes part in no fan-out; its queued deliveries stay
// as they are and are attempted whether it is paused or not, so that
// pausing never loses an event that was already accepted for it.
func (s *Store) SetEndpointEnabled(ctx context.Context, id int64, enabled bool) (event.Endpoint, error) {
	ep, err := scanEndpoint(s.db.QueryRowContext(ctx, `
		UPDATE endpoints SET enabled = $2, updated_at = now() WHERE id = $1
		RETURNING `+endpointColumns, id, enabled))
	if errors.Is(err, sql.ErrNoRows) {
		return event.Endpoint{}, fmt.Errorf("set endpoint %d enabled: %w", id, event.ErrNotFound)
	}
	if err != nil {
		return event.Endpoint{}, fmt.Errorf("set endpoint %d enabled: %w", id, err)
	}
	return ep, nil
}

// Secrets returns the signing secrets of an endpoint. They live apart
// from the endpoint itself so that listings and logs never carry them.
func (s *Store) Secrets(ctx context.Context, id int64) (signing.Rotation, error) {
	r, err := scanRotation(s.db.QueryRowContext(ctx,
		`SELECT secret, previous_secret, previous_secret_until FROM endpoints WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return signing.Rotation{}, fmt.Errorf("secrets of endpoint %d: %w", id, event.ErrNotFound)
	}
	if err != nil {
		return signing.Rotation{}, fmt.Errorf("secrets of endpoint %d: %w", id, err)
	}
	return r, nil
}

// RotateSecret issues a new signing secret for an endpoint and returns
// it. The old secret keeps signing alongside the new one for overlap, as
// signing.Rotation.Rotate describes, so that a receiver that has not
// switched yet keeps verifying. The row is locked for the rotation, so
// two rotations at once cannot both think they retired the same secret.
func (s *Store) RotateSecret(ctx context.Context, id int64, overlap time.Duration) (secret string, err error) {
	op := fmt.Sprintf("rotate secret of endpoint %d", id)
	err = s.inTx(ctx, op, func(tx *sql.Tx) error {
		var now time.Time
		current, err := scanRotation(tx.QueryRowContext(ctx,
			`SELECT secret, previous_secret, previous_secret_until, now() FROM endpoints WHERE id = $1 FOR UPDATE`, id), &now)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%s: %w", op, event.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		next, err := signing.NewSecret()
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		rotated, err := current.Rotate(next, now, overlap)
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		var until any
		if rotated.Previous != "" {
			until = rotated.PreviousUntil
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE endpoints
			SET secret = $2, previous_secret = $3, previous_secret_until = $4, updated_at = now()
			WHERE id = $1`, id, rotated.Current, rotated.Previous, until); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		secret = next
		return nil
	})
	return secret, err
}

func scanRotation(row scanner, extra ...any) (signing.Rotation, error) {
	var (
		r     signing.Rotation
		until sql.NullTime
	)
	dest := append([]any{&r.Current, &r.Previous, &until}, extra...)
	if err := row.Scan(dest...); err != nil {
		return signing.Rotation{}, err
	}
	r.PreviousUntil = until.Time
	return r, nil
}

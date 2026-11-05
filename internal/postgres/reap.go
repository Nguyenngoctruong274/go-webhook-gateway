package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// Reap ends up to limit in-flight attempts whose lease has run out, as if
// their deliverer had reported a transient failure: the delivery retries
// after the backoff or fails when its attempts are spent. It returns the
// ids it expired. Several reapers may run at once: whoever locks a row
// first expires it, and the others find it no longer in flight.
//
// Each delivery is expired in its own transaction so that a long batch
// does not hold locks over other deliverers' rows.
func (s *Store) Reap(ctx context.Context, limit int) ([]int64, error) {
	if limit < 1 || limit > MaxClaimBatch {
		return nil, fmt.Errorf("reap: limit must be between 1 and %d, got %d", MaxClaimBatch, limit)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM deliveries
		WHERE state = 'in_flight' AND lease_until <= now()
		ORDER BY lease_until
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("reap: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("reap: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reap: %w", err)
	}

	expired := make([]int64, 0, len(ids))
	for _, id := range ids {
		_, err := s.transition(ctx, "expire", id, func(d *event.Delivery, now time.Time) error {
			return d.Expire(now, s.backoff, s.rnd)
		})
		switch {
		case errors.Is(err, event.ErrInvalidTransition), errors.Is(err, event.ErrLeaseActive):
			// Between the scan and the lock the deliverer finished or
			// sent a heartbeat. Its report stands.
			continue
		case err != nil:
			return expired, err
		}
		expired = append(expired, id)
	}
	return expired, nil
}

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// MaxHistoryPage caps one page of delivery history.
const MaxHistoryPage = 200

// HistoryFilter selects a page of an endpoint's delivery history, newest
// first. Pages are keyed by id rather than offset, so a page stays
// stable while new deliveries arrive.
type HistoryFilter struct {
	EndpointID int64
	// State, when set, keeps only deliveries in that state.
	State event.State
	// Before, when set, returns deliveries with a smaller id: pass the
	// last id of the previous page to get the next one.
	Before int64
	// Limit is the page size; 0 means MaxHistoryPage.
	Limit int
}

// History returns one page of deliveries to an endpoint, newest first,
// and whether an older page exists. It is the customer's view of what
// happened: state, attempts, the last status and the error excerpt.
func (s *Store) History(ctx context.Context, f HistoryFilter) (page []event.Delivery, more bool, err error) {
	if f.Limit <= 0 || f.Limit > MaxHistoryPage {
		f.Limit = MaxHistoryPage
	}
	if f.State != "" {
		if _, err := event.ParseState(string(f.State)); err != nil {
			return nil, false, fmt.Errorf("history: %w: %w", event.ErrInvalid, err)
		}
	}
	// Optional filters are passed as NULL rather than spliced into the
	// SQL; the planner handles the constant comparisons.
	var (
		state  sql.NullString
		before sql.NullInt64
	)
	if f.State != "" {
		state = sql.NullString{String: string(f.State), Valid: true}
	}
	if f.Before > 0 {
		before = sql.NullInt64{Int64: f.Before, Valid: true}
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+deliveryColumns+` FROM deliveries
		WHERE endpoint_id = $1 AND ($3::text IS NULL OR state = $3) AND ($4::bigint IS NULL OR id < $4)
		ORDER BY id DESC LIMIT $2`, f.EndpointID, f.Limit+1, state, before)
	if err != nil {
		return nil, false, fmt.Errorf("history of endpoint %d: %w", f.EndpointID, err)
	}
	defer rows.Close()
	page = make([]event.Delivery, 0, f.Limit)
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, false, fmt.Errorf("history of endpoint %d: %w", f.EndpointID, err)
		}
		page = append(page, d)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("history of endpoint %d: %w", f.EndpointID, err)
	}
	if len(page) > f.Limit {
		return page[:f.Limit], true, nil
	}
	return page, false, nil
}

// Replay queues a finished delivery (delivered, failed, rejected,
// blocked or cancelled) again with a fresh set of attempts, due now. A
// pending or in-flight delivery is refused with
// event.ErrInvalidTransition.
func (s *Store) Replay(ctx context.Context, id int64) (event.Delivery, error) {
	return s.transition(ctx, "replay", id, func(d *event.Delivery, now time.Time) error {
		return d.Replay(now)
	})
}

// Cancel stops a pending delivery before it is attempted again. An
// in-flight delivery is refused: its request may be on the wire.
func (s *Store) Cancel(ctx context.Context, id int64) (event.Delivery, error) {
	return s.transition(ctx, "cancel", id, func(d *event.Delivery, _ time.Time) error {
		return d.Cancel()
	})
}

// Stats counts an endpoint's deliveries by state, for the endpoint view.
func (s *Store) Stats(ctx context.Context, endpointID int64) (map[event.State]int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT state, count(*) FROM deliveries WHERE endpoint_id = $1 GROUP BY state`, endpointID)
	if err != nil {
		return nil, fmt.Errorf("stats of endpoint %d: %w", endpointID, err)
	}
	defer rows.Close()
	out := map[event.State]int{}
	for rows.Next() {
		var (
			st string
			n  int
		)
		if err := rows.Scan(&st, &n); err != nil {
			return nil, fmt.Errorf("stats of endpoint %d: %w", endpointID, err)
		}
		out[event.State(st)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("stats of endpoint %d: %w", endpointID, err)
	}
	return out, nil
}

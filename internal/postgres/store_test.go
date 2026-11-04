package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/testdb"
)

// newStore returns a Store on a fresh schema, or skips the test when no
// database is configured.
func newStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	db := testdb.New(t, Migrate)
	s, err := NewStore(db, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNewStoreRejectsInvalidBackoff(t *testing.T) {
	_, err := NewStore(nil, WithBackoff(event.Backoff{Base: -time.Second}))
	if err == nil {
		t.Fatal("expected an error for a negative backoff base")
	}
}

// mustEndpoint registers an endpoint and returns it with its secret.
func mustEndpoint(t *testing.T, s *Store, spec event.EndpointSpec) (event.Endpoint, string) {
	t.Helper()
	if spec.URL == "" {
		spec.URL = "https://receiver.example/hook"
	}
	ep, secret, err := s.CreateEndpoint(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return ep, secret
}

// mustPublish publishes an event and returns it.
func mustPublish(t *testing.T, s *Store, spec event.Spec) Published {
	t.Helper()
	p, err := s.Publish(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// dbNow returns the database clock, which the store uses for every time.
func dbNow(t *testing.T, s *Store) time.Time {
	t.Helper()
	var now time.Time
	if err := s.db.QueryRowContext(context.Background(), `SELECT now()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

func assertNear(t *testing.T, what string, got, want time.Time, tolerance time.Duration) {
	t.Helper()
	if d := got.Sub(want).Abs(); d > tolerance {
		t.Fatalf("%s = %s, want within %s of %s (off by %s)", what, got, tolerance, want, d)
	}
}

func assertIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func TestSchemaConstraintsRefuseImpossibleRows(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ep, _ := mustEndpoint(t, s, event.EndpointSpec{})
	ev := mustPublish(t, s, event.Spec{Type: "x"}).Event

	tests := []struct {
		name, cols, vals string
	}{
		{"pending with attempts spent", "state, attempt, max_attempts", "'pending', 2, 2"},
		{"in flight without lease", "state, attempt, max_attempts", "'in_flight', 1, 2"},
		{"in flight at attempt 0", "state, attempt, max_attempts, lease_until", "'in_flight', 0, 2, now()"},
		{"pending with lease", "state, attempt, max_attempts, lease_until", "'pending', 0, 2, now()"},
		{"delivered without delivered_at", "state, attempt, max_attempts", "'delivered', 1, 2"},
		{"attempt above max", "state, attempt, max_attempts", "'failed', 3, 2"},
		{"unknown state", "state, attempt, max_attempts", "'lost', 0, 2"},
		{"status out of range", "state, attempt, max_attempts, last_status", "'rejected', 1, 2, 600"},
		{"second delivery of an event to an endpoint", "state, attempt, max_attempts", "'pending', 0, 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.db.ExecContext(ctx,
				`INSERT INTO deliveries (event_id, endpoint_id, `+tt.cols+`) VALUES ($1, $2, `+tt.vals+`)`,
				ev.ID, ep.ID)
			if err == nil {
				t.Fatalf("insert (%s) succeeded, want a constraint violation", tt.vals)
			}
		})
	}
}

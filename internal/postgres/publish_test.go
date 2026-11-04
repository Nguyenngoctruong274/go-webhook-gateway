package postgres

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

func TestPublishFansOutToEnabledSubscribedEndpoints(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	all, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://all.example", MaxAttempts: 3})
	paid, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://paid.example", Types: []string{"order.paid"}})
	mustEndpoint(t, s, event.EndpointSpec{URL: "https://users.example", Types: []string{"user.created"}})
	paused, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://paused.example"})
	if _, err := s.SetEndpointEnabled(ctx, paused.ID, false); err != nil {
		t.Fatal(err)
	}

	p := mustPublish(t, s, event.Spec{Type: "order.paid", Payload: []byte(`{"id":42}`)})
	if !p.Created || p.Deliveries != 2 {
		t.Fatalf("published = %+v, want created with 2 deliveries", p)
	}
	if p.Event.ID == 0 || p.Event.Type != "order.paid" || p.Event.CreatedAt.IsZero() {
		t.Fatalf("event = %+v", p.Event)
	}

	ds, err := s.DeliveriesOfEvent(ctx, p.Event.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 || ds[0].EndpointID != all.ID || ds[1].EndpointID != paid.ID {
		t.Fatalf("deliveries = %+v, want one for %d and one for %d", ds, all.ID, paid.ID)
	}
	if ds[0].MaxAttempts != 3 || ds[1].MaxAttempts != event.DefaultMaxAttempts {
		t.Fatalf("max attempts = %d, %d; want each endpoint's own (3, %d)", ds[0].MaxAttempts, ds[1].MaxAttempts, event.DefaultMaxAttempts)
	}
	for _, d := range ds {
		if d.State != event.StatePending || d.Attempt != 0 || d.EventID != p.Event.ID || !d.LeaseUntil.IsZero() {
			t.Fatalf("delivery = %+v, want pending at attempt 0 with no lease", d)
		}
		assertNear(t, "next_attempt_at", d.NextAttemptAt, p.Event.CreatedAt, 0)
	}

	// Nothing subscribed: the event is kept for the record, with no fan-out.
	p = mustPublish(t, s, event.Spec{Type: "invoice.sent"})
	if !p.Created || p.Deliveries != 1 {
		t.Fatalf("published = %+v, want created with 1 delivery (the catch-all endpoint)", p)
	}
	p = mustPublish(t, s, event.Spec{Type: "user.created"})
	if p.Deliveries != 2 {
		t.Fatalf("user.created fanned out to %d, want 2 (catch-all and users)", p.Deliveries)
	}
}

func TestPublishKeepsPayloadBytes(t *testing.T) {
	s := newStore(t)
	// Key order, spacing and escapes are the producer's; a receiver may
	// have a signature or a hash over exactly these bytes.
	payload := []byte("{\"z\": 1,  \"a\": [1, 2],\n \"s\": \"\\u00e9\"}")
	p := mustPublish(t, s, event.Spec{Type: "x", Payload: payload})
	got, err := s.GetEvent(context.Background(), p.Event.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("payload = %s, want %s byte for byte", got.Payload, payload)
	}
	if !bytes.Equal(p.Event.Payload, payload) {
		t.Fatalf("returned payload = %s, want %s", p.Event.Payload, payload)
	}
	// An empty payload becomes an empty object.
	p = mustPublish(t, s, event.Spec{Type: "x"})
	if string(p.Event.Payload) != "{}" {
		t.Fatalf("empty payload stored as %q, want {}", p.Event.Payload)
	}
}

func TestPublishIdempotencyKey(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{})
	spec := event.Spec{Type: "order.paid", Payload: []byte(`{"id":1}`), IdempotencyKey: "order-1-paid"}

	first := mustPublish(t, s, spec)
	if !first.Created || first.Deliveries != 1 || first.Event.IdempotencyKey != spec.IdempotencyKey {
		t.Fatalf("first = %+v", first)
	}

	again := mustPublish(t, s, spec)
	if again.Created || again.Deliveries != 0 || again.Event.ID != first.Event.ID {
		t.Fatalf("retry = %+v, want the first event (%d) with no new fan-out", again, first.Event.ID)
	}
	ds, err := s.DeliveriesOfEvent(ctx, first.Event.ID)
	if err != nil || len(ds) != 1 {
		t.Fatalf("deliveries after retry = %d, %v; want still 1", len(ds), err)
	}

	// The same key for different work is a producer bug worth surfacing.
	conflict := spec
	conflict.Payload = []byte(`{"id":2}`)
	_, err = s.Publish(ctx, conflict)
	assertIs(t, err, event.ErrIdempotencyConflict)
	conflict = spec
	conflict.Type = "order.refunded"
	_, err = s.Publish(ctx, conflict)
	assertIs(t, err, event.ErrIdempotencyConflict)

	// Without a key, every publish is a new event.
	spec.IdempotencyKey = ""
	a, b := mustPublish(t, s, spec), mustPublish(t, s, spec)
	if a.Event.ID == b.Event.ID || !a.Created || !b.Created {
		t.Fatalf("unkeyed publishes = %d, %d; want two events", a.Event.ID, b.Event.ID)
	}
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("events = %d, %v; want 3", n, err)
	}
}

func TestPublishRefusesInvalidSpecWithoutWriting(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	tests := []struct {
		name string
		spec event.Spec
		want string
	}{
		{"bad type", event.Spec{Type: "Order Paid"}, "type"},
		{"scalar payload", event.Spec{Type: "x", Payload: []byte(`42`)}, "object or array"},
		{"broken json", event.Spec{Type: "x", Payload: []byte(`{"a":`)}, "valid JSON"},
		{"key too long", event.Spec{Type: "x", IdempotencyKey: strings.Repeat("k", 201)}, "idempotency key"},
		// encoding/json accepts bytes that are not UTF-8 inside a string;
		// PostgreSQL does not, and its refusal must come back as
		// ErrInvalid too, not as a database failure.
		{"not UTF-8", event.Spec{Type: "x", Payload: []byte("{\"a\":\"\xff\"}")}, "payload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Publish(ctx, tt.spec)
			assertIs(t, err, event.ErrInvalid)
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q, want it to mention %q", err, tt.want)
			}
		})
	}
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("events = %d, %v; want none", n, err)
	}
}

func TestGetEventNotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.GetEvent(context.Background(), 404)
	assertIs(t, err, event.ErrNotFound)
}

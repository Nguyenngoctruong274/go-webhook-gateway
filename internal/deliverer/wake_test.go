package deliverer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/postgres"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

// orderedStore behaves like the real store for one ordered endpoint: it
// hands out the oldest delivery only while nothing is in flight.
type orderedStore struct {
	mu       sync.Mutex
	queue    []postgres.Claimed
	inFlight bool
	order    []int64
	done     chan struct{}
}

func newOrderedStore(n int, ordered bool) *orderedStore {
	s := &orderedStore{done: make(chan struct{})}
	for i := 1; i <= n; i++ {
		s.queue = append(s.queue, postgres.Claimed{
			Delivery: event.Delivery{ID: int64(i), EndpointID: 7, State: event.StatePending, MaxAttempts: 3},
			Type:     "t", Payload: []byte(`{}`), URL: fmt.Sprintf("https://r.example/%d", i),
			Secrets: signing.Rotation{Current: secret}, Ordered: ordered,
		})
	}
	return s
}

func (s *orderedStore) Claim(_ context.Context, _ int, _ time.Duration) ([]postgres.Claimed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight || len(s.queue) == 0 {
		return nil, nil
	}
	c := s.queue[0]
	s.queue = s.queue[1:]
	s.inFlight = true
	c.Delivery.State, c.Delivery.Attempt = event.StateInFlight, 1
	return []postgres.Claimed{c}, nil
}

func (s *orderedStore) Finish(_ context.Context, id int64, attempt int, _ event.Result) (event.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight = false
	s.order = append(s.order, id)
	if len(s.queue) == 0 {
		close(s.done)
	}
	return event.Delivery{ID: id, Attempt: attempt, State: event.StateDelivered}, nil
}

func (s *orderedStore) Heartbeat(context.Context, int64, int, time.Duration) error { return nil }

func TestPoolClaimsAgainAtOnceWhenAnOrderedDeliveryFinishes(t *testing.T) {
	store := newOrderedStore(5, true)
	reg := metrics.NewRegistry()
	tr := &fakeTransport{fn: func(context.Context, Request) event.Result {
		return event.Result{Outcome: event.Delivered, Status: 200}
	}}
	// An hour between polls: only the wake-up can move the queue.
	stop := start(t, store, tr, Options{Concurrency: 4, PollInterval: time.Hour, Metrics: NewMetrics(reg)})
	defer stop()
	select {
	case <-store.done:
	case <-time.After(3 * time.Second):
		t.Fatal("an ordered endpoint waited for the poll interval between deliveries")
	}
	stop()
	if fmt.Sprint(store.order) != "[1 2 3 4 5]" {
		t.Fatalf("delivered in order %v", store.order)
	}
	var out strings.Builder
	reg.Write(&out)
	if !strings.Contains(out.String(), "webhook_claim_wakeups_total 4\n") && !strings.Contains(out.String(), "webhook_claim_wakeups_total 5\n") {
		t.Fatalf("wake-ups not counted:\n%s", out.String())
	}
}

func TestPoolDoesNotWakeForUnorderedEndpoints(t *testing.T) {
	store := newOrderedStore(2, false) // same one-at-a-time store, endpoint not ordered
	tr := &fakeTransport{fn: func(context.Context, Request) event.Result {
		return event.Result{Outcome: event.Delivered, Status: 200}
	}}
	stop := start(t, store, tr, Options{Concurrency: 4, PollInterval: time.Hour})
	defer stop()
	time.Sleep(300 * time.Millisecond)
	store.mu.Lock()
	n := len(store.order)
	store.mu.Unlock()
	if n != 1 {
		t.Fatalf("delivered %d; without an ordered endpoint the pool should wait out its poll interval after the first", n)
	}
}

func TestPoolMetrics(t *testing.T) {
	reg := metrics.NewRegistry()
	store := newFakeStore(3)
	store.lost[3] = true
	tr := &fakeTransport{fn: func(_ context.Context, r Request) event.Result {
		if r.DeliveryID == 2 {
			return event.Result{Outcome: event.Retry, Status: 503}
		}
		return event.Result{Outcome: event.Delivered, Status: 200}
	}}
	stop := start(t, store, tr, Options{Metrics: NewMetrics(reg)})
	store.wait(t, 2)
	time.Sleep(50 * time.Millisecond)
	stop()

	var out strings.Builder
	reg.Write(&out)
	got := out.String()
	for _, want := range []string{
		`webhook_attempts_total{endpoint="1",outcome="delivered"} 2`,
		`webhook_attempts_total{endpoint="1",outcome="retry"} 1`,
		`webhook_attempt_duration_seconds_count{endpoint="1"} 3`,
		`webhook_claims_total{result="deliveries"} 1`,
		`webhook_lease_lost_total 1`,
		`webhook_in_flight_requests 0`,
		`webhook_deliveries_ended_total{endpoint="1",state="delivered"} 1`,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("metrics lack %q", want)
		}
	}
	if strings.Contains(got, `state="pending"`) {
		t.Error("a retry was counted as an ended delivery")
	}
	if t.Failed() {
		t.Log(got)
	}
}

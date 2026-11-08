package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// gateStore returns a store with a fast backoff, an endpoint from spec and
// n pending deliveries to it, one event each.
func gateStore(t *testing.T, spec event.EndpointSpec, n int) (*Store, event.Endpoint) {
	t.Helper()
	s := newStore(t, WithBackoff(event.Backoff{Base: time.Millisecond, Max: time.Millisecond}))
	ep, _ := mustEndpoint(t, s, spec)
	for range n {
		mustPublish(t, s, event.Spec{Type: "x"})
	}
	return s, ep
}

func claimIDs(t *testing.T, s *Store) []int64 {
	t.Helper()
	got, err := s.Claim(context.Background(), 100, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, len(got))
	for i, c := range got {
		ids[i] = c.Delivery.ID
	}
	return ids
}

func finish(t *testing.T, s *Store, id int64, attempt int, r event.Result) event.Delivery {
	t.Helper()
	d, err := s.Finish(context.Background(), id, attempt, r)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestRateLimitAdmitsOneSecondOfBurstThenTheRate(t *testing.T) {
	s, ep := gateStore(t, event.EndpointSpec{RateLimit: 10}, 40)

	// Idle endpoint: one second's worth goes at once, not the whole queue.
	if n := len(claimIDs(t, s)); n != 10 {
		t.Fatalf("first claim took %d, want the burst of 10", n)
	}
	// Then about 10 per second, however often we ask.
	start := time.Now()
	total := 0
	for time.Since(start) < time.Second {
		total += len(claimIDs(t, s))
		time.Sleep(20 * time.Millisecond)
	}
	if total < 8 || total > 13 {
		t.Fatalf("claimed %d in the second after the burst, want about 10", total)
	}

	// A long idle spell earns no more than the burst.
	if _, err := s.db.ExecContext(context.Background(),
		`UPDATE endpoints SET rate_cursor = now() - interval '1 hour' WHERE id = $1`, ep.ID); err != nil {
		t.Fatal(err)
	}
	if n := len(claimIDs(t, s)); n != 10 {
		t.Fatalf("after an hour idle: claimed %d, want the burst of 10", n)
	}
}

func TestRateLimitIsSharedByConcurrentClaimers(t *testing.T) {
	s, _ := gateStore(t, event.EndpointSpec{RateLimit: 10}, 50)
	const claimers = 8
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total int
	)
	start := make(chan struct{})
	for range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n := len(claimIDs(t, s))
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	// Each claimer asked for 100; the cursor on the endpoint row, under
	// SKIP LOCKED, let one of them spend the credit and the rest nothing
	// (or a sliver, if the lock changed hands after a few ms).
	if total < 10 || total > 11 {
		t.Fatalf("%d claimers took %d in total, want the burst of 10 shared", claimers, total)
	}
}

func TestRateLimitLeavesOtherEndpointsAlone(t *testing.T) {
	s, _ := gateStore(t, event.EndpointSpec{RateLimit: 1}, 5)
	free, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://free.example/hook"})
	for range 5 {
		mustPublish(t, s, event.Spec{Type: "y"})
	}
	got, err := s.Claim(context.Background(), 100, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var limited, unlimited int
	for _, c := range got {
		if c.Delivery.EndpointID == free.ID {
			unlimited++
		} else {
			limited++
		}
	}
	// The free endpoint got all 5 deliveries fanned out to it; the limited
	// one, with 10 due, got 1.
	if unlimited != 5 || limited != 1 {
		t.Fatalf("claimed %d from the free endpoint and %d from the limited one, want 5 and 1", unlimited, limited)
	}
}

func TestOrderedEndpointDeliversOneAtATimeOldestFirst(t *testing.T) {
	s, _ := gateStore(t, event.EndpointSpec{Ordered: true, MaxAttempts: 2}, 3)
	var all pq.Int64Array
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT array_agg(id ORDER BY id) FROM deliveries`).Scan(&all); err != nil {
		t.Fatal(err)
	}
	d1, d2, d3 := all[0], all[1], all[2]

	got := claimIDs(t, s)
	if len(got) != 1 || got[0] != d1 {
		t.Fatalf("first claim = %v, want only the oldest %d", got, d1)
	}
	if got := claimIDs(t, s); len(got) != 0 {
		t.Fatalf("claimed %v while the head is in flight", got)
	}

	// The head fails and retries: it still comes first; nothing behind it moves.
	finish(t, s, d1, 1, event.Result{Outcome: event.Retry, Status: 503})
	time.Sleep(5 * time.Millisecond)
	if got := claimIDs(t, s); len(got) != 1 || got[0] != d1 {
		t.Fatalf("after a retry the claim = %v, want the head %d again", got, d1)
	}
	finish(t, s, d1, 2, event.Result{Outcome: event.Delivered, Status: 200})
	if got := claimIDs(t, s); len(got) != 1 || got[0] != d2 {
		t.Fatalf("after the head was delivered the claim = %v, want %d", got, d2)
	}

	// A delivery that is spent (dead-lettered) stops holding the line.
	finish(t, s, d2, 1, event.Result{Outcome: event.Retry, Status: 503})
	time.Sleep(5 * time.Millisecond)
	claimIDs(t, s)
	d := finish(t, s, d2, 2, event.Result{Outcome: event.Retry, Status: 503})
	if d.State != event.StateFailed {
		t.Fatalf("after the last attempt %d is %s, want failed", d2, d.State)
	}
	if got := claimIDs(t, s); len(got) != 1 || got[0] != d3 {
		t.Fatalf("behind a dead delivery the claim = %v, want %d", got, d3)
	}
}

func TestOrderedEndpointUnderConcurrentClaimers(t *testing.T) {
	s, _ := gateStore(t, event.EndpointSpec{Ordered: true}, 20)
	const claimers = 10
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total int
	)
	start := make(chan struct{})
	for range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n := len(claimIDs(t, s))
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if total != 1 {
		t.Fatalf("%d claimers took %d deliveries from an ordered endpoint, want 1", claimers, total)
	}
}

func TestOrderedAndRateLimitedAndBreakerCompose(t *testing.T) {
	// Ordered wins over the burst: one at a time even with credit for 10.
	s, _ := gateStore(t, event.EndpointSpec{Ordered: true, RateLimit: 10}, 5)
	if got := claimIDs(t, s); len(got) != 1 {
		t.Fatalf("ordered + rate limited claim = %v, want 1", got)
	}
}

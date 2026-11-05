package postgres

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

func TestReapExpiresOnlyRunOutLeases(t *testing.T) {
	s := newStore(t, WithBackoff(event.Backoff{Base: time.Second, Max: time.Minute}))
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{MaxAttempts: 2})
	mustPublish(t, s, event.Spec{Type: "a"})
	mustPublish(t, s, event.Spec{Type: "b"})
	stale := claimOne(t, s, time.Microsecond)
	live := claimOne(t, s, time.Hour)
	time.Sleep(2 * time.Millisecond)

	expired, err := s.Reap(ctx, 10)
	if err != nil || !slices.Equal(expired, []int64{stale.Delivery.ID}) {
		t.Fatalf("Reap = %v, %v; want [%d]", expired, err, stale.Delivery.ID)
	}
	d, err := s.GetDelivery(ctx, stale.Delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != event.StatePending || d.Attempt != 1 || !strings.Contains(d.LastError, "lease expired") || !d.LeaseUntil.IsZero() {
		t.Fatalf("reaped delivery = %+v", d)
	}
	if l, _ := s.GetDelivery(ctx, live.Delivery.ID); l.State != event.StateInFlight || !l.LeaseUntil.Equal(live.Delivery.LeaseUntil) {
		t.Fatalf("live delivery was touched: %+v", l)
	}
	if again, err := s.Reap(ctx, 10); err != nil || len(again) != 0 {
		t.Fatalf("second Reap = %v, %v", again, err)
	}

	// The last attempt expiring fails the delivery (dead letter).
	if _, err := s.db.ExecContext(ctx, `UPDATE deliveries SET next_attempt_at = now() WHERE id = $1`, stale.Delivery.ID); err != nil {
		t.Fatal(err)
	}
	claimOne(t, s, time.Microsecond)
	time.Sleep(2 * time.Millisecond)
	if expired, err := s.Reap(ctx, 10); err != nil || !slices.Equal(expired, []int64{stale.Delivery.ID}) {
		t.Fatalf("Reap of the last attempt = %v, %v", expired, err)
	}
	if d, _ := s.GetDelivery(ctx, stale.Delivery.ID); d.State != event.StateFailed || d.Attempt != 2 {
		t.Fatalf("after the last lease expired: %+v", d)
	}
	if _, err := s.Reap(ctx, 0); err == nil {
		t.Fatal("limit 0 accepted")
	}
}

// TestReapersRaceDeliverers runs reapers against deliverers finishing
// just as their lease runs out: every attempt ends exactly once.
func TestReapersRaceDeliverers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{MaxAttempts: 10})
	const n = 30
	for i := 0; i < n; i++ {
		mustPublish(t, s, event.Spec{Type: "t"})
	}
	claimed, err := s.Claim(ctx, n, 3*time.Millisecond)
	if err != nil || len(claimed) != n {
		t.Fatalf("claimed %d, %v", len(claimed), err)
	}

	results := make(chan string, n)
	reaped := make(chan int, 3)
	for _, c := range claimed {
		go func(c Claimed) {
			if c.Delivery.ID%2 == 0 {
				time.Sleep(60 * time.Millisecond) // outlives the lease
			}
			_, err := s.Finish(ctx, c.Delivery.ID, c.Delivery.Attempt, event.Result{Outcome: event.Delivered, Status: 200})
			switch {
			case err == nil:
				results <- "delivered"
			case strings.Contains(err.Error(), "lease lost"):
				results <- "lost"
			default:
				t.Error(err)
				results <- "error"
			}
		}(c)
	}
	for r := 0; r < 3; r++ {
		go func() {
			total := 0
			deadline := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(deadline) {
				ids, err := s.Reap(ctx, 10)
				if err != nil {
					t.Error(err)
				}
				total += len(ids)
				time.Sleep(time.Millisecond)
			}
			reaped <- total
		}()
	}
	var delivered, lost int
	for i := 0; i < n; i++ {
		switch <-results {
		case "delivered":
			delivered++
		case "lost":
			lost++
		}
	}
	totalReaped := 0
	for i := 0; i < 3; i++ {
		totalReaped += <-reaped
	}
	if delivered+lost != n || totalReaped != lost {
		t.Fatalf("delivered %d, lost %d, reaped %d: every attempt must end exactly once", delivered, lost, totalReaped)
	}
	var done, pending, inFlight int
	if err := s.db.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE state = 'delivered'), count(*) FILTER (WHERE state = 'pending'),
		       count(*) FILTER (WHERE state = 'in_flight') FROM deliveries`).Scan(&done, &pending, &inFlight); err != nil {
		t.Fatal(err)
	}
	if done != delivered || pending != lost || inFlight != 0 {
		t.Fatalf("table says delivered %d pending %d in_flight %d; deliverers saw %d/%d", done, pending, inFlight, delivered, lost)
	}
	t.Logf("%d delivered in time, %d lost their lease and were reaped", delivered, lost)
}

package postgres

import (
	"context"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

func TestClaimHandsOutDueDeliveriesWithWhatTheDelivererNeeds(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	a, secretA := mustEndpoint(t, s, event.EndpointSpec{URL: "https://a.example/hook"})
	b, secretB := mustEndpoint(t, s, event.EndpointSpec{URL: "https://b.example/hook"})
	e1 := mustPublish(t, s, event.Spec{Type: "order.paid", Payload: []byte(`{"n":1}`)}).Event
	e2 := mustPublish(t, s, event.Spec{Type: "order.refunded", Payload: []byte(`{"n":2}`)}).Event

	before := dbNow(t, s)
	got, err := s.Claim(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("claimed %d, want 4", len(got))
	}
	want := []struct {
		ev     event.Event
		ep     event.Endpoint
		secret string
	}{{e1, a, secretA}, {e1, b, secretB}, {e2, a, secretA}, {e2, b, secretB}}
	for i, c := range got {
		w := want[i]
		d := c.Delivery
		if d.EventID != w.ev.ID || d.EndpointID != w.ep.ID {
			t.Fatalf("claim %d is event %d → endpoint %d, want %d → %d", i, d.EventID, d.EndpointID, w.ev.ID, w.ep.ID)
		}
		if d.State != event.StateInFlight || d.Attempt != 1 {
			t.Fatalf("claim %d: state %s attempt %d, want in_flight attempt 1", i, d.State, d.Attempt)
		}
		assertNear(t, "lease_until", d.LeaseUntil, before.Add(time.Minute), 5*time.Second)
		if c.Type != w.ev.Type || string(c.Payload) != string(w.ev.Payload) {
			t.Fatalf("claim %d carries %s %s, want %s %s", i, c.Type, c.Payload, w.ev.Type, w.ev.Payload)
		}
		if c.URL != w.ep.URL || c.Secrets.Current != w.secret || c.Secrets.Previous != "" {
			t.Fatalf("claim %d carries url %q secrets %+v, want %q %q", i, c.URL, c.Secrets, w.ep.URL, w.secret)
		}
		stored, err := s.GetDelivery(ctx, d.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.State != event.StateInFlight || stored.Attempt != 1 || !stored.LeaseUntil.Equal(d.LeaseUntil) {
			t.Fatalf("stored delivery %d = %+v, want what Claim returned", d.ID, stored)
		}
	}

	// Everything is in flight now.
	got, err = s.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("second claim = %d deliveries, %v; want none", len(got), err)
	}
}

func TestClaimSkipsDeliveriesNotDueYet(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{})
	ev := mustPublish(t, s, event.Spec{Type: "x"}).Event
	if _, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET next_attempt_at = now() + interval '1 hour' WHERE event_id = $1`, ev.ID); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("claimed %d, %v; want none", len(got), err)
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET next_attempt_at = now() WHERE event_id = $1`, ev.ID); err != nil {
		t.Fatal(err)
	}
	got, err = s.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 1 {
		t.Fatalf("claimed %d, %v; want 1 once due", len(got), err)
	}
}

func TestClaimHonoursTheLimit(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{})
	for range 5 {
		mustPublish(t, s, event.Spec{Type: "x"})
	}
	var sizes []int
	for {
		got, err := s.Claim(ctx, 2, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 {
			break
		}
		sizes = append(sizes, len(got))
	}
	if want := []int{2, 2, 1}; len(sizes) != 3 || sizes[0] != 2 || sizes[1] != 2 || sizes[2] != 1 {
		t.Fatalf("batch sizes = %v, want %v", sizes, want)
	}
}

func TestClaimRefusesBadArguments(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for _, tt := range []struct {
		limit int
		lease time.Duration
		want  string
	}{
		{0, time.Minute, "limit"},
		{MaxClaimBatch + 1, time.Minute, "limit"},
		{1, 0, "lease"},
		{1, -time.Second, "lease"},
	} {
		_, err := s.Claim(ctx, tt.limit, tt.lease)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Fatalf("Claim(%d, %v) error = %v, want one about the %s", tt.limit, tt.lease, err, tt.want)
		}
	}
}

func claimOne(t *testing.T, s *Store, lease time.Duration) Claimed {
	t.Helper()
	got, err := s.Claim(context.Background(), 1, lease)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("claimed %d, want 1", len(got))
	}
	return got[0]
}

func TestFinishDelivered(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{})
	mustPublish(t, s, event.Spec{Type: "x"})
	c := claimOne(t, s, time.Minute)

	before := dbNow(t, s)
	d, err := s.Finish(ctx, c.Delivery.ID, c.Delivery.Attempt, event.Result{Outcome: event.Delivered, Status: 200})
	if err != nil {
		t.Fatal(err)
	}
	if d.State != event.StateDelivered || d.LastStatus != 200 || d.LastError != "" || !d.LeaseUntil.IsZero() {
		t.Fatalf("finished = %+v, want delivered with no lease", d)
	}
	assertNear(t, "delivered_at", d.DeliveredAt, before, 5*time.Second)
	stored, err := s.GetDelivery(ctx, d.ID)
	if err != nil || stored.State != event.StateDelivered || !stored.DeliveredAt.Equal(d.DeliveredAt) {
		t.Fatalf("stored = %+v, %v; want what Finish returned", stored, err)
	}

	// Reporting again, for example after a retried HTTP call to the
	// store, changes nothing: the attempt is over.
	_, err = s.Finish(ctx, d.ID, c.Delivery.Attempt, event.Result{Outcome: event.Retry, Status: 503})
	assertIs(t, err, event.ErrLeaseLost)
	again, err := s.GetDelivery(ctx, d.ID)
	if err != nil || again != stored {
		t.Fatalf("after refused finish = %+v, %v; want unchanged %+v", again, err, stored)
	}
}

func TestFinishRetrySchedulesTheBackoffThenFails(t *testing.T) {
	s := newStore(t, WithBackoff(event.Backoff{Base: 10 * time.Second, Max: time.Hour, JitterPercent: 0}))
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{MaxAttempts: 2})
	mustPublish(t, s, event.Spec{Type: "x"})
	c := claimOne(t, s, time.Minute)

	before := dbNow(t, s)
	d, err := s.Finish(ctx, c.Delivery.ID, 1, event.Result{Outcome: event.Retry, Status: 503, Error: "HTTP 503: try later"})
	if err != nil {
		t.Fatal(err)
	}
	if d.State != event.StatePending || d.Attempt != 1 || d.LastStatus != 503 || d.LastError != "HTTP 503: try later" || !d.LeaseUntil.IsZero() {
		t.Fatalf("after first failure = %+v, want pending at attempt 1 with the error kept", d)
	}
	assertNear(t, "next_attempt_at", d.NextAttemptAt, before.Add(10*time.Second), 5*time.Second)

	// Not due for 10s: nobody can claim it yet.
	got, err := s.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("claim during backoff = %d, %v; want none", len(got), err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE deliveries SET next_attempt_at = now() WHERE id = $1`, d.ID); err != nil {
		t.Fatal(err)
	}
	c = claimOne(t, s, time.Minute)
	if c.Delivery.ID != d.ID || c.Delivery.Attempt != 2 {
		t.Fatalf("second claim = %+v, want delivery %d at attempt 2", c.Delivery, d.ID)
	}

	// The last attempt fails too: dead letter.
	d, err = s.Finish(ctx, d.ID, 2, event.Result{Outcome: event.Retry, Error: "timeout: context deadline exceeded"})
	if err != nil {
		t.Fatal(err)
	}
	if d.State != event.StateFailed || d.Attempt != 2 || d.LastStatus != 0 || !strings.HasPrefix(d.LastError, "timeout") {
		t.Fatalf("after last failure = %+v, want failed", d)
	}
	got, err = s.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 0 {
		t.Fatalf("claim of a failed delivery = %d, %v; want none", len(got), err)
	}
}

func TestFinishFinalOutcomes(t *testing.T) {
	for _, tt := range []struct {
		outcome event.Outcome
		status  int
		state   event.State
	}{
		{event.Rejected, 404, event.StateRejected},
		{event.Blocked, 0, event.StateBlocked},
	} {
		t.Run(tt.outcome.String(), func(t *testing.T) {
			s := newStore(t)
			ctx := context.Background()
			mustEndpoint(t, s, event.EndpointSpec{})
			mustPublish(t, s, event.Spec{Type: "x"})
			c := claimOne(t, s, time.Minute)
			d, err := s.Finish(ctx, c.Delivery.ID, 1, event.Result{Outcome: tt.outcome, Status: tt.status, Error: "nope"})
			if err != nil {
				t.Fatal(err)
			}
			if d.State != tt.state || d.LastStatus != tt.status || d.LastError != "nope" || !d.DeliveredAt.IsZero() {
				t.Fatalf("finished = %+v, want %s", d, tt.state)
			}
		})
	}
}

func TestFinishWithAStaleAttemptIsRefused(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{})
	mustPublish(t, s, event.Spec{Type: "x"})
	first := claimOne(t, s, time.Minute)

	// The reaper (day 5) hands a delivery whose deliverer stalled to the
	// next claim; do that by hand here.
	if _, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET state = 'pending', lease_until = NULL, next_attempt_at = now() WHERE id = $1`,
		first.Delivery.ID); err != nil {
		t.Fatal(err)
	}
	second := claimOne(t, s, time.Minute)
	if second.Delivery.ID != first.Delivery.ID || second.Delivery.Attempt != 2 {
		t.Fatalf("second claim = %+v, want the same delivery at attempt 2", second.Delivery)
	}

	// The stalled deliverer reports late with attempt 1: refused, and the
	// attempt that replaced it is untouched.
	_, err := s.Finish(ctx, first.Delivery.ID, 1, event.Result{Outcome: event.Delivered, Status: 200})
	assertIs(t, err, event.ErrLeaseLost)
	err = s.Heartbeat(ctx, first.Delivery.ID, 1, time.Minute)
	assertIs(t, err, event.ErrLeaseLost)
	d, err := s.GetDelivery(ctx, first.Delivery.ID)
	if err != nil || d.State != event.StateInFlight || d.Attempt != 2 || !d.LeaseUntil.Equal(second.Delivery.LeaseUntil) {
		t.Fatalf("delivery after stale reports = %+v, %v; want attempt 2 in flight untouched", d, err)
	}

	// The current holder's report goes through.
	d, err = s.Finish(ctx, first.Delivery.ID, 2, event.Result{Outcome: event.Delivered, Status: 200})
	if err != nil || d.State != event.StateDelivered {
		t.Fatalf("current holder's finish = %+v, %v", d, err)
	}
}

func TestFinishAndHeartbeatNotFound(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, err := s.Finish(ctx, 404, 1, event.Result{Outcome: event.Delivered})
	assertIs(t, err, event.ErrNotFound)
	err = s.Heartbeat(ctx, 404, 1, time.Minute)
	assertIs(t, err, event.ErrNotFound)
	_, err = s.GetDelivery(ctx, 404)
	assertIs(t, err, event.ErrNotFound)
}

func TestHeartbeatExtendsTheLease(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{})
	mustPublish(t, s, event.Spec{Type: "x"})
	c := claimOne(t, s, time.Minute)

	before := dbNow(t, s)
	if err := s.Heartbeat(ctx, c.Delivery.ID, 1, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	d, err := s.GetDelivery(ctx, c.Delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != event.StateInFlight || d.Attempt != 1 || !d.LeaseUntil.After(c.Delivery.LeaseUntil) {
		t.Fatalf("after heartbeat = %+v, want the lease extended past %s", d, c.Delivery.LeaseUntil)
	}
	assertNear(t, "lease_until", d.LeaseUntil, before.Add(10*time.Minute), 5*time.Second)

	err = s.Heartbeat(ctx, c.Delivery.ID, 1, 0)
	if err == nil || !strings.Contains(err.Error(), "lease must be positive") {
		t.Fatalf("heartbeat with zero lease: %v", err)
	}
}

// claimLog records which deliverer claimed which delivery, and fails on a
// repeat.
type claimLog struct {
	mu sync.Mutex
	by map[int64]int
}

func (l *claimLog) add(t *testing.T, worker int, cs []Claimed) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range cs {
		d := c.Delivery
		if prev, dup := l.by[d.ID]; dup {
			t.Errorf("delivery %d claimed by deliverer %d and again by deliverer %d", d.ID, prev, worker)
		}
		l.by[d.ID] = worker
		if d.Attempt != 1 || d.State != event.StateInFlight {
			t.Errorf("delivery %d claimed at attempt %d in state %s, want attempt 1, in_flight", d.ID, d.Attempt, d.State)
		}
	}
}

// TestClaimEachDeliveryOnceUnderContention has many deliverers drain a
// full queue at once with random batch sizes. Every delivery must be
// claimed exactly once: that is what SKIP LOCKED plus the state predicate
// promise, and what the fencing token relies on.
func TestClaimEachDeliveryOnceUnderContention(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const deliveries, workers = 1500, 16
	ep, _ := mustEndpoint(t, s, event.EndpointSpec{})
	if _, err := s.db.ExecContext(ctx, `
		WITH evs AS (
			INSERT INTO events (type, payload)
			SELECT 'bulk', '{}' FROM generate_series(1, $1) RETURNING id
		)
		INSERT INTO deliveries (event_id, endpoint_id, max_attempts)
		SELECT id, $2, 3 FROM evs`, deliveries, ep.ID); err != nil {
		t.Fatal(err)
	}

	log := &claimLog{by: make(map[int64]int)}
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		calls atomic.Int64
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rnd := rand.New(rand.NewPCG(uint64(w), 1))
			<-start
			for {
				calls.Add(1)
				got, err := s.Claim(ctx, 1+rnd.IntN(25), time.Minute)
				if err != nil {
					t.Errorf("deliverer %d: %v", w, err)
					return
				}
				if len(got) == 0 {
					// Rows still locked by other deliverers' claims are theirs.
					return
				}
				log.add(t, w, got)
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(log.by) != deliveries {
		t.Fatalf("claimed %d distinct deliveries, want %d", len(log.by), deliveries)
	}
	perWorker := make(map[int]int)
	for _, w := range log.by {
		perWorker[w]++
	}
	t.Logf("%d claim calls; deliveries per deliverer: %v", calls.Load(), perWorker)

	var inFlight, other int
	if err := s.db.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE state = 'in_flight' AND attempt = 1 AND lease_until IS NOT NULL),
		       count(*) FILTER (WHERE NOT (state = 'in_flight' AND attempt = 1))
		FROM deliveries`).Scan(&inFlight, &other); err != nil {
		t.Fatal(err)
	}
	if inFlight != deliveries || other != 0 {
		t.Fatalf("%d in flight at attempt 1, %d other; want %d and 0", inFlight, other, deliveries)
	}
}

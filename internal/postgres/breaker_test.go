package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/breaker"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// testPolicy opens after 3 transient failures; the periods are long so
// a test decides when they elapse by rewinding the stored deadlines.
var testPolicy = breaker.Policy{
	FailureThreshold: 3, OpenFor: time.Hour, MaxOpenFor: 4 * time.Hour, ProbeTimeout: time.Hour,
}

var (
	retry503  = event.Result{Outcome: event.Retry, Status: 503, Error: "HTTP 503"}
	ok200     = event.Result{Outcome: event.Delivered, Status: 200}
	reject404 = event.Result{Outcome: event.Rejected, Status: 404, Error: "HTTP 404"}
)

// breakerStore returns a store with testPolicy, one endpoint and n
// pending deliveries to it, each from its own event so they are claimable
// one at a time.
func breakerStore(t *testing.T, n int) (*Store, event.Endpoint) {
	t.Helper()
	s := newStore(t, WithBreaker(testPolicy), WithBackoff(event.Backoff{Base: time.Millisecond, Max: time.Millisecond}))
	ep, _ := mustEndpoint(t, s, event.EndpointSpec{})
	for range n {
		mustPublish(t, s, event.Spec{Type: "x"})
	}
	return s, ep
}

// attempt claims one delivery and finishes it with r.
func attempt(t *testing.T, s *Store, r event.Result) event.Delivery {
	t.Helper()
	c := claimOne(t, s, time.Minute)
	d, err := s.Finish(context.Background(), c.Delivery.ID, c.Delivery.Attempt, r)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustBreaker(t *testing.T, s *Store, id int64) breaker.State {
	t.Helper()
	st, err := s.Breaker(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// rewind moves the breaker's deadlines into the past, as if the open
// period or the probe timeout had elapsed.
func rewind(t *testing.T, s *Store, id int64) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(), `
		UPDATE endpoints SET breaker_opened_until = breaker_opened_until - interval '1 day',
			breaker_probe_until = breaker_probe_until - interval '1 day' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

func claimCount(t *testing.T, s *Store) int {
	t.Helper()
	got, err := s.Claim(context.Background(), 100, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return len(got)
}

func TestBreakerOpensAfterConsecutiveFailuresAndClaimSkipsTheEndpoint(t *testing.T) {
	s, ep := breakerStore(t, 6)
	if st := mustBreaker(t, s, ep.ID); st.Status != breaker.Closed {
		t.Fatalf("new endpoint: %+v", st)
	}
	before := dbNow(t, s)
	for i := range 3 {
		attempt(t, s, retry503) // the same deliveries retry after 1ms
		time.Sleep(5 * time.Millisecond)
		if st := mustBreaker(t, s, ep.ID); i < 2 && (st.Status != breaker.Closed || st.Failures != i+1) {
			t.Fatalf("after %d failures: %+v", i+1, st)
		}
	}
	st := mustBreaker(t, s, ep.ID)
	if st.Status != breaker.Open || st.Opens != 1 || st.Failures != 0 {
		t.Fatalf("after the threshold: %+v", st)
	}
	assertNear(t, "opened_until", st.OpenedUntil, before.Add(time.Hour), 5*time.Second)

	// Six pending, due deliveries, none claimable while the breaker is open.
	if n := claimCount(t, s); n != 0 {
		t.Fatalf("claimed %d behind an open breaker", n)
	}
	var pending int
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM deliveries WHERE state = 'pending' AND next_attempt_at <= now()`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 6 {
		t.Fatalf("%d due pending deliveries, want 6 untouched", pending)
	}

	// A different endpoint is unaffected.
	other, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://other.example/hook"})
	mustPublish(t, s, event.Spec{Type: "y"})
	got, err := s.Claim(context.Background(), 100, time.Minute)
	if err != nil || len(got) != 1 || got[0].Delivery.EndpointID != other.ID {
		t.Fatalf("claim with one open and one closed endpoint = %+v, %v", got, err)
	}
}

func TestBreakerRejectionsDoNotOpenIt(t *testing.T) {
	s, ep := breakerStore(t, 10)
	for range 10 {
		attempt(t, s, reject404)
	}
	if st := mustBreaker(t, s, ep.ID); st.Status != breaker.Closed || st.Failures != 0 {
		t.Fatalf("4xx moved the breaker: %+v", st)
	}
	// A success in between resets the consecutive count.
	s2, ep2 := breakerStore(t, 4)
	attempt(t, s2, retry503)
	attempt(t, s2, retry503)
	attempt(t, s2, ok200)
	if st := mustBreaker(t, s2, ep2.ID); st.Status != breaker.Closed || st.Failures != 0 {
		t.Fatalf("success did not reset: %+v", st)
	}
}

func openBreaker(t *testing.T, s *Store) {
	t.Helper()
	for range 3 {
		attempt(t, s, retry503)
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBreakerAdmitsOneProbeAfterThePeriod(t *testing.T) {
	s, ep := breakerStore(t, 5)
	openBreaker(t, s)
	rewind(t, s, ep.ID)

	// Five due deliveries, one claim of up to 100: exactly one goes as
	// the probe and the breaker is half-open with a probe deadline.
	got, err := s.Claim(context.Background(), 100, time.Minute)
	if err != nil || len(got) != 1 {
		t.Fatalf("probe claim = %d deliveries, %v; want 1", len(got), err)
	}
	st := mustBreaker(t, s, ep.ID)
	if st.Status != breaker.HalfOpen || st.ProbeUntil.Before(dbNow(t, s)) {
		t.Fatalf("after the probe claim: %+v", st)
	}
	// While the probe is in flight nothing else is claimable.
	if n := claimCount(t, s); n != 0 {
		t.Fatalf("claimed %d during the probe", n)
	}

	// The probe succeeds: closed, and the rest flow.
	if _, err := s.Finish(context.Background(), got[0].Delivery.ID, got[0].Delivery.Attempt, ok200); err != nil {
		t.Fatal(err)
	}
	if st := mustBreaker(t, s, ep.ID); st != (breaker.State{Status: breaker.Closed}) {
		t.Fatalf("after a good probe: %+v", st)
	}
	if n := claimCount(t, s); n != 4 {
		t.Fatalf("claimed %d after the breaker closed, want the other 4", n)
	}
}

func TestBreakerFailedProbeReopensForLonger(t *testing.T) {
	s, ep := breakerStore(t, 5)
	openBreaker(t, s)

	for i, wantPeriod := range []time.Duration{2 * time.Hour, 4 * time.Hour, 4 * time.Hour} {
		rewind(t, s, ep.ID)
		probe := claimOne(t, s, time.Minute)
		before := dbNow(t, s)
		if _, err := s.Finish(context.Background(), probe.Delivery.ID, probe.Delivery.Attempt, retry503); err != nil {
			t.Fatal(err)
		}
		st := mustBreaker(t, s, ep.ID)
		if st.Status != breaker.Open || st.Opens != i+2 {
			t.Fatalf("after failed probe %d: %+v", i+1, st)
		}
		assertNear(t, "opened_until", st.OpenedUntil, before.Add(wantPeriod), 5*time.Second)
		if n := claimCount(t, s); n != 0 {
			t.Fatalf("claimed %d right after reopening", n)
		}
	}

	// A 4xx on the probe proves the receiver is up again.
	rewind(t, s, ep.ID)
	probe := claimOne(t, s, time.Minute)
	if _, err := s.Finish(context.Background(), probe.Delivery.ID, probe.Delivery.Attempt, reject404); err != nil {
		t.Fatal(err)
	}
	if st := mustBreaker(t, s, ep.ID); st.Status != breaker.Closed {
		t.Fatalf("after a 4xx probe: %+v", st)
	}
}

func TestBreakerProbeTimeoutAllowsAnotherProbe(t *testing.T) {
	s, ep := breakerStore(t, 3)
	openBreaker(t, s)
	rewind(t, s, ep.ID)
	first := claimOne(t, s, time.Minute) // the deliverer dies with it
	if n := claimCount(t, s); n != 0 {
		t.Fatalf("claimed %d while a probe is in flight", n)
	}
	rewind(t, s, ep.ID) // probe deadline passes
	second := claimOne(t, s, time.Minute)
	if second.Delivery.ID == first.Delivery.ID {
		t.Fatal("the lost probe was claimed again while still in flight")
	}
	st := mustBreaker(t, s, ep.ID)
	if st.Status != breaker.HalfOpen || st.ProbeUntil.Before(dbNow(t, s)) {
		t.Fatalf("after the second probe: %+v", st)
	}
	// The late verdict of the first probe (reaped and retried, or a slow
	// report) does not take the second's place: a late failure while
	// half-open counts, as it is still evidence about the receiver.
	if _, err := s.Finish(context.Background(), second.Delivery.ID, second.Delivery.Attempt, ok200); err != nil {
		t.Fatal(err)
	}
	if st := mustBreaker(t, s, ep.ID); st.Status != breaker.Closed {
		t.Fatalf("after the second probe succeeded: %+v", st)
	}
}

// TestBreakerConcurrentClaimersElectOneProbe has many claimers race for
// the probe of an endpoint whose open period just elapsed. Exactly one
// delivery may be in flight afterwards.
func TestBreakerConcurrentClaimersElectOneProbe(t *testing.T) {
	s, ep := breakerStore(t, 20)
	openBreaker(t, s)
	rewind(t, s, ep.ID)

	const claimers = 12
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
			got, err := s.Claim(context.Background(), 10, time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			total += len(got)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if total != 1 {
		t.Fatalf("%d claimers claimed %d deliveries in total, want exactly 1 probe", claimers, total)
	}
	var inFlight int
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM deliveries WHERE state = 'in_flight'`).Scan(&inFlight); err != nil {
		t.Fatal(err)
	}
	if inFlight != 1 {
		t.Fatalf("%d deliveries in flight, want 1", inFlight)
	}
	if st := mustBreaker(t, s, ep.ID); st.Status != breaker.HalfOpen {
		t.Fatalf("after the election: %+v", st)
	}
}

func TestResetBreakerMakesDeliveriesClaimableAgain(t *testing.T) {
	s, ep := breakerStore(t, 4)
	openBreaker(t, s)
	if n := claimCount(t, s); n != 0 {
		t.Fatalf("claimed %d while open", n)
	}
	if err := s.ResetBreaker(context.Background(), ep.ID); err != nil {
		t.Fatal(err)
	}
	if st := mustBreaker(t, s, ep.ID); st != (breaker.State{Status: breaker.Closed}) {
		t.Fatalf("after reset: %+v", st)
	}
	if n := claimCount(t, s); n != 4 {
		t.Fatalf("claimed %d after reset, want all 4", n)
	}
	assertIs(t, s.ResetBreaker(context.Background(), 999999), event.ErrNotFound)
	_, err := s.Breaker(context.Background(), 999999)
	assertIs(t, err, event.ErrNotFound)
}

func TestBreakerSchemaRefusesInconsistentRows(t *testing.T) {
	s, ep := breakerStore(t, 0)
	ctx := context.Background()
	for _, q := range []string{
		`UPDATE endpoints SET breaker_status = 'tripped' WHERE id = $1`,
		`UPDATE endpoints SET breaker_status = 'open', breaker_opened_until = NULL WHERE id = $1`,
		`UPDATE endpoints SET breaker_status = 'half_open', breaker_probe_until = NULL WHERE id = $1`,
		`UPDATE endpoints SET breaker_failures = -1 WHERE id = $1`,
	} {
		if _, err := s.db.ExecContext(ctx, q, ep.ID); err == nil {
			t.Errorf("accepted: %s", q)
		}
	}
}

func TestNewStoreRejectsInvalidBreakerPolicy(t *testing.T) {
	_, err := NewStore(nil, WithBreaker(breaker.Policy{}))
	if err == nil {
		t.Fatal("accepted an empty breaker policy")
	}
}

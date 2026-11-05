package deliverer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/postgres"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeStore hands out deliveries and records what the pool reports.
type fakeStore struct {
	mu       sync.Mutex
	queue    []postgres.Claimed
	finished map[int64]event.Result
	beats    map[int64]int
	lost     map[int64]bool // ids whose lease another deliverer took
	claims   []int
	reported chan int64
}

func newFakeStore(n int) *fakeStore {
	s := &fakeStore{finished: map[int64]event.Result{}, beats: map[int64]int{}, lost: map[int64]bool{}, reported: make(chan int64, 1000)}
	for i := 1; i <= n; i++ {
		s.queue = append(s.queue, postgres.Claimed{
			Delivery: event.Delivery{ID: int64(i), EventID: 1, EndpointID: 1, State: event.StatePending, MaxAttempts: 3},
			Type:     "t", Payload: []byte(`{}`), URL: fmt.Sprintf("https://r.example/%d", i),
			Secrets: signing.Rotation{Current: secret},
		})
	}
	return s
}

func (s *fakeStore) Claim(_ context.Context, limit int, lease time.Duration) ([]postgres.Claimed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claims = append(s.claims, limit)
	n := min(limit, len(s.queue))
	out := s.queue[:n:n]
	s.queue = s.queue[n:]
	for i := range out {
		out[i].Delivery.State = event.StateInFlight
		out[i].Delivery.Attempt = 1
		out[i].Delivery.LeaseUntil = time.Now().Add(lease)
	}
	return out, nil
}

func (s *fakeStore) Finish(_ context.Context, id int64, attempt int, r event.Result) (event.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lost[id] {
		return event.Delivery{}, fmt.Errorf("finish delivery %d: %w", id, event.ErrLeaseLost)
	}
	s.finished[id] = r
	s.reported <- id
	d := event.Delivery{ID: id, Attempt: attempt, State: event.StatePending}
	if r.Outcome == event.Delivered {
		d.State = event.StateDelivered
	}
	return d, nil
}

func (s *fakeStore) Heartbeat(_ context.Context, id int64, _ int, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lost[id] {
		return fmt.Errorf("heartbeat delivery %d: %w", id, event.ErrLeaseLost)
	}
	s.beats[id]++
	return nil
}

func (s *fakeStore) wait(t *testing.T, n int) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for range n {
		select {
		case <-s.reported:
		case <-timeout:
			t.Fatalf("timed out waiting for %d reports", n)
		}
	}
}

// fakeTransport answers from a function and counts concurrent sends.
type fakeTransport struct {
	fn      func(ctx context.Context, r Request) event.Result
	running atomic.Int32
	peak    atomic.Int32
}

func (f *fakeTransport) Send(ctx context.Context, r Request) event.Result {
	n := f.running.Add(1)
	for {
		p := f.peak.Load()
		if n <= p || f.peak.CompareAndSwap(p, n) {
			break
		}
	}
	defer f.running.Add(-1)
	return f.fn(ctx, r)
}

func start(t *testing.T, store Store, tr Transport, opts Options) (stop func()) {
	t.Helper()
	if opts.PollInterval == 0 {
		opts.PollInterval = 5 * time.Millisecond
	}
	p, err := New(store, tr, quiet, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return")
			}
		})
	}
}

func TestPoolSendsEveryClaimedDeliveryAndReportsResults(t *testing.T) {
	store := newFakeStore(5)
	tr := &fakeTransport{fn: func(_ context.Context, r Request) event.Result {
		switch r.DeliveryID {
		case 2:
			return event.Result{Outcome: event.Retry, Status: 503, Error: "HTTP 503"}
		case 3:
			return event.Result{Outcome: event.Rejected, Status: 410, Error: "HTTP 410"}
		}
		if len(r.Secrets) != 1 || r.Secrets[0] != secret || r.EventType != "t" {
			t.Errorf("request %+v lacks secrets or type", r)
		}
		return event.Result{Outcome: event.Delivered, Status: 200}
	}}
	stop := start(t, store, tr, Options{})
	store.wait(t, 5)
	stop()

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.finished) != 5 || store.finished[2].Outcome != event.Retry || store.finished[3].Outcome != event.Rejected || store.finished[1].Outcome != event.Delivered {
		t.Fatalf("finished = %+v", store.finished)
	}
}

func TestPoolBoundsConcurrencyAndClaimsOnlyFreeSlots(t *testing.T) {
	store := newFakeStore(40)
	gate := make(chan struct{})
	tr := &fakeTransport{fn: func(ctx context.Context, _ Request) event.Result {
		select {
		case <-gate:
		case <-ctx.Done():
		}
		return event.Result{Outcome: event.Delivered, Status: 200}
	}}
	stop := start(t, store, tr, Options{Concurrency: 4})
	time.Sleep(50 * time.Millisecond)
	if tr.running.Load() != 4 {
		t.Fatalf("%d sends in flight, want 4", tr.running.Load())
	}
	close(gate)
	store.wait(t, 40)
	stop()
	if tr.peak.Load() > 4 {
		t.Fatalf("peak concurrency %d, want at most 4", tr.peak.Load())
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, limit := range store.claims {
		if limit < 1 || limit > 4 {
			t.Fatalf("claimed with limit %d, want 1..4 (free slots)", limit)
		}
	}
}

func TestPoolHeartbeatsAndStopsOnLostLease(t *testing.T) {
	store := newFakeStore(2)
	var cancelled atomic.Bool
	tr := &fakeTransport{fn: func(ctx context.Context, r Request) event.Result {
		if r.DeliveryID == 2 {
			store.mu.Lock()
			store.lost[2] = true // another deliverer took it mid-request
			store.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			cancelled.Store(true)
			return event.Result{Outcome: event.Retry, Error: "canceled"}
		case <-time.After(300 * time.Millisecond):
			return event.Result{Outcome: event.Delivered, Status: 200}
		}
	}}
	stop := start(t, store, tr, Options{Concurrency: 2, Lease: 60 * time.Millisecond, Heartbeat: 20 * time.Millisecond})
	store.wait(t, 1) // delivery 1 reports; 2 is lost and never does
	time.Sleep(50 * time.Millisecond)
	stop()

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.beats[1] < 5 {
		t.Fatalf("delivery 1 got %d heartbeats over 300ms at 20ms, want several", store.beats[1])
	}
	if _, reported := store.finished[2]; reported || !cancelled.Load() {
		t.Fatalf("lost delivery: reported=%t cancelled=%t; want its request cancelled and nothing reported", reported, cancelled.Load())
	}
}

func TestPoolDrainsThenCancels(t *testing.T) {
	store := newFakeStore(2)
	tr := &fakeTransport{fn: func(ctx context.Context, r Request) event.Result {
		d := 50 * time.Millisecond
		if r.DeliveryID == 2 {
			d = 5 * time.Second
		}
		select {
		case <-time.After(d):
			return event.Result{Outcome: event.Delivered, Status: 200}
		case <-ctx.Done():
			return event.Result{Outcome: event.ClassifyError(ctx.Err()), Error: event.Describe(0, nil, ctx.Err())}
		}
	}}
	stop := start(t, store, tr, Options{Concurrency: 2, DrainTimeout: 200 * time.Millisecond})
	time.Sleep(20 * time.Millisecond)
	begin := time.Now()
	stop()
	took := time.Since(begin)
	if took < 150*time.Millisecond || took > 2*time.Second {
		t.Fatalf("shutdown took %v, want about the drain timeout", took)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.finished[1].Outcome != event.Delivered {
		t.Fatalf("the quick request should finish during the drain: %+v", store.finished[1])
	}
	if store.finished[2].Outcome != event.Retry {
		t.Fatalf("the slow request should be cancelled and retried: %+v", store.finished[2])
	}
}

func TestNewValidates(t *testing.T) {
	tr := &fakeTransport{}
	for name, opts := range map[string]Options{
		"concurrency":        {Concurrency: MaxConcurrency + 1},
		"heartbeat >= lease": {Lease: time.Second, Heartbeat: time.Second},
		"negative poll":      {PollInterval: -1},
	} {
		if _, err := New(newFakeStore(0), tr, nil, opts); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := New(nil, tr, nil, Options{}); err == nil || !errors.Is(err, err) {
		t.Fatal("nil store accepted")
	}
	p, err := New(newFakeStore(0), tr, nil, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if p.opts.Concurrency != 16 || p.opts.Lease != 60*time.Second || p.opts.Heartbeat != 20*time.Second {
		t.Fatalf("defaults = %+v", p.opts)
	}
}

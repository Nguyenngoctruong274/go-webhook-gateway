package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/testdb"
)

func benchStore(b *testing.B, endpoints int) *Store {
	b.Helper()
	s, err := NewStore(testdb.New(b, Migrate))
	if err != nil {
		b.Fatal(err)
	}
	for i := range endpoints {
		if _, _, err := s.CreateEndpoint(context.Background(), event.EndpointSpec{URL: fmt.Sprintf("https://r%d.example/hook", i)}); err != nil {
			b.Fatal(err)
		}
	}
	return s
}

// BenchmarkPublish measures accepting one event and fanning it out, in
// one transaction, to 1 and to 10 subscribed endpoints.
func BenchmarkPublish(b *testing.B) {
	for _, n := range []int{1, 10} {
		b.Run(fmt.Sprintf("endpoints=%d", n), func(b *testing.B) {
			s := benchStore(b, n)
			payload := []byte(`{"order":42,"amount":"10.00","currency":"VND"}`)
			b.ResetTimer()
			for b.Loop() {
				if _, err := s.Publish(context.Background(), event.Spec{Type: "order.paid", Payload: payload}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N*n)/b.Elapsed().Seconds(), "deliveries/s")
		})
	}
}

// BenchmarkClaimFinish measures the store's side of delivering: claim a
// batch under SKIP LOCKED, then record a success for each, from several
// deliverers at once. The HTTP request is left out: this is the ceiling
// the database puts on throughput.
func BenchmarkClaimFinish(b *testing.B) {
	const (
		deliverers = 4
		batch      = 16
	)
	s := benchStore(b, 4)
	ctx := context.Background()
	// Enough events for every iteration: b.N deliveries, 4 per event.
	seed := func(n int) {
		if _, err := s.db.ExecContext(ctx, `
			WITH ev AS (INSERT INTO events (type, payload) SELECT 'bench', '{}' FROM generate_series(1, $1) RETURNING id)
			INSERT INTO deliveries (event_id, endpoint_id, max_attempts)
			SELECT ev.id, p.id, 8 FROM ev CROSS JOIN endpoints p`, (n+3)/4); err != nil {
			b.Fatal(err)
		}
	}
	seed(b.N + batch*deliverers)
	b.ResetTimer()

	var (
		mu   sync.Mutex
		left = b.N
		wg   sync.WaitGroup
	)
	take := func(n int) int {
		mu.Lock()
		defer mu.Unlock()
		n = min(n, left)
		left -= n
		return n
	}
	for range deliverers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n := take(batch)
				if n == 0 {
					return
				}
				got, err := s.Claim(ctx, n, time.Minute)
				if err != nil {
					b.Error(err)
					return
				}
				for _, c := range got {
					if _, err := s.Finish(ctx, c.Delivery.ID, c.Delivery.Attempt, event.Result{Outcome: event.Delivered, Status: 200}); err != nil {
						b.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "deliveries/s")
}

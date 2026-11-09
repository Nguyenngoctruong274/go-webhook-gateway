package postgres

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/breaker"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
)

func TestSnapshotAndQueueMetrics(t *testing.T) {
	s := newStore(t, WithBreaker(breaker.Policy{FailureThreshold: 1, OpenFor: time.Hour, MaxOpenFor: time.Hour, ProbeTimeout: time.Hour}))
	ctx := context.Background()
	a, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://a.example/hook"})
	b, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://b.example/hook"})
	for range 3 {
		mustPublish(t, s, event.Spec{Type: "x"}) // 6 deliveries
	}
	// One delivery is attempted and fails: with a threshold of 1 its
	// endpoint's breaker opens. The other endpoint is paused.
	c := claimOne(t, s, time.Minute)
	if _, err := s.Finish(ctx, c.Delivery.ID, c.Delivery.Attempt, event.Result{Outcome: event.Retry, Status: 503}); err != nil {
		t.Fatal(err)
	}
	opened, other := c.Delivery.EndpointID, b.ID
	if opened == b.ID {
		other = a.ID
	}
	if _, err := s.SetEndpointEnabled(ctx, other, false); err != nil {
		t.Fatal(err)
	}
	// Age the deliveries: on the healthy endpoint one has been due for
	// 90s and one is scheduled an hour out; on the open one, one has been
	// due for 200s, which must not count as the gateway falling behind.
	for _, q := range []string{
		`UPDATE deliveries SET next_attempt_at = now() - interval '90 seconds'
		 WHERE id = (SELECT min(id) FROM deliveries WHERE endpoint_id = $1)`,
		`UPDATE deliveries SET next_attempt_at = now() + interval '1 hour'
		 WHERE id = (SELECT max(id) FROM deliveries WHERE endpoint_id = $1)`,
	} {
		if _, err := s.db.ExecContext(ctx, q, other); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE deliveries SET next_attempt_at = now() - interval '200 seconds'
		WHERE id = (SELECT max(id) FROM deliveries WHERE endpoint_id = $1)`, opened); err != nil {
		t.Fatal(err)
	}

	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Healthy endpoint: 2 due, 1 scheduled. Open endpoint: the failed one
	// waits for its backoff (scheduled or, with a tiny backoff, held) and
	// the other two are held.
	if snap.InFlight != 0 || snap.DueNow != 2 || snap.DueNow+snap.HeldBack+snap.Scheduled != 6 || snap.HeldBack < 2 {
		t.Fatalf("snapshot counts = due %d, held %d, scheduled %d, in flight %d", snap.DueNow, snap.HeldBack, snap.Scheduled, snap.InFlight)
	}
	if snap.OldestDue < 85*time.Second || snap.OldestDue > 120*time.Second {
		t.Fatalf("oldest due = %v, want about 90s (the 200s one is behind an open breaker)", snap.OldestDue)
	}
	if len(snap.Endpoints) != 2 {
		t.Fatalf("endpoints = %+v", snap.Endpoints)
	}

	reg := metrics.NewRegistry()
	s.RegisterQueueMetrics(reg, nil, time.Minute)
	var out strings.Builder
	reg.Write(&out)
	got := out.String()
	for _, want := range []string{
		`webhook_breakers{status="open"} 1`,
		`webhook_breakers{status="closed"} 1`,
		`webhook_breakers{status="half_open"} 0`,
		`webhook_endpoint_breaker{endpoint="` + itoa(opened) + `",status="open"} 1`,
		`webhook_endpoint_breaker{endpoint="` + itoa(opened) + `",status="closed"} 0`,
		`webhook_endpoint_breaker{endpoint="` + itoa(other) + `",status="closed"} 1`,
		`webhook_endpoints{enabled="false"} 1`,
		`webhook_endpoints{enabled="true"} 1`,
		`webhook_backlog{kind="in_flight"} 0`,
		`webhook_backlog{kind="due"} 2`,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("metrics lack %q", want)
		}
	}
	if t.Failed() {
		t.Log(got)
	}

	// Cached: a change within cacheFor is not visible; the read happens
	// once per cacheFor however many families ask.
	claimOne(t, s, time.Minute)
	out.Reset()
	reg.Write(&out)
	if !strings.Contains(out.String(), `webhook_backlog{kind="in_flight"} 0`+"\n") {
		t.Fatal("snapshot was read again within cacheFor")
	}

	// A broken database keeps the last values and counts the failure.
	_ = s.db.Close()
	reg2 := metrics.NewRegistry()
	s.RegisterQueueMetrics(reg2, slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	out.Reset()
	reg2.Write(&out)
	if !strings.Contains(out.String(), "webhook_metrics_read_errors_total ") || strings.Contains(out.String(), "webhook_metrics_read_errors_total 0\n") {
		t.Fatalf("read error not counted:\n%s", out.String())
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

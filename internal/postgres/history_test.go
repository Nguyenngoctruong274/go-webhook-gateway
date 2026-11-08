package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

func TestHistoryPagesNewestFirstByID(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ep, _ := mustEndpoint(t, s, event.EndpointSpec{})
	other, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://other.example/hook"})
	for i := range 7 {
		mustPublish(t, s, event.Spec{Type: "x", Payload: []byte(`{"n":` + string(rune('0'+i)) + `}`)})
	}
	// The first two events (to both endpoints) end up delivered, the
	// rest pending.
	got, err := s.Claim(ctx, 4, time.Minute)
	if err != nil || len(got) != 4 {
		t.Fatal(err)
	}
	for _, c := range got {
		if _, err := s.Finish(ctx, c.Delivery.ID, 1, event.Result{Outcome: event.Delivered, Status: 200}); err != nil {
			t.Fatal(err)
		}
	}

	page, more, err := s.History(ctx, HistoryFilter{EndpointID: ep.ID, Limit: 3})
	if err != nil || len(page) != 3 || !more {
		t.Fatalf("page 1 = %d deliveries, more=%t, %v; want 3, true", len(page), more, err)
	}
	for i := 1; i < len(page); i++ {
		if page[i].ID >= page[i-1].ID {
			t.Fatalf("page not newest first: %d then %d", page[i-1].ID, page[i].ID)
		}
	}
	for _, d := range page {
		if d.EndpointID != ep.ID {
			t.Fatalf("delivery %d belongs to endpoint %d", d.ID, d.EndpointID)
		}
	}
	page2, more, err := s.History(ctx, HistoryFilter{EndpointID: ep.ID, Limit: 3, Before: page[2].ID})
	if err != nil || len(page2) != 3 || !more || page2[0].ID >= page[2].ID {
		t.Fatalf("page 2 = %d, more=%t, %v", len(page2), more, err)
	}
	page3, more, err := s.History(ctx, HistoryFilter{EndpointID: ep.ID, Limit: 3, Before: page2[2].ID})
	if err != nil || len(page3) != 1 || more {
		t.Fatalf("page 3 = %d, more=%t, %v; want the last 1, false", len(page3), more, err)
	}

	delivered, _, err := s.History(ctx, HistoryFilter{EndpointID: ep.ID, State: event.StateDelivered})
	if err != nil || len(delivered) != 2 {
		t.Fatalf("delivered filter = %d, %v; want 2", len(delivered), err)
	}
	if _, _, err := s.History(ctx, HistoryFilter{EndpointID: ep.ID, State: "lost"}); err == nil {
		t.Fatal("unknown state accepted")
	}
	none, more, err := s.History(ctx, HistoryFilter{EndpointID: other.ID, State: event.StateFailed})
	if err != nil || len(none) != 0 || more {
		t.Fatalf("other endpoint failed = %d, %v", len(none), err)
	}

	stats, err := s.Stats(ctx, ep.ID)
	if err != nil || stats[event.StateDelivered] != 2 || stats[event.StatePending] != 5 {
		t.Fatalf("stats = %v, %v", stats, err)
	}
}

func TestReplayAndCancel(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, s, event.EndpointSpec{})
	mustPublish(t, s, event.Spec{Type: "x"})
	mustPublish(t, s, event.Spec{Type: "y"})

	// Pending: cancel works, replay does not.
	pending, _, err := s.History(ctx, HistoryFilter{EndpointID: 1})
	if err != nil || len(pending) != 2 {
		t.Fatal(err)
	}
	if _, err := s.Replay(ctx, pending[0].ID); err == nil {
		t.Fatal("replayed a pending delivery")
	} else {
		assertIs(t, err, event.ErrInvalidTransition)
	}
	cancelled, err := s.Cancel(ctx, pending[0].ID)
	if err != nil || cancelled.State != event.StateCancelled {
		t.Fatalf("cancel = %+v, %v", cancelled, err)
	}
	if got, err := s.Claim(ctx, 10, time.Minute); err != nil || len(got) != 1 || got[0].Delivery.ID == cancelled.ID {
		t.Fatalf("claim after cancel = %v, %v; want only the other delivery", got, err)
	}

	// In flight: neither.
	inFlight := pending[1].ID
	if _, err := s.Cancel(ctx, inFlight); err == nil {
		t.Fatal("cancelled an in-flight delivery")
	}
	if _, err := s.Replay(ctx, inFlight); err == nil {
		t.Fatal("replayed an in-flight delivery")
	}
	if _, err := s.Finish(ctx, inFlight, 1, event.Result{Outcome: event.Rejected, Status: 410, Error: "HTTP 410"}); err != nil {
		t.Fatal(err)
	}

	// Finished: replay queues it again with fresh attempts, keeping the
	// last result visible until the next attempt.
	before := dbNow(t, s)
	replayed, err := s.Replay(ctx, inFlight)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.State != event.StatePending || replayed.Attempt != 0 || replayed.LastStatus != 410 {
		t.Fatalf("replayed = %+v", replayed)
	}
	assertNear(t, "next_attempt_at", replayed.NextAttemptAt, before, 5*time.Second)
	got, err := s.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 1 || got[0].Delivery.ID != inFlight || got[0].Delivery.Attempt != 1 {
		t.Fatalf("claim after replay = %v, %v", got, err)
	}
	// Cancelled ones can be replayed too.
	if d, err := s.Replay(ctx, cancelled.ID); err != nil || d.State != event.StatePending {
		t.Fatalf("replay cancelled = %+v, %v", d, err)
	}
	_, err = s.Replay(ctx, 404)
	assertIs(t, err, event.ErrNotFound)
}

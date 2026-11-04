package event

import (
	"errors"
	"testing"
	"time"
)

var (
	t0       = time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	noJitter = Backoff{Base: 5 * time.Second, Max: time.Minute}
)

func pending(attempt, maxAttempts int) Delivery {
	return Delivery{ID: 7, EventID: 1, EndpointID: 2, State: StatePending, Attempt: attempt, MaxAttempts: maxAttempts, NextAttemptAt: t0}
}

func inFlight(attempt, maxAttempts int) Delivery {
	d := pending(attempt-1, maxAttempts)
	if _, err := d.Claim(t0, time.Minute); err != nil {
		panic(err)
	}
	return d
}

func TestDeliveryLifecycleHappyPath(t *testing.T) {
	d := pending(0, 3)
	token, err := d.Claim(t0, 30*time.Second)
	if err != nil || token != 1 || d.State != StateInFlight || !d.LeaseUntil.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("after Claim: %+v, token %d, err %v", d, token, err)
	}
	if err := d.Heartbeat(1, t0.Add(10*time.Second), 30*time.Second); err != nil || !d.LeaseUntil.Equal(t0.Add(40*time.Second)) {
		t.Fatalf("after Heartbeat: %+v, %v", d, err)
	}
	if err := d.Finish(1, t0.Add(12*time.Second), Result{Outcome: Delivered, Status: 200}, noJitter, nil); err != nil {
		t.Fatal(err)
	}
	if d.State != StateDelivered || d.LastStatus != 200 || d.LastError != "" || !d.DeliveredAt.Equal(t0.Add(12*time.Second)) || !d.LeaseUntil.IsZero() {
		t.Fatalf("after Finish: %+v", d)
	}
}

func TestDeliveryRetriesThenFails(t *testing.T) {
	d := pending(0, 3)
	for attempt := 1; attempt <= 3; attempt++ {
		now := t0.Add(time.Duration(attempt) * time.Hour)
		d.NextAttemptAt = now
		if _, err := d.Claim(now, time.Minute); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if err := d.Finish(attempt, now, Result{Outcome: Retry, Status: 503, Error: "HTTP 503"}, noJitter, nil); err != nil {
			t.Fatal(err)
		}
		if attempt < 3 {
			want := now.Add(noJitter.Ceiling(attempt)) // 5s, 10s
			if d.State != StatePending || !d.NextAttemptAt.Equal(want) || d.LastStatus != 503 {
				t.Fatalf("after attempt %d: %+v, want pending at %s", attempt, d, want)
			}
		}
	}
	if d.State != StateFailed || d.Attempt != 3 || d.LastError != "HTTP 503" {
		t.Fatalf("after last attempt: %+v", d)
	}
	if _, err := d.Claim(t0.Add(24*time.Hour), time.Minute); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("claiming a failed delivery: %v", err)
	}
}

func TestDeliveryFinalOutcomes(t *testing.T) {
	for _, tt := range []struct {
		outcome Outcome
		want    State
	}{{Delivered, StateDelivered}, {Rejected, StateRejected}, {Blocked, StateBlocked}} {
		d := inFlight(1, 5)
		if err := d.Finish(1, t0, Result{Outcome: tt.outcome, Status: 400, Error: "x"}, noJitter, nil); err != nil {
			t.Fatal(err)
		}
		if d.State != tt.want || d.Attempt != 1 {
			t.Errorf("%v: state %s attempt %d, want %s", tt.outcome, d.State, d.Attempt, tt.want)
		}
	}
	d := inFlight(1, 5)
	if err := d.Finish(1, t0, Result{Outcome: Outcome(42)}, noJitter, nil); err == nil {
		t.Fatal("unknown outcome accepted")
	}
}

func TestDeliveryInvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		d    Delivery
		op   func(d *Delivery) error
		want error
	}{
		{"claim in-flight", inFlight(1, 3), func(d *Delivery) error { _, err := d.Claim(t0, time.Minute); return err }, ErrInvalidTransition},
		{"claim before due", Delivery{State: StatePending, MaxAttempts: 3, NextAttemptAt: t0.Add(time.Second)},
			func(d *Delivery) error { _, err := d.Claim(t0, time.Minute); return err }, ErrNotDue},
		{"claim with no attempts left", Delivery{State: StatePending, Attempt: 3, MaxAttempts: 3, NextAttemptAt: t0},
			func(d *Delivery) error { _, err := d.Claim(t0, time.Minute); return err }, ErrInvalidTransition},
		{"claim zero lease", pending(0, 3), func(d *Delivery) error { _, err := d.Claim(t0, 0); return err }, nil},
		{"finish wrong attempt", inFlight(2, 3), func(d *Delivery) error {
			return d.Finish(1, t0, Result{Outcome: Delivered}, noJitter, nil)
		}, ErrLeaseLost},
		{"finish pending", pending(1, 3), func(d *Delivery) error {
			return d.Finish(1, t0, Result{Outcome: Delivered}, noJitter, nil)
		}, ErrLeaseLost},
		{"heartbeat wrong attempt", inFlight(2, 3), func(d *Delivery) error { return d.Heartbeat(1, t0, time.Minute) }, ErrLeaseLost},
		{"heartbeat zero lease", inFlight(1, 3), func(d *Delivery) error { return d.Heartbeat(1, t0, 0) }, nil},
		{"expire pending", pending(1, 3), func(d *Delivery) error { return d.Expire(t0, noJitter, nil) }, ErrInvalidTransition},
		{"expire active lease", inFlight(1, 3), func(d *Delivery) error { return d.Expire(t0.Add(30*time.Second), noJitter, nil) }, ErrLeaseActive},
		{"cancel in-flight", inFlight(1, 3), func(d *Delivery) error { return d.Cancel() }, ErrInvalidTransition},
		{"replay pending", pending(0, 3), func(d *Delivery) error { return d.Replay(t0) }, ErrInvalidTransition},
		{"replay in-flight", inFlight(1, 3), func(d *Delivery) error { return d.Replay(t0) }, ErrInvalidTransition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.d
			err := tt.op(&tt.d)
			if err == nil {
				t.Fatal("accepted")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if tt.d != before {
				t.Fatalf("a refused transition changed the delivery: %+v -> %+v", before, tt.d)
			}
		})
	}
}

func TestDeliveryExpireCancelReplay(t *testing.T) {
	d := inFlight(1, 2)
	if err := d.Expire(t0.Add(time.Minute), noJitter, nil); err != nil {
		t.Fatal(err)
	}
	if d.State != StatePending || d.LastError == "" || !d.NextAttemptAt.Equal(t0.Add(time.Minute+5*time.Second)) || !d.LeaseUntil.IsZero() {
		t.Fatalf("after Expire: %+v", d)
	}
	if err := d.Cancel(); err != nil || d.State != StateCancelled {
		t.Fatalf("after Cancel: %+v, %v", d, err)
	}
	if err := d.Replay(t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if d.State != StatePending || d.Attempt != 0 || !d.NextAttemptAt.Equal(t0.Add(time.Hour)) || d.LastError == "" {
		t.Fatalf("after Replay: %+v (last error must stay visible)", d)
	}

	// The last attempt expiring fails the delivery.
	d = inFlight(2, 2)
	if err := d.Expire(t0.Add(time.Hour), noJitter, nil); err != nil || d.State != StateFailed {
		t.Fatalf("expire last attempt: %+v, %v", d, err)
	}
	// A delivered one can be replayed too (the customer lost the data).
	d = inFlight(1, 2)
	_ = d.Finish(1, t0, Result{Outcome: Delivered, Status: 200}, noJitter, nil)
	if err := d.Replay(t0); err != nil || d.State != StatePending || !d.DeliveredAt.IsZero() {
		t.Fatalf("replay delivered: %+v, %v", d, err)
	}
}

func TestParseState(t *testing.T) {
	for _, s := range []State{StatePending, StateInFlight, StateDelivered, StateFailed, StateRejected, StateBlocked, StateCancelled} {
		if got, err := ParseState(string(s)); err != nil || got != s {
			t.Errorf("ParseState(%q) = %q, %v", s, got, err)
		}
	}
	if _, err := ParseState("done"); err == nil {
		t.Fatal("unknown state accepted")
	}
}

// FuzzDeliveryInvariants drives a delivery through random operations and
// checks what must always hold, whatever the order:
//
//  1. the attempt counter never exceeds MaxAttempts and never decreases
//     except through Replay;
//  2. a lease exists exactly while in flight;
//  3. a pending delivery always has attempts left;
//  4. a refused operation changes nothing;
//  5. every delivery with a full set of transient failures ends failed.
func FuzzDeliveryInvariants(f *testing.F) {
	f.Add(uint8(3), []byte{0, 2, 0, 2, 0, 2, 5})
	f.Add(uint8(1), []byte{0, 1, 6, 0, 3})
	f.Add(uint8(5), []byte{0, 4, 0, 7, 8, 0, 2})
	f.Fuzz(func(t *testing.T, maxAttempts uint8, ops []byte) {
		if maxAttempts == 0 || int(maxAttempts) > MaxAttempts {
			t.Skip()
		}
		d := Delivery{ID: 1, State: StatePending, MaxAttempts: int(maxAttempts), NextAttemptAt: t0}
		now := t0
		token := 0
		for i, op := range ops {
			now = now.Add(time.Duration(op%7) * 10 * time.Second)
			before := d
			var err error
			switch op % 9 {
			case 0:
				var tok int
				tok, err = d.Claim(now, time.Minute)
				if err == nil {
					token = tok
				}
			case 1:
				err = d.Finish(token, now, Result{Outcome: Delivered, Status: 200}, noJitter, nil)
			case 2:
				err = d.Finish(token, now, Result{Outcome: Retry, Status: 503, Error: "HTTP 503"}, noJitter, nil)
			case 3:
				err = d.Finish(token, now, Result{Outcome: Rejected, Status: 404}, noJitter, nil)
			case 4:
				err = d.Finish(token+1, now, Result{Outcome: Delivered}, noJitter, nil) // stale token: always refused
				if err == nil {
					t.Fatalf("op %d: a stale token was accepted", i)
				}
			case 5:
				err = d.Expire(now, noJitter, nil)
			case 6:
				err = d.Cancel()
			case 7:
				err = d.Replay(now)
			case 8:
				err = d.Heartbeat(token, now, time.Minute)
			}
			if err != nil && d != before {
				t.Fatalf("op %d (%d) failed with %v but changed %+v -> %+v", i, op%9, err, before, d)
			}
			if d.Attempt < 0 || d.Attempt > d.MaxAttempts {
				t.Fatalf("op %d: attempt %d outside [0, %d]", i, d.Attempt, d.MaxAttempts)
			}
			if err == nil && op%9 != 7 && d.Attempt < before.Attempt {
				t.Fatalf("op %d: attempt went down without a replay: %d -> %d", i, before.Attempt, d.Attempt)
			}
			if (d.State == StateInFlight) != !d.LeaseUntil.IsZero() {
				t.Fatalf("op %d: state %s with lease %v", i, d.State, d.LeaseUntil)
			}
			if d.State == StatePending && d.Attempt >= d.MaxAttempts {
				t.Fatalf("op %d: pending with no attempts left: %+v", i, d)
			}
			if d.State == StateDelivered && (d.LastError != "" || d.DeliveredAt.IsZero()) {
				t.Fatalf("op %d: delivered with error %q / time %v", i, d.LastError, d.DeliveredAt)
			}
		}
	})
}

package breaker

import (
	"testing"
	"time"
)

var (
	t0 = time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC)
	p  = Policy{FailureThreshold: 3, OpenFor: 10 * time.Second, MaxOpenFor: 40 * time.Second, ProbeTimeout: 5 * time.Second}
)

func TestBreakerOpensAfterConsecutiveTransientFailures(t *testing.T) {
	s := State{Status: Closed}
	for i := 0; i < 2; i++ {
		s.Record(false, true, t0, p)
		if s.Status != Closed || s.Failures != i+1 {
			t.Fatalf("after %d failures: %+v", i+1, s)
		}
	}
	// A success in between resets the count: failures must be consecutive.
	s.Record(true, false, t0, p)
	if s.Failures != 0 {
		t.Fatalf("success did not reset: %+v", s)
	}
	for i := 0; i < 3; i++ {
		s.Record(false, true, t0, p)
	}
	if s.Status != Open || !s.OpenedUntil.Equal(t0.Add(10*time.Second)) || s.Opens != 1 || s.Failures != 0 {
		t.Fatalf("after the threshold: %+v", s)
	}
	for _, at := range []time.Time{t0, t0.Add(9 * time.Second)} {
		if d := s.Admit(at, p); d != Skip {
			t.Fatalf("Admit while open at %s = %v, want skip", at, d)
		}
	}
}

func TestRejectionsDoNotCountWhileClosed(t *testing.T) {
	s := State{Status: Closed}
	for i := 0; i < 10; i++ {
		s.Record(false, false, t0, p) // 4xx: the receiver answered
	}
	if s.Status != Closed || s.Failures != 0 {
		t.Fatalf("4xx moved the breaker: %+v", s)
	}
}

func TestHalfOpenProbeDecidesTheOutcome(t *testing.T) {
	opened := State{Status: Closed}
	for i := 0; i < 3; i++ {
		opened.Record(false, true, t0, p)
	}

	// After the open period one probe goes through; a second caller is told to wait.
	s := opened
	at := t0.Add(10 * time.Second)
	if d := s.Admit(at, p); d != Probe || s.Status != HalfOpen || !s.ProbeUntil.Equal(at.Add(5*time.Second)) {
		t.Fatalf("first Admit after the period = %v, %+v", d, s)
	}
	if d := s.Admit(at.Add(time.Second), p); d != Skip {
		t.Fatalf("second Admit during the probe = %v, want skip", d)
	}
	// Probe succeeds: closed, counters cleared.
	s.Record(true, false, at.Add(2*time.Second), p)
	if s != (State{Status: Closed}) {
		t.Fatalf("after a good probe: %+v", s)
	}

	// Probe fails: open again, for twice as long.
	s = opened
	s.Admit(at, p)
	s.Record(false, true, at.Add(time.Second), p)
	if s.Status != Open || s.Opens != 2 || !s.OpenedUntil.Equal(at.Add(time.Second+20*time.Second)) {
		t.Fatalf("after a failed probe: %+v", s)
	}
	// Third and fourth opens: 40s (the cap), then 40s again.
	for want := range []time.Duration{40 * time.Second, 40 * time.Second} {
		_ = want
		s.Admit(s.OpenedUntil, p)
		before := s.OpenedUntil
		s.Record(false, true, before, p)
		if got := s.OpenedUntil.Sub(before); got != 40*time.Second {
			t.Fatalf("open period after %d opens = %v, want the 40s cap", s.Opens, got)
		}
	}

	// A 4xx on the probe proves the receiver is up: closed.
	s = opened
	s.Admit(at, p)
	s.Record(false, false, at, p)
	if s.Status != Closed {
		t.Fatalf("after a 4xx probe: %+v", s)
	}
}

func TestProbeTimeoutAllowsAnotherProbe(t *testing.T) {
	s := State{Status: Open, OpenedUntil: t0, Opens: 1}
	s.Admit(t0, p) // probe 1, deliverer dies
	if d := s.Admit(t0.Add(4*time.Second), p); d != Skip {
		t.Fatalf("before the probe timeout = %v", d)
	}
	if d := s.Admit(t0.Add(5*time.Second), p); d != Probe || !s.ProbeUntil.Equal(t0.Add(10*time.Second)) {
		t.Fatalf("after the probe timeout = %v, %+v", d, s)
	}
}

func TestLateReportsWhileOpenChangeNothing(t *testing.T) {
	s := State{Status: Open, OpenedUntil: t0.Add(time.Minute), Opens: 1}
	before := s
	s.Record(false, true, t0, p) // an attempt that started before the breaker opened
	if s != before {
		t.Fatalf("late failure moved an open breaker: %+v", s)
	}
	s.Record(true, false, t0, p) // a late success closes it: the receiver answered
	if s.Status != Closed {
		t.Fatalf("late success did not close: %+v", s)
	}
}

func TestResetAndParse(t *testing.T) {
	s := State{Status: Open, Opens: 3, OpenedUntil: t0}
	s.Reset()
	if s != (State{Status: Closed}) {
		t.Fatalf("Reset: %+v", s)
	}
	for _, st := range []Status{Closed, Open, HalfOpen} {
		if got, err := ParseStatus(string(st)); err != nil || got != st {
			t.Errorf("ParseStatus(%q) = %q, %v", st, got, err)
		}
	}
	if _, err := ParseStatus("tripped"); err == nil {
		t.Fatal("unknown status accepted")
	}
	if Send.String() != "send" || Probe.String() != "probe" || Skip.String() != "skip" || Decision(7).String() != "decision(7)" {
		t.Fatal("Decision.String")
	}
}

func TestPolicyValidate(t *testing.T) {
	if err := DefaultPolicy().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := Policy{FailureThreshold: 0, OpenFor: 0, MaxOpenFor: -1, ProbeTimeout: 0}
	if err := bad.Validate(); err == nil {
		t.Fatal("accepted")
	}
}

// FuzzBreakerInvariants feeds random admits and outcomes and checks:
// never two probes in flight at once; an open breaker never admits
// before its period; the open period never exceeds MaxOpenFor; a
// success always closes.
func FuzzBreakerInvariants(f *testing.F) {
	f.Add([]byte{1, 1, 1, 0, 2, 0, 0, 1, 3})
	f.Fuzz(func(t *testing.T, ops []byte) {
		s := State{Status: Closed}
		now := t0
		probeOpen := false
		for i, op := range ops {
			now = now.Add(time.Duration(op%8) * 2 * time.Second)
			switch op % 4 {
			case 0:
				before := s
				d := s.Admit(now, p)
				if d == Probe {
					if probeOpen && now.Before(before.ProbeUntil) {
						t.Fatalf("op %d: second probe admitted while one is in flight", i)
					}
					probeOpen = true
				}
				if before.Status == Open && now.Before(before.OpenedUntil) && d != Skip {
					t.Fatalf("op %d: admitted %v during the open period", i, d)
				}
			case 1:
				s.Record(true, false, now, p)
				probeOpen = false
				if s.Status != Closed {
					t.Fatalf("op %d: success left the breaker %s", i, s.Status)
				}
			case 2:
				s.Record(false, true, now, p)
				if s.Status != HalfOpen {
					probeOpen = false
				}
			case 3:
				s.Record(false, false, now, p)
				if s.Status != HalfOpen {
					probeOpen = false
				}
			}
			if s.Status == Open && s.OpenedUntil.Sub(now) > p.MaxOpenFor {
				t.Fatalf("op %d: open for %v, cap is %v", i, s.OpenedUntil.Sub(now), p.MaxOpenFor)
			}
			if s.Failures < 0 || s.Failures >= p.FailureThreshold {
				t.Fatalf("op %d: failures %d", i, s.Failures)
			}
		}
	})
}

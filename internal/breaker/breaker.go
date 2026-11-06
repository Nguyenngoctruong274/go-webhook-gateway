// Package breaker is the per-endpoint circuit breaker: when a receiver
// keeps failing, the gateway stops sending to it for a while instead of
// having every pending delivery retry against it, then lets a single
// probe through to see whether it recovered. The state is a value with
// no I/O; the store keeps one per endpoint in PostgreSQL, so every
// gateway instance sees the same breaker.
package breaker

import (
	"errors"
	"fmt"
	"time"
)

// Status is the breaker's position.
type Status string

// The three positions. Closed: deliveries flow. Open: nothing is sent
// until OpenedUntil. HalfOpen: one probe delivery is in flight; its
// outcome decides between Closed and Open again.
const (
	Closed   Status = "closed"
	Open     Status = "open"
	HalfOpen Status = "half_open"
)

// ParseStatus converts a stored status back to a Status.
func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case Closed, Open, HalfOpen:
		return st, nil
	}
	return "", fmt.Errorf("unknown breaker status %q", s)
}

// Policy decides when to open and for how long.
type Policy struct {
	// FailureThreshold consecutive transient failures open the breaker.
	FailureThreshold int
	// OpenFor is how long the breaker stays open the first time. Each
	// time a probe fails the next open period doubles, up to MaxOpenFor,
	// so a receiver that is down for hours is probed less and less often.
	OpenFor    time.Duration
	MaxOpenFor time.Duration
	// ProbeTimeout bounds a half-open probe: if its outcome has not
	// arrived by then (the deliverer died), another probe may be sent.
	ProbeTimeout time.Duration
}

// DefaultPolicy opens after 5 consecutive failures for 30s, doubling to
// at most 15 minutes, and gives a probe 2 minutes to report.
func DefaultPolicy() Policy {
	return Policy{FailureThreshold: 5, OpenFor: 30 * time.Second, MaxOpenFor: 15 * time.Minute, ProbeTimeout: 2 * time.Minute}
}

// Validate reports every invalid field at once.
func (p Policy) Validate() error {
	var errs []error
	if p.FailureThreshold < 1 {
		errs = append(errs, errors.New("breaker failure threshold must be at least 1"))
	}
	if p.OpenFor <= 0 {
		errs = append(errs, errors.New("breaker open period must be positive"))
	}
	if p.MaxOpenFor < p.OpenFor {
		errs = append(errs, errors.New("breaker max open period must be at least the open period"))
	}
	if p.ProbeTimeout <= 0 {
		errs = append(errs, errors.New("breaker probe timeout must be positive"))
	}
	return errors.Join(errs...)
}

// State is one endpoint's breaker.
type State struct {
	Status Status
	// Failures counts consecutive transient failures while closed.
	Failures int
	// Opens counts how many times the breaker opened without a success
	// in between; it drives the doubling of the open period.
	Opens int
	// OpenedUntil is when an open breaker may let a probe through.
	OpenedUntil time.Time
	// ProbeUntil is when a half-open breaker gives up on its probe.
	ProbeUntil time.Time
}

// Decision is what Admit says about sending now.
type Decision int

// The decisions. Send: go ahead. Probe: go ahead, and this delivery is
// the one probe allowed; the breaker is now half-open. Skip: do not
// send; leave the delivery for later.
const (
	Send Decision = iota
	Probe
	Skip
)

func (d Decision) String() string {
	switch d {
	case Send:
		return "send"
	case Probe:
		return "probe"
	case Skip:
		return "skip"
	}
	return fmt.Sprintf("decision(%d)", int(d))
}

// Admit asks whether a delivery to this endpoint may be sent at time now
// and moves the breaker accordingly. An open breaker whose period has
// elapsed, or a half-open one whose probe timed out, becomes half-open
// with a fresh probe deadline and answers Probe.
func (s *State) Admit(now time.Time, p Policy) Decision {
	switch s.Status {
	case Closed:
		return Send
	case Open:
		if now.Before(s.OpenedUntil) {
			return Skip
		}
	case HalfOpen:
		if now.Before(s.ProbeUntil) {
			return Skip // a probe is in flight; wait for its verdict
		}
	}
	s.Status = HalfOpen
	s.ProbeUntil = now.Add(p.ProbeTimeout)
	s.OpenedUntil = time.Time{}
	return Probe
}

// Record feeds the outcome of an attempt. transient says whether the
// failure is the kind that means the receiver is unwell (5xx, timeouts,
// connection errors) rather than a final answer about one request (4xx),
// which does not move the breaker. ok means the request was delivered.
func (s *State) Record(ok, transient bool, now time.Time, p Policy) {
	switch {
	case ok:
		*s = State{Status: Closed}
	case !transient:
		// A 4xx while half-open still proves the receiver answers, so
		// close; while closed it is not evidence either way.
		if s.Status == HalfOpen {
			*s = State{Status: Closed}
		}
	case s.Status == HalfOpen:
		s.open(now, p)
	case s.Status == Closed:
		s.Failures++
		if s.Failures >= p.FailureThreshold {
			s.open(now, p)
		}
	}
	// Open and transient: a late report from before the breaker opened;
	// nothing changes.
}

func (s *State) open(now time.Time, p Policy) {
	d := p.OpenFor
	for i := 0; i < s.Opens && d < p.MaxOpenFor; i++ {
		d *= 2
	}
	d = min(d, p.MaxOpenFor)
	s.Status = Open
	s.Opens++
	s.Failures = 0
	s.OpenedUntil = now.Add(d)
	s.ProbeUntil = time.Time{}
}

// Reset closes the breaker, as an operator does after fixing the receiver.
func (s *State) Reset() { *s = State{Status: Closed} }

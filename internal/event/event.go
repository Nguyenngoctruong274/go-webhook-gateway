// Package event is the domain of the gateway: the events producers
// publish, the endpoints customers register, and the deliveries that pair
// them, with the rules that move a delivery through its attempts. It does
// no I/O; the PostgreSQL store and the deliverer apply these rules and the
// tests here are their reference.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Limits on what producers and customers may submit.
const (
	MaxTypeLen        = 128
	MaxPayloadBytes   = 256 << 10 // 256 KiB
	MaxURLLen         = 2048
	MaxDescriptionLen = 500
	MaxIdempotencyKey = 200
	MaxAttempts       = 20
	MaxErrorBytes     = 1024
)

// Errors of the domain. They are wrapped with context; match them with
// errors.Is.
var (
	ErrInvalid           = errors.New("invalid")
	ErrInvalidTransition = errors.New("invalid delivery transition")
	ErrNotDue            = errors.New("delivery not due yet")
	ErrLeaseLost         = errors.New("delivery lease lost")
	ErrLeaseActive       = errors.New("delivery lease still active")
	ErrNotFound          = errors.New("not found")
)

var typeRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// ValidateType checks an event type such as "order.paid": lowercase
// letters, digits, dots, underscores and dashes, up to MaxTypeLen.
func ValidateType(t string) error {
	if t == "" || len(t) > MaxTypeLen || !typeRe.MatchString(t) {
		return fmt.Errorf("%w: type %q must match %s and be at most %d bytes", ErrInvalid, t, typeRe, MaxTypeLen)
	}
	return nil
}

// Event is one thing that happened, published once by a producer and
// delivered to every endpoint subscribed to its type.
type Event struct {
	ID   int64
	Type string
	// Payload is the JSON document receivers get as the request body.
	Payload []byte
	// IdempotencyKey, if set, makes publishing the same event again return
	// this one instead of a duplicate.
	IdempotencyKey string
	CreatedAt      time.Time
}

// Spec is what a producer sends to publish an event.
type Spec struct {
	Type           string
	Payload        []byte
	IdempotencyKey string
}

// Normalize validates the spec and fills defaults: an empty payload
// becomes {}. The payload must be a JSON object or array, so that
// receivers can always parse the body.
func (s Spec) Normalize() (Spec, error) {
	var errs []error
	if err := ValidateType(s.Type); err != nil {
		errs = append(errs, err)
	}
	if len(s.Payload) == 0 {
		s.Payload = []byte("{}")
	}
	switch {
	case len(s.Payload) > MaxPayloadBytes:
		errs = append(errs, fmt.Errorf("%w: payload is %d bytes, at most %d", ErrInvalid, len(s.Payload), MaxPayloadBytes))
	case !json.Valid(s.Payload):
		errs = append(errs, fmt.Errorf("%w: payload is not valid JSON", ErrInvalid))
	default:
		if first := firstNonSpace(s.Payload); first != '{' && first != '[' {
			errs = append(errs, fmt.Errorf("%w: payload must be a JSON object or array", ErrInvalid))
		}
	}
	if err := validateKey(s.IdempotencyKey); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return Spec{}, err
	}
	return s, nil
}

func firstNonSpace(b []byte) byte {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		}
		return c
	}
	return 0
}

func validateKey(k string) error {
	if k == "" {
		return nil
	}
	if len(k) > MaxIdempotencyKey || !utf8.ValidString(k) || strings.ContainsRune(k, 0) {
		return fmt.Errorf("%w: idempotency key must be valid UTF-8 without NUL, at most %d bytes", ErrInvalid, MaxIdempotencyKey)
	}
	return nil
}

// Endpoint is a customer's receiver.
type Endpoint struct {
	ID          int64
	URL         string
	Description string
	// Types lists the event types the endpoint subscribes to; empty means
	// every type.
	Types []string
	// Enabled endpoints get deliveries. A disabled one keeps its history
	// and gets nothing new until re-enabled; deliveries in flight finish.
	Enabled     bool
	MaxAttempts int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// EndpointSpec is what a customer sends to register or update an endpoint.
type EndpointSpec struct {
	URL         string
	Description string
	Types       []string
	// MaxAttempts per delivery; 0 means the default of 8.
	MaxAttempts int
}

// DefaultMaxAttempts is used when a spec does not set one. With
// DefaultBackoff the eighth attempt comes about 21 minutes after the
// first, long enough for a deploy or a restart on the receiver's side.
const DefaultMaxAttempts = 8

// Normalize validates the spec and fills defaults. The URL must be
// absolute http(s) with a host; whether the host is one the gateway may
// talk to at all (not loopback, not private) is checked at delivery time,
// against the resolved address, since DNS can change after registration.
func (s EndpointSpec) Normalize() (EndpointSpec, error) {
	var errs []error
	s.URL = strings.TrimSpace(s.URL)
	if err := ValidateURL(s.URL); err != nil {
		errs = append(errs, err)
	}
	if len(s.Description) > MaxDescriptionLen || !utf8.ValidString(s.Description) || strings.ContainsRune(s.Description, 0) {
		errs = append(errs, fmt.Errorf("%w: description must be valid UTF-8 without NUL, at most %d bytes", ErrInvalid, MaxDescriptionLen))
	}
	seen := map[string]bool{}
	types := s.Types[:0:0]
	for _, t := range s.Types {
		if err := ValidateType(t); err != nil {
			errs = append(errs, err)
			continue
		}
		if !seen[t] {
			seen[t] = true
			types = append(types, t)
		}
	}
	s.Types = types
	if s.MaxAttempts == 0 {
		s.MaxAttempts = DefaultMaxAttempts
	}
	if s.MaxAttempts < 1 || s.MaxAttempts > MaxAttempts {
		errs = append(errs, fmt.Errorf("%w: max attempts must be between 1 and %d, got %d", ErrInvalid, MaxAttempts, s.MaxAttempts))
	}
	if err := errors.Join(errs...); err != nil {
		return EndpointSpec{}, err
	}
	return s, nil
}

// ValidateURL accepts absolute http or https URLs with a host and no
// user info (credentials in URLs end up in logs and histories).
func ValidateURL(raw string) error {
	if raw == "" || len(raw) > MaxURLLen {
		return fmt.Errorf("%w: url is required and at most %d bytes", ErrInvalid, MaxURLLen)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: url: %w", ErrInvalid, err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return fmt.Errorf("%w: url scheme must be http or https", ErrInvalid)
	case u.Host == "" || u.Hostname() == "":
		return fmt.Errorf("%w: url must have a host", ErrInvalid)
	case u.User != nil:
		return fmt.Errorf("%w: url must not contain credentials", ErrInvalid)
	case u.Fragment != "":
		return fmt.Errorf("%w: url must not contain a fragment", ErrInvalid)
	}
	return nil
}

// Subscribed reports whether the endpoint wants events of type t.
func (e Endpoint) Subscribed(t string) bool {
	if len(e.Types) == 0 {
		return true
	}
	for _, et := range e.Types {
		if et == t {
			return true
		}
	}
	return false
}

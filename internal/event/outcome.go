package event

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"unicode/utf8"
)

// Outcome is the gateway's reading of one attempt to deliver.
type Outcome int

// The outcomes. Only Retry schedules another attempt.
const (
	// Delivered: the receiver answered 2xx.
	Delivered Outcome = iota
	// Retry: a transient failure, such as 5xx, 408, 429, a timeout, a
	// connection error, or a response too large to read. Try again
	// later, after the backoff.
	Retry
	// Rejected: the receiver's final answer, such as 400, 401, 404 or
	// 410. Retrying would send the same request to the same code; it is
	// the customer's configuration that must change.
	Rejected
	// Blocked: the gateway refused to send, for example because the URL
	// resolved to a private address. Nothing on the receiver's side can
	// change that; the endpoint needs fixing.
	Blocked
)

func (o Outcome) String() string {
	switch o {
	case Delivered:
		return "delivered"
	case Retry:
		return "retry"
	case Rejected:
		return "rejected"
	case Blocked:
		return "blocked"
	}
	return fmt.Sprintf("outcome(%d)", int(o))
}

// Final reports whether the outcome ends the delivery.
func (o Outcome) Final() bool { return o != Retry }

// ClassifyStatus maps an HTTP status code to an outcome.
//
// 2xx is success, whatever the body says. 5xx is the receiver's problem
// and transient by definition. Among 4xx, 408 (request timeout) and 429
// (too many requests) are explicit invitations to retry; every other 4xx
// is the receiver stating that this request, as sent, is not acceptable,
// and repeating it would only add to its logs. 3xx is not followed
// (redirects are a way to redirect a signed request to another host), so
// it counts as a rejection too.
func ClassifyStatus(code int) Outcome {
	switch {
	case code >= 200 && code <= 299:
		return Delivered
	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
		return Retry
	case code >= 500 && code <= 599:
		return Retry
	default:
		return Rejected
	}
}

// ErrBlocked marks a transport error the gateway raised itself before
// sending, such as a URL resolving to a forbidden address. Wrap it with
// context; ClassifyError turns it into Blocked.
var ErrBlocked = errors.New("delivery blocked")

// ClassifyError maps a transport error (no HTTP response) to an outcome.
// Timeouts, refused connections, DNS failures and resets are transient:
// the receiver may well be back next time. Errors wrapping ErrBlocked are
// Blocked. A context cancellation is the gateway shutting down, which is
// no verdict on the receiver, so it is Retry as well.
func ClassifyError(err error) Outcome {
	switch {
	case err == nil:
		return Delivered
	case errors.Is(err, ErrBlocked):
		return Blocked
	default:
		return Retry
	}
}

// Describe renders the failure of an attempt for the delivery history:
// the status code and a short, sanitized response excerpt, or the
// transport error with its class, at most MaxErrorBytes.
func Describe(status int, body []byte, err error) string {
	var s string
	switch {
	case err != nil:
		s = errorClass(err) + ": " + err.Error()
	case status != 0:
		s = fmt.Sprintf("HTTP %d", status)
		if excerpt := strings.TrimSpace(string(body)); excerpt != "" {
			s += ": " + excerpt
		}
	default:
		return ""
	}
	return sanitize(s)
}

func errorClass(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrBlocked):
		return "blocked"
	case errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	default:
		return "transport"
	}
}

// sanitize makes text fit for storage: valid UTF-8, no NUL, no control
// characters other than newline and tab, at most MaxErrorBytes, cut on a
// rune boundary.
func sanitize(s string) string {
	s = strings.ToValidUTF8(s, "�")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, s)
	if len(s) <= MaxErrorBytes {
		return s
	}
	cut := MaxErrorBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// Package deliverer sends claimed deliveries to their endpoints and reports
// the outcome: a Sender makes one signed HTTP request, a Pool runs senders
// against the queue.
package deliverer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/egress"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

// UserAgent identifies the gateway to receivers.
const UserAgent = "go-webhook-gateway/1.0"

// MaxResponseBytes is how much of a response body is read for the
// delivery history. Receivers are expected to answer with a status code,
// not a document; the rest is discarded without being downloaded.
const MaxResponseBytes = 4 << 10

// Request is what a Sender needs to make one attempt.
type Request struct {
	DeliveryID int64
	EventType  string
	Body       []byte
	URL        string
	Secrets    []string
}

// Sender makes one signed POST per attempt. It never follows redirects:
// a redirect would send a signed request, secret-bound, to a host the
// customer did not register. Every connection goes through an
// egress.Dialer, so a URL whose host resolves into the gateway's own
// network is refused at the socket, whatever DNS says at that moment.
type Sender struct {
	client *http.Client
	now    func() time.Time
}

// NewSender returns a Sender whose requests time out after timeout, end
// to end (dial, TLS, request, response headers and body). A receiver
// slower than that gets a Retry; it had its chance. policy decides
// which addresses may be connected to; a refused destination is a
// Blocked outcome, final, since no retry changes where a host points.
func NewSender(timeout time.Duration, policy egress.Policy) *Sender {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialer := &egress.Dialer{Policy: policy, Timeout: 5 * time.Second}
	transport := &http.Transport{
		// No proxy from the environment: a proxy would resolve and
		// connect on the gateway's behalf, past the egress check.
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: timeout,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &Sender{
		client: &http.Client{
			Transport: transport,
			Timeout:   timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse // report the 3xx, do not follow it
			},
		},
		now: time.Now,
	}
}

// Send makes the attempt and reports what happened. It never returns an
// error: every failure is an outcome the delivery records.
func (s *Sender) Send(ctx context.Context, r Request) event.Result {
	ts := s.now()
	headers, err := signing.Headers(r.Secrets, r.DeliveryID, ts, r.Body)
	if err != nil {
		// No usable secret is the gateway's own fault, not the receiver's.
		return event.Result{Outcome: event.Blocked, Error: event.Describe(0, nil, fmt.Errorf("%w: %w", event.ErrBlocked, err))}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return event.Result{Outcome: event.Blocked, Error: event.Describe(0, nil, fmt.Errorf("%w: build request: %w", event.ErrBlocked, err))}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Webhook-Event-Type", r.EventType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		if errors.Is(err, egress.ErrDenied) {
			err = fmt.Errorf("%w: %w", event.ErrBlocked, err)
		}
		return event.Result{Outcome: event.ClassifyError(err), Error: event.Describe(0, nil, err)}
	}
	defer resp.Body.Close()

	// Read a bounded excerpt; a receiver that streams gigabytes back does
	// not get to fill the history table or the deliverer's memory.
	excerpt, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes))
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		// Headers said 2xx but the body broke mid-way: the receiver's
		// handler may or may not have run. Retry; receivers deduplicate.
		return event.Result{Outcome: event.Retry, Status: resp.StatusCode,
			Error: event.Describe(0, nil, fmt.Errorf("reading response after HTTP %d: %w", resp.StatusCode, readErr))}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // let the connection be reused

	outcome := event.ClassifyStatus(resp.StatusCode)
	res := event.Result{Outcome: outcome, Status: resp.StatusCode}
	if outcome != event.Delivered {
		res.Error = event.Describe(resp.StatusCode, excerpt, nil)
	}
	return res
}

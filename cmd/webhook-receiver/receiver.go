package main

import (
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

// maxBody bounds what the receiver reads: the gateway never sends more
// than the payload limit plus a little envelope.
const maxBody = 1 << 20

// Receiver is what a customer's endpoint should do with a webhook:
// verify the signature before trusting a byte of the body, then ignore a
// delivery it has already processed, because the gateway retries and a
// retry may arrive after a success whose response was lost.
type Receiver struct {
	verifier signing.Verifier
	logger   *slog.Logger
	// Handle processes an authentic, first-time delivery. An error makes
	// the receiver answer 500 so that the gateway retries.
	Handle func(deliveryID int64, body []byte) error

	mu   sync.Mutex
	seen map[int64]time.Time
}

// NewReceiver returns a receiver that accepts signatures under secrets.
func NewReceiver(secrets []string, logger *slog.Logger) *Receiver {
	return &Receiver{
		verifier: signing.Verifier{Secrets: secrets},
		logger:   logger,
		seen:     make(map[int64]time.Time),
		Handle:   func(int64, []byte) error { return nil },
	}
}

// ServeHTTP verifies and dispatches one delivery.
//
// Replies: 401 for anything that fails verification (one code for every
// cause, so a probe learns nothing), 200 for a duplicate (already done,
// nothing to retry), 500 when Handle fails (please retry), 200 on success.
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, maxBody+1))
	if err != nil || len(body) > maxBody {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}

	ts, id, err := r.verifier.Verify(req.Header.Get, body)
	if err != nil {
		r.logger.Warn("rejected webhook", slog.Any("reason", err), slog.String("remote", req.RemoteAddr))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if !r.firstTime(id, ts) {
		r.logger.Info("duplicate delivery ignored", slog.Int64("delivery_id", id))
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := r.Handle(id, body); err != nil {
		r.forget(id)
		r.logger.Error("handling failed; asking for a retry", slog.Int64("delivery_id", id), slog.Any("error", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	r.logger.Info("delivery processed", slog.Int64("delivery_id", id), slog.Time("signed_at", ts), slog.Int("bytes", len(body)))
	w.WriteHeader(http.StatusOK)
}

// firstTime records id and reports whether it was new. Entries older
// than the verifier's tolerance are dropped: a signature that old is
// refused before this check, so remembering it any longer buys nothing.
// In a real receiver this set lives in its database, keyed by Webhook-Id.
func (r *Receiver) firstTime(id int64, ts time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-2 * signing.DefaultTolerance)
	for old, seenAt := range r.seen {
		if seenAt.Before(cutoff) {
			delete(r.seen, old)
		}
	}
	if _, dup := r.seen[id]; dup {
		return false
	}
	r.seen[id] = ts
	return true
}

func (r *Receiver) forget(id int64) {
	r.mu.Lock()
	delete(r.seen, id)
	r.mu.Unlock()
}

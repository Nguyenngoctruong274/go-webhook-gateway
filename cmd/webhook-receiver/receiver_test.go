package main

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

const secret = "whsec_0123456789abcdef0123456789abcdef"

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func signed(t *testing.T, secrets []string, id int64, ts time.Time, body string) *http.Request {
	t.Helper()
	h, err := signing.Headers(secrets, id, ts, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body))
	for k, v := range h {
		req.Header.Set(k, v)
	}
	return req
}

func serve(rc *Receiver, req *http.Request) int {
	rec := httptest.NewRecorder()
	rc.ServeHTTP(rec, req)
	return rec.Code
}

func TestReceiverAcceptsOnceAndIgnoresDuplicates(t *testing.T) {
	rc := NewReceiver([]string{secret}, quiet)
	var handled []int64
	rc.Handle = func(id int64, _ []byte) error { handled = append(handled, id); return nil }
	now := time.Now()

	if code := serve(rc, signed(t, []string{secret}, 1, now, `{"a":1}`)); code != 200 {
		t.Fatalf("first: %d", code)
	}
	// The gateway retries after a lost response: same id, maybe later.
	if code := serve(rc, signed(t, []string{secret}, 1, now.Add(10*time.Second), `{"a":1}`)); code != 200 {
		t.Fatalf("retry: %d", code)
	}
	if code := serve(rc, signed(t, []string{secret}, 2, now, `{"a":2}`)); code != 200 {
		t.Fatalf("second: %d", code)
	}
	if len(handled) != 2 || handled[0] != 1 || handled[1] != 2 {
		t.Fatalf("handled %v, want [1 2]: the duplicate must not reach the handler", handled)
	}
}

func TestReceiverRejectsForgeriesWithOneCode(t *testing.T) {
	rc := NewReceiver([]string{secret}, quiet)
	rc.Handle = func(int64, []byte) error { t.Fatal("handler ran on a rejected request"); return nil }
	now := time.Now()

	tamper := signed(t, []string{secret}, 1, now, `{"a":1}`)
	tamper.Body = io.NopCloser(bytes.NewBufferString(`{"a":2}`))
	wrongSecret := signed(t, []string{"whsec_ffffffffffffffffffffffffffffffff"}, 1, now, `{"a":1}`)
	stale := signed(t, []string{secret}, 1, now.Add(-10*time.Minute), `{"a":1}`)
	unsigned := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader("{}"))

	for name, req := range map[string]*http.Request{"tampered": tamper, "wrong secret": wrongSecret, "stale": stale, "unsigned": unsigned} {
		if code := serve(rc, req); code != http.StatusUnauthorized {
			t.Errorf("%s: %d, want 401", name, code)
		}
	}
	if code := serve(rc, httptest.NewRequest(http.MethodGet, "/webhook", nil)); code != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d", code)
	}
	huge := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(make([]byte, maxBody+1)))
	if code := serve(rc, huge); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized: %d", code)
	}
}

func TestReceiverAsksForRetryWhenHandlingFails(t *testing.T) {
	rc := NewReceiver([]string{secret}, quiet)
	calls := 0
	rc.Handle = func(int64, []byte) error {
		calls++
		if calls == 1 {
			return errors.New("database down")
		}
		return nil
	}
	now := time.Now()
	if code := serve(rc, signed(t, []string{secret}, 1, now, `{}`)); code != http.StatusInternalServerError {
		t.Fatalf("failing handler: %d, want 500", code)
	}
	// The retry must reach the handler again: a failed attempt is not "seen".
	if code := serve(rc, signed(t, []string{secret}, 1, now, `{}`)); code != 200 || calls != 2 {
		t.Fatalf("retry: %d, calls %d", code, calls)
	}
}

func TestReceiverAcceptsBothSecretsDuringRotation(t *testing.T) {
	old, cur := "whsec_oldoldoldoldoldoldoldoldoldoldold", secret
	rc := NewReceiver([]string{cur, old}, quiet)
	now := time.Now()
	if code := serve(rc, signed(t, []string{old}, 1, now, `{}`)); code != 200 {
		t.Fatalf("signed with the old secret: %d", code)
	}
	if code := serve(rc, signed(t, []string{cur}, 2, now, `{}`)); code != 200 {
		t.Fatalf("signed with the current secret: %d", code)
	}
}

func TestSeenSetIsBounded(t *testing.T) {
	rc := NewReceiver([]string{secret}, quiet)
	// Plant an entry older than the window; the next request sweeps it.
	rc.seen[99] = time.Now().Add(-time.Hour)
	serve(rc, signed(t, []string{secret}, 1, time.Now(), `{}`))
	if _, still := rc.seen[99]; still {
		t.Fatal("old entry not swept")
	}
}

package event

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestClassifyStatus(t *testing.T) {
	cases := map[int]Outcome{
		200: Delivered, 201: Delivered, 204: Delivered, 299: Delivered,
		301: Rejected, 302: Rejected, 307: Rejected,
		400: Rejected, 401: Rejected, 403: Rejected, 404: Rejected, 410: Rejected, 413: Rejected, 422: Rejected,
		408: Retry, 429: Retry,
		500: Retry, 502: Retry, 503: Retry, 504: Retry, 599: Retry,
		100: Rejected, 0: Rejected, 999: Rejected,
	}
	for code, want := range cases {
		if got := ClassifyStatus(code); got != want {
			t.Errorf("ClassifyStatus(%d) = %v, want %v", code, got, want)
		}
	}
	if Delivered.Final() != true || Retry.Final() != false || Rejected.Final() != true || Blocked.Final() != true {
		t.Fatal("Final is wrong")
	}
	if Outcome(9).String() != "outcome(9)" || Retry.String() != "retry" {
		t.Fatal("String is wrong")
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassifyErrorAndDescribe(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		outcome Outcome
		class   string
	}{
		{"nil", nil, Delivered, ""},
		{"deadline", fmt.Errorf("post: %w", context.DeadlineExceeded), Retry, "timeout"},
		{"net timeout", &net.OpError{Op: "dial", Err: timeoutErr{}}, Retry, "timeout"},
		{"refused", errors.New("dial tcp: connection refused"), Retry, "transport"},
		{"canceled", context.Canceled, Retry, "canceled"},
		{"blocked", fmt.Errorf("%w: resolves to 10.0.0.1", ErrBlocked), Blocked, "blocked"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != tt.outcome {
				t.Fatalf("ClassifyError = %v, want %v", got, tt.outcome)
			}
			if tt.err == nil {
				return
			}
			if d := Describe(0, nil, tt.err); !strings.HasPrefix(d, tt.class+": ") {
				t.Fatalf("Describe = %q, want prefix %q", d, tt.class+": ")
			}
		})
	}
}

func TestDescribeResponses(t *testing.T) {
	if d := Describe(503, []byte("  upstream down\n"), nil); d != "HTTP 503: upstream down" {
		t.Fatalf("Describe = %q", d)
	}
	if d := Describe(404, nil, nil); d != "HTTP 404" {
		t.Fatalf("Describe = %q", d)
	}
	if d := Describe(0, nil, nil); d != "" {
		t.Fatalf("Describe = %q, want empty", d)
	}
	// Binary garbage, control characters and an oversized body are
	// sanitized and cut on a rune boundary.
	body := append([]byte("\x00\x01err\xff "), []byte(strings.Repeat("é", MaxErrorBytes))...)
	d := Describe(500, body, nil)
	if !utf8.ValidString(d) || strings.ContainsAny(d, "\x00\x01") || len(d) > MaxErrorBytes || !strings.HasPrefix(d, "HTTP 500: err�") {
		t.Fatalf("Describe = %q (%d bytes)", d[:40], len(d))
	}
}

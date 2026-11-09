package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestProbes(t *testing.T) {
	down := func(context.Context) error { return errors.New("postgres: connection refused") }
	up := func(context.Context) error { return nil }

	tests := []struct {
		name   string
		ready  func(context.Context) error
		method string
		path   string
		status int
		body   string
	}{
		{"healthz", nil, http.MethodGet, "/healthz", 200, "{\"status\":\"ok\"}\n"},
		{"healthz ignores the database", down, http.MethodGet, "/healthz", 200, "{\"status\":\"ok\"}\n"},
		{"healthz wrong method", nil, http.MethodPost, "/healthz", 405, ""},
		{"readyz without check", nil, http.MethodGet, "/readyz", 200, "{\"status\":\"ready\"}\n"},
		{"readyz up", up, http.MethodGet, "/readyz", 200, "{\"status\":\"ready\"}\n"},
		{"readyz down", down, http.MethodGet, "/readyz", 503, "{\"status\":\"unavailable\"}\n"},
		{"unknown path", nil, http.MethodGet, "/nope", 404, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(discardLogger(), Options{Ready: tt.ready})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d", rec.Code, tt.status)
			}
			if tt.body != "" && rec.Body.String() != tt.body {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tt.body)
			}
		})
	}
}

func TestRequestID(t *testing.T) {
	var seen string
	h := (&api{logger: discardLogger()}).withRequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = RequestID(r.Context())
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if len(seen) != 24 || rec.Header().Get(RequestIDHeader) != seen {
		t.Fatalf("generated id %q, header %q", seen, rec.Header().Get(RequestIDHeader))
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(RequestIDHeader, "client-abc")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if seen != "client-abc" || rec.Header().Get(RequestIDHeader) != "client-abc" {
		t.Fatalf("client id not propagated: %q", seen)
	}
}

func TestMetricsEndpointAndRequestMetrics(t *testing.T) {
	reg := metrics.NewRegistry()
	h := NewHandler(discardLogger(), Options{Metrics: reg, Token: "a-token-of-sixteen+"})
	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/healthz", nil),
		httptest.NewRequest(http.MethodGet, "/healthz", nil),
		httptest.NewRequest(http.MethodGet, "/nope/123", nil),
		httptest.NewRequest("PROPFIND", "/healthz", nil),
	} {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil)) // no token needed
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("/metrics: %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	for _, want := range []string{
		`http_requests_total{method="GET",route="/healthz",status="200"} 2`,
		`http_requests_total{method="GET",route="unmatched",status="404"} 1`,
		`http_requests_total{method="other",route="unmatched",status="405"} 1`,
		`http_request_duration_seconds_count{method="GET",route="/healthz"} 2`,
	} {
		if !strings.Contains(body, want+"\n") {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if strings.Contains(body, "/nope/123") || strings.Contains(body, "PROPFIND") {
		t.Error("raw paths or methods became series")
	}
	if t.Failed() {
		t.Log(body)
	}
	// Without a registry there is no /metrics route.
	rec = httptest.NewRecorder()
	NewHandler(discardLogger(), Options{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/metrics without a registry: %d", rec.Code)
	}
}

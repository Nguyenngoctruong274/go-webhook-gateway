// Package httpapi exposes webhookd over HTTP: the probes for the
// platform and, under /v1, the customer API for endpoints, events and
// deliveries.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
)

// RequestIDHeader carries the request id in and out of the service.
const RequestIDHeader = "X-Request-Id"

// Options configures the handler. Zero values are valid.
type Options struct {
	// Ready reports whether the service can take traffic (dependencies up,
	// not draining). nil means always ready.
	Ready func(context.Context) error
	// Store backs the customer API; nil serves the probes only.
	Store Store
	// Token, when set, is the bearer token every /v1 request must carry.
	Token string
	// Metrics, when set, receives the HTTP metrics and is served at
	// GET /metrics, next to the probes and like them without the token:
	// expose it to the scraper's network only.
	Metrics *metrics.Registry
}

type api struct {
	logger *slog.Logger
	ready  func(context.Context) error
	store  Store
	token  string

	requests *metrics.CounterVec
	latency  *metrics.HistogramVec
}

// NewHandler returns the root handler of the service.
func NewHandler(logger *slog.Logger, opts Options) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	reg := opts.Metrics
	if reg == nil {
		reg = metrics.NewRegistry() // instrument anyway; nothing serves it
	}
	a := &api{
		logger: logger, ready: opts.Ready, store: opts.Store, token: opts.Token,
		requests: reg.NewCounterVec("http_requests_total",
			"HTTP requests by method, route pattern and status code.", "method", "route", "status"),
		latency: reg.NewHistogramVec("http_request_duration_seconds",
			"HTTP request latency by method and route pattern.", metrics.DefaultLatencyBuckets, "method", "route"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	if opts.Metrics != nil {
		mux.Handle("GET /metrics", opts.Metrics.Handler())
	}
	if a.store != nil {
		a.routes(mux)
	}
	return a.withRequestID(a.instrument(mux, a.authenticate(mux)))
}

// healthz answers as long as the process is serving: it is a liveness probe
// and must not depend on PostgreSQL, or a database blip would restart every
// pod.
func (a *api) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz fails while PostgreSQL is unreachable or the process is draining, so the
// load balancer stops sending traffic without the pod being restarted.
func (a *api) readyz(w http.ResponseWriter, r *http.Request) {
	if a.ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := a.ready(ctx); err != nil {
			a.logger.WarnContext(ctx, "not ready", slog.Any("error", err))
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type ctxKey struct{}

// RequestID returns the id assigned to the request, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

func (a *api) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if id == "" || len(id) > 128 {
			id = newRequestID()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// instrument counts and times every request by its route pattern rather
// than the raw path, so ids in paths do not each become a series, and
// logs it at debug level. Methods outside the usual set are counted as
// "other" for the same reason.
func (a *api) instrument(mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start)

		route := "unmatched"
		if _, pattern := mux.Handler(r); pattern != "" {
			if _, path, ok := strings.Cut(pattern, " "); ok {
				route = path
			} else {
				route = pattern
			}
		}
		method := r.Method
		switch method {
		case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodHead, http.MethodPatch:
		default:
			method = "other"
		}
		a.requests.Inc(method, route, strconv.Itoa(rec.status))
		a.latency.Observe(elapsed.Seconds(), method, route)
		a.logger.DebugContext(r.Context(), "request",
			slog.String("request_id", RequestID(r.Context())),
			slog.String("method", r.Method), slog.String("route", route),
			slog.Int("status", rec.status), slog.Duration("duration", elapsed))
	})
}

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

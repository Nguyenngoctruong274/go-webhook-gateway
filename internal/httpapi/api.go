package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/breaker"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/postgres"
)

// Store is what the API needs from the delivery store; *postgres.Store
// is the real one.
type Store interface {
	CreateEndpoint(ctx context.Context, spec event.EndpointSpec) (event.Endpoint, string, error)
	GetEndpoint(ctx context.Context, id int64) (event.Endpoint, error)
	ListEndpoints(ctx context.Context) ([]event.Endpoint, error)
	UpdateEndpoint(ctx context.Context, id int64, spec event.EndpointSpec) (event.Endpoint, error)
	SetEndpointEnabled(ctx context.Context, id int64, enabled bool) (event.Endpoint, error)
	RotateSecret(ctx context.Context, id int64, overlap time.Duration) (string, error)
	Breaker(ctx context.Context, endpointID int64) (breaker.State, error)
	ResetBreaker(ctx context.Context, endpointID int64) error
	Stats(ctx context.Context, endpointID int64) (map[event.State]int, error)
	History(ctx context.Context, f postgres.HistoryFilter) ([]event.Delivery, bool, error)

	Publish(ctx context.Context, spec event.Spec) (postgres.Published, error)
	GetEvent(ctx context.Context, id int64) (event.Event, error)
	DeliveriesOfEvent(ctx context.Context, eventID int64) ([]event.Delivery, error)

	GetDelivery(ctx context.Context, id int64) (event.Delivery, error)
	Replay(ctx context.Context, id int64) (event.Delivery, error)
	Cancel(ctx context.Context, id int64) (event.Delivery, error)
}

// MaxRequestBody bounds a request body: the largest legitimate one is an
// event payload (event.MaxPayloadBytes) plus its envelope.
const MaxRequestBody = event.MaxPayloadBytes + 64<<10

// MaxRotationOverlap bounds how long a previous secret stays valid.
const MaxRotationOverlap = 7 * 24 * time.Hour

// IdempotencyKeyHeader is the usual place for an event's idempotency
// key; the body's field wins when both are set.
const IdempotencyKeyHeader = "Idempotency-Key"

// routes mounts the customer API on mux.
func (a *api) routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/endpoints", a.createEndpoint)
	mux.HandleFunc("GET /v1/endpoints", a.listEndpoints)
	mux.HandleFunc("GET /v1/endpoints/{id}", a.getEndpoint)
	mux.HandleFunc("PUT /v1/endpoints/{id}", a.updateEndpoint)
	mux.HandleFunc("POST /v1/endpoints/{id}/pause", a.setEnabled(false))
	mux.HandleFunc("POST /v1/endpoints/{id}/resume", a.setEnabled(true))
	mux.HandleFunc("POST /v1/endpoints/{id}/secret", a.rotateSecret)
	mux.HandleFunc("GET /v1/endpoints/{id}/breaker", a.getBreaker)
	mux.HandleFunc("DELETE /v1/endpoints/{id}/breaker", a.resetBreaker)
	mux.HandleFunc("GET /v1/endpoints/{id}/deliveries", a.history)

	mux.HandleFunc("POST /v1/events", a.publish)
	mux.HandleFunc("GET /v1/events/{id}", a.getEvent)

	mux.HandleFunc("GET /v1/deliveries/{id}", a.getDelivery)
	mux.HandleFunc("POST /v1/deliveries/{id}/replay", a.replay)
	mux.HandleFunc("POST /v1/deliveries/{id}/cancel", a.cancel)
}

// authenticate requires the bearer token on /v1 when one is configured.
// The probes stay open: they are for the platform, not customers.
func (a *api) authenticate(next http.Handler) http.Handler {
	if a.token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(a.token)) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="webhookd"`)
				writeError(w, http.StatusUnauthorized, "unauthorized", "a valid bearer token is required")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// --- endpoints ---

type endpointRequest struct {
	URL         string   `json:"url"`
	Description string   `json:"description"`
	Types       []string `json:"types"`
	MaxAttempts int      `json:"max_attempts"`
	RateLimit   int      `json:"rate_limit"`
	Ordered     bool     `json:"ordered"`
}

func (r endpointRequest) spec() event.EndpointSpec {
	return event.EndpointSpec{URL: r.URL, Description: r.Description, Types: r.Types,
		MaxAttempts: r.MaxAttempts, RateLimit: r.RateLimit, Ordered: r.Ordered}
}

func (a *api) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req endpointRequest
	if !a.decode(w, r, &req) {
		return
	}
	ep, secret, err := a.store.CreateEndpoint(r.Context(), req.spec())
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, createdEndpoint{endpointResponse: toEndpoint(ep), Secret: secret})
}

func (a *api) listEndpoints(w http.ResponseWriter, r *http.Request) {
	eps, err := a.store.ListEndpoints(r.Context())
	if err != nil {
		a.fail(w, r, err)
		return
	}
	out := make([]endpointResponse, len(eps))
	for i, ep := range eps {
		out[i] = toEndpoint(ep)
	}
	writeJSON(w, http.StatusOK, listResponse[endpointResponse]{Items: out})
}

func (a *api) getEndpoint(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	ep, err := a.store.GetEndpoint(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	st, err := a.store.Breaker(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	stats, err := a.store.Stats(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	resp := endpointDetail{endpointResponse: toEndpoint(ep), Breaker: toBreaker(st), Deliveries: stats}
	writeJSON(w, http.StatusOK, resp)
}

func (a *api) updateEndpoint(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req endpointRequest
	if !a.decode(w, r, &req) {
		return
	}
	ep, err := a.store.UpdateEndpoint(r.Context(), id, req.spec())
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpoint(ep))
}

func (a *api) setEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		ep, err := a.store.SetEndpointEnabled(r.Context(), id, enabled)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, toEndpoint(ep))
	}
}

type rotateRequest struct {
	// Overlap is how long the previous secret keeps verifying, as a Go
	// duration ("24h"); default 24h, at most MaxRotationOverlap.
	Overlap string `json:"overlap"`
}

func (a *api) rotateSecret(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	req := rotateRequest{Overlap: "24h"}
	if r.ContentLength != 0 && !a.decode(w, r, &req) {
		return
	}
	overlap, err := time.ParseDuration(req.Overlap)
	if err != nil || overlap < 0 || overlap > MaxRotationOverlap {
		writeError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("overlap must be a duration between 0 and %s", MaxRotationOverlap))
		return
	}
	secret, err := a.store.RotateSecret(r.Context(), id, overlap)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"secret": secret, "previous_secret_until": time.Now().Add(overlap).UTC().Format(time.RFC3339)})
}

func (a *api) getBreaker(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	st, err := a.store.Breaker(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBreaker(st))
}

func (a *api) resetBreaker(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := a.store.ResetBreaker(r.Context(), id); err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBreaker(breaker.State{Status: breaker.Closed}))
}

func (a *api) history(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if _, err := a.store.GetEndpoint(r.Context(), id); err != nil {
		a.fail(w, r, err)
		return
	}
	f := postgres.HistoryFilter{EndpointID: id, State: event.State(r.URL.Query().Get("state"))}
	q := r.URL.Query()
	if raw := q.Get("before"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "invalid", "before must be a positive delivery id")
			return
		}
		f.Before = n
	}
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > postgres.MaxHistoryPage {
			writeError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("limit must be between 1 and %d", postgres.MaxHistoryPage))
			return
		}
		f.Limit = n
	}
	page, more, err := a.store.History(r.Context(), f)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	resp := listResponse[deliveryResponse]{Items: make([]deliveryResponse, len(page))}
	for i, d := range page {
		resp.Items[i] = toDelivery(d)
	}
	if more {
		resp.NextBefore = page[len(page)-1].ID
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- events ---

type publishRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (a *api) publish(w http.ResponseWriter, r *http.Request) {
	var req publishRequest
	if !a.decode(w, r, &req) {
		return
	}
	key := req.IdempotencyKey
	if key == "" {
		key = r.Header.Get(IdempotencyKeyHeader)
	}
	p, err := a.store.Publish(r.Context(), event.Spec{Type: req.Type, Payload: req.Payload, IdempotencyKey: key})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	status := http.StatusCreated
	if !p.Created {
		status = http.StatusOK // the key was seen: here is the event it made
	}
	writeJSON(w, status, publishedResponse{Event: toEvent(p.Event), Created: p.Created, Deliveries: p.Deliveries})
}

func (a *api) getEvent(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	ev, err := a.store.GetEvent(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	ds, err := a.store.DeliveriesOfEvent(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	resp := eventDetail{eventResponse: toEvent(ev), Deliveries: make([]deliveryResponse, len(ds))}
	for i, d := range ds {
		resp.Deliveries[i] = toDelivery(d)
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- deliveries ---

func (a *api) getDelivery(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	d, err := a.store.GetDelivery(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDelivery(d))
}

func (a *api) replay(w http.ResponseWriter, r *http.Request) {
	a.deliveryAction(w, r, a.store.Replay)
}

func (a *api) cancel(w http.ResponseWriter, r *http.Request) {
	a.deliveryAction(w, r, a.store.Cancel)
}

func (a *api) deliveryAction(w http.ResponseWriter, r *http.Request, act func(context.Context, int64) (event.Delivery, error)) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	d, err := act(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDelivery(d))
}

// --- plumbing ---

// decode reads a JSON body into v, refusing unknown fields, trailing
// data and bodies over MaxRequestBody. It writes the 400 itself and
// returns false when the body is unusable.
func (a *api) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		writeError(w, http.StatusUnsupportedMediaType, "invalid", "Content-Type must be application/json")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRequestBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "invalid", fmt.Sprintf("request body must be at most %d bytes", MaxRequestBody))
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid", "malformed JSON body: "+err.Error())
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid", "request body must be a single JSON document")
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusNotFound, "not_found", "no such resource")
		return 0, false
	}
	return id, true
}

// fail maps a store error to a status: the domain's sentinels are the
// contract, the rest is a 500 whose cause is logged, not shown.
func (a *api) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, event.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "no such resource")
	case errors.Is(err, event.ErrInvalid):
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, event.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, event.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case r.Context().Err() != nil:
		// The client went away; nobody is reading the answer.
		writeError(w, http.StatusRequestTimeout, "cancelled", "request cancelled")
	default:
		a.logger.ErrorContext(r.Context(), "request failed",
			slog.String("request_id", RequestID(r.Context())), slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, "internal", "internal error; see request id "+RequestID(r.Context()))
	}
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorResponse{Error: errorBody{Code: code, Message: msg}})
}

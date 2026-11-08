package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/postgres"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/testdb"
)

const token = "test-token-0123456789"

// newAPI mounts the API on a fresh schema; the test is skipped when no
// database is configured.
func newAPI(t *testing.T) (http.Handler, *postgres.Store) {
	t.Helper()
	db := testdb.New(t, postgres.Migrate)
	store, err := postgres.NewStore(db, postgres.WithBackoff(event.Backoff{Base: time.Millisecond, Max: time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(discardLogger(), Options{Store: store, Token: token}), store
}

type response struct {
	status int
	body   map[string]any
	raw    string
}

func (r response) str(path string) string { return fmt.Sprint(r.get(path)) }

func (r response) num(path string) float64 {
	n, _ := r.get(path).(float64)
	return n
}

// get walks "a.b.0.c" through the decoded body.
func (r response) get(path string) any {
	var cur any = r.body
	for _, k := range strings.Split(path, ".") {
		switch c := cur.(type) {
		case map[string]any:
			cur = c[k]
		case []any:
			i, _ := strconv.Atoi(k)
			if i < 0 || i >= len(c) {
				return nil
			}
			cur = c[i]
		default:
			return nil
		}
	}
	return cur
}

func call(t *testing.T, h http.Handler, method, path string, body any, headers ...string) response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if s, ok := body.(string); ok {
			buf.WriteString(s)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	resp := response{status: rec.Code, raw: rec.Body.String()}
	if rec.Body.Len() > 0 {
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s: Content-Type %q", method, path, ct)
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp.body); err != nil {
			t.Fatalf("%s %s: body %q is not a JSON object: %v", method, path, rec.Body.String(), err)
		}
	}
	return resp
}

func expect(t *testing.T, r response, status int, code string) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status %d, want %d; body %s", r.status, status, r.raw)
	}
	if code != "" && r.str("error.code") != code {
		t.Fatalf("error code %q, want %q; body %s", r.str("error.code"), code, r.raw)
	}
}

func TestAuthentication(t *testing.T) {
	h, _ := newAPI(t)
	for _, auth := range []string{"", "Bearer wrong", "Basic " + token, "Bearer " + token + "x"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/endpoints", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
			t.Fatalf("auth %q: status %d", auth, rec.Code)
		}
	}
	// The probes need no token; a store-less handler has no /v1 at all.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != 200 {
		t.Fatalf("healthz with no token: %d", rec.Code)
	}
	bare := NewHandler(discardLogger(), Options{})
	rec = httptest.NewRecorder()
	bare.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/endpoints", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/v1 without a store: %d", rec.Code)
	}
}

func TestEndpointLifecycle(t *testing.T) {
	h, _ := newAPI(t)

	created := call(t, h, "POST", "/v1/endpoints", map[string]any{
		"url": "https://hooks.example.com/in", "types": []string{"order.paid"}, "rate_limit": 10,
	})
	expect(t, created, http.StatusCreated, "")
	id := created.str("id")
	secret := created.str("secret")
	if !strings.HasPrefix(secret, "whsec_") || created.str("url") != "https://hooks.example.com/in" ||
		created.num("max_attempts") != 8 || created.num("rate_limit") != 10 || created.get("enabled") != true {
		t.Fatalf("created = %s", created.raw)
	}

	got := call(t, h, "GET", "/v1/endpoints/"+id, nil)
	expect(t, got, 200, "")
	if _, hasSecret := got.body["secret"]; hasSecret {
		t.Fatal("GET returns the secret")
	}
	if got.str("breaker.status") != "closed" || got.get("deliveries") == nil {
		t.Fatalf("detail = %s", got.raw)
	}

	list := call(t, h, "GET", "/v1/endpoints", nil)
	expect(t, list, 200, "")
	if list.str("items.0.id") != id {
		t.Fatalf("list = %s", list.raw)
	}

	updated := call(t, h, "PUT", "/v1/endpoints/"+id, map[string]any{"url": "https://hooks.example.com/v2", "ordered": true, "max_attempts": 3})
	expect(t, updated, 200, "")
	if updated.str("url") != "https://hooks.example.com/v2" || updated.get("ordered") != true || updated.num("max_attempts") != 3 || updated.num("rate_limit") != 0 {
		t.Fatalf("updated = %s", updated.raw)
	}

	paused := call(t, h, "POST", "/v1/endpoints/"+id+"/pause", nil)
	expect(t, paused, 200, "")
	if paused.get("enabled") != false {
		t.Fatalf("paused = %s", paused.raw)
	}
	resumed := call(t, h, "POST", "/v1/endpoints/"+id+"/resume", nil)
	if resumed.get("enabled") != true {
		t.Fatalf("resumed = %s", resumed.raw)
	}

	rotated := call(t, h, "POST", "/v1/endpoints/"+id+"/secret", map[string]any{"overlap": "1h"})
	expect(t, rotated, 200, "")
	if rotated.str("secret") == secret || !strings.HasPrefix(rotated.str("secret"), "whsec_") {
		t.Fatalf("rotated = %s", rotated.raw)
	}
	expect(t, call(t, h, "POST", "/v1/endpoints/"+id+"/secret", map[string]any{"overlap": "30d"}), 400, "invalid")
	expect(t, call(t, h, "POST", "/v1/endpoints/"+id+"/secret", nil), 200, "") // default overlap

	expect(t, call(t, h, "GET", "/v1/endpoints/"+id+"/breaker", nil), 200, "")
	expect(t, call(t, h, "DELETE", "/v1/endpoints/"+id+"/breaker", nil), 200, "")

	// Errors: validation, not found, bad ids, bad bodies.
	bad := call(t, h, "POST", "/v1/endpoints", map[string]any{"url": "ftp://x", "max_attempts": 99})
	expect(t, bad, 400, "invalid")
	if msg := bad.str("error.message"); !strings.Contains(msg, "scheme") || !strings.Contains(msg, "max attempts") {
		t.Fatalf("validation message does not list every problem: %s", msg)
	}
	expect(t, call(t, h, "GET", "/v1/endpoints/999", nil), 404, "not_found")
	expect(t, call(t, h, "GET", "/v1/endpoints/abc", nil), 404, "not_found")
	expect(t, call(t, h, "POST", "/v1/endpoints", `{"url": "https://x.example", "unknown": 1}`), 400, "invalid")
	expect(t, call(t, h, "POST", "/v1/endpoints", `{"url": "https://x.example"} {}`), 400, "invalid")
	expect(t, call(t, h, "POST", "/v1/endpoints", `{"url":`), 400, "invalid")
	expect(t, call(t, h, "POST", "/v1/endpoints", `{"url": "https://x.example"}`, "Content-Type", "text/plain"), 415, "invalid")
	huge := `{"url": "https://x.example", "description": "` + strings.Repeat("a", MaxRequestBody) + `"}`
	expect(t, call(t, h, "POST", "/v1/endpoints", huge), 413, "invalid")
}

func TestPublishAndHistory(t *testing.T) {
	h, store := newAPI(t)
	ctx := context.Background()
	ep := call(t, h, "POST", "/v1/endpoints", map[string]any{"url": "https://hooks.example.com/in"})
	epID := ep.str("id")
	call(t, h, "POST", "/v1/endpoints", map[string]any{"url": "https://other.example.com/in", "types": []string{"other.type"}})

	pub := call(t, h, "POST", "/v1/events", `{"type":"order.paid","payload":{"order":42,"amount":"10.00"}}`, IdempotencyKeyHeader, "k-1")
	expect(t, pub, http.StatusCreated, "")
	if pub.get("created") != true || pub.num("deliveries") != 1 || pub.str("event.type") != "order.paid" || pub.str("event.idempotency_key") != "k-1" {
		t.Fatalf("published = %s", pub.raw)
	}
	if pub.str("event.payload.amount") != "10.00" {
		t.Fatalf("payload not echoed as JSON: %s", pub.raw)
	}
	evID := pub.str("event.id")

	// Same key again: 200, the same event, nothing new.
	again := call(t, h, "POST", "/v1/events", `{"type":"order.paid","payload":{"order":42,"amount":"10.00"}}`, IdempotencyKeyHeader, "k-1")
	expect(t, again, 200, "")
	if again.get("created") != false || again.str("event.id") != evID {
		t.Fatalf("replayed publish = %s", again.raw)
	}
	// Same key, different event: conflict.
	expect(t, call(t, h, "POST", "/v1/events", `{"type":"order.paid","payload":{"order":43},"idempotency_key":"k-1"}`), 409, "conflict")
	expect(t, call(t, h, "POST", "/v1/events", `{"type":"Bad Type","payload":"not an object"}`), 400, "invalid")
	expect(t, call(t, h, "POST", "/v1/events", `{"type":"order.paid"}`), http.StatusCreated, "") // empty payload becomes {}

	ev := call(t, h, "GET", "/v1/events/"+evID, nil)
	expect(t, ev, 200, "")
	if ev.str("deliveries.0.state") != "pending" || ev.str("deliveries.0.endpoint_id") != epID || ev.get("deliveries.0.next_attempt_at") == nil {
		t.Fatalf("event detail = %s", ev.raw)
	}
	delivID := ev.str("deliveries.0.id")

	// Nothing may be replayed while pending; cancel works; replay then
	// queues it again.
	expect(t, call(t, h, "POST", "/v1/deliveries/"+delivID+"/replay", nil), 409, "conflict")
	cancelled := call(t, h, "POST", "/v1/deliveries/"+delivID+"/cancel", nil)
	expect(t, cancelled, 200, "")
	if cancelled.str("state") != "cancelled" || cancelled.get("next_attempt_at") != nil {
		t.Fatalf("cancelled = %s", cancelled.raw)
	}
	expect(t, call(t, h, "POST", "/v1/deliveries/"+delivID+"/cancel", nil), 409, "conflict")
	replayed := call(t, h, "POST", "/v1/deliveries/"+delivID+"/replay", nil)
	expect(t, replayed, 200, "")
	if replayed.str("state") != "pending" || replayed.num("attempt") != 0 {
		t.Fatalf("replayed = %s", replayed.raw)
	}

	// A deliverer finishes it with a 503 excerpt; the history shows it.
	claimed, err := store.Claim(ctx, 10, time.Minute)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	for _, c := range claimed {
		r := event.Result{Outcome: event.Retry, Status: 503, Error: "HTTP 503: down for maintenance"}
		if c.Delivery.ID != atoi(delivID) {
			r = event.Result{Outcome: event.Delivered, Status: 204}
		}
		if _, err := store.Finish(ctx, c.Delivery.ID, c.Delivery.Attempt, r); err != nil {
			t.Fatal(err)
		}
	}
	hist := call(t, h, "GET", "/v1/endpoints/"+epID+"/deliveries", nil)
	expect(t, hist, 200, "")
	items, _ := hist.get("items").([]any)
	if len(items) != 2 || hist.get("next_before") != nil {
		t.Fatalf("history = %s", hist.raw)
	}
	// Newest first: the second event's delivery, then the first.
	if hist.str("items.0.state") != "delivered" || hist.num("items.0.last_status") != 204 || hist.get("items.0.delivered_at") == nil {
		t.Fatalf("history[0] = %s", hist.raw)
	}
	if hist.str("items.1.id") != delivID || hist.str("items.1.state") != "pending" || hist.str("items.1.last_error") != "HTTP 503: down for maintenance" || hist.num("items.1.last_status") != 503 {
		t.Fatalf("history[1] = %s", hist.raw)
	}
	page := call(t, h, "GET", "/v1/endpoints/"+epID+"/deliveries?limit=1", nil)
	expect(t, page, 200, "")
	if page.num("next_before") == 0 || len(page.get("items").([]any)) != 1 {
		t.Fatalf("page 1 = %s", page.raw)
	}
	page2 := call(t, h, "GET", fmt.Sprintf("/v1/endpoints/%s/deliveries?limit=1&before=%d", epID, int64(page.num("next_before"))), nil)
	if page2.str("items.0.id") != delivID || page2.get("next_before") != nil {
		t.Fatalf("page 2 = %s", page2.raw)
	}
	onlyDelivered := call(t, h, "GET", "/v1/endpoints/"+epID+"/deliveries?state=delivered", nil)
	if len(onlyDelivered.get("items").([]any)) != 1 {
		t.Fatalf("filtered = %s", onlyDelivered.raw)
	}
	expect(t, call(t, h, "GET", "/v1/endpoints/"+epID+"/deliveries?state=lost", nil), 400, "invalid")
	expect(t, call(t, h, "GET", "/v1/endpoints/"+epID+"/deliveries?limit=0", nil), 400, "invalid")
	expect(t, call(t, h, "GET", "/v1/endpoints/"+epID+"/deliveries?before=x", nil), 400, "invalid")
	expect(t, call(t, h, "GET", "/v1/endpoints/999/deliveries", nil), 404, "not_found")
	expect(t, call(t, h, "GET", "/v1/deliveries/999", nil), 404, "not_found")
	expect(t, call(t, h, "GET", "/v1/events/999", nil), 404, "not_found")

	detail := call(t, h, "GET", "/v1/endpoints/"+epID, nil)
	if detail.num("deliveries.delivered") != 1 || detail.num("deliveries.pending") != 1 {
		t.Fatalf("endpoint stats = %s", detail.raw)
	}
}

func atoi(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// Package e2e runs the gateway end to end: the customer API, the store,
// a deliverer pool with the real HTTP sender, and the reference receiver,
// all in one process against PostgreSQL. Nothing is faked but the clock
// of patience.
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/deliverer"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/egress"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/postgres"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/reaper"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/receiver"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/testdb"
)

const apiToken = "e2e-token-0123456789"

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// gateway is the system under test.
type gateway struct {
	api   *httptest.Server
	store *postgres.Store
}

// registerAll builds the registry webhookd serves: every family the
// process exports, the same way cmd/webhookd wires them.
func registerAll(t *testing.T, store *postgres.Store, db *sql.DB) (*metrics.Registry, *deliverer.Metrics) {
	t.Helper()
	reg := metrics.NewRegistry()
	metrics.RegisterRuntime(reg)
	postgres.RegisterPoolMetrics(reg, db)
	store.RegisterQueueMetrics(reg, quiet, 0)
	_ = reaper.New(store, quiet, reaper.Options{Metrics: reg})
	return reg, deliverer.NewMetrics(reg)
}

func startGateway(t *testing.T) *gateway {
	t.Helper()
	db := testdb.New(t, postgres.Migrate)
	store, err := postgres.NewStore(db, postgres.WithBackoff(event.Backoff{Base: 20 * time.Millisecond, Max: 20 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	reg, poolMetrics := registerAll(t, store, db)
	api := httptest.NewServer(httpapi.NewHandler(quiet, httpapi.Options{Store: store, Token: apiToken, Metrics: reg}))
	t.Cleanup(api.Close)

	loopback := egress.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}
	pool, err := deliverer.New(store, deliverer.NewSender(2*time.Second, loopback), quiet, deliverer.Options{
		Concurrency: 4, Lease: 5 * time.Second, PollInterval: 20 * time.Millisecond, DrainTimeout: time.Second,
		Metrics: poolMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = pool.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return &gateway{api: api, store: store}
}

// call makes one API request and decodes the JSON object it returns.
func (g *gateway) call(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequestWithContext(context.Background(), method, g.api.URL+path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp.StatusCode, out
}

func (g *gateway) must(t *testing.T, method, path string, body any, want int) map[string]any {
	t.Helper()
	status, out := g.call(t, method, path, body)
	if status != want {
		t.Fatalf("%s %s: status %d, want %d: %v", method, path, status, want, out)
	}
	return out
}

func id(m map[string]any, path ...string) string {
	var cur any = m
	for _, k := range path {
		cur = cur.(map[string]any)[k]
	}
	return fmt.Sprintf("%v", cur)
}

// waitFor polls the delivery until cond holds or 5s pass.
func (g *gateway) waitFor(t *testing.T, deliveryID string, cond func(d map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d := g.must(t, "GET", "/v1/deliveries/"+deliveryID, nil, 200)
		if cond(d) {
			return d
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery %s did not reach the wanted state: %v", deliveryID, d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// customer is the receiving side: the reference receiver with a handler
// that records what it was given and can be told to fail.
type customer struct {
	srv  *httptest.Server
	rc   *receiver.Receiver
	mu   sync.Mutex
	got  []json.RawMessage
	fail bool
}

func startCustomer(t *testing.T, secret string) *customer {
	t.Helper()
	c := &customer{rc: receiver.New([]string{secret}, quiet)}
	c.rc.Handle = func(_ int64, body []byte) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.fail {
			return errors.New("database is down")
		}
		c.got = append(c.got, append(json.RawMessage(nil), body...))
		return nil
	}
	c.srv = httptest.NewServer(c.rc)
	t.Cleanup(c.srv.Close)
	return c
}

func (c *customer) setFail(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fail = v
}

func (c *customer) received() []json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]json.RawMessage(nil), c.got...)
}

func TestEndToEnd(t *testing.T) {
	g := startGateway(t)

	// The customer registers; the gateway hands out the secret once and
	// the customer configures its receiver with it.
	created := g.must(t, "POST", "/v1/endpoints", map[string]any{"url": "http://placeholder.invalid/webhook", "types": []string{"order.paid"}}, 201)
	epID := id(created, "id")
	cust := startCustomer(t, id(created, "secret"))
	g.must(t, "PUT", "/v1/endpoints/"+epID, map[string]any{"url": cust.srv.URL + "/webhook", "types": []string{"order.paid"}, "max_attempts": 3}, 200)

	// 1. A published event reaches the receiver byte for byte, signed
	// so that the receiver's verifier accepts it.
	payload := `{"order":42,"amount":"10.00","items":["a","b"]}`
	pub := g.must(t, "POST", "/v1/events", json.RawMessage(`{"type":"order.paid","payload":`+payload+`}`), 201)
	if pub["deliveries"].(float64) != 1 {
		t.Fatalf("published = %v", pub)
	}
	ev := g.must(t, "GET", "/v1/events/"+id(pub, "event", "id"), nil, 200)
	d1 := id(ev["deliveries"].([]any)[0].(map[string]any), "id")
	done := g.waitFor(t, d1, func(d map[string]any) bool { return d["state"] == "delivered" })
	if done["last_status"].(float64) != 200 || done["attempt"].(float64) != 1 || done["delivered_at"] == nil {
		t.Fatalf("delivered = %v", done)
	}
	if got := cust.received(); len(got) != 1 || string(got[0]) != payload {
		t.Fatalf("receiver got %s, want %s", got, payload)
	}

	// 2. An event the endpoint does not subscribe to produces no delivery.
	skipped := g.must(t, "POST", "/v1/events", map[string]any{"type": "order.refunded", "payload": map[string]any{}}, 201)
	if skipped["deliveries"].(float64) != 0 {
		t.Fatalf("unsubscribed type was fanned out: %v", skipped)
	}

	// 3. The receiver's handler fails: the gateway sees 500, records the
	// excerpt, retries after the backoff and succeeds once it recovers.
	cust.setFail(true)
	pub2 := g.must(t, "POST", "/v1/events", map[string]any{"type": "order.paid", "payload": map[string]any{"order": 43}}, 201)
	ev2 := g.must(t, "GET", "/v1/events/"+id(pub2, "event", "id"), nil, 200)
	d2 := id(ev2["deliveries"].([]any)[0].(map[string]any), "id")
	failing := g.waitFor(t, d2, func(d map[string]any) bool { return d["last_status"] == float64(500) })
	if failing["state"] != "pending" && failing["state"] != "in_flight" || failing["last_error"].(string)[:8] != "HTTP 500" {
		t.Fatalf("after a 500: %v", failing)
	}
	cust.setFail(false)
	recovered := g.waitFor(t, d2, func(d map[string]any) bool { return d["state"] == "delivered" })
	if recovered["attempt"].(float64) < 2 {
		t.Fatalf("recovered without a retry: %v", recovered)
	}
	if got := cust.received(); len(got) != 2 {
		t.Fatalf("receiver handled %d deliveries, want 2 (the 500s ran no handler)", len(got))
	}

	// 4. Replaying a delivered delivery sends it again; the receiver
	// recognises the delivery id and answers 200 without handling it
	// twice, and the gateway records a fresh success.
	g.must(t, "POST", "/v1/deliveries/"+d1+"/replay", nil, 200)
	replayed := g.waitFor(t, d1, func(d map[string]any) bool {
		return d["state"] == "delivered" && d["attempt"] == float64(1)
	})
	if replayed["last_status"].(float64) != 200 {
		t.Fatalf("replayed = %v", replayed)
	}
	if got := cust.received(); len(got) != 2 {
		t.Fatalf("the replay was handled again: %d bodies", len(got))
	}

	// 5. A paused endpoint takes part in no fan-out; resumed, it does.
	g.must(t, "POST", "/v1/endpoints/"+epID+"/pause", nil, 200)
	paused := g.must(t, "POST", "/v1/events", map[string]any{"type": "order.paid", "payload": map[string]any{"order": 44}}, 201)
	if paused["deliveries"].(float64) != 0 {
		t.Fatalf("paused endpoint got a delivery: %v", paused)
	}
	g.must(t, "POST", "/v1/endpoints/"+epID+"/resume", nil, 200)
	resumed := g.must(t, "POST", "/v1/events", map[string]any{"type": "order.paid", "payload": map[string]any{"order": 45}}, 201)
	if resumed["deliveries"].(float64) != 1 {
		t.Fatalf("resumed endpoint got no delivery: %v", resumed)
	}

	// 6. The history tells the story, newest first, with the excerpts.
	deadline := time.Now().Add(5 * time.Second)
	var hist map[string]any
	for {
		hist = g.must(t, "GET", "/v1/endpoints/"+epID+"/deliveries", nil, 200)
		items := hist["items"].([]any)
		if len(items) == 3 && items[0].(map[string]any)["state"] == "delivered" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("history = %v", hist)
		}
		time.Sleep(20 * time.Millisecond)
	}
	detail := g.must(t, "GET", "/v1/endpoints/"+epID, nil, 200)
	if detail["deliveries"].(map[string]any)["delivered"].(float64) != 3 || detail["breaker"].(map[string]any)["status"] != "closed" {
		t.Fatalf("endpoint detail = %v", detail)
	}

	// 7. A rotated secret: the receiver still verifying the old one keeps
	// working through the overlap, since the gateway signs with both.
	rotated := g.must(t, "POST", "/v1/endpoints/"+epID+"/secret", map[string]any{"overlap": "1h"}, 200)
	if rotated["secret"] == created["secret"] {
		t.Fatal("rotation returned the same secret")
	}
	pub3 := g.must(t, "POST", "/v1/events", map[string]any{"type": "order.paid", "payload": map[string]any{"order": 46}}, 201)
	ev3 := g.must(t, "GET", "/v1/events/"+id(pub3, "event", "id"), nil, 200)
	d3 := id(ev3["deliveries"].([]any)[0].(map[string]any), "id")
	g.waitFor(t, d3, func(d map[string]any) bool { return d["state"] == "delivered" })
}

func TestEndToEndBlockedDestination(t *testing.T) {
	g := startGateway(t)
	// The egress policy of the test pool allows loopback only; a private
	// address is refused at the socket and the delivery ends as blocked.
	g.must(t, "POST", "/v1/endpoints", map[string]any{"url": "http://10.255.255.1:9/hook"}, 201)
	pub := g.must(t, "POST", "/v1/events", map[string]any{"type": "x", "payload": map[string]any{}}, 201)
	ev := g.must(t, "GET", "/v1/events/"+id(pub, "event", "id"), nil, 200)
	d := id(ev["deliveries"].([]any)[0].(map[string]any), "id")
	blocked := g.waitFor(t, d, func(d map[string]any) bool { return d["state"] == "blocked" })
	if blocked["attempt"].(float64) != 1 || blocked["last_error"] == "" {
		t.Fatalf("blocked = %v", blocked)
	}
}

// scrape fetches /metrics the way Prometheus does: no token.
func (g *gateway) scrape(t *testing.T) string {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, g.api.URL+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("/metrics: %d, %v", resp.StatusCode, err)
	}
	return string(body)
}

func TestMetricsTellTheStory(t *testing.T) {
	g := startGateway(t)
	created := g.must(t, "POST", "/v1/endpoints", map[string]any{"url": "http://placeholder.invalid/webhook"}, 201)
	epID := id(created, "id")
	cust := startCustomer(t, id(created, "secret"))
	g.must(t, "PUT", "/v1/endpoints/"+epID, map[string]any{"url": cust.srv.URL + "/webhook", "max_attempts": 1}, 200)

	pub := g.must(t, "POST", "/v1/events", map[string]any{"type": "a", "payload": map[string]any{}}, 201)
	ev := g.must(t, "GET", "/v1/events/"+id(pub, "event", "id"), nil, 200)
	g.waitFor(t, id(ev["deliveries"].([]any)[0].(map[string]any), "id"), func(d map[string]any) bool { return d["state"] == "delivered" })

	cust.setFail(true) // one attempt allowed: this one ends in the dead-letter queue
	pub = g.must(t, "POST", "/v1/events", map[string]any{"type": "a", "payload": map[string]any{}}, 201)
	ev = g.must(t, "GET", "/v1/events/"+id(pub, "event", "id"), nil, 200)
	g.waitFor(t, id(ev["deliveries"].([]any)[0].(map[string]any), "id"), func(d map[string]any) bool { return d["state"] == "failed" })

	got := g.scrape(t)
	for _, want := range []string{
		`webhook_attempts_total{endpoint="` + epID + `",outcome="delivered"} 1`,
		`webhook_attempts_total{endpoint="` + epID + `",outcome="retry"} 1`,
		`webhook_deliveries_ended_total{endpoint="` + epID + `",state="delivered"} 1`,
		`webhook_deliveries_ended_total{endpoint="` + epID + `",state="failed"} 1`,
		`webhook_attempt_duration_seconds_count{endpoint="` + epID + `"} 2`,
		`webhook_endpoint_breaker{endpoint="` + epID + `",status="closed"} 1`,
		`webhook_backlog{kind="due"} 0`,
		`http_requests_total{method="POST",route="/v1/events",status="201"} 2`,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if t.Failed() {
		t.Log(got)
	}
}

// metricName finds the series names in a PromQL expression.
var metricName = regexp.MustCompile(`\b((?:webhook|db_pool|http|go|process)_[a-z0-9_]+)\b`)

// TestAlertsUseExportedMetrics keeps deploy/alerts.yml honest: a renamed
// metric or a typo in an alert would otherwise make the alert silently
// never fire.
func TestAlertsUseExportedMetrics(t *testing.T) {
	g := startGateway(t)
	exported := g.scrape(t)
	raw, err := os.ReadFile("../../deploy/alerts.yml")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") || strings.Contains(line, "runbook:") || strings.Contains(line, "summary:") {
			continue
		}
		for _, m := range metricName.FindAllStringSubmatch(line, -1) {
			name := m[1]
			for _, suffix := range []string{"_bucket", "_count", "_sum"} {
				if base, ok := strings.CutSuffix(name, suffix); ok && strings.Contains(exported, "# TYPE "+base+" histogram") {
					name = base
				}
			}
			seen[name] = true
			if !strings.Contains(exported, "# TYPE "+name+" ") {
				t.Errorf("alerts.yml uses %s, which webhookd does not export", m[1])
			}
		}
	}
	if len(seen) < 10 {
		t.Fatalf("found only %d metric names in alerts.yml: %v", len(seen), seen)
	}
}

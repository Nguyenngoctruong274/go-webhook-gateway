package deliverer

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/egress"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

const secret = "whsec_0123456789abcdef0123456789abcdef"

var body = []byte(`{"type":"order.paid","data":{"id":42}}`)

// loopback lets the tests' httptest servers through the egress guard.
var loopback = egress.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}

func newTestSender(timeout time.Duration) *Sender { return NewSender(timeout, loopback) }

func request(url string) Request {
	return Request{DeliveryID: 17, EventType: "order.paid", Body: body, URL: url, Secrets: []string{secret}}
}

func TestSendSignsAndReportsStatus(t *testing.T) {
	var got struct {
		body    []byte
		headers http.Header
		method  string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.body, _ = io.ReadAll(r.Body)
		got.headers = r.Header.Clone()
		got.method = r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	s := newTestSender(2 * time.Second)
	res := s.Send(context.Background(), request(srv.URL+"/hook"))
	if res.Outcome != event.Delivered || res.Status != 204 || res.Error != "" {
		t.Fatalf("result = %+v", res)
	}
	if got.method != http.MethodPost || string(got.body) != string(body) {
		t.Fatalf("receiver saw %s %q", got.method, got.body)
	}
	if ct := got.headers.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if got.headers.Get("User-Agent") != UserAgent || got.headers.Get("Webhook-Event-Type") != "order.paid" {
		t.Fatalf("headers = %v", got.headers)
	}
	// The receiver-side verifier accepts what the sender signed.
	v := signing.Verifier{Secrets: []string{secret}}
	if _, id, err := v.Verify(got.headers.Get, got.body); err != nil || id != 17 {
		t.Fatalf("Verify = %d, %v", id, err)
	}
}

func TestSendClassifiesResponses(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		outcome event.Outcome
		status  int
		errPart string
	}{
		{"200", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }, event.Delivered, 200, ""},
		{"503 with body", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(503)
			_, _ = w.Write([]byte("down for maintenance"))
		}, event.Retry, 503, "HTTP 503: down for maintenance"},
		{"429", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(429) }, event.Retry, 429, "HTTP 429"},
		{"404", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(404) }, event.Rejected, 404, "HTTP 404"},
		{"redirect is not followed", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/elsewhere" {
				t.Error("the sender followed a redirect")
			}
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
		}, event.Rejected, 302, "HTTP 302"},
		{"huge 500 body is cut", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(strings.Repeat("x", 1<<20)))
		}, event.Retry, 500, "HTTP 500: xxxx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			res := newTestSender(2*time.Second).Send(context.Background(), request(srv.URL))
			if res.Outcome != tt.outcome || res.Status != tt.status || !strings.HasPrefix(res.Error, tt.errPart) {
				t.Fatalf("result = %+v, want %v/%d/%q", res, tt.outcome, tt.status, tt.errPart)
			}
			if len(res.Error) > event.MaxErrorBytes {
				t.Fatalf("error excerpt is %d bytes", len(res.Error))
			}
		})
	}
}

func TestSendTransportFailures(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
			w.WriteHeader(200)
		}
	}))
	defer slow.Close()

	start := time.Now()
	res := newTestSender(100*time.Millisecond).Send(context.Background(), request(slow.URL))
	if res.Outcome != event.Retry || res.Status != 0 || !strings.HasPrefix(res.Error, "timeout: ") {
		t.Fatalf("timeout: %+v", res)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("timeout took %v", time.Since(start))
	}

	res = newTestSender(time.Second).Send(context.Background(), request("http://127.0.0.1:1/hook"))
	if res.Outcome != event.Retry || !strings.HasPrefix(res.Error, "transport: ") {
		t.Fatalf("refused: %+v", res)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res = newTestSender(time.Second).Send(ctx, request(slow.URL))
	if res.Outcome != event.Retry || !strings.HasPrefix(res.Error, "canceled: ") {
		t.Fatalf("cancelled: %+v", res)
	}

	res = newTestSender(time.Second).Send(context.Background(), Request{DeliveryID: 1, URL: slow.URL, Body: body})
	if res.Outcome != event.Blocked || !strings.Contains(res.Error, "no secrets") {
		t.Fatalf("no secrets: %+v", res)
	}
	res = newTestSender(time.Second).Send(context.Background(), Request{DeliveryID: 1, URL: "::not a url", Body: body, Secrets: []string{secret}})
	if res.Outcome != event.Blocked {
		t.Fatalf("bad url: %+v", res)
	}
}

func TestSendReusesConnections(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	s := newTestSender(time.Second)
	for i := 0; i < 5; i++ {
		if res := s.Send(context.Background(), request(srv.URL)); res.Outcome != event.Delivered {
			t.Fatalf("send %d: %+v", i, res)
		}
	}
	// Bodies are drained, so the transport can keep the connection.
	if n := conns.Load(); n != 1 {
		t.Fatalf("%d connections for 5 sends, want 1", n)
	}
}

func TestSendBlocksDestinationsTheEgressPolicyRefuses(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()
	port := srv.URL[len("http://127.0.0.1:"):]

	strict := NewSender(time.Second, egress.Policy{})
	for _, url := range []string{srv.URL, "http://localhost:" + port + "/hook", "http://[::1]:" + port + "/hook", "http://169.254.169.254/latest/meta-data/"} {
		res := strict.Send(context.Background(), request(url))
		if res.Outcome != event.Blocked || !strings.Contains(res.Error, "not allowed") {
			t.Fatalf("%s: %+v, want blocked by the egress policy", url, res)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("the guarded sender reached the loopback server %d times", hits.Load())
	}
	// The same request with the loopback allowed goes through, so it was
	// the policy that blocked it, not the URL.
	if res := newTestSender(time.Second).Send(context.Background(), request(srv.URL)); res.Outcome != event.Delivered || hits.Load() != 1 {
		t.Fatalf("permissive sender: %+v, hits %d", res, hits.Load())
	}
	// A port outside the policy's list is refused before any connection.
	ports := NewSender(time.Second, egress.Policy{Allow: loopback.Allow, Ports: []int{443}})
	if res := ports.Send(context.Background(), request(srv.URL)); res.Outcome != event.Blocked || hits.Load() != 1 {
		t.Fatalf("port policy: %+v, hits %d", res, hits.Load())
	}
}

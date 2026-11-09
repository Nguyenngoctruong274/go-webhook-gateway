package deliverer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// BenchmarkSend measures one signed attempt against a loopback receiver
// that answers 204: signing, the request, reading the response, with
// connections reused. It is the gateway's own cost per attempt; a real
// receiver's latency comes on top.
func BenchmarkSend(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	s := newTestSender(5 * time.Second)
	req := request(srv.URL + "/hook")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if res := s.Send(context.Background(), req); res.Outcome != event.Delivered {
			b.Fatalf("result = %+v", res)
		}
	}
}

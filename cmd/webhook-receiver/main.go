// Command webhook-receiver is the reference receiver: what a customer's
// endpoint should do with a delivery from the gateway. It verifies the
// signature, ignores duplicates by delivery id, logs the payload and
// answers 200.
//
//	RECEIVER_SECRETS=whsec_...[,whsec_old...] RECEIVER_ADDR=:9000 webhook-receiver
//
// Two secrets may be listed during a rotation.
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/receiver"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "webhook-receiver:", err)
		os.Exit(1)
	}
}

func run() error {
	var secrets []string
	for _, s := range strings.Split(os.Getenv("RECEIVER_SECRETS"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			if err := signing.ValidateSecret(s); err != nil {
				return fmt.Errorf("RECEIVER_SECRETS: %w", err)
			}
			secrets = append(secrets, s)
		}
	}
	if len(secrets) == 0 {
		return errors.New("RECEIVER_SECRETS: required (comma-separated, at most two during a rotation)")
	}
	addr := os.Getenv("RECEIVER_ADDR")
	if addr == "" {
		addr = ":9000"
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	rc := receiver.New(secrets, logger)
	rc.Handle = func(id int64, body []byte) error {
		logger.Info("event", slog.Int64("delivery_id", id), slog.String("body", string(body)))
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("POST /webhook", rc)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second}
	logger.Info("listening", slog.String("addr", addr), slog.Int("secrets", len(secrets)))
	return srv.ListenAndServe()
}

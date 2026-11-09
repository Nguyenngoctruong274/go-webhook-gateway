// Command webhook-receiver is the reference receiver: what a customer's
// endpoint should do with a delivery from the gateway. It verifies the
// signature, ignores duplicates by delivery id, logs the payload and
// answers 200.
//
//	RECEIVER_SECRETS=whsec_...[,whsec_old...] RECEIVER_ADDR=:9000 webhook-receiver
//	RECEIVER_SECRETS_FILE=/run/secrets/webhook RECEIVER_ADDR=:9000 webhook-receiver
//
// Two secrets may be listed during a rotation. With RECEIVER_SECRETS_FILE
// the file is watched and reloaded when it changes, so a rotation needs
// no restart; until it holds a secret the receiver answers 503 and the
// gateway retries.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/receiver"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "webhook-receiver:", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addr := os.Getenv("RECEIVER_ADDR")
	if addr == "" {
		addr = ":9000"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rc := receiver.New(nil, logger)
	file := os.Getenv("RECEIVER_SECRETS_FILE")
	switch inline := os.Getenv("RECEIVER_SECRETS"); {
	case inline != "" && file != "":
		return errors.New("set RECEIVER_SECRETS or RECEIVER_SECRETS_FILE, not both")
	case inline != "":
		secrets, err := receiver.ParseSecrets(inline)
		if err != nil {
			return fmt.Errorf("RECEIVER_SECRETS: %w", err)
		}
		if len(secrets) == 0 {
			return errors.New("RECEIVER_SECRETS: no secret listed")
		}
		rc.SetSecrets(secrets)
	case file != "":
		go receiver.WatchSecretsFile(ctx, rc, file, 2*time.Second, logger)
	default:
		return errors.New("RECEIVER_SECRETS or RECEIVER_SECRETS_FILE is required")
	}
	rc.Handle = func(id int64, body []byte) error {
		logger.Info("event", slog.Int64("delivery_id", id), slog.String("body", string(body)))
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("POST /webhook", rc)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("listening", slog.String("addr", addr))
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

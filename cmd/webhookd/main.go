// Command webhookd runs the go-webhook-gateway service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/breaker"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/config"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/deliverer"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/egress"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/metrics"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/postgres"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/reaper"
)

var errDraining = errors.New("shutting down")

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "webhookd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancelStart := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStart()
	db, err := postgres.Open(startCtx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	// Instances starting together serialize on an advisory lock, so the
	// schema is migrated once and nobody serves before it is current.
	if err := postgres.Migrate(startCtx, db); err != nil {
		return err
	}
	logger.Info("database ready", slog.Int("max_conns", cfg.DBMaxConns))
	store, err := postgres.NewStore(db, postgres.WithBreaker(breaker.Policy{
		FailureThreshold: cfg.BreakerFailures,
		OpenFor:          cfg.BreakerOpenFor,
		MaxOpenFor:       cfg.BreakerMaxOpenFor,
		ProbeTimeout:     cfg.BreakerProbeTimeout,
	}))
	if err != nil {
		return err
	}

	// One registry for the whole process, served at /metrics.
	reg := metrics.NewRegistry()
	metrics.RegisterRuntime(reg)
	postgres.RegisterPoolMetrics(reg, db)
	store.RegisterQueueMetrics(reg, logger, time.Second)

	// The reaper runs until shutdown and is awaited before the database
	// handle is closed (defers run in reverse order).
	reapCtx, stopReaper := context.WithCancel(context.Background())
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		_ = reaper.New(store, logger, reaper.Options{Interval: cfg.ReapInterval, Metrics: reg}).Run(reapCtx)
	}()
	defer func() {
		stopReaper()
		<-reaperDone
		logger.Info("reaper stopped")
	}()

	// Deliverers start draining as soon as the shutdown signal arrives, in
	// parallel with the HTTP drain, and are awaited before the database
	// handle closes, so every in-flight request is reported.
	if cfg.DeliverConcurrency > 0 {
		pool, err := deliverer.New(store, deliverer.NewSender(cfg.DeliverTimeout, egress.Policy{Allow: cfg.EgressAllow, Ports: cfg.EgressPorts}), logger, deliverer.Options{
			Concurrency:  cfg.DeliverConcurrency,
			Lease:        cfg.DeliverLease,
			DrainTimeout: cfg.DeliverDrain,
			Metrics:      deliverer.NewMetrics(reg),
		})
		if err != nil {
			return err
		}
		poolCtx, stopPool := context.WithCancel(ctx)
		poolDone := make(chan struct{})
		go func() {
			defer close(poolDone)
			_ = pool.Run(poolCtx)
		}()
		defer func() {
			stopPool()
			<-poolDone
		}()
	}

	// Readiness turns false the moment shutdown starts, so load balancers
	// stop routing new requests while in-flight ones finish.
	var draining atomic.Bool
	ready := func(ctx context.Context) error {
		if draining.Load() {
			return errDraining
		}
		return db.PingContext(ctx)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewHandler(logger, httpapi.Options{Ready: ready, Store: store, Token: cfg.APIToken, Metrics: reg}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Info("listening", slog.String("addr", cfg.HTTPAddr))
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutdown started", slog.Duration("drain", cfg.ShutdownDrain))
	draining.Store(true)
	time.Sleep(cfg.ShutdownDrain)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	logger.Info("http server stopped")
	return nil
}

// Package config loads runtime configuration for webhookd from the environment.
package config

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the complete runtime configuration of the service.
type Config struct {
	HTTPAddr string
	// DatabaseURL points at the PostgreSQL database that holds endpoints,
	// events and deliveries.
	DatabaseURL string
	// DBMaxConns caps open database connections; the API and the
	// deliverers share the pool.
	DBMaxConns      int
	ShutdownTimeout time.Duration
	// ShutdownDrain is how long to keep serving with /readyz failing before
	// closing the listener. 0 locally; a few seconds behind a load balancer.
	ShutdownDrain time.Duration
	LogLevel      string

	// DeliverConcurrency is how many requests this instance has in flight
	// at once; 0 runs no deliverers (an API-only instance).
	DeliverConcurrency int
	// DeliverTimeout bounds one request to a receiver, end to end.
	DeliverTimeout time.Duration
	// DeliverLease is how long a claimed delivery stays with this
	// instance without a heartbeat; it must exceed DeliverTimeout.
	DeliverLease time.Duration
	// DeliverDrain is how long shutdown lets in-flight requests finish.
	DeliverDrain time.Duration
	// ReapInterval is how often expired leases are swept.
	ReapInterval time.Duration

	// BreakerFailures consecutive transient failures open an endpoint's
	// circuit breaker.
	BreakerFailures int
	// BreakerOpenFor is the first open period; it doubles after each
	// failed probe up to BreakerMaxOpenFor.
	BreakerOpenFor    time.Duration
	BreakerMaxOpenFor time.Duration
	// BreakerProbeTimeout is how long a half-open probe may stay silent
	// before another probe is allowed.
	BreakerProbeTimeout time.Duration

	// EgressAllow lists networks deliveries may reach even though they
	// are private, loopback or otherwise reserved: receivers inside the
	// same VPC, or the loopback in development. Empty in production.
	EgressAllow []netip.Prefix
	// EgressPorts, when set, is the only ports receivers may listen on.
	EgressPorts []int
}

// Load reads configuration from environment variables, applying defaults
// for optional values. It reports every invalid value at once so that a
// misconfigured deployment fails fast with a complete error message.
func Load() (Config, error) {
	return load(os.Getenv)
}

func load(getenv func(string) string) (Config, error) {
	cfg := Config{
		HTTPAddr:        valueOr(getenv("WH_HTTP_ADDR"), ":8080"),
		DatabaseURL:     getenv("WH_DATABASE_URL"),
		DBMaxConns:      20,
		ShutdownTimeout: 15 * time.Second,
		LogLevel:        valueOr(getenv("WH_LOG_LEVEL"), "info"),

		DeliverConcurrency: 16,
		DeliverTimeout:     10 * time.Second,
		DeliverLease:       60 * time.Second,
		DeliverDrain:       15 * time.Second,
		ReapInterval:       5 * time.Second,

		BreakerFailures:     5,
		BreakerOpenFor:      30 * time.Second,
		BreakerMaxOpenFor:   15 * time.Minute,
		BreakerProbeTimeout: 2 * time.Minute,
	}

	var errs []error

	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("WH_DATABASE_URL: required"))
	} else if u, err := url.Parse(cfg.DatabaseURL); err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" {
		// The URL holds a password, so it is never echoed back.
		errs = append(errs, errors.New("WH_DATABASE_URL: must be a postgres:// or postgresql:// URL with a host"))
	}

	if raw := getenv("WH_DB_MAX_CONNS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			errs = append(errs, errors.New("WH_DB_MAX_CONNS: must be a positive integer"))
		} else {
			cfg.DBMaxConns = n
		}
	}

	if raw := getenv("WH_SHUTDOWN_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("WH_SHUTDOWN_TIMEOUT: %w", err))
		case d <= 0:
			errs = append(errs, errors.New("WH_SHUTDOWN_TIMEOUT: must be positive"))
		default:
			cfg.ShutdownTimeout = d
		}
	}

	if raw := getenv("WH_SHUTDOWN_DRAIN"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			errs = append(errs, errors.New("WH_SHUTDOWN_DRAIN: must be a non-negative duration"))
		} else {
			cfg.ShutdownDrain = d
		}
	}

	if raw := getenv("WH_DELIVER_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > 1000 {
			errs = append(errs, errors.New("WH_DELIVER_CONCURRENCY: must be an integer between 0 and 1000"))
		} else {
			cfg.DeliverConcurrency = n
		}
	}
	if raw := getenv("WH_BREAKER_FAILURES"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			errs = append(errs, errors.New("WH_BREAKER_FAILURES: must be a positive integer"))
		} else {
			cfg.BreakerFailures = n
		}
	}
	for _, d := range []struct {
		name string
		dst  *time.Duration
		min  time.Duration
	}{
		{"WH_DELIVER_TIMEOUT", &cfg.DeliverTimeout, time.Second},
		{"WH_DELIVER_LEASE", &cfg.DeliverLease, time.Second},
		{"WH_DELIVER_DRAIN", &cfg.DeliverDrain, 0},
		{"WH_REAP_INTERVAL", &cfg.ReapInterval, 100 * time.Millisecond},
		{"WH_BREAKER_OPEN_FOR", &cfg.BreakerOpenFor, time.Second},
		{"WH_BREAKER_MAX_OPEN_FOR", &cfg.BreakerMaxOpenFor, time.Second},
		{"WH_BREAKER_PROBE_TIMEOUT", &cfg.BreakerProbeTimeout, time.Second},
	} {
		raw := getenv(d.name)
		if raw == "" {
			continue
		}
		v, err := time.ParseDuration(raw)
		if err != nil || v < d.min {
			errs = append(errs, fmt.Errorf("%s: must be a duration of at least %v", d.name, d.min))
			continue
		}
		*d.dst = v
	}
	if cfg.DeliverLease <= cfg.DeliverTimeout {
		errs = append(errs, errors.New("WH_DELIVER_LEASE: must be longer than WH_DELIVER_TIMEOUT, or a slow receiver could get a request twice"))
	}
	if raw := getenv("WH_EGRESS_ALLOW"); raw != "" {
		for _, item := range strings.Split(raw, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			p, err := netip.ParsePrefix(item)
			if err != nil {
				errs = append(errs, fmt.Errorf("WH_EGRESS_ALLOW: %q is not a CIDR prefix", item))
				continue
			}
			cfg.EgressAllow = append(cfg.EgressAllow, p.Masked())
		}
	}
	if raw := getenv("WH_EGRESS_PORTS"); raw != "" {
		for _, item := range strings.Split(raw, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			n, err := strconv.Atoi(item)
			if err != nil || n < 1 || n > 65535 {
				errs = append(errs, fmt.Errorf("WH_EGRESS_PORTS: %q is not a port", item))
				continue
			}
			cfg.EgressPorts = append(cfg.EgressPorts, n)
		}
	}

	if cfg.BreakerMaxOpenFor < cfg.BreakerOpenFor {
		errs = append(errs, errors.New("WH_BREAKER_MAX_OPEN_FOR: must be at least WH_BREAKER_OPEN_FOR"))
	}
	if cfg.BreakerProbeTimeout <= cfg.DeliverTimeout {
		errs = append(errs, errors.New("WH_BREAKER_PROBE_TIMEOUT: must be longer than WH_DELIVER_TIMEOUT, or a probe still in flight would be duplicated"))
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("WH_LOG_LEVEL: unsupported level %s", strconv.Quote(cfg.LogLevel)))
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

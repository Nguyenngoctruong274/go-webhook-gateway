package config

import (
	"strings"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(envFrom(map[string]string{"WH_DATABASE_URL": "postgres://webhooks:webhooks@localhost:5432/webhooks"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		HTTPAddr:        ":8080",
		DatabaseURL:     "postgres://webhooks:webhooks@localhost:5432/webhooks",
		DBMaxConns:      20,
		ShutdownTimeout: 15 * time.Second,
		LogLevel:        "info",

		DeliverConcurrency: 16,
		DeliverTimeout:     10 * time.Second,
		DeliverLease:       60 * time.Second,
		DeliverDrain:       15 * time.Second,
		ReapInterval:       5 * time.Second,
	}
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(envFrom(map[string]string{
		"WH_HTTP_ADDR":           "127.0.0.1:9090",
		"WH_DATABASE_URL":        "postgresql://u:p@db.internal:5433/webhooks?sslmode=require",
		"WH_DB_MAX_CONNS":        "50",
		"WH_SHUTDOWN_TIMEOUT":    "30s",
		"WH_SHUTDOWN_DRAIN":      "5s",
		"WH_LOG_LEVEL":           "debug",
		"WH_DELIVER_CONCURRENCY": "0",
		"WH_DELIVER_TIMEOUT":     "3s",
		"WH_DELIVER_LEASE":       "20s",
		"WH_DELIVER_DRAIN":       "0s",
		"WH_REAP_INTERVAL":       "1s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" || cfg.ShutdownTimeout != 30*time.Second ||
		cfg.ShutdownDrain != 5*time.Second || cfg.LogLevel != "debug" || cfg.DBMaxConns != 50 ||
		cfg.DeliverConcurrency != 0 || cfg.DeliverTimeout != 3*time.Second || cfg.DeliverLease != 20*time.Second ||
		cfg.DeliverDrain != 0 || cfg.ReapInterval != time.Second {
		t.Fatalf("unexpected config %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string // every substring must appear in the error
	}{
		{name: "missing database url", env: map[string]string{}, want: []string{"WH_DATABASE_URL: required"}},
		{name: "wrong scheme", env: map[string]string{"WH_DATABASE_URL": "mysql://localhost/x"}, want: []string{"WH_DATABASE_URL: must be"}},
		{name: "no host", env: map[string]string{"WH_DATABASE_URL": "postgres://"}, want: []string{"WH_DATABASE_URL: must be"}},
		{
			name: "password is not echoed",
			env:  map[string]string{"WH_DATABASE_URL": "postgres://u:hunter2@"},
			want: []string{"WH_DATABASE_URL: must be"},
		},
		{
			name: "bad numbers and durations",
			env: map[string]string{
				"WH_DATABASE_URL":     "postgres://localhost/webhooks",
				"WH_DB_MAX_CONNS":     "0",
				"WH_SHUTDOWN_TIMEOUT": "0s",
				"WH_SHUTDOWN_DRAIN":   "-1s",
			},
			want: []string{"WH_DB_MAX_CONNS", "WH_SHUTDOWN_TIMEOUT: must be positive", "WH_SHUTDOWN_DRAIN"},
		},
		{
			name: "deliverer settings",
			env: map[string]string{
				"WH_DATABASE_URL":        "postgres://localhost/webhooks",
				"WH_DELIVER_CONCURRENCY": "1001",
				"WH_DELIVER_TIMEOUT":     "500ms",
				"WH_REAP_INTERVAL":       "10ms",
			},
			want: []string{"WH_DELIVER_CONCURRENCY", "WH_DELIVER_TIMEOUT: must be a duration of at least 1s", "WH_REAP_INTERVAL"},
		},
		{
			name: "lease must exceed the timeout",
			env:  map[string]string{"WH_DATABASE_URL": "postgres://localhost/webhooks", "WH_DELIVER_TIMEOUT": "60s"},
			want: []string{"WH_DELIVER_LEASE: must be longer"},
		},
		{
			name: "every error is reported at once",
			env:  map[string]string{"WH_LOG_LEVEL": "loud"},
			want: []string{"WH_DATABASE_URL: required", `WH_LOG_LEVEL: unsupported level "loud"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(envFrom(tt.env))
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Error("error message echoes the password")
			}
		})
	}
}

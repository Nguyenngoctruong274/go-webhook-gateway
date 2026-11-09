package receiver

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/signing"
)

// ParseSecrets splits a list of secrets separated by commas or
// whitespace, checks each and allows at most two: the current one and,
// during a rotation, the previous one. Secrets never contain whitespace,
// so text that is not a list of secrets (a half-written file, the wrong
// file) fails here instead of being taken as one long secret.
func ParseSecrets(raw string) ([]string, error) {
	var out []string
	for _, s := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || unicode.IsSpace(r) }) {
		if err := signing.ValidateSecret(s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if len(out) > 2 {
		return nil, fmt.Errorf("%d secrets listed; at most two (current and previous) are accepted", len(out))
	}
	return out, nil
}

// WatchSecretsFile loads secrets from path into r, then reloads them
// whenever the file's modification time or size changes, until ctx
// ends. A missing file means no secret yet (the receiver answers 503);
// an unreadable or invalid one keeps the secrets already loaded and is
// logged, so a half-written file never locks the receiver out. This is
// how a mounted secret (a Kubernetes Secret, a Docker volume) reaches a
// running receiver, rotations included, without a restart.
func WatchSecretsFile(ctx context.Context, r *Receiver, path string, every time.Duration, logger *slog.Logger) {
	var last fs.FileInfo
	load := func() {
		info, err := os.Stat(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			if last != nil {
				logger.Warn("secrets file removed; keeping the secrets already loaded", slog.String("path", path))
			}
			return
		case err != nil:
			logger.Error("secrets file", slog.String("path", path), slog.Any("error", err))
			return
		case last != nil && info.ModTime().Equal(last.ModTime()) && info.Size() == last.Size():
			return
		}
		raw, err := os.ReadFile(path) // #nosec G304 -- the operator names the file
		if err != nil {
			logger.Error("secrets file", slog.String("path", path), slog.Any("error", err))
			return
		}
		last = info // an invalid file is reported once, then again when it changes
		secrets, err := ParseSecrets(string(raw))
		if err != nil {
			logger.Error("secrets file is invalid; keeping the secrets already loaded", slog.String("path", path), slog.Any("error", err))
			return
		}
		if len(secrets) == 0 {
			return // still empty: being written, or not provisioned yet
		}
		r.SetSecrets(secrets)
		logger.Info("secrets loaded", slog.String("path", path), slog.Int("secrets", len(secrets)))
	}
	load()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			load()
		}
	}
}

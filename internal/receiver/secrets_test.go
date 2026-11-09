package receiver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const secret2 = "whsec_fedcba9876543210fedcba9876543210"

func TestParseSecrets(t *testing.T) {
	for _, tt := range []struct {
		raw  string
		want int
		bad  bool
	}{
		{"", 0, false},
		{secret, 1, false},
		{secret + "\n", 1, false},
		{" " + secret + " , " + secret2 + " ", 2, false},
		{secret + "\r\n" + secret2 + "\n", 2, false},
		{secret + "," + secret2 + "," + secret, 0, true},
		{"short", 0, true},
		{"garbage that is not a secret", 0, true},
		{secret + " " + secret2, 2, false},
	} {
		got, err := ParseSecrets(tt.raw)
		if (err != nil) != tt.bad || len(got) != tt.want {
			t.Errorf("ParseSecrets(%q) = %v, %v", tt.raw, got, err)
		}
	}
}

func TestReceiverWithoutSecretsAsksForRetry(t *testing.T) {
	rc := New(nil, quiet)
	if code := serve(rc, signed(t, []string{secret}, 1, time.Now(), `{}`)); code != 503 {
		t.Fatalf("unconfigured receiver answered %d, want 503 so the gateway retries", code)
	}
	rc.SetSecrets([]string{secret})
	if code := serve(rc, signed(t, []string{secret}, 1, time.Now(), `{}`)); code != 200 {
		t.Fatalf("after SetSecrets: %d", code)
	}
}

// waitCode polls until the receiver answers want for a fresh delivery.
func waitCode(t *testing.T, rc *Receiver, secrets []string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for id := int64(100); ; id++ {
		if serve(rc, signed(t, secrets, id, time.Now(), `{}`)) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("receiver never answered %d", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWatchSecretsFileFollowsTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secrets")
	rc := New(nil, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		WatchSecretsFile(ctx, rc, path, 5*time.Millisecond, quiet)
	}()
	defer func() { cancel(); <-done }()

	// No file yet: 503.
	waitCode(t, rc, []string{secret}, 503)

	// Provisioned: verifies.
	if err := os.WriteFile(path, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitCode(t, rc, []string{secret}, 200)

	// Rotation: both secrets listed, the new one verifies too.
	if err := os.WriteFile(path, []byte(secret2+"\n"+secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitCode(t, rc, []string{secret2}, 200)

	// A broken write keeps the last good secrets.
	if err := os.WriteFile(path, []byte("garbage that is not a secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	waitCode(t, rc, []string{secret2}, 200)

	// The old secret is dropped once the rotation ends.
	if err := os.WriteFile(path, []byte(secret2), 0o600); err != nil {
		t.Fatal(err)
	}
	waitCode(t, rc, []string{secret}, 401)

	// The file disappearing keeps what was loaded.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	waitCode(t, rc, []string{secret2}, 200)
}

package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	secretA = "whsec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	secretB = "whsec_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

var (
	t0   = time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	body = []byte(`{"type":"order.paid","data":{"id":42}}`)
)

func TestSignIsHMACOverTimestampDotBody(t *testing.T) {
	// Independent computation of the documented formula, so a receiver in
	// any language can be written from the README alone.
	mac := hmac.New(sha256.New, []byte(secretA))
	mac.Write([]byte("1793786400." + string(body)))
	want := hex.EncodeToString(mac.Sum(nil))
	if got := Sign(secretA, t0, body); got != want {
		t.Fatalf("Sign = %s, want %s", got, want)
	}
	if Sign(secretA, t0, body) == Sign(secretA, t0.Add(time.Second), body) {
		t.Fatal("timestamp not bound into the signature")
	}
	if Sign(secretA, t0, body) == Sign(secretB, t0, body) {
		t.Fatal("secret not bound into the signature")
	}
}

func headersOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestHeadersAndVerifyRoundTrip(t *testing.T) {
	h, err := Headers([]string{secretA}, 17, t0, body)
	if err != nil {
		t.Fatal(err)
	}
	if h[HeaderTimestamp] != "1793786400" || h[HeaderID] != "17" || !strings.HasPrefix(h[HeaderSignature], "v1=") {
		t.Fatalf("headers = %v", h)
	}
	v := Verifier{Secrets: []string{secretA}, Now: func() time.Time { return t0.Add(30 * time.Second) }}
	ts, id, err := v.Verify(headersOf(h), body)
	if err != nil || !ts.Equal(t0) || id != 17 {
		t.Fatalf("Verify = %v, %d, %v", ts, id, err)
	}
}

func TestVerifyRejects(t *testing.T) {
	good, _ := Headers([]string{secretA}, 17, t0, body)
	now := func() time.Time { return t0.Add(time.Minute) }

	tests := []struct {
		name    string
		mutate  func(h map[string]string) []byte // returns the body to verify
		secrets []string
		want    error
	}{
		{"tampered body", func(_ map[string]string) []byte { return []byte(`{"type":"order.paid","data":{"id":43}}`) }, []string{secretA}, ErrNoMatch},
		{"wrong secret", func(_ map[string]string) []byte { return body }, []string{secretB}, ErrNoMatch},
		{"missing signature", func(h map[string]string) []byte { delete(h, HeaderSignature); return body }, []string{secretA}, ErrMissingHeader},
		{"missing timestamp", func(h map[string]string) []byte { delete(h, HeaderTimestamp); return body }, []string{secretA}, ErrMissingHeader},
		{"missing id", func(h map[string]string) []byte { delete(h, HeaderID); return body }, []string{secretA}, ErrMissingHeader},
		{"bad id", func(h map[string]string) []byte { h[HeaderID] = "-1"; return body }, []string{secretA}, ErrMissingHeader},
		{"garbage timestamp", func(h map[string]string) []byte { h[HeaderTimestamp] = "yesterday"; return body }, []string{secretA}, ErrBadTimestamp},
		{"too old", func(h map[string]string) []byte { h[HeaderTimestamp] = "1793786000"; return body }, []string{secretA}, ErrExpired},
		{"too far in the future", func(h map[string]string) []byte { h[HeaderTimestamp] = "1793786800"; return body }, []string{secretA}, ErrExpired},
		{"moved timestamp with old signature", func(h map[string]string) []byte { h[HeaderTimestamp] = "1793786460"; return body }, []string{secretA}, ErrNoMatch},
		{"unknown scheme only", func(h map[string]string) []byte { h[HeaderSignature] = "v9=" + strings.Repeat("ab", 32); return body }, []string{secretA}, ErrNoMatch},
		{"malformed signature", func(h map[string]string) []byte { h[HeaderSignature] = "v1=zz"; return body }, []string{secretA}, ErrNoMatch},
		{"truncated signature", func(h map[string]string) []byte { h[HeaderSignature] = h[HeaderSignature][:20]; return body }, []string{secretA}, ErrNoMatch},
		{"receiver secret too short", func(_ map[string]string) []byte { return body }, []string{"short"}, ErrNoMatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := make(map[string]string, len(good))
			for k, v := range good {
				h[k] = v
			}
			b := tt.mutate(h)
			_, _, err := Verifier{Secrets: tt.secrets, Now: now}.Verify(headersOf(h), b)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestVerifyAcceptsAnyMatchingSignatureAndIgnoresUnknownSchemes(t *testing.T) {
	// The gateway signs with both secrets during a rotation and may add a
	// scheme the receiver does not know; the receiver needs only one match.
	h, _ := Headers([]string{secretA, secretB}, 1, t0, body)
	h[HeaderSignature] = "v2=deadbeef, " + h[HeaderSignature]
	now := func() time.Time { return t0 }
	for _, secrets := range [][]string{{secretA}, {secretB}, {secretB, secretA}} {
		if _, _, err := (Verifier{Secrets: secrets, Now: now}).Verify(headersOf(h), body); err != nil {
			t.Fatalf("secrets %v: %v", secrets, err)
		}
	}
}

func TestRotation(t *testing.T) {
	r := Rotation{Current: secretA}
	if got := r.Active(t0); len(got) != 1 || got[0] != secretA {
		t.Fatalf("Active before rotation = %v", got)
	}
	r, err := r.Rotate(secretB, t0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Active(t0.Add(59 * time.Minute)); len(got) != 2 || got[0] != secretB || got[1] != secretA {
		t.Fatalf("Active during overlap = %v, want new first then old", got)
	}
	if got := r.Active(t0.Add(time.Hour)); len(got) != 1 || got[0] != secretB {
		t.Fatalf("Active after overlap = %v", got)
	}

	// Receivers that switched and those that did not both verify during
	// the overlap; only the switched one does afterwards.
	during, _ := Headers(r.Active(t0.Add(time.Minute)), 1, t0.Add(time.Minute), body)
	now := func() time.Time { return t0.Add(time.Minute) }
	for _, s := range []string{secretA, secretB} {
		if _, _, err := (Verifier{Secrets: []string{s}, Now: now}).Verify(headersOf(during), body); err != nil {
			t.Fatalf("during overlap, receiver on %s: %v", s[:8], err)
		}
	}
	after, _ := Headers(r.Active(t0.Add(2*time.Hour)), 1, t0.Add(2*time.Hour), body)
	later := func() time.Time { return t0.Add(2 * time.Hour) }
	if _, _, err := (Verifier{Secrets: []string{secretA}, Now: later}).Verify(headersOf(after), body); !errors.Is(err, ErrNoMatch) {
		t.Fatalf("after overlap, old secret still verifies: %v", err)
	}
	if _, _, err := (Verifier{Secrets: []string{secretB}, Now: later}).Verify(headersOf(after), body); err != nil {
		t.Fatalf("after overlap, new secret: %v", err)
	}

	// A second rotation during the first drops the oldest secret.
	const secretC = "whsec_cccccccccccccccccccccccccccccccc"
	r2, err := r.Rotate(secretC, t0.Add(time.Minute), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := r2.Active(t0.Add(2 * time.Minute)); len(got) != 2 || got[0] != secretC || got[1] != secretB {
		t.Fatalf("Active after double rotation = %v", got)
	}

	for name, bad := range map[string]func() error{
		"same secret":      func() error { _, err := r.Rotate(secretB, t0, time.Hour); return err },
		"short secret":     func() error { _, err := r.Rotate("short", t0, time.Hour); return err },
		"negative overlap": func() error { _, err := r.Rotate(secretC, t0, -time.Second); return err },
	} {
		if err := bad(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestNewSecretAndValidate(t *testing.T) {
	a, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewSecret()
	if !strings.HasPrefix(a, "whsec_") || len(a) != 6+2*SecretBytes || a == b {
		t.Fatalf("NewSecret = %q, %q", a, b)
	}
	if err := ValidateSecret(a); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "tooshort", " " + secretA, secretA + "\n"} {
		if err := ValidateSecret(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := Headers(nil, 1, t0, body); err == nil {
		t.Fatal("Headers with no secrets accepted")
	}
	if _, err := Headers([]string{"short"}, 1, t0, body); err == nil {
		t.Fatal("Headers with a short secret accepted")
	}
}

// FuzzVerify feeds random headers and bodies to the verifier: it must
// never accept a request whose signature was not made with its secret,
// and must always accept one that was.
func FuzzVerify(f *testing.F) {
	f.Add([]byte("{}"), "v1=00", "1793872800", "1")
	f.Add(body, "", "", "")
	f.Fuzz(func(t *testing.T, b []byte, sig, ts, id string) {
		now := func() time.Time { return t0 }
		v := Verifier{Secrets: []string{secretA}, Now: now}
		h := map[string]string{HeaderSignature: sig, HeaderTimestamp: ts, HeaderID: id}
		if _, _, err := v.Verify(headersOf(h), b); err == nil {
			// Accepted: then the signature must be one we could have
			// produced for that timestamp (within tolerance of t0).
			unix, _ := strconv.ParseInt(ts, 10, 64)
			if want := Sign(secretA, time.Unix(unix, 0), b); !strings.Contains(sig, want) {
				t.Fatalf("accepted a forged signature %q for body %q at %s", sig, b, ts)
			}
		}
		good, _ := Headers([]string{secretA}, 5, t0, b)
		if _, _, err := v.Verify(headersOf(good), b); err != nil {
			t.Fatalf("rejected a genuine request: %v", err)
		}
	})
}

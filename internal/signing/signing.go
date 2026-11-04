// Package signing authenticates webhook requests. The gateway signs
// "<timestamp>.<body>" with HMAC-SHA256 under the endpoint's secret and
// sends the result in headers; a receiver recomputes it and rejects
// forgeries and replays. Secrets rotate with an overlap: for a while the
// gateway signs with both the new and the old secret, so a receiver that
// has not switched yet keeps accepting deliveries.
package signing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Header names. A receiver needs all three.
const (
	// HeaderSignature carries one or more "v1=<hex>" signatures separated
	// by commas: one per active secret.
	HeaderSignature = "Webhook-Signature"
	// HeaderTimestamp is the Unix time in seconds the request was signed.
	HeaderTimestamp = "Webhook-Timestamp"
	// HeaderID is the delivery id. A receiver can use it to ignore a
	// retry of a request it already processed.
	HeaderID = "Webhook-Id"
)

// Scheme is the version prefix of a signature. A future change to what
// is signed gets a new prefix; receivers ignore prefixes they do not know.
const Scheme = "v1"

// SecretBytes is the length of generated secrets: 32 bytes, 256 bits.
const SecretBytes = 32

// MinSecretBytes is the shortest secret accepted. HMAC-SHA256 is only as
// strong as its key; 16 bytes is the floor, 32 the default.
const MinSecretBytes = 16

// DefaultTolerance is how far a timestamp may be from the receiver's
// clock. Five minutes covers clock skew and a slow delivery; a replay
// older than that is refused.
const DefaultTolerance = 5 * time.Minute

// Errors a receiver reports. They are deliberately coarse: a verifier
// should answer 401 for all of them and log the detail.
var (
	ErrMissingHeader = errors.New("missing signature header")
	ErrBadTimestamp  = errors.New("bad signature timestamp")
	ErrExpired       = errors.New("signature timestamp outside tolerance")
	ErrNoMatch       = errors.New("no signature matches")
)

// NewSecret returns a random secret as a "whsec_" prefixed string. The
// prefix makes a leaked value recognisable in logs and scanners; the body
// is base64url-free hex so it survives any config format.
func NewSecret() (string, error) {
	var b [SecretBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate secret: %w", err)
	}
	return "whsec_" + hex.EncodeToString(b[:]), nil
}

// ValidateSecret checks that a secret is usable: long enough and without
// surrounding whitespace, the two mistakes that make signatures silently
// never match.
func ValidateSecret(secret string) error {
	if strings.TrimSpace(secret) != secret {
		return errors.New("secret has leading or trailing whitespace")
	}
	if len(secret) < MinSecretBytes {
		return fmt.Errorf("secret is %d bytes, at least %d required", len(secret), MinSecretBytes)
	}
	return nil
}

// Sign computes the v1 signature of body at time ts under secret.
func Sign(secret string, ts time.Time, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload(ts, body))
	return hex.EncodeToString(mac.Sum(nil))
}

// payload is what gets signed: the timestamp, a dot, the body. Binding
// the timestamp into the MAC is what lets the receiver reject replays:
// an attacker cannot move an old signature to a fresh timestamp.
func payload(ts time.Time, body []byte) []byte {
	p := make([]byte, 0, 24+len(body))
	p = strconv.AppendInt(p, ts.Unix(), 10)
	p = append(p, '.')
	return append(p, body...)
}

// Headers returns the three headers for a request, signed with every
// secret in secrets (the current one first, then any still-overlapping
// old one). An empty secrets list is a programming error.
func Headers(secrets []string, deliveryID int64, ts time.Time, body []byte) (map[string]string, error) {
	if len(secrets) == 0 {
		return nil, errors.New("sign: no secrets")
	}
	sigs := make([]string, 0, len(secrets))
	for _, s := range secrets {
		if err := ValidateSecret(s); err != nil {
			return nil, fmt.Errorf("sign: %w", err)
		}
		sigs = append(sigs, Scheme+"="+Sign(s, ts, body))
	}
	return map[string]string{
		HeaderSignature: strings.Join(sigs, ","),
		HeaderTimestamp: strconv.FormatInt(ts.Unix(), 10),
		HeaderID:        strconv.FormatInt(deliveryID, 10),
	}, nil
}

// Verifier checks incoming requests on the receiver's side.
type Verifier struct {
	// Secrets the receiver accepts: its current one and, during a
	// rotation, the previous one.
	Secrets []string
	// Tolerance for the timestamp; DefaultTolerance when zero.
	Tolerance time.Duration
	// Now overrides the clock in tests.
	Now func() time.Time
}

// Verify checks the headers of a request against its body. It returns
// the timestamp and the delivery id when the request is authentic.
//
// The timestamp is checked before any signature is computed, so an
// attacker cannot use the verifier as a signing oracle on stale inputs,
// and signature comparison is constant-time.
func (v Verifier) Verify(header func(string) string, body []byte) (ts time.Time, deliveryID int64, err error) {
	sigHeader, tsHeader, idHeader := header(HeaderSignature), header(HeaderTimestamp), header(HeaderID)
	if sigHeader == "" || tsHeader == "" || idHeader == "" {
		return time.Time{}, 0, ErrMissingHeader
	}
	unix, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return time.Time{}, 0, ErrBadTimestamp
	}
	deliveryID, err = strconv.ParseInt(idHeader, 10, 64)
	if err != nil || deliveryID <= 0 {
		return time.Time{}, 0, fmt.Errorf("%w: bad delivery id", ErrMissingHeader)
	}
	ts = time.Unix(unix, 0)
	now := time.Now()
	if v.Now != nil {
		now = v.Now()
	}
	tol := v.Tolerance
	if tol == 0 {
		tol = DefaultTolerance
	}
	if d := now.Sub(ts); d > tol || d < -tol {
		return time.Time{}, 0, ErrExpired
	}

	for _, part := range strings.Split(sigHeader, ",") {
		scheme, hexSig, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || scheme != Scheme {
			continue
		}
		got, err := hex.DecodeString(hexSig)
		if err != nil || len(got) != sha256.Size {
			continue
		}
		for _, secret := range v.Secrets {
			if ValidateSecret(secret) != nil {
				continue
			}
			mac := hmac.New(sha256.New, []byte(secret))
			mac.Write(payload(ts, body))
			if hmac.Equal(got, mac.Sum(nil)) {
				return ts, deliveryID, nil
			}
		}
	}
	return time.Time{}, 0, ErrNoMatch
}

// Rotation describes an endpoint's secrets at a point in time.
type Rotation struct {
	// Current is the secret new signatures use.
	Current string
	// Previous is the retired secret, still used alongside Current until
	// PreviousUntil; empty when there is no rotation in progress.
	Previous      string
	PreviousUntil time.Time
}

// Active returns the secrets to sign with at time now: the current one,
// plus the previous one while its overlap lasts.
func (r Rotation) Active(now time.Time) []string {
	if r.Previous != "" && now.Before(r.PreviousUntil) {
		return []string{r.Current, r.Previous}
	}
	return []string{r.Current}
}

// Rotate retires the current secret in favour of next, keeping it active
// for overlap. A rotation started while one is in progress drops the
// older secret at once: at most two secrets are ever active, so a
// receiver never has to accept more than two.
func (r Rotation) Rotate(next string, now time.Time, overlap time.Duration) (Rotation, error) {
	if err := ValidateSecret(next); err != nil {
		return r, fmt.Errorf("rotate: new %w", err)
	}
	if overlap < 0 {
		return r, errors.New("rotate: overlap must not be negative")
	}
	if next == r.Current {
		return r, errors.New("rotate: new secret equals the current one")
	}
	return Rotation{Current: next, Previous: r.Current, PreviousUntil: now.Add(overlap)}, nil
}

package event

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestValidateType(t *testing.T) {
	for _, ok := range []string{"a", "order.paid", "user_signed-up.v2", strings.Repeat("x", MaxTypeLen)} {
		if err := ValidateType(ok); err != nil {
			t.Errorf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Order.Paid", ".paid", "order paid", "order/paid", strings.Repeat("x", MaxTypeLen+1), "ünïcode"} {
		if err := ValidateType(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q accepted (%v)", bad, err)
		}
	}
}

func TestEventSpecNormalize(t *testing.T) {
	tests := []struct {
		name    string
		spec    Spec
		want    string // payload after normalization
		wantErr []string
	}{
		{name: "defaults", spec: Spec{Type: "order.paid"}, want: "{}"},
		{name: "object", spec: Spec{Type: "t", Payload: []byte(` {"a":1} `)}, want: ` {"a":1} `},
		{name: "array", spec: Spec{Type: "t", Payload: []byte(`[1,2]`)}, want: `[1,2]`},
		{name: "key", spec: Spec{Type: "t", IdempotencyKey: "order-42"}, want: "{}"},
		{name: "scalar payload", spec: Spec{Type: "t", Payload: []byte(`"hi"`)}, wantErr: []string{"object or array"}},
		{name: "invalid json", spec: Spec{Type: "t", Payload: []byte(`{`)}, wantErr: []string{"not valid JSON"}},
		{name: "too large", spec: Spec{Type: "t", Payload: append([]byte(`["`), append(make([]byte, MaxPayloadBytes), '"', ']')...)}, wantErr: []string{"bytes, at most"}},
		{name: "bad key", spec: Spec{Type: "t", IdempotencyKey: "a\x00b"}, wantErr: []string{"idempotency key"}},
		{name: "everything at once", spec: Spec{Type: "", Payload: []byte("x"), IdempotencyKey: strings.Repeat("k", 201)},
			wantErr: []string{"type", "not valid JSON", "idempotency key"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.spec.Normalize()
			if len(tt.wantErr) > 0 {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("error = %v, want ErrInvalid", err)
				}
				for _, w := range tt.wantErr {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("error %q lacks %q", err, w)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(got.Payload) != tt.want {
				t.Fatalf("payload %q, want %q", got.Payload, tt.want)
			}
		})
	}
}

func TestEndpointSpecNormalize(t *testing.T) {
	tests := []struct {
		name    string
		spec    EndpointSpec
		want    EndpointSpec
		wantErr []string
	}{
		{
			name: "defaults and dedup",
			spec: EndpointSpec{URL: " https://hooks.example.com/in ", Types: []string{"b", "a", "b"}},
			want: EndpointSpec{URL: "https://hooks.example.com/in", Types: []string{"b", "a"}, MaxAttempts: DefaultMaxAttempts},
		},
		{
			name: "all types, explicit attempts",
			spec: EndpointSpec{URL: "http://localhost:9000/hook", MaxAttempts: 3, Description: "staging"},
			want: EndpointSpec{URL: "http://localhost:9000/hook", Description: "staging", MaxAttempts: 3},
		},
		{
			name: "rate limit and ordering",
			spec: EndpointSpec{URL: "https://example.com", RateLimit: 50, Ordered: true},
			want: EndpointSpec{URL: "https://example.com", MaxAttempts: DefaultMaxAttempts, RateLimit: 50, Ordered: true},
		},
		{name: "rate limit too high", spec: EndpointSpec{URL: "https://example.com", RateLimit: MaxRateLimit + 1}, wantErr: []string{"rate limit must be between 0 and 1000"}},
		{name: "negative rate limit", spec: EndpointSpec{URL: "https://example.com", RateLimit: -1}, wantErr: []string{"rate limit"}},
		{name: "no url", spec: EndpointSpec{}, wantErr: []string{"url is required"}},
		{name: "ftp", spec: EndpointSpec{URL: "ftp://example.com/x"}, wantErr: []string{"scheme"}},
		{name: "no host", spec: EndpointSpec{URL: "https:///path"}, wantErr: []string{"host"}},
		{name: "credentials", spec: EndpointSpec{URL: "https://user:pw@example.com/x"}, wantErr: []string{"credentials"}},
		{name: "fragment", spec: EndpointSpec{URL: "https://example.com/x#frag"}, wantErr: []string{"fragment"}},
		{name: "too long", spec: EndpointSpec{URL: "https://example.com/" + strings.Repeat("a", MaxURLLen)}, wantErr: []string{"at most"}},
		{name: "bad type and attempts", spec: EndpointSpec{URL: "https://example.com", Types: []string{"OK"}, MaxAttempts: 99},
			wantErr: []string{`type "OK"`, "max attempts must be between 1 and 20"}},
		{name: "description with NUL", spec: EndpointSpec{URL: "https://example.com", Description: "a\x00"}, wantErr: []string{"description"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.spec.Normalize()
			if len(tt.wantErr) > 0 {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("error = %v, want ErrInvalid", err)
				}
				for _, w := range tt.wantErr {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("error %q lacks %q", err, w)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.URL != tt.want.URL || got.Description != tt.want.Description || got.MaxAttempts != tt.want.MaxAttempts ||
				!slices.Equal(got.Types, tt.want.Types) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestEndpointSubscribed(t *testing.T) {
	all := Endpoint{}
	some := Endpoint{Types: []string{"order.paid", "order.refunded"}}
	if !all.Subscribed("anything") || !some.Subscribed("order.paid") || some.Subscribed("user.created") {
		t.Fatal("subscription check is wrong")
	}
}

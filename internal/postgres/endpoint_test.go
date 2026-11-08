package postgres

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

func TestCreateEndpoint(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ep, secret, err := s.CreateEndpoint(ctx, event.EndpointSpec{
		URL:         "  https://receiver.example/hook ",
		Description: "orders",
		Types:       []string{"order.paid", "order.paid", "order.refunded"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ep.URL != "https://receiver.example/hook" || ep.Description != "orders" || !ep.Enabled {
		t.Fatalf("endpoint = %+v", ep)
	}
	if want := []string{"order.paid", "order.refunded"}; !slices.Equal(ep.Types, want) {
		t.Fatalf("types = %q, want %q (deduplicated)", ep.Types, want)
	}
	if ep.MaxAttempts != event.DefaultMaxAttempts {
		t.Fatalf("max attempts = %d, want the default %d", ep.MaxAttempts, event.DefaultMaxAttempts)
	}
	if !strings.HasPrefix(secret, "whsec_") {
		t.Fatalf("secret %q does not look generated", secret)
	}
	got, err := s.GetEndpoint(ctx, ep.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != ep.ID || got.URL != ep.URL || !slices.Equal(got.Types, ep.Types) || !got.CreatedAt.Equal(ep.CreatedAt) {
		t.Fatalf("Get = %+v, want %+v", got, ep)
	}
	rot, err := s.Secrets(ctx, ep.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rot.Current != secret || rot.Previous != "" || !rot.PreviousUntil.IsZero() {
		t.Fatalf("secrets = %+v, want only the created secret", rot)
	}
}

func TestCreateEndpointRefusesInvalidSpecWithoutWriting(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _, err := s.CreateEndpoint(ctx, event.EndpointSpec{URL: "ftp://files.example", MaxAttempts: 99})
	assertIs(t, err, event.ErrInvalid)
	if !strings.Contains(err.Error(), "scheme") || !strings.Contains(err.Error(), "max attempts") {
		t.Fatalf("error %q should report every problem", err)
	}
	eps, err := s.ListEndpoints(ctx)
	if err != nil || len(eps) != 0 {
		t.Fatalf("endpoints = %v, %v; want none", eps, err)
	}
}

func TestEndpointNotFound(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, err := s.GetEndpoint(ctx, 404)
	assertIs(t, err, event.ErrNotFound)
	_, err = s.UpdateEndpoint(ctx, 404, event.EndpointSpec{URL: "https://x.example"})
	assertIs(t, err, event.ErrNotFound)
	_, err = s.SetEndpointEnabled(ctx, 404, false)
	assertIs(t, err, event.ErrNotFound)
	_, err = s.Secrets(ctx, 404)
	assertIs(t, err, event.ErrNotFound)
	_, err = s.RotateSecret(ctx, 404, time.Hour)
	assertIs(t, err, event.ErrNotFound)
}

func TestListEndpointsOldestFirst(t *testing.T) {
	s := newStore(t)
	a, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://a.example"})
	b, _ := mustEndpoint(t, s, event.EndpointSpec{URL: "https://b.example"})
	eps, err := s.ListEndpoints(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(eps) != 2 || eps[0].ID != a.ID || eps[1].ID != b.ID {
		t.Fatalf("list = %+v, want a then b", eps)
	}
}

func TestUpdateEndpoint(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ep, secret := mustEndpoint(t, s, event.EndpointSpec{Types: []string{"order.paid"}})

	_, err := s.UpdateEndpoint(ctx, ep.ID, event.EndpointSpec{URL: "not a url"})
	assertIs(t, err, event.ErrInvalid)

	got, err := s.UpdateEndpoint(ctx, ep.ID, event.EndpointSpec{
		URL: "https://receiver.example/v2", Description: "moved", MaxAttempts: 3, RateLimit: 20, Ordered: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "https://receiver.example/v2" || got.Description != "moved" || got.MaxAttempts != 3 || len(got.Types) != 0 ||
		got.RateLimit != 20 || !got.Ordered {
		t.Fatalf("updated = %+v", got)
	}
	if got.UpdatedAt.Before(ep.UpdatedAt) || !got.CreatedAt.Equal(ep.CreatedAt) {
		t.Fatalf("updated_at %s, created_at %s; want updated_at moved and created_at kept (%s, %s)",
			got.UpdatedAt, got.CreatedAt, ep.UpdatedAt, ep.CreatedAt)
	}
	// Updating the spec does not touch the secret.
	rot, err := s.Secrets(ctx, ep.ID)
	if err != nil || rot.Current != secret {
		t.Fatalf("secrets after update = %+v, %v; want %q kept", rot, err, secret)
	}
}

func TestSetEndpointEnabled(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ep, _ := mustEndpoint(t, s, event.EndpointSpec{})
	got, err := s.SetEndpointEnabled(ctx, ep.ID, false)
	if err != nil || got.Enabled {
		t.Fatalf("after pause: %+v, %v", got, err)
	}
	got, err = s.SetEndpointEnabled(ctx, ep.ID, true)
	if err != nil || !got.Enabled {
		t.Fatalf("after resume: %+v, %v", got, err)
	}
}

func TestRotateSecretKeepsTheOldOneForTheOverlap(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ep, first := mustEndpoint(t, s, event.EndpointSpec{})

	before := dbNow(t, s)
	second, err := s.RotateSecret(ctx, ep.ID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if second == first || !strings.HasPrefix(second, "whsec_") {
		t.Fatalf("rotated secret %q, previous %q", second, first)
	}
	rot, err := s.Secrets(ctx, ep.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rot.Current != second || rot.Previous != first {
		t.Fatalf("secrets = %+v, want current %q, previous %q", rot, second, first)
	}
	assertNear(t, "previous_until", rot.PreviousUntil, before.Add(time.Hour), 5*time.Second)
	if active := rot.Active(before); !slices.Equal(active, []string{second, first}) {
		t.Fatalf("active during overlap = %q, want both", active)
	}
	if active := rot.Active(before.Add(2 * time.Hour)); !slices.Equal(active, []string{second}) {
		t.Fatalf("active after overlap = %q, want the new one only", active)
	}

	// A second rotation retires the first secret for good: at most two
	// are ever active.
	third, err := s.RotateSecret(ctx, ep.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	rot, err = s.Secrets(ctx, ep.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rot.Current != third || rot.Previous != second {
		t.Fatalf("secrets after second rotation = %+v, want current %q, previous %q", rot, third, second)
	}
	if active := rot.Active(dbNow(t, s)); !slices.Equal(active, []string{third}) {
		t.Fatalf("active with zero overlap = %q, want the new one only", active)
	}

	_, err = s.RotateSecret(ctx, ep.ID, -time.Second)
	if err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("error = %v, want a refused negative overlap", err)
	}
}

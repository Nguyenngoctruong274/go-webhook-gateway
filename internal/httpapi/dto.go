package httpapi

import (
	"encoding/json"
	"time"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/breaker"
	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/event"
)

// The wire shapes of the API. They are separate from the domain types so
// that renaming a field in Go never changes the JSON customers parse,
// and so that times are RFC 3339 strings and absent times are null.

type endpointResponse struct {
	ID          int64     `json:"id"`
	URL         string    `json:"url"`
	Description string    `json:"description"`
	Types       []string  `json:"types"`
	Enabled     bool      `json:"enabled"`
	MaxAttempts int       `json:"max_attempts"`
	RateLimit   int       `json:"rate_limit"`
	Ordered     bool      `json:"ordered"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// createdEndpoint carries the secret, shown once.
type createdEndpoint struct {
	endpointResponse
	Secret string `json:"secret"`
}

type endpointDetail struct {
	endpointResponse
	Breaker    breakerResponse     `json:"breaker"`
	Deliveries map[event.State]int `json:"deliveries"`
}

type breakerResponse struct {
	Status      breaker.Status `json:"status"`
	Failures    int            `json:"failures"`
	Opens       int            `json:"opens"`
	OpenedUntil *time.Time     `json:"opened_until"`
	ProbeUntil  *time.Time     `json:"probe_until"`
}

type eventResponse struct {
	ID             int64           `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

type publishedResponse struct {
	Event      eventResponse `json:"event"`
	Created    bool          `json:"created"`
	Deliveries int           `json:"deliveries"`
}

type eventDetail struct {
	eventResponse
	Deliveries []deliveryResponse `json:"deliveries"`
}

type deliveryResponse struct {
	ID            int64       `json:"id"`
	EventID       int64       `json:"event_id"`
	EndpointID    int64       `json:"endpoint_id"`
	State         event.State `json:"state"`
	Attempt       int         `json:"attempt"`
	MaxAttempts   int         `json:"max_attempts"`
	NextAttemptAt *time.Time  `json:"next_attempt_at"`
	LeaseUntil    *time.Time  `json:"lease_until"`
	LastStatus    int         `json:"last_status"`
	LastError     string      `json:"last_error"`
	DeliveredAt   *time.Time  `json:"delivered_at"`
	CreatedAt     time.Time   `json:"created_at"`
}

type listResponse[T any] struct {
	Items []T `json:"items"`
	// NextBefore is the "before" to pass for the next page, 0 at the end.
	NextBefore int64 `json:"next_before,omitempty"`
}

func toEndpoint(e event.Endpoint) endpointResponse {
	types := e.Types
	if types == nil {
		types = []string{}
	}
	return endpointResponse{ID: e.ID, URL: e.URL, Description: e.Description, Types: types, Enabled: e.Enabled,
		MaxAttempts: e.MaxAttempts, RateLimit: e.RateLimit, Ordered: e.Ordered, CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt}
}

func toBreaker(s breaker.State) breakerResponse {
	return breakerResponse{Status: s.Status, Failures: s.Failures, Opens: s.Opens,
		OpenedUntil: optTime(s.OpenedUntil), ProbeUntil: optTime(s.ProbeUntil)}
}

func toEvent(e event.Event) eventResponse {
	return eventResponse{ID: e.ID, Type: e.Type, Payload: json.RawMessage(e.Payload), IdempotencyKey: e.IdempotencyKey, CreatedAt: e.CreatedAt}
}

func toDelivery(d event.Delivery) deliveryResponse {
	var next *time.Time
	if d.State == event.StatePending {
		next = optTime(d.NextAttemptAt)
	}
	return deliveryResponse{ID: d.ID, EventID: d.EventID, EndpointID: d.EndpointID, State: d.State,
		Attempt: d.Attempt, MaxAttempts: d.MaxAttempts, NextAttemptAt: next, LeaseUntil: optTime(d.LeaseUntil),
		LastStatus: d.LastStatus, LastError: d.LastError, DeliveredAt: optTime(d.DeliveredAt), CreatedAt: d.CreatedAt}
}

func optTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

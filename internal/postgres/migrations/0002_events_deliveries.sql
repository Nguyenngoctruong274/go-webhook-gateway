-- Endpoints gain what the domain's EndpointSpec carries (subscribed
-- types, attempt limit) and the signing secrets the deliverer needs. The
-- day-1 schema never had a writer, so the secret constraint is safe to
-- add as is.
ALTER TABLE endpoints
    ADD COLUMN types                 TEXT[]      NOT NULL DEFAULT '{}',
    ADD COLUMN max_attempts          INT         NOT NULL DEFAULT 8 CHECK (max_attempts BETWEEN 1 AND 20),
    ADD COLUMN secret                TEXT        NOT NULL DEFAULT '',
    ADD COLUMN previous_secret       TEXT        NOT NULL DEFAULT '',
    ADD COLUMN previous_secret_until TIMESTAMPTZ;
ALTER TABLE endpoints
    ALTER COLUMN secret DROP DEFAULT,
    ADD CONSTRAINT endpoints_secret_set CHECK (secret <> ''),
    ADD CONSTRAINT endpoints_previous_secret_has_end
        CHECK ((previous_secret = '') = (previous_secret_until IS NULL));

-- Events are published once and never change. The payload is the json
-- type, not jsonb: it keeps the producer's bytes as they were, so the
-- body a receiver gets is byte for byte what was published.
CREATE TABLE events (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type            TEXT        NOT NULL CHECK (type ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    payload         JSON        NOT NULL,
    idempotency_key TEXT        CHECK (octet_length(idempotency_key) BETWEEN 1 AND 200),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A key is taken for as long as its event exists, so a producer that
-- retries a publish after a timeout gets the event it already made.
CREATE UNIQUE INDEX events_idempotency ON events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- One row per event × subscribed endpoint, created at fan-out time. The
-- CHECK constraints restate the invariants of internal/event, so a bug in
-- a query cannot store a delivery the state machine would never produce.
CREATE TABLE deliveries (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id        BIGINT      NOT NULL REFERENCES events (id),
    endpoint_id     BIGINT      NOT NULL REFERENCES endpoints (id),
    state           TEXT        NOT NULL DEFAULT 'pending' CHECK (state IN
                    ('pending', 'in_flight', 'delivered', 'failed', 'rejected', 'blocked', 'cancelled')),
    attempt         INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until     TIMESTAMPTZ,
    last_status     INT         NOT NULL DEFAULT 0 CHECK (last_status BETWEEN 0 AND 599),
    last_error      TEXT        NOT NULL DEFAULT '' CHECK (octet_length(last_error) <= 1024),
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT deliveries_one_per_endpoint   UNIQUE (event_id, endpoint_id),
    CONSTRAINT deliveries_attempt_range      CHECK (attempt BETWEEN 0 AND max_attempts),
    CONSTRAINT deliveries_lease_iff_in_flight CHECK ((state = 'in_flight') = (lease_until IS NOT NULL)),
    CONSTRAINT deliveries_pending_has_attempts CHECK (state <> 'pending' OR attempt < max_attempts),
    CONSTRAINT deliveries_in_flight_has_attempt CHECK (state <> 'in_flight' OR attempt > 0),
    CONSTRAINT deliveries_delivered_at_iff_delivered CHECK ((state = 'delivered') = (delivered_at IS NOT NULL))
);

-- The claim query walks this index in due order and stops at the first
-- rows it can lock. Only pending deliveries are in it, so history piling
-- up does not slow claiming down.
CREATE INDEX deliveries_due ON deliveries (next_attempt_at, id) WHERE state = 'pending';
-- The reaper finds expired leases here.
CREATE INDEX deliveries_leases ON deliveries (lease_until) WHERE state = 'in_flight';
-- Delivery history per endpoint, newest first, and the deliveries of one event.
CREATE INDEX deliveries_by_endpoint ON deliveries (endpoint_id, id DESC);
CREATE INDEX deliveries_by_event ON deliveries (event_id);

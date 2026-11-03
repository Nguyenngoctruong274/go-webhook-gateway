-- Endpoints are the customers' receivers. The schema grows per day:
-- events and deliveries follow once the domain exists.
CREATE TABLE endpoints (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url         TEXT        NOT NULL CHECK (url ~ '^https?://'),
    description TEXT        NOT NULL DEFAULT '',
    enabled     BOOLEAN     NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

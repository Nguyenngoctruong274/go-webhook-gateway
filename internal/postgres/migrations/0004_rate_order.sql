-- Per-endpoint rate limit and in-order delivery, both enforced by the
-- claim scan against state on the endpoint row, so every gateway
-- instance honours the same cap and the same order.
--
-- rate_limit is deliveries per second, 0 for none. rate_cursor is the
-- time the next delivery may be claimed at: each admitted delivery moves
-- it forward by 1/rate_limit, and the scan starts the cursor no further
-- back than one second ago, so an idle endpoint earns at most one
-- second's worth of burst. It starts at the epoch: a new endpoint has
-- its full burst.
ALTER TABLE endpoints
    ADD COLUMN rate_limit  INT         NOT NULL DEFAULT 0 CHECK (rate_limit BETWEEN 0 AND 1000),
    ADD COLUMN rate_cursor TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    ADD COLUMN ordered     BOOLEAN     NOT NULL DEFAULT false;

-- The ordering check asks "is there an older delivery to this endpoint
-- that is pending or in flight"; this index answers it without reading
-- the endpoint's whole history.
CREATE INDEX deliveries_open_by_endpoint ON deliveries (endpoint_id, id)
    WHERE state IN ('pending', 'in_flight');

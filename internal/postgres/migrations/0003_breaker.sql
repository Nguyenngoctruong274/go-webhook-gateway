-- The circuit breaker of each endpoint lives on its row, so every gateway
-- instance opens, probes and closes the same breaker. The columns mirror
-- breaker.State; the CHECK constraints restate its invariants.
ALTER TABLE endpoints
    ADD COLUMN breaker_status       TEXT        NOT NULL DEFAULT 'closed'
                                    CHECK (breaker_status IN ('closed', 'open', 'half_open')),
    ADD COLUMN breaker_failures     INT         NOT NULL DEFAULT 0 CHECK (breaker_failures >= 0),
    ADD COLUMN breaker_opens        INT         NOT NULL DEFAULT 0 CHECK (breaker_opens >= 0),
    ADD COLUMN breaker_opened_until TIMESTAMPTZ,
    ADD COLUMN breaker_probe_until  TIMESTAMPTZ,
    ADD CONSTRAINT endpoints_breaker_open_has_until
        CHECK ((breaker_status = 'open') = (breaker_opened_until IS NOT NULL)),
    ADD CONSTRAINT endpoints_breaker_half_open_has_probe
        CHECK ((breaker_status = 'half_open') = (breaker_probe_until IS NOT NULL));

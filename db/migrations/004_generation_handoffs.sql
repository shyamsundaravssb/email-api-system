CREATE TABLE generation_handoffs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    batch_id            UUID NOT NULL UNIQUE
                        REFERENCES batches(id),

    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN (
                            'pending',
                            'processing',
                            'submitted'
                        )),

    attempts            INT NOT NULL DEFAULT 0
                        CHECK (attempts >= 0),

    next_retry_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    claim_token         UUID,
    locked_until        TIMESTAMPTZ,

    last_error          TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at        TIMESTAMPTZ,

    CHECK (
        (status = 'processing' AND claim_token IS NOT NULL
                                AND locked_until IS NOT NULL)
        OR
        (status != 'processing' AND claim_token IS NULL
                                 AND locked_until IS NULL)
    )
);

CREATE INDEX idx_generation_handoffs_pending
ON generation_handoffs (next_retry_at, created_at)
WHERE status = 'pending';

CREATE INDEX idx_generation_handoffs_expired
ON generation_handoffs (locked_until)
WHERE status = 'processing';

CREATE TABLE accounts (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT NOT NULL,
    api_key_hash  TEXT NOT NULL UNIQUE,      -- SHA-256 of the API key
    from_email    TEXT NOT NULL,
    from_name     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE templates (
    template_id         TEXT PRIMARY KEY,    -- readable slug, e.g. 'otp_email'
    subject_template    TEXT NOT NULL,       -- Jinja2
    html_template       TEXT NOT NULL,       -- Jinja2
    text_template       TEXT,                -- Jinja2, optional plain-text part
    required_variables  TEXT[] NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE batches (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID NOT NULL REFERENCES accounts(id),
    total_count     INT NOT NULL,
    accepted_count  INT NOT NULL DEFAULT 0,
    rejected_count  INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE emails (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID NOT NULL REFERENCES batches(id),
    position        INT NOT NULL,            -- index of the recipient in the request
    template_id     TEXT NOT NULL,           -- no FK: unknown ids are stored as rejected
    to_email        TEXT NOT NULL,
    to_name         TEXT,
    variables       JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL CHECK (status IN
                    ('rejected','accepted','queued','retrying','sent','failed')),
    reject_reason   TEXT,                    -- only for status 'rejected'
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    message_id      TEXT,                    -- SMTP Message-ID, set by Part B
    tracking_token  UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    queued_at       TIMESTAMPTZ,
    sent_at         TIMESTAMPTZ,
    failed_at       TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_emails_batch_status ON emails (batch_id, status);
CREATE INDEX idx_emails_live ON emails (status)
    WHERE status IN ('accepted','queued','retrying');
CREATE INDEX idx_emails_message_id ON emails (message_id);

CREATE TABLE email_attempts (
    id             BIGSERIAL PRIMARY KEY,
    email_id       UUID NOT NULL REFERENCES emails(id),
    attempt_no     INT NOT NULL,
    outcome        TEXT NOT NULL CHECK (outcome IN
                   ('success','transient_error','permanent_error')),
    smtp_code      INT,
    smtp_response  TEXT,
    attempted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_attempts_email ON email_attempts (email_id);

CREATE TABLE idempotency_keys (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID NOT NULL REFERENCES accounts(id),
    idem_key         TEXT NOT NULL,
    request_hash     TEXT NOT NULL,          -- SHA-256 of the request body
    state            TEXT NOT NULL CHECK (state IN ('in_progress','completed')),
    batch_id         UUID REFERENCES batches(id),
    response_status  INT,
    response_body    JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at       TIMESTAMPTZ NOT NULL,   -- created_at + 24 hours
    UNIQUE (account_id, idem_key)
);

-- Created now, filled later (open and click tracking, MTA events)
CREATE TABLE tracking_links (
    id            BIGSERIAL PRIMARY KEY,
    email_id      UUID NOT NULL REFERENCES emails(id),
    link_token    TEXT NOT NULL UNIQUE,
    original_url  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE email_events (
    id           BIGSERIAL PRIMARY KEY,
    email_id     UUID NOT NULL REFERENCES emails(id),
    event_type   TEXT NOT NULL CHECK (event_type IN
                 ('delivered','deferred','bounced','opened','clicked','complained')),
    link_id      BIGINT REFERENCES tracking_links(id),   -- for 'clicked'
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ip_address   INET,
    user_agent   TEXT,
    raw          JSONB
);
CREATE INDEX idx_events_email_type ON email_events (email_id, event_type);
CREATE INDEX idx_events_type_time ON email_events (event_type, occurred_at);
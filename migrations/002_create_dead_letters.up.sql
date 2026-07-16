CREATE TABLE dead_letters (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id     UUID        NOT NULL,
    worker_id  UUID,
    job_type   TEXT        NOT NULL DEFAULT '',
    payload    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    attempts   INTEGER     NOT NULL DEFAULT 0,
    error      TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dead_letters_job_id ON dead_letters (job_id);
CREATE INDEX idx_dead_letters_created_at ON dead_letters (created_at DESC);

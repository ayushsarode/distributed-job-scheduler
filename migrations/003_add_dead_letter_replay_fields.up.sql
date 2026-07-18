ALTER TABLE dead_letters
    ADD COLUMN status TEXT NOT NULL DEFAULT 'OPEN',
    ADD COLUMN replayed_at TIMESTAMPTZ,
    ADD COLUMN replayed_job_id UUID;

CREATE INDEX idx_dead_letters_status ON dead_letters (status);

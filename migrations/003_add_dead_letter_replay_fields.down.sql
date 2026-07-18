DROP INDEX IF EXISTS idx_dead_letters_status;

ALTER TABLE dead_letters
    DROP COLUMN IF EXISTS replayed_job_id,
    DROP COLUMN IF EXISTS replayed_at,
    DROP COLUMN IF EXISTS status;

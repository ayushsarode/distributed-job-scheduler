-- Tables: jobs, workers

-- enums

CREATE TYPE job_status AS ENUM (
    'QUEUED',
    'RUNNING',
    'COMPLETED',
    'FAILED',
    'RETRYING',
    'DEAD'
)

CREATE TYPE worker_status as ENUM (
    'ACTIVE',
    'IDLE',
    'UNHEALTHY',
    'OFFLINE'
)

-- workers table 
CREATE TABLE workers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  host  TEXT NOT NULL,
  status  worker_status NOT NULL DEFAULT 'IDLE',
  cpu NUMERIC(5,2),
  memory NUMERIC(5,2),
  running_jobs  INTEGER NOT NULL DEFAULT 0,
  last_heartbeat TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)

CREATE INDEX idx_workers_status ON workers (status);
CREATE INDEX idx_workers_last_heartbeat ON workers (last_heartbeat);

-- jobs table

CREATE TABLE jobs (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status      job_status NOT NULL DEFAULT 'QUEUED',
  type        TEXT NOT NULL,
  payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
  priority    SMALLINT NOT NULL DEFAULT 0,
  attempts    INTEGER NOT NULL DEFAULT 0,
  worker_id   UUID REFERENCES workers(id) ON DELETE SET NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)

-- fetching pending jobs is the hottest read path: we filter it by status
-- order by priority then age
CREATE INDEX idx_jobs_status_priority ON jobs (status, priority DESC, created_at ASC);
CREATE INDEX idx_jobs_worker_id ON jobs (worker_id);
CREATE INDEX idx_jobs_type ON jobs (type);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_jobs_updated_at ON jobs;
DROP FUNCTION IF EXISTS set_updated_at();
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS workers;
DROP TYPE IF EXISTS job_status;
DROP TYPE IF EXISTS worker_status;

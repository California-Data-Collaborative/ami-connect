-- SOURCES: i.e. utilities that should get Airflow DAGs
CREATE TABLE sources (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    org_id TEXT NOT NULL UNIQUE,
    timezone TEXT NOT NULL,
    config JSONB DEFAULT '{}'::jsonb  -- holds type-specific config
);

-- SOURCES ↔ SINKS (many-to-many)
CREATE TABLE source_sinks (
    source_id INT REFERENCES sources(id) ON DELETE CASCADE,
    sink_id TEXT NOT NULL,
    PRIMARY KEY (source_id, sink_id)
);

-- SINKS: AMI data storage systems (snowflake, etc.)
CREATE TABLE sinks (
    id TEXT PRIMARY KEY,  -- e.g. "cadc_snowflake"
    type TEXT NOT NULL
);

-- SINK CHECKS: data quality checks linked to a sink
CREATE TABLE sink_checks (
    id SERIAL PRIMARY KEY,
    sink_id TEXT REFERENCES sinks(id) ON DELETE CASCADE,
    check_name TEXT NOT NULL
);

-- TASK OUTPUT: external outputs (S3, etc.)
CREATE TABLE task_outputs (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    s3_bucket TEXT,
		local_output_path TEXT
);

-- NOTIFICATIONS: keyed by event type (dag_failure, etc.)
CREATE TABLE notifications (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL UNIQUE,
    sns_arn TEXT NOT NULL
);

-- BACKFILLS: automated backfills
CREATE TABLE backfills (
    id SERIAL PRIMARY KEY,
    org_id TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    interval_days INT NOT NULL,
    schedule TEXT NOT NULL  -- cron format
);
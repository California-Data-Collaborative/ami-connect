-- =========================================
-- SOURCES: utilities that should get Airflow DAGs
-- =========================================
CREATE TABLE if not exists configuration_sources (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    type STRING NOT NULL,
    org_id STRING NOT NULL UNIQUE,
    timezone STRING NOT NULL,
    config VARIANT DEFAULT OBJECT_CONSTRUCT()  -- holds type-specific config
);

-- Example: query sources of type 'aclara' and extract sftp_host:
-- SELECT id, config:sftp_host::STRING AS sftp_host FROM configuration_sources WHERE type='aclara';

-- =========================================
-- SINKS: AMI data storage systems (Snowflake, S3, etc.)
-- =========================================
CREATE TABLE if not exists configuration_sinks (
    id STRING PRIMARY KEY,  -- e.g. "cadc_snowflake"
    type STRING NOT NULL
);

-- =========================================
-- SOURCES ↔ SINKS (many-to-many)
-- =========================================
CREATE TABLE if not exists configuration_source_sinks (
    source_id INTEGER REFERENCES configuration_sources(id) ON DELETE CASCADE,
    sink_id STRING REFERENCES configuration_sinks(id) ON DELETE CASCADE,
    PRIMARY KEY (source_id, sink_id)
);

-- =========================================
-- SINK CHECKS: data quality checks linked to a sink
-- =========================================
CREATE TABLE if not exists configuration_sink_checks (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    sink_id STRING REFERENCES configuration_sinks(id) ON DELETE CASCADE,
    check_name STRING NOT NULL
);

-- =========================================
-- TASK OUTPUTS: external outputs (S3, local paths, etc.)
-- =========================================
CREATE TABLE if not exists configuration_task_outputs (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    type STRING NOT NULL,
    s3_bucket STRING,
    local_output_path STRING
);

-- =========================================
-- NOTIFICATIONS: keyed by event type (dag_failure, etc.)
-- =========================================
CREATE TABLE if not exists configuration_notifications (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    event_type STRING NOT NULL UNIQUE,
    sns_arn STRING NOT NULL
);

-- =========================================
-- BACKFILLS: automated backfills
-- =========================================
CREATE TABLE if not exists configuration_backfills (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    org_id STRING NOT NULL REFERENCES configuration_sources(org_id) ON DELETE CASCADE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    interval_days INTEGER NOT NULL,
    schedule STRING NOT NULL  -- cron format
);

-- =========================================
-- Example JSON queries
-- =========================================
-- Get all sources with use_raw_data_cache = true
-- SELECT id, org_id, config:use_raw_data_cache::BOOLEAN AS use_cache
-- FROM configuration_sources
-- WHERE config:use_raw_data_cache::BOOLEAN = TRUE;

-- Update a source's sftp_host
-- UPDATE configuration_sources
-- SET config = OBJECT_INSERT(config, 'sftp_host', 'new_host.example.com')
-- WHERE org_id = 'cadc_coastside';

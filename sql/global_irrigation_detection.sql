-- The irrigation detection table, CADC_AMI.WAVELET.GLOBAL_IRRIGATION_DETECTION: one row per meter and hour, written by the
-- detection procedure (sql/irrigation_detection_flowtime.sql) and read by the post-processor to build irrigation events.
-- Columns as in production on 2026-11-25, plus FLOWTIME from PR #239. TIMESTAMP is the reading's clock time without its
-- offset and is the procedure's merge key; FLOWTIME is the same hour as an instant, time zone included.
-- The behavior test (test/integration/snowflake_postprocessor_behavior_test.py) creates its copy from this file.
CREATE TABLE IF NOT EXISTS GLOBAL_IRRIGATION_DETECTION (
	SOURCE VARCHAR(16777216),
	DEVICE_ID VARCHAR(16777216),
	TIMESTAMP TIMESTAMP_NTZ(9),
	TOTAL_READING FLOAT,
	NON_IRRIGATION_READING FLOAT,
	IRRIGATION_READING FLOAT,
	FLOWTIME TIMESTAMP_TZ(9)
);

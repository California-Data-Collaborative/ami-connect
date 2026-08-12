CREATE TABLE IF NOT EXISTS XYLEM_SENSUS_METER_AND_READS_BASE (
    ORG_ID VARCHAR(16777216) NOT NULL,
    METER_ID VARCHAR(16777216) NOT NULL,
    -- Raw CMEP file-generation stamp as received (YYYYMMDDHHMM), stored as
    -- text like every other raw base table's timestamps. A TIMESTAMP_TZ
    -- column here would be silently mis-parsed: Snowflake AUTO conversion
    -- reads a 12-digit string as epoch milliseconds.
    TIME_STAMP VARCHAR(16777216) NOT NULL,
    RECORD_TYPE               VARCHAR(16777216),
    RECORD_VERSION            VARCHAR(16777216),
    SENDER_ID                 VARCHAR(16777216),
    SENDER_CUSTOMER_ID        VARCHAR(16777216),
    RECEIVER_ID               VARCHAR(16777216),
    RECEIVER_CUSTOMER_ID      VARCHAR(16777216),
    PURPOSE                   VARCHAR(16777216),
    COMMODITY                 VARCHAR(16777216),
    UNITS                     VARCHAR(16777216),
    CALCULATION_CONSTANT      VARCHAR(16777216),
    INTERVAL                  VARCHAR(16777216),
    QUANTITY                  VARCHAR(16777216),
    READS                     VARCHAR(16777216),
    -- Timestamp of the row's first reading. TIME_STAMP is shared by every
    -- row in a file, so a meter's multiple same-channel rows (catch-up
    -- deliveries for disjoint windows) need this to stay distinct.
    FIRST_READ_TIME           VARCHAR(16777216),
    CREATED_TIME TIMESTAMP_TZ(9) NOT NULL,
    UNIQUE (ORG_ID, METER_ID, TIME_STAMP, UNITS, FIRST_READ_TIME)
);

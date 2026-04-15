CREATE TABLE IF NOT EXISTS XYLEM_DATALAKE_ACCOUNT_BASE (
    ORG_ID VARCHAR(16777216) NOT NULL,
    CREATED_TIME TIMESTAMP_TZ(9) NOT NULL,
    device_id VARCHAR(16777216),
    commodity VARCHAR(16777216),
    device_type VARCHAR(16777216),
    radio_id VARCHAR(16777216),
    account_id VARCHAR(16777216),
    account_name VARCHAR(16777216),
    account_rate_code VARCHAR(16777216),
    account_service_type VARCHAR(16777216),
    account_status VARCHAR(16777216),
    asset_address VARCHAR(16777216),
    asset_city VARCHAR(16777216),
    asset_state VARCHAR(16777216),
    asset_zip VARCHAR(16777216),
    meter_manufacturer VARCHAR(16777216),
    meter_size VARCHAR(16777216),
    display_multiplier VARCHAR(16777216),
    sdp_id VARCHAR(16777216),
    record_active_flag VARCHAR(16777216),
    UNIQUE (ORG_ID, device_id)
);

CREATE TABLE IF NOT EXISTS XYLEM_DATALAKE_WATER_INTERVALS_BASE (
    ORG_ID VARCHAR(16777216) NOT NULL,
    CREATED_TIME TIMESTAMP_TZ(9) NOT NULL,
    meter_id VARCHAR(16777216),
    radio_id VARCHAR(16777216),
    commodity VARCHAR(16777216),
    unit_of_measure VARCHAR(16777216),
    read_time_timestamp VARCHAR(16777216),
    rni_multiplier VARCHAR(16777216),
    interval_value VARCHAR(16777216),
    read_time_timestamp_local VARCHAR(16777216),
    UNIQUE (ORG_ID, meter_id, read_time_timestamp)
);

CREATE TABLE IF NOT EXISTS XYLEM_DATALAKE_WATER_REGISTERS_BASE (
    ORG_ID VARCHAR(16777216) NOT NULL,
    CREATED_TIME TIMESTAMP_TZ(9) NOT NULL,
    meter_id VARCHAR(16777216),
    radio_id VARCHAR(16777216),
    commodity VARCHAR(16777216),
    unit_of_measure VARCHAR(16777216),
    read_time_timestamp VARCHAR(16777216),
    read_quality VARCHAR(16777216),
    flag VARCHAR(16777216),
    rni_multiplier VARCHAR(16777216),
    register_value VARCHAR(16777216),
    read_time_timestamp_local VARCHAR(16777216),
    UNIQUE (ORG_ID, meter_id, read_time_timestamp)
);

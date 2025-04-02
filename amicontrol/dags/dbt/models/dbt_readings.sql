{{ config(materialized='table') }}

with latest_beacon_reads as (

    select *
    from beacon_360_base where created_time = (select max(created_time) from beacon_360_base)

),

final_beacon as (
    select 
        l.org_id as ORG_ID,
        l.Account_ID as ACCOUNT_ID,
        l.Location_ID as LOCATION_ID,
        l.Meter_ID as DEVICE_ID,
        -- TODO timestamp, handle null
        l.Read_Time as FLOWTIME,
        CASE WHEN l.Read <> '' THEN CAST(l.Read as float) ELSE NULL END as REGISTER_VALUE,
        -- TODO use macro to map unit of measure
        l.Read_Unit as REGISTER_UNIT,
        NULL as INTERVAL_VALUE,
        NULL as INTERVAL_UNIT,
    from latest_beacon_reads l
)

-- create or replace TABLE READINGS (
--   ORG_ID VARCHAR(16777216),  // Allows for multitenant storage
-- 	ACCOUNT_ID VARCHAR(16777216),
-- 	LOCATION_ID VARCHAR(16777216),
-- 	DEVICE_ID VARCHAR(16777216) NOT NULL,
-- 	FLOWTIME TIMESTAMP_TZ(9) NOT NULL,  // Time of measurement in UTC
-- 	REGISTER_VALUE FLOAT,  // Cumulative measurement of usage
-- 	REGISTER_UNIT VARCHAR(16777216),  // e.g. CCF or GAL
-- 	INTERVAL_VALUE FLOAT,  // Measurement of usage since last hour/day/etc
-- 	INTERVAL_UNIT VARCHAR(16777216),  // e.g. CCF or GAL
--   UNIQUE (ORG_ID, ACCOUNT_ID, LOCATION_ID, DEVICE_ID, FLOWTIME)
-- );

select * from final_beacon
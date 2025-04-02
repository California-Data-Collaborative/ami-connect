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

select * from final_beacon
from datetime import datetime
import json
from typing import List
import pytz

import snowflake.connector

from amiadapters.base import GeneralMeter, GeneralMeterRead
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.storage.base import BaseAMIStorageSink


class SnowflakeStorageSink(BaseAMIStorageSink):
    """
    AMI Storage Sink for Snowflake database. Implementors must specify how to store
    raw data in Snowflake.
    """

    def __init__(self, transformed_meter_file: str, transformed_reads_file: str):
        super().__init__(transformed_meter_file, transformed_reads_file)

    def store_transformed(self, config: AMIAdapterConfiguration):
        with open(self.transformed_meter_file, "r") as f:
            text = f.read()
            meters = [GeneralMeter(**json.loads(d)) for d in text.strip().split("\n")]

        with open(self.transformed_reads_file, "r") as f:
            text = f.read()
            reads = [GeneralMeterRead(**json.loads(d)) for d in text.strip().split("\n")]

        conn = snowflake.connector.connect(
            account=config.snowflake_account,
            user=config.snowflake_user,
            password=config.snowflake_password,
            warehouse=config.snowflake_warehouse,
            database=config.snowflake_database,
            schema=config.snowflake_schema,
            role=config.snowflake_role,
            paramstyle='qmark',
        )

        self._upsert_meters(meters, conn)
        self._upsert_reads(reads, conn)
    
    def _upsert_meters(self, meters: List[GeneralMeter], conn):

        sql = "CREATE OR REPLACE TEMPORARY TABLE TEMP_METERS LIKE METERS;"
        conn.cursor().execute(sql)

        row_active_from = datetime.now(tz=pytz.UTC)

        sql = """
            INSERT INTO temp_meters (
                org_id, device_id, account_id, location_id, meter_id, 
                endpoint_id, meter_install_date, meter_size, meter_manufacturer, 
                multiplier, location_address, location_city, location_state, location_zip,
                row_active_from
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [self._meter_tuple(m, row_active_from) for m in meters]
        conn.cursor().executemany(sql, rows)

        # We use a Type 2 Slowly Changing Dimension pattern for our meters table
        # Our implementation follows a pattern in this blog post: https://medium.com/@amit-jsr/implementing-scd2-in-snowflake-slowly-changing-dimension-type-2-7ff793647150
        # It's extremely important that the source table has no duplicates on the "merge_key"!
        # Also, if new columns are added, be careful to update this query in each place column names are referenced.
        merge_sql = f"""
            MERGE INTO meters AS target
            USING(
                SELECT CONCAT(tm.org_id, '|', tm.device_id) as merge_key, tm.*
                FROM temp_meters tm
                
                UNION ALL
                
                SELECT NULL as merge_key, tm2.*
                FROM temp_meters tm2
                JOIN meters m2 ON tm2.org_id = m2.org_id AND tm2.device_id = m2.device_id
                WHERE m2.row_active_until IS NULL AND
                    CONCAT(tm2.account_id, '|', tm2.location_id, '|', tm2.meter_id, '|', tm2.endpoint_id, '|', tm2.meter_install_date, '|', tm2.meter_size, '|', tm2.meter_manufacturer, '|', tm2.multiplier, '|', tm2.location_address, '|', tm2.location_city, '|', tm2.location_state, '|', tm2.location_zip, '|')
                    <>
                    CONCAT(m2.account_id, '|', m2.location_id, '|', m2.meter_id, '|', m2.endpoint_id, '|', m2.meter_install_date, '|', m2.meter_size, '|', m2.meter_manufacturer, '|', m2.multiplier, '|', m2.location_address, '|', m2.location_city, '|', m2.location_state, '|', m2.location_zip, '|')
            ) AS source

            ON CONCAT(target.org_id, '|', target.device_id) = source.merge_key
            WHEN MATCHED
                AND target.row_active_until IS NULL
                AND CONCAT(target.account_id, '|', target.location_id, '|', target.meter_id, '|', target.endpoint_id, '|', target.meter_install_date, '|', target.meter_size, '|', target.meter_manufacturer, '|', target.multiplier, '|', target.location_address, '|', target.location_city, '|', target.location_state, '|', target.location_zip, '|')
                    <>
                    CONCAT(source.account_id, '|', source.location_id, '|', source.meter_id, '|', source.endpoint_id, '|', source.meter_install_date, '|', source.meter_size, '|', source.meter_manufacturer, '|', source.multiplier, '|', source.location_address, '|', source.location_city, '|', source.location_state, '|', source.location_zip, '|')
            THEN
                UPDATE SET
                    target.row_active_until = '{row_active_from.isoformat()}'
            WHEN NOT MATCHED THEN
                INSERT (org_id, device_id, account_id, location_id, meter_id, 
                        endpoint_id, meter_install_date, meter_size, meter_manufacturer, 
                        multiplier, location_address, location_city, location_state, location_zip,
                        row_active_from)
                VALUES (source.org_id, source.device_id, source.account_id, source.location_id, source.meter_id, 
                        source.endpoint_id, source.meter_install_date, source.meter_size, source.meter_manufacturer, 
                        source.multiplier, source.location_address, source.location_city, source.location_state, source.location_zip,
                        '{row_active_from.isoformat()}');
        """
        conn.cursor().execute(merge_sql)
    
    def _meter_tuple(self, meter: GeneralMeter, row_active_from: datetime):
        result = [
            "org_id",
            meter.device_id,
            meter.account_id,
            meter.location_id,
            meter.meter_id,
            meter.endpoint_id,
            meter.meter_install_date,
            meter.meter_size,
            meter.meter_manufacturer,
            meter.multiplier,
            meter.location_address,
            None,
            meter.location_state,
            meter.location_zip,
            row_active_from
        ]
        return tuple(result)

    def _upsert_reads(self, reads: List[GeneralMeterRead], conn):

        sql = "CREATE OR REPLACE TEMPORARY TABLE temp_readings LIKE readings;"
        conn.cursor().execute(sql)

        sql = """
            INSERT INTO temp_readings (
                org_id, device_id, account_id, location_id, flowtime, 
                register_value, register_unit, interval_value, interval_unit
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [self._meter_read_tuple(m) for m in reads]
        conn.cursor().executemany(sql, rows)

        merge_sql = """
            MERGE INTO readings AS target
            USING (
                -- Use GROUP BY to ensure there are no duplicate rows before merge
                SELECT org_id, device_id, flowtime, max(account_id) as account_id, max(location_id) as location_id, 
                    max(register_value) as register_value, max(register_unit) as register_unit,
                    max(interval_value) as interval_value, max(interval_unit) as interval_unit
                FROM temp_readings
                GROUP BY org_id, device_id, flowtime
            ) AS source
            ON source.org_id = target.org_id 
                AND source.device_id = target.device_id
                AND source.flowtime = target.flowtime
            WHEN MATCHED THEN
                UPDATE SET
                    target.account_id = source.account_id,
                    target.location_id = source.location_id,
                    target.register_value = source.register_value,
                    target.register_unit = source.register_unit,
                    target.interval_value = source.interval_value,
                    target.interval_unit = source.interval_unit
            WHEN NOT MATCHED THEN
                INSERT (org_id, device_id, account_id, location_id, flowtime, 
                        register_value, register_unit, interval_value, interval_unit) 
                        VALUES (source.org_id, source.device_id, source.account_id, source.location_id, source.flowtime, 
                    source.register_value, source.register_unit, source.interval_value, source.interval_unit)
        """
        conn.cursor().execute(merge_sql)
    
    def _meter_read_tuple(self, read: GeneralMeterRead):
        result = [
            "org_id",
            read.device_id,
            read.account_id,
            read.location_id,
            read.flowtime,
            read.register_value,
            read.register_unit,
            read.interval_value,
            read.interval_unit,
        ]
        return tuple(result)
        



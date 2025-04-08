from datetime import datetime
import json
import pytz

import snowflake.connector

from amiadapters.base import GeneralMeter, GeneralMeterRead
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.storage.base import BaseAMIStorageAdapter


class SnowflakeStorageAdapter(BaseAMIStorageAdapter):

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
        # import pdb; pdb.set_trace()
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
        



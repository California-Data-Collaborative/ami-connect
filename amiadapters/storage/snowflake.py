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
        # import pdb; pdb.set_trace()
        conn.cursor().executemany(sql, rows)

        merge_sql = f"""
            MERGE INTO meters AS target
            USING(
                SELECT
                    *
                FROM
                    temp_meters
            ) AS source

            ON target.org_id = source.org_id 
                AND target.device_id = source.device_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.account_id = source.account_id,
                    target.location_id = source.location_id,
                    target.meter_id = source.meter_id,
                    target.endpoint_id = source.endpoint_id,
                    target.meter_install_date = source.meter_install_date,
                    target.meter_size = source.meter_size,
                    target.meter_manufacturer = source.meter_manufacturer,
                    target.multiplier = source.multiplier,
                    target.location_address = source.location_address,
                    target.location_city = source.location_city,
                    target.location_state = source.location_state,
                    target.location_zip = source.location_zip,
                    target.row_active_until = source.row_active_from
            WHEN NOT MATCHED THEN
                INSERT (org_id, device_id, account_id, location_id, meter_id, 
                        endpoint_id, meter_install_date, meter_size, meter_manufacturer, 
                        multiplier, location_address, location_city, location_state, location_zip,
                        row_active_from)
                VALUES (source.org_id, source.device_id, source.account_id, source.location_id, source.meter_id, 
                        source.endpoint_id, source.meter_install_date, source.meter_size, source.meter_manufacturer, 
                        source.multiplier, source.location_address, source.location_city, source.location_state, source.location_zip,
                        source.row_active_from);
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
        



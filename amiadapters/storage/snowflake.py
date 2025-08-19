from abc import ABC, abstractmethod
from datetime import datetime, time
import logging
from typing import List

import pytz
from pytz.tzinfo import DstTzInfo

from amiadapters.models import GeneralMeterRead
from amiadapters.models import GeneralMeter
from amiadapters.config import ConfiguredStorageSink
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.base import BaseAMIStorageSink, BaseAMIDataQualityCheck

logger = logging.getLogger(__name__)


class RawSnowflakeLoader(ABC):
    """
    An adapter must define how it stores raw data in Snowflake because, by nature,
    raw data schemas are specific to the adapter. This abstract class
    allows an adapter to define its implementation, then pass it up to
    the Snowflake sink abstractions during instantiation.
    """

    @abstractmethod
    def load(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ):
        """
        Using a Snowflake connection and output controller, get the raw
        data and store it in Snowflake.
        """
        pass


class SnowflakeStorageSink(BaseAMIStorageSink):
    """
    AMI Storage Sink for Snowflake database.
    """

    def __init__(
        self,
        org_id: str,
        org_timezone: DstTzInfo,
        sink_config: ConfiguredStorageSink,
        raw_loader: RawSnowflakeLoader,
    ):
        self.org_id = org_id
        self.org_timezone = org_timezone
        self.raw_loader = raw_loader
        super().__init__(sink_config)

    def store_raw(self, run_id: str, extract_outputs: ExtractOutput):
        """
        Store raw data using the specified RawSnowflakeLoader.
        """
        # Adapter may choose not to specify a raw data loader. If so, skip it.
        if self.raw_loader is None:
            return
        conn = self.sink_config.connection()
        return self.raw_loader.load(
            run_id, self.org_id, self.org_timezone, extract_outputs, conn
        )

    def store_transformed(
        self, run_id: str, meters: List[GeneralMeter], reads: List[GeneralMeterRead]
    ):
        """
        Store transformed data into our generalized tables in Snowflake.
        """
        conn = self.sink_config.connection()
        self._upsert_meters(meters, conn)
        self._upsert_reads(reads, conn)

    def _upsert_meters(
        self,
        meters: List[GeneralMeter],
        conn,
        row_active_from=None,
        table_name="meters",
    ):
        self._verify_no_duplicate_meters(meters)

        if row_active_from is None:
            row_active_from = datetime.now(tz=pytz.UTC)

        temp_table_name = f"temp_{table_name}"
        create_temp_table_sql = (
            f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} LIKE {table_name};"
        )
        conn.cursor().execute(create_temp_table_sql)

        insert_to_temp_table_sql = f"""
            INSERT INTO {temp_table_name} (
                org_id, device_id, account_id, location_id, meter_id, 
                endpoint_id, meter_install_date, meter_size, meter_manufacturer, 
                multiplier, location_address, location_city, location_state, location_zip,
                row_active_from
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [self._meter_tuple(m, row_active_from) for m in meters]
        conn.cursor().executemany(insert_to_temp_table_sql, rows)

        # We use a Type 2 Slowly Changing Dimension pattern for our meters table
        # Our implementation follows a pattern in this blog post: https://medium.com/@amit-jsr/implementing-scd2-in-snowflake-slowly-changing-dimension-type-2-7ff793647150
        # It's extremely important that the source table has no duplicates on the "merge_key"!
        # Also, if new columns are added, be careful to update this query in each place column names are referenced.
        merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING(
                SELECT CONCAT(tm.org_id, '|', tm.device_id) as merge_key, tm.*
                FROM {temp_table_name} tm
                
                UNION ALL
                
                SELECT NULL as merge_key, tm2.*
                FROM {temp_table_name} tm2
                JOIN {table_name} m2 ON tm2.org_id = m2.org_id AND tm2.device_id = m2.device_id
                WHERE m2.row_active_until IS NULL AND
                    ARRAY_CONSTRUCT(tm2.account_id, tm2.location_id, tm2.meter_id, tm2.endpoint_id, tm2.meter_install_date, tm2.meter_size, tm2.meter_manufacturer, tm2.multiplier, tm2.location_address, tm2.location_city, tm2.location_state, tm2.location_zip)
                    <>
                    ARRAY_CONSTRUCT(m2.account_id, m2.location_id, m2.meter_id, m2.endpoint_id, m2.meter_install_date, m2.meter_size, m2.meter_manufacturer, m2.multiplier, m2.location_address, m2.location_city, m2.location_state, m2.location_zip)
            ) AS source

            ON CONCAT(target.org_id, '|', target.device_id) = source.merge_key
            WHEN MATCHED
                AND target.row_active_until IS NULL
                AND ARRAY_CONSTRUCT(target.account_id, target.location_id, target.meter_id, target.endpoint_id, target.meter_install_date, target.meter_size, target.meter_manufacturer, target.multiplier, target.location_address, target.location_city, target.location_state, target.location_zip)
                    <>
                    ARRAY_CONSTRUCT(source.account_id, source.location_id, source.meter_id, source.endpoint_id, source.meter_install_date, source.meter_size, source.meter_manufacturer, source.multiplier, source.location_address, source.location_city, source.location_state, source.location_zip)
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
            meter.org_id,
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
            meter.location_city,
            meter.location_state,
            meter.location_zip,
            row_active_from,
        ]
        return tuple(result)

    def _upsert_reads(self, reads: List[GeneralMeterRead], conn, table_name="readings"):
        self._verify_no_duplicate_reads(reads)

        temp_table_name = f"temp_{table_name}"
        create_temp_table_sql = (
            f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} LIKE {table_name};"
        )
        conn.cursor().execute(create_temp_table_sql)

        insert_to_temp_table_sql = f"""
            INSERT INTO {temp_table_name} (
                org_id, device_id, account_id, location_id, flowtime, 
                register_value, register_unit, interval_value, interval_unit
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [self._meter_read_tuple(m) for m in reads]
        conn.cursor().executemany(insert_to_temp_table_sql, rows)

        merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING (
                -- Use GROUP BY to ensure there are no duplicate rows before merge
                SELECT org_id, device_id, flowtime, max(account_id) as account_id, max(location_id) as location_id, 
                    max(register_value) as register_value, max(register_unit) as register_unit,
                    max(interval_value) as interval_value, max(interval_unit) as interval_unit
                FROM {temp_table_name}
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
            read.org_id,
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

    def _verify_no_duplicate_meters(self, meters: List[GeneralMeter]):
        seen = set()
        for meter in meters:
            key = (meter.org_id, meter.device_id)
            if key in seen:
                raise ValueError(
                    f"Encountered duplicate meter in data for Snowflake: {key}"
                )
            seen.add(key)

    def _verify_no_duplicate_reads(self, reads: List[GeneralMeterRead]):
        seen = set()
        for read in reads:
            key = (read.org_id, read.device_id, read.flowtime)
            if key in seen:
                raise ValueError(
                    f"Encountered duplicate read in data for Snowflake: {key}"
                )
            seen.add(key)

    def calculate_end_of_backfill_range(
        self, org_id: str, min_date: datetime, max_date: datetime
    ) -> datetime:
        """
        Find the end day of the range we should backfill. Try to automatically calculate
        the oldest day in the range that we've already backfilled.
        """
        conn = self.sink_config.connection()

        # Calculate nth percentile of number of readings per day
        # We will use that as a threshold for what we consider "already backfilled"
        percentile_query = """
        SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY num_readings) AS nth_percentile
        FROM (select count(*) as num_readings FROM readings WHERE org_id = ? GROUP BY date(flowtime))
        """
        percentile_result = conn.cursor().execute(percentile_query, (org_id,))
        percentile_rows = [i for i in percentile_result]
        if len(percentile_rows) != 1:
            threshold = 0
        else:
            threshold = float(percentile_rows[0][0])

        # Lower threshold by x% to allow dates with legitimately lower volume to be considered backfilled
        threshold = 0.5 * threshold

        # Find the oldest day in the range that we've already backfilled
        query = """
        SELECT MIN(flow_date) from (
            SELECT DATE(flowtime) as flow_date FROM readings 
            WHERE org_id = ? AND flowtime > ? AND flowtime < ?
            GROUP BY DATE(flowtime)
            HAVING COUNT(*) > ?
        ) 
        """
        result = conn.cursor().execute(
            query,
            (
                org_id,
                min_date,
                max_date,
                threshold,
            ),
        )
        rows = [i for i in result]
        if rows is None or len(rows) != 1 or rows[0][0] is None:
            return max_date

        result = rows[0][0]
        return datetime.combine(result, time(0, 0))


class SnowflakeMetersUniqueByDeviceIdCheck(BaseAMIDataQualityCheck):
    """
    Assert that meters are unique by org_id and device_id when their row_active_until is null.
    """

    def __init__(
        self,
        connection,
        meter_table_name: str = "meters",
    ):
        self.connection = connection
        self.meter_table_name = meter_table_name

    def name(self) -> str:
        return "snowflake-meters-unique-by-device-id"

    def check(self) -> bool:
        """
        Run the check.

        :return: True if check passes, else False.
        """
        sql = f"""
            SELECT distinct(deduped.device_id)
            FROM (
                SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY org_id, device_id
                    ORDER BY row_active_from
                ) AS row_num
                FROM {self.meter_table_name}
                WHERE row_active_until is null
            ) as deduped
            WHERE row_num > 1
            """
        logger.info("Running meter uniqueness check")
        result = self.connection.cursor().execute(sql).fetchall()
        row_count = len(result)
        logger.info(f"Found {row_count} non-unique meters. First 10: {result[:10]}")
        return row_count == 0


class SnowflakeReadingsUniqueByDeviceIdAndFlowtimeCheck(BaseAMIDataQualityCheck):
    """
    Assert that readings are unique by org_id, device_id, and flowtime.
    """

    def __init__(
        self,
        connection,
        readings_table_name: str = "readings",
    ):
        self.connection = connection
        self.readings_table_name = readings_table_name

    def name(self) -> str:
        return "snowflake-readings-unique-by-device-id-and-flowtime"

    def check(self) -> bool:
        sql = f"""
            SELECT org_id, device_id, flowtime
            FROM (
                SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY org_id, device_id, flowtime
                    ORDER BY interval_value, register_value
                ) AS row_num
                FROM readings
            ) as deduped
            WHERE row_num > 1
            """
        logger.info("Running readings uniqueness check")
        result = self.connection.cursor().execute(sql).fetchall()
        row_count = len(result)
        logger.info(f"Found non-unique readings. First 10: {result[:10]}")
        return row_count == 0

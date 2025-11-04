from abc import ABC, abstractmethod
from datetime import datetime, time
import logging
from typing import List

import pytz
from pytz.tzinfo import DstTzInfo

from amiadapters.models import GeneralMeterRead
from amiadapters.models import GeneralMeter
from amiadapters.configuration.models import ConfiguredStorageSink
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

    def exec_postprocessor(self, run_id: str, min_date: datetime, max_date: datetime):

        conn = self.sink_config.connection()

        ami_leaks_sql = f"""
            create or replace table leaks_{self.org_id}
            as
            with data0_readings as
            (
                select org_id
                    ,device_id
                    ,flowtime
                    ,datediff(second, lag(flowtime) over(partition by org_id, device_id order by flowtime), flowtime) as flowinterval
                    ,interval_value
                    ,register_value
                    ,coalesce(estimated::boolean, false) as estimated
                    ,case when not coalesce(estimated::boolean, false) and flowinterval = 3600 and interval_value > 0 then interval_value end as interval_value_clean
                from ami_connect.readings
                where 1=1
                and org_id = '{self.org_id}'
                and flowtime::date >= '{min_date.isoformat()}'
                and flowtime::date <= '{max_date.isoformat()}'
            )
            ,leaks_groups0 as
            (
                select
                    org_id
                    ,device_id
                    ,min(flowtime) as stime0
                    ,max(flowtime) as etime0
                    ,datediff(hour, stime0, etime0) + 1 as duration0
                from
                (
                    select
                        org_id
                        ,device_id
                        ,flowtime
                        ,date_part(epoch_second, flowtime) / 3600 - row_number() over (partition by org_id, device_id order by flowtime) as grp
                    from data0_readings
                    where interval_value_clean is not null
                )
                group by org_id, device_id, grp
            )
            ,leaks_groups1 as
            (
                select
                    a.org_id
                    ,a.device_id
                    ,a.stime0 as stime1
                    ,ifnull(b.etime0, a.etime0) as etime1
                    ,datediff(hour, stime1, etime1) + 1 as duration1
                from leaks_groups0 a
                left join leaks_groups0 b on a.org_id = b.org_id and a.device_id = b.device_id and datediff(hour, a.etime0, b.stime0) = 2
                where duration1 >= 24
            )
            ,leaks_groups2 as
            (
                with cte as
                (
                    select *
                        ,row_number() over(partition by org_id, device_id order by stime1, etime1) as rn
                    from leaks_groups1
                )
                ,rec as
                (
                    select *
                        ,rn as rn2
                    from cte
                    where rn = 1
                    union all
                    select a.*
                        ,case when b.etime1 between a.stime1 and a.etime1 then b.rn2 else a.rn end
                    from cte a
                    join rec b on a.org_id = b.org_id and a.device_id = b.device_id and a.rn = b.rn + 1
                )
                select
                    org_id
                    ,device_id
                    ,min(stime1) as stime2
                    ,max(etime1) as etime2
                    ,datediff(hour, stime2, etime2) + 1 as duration2
                from rec
                group by org_id, device_id, rn2
            )
            ,leaks as
            (
                with cte as
                (
                    select a.*
                        ,(b.device_id is not null) as is_leak
                        ,conditional_change_event(is_leak) over(partition by a.org_id, a.device_id order by flowtime) as grp
                    from data0_readings a
                    left join leaks_groups2 b on a.org_id = b.org_id and a.device_id = b.device_id and flowtime between stime2 and etime2
                    where concat_ws('-', a.org_id, a.device_id) in (select distinct concat_ws('-', org_id, device_id) from leaks_groups2) --and a.device_id = '38919022'
                )
                ,cte2 as
                (
                    select *
                        ,min(flowtime) over(partition by org_id, device_id, grp) as stime
                        ,max(flowtime) over(partition by org_id, device_id, grp) as etime
                        ,datediff(hour, stime, etime) + 1 as duration
                        ,min(case when is_leak then interval_value_clean end) over (partition by org_id, device_id, grp order by flowtime rows between 23 preceding and current row) as min_leak_bck
                        ,min(case when is_leak then interval_value_clean end) over (partition by org_id, device_id, grp order by flowtime rows between current row and 23 following) as min_leak_fwd
                        ,greatest(min_leak_bck, min_leak_fwd) as leak_calc
                    from cte
                )
                select *
                    ,avg(leak_calc) over(partition by org_id, device_id, stime, etime) as leak_avg
                    ,stddev(leak_calc) over(partition by org_id, device_id, stime, etime) as leak_stdev
                    ,case when leak_calc > leak_avg + leak_stdev then leak_avg else leak_calc end as leak_clean
                    ,to_varchar(stime, 'yyyy/mm/dd@hh - ') || duration || iff(is_leak, 'h', 'h*') as event_id
                    ,case when estimated then 0 when leak_clean <= interval_value then leak_clean else 0 end as leak
                    ,case when estimated then 0 when leak_clean <= interval_value then interval_value - leak_clean else ifnull(interval_value, 0) end as usage
                    ,case when estimated then interval_value else 0 end as est_usage
                from cte2
            )
            select *
            from leaks
        """
        conn.cursor().execute(ami_leaks_sql)

        ami_leaks_agg_sql = f"""
            create or replace table leaks_{self.org_id}_agg
            as
            select
                org_id
                , device_id
                , event_id
                , stime
                , etime
                , duration
                , is_leak
                , array_agg(flowtime) as flowtime
                , array_agg(flowinterval) as flowinterval
                , array_agg(ifnull(estimated, 'NaN')) as estimated
                , array_agg(ifnull(interval_value, 'NaN')) as interval_value
                , array_agg(ifnull(interval_value_clean, 'NaN')) as interval_value_clean
                , array_agg(ifnull(leak_calc, 'NaN')) as leak_calc
                , avg(leak_calc) as leak_avg
                , stddev(leak_calc) as leak_stdev
                , array_agg(ifnull(leak_clean, 'NaN')) as leak_clean
                , avg(leak) as leak_rate
                , array_agg(ifnull(leak, 'NaN')) as leak
                , array_agg(ifnull(usage, 'NaN')) as usage
                , array_agg(ifnull(est_usage, 'NaN')) as est_usage
            from leaks_{self.org_id}
            group by org_id, device_id, event_id, stime, etime, duration, is_leak
        """
        conn.cursor().execute(ami_leaks_agg_sql)

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
                register_value, register_unit, interval_value, interval_unit,
                battery, install_date, connection, estimated
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [self._meter_read_tuple(m) for m in reads]
        conn.cursor().executemany(insert_to_temp_table_sql, rows)

        merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING (
                -- Use GROUP BY to ensure there are no duplicate rows before merge
                SELECT org_id, device_id, flowtime, 
                    max(account_id) as account_id, max(location_id) as location_id, 
                    max(register_value) as register_value, max(register_unit) as register_unit,
                    max(interval_value) as interval_value, max(interval_unit) as interval_unit,
                    max(battery) as battery, max(install_date) as install_date,
                    max(connection) as connection, max(estimated) as estimated
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
                    target.interval_unit = source.interval_unit,
                    target.battery = source.battery,
                    target.install_date = source.install_date,
                    target.connection = source.connection,
                    target.estimated = source.estimated
            WHEN NOT MATCHED THEN
                INSERT (org_id, device_id, account_id, location_id, flowtime, 
                        register_value, register_unit, interval_value, interval_unit, battery, install_date, connection, estimated) 
                    VALUES (source.org_id, source.device_id, source.account_id, source.location_id, source.flowtime, 
                            source.register_value, source.register_unit, source.interval_value, source.interval_unit,
                            source.battery, source.install_date, source.connection, source.estimated)
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
            read.battery,
            read.install_date,
            read.connection,
            read.estimated,
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

        org_id: organization's ID
        min_date: earliest possible date that we might backfill
        max_date: oldest possible date that we might backfill
        """
        conn = self.sink_config.connection()

        # Temporary hack for Thousand Oaks backfill, remove after finished
        if org_id == "cadc_thousand_oaks":
            earliest = (
                conn.cursor()
                .execute(
                    "select earliest from backfills where org_id = 'cadc_thousand_oaks'"
                )
                .fetchall()[0][0]
            )
            logger.info(f"Earliest end date of backfill for {org_id} is {earliest}")
            from datetime import timedelta

            next_end = earliest - timedelta(days=1)
            conn.cursor().execute(
                f"update backfills set earliest = '{next_end}' where org_id = 'cadc_thousand_oaks'"
            ).fetchall()[0][0]
            logger.info(f"Set earliest to {next_end.isoformat()}")
            return earliest

        # Calculate nth percentile of number of readings per day
        # We will use that as a threshold for what we consider "already backfilled"
        percentile_query = """
        SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY num_readings) AS nth_percentile
        FROM (
            SELECT count(*) as num_readings 
            FROM readings WHERE org_id = ? AND flowtime > ? AND flowtime < ? 
            GROUP BY date(flowtime)
        )
        """
        percentile_result = conn.cursor().execute(
            percentile_query, (org_id, min_date, max_date)
        )
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
            SELECT DATE(flowtime) as flow_date 
            FROM readings 
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

    def notify_on_failure(self) -> bool:
        return True

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
        super().__init__(connection)
        self.readings_table_name = readings_table_name

    def name(self) -> str:
        return "snowflake-readings-unique-by-device-id-and-flowtime"

    def notify_on_failure(self) -> bool:
        return True

    def check(self) -> bool:
        sql = f"""
            SELECT org_id, device_id, flowtime
            FROM (
                SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY org_id, device_id, flowtime
                    ORDER BY interval_value, register_value
                ) AS row_num
                FROM {self.readings_table_name}
            ) as deduped
            WHERE row_num > 1
            """
        logger.info("Running readings uniqueness check")
        result = self.connection.cursor().execute(sql).fetchall()
        row_count = len(result)
        logger.info(f"Found non-unique readings. First 10: {result[:10]}")
        return row_count == 0


class SnowflakeReadingsHaveNoDataGapsCheck(BaseAMIDataQualityCheck):
    """
    For each org, report days of readings data that fall below the org's normal amount of daily readings.
    """

    def __init__(
        self,
        connection,
        readings_table_name: str = "readings",
    ):
        super().__init__(connection)
        self.readings_table_name = readings_table_name

    def name(self) -> str:
        return "snowflake-readings-have-no-data-gaps"

    def notify_on_failure(self) -> bool:
        """
        Gaps are occasionally caused by the source, which we can't control, so we just log the gaps
        and we don't notify.
        """
        return False

    def check(self) -> bool:
        threshold_percentile = 0.99
        percent_of_threshold_before_reporting_gap = 0.7
        month_window_size = 2

        # Generate every day between org's min and max flowtime and report row counts that are below threshold
        # Threshold is calculated by finding Nth percentile of row counts for preceeding and following X months
        sql = f"""
            WITH daily_counts AS (
                SELECT
                    org_id,
                    TO_DATE (flowtime) AS day,
                    COUNT(*) AS row_count
                FROM
                    {self.readings_table_name}
                GROUP BY
                    org_id,
                    day
            ),
            org_ranges AS (
                SELECT
                    org_id,
                    MIN(TO_DATE (flowtime)) AS min_day,
                    MAX(TO_DATE (flowtime)) - INTERVAL '2 DAY' AS max_day
                FROM
                    {self.readings_table_name}
                GROUP BY
                    org_id
            ),
            calendar as (
                select r.org_id, r.min_day, r.max_day, c.generated_date
                from org_ranges r
                cross join (
                        select -1 + row_number() over(order by 0) as i, start_date + i as generated_date 
                            from (select MIN(TO_DATE (flowtime)) AS start_date, MAX(TO_DATE (flowtime)) AS end_date from {self.readings_table_name})
                            join table(generator(rowcount => 10000 )) x
                            qualify i < 1 + end_date - start_date
                    ) c
                    where c.generated_date between r.min_day and r.max_day
                    order by org_id, generated_date 
            ),
            filled AS (
                SELECT
                    c.org_id,
                    c.generated_date as day,
                    COALESCE(d.row_count, 0) AS row_count
                FROM
                    calendar AS c
                    LEFT JOIN daily_counts AS d ON c.org_id = d.org_id
                    AND c.generated_date = d.day
            ),
            baselines AS (
                SELECT
                    f1.org_id,
                    f1.day,
                    PERCENTILE_CONT({threshold_percentile}) WITHIN GROUP (
                    ORDER BY
                        f2.row_count
                    ) AS local_threshold
                FROM
                    filled AS f1
                    JOIN filled AS f2 ON f1.org_id = f2.org_id
                    AND f2.day BETWEEN DATEADD (MONTH, -{month_window_size}, f1.day)
                    AND DATEADD (MONTH, {month_window_size}, f1.day)
                    AND f2.day <> f1.day
                GROUP BY
                    f1.org_id,
                    f1.day
            )
            SELECT
                f.org_id,
                f.day,
                f.row_count,
                b.local_threshold,
                CAST(f.row_count AS FLOAT) / NULLIF(b.local_threshold, 0) AS pct_of_local
            FROM filled AS f
            JOIN baselines AS b ON f.org_id = b.org_id
                AND f.day = b.day
            WHERE
                f.row_count = 0
                OR f.row_count < b.local_threshold * {percent_of_threshold_before_reporting_gap}
            ORDER BY f.org_id, f.day
            ;
            """
        logger.info(
            f"Running readings gap check on table {self.readings_table_name}. Will report counts below {percent_of_threshold_before_reporting_gap} * {threshold_percentile}th percentile of surrounding {month_window_size * 2} months"
        )
        result = self.connection.cursor().execute(sql).fetchall()
        count = 0
        for row in result:
            # Example row: ('my_org', datetime.date(2025, 8, 25), 100448, Decimal('299104.800'), 0.335828779745427)
            count += 1
            org_id = row[0]
            date = row[1]
            number_of_rows = row[2]
            local_threshold = row[3]
            logger.info(
                f"Org {org_id} has {number_of_rows} rows on {date}. Reporting threshold is {float(local_threshold) * float(percent_of_threshold_before_reporting_gap)}."
            )
        return count == 0

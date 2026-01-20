import datetime
import json
import re
from unittest.mock import Mock

import pytz

from amiadapters.adapters.beacon import BEACON_RAW_SNOWFLAKE_LOADER
from amiadapters.metrics.base import NOOP_METRICS
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import (
    SnowflakeStorageSink,
)
from test.base_test_case import BaseTestCase
from test.amiadapters.test_beacon import beacon_meter_and_read_factory


class TestSnowflakeStorageSink(BaseTestCase):

    def setUp(self):
        self.conn = Mock()
        self.mock_cursor = Mock()
        self.conn.cursor.return_value = self.mock_cursor
        sink_config = Mock()
        sink_config.connection.return_value = self.conn
        self.extract_outputs = ExtractOutput(
            {
                "meters_and_reads.json": json.dumps(
                    beacon_meter_and_read_factory(), cls=DataclassJSONEncoder
                )
            }
        )
        self.snowflake_sink = SnowflakeStorageSink(
            "org-id",
            pytz.timezone("Africa/Algiers"),
            sink_config,
            BEACON_RAW_SNOWFLAKE_LOADER,
            NOOP_METRICS,
        )

    def test_upsert_meters(self):
        meters = [
            GeneralMeter(
                org_id="this-utility",
                device_id="1",
                account_id="101",
                location_id=None,
                meter_id="1",
                endpoint_id=None,
                meter_install_date=datetime.datetime(
                    2022, 2, 8, 22, 10, 43, tzinfo=pytz.timezone("Africa/Algiers")
                ),
                meter_size="0.375",
                meter_manufacturer="manufacturer",
                multiplier=None,
                location_address="my street",
                location_city="my town",
                location_state="CA",
                location_zip="12312",
            ),
        ]

        self.snowflake_sink._upsert_meters(
            meters,
            self.conn,
            row_active_from=datetime.datetime.fromisoformat(
                "2025-04-22T21:01:37.605366+00:00"
            ),
        )

        expected_merge_sql = """
            MERGE INTO meters AS target
            USING(
                SELECT CONCAT(tm.org_id, '|', tm.device_id) as merge_key, tm.*
                FROM temp_meters tm
                
                UNION ALL
                
                SELECT NULL as merge_key, tm2.*
                FROM temp_meters tm2
                JOIN meters m2 ON tm2.org_id = m2.org_id AND tm2.device_id = m2.device_id
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
                    target.row_active_until = '2025-04-22T21:01:37.605366+00:00'
            WHEN NOT MATCHED THEN
                INSERT (org_id, device_id, account_id, location_id, meter_id, 
                        endpoint_id, meter_install_date, meter_size, meter_manufacturer, 
                        multiplier, location_address, location_city, location_state, location_zip,
                        row_active_from)
                VALUES (source.org_id, source.device_id, source.account_id, source.location_id, source.meter_id, 
                        source.endpoint_id, source.meter_install_date, source.meter_size, source.meter_manufacturer, 
                        source.multiplier, source.location_address, source.location_city, source.location_state, source.location_zip,
                        '2025-04-22T21:01:37.605366+00:00');
        """
        called_query = self.mock_cursor.execute.call_args[0][0]

        # Normalize both queries before comparing
        self.assertEqual(
            self.normalize_sql(called_query), self.normalize_sql(expected_merge_sql)
        )

    def test_upsert_meters__fail_on_duplicate_meter(self):
        meter = GeneralMeter(
            org_id="this-utility",
            device_id="1",
            account_id="101",
            location_id=None,
            meter_id="1",
            endpoint_id=None,
            meter_install_date=datetime.datetime(
                2022, 2, 8, 22, 10, 43, tzinfo=pytz.timezone("Africa/Algiers")
            ),
            meter_size="0.375",
            meter_manufacturer="manufacturer",
            multiplier=None,
            location_address="my street",
            location_city="my town",
            location_state="CA",
            location_zip="12312",
        )

        with self.assertRaises(ValueError):
            self.snowflake_sink._upsert_meters(
                # Same meter twice
                [meter, meter],
                self.conn,
                row_active_from=datetime.datetime.fromisoformat(
                    "2025-04-22T21:01:37.605366+00:00"
                ),
            )

    def test_upsert_reads(self):
        reads = [
            GeneralMeterRead(
                org_id="this-utility",
                device_id="1",
                account_id="101",
                location_id=None,
                flowtime=datetime.datetime(
                    2024, 7, 7, 1, 0, tzinfo=pytz.timezone("Africa/Algiers")
                ),
                register_value=116233.61,
                register_unit="CF",
                interval_value=None,
                interval_unit=None,
                battery=None,
                install_date=None,
                connection=None,
                estimated=None,
            ),
            GeneralMeterRead(
                org_id="this-utility",
                device_id="2",
                account_id=None,
                location_id=None,
                flowtime=datetime.datetime(
                    2024, 7, 7, 1, 0, tzinfo=pytz.timezone("Africa/Algiers")
                ),
                register_value=11,
                register_unit="CF",
                interval_value=None,
                interval_unit=None,
                battery=None,
                install_date=None,
                connection=None,
                estimated=None,
            ),
        ]

        self.snowflake_sink._upsert_reads(reads, self.conn)

        expected_merge_sql = """
            MERGE INTO readings AS target
            USING (
                -- Use GROUP BY to ensure there are no duplicate rows before merge
                SELECT org_id, device_id, flowtime, max(account_id) as account_id, max(location_id) as location_id, 
                    max(register_value) as register_value, max(register_unit) as register_unit,
                    max(interval_value) as interval_value, max(interval_unit) as interval_unit,
                    max(battery) as battery, max(install_date) as install_date, 
                    max(connection) as connection, max(estimated) as estimated
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
                    target.interval_unit = source.interval_unit,
                    target.battery = source.battery,
                    target.install_date = source.install_date,
                    target.connection = source.connection,
                    target.estimated = source.estimated
            WHEN NOT MATCHED THEN
                INSERT (org_id, device_id, account_id, location_id, flowtime, 
                        register_value, register_unit, interval_value, interval_unit, battery, install_date, connection, estimated) 
                        VALUES (source.org_id, source.device_id, source.account_id, source.location_id, source.flowtime, 
                    source.register_value, source.register_unit, source.interval_value, source.interval_unit, source.battery, source.install_date, source.connection, source.estimated)
        """
        called_query = self.mock_cursor.execute.call_args[0][0]

        # Normalize both queries before comparing
        self.assertEqual(
            self.normalize_sql(called_query), self.normalize_sql(expected_merge_sql)
        )

    def test_upsert_reads__fail_on_duplicate_meter(self):
        read = GeneralMeterRead(
            org_id="this-utility",
            device_id="1",
            account_id="101",
            location_id=None,
            flowtime=datetime.datetime(
                2024, 7, 7, 1, 0, tzinfo=pytz.timezone("Africa/Algiers")
            ),
            register_value=116233.61,
            register_unit="CF",
            interval_value=None,
            interval_unit=None,
            battery=None,
            install_date=None,
            connection=None,
            estimated=None,
        )

        with self.assertRaises(ValueError):
            self.snowflake_sink._upsert_reads(
                # Same read twice
                [read, read],
                self.conn,
            )

    def normalize_sql(self, sql):
        """Normalize SQL by removing extra whitespace"""
        # Replace multiple spaces, tabs, and newlines with a single space
        normalized = re.sub(r"\s+", " ", sql)
        # Trim leading and trailing whitespace
        return normalized.strip()

    def test_store_raw(self):
        self.snowflake_sink.store_raw(
            "run-id",
            self.extract_outputs,
        )
        self.assertEqual(2, self.mock_cursor.execute.call_count)

    def test_store_raw__skips_when_no_raw_loader(self):
        self.snowflake_sink.raw_loader = None
        self.snowflake_sink.store_raw(
            "run-id",
            self.extract_outputs,
        )
        self.assertEqual(0, self.mock_cursor.execute.call_count)

    def test_verify_no_duplicate_reads_and_return_oldest_flowtime_finds_oldest_flowtime(
        self,
    ):
        reads = [
            GeneralMeterRead(
                org_id="this-utility",
                device_id="1",
                account_id="101",
                location_id=None,
                flowtime=datetime.datetime(
                    2024, 7, 7, 1, 0, tzinfo=pytz.timezone("Africa/Algiers")
                ),
                register_value=116233.61,
                register_unit="CF",
                interval_value=None,
                interval_unit=None,
                battery=None,
                install_date=None,
                connection=None,
                estimated=None,
            ),
            GeneralMeterRead(
                org_id="this-utility",
                device_id="2",
                account_id=None,
                location_id=None,
                flowtime=datetime.datetime(
                    2024, 9, 7, 1, 0, tzinfo=pytz.timezone("Africa/Algiers")
                ),
                register_value=11,
                register_unit="CF",
                interval_value=None,
                interval_unit=None,
                battery=None,
                install_date=None,
                connection=None,
                estimated=None,
            ),
        ]
        oldest_flowtime = (
            self.snowflake_sink._verify_no_duplicate_reads_and_return_oldest_flowtime(
                reads
            )
        )
        self.assertEqual(
            reads[1].flowtime,
            oldest_flowtime,
        )

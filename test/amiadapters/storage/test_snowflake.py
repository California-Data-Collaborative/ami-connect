import datetime
import re
from unittest.mock import Mock

import pytz

from amiadapters.base import GeneralMeter, GeneralMeterRead
from amiadapters.beacon import BeaconSnowflakeStorageSink
from test.base_test_case import BaseTestCase


class TestSnowflakeStorageSink(BaseTestCase):

    def setUp(self):
        self.snowflake_sink = BeaconSnowflakeStorageSink(
            None, None, None, "org-id", pytz.timezone("Africa/Algiers"), None
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

        conn = Mock()
        mock_cursor = Mock()
        conn.cursor.return_value = mock_cursor

        self.snowflake_sink._upsert_meters(
            meters,
            conn,
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
        called_query = mock_cursor.execute.call_args[0][0]

        # Normalize both queries before comparing
        self.assertEqual(
            self.normalize_sql(called_query), self.normalize_sql(expected_merge_sql)
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
            ),
        ]

        conn = Mock()
        mock_cursor = Mock()
        conn.cursor.return_value = mock_cursor

        self.snowflake_sink._upsert_reads(reads, conn)

        expected_merge_sql = """
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
        called_query = mock_cursor.execute.call_args[0][0]

        # Normalize both queries before comparing
        self.assertEqual(
            self.normalize_sql(called_query), self.normalize_sql(expected_merge_sql)
        )

    def normalize_sql(self, sql):
        """Normalize SQL by removing extra whitespace"""
        # Replace multiple spaces, tabs, and newlines with a single space
        normalized = re.sub(r"\s+", " ", sql)
        # Trim leading and trailing whitespace
        return normalized.strip()

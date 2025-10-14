"""
Run integration test for snowflake queries.

Does not run with CI, you must run this manually.
Assumes config.yaml and secrets.yaml are set up with an adapter that uses a Snowflake sink.
Connects to production Snowflake.

Usage:
    python -m test.integration.snowflake_integration_test

"""

import unittest
import datetime
import pytz

from amiadapters.config import AMIAdapterConfiguration
from amiadapters.models import GeneralMeter, GeneralMeterRead
from amiadapters.storage.snowflake import (
    SnowflakeStorageSink,
    SnowflakeMetersUniqueByDeviceIdCheck,
    SnowflakeReadingsUniqueByDeviceIdAndFlowtimeCheck,
)


class BaseSnowflakeIntegrationTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = AMIAdapterConfiguration.from_database()
        # Hack! Pick an adapter out of the config so we can create a connection to Snowflake.
        adapter = cls.config.adapters()[0]
        cls.snowflake_sink = adapter.storage_sinks[0]
        assert isinstance(cls.snowflake_sink, SnowflakeStorageSink)
        cls.test_meters_table = "meters_int_test"
        cls.test_readings_table = "readings_int_test"
        cls.conn = cls.snowflake_sink.sink_config.connection()
        cls.cs = cls.conn.cursor()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def _create_meter(
        self, org_id="org1", device_id="device1", endpoint_id="130615549"
    ) -> GeneralMeter:
        return GeneralMeter(
            org_id=org_id,
            device_id=device_id,
            account_id="303022",
            location_id="303022",
            meter_id="1470158170",
            endpoint_id=endpoint_id,
            meter_install_date=datetime.datetime(
                2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
            ),
            meter_size="0.625",
            meter_manufacturer="BADGER",
            multiplier=1,
            location_address="5391 E. MYSTREET",
            location_city="Apple",
            location_state="CA",
            location_zip="93727",
        )

    def _create_read(
        self,
        device_id: str = "dev1",
        account_id: str = "acct1",
        location_id: str = "loc1",
        estimated: int = 0,
    ) -> GeneralMeterRead:
        return GeneralMeterRead(
            org_id="org1",
            device_id=device_id,
            account_id=account_id,
            location_id=location_id,
            flowtime=datetime.datetime(2024, 1, 1, tzinfo=pytz.UTC),
            register_value=100.0,
            register_unit="GAL",
            interval_value=10.0,
            interval_unit="GAL",
            battery="good",
            install_date=None,
            connection=None,
            estimated=estimated,
        )


class TestSnowflakeUpserts(BaseSnowflakeIntegrationTestCase):

    def setUp(self):
        self.row_active_from = datetime.datetime.now(tz=pytz.UTC)
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_meters_table} LIKE meters;"
        )
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_readings_table} LIKE readings;"
        )

    def test_upsert_meters_does_not_insert_or_update_on_duplicate(self):
        self._assert_num_rows(self.test_meters_table, 0)

        meter = self._create_meter(device_id="device1")
        self.snowflake_sink._upsert_meters(
            [meter],
            self.conn,
            row_active_from=self.row_active_from,
            table_name=self.test_meters_table,
        )

        self._assert_num_rows(self.test_meters_table, 1)

        self.snowflake_sink._upsert_meters(
            [meter], self.conn, table_name=self.test_meters_table
        )

        self._assert_num_rows(self.test_meters_table, 1)
        self.cs.execute(f"SELECT * FROM {self.test_meters_table}")
        result = self.cs.fetchone()
        self.assertEqual(result[-2], self.row_active_from)
        self.assertIsNone(result[-1])

    def test_upsert_meters_inserts_when_non_matched_row_introduced(self):
        self._assert_num_rows(self.test_meters_table, 0)

        meter1 = self._create_meter(device_id="device1")
        self.snowflake_sink._upsert_meters(
            [meter1],
            self.conn,
            row_active_from=self.row_active_from,
            table_name=self.test_meters_table,
        )

        meter2 = self._create_meter(device_id="device2")
        meter3 = self._create_meter(device_id="device1", org_id="org2")
        self.snowflake_sink._upsert_meters(
            [meter2, meter3],
            self.conn,
            row_active_from=self.row_active_from,
            table_name=self.test_meters_table,
        )

        self._assert_num_rows(self.test_meters_table, 3)

    def test_upsert_meters_updates_when_matched_row_has_new_value(self):
        self._assert_num_rows(self.test_meters_table, 0)

        meter = self._create_meter(device_id="device1", endpoint_id="130615549")
        self.snowflake_sink._upsert_meters(
            [meter],
            self.conn,
            row_active_from=self.row_active_from,
            table_name=self.test_meters_table,
        )

        updated_meter = self._create_meter(device_id="device1", endpoint_id="9090909")
        self.snowflake_sink._upsert_meters(
            [updated_meter],
            self.conn,
            row_active_from=self.row_active_from,
            table_name=self.test_meters_table,
        )

        self._assert_num_rows(self.test_meters_table, 2)
        self.cs.execute(
            f"SELECT COUNT(*) FROM {self.test_meters_table} WHERE row_active_until IS NULL"
        )
        self.assertEqual(self.cs.fetchone()[0], 1)

    def test_upsert_reads_inserts_new_reads(self):
        self._assert_num_rows(self.test_readings_table, 0)
        self.snowflake_sink._upsert_reads(
            [self._create_read()], self.conn, table_name=self.test_readings_table
        )
        self._assert_num_rows(self.test_readings_table, 1)

    def test_upsert_reads_updates_existing_read(self):
        self._assert_num_rows(self.test_readings_table, 0)
        read_initial = self._create_read()
        self.snowflake_sink._upsert_reads(
            [read_initial], self.conn, table_name=self.test_readings_table
        )

        read_updated = self._create_read(
            account_id="acct2", location_id="loc2", estimated=1
        )

        self.snowflake_sink._upsert_reads(
            [read_updated], self.conn, table_name=self.test_readings_table
        )

        self.cs.execute(f"SELECT * FROM {self.test_readings_table}")
        rows = self.cs.fetchall()
        self.assertEqual(len(rows), 1)
        updated_row = rows[0]
        self.assertEqual(updated_row[1], "acct2")  # account_id
        self.assertEqual(updated_row[2], "loc2")  # location_id
        self.assertEqual(updated_row[5], 100.0)  # register_value
        self.assertEqual(updated_row[11], 1)  # estimated

    def test_upsert_reads_inserts_new_row_when_not_matched(self):
        self._assert_num_rows(self.test_readings_table, 0)

        read_initial = self._create_read()
        self.snowflake_sink._upsert_reads(
            [read_initial], self.conn, table_name=self.test_readings_table
        )

        new_read = self._create_read(device_id="other")
        self.snowflake_sink._upsert_reads(
            [new_read], self.conn, table_name=self.test_readings_table
        )

        self._assert_num_rows(self.test_readings_table, 2)

    def _assert_num_rows(self, table_name: str, expected_number_of_rows: int):
        self.cs.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = self.cs.fetchone()[0]
        self.assertEqual(result, expected_number_of_rows)


class TestSnowflakeDataQualityChecks(BaseSnowflakeIntegrationTestCase):

    def setUp(self):
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_meters_table} LIKE meters;"
        )
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_readings_table} LIKE readings;"
        )

    def test_meter_uniqueness__passes_when_meters_unique(self):
        check = SnowflakeMetersUniqueByDeviceIdCheck(
            connection=self.conn,
            meter_table_name=self.test_meters_table,
        )
        meter1 = self._create_meter(device_id="1")
        meter2 = self._create_meter(device_id="2")
        self.snowflake_sink._upsert_meters(
            [meter1, meter2],
            self.conn,
            row_active_from=datetime.datetime.now(),
            table_name=self.test_meters_table,
        )
        self.assertTrue(check.check())

    def test_meter_uniqueness__passes_when_meters_unique(self):
        check = SnowflakeReadingsUniqueByDeviceIdAndFlowtimeCheck(
            connection=self.conn,
            readings_table_name=self.test_readings_table,
        )
        reading1 = self._create_read(device_id="1")
        reading2 = self._create_read(device_id="2")
        self.snowflake_sink._upsert_reads(
            [reading1, reading2],
            self.conn,
            table_name=self.test_readings_table,
        )
        self.assertTrue(check.check())


if __name__ == "__main__":
    unittest.main()

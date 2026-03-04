"""
Run integration test for snowflake queries.

Does not run with CI, you must run this manually.
Assumes configuration is set up with an adapter that uses a Snowflake sink.
Connects to production Snowflake.

Usage:
    AMI_CONNECT__AWS_PROFILE=my-profile python -m test.integration.snowflake_integration_test

"""

import datetime
import json
import os
import pytz
import unittest

from amiadapters.adapters.aclara import AclaraBaseTableLoader, AclaraMeterAndRead
from amiadapters.adapters.subeca import (
    RawAccountsLoader,
    RawLatestReadingLoader,
    RawUsageLoader,
    SubecaAccount,
    SubecaReading,
)
from amiadapters.configuration.env import set_global_aws_profile, set_global_aws_region
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.models import GeneralMeter, GeneralMeterRead, DataclassJSONEncoder
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import (
    RawSnowflakeLoader,
    SnowflakeStorageSink,
    SnowflakeMetersUniqueByDeviceIdCheck,
    SnowflakeReadingsUniqueByDeviceIdAndFlowtimeCheck,
)


class BaseSnowflakeIntegrationTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        profile = os.environ.get("AMI_CONNECT__AWS_PROFILE")
        set_global_aws_profile(aws_profile=profile)
        set_global_aws_region(None)
        cls.config = AMIAdapterConfiguration.from_database()
        # Hack! Pick an adapter out of the config so we can create a connection to Snowflake.
        adapter = cls.config.adapters()[0]
        cls.snowflake_sink = adapter.storage_sinks[0]
        assert isinstance(cls.snowflake_sink, SnowflakeStorageSink)
        cls.test_meters_table = "meters_int_test"
        cls.test_readings_table = "readings_int_test"
        cls.test_meter_alerts_table = "meter_alerts_int_test"
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
        interval_value: float = 10.0,
        flowtime: datetime = datetime.datetime(2024, 1, 1, 0, 0, tzinfo=pytz.UTC),
    ) -> GeneralMeterRead:
        return GeneralMeterRead(
            org_id="org1",
            device_id=device_id,
            account_id=account_id,
            location_id=location_id,
            flowtime=flowtime,
            register_value=100.0,
            register_unit="GAL",
            interval_value=interval_value,
            interval_unit="GAL",
            battery="good",
            install_date=None,
            connection=None,
            estimated=estimated,
        )

    def _assert_num_rows(self, table_name: str, expected_number_of_rows: int):
        self.cs.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = self.cs.fetchone()[0]
        self.assertEqual(result, expected_number_of_rows)


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

        self.cs.execute(
            f"SELECT account_id, location_id, register_value, estimated FROM {self.test_readings_table}"
        )
        rows = self.cs.fetchall()
        self.assertEqual(len(rows), 1)
        updated_row = rows[0]
        self.assertEqual(updated_row[0], "acct2")  # account_id
        self.assertEqual(updated_row[1], "loc2")  # location_id
        self.assertEqual(updated_row[2], 100.0)  # register_value
        self.assertEqual(updated_row[3], 1)  # estimated

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


class TestSnowflakeContinuousFlowAlerts(BaseSnowflakeIntegrationTestCase):

    def setUp(self):
        self.row_active_from = datetime.datetime.now(tz=pytz.UTC)
        self.now = datetime.datetime(2024, 1, 10, 12, 0, tzinfo=pytz.UTC)
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_meters_table} LIKE meters;"
        )
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_readings_table} LIKE readings;"
        )
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_meter_alerts_table} LIKE meter_alerts;"
        )
        # TODO why? is this affecting production flowtimes or flowtimes when I run from my laptop?
        self.conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")
        self.conn.cursor().execute(
            "ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_TZ'"
        )

    def _insert_reading_streak(
        self,
        device_id: str,
        start_time: datetime,
        hours: int,
        value: float,
        insert_leading_zero: bool = True,
    ):
        """Helper to create consecutive hourly readings."""
        readings = []
        if insert_leading_zero:
            # Start with a "zero" reading before the streak to ensure it starts at the correct time.
            # Otherwise our model does not consider the first read to be "clean", given that it did not occur one hour after the previous read.
            readings.append(
                self._create_read(
                    device_id=device_id,
                    flowtime=start_time - datetime.timedelta(hours=1),
                    interval_value=0,
                    estimated=0,
                )
            )
        # Add a read for every hour in the streak
        for i in range(hours):
            flowtime = start_time + datetime.timedelta(hours=i)
            # We use the GeneralMeterRead model to ensure compatibility with your existing schema
            read = self._create_read(
                device_id=device_id,
                flowtime=flowtime,
                interval_value=value,
                estimated=0,
            )
            readings.append(read)

        self.snowflake_sink._upsert_reads(
            readings, self.conn, table_name=self.test_readings_table
        )

    def test_alert_created_after_24_hour_streak(self):
        device_id = "leak_device"
        min_date = self.now - datetime.timedelta(hours=30)
        max_date = self.now + datetime.timedelta(days=3)
        self._assert_num_rows(self.test_meter_alerts_table, 0)

        # 1. Insert 25 hours of continuous flow (value > 0)
        start_streak = self.now - datetime.timedelta(hours=25)
        self._insert_reading_streak(device_id, start_streak, 25, 1.5)

        # 2. Run the function
        # Note: self.snowflake_sink should be the object owning the method
        self.snowflake_sink._upsert_continuous_flow_alerts(
            self.conn,
            min_date,
            max_date,
            org_id="org1",
            meter_alerts_table_name=self.test_meter_alerts_table,
            readings_table_name=self.test_readings_table,
        )

        # 3. Assertions
        self._assert_num_rows(self.test_meter_alerts_table, 1)
        self.cs.execute(
            f"SELECT alert_type, start_time, end_time FROM {self.test_meter_alerts_table}"
        )
        alert = self.cs.fetchone()
        self.assertEqual(alert[0], "continuous_flow")
        self.assertEqual(
            alert[1].isoformat(), start_streak.isoformat()
        )  # START_TIME should match the start of the streak
        self.assertIsNone(
            alert[2]
        )  # IS_ACTIVE should be true, so end_time should be NULL

    def test_no_alert_when_streak_is_too_short(self):
        device_id = "short_streak_device"
        min_date = self.now - datetime.timedelta(hours=30)
        max_date = self.now + datetime.timedelta(days=3)

        # Insert only 10 hours of flow
        self._insert_reading_streak(
            device_id, self.now - datetime.timedelta(hours=10), 10, 5.0
        )

        self.snowflake_sink._upsert_continuous_flow_alerts(
            self.conn,
            min_date,
            max_date,
            org_id="org1",
            meter_alerts_table_name=self.test_meter_alerts_table,
            readings_table_name=self.test_readings_table,
        )

        # Should NOT result in an alert because threshold is 24 hours
        self._assert_num_rows(self.test_meter_alerts_table, 0)

    def test_alert_closed_when_flow_stops(self):
        device_id = "closed_leak_device"

        # 1. Create an active leak (25 hours of flow)
        min_date = self.now - datetime.timedelta(hours=30)
        max_date = self.now + datetime.timedelta(days=3)
        self._insert_reading_streak(device_id, min_date, 25, 1.0)
        self.snowflake_sink._upsert_continuous_flow_alerts(
            self.conn,
            min_date,
            max_date,
            org_id="org1",
            meter_alerts_table_name=self.test_meter_alerts_table,
            readings_table_name=self.test_readings_table,
        )

        # Validate that an alert was created and is active (end_time is NULL)
        self.cs.execute(
            f"SELECT end_time FROM {self.test_meter_alerts_table} WHERE device_id = '{device_id}'"
        )
        end_time = self.cs.fetchone()[0]
        self.assertIsNone(end_time)

        # 2. Insert a "zero" reading to break the leak
        zero_read = self._create_read(
            device_id=device_id,
            flowtime=self.now + datetime.timedelta(hours=1),
            interval_value=0.0,
        )
        self.snowflake_sink._upsert_reads(
            [zero_read], self.conn, table_name=self.test_readings_table
        )

        # 3. Run alerts again
        self.snowflake_sink._upsert_continuous_flow_alerts(
            self.conn,
            min_date,
            max_date,
            org_id="org1",
            meter_alerts_table_name=self.test_meter_alerts_table,
            readings_table_name=self.test_readings_table,
        )

        # 4. Verify the alert now has an END_TIME
        self._assert_num_rows(self.test_meter_alerts_table, 1)
        self.cs.execute(
            f"SELECT end_time FROM {self.test_meter_alerts_table} WHERE device_id = '{device_id}'"
        )
        end_time = self.cs.fetchone()[0]
        self.assertIsNotNone(end_time)


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


class IntTestSubecaRawAccountsLoader(RawAccountsLoader):
    # Override the table name for the integration test
    def table_name(self) -> str:
        return "SUBECA_ACCOUNT_BASE_int_test"


class IntTestSubecaRawLatestReadingLoader(RawLatestReadingLoader):
    # Override the table name for the integration test
    def table_name(self) -> str:
        return "SUBECA_DEVICE_LATEST_READ_BASE_int_test"


class IntTestSubecaRawUsageLoader(RawUsageLoader):
    # Override the table name for the integration test
    def table_name(self) -> str:
        return "SUBECA_USAGE_BASE_int_test"


class TestSubecaRawSnowflakeLoader(BaseSnowflakeIntegrationTestCase):

    def setUp(self):
        self.test_subeca_account_base_table = "SUBECA_ACCOUNT_BASE_int_test"
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_subeca_account_base_table} LIKE SUBECA_ACCOUNT_BASE;"
        )
        self.test_subeca_device_latest_read_base_table = (
            "SUBECA_DEVICE_LATEST_READ_BASE_int_test"
        )
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_subeca_device_latest_read_base_table} LIKE SUBECA_DEVICE_LATEST_READ_BASE;"
        )
        self.test_subeca_usage_base_table = "SUBECA_USAGE_BASE_int_test"
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_subeca_usage_base_table} LIKE SUBECA_USAGE_BASE;"
        )
        self.loader = RawSnowflakeLoader.with_table_loaders(
            [
                IntTestSubecaRawAccountsLoader(),
                IntTestSubecaRawLatestReadingLoader(),
                IntTestSubecaRawUsageLoader(),
            ]
        )

    def test_load_upserts_new_row(self):
        latest_reading = SubecaReading(
            deviceId="testDeviceId",
            usageTime="2025-01-01",
            value="1",
            unit="CF",
        )
        accounts = [
            SubecaAccount(
                accountId="accountId",
                accountStatus="accountStatus",
                meterSerial="meterSerial",
                billingRoute="billingRoute",
                registerSerial="registerSerial",
                meterSize="meterSize",
                createdAt="createdAt",
                deviceId="testDeviceId",
                activeProtocol="activeProtocol",
                installationDate="installationDate",
                latestCommunicationDate="latestCommunicationDate",
                latestReading=latest_reading,
            )
        ]
        usages = [
            SubecaReading(
                deviceId="testDeviceId",
                usageTime="2025-02-01",
                value="44",
                unit="CF",
            )
        ]
        extract_outputs = ExtractOutput(
            {
                "accounts.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in accounts
                ),
                "usages.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in usages
                ),
            }
        )

        self._assert_num_rows(self.test_subeca_account_base_table, 0)
        self._assert_num_rows(self.test_subeca_usage_base_table, 0)
        self._assert_num_rows(self.test_subeca_device_latest_read_base_table, 0)

        # Load data into empty table
        self.loader.load(
            "run-1",
            "org1",
            pytz.UTC,
            extract_outputs,
            self.conn,
        )
        self._assert_num_rows(self.test_subeca_account_base_table, 1)
        self._assert_num_rows(self.test_subeca_usage_base_table, 1)
        self._assert_num_rows(self.test_subeca_device_latest_read_base_table, 1)

        # Load data again and make sure it didn't create new rows
        self.loader.load(
            "run-1",
            "org1",
            pytz.UTC,
            extract_outputs,
            self.conn,
        )
        self._assert_num_rows(self.test_subeca_account_base_table, 1)
        self._assert_num_rows(self.test_subeca_usage_base_table, 1)
        self._assert_num_rows(self.test_subeca_device_latest_read_base_table, 1)

        # Check account table has correct values
        self.cs.execute(f"SELECT * FROM {self.test_subeca_account_base_table}")
        account = self.cs.fetchone()
        self.assertEqual("org1", account[0])
        self.assertEqual("testDeviceId", account[9])

        # Check usage table has correct values
        self.cs.execute(f"SELECT * FROM {self.test_subeca_usage_base_table}")
        usage = self.cs.fetchone()
        self.assertEqual("testDeviceId", usage[2])
        self.assertEqual("2025-02-01", usage[3])
        self.assertEqual("CF", usage[4])
        self.assertEqual("44", usage[5])

        # Check latest reading table has correct values
        self.cs.execute(
            f"SELECT * FROM {self.test_subeca_device_latest_read_base_table}"
        )
        latest_read = self.cs.fetchone()
        self.assertEqual("testDeviceId", latest_read[2])
        self.assertEqual("2025-01-01", latest_read[3])
        self.assertEqual("CF", latest_read[4])
        self.assertEqual("1", latest_read[5])


class IntTestAclaraBaseTableLoader(AclaraBaseTableLoader):
    # Override the table name for the integration test
    def table_name(self) -> str:
        return "ACLARA_BASE_int_test"


class TestAclaraRawSnowflakeLoader(BaseSnowflakeIntegrationTestCase):

    def setUp(self):
        table_loader = IntTestAclaraBaseTableLoader()
        self.test_aclara_base_table = table_loader.table_name()
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_aclara_base_table} LIKE ACLARA_BASE;"
        )
        self.loader = RawSnowflakeLoader.with_table_loaders([table_loader])

    def test_load_upserts_new_row(self):
        meter_and_read = AclaraMeterAndRead(
            AccountNumber="17305709",
            MeterSN="1",
            MTUID="2",
            Port="1",
            AccountType="Residential",
            Address1="12 MY LN",
            City="LOS ANGELES",
            State="CA",
            Zip="00000",
            RawRead="23497071",
            ScaledRead="1",
            ReadingTime="2025-05-25 16:00:00.000",
            LocalTime="2025-05-25 09:00:00.000",
            Active="1",
            Scalar="0.001",
            MeterTypeID="2212",
            Vendor="BADGER",
            Model="HR-E LCD",
            Description="Badger M25/LP HRE LCD 5/8x3/4in 9D 0.001CuFt",
            ReadInterval="60",
        )

        extract_outputs = ExtractOutput(
            {
                "meters_and_reads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in [meter_and_read]
                ),
            }
        )

        self._assert_num_rows(self.test_aclara_base_table, 0)

        # Load data into empty table
        self.loader.load(
            "run-1",
            "org1",
            pytz.UTC,
            extract_outputs,
            self.conn,
        )
        self._assert_num_rows(self.test_aclara_base_table, 1)

        # Load data again and make sure it didn't create new rows
        self.loader.load(
            "run-1",
            "org1",
            pytz.UTC,
            extract_outputs,
            self.conn,
        )
        self._assert_num_rows(self.test_aclara_base_table, 1)

        # Check account table has correct values
        self.cs.execute(f"SELECT * FROM {self.test_aclara_base_table}")
        meter_and_read = self.cs.fetchone()
        self.assertEqual("org1", meter_and_read[0])
        self.assertEqual("17305709", meter_and_read[3])


if __name__ == "__main__":
    unittest.main()

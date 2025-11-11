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

from amiadapters.adapters.aclara import AclaraRawSnowflakeLoader, AclaraMeterAndRead
from amiadapters.adapters.subeca import (
    SubecaAccount,
    SubecaRawSnowflakeLoader,
    SubecaReading,
)
from amiadapters.configuration.env import set_global_aws_profile, set_global_aws_region
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.models import GeneralMeter, GeneralMeterRead, DataclassJSONEncoder
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import (
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
        self.loader = SubecaRawSnowflakeLoader(
            base_accounts_table=self.test_subeca_account_base_table,
            base_usage_table=self.test_subeca_usage_base_table,
            base_latest_read_table=self.test_subeca_device_latest_read_base_table,
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


class TestAclaraRawSnowflakeLoader(BaseSnowflakeIntegrationTestCase):

    def setUp(self):
        self.test_aclara_base_table = "ACLARA_BASE_int_test"
        self.cs.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {self.test_aclara_base_table} LIKE ACLARA_BASE;"
        )
        self.loader = AclaraRawSnowflakeLoader(
            base_table=self.test_aclara_base_table,
        )

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
        print(meter_and_read)
        self.assertEqual("org1", meter_and_read[0])
        self.assertEqual("17305709", meter_and_read[3])


if __name__ == "__main__":
    unittest.main()

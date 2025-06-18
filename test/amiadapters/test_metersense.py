import datetime
from unittest.mock import Mock

import pytz

from amiadapters.config import ConfiguredLocalTaskOutputController
from amiadapters.adapters.metersense import (
    MetersenseAdapter,
    MetersenseIntervalRead,
    MetersenseMeterLocation,
    MetersenseRegisterRead,
)
from test.base_test_case import BaseTestCase


class TestMetersenseAdapter(BaseTestCase):

    def setUp(self):
        self.tz = pytz.timezone("Europe/Rome")
        self.adapter = MetersenseAdapter(
            org_id="this-org",
            org_timezone=self.tz,
            configured_task_output_controller=ConfiguredLocalTaskOutputController(
                "/tmp/output"
            ),
            configured_sinks=[],
            ssh_tunnel_server_host="tunnel-ip",
            ssh_tunnel_username="ubuntu",
            ssh_tunnel_key_path="/key",
            database_host="db-host",
            database_port=1521,
            database_db_name="db-name",
            database_user="dbu",
            database_password="dbp",
        )

    def _meter_location_factory(self) -> MetersenseMeterLocation:
        return MetersenseMeterLocation(
            meter_id="m1",
            alt_meter_id="altm1",
            meter_tp="W-DISC34",
            meters_commodity_tp="W",
            region_id="California",
            interval_length="60",
            regread_frequency="1440",
            channel1_raw_uom="CF",
            channel2_raw_uom="",
            channel3_raw_uom="",
            channel4_raw_uom="",
            channel5_raw_uom="",
            channel6_raw_uom="",
            channel7_raw_uom="",
            channel8_raw_uom="",
            channel1_multiplier="0.01",
            channel2_multiplier="",
            channel3_multiplier="",
            channel4_multiplier="",
            channel5_multiplier="",
            channel6_multiplier="",
            channel7_multiplier="",
            channel8_multiplier="",
            channel1_final_uom="CCF",
            channel2_final_uom="",
            channel3_final_uom="",
            channel4_final_uom="",
            channel5_final_uom="",
            channel6_final_uom="",
            channel7_final_uom="",
            channel8_final_uom="",
            first_data_ts="2023-02-18 00:00:00.000",
            last_data_ts="2025-06-15 00:00:00.000",
            ami_id="default",
            power_status="ON",
            meters_latitude="33.8252339",
            meters_longitude="118.1034094",
            exclude_in_reports="N",
            meters_add_by="ODS",
            meters_add_dt="2023-02-17 23:12:13.000",
            meters_change_by="ODS",
            meters_change_dt="2025-06-15 02:22:07.000",
            locations_location_no="loc1",
            alt_location_id="loc1",
            location_class="SFD",
            unit_no="",
            street_no="100",
            street_pfx="",
            street_name="Metropolis",
            street_sfx="",
            street_sfx_dir="",
            city="Metropolis",
            state="NY",
            postal_cd="12345",
            billing_cycle="",
            locations_add_by="ODS",
            locations_add_dt="2019-10-24 11:47:48.000",
            locations_change_by="ODS",
            locations_change_dt="2025-06-10 23:13:20.000",
            locations_latitude="33.825194555",
            locations_longitude="118.10331495",
            service_id="serv1",
            account_id="acc1",
            accounts_location_no="loc1",
            accounts_commodity_tp="W",
            last_read_dt="2023-12-25 00:00:00.000",
            active_dt="2021-03-01 00:00:00.000",
            inactive_dt="9999-12-31 00:00:00.000",
        )

    def _interval_read_factory(
        self, meter_id: str = "m1", read_dtm: str = "2024-01-01 01:00:00"
    ) -> MetersenseIntervalRead:
        return MetersenseIntervalRead(
            meter_id=meter_id,
            channel_id="1",
            read_dt=None,
            read_hr=None,
            read_30min_int=None,
            read_15min_int=None,
            read_5min_int=None,
            status="status",
            read_version=1,
            read_dtm=read_dtm,
            read_value=0.5,
            uom="CCF",
        )

    def _register_read_factory(
        self, meter_id: str = "m1", read_dtm: str = "2024-01-01 01:00:00"
    ) -> MetersenseRegisterRead:
        return MetersenseRegisterRead(
            meter_id=meter_id,
            read_dtm=read_dtm,
            read_value=10.5,
            uom="CCF",
            channel_id="1",
            status="status",
            read_version=1,
        )

    def test_init(self):
        self.assertEqual("tunnel-ip", self.adapter.ssh_tunnel_server_host)
        self.assertEqual("ubuntu", self.adapter.ssh_tunnel_username)
        self.assertEqual("/key", self.adapter.ssh_tunnel_key_path)
        self.assertEqual("db-host", self.adapter.database_host)
        self.assertEqual(1521, self.adapter.database_port)
        self.assertEqual("db-name", self.adapter.database_db_name)
        self.assertEqual("dbu", self.adapter.database_user)
        self.assertEqual("dbp", self.adapter.database_password)

    def test_extract_meters_executes_expected_query_and_parses_results(self):
        # Arrange
        mock_cursor = Mock()
        mock_execute_result = Mock()
        mock_cursor.execute.return_value = mock_execute_result

        # Mock a single row of results
        mock_row = [
            "87195806",
            "ALT87195806",
            "W-DISC34",
            "W",
            "California",
            "60",
            "1440",
            "CF",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "0.01",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "CCF",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            datetime.datetime(2023, 2, 18, 0, 0),
            datetime.datetime(2025, 6, 15, 0, 0),
            "default",
            "ON",
            "33.8252339",
            "118.1034094",
            "N",
            "ODS",
            datetime.datetime(2023, 2, 17, 23, 12, 13),
            "ODS",
            datetime.datetime(2025, 6, 15, 2, 22, 7),
            "6523400466",
            "6523400466",
            "SFD",
            "",
            "",
            "",
            "LOS COYOTES DIA",
            "",
            "",
            "LONG BEACH",
            "CA",
            "90808-2409",
            "",
            "ODS",
            datetime.datetime(2019, 10, 24, 11, 47, 48),
            "ODS",
            datetime.datetime(2025, 6, 10, 23, 13, 20),
            "33.825194555",
            "118.10331495",
            "4453033580",
            "445303358044515438576523400466",
            "6523400466",
            "W",
            datetime.datetime(2023, 12, 25, 0, 0),
            datetime.datetime(2021, 3, 1, 0, 0),
            datetime.datetime(9999, 12, 31, 0, 0),
        ]
        mock_cursor.fetchall.return_value = [mock_row]

        # Act
        result = self.adapter._extract_meters(mock_cursor)

        # Assert
        mock_cursor.execute.assert_called_once()
        mock_cursor.fetchall.assert_called_once()
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MetersenseMeterLocation)
        self.assertEqual(result[0].meter_id, "87195806")
        self.assertEqual(result[0].last_read_dt, "2023-12-25T00:00:00")
        self.assertEqual(result[0].channel1_multiplier, "0.01")
        self.assertEqual(result[0].street_name, "LOS COYOTES DIA")
        self.assertEqual(result[0].accounts_commodity_tp, "W")

    def test_extract_interval_reads_executes_query_and_parses_rows(self):
        # Arrange
        mock_cursor = Mock()

        # Define mock input range
        start = datetime.datetime(2024, 1, 1, 0, 0)
        end = datetime.datetime(2024, 1, 2, 0, 0)

        # Define a mock row with all fields required by MetersenseIntervalRead
        mock_row = [
            "87195806",
            "1",
            "2024-01-01",
            "23",
            "1",
            "0",
            "0",
            datetime.datetime(2024, 1, 1, 23, 0),
            "0.003000",
            "CCF",
            "1",
            "1",
        ]
        mock_cursor.execute.return_value = [mock_row]

        # Act
        result = self.adapter._extract_interval_reads(mock_cursor, start, end)

        # Assert
        mock_cursor.execute.assert_called_once()
        args, kwargs = mock_cursor.execute.call_args
        self.assertIn("FROM intervalreads r", args[0])
        self.assertEqual(kwargs["range_start"], start)
        self.assertEqual(kwargs["range_end"], end)

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MetersenseIntervalRead)
        self.assertEqual(result[0].meter_id, "87195806")
        self.assertEqual(result[0].read_value, "0.003000")
        self.assertEqual(result[0].read_dtm, "2024-01-01T23:00:00")
        self.assertEqual(result[0].uom, "CCF")
        self.assertEqual(result[0].read_version, "1")

    def test_extract_register_reads_executes_query_and_parses_rows(self):
        # Arrange
        mock_cursor = Mock()
        start = datetime.datetime(2024, 1, 1, 0, 0)
        end = datetime.datetime(2024, 1, 2, 0, 0)

        # Mocked row data matching MetersenseRegisterRead fields
        mock_row = [
            "87195806",
            "1",
            datetime.datetime(2024, 1, 1, 23, 0),
            "171.697000",
            "CCF",
            "1",
            "1",
        ]
        mock_cursor.execute.return_value = [mock_row]

        # Act
        result = self.adapter._extract_register_reads(mock_cursor, start, end)

        # Assert
        mock_cursor.execute.assert_called_once()
        args, kwargs = mock_cursor.execute.call_args
        self.assertIn("FROM registerreads r", args[0])
        self.assertEqual(kwargs["range_start"], start)
        self.assertEqual(kwargs["range_end"], end)

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MetersenseRegisterRead)
        self.assertEqual(result[0].meter_id, "87195806")
        self.assertEqual(result[0].read_value, "171.697000")
        self.assertEqual(result[0].read_dtm, "2024-01-01T23:00:00")
        self.assertEqual(result[0].uom, "CCF")
        self.assertEqual(result[0].status, "1")

    def test_transform_single_meter_and_reads(self):
        m = self._meter_location_factory()
        raw_meters = [m]
        raw_interval_reads = [
            self._interval_read_factory(
                meter_id=m.meter_id, read_dtm="2024-01-01 01:00:00"
            ),
            self._interval_read_factory(
                meter_id=m.meter_id, read_dtm="2024-01-01 02:00:00"
            ),
        ]
        raw_register_reads = [
            # Same read_dtm as first interval read
            self._register_read_factory(
                meter_id=m.meter_id, read_dtm="2024-01-01 01:00:00"
            )
        ]

        meters, reads = self.adapter._transform_meters_and_reads(
            raw_meters, raw_interval_reads, raw_register_reads
        )

        # Meter assertions
        self.assertEqual(len(meters), 1)
        meter = meters[0]
        self.assertEqual(meter.device_id, "m1")
        self.assertEqual(meter.account_id, "acc1")
        self.assertEqual(meter.location_zip, "12345")

        # Read assertions
        self.assertEqual(len(reads), 2)
        read_1 = reads[0]
        self.assertEqual(read_1.device_id, "m1")
        self.assertEqual(read_1.account_id, "acc1")
        self.assertEqual(read_1.location_id, "loc1")
        self.assertEqual(read_1.interval_value, 0.5)
        self.assertEqual(read_1.register_value, 10.5)
        self.assertEqual(read_1.interval_unit, "CCF")
        self.assertEqual(read_1.register_unit, "CCF")
        self.assertEqual(
            read_1.flowtime, datetime.datetime(2024, 1, 1, 1, 0, 0, tzinfo=self.tz)
        )

        read_2 = reads[1]
        self.assertEqual(read_2.device_id, "m1")
        self.assertEqual(read_2.account_id, "acc1")
        self.assertEqual(read_2.location_id, "loc1")
        self.assertEqual(read_2.interval_value, 0.5)
        self.assertEqual(read_2.register_value, None)
        self.assertEqual(read_2.interval_unit, "CCF")
        self.assertEqual(read_2.register_unit, None)
        self.assertEqual(
            read_2.flowtime, datetime.datetime(2024, 1, 1, 2, 0, 0, tzinfo=self.tz)
        )

    def test_transform_missing_register_read(self):
        m = self._meter_location_factory()
        raw_meters = [m]
        raw_interval_reads = [
            self._interval_read_factory(
                meter_id=m.meter_id, read_dtm="2024-01-01 01:00:00"
            ),
        ]
        raw_register_reads = []

        meters, reads = self.adapter._transform_meters_and_reads(
            raw_meters, raw_interval_reads, raw_register_reads
        )

        self.assertEqual(len(meters), 1)
        self.assertEqual(len(reads), 1)
        read = reads[0]
        self.assertIsNone(read.register_value)
        self.assertEqual(read.interval_value, 0.5)

    def test_transform_missing_interval_read(self):
        m = self._meter_location_factory()
        raw_meters = [m]
        raw_interval_reads = []
        raw_register_reads = [self._register_read_factory(meter_id=m.meter_id)]

        meters, reads = self.adapter._transform_meters_and_reads(
            raw_meters, raw_interval_reads, raw_register_reads
        )

        self.assertEqual(len(meters), 1)
        self.assertEqual(len(reads), 1)
        read = reads[0]
        self.assertIsNone(read.interval_value)
        self.assertEqual(read.register_value, 10.5)

    def test_transform_missing_meters(self):
        raw_meters = []
        raw_interval_reads = [self._interval_read_factory(meter_id="some-meter")]
        raw_register_reads = [self._register_read_factory(meter_id="another-meter")]

        meters, reads = self.adapter._transform_meters_and_reads(
            raw_meters, raw_interval_reads, raw_register_reads
        )

        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 2)
        read = reads[0]
        self.assertIsNone(read.account_id)
        self.assertIsNone(read.location_id)

    def test_transform_duplicate_meters(self):
        raw_meters = [
            self._meter_location_factory(),
            self._meter_location_factory(),
            self._meter_location_factory(),
        ]
        raw_interval_reads = []
        raw_register_reads = []

        meters, reads = self.adapter._transform_meters_and_reads(
            raw_meters, raw_interval_reads, raw_register_reads
        )

        self.assertEqual(len(meters), 1)

    def test_transform_duplicate_interval_reads(self):
        raw_meters = []
        raw_interval_reads = [
            self._interval_read_factory(),
            self._interval_read_factory(),
        ]
        raw_register_reads = []

        meters, reads = self.adapter._transform_meters_and_reads(
            raw_meters, raw_interval_reads, raw_register_reads
        )

        self.assertEqual(len(reads), 1)

    def test_transform_duplicate_register_reads(self):
        raw_meters = []
        raw_interval_reads = []
        raw_register_reads = [
            self._register_read_factory(),
            self._register_read_factory(),
        ]

        meters, reads = self.adapter._transform_meters_and_reads(
            raw_meters, raw_interval_reads, raw_register_reads
        )

        self.assertEqual(len(reads), 1)

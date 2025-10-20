import datetime
import json
from unittest.mock import MagicMock, patch

import pytz

from amiadapters.configuration.models import (
    LocalIntermediateOutputControllerConfiguration,
)
from amiadapters.adapters.metersense import (
    MetersenseAdapter,
    MetersenseIntervalRead,
    MetersenseMeter,
    MetersenseAccountService,
    MetersenseLocation,
    MetersenseMeterLocationXref,
    MetersenseMetersView,
    MetersenseRawSnowflakeLoader,
    MetersenseRegisterRead,
)
from amiadapters.models import DataclassJSONEncoder
from amiadapters.outputs.base import ExtractOutput
from test.base_test_case import BaseTestCase


class TestMetersenseAdapter(BaseTestCase):

    def setUp(self):
        self.tz = pytz.timezone("Europe/Rome")
        self.adapter = MetersenseAdapter(
            org_id="this-org",
            org_timezone=self.tz,
            configured_task_output_controller=LocalIntermediateOutputControllerConfiguration(
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

    def _account_service_factory(
        self, active_dt="2024-01-01T00:00:00", inactive_dt="2025-01-01T00:00:00"
    ) -> MetersenseAccountService:
        return MetersenseAccountService(
            service_id="SVC123",
            account_id="ACC456",
            location_no="1001",
            commodity_tp="W",
            last_read_dt="2024-12-01T10:00:00",
            active_dt=active_dt,
            inactive_dt=inactive_dt,
        )

    def _location_factory(self, location_no="1001") -> MetersenseLocation:
        return MetersenseLocation(
            location_no=location_no,
            alt_location_id="ALT1001",
            location_class="RES",
            unit_no="1A",
            street_no="123",
            street_pfx="N",
            street_name="Main",
            street_sfx="St",
            street_sfx_dir="E",
            city="Anytown",
            state="NY",
            postal_cd="12345",
            billing_cycle="Monthly",
            add_by="admin",
            add_dt="2023-06-01T12:00:00",
            change_by="admin2",
            change_dt="2024-01-01T12:00:00",
            latitude="40.7128",
            longitude="-74.0060",
        )

    def _meter_location_xref_factory(
        self,
        meter_id="MTR001",
        location_no="1001",
        active_dt="2024-01-01T00:00:00",
        inactive_dt="2025-01-01T00:00:00",
    ) -> MetersenseMeterLocationXref:
        return MetersenseMeterLocationXref(
            meter_id=meter_id,
            active_dt=active_dt,
            location_no=location_no,
            inactive_dt=inactive_dt,
            add_by="admin",
            add_dt="2024-01-01T12:00:00",
            change_by="tech2",
            change_dt="2025-01-01T12:00:00",
        )

    def _meter_view_factory(self, meter_id="MTR001") -> MetersenseMetersView:
        return MetersenseMetersView(
            meter_id=meter_id,
            alt_meter_id="ALT001",
            meter_tp="Digital",
            commodity_tp="W",
            region_id="Region1",
            interval_length="60",
            regread_frequency="24",
            channel1_raw_uom="GAL",
            channel2_raw_uom="",
            channel3_raw_uom="",
            channel4_raw_uom="",
            channel5_raw_uom="",
            channel6_raw_uom="",
            channel7_raw_uom="",
            channel8_raw_uom="",
            channel1_multiplier="1.0",
            channel2_multiplier="",
            channel3_multiplier="",
            channel4_multiplier="",
            channel5_multiplier="",
            channel6_multiplier="",
            channel7_multiplier="",
            channel8_multiplier="",
            channel1_final_uom="GAL",
            channel2_final_uom="",
            channel3_final_uom="",
            channel4_final_uom="",
            channel5_final_uom="",
            channel6_final_uom="",
            channel7_final_uom="",
            channel8_final_uom="",
            first_data_ts="2024-01-01T00:00:00",
            last_data_ts="2025-06-01T00:00:00",
            ami_id="AMI100",
            power_status="ON",
            latitude="34.0522",
            longitude="-118.2437",
            exclude_in_reports="N",
            nb_dials="6",
            backflow="None",
            service_point_type="Residential",
            reclaim_inter_prog="No",
            power_status_details="Normal operation",
            comm_module_id="CM12345",
            register_constant="100",
        )

    def _meter_factory(self, add_dt="2024-01-01T00:00:00") -> MetersenseMeter:
        return MetersenseMeter(
            meter_id="MTR001",
            alt_meter_id="ALT001",
            meter_tp="W-TRB8",
            commodity_tp="W",
            region_id="Region1",
            interval_length="60",
            regread_frequency="24",
            channel1_raw_uom="GAL",
            channel2_raw_uom="",
            channel3_raw_uom="",
            channel4_raw_uom="",
            channel5_raw_uom="",
            channel6_raw_uom="",
            channel7_raw_uom="",
            channel8_raw_uom="",
            channel1_multiplier="1.0",
            channel2_multiplier="",
            channel3_multiplier="",
            channel4_multiplier="",
            channel5_multiplier="",
            channel6_multiplier="",
            channel7_multiplier="",
            channel8_multiplier="",
            channel1_final_uom="GAL",
            channel2_final_uom="",
            channel3_final_uom="",
            channel4_final_uom="",
            channel5_final_uom="",
            channel6_final_uom="",
            channel7_final_uom="",
            channel8_final_uom="",
            first_data_ts="2024-01-01T00:00:00",
            last_data_ts="2025-06-01T00:00:00",
            ami_id="AMI100",
            power_status="ON",
            latitude="34.0522",
            longitude="-118.2437",
            exclude_in_reports="N",
            add_by="admin",
            add_dt="2024-01-01T00:00:00",
            change_by="tech1",
            change_dt="2025-01-01T00:00:00",
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
            status="3",  # Estimated read
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

    def _build_extract_output(
        self,
        account_services=None,
        locations=None,
        meters=None,
        xrefs=None,
        meter_views=None,
        reg_reads=None,
        int_reads=None,
    ):
        def to_jsonl(objs):
            return "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in objs)

        return ExtractOutput(
            {
                "account_services.json": to_jsonl(account_services or []),
                "locations.json": to_jsonl(locations or []),
                "meters.json": to_jsonl(meters or []),
                "meter_location_xref.json": to_jsonl(xrefs or []),
                "meters_view.json": to_jsonl(meter_views or []),
                "registerreads.json": to_jsonl(reg_reads or []),
                "intervalreads.json": to_jsonl(int_reads or []),
            }
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
        self.assertEqual(3, self.adapter.default_extract_interval_days())

    @patch("amiadapters.adapters.metersense.open_ssh_tunnel")
    @patch("amiadapters.adapters.metersense.oracledb.connect")
    def test_extract_returns_expected_files(self, mock_connect, mock_open_tunnel):
        # Mock the SSH tunnel context manager
        mock_ctx = MagicMock()
        mock_ctx.local_bind_port = 1234
        mock_open_tunnel.return_value.__enter__.return_value = mock_ctx

        # Mock DB connection + cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Return empty results from SQL queries
        mock_cursor.fetchall.return_value = []

        # Run
        result = self.adapter._extract(
            run_id="test-run",
            extract_range_start=datetime.datetime(2023, 1, 1),
            extract_range_end=datetime.datetime(2023, 1, 2),
        )

        # Check that files contain JSON
        files = result.get_outputs()
        self.assertTrue("account_services.json" in files)
        self.assertTrue("intervalreads.json" in files)

        # Ensure mocks were called
        mock_open_tunnel.assert_called_once()
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called()

    def test_transform(self):
        meter = self._meter_factory()
        location = self._location_factory()
        account_service = self._account_service_factory()
        xref = self._meter_location_xref_factory(
            meter_id=meter.meter_id, location_no=location.location_no
        )
        meter_view = self._meter_view_factory()
        register_read = self._register_read_factory(meter_id=meter.meter_id)
        interval_read = self._interval_read_factory(meter_id=meter.meter_id)
        extract_outputs = self._build_extract_output(
            account_services=[account_service],
            locations=[location],
            meters=[meter],
            xrefs=[xref],
            meter_views=[meter_view],
            reg_reads=[register_read],
            int_reads=[interval_read],
        )

        transformed_meters, transformed_reads = self.adapter._transform(
            "run-id", extract_outputs
        )
        self.assertEqual(1, len(transformed_meters))
        meter = transformed_meters[0]
        self.assertEqual("MTR001", meter.meter_id)
        self.assertEqual("8", meter.meter_size)
        self.assertEqual("CM12345", meter.endpoint_id)
        self.assertEqual("Main", meter.location_address)
        self.assertEqual("ACC456", meter.account_id)

        self.assertEqual(1, len(transformed_reads))
        read = transformed_reads[0]
        self.assertEqual("ACC456", read.account_id)
        self.assertEqual("1001", read.location_id)
        self.assertEqual(1050.0, read.register_value)
        self.assertEqual(50.0, read.interval_value)
        self.assertEqual(1, read.estimated)

    def test_missing_meter_view(self):
        meter = self._meter_factory()
        location = self._location_factory()
        account_service = self._account_service_factory()
        xref = self._meter_location_xref_factory(
            meter_id=meter.meter_id, location_no=location.location_no
        )
        extract_outputs = self._build_extract_output(
            account_services=[account_service],
            locations=[location],
            meters=[meter],
            xrefs=[xref],
            meter_views=[],
        )
        meters, reads = self.adapter._transform("run-id", extract_outputs)
        self.assertEqual(1, len(meters))
        self.assertIsNone(meters[0].endpoint_id)

    def test_missing_account_for_location(self):
        meter = self._meter_factory()
        location = self._location_factory()
        xref = self._meter_location_xref_factory(
            meter_id=meter.meter_id, location_no=location.location_no
        )
        extract_outputs = self._build_extract_output(
            account_services=[],
            locations=[location],
            meters=[meter],
            xrefs=[xref],
            meter_views=[self._meter_view_factory(meter_id=meter.meter_id)],
        )
        meters, reads = self.adapter._transform("run-id", extract_outputs)
        self.assertEqual(1, len(meters))
        self.assertIsNone(meters[0].account_id)

    def test_missing_location_for_xref(self):
        meter = self._meter_factory()
        account_service = self._account_service_factory()
        xref = self._meter_location_xref_factory(
            meter_id=meter.meter_id, location_no="9999"  # doesn't exist
        )
        extract_outputs = self._build_extract_output(
            account_services=[account_service],
            locations=[],
            meters=[meter],
            xrefs=[xref],
            meter_views=[self._meter_view_factory(meter_id=meter.meter_id)],
        )
        meters, reads = self.adapter._transform("run-id", extract_outputs)
        self.assertEqual(1, len(meters))
        self.assertIsNone(meters[0].location_id)

    def test_no_interval_or_register_reads(self):
        meter = self._meter_factory()
        location = self._location_factory()
        account_service = self._account_service_factory()
        xref = self._meter_location_xref_factory(
            meter_id=meter.meter_id, location_no=location.location_no
        )
        meter_view = self._meter_view_factory(meter_id=meter.meter_id)
        extract_outputs = self._build_extract_output(
            account_services=[account_service],
            locations=[location],
            meters=[meter],
            xrefs=[xref],
            meter_views=[meter_view],
            reg_reads=[],
            int_reads=[],
        )
        meters, reads = self.adapter._transform("run-id", extract_outputs)
        self.assertEqual(1, len(meters))
        self.assertEqual(0, len(reads))

    def test_excludes_interval_and_register_read_when_no_meter(self):
        meter = self._meter_factory()
        location = self._location_factory()
        account_service = self._account_service_factory()
        xref = self._meter_location_xref_factory(
            meter_id=meter.meter_id, location_no=location.location_no
        )
        meter_view = self._meter_view_factory(meter_id=meter.meter_id)
        register_read = self._register_read_factory(
            meter_id="another_id", read_dtm="2024-01-01 12:00:00"
        )
        interval_read = self._interval_read_factory(
            meter_id="another_id", read_dtm="2024-01-01 12:00:00"
        )
        extract_outputs = self._build_extract_output(
            account_services=[account_service],
            locations=[location],
            meters=[meter],
            xrefs=[xref],
            meter_views=[meter_view],
            reg_reads=[register_read],
            int_reads=[interval_read],
        )
        meters, reads = self.adapter._transform("run-id", extract_outputs)
        self.assertEqual(1, len(meters))
        self.assertEqual(0, len(reads))

    def test_interval_and_register_reads_when_they_do_not_join(self):
        meter = self._meter_factory()
        location = self._location_factory()
        account_service = self._account_service_factory()
        xref = self._meter_location_xref_factory(
            meter_id=meter.meter_id, location_no=location.location_no
        )
        meter_view = self._meter_view_factory(meter_id=meter.meter_id)
        interval_read = self._interval_read_factory(
            meter_id=meter.meter_id, read_dtm="2024-01-01 01:00:00"
        )
        register_read = self._register_read_factory(
            meter_id=meter.meter_id, read_dtm="2024-01-01 12:00:00"
        )
        extract_outputs = self._build_extract_output(
            account_services=[account_service],
            locations=[location],
            meters=[meter],
            xrefs=[xref],
            meter_views=[meter_view],
            reg_reads=[register_read],
            int_reads=[interval_read],
        )
        meters, reads = self.adapter._transform("run-id", extract_outputs)
        self.assertEqual(1, len(meters))
        self.assertEqual(2, len(reads))
        self.assertEqual(
            reads[0].interval_value, interval_read.read_value * 100
        )  # CCF to CF
        self.assertEqual(
            reads[1].register_value, register_read.read_value * 100
        )  # CCF to CF

    def test_interval_and_register_reads_match_to_correct_meter(self):
        meter = self._meter_factory()
        location = self._location_factory()
        account_service = self._account_service_factory(
            active_dt="2024-01-01T00:00:00", inactive_dt="2025-01-01T00:00:00"
        )
        xref = self._meter_location_xref_factory(
            meter_id=meter.meter_id,
            location_no=location.location_no,
            active_dt="2024-01-01T00:00:00",
            inactive_dt="2025-01-01T00:00:00",
        )
        meter_view = self._meter_view_factory(meter_id=meter.meter_id)
        # Inside active date range
        interval_read_inside_date_range = self._interval_read_factory(
            meter_id=meter.meter_id, read_dtm="2024-06-01 01:00:00"
        )
        # Outside active date range
        interval_read_outside_date_range = self._interval_read_factory(
            meter_id=meter.meter_id, read_dtm="2025-06-01 01:00:00"
        )
        extract_outputs = self._build_extract_output(
            account_services=[account_service],
            locations=[location],
            meters=[meter],
            xrefs=[xref],
            meter_views=[meter_view],
            reg_reads=[],
            int_reads=[
                interval_read_inside_date_range,
                interval_read_outside_date_range,
            ],
        )
        meters, reads = self.adapter._transform("run-id", extract_outputs)
        self.assertEqual(1, len(meters))
        self.assertEqual(2, len(reads))
        self.assertEqual(
            reads[0].interval_value,
            interval_read_inside_date_range.read_value * 100,  # CCF to CF
        )
        self.assertIsNotNone(reads[0].account_id)
        self.assertIsNotNone(reads[0].location_id)
        self.assertEqual(
            reads[1].interval_value,
            interval_read_outside_date_range.read_value * 100,  # CCF to CF
        )
        self.assertIsNone(reads[1].account_id)
        self.assertIsNone(reads[1].location_id)

    def make_row_from_dataclass(self, instance):
        return tuple(getattr(instance, f) for f in instance.__dataclass_fields__)

    def test_query_tables_with_interval_and_register_reads(self):
        start = datetime.datetime(2024, 1, 1)
        end = datetime.datetime(2024, 1, 2)

        interval_read = MetersenseIntervalRead(
            meter_id="91028496",
            channel_id="1",
            read_dt="2024-01-01",
            read_hr="12",
            read_30min_int="",
            read_15min_int="",
            read_5min_int="",
            read_dtm="2024-01-01T12:00:00",
            read_value="100.0",
            uom="G",
            status="V",
            read_version="1",
        )
        register_read = MetersenseRegisterRead(
            meter_id="91028496",
            channel_id="1",
            read_dtm="2024-01-01T12:00:00",
            read_value="200.0",
            uom="G",
            status="V",
            read_version="1",
        )

        cursor = MagicMock()

        def execute_side_effect(query, params=None):
            if "FROM INTERVALREADS" in query:
                cursor.fetchall.return_value = [
                    self.make_row_from_dataclass(interval_read)
                ]
            elif "FROM REGISTERREADS" in query:
                cursor.fetchall.return_value = [
                    self.make_row_from_dataclass(register_read)
                ]
            else:
                cursor.fetchall.return_value = []
            return None

        cursor.execute.side_effect = execute_side_effect

        result = self.adapter._query_tables(cursor, start, end)

        self.assertIn("intervalreads.json", result)
        self.assertIn("registerreads.json", result)

        interval_json = json.loads(result["intervalreads.json"].strip())
        register_json = json.loads(result["registerreads.json"].strip())

        self.assertEqual(interval_json["meter_id"], "91028496")
        self.assertEqual(register_json["read_value"], "200.0")
        self.assertEqual(register_json["read_dtm"], "2024-01-01T12:00:00")

        self.assertIn("READ_DTM", cursor.execute.call_args[0][0])
        self.assertEqual(start, cursor.execute.call_args[0][1]["extract_range_start"])
        self.assertEqual(end, cursor.execute.call_args[0][1]["extract_range_end"])

    def test_query_tables_empty_tables(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []

        result = self.adapter._query_tables(cursor, None, None)

        # All table keys should exist even if they are empty
        expected_keys = [
            "account_services.json",
            "intervalreads.json",
            "locations.json",
            "meters.json",
            "meters_view.json",
            "meter_location_xref.json",
            "registerreads.json",
        ]
        for key in expected_keys:
            self.assertEqual(result[key], "")


class TestMetersenseRawSnowflakeLoader(BaseTestCase):
    def test_load_calls_snowflake_cursor_expected_times(self):
        loader = MetersenseRawSnowflakeLoader()

        fake_extract_output_files = {
            "account_services.json": '{"account_id": "1", "location_no": "2", "commodity_tp": "W", "last_read_dt": "2024-01-01T00:00:00", "service_id": "svc", "active_dt": "2023-01-01T00:00:00", "inactive_dt": "2025-01-01T00:00:00"}\n',
            "locations.json": '{"location_no": "2", "alt_location_id": "alt", "location_class": "", "unit_no": "", "street_no": "", "street_pfx": "", "street_name": "", "street_sfx": "", "street_sfx_dir": "", "city": "", "state": "", "postal_cd": "", "billing_cycle": "", "add_by": "", "add_dt": "", "change_by": "", "change_dt": "", "latitude": "", "longitude": ""}\n',
            "meters.json": '{"meter_id": "91028496", "alt_meter_id": "", "meter_tp": "", "commodity_tp": "W", "region_id": "", "interval_length": "", "regread_frequency": "", "channel1_raw_uom": "", "channel2_raw_uom": "", "channel3_raw_uom": "", "channel4_raw_uom": "", "channel5_raw_uom": "", "channel6_raw_uom": "", "channel7_raw_uom": "", "channel8_raw_uom": "", "channel1_multiplier": "", "channel2_multiplier": "", "channel3_multiplier": "", "channel4_multiplier": "", "channel5_multiplier": "", "channel6_multiplier": "", "channel7_multiplier": "", "channel8_multiplier": "", "channel1_final_uom": "", "channel2_final_uom": "", "channel3_final_uom": "", "channel4_final_uom": "", "channel5_final_uom": "", "channel6_final_uom": "", "channel7_final_uom": "", "channel8_final_uom": "", "first_data_ts": "", "last_data_ts": "", "ami_id": "", "power_status": "", "latitude": "", "longitude": "", "exclude_in_reports": "", "add_by": "", "add_dt": "", "change_by": "", "change_dt": ""}\n',
            "meters_view.json": '{"meter_id": "91028496", "alt_meter_id": "", "meter_tp": "", "commodity_tp": "", "region_id": "", "interval_length": "", "regread_frequency": "", "channel1_raw_uom": "", "channel2_raw_uom": "", "channel3_raw_uom": "", "channel4_raw_uom": "", "channel5_raw_uom": "", "channel6_raw_uom": "", "channel7_raw_uom": "", "channel8_raw_uom": "", "channel1_multiplier": "", "channel2_multiplier": "", "channel3_multiplier": "", "channel4_multiplier": "", "channel5_multiplier": "", "channel6_multiplier": "", "channel7_multiplier": "", "channel8_multiplier": "", "channel1_final_uom": "", "channel2_final_uom": "", "channel3_final_uom": "", "channel4_final_uom": "", "channel5_final_uom": "", "channel6_final_uom": "", "channel7_final_uom": "", "channel8_final_uom": "", "first_data_ts": "", "last_data_ts": "", "ami_id": "", "power_status": "", "latitude": "", "longitude": "", "exclude_in_reports": "", "nb_dials": "", "backflow": "", "service_point_type": "", "reclaim_inter_prog": "", "power_status_details": "", "comm_module_id": "", "register_constant": ""}\n',
            "meter_location_xref.json": '{"meter_id": "91028496", "active_dt": "2023-01-01T00:00:00", "location_no": "2", "inactive_dt": "2025-01-01T00:00:00", "add_by": "", "add_dt": "", "change_by": "", "change_dt": ""}\n',
            "intervalreads.json": '{"meter_id": "91028496", "channel_id": "1", "read_dt": "", "read_hr": "", "read_30min_int": "", "read_15min_int": "", "read_5min_int": "", "read_dtm": "2024-01-01T00:00:00", "read_value": "123.4", "uom": "CCF", "status": "", "read_version": ""}\n',
            "registerreads.json": '{"meter_id": "91028496", "channel_id": "1", "read_dtm": "2024-01-01T00:00:00", "read_value": "123.4", "uom": "CCF", "status": "", "read_version": ""}\n',
        }

        mock_cursor = MagicMock()
        mock_snowflake_conn = MagicMock()
        mock_snowflake_conn.cursor.return_value = mock_cursor

        loader.load(
            "test_run",
            "test_org",
            pytz.timezone("America/Los_Angeles"),
            ExtractOutput(fake_extract_output_files),
            mock_snowflake_conn,
        )

        # Each of the 7 load methods calls cursor() three times
        # So we expect at least 7 * 3 = 21 calls to snowflake_conn.cursor()
        self.assertEqual(mock_snowflake_conn.cursor.call_count, 21)

import datetime
import json
from unittest.mock import MagicMock

import pytz

from amiadapters.config import ConfiguredLocalTaskOutputController
from amiadapters.adapters.xylem_moulton_niguel import (
    XylemMoultonNiguelAdapter,
    Ami,
    Meter,
    ServicePoint,
)
from amiadapters.models import GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from test.base_test_case import BaseTestCase


class TestMetersenseAdapter(BaseTestCase):

    def setUp(self):
        self.tz = pytz.timezone("Europe/Rome")
        self.adapter = XylemMoultonNiguelAdapter(
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

    def test_init(self):
        self.assertEqual("tunnel-ip", self.adapter.ssh_tunnel_server_host)
        self.assertEqual("ubuntu", self.adapter.ssh_tunnel_username)
        self.assertEqual("/key", self.adapter.ssh_tunnel_key_path)
        self.assertEqual("db-host", self.adapter.database_host)
        self.assertEqual(1521, self.adapter.database_port)
        self.assertEqual("db-name", self.adapter.database_db_name)
        self.assertEqual("dbu", self.adapter.database_user)
        self.assertEqual("dbp", self.adapter.database_password)

    def _mock_extract_output(self, meters, service_points, reads):
        files = {
            "meter.json": "\n".join(json.dumps(m.__dict__) for m in meters),
            "service_point.json": "\n".join(
                json.dumps(sp.__dict__) for sp in service_points
            ),
            "ami.json": "\n".join(json.dumps(r.__dict__) for r in reads),
        }
        return ExtractOutput(files)

    def _meter_factory(self) -> Meter:
        return Meter(
            **{
                "id": "1",
                "account_rate_code": "R1",
                "service_address": "100",
                "meter_status": "Active",
                "ert_id": "ERT1",
                "meter_id": "M1",
                "meter_id_2": "M1",
                "meter_manufacturer": "S",
                "number_of_dials": "4",
                "spd_meter_mult": "1",
                "spd_meter_size": "1",
                "spd_usage_uom": "CF",
                "service_point": "1",
                "asset_number": "A1",
                "start_date": "2020-01-01",
                "end_date": "2021-01-01",
                "is_current": "TRUE",
                "batch_id": "B1",
            }
        )

    def _service_point_factory(self) -> ServicePoint:
        return ServicePoint(
            **{
                "service_address": "100",
                "service_point": "1",
                "account_billing_cycle": "C1",
                "read_cycle": "RC",
                "asset_address": "Addr",
                "asset_city": "City",
                "asset_zip": "Zip",
                "sdp_id": "S1",
                "sdp_lat": "1",
                "sdp_lon": "-1",
                "service_route": "Route",
                "start_date": "2020-01-01",
                "end_date": "2021-01-01",
                "is_current": "TRUE",
                "batch_id": "B1",
            }
        )

    def _ami_read_factory(self, flowtime="2023-01-01 00:00:00.000 -0700") -> Ami:
        return Ami(
            **{
                "id": "1",
                "encid": "1",
                "datetime": flowtime,
                "code": "R1",
                "consumption": "10",
                "service_address": "100",
                "service_point": "1",
                "batch_id": "B1",
                "meter_serial_id": "M1",
                "ert_id": "ERT1",
            }
        )

    def test_transform(self):
        meter = self._meter_factory()
        sp = self._service_point_factory()
        read_1 = self._ami_read_factory(flowtime="2023-01-01 00:00:00.000 -0700")
        read_2 = self._ami_read_factory(flowtime="2023-01-01 00:01:00.000 -0700")
        extract_outputs = self._mock_extract_output([meter], [sp], [read_1, read_2])
        meters, reads = self.adapter._transform("run1", extract_outputs)
        self.assertEqual(len(meters), 1)
        self.assertEqual(
            GeneralMeter(
                org_id="this-org",
                device_id="M1",
                account_id=None,
                location_id="100",
                meter_id="M1",
                endpoint_id="ERT1",
                meter_install_date=datetime.datetime(
                    2020, 1, 1, 0, 0, tzinfo=pytz.timezone("Europe/Rome")
                ),
                meter_size="1",
                meter_manufacturer="S",
                multiplier="1",
                location_address="Addr",
                location_city="City",
                location_state=None,
                location_zip="Zip",
            ),
            meters[0],
        )

        self.assertEqual(len(reads), 2)
        self.assertEqual(
            GeneralMeterRead(
                org_id="this-org",
                device_id="M1",
                account_id=None,
                location_id="100",
                flowtime=datetime.datetime(
                    2023,
                    1,
                    1,
                    0,
                    0,
                    tzinfo=datetime.timezone(
                        datetime.timedelta(days=-1, seconds=61200)
                    ),
                ),
                register_value=None,
                register_unit=None,
                interval_value=10.0,
                interval_unit="CF",
            ),
            reads[0],
        )

    def test_meter_with_no_reads_included(self):
        meter = self._meter_factory()
        sp = self._service_point_factory()

        extract_outputs = self._mock_extract_output([meter], [sp], [])
        meters, reads = self.adapter._transform("run1", extract_outputs)

        self.assertEqual(len(meters), 1)
        self.assertEqual(len(reads), 0)

    def test_read_with_no_meter_excluded(self):
        sp = self._service_point_factory()
        read = self._ami_read_factory()
        extract_outputs = self._mock_extract_output([], [sp], [read])
        meters, reads = self.adapter._transform("run1", extract_outputs)
        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 0)

    def test_two_service_points_same_address(self):
        meter = self._meter_factory()
        sp1 = self._service_point_factory()
        # Same service_address value, different service_point
        sp2 = ServicePoint(
            **{
                **sp1.__dict__,
                "service_point": "2",
                "asset_address": "Addr2",
                "asset_city": "City2",
            }
        )

        extract_outputs = self._mock_extract_output([meter], [sp1, sp2], [])
        meters, _ = self.adapter._transform("run1", extract_outputs)
        self.assertEqual(len(meters), 1)
        self.assertEqual(meters[0].location_address, sp1.asset_address)
        self.assertEqual(meters[0].location_city, sp1.asset_city)

    def test_no_service_point_found(self):
        meter = self._meter_factory()

        extract_outputs = self._mock_extract_output([meter], [], [])
        meters, _ = self.adapter._transform("run1", extract_outputs)
        self.assertEqual(len(meters), 1)
        self.assertIsNone(meters[0].location_address)
        self.assertIsNone(meters[0].location_city)

    def test_query_tables_with_interval_reads(self):
        start = datetime.datetime(2024, 1, 1)
        end = datetime.datetime(2024, 1, 2)

        interval_read = self._ami_read_factory()

        cursor = MagicMock()

        def execute_side_effect(query, params=None):
            if "FROM ami" in query:
                cursor.fetchall.return_value = [
                    tuple(
                        getattr(interval_read, f)
                        for f in interval_read.__dataclass_fields__
                    )
                ]
            else:
                cursor.fetchall.return_value = []
            return None

        cursor.execute.side_effect = execute_side_effect

        result = self.adapter._query_tables(cursor, start, end)

        self.assertIn("meter.json", result)
        self.assertIn("service_point.json", result)
        self.assertIn("ami.json", result)

        interval_json = json.loads(result["ami.json"].strip())

        self.assertEqual(interval_json["meter_serial_id"], "M1")

        self.assertIn("datetime", cursor.execute.call_args[0][0])
        self.assertEqual(start, cursor.execute.call_args[0][1]["extract_range_start"])
        self.assertEqual(end, cursor.execute.call_args[0][1]["extract_range_end"])

    def test_query_tables_empty_tables(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []

        result = self.adapter._query_tables(cursor, None, None)

        # All table keys should exist even if they are empty
        expected_keys = [
            "meter.json",
            "service_point.json",
            "ami.json",
        ]
        for key in expected_keys:
            self.assertEqual(result[key], "")

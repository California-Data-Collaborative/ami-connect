import datetime
import json
from unittest.mock import Mock

import pytz

from amiadapters.config import ConfiguredLocalTaskOutputController
from amiadapters.adapters.metersense import (
    MetersenseAdapter,
    MetersenseIntervalRead,
    MetersenseMeter,
    MetersenseAccountService,
    MetersenseLocation,
    MetersenseMeterLocationXref,
    MetersenseMetersView,
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

    def _account_service_factory(self) -> MetersenseAccountService:
        return MetersenseAccountService(
            service_id="SVC123",
            account_id="ACC456",
            location_no="1001",
            commodity_tp="W",
            last_read_dt="2024-12-01T10:00:00",
            active_dt="2024-01-01T00:00:00",
            inactive_dt="2025-01-01T00:00:00",
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
        self, meter_id="MTR001", location_no="1001"
    ) -> MetersenseMeterLocationXref:
        return MetersenseMeterLocationXref(
            meter_id=meter_id,
            active_dt="2024-01-01T00:00:00",
            location_no=location_no,
            inactive_dt="2025-01-01T00:00:00",
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

    def _meter_factory(self) -> MetersenseMeter:
        return MetersenseMeter(
            meter_id="MTR001",
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

    def test_transform(self):
        account_services = [self._account_service_factory()]
        locations = [
            self._location_factory(location_no=account_services[0].location_no)
        ]
        meters = [self._meter_factory()]
        xrefs = [
            self._meter_location_xref_factory(
                meter_id=meters[0].meter_id, location_no=account_services[0].location_no
            )
        ]
        meter_views = [self._meter_view_factory(meter_id=meters[0].meter_id)]
        reg_reads = [self._register_read_factory(meter_id=meters[0].meter_id)]
        int_reads = [self._interval_read_factory(meter_id=meters[0].meter_id)]
        extract_outputs = ExtractOutput(
            {
                "account_services.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in account_services
                ),
                "locations.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in locations
                ),
                "meters.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in meters
                ),
                "meter_location_xref.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in xrefs
                ),
                "meters_view.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in meter_views
                ),
                "registerreads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in reg_reads
                ),
                "intervalreads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in int_reads
                ),
            }
        )

        transformed_meters, transformed_reads = self.adapter._transform(
            "run-id", extract_outputs
        )
        self.assertEqual(1, len(transformed_meters))
        self.assertEqual(1, len(transformed_reads))

import json
from amiadapters.outputs.base import ExtractOutput

from amiadapters.models import DataclassJSONEncoder
from amiadapters.adapters.xylem_sensus import (
    XylemSensusAdapter,
    XylemSensusMeterAndReads,
    XylemSensusRead,
)
from test.base_test_case import BaseTestCase
from amiadapters.configuration.models import SftpConfiguration
from amiadapters.models import GeneralMeter, GeneralMeterRead


class TestXylemSensusAdapter(BaseTestCase):
    def setUp(self):
        self.sample_meter = {
            "record_type": "MEPMD01",
            "record_version": "1",
            "sender_id": "SENDER",
            "sender_customer_id": "SCUST",
            "receiver_id": "RECV",
            "receiver_customer_id": "RCUST",
            "time_stamp": "202406010000",
            "meter_id": "MTR123",
            "purpose": "OK",
            "commodity": "W",
            "units": "CF",
            "calculation_constant": "1.0",
            "interval": "60",
            "quantity": "2",
            "reads": [
                {"time": "202406010100", "code": "A", "quantity": "10.5"},
                {"time": "202406010200", "code": "E", "quantity": "11.0"},
            ],
        }
        self.extract_output = ExtractOutput(
            {
                "meters_and_reads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in [self.sample_meter]
                ),
            }
        )
        self.adapter = XylemSensusAdapter(
            org_id="ORG1",
            org_timezone="UTC",
            pipeline_configuration=None,
            configured_sftp=SftpConfiguration(
                host="host",
                remote_data_directory="./dir",
                local_download_directory="./dir",
                local_known_hosts_file=None,
            ),
            sftp_user="user",
            sftp_password="pass",
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_sinks=[],
        )

    def test_from_json_file_parses_meter_and_reads(self):
        meters = XylemSensusMeterAndReads.from_json_file(
            self.extract_output, "meters_and_reads.json"
        )
        self.assertEqual(len(meters), 1)
        meter = meters[0]
        self.assertEqual(meter.meter_id, "MTR123")
        self.assertEqual(meter.commodity, "W")
        self.assertEqual(meter.purpose, "OK")
        self.assertEqual(len(meter.reads), 2)
        self.assertIsInstance(meter.reads[0], XylemSensusRead)
        self.assertEqual(meter.reads[1].code, "E")
        self.assertEqual(meter.reads[1].quantity, "11.0")

    def test_transform_returns_general_meter_and_reads(self):
        meters, reads = self.adapter._transform("run1", self.extract_output)
        self.assertEqual(len(meters), 1)
        self.assertIsInstance(meters[0], GeneralMeter)
        self.assertEqual(meters[0].device_id, "MTR123")
        self.assertEqual(meters[0].account_id, "RCUST")
        self.assertEqual(meters[0].multiplier, "1.0")
        self.assertEqual(meters[0].meter_id, "MTR123")

        self.assertEqual(len(reads), 2)
        self.assertIsInstance(reads[0], GeneralMeterRead)
        self.assertEqual(reads[0].device_id, "MTR123")
        self.assertEqual(reads[0].account_id, "RCUST")
        self.assertEqual(reads[0].interval_value, 10.5)
        self.assertEqual(reads[1].estimated, 1)  # code "E" should set estimated=1

    def test_transform_handles_multiple_meters_with_same_id(self):
        meters = [self.sample_meter, self.sample_meter]
        extract_output = ExtractOutput(
            {
                "meters_and_reads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in meters
                ),
            }
        )
        meters, reads = self.adapter._transform("run1", extract_output)
        self.assertEqual(len(meters), 1)
        self.assertEqual(len(reads), 2)

    def test_transform_skips_non_water_meter(self):
        non_water_meter = self.sample_meter.copy()
        non_water_meter["commodity"] = "E"  # Not water
        extract_output = ExtractOutput(
            {
                "meters_and_reads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in [non_water_meter]
                ),
            }
        )
        meters, reads = self.adapter._transform("run1", extract_output)
        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 0)

    def test_transform_skips_non_OK_purpose(self):
        non_ok_meter = self.sample_meter.copy()
        non_ok_meter["purpose"] = "SUMMARY"
        extract_output = ExtractOutput(
            {
                "meters_and_reads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in [non_ok_meter]
                ),
            }
        )
        meters, reads = self.adapter._transform("run1", extract_output)
        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 0)

    def test_transform_skips_meter_with_null_device_id(self):
        null_device_meter = self.sample_meter.copy()
        null_device_meter["meter_id"] = None
        extract_output = ExtractOutput(
            {
                "meters_and_reads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in [null_device_meter]
                ),
            }
        )
        meters, reads = self.adapter._transform("run1", extract_output)
        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 0)

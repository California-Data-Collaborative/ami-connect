import datetime
import os
import shutil
import tempfile

import pytz

from amiadapters.models import GeneralMeter, GeneralMeterRead
from amiadapters.outputs.local import LocalTaskOutputController
from amiadapters.outputs.base import ExtractOutput
from test.base_test_case import BaseTestCase


class TestLocalTaskOutputController(BaseTestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.controller = LocalTaskOutputController(
            output_folder=self.test_dir, org_id="org456"
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_write_and_read_extract_outputs(self):
        data = {"file1.txt": "hello world", "file2.txt": "more data"}
        extract_output = ExtractOutput(data)
        self.controller.write_extract_outputs("run123", extract_output)

        for filename, content in data.items():
            path = os.path.join(self.test_dir, "run123/org456/e", filename)
            self.assertTrue(os.path.exists(path))
            with open(path, "r") as f:
                self.assertEqual(f.read(), content)

        result = self.controller.read_extract_outputs("run123")
        self.assertEqual(data, result.get_outputs())
        self.assertEqual("hello world", result.from_file("file1.txt"))

    def test_write_and_read_transformed_meters(self):
        meters = [
            GeneralMeter(
                org_id="org456",
                device_id="1",
                account_id="303022",
                location_id="303022",
                meter_id="1",
                endpoint_id="130615549",
                meter_install_date=datetime.datetime(
                    2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                meter_size="0.625",
                meter_manufacturer="Sensus",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            ),
            GeneralMeter(
                org_id="org456",
                device_id="2",
                account_id="303022",
                location_id="303022",
                meter_id="2",
                endpoint_id="130615549",
                meter_install_date=datetime.datetime(
                    2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                meter_size="0.625",
                meter_manufacturer="Sensus",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            ),
        ]
        self.controller.write_transformed_meters("run123", meters)

        path = os.path.join(self.test_dir, "run123/org456/t/meters.json")
        self.assertTrue(os.path.exists(path))

        with open(path, "r") as f:
            lines = f.read().strip().split("\n")
            self.assertEqual(len(lines), 2)

        meters_out = self.controller.read_transformed_meters("run123")
        self.assertEqual(len(meters_out), 2)
        self.assertEqual(meters_out[0].device_id, "1")
        self.assertEqual(meters_out[1].device_id, "2")

    def test_write_and_read_transformed_meter_reads(self):
        reads = [
            GeneralMeterRead(
                org_id="org456",
                device_id="1",
                account_id="303022",
                location_id="303022",
                flowtime=datetime.datetime(
                    2024, 8, 1, 0, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                register_value=123.4,
                register_unit="CCF",
                interval_value=None,
                interval_unit=None,
                battery=None,
                install_date=None,
                connection=None,
                estimated=None,
            ),
            GeneralMeterRead(
                org_id="org456",
                device_id="2",
                account_id="303022",
                location_id="303022",
                flowtime=datetime.datetime(
                    2024, 8, 1, 1, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                register_value=227.6,
                register_unit="CCF",
                interval_value=None,
                interval_unit=None,
                battery=None,
                install_date=None,
                connection=None,
                estimated=None,
            ),
        ]
        self.controller.write_transformed_meter_reads("run123", reads)

        path = os.path.join(self.test_dir, "run123/org456/t/reads.json")
        self.assertTrue(os.path.exists(path))

        reads_out = self.controller.read_transformed_meter_reads("run123")
        self.assertEqual(len(reads_out), 2)
        self.assertEqual(reads_out[0].device_id, "1")
        self.assertEqual(reads_out[0].register_value, 123.4)
        self.assertEqual(reads_out[1].device_id, "2")

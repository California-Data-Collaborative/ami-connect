import datetime
import json
from unittest.mock import MagicMock, call

import pytz

from amiadapters.models import GeneralModelJSONEncoder
from amiadapters.models import GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.outputs.s3 import S3TaskOutputController
from test.base_test_case import BaseTestCase


class TestS3TaskOutputController(BaseTestCase):

    def setUp(self):
        self.bucket = "test-bucket"
        self.org_id = "org-abc"
        self.prefix = "my-prefix"
        self.controller = S3TaskOutputController(
            bucket_name=self.bucket, org_id=self.org_id, s3_prefix=self.prefix
        )

        self.mock_s3 = MagicMock()
        self.controller.s3 = self.mock_s3  # Inject mock S3 client

    def test_write_extract_outputs(self):
        outputs = ExtractOutput({"file1.txt": "data1", "file2.txt": "data2"})
        self.controller.write_extract_outputs("run-001", outputs)

        calls = self.mock_s3.put_object.call_args_list
        expected = [
            call(
                Bucket="test-bucket",
                Key="my-prefix/run-001/org-abc/e/file1.txt",
                Body=b"data1",
            ),
            call(
                Bucket="test-bucket",
                Key="my-prefix/run-001/org-abc/e/file2.txt",
                Body=b"data2",
            ),
        ]
        self.assertEqual(expected, calls)

    def test_read_extract_outputs(self):
        self.mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"{self.prefix}/run-001/{self.org_id}/e/file1.txt"},
                {"Key": f"{self.prefix}/run-001/{self.org_id}/e/file2.txt"},
            ]
        }
        self.mock_s3.get_object.side_effect = lambda Bucket, Key: {
            "Body": MagicMock(read=lambda: b"test-content-" + Key.encode())
        }

        result = self.controller.read_extract_outputs("run-001")
        self.assertIsInstance(result, ExtractOutput)
        self.assertEqual(len(result.get_outputs()), 2)
        self.assertTrue("file1.txt" in result.get_outputs())
        self.assertTrue(result.get_outputs()["file1.txt"].startswith("test-content-"))

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
        self.controller.write_transformed_meters("runid", meters)

        # Simulate download
        data = "\n".join(json.dumps(m, cls=GeneralModelJSONEncoder) for m in meters)
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: data.encode())
        }

        result = self.controller.read_transformed_meters("runid")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].device_id, "1")

    def test_write_and_read_transformed_meter_reads(self):
        reads = reads = [
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
            ),
        ]
        self.controller.write_transformed_meter_reads("runid", reads)

        # Simulate download
        data = "\n".join(json.dumps(r, cls=GeneralModelJSONEncoder) for r in reads)
        self.mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: data.encode())
        }

        result = self.controller.read_transformed_meter_reads("runid")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1].device_id, "2")
        self.assertEqual(result[1].register_value, 227.6)

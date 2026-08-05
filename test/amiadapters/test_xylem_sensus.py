import csv
import io
import json
from datetime import datetime

import pytz

from amiadapters.outputs.base import ExtractOutput
from amiadapters.models import DataclassJSONEncoder
from amiadapters.adapters.xylem_sensus import (
    CmepMeterAndReads,
    CmepRead,
    XylemSensusAdapter,
    XylemSensusBaseTableLoader,
    files_for_date_range,
    parse_cmep_row,
)
from test.base_test_case import BaseTestCase

# Real rows from the STPUD drop file STAHO_IntervalReport_202606030815.txt.
# The CF row carries meter B74741489's 24 hourly interval values for
# 2026-06-02 08:00 through 2026-06-03 07:00; the CFREG row carries the same
# meter's cumulative register reads at the two daily 07:00 boundaries. The
# interval values sum to exactly the register delta (479 CF).
REAL_CF_ROW = "MEPMD01,20080501,SENSUS,STAHO:133000,13169370,B74741489,202606031515,B74741489,OK,W,CF,1.0,00000100,24,202606020800,R0,8,202606020900,R0,9,202606021000,R0,6,202606021100,R0,8,202606021200,R0,7,202606021300,R0,10,202606021400,R0,27,202606021500,R0,35,202606021600,R0,27,202606021700,R0,23,202606021800,R0,25,202606021900,R0,20,202606022000,R0,22,202606022100,R0,12,202606022200,R0,8,202606022300,R0,20,202606030000,R0,16,202606030100,R0,28,202606030200,R0,23,202606030300,R0,31,202606030400,R0,53,202606030500,R0,28,202606030600,R0,20,202606030700,R0,13"
REAL_CFREG_ROW = "MEPMD01,20080501,SENSUS,STAHO:133000,13169370,B74741489,202606031515,B74741489,OK,W,CFREG,1.0,00000100,2,202606020700,R0,2931567,202606030700,R0,2932046"
# A small share of feed meter ids arrive without the B prefix (4 of 15,284
# observed); modeled on real meter 68567673.
BARE_ID_CF_ROW = "MEPMD01,20080501,SENSUS,STAHO:133000,13248001,68567673,202606031515,68567673,OK,W,CF,1.0,00000100,2,202606030600,R0,3,202606030700,R0,4"

# The kraken-side export writes each billing meter under both its B-prefixed
# and bare key so the adapter lookup needs no id logic.
CROSSWALK_CSV = (
    "meter_id,account_id,location_id\n"
    "B74741489,3210987-001,3210987-1\n"
    "74741489,3210987-001,3210987-1\n"
    "B68567673,3231422-001,3231422-1\n"
    "68567673,3231422-001,3231422-1\n"
)


class FakeS3Client:
    """Stands in for the boto3 S3 client; returns a fixed object body."""

    def __init__(self, body: str):
        self.body = body
        self.requests = []

    def get_object(self, Bucket=None, Key=None):
        self.requests.append((Bucket, Key))
        return {"Body": io.BytesIO(self.body.encode("utf-8"))}


def cmep_rows_to_extract_output(rows: list) -> ExtractOutput:
    """Build an ExtractOutput the way _extract does: parse each CMEP line."""
    parsed = [parse_cmep_row(next(csv.reader([row]))) for row in rows]
    return ExtractOutput(
        {
            "meters_and_reads.json": "\n".join(
                json.dumps(i, cls=DataclassJSONEncoder) for i in parsed
            )
        }
    )


class TestXylemSensusAdapter(BaseTestCase):

    def _make_adapter(self, s3_client=None, with_crosswalk=True):
        return XylemSensusAdapter(
            org_id="cadc_south_tahoe",
            org_timezone="America/Los_Angeles",
            pipeline_configuration=None,
            sftp_host="host",
            sftp_remote_data_directory="./dir",
            sftp_local_download_directory="./dir",
            sftp_known_hosts_str=None,
            sftp_user="user",
            sftp_password="pass",
            crosswalk_s3_region="us-west-2" if with_crosswalk else None,
            crosswalk_s3_bucket="a-bucket" if with_crosswalk else None,
            crosswalk_s3_key="a/key.csv" if with_crosswalk else None,
            crosswalk_aws_access_key_id="AKIA" if with_crosswalk else None,
            crosswalk_aws_secret_access_key="secret" if with_crosswalk else None,
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
            s3_client=s3_client,
        )

    ###########################################################################
    # CMEP parsing
    ###########################################################################

    def test_parse_cmep_row_parses_interval_row(self):
        row = next(csv.reader([REAL_CF_ROW]))
        parsed = parse_cmep_row(row)
        self.assertEqual("MEPMD01", parsed.record_type)
        self.assertEqual("B74741489", parsed.meter_id)
        self.assertEqual("13169370", parsed.receiver_id)
        self.assertEqual("CF", parsed.units)
        self.assertEqual(24, len(parsed.reads))
        self.assertEqual(
            CmepRead(time="202606020800", code="R0", quantity="8"), parsed.reads[0]
        )

    def test_parse_cmep_row_parses_register_row(self):
        row = next(csv.reader([REAL_CFREG_ROW]))
        parsed = parse_cmep_row(row)
        self.assertEqual("CFREG", parsed.units)
        self.assertEqual(2, len(parsed.reads))
        self.assertEqual("2932046", parsed.reads[1].quantity)

    def test_parse_cmep_row_raises_on_unknown_record_type(self):
        row = next(csv.reader([REAL_CF_ROW.replace("MEPMD01", "MLA01", 1)]))
        with self.assertRaises(Exception):
            parse_cmep_row(row)

    def test_parse_cmep_row_raises_on_truncated_row(self):
        row = next(csv.reader([REAL_CFREG_ROW]))[:-1]
        with self.assertRaises(Exception):
            parse_cmep_row(row)

    def test_from_json_file_parses_meter_and_reads(self):
        output = cmep_rows_to_extract_output([REAL_CFREG_ROW])
        meters = CmepMeterAndReads.from_json_file(output, "meters_and_reads.json")
        self.assertEqual(1, len(meters))
        self.assertEqual("B74741489", meters[0].meter_id)
        self.assertEqual(2, len(meters[0].reads))
        self.assertIsInstance(meters[0].reads[0], CmepRead)

    ###########################################################################
    # File selection
    ###########################################################################

    def test_files_for_date_range_selects_files_covering_range(self):
        files = [
            "STAHO_IntervalReport_202606020815.txt",
            "STAHO_IntervalReport_202606030815.txt",
            "STAHO_IntervalReport_202606040815.txt",
            "STAHO_IntervalReport_202606050815.txt",
            "not-an-interval-report.txt",
        ]
        # Reads for Jun 3 arrive in the file stamped Jun 4, so a Jun 3 - Jun 3
        # range selects the Jun 3 and Jun 4 files.
        selected = files_for_date_range(
            files, datetime(2026, 6, 3), datetime(2026, 6, 3)
        )
        self.assertEqual(
            [
                "STAHO_IntervalReport_202606030815.txt",
                "STAHO_IntervalReport_202606040815.txt",
            ],
            selected,
        )

    def test_files_for_date_range_skips_unrecognized_names(self):
        selected = files_for_date_range(
            ["vflex_export.csv", "STAHO_AlarmReport_202606030815.txt"],
            datetime(2026, 6, 1),
            datetime(2026, 6, 30),
        )
        self.assertEqual([], selected)

    ###########################################################################
    # Transform
    ###########################################################################

    def test_transform_merges_interval_and_register_channels(self):
        adapter = self._make_adapter(s3_client=FakeS3Client(CROSSWALK_CSV))
        output = cmep_rows_to_extract_output([REAL_CF_ROW, REAL_CFREG_ROW])
        meters, reads = adapter._transform("runid", output)

        self.assertEqual(1, len(meters))
        # 24 interval hours + the first day's 07:00 register-only boundary
        self.assertEqual(25, len(reads))

        tz = pytz.timezone("America/Los_Angeles")
        by_flowtime = {r.flowtime: r for r in reads}
        boundary = tz.localize(datetime(2026, 6, 3, 7, 0))
        merged = by_flowtime[boundary]
        self.assertEqual(13.0, merged.interval_value)
        self.assertEqual(2932046.0, merged.register_value)
        self.assertEqual("CF", merged.interval_unit)
        self.assertEqual("CF", merged.register_unit)

        register_only = by_flowtime[tz.localize(datetime(2026, 6, 2, 7, 0))]
        self.assertEqual(2931567.0, register_only.register_value)
        self.assertIsNone(register_only.interval_value)

    def test_transform_interval_sum_matches_register_delta(self):
        # Invariant observed in the real feed: a day's interval values sum to
        # the register delta across the same window.
        adapter = self._make_adapter(s3_client=FakeS3Client(CROSSWALK_CSV))
        output = cmep_rows_to_extract_output([REAL_CF_ROW, REAL_CFREG_ROW])
        _, reads = adapter._transform("runid", output)
        interval_sum = sum(r.interval_value for r in reads if r.interval_value)
        register_values = sorted(
            r.register_value for r in reads if r.register_value is not None
        )
        self.assertEqual(register_values[1] - register_values[0], interval_sum)

    def test_transform_stamps_crosswalk_ids_for_prefixed_and_bare_meters(self):
        adapter = self._make_adapter(s3_client=FakeS3Client(CROSSWALK_CSV))
        output = cmep_rows_to_extract_output(
            [REAL_CF_ROW, REAL_CFREG_ROW, BARE_ID_CF_ROW]
        )
        meters, reads = adapter._transform("runid", output)

        by_device = {m.device_id: m for m in meters}
        self.assertEqual("3210987-001", by_device["B74741489"].account_id)
        self.assertEqual("3210987-1", by_device["B74741489"].location_id)
        self.assertEqual("3231422-001", by_device["68567673"].account_id)
        self.assertEqual("3231422-1", by_device["68567673"].location_id)
        # endpoint_id carries the feed's receiver_id
        self.assertEqual("13169370", by_device["B74741489"].endpoint_id)
        for read in reads:
            self.assertIsNotNone(read.account_id)
            self.assertIsNotNone(read.location_id)

    def test_transform_loads_unmatched_meter_with_null_ids(self):
        crosswalk_without_bare_meter = (
            "meter_id,account_id,location_id\nB74741489,3210987-001,3210987-1\n"
        )
        adapter = self._make_adapter(
            s3_client=FakeS3Client(crosswalk_without_bare_meter)
        )
        output = cmep_rows_to_extract_output([BARE_ID_CF_ROW])
        meters, reads = adapter._transform("runid", output)
        self.assertEqual(1, len(meters))
        self.assertIsNone(meters[0].account_id)
        self.assertIsNone(meters[0].location_id)
        for read in reads:
            self.assertIsNone(read.account_id)

    def test_transform_without_crosswalk_configured(self):
        s3 = FakeS3Client(CROSSWALK_CSV)
        adapter = self._make_adapter(s3_client=s3, with_crosswalk=False)
        output = cmep_rows_to_extract_output([REAL_CF_ROW])
        meters, _ = adapter._transform("runid", output)
        self.assertIsNone(meters[0].account_id)
        self.assertEqual([], s3.requests)

    def test_transform_localizes_flowtimes_to_org_timezone(self):
        adapter = self._make_adapter(s3_client=FakeS3Client(CROSSWALK_CSV))
        output = cmep_rows_to_extract_output([REAL_CF_ROW])
        _, reads = adapter._transform("runid", output)
        first = min(r.flowtime for r in reads)
        self.assertIsNotNone(first.utcoffset())
        # 2026-06-02 08:00 Pacific (PDT, UTC-7) == 15:00 UTC
        self.assertEqual(
            datetime(2026, 6, 2, 15, 0, tzinfo=pytz.UTC),
            first.astimezone(pytz.UTC),
        )

    def test_transform_raises_on_unrecognized_units(self):
        adapter = self._make_adapter(s3_client=FakeS3Client(CROSSWALK_CSV))
        output = cmep_rows_to_extract_output([REAL_CF_ROW.replace(",CF,", ",GAL5,", 1)])
        with self.assertRaises(ValueError):
            adapter._transform("runid", output)

    def test_transform_skips_non_water_commodity(self):
        adapter = self._make_adapter(s3_client=FakeS3Client(CROSSWALK_CSV))
        output = cmep_rows_to_extract_output(
            [REAL_CF_ROW.replace(",OK,W,", ",OK,E,", 1)]
        )
        meters, reads = adapter._transform("runid", output)
        self.assertEqual([], meters)
        self.assertEqual([], reads)

    def test_transform_skips_non_ok_purpose(self):
        adapter = self._make_adapter(s3_client=FakeS3Client(CROSSWALK_CSV))
        output = cmep_rows_to_extract_output(
            [REAL_CF_ROW.replace(",OK,W,", ",SUMMARY,W,", 1)]
        )
        meters, reads = adapter._transform("runid", output)
        self.assertEqual([], meters)
        self.assertEqual([], reads)

    def test_transform_skips_meter_with_null_device_id(self):
        parsed = parse_cmep_row(next(csv.reader([REAL_CF_ROW])))
        parsed.meter_id = ""
        output = ExtractOutput(
            {"meters_and_reads.json": json.dumps(parsed, cls=DataclassJSONEncoder)}
        )
        adapter = self._make_adapter(s3_client=FakeS3Client(CROSSWALK_CSV))
        meters, reads = adapter._transform("runid", output)
        self.assertEqual([], meters)
        self.assertEqual([], reads)

    ###########################################################################
    # Crosswalk validation
    ###########################################################################

    def test_crosswalk_raises_on_wrong_columns(self):
        adapter = self._make_adapter(s3_client=FakeS3Client("meter,acct,loc\nB1,a,l\n"))
        with self.assertRaises(ValueError):
            adapter._load_crosswalk()

    def test_crosswalk_raises_on_malformed_row(self):
        adapter = self._make_adapter(
            s3_client=FakeS3Client("meter_id,account_id,location_id\nB1,,l\n")
        )
        with self.assertRaises(ValueError):
            adapter._load_crosswalk()

    def test_crosswalk_raises_on_empty_file(self):
        adapter = self._make_adapter(
            s3_client=FakeS3Client("meter_id,account_id,location_id\n")
        )
        with self.assertRaises(ValueError):
            adapter._load_crosswalk()

    def test_crosswalk_returns_none_when_not_configured(self):
        adapter = self._make_adapter(with_crosswalk=False)
        self.assertIsNone(adapter._load_crosswalk())

    ###########################################################################
    # Raw loader
    ###########################################################################

    def test_loader_unique_by_separates_channels(self):
        loader = XylemSensusBaseTableLoader()
        self.assertIn("units", loader.unique_by())
        self.assertIn("meter_id", loader.unique_by())
        self.assertIn("time_stamp", loader.unique_by())

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytz

from amiadapters.adapters.itron_roseville import (
    READ_VALUE_ERROR_THRESHOLD,
    ItronRosevilleAdapter,
    ItronRosevilleIntervalBaseTableLoader,
    ItronRosevilleIntervalRead,
    ItronRosevilleRegisterBaseTableLoader,
    ItronRosevilleRegisterRead,
)
from amiadapters.models import DataclassJSONEncoder
from amiadapters.outputs.base import ExtractOutput
from test.base_test_case import BaseTestCase

CSV_HEADER = "Timestamp,Read_Value,Meter_Serial_Number,EndpointID,Location_ID,Meter_Install_Date,Read_Units"


def csv_row(
    timestamp="06/17/2026 16:00:00.000000",
    read_value="43490.000000000",
    meter_serial_number="63735693",
    endpoint_id="2.16.840.1.114416.17.0120343350",
    location_id="3054011_2",
    meter_install_date="11/18/2011 00:00:00.000000",
    read_units="CF_WAT",
):
    return f"{timestamp},{read_value},{meter_serial_number},{endpoint_id},{location_id},{meter_install_date},{read_units}"


class TestItronRosevilleAdapter(BaseTestCase):

    def setUp(self):
        self.mock_s3_client = MagicMock()
        self.adapter = ItronRosevilleAdapter(
            "test-org",
            pytz.timezone("America/Los_Angeles"),
            self.TEST_PIPELINE_CONFIGURATION,
            "my-bucket",
            "my-prefix/",
            "us-east-1",
            "my_key_id",
            "my_secret_key",
            self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
            s3_client=self.mock_s3_client,
        )

    def _stub_s3(self, keys_to_csv_text: dict, last_modified: dict = None):
        last_modified = last_modified or {}
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": key,
                        "LastModified": last_modified.get(
                            key, datetime(2026, 7, 1, tzinfo=pytz.UTC)
                        ),
                    }
                    for key in keys_to_csv_text
                ]
            }
        ]
        self.mock_s3_client.get_paginator.return_value = paginator

        def get_object(Bucket, Key):
            body = MagicMock()
            body.read.return_value = keys_to_csv_text[Key].encode("utf-8")
            return {"Body": body}

        self.mock_s3_client.get_object.side_effect = get_object

    def test_extract(self):
        register_csv = "\n".join(
            [
                CSV_HEADER,
                csv_row(read_value="43490.000000000"),
                csv_row(timestamp="06/18/2026 00:00:00.000000", read_value="43495.0"),
            ]
        )
        interval_csv = "\n".join(
            [
                CSV_HEADER,
                csv_row(timestamp="06/17/2026 17:00:00.000000", read_value="3.0"),
            ]
        )
        self._stub_s3(
            {
                "my-prefix/rosevillecityof_Register_20260616_20260619.csv": register_csv,
                "my-prefix/rosevillecityof_Interval_20260616_20260619.csv": interval_csv,
            }
        )

        result = self.adapter._extract(
            "run-1", datetime(2026, 6, 17), datetime(2026, 6, 19)
        )

        register_rows = result.load_from_file(
            "register.json", ItronRosevilleRegisterRead, allow_empty=True
        )
        interval_rows = result.load_from_file(
            "interval.json", ItronRosevilleIntervalRead, allow_empty=True
        )
        self.assertEqual(2, len(register_rows))
        self.assertEqual(1, len(interval_rows))
        self.assertEqual("63735693", register_rows[0].Meter_Serial_Number)
        self.assertEqual("CF_WAT", interval_rows[0].Read_Units)

    def test_extract_lists_only_top_level_keys(self):
        self._stub_s3({})
        self.adapter._extract("run-1", datetime(2026, 6, 17), datetime(2026, 6, 19))
        paginator = self.mock_s3_client.get_paginator.return_value
        paginator.paginate.assert_called_once_with(
            Bucket="my-bucket", Prefix="my-prefix/", Delimiter="/"
        )

    def test_extract_skips_files_outside_range_and_unrecognized_files(self):
        register_csv = "\n".join([CSV_HEADER, csv_row()])
        self._stub_s3(
            {
                "my-prefix/rosevillecityof_Register_20260616_20260619.csv": register_csv,
                "my-prefix/rosevillecityof_Register_20260501_20260504.csv": register_csv,
                "my-prefix/testAddress.csv": "AddressID,AddressLine1\n1,110 Main St",
            }
        )
        result = self.adapter._extract(
            "run-1", datetime(2026, 6, 17), datetime(2026, 6, 19)
        )
        register_rows = result.load_from_file(
            "register.json", ItronRosevilleRegisterRead, allow_empty=True
        )
        self.assertEqual(1, len(register_rows))
        self.assertEqual(1, self.mock_s3_client.get_object.call_count)

    def test_extract_strips_utf8_bom(self):
        register_csv = "\ufeff" + "\n".join([CSV_HEADER, csv_row()])
        self._stub_s3(
            {"my-prefix/rosevillecityof_Register_20260616_20260619.csv": register_csv}
        )
        result = self.adapter._extract(
            "run-1", datetime(2026, 6, 17), datetime(2026, 6, 19)
        )
        register_rows = result.load_from_file(
            "register.json", ItronRosevilleRegisterRead, allow_empty=True
        )
        self.assertEqual(1, len(register_rows))
        self.assertEqual("06/17/2026 16:00:00.000000", register_rows[0].Timestamp)

    def test_extract_processes_files_in_last_modified_order(self):
        register_csv = "\n".join([CSV_HEADER, csv_row()])
        old_key = "my-prefix/rosevillecityof_Register_20260616_20260619.csv"
        new_key = "my-prefix/rosevillecityof_Register_20260617_20260620.csv"
        self._stub_s3(
            # dict order puts the newer-delivered file first; LastModified
            # ordering must reverse that
            {new_key: register_csv, old_key: register_csv},
            last_modified={
                new_key: datetime(2026, 6, 21, tzinfo=pytz.UTC),
                old_key: datetime(2026, 6, 20, tzinfo=pytz.UTC),
            },
        )
        self.adapter._extract("run-1", datetime(2026, 6, 17), datetime(2026, 6, 19))
        downloaded = [
            call.kwargs["Key"] for call in self.mock_s3_client.get_object.call_args_list
        ]
        self.assertEqual([old_key, new_key], downloaded)

    def test_prefix_without_trailing_slash_is_normalized(self):
        adapter = ItronRosevilleAdapter(
            "test-org",
            pytz.timezone("America/Los_Angeles"),
            self.TEST_PIPELINE_CONFIGURATION,
            "my-bucket",
            "my-prefix",  # no trailing slash
            "us-east-1",
            "my_key_id",
            "my_secret_key",
            self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
            s3_client=self.mock_s3_client,
        )
        self.assertEqual("my-prefix/", adapter.s3_prefix)

    def test_transform_merges_register_onto_interval_read_at_same_flowtime(self):
        extract_outputs = self._extract_outputs(
            register_rows=[
                self.register_read_factory(
                    Timestamp="06/17/2026 16:00:00.000000", Read_Value="43490.0"
                )
            ],
            interval_rows=[
                self.interval_read_factory(
                    Timestamp="06/17/2026 16:00:00.000000", Read_Value="5.0"
                )
            ],
        )

        meters, reads = self.adapter._transform("run-1", extract_outputs)

        self.assertEqual(1, len(meters))
        self.assertEqual(1, len(reads))
        read = reads[0]
        self.assertEqual(43490.0, read.register_value)
        self.assertEqual("CF", read.register_unit)
        self.assertEqual(5.0, read.interval_value)
        self.assertEqual("CF", read.interval_unit)
        expected_flowtime = pytz.timezone("America/Los_Angeles").localize(
            datetime(2026, 6, 17, 16, 0, 0)
        )
        self.assertEqual(expected_flowtime, read.flowtime)

    def test_transform_keeps_register_only_and_interval_only_reads(self):
        extract_outputs = self._extract_outputs(
            register_rows=[
                self.register_read_factory(
                    Timestamp="06/17/2026 08:00:00.000000", Read_Value="43490.0"
                )
            ],
            interval_rows=[
                self.interval_read_factory(
                    Timestamp="06/17/2026 09:00:00.000000", Read_Value="2.0"
                )
            ],
        )

        meters, reads = self.adapter._transform("run-1", extract_outputs)

        self.assertEqual(2, len(reads))
        by_flowtime = {r.flowtime.hour: r for r in reads}
        self.assertEqual(43490.0, by_flowtime[8].register_value)
        self.assertIsNone(by_flowtime[8].interval_value)
        self.assertEqual(2.0, by_flowtime[9].interval_value)
        self.assertIsNone(by_flowtime[9].register_value)

    def test_transform_excludes_register_error_codes_but_keeps_interval(self):
        extract_outputs = self._extract_outputs(
            register_rows=[
                self.register_read_factory(
                    Timestamp="06/17/2026 16:00:00.000000",
                    Read_Value="4294967294",  # 2**32 - 2, the observed fault code
                    Meter_Serial_Number="60603374",
                )
            ],
            interval_rows=[
                self.interval_read_factory(
                    Timestamp="06/17/2026 16:00:00.000000",
                    Read_Value="0.0",
                    Meter_Serial_Number="60603374",
                )
            ],
        )

        meters, reads = self.adapter._transform("run-1", extract_outputs)

        # The meter and its interval read survive; the sentinel register value does not
        self.assertEqual(1, len(meters))
        self.assertEqual(1, len(reads))
        self.assertIsNone(reads[0].register_value)
        self.assertEqual(0.0, reads[0].interval_value)

    def test_transform_excludes_interval_error_codes(self):
        extract_outputs = self._extract_outputs(
            register_rows=[],
            interval_rows=[
                self.interval_read_factory(Read_Value="4294967294"),
                self.interval_read_factory(
                    Timestamp="06/17/2026 17:00:00.000000", Read_Value="2.0"
                ),
            ],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(reads))
        self.assertEqual(2.0, reads[0].interval_value)

    def test_transform_excludes_values_at_or_above_error_threshold(self):
        # Fault codes are a threshold, not one magic number: 2**32 - 1 and the
        # threshold itself are excluded; the largest plausible real register
        # (well below the threshold) survives.
        extract_outputs = self._extract_outputs(
            register_rows=[
                self.register_read_factory(Read_Value="4294967295"),  # 2**32 - 1
                self.register_read_factory(
                    Timestamp="06/17/2026 08:00:00.000000",
                    Read_Value=str(READ_VALUE_ERROR_THRESHOLD),
                ),
                self.register_read_factory(
                    Timestamp="06/17/2026 00:00:00.000000", Read_Value="97830982"
                ),
            ],
            interval_rows=[],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(reads))
        self.assertEqual(97830982.0, reads[0].register_value)

    def test_normalize_unit_strips_commodity_suffix(self):
        # _WAT is Itron's commodity suffix; the remaining unit goes through
        # map_reading, so GAL_WAT converts to CF and garbage still raises.
        extract_outputs = self._extract_outputs(
            register_rows=[],
            interval_rows=[
                self.interval_read_factory(Read_Value="7.48052", Read_Units="GAL_WAT")
            ],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(reads))
        self.assertAlmostEqual(1.0, reads[0].interval_value, places=6)
        self.assertEqual("CF", reads[0].interval_unit)

    def test_transform_counts_missing_device_id(self):
        extract_outputs = self._extract_outputs(
            register_rows=[self.register_read_factory(Meter_Serial_Number="")],
            interval_rows=[self.interval_read_factory(Read_Value="2.0")],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(reads))  # blank-serial row skipped, not fatal
        self.assertEqual(1, len(meters))

    def test_transform_skips_reads_with_blank_timestamp(self):
        extract_outputs = self._extract_outputs(
            register_rows=[self.register_read_factory(Timestamp="")],
            interval_rows=[
                self.interval_read_factory(Timestamp=""),
                self.interval_read_factory(Read_Value="2.0"),
            ],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(reads))
        self.assertIsNotNone(reads[0].flowtime)

    def test_transform_skips_unparseable_read_values(self):
        extract_outputs = self._extract_outputs(
            register_rows=[self.register_read_factory(Read_Value="ERROR")],
            interval_rows=[
                self.interval_read_factory(Read_Value=""),
                self.interval_read_factory(
                    Timestamp="06/17/2026 17:00:00.000000", Read_Value="2.0"
                ),
            ],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(reads))
        self.assertEqual(2.0, reads[0].interval_value)

    def test_transform_meter_attributes_last_occurrence_wins(self):
        extract_outputs = self._extract_outputs(
            register_rows=[
                self.register_read_factory(Location_ID="3054011_2"),
                self.register_read_factory(
                    Timestamp="06/18/2026 00:00:00.000000", Location_ID="3054011_3"
                ),
            ],
            interval_rows=[],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(meters))
        self.assertEqual("3054011_3", meters[0].location_id)

    def test_transform_keeps_zero_interval_values(self):
        extract_outputs = self._extract_outputs(
            register_rows=[],
            interval_rows=[
                self.interval_read_factory(
                    Timestamp="06/17/2026 16:00:00.000000", Read_Value="0.0"
                )
            ],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(reads))
        self.assertEqual(0.0, reads[0].interval_value)

    def test_transform_creates_meter_from_interval_only_data(self):
        extract_outputs = self._extract_outputs(
            register_rows=[],
            interval_rows=[self.interval_read_factory(Meter_Serial_Number="101929454")],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        self.assertEqual(1, len(meters))
        meter = meters[0]
        self.assertEqual("101929454", meter.device_id)
        self.assertEqual("101929454", meter.meter_id)
        self.assertEqual("3054011_2", meter.location_id)
        self.assertIsNone(meter.account_id)

    def test_transform_meter_fields(self):
        extract_outputs = self._extract_outputs(
            register_rows=[self.register_read_factory()],
            interval_rows=[],
        )
        meters, reads = self.adapter._transform("run-1", extract_outputs)
        meter = meters[0]
        self.assertEqual("test-org", meter.org_id)
        self.assertEqual("63735693", meter.device_id)
        self.assertEqual("2.16.840.1.114416.17.0120343350", meter.endpoint_id)
        expected_install = pytz.timezone("America/Los_Angeles").localize(
            datetime(2011, 11, 18)
        )
        self.assertEqual(expected_install, meter.meter_install_date)
        self.assertIsNone(meter.meter_size)
        self.assertIsNone(meter.multiplier)

    def test_transform_raises_on_unknown_unit(self):
        extract_outputs = self._extract_outputs(
            register_rows=[],
            interval_rows=[self.interval_read_factory(Read_Units="AF_WAT")],
        )
        with self.assertRaises(ValueError) as context:
            self.adapter._transform("run-1", extract_outputs)
        self.assertIn("Unrecognized unit of measure", str(context.exception))

    def test_normalize_unit(self):
        for unit, expected in [
            ("CF_WAT", "CF"),
            ("cf_wat", "CF"),
            ("CF", "CF"),
            (None, None),
        ]:
            self.assertEqual(
                expected,
                ItronRosevilleAdapter._normalize_unit(unit),
                f"unexpected normalization for {unit}",
            )

    def test_parse_timestamp(self):
        result = self.adapter._parse_timestamp("06/16/2026 17:00:00.000000")
        expected = pytz.timezone("America/Los_Angeles").localize(
            datetime(2026, 6, 16, 17, 0, 0)
        )
        self.assertEqual(expected, result)
        self.assertIsNone(self.adapter._parse_timestamp(""))
        self.assertIsNone(self.adapter._parse_timestamp(None))

    def register_read_factory(self, **kwargs) -> ItronRosevilleRegisterRead:
        defaults = dict(
            Timestamp="06/17/2026 16:00:00.000000",
            Read_Value="43490.000000000",
            Meter_Serial_Number="63735693",
            EndpointID="2.16.840.1.114416.17.0120343350",
            Location_ID="3054011_2",
            Meter_Install_Date="11/18/2011 00:00:00.000000",
            Read_Units="CF_WAT",
        )
        defaults.update(kwargs)
        return ItronRosevilleRegisterRead(**defaults)

    def interval_read_factory(self, **kwargs) -> ItronRosevilleIntervalRead:
        defaults = dict(
            Timestamp="06/17/2026 16:00:00.000000",
            Read_Value="1.0",
            Meter_Serial_Number="63735693",
            EndpointID="2.16.840.1.114416.17.0120343350",
            Location_ID="3054011_2",
            Meter_Install_Date="11/18/2011 00:00:00.000000",
            Read_Units="CF_WAT",
        )
        defaults.update(kwargs)
        return ItronRosevilleIntervalRead(**defaults)

    def _extract_outputs(self, register_rows, interval_rows) -> ExtractOutput:
        return ExtractOutput(
            {
                "register.json": "\n".join(
                    json.dumps(r, cls=DataclassJSONEncoder) for r in register_rows
                ),
                "interval.json": "\n".join(
                    json.dumps(r, cls=DataclassJSONEncoder) for r in interval_rows
                ),
            }
        )


class TestItronRosevilleRawLoaders(BaseTestCase):

    def test_register_loader(self):
        loader = ItronRosevilleRegisterBaseTableLoader()
        self.assertEqual("ITRON_ROSEVILLE_REGISTER_BASE", loader.table_name())
        self.assertEqual(
            [
                "Timestamp",
                "Read_Value",
                "Meter_Serial_Number",
                "EndpointID",
                "Location_ID",
                "Meter_Install_Date",
                "Read_Units",
            ],
            loader.columns(),
        )
        self.assertEqual(["meter_serial_number", "timestamp"], loader.unique_by())

    def test_interval_loader(self):
        loader = ItronRosevilleIntervalBaseTableLoader()
        self.assertEqual("ITRON_ROSEVILLE_INTERVAL_BASE", loader.table_name())
        self.assertEqual(["meter_serial_number", "timestamp"], loader.unique_by())

    def test_unique_by_matches_lowercased_columns(self):
        for loader in [
            ItronRosevilleRegisterBaseTableLoader(),
            ItronRosevilleIntervalBaseTableLoader(),
        ]:
            lowercased = [c.lower() for c in loader.columns()]
            for key in loader.unique_by():
                self.assertIn(key, lowercased)

    def test_prepare_raw_data_tuple_order_matches_columns(self):
        loader = ItronRosevilleRegisterBaseTableLoader()
        row = ItronRosevilleRegisterRead(
            Timestamp="06/17/2026 16:00:00.000000",
            Read_Value="43490.0",
            Meter_Serial_Number="63735693",
            EndpointID="endpoint",
            Location_ID="3054011_2",
            Meter_Install_Date="11/18/2011 00:00:00.000000",
            Read_Units="CF_WAT",
        )
        extract_outputs = ExtractOutput(
            {
                "register.json": json.dumps(row, cls=DataclassJSONEncoder),
                "interval.json": "",
            }
        )
        prepared = loader.prepare_raw_data(extract_outputs)
        self.assertEqual(1, len(prepared))
        self.assertEqual(
            (
                "06/17/2026 16:00:00.000000",
                "43490.0",
                "63735693",
                "endpoint",
                "3054011_2",
                "11/18/2011 00:00:00.000000",
                "CF_WAT",
            ),
            prepared[0],
        )

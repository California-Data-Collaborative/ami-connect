from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytz

from amiadapters.adapters.base import ExtractRangeCalculator
from amiadapters.adapters.beacon import Beacon360Adapter
from amiadapters.configuration.models import BackfillConfiguration
from amiadapters.storage.snowflake import SnowflakeStorageSink
from test.base_test_case import BaseTestCase


class TestBaseAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = Beacon360Adapter(
            api_user="user",
            api_password="pass",
            use_cache=False,
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            org_id="test-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
        )
        self.range_start = datetime(2026, 1, 1, tzinfo=pytz.UTC)
        self.range_end = datetime(2026, 1, 3, tzinfo=pytz.UTC)

    def test_datetime_from_iso_str__None_results_in_None(self):
        result = self.adapter.datetime_from_iso_str(None, self.adapter.org_timezone)
        self.assertIsNone(result)

    def test_datetime_from_iso_str__parses_naive_dt_and_replaces_with_utc_offset_if_none_provided(
        self,
    ):
        result = self.adapter.datetime_from_iso_str("2024-08-01 00:54", None)
        self.assertEqual("2024-08-01T00:54:00+00:00", result.isoformat())

    def test_datetime_from_iso_str__parses_naive_dt_and_replaces_offset(
        self,
    ):
        result = self.adapter.datetime_from_iso_str(
            "2024-08-01T00:54:00", self.adapter.org_timezone
        )
        self.assertEqual("2024-08-01T00:54:00+02:00", result.isoformat())

    def test_datetime_from_iso_str__preserves_offset_of_aware_dt(
        self,
    ):
        result = self.adapter.datetime_from_iso_str(
            "2024-08-01T00:54:00+02:00", self.adapter.org_timezone
        )
        self.assertEqual("2024-08-01T00:54:00+02:00", result.isoformat())

    def test_map_meter_size(self):
        cases = [
            ('3/8"', "0.375"),
            (None, None),
        ]
        for size, expected in cases:
            result = self.adapter.map_meter_size(size)
            self.assertEqual(result, expected)

    def test_extract_consumption_for_all_meters__throws_exception_when_range_not_valid(
        self,
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._validate_extract_range(None, self.range_end)

        with self.assertRaises(Exception) as context:
            self.adapter._validate_extract_range(self.range_start, None)

        with self.assertRaises(Exception) as context:
            # End after start
            self.adapter._validate_extract_range(self.range_end, self.range_start)

    def _adapter_with_limit(self, max_extract_days):
        return Beacon360Adapter(
            api_user="user",
            api_password="pass",
            use_cache=False,
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            org_id="test-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
            max_extract_days=max_extract_days,
        )

    def test_split_extract_range__range_within_limit_is_one_piece(self):
        end = self.range_start + timedelta(days=14)
        self.assertEqual(
            [(self.range_start, end)],
            self.adapter.split_extract_range(self.range_start, end),
        )

    def test_split_extract_range__longer_range_splits_oldest_first(self):
        end = self.range_start + timedelta(days=31)
        pieces = self.adapter.split_extract_range(self.range_start, end)
        day = timedelta(days=1)
        self.assertEqual(
            [
                (self.range_start, self.range_start + 14 * day),
                (self.range_start + 14 * day, self.range_start + 28 * day),
                (self.range_start + 28 * day, end),
            ],
            pieces,
        )

    def test_split_extract_range__configured_limit_wins_over_default(self):
        adapter = self._adapter_with_limit(30)
        end = self.range_start + timedelta(days=31)
        pieces = adapter.split_extract_range(self.range_start, end)
        self.assertEqual(2, len(pieces))
        self.assertEqual(self.range_start + timedelta(days=30), pieces[0][1])

    def test_split_extract_range__rejects_end_before_start(self):
        with self.assertRaises(Exception):
            self.adapter.split_extract_range(self.range_end, self.range_start)

    def test_calculate_extract_range__long_manual_range_is_returned_whole(self):
        end = self.range_start + timedelta(days=60)
        start, calculated_end = self.adapter.calculate_extract_range(
            self.range_start, end, interval=timedelta(days=2), lag=timedelta(days=0)
        )
        self.assertEqual((self.range_start, end), (start, calculated_end))

    def test_calculate_extract_range__backfill_chunk_trimmed_to_limit(self):
        backfill = BackfillConfiguration(
            org_id="test-org",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 12, 31),
            interval_days=30,
            schedule="*/15 * * * *",
        )
        with patch.object(
            ExtractRangeCalculator,
            "calculate_extract_range",
            return_value=(self.range_start, self.range_end),
        ) as calculate:
            self.adapter.calculate_extract_range(
                None,
                None,
                interval=timedelta(days=2),
                lag=timedelta(days=0),
                backfill_params=backfill,
            )
        self.assertEqual(
            14, calculate.call_args.kwargs["backfill_params"].interval_days
        )

    def test_extract_and_output__refuses_range_over_limit(self):
        with patch.object(self.adapter, "_extract") as extract:
            with self.assertRaises(Exception) as ctx:
                self.adapter.extract_and_output(
                    "run-1",
                    self.range_start,
                    self.range_start + timedelta(days=14, hours=1),
                )
        extract.assert_not_called()
        self.assertIn("split_extract_range", str(ctx.exception))

    def _post_process_window(self, cover_extract_range):
        sink = MagicMock()
        self.adapter.storage_sinks = [sink]
        self.adapter.pipeline_configuration = MagicMock(
            should_run_post_processor=True, should_publish_load_finished_events=False
        )
        self.adapter.post_process(
            "run-1",
            datetime(2025, 3, 1, tzinfo=pytz.UTC),
            datetime(2025, 3, 31, tzinfo=pytz.UTC),
            cover_extract_range=cover_extract_range,
        )
        _, window_start, window_end = sink.exec_postprocessor.call_args.args
        return window_start, window_end

    def test_post_process__default_window_is_last_thirty_days(self):
        window_start, window_end = self._post_process_window(False)
        self.assertEqual(timedelta(days=30), window_end - window_start)

    def test_post_process__cover_extract_range_starts_at_range_start(self):
        window_start, window_end = self._post_process_window(True)
        self.assertEqual(datetime(2025, 3, 1), window_start)
        self.assertEqual(datetime.today().date(), window_end.date())

    def test_map_reading__valid_ccf_conversion(self):
        value, unit = self.adapter.map_reading(12.5, "CCF")
        self.assertEqual(value, 1250)
        self.assertEqual(unit, "CF")

    def test_map_reading__valid_cf_conversion(self):
        value, unit = self.adapter.map_reading(1200, "CF")
        self.assertEqual(value, 1200)
        self.assertEqual(unit, "CF")

    def test_map_reading__valid_gal_conversion(self):
        value, unit = self.adapter.map_reading(2000, "Gallon")
        self.assertAlmostEqual(value, 267.36, delta=0.01)
        self.assertEqual(unit, "CF")

    def test_map_reading__valid_kgal_conversion(self):
        value, unit = self.adapter.map_reading(5, "KGAL")
        self.assertAlmostEqual(value, 668.405, delta=0.01)
        self.assertEqual(unit, "CF")

    def test_map_reading__none_reading(self):
        value, unit = self.adapter.map_reading(None, "CCF")
        self.assertIsNone(value)
        self.assertIsNone(unit)

    def test_map_reading__none_unit(self):
        value, unit = self.adapter.map_reading(10.0, None)
        self.assertEqual(value, 10.0)
        self.assertIsNone(unit)

    def test_map_reading__none_both(self):
        value, unit = self.adapter.map_reading(None, None)
        self.assertIsNone(value)
        self.assertIsNone(unit)

    def test_map_reading__unrecognized_unit(self):
        with self.assertRaises(ValueError, msg="Unrecognized unit of measure: Pounds"):
            self.adapter.map_reading(5.0, "Pounds")


class TestExtractRangeCalculator(BaseTestCase):

    def setUp(self):
        self.snowflake_sink = MagicMock(spec=SnowflakeStorageSink)
        self.snowflake_sink.calculate_end_of_backfill_range.return_value = 3
        sinks = [self.snowflake_sink]
        self.calculator = ExtractRangeCalculator(
            org_id="my_org",
            storage_sinks=sinks,
        )
        self.interval = timedelta(days=2)
        self.lag = timedelta(days=0)

    @patch("amiadapters.adapters.base.datetime")
    def test_calculate_extract_range__both_dates_none(self, mock_datetime):
        # Set up mock for datetime.now()
        now = datetime(2025, 4, 22, 12, 0, 0)
        mock_datetime.now.return_value = now

        # Expected values
        expected_end = now
        expected_start = now - timedelta(days=2)

        # Test when both start and end are None
        result_start, result_end = self.calculator.calculate_extract_range(
            None, None, self.interval, self.lag, backfill_params=None
        )

        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)
        mock_datetime.now.assert_called_once()

    @patch("amiadapters.adapters.base.datetime")
    def test_calculate_extract_range__honors_interval_param(self, mock_datetime):
        other_interval = timedelta(days=5)
        calculator = ExtractRangeCalculator(
            org_id="my_org",
            storage_sinks=[],
        )

        end = datetime(2024, 1, 1)
        result_start, result_end = calculator.calculate_extract_range(
            None, end, other_interval, self.lag, backfill_params=None
        )

        # Expected values
        expected_end = end
        expected_start = end - timedelta(days=5)

        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)

    def test_calculate_extract_range__start_none_end_provided(self):
        # Provide end date
        end_date = datetime(2025, 4, 22, 12, 0, 0)

        # Expected values
        expected_end = end_date
        expected_start = end_date - timedelta(days=2)

        # Test when start is None and end is provided
        result_start, result_end = self.calculator.calculate_extract_range(
            None, end_date, self.interval, self.lag, backfill_params=None
        )

        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)

    def test_calculate_extract_range__start_provided_end_none(self):
        # Provide start date
        start_date = datetime(2025, 4, 20, 12, 0, 0)

        # Expected values
        expected_start = start_date
        expected_end = start_date + timedelta(days=2)

        # Test when start is provided and end is None
        result_start, result_end = self.calculator.calculate_extract_range(
            start_date, None, self.interval, self.lag, backfill_params=None
        )

        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)

    def test_calculate_extract_range__both_dates_provided(self):
        # Provide both dates
        start_date = datetime(2025, 4, 20, 12, 0, 0)
        end_date = datetime(2025, 4, 25, 12, 0, 0)

        # Expected values are the same as input
        expected_start = start_date
        expected_end = end_date

        # Test when both start and end are provided
        result_start, result_end = self.calculator.calculate_extract_range(
            start_date, end_date, self.interval, self.lag, backfill_params=None
        )

        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)

    def test_calculate_extract_range__backfill_with_no_snowflake_sink(self):
        start_date = datetime(2025, 4, 20, 12, 0, 0)
        end_date = datetime(2025, 4, 25, 12, 0, 0)
        backfill_params = BackfillConfiguration(
            org_id=self.calculator.org_id,
            start_date=start_date,
            end_date=end_date,
            interval_days=3,
            schedule="",
        )

        self.calculator.storage_sinks = [MagicMock()]

        with self.assertRaises(Exception):
            self.calculator.calculate_extract_range(
                None, None, backfill_params=backfill_params
            )

    def test_calculate_extract_range__backfill_with_snowflake_sink(self):
        start_date = datetime(2025, 4, 20, 12, 0, 0)
        end_date = datetime(2025, 4, 25, 12, 0, 0)
        self.snowflake_sink.calculate_end_of_backfill_range.return_value = end_date
        backfill_params = BackfillConfiguration(
            org_id=self.calculator.org_id,
            start_date=start_date,
            end_date=end_date,
            interval_days=3,
            schedule="",
        )

        # Test when backfill
        result_start, result_end = self.calculator.calculate_extract_range(
            None, None, self.interval, self.lag, backfill_params=backfill_params
        )

        expected_start = end_date - timedelta(days=3)
        expected_end = end_date

        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)

    def test_calculate_extract_range__backfill_with_snowflake_sink_that_gives_no_oldest_time(
        self,
    ):
        start_date = datetime(2025, 4, 20, 12, 0, 0)
        end_date = datetime(2025, 4, 25, 12, 0, 0)
        self.snowflake_sink.calculate_end_of_backfill_range.return_value = None
        backfill_params = BackfillConfiguration(
            org_id=self.calculator.org_id,
            start_date=start_date,
            end_date=end_date,
            interval_days=3,
            schedule="",
        )

        with self.assertRaises(Exception):
            self.calculator.calculate_extract_range(
                None, None, backfill_params=backfill_params
            )


class TestLoadTransformed(BaseTestCase):

    def setUp(self):
        self.adapter = Beacon360Adapter(
            api_user="user",
            api_password="pass",
            use_cache=False,
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            org_id="test-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
        )
        self.adapter.output_controller = MagicMock()
        self.sink = MagicMock()
        self.adapter.storage_sinks = [self.sink]

    def test_load_transformed__happy_path_calls_sink(self):
        self.adapter.output_controller.read_transformed_meters.return_value = [
            MagicMock()
        ]
        self.adapter.output_controller.read_transformed_meter_reads.return_value = [
            MagicMock()
        ]

        self.adapter.load_transformed("run-1")

        self.sink.store_transformed.assert_called_once()

    def test_load_transformed__raises_when_meters_empty(self):
        self.adapter.output_controller.read_transformed_meters.return_value = []
        self.adapter.output_controller.read_transformed_meter_reads.return_value = [
            MagicMock()
        ]

        with self.assertRaises(Exception) as cm:
            self.adapter.load_transformed("run-1")

        self.assertIn("remove-backfill", str(cm.exception))
        self.sink.store_transformed.assert_not_called()

    def test_load_transformed__raises_when_reads_empty(self):
        self.adapter.output_controller.read_transformed_meters.return_value = [
            MagicMock()
        ]
        self.adapter.output_controller.read_transformed_meter_reads.return_value = []

        with self.assertRaises(Exception) as cm:
            self.adapter.load_transformed("run-1")

        self.assertIn("remove-backfill", str(cm.exception))
        self.sink.store_transformed.assert_not_called()

    def test_load_transformed__raises_when_both_empty(self):
        self.adapter.output_controller.read_transformed_meters.return_value = []
        self.adapter.output_controller.read_transformed_meter_reads.return_value = []

        with self.assertRaises(Exception) as cm:
            self.adapter.load_transformed("run-1")

        self.assertIn("remove-backfill", str(cm.exception))
        self.sink.store_transformed.assert_not_called()

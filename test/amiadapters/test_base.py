from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytz

from amiadapters.adapters.base import ExtractRangeCalculator
from amiadapters.adapters.beacon import Beacon360Adapter
from amiadapters.configuration.models import (
    BackfillConfiguration,
    LocalIntermediateOutputControllerConfiguration,
)
from amiadapters.storage.snowflake import SnowflakeStorageSink
from test.base_test_case import BaseTestCase


class TestBaseAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = Beacon360Adapter(
            api_user="user",
            api_password="pass",
            use_cache=False,
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            org_id="this-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_sinks=[],
        )

    def test_datetime_from_iso_str__None_results_in_None(self):
        result = self.adapter.datetime_from_iso_str(None, self.adapter.org_timezone)
        self.assertIsNone(result)

    def test_datetime_from_iso_str__parses_unaware_dt_and_replaces_tz_without_conversion(
        self,
    ):
        result = self.adapter.datetime_from_iso_str(
            "2024-08-01 00:54", self.adapter.org_timezone
        )
        self.assertEqual(
            datetime(2024, 8, 1, 0, 54, tzinfo=pytz.timezone("Europe/Rome")), result
        )

    def test_datetime_from_iso_str__parses_aware_dt_and_replaces_tz_without_conversion(
        self,
    ):
        result = self.adapter.datetime_from_iso_str(
            "2024-08-01T00:54:00Z", self.adapter.org_timezone
        )
        self.assertEqual(
            datetime(2024, 8, 1, 0, 54, tzinfo=pytz.timezone("Europe/Rome")), result
        )

    def test_datetime_from_iso_str__parses_aware_dt_and_defaults_to_utc_if_no_tz_provided(
        self,
    ):
        result = self.adapter.datetime_from_iso_str("2024-08-01T00:54:00+02:00", None)
        self.assertEqual(
            datetime(2024, 8, 1, 0, 54, tzinfo=pytz.timezone("UTC")), result
        )

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
            org_id="my_org", storage_sinks=sinks, default_interval_days=2
        )

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
            None, None, backfill_params=None
        )

        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)
        mock_datetime.now.assert_called_once()

    @patch("amiadapters.adapters.base.datetime")
    def test_calculate_extract_range__honors_default_interval_param(
        self, mock_datetime
    ):
        default_interval_days = 5
        calculator = ExtractRangeCalculator(
            org_id="my_org", storage_sinks=[], default_interval_days=5
        )

        end = datetime(2024, 1, 1)
        result_start, result_end = calculator.calculate_extract_range(
            None, end, backfill_params=None
        )

        # Expected values
        expected_end = end
        expected_start = end - timedelta(days=default_interval_days)

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
            None, end_date, backfill_params=None
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
            start_date, None, backfill_params=None
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
            start_date, end_date, backfill_params=None
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
            None, None, backfill_params=backfill_params
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

from datetime import datetime, timedelta
from unittest.mock import patch

import pytz

from amiadapters.base import default_date_range
from amiadapters.beacon import Beacon360Adapter
from test.base_test_case import BaseTestCase


class TestBaseAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = Beacon360Adapter(
            api_user="user",
            api_password="pass",
            intermediate_output="output",
            use_cache=False,
            org_id="this-org",
            org_timezone=pytz.timezone("Europe/Rome"),
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

    def test_unit_of_measure(self):
        cases = [
            ("CF", "CF"),
            ("CCF", "CCF"),
            ("Gallon", "Gallon"),
            (None, None),
        ]
        for size, expected in cases:
            result = self.adapter.map_unit_of_measure(size)
            self.assertEqual(result, expected)


class TestDefaultDateRange(BaseTestCase):

    @patch('amiadapters.base.datetime')
    def test_both_dates_none(self, mock_datetime):
        # Set up mock for datetime.now()
        now = datetime(2025, 4, 22, 12, 0, 0)
        mock_datetime.now.return_value = now
        
        # Expected values
        expected_end = now
        expected_start = now - timedelta(days=2)
        
        # Test when both start and end are None
        result_start, result_end = default_date_range(None, None)
        
        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)
        mock_datetime.now.assert_called_once()
    
    def test_start_none_end_provided(self):
        # Provide end date
        end_date = datetime(2025, 4, 22, 12, 0, 0)
        
        # Expected values
        expected_end = end_date
        expected_start = end_date - timedelta(days=2)
        
        # Test when start is None and end is provided
        result_start, result_end = default_date_range(None, end_date)
        
        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)
    
    def test_start_provided_end_none(self):
        # Provide start date
        start_date = datetime(2025, 4, 20, 12, 0, 0)
        
        # Expected values
        expected_start = start_date
        expected_end = start_date + timedelta(days=2)
        
        # Test when start is provided and end is None
        result_start, result_end = default_date_range(start_date, None)
        
        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)
    
    def test_both_dates_provided(self):
        # Provide both dates
        start_date = datetime(2025, 4, 20, 12, 0, 0)
        end_date = datetime(2025, 4, 25, 12, 0, 0)
        
        # Expected values are the same as input
        expected_start = start_date
        expected_end = end_date
        
        # Test when both start and end are provided
        result_start, result_end = default_date_range(start_date, end_date)
        
        # Verify results
        self.assertEqual(result_start, expected_start)
        self.assertEqual(result_end, expected_end)
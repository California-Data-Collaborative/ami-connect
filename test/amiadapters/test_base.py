from datetime import datetime

import pytz

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

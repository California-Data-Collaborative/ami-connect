from amiadapters.adapters.base import DEFAULT_SCHEDULE_CRONTAB, ScheduledExtract
import datetime
import unittest
from unittest.mock import MagicMock

from airflow.exceptions import ParamValidationError
from airflow.models.param import ParamsDict

from amicontrol.dags.meter_read_dags import (
    ami_control_dag_factory,
    manual_run_params,
    STAGGER_WINDOW_MINUTES,
    STAGGER_WINDOW_START_HOUR,
    staggered_schedule,
)
from test.base_test_case import BaseTestCase


class TestStaggeredSchedule(BaseTestCase):

    def test_nine_orgs_get_thirty_minute_spacing(self):
        # 270-minute window / 9 orgs = 30-minute spacing from 12:00
        self.assertEqual(
            "0 12 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 0, 9)
        )
        self.assertEqual(
            "30 12 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 1, 9)
        )
        self.assertEqual(
            "0 16 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 8, 9)
        )

    def test_spacing_tightens_as_orgs_are_added(self):
        # 270 / 10 = 27-minute spacing
        self.assertEqual(
            "0 12 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 0, 10)
        )
        self.assertEqual(
            "27 12 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 1, 10)
        )
        self.assertEqual(
            "3 16 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 9, 10)
        )

    def test_all_starts_fall_inside_the_window(self):
        window_end = STAGGER_WINDOW_START_HOUR * 60 + STAGGER_WINDOW_MINUTES
        for org_count in range(1, 17):
            for org_index in range(org_count):
                crontab = staggered_schedule(
                    DEFAULT_SCHEDULE_CRONTAB, org_index, org_count
                )
                minute, hour = (int(part) for part in crontab.split()[:2])
                start = hour * 60 + minute
                self.assertGreaterEqual(start, STAGGER_WINDOW_START_HOUR * 60)
                self.assertLess(start, window_end)

    def test_explicit_crontab_passes_through_unchanged(self):
        # Beacon's lagged extracts choose their own hours; they must not move.
        self.assertEqual("0 10 * * *", staggered_schedule("0 10 * * *", 3, 8))
        self.assertEqual("0 11 * * *", staggered_schedule("0 11 * * *", 5, 8))

    def test_scheduled_extract_default_matches_exported_constant(self):
        # staggered_schedule recognizes defaults by equality with the exported
        # constant; if the dataclass default ever diverges, staggering silently
        # stops applying.
        self.assertEqual(DEFAULT_SCHEDULE_CRONTAB, ScheduledExtract().schedule_crontab)


class TestManualRunForm(unittest.TestCase):
    def _validate(self, max_days, conf):
        params = ParamsDict(manual_run_params(max_days))
        params.update(conf)
        params.validate()

    def test_accepts_days_up_to_the_source_limit(self):
        self._validate(
            6, {"extract_range_start": "2026-01-01", "extract_range_days": 6}
        )

    def test_rejects_more_days_than_the_source_limit(self):
        with self.assertRaises(ParamValidationError):
            self._validate(
                6, {"extract_range_start": "2026-01-01", "extract_range_days": 7}
            )

    def test_rejects_zero_days(self):
        with self.assertRaises(ParamValidationError):
            self._validate(
                6, {"extract_range_start": "2026-01-01", "extract_range_days": 0}
            )

    def test_defaults_are_valid_for_a_one_day_limit(self):
        self._validate(1, {})

    def _extract_call(self, params, dag_params):
        adapter = MagicMock()
        adapter.name.return_value = "vendor-my_org"
        adapter.calculate_extract_range.return_value = ("start", "end")
        dag = ami_control_dag_factory(
            dag_id="my_org-ami-meter-read-dag",
            schedule=None,
            params=dag_params,
            adapter=adapter,
            on_failure_sns_notifier=None,
        )
        dag.get_task("extract-vendor-my_org").python_callable(
            adapter, params=params, dag_run=MagicMock(run_id="r")
        )
        return adapter.calculate_extract_range.call_args.args

    def test_start_and_days_become_the_extract_range(self):
        start, end, *_ = self._extract_call(
            {"extract_range_start": "2026-01-01", "extract_range_days": 5},
            manual_run_params(6),
        )
        self.assertEqual(datetime.datetime(2026, 1, 1), start)
        self.assertEqual(datetime.datetime(2026, 1, 6), end)

    def test_end_date_in_run_config_is_ignored_by_the_manual_dag(self):
        start, end, *_ = self._extract_call(
            {
                "extract_range_start": "2026-01-01",
                "extract_range_days": 5,
                "extract_range_end": "2026-12-31",
            },
            manual_run_params(6),
        )
        self.assertEqual(datetime.datetime(2026, 1, 6), end)

    def test_manual_run_without_start_takes_the_most_recent_days(self):
        start, end, interval, _ = self._extract_call(
            {"extract_range_start": "", "extract_range_days": 3}, manual_run_params(6)
        )
        self.assertEqual((None, None), (start, end))
        self.assertEqual(datetime.timedelta(days=3), interval)

    def test_other_dags_ignore_days_and_read_start_and_end_as_before(self):
        start, end, *_ = self._extract_call(
            {
                "extract_range_start": "2026-10-01",
                "extract_range_end": "2026-10-04",
                "extract_range_days": 365,
            },
            {},
        )
        self.assertEqual(("2026-10-01", "2026-10-04"), (start, end))

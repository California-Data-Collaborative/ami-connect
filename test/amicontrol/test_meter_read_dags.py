import unittest
from unittest.mock import MagicMock

from amiadapters.adapters.base import DEFAULT_SCHEDULE_CRONTAB, ScheduledExtract
from amicontrol.dags.meter_read_dags import (
    ami_control_dag_factory,
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


class TestMeterReadDagWiring(unittest.TestCase):
    def test_loads_run_in_series_transformed_before_raw(self):
        adapter = MagicMock()
        adapter.name.return_value = "vendor-my_org"
        dag = ami_control_dag_factory(
            dag_id="my_org-ami-meter-read-dag-test",
            schedule=None,
            params={},
            adapter=adapter,
            on_failure_sns_notifier=None,
        )
        upstream = {t.task_id: set(t.upstream_task_ids) for t in dag.tasks}
        self.assertEqual(set(), upstream["extract-vendor-my_org"])
        self.assertEqual({"extract-vendor-my_org"}, upstream["transform-vendor-my_org"])
        self.assertEqual(
            {"transform-vendor-my_org"}, upstream["load-transformed-vendor-my_org"]
        )
        self.assertEqual(
            {"load-transformed-vendor-my_org"}, upstream["load-raw-vendor-my_org"]
        )
        self.assertEqual(
            {"load-raw-vendor-my_org"}, upstream["post-process-vendor-my_org"]
        )

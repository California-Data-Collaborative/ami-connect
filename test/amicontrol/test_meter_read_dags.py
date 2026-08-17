from amiadapters.adapters.base import DEFAULT_SCHEDULE_CRONTAB, ScheduledExtract
from amicontrol.dags.meter_read_dags import staggered_schedule
from test.base_test_case import BaseTestCase


class TestStaggeredSchedule(BaseTestCase):

    def test_default_crontab_gets_hourly_slot_by_org_index(self):
        self.assertEqual("0 12 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 0))
        self.assertEqual("0 13 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 1))
        self.assertEqual("0 18 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 6))
        self.assertEqual("0 23 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 11))

    def test_slots_wrap_after_23(self):
        self.assertEqual("0 12 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 12))
        self.assertEqual("0 13 * * *", staggered_schedule(DEFAULT_SCHEDULE_CRONTAB, 13))

    def test_explicit_crontab_passes_through_unchanged(self):
        # Beacon's lagged extracts choose their own hours; they must not move.
        self.assertEqual("0 10 * * *", staggered_schedule("0 10 * * *", 3))
        self.assertEqual("0 11 * * *", staggered_schedule("0 11 * * *", 5))

    def test_scheduled_extract_default_matches_exported_constant(self):
        # staggered_schedule recognizes defaults by equality with the exported
        # constant; if the dataclass default ever diverges, staggering silently
        # stops applying.
        self.assertEqual(DEFAULT_SCHEDULE_CRONTAB, ScheduledExtract().schedule_crontab)

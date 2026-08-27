from unittest.mock import MagicMock

from amiadapters.adapters.base import (
    DEFAULT_SCHEDULE_CRONTAB,
    BaseAMIAdapter,
    ScheduledExtract,
)
from amicontrol.dags.meter_read_dags import (
    MEMORY_HEAVY_POOL,
    STAGGER_WINDOW_MINUTES,
    STAGGER_WINDOW_START_HOUR,
    ami_control_dag_factory,
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


class TestMemoryHeavyPool(BaseTestCase):
    """
    A pool only bounds tasks that name it, so a task added without `pool=`
    silently escapes the ceiling. Lock every task in the DAG to the pool.
    """

    def _build_dag(self):
        adapter = MagicMock(spec=BaseAMIAdapter)
        adapter.name.return_value = "test-adapter"
        return ami_control_dag_factory(
            dag_id="test-dag",
            schedule=DEFAULT_SCHEDULE_CRONTAB,
            params={},
            adapter=adapter,
            on_failure_sns_notifier=MagicMock(),
        )

    def test_every_task_uses_the_memory_heavy_pool(self):
        dag = self._build_dag()
        self.assertEqual(5, len(dag.tasks))
        for task in dag.tasks:
            self.assertEqual(
                MEMORY_HEAVY_POOL,
                task.pool,
                f"{task.task_id} is not in {MEMORY_HEAVY_POOL}; it would bypass the cap",
            )

    def test_pool_name_is_stable(self):
        # The pool is created by hand in the metastore before deploy. Renaming
        # it here without renaming it there sends every task to a pool that
        # does not exist.
        self.assertEqual("ami_meter_read", MEMORY_HEAVY_POOL)

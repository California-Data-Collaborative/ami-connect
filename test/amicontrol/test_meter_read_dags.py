import unittest
from unittest.mock import MagicMock

from amiadapters.adapters.base import DEFAULT_SCHEDULE_CRONTAB, ScheduledExtract
from airflow.utils.trigger_rule import TriggerRule

from amicontrol.dags.meter_read_dags import (
    ami_control_dag_factory,
    MAX_PIECE_RETRIES,
    PIECE_RUN_CONF_KEY,
    piece_run_id,
    run_pieces_in_order,
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

    def test_manual_dag_plans_then_loads_or_runs_pieces(self):
        adapter = MagicMock()
        adapter.name.return_value = "vendor-my_org"
        dag = ami_control_dag_factory(
            dag_id="my_org-ami-meter-read-dag-manual",
            schedule=None,
            params={},
            adapter=adapter,
            on_failure_sns_notifier=None,
            split_long_ranges=True,
        )
        tasks = {t.task_id: t for t in dag.tasks}
        upstream = {task_id: set(t.upstream_task_ids) for task_id, t in tasks.items()}
        self.assertEqual(set(), upstream["plan-vendor-my_org"])
        self.assertEqual({"plan-vendor-my_org"}, upstream["extract-vendor-my_org"])
        self.assertEqual({"plan-vendor-my_org"}, upstream["run-pieces-vendor-my_org"])
        self.assertEqual(
            {"load-raw-vendor-my_org", "run-pieces-vendor-my_org"},
            upstream["post-process-vendor-my_org"],
        )
        self.assertEqual(
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            tasks["post-process-vendor-my_org"].trigger_rule,
        )

    def test_scheduled_dag_has_no_plan_or_pieces(self):
        adapter = MagicMock()
        adapter.name.return_value = "vendor-my_org"
        dag = ami_control_dag_factory(
            dag_id="my_org-ami-meter-read-dag-standard",
            schedule="0 12 * * *",
            params={},
            adapter=adapter,
            on_failure_sns_notifier=None,
        )
        task_ids = {t.task_id for t in dag.tasks}
        self.assertNotIn("plan-vendor-my_org", task_ids)
        self.assertNotIn("run-pieces-vendor-my_org", task_ids)


class FakeRuns:
    """Stands in for Airflow's DagRun table: a started run finishes with its scripted outcome
    (success by default) the first time it is polled on its own."""

    def __init__(self, existing=None, outcomes=None):
        self.states = dict(existing or {})
        self.outcomes = dict(outcomes or {})
        self.started = []

    def start_run(self, dag_id, run_id, conf):
        self.started.append((run_id, conf))
        self.states[run_id] = "running"

    def find_runs(self, dag_id, run_ids):
        if len(run_ids) == 1 and self.states.get(run_ids[0]) == "running":
            self.states[run_ids[0]] = self.outcomes.get(run_ids[0], "success")
        return {r: self.states[r] for r in run_ids if r in self.states}


class TestRunPiecesInOrder(unittest.TestCase):
    PIECES = [
        ("2026-01-01T00:00:00", "2026-01-15T00:00:00"),
        ("2026-01-15T00:00:00", "2026-01-29T00:00:00"),
        ("2026-01-29T00:00:00", "2026-02-01T00:00:00"),
    ]

    def _run(self, runs):
        run_pieces_in_order(
            dag_id="d",
            parent_run_id="manual__p",
            pieces=self.PIECES,
            start_run=runs.start_run,
            find_runs=runs.find_runs,
            sleep=lambda seconds: None,
        )

    def test_piece_run_id(self):
        self.assertEqual("manual__p__piece_02_of_13", piece_run_id("manual__p", 2, 13))
        self.assertEqual(
            "manual__p__piece_02_of_13__retry_1", piece_run_id("manual__p", 2, 13, 1)
        )

    def test_starts_each_piece_once_oldest_first_with_its_range(self):
        runs = FakeRuns()
        self._run(runs)
        self.assertEqual(
            [piece_run_id("manual__p", i, 3) for i in (1, 2, 3)],
            [run_id for run_id, _ in runs.started],
        )
        first_conf = runs.started[0][1]
        self.assertEqual("2026-01-01T00:00:00", first_conf["extract_range_start"])
        self.assertEqual("2026-01-15T00:00:00", first_conf["extract_range_end"])
        self.assertTrue(first_conf[PIECE_RUN_CONF_KEY])

    def test_stops_at_a_failed_piece(self):
        runs = FakeRuns(outcomes={piece_run_id("manual__p", 2, 3): "failed"})
        with self.assertRaises(Exception) as ctx:
            self._run(runs)
        self.assertEqual(2, len(runs.started))
        self.assertIn("Piece 2 of 3", str(ctx.exception))

    def test_resume_skips_loaded_pieces_and_retries_the_failed_one(self):
        runs = FakeRuns(
            existing={
                piece_run_id("manual__p", 1, 3): "success",
                piece_run_id("manual__p", 2, 3): "failed",
            }
        )
        self._run(runs)
        self.assertEqual(
            [piece_run_id("manual__p", 2, 3, 1), piece_run_id("manual__p", 3, 3)],
            [run_id for run_id, _ in runs.started],
        )

    def test_waits_on_a_piece_that_is_already_running_instead_of_starting_it_again(
        self,
    ):
        runs = FakeRuns(existing={piece_run_id("manual__p", 1, 3): "running"})
        self._run(runs)
        self.assertEqual(
            [piece_run_id("manual__p", i, 3) for i in (2, 3)],
            [run_id for run_id, _ in runs.started],
        )

    def test_gives_up_on_a_piece_that_keeps_failing(self):
        existing = {
            piece_run_id("manual__p", 1, 3, attempt): "failed"
            for attempt in range(MAX_PIECE_RETRIES + 1)
        }
        runs = FakeRuns(existing=existing)
        with self.assertRaises(Exception):
            self._run(runs)
        self.assertEqual([], runs.started)

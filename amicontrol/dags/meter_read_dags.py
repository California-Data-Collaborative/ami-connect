from datetime import datetime, timedelta
import logging
import time
from typing import Callable, Dict, List, Tuple

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.notifications.basenotifier import BaseNotifier
from airflow.utils.trigger_rule import TriggerRule

from amiadapters.adapters.base import DEFAULT_SCHEDULE_CRONTAB, BaseAMIAdapter

# Window over which default-scheduled extracts are spread, divided evenly
# by the number of configured orgs. Sized to give ~30-minute spacing at
# nine orgs — enough that one org's memory-heavy extract+transform phase
# finishes before the next org's begins (observed heavy phases run up to
# ~23 minutes, except two extract-bound orgs whose multi-hour vendor pulls
# no spacing can serialize). Spacing tightens as orgs are added; widen the
# window when it gets too tight for the observed heavy-phase durations.
STAGGER_WINDOW_START_HOUR = 12
STAGGER_WINDOW_MINUTES = 270

# A manual run whose range is longer than the source's max_extract_days loads it as
# pieces: one run of the same manual DAG per piece, oldest first, one at a time. This
# dag_run.conf key marks those piece runs, so they skip post processing; the run that
# started them post processes the whole range once, after the last piece has loaded.
PIECE_RUN_CONF_KEY = "piece_of_longer_run"
PIECE_POLL_SECONDS = 60
# How many times one piece may be started again by clearing the parent's run-pieces task.
MAX_PIECE_RETRIES = 20

logger = logging.getLogger(__name__)


def staggered_schedule(schedule_crontab: str, org_index: int, org_count: int) -> str:
    """
    Spread default-scheduled extracts evenly across a fixed daily window.

    Extracts that inherit ScheduledExtract's default crontab would otherwise
    all start at exactly 12:00 UTC, running every org's memory-heavy extract
    and transform tasks simultaneously and letting them compete for the
    host's memory. Each org gets an evenly spaced start time in the window
    by its position in the sorted org list, and the spacing adapts as orgs
    are added or removed. Explicitly configured crontabs (e.g. Beacon's
    lagged extracts) pass through unchanged.
    """
    if schedule_crontab != DEFAULT_SCHEDULE_CRONTAB:
        return schedule_crontab
    offset_minutes = (STAGGER_WINDOW_MINUTES // max(org_count, 1)) * org_index
    hour = (STAGGER_WINDOW_START_HOUR + offset_minutes // 60) % 24
    minute = offset_minutes % 60
    return f"{minute} {hour} * * *"


def ami_control_dag_factory(
    dag_id: str,
    schedule: str,
    params: dict,
    adapter: BaseAMIAdapter,
    on_failure_sns_notifier: BaseNotifier,
    interval=timedelta(days=2),
    lag=timedelta(days=0),
    backfill_params=None,
    split_long_ranges=False,
):
    """
    Factory for AMI control meter read DAGs that run on different schedules:
    - The regular run, which refreshes recent data
    - The backfill runs, which run more frequently and attempt to backfill data
    - Manual runs whose range can be parameterized in the Airflow UI

    split_long_ranges (manual DAGs): a range longer than the source's max_extract_days
    is not extracted by the run itself. Its plan task sends it to run-pieces, which starts
    one run of this same DAG per piece, oldest first, waits for each to finish, and stops
    at the first failure. The run then post processes the whole range once. Clearing
    run-pieces after a failure resumes at the failed piece; finished pieces are skipped.
    """

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        params=params,
        catchup=False,
        start_date=datetime(2024, 1, 1),
        tags=["ami"],
        default_args={
            "on_failure_callback": on_failure_sns_notifier,
        },
    )
    def ami_control_dag():
        plan_task_id = f"plan-{adapter.name()}"
        extract_task_id = f"extract-{adapter.name()}"
        run_pieces_task_id = f"run-pieces-{adapter.name()}"

        @task.branch()
        def plan(**context):
            start, end = _calculate_extract_range(adapter, context, interval, lag)
            pieces = adapter.split_extract_range(start, end)
            if len(pieces) == 1:
                return extract_task_id
            logger.info(
                f"Range {start} -> {end} is longer than {adapter.max_extract_days} days; "
                f"loading it as {len(pieces)} pieces, oldest first"
            )
            context["ti"].xcom_push(
                key="pieces",
                value=[[s.isoformat(), e.isoformat()] for s, e in pieces],
            )
            return run_pieces_task_id

        @task()
        def run_pieces(**context):
            pieces = context["ti"].xcom_pull(task_ids=plan_task_id, key="pieces")
            run_pieces_in_order(
                dag_id=context["dag"].dag_id,
                parent_run_id=context["dag_run"].run_id,
                pieces=[(s, e) for s, e in pieces],
                start_run=_trigger_piece_run,
                find_runs=_find_piece_runs,
            )

        @task()
        def extract(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            start, end = _calculate_extract_range(adapter, context, interval, lag)
            adapter.extract_and_output(run_id, start, end)

        @task()
        def transform(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.transform_and_output(run_id)

        @task()
        def load_raw(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.load_raw(run_id)

        @task()
        def load_transformed(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.load_transformed(run_id)

        @task()
        def post_process(**context):
            dag_run = context["dag_run"]
            if (dag_run.conf or {}).get(PIECE_RUN_CONF_KEY):
                raise AirflowSkipException(
                    "Piece of a longer manual run: the run that started it post processes "
                    "the whole range after the last piece has loaded."
                )
            start, end = _calculate_extract_range(adapter, context, interval, lag)
            loaded_in_pieces = split_long_ranges and bool(
                context["ti"].xcom_pull(task_ids=plan_task_id, key="pieces")
            )
            adapter.post_process(
                dag_run.run_id, start, end, cover_extract_range=loaded_in_pieces
            )

        # Set sequence of tasks for this utility
        extract_step = extract.override(task_id=extract_task_id)(adapter)
        post_process_step = post_process.override(
            task_id=f"post-process-{adapter.name()}",
            # With split_long_ranges, exactly one of load-raw and run-pieces runs and the
            # other is skipped by plan; post process follows whichever ran, if it succeeded.
            trigger_rule=(
                TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
                if split_long_ranges
                else TriggerRule.ALL_SUCCESS
            ),
        )()
        if split_long_ranges:
            run_pieces_step = run_pieces.override(task_id=run_pieces_task_id)()
            plan.override(task_id=plan_task_id)() >> [extract_step, run_pieces_step]
            run_pieces_step >> post_process_step
        (
            extract_step
            >> transform.override(task_id=f"transform-{adapter.name()}")(adapter)
            # The two loads run one after the other, not in parallel: each one
            # re-materializes the run's full payload from the intermediate outputs,
            # and two copies at once is what exhausted the Airflow host on
            # 2026-08-25 and 2026-09-22. Transformed data first, so readings and
            # meters land even if the raw archive load fails.
            >> load_transformed.override(task_id=f"load-transformed-{adapter.name()}")(
                adapter
            )
            >> load_raw.override(task_id=f"load-raw-{adapter.name()}")(adapter)
            >> post_process_step
        )

        def _calculate_extract_range(
            adapter: BaseAMIAdapter,
            context: dict,
            interval: timedelta,
            lag: timedelta,
        ) -> tuple[datetime, datetime]:
            """
            Given the DAG's inputs, figure out the start and end range for the pipeline's extract.
            Could come from DAG params, from backfill configuration, or could rely on default values.
            """
            # start and end dates from Airflow UI, if specified
            start_from_params = context["params"].get("extract_range_start")
            end_from_params = context["params"].get("extract_range_end")
            return adapter.calculate_extract_range(
                start_from_params,
                end_from_params,
                interval,
                lag,
                backfill_params=backfill_params,
            )

    return ami_control_dag()


def piece_run_id(parent_run_id: str, index: int, count: int, attempt: int = 0) -> str:
    """Run id of piece `index` (1-based) of `count`; attempt > 0 for a piece started again."""
    run_id = f"{parent_run_id}__piece_{index:02d}_of_{count:02d}"
    return run_id if attempt == 0 else f"{run_id}__retry_{attempt}"


def run_pieces_in_order(
    dag_id: str,
    parent_run_id: str,
    pieces: List[Tuple[str, str]],
    start_run: Callable[[str, str, dict], None],
    find_runs: Callable[[str, List[str]], Dict[str, str]],
    sleep: Callable[[float], None] = time.sleep,
    poll_seconds: float = PIECE_POLL_SECONDS,
):
    """
    Load the pieces of a long manual run one at a time, oldest first, each as its own run
    of the same DAG. Waits for each piece's run to finish before starting the next and
    stops at the first piece that does not succeed, so no newer piece loads ahead of it.

    Safe to call again after a failure (clear the run-pieces task): pieces with a
    successful run are skipped, a piece whose run is still active is waited on rather
    than started twice, and a failed piece is started again under a __retry_N run id.

    start_run(dag_id, run_id, conf) starts a run; find_runs(dag_id, run_ids) returns
    {run_id: state} for the runs among run_ids that exist. Both are passed in so this
    logic is testable without an Airflow database.
    """
    count = len(pieces)
    for index, (piece_start, piece_end) in enumerate(pieces, start=1):
        attempts = [
            piece_run_id(parent_run_id, index, count, attempt)
            for attempt in range(MAX_PIECE_RETRIES + 1)
        ]
        existing = find_runs(dag_id, attempts)
        if "success" in existing.values():
            logger.info(f"Piece {index} of {count} already loaded; skipping")
            continue
        active = [r for r in attempts if existing.get(r) in ("queued", "running")]
        if active:
            run_id = active[0]
            logger.info(f"Piece {index} of {count} is already running as {run_id}")
        else:
            unused = [r for r in attempts if r not in existing]
            if not unused:
                raise Exception(
                    f"Piece {index} of {count} ({piece_start} -> {piece_end}) has failed "
                    f"{len(attempts)} times; not starting it again."
                )
            run_id = unused[0]
            logger.info(
                f"Starting piece {index} of {count}: {piece_start} -> {piece_end} as {run_id}"
            )
            start_run(
                dag_id,
                run_id,
                {
                    "extract_range_start": piece_start,
                    "extract_range_end": piece_end,
                    PIECE_RUN_CONF_KEY: True,
                },
            )
        while True:
            state = find_runs(dag_id, [run_id]).get(run_id)
            if state is None:
                raise Exception(
                    f"Run {run_id} for piece {index} of {count} no longer exists; "
                    f"clear this task to start that piece again."
                )
            if state in ("success", "failed"):
                break
            sleep(poll_seconds)
        if state != "success":
            raise Exception(
                f"Piece {index} of {count} ({piece_start} -> {piece_end}) failed in run "
                f"{run_id}; later pieces were not started. Fix the cause, then clear this "
                f"task to start again from piece {index}."
            )
        logger.info(f"Piece {index} of {count} loaded")


def _trigger_piece_run(dag_id: str, run_id: str, conf: dict):
    from airflow.api.common.trigger_dag import trigger_dag

    trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf, replace_microseconds=False)


def _find_piece_runs(dag_id: str, run_ids: List[str]) -> Dict[str, str]:
    from airflow.models import DagRun

    return {
        dag_run.run_id: dag_run.state
        for dag_run in DagRun.find(dag_id=dag_id, run_id=run_ids)
    }

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.notifications.basenotifier import BaseNotifier

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

# Airflow pool that serializes the meter-read tasks. The extract, transform
# and load tasks each hold a full batch of readings in memory and the host is
# 32 GB with no swap, so more than one at a time can exhaust it. post_process
# does its work server-side in Snowflake but is pooled too: that keeps runs of
# the same org from interleaving DELETE+INSERT on the per-org result tables.
# Unlike the staggered schedules, which only spread start times, a pool bounds
# what is in flight.
#
# Two operational facts, verified against Airflow 2.10.5: the pool must
# already exist in the metastore when this code deploys (Airflow does not
# create it, and tasks naming a missing pool are never scheduled — no
# failure and no alert, only a warning on the DAG's Grid view),
# and the pool binds a task instance when its row is created — dag runs
# already open at deploy time keep default_pool for their lifetime, including
# through orphan adoption on a scheduler restart. Deploy with nothing in
# flight. See the deploy note in the pull request.
MEMORY_HEAVY_POOL = "ami_meter_read"


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
):
    """
    Factory for AMI control meter read DAGs that run on different schedules:
    - The regular run, which refreshes recent data
    - The backfill runs, which run more frequently and attempt to backfill data
    - Manual runs whose range can be parameterized in the Airflow UI
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

        @task(pool=MEMORY_HEAVY_POOL)
        def extract(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            start, end = _calculate_extract_range(adapter, context, interval, lag)
            adapter.extract_and_output(run_id, start, end)

        @task(pool=MEMORY_HEAVY_POOL)
        def transform(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.transform_and_output(run_id)

        @task(pool=MEMORY_HEAVY_POOL)
        def load_raw(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.load_raw(run_id)

        @task(pool=MEMORY_HEAVY_POOL)
        def load_transformed(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.load_transformed(run_id)

        @task(pool=MEMORY_HEAVY_POOL)
        def post_process(**context):
            run_id = context["dag_run"].run_id
            start, end = _calculate_extract_range(adapter, context, interval, lag)
            adapter.post_process(run_id, start, end)

        # Set sequence of tasks for this utility
        (
            extract.override(task_id=f"extract-{adapter.name()}")(adapter)
            >> transform.override(task_id=f"transform-{adapter.name()}")(adapter)
            >> [
                # Structured to run in parallel; the 1-slot pool serializes
                # them in practice (each re-materializes the full payload, so
                # their peaks add — a larger pool would let them overlap).
                load_raw.override(task_id=f"load-raw-{adapter.name()}")(adapter),
                load_transformed.override(task_id=f"load-transformed-{adapter.name()}")(
                    adapter
                ),
            ]
            >> post_process.override(task_id=f"post-process-{adapter.name()}")()
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

    # Returned so tests can assert the pool is set on every task. The DAG is
    # registered by the decorator on call, as before; returning it changes
    # nothing about discovery.
    return ami_control_dag()

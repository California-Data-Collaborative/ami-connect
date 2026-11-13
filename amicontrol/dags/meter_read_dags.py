from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
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


def manual_run_params(max_extract_days: int) -> dict:
    """
    Inputs of a source's manual run DAG: a start date and a number of days, at most the
    source's max_extract_days. Airflow checks them before it creates a run, so a request
    for more days is rejected at the trigger form (or the API or CLI trigger) and no run starts.
    """
    return {
        "extract_range_start": Param(
            "",
            type="string",
            description="First day to extract, YYYY-MM-DD.",
        ),
        "extract_range_days": Param(
            1,
            type="integer",
            minimum=1,
            maximum=max_extract_days,
            description=(
                f"Number of days to extract from the start date, at most {max_extract_days} "
                f"for this source (its max_extract_days setting). For a longer range, run "
                f"consecutive manual runs, starting each after the previous one finishes."
            ),
        ),
    }


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

    # Only a manual DAG (built with manual_run_params) takes a start date and a number of
    # days; every other DAG reads its range exactly as before.
    takes_days = "extract_range_days" in (params or {})

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
            run_id = context["dag_run"].run_id
            start, end = _calculate_extract_range(adapter, context, interval, lag)
            adapter.post_process(run_id, start, end)

        # Set sequence of tasks for this utility
        (
            extract.override(task_id=f"extract-{adapter.name()}")(adapter)
            >> transform.override(task_id=f"transform-{adapter.name()}")(adapter)
            >> [
                # Run load tasks in parallel
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
            if takes_days:
                # Manual run: a start date and a number of days, checked by Airflow against
                # the source's max_extract_days. Without a start date (possible only through
                # the API), the most recent that many days.
                start = context["params"].get("extract_range_start") or None
                days = timedelta(days=int(context["params"]["extract_range_days"]))
                if start is None:
                    return adapter.calculate_extract_range(None, None, days, lag)
                start = datetime.fromisoformat(str(start).strip())
                return adapter.calculate_extract_range(start, start + days, days, lag)
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

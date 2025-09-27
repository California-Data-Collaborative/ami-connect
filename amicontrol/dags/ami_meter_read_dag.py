from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.config import (
    AMIAdapterConfiguration,
    find_secrets_yaml,
)


def ami_control_dag_factory(
    dag_id,
    schedule,
    params,
    adapter,
    on_failure_sns_notifier,
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
        on_failure_callback=on_failure_sns_notifier,
    )
    def ami_control_dag():

        @task()
        def extract(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id

            # start and end dates from Airflow UI, if specified
            start_from_params = context["params"].get("extract_range_start")
            end_from_params = context["params"].get("extract_range_end")

            start, end = adapter.calculate_extract_range(
                start_from_params, end_from_params, backfill_params=backfill_params
            )
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
            adapter.post_process(run_id)

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

    ami_control_dag()


#######################################################
# Configure DAGs
#######################################################
config = AMIAdapterConfiguration.from_database(find_secrets_yaml())
utility_adapters = config.adapters()
backfills = config.backfills()
on_failure_sns_notifier = config.on_failure_sns_notifier()

# Create DAGs for each configured utility
for adapter in utility_adapters:
    # Manual runs
    standard_params = {
        "extract_range_start": Param(
            type="string",
            description="Start of date range for which we'll extract meter read data",
            default="",
        ),
        "extract_range_end": Param(
            type="string",
            description="End of date range for which we'll extract meter read data",
            default="",
        ),
    }
    ami_control_dag_factory(
        f"{adapter.org_id}-ami-meter-read-dag-manual",
        None,
        standard_params,
        adapter,
        on_failure_sns_notifier,
    )

    # Standard run that fetches most recent meter read data
    ami_control_dag_factory(
        f"{adapter.org_id}-ami-meter-read-dag-standard",
        "0 12 * * *",
        {},
        adapter,
        on_failure_sns_notifier,
    )

# Create DAGs for configured backfill runs
for backfill in backfills:
    matching_adapters = [a for a in utility_adapters if a.org_id == backfill.org_id]
    if len(matching_adapters) != 1:
        continue
    ami_control_dag_factory(
        f"{backfill.org_id}-ami-meter-read-dag-backfill-{datetime.strftime(backfill.start_date, "%Y-%m-%d")}-{datetime.strftime(backfill.end_date, "%Y-%m-%d")}",
        backfill.schedule,
        {},
        matching_adapters[0],
        on_failure_sns_notifier,
        backfill_params=backfill,
    )

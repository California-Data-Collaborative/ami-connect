from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from amiadapters.base import BaseAMIAdapter, default_date_range
from amiadapters.config import (
    AMIAdapterConfiguration,
    find_config_yaml,
    find_secrets_yaml,
)


def ami_control_dag_factory(
    dag_id,
    schedule,
    params,
    adapters,
    min_date=None,
    max_date=None,
    interval_days=None,
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
    )
    def ami_control_dag():

        @task()
        def extract(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id

            if min_date and max_date and interval_days:
                range = adapter.calculate_backfill_range(
                    min_date, max_date, interval_days
                )
                if range is None:
                    raise Exception(
                        f"Backfill with min_date={min_date} max_date={max_date} is finished, consider removing it from config"
                    )
                start, end = range
            else:
                start, end = (
                    context["params"].get("extract_range_start"),
                    context["params"].get("extract_range_end"),
                )

                if isinstance(start, str):
                    start = datetime.fromisoformat(start)
                if isinstance(end, str):
                    end = datetime.fromisoformat(end)

                if start is None or end is None:
                    start, end = default_date_range(start, end)

            adapter.extract(run_id, start, end)

        @task()
        def transform(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.transform(run_id)

        @task()
        def load_raw(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.load_raw(run_id)

        @task()
        def load_transformed(adapter: BaseAMIAdapter, **context):
            run_id = context["dag_run"].run_id
            adapter.load_transformed(run_id)

        @task()
        def final_task():
            # Placeholder to gather results of parallel tasks in DAG
            return

        for adapter in adapters:
            # Set sequence of tasks for this utility
            (
                extract.override(task_id=f"extract-{adapter.name()}")(adapter)
                >> transform.override(task_id=f"transform-{adapter.name()}")(adapter)
                >> [
                    # Run load tasks in parallel
                    load_raw.override(task_id=f"load-raw-{adapter.name()}")(adapter),
                    load_transformed.override(
                        task_id=f"load-transformed-{adapter.name()}"
                    )(adapter),
                ]
                >> final_task.override(task_id=f"finish-{adapter.name()}")()
            )

    ami_control_dag()


#######################################################
# Configure DAGs
#######################################################
config = AMIAdapterConfiguration.from_yaml(find_config_yaml(), find_secrets_yaml())
adapters = config.adapters()
backfills = config.backfills()

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
ami_control_dag_factory("ami-meter-read-dag-manual", None, standard_params, adapters)

# Standard run that fetches most recent meter read data
ami_control_dag_factory("ami-meter-read-dag-standard", "0 12 * * *", {}, adapters)

# Backfill runs
for backfill in backfills:
    matching_adapters = [a for a in adapters if a.org_id == backfill.org_id]
    if len(matching_adapters) != 1:
        continue
    ami_control_dag_factory(
        f"ami-meter-read-dag-backfill-{backfill.org_id}-{datetime.strftime(backfill.start_date, "%Y-%m-%d")}-{datetime.strftime(backfill.end_date, "%Y-%m-%d")}",
        backfill.schedule,
        {},
        matching_adapters,
        min_date=backfill.start_date,
        max_date=backfill.end_date,
        interval_days=backfill.interval_days,
    )

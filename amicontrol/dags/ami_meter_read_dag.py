from datetime import datetime
import os
import pathlib

from airflow.decorators import dag, task
from airflow.models.param import Param

from amiadapters.base import BaseAMIAdapter, default_date_range
from amiadapters.config import AMIAdapterConfiguration


def ami_control_dag_factory(dag_id, schedule, params, is_backfill=False):
    """
    Factory for AMI control meter read DAGs that run on different schedules:
    - The regular run, which refreshes recent data
    - The backfill run, which runs more frequently and attempts to backfill data
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
            if is_backfill:
                start, end = adapter.calculate_backfill_range()
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

            adapter.extract(start, end)

        @task()
        def transform(adapter: BaseAMIAdapter):
            adapter.transform()

        @task()
        def load_raw(adapter: BaseAMIAdapter):
            adapter.load_raw()

        @task()
        def load_transformed(adapter: BaseAMIAdapter):
            adapter.load_transformed()

        @task()
        def final_task():
            # Placeholder to gather results of parallel tasks in DAG
            return

        config = AMIAdapterConfiguration.from_yaml(
            os.environ.get(
                "AMI_CONFIG_YAML",
                pathlib.Path(__file__).joinpath("..", "..", "config.yaml").resolve(),
            ),
            os.environ.get(
                "AMI_SECRET_YAML",
                pathlib.Path(__file__).joinpath("..", "..", "secrets.yaml").resolve(),
            ),
        )

        adapters = config.adapters()

        for adapter in adapters:
            # Set sequence of tasks for this utility
            extract.override(task_id=f"extract-{adapter.name()}")(adapter) >> \
            transform.override(task_id=f"transform-{adapter.name()}")(adapter) >> \
            [
                # Run load tasks in parallel
                load_raw.override(task_id=f"load-raw-{adapter.name()}")(adapter),
                load_transformed.override(task_id=f"load-transformed-{adapter.name()}")(adapter)
            ] >> \
            final_task.override(task_id=f"finish-{adapter.name()}")()


    ami_control_dag()


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
    )
}
ami_control_dag_factory("ami-meter-read-dag-manual", None, standard_params)

# Standard run that fetches most recent meter read data
ami_control_dag_factory("ami-meter-read-dag-standard", "0 12 * * *", {})

# Backfill run
ami_control_dag_factory("ami-meter-read-dag-backfill", "45 * * * *", {}, is_backfill=True)

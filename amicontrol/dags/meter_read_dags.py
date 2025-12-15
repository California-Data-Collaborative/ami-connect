from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.notifications.basenotifier import BaseNotifier

from amiadapters.adapters.base import BaseAMIAdapter


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

    ami_control_dag()

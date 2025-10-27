"""
Configure all DAGs in this file so that we can reuse results of
SQL queries to get configuration.
"""

from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.models.param import Param

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.configuration.env import set_global_aws_region
from amiadapters.storage.base import BaseAMIDataQualityCheck

logger = logging.getLogger(__name__)


def ami_control_dag_factory(
    dag_id,
    schedule,
    params,
    adapter,
    on_failure_sns_notifier,
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
        on_failure_callback=on_failure_sns_notifier,
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


# Load configuration. By default, Airflow calls this twice for DAG refreshes
# every min_file_process_interval = 30 seconds: Once for the scheduler, once for the webserver.
# We configure all DAGs in this file to limit the number of config loads every DAG refresh.
set_global_aws_region("us-west-2")
config = AMIAdapterConfiguration.from_database()

#######################################################
# Configure AMI ETL DAGs
#######################################################
utility_adapters = config.adapters()
backfills = config.backfills()
on_failure_sns_notifier = config.on_failure_sns_notifier()

# Create DAGs for each configured utility
for adapter in utility_adapters:
    # Manual runs
    user_provided_params = {
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
        user_provided_params,
        adapter,
        on_failure_sns_notifier,
    )

    # Scheduled runs
    for scheduled_extract in adapter.scheduled_extracts():
        ami_control_dag_factory(
            f"{adapter.org_id}-ami-meter-read-dag-{scheduled_extract.name}",
            schedule=scheduled_extract.schedule_crontab,
            interval=scheduled_extract.interval,
            lag=scheduled_extract.lag,
            params={},
            adapter=adapter,
            on_failure_sns_notifier=on_failure_sns_notifier,
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

#######################################################
# Configure data quality check DAG
#######################################################
checks = [check for storage_sink in config.sinks() for check in storage_sink.checks()]
if checks:

    @dag(
        dag_id="ami_data_quality_checks_dag",
        schedule="0 4 * * *",
        catchup=False,
        start_date=datetime(2024, 1, 1),
        tags=["ami"],
        on_failure_callback=on_failure_sns_notifier,
    )
    def ami_data_quality_checks_dag():

        @task()
        def run_check(data_quality_check: BaseAMIDataQualityCheck):
            check_passed = data_quality_check.check()
            if not check_passed:
                if data_quality_check.notify_on_failure():
                    raise Exception(f"Check {data_quality_check.name()} did not pass")
                else:
                    logger.info(f"Check {data_quality_check.name()} did not pass")

        for data_quality_check in checks:
            run_check.override(task_id=f"{data_quality_check.name()}")(
                data_quality_check
            )

    ami_data_quality_checks_dag()

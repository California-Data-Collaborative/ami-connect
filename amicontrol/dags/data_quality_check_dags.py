from datetime import datetime
import logging

from airflow.decorators import dag, task
from airflow.notifications.basenotifier import BaseNotifier

from amiadapters.storage.base import BaseAMIDataQualityCheck

logger = logging.getLogger(__name__)


def data_quality_check_dag_factory(
    checks: list[BaseAMIDataQualityCheck],
    schedule: str,
    on_failure_sns_notifier: BaseNotifier,
):
    """
    Factory for AMI data quality check DAG. Turns configured data quality checks
    into Airflow tasks.
    """

    @dag(
        dag_id="ami_data_quality_checks_dag",
        schedule=schedule,
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

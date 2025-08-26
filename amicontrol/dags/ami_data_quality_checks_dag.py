from datetime import datetime
import logging

from airflow.decorators import dag, task

from amiadapters.config import (
    AMIAdapterConfiguration,
    find_config_yaml,
    find_secrets_yaml,
)

from amiadapters.storage.base import BaseAMIDataQualityCheck

logger = logging.getLogger(__name__)

config = AMIAdapterConfiguration.from_yaml(find_config_yaml(), find_secrets_yaml())
on_failure_sns_notifier = config.on_failure_sns_notifier()

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

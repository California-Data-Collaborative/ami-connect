"""
Configure all DAGs in this file so that we can reuse results of
SQL queries to get configuration.
"""

from datetime import datetime
import logging

from airflow.models.param import Param

from amiadapters.config import AMIAdapterConfiguration
from amiadapters.configuration.env import set_global_aws_region
from amicontrol.dags.administration_dags import log_cleanup_dag_factory
from amicontrol.dags.data_quality_check_dags import data_quality_check_dag_factory
from amicontrol.dags.meter_read_dags import ami_control_dag_factory

logger = logging.getLogger(__name__)


# Load configuration. By default, Airflow calls this twice for DAG refreshes
# every min_file_process_interval = 30 seconds: Once for the scheduler, once for the webserver.
# We configure all DAGs in this file to limit the number of config loads every DAG refresh.
set_global_aws_region("us-west-2")
config = AMIAdapterConfiguration.from_database()

#######################################################
# AMI Meter Read ETL DAGs
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
# Data quality check DAG
#######################################################
checks = [check for storage_sink in config.sinks() for check in storage_sink.checks()]
if checks:
    data_quality_check_dag_factory(
        checks,
        schedule="0 4 * * *",
        on_failure_sns_notifier=on_failure_sns_notifier,
    )

#######################################################
# Log cleanup DAG
#######################################################
log_cleanup_dag_factory(
    schedule="0 1 * * *",
    on_failure_sns_notifier=on_failure_sns_notifier,
)

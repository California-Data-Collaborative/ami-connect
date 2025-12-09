"""
Configure all DAGs in this file so that we can reuse results of
SQL queries to get configuration.
"""

from datetime import datetime, timedelta
import logging
import subprocess

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import DAG, Variable
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

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

from airflow.utils.context import Context
def notify_sns_on_failure(context: Context):
    ti = context["task_instance"]
    dag = context["dag"]

    # Airflow UI links
    log_url = ti.log_url
    dag_url = f"{context['conf'].get('webserver', {}).get('BASE_URL', '')}/dags/{dag.dag_id}"

    exception = context.get("exception")
    exception_msg = str(exception) if exception else "Unknown error"

    message = f"""
🚨 Airflow DAG Failure 🚨

DAG: {dag.dag_id}
Task: {ti.task_id}
Run ID: {ti.run_id}

Error:
{exception_msg}

Logs:
{log_url}

DAG:
{dag_url}
""".strip()
    import boto3
    sns = boto3.client("sns")
    sns.publish(
        TopicArn=config._notifications.on_failure_sns_arn,
        Subject=f"Airflow failure: {dag.dag_id}.{ti.task_id}",
        Message=message,
    )

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
        notify_sns_on_failure,
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
            on_failure_sns_notifier=notify_sns_on_failure,
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
        notify_sns_on_failure,
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

#######################################################
# Configure log cleanup DAG
#######################################################
"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

Source: https://github.com/teamclairvoyant/airflow-maintenance-dags/blob/master/log-cleanup/airflow-log-cleanup.py

airflow trigger_dag --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup

--conf options:
    maxLogAgeInDays:<INT> - Optional

"""
BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER")
DEFAULT_MAX_LOG_AGE_IN_DAYS = 60  # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older
SCHEDULER_MAX_LOG_AGE_IN_DAYS = 14  # Length to retain the scheduler log files
ENABLE_DELETE = True  # Whether the job should delete the logs or not. Included if you want to temporarily avoid deleting the logs
NUMBER_OF_WORKERS = 1  # The number of worker nodes you have in Airflow. Will attempt to run this process for however many workers there are so that each worker gets its logs cleared.


@dag(
    dag_id="airflow-log-cleanup",
    schedule="0 1 * * *",
    catchup=False,
    start_date=datetime(2024, 1, 1),
    tags=["ami"],
    on_failure_callback=on_failure_sns_notifier,
)
def log_cleanup_dag():

    log_cleanup_str = (
        """
        echo "Getting Configurations..."
        BASE_LOG_FOLDER='"""
        + BASE_LOG_FOLDER
        + """'
        MAX_LOG_AGE_IN_DAYS='"""
        + str(DEFAULT_MAX_LOG_AGE_IN_DAYS)
        + """'
        ENABLE_DELETE="""
        + str("true" if ENABLE_DELETE else "false")
        + """
        echo "Finished Getting Configurations"
        echo ""

        echo "Configurations:"
        echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
        echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
        echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"
        echo ""

        echo "Running Cleanup Process..."
        FIND_STATEMENT="find ${BASE_LOG_FOLDER}/{log_type}/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
        FIND_EMPTY_DIR_STATEMENT="find ${BASE_LOG_FOLDER}/ -empty -type d"
        echo "Executing Find Statement: ${FIND_STATEMENT}"
        FILES_MARKED_FOR_DELETE=`eval ${FIND_STATEMENT}`
        echo "Process will be Deleting the following directories:"
        echo "${FILES_MARKED_FOR_DELETE}"
        echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | grep -v '^$' | wc -l ` file(s)"     # "grep -v '^$'" - removes empty lines. "wc -l" - Counts the number of lines
        echo ""

        if [ "${ENABLE_DELETE}" == "true" ];
        then
            DELETE_STMT="${FIND_STATEMENT} -delete"
            echo "Executing Delete Statement: ${DELETE_STMT}"
            eval ${DELETE_STMT}
            DELETE_STMT_EXIT_CODE=$?
            if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
                echo "Delete process failed with exit code '${DELETE_STMT_EXIT_CODE}'"
                exit ${DELETE_STMT_EXIT_CODE}
            fi
            DELETE_STMT="${FIND_EMPTY_DIR_STATEMENT} -delete"
            echo "Executing Delete Empty Log Directories Statement: ${DELETE_STMT}"
            eval ${DELETE_STMT}
            DELETE_STMT_EXIT_CODE=$?
            if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
                echo "Delete Empty Log Directories process failed with exit code '${DELETE_STMT_EXIT_CODE}'"
                exit ${DELETE_STMT_EXIT_CODE}
            fi

        else
            echo "WARN: You're opted to skip deleting the files!!!"
        fi
        echo "Finished Running Cleanup Process"
    """
    )

    @task()
    def log_cleanup():
        _run_bash_task(log_cleanup_str.replace("{log_type}", "*"))

    @task()
    def scheduler_log_cleanup():
        _run_bash_task(log_cleanup_str.replace("{log_type}", "scheduler"))

    def _run_bash_task(bash_command: str):
        process = subprocess.Popen(
            ["bash", "-c", bash_command],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for line in process.stdout:
            logger.info(line.strip())

        returncode = process.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, process.args)

    log_cleanup() >> scheduler_log_cleanup()


log_cleanup_dag()

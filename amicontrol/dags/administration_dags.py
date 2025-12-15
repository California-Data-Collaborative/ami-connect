from datetime import datetime
import logging
import subprocess

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.notifications.basenotifier import BaseNotifier

logger = logging.getLogger(__name__)


def log_cleanup_dag_factory(
    schedule: str,
    on_failure_sns_notifier: BaseNotifier,
):
    """
    A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

    Source: https://github.com/teamclairvoyant/airflow-maintenance-dags/blob/master/log-cleanup/airflow-log-cleanup.py

    airflow trigger_dag --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup

    --conf options:
        maxLogAgeInDays:<INT> - Optional

    """
    BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER")
    DEFAULT_MAX_LOG_AGE_IN_DAYS = 60  # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older
    ENABLE_DELETE = True  # Whether the job should delete the logs or not. Included if you want to temporarily avoid deleting the logs

    @dag(
        dag_id="airflow-log-cleanup",
        schedule=schedule,
        catchup=False,
        start_date=datetime(2024, 1, 1),
        tags=["ami"],
        on_failure_callback=on_failure_sns_notifier,
    )
    def log_cleanup_dag():

        log_cleanup_str = (
            """
            echo "Getting Configurations..."
            echo "THIS IS A TEST!"
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

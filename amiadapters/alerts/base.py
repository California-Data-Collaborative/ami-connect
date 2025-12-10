import traceback

from airflow.notifications.basenotifier import BaseNotifier
from airflow.utils.context import Context
import boto3


class AmiConnectDagFailureNotifier(BaseNotifier):
    """
    Notifier that sends SNS notifications on DAG task failures.
    """

    def __init__(
        self,
        sns_topic_arn: str,
        base_airflow_url: str,
        aws_profile_name: str,
        aws_region: str,
    ) -> None:
        super().__init__()
        self.sns_topic_arn = sns_topic_arn
        self.base_airflow_url = base_airflow_url
        self.profile = aws_profile_name
        self.region = aws_region

    def notify(self, context: Context) -> None:
        # Must create a new boto3 client each time to avoid serialization issues
        # (Airflow gives "TypeError: cannot pickle '_thread.lock' object" otherwise)
        if self.profile:
            session = boto3.Session(profile_name=self.profile)
            sns = session.client("sns", region_name=self.region)
        else:
            sns = boto3.client("sns", region_name=self.region)

        task_instance = context["task_instance"]
        dag = context["dag"]

        # Airflow UI links
        log_url = task_instance.log_url
        if log_url:
            log_url = log_url.replace("localhost:8080", self.base_airflow_url)
        dag_url = f"http://{self.base_airflow_url}/dags/{dag.dag_id}"

        exception = context.get("exception")
        if exception:
            exception_msg = "\n".join(
                traceback.format_exception_only(type(exception), exception)
            )
            stack = "".join(
                traceback.format_exception(
                    type(exception), exception, exception.__traceback__
                )
            )
        else:
            exception_msg = "Unknown exception"
            stack = ""

        message = f"""
🚨 Airflow DAG Failure 🚨

DAG: {dag.dag_id} ({dag_url})
Task: {task_instance.task_id}
Run ID: {task_instance.run_id}

{exception_msg}

Logs: {log_url}

{stack[:1500]}
...
{stack[1500:]}

""".strip()

        subject = f"AMI Connect DAG failure: {dag.dag_id}"
        sns.publish(
            TopicArn=self.sns_topic_arn,
            Subject=subject,
            Message=message,
        )

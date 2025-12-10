from unittest.mock import patch, MagicMock

from amiadapters.alerts.base import AmiConnectDagFailureNotifier
from test.base_test_case import BaseTestCase


class TestAmiConnectDagFailureNotifier(BaseTestCase):

    def setUp(self):
        self.sns_topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        self.base_airflow_url = "airflow.example.com"
        self.aws_profile_name = "test-profile"
        self.aws_region = "us-east-1"
        self.notifier = AmiConnectDagFailureNotifier(
            sns_topic_arn=self.sns_topic_arn,
            base_airflow_url=self.base_airflow_url,
            aws_profile_name=self.aws_profile_name,
            aws_region=self.aws_region,
        )

    @patch("amiadapters.alerts.base.boto3.Session")
    def test_notify_with_exception_and_profile(self, mock_boto3_session):
        mock_sns = MagicMock()
        mock_boto3_session.return_value.client.return_value = mock_sns

        class DummyTaskInstance:
            task_id = "test_task"
            run_id = "test_run"
            log_url = "http://localhost:8080/log"

        class DummyDag:
            dag_id = "test_dag"

        context = {
            "task_instance": DummyTaskInstance(),
            "dag": DummyDag(),
            "exception": ValueError("Test error"),
        }

        self.notifier.notify(context)
        mock_boto3_session.assert_called_with(profile_name=self.aws_profile_name)
        mock_boto3_session.return_value.client.assert_called_with(
            "sns", region_name=self.aws_region
        )
        mock_sns.publish.assert_called_once()
        args, kwargs = mock_sns.publish.call_args
        self.assertIn("ValueError: Test error", kwargs["Message"])
        self.assertIn("test_dag", kwargs["Subject"])

    @patch("amiadapters.alerts.base.boto3.client")
    def test_notify_without_profile_and_without_exception(self, mock_boto3_client):
        mock_sns = MagicMock()
        mock_boto3_client.return_value = mock_sns

        class DummyTaskInstance:
            task_id = "task2"
            run_id = "run2"
            log_url = "http://localhost:8080/log2"

        class DummyDag:
            dag_id = "dag2"

        notifier = AmiConnectDagFailureNotifier(
            sns_topic_arn=self.sns_topic_arn,
            base_airflow_url=self.base_airflow_url,
            # Make sure it still works without a profile passed in
            aws_profile_name=None,
            aws_region=self.aws_region,
        )

        context = {
            "task_instance": DummyTaskInstance(),
            "dag": DummyDag(),
            "exception": None,
        }

        notifier.notify(context)
        mock_boto3_client.assert_called_with("sns", region_name=self.aws_region)
        mock_sns.publish.assert_called_once()
        args, kwargs = mock_sns.publish.call_args
        self.assertIn("Unknown exception", kwargs["Message"])
        self.assertIn("http://airflow.example.com/log2", kwargs["Message"])
        self.assertIn("http://airflow.example.com/dags/dag2", kwargs["Message"])
        self.assertIn("dag2", kwargs["Subject"])

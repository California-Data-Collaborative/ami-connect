from unittest.mock import patch

from test.base_test_case import BaseTestCase


CONFIG = "../fixtures/all-config.yaml"
SECRETS = "../fixtures/all-secrets.yaml"


class TestDagImport(BaseTestCase):

    @patch("amicontrol.dags.ami_meter_read_dag.find_config_yaml", return_value=CONFIG)
    @patch("amicontrol.dags.ami_meter_read_dag.find_secrets_yaml", return_value=SECRETS)
    def test_dag_import(self, mock_secrets, mock_config):
        """
        Fingers and toes check, tries to ensure DAG will parse.
        """
        try:
            from amicontrol.dags import ami_meter_read_dag
        except Exception as e:
            self.fail(f"Importing DAG failed: {e}")

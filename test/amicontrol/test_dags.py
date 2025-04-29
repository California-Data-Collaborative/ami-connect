from test.base_test_case import BaseTestCase


class TestDagImport(BaseTestCase):

    def test_dag_import(self):
        """
        Fingers and toes check, tries to ensure DAG will parse.
        """
        try:
            from amicontrol.dags import ami_meter_read_dag
        except Exception as e:
            self.fail(f"Importing DAG failed: {e}")

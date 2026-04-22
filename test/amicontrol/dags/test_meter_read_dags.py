import unittest
from unittest.mock import MagicMock

from amicontrol.dags.meter_read_dags import ami_control_dag_factory
from test.base_test_case import BaseTestCase


class TestMeterReadDagFactory(BaseTestCase):

    ADAPTER_NAME = "test_utility"

    def _create_dag(self, dag_id="test-meter-read-dag"):
        adapter = MagicMock()
        adapter.name.return_value = self.ADAPTER_NAME
        notifier = MagicMock()
        return ami_control_dag_factory(dag_id, None, {}, adapter, notifier)

    def test_dag_builds_without_error(self):
        # Catches >> operator errors that would prevent the DAG from loading
        dag = self._create_dag()
        self.assertIsNotNone(dag)

    def test_expected_tasks_exist(self):
        dag = self._create_dag()
        task_ids = {task.task_id for task in dag.tasks}
        n = self.ADAPTER_NAME
        self.assertIn(f"extract-{n}", task_ids)
        self.assertIn(f"transform-{n}", task_ids)
        self.assertIn(f"transform-alerts-{n}", task_ids)
        self.assertIn(f"load-raw-{n}", task_ids)
        self.assertIn(f"load-transformed-{n}", task_ids)
        self.assertIn(f"load-transformed-alerts-{n}", task_ids)
        self.assertIn(f"post-process-{n}", task_ids)

    def test_task_dependencies(self):
        dag = self._create_dag()
        n = self.ADAPTER_NAME

        def downstream_ids(task_id):
            return {t.task_id for t in dag.get_task(task_id).downstream_list}

        def upstream_ids(task_id):
            return {t.task_id for t in dag.get_task(task_id).upstream_list}

        # extract fans out to both transform tasks
        self.assertEqual(
            downstream_ids(f"extract-{n}"),
            {f"transform-{n}", f"transform-alerts-{n}"},
        )

        # all load tasks must wait for all transform tasks
        for load in [
            f"load-raw-{n}",
            f"load-transformed-{n}",
            f"load-transformed-alerts-{n}",
        ]:
            self.assertEqual(
                upstream_ids(load),
                {f"transform-{n}", f"transform-alerts-{n}"},
            )

        # post-process waits for all load tasks
        self.assertEqual(
            upstream_ids(f"post-process-{n}"),
            {f"load-raw-{n}", f"load-transformed-{n}", f"load-transformed-alerts-{n}"},
        )

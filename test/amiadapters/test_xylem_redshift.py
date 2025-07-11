import datetime
import json
from unittest.mock import MagicMock

import pytz

from amiadapters.config import ConfiguredLocalTaskOutputController
from amiadapters.adapters.xylem_redshift import (
    XylemRedshiftAdapter,
)
from amiadapters.models import DataclassJSONEncoder
from amiadapters.outputs.base import ExtractOutput
from test.base_test_case import BaseTestCase


class TestMetersenseAdapter(BaseTestCase):

    def setUp(self):
        self.tz = pytz.timezone("Europe/Rome")
        self.adapter = XylemRedshiftAdapter(
            org_id="this-org",
            org_timezone=self.tz,
            configured_task_output_controller=ConfiguredLocalTaskOutputController(
                "/tmp/output"
            ),
            configured_sinks=[],
            ssh_tunnel_server_host="tunnel-ip",
            ssh_tunnel_username="ubuntu",
            ssh_tunnel_key_path="/key",
            database_host="db-host",
            database_port=1521,
            database_db_name="db-name",
            database_user="dbu",
            database_password="dbp",
        )

    def test_init(self):
        self.assertEqual("tunnel-ip", self.adapter.ssh_tunnel_server_host)
        self.assertEqual("ubuntu", self.adapter.ssh_tunnel_username)
        self.assertEqual("/key", self.adapter.ssh_tunnel_key_path)
        self.assertEqual("db-host", self.adapter.database_host)
        self.assertEqual(1521, self.adapter.database_port)
        self.assertEqual("db-name", self.adapter.database_db_name)
        self.assertEqual("dbu", self.adapter.database_user)
        self.assertEqual("dbp", self.adapter.database_password)

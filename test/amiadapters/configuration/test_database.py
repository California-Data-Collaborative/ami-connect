from datetime import date
from unittest.mock import MagicMock, patch

import yaml

from amiadapters.configuration.database import load_database_config
from test.base_test_case import BaseTestCase


class TestDatabase(BaseTestCase):

    def fake_fetch(self, cursor, table_name):
        if table_name == "configuration_sources":
            return [
                {
                    "id": 1,
                    "type": "beacon_360",
                    "org_id": "my_beacon_utility",
                    "timezone": "America/Los_Angeles",
                    "config": {"use_raw_data_cache": False},
                },
                {
                    "id": 2,
                    "type": "sentryx",
                    "org_id": "my_sentryx_utility",
                    "timezone": "America/Los_Angeles",
                    "config": {"use_raw_data_cache": False},
                },
                {
                    "id": 3,
                    "type": "subeca",
                    "org_id": "my_subeca_utility",
                    "timezone": "America/Los_Angeles",
                    "config": {},
                },
                {
                    "id": 4,
                    "type": "aclara",
                    "org_id": "my_aclara_utility",
                    "timezone": "America/Los_Angeles",
                    "config": {
                        "sftp_host": "example.com",
                        "sftp_remote_data_directory": "./data",
                        "sftp_local_download_directory": "./output",
                        "sftp_local_known_hosts_file": "./known-hosts",
                    },
                },
                {
                    "id": 5,
                    "type": "metersense",
                    "org_id": "my_metersense_utility",
                    "timezone": "America/Los_Angeles",
                    "config": {
                        "ssh_tunnel_server_host": "tunnel-ip",
                        "ssh_tunnel_key_path": "/key",
                        "database_host": "db-host",
                        "database_port": 1521,
                    },
                },
                {
                    "id": 6,
                    "type": "xylem_moulton_niguel",
                    "org_id": "my_xylem_moulton_niguel_utility",
                    "timezone": "America/Los_Angeles",
                    "config": {
                        "ssh_tunnel_server_host": "tunnel-ip",
                        "ssh_tunnel_key_path": "/key",
                        "database_host": "db-host",
                        "database_port": 1521,
                    },
                },
            ]
        elif table_name == "configuration_sinks":
            return [{"id": "my_snowflake_instance", "type": "snowflake"}]
        elif table_name == "configuration_source_sinks":
            return [
                {"source_id": 1, "sink_id": "my_snowflake_instance"},
                {"source_id": 2, "sink_id": "my_snowflake_instance"},
                {"source_id": 3, "sink_id": "my_snowflake_instance"},
                {"source_id": 4, "sink_id": "my_snowflake_instance"},
                {"source_id": 5, "sink_id": "my_snowflake_instance"},
                {"source_id": 6, "sink_id": "my_snowflake_instance"},
                {"source_id": 7, "sink_id": "my_snowflake_instance"},
            ]
        elif table_name == "configuration_sink_checks":
            return [
                {
                    "id": 1,
                    "sink_id": "my_snowflake_instance",
                    "check_name": "snowflake-meters-unique-by-device-id",
                },
            ]
        elif table_name == "configuration_task_outputs":
            return [
                {
                    "id": 1,
                    "type": "s3",
                    "s3_bucket": "my-bucket",
                    "local_output_path": None,
                },
            ]
        elif table_name == "configuration_notifications":
            return [{"id": 1, "event_type": "dag_failure", "sns_arn": "my-sns-arn"}]
        elif table_name == "configuration_backfills":
            return [
                {
                    "id": 1,
                    "org_id": "my_aclara_utility",
                    "start_date": date(2023, 1, 1),
                    "end_date": date(2025, 7, 5),
                    "interval_days": 3,
                    "schedule": "45 */2 * * *",
                }
            ]
        raise Exception(table_name)

    @patch("amiadapters.configuration.database._fetch_table")
    def test_load_database_config(self, mock_fetch_table):
        mock_fetch_table.side_effect = self.fake_fetch

        sources, sinks, task_output, notifications, backfills = load_database_config(
            MagicMock()
        )

        with open(self.get_fixture_path("all-config.yaml"), "r") as f:
            expected = yaml.safe_load(f)

        self.maxDiff = None
        self.assertEqual(expected["sources"], sources)
        self.assertEqual(expected["sinks"], sinks)
        self.assertEqual(expected["task_output"]["type"], task_output["type"])
        self.assertEqual(expected["task_output"]["bucket"], task_output["bucket"])
        self.assertEqual(expected["notifications"], notifications)
        self.assertEqual(expected["backfills"], backfills)

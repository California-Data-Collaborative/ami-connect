from datetime import date
import json
from unittest.mock import call, MagicMock, patch

import yaml

from amiadapters.configuration.database import (
    add_source_configuration,
    get_configuration,
    remove_sink_configuration,
    update_sink_configuration,
    update_task_output_configuration,
)
from test.base_test_case import BaseTestCase


class TestDatabase(BaseTestCase):

    def setUp(self):
        # Create a mock connection and cursor
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_connection.cursor.return_value = self.mock_cursor

    def fake_fetch(self, cursor, table_name):
        if table_name == "configuration_sources":
            return [
                {
                    "id": 1,
                    "type": "beacon_360",
                    "org_id": "my_beacon_utility",
                    "timezone": "America/Los_Angeles",
                    "config": json.dumps({"use_raw_data_cache": False}),
                },
                {
                    "id": 2,
                    "type": "sentryx",
                    "org_id": "my_sentryx_utility",
                    "timezone": "America/Los_Angeles",
                    "config": json.dumps({"use_raw_data_cache": False}),
                },
                {
                    "id": 3,
                    "type": "subeca",
                    "org_id": "my_subeca_utility",
                    "timezone": "America/Los_Angeles",
                    "config": json.dumps({}),
                },
                {
                    "id": 4,
                    "type": "aclara",
                    "org_id": "my_aclara_utility",
                    "timezone": "America/Los_Angeles",
                    "config": json.dumps(
                        {
                            "sftp_host": "example.com",
                            "sftp_remote_data_directory": "./data",
                            "sftp_local_download_directory": "./output",
                            "sftp_local_known_hosts_file": "./known-hosts",
                        }
                    ),
                },
                {
                    "id": 5,
                    "type": "metersense",
                    "org_id": "my_metersense_utility",
                    "timezone": "America/Los_Angeles",
                    "config": json.dumps(
                        {
                            "ssh_tunnel_server_host": "tunnel-ip",
                            "ssh_tunnel_key_path": "/key",
                            "database_host": "db-host",
                            "database_port": 1521,
                        }
                    ),
                },
                {
                    "id": 6,
                    "type": "xylem_moulton_niguel",
                    "org_id": "my_xylem_moulton_niguel_utility",
                    "timezone": "America/Los_Angeles",
                    "config": json.dumps(
                        {
                            "ssh_tunnel_server_host": "tunnel-ip",
                            "ssh_tunnel_key_path": "/key",
                            "database_host": "db-host",
                            "database_port": 1521,
                        }
                    ),
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
    def test_get_database_config(self, mock_fetch_table):
        mock_fetch_table.side_effect = self.fake_fetch

        sources, sinks, task_output, notifications, backfills = get_configuration(
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

    def test_update_task_output_configuration_valid_s3_configuration_executes_queries(
        self,
    ):
        config = {
            "type": "s3",
            "s3_bucket": "my-bucket",
            "local_output_path": None,
        }

        update_task_output_configuration(self.mock_connection, config)

        # Ensure TRUNCATE then INSERT are executed
        expected_calls = [
            call.execute("TRUNCATE TABLE configuration_task_outputs"),
            call.execute(
                """
        INSERT INTO configuration_task_outputs (type, s3_bucket, local_output_path)
        VALUES (?, ?, ?)
        """,
                ["s3", "my-bucket", None],
            ),
        ]
        self.mock_cursor.assert_has_calls(expected_calls)

    def test_update_task_output_configuration_valid_local_configuration_executes_queries(
        self,
    ):
        config = {
            "type": "local",
            "s3_bucket": None,
            "local_output_path": "/tmp/data",
        }

        update_task_output_configuration(self.mock_connection, config)

        self.mock_cursor.execute.assert_any_call(
            "TRUNCATE TABLE configuration_task_outputs"
        )
        self.mock_cursor.execute.assert_any_call(
            """
        INSERT INTO configuration_task_outputs (type, s3_bucket, local_output_path)
        VALUES (?, ?, ?)
        """,
            ["local", None, "/tmp/data"],
        )

    def test_update_task_output_configuration_missing_type_raises(self):
        config = {"s3_bucket": "bucket", "local_output_path": None}
        with self.assertRaises(ValueError) as cm:
            update_task_output_configuration(self.mock_connection, config)
        self.assertIn("missing field: type", str(cm.exception))

    def test_update_task_output_configuration_missing_s3_bucket_raises(self):
        config = {"type": "s3", "s3_bucket": None, "local_output_path": None}
        with self.assertRaises(ValueError) as cm:
            update_task_output_configuration(self.mock_connection, config)
        self.assertIn("missing field: s3_bucket", str(cm.exception))

    def test_update_task_output_configuration_missing_local_output_path_raises(self):
        config = {"type": "local", "s3_bucket": None, "local_output_path": None}
        with self.assertRaises(ValueError) as cm:
            update_task_output_configuration(self.mock_connection, config)
        self.assertIn("missing field: local_output_path", str(cm.exception))

    def test_update_sink_configuration_missing_id_raises_value_error(self):
        config = {"type": "snowflake"}
        with self.assertRaisesRegex(
            ValueError, "Sink configuration is missing field: id"
        ):
            update_sink_configuration(self.mock_connection, config)

    def test_update_sink_configuration_missing_type_raises_value_error(self):
        config = {"id": "sink1"}
        with self.assertRaisesRegex(
            ValueError, "Sink configuration is missing field: type"
        ):
            update_sink_configuration(self.mock_connection, config)

    def test_update_sink_configuration_invalid_type_raises_value_error(self):
        config = {"id": "sink1", "type": "mysql"}
        with self.assertRaisesRegex(ValueError, "Unrecognized sink type: mysql"):
            update_sink_configuration(self.mock_connection, config)

    def test_update_sink_configuration_valid_snowflake_configuration_executes_merge(
        self,
    ):
        config = {"id": "SINK1", "type": "snowflake"}

        update_sink_configuration(self.mock_connection, config)

        self.mock_cursor.execute.assert_called_once()
        sql, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("MERGE INTO configuration_sinks", sql)
        self.assertEqual(params, ("sink1", "snowflake"))

    def test_update_sink_configuration_id_and_type_are_lowercased_before_insert(self):
        config = {"id": "Cadc_Snowflake", "type": "SnowFlake"}

        update_sink_configuration(self.mock_connection, config)

        _, params = self.mock_cursor.execute.call_args[0]
        self.assertEqual(params, ("cadc_snowflake", "snowflake"))

    def test_remove_sink_configuration_missing_id_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "Missing field: id"):
            remove_sink_configuration(self.mock_connection, "")

    @patch("amiadapters.configuration.database._get_sink_by_id")
    def test_remove_sink_error_if_attached_to_source(self, mock_get_sink):
        mock_get_sink.return_value = [
            [
                1,
            ]
        ]
        with self.assertRaises(ValueError):
            remove_sink_configuration(self.mock_connection, "Cadc_Snowflake")

    @patch("amiadapters.configuration.database._get_sink_by_id")
    def test_remove_sink_configuration_valid_id_executes_delete(self, mock_get_sink):
        mock_get_sink.return_value = []
        remove_sink_configuration(self.mock_connection, "SINK1")

        self.mock_cursor.execute.assert_called_once()
        sql, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("DELETE FROM configuration_sinks", sql)
        self.assertEqual(params, ("sink1",))  # should be lowercased

    @patch("amiadapters.configuration.database._get_sink_by_id")
    def test_remove_sink_configuration_id_is_lowercased_before_delete(
        self, mock_get_sink
    ):
        mock_get_sink.return_value = []
        remove_sink_configuration(self.mock_connection, "Cadc_Snowflake")

        _, params = self.mock_cursor.execute.call_args[0]
        self.assertEqual(params, ("cadc_snowflake",))

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_add_source_configuration_raises_if_missing_org_id(self, mock_get_source):
        with self.assertRaises(ValueError) as context:
            add_source_configuration(
                self.mock_connection, {"type": "beacon_360", "timezone": "UTC"}
            )
        self.assertIn("org_id", str(context.exception))

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_add_source_configuration_raises_if_org_id_exists(self, mock_get_source):
        mock_get_source.return_value = [(1,)]
        with self.assertRaises(Exception) as context:
            add_source_configuration(
                self.mock_connection,
                {"org_id": "CADC", "type": "beacon_360", "timezone": "UTC"},
            )
        self.assertIn("already exists", str(context.exception))

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_add_source_configuration_raises_if_missing_type(self, mock_get_source):
        mock_get_source.return_value = []
        with self.assertRaises(ValueError) as context:
            add_source_configuration(
                self.mock_connection, {"org_id": "CADC", "timezone": "UTC"}
            )
        self.assertIn("type", str(context.exception))

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_add_source_configuration_raises_if_missing_timezone(self, mock_get_source):
        mock_get_source.return_value = []
        with self.assertRaises(ValueError) as context:
            add_source_configuration(
                self.mock_connection, {"org_id": "CADC", "type": "beacon_360"}
            )
        self.assertIn("timezone", str(context.exception))

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_add_source_configuration_raises_if_invalid_timezone(self, mock_get_source):
        mock_get_source.return_value = []
        with self.assertRaises(ValueError) as context:
            add_source_configuration(
                self.mock_connection,
                {"org_id": "CADC", "type": "beacon_360", "timezone": "Invalid/Zone"},
            )
        self.assertIn("Invalid timezone", str(context.exception))

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_add_source_configuration_executes_insert(self, mock_get_source):
        mock_get_source.return_value = []
        source_config = {"org_id": "CADC", "type": "beacon_360", "timezone": "UTC"}
        add_source_configuration(self.mock_connection, source_config)

        self.mock_cursor.execute.assert_called_once()

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_add_source_configuration_with_sinks_executes_merges(self, mock_get_source):
        mock_get_source.side_effect = [
            [],  # first call: no existing source
            [(123,)],  # second call: return newly inserted source id
        ]
        source_config = {
            "org_id": "CADC",
            "type": "beacon_360",
            "timezone": "UTC",
            "sinks": ["cadc_snowflake"],
        }
        self.mock_cursor.execute.return_value.fetchall.return_value = [(1,)]
        add_source_configuration(self.mock_connection, source_config)

        merge_call = """
                MERGE INTO configuration_source_sinks AS target
                USING (
                    SELECT ? AS source_id, ? AS sink_id
                ) AS source
                ON target.source_id = source.source_id AND target.sink_id = source.sink_id
                WHEN NOT MATCHED THEN
                    INSERT (source_id, sink_id)
                    VALUES (source.source_id, source.sink_id);

            """
        self.mock_cursor.execute.assert_any_call(merge_call, (123, "cadc_snowflake"))

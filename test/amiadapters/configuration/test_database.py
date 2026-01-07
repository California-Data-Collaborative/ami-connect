from datetime import date
import json
from unittest.mock import MagicMock, patch

from pytz.exceptions import UnknownTimeZoneError
import yaml

from amiadapters.configuration.database import (
    add_data_quality_check_configurations,
    add_source_configuration,
    get_configuration,
    remove_data_quality_check_configurations,
    remove_sink_configuration,
    remove_source_configuration,
    update_backfill_configuration,
    update_notification_configuration,
    update_post_processor_configuration,
    update_sink_configuration,
    update_source_configuration,
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
                    "config": json.dumps(
                        {
                            "use_raw_data_cache": False,
                            "utility_name": "my-sentryx-utility",
                        }
                    ),
                },
                {
                    "id": 3,
                    "type": "subeca",
                    "org_id": "my_subeca_utility",
                    "timezone": "America/Los_Angeles",
                    "config": json.dumps({"api_url": "my-url"}),
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
                            "sftp_known_hosts_str": "example.com ssh-rsa abc",
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
                {
                    "id": 7,
                    "type": "xylem_sensus",
                    "org_id": "my_xylem_sensus_utility",
                    "timezone": "America/Los_Angeles",
                    "config": json.dumps(
                        {
                            "sftp_host": "example.com",
                            "sftp_remote_data_directory": "./data",
                            "sftp_local_download_directory": "./output",
                            "sftp_known_hosts_str": None,
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
        elif table_name == "configuration_pipeline":
            return [
                {
                    "id": 1,
                    "intermediate_output_type": "s3",
                    "intermediate_output_s3_bucket": "my-bucket",
                    "intermediate_output_dev_profile": "my-aws-profile",
                    "intermediate_output_local_output_path": None,
                    "run_post_processors": True,
                    "publish_load_finished_events": False,
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

        sources, sinks, pipeline_config, notifications, backfills = get_configuration(
            MagicMock()
        )

        with open(self.get_fixture_path("all-config.yaml"), "r") as f:
            expected = yaml.safe_load(f)

        self.maxDiff = None
        self.assertEqual(expected["sources"], sources)
        self.assertEqual(expected["sinks"], sinks)
        self.assertEqual(
            expected["task_output"]["type"], pipeline_config.intermediate_output_type
        )
        self.assertEqual(
            expected["task_output"]["bucket"],
            pipeline_config.intermediate_output_s3_bucket,
        )
        self.assertTrue(pipeline_config.should_run_post_processor)
        self.assertFalse(pipeline_config.should_publish_load_finished_events)
        self.assertEqual(expected["notifications"], notifications)
        self.assertEqual(expected["backfills"], backfills)

    def test_update_task_output_configuration_valid_s3_configuration_executes_queries(
        self,
    ):
        config = {
            "type": "s3",
            "s3_bucket": "my-bucket",
            "dev_profile": "my-aws-profile",
            "local_output_path": None,
        }

        update_task_output_configuration(self.mock_connection, config)
        self.assertEqual(len(self.mock_cursor.execute.call_args_list), 2)

    def test_update_task_output_configuration_missing_type_raises(self):
        config = {"s3_bucket": "bucket", "local_output_path": None}
        with self.assertRaises(ValueError) as cm:
            update_task_output_configuration(self.mock_connection, config)
        self.assertIn("missing field: type", str(cm.exception))

    def test_update_task_output_configuration_missing_s3_bucket_raises(self):
        config = {
            "type": "s3",
            "s3_bucket": None,
            "dev_profile": "dev",
            "local_output_path": None,
        }
        with self.assertRaises(ValueError) as cm:
            update_task_output_configuration(self.mock_connection, config)
        self.assertIn("missing field: s3_bucket", str(cm.exception))

    def test_update_task_output_configuration_missing_dev_profile_raises(self):
        config = {
            "type": "s3",
            "s3_bucket": "bucket",
            "dev_profile": None,
            "local_output_path": None,
        }
        with self.assertRaises(ValueError) as cm:
            update_task_output_configuration(self.mock_connection, config)
        self.assertIn("missing field: dev_profile", str(cm.exception))

    def test_update_task_output_configuration_missing_local_output_path_raises(self):
        config = {
            "type": "local",
            "s3_bucket": None,
            "dev_profile": None,
            "local_output_path": None,
        }
        with self.assertRaises(ValueError) as cm:
            update_task_output_configuration(self.mock_connection, config)
        self.assertIn("missing field: local_output_path", str(cm.exception))

    def test_update_post_processor_configuration_none_raises_value_error(self):
        """Should raise ValueError if should_run_post_processor is None"""
        with self.assertRaises(ValueError) as ctx:
            update_post_processor_configuration(self.mock_connection, None)
        self.assertIn("should_run_post_processor argument is None", str(ctx.exception))

    @patch("amiadapters.configuration.database._ensure_pipeline_config_row_exists")
    def test_update_post_processor_configuration_executes_update(self, mock_ensure_row):
        """Should call _ensure_pipeline_config_row_exists and execute UPDATE with correct param"""
        update_post_processor_configuration(self.mock_connection, True)
        mock_ensure_row.assert_called_once_with(self.mock_cursor)
        self.mock_cursor.execute.assert_called_once()
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("UPDATE configuration_pipeline", query)
        self.assertEqual(params, (True,))

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

    @patch("amiadapters.configuration.database._get_sources_associated_with_sink_id")
    def test_remove_sink_error_if_attached_to_source(self, mock_get_sink):
        mock_get_sink.return_value = [
            [
                1,
            ]
        ]
        with self.assertRaises(ValueError):
            remove_sink_configuration(self.mock_connection, "Cadc_Snowflake")

    @patch("amiadapters.configuration.database._get_sources_associated_with_sink_id")
    def test_remove_sink_configuration_valid_id_executes_delete(self, mock_get_sink):
        mock_get_sink.return_value = []
        remove_sink_configuration(self.mock_connection, "SINK1")

        self.mock_cursor.execute.assert_called_once()
        sql, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("DELETE FROM configuration_sinks", sql)
        self.assertEqual(params, ("sink1",))  # should be lowercased

    @patch("amiadapters.configuration.database._get_sources_associated_with_sink_id")
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
    def test_add_source_configuration_raises_if_invalid_timezone(self, mock_get_source):
        mock_get_source.return_value = []
        with self.assertRaises(UnknownTimeZoneError) as context:
            add_source_configuration(
                self.mock_connection,
                {"org_id": "CADC", "type": "beacon_360", "timezone": "Invalid/Zone"},
            )

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

        sql, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("MERGE INTO configuration_source_sinks", sql)
        self.assertEqual(params, (123, "cadc_snowflake"))

    def test_update_source_configuration_missing_org_id_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            update_source_configuration(self.mock_connection, {})
        self.assertIn(
            "Source configuration is missing field: org_id", str(ctx.exception)
        )

    def test_update_source_configuration_multiple_existing_sources_raises_exception(
        self,
    ):
        # Simulate _get_source_by_org_id -> returns multiple
        self.mock_cursor.fetchall.return_value = [
            ("id1", "snowflake", "org1", "UTC", json.dumps({})),
            ("id2", "snowflake", "org1", "UTC", json.dumps({})),
        ]

        with self.assertRaises(Exception) as ctx:
            update_source_configuration(self.mock_connection, {"org_id": "org1"})

        self.assertIn(
            "Expected to find one source with org_id org1", str(ctx.exception)
        )

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_update_source_configuration_updates_existing_source(self, mock_get_source):
        # Simulate one existing row
        existing_config = {"use_raw_data_cache": False}
        mock_get_source.return_value = [
            ("id1", "beacon_360", "org1", "UTC", json.dumps(existing_config))
        ]
        update_source_configuration(
            self.mock_connection,
            {"org_id": "org1", "timezone": "PST", "use_raw_data_cache": True},
        )

        # Ensure UPDATE executed with correct parameters
        update_call = self.mock_cursor.execute.call_args_list[-1]
        query, params = update_call[0]
        self.assertIn("UPDATE configuration_sources", query)
        self.assertEqual(params[0], "beacon_360")  # type unchanged
        self.assertEqual(params[1], "PST")  # timezone updated
        updated_config = json.loads(params[2])
        self.assertEqual(updated_config["use_raw_data_cache"], True)
        self.assertEqual(params[3], "org1")  # org_id lowercased

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    @patch("amiadapters.configuration.database._get_sink_by_id")
    def test_update_source_configuration_associates_sinks_if_provided(
        self, mock_get_sink, mock_get_source
    ):
        # One existing row
        mock_get_source.return_value = [
            ("id1", "beacon_360", "org1", "UTC", json.dumps({}))
        ]

        # One existing sink
        mock_get_sink.return_value = [("sink1", "org1")]

        update_source_configuration(
            self.mock_connection, {"org_id": "org1", "sinks": ["sink1", "sink2"]}
        )

        # Check that UPDATE was executed
        update_call = self.mock_cursor.execute.call_args_list[0]
        query, params = update_call[0]
        self.assertIn("UPDATE configuration_sources", query)
        self.assertEqual(params[3], "org1")

        # Check that _associate_sinks_with_source query was executed
        all_queries = [c[0][0] for c in self.mock_cursor.execute.call_args_list]
        self.assertTrue(any("INSERT" in q or "DELETE" in q for q in all_queries))

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    def test_update_source_configuration_removes_none_values_from_new_config(
        self, mock_get_source
    ):
        # Existing config has "use_raw_data_cache"
        mock_get_source.return_value = [
            (
                "id1",
                "beacon_360",
                "org1",
                "UTC",
                json.dumps({"use_raw_data_cache": False}),
            )
        ]

        update_source_configuration(
            self.mock_connection, {"org_id": "org1", "use_raw_data_cache": None}
        )

        update_call = self.mock_cursor.execute.call_args_list[-1]
        query, params = update_call[0]
        updated_config = json.loads(params[2])
        self.assertIn("use_raw_data_cache", updated_config)  # None explicitly set

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    @patch("amiadapters.configuration.database._get_backfills_by_org_id")
    def test_remove_source_configuration_success(
        self, mock_get_backfills, mock_get_source
    ):
        # Arrange
        mock_get_source.return_value = [
            ("id1", "snowflake", "org1", "UTC", json.dumps({}))
        ]
        mock_get_backfills.return_value = []

        # Act
        remove_source_configuration(self.mock_connection, "ORG1")

        # Assert
        self.mock_cursor.execute.assert_any_call("BEGIN")
        self.mock_cursor.execute.assert_any_call(
            "DELETE FROM configuration_source_sinks WHERE source_id = ?",
            ("id1",),
        )
        self.mock_cursor.execute.assert_any_call(
            "DELETE FROM configuration_sources WHERE id = ?",
            ("id1",),
        )
        self.mock_cursor.execute.assert_any_call("COMMIT")

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    @patch("amiadapters.configuration.database._get_backfills_by_org_id")
    def test_remove_source_configuration_raises_if_not_one_source(
        self, mock_get_backfills, mock_get_source
    ):
        mock_get_source.return_value = []  # No sources
        mock_get_backfills.return_value = []

        with self.assertRaises(Exception) as ctx:
            remove_source_configuration(self.mock_connection, "org1")
        self.assertIn(
            "Expected to find one source with org_id org1", str(ctx.exception)
        )

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    @patch("amiadapters.configuration.database._get_backfills_by_org_id")
    def test_remove_source_configuration_raises_if_backfills_exist(
        self, mock_get_backfills, mock_get_source
    ):
        mock_get_source.return_value = [
            ("id1", "snowflake", "org1", "UTC", json.dumps({}))
        ]
        mock_get_backfills.return_value = [("backfill1",)]

        with self.assertRaises(Exception) as ctx:
            remove_source_configuration(self.mock_connection, "org1")
        self.assertIn(
            "Cannot remove source with 1 associated backfills", str(ctx.exception)
        )

    @patch("amiadapters.configuration.database._get_source_by_org_id")
    @patch("amiadapters.configuration.database._get_backfills_by_org_id")
    def test_remove_source_configuration_rolls_back_on_error(
        self, mock_get_backfills, mock_get_source
    ):
        mock_get_source.return_value = [
            ("id1", "snowflake", "org1", "UTC", json.dumps({}))
        ]
        mock_get_backfills.return_value = []

        # Force an exception during delete
        self.mock_cursor.execute.side_effect = [None, Exception("DB error")]

        with self.assertRaises(Exception):
            remove_source_configuration(self.mock_connection, "org1")

        # Ensure rollback was called
        self.mock_cursor.execute.assert_any_call("ROLLBACK")

    def test_update_backfill_configuration_missing_field_raises(self):
        """update_backfill_configuration should raise ValueError if a required field is missing"""
        for field in ["org_id", "start_date", "end_date", "interval_days", "schedule"]:
            bad_config = {
                "org_id": "Org1",
                "start_date": date(2023, 1, 1),
                "end_date": date(2023, 12, 31),
                "interval_days": 5,
                "schedule": "0 0 * * *",
            }

            bad_config[field] = None  # remove one required field
            with self.assertRaises(ValueError) as ctx:
                update_backfill_configuration(self.mock_connection, bad_config)
            self.assertIn(
                f"Backfill configuration is missing field: {field}", str(ctx.exception)
            )

    def test_update_backfill_configuration_executes_merge(self):
        """update_backfill_configuration should execute a MERGE statement with correct parameters"""
        valid_config = {
            "org_id": "Org1",
            "start_date": date(2023, 1, 1),
            "end_date": date(2023, 12, 31),
            "interval_days": 5,
            "schedule": "0 0 * * *",
        }

        update_backfill_configuration(self.mock_connection, valid_config)

        self.mock_cursor.execute.assert_called_once()
        query, params = self.mock_cursor.execute.call_args[0]

        # Check query structure
        self.assertIn("MERGE INTO configuration_backfills", query)
        self.assertIn("WHEN MATCHED THEN", query)
        self.assertIn("WHEN NOT MATCHED THEN", query)

        # Validate parameters
        self.assertEqual(params[0], "org1")  # org_id lowercased
        self.assertEqual(params[1], valid_config["start_date"])
        self.assertEqual(params[2], valid_config["end_date"])
        self.assertEqual(params[3], valid_config["interval_days"])
        self.assertEqual(params[4], valid_config["schedule"])

    def test_update_backfill_configuration_allows_multiple_calls(self):
        """update_backfill_configuration should execute independently for each call"""
        config2 = {
            "org_id": "Org1",
            "start_date": date(2023, 1, 1),
            "end_date": date(2023, 12, 31),
            "interval_days": 5,
            "schedule": "0 0 * * *",
        }

        config2_copy = dict(config2)

        config2["org_id"] = "AnotherOrg"

        update_backfill_configuration(self.mock_connection, config2_copy)
        update_backfill_configuration(self.mock_connection, config2)

        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        _, first_params = self.mock_cursor.execute.call_args_list[0][0]
        _, second_params = self.mock_cursor.execute.call_args_list[1][0]

        self.assertEqual(first_params[0], "org1")
        self.assertEqual(second_params[0], "anotherorg")

    def test_update_notification_configuration_missing_field_raises(self):
        """Should raise ValueError if event_type or sns_arn is missing"""
        for field in ["event_type", "sns_arn"]:
            bad_config = {
                "event_type": "BACKFILL_COMPLETED",
                "sns_arn": "arn:aws:sns:us-west-2:123456789012:my-topic",
            }
            bad_config[field] = None
            with self.assertRaises(ValueError) as ctx:
                update_notification_configuration(self.mock_connection, bad_config)
            self.assertIn(
                f"Notification configuration is missing field: {field}",
                str(ctx.exception),
            )

    def test_update_notification_configuration_executes_merge_statement(self):
        """Should call cursor.execute with MERGE SQL and correct params"""
        valid_config = {
            "event_type": "BACKFILL_COMPLETED",
            "sns_arn": "arn:aws:sns:us-west-2:123456789012:my-topic",
        }
        update_notification_configuration(self.mock_connection, valid_config)

        self.mock_cursor.execute.assert_called_once()
        query, params = self.mock_cursor.execute.call_args[0]

        # Validate query structure
        self.assertIn("MERGE INTO configuration_notifications", query)
        self.assertIn("WHEN MATCHED THEN", query)
        self.assertIn("WHEN NOT MATCHED THEN", query)

        # Validate parameters
        self.assertEqual(params[0], valid_config["event_type"])
        self.assertEqual(params[1], valid_config["sns_arn"])

    def test_update_notification_configuration_multiple_calls_are_independent(self):
        """Each call should execute independently with correct params"""
        valid_config = {
            "event_type": "BACKFILL_COMPLETED",
            "sns_arn": "arn:aws:sns:us-west-2:123456789012:my-topic",
        }
        config2 = {
            "event_type": "METER_READING_FAILED",
            "sns_arn": "arn:aws:sns:us-east-1:111111111111:other-topic",
        }

        update_notification_configuration(self.mock_connection, valid_config)
        update_notification_configuration(self.mock_connection, config2)

        self.assertEqual(self.mock_cursor.execute.call_count, 2)

        # First call
        _, first_params = self.mock_cursor.execute.call_args_list[0][0]
        self.assertEqual(first_params[0], valid_config["event_type"])
        self.assertEqual(first_params[1], valid_config["sns_arn"])

        # Second call
        _, second_params = self.mock_cursor.execute.call_args_list[1][0]
        self.assertEqual(second_params[0], config2["event_type"])
        self.assertEqual(second_params[1], config2["sns_arn"])

    def test_add_data_quality_check_configurations_missing_sink_id_raises_value_error(
        self,
    ):
        with self.assertRaises(ValueError) as ctx:
            add_data_quality_check_configurations(
                self.mock_connection, {"check_names": ["check1"]}
            )
        self.assertIn(
            "Check configuration is missing field: sink_id", str(ctx.exception)
        )

    def test_add_data_quality_check_configurations_missing_check_names_raises_value_error(
        self,
    ):
        with self.assertRaises(ValueError) as ctx:
            add_data_quality_check_configurations(
                self.mock_connection, {"sink_id": "sink1"}
            )
        self.assertIn(
            "Check configuration is missing field: check_names", str(ctx.exception)
        )

    @patch("amiadapters.configuration.database._get_sink_by_id")
    def test_add_data_quality_check_configurations_invalid_sink_id_raises_value_error(
        self, mock_get_sink
    ):
        mock_get_sink.return_value = None
        with self.assertRaises(ValueError) as ctx:
            add_data_quality_check_configurations(
                self.mock_connection, {"sink_id": "sink1", "check_names": ["check1"]}
            )
        self.assertIn("No sink found for id: sink1", str(ctx.exception))

    @patch("amiadapters.configuration.database._get_sink_by_id")
    def test_add_data_quality_check_configurations_adds_multiple_checks(
        self, mock_get_sink
    ):
        mock_get_sink.return_value = True
        add_data_quality_check_configurations(
            self.mock_connection,
            {"sink_id": "sink1", "check_names": ["check1", "check2"]},
        )

        # Should execute twice (once per check name)
        self.assertEqual(self.mock_cursor.execute.call_count, 2)

        # Check first call parameters
        first_query, first_params = self.mock_cursor.execute.call_args_list[0][0]
        self.assertIn("MERGE INTO configuration_sink_checks", first_query)
        self.assertEqual(first_params, ("sink1", "check1"))

        # Check second call parameters
        _, second_params = self.mock_cursor.execute.call_args_list[1][0]
        self.assertEqual(second_params, ("sink1", "check2"))

    def test_remove_data_quality_check_configurations_missing_sink_id_raises_value_error(
        self,
    ):
        with self.assertRaises(ValueError) as ctx:
            remove_data_quality_check_configurations(
                self.mock_connection, {"check_names": ["check1"]}
            )
        self.assertIn(
            "Check configuration is missing field: sink_id", str(ctx.exception)
        )

    def test_remove_data_quality_check_configurations_missing_check_names_raises_value_error(
        self,
    ):
        with self.assertRaises(ValueError) as ctx:
            remove_data_quality_check_configurations(
                self.mock_connection, {"sink_id": "sink1"}
            )
        self.assertIn(
            "Check configuration is missing field: check_names", str(ctx.exception)
        )

    @patch("amiadapters.configuration.database._get_sink_by_id")
    def test_remove_data_quality_check_configurations_invalid_sink_id_raises_value_error(
        self, mock_get_sink
    ):
        mock_get_sink.return_value = None
        with self.assertRaises(ValueError) as ctx:
            remove_data_quality_check_configurations(
                self.mock_connection, {"sink_id": "sink1", "check_names": ["check1"]}
            )
        self.assertIn("No sink found for id: sink1", str(ctx.exception))

    @patch("amiadapters.configuration.database._get_sink_by_id")
    def test_remove_data_quality_check_configurations_removes_multiple_checks(
        self, mock_get_sink
    ):
        mock_get_sink.return_value = True
        remove_data_quality_check_configurations(
            self.mock_connection,
            {"sink_id": "sink1", "check_names": ["check1", "check2"]},
        )

        # Should execute twice (once per check name)
        self.assertEqual(self.mock_cursor.execute.call_count, 2)

        # Check first call parameters
        first_query, first_params = self.mock_cursor.execute.call_args_list[0][0]
        self.assertIn("DELETE FROM configuration_sink_checks", first_query)
        self.assertEqual(first_params, ("sink1", "check1"))

        # Check second call parameters
        _, second_params = self.mock_cursor.execute.call_args_list[1][0]
        self.assertEqual(second_params, ("sink1", "check2"))

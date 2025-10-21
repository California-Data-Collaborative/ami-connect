from datetime import datetime
import pathlib
from unittest.mock import patch

from airflow.providers.amazon.aws.notifications.sns import SnsNotifier
import pytz

from amiadapters.adapters.aclara import AclaraAdapter
from amiadapters.adapters.beacon import Beacon360Adapter
from amiadapters.config import (
    AMIAdapterConfiguration,
    find_config_yaml,
    find_secrets_yaml,
)
from amiadapters.adapters.metersense import MetersenseAdapter
from amiadapters.adapters.sentryx import SentryxAdapter
from amiadapters.adapters.subeca import SubecaAdapter
from amiadapters.storage.snowflake import SnowflakeStorageSink
from test.base_test_case import BaseTestCase


class TestConfig(BaseTestCase):

    def test_can_instantiate_sentryx_via_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("sentryx-config.yaml"),
            self.get_fixture_path("sentryx-secrets.yaml"),
        )
        self.assertEqual(1, len(config._sources))
        source = config._sources[0]
        self.assertEqual("sentryx", source.type)
        self.assertEqual("my_utility", source.org_id)
        self.assertEqual("u_name", source.utility_name)
        self.assertEqual("America/Los_Angeles", str(source.timezone))
        self.assertEqual(False, source.use_raw_data_cache)
        self.assertEqual("outputs", source.task_output_controller.output_folder)
        self.assertEqual("key", source.secrets.api_key)

        self.assertEqual(1, len(source.storage_sinks))
        sink = source.storage_sinks[0]
        self.assertEqual("snowflake", sink.type)
        self.assertEqual("my_snowflake_instance", sink.id)
        self.assertEqual("my_account", sink.secrets.account)
        self.assertEqual("my_user", sink.secrets.user)
        self.assertEqual("my_password", sink.secrets.password)
        self.assertEqual("my_role", sink.secrets.role)
        self.assertEqual("my_warehouse", sink.secrets.warehouse)
        self.assertEqual("my_database", sink.secrets.database)
        self.assertEqual("my_schema", sink.secrets.schema)

        self.assertEqual([], config._backfills)

        self.assertIsNone(config.on_failure_sns_notifier())

    def test_can_instantiate_beacon_via_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("beacon-360-config.yaml"),
            self.get_fixture_path("beacon-360-secrets.yaml"),
        )
        self.assertEqual(1, len(config._sources))
        source = config._sources[0]
        self.assertEqual("beacon_360", source.type)
        self.assertEqual("my_utility", source.org_id)
        self.assertEqual("America/Los_Angeles", str(source.timezone))
        self.assertEqual(True, source.use_raw_data_cache)
        self.assertEqual("my-bucket", source.task_output_controller.s3_bucket_name)
        self.assertEqual("my_user", source.secrets.user)
        self.assertEqual("my_password", source.secrets.password)

        self.assertEqual(1, len(source.storage_sinks))
        sink = source.storage_sinks[0]
        self.assertEqual("snowflake", sink.type)
        self.assertEqual("my_snowflake_instance", sink.id)
        self.assertEqual("my_account", sink.secrets.account)
        self.assertEqual("my_user", sink.secrets.user)
        self.assertEqual("my_password", sink.secrets.password)
        self.assertEqual("my_role", sink.secrets.role)
        self.assertEqual("my_warehouse", sink.secrets.warehouse)
        self.assertEqual("my_database", sink.secrets.database)
        self.assertEqual("my_schema", sink.secrets.schema)

    def test_can_instantiate_aclara_via_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("aclara-config.yaml"),
            self.get_fixture_path("aclara-secrets.yaml"),
        )
        self.assertEqual(1, len(config._sources))
        source = config._sources[0]
        self.assertEqual("aclara", source.type)
        self.assertEqual("my_utility", source.org_id)
        self.assertEqual("America/Los_Angeles", str(source.timezone))
        self.assertEqual("my-bucket", source.task_output_controller.s3_bucket_name)
        self.assertEqual("example.com", source.configured_sftp.host)
        self.assertEqual("./data", source.configured_sftp.remote_data_directory)
        self.assertEqual("./output", source.configured_sftp.local_download_directory)
        self.assertEqual("./known-hosts", source.configured_sftp.local_known_hosts_file)
        self.assertEqual("my_user", source.secrets.sftp_user)
        self.assertEqual("my_password", source.secrets.sftp_password)

    def test_can_instantiate_metersense_via_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("metersense-config.yaml"),
            self.get_fixture_path("metersense-secrets.yaml"),
        )
        self.assertEqual(1, len(config._sources))
        source = config._sources[0]
        self.assertEqual("metersense", source.type)
        self.assertEqual("my_utility", source.org_id)
        self.assertEqual("America/Los_Angeles", str(source.timezone))
        self.assertEqual(
            "tunnel-ip", source.configured_ssh_tunnel_to_database.ssh_tunnel_server_host
        )
        self.assertEqual("ubuntu", source.secrets.ssh_tunnel_username)
        self.assertEqual(
            "/key", source.configured_ssh_tunnel_to_database.ssh_tunnel_key_path
        )
        self.assertEqual(
            "db-host", source.configured_ssh_tunnel_to_database.database_host
        )
        self.assertEqual(1521, source.configured_ssh_tunnel_to_database.database_port)
        self.assertEqual("db-name", source.secrets.database_db_name)
        self.assertEqual("dbu", source.secrets.database_user)
        self.assertEqual("dbp", source.secrets.database_password)

    def test_can_instantiate_xylem_moulton_niguel_via_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("xylem-moulton-niguel-config.yaml"),
            self.get_fixture_path("xylem-moulton-niguel-secrets.yaml"),
        )
        self.assertEqual(1, len(config._sources))
        source = config._sources[0]
        self.assertEqual("xylem_moulton_niguel", source.type)
        self.assertEqual("my_utility", source.org_id)
        self.assertEqual("America/Los_Angeles", str(source.timezone))
        self.assertEqual(
            "tunnel-ip", source.configured_ssh_tunnel_to_database.ssh_tunnel_server_host
        )
        self.assertEqual("ubuntu", source.secrets.ssh_tunnel_username)
        self.assertEqual(
            "/key", source.configured_ssh_tunnel_to_database.ssh_tunnel_key_path
        )
        self.assertEqual(
            "db-host", source.configured_ssh_tunnel_to_database.database_host
        )
        self.assertEqual(1521, source.configured_ssh_tunnel_to_database.database_port)
        self.assertEqual("db-name", source.secrets.database_db_name)
        self.assertEqual("dbu", source.secrets.database_user)
        self.assertEqual("dbp", source.secrets.database_password)

    def test_can_instantiate_subeca_via_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("subeca-config.yaml"),
            self.get_fixture_path("subeca-secrets.yaml"),
        )
        self.assertEqual(1, len(config._sources))
        source = config._sources[0]
        self.assertEqual("subeca", source.type)
        self.assertEqual("my_utility", source.org_id)
        self.assertEqual("America/Los_Angeles", str(source.timezone))
        self.assertEqual("outputs", source.task_output_controller.output_folder)
        self.assertEqual("my_url", source.api_url)
        self.assertEqual("key", source.secrets.api_key)
        self.assertEqual(1, len(source.storage_sinks))
        self.assertEqual([], config._backfills)
        self.assertIsNone(config.on_failure_sns_notifier())

    def test_can_instantiate_backfills_from_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("beacon-360-config.yaml"),
            self.get_fixture_path("beacon-360-secrets.yaml"),
        )
        self.assertEqual(1, len(config._sources))
        backfills = config.backfills()
        self.assertEqual(2, len(backfills))

        self.assertEqual(datetime(2025, 1, 1, tzinfo=pytz.UTC), backfills[0].start_date)
        self.assertEqual(datetime(2025, 2, 1, tzinfo=pytz.UTC), backfills[0].end_date)
        self.assertEqual(3, backfills[0].interval_days)
        self.assertEqual("15 * * * *", backfills[0].schedule)

        self.assertEqual(
            datetime(2024, 10, 22, tzinfo=pytz.UTC), backfills[1].start_date
        )
        self.assertEqual(datetime(2024, 11, 22, tzinfo=pytz.UTC), backfills[1].end_date)
        self.assertEqual(4, backfills[1].interval_days)

    def test_can_create_adapters(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("all-config.yaml"),
            self.get_fixture_path("all-secrets.yaml"),
        )
        adapters = config.adapters()

        self.assertEqual(6, len(adapters))
        self.assertIn(AclaraAdapter, map(lambda a: type(a), adapters))
        self.assertIn(Beacon360Adapter, map(lambda a: type(a), adapters))
        self.assertIn(MetersenseAdapter, map(lambda a: type(a), adapters))
        self.assertIn(SentryxAdapter, map(lambda a: type(a), adapters))
        self.assertIn(SubecaAdapter, map(lambda a: type(a), adapters))

    def test_can_create_on_failure_notifier(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("beacon-360-config.yaml"),
            self.get_fixture_path("beacon-360-secrets.yaml"),
        )
        notifier = config.on_failure_sns_notifier()
        self.assertIsInstance(notifier, SnsNotifier)
        self.assertEqual("my-sns-arn", notifier.target_arn)

    @patch("amiadapters.config.ConfiguredStorageSink.connection")
    def test_can_create_list_of_data_quality_checks(self, mock_connection):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("all-config.yaml"),
            self.get_fixture_path("all-secrets.yaml"),
        )
        checks = config.sinks()[0].checks()
        self.assertEqual(1, len(checks))


class TestFindConfigAndSecrets(BaseTestCase):

    @patch("pathlib.Path.exists", return_value=True)
    def test_find_config(self, mock_exists):
        path = find_config_yaml()
        expected = (
            pathlib.Path(__file__).joinpath("..", "..", "..", "config.yaml").resolve()
        )
        self.assertEqual(expected, path)

    @patch("pathlib.Path.exists", return_value=False)
    def test_find_config__error_when_no_file(self, mock_exists):
        with self.assertRaises(Exception):
            find_config_yaml()

    @patch("pathlib.Path.exists", return_value=True)
    def test_find_secrets(self, mock_exists):
        path = find_secrets_yaml()
        expected = (
            pathlib.Path(__file__).joinpath("..", "..", "..", "secrets.yaml").resolve()
        )
        self.assertEqual(expected, path)

    @patch("pathlib.Path.exists", return_value=False)
    def test_find_secrets__error_when_no_file(self, mock_exists):
        with self.assertRaises(Exception):
            find_secrets_yaml()

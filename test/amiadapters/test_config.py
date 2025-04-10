from amiadapters.beacon import Beacon360Adapter
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.sentryx import SentryxAdapter

from test.base_test_case import BaseTestCase


class TestConfig(BaseTestCase):

    def test_can_instantiate_sentryx_via_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("sentryx-config.yaml"),
            self.get_fixture_path("sentryx-secrets.yaml"),
        )
        self.assertEqual(1, len(config.sources))
        source = config.sources[0]
        self.assertEqual("sentryx", source.type)
        self.assertEqual("my_utility", source.org_id)
        self.assertEqual("America/Los_Angeles", str(source.timezone))
        self.assertEqual(False, source.use_raw_data_cache)
        self.assertEqual("./output", source.intermediate_output)
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

    def test_can_instantiate_beacon_via_yaml(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("beacon-360-config.yaml"),
            self.get_fixture_path("beacon-360-secrets.yaml"),
        )
        self.assertEqual(1, len(config.sources))
        source = config.sources[0]
        self.assertEqual("beacon_360", source.type)
        self.assertEqual("my_utility", source.org_id)
        self.assertEqual("America/Los_Angeles", str(source.timezone))
        self.assertEqual(True, source.use_raw_data_cache)
        self.assertEqual("./output", source.intermediate_output)
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

    def test_can_create_adapters(self):
        config = AMIAdapterConfiguration.from_yaml(
            self.get_fixture_path("config.yaml"), self.get_fixture_path("secrets.yaml")
        )
        adapters = config.adapters()

        self.assertEqual(2, len(adapters))
        self.assertIn(SentryxAdapter, map(lambda a: type(a), adapters))
        self.assertIn(Beacon360Adapter, map(lambda a: type(a), adapters))

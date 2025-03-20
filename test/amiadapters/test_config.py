import os
from unittest import mock, TestCase

from amiadapters.config import AMIAdapterConfiguration


class TestConfig(TestCase):

    @mock.patch.dict(
        os.environ, 
        {
            "AMI_DATA_OUTPUT_FOLDER": "ad_output_folder", 
            "SENTRYX_API_KEY": "sentryx_key",
            "BEACON_AUTH_USER": "beacon_user",
            "BEACON_AUTH_PASSWORD": "beacon_pass",
        }, 
        clear=True
    )
    def test_can_instantiate_via_env(self):
        config = AMIAdapterConfiguration.from_env()
        self.assertEqual("ad_output_folder", config.output_folder)
        self.assertEqual("sentryx_key", config.sentryx_api_key)
        self.assertEqual("beacon_user", config.beacon_360_user)
        self.assertEqual("beacon_pass", config.beacon_360_password)

import os
from unittest import mock, TestCase

from config import AMIAdapterConfiguration


class TestConfig(TestCase):

    @mock.patch.dict(os.environ, {"AMI_DATA_OUTPUT_FOLDER": "ad_output_folder", "SENTRYX_API_KEY": "sentryx_key"}, clear=True)
    def test_can_instantiate_via_env(self):
        config = AMIAdapterConfiguration.from_env()
        self.assertEqual("ad_output_folder", config.output_folder)
        self.assertEqual("sentryx_key", config.sentryx_api_key)

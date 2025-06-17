import datetime

import pytz

from amiadapters.config import ConfiguredLocalTaskOutputController
from amiadapters.metersense import MetersenseAdapter
from test.base_test_case import BaseTestCase


class TestMetersenseAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = MetersenseAdapter(
            org_id="this-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            configured_task_output_controller=ConfiguredLocalTaskOutputController(
                "/tmp/output"
            ),
            configured_sinks=[],
        )

    def test_init(self):
        self.assertIsNotNone(self.adapter)

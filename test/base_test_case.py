import logging
from pathlib import Path
import unittest

from amiadapters.configuration.models import (
    LocalIntermediateOutputControllerConfiguration,
    NoopMetricsConfiguration,
    PipelineConfiguration,
)


class BaseTestCase(unittest.TestCase):

    FIXTURE_DIR = Path(__file__).parent / "fixtures"
    TEST_PIPELINE_CONFIGURATION = PipelineConfiguration(
        intermediate_output_type=None,
        intermediate_output_s3_bucket=None,
        intermediate_output_dev_profile=None,
        intermediate_output_local_output_path="/tmp/output",
        should_run_post_processor=True,
        should_publish_load_finished_events=False,
    )
    TEST_METRICS_CONFIGURATION = NoopMetricsConfiguration()
    TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION = (
        LocalIntermediateOutputControllerConfiguration("/tmp/output")
    )

    # Silence logging for all tests
    logging.disable(logging.CRITICAL)

    @classmethod
    def load_fixture(cls, filename: str) -> str:
        fixture_path = cls.FIXTURE_DIR / filename
        with open(fixture_path, "r") as f:
            return f.read()

    @classmethod
    def get_fixture_path(cls, filename):
        fixture_path = cls.FIXTURE_DIR / filename
        return fixture_path.resolve()


class MockResponse:
    """
    Mock HTTP response, e.g. from requests.get()
    """

    def __init__(self, json_data, status_code, text=None):
        self.json_data = json_data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self.json_data


def mocked_response_500(*args, **kwargs):
    return MockResponse({}, 500)


def mocked_response_429(*args, **kwargs):
    data = {"args": [None, None, 3]}
    return MockResponse(data, 429)

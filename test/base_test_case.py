from pathlib import Path
import unittest


class BaseTestCase(unittest.TestCase):

    FIXTURE_DIR = Path(__file__).parent / "fixtures"

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

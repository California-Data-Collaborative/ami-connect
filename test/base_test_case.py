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

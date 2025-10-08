from dataclasses import dataclass

from amiadapters.outputs.base import ExtractOutput
from test.base_test_case import BaseTestCase


@dataclass
class DummyDataType:
    a: str
    b: str


class TestLocalTaskOutputController(BaseTestCase):
    def setUp(self):
        self.sample_outputs = {
            "file1.txt": '{"a": 1, "b": 2}\n{"a": 3, "b": 4}',
            "file2.txt": '{"a": 5, "b": 6}',
        }
        self.extract_output = ExtractOutput(self.sample_outputs)

    def test_get_outputs_returns_dict(self):
        outputs = self.extract_output.get_outputs()
        self.assertEqual(outputs, self.sample_outputs)

    def test_from_file_returns_content(self):
        content = self.extract_output.from_file("file1.txt")
        self.assertEqual(content, self.sample_outputs["file1.txt"])

    def test_from_file_returns_none_for_missing_file(self):
        content = self.extract_output.from_file("missing.txt")
        self.assertIsNone(content)

    def test_load_from_file_deserializes_data(self):
        result = self.extract_output.load_from_file("file1.txt", DummyDataType)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].a, 1)
        self.assertEqual(result[0].b, 2)
        self.assertEqual(result[1].a, 3)
        self.assertEqual(result[1].b, 4)

    def test_load_from_file_empty_file_raises_exception(self):
        eo = ExtractOutput({"empty.txt": ""})
        with self.assertRaises(Exception) as context:
            eo.load_from_file("empty.txt", DummyDataType)
        self.assertIn("No data found for file empty.txt", str(context.exception))

    def test_load_from_file_empty_file_allow_empty(self):
        eo = ExtractOutput({"empty.txt": ""})
        result = eo.load_from_file("empty.txt", DummyDataType, allow_empty=True)
        self.assertEqual(result, [])

    def test_load_from_file_missing_file_raises_exception(self):
        with self.assertRaises(Exception):
            self.extract_output.load_from_file("missing.txt", DummyDataType)

    def test_load_from_file_missing_file_allow_empty(self):
        result = self.extract_output.load_from_file(
            "missing.txt", DummyDataType, allow_empty=True
        )
        self.assertEqual(result, [])

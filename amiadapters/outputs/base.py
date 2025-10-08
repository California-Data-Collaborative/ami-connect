from abc import ABC, abstractmethod
import json
from typing import List

from amiadapters.models import GeneralMeterRead
from amiadapters.models import GeneralMeter


class ExtractOutput:
    """
    Abstraction that allows an Extract task to define how many files it outputs.
    """

    def __init__(self, outputs: dict[str, str]):
        self.outputs = outputs

    def get_outputs(self) -> dict[str, str]:
        """
        Return output from Extract task as dictionary of filename to file's contents.
        """
        return self.outputs

    def from_file(self, filename: str) -> str:
        return self.outputs.get(filename)

    def load_from_file(
        self, filename: str, data_type, allow_empty: bool = False
    ) -> List:
        """
        Return output from a file deserialized into data_type, the dataclass that represents rows
        in this file. File is expected to be one JSON-serialized instance of data_type per row.

        By default, fails if the file is empty (this often means extract failed in some way). You can
        change allow_empty to True and get an empty list instead.
        """
        text = self.outputs.get(filename)
        if not text:
            if allow_empty:
                return []
            else:
                raise Exception(f"No data found for file {filename}")
        return [data_type(**json.loads(d)) for d in text.strip().split("\n")]


class BaseTaskOutputController(ABC):
    """
    Controls read and write of intermediate task outputs, i.e. files passed between tasks.
    """

    @abstractmethod
    def write_extract_outputs(self, outputs: ExtractOutput):
        pass

    @abstractmethod
    def read_extract_outputs(self) -> ExtractOutput:
        pass

    @abstractmethod
    def write_transformed_meters(self, meters: List[GeneralMeter]):
        pass

    @abstractmethod
    def read_transformed_meters(self) -> List[GeneralMeter]:
        pass

    @abstractmethod
    def write_transformed_meter_reads(self, reads: List[GeneralMeterRead]):
        pass

    @abstractmethod
    def read_transformed_meter_reads(self) -> List[GeneralMeterRead]:
        pass

    @abstractmethod
    def download_for_path(self, path: str, output_directory: str):
        """
        Given a path to task outputs, download all files to the output_directory. Useful for
        debugging.
        """
        pass

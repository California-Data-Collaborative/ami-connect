from abc import ABC, abstractmethod
from typing import List

from amiadapters.config import ConfiguredStorageSink
from amiadapters.models import GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput


class BaseAMIStorageSink(ABC):
    """
    A storage sink is any place the AMI Connect pipeline
    outputs data. Examples: Snowflake, local storage.
    """

    def __init__(
        self,
        sink_config: ConfiguredStorageSink,
    ):
        self.sink_config = sink_config

    @abstractmethod
    def store_raw(self, run_id: str, extract_outputs: ExtractOutput):
        pass

    @abstractmethod
    def store_transformed(
        self, run_id: str, meters: List[GeneralMeter], reads: List[GeneralMeterRead]
    ):
        pass


class BaseAMIDataQualityCheck(ABC):
    """
    A data quality check for the data stored in this sink. A sink may have many
    data quality checks. They're run together to ensure the data in the sink meets
    standard assumptions we've built into our data model.
    """

    @abstractmethod
    def name(self) -> str:
        """
        Name of this check, hyphenated, used to identify the check in our configuration system.
        """
        pass

    @abstractmethod
    def check(self) -> bool:
        """
        Run the check.

        :return: True if check passes, else False.
        """
        pass

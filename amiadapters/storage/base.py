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

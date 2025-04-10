from abc import ABC, abstractmethod

from amiadapters.config import ConfiguredStorageSink


class BaseAMIStorageSink(ABC):

    def __init__(
        self,
        transformed_meter_file: str,
        transformed_reads_file,
        sink_config: ConfiguredStorageSink,
    ):
        self.transformed_meter_file = transformed_meter_file
        self.transformed_reads_file = transformed_reads_file
        self.sink_config = sink_config

    @abstractmethod
    def store_raw(self):
        pass

    @abstractmethod
    def store_transformed(self):
        pass

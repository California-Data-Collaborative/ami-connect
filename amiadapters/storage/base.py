from abc import ABC, abstractmethod

from amiadapters.config import AMIAdapterConfiguration


class BaseAMIStorageSink(ABC):

    def __init__(self, transformed_meter_file: str, transformed_reads_file):
        self.transformed_meter_file = transformed_meter_file
        self.transformed_reads_file = transformed_reads_file

    @abstractmethod
    def store_raw(self, config: AMIAdapterConfiguration):
        pass

    @abstractmethod
    def store_transformed(self, config: AMIAdapterConfiguration):
        pass

from abc import ABC, abstractmethod

from amiadapters.config import ConfiguredStorageSink
from amiadapters.outputs.base import BaseTaskOutputController


class BaseAMIStorageSink(ABC):

    def __init__(
        self,
        output_controller: BaseTaskOutputController,
        sink_config: ConfiguredStorageSink,
    ):
        self.output_controller = output_controller
        self.sink_config = sink_config

    @abstractmethod
    def store_raw(self, run_id: str):
        pass

    @abstractmethod
    def store_transformed(self, run_id: str):
        pass

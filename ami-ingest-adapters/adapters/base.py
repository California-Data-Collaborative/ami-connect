from abc import ABC, abstractmethod
import dataclasses
import json


class BaseAMIAdapter(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass


class DataclassJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)

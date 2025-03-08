from abc import ABC, abstractmethod


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

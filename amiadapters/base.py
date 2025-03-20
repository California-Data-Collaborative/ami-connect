from abc import ABC, abstractmethod
import dataclasses
from datetime import datetime
import json


class BaseAMIAdapter(ABC):
    """
    Abstraction of an AMI data source. If you're adding a new type of data source,
    you'll inherit from this class and implement its abstract methods. That should
    set you up to include it in our data pipeline.
    """

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass


@dataclasses.dataclass(frozen=True)
class GeneralMeter:
    """
    General model of a Meter and its metadata, which includes information about the
    account and location it's currently associated with.
    """
    meter_id: str
    account_id: str
    location_id: str
    size_inches: str


@dataclasses.dataclass(frozen=True)
class GeneralMeterRead:
    """
    General model of a Meter Read at a point in time. Includes metadata we'd use to join it with
    other data.
    """
    meter_id: str
    account_id: str
    location_id: str
    flowtime: datetime
    raw_value: float
    raw_unit: str


class DataclassJSONEncoder(json.JSONEncoder):
    """
    Helps write data classes into JSON.
    """
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)
    

class GeneralModelJSONEncoder(DataclassJSONEncoder):
    """
    Standardizes how we serialize our general models into JSON.
    """
    def default(self, o):
        # Datetimes use isoformat
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)

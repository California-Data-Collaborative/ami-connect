from abc import ABC, abstractmethod
import dataclasses
from datetime import datetime, timedelta
import json
from typing import List, Tuple

from pytz import timezone
from pytz.tzinfo import DstTzInfo

from amiadapters.storage.base import BaseAMIStorageSink


class BaseAMIAdapter(ABC):
    """
    Abstraction of an AMI data source. If you're adding a new type of data source,
    you'll inherit from this class and implement its abstract methods. That should
    set you up to include it in our data pipeline.
    """

    def __init__(self, storage_sinks: List[BaseAMIStorageSink] = None):
        self.storage_sinks = storage_sinks if storage_sinks is not None else []

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def extract(self, extract_range_start: datetime, extract_range_end: datetime):
        """
        Extract data from an AMI data source.

        :extract_range_start datetime: start of meter read datetime range for which we'll extract data
        :extract_range_end datetime:   end of meter read datetime range for which we'll extract data
        """
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def calculate_backfill_range(self) -> Tuple[datetime, datetime]:
        pass

    def load_raw(self):
        for sink in self.storage_sinks:
            sink.store_raw()

    def load_transformed(self):
        for sink in self.storage_sinks:
            sink.store_transformed()

    def datetime_from_iso_str(
        self, datetime_str: str, org_timezone: DstTzInfo
    ) -> datetime:
        if datetime_str:
            result = datetime.fromisoformat(datetime_str)
            tz = org_timezone if org_timezone is not None else timezone("UTC")
            result = result.replace(tzinfo=tz)
        else:
            result = None
        return result

    def map_meter_size(self, size: str) -> str:
        """
        Map an AMI data provider meter's size to one
        of our generalized values. Return None if it can't be mapped.
        """
        mapping = {
            '3/8"': "0.375",
        }
        return mapping.get(size)

    def map_unit_of_measure(self, unit_of_measure: str) -> str:
        """
        Map an AMI data provider meter read's unit of measure to one
        of our generalized values. Return None if it can't be mapped.
        """
        mapping = {
            "CF": GeneralMeterUnitOfMeasure.CUBIC_FEET,
            "CCF": GeneralMeterUnitOfMeasure.HUNDRED_CUBIC_FEET,
            "Gallon": GeneralMeterUnitOfMeasure.GALLON,
        }
        return mapping.get(unit_of_measure)

    def validate_extract_range(
        self, extract_range_start: datetime, extract_range_end: datetime
    ):
        if extract_range_start is None or extract_range_end is None:
            raise Exception(
                f"Expected range start and end, got extract_range_start={extract_range_start} and extract_range_end={extract_range_end}"
            )
        if extract_range_end < extract_range_start:
            raise Exception(
                f"Range start must be before end, got extract_range_start={extract_range_start} and extract_range_end={extract_range_end}"
            )


class GeneralMeterUnitOfMeasure:
    """
    Normalized values for a meter's unit of measure. All AMI-provided
    values should be mapped to one of these.
    """

    CUBIC_FEET = "CF"
    HUNDRED_CUBIC_FEET = "CCF"
    GALLON = "Gallon"


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


def default_date_range(start: datetime, end: datetime):
    default_number_of_days = 2

    if start is None and end is None:
        end = datetime.now()
        start = end - timedelta(days=default_number_of_days)
    elif start is not None and end is None:
        end = start + timedelta(days=default_number_of_days)
    elif start is None and end is not None:
        start = end - timedelta(days=default_number_of_days)

    return start, end

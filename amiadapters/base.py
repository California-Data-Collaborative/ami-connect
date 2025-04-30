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
    def extract(
        self, run_id: str, extract_range_start: datetime, extract_range_end: datetime
    ):
        """
        Extract data from an AMI data source.

        :run_id: identifier for this run of the pipeline, is used to store intermediate output files
        :extract_range_start datetime: start of meter read datetime range for which we'll extract data
        :extract_range_end datetime:   end of meter read datetime range for which we'll extract data
        """
        pass

    @abstractmethod
    def transform(self, run_id: str):
        """
        Transform data from an AMI data source into the generalized format.

        :run_id: identifier for this run of the pipeline, is used to find and store intermediate output files
        """
        pass

    @abstractmethod
    def calculate_backfill_range(self) -> Tuple[datetime, datetime]:
        """
        Used by orchestration code when automated backfills are run. Returns a date range
        for which we should backfill data. Used by the automated backfills to determine their
        extract start and end dates.
        """
        pass

    def load_raw(self, run_id: str):
        """
        Stores raw data from extract step into all storage sinks.

        :run_id: identifier for this run of the pipeline, is used to find intermediate output files
        """
        for sink in self.storage_sinks:
            sink.store_raw(run_id)

    def load_transformed(self, run_id: str):
        """
        Stores transformed data from transform step into all storage sinks.

        :run_id: identifier for this run of the pipeline, is used to find intermediate output files
        """
        for sink in self.storage_sinks:
            sink.store_transformed(run_id)

    def datetime_from_iso_str(
        self, datetime_str: str, org_timezone: DstTzInfo
    ) -> datetime:
        """
        Parse an ISO format date string into a datetime object with timezone.
        Uses org_timezone from arguments if provided or defaults to UTC.
        """
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

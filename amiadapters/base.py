from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Tuple

from pytz import timezone
from pytz.tzinfo import DstTzInfo

from amiadapters.config import Backfill
from amiadapters.outputs.base import BaseTaskOutputController
from amiadapters.outputs.local import LocalTaskOutputController
from amiadapters.outputs.s3 import S3TaskOutputController
from amiadapters.storage.base import BaseAMIStorageSink
from amiadapters.storage.snowflake import SnowflakeStorageSink


class BaseAMIAdapter(ABC):
    """
    Abstraction of an AMI data source. If you're adding a new type of data source,
    you'll inherit from this class and implement its abstract methods. That should
    set you up to include it in our data pipeline.
    """

    def __init__(
        self,
        org_id: str,
        org_timezone: DstTzInfo,
        task_output_controller: BaseTaskOutputController,
        storage_sinks: List[BaseAMIStorageSink] = None,
    ):
        self.org_id = org_id
        self.org_timezone = org_timezone
        self.output_controller = task_output_controller
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

    def calculate_extract_range(
        self, start: datetime, end: datetime, backfill_params: Backfill = None
    ) -> Tuple[datetime, datetime]:
        """
        Returns a date range for which we should extract data. Automatically determines if
        this is a backfill and calculates a range based on backfill parameters. Otherwise calculates
        a range for extracting recent data.

        min_date: caps how far back we will backfill
        max_date: caps how far forward we will backfill
        interval_days: the number of days of data we should backfill
        """
        range_calculator = ExtractRangeCalculator(self.org_id, self.storage_sinks)
        calculated_start, calculated_end = range_calculator.calculate_extract_range(
            start, end, backfill_params=backfill_params
        )
        self._validate_extract_range(calculated_start, calculated_end)
        return calculated_start, calculated_end

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

    def _validate_extract_range(
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

    @staticmethod
    def create_task_output_controller(configured_task_output_controller, org_id):
        """
        Create a task output controller from the config object.
        """
        from amiadapters.config import ConfiguredTaskOutputControllerType

        if (
            configured_task_output_controller.type
            == ConfiguredTaskOutputControllerType.LOCAL
        ):
            return LocalTaskOutputController(
                configured_task_output_controller.output_folder, org_id
            )
        elif (
            configured_task_output_controller.type
            == ConfiguredTaskOutputControllerType.S3
        ):
            return S3TaskOutputController(
                configured_task_output_controller.s3_bucket_name,
                org_id,
            )
        raise ValueError(
            f"Task output configuration with invalid type {configured_task_output_controller.type}"
        )


class GeneralMeterUnitOfMeasure:
    """
    Normalized values for a meter's unit of measure. All AMI-provided
    values should be mapped to one of these.
    """

    CUBIC_FEET = "CF"
    HUNDRED_CUBIC_FEET = "CCF"
    GALLON = "Gallon"


class ExtractRangeCalculator:
    """
    Helper class for calculating the start and end date for an org's Extract
    task.
    """

    def __init__(self, org_id: str, storage_sinks: List[BaseAMIStorageSink]):
        self.org_id = org_id
        self.storage_sinks = storage_sinks

    def calculate_extract_range(
        self, start: datetime, end: datetime, backfill_params: Backfill
    ) -> Tuple[datetime, datetime]:
        """
        Returns a date range for which we should extract data. Automatically determines if
        this is a backfill and calculates a range based on backfill parameters. Otherwise calculates
        a range for extracting recent data.

        min_date: caps how far back we will backfill
        max_date: caps how far forward we will backfill
        interval_days: the number of days of data we should backfill
        """
        if backfill_params is not None:
            return self._calculate_backfill_range(
                backfill_params.start_date,
                backfill_params.end_date,
                backfill_params.interval_days,
            )
        else:
            if isinstance(start, str):
                start = datetime.fromisoformat(start)
            if isinstance(end, str):
                end = datetime.fromisoformat(end)

            if start is None or end is None:
                start, end = self._default_date_range(start, end)

            return start, end

    def _calculate_backfill_range(
        self, min_date: datetime, max_date: datetime, interval_days: int
    ) -> Tuple[datetime, datetime]:
        """
        Used by orchestration code when automated backfills are run. Returns a date range
        for which we should backfill data. Used by the automated backfills to determine their
        extract start and end dates.

        min_date: caps how far back we will backfill
        max_date: caps how far forward we will backfill
        interval_days: the number of days of data we should backfill
        """
        snowflake_sink = [
            s for s in self.storage_sinks if isinstance(s, SnowflakeStorageSink)
        ]
        if not snowflake_sink:
            raise Exception(
                "Could not calculate backfill range, no Snowflake sink available"
            )

        sink = snowflake_sink[0]
        end = sink.get_oldest_meter_read_time(self.org_id, min_date, max_date)
        if not end:
            raise Exception(
                f"No backfillable days found between {min_date} and {max_date} for {self.org_id}, consider removing this backfill from the configuration."
            )
        start = end - timedelta(days=interval_days)
        return start, end

    def _default_date_range(self, start: datetime, end: datetime):
        default_number_of_days = 2

        if start is None and end is None:
            end = datetime.now()
            start = end - timedelta(days=default_number_of_days)
        elif start is not None and end is None:
            end = start + timedelta(days=default_number_of_days)
        elif start is None and end is not None:
            start = end - timedelta(days=default_number_of_days)

        return start, end

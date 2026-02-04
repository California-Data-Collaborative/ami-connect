from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import List, Tuple

from pytz import timezone
from pytz.tzinfo import DstTzInfo

from amiadapters.configuration.models import BackfillConfiguration
from amiadapters.configuration.models import ConfiguredStorageSink
from amiadapters.configuration.models import ConfiguredStorageSinkType
from amiadapters.configuration.models import IntermediateOutputType
from amiadapters.configuration.models import MetricsConfigurationBase
from amiadapters.configuration.models import PipelineConfiguration
from amiadapters.events.base import EventPublisher
from amiadapters.metrics.base import Metrics
from amiadapters.outputs.base import ExtractOutput
from amiadapters.outputs.local import LocalTaskOutputController
from amiadapters.outputs.s3 import S3TaskOutputController
from amiadapters.storage.base import BaseAMIStorageSink
from amiadapters.storage.snowflake import SnowflakeStorageSink, RawSnowflakeLoader

logger = logging.getLogger(__name__)


@dataclass
class ScheduledExtract:
    """
    Used to configure a scheduled extract for a source using this adapter.
    """

    name: str = "standard"
    lag: timedelta = timedelta(days=0)
    interval: timedelta = timedelta(days=2)
    schedule_crontab: str = "0 12 * * *"


# Most adapters will use this standard daily extract with the default values
STANDARD_DAILY_SCHEDULED_EXTRACT = ScheduledExtract()


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
        pipeline_configuration: PipelineConfiguration,
        configured_task_output_controller,
        configured_metrics: MetricsConfigurationBase,
        configured_sinks: List[ConfiguredStorageSink] = None,
        raw_snowflake_loader: RawSnowflakeLoader = None,
    ):
        """
        The code takes a shortcut: raw_snowflake_loader is used for any Snowflake sink. We may
        want to improve this someday because it is not very generic, but it works for now.
        """
        self.org_id = org_id
        self.org_timezone = org_timezone
        self.pipeline_configuration = pipeline_configuration
        self.output_controller = self._create_task_output_controller(
            configured_task_output_controller, org_id
        )
        self.metrics = Metrics.from_configuration(configured_metrics)
        self._base_adapter_metrics = self._BaseAdapterMetrics(
            self.metrics, self.org_id, type(self).__name__
        )
        self.storage_sinks = self._create_storage_sinks(
            configured_sinks,
            self.org_id,
            self.org_timezone,
            raw_snowflake_loader,
            self.metrics,
        )

    @abstractmethod
    def name(self) -> str:
        pass

    def scheduled_extracts(self) -> List[ScheduledExtract]:
        """
        List of scheduled extractions that must be run for a source that uses
        this adapter type in order for their AMI data set to be complete.
        Most adapter types will run a single "standard" daily extract that retrieves
        the last ~2 days of data. Others will schedule additional extracts, e.g. on a lag that
        re-retrieves last week's data. Adapters should override this function if they don't want
        the default behavior.
        """
        return [STANDARD_DAILY_SCHEDULED_EXTRACT]

    def extract_and_output(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        """
        Public function for extract stage.
        """
        with self._base_adapter_metrics.extract_timer():
            logger.info(
                f"Extracting data for range {extract_range_start} to {extract_range_end}"
            )
            # Use adapter implementation to extract data
            extracted_output = self._extract(
                run_id, extract_range_start, extract_range_end
            )
            # Output to intermediate storage, e.g. S3 or local files
            self.output_controller.write_extract_outputs(run_id, extracted_output)

    @abstractmethod
    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:
        """
        Extract data from an AMI data source as defined by the implementing adapter.

        :run_id: identifier for this run of the pipeline, is used to store intermediate output files
        :extract_range_start datetime:  start of meter read datetime range for which we'll extract data
        :extract_range_end datetime:    end of meter read datetime range for which we'll extract data
        :return: ExtractOutput instance that defines name and contents of extracted outputs
        """
        pass

    def transform_and_output(self, run_id: str):
        """
        Public function for transform stage.
        """
        with self._base_adapter_metrics.transform_timer():
            # Read extract outputs from intermediate storage
            extract_outputs = self.output_controller.read_extract_outputs(run_id)

            # Transform
            transformed_meters, transformed_reads = self._transform(
                run_id, extract_outputs
            )
            self._base_adapter_metrics.mark_meters_transformed(len(transformed_meters))
            self._base_adapter_metrics.mark_reads_transformed(len(transformed_reads))

            # Write transformed outputs to intermediate storage
            logger.info(
                f"Transformed {len(transformed_meters)} meters for org {self.org_id}"
            )
            self.output_controller.write_transformed_meters(run_id, transformed_meters)
            self.output_controller.write_transformed_meter_reads(
                run_id, transformed_reads
            )

    @abstractmethod
    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        """
        Transform data from an AMI data source into the generalized format.

        :run_id: identifier for this run of the pipeline
        :extract_outputs: Data from the extract stage expected to be the same as the output of the extract stage.
        """
        pass

    def calculate_extract_range(
        self,
        specified_start: datetime,
        specified_end: datetime,
        interval: timedelta,
        lag: timedelta,
        backfill_params: BackfillConfiguration = None,
    ) -> Tuple[datetime, datetime]:
        """
        Returns a date range for which we should extract data. Automatically determines if
        this is a backfill and calculates a range based on backfill parameters. Otherwise calculates
        a range for extracting recent data.

        specified_start: explicitly stated start time, overrides other settings if this is not a backfill
        specified_end: explicitly stated end time, overrides other settings if this is not a backfill
        interval: if specified_start or specified_end is None, interval tells us how big the extract range should be.
        lag: specified_start and specified_end are None, lag tells us if we should extract something farther
            back than the most recent data. Defaults to a lag of zero, i.e. "extract the most recent data".
        """
        range_calculator = ExtractRangeCalculator(self.org_id, self.storage_sinks)
        calculated_start, calculated_end = range_calculator.calculate_extract_range(
            specified_start,
            specified_end,
            interval,
            lag,
            backfill_params=backfill_params,
        )
        self._validate_extract_range(calculated_start, calculated_end)
        return calculated_start, calculated_end

    def load_raw(self, run_id: str):
        """
        Stores raw data from extract step into all storage sinks.

        :run_id: identifier for this run of the pipeline, is used to find intermediate output files
        """
        with self._base_adapter_metrics.load_raw_timer():
            extract_outputs = self.output_controller.read_extract_outputs(run_id)
            for sink in self.storage_sinks:
                sink.store_raw(run_id, extract_outputs)

    def load_transformed(self, run_id: str):
        """
        Stores transformed data from transform step into all storage sinks.

        :run_id: identifier for this run of the pipeline, is used to find intermediate output files
        """
        with self._base_adapter_metrics.load_transformed_timer():
            meters = self.output_controller.read_transformed_meters(run_id)
            reads = self.output_controller.read_transformed_meter_reads(run_id)
            for sink in self.storage_sinks:
                sink.store_transformed(run_id, meters, reads)

    def post_process(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        """
        Post processing step after loading data into storage sinks. Includes
        sink-specific post processing, e.g. queries that run on the loaded data to refresh downstream tables.

        Also can be configured to publish an event to a message queue saying we finished loading data.
        """
        with self._base_adapter_metrics.post_process_timer():
            # Sink post processing, e.g. leak detection queries
            if self.pipeline_configuration.should_run_post_processor:
                logger.info("Running sink post processor")
                for sink in self.storage_sinks:
                    sink_post_process_end_date = datetime.today()
                    sink_post_process_start_date = (
                        sink_post_process_end_date - timedelta(days=30)
                    )
                    logger.info(
                        f"Running post processor for sink {sink.__class__.__name__} from {sink_post_process_start_date} to {sink_post_process_end_date}"
                    )
                    sink.exec_postprocessor(
                        run_id, sink_post_process_start_date, sink_post_process_end_date
                    )
            else:
                logger.info("Skipping sink post processor as configured")

            # Publish event saying we finished loading data
            if self.pipeline_configuration.should_publish_load_finished_events:
                logger.info("Publishing load finished event")
                event_publisher = EventPublisher()
                event_publisher.publish_load_finished_event(
                    run_id=run_id,
                    org_id=self.org_id,
                    start_date=extract_range_start,
                    end_date=extract_range_end,
                )
            else:
                logger.info("Skipping load finished event publication as configured")

    def datetime_from_iso_str(
        self, datetime_str: str, timezone_of_measurement: DstTzInfo
    ) -> datetime:
        """
        Parse an ISO format date string into a datetime object with an offset.
        Uses timezone_of_measurement from arguments if provided or defaults to UTC.
        Returned datetimes should never be naive - they should always have an offset.
        If the datetime_str already includes an offset, do not alter it.

        We must take care to interpret input datetime strings with no offset information - their
        offset should come from the org_timezone, which tells us which timezone the datetime was produced in.
        """
        # TODO remove allowlist after all data and code is fixed
        if self.org_id in (
            "test-org",
            "cadc_long_beach",
            "cadc_thousand_oaks",
            "cadc_valley_county",
            "current_bakman",
        ):
            # New and fixed way of creating datetime w/ offset
            if datetime_str:
                result = datetime.fromisoformat(datetime_str)
                if result.tzinfo is None:
                    tz = (
                        timezone_of_measurement
                        if timezone_of_measurement is not None
                        else timezone("UTC")
                    )
                    result = tz.localize(result)
            else:
                result = None
            return result
        else:
            # Old and broken way of creating datetime w/ offset
            if datetime_str:
                result = datetime.fromisoformat(datetime_str)
                tz = (
                    timezone_of_measurement
                    if timezone_of_measurement is not None
                    else timezone("UTC")
                )
                result = result.replace(tzinfo=tz)
            else:
                result = None
            return result

    def map_meter_size(self, size: str) -> str:
        """
        Map an AMI data provider meter's size to one
        of our generalized values. Return None if it can't be mapped.
        """
        if size is None:
            return None
        mapping = {
            '3/8"': "0.375",
            "3/8": "0.375",
            "0.625": "0.625",
            "5/8in": "0.625",
            '5/8"': "0.625",
            "5/8": "0.625",
            "0.75": "0.75",
            "3/4": "0.75",
            "3/4in": "0.75",
            '3/4"': "0.75",
            "1": "1",
            "1.0": "1",
            "1in": "1",
            '1"': "1",
            "1.5": "1.5",
            '1.5"': "1.5",
            "1.5in": "1.5",
            "2": "2",
            "2.0": "2",
            "2in": "2",
            '2"': "2",
            "2.5": "2.5",
            "3": "3",
            "3.0": "3",
            '3"': "3",
            "3in": "3",
            "4": "4",
            "4.0": "4",
            '4"': "4",
            "4in": "4",
            "6": "6",
            "6.0": "6",
            '6"': "6",
            "6in": "6",
            "8": "8",
            "8.0": "8",
            '8"': "8",
            "10": "10",
            '10"': "10",
            "10.0": "10",
            "12": "12",
            "12.0": "12",
            '12"': "12",
            "5/8x3/4": "0.625x0.75",
            "5/8 x 3/4": "0.625x0.75",
            "5/8x3/4in": "0.625x0.75",
            "W-TSCR3": "3",
            "W-TSC4": "4",
            "Virtual": "Virtual",
            "W-DSC1.5": "1.5",
            "W-TRB2": "2",
            "W-TRB3": "3",
            "W-CMP8": "8",
            "W-DISC1": "1",
            "W-CMP4": "4",
            "W-CMP3": "3",
            "W-TRB6": "6",
            "W-TRB8": "8",
            "W-TSC3": "3",
            "W-RDSC2": "2",
            "W-RUM8": "8",
            "W-DISC34": "0.75",
            "W-DISC2": "2",
            "W-TRB1.5": "1.5",
            "W-FM6": "6",
            "W-FM10": "10",
            "W-FM4": "4",
            "W-FM8": "8",
            "W-CMP6": "6",
            "W-UM10": "10",
            "W-UM6": "6",
            "W-RTRB8": "8",
            "W-TRB4": "4",
            "W-UM8": "8",
        }
        size = size.strip()
        result = mapping.get(size)
        if size is not None and result is None:
            logger.info(f"Unable to map meter size: {size}")
        return result

    def map_reading(
        self, reading: float, original_unit_of_measure: str
    ) -> Tuple[float, str]:
        """
        All readings values should be mapped to CF.
        """
        if reading is None:
            return None, None

        if original_unit_of_measure is None:
            return reading, None

        multiplier = 1
        match original_unit_of_measure.upper():
            case GeneralMeterUnitOfMeasure.CUBIC_FEET:
                multiplier = 1
            case GeneralMeterUnitOfMeasure.HUNDRED_CUBIC_FEET:
                multiplier = 100
            case GeneralMeterUnitOfMeasure.GALLON | GeneralMeterUnitOfMeasure.GALLONS:
                multiplier = 0.133680546  # 1 / 7.48052
            case GeneralMeterUnitOfMeasure.KILO_GALLON:
                multiplier = 133.680546  # 1000 * 1 / 7.48052
            case _:
                raise ValueError(
                    f"Unrecognized unit of measure: {original_unit_of_measure}"
                )

        # 8 was picked arbitrarily, a balance between our Gallon multiplier and a precision
        # that reflects increases in a fraction of a gallon
        value = round(reading * multiplier, 8)

        return value, GeneralMeterUnitOfMeasure.CUBIC_FEET

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
    def _create_task_output_controller(configured_task_output_controller, org_id):
        """
        Create a task output controller from the config object.
        """
        if configured_task_output_controller.type == IntermediateOutputType.LOCAL.value:
            return LocalTaskOutputController(
                configured_task_output_controller.output_folder, org_id
            )
        elif configured_task_output_controller.type == IntermediateOutputType.S3.value:
            return S3TaskOutputController(
                configured_task_output_controller.s3_bucket_name,
                org_id,
                aws_profile_name=configured_task_output_controller.dev_aws_profile_name,
            )
        raise ValueError(
            f"Task output configuration with invalid type {configured_task_output_controller.type}"
        )

    @staticmethod
    def _create_storage_sinks(
        configured_sinks: List[ConfiguredStorageSink],
        org_id: str,
        org_timezone: DstTzInfo,
        raw_snowflake_loader: RawSnowflakeLoader,
        metrics: Metrics,
    ) -> List[BaseAMIStorageSink]:
        result = []
        configured_sinks = configured_sinks if configured_sinks else []
        for sink in configured_sinks:
            if sink.type == ConfiguredStorageSinkType.SNOWFLAKE.value:
                result.append(
                    SnowflakeStorageSink(
                        org_id,
                        org_timezone,
                        sink,
                        raw_snowflake_loader,
                        metrics,
                    )
                )
        return result

    class _BaseAdapterMetrics:
        """
        Helper class for tracking metrics relevant to all adapters.
        """

        def __init__(self, metrics: Metrics, org_id: str, adapter_type: str):
            self.metrics = metrics
            self.org_id = org_id
            self.adapter_type = adapter_type

        def extract_timer(self):
            return self.metrics.timed_task(
                "adapter.extract.duration_seconds",
                tags={
                    "org_id": self.org_id,
                    "adapter_type": self.adapter_type,
                },
            )

        def transform_timer(self):
            return self.metrics.timed_task(
                "adapter.transform.duration_seconds",
                tags={
                    "org_id": self.org_id,
                    "adapter_type": self.adapter_type,
                },
            )

        def mark_meters_transformed(self, count: int):
            self.metrics.incr(
                "adapter.transform.meters_transformed",
                count,
                tags={"org_id": self.org_id, "adapter_type": self.adapter_type},
            )

        def mark_reads_transformed(self, count: int):
            self.metrics.incr(
                "adapter.transform.reads_transformed",
                count,
                tags={"org_id": self.org_id, "adapter_type": self.adapter_type},
            )

        def load_raw_timer(self):
            return self.metrics.timed_task(
                "adapter.load_raw.duration_seconds",
                tags={
                    "org_id": self.org_id,
                    "adapter_type": self.adapter_type,
                },
            )

        def load_transformed_timer(self):
            return self.metrics.timed_task(
                "adapter.load_transformed.duration_seconds",
                tags={
                    "org_id": self.org_id,
                    "adapter_type": self.adapter_type,
                },
            )

        def post_process_timer(self):
            return self.metrics.timed_task(
                "adapter.post_process.duration_seconds",
                tags={
                    "org_id": self.org_id,
                    "adapter_type": self.adapter_type,
                },
            )


class GeneralMeterUnitOfMeasure:
    """
    Normalized values for a meter's unit of measure.
    """

    CUBIC_FEET = "CF"
    HUNDRED_CUBIC_FEET = "CCF"
    GALLON = "GALLON"
    GALLONS = "GALLONS"
    KILO_GALLON = "KGAL"


class ExtractRangeCalculator:
    """
    Helper class for calculating the start and end date for an org's Extract
    task.
    """

    def __init__(
        self,
        org_id: str,
        storage_sinks: List[BaseAMIStorageSink],
    ):
        """
        org_id: the org ID
        storage_sinks: sinks configured for this run. We use the Snowflake sink if we need to smartly calculate a range for backfills.
        """
        self.org_id = org_id
        self.storage_sinks = storage_sinks

    def calculate_extract_range(
        self,
        specified_start: datetime,
        specified_end: datetime,
        interval: timedelta,
        lag: timedelta,
        backfill_params: BackfillConfiguration,
    ) -> Tuple[datetime, datetime]:
        """
        Returns a date range for which we should extract data. Automatically determines if
        this is a backfill and calculates a range based on backfill parameters. Otherwise calculates
        a range for extracting recent data.

        specified_start: explicitly stated start time, overrides other settings if this is not a backfill
        specified_end: explicitly stated end time, overrides other settings if this is not a backfill
        interval: if specified_start or specified_end is None, interval tells us how big the extract range should be.
        lag: specified_start and specified_end are None, lag tells us if we should extract something farther
            back than the most recent data. Defaults to a lag of zero, i.e. "extract the most recent data".
        """
        if backfill_params is not None:
            # This is a backfill, use the backfill config to find the range
            return self._calculate_backfill_range(
                backfill_params.start_date,
                backfill_params.end_date,
                backfill_params.interval_days,
            )
        else:
            # Make sure our start and end are of type datetime
            if isinstance(specified_start, str):
                specified_start = datetime.fromisoformat(specified_start.strip())
            if isinstance(specified_end, str):
                specified_end = datetime.fromisoformat(specified_end.strip())

            # If start and end are explicitly specified, just use them
            if specified_start is not None and specified_end is not None:
                start, end = specified_start, specified_end
            else:
                # Otherwise we have to calculate the range
                if specified_start is None and specified_end is None:
                    end = datetime.now() - lag
                    start = end - interval
                elif specified_start is not None and specified_end is None:
                    end = specified_start + interval
                    start = specified_start
                elif specified_start is None and specified_end is not None:
                    end = specified_end
                    start = specified_end - interval

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
        end = sink.calculate_end_of_backfill_range(self.org_id, min_date, max_date)
        if not end:
            raise Exception(
                f"No backfillable days found between {min_date} and {max_date} for {self.org_id}, consider removing this backfill from the configuration."
            )
        start = end - timedelta(days=interval_days)
        return start, end

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import json
import logging
import re
from typing import List, Tuple

from amiadapters.adapters.base import (
    BaseAMIAdapter,
    ScheduledExtract,
)
from amiadapters.adapters.s3_drop import (
    download_csv_rows,
    list_drop_files,
    s3_client_from_keys,
    select_files_for_range,
)
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

logger = logging.getLogger(__name__)


# Itron emits max-value error codes from a 32-bit register for faulted
# channels: 4294967294 (2**32 - 2) was observed on 296 rows across 68 meters
# in the 2026-07 full extract (register file only), and neighboring codes like
# 2**32 - 1 are equally plausible. Real values sit orders of magnitude lower
# (largest genuine register in 485k rows: ~97.8M CF), so anything at or above
# this threshold is a fault code, not a reading. Rows are kept in the raw
# extract and base tables for the historical record but excluded from
# transformed reads so impossible values never reach READINGS.
READ_VALUE_ERROR_THRESHOLD = 4_000_000_000.0

# Filenames look like rosevillecityof_Register_202607.csv (month suffix) or,
# per our request to Roseville, rosevillecityof_Interval_20260616_20260619.csv
# (date-range suffix). Matched against the S3 key's basename.
FILENAME_PATTERN = re.compile(
    r"(?P<type>register|interval)_(?P<dates>\d{6}|\d{8}_\d{8})\.csv$",
    re.IGNORECASE,
)


@dataclass
class ItronRosevilleRegisterRead:
    """
    Representation of a row in a Register CSV delivered by Roseville.
    Register reads are cumulative meter readings at ~8-hour cadence.

    NOTE: We make the attribute names match the column names in the CSV
    for code convenience.
    """

    Timestamp: str
    Read_Value: str
    Meter_Serial_Number: str
    EndpointID: str
    Location_ID: str
    Meter_Install_Date: str
    Read_Units: str


@dataclass
class ItronRosevilleIntervalRead:
    """
    Representation of a row in an Interval CSV delivered by Roseville.
    Interval reads are hourly consumption; the Timestamp marks the END of the
    measured hour (verified empirically against register diffs: hour-ending
    alignment reconciles 99.6% of 8-hour windows exactly, hour-starting only 39.7%).

    NOTE: We make the attribute names match the column names in the CSV
    for code convenience.
    """

    Timestamp: str
    Read_Value: str
    Meter_Serial_Number: str
    EndpointID: str
    Location_ID: str
    Meter_Install_Date: str
    Read_Units: str


class ItronRosevilleAdapter(BaseAMIAdapter):
    """
    AMI Adapter for City of Roseville's Itron data.

    Roseville IT exports two CSV views of Itron AMI data — Register (cumulative
    reads, 8-hour cadence) and Interval (hourly consumption) — and delivers them
    via Informatica into a CaDC-owned S3 prefix. This is the first adapter whose
    source is a utility-pushed S3 file drop rather than a vendor API/SFTP/DB;
    the transport-level pieces that are not Roseville-specific live in
    amiadapters/adapters/s3_drop.py for reuse by future S3-drop sources.

    This adapter was built specially for Roseville and is not compatible with
    other utilities: the CSVs are Roseville's own database views, NOT Itron's
    native ChoiceConnect export format. An Itron utility onboarding through
    Itron's standard hosted-SFTP path (e.g. Montecito's setup) would need a
    different adapter.

    Identifier mapping (each adapter picks its device_id, see GeneralMeter):
    - device_id = meter_id = Meter_Serial_Number. Every read row carries the
      serial, meter:endpoint:location is 1:1 in the data, and a physical meter
      swap correctly starts a new device under the METERS SCD2 history.
    - endpoint_id = EndpointID, Roseville's Itron radio identifier (an OID,
      prefix 2.16.840.1.114416 = Itron's registered arc). Same convention as
      xylem_moulton_niguel, which maps its Itron radio fields ert_id ->
      endpoint_id; that adapter keys reads by radio (encid) instead because
      MNWD's reads tables only carry the radio id.

    Notable properties of the feed:
    - Timestamps are Pacific local time, format "MM/DD/YYYY HH:MM:SS.ffffff"
      (Roseville confirmed all timestamps are Pacific, 2026-06-03).
    - Read_Units carries an Itron commodity suffix ("CF_WAT" = cubic feet,
      water); the suffix is stripped and the remaining unit normalized via
      map_reading.
    - account_id is permanently unavailable (Roseville's AMI system does not
      receive account info from CIS, confirmed 2026-06-10). Location_ID equals
      Cayenta's SERVICE_POINT ("{LOCATION_NO}_{SERVICE_SEQUENCE}") and is the
      join key to billing data on the CaDC side.
    - Roseville delivers a rolling ~3-day "correction window" of un-finalized
      reads that may be re-sent with corrections. In transform, files are
      processed oldest-LastModified-first so the most recently delivered value
      wins for a given (meter, timestamp) in READINGS.
    - The prefix may contain an archive/ subfolder of old files; S3 listing
      uses Delimiter="/" so only top-level keys are considered.

    Known limitations:
    - DST fall-back: timestamps are wall-clock with no offset, so the repeated
      01:00 hour on the November transition produces two indistinguishable
      rows per meter; one overwrites the other (counted in the transform's
      "overwritten_differing" log counter). Every local-time adapter in this
      repo shares this property. The lost hour is recoverable in aggregate
      from register diffs.
    - Raw base tables dedupe re-delivered rows with the framework's per-column
      max() MERGE, which is lexicographic on VARCHAR — the raw record keeps
      *a* value for each (meter, timestamp), not necessarily the newest.
      READINGS (via transform ordering) is the corrections-accurate record.
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        pipeline_configuration,
        s3_bucket,
        s3_prefix,
        s3_region,
        aws_access_key_id,
        aws_secret_access_key,
        configured_task_output_controller,
        configured_metrics,
        configured_sinks,
        s3_client=None,
    ):
        self.s3_bucket = s3_bucket
        # Normalize the trailing slash: we list with Delimiter="/", so without
        # it every key under "prefix/" rolls into CommonPrefixes and the
        # extract silently matches zero files.
        self.s3_prefix = s3_prefix if s3_prefix.endswith("/") else s3_prefix + "/"
        self.s3_region = s3_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        # Injectable for tests. In production this stays None until _extract
        # runs — adapters are constructed at Airflow DAG-parse time, so the
        # constructor must not do network or client setup work.
        self._s3_client = s3_client
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_metrics,
            configured_sinks,
            ITRON_ROSEVILLE_RAW_SNOWFLAKE_LOADER,
        )

    def name(self) -> str:
        return f"itron-roseville-{self.org_id}"

    def scheduled_extracts(self) -> List[ScheduledExtract]:
        """
        Roseville pushes a file per day covering a rolling ~3-day window. We use
        a 4-day extract window so a file pushed late (or a day skipped on our
        side) is still picked up by filename-date overlap.
        """
        return [ScheduledExtract(interval=timedelta(days=4))]

    def _get_s3_client(self):
        if self._s3_client is None:
            # The bucket lives in the CaDC AWS account, not the ami-connect
            # account, so the EC2 instance role can't read it. We authenticate
            # with a read-only IAM user's keys from this source's secrets.
            self._s3_client = s3_client_from_keys(
                self.s3_region, self.aws_access_key_id, self.aws_secret_access_key
            )
        return self._s3_client

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:
        logger.info(
            f"Listing s3://{self.s3_bucket}/{self.s3_prefix} for files between {extract_range_start} and {extract_range_end}"
        )
        s3 = self._get_s3_client()

        files = list_drop_files(s3, self.s3_bucket, self.s3_prefix)
        keys_by_type = select_files_for_range(
            files, FILENAME_PATTERN, extract_range_start, extract_range_end
        )
        logger.info(
            f"Found {len(files)} keys, matched register={keys_by_type.get('register', [])} interval={keys_by_type.get('interval', [])}"
        )

        register_rows = []
        for key in keys_by_type.get("register", []):
            register_rows.extend(
                download_csv_rows(s3, self.s3_bucket, key, ItronRosevilleRegisterRead)
            )
        interval_rows = []
        for key in keys_by_type.get("interval", []):
            interval_rows.extend(
                download_csv_rows(s3, self.s3_bucket, key, ItronRosevilleIntervalRead)
            )

        return ExtractOutput(
            {
                "register.json": "\n".join(
                    json.dumps(r, cls=DataclassJSONEncoder) for r in register_rows
                ),
                "interval.json": "\n".join(
                    json.dumps(r, cls=DataclassJSONEncoder) for r in interval_rows
                ),
            }
        )

    def _transform(
        self, run_id: str, extract_outputs: ExtractOutput
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        register_reads = extract_outputs.load_from_file(
            "register.json", ItronRosevilleRegisterRead, allow_empty=True
        )
        interval_reads = extract_outputs.load_from_file(
            "interval.json", ItronRosevilleIntervalRead, allow_empty=True
        )

        # Last occurrence wins so that meter attributes (e.g. Location_ID)
        # reflect the most recently delivered file — matching peer adapters'
        # last-wins convention and the SCD2 METERS upsert's expectations.
        meters_by_device_id = {}
        for raw in register_reads + interval_reads:
            device_id = raw.Meter_Serial_Number
            if not device_id:
                continue
            meters_by_device_id[device_id] = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=None,
                location_id=raw.Location_ID,
                meter_id=device_id,
                endpoint_id=raw.EndpointID,
                meter_install_date=self._parse_timestamp(raw.Meter_Install_Date),
                meter_size=None,
                meter_manufacturer=None,
                multiplier=None,
                location_address=None,
                location_city=None,
                location_state=None,
                location_zip=None,
            )

        reads_by_device_and_time = {}
        counters = {
            "error_code_value": 0,
            "no_flowtime": 0,
            "missing_device_id": 0,
            "unparseable_value": 0,
            "overwritten_differing": 0,
        }

        for raw in interval_reads:
            device_id = raw.Meter_Serial_Number
            if not device_id:
                counters["missing_device_id"] += 1
                continue
            flowtime = self._parse_timestamp(raw.Timestamp)
            if flowtime is None:
                counters["no_flowtime"] += 1
                continue
            value = self._parse_value(raw.Read_Value, counters)
            if value is None:
                continue
            interval_value, interval_unit = self.map_reading(
                value, self._normalize_unit(raw.Read_Units)
            )
            key = (device_id, flowtime)
            existing = reads_by_device_and_time.get(key)
            if existing is not None and existing.interval_value != interval_value:
                # Re-delivered correction (expected) or a DST fall-back
                # collision (two wall-clock 01:00 hours; see class docstring).
                counters["overwritten_differing"] += 1
            reads_by_device_and_time[key] = GeneralMeterRead(
                org_id=self.org_id,
                device_id=device_id,
                account_id=None,
                location_id=raw.Location_ID,
                flowtime=flowtime,
                register_value=None,
                register_unit=None,
                interval_value=interval_value,
                interval_unit=interval_unit,
                battery=None,
                install_date=self._parse_timestamp(raw.Meter_Install_Date),
                estimated=None,
                connection=None,
            )

        for raw in register_reads:
            device_id = raw.Meter_Serial_Number
            if not device_id:
                counters["missing_device_id"] += 1
                continue
            flowtime = self._parse_timestamp(raw.Timestamp)
            if flowtime is None:
                counters["no_flowtime"] += 1
                continue
            value = self._parse_value(raw.Read_Value, counters)
            if value is None:
                continue
            register_value, register_unit = self.map_reading(
                value, self._normalize_unit(raw.Read_Units)
            )
            if (device_id, flowtime) in reads_by_device_and_time:
                existing = reads_by_device_and_time[(device_id, flowtime)]
                if (
                    existing.register_value is not None
                    and existing.register_value != register_value
                ):
                    # Same observability as the interval loop: a re-delivered
                    # correction (or DST collision) overwriting a differing value
                    counters["overwritten_differing"] += 1
                read = replace(
                    existing,
                    register_value=register_value,
                    register_unit=register_unit,
                )
            else:
                read = GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=device_id,
                    account_id=None,
                    location_id=raw.Location_ID,
                    flowtime=flowtime,
                    register_value=register_value,
                    register_unit=register_unit,
                    interval_value=None,
                    interval_unit=None,
                    battery=None,
                    install_date=self._parse_timestamp(raw.Meter_Install_Date),
                    estimated=None,
                    connection=None,
                )
            reads_by_device_and_time[(device_id, flowtime)] = read

        for counter, count in counters.items():
            if count:
                logger.info(f"Transform counter {counter}: {count}")

        return list(meters_by_device_id.values()), list(
            reads_by_device_and_time.values()
        )

    def _parse_value(self, value_str: str, counters: dict) -> float:
        """
        Parse a Read_Value into a float, or None if the row should be skipped.
        Non-numeric values (blank, error tokens) are counted and skipped rather
        than failing the whole run. Values at or above the fault-code threshold
        are excluded on BOTH file types — fault codes have only been observed
        in register files, but they are equally impossible as hourly interval
        values.
        """
        try:
            value = float(value_str)
        except (TypeError, ValueError):
            counters["unparseable_value"] += 1
            return None
        if value >= READ_VALUE_ERROR_THRESHOLD:
            counters["error_code_value"] += 1
            return None
        return value

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse Roseville's "MM/DD/YYYY HH:MM:SS.ffffff" Pacific-local timestamps
        into timezone-aware datetimes. Blank values return None (callers skip
        or null the field); a malformed non-blank value raises, deliberately —
        a wholesale format change should fail loudly, not silently drop rows.
        """
        if not timestamp_str:
            return None
        naive = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S.%f")
        return self.org_timezone.localize(naive)

    @staticmethod
    def _normalize_unit(unit: str) -> str:
        """
        Roseville tags read units with an Itron commodity suffix: "CF_WAT" is
        cubic feet of water (Itron systems also serve gas and electric).
        Strip the "_WAT" suffix and pass the remaining unit to map_reading,
        which still raises on genuinely unknown units — so a future "GAL_WAT"
        maps cleanly while garbage keeps failing loudly.
        """
        if unit is None:
            return None
        normalized = unit.strip().upper()
        if normalized.endswith("_WAT"):
            normalized = normalized[: -len("_WAT")]
        if normalized == "GAL":
            # map_reading's vocabulary is GALLON/GALLONS (same aliasing as
            # xylem_datalake._normalize_unit)
            return "GALLON"
        return normalized


class ItronRosevilleRegisterBaseTableLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "ITRON_ROSEVILLE_REGISTER_BASE"

    def columns(self) -> List[str]:
        return list(ItronRosevilleRegisterRead.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_serial_number", "timestamp"]

    def prepare_raw_data(self, extract_outputs: ExtractOutput):
        raw_data = extract_outputs.load_from_file(
            "register.json", ItronRosevilleRegisterRead, allow_empty=True
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class ItronRosevilleIntervalBaseTableLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "ITRON_ROSEVILLE_INTERVAL_BASE"

    def columns(self) -> List[str]:
        return list(ItronRosevilleIntervalRead.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_serial_number", "timestamp"]

    def prepare_raw_data(self, extract_outputs: ExtractOutput):
        raw_data = extract_outputs.load_from_file(
            "interval.json", ItronRosevilleIntervalRead, allow_empty=True
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


ITRON_ROSEVILLE_RAW_SNOWFLAKE_LOADER = RawSnowflakeLoader.with_table_loaders(
    [ItronRosevilleRegisterBaseTableLoader(), ItronRosevilleIntervalBaseTableLoader()]
)

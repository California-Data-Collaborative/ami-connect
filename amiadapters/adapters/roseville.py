import csv
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import io
import json
import logging
import re
from typing import List, Tuple

import boto3

from amiadapters.adapters.base import (
    BaseAMIAdapter,
    GeneralMeterUnitOfMeasure,
    ScheduledExtract,
)
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

logger = logging.getLogger(__name__)


# Itron emits 4294967294 (2**32 - 2) as the register Read_Value for faulted
# register channels. In the 2026-07 full extract this appeared on 296 rows across
# 68 meters (register file only, never interval). We keep these rows in the raw
# extract and base tables for the historical record, but exclude them from
# transformed reads so impossible values never reach READINGS.
REGISTER_ERROR_SENTINEL = 4294967294.0

# Filenames look like rosevillecityof_Register_202607.csv (month suffix) or,
# per our request to Roseville, rosevillecityof_Interval_20260616_20260619.csv
# (date-range suffix). Matched against the S3 key's basename.
FILENAME_PATTERN = re.compile(
    r"(?P<type>register|interval)_(?P<dates>\d{6}|\d{8}_\d{8})\.csv$",
    re.IGNORECASE,
)


@dataclass
class RosevilleRegisterRead:
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
class RosevilleIntervalRead:
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


class RosevilleAdapter(BaseAMIAdapter):
    """
    AMI Adapter for City of Roseville's Itron data.

    Roseville IT exports two CSV views of Itron AMI data — Register (cumulative
    reads, 8-hour cadence) and Interval (hourly consumption) — and delivers them
    via Informatica into a CaDC-owned S3 prefix. This is the first adapter whose
    source is a utility-pushed S3 file drop rather than a vendor API/SFTP/DB.

    Notable properties of the feed:
    - Timestamps are Pacific local time, format "MM/DD/YYYY HH:MM:SS.ffffff"
      (Roseville confirmed all timestamps are Pacific, 2026-06-03).
    - Read_Units is "CF_WAT" (cubic feet, water); translated to CF.
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
            ROSEVILLE_RAW_SNOWFLAKE_LOADER,
        )

    def name(self) -> str:
        return f"roseville-{self.org_id}"

    def scheduled_extracts(self) -> List[ScheduledExtract]:
        """
        Roseville pushes a file per day covering a rolling ~3-day window. We use
        a 4-day extract window so a file pushed late (or a day skipped on our
        side) is still picked up by filename-date overlap; re-processing is
        idempotent via MERGE upserts.
        """
        return [ScheduledExtract(interval=timedelta(days=4))]

    def _get_s3_client(self):
        if self._s3_client is None:
            # The bucket lives in the CaDC AWS account, not the ami-connect
            # account, so the EC2 instance role can't read it. We authenticate
            # with a read-only IAM user's keys from this source's secrets.
            self._s3_client = boto3.client(
                "s3",
                region_name=self.s3_region,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
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

        last_modified_by_key = {}
        paginator = s3.get_paginator("list_objects_v2")
        # Delimiter="/" excludes keys in subfolders, e.g. an archive/ folder
        # of old files, which would otherwise be re-ingested by prefix listing.
        for page in paginator.paginate(
            Bucket=self.s3_bucket, Prefix=self.s3_prefix, Delimiter="/"
        ):
            for obj in page.get("Contents", []):
                # ListObjectsV2 always returns LastModified
                last_modified_by_key[obj["Key"]] = obj["LastModified"]

        keys_by_type = keys_for_date_range(
            list(last_modified_by_key.keys()), extract_range_start, extract_range_end
        )
        # Process files oldest-delivery-first so that when overlapping files
        # carry the same (meter, timestamp) — Roseville re-sends a rolling
        # correction window — the most recently delivered value wins in
        # transform. Sorting by S3 LastModified, not filename.
        for file_type in keys_by_type:
            keys_by_type[file_type].sort(key=lambda k: last_modified_by_key[k])
        logger.info(
            f"Found {len(last_modified_by_key)} keys, matched register={keys_by_type['register']} interval={keys_by_type['interval']}"
        )

        register_rows = []
        for key in keys_by_type["register"]:
            register_rows.extend(
                self._download_and_parse_csv(s3, key, RosevilleRegisterRead)
            )
        interval_rows = []
        for key in keys_by_type["interval"]:
            interval_rows.extend(
                self._download_and_parse_csv(s3, key, RosevilleIntervalRead)
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

    def _download_and_parse_csv(self, s3, key: str, row_type) -> List:
        logger.info(f"Downloading s3://{self.s3_bucket}/{key}")
        response = s3.get_object(Bucket=self.s3_bucket, Key=key)
        # utf-8-sig transparently strips a UTF-8 BOM if present (common in
        # Windows/Informatica exports); a BOM would otherwise corrupt the
        # first CSV header name and crash the row dataclass construction.
        text = response["Body"].read().decode("utf-8-sig")
        rows = []
        for row in csv.DictReader(io.StringIO(text)):
            rows.append(row_type(**row))
        logger.info(f"Parsed {len(rows)} rows from {key}")
        return rows

    def _transform(
        self, run_id: str, extract_outputs: ExtractOutput
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        register_reads = extract_outputs.load_from_file(
            "register.json", RosevilleRegisterRead, allow_empty=True
        )
        interval_reads = extract_outputs.load_from_file(
            "interval.json", RosevilleIntervalRead, allow_empty=True
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
            "sentinel": 0,
            "no_flowtime": 0,
            "unparseable_value": 0,
            "overwritten_differing": 0,
        }

        for raw in interval_reads:
            device_id = raw.Meter_Serial_Number
            if not device_id:
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
                read = replace(
                    reads_by_device_and_time[(device_id, flowtime)],
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
        than failing the whole run. The Itron faulted-channel sentinel is
        excluded on BOTH file types — it has only been observed in register
        files, but it is equally impossible as an hourly interval value.
        """
        try:
            value = float(value_str)
        except (TypeError, ValueError):
            counters["unparseable_value"] += 1
            return None
        if value == REGISTER_ERROR_SENTINEL:
            counters["sentinel"] += 1
            return None
        return value

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse Roseville's "MM/DD/YYYY HH:MM:SS.ffffff" Pacific-local timestamps
        into timezone-aware datetimes.
        """
        if not timestamp_str:
            return None
        naive = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S.%f")
        return self.org_timezone.localize(naive)

    @staticmethod
    def _normalize_unit(unit: str) -> str:
        """
        Roseville tags water reads as "CF_WAT" (cubic feet, water). Translate to
        the CF unit that map_reading recognizes; pass anything else through so
        map_reading raises on genuinely unknown units.
        """
        if unit is None:
            return None
        normalized = unit.strip().upper()
        if normalized == "CF_WAT":
            return GeneralMeterUnitOfMeasure.CUBIC_FEET
        return normalized


def keys_for_date_range(
    keys: List[str], range_start: datetime, range_end: datetime
) -> dict:
    """
    Given S3 keys from Roseville's prefix, return {"register": [...], "interval": [...]}
    with the keys whose filename date range overlaps the extract range.

    Filenames carry either a month suffix (rosevillecityof_Register_202607.csv)
    or a date-range suffix (rosevillecityof_Interval_20260616_20260619.csv). A
    file is selected if its date range overlaps [range_start, range_end]. Files
    whose names don't match the pattern are logged and skipped.

    Scheduled runs deliver naive range bounds (datetime.now() server time), but
    manually triggered runs can carry offsets (the Airflow UI params pass
    through datetime.fromisoformat unmodified). Filename dates are naive wall
    dates, so we drop any offset and compare wall-clock-to-wall-clock — the
    fuzzy multi-day window absorbs the hour-level slop.
    """
    if range_start.tzinfo is not None:
        range_start = range_start.replace(tzinfo=None)
    if range_end.tzinfo is not None:
        range_end = range_end.replace(tzinfo=None)
    result = {"register": [], "interval": []}
    for key in keys:
        filename = key.split("/")[-1]
        match = FILENAME_PATTERN.search(filename)
        if not match:
            logger.info(f"Skipping unrecognized file: {key}")
            continue
        try:
            file_start, file_end = _date_range_from_filename(match.group("dates"))
        except Exception as e:
            logger.warning(f"Skipping file {key}, could not parse dates: {str(e)}")
            continue
        if file_start <= range_end and range_start <= file_end:
            result[match.group("type").lower()].append(key)
        else:
            logger.info(
                f"Skipping file outside extract range ({file_start} to {file_end}): {key}"
            )
    return result


def _date_range_from_filename(dates: str) -> Tuple[datetime, datetime]:
    """
    Parse a filename date token into an inclusive [start, end] datetime range.
    "202607" (YYYYMM) covers the whole month; "20260616_20260619" covers the
    named days through end of the last day.
    """
    if "_" in dates:
        start_str, end_str = dates.split("_")
        start = datetime.strptime(start_str, "%Y%m%d")
        end = datetime.strptime(end_str, "%Y%m%d") + timedelta(days=1)
    else:
        start = datetime.strptime(dates, "%Y%m")
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
    return start, end


class RosevilleRegisterBaseTableLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "ROSEVILLE_REGISTER_BASE"

    def columns(self) -> List[str]:
        return list(RosevilleRegisterRead.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_serial_number", "timestamp"]

    def prepare_raw_data(self, extract_outputs: ExtractOutput):
        raw_data = extract_outputs.load_from_file(
            "register.json", RosevilleRegisterRead, allow_empty=True
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class RosevilleIntervalBaseTableLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "ROSEVILLE_INTERVAL_BASE"

    def columns(self) -> List[str]:
        return list(RosevilleIntervalRead.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_serial_number", "timestamp"]

    def prepare_raw_data(self, extract_outputs: ExtractOutput):
        raw_data = extract_outputs.load_from_file(
            "interval.json", RosevilleIntervalRead, allow_empty=True
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


ROSEVILLE_RAW_SNOWFLAKE_LOADER = RawSnowflakeLoader.with_table_loaders(
    [RosevilleRegisterBaseTableLoader(), RosevilleIntervalBaseTableLoader()]
)

import csv
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import io
import logging
import json
import os
import re
import tempfile
from typing import Dict, List, Optional, Tuple

import boto3
import paramiko
import pytz

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader
from amiadapters.outputs.base import ExtractOutput

logger = logging.getLogger(__name__)


###############################################################################
# CMEP record parsing
#
# CMEP is the California Metering Exchange Protocol, the flat-file format
# Sensus/Xylem use for scheduled data transfers. These functions and
# dataclasses are deliberately module-level and adapter-agnostic: if a second
# consumer appears (another utility's CMEP delivery, or an AlarmReport/MLA01
# parser — see the RNI Extended CMEP Specs Reference Manual), extract this
# section into its own module. Kept in-file until that second consumer exists.
#
# See MEPMD01 section of
# https://www.sce.com/sites/default/files/inline-files/14%2B-%2BCalifornia%2BMetering%2BExchange%2BProtocol%2B-%2BV4.1-022013_AA.pdf
###############################################################################

CMEP_INTERVAL_DATA_RECORD_TYPE = "MEPMD01"

# Data quality bitmask bits (RNI Extended CMEP Specs Reference Manual,
# ARM-10006-28, Table 22) whose set values are not usable measurements.
CMEP_QUALITY_BIT_OVERFLOW = 1 << 9
CMEP_QUALITY_BIT_REGISTER_ROLLOVER = 1 << 13


@dataclass
class CmepRead:
    """
    A single reading within a CMEP record: a timestamp, a protocol
    quality/status code (e.g. "R0"), and a value.
    """

    time: str
    code: str
    quantity: str


@dataclass
class CmepMeterAndReads:
    """
    One CMEP MEPMD01 record: meter/transmission metadata plus that meter's
    readings for one channel. The `units` field identifies the channel, e.g.
    "CF" for hourly interval values and "CFREG" for cumulative register reads.
    """

    record_type: str
    record_version: str
    sender_id: str
    sender_customer_id: str
    receiver_id: str
    receiver_customer_id: str
    time_stamp: str
    meter_id: str
    purpose: str
    commodity: str
    units: str
    calculation_constant: str
    interval: str
    quantity: str
    reads: List[CmepRead]
    # Derived: timestamp of the row's first reading. time_stamp is the
    # file-generation stamp shared by every row in a file, so a meter's
    # multiple same-channel rows (catch-up deliveries for disjoint windows)
    # need this to stay distinct in the raw table's dedup key.
    first_read_time: str = ""

    @classmethod
    def from_json_file(cls, extract_output: ExtractOutput, filename: str) -> List:
        """
        Parses instances from JSON file, including nested reads.
        """
        raw_meters_with_reads = extract_output.load_from_file(filename, cls)
        for raw_meter in raw_meters_with_reads:
            raw_meter: CmepMeterAndReads = raw_meter
            reads = []
            for read in raw_meter.reads:
                reads.append(CmepRead(**read))
            raw_meter.reads = reads
        return raw_meters_with_reads


def parse_cmep_row(row: List[str]) -> CmepMeterAndReads:
    """
    Parses a single row in a CMEP-formatted file. The protocol puts all of a
    meter's readings for one channel on a single line: 14 metadata fields,
    then (timestamp, code, value) triplets. Field 13 says how many triplets
    follow.

    Raises on any structural violation — a malformed file should fail the run
    loudly rather than load partially.
    """
    if row[0] != CMEP_INTERVAL_DATA_RECORD_TYPE:
        raise Exception(f"Unrecognized report format: {row[0]}")

    if len(row) < 15:
        raise Exception(f"Row does not match MEPMD01 format: {row}")

    quantity_index = 13
    number_of_reads = int(row[quantity_index]) if row[quantity_index] else 0
    expected_length = quantity_index + 1 + (number_of_reads * 3)
    if len(row) != expected_length:
        raise Exception(
            f"Row declares {number_of_reads} readings but has {len(row)} fields "
            f"instead of {expected_length}: {row[:14]}..."
        )
    reads = []
    for i in range(number_of_reads):
        start_of_read = quantity_index + 1 + (i * 3)
        date_time_text, code, quantity = (
            row[start_of_read],
            row[start_of_read + 1],
            row[start_of_read + 2],
        )
        if not date_time_text:
            # This is a valid state according to protocol - we'd need to calculate the date from the row's base date time plus intervals
            # We've punted on handling it. For now, throw an error if it comes up.
            raise Exception("No date time text for reading, which we do not support")
        reads.append(CmepRead(time=date_time_text, code=code, quantity=quantity))

    return CmepMeterAndReads(
        record_type=row[0],
        record_version=row[1],
        sender_id=row[2],
        sender_customer_id=row[3],
        receiver_id=row[4],
        receiver_customer_id=row[5],
        time_stamp=row[6],
        meter_id=row[7],
        purpose=row[8],
        commodity=row[9],
        units=row[10],
        calculation_constant=row[11],
        interval=row[12],
        quantity=row[13],
        reads=reads,
        first_read_time=reads[0].time if reads else "",
    )


###############################################################################
# Adapter
###############################################################################

# Sensus delivery filenames look like STAHO_IntervalReport_202606030815.txt:
# a utility prefix, the report type, and a YYYYMMDDHHMM generation stamp.
FILENAME_TIMESTAMP_PATTERN = re.compile(r"_IntervalReport_(\d{12})\.txt$")


def files_for_date_range(
    files: List[str], extract_range_start: datetime, extract_range_end: datetime
) -> List[str]:
    """
    Filter server filenames to those with data in the given date range.

    A file stamped day D carries readings from roughly D-1 08:00 through
    D 07:00 local, plus catch-up rows for meters whose earlier deliveries
    were missed — so readings for a given day arrive in the file stamped the
    NEXT day, and we select stamps in [start, end + 1 day]. Catch-up copies
    of a day's readings can also appear in files later than that window;
    ongoing daily runs pick those up as they fetch newer files.
    """
    result = []
    for filename in files:
        match = FILENAME_TIMESTAMP_PATTERN.search(filename)
        if not match:
            logger.info(
                f"Skipping file {filename}: does not match IntervalReport naming convention"
            )
            continue
        stamp_day = datetime.strptime(match.group(1), "%Y%m%d%H%M").date()
        first_day = extract_range_start.date()
        last_day = (extract_range_end + timedelta(days=1)).date()
        if first_day <= stamp_day <= last_day:
            result.append((match.group(1), filename))
    # Oldest first, so when overlapping files re-deliver a (meter, hour) the
    # newest file's value deterministically wins in the transform.
    return [filename for _, filename in sorted(result)]


class XylemSensusAdapter(BaseAMIAdapter):
    """
    AMI Adapter for Xylem/Sensus CMEP files delivered to an SFTP folder.

    Optionally stamps account_id/location_id onto meters and reads from a
    billing crosswalk file in S3 (columns meter_id,account_id,location_id),
    produced by the utility's billing parser. Meters absent from the
    crosswalk load with null ids; a configured-but-missing or malformed
    crosswalk fails the run.
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        pipeline_configuration,
        sftp_host,
        sftp_remote_data_directory,
        sftp_local_download_directory,
        sftp_known_hosts_str,
        sftp_user,
        sftp_password,
        crosswalk_s3_region,
        crosswalk_s3_bucket,
        crosswalk_s3_key,
        crosswalk_aws_access_key_id,
        crosswalk_aws_secret_access_key,
        configured_task_output_controller,
        configured_metrics,
        configured_sinks,
        s3_client=None,
    ):
        self.sftp_host = sftp_host
        self.sftp_user = sftp_user
        self.sftp_password = sftp_password
        self.sftp_meter_and_reads_folder = sftp_remote_data_directory
        self.local_download_directory = sftp_local_download_directory
        self.known_hosts = sftp_known_hosts_str
        self.crosswalk_s3_region = crosswalk_s3_region
        self.crosswalk_s3_bucket = crosswalk_s3_bucket
        self.crosswalk_s3_key = crosswalk_s3_key
        self.crosswalk_aws_access_key_id = crosswalk_aws_access_key_id
        self.crosswalk_aws_secret_access_key = crosswalk_aws_secret_access_key
        # Injectable for tests. In production this stays None until the
        # crosswalk is fetched — adapters are constructed at Airflow DAG-parse
        # time, so the constructor must not do network or client setup work.
        self._s3_client = s3_client
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_metrics,
            configured_sinks,
            RawSnowflakeLoader.with_table_loaders([XylemSensusBaseTableLoader()]),
        )

    def name(self) -> str:
        return f"xylem-sensus-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:
        logger.info(
            f"Connecting to Xylem Sensus SFTP for data between {extract_range_start} and {extract_range_end}"
        )
        downloaded_files = []
        try:
            with paramiko.SSHClient() as ssh:
                # Prepare known hosts
                tmp = tempfile.NamedTemporaryFile()
                tmp.write(self.known_hosts.encode("utf-8"))
                tmp.flush()
                ssh.load_host_keys(tmp.name)

                # Perform sftp
                ssh.connect(
                    self.sftp_host,
                    username=self.sftp_user,
                    password=self.sftp_password,
                    look_for_keys=False,
                    allow_agent=False,
                )
                with ssh.open_sftp() as sftp:
                    downloaded_files = (
                        self._download_meter_and_read_files_for_date_range(
                            sftp, extract_range_start, extract_range_end
                        )
                    )

            meters_and_reads = self._parse_downloaded_files(downloaded_files)
            output = "\n".join(
                json.dumps(i, cls=DataclassJSONEncoder) for i in meters_and_reads
            )
        finally:
            for f in downloaded_files:
                logger.info(f"Cleaning up downloaded file {f}")
                os.remove(f)

        return ExtractOutput({"meters_and_reads.json": output})

    def _download_meter_and_read_files_for_date_range(
        self,
        sftp: paramiko.SFTPClient,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> List[str]:
        downloaded_files = []
        all_files_on_server = sftp.listdir(self.sftp_meter_and_reads_folder)
        logger.info(f"Found {len(all_files_on_server)} total files on server")
        files_to_download = files_for_date_range(
            all_files_on_server, extract_range_start, extract_range_end
        )
        if not files_to_download:
            raise Exception(
                f"No files found on server for range {extract_range_start} to {extract_range_end}"
            )
        os.makedirs(self.local_download_directory, exist_ok=True)
        for file in files_to_download:
            local_file = f"{self.local_download_directory}/{file}"
            downloaded_files.append(local_file)
            logger.info(
                f"Downloading {file} from SFTP at {self.sftp_host} to {local_file}"
            )
            sftp.get(self.sftp_meter_and_reads_folder + "/" + file, local_file)
        return downloaded_files

    def _parse_downloaded_files(self, files: List[str]) -> List[CmepMeterAndReads]:
        result = []
        for cmep_file in files:
            with open(cmep_file, newline="", encoding="utf-8") as f:
                for row in csv.reader(f):
                    result.append(parse_cmep_row(row))
        return result

    def _transform(
        self, run_id: str, extract_outputs: ExtractOutput
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        raw_meters_with_reads = CmepMeterAndReads.from_json_file(
            extract_outputs, "meters_and_reads.json"
        )

        crosswalk = self._load_crosswalk()

        transformed_meters_by_device_id = {}
        # File-generation stamp of the row each stored meter came from, for
        # the metadata-change handling below.
        meter_source_stamps = {}
        transformed_reads_by_key = {}
        # Last naive timestamp seen per (device_id, units), for DST fall-back
        # disambiguation - see the comment where it is used below.
        previous_naive_read = {}

        for raw_meter_with_reads in raw_meters_with_reads:
            raw_meter_with_reads: CmepMeterAndReads = raw_meter_with_reads

            device_id = raw_meter_with_reads.meter_id
            if not device_id:
                logger.warning(
                    f"Skipping meter {raw_meter_with_reads} with null device ID"
                )
                continue

            if raw_meter_with_reads.commodity != "W":
                logger.info(
                    f"Skipping meter {device_id} with commodity type {raw_meter_with_reads.commodity} which is not a water meter"
                )
                continue

            if raw_meter_with_reads.purpose != "OK":
                # Other transmission purposes include, for example, "SUMMARY" which might include monthly totals which
                # we aren't prepared to handle
                logger.info(
                    f"Skipping meter {device_id} with data transmission purpose {raw_meter_with_reads.purpose} because transmission may not contain hourly readings"
                )
                continue

            # The CMEP feed carries no billing identifiers (receiver_customer_id
            # duplicates the meter id), so account and location come from the
            # configured crosswalk. Meters absent from the crosswalk load with
            # null ids and become linkable when the crosswalk refreshes.
            if crosswalk is not None and device_id in crosswalk:
                account_id, location_id = crosswalk[device_id]
            else:
                account_id, location_id = None, None
            # 1:1 with meter_id in observed data; believed to be the FlexNet
            # radio (MXU) id, unconfirmed by the vendor.
            endpoint_id = raw_meter_with_reads.receiver_id

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                meter_id=device_id,
                endpoint_id=endpoint_id,
                meter_install_date=None,
                meter_size=None,
                meter_manufacturer=None,
                multiplier=raw_meter_with_reads.calculation_constant,
                location_address=None,
                location_city=None,
                location_state=None,
                location_zip=None,
            )
            if (
                device_id in transformed_meters_by_device_id
                and meter != transformed_meters_by_device_id[device_id]
            ):
                # A meter's rows within one file share a generation instant
                # (time_stamp is the file stamp), so two conflicting views in
                # the SAME file are a genuine feed inconsistency: fail loudly.
                # Across files, metadata can legitimately change - e.g. a
                # radio swap changes receiver_id, and so endpoint_id. Files
                # iterate oldest-first, so keep the newer file's view - the
                # same newest-file-wins rule used for re-delivered readings -
                # and log what changed. Only the final state loads from one
                # extract; changes between runs are versioned in METERS.
                previous_meter = transformed_meters_by_device_id[device_id]
                previous_stamp = meter_source_stamps[device_id]
                current_stamp = raw_meter_with_reads.time_stamp
                if current_stamp == previous_stamp:
                    raise Exception(
                        f"Found duplicate meters that do not match for device_id {device_id}"
                    )
                changed = ", ".join(
                    f"{field}: {getattr(previous_meter, field)!r} -> {getattr(meter, field)!r}"
                    for field in meter.__dataclass_fields__
                    if getattr(meter, field) != getattr(previous_meter, field)
                )
                logger.warning(
                    f"Meter metadata changed for device_id {device_id} between "
                    f"file stamps {previous_stamp} and {current_stamp}: {changed}"
                )

            transformed_meters_by_device_id[device_id] = meter
            meter_source_stamps[device_id] = raw_meter_with_reads.time_stamp

            units = raw_meter_with_reads.units
            if units not in ("CF", "CFREG"):
                raise ValueError(
                    f"Unrecognized CMEP units {units} for meter {device_id}"
                )

            for raw_read in raw_meter_with_reads.reads:
                # CMEP data quality codes, per the Sensus RNI Extended CMEP
                # Specs Reference Manual (ARM-10006-28, "Data quality
                # flags"): a letter - R raw, N missing/unusable, E estimated,
                # A adjusted, D derived (RNI-computed intervals), M adjusted
                # and derived - followed by an integer bitmask of status
                # flags (Table 22). Classification is by the letter; the
                # bitmask is consulted only for the two value-corrupting bits
                # handled below. Any other letter is unknown - fail loudly
                # rather than guess at its meaning. The full code is
                # preserved in the raw table either way.
                code = (raw_read.code or "").upper()
                naive = datetime.strptime(raw_read.time, "%Y%m%d%H%M")

                # On a DST fall-back day one wall-clock hour is sent twice.
                # Readings run in chronological order, so a timestamp that
                # repeats the one immediately before it - for this meter and
                # channel - is the second (standard time) occurrence. This is
                # tracked per meter and channel rather than per record because
                # CMEP caps a record at 48 readings, so a catch-up backlog can
                # split the pair across two records. It is deliberately a
                # comparison against the previous reading rather than a count
                # of how often a timestamp has been seen: a later file
                # re-delivering the same day must localize it the same way,
                # not treat every reading as another occurrence.
                repeat_key = (device_id, units)
                occurrence = 1 if previous_naive_read.get(repeat_key) == naive else 0
                # Recorded even for skipped readings below, so that a
                # placeholder in the first ambiguous hour still marks the
                # surviving second one as standard time.
                previous_naive_read[repeat_key] = naive

                # N rows are skipped: they carry a filler value (0 on the
                # interval channel, a stale reading on the register channel)
                # for hours the network never heard, and loading them would
                # fabricate consumption.
                if code.startswith("N"):
                    continue
                # Readings whose status bitmask flags overflow or register
                # rollover are skipped the same way: the flagged values are
                # not measurements (observed: registers saturated at
                # 999,99x, a negative interval). Informational bits (e.g.
                # 64, daylight saving in effect) do not skip.
                bitmask = int(code[1:]) if code[1:].isdigit() else 0
                if bitmask & (
                    CMEP_QUALITY_BIT_OVERFLOW | CMEP_QUALITY_BIT_REGISTER_ROLLOVER
                ):
                    logger.warning(
                        f"Skipping reading with quality code {raw_read.code} for "
                        f"meter {device_id} at {raw_read.time}: overflow/rollover-"
                        f"flagged values are not usable measurements"
                    )
                    continue
                if code == "" or code.startswith(("R", "A", "D", "M")):
                    estimated = 0
                elif code.startswith("E"):
                    estimated = 1
                else:
                    raise ValueError(
                        f"Unrecognized CMEP read code {raw_read.code} for meter {device_id}"
                    )

                flowtime = self._localize(naive, occurrence)
                if flowtime is None:
                    continue
                key = (device_id, flowtime)
                # Both channels arrive in cubic feet; CFREG marks the
                # cumulative register channel, CF the hourly interval channel.
                value, unit = self.map_reading(float(raw_read.quantity), "CF")

                if key in transformed_reads_by_key:
                    # Join this channel onto the read from the other channel
                    # at the same flowtime (e.g. the register read at 07:00
                    # joins the 07:00 interval read). Same pattern as the
                    # Metersense adapter. The estimated flag always reflects
                    # the interval channel when an interval value exists:
                    # an incoming interval read carries its flag in, an
                    # incoming register read never overrides it.
                    old_read = transformed_reads_by_key[key]
                    if units == "CF":
                        read = replace(
                            old_read,
                            interval_value=value,
                            interval_unit=unit,
                            estimated=estimated,
                        )
                    else:
                        read = replace(
                            old_read, register_value=value, register_unit=unit
                        )
                else:
                    read = GeneralMeterRead(
                        org_id=self.org_id,
                        device_id=device_id,
                        account_id=account_id,
                        location_id=location_id,
                        flowtime=flowtime,
                        register_value=value if units == "CFREG" else None,
                        register_unit=unit if units == "CFREG" else None,
                        interval_value=value if units == "CF" else None,
                        interval_unit=unit if units == "CF" else None,
                        battery=None,
                        install_date=None,
                        connection=None,
                        estimated=estimated,
                    )
                transformed_reads_by_key[key] = read

        return list(transformed_meters_by_device_id.values()), list(
            transformed_reads_by_key.values()
        )

    def _localize(self, naive: datetime, occurrence: int = 0) -> Optional[datetime]:
        """
        Attach the org's timezone to a naive feed timestamp. Returned
        datetimes should never be naive, per BaseAMIAdapter convention.

        On a DST fall-back day the feed repeats one wall-clock hour: the
        caller passes occurrence 0 for the first of the pair (daylight time)
        and 1 for the second (standard time).

        A timestamp in the spring-forward gap names an hour that does not
        exist in this timezone, so there is no correct instant to load it at.
        Returns None and logs, dropping that one reading, rather than raising
        and failing every meter in the run over one unrepresentable hour.
        """
        tz = (
            pytz.timezone(self.org_timezone)
            if isinstance(self.org_timezone, str)
            else self.org_timezone
        )
        try:
            return tz.localize(naive, is_dst=None)
        except pytz.exceptions.AmbiguousTimeError:
            return tz.localize(naive, is_dst=(occurrence == 0))
        except pytz.exceptions.NonExistentTimeError:
            logger.warning(
                f"Skipping reading at {naive}: that local time does not exist "
                f"in {self.org_timezone} (daylight saving time gap)"
            )
            return None

    def _load_crosswalk(self) -> Optional[Dict[str, Tuple[str, str]]]:
        """
        Fetch the billing crosswalk from S3 and return meter_id ->
        (account_id, location_id). Returns None when no crosswalk is
        configured for this org. Raises when a configured crosswalk is
        missing, empty, or malformed: proceeding without it would load reads
        with null ids that later duplicate against correctly-linked re-loads.
        """
        crosswalk_fields = {
            "crosswalk_s3_region": self.crosswalk_s3_region,
            "crosswalk_s3_bucket": self.crosswalk_s3_bucket,
            "crosswalk_s3_key": self.crosswalk_s3_key,
        }
        missing = [name for name, value in crosswalk_fields.items() if not value]
        if len(missing) == len(crosswalk_fields):
            logger.info(f"No billing crosswalk configured for {self.org_id}")
            return None
        if missing:
            # A partially configured crosswalk is an operator error, not an
            # unconfigured org - treating it as the latter would silently
            # load the whole fleet with null ids.
            raise ValueError(
                f"Crosswalk partially configured for {self.org_id}: missing {missing}"
            )

        client = self._s3_client
        if client is None:
            client = boto3.client(
                "s3",
                region_name=self.crosswalk_s3_region,
                aws_access_key_id=self.crosswalk_aws_access_key_id,
                aws_secret_access_key=self.crosswalk_aws_secret_access_key,
            )
        obj = client.get_object(
            Bucket=self.crosswalk_s3_bucket, Key=self.crosswalk_s3_key
        )
        body = obj["Body"].read().decode("utf-8")

        reader = csv.DictReader(io.StringIO(body))
        expected_columns = {"meter_id", "account_id", "location_id"}
        if set(reader.fieldnames or []) != expected_columns:
            raise ValueError(
                f"Crosswalk at s3://{self.crosswalk_s3_bucket}/{self.crosswalk_s3_key} "
                f"has columns {reader.fieldnames}, expected {sorted(expected_columns)}"
            )
        crosswalk = {}
        for row in reader:
            meter_id = row["meter_id"].strip()
            account_id = row["account_id"].strip()
            location_id = row["location_id"].strip()
            if not (meter_id and account_id and location_id):
                raise ValueError(f"Malformed crosswalk row: {row}")
            crosswalk[meter_id] = (account_id, location_id)
        if not crosswalk:
            raise ValueError(
                f"Crosswalk at s3://{self.crosswalk_s3_bucket}/{self.crosswalk_s3_key} is empty"
            )
        logger.info(f"Loaded billing crosswalk with {len(crosswalk)} meter keys")
        return crosswalk


class XylemSensusBaseTableLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "XYLEM_SENSUS_METER_AND_READS_BASE"

    def columns(self) -> List[str]:
        return list(CmepMeterAndReads.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        # time_stamp is the file-generation stamp, identical for every row in
        # a file - so it distinguishes files, not rows. units distinguishes a
        # meter's interval (CF) and register (CFREG) channel rows, and
        # first_read_time distinguishes a meter's multiple same-channel rows
        # (catch-up deliveries covering disjoint windows).
        return ["meter_id", "time_stamp", "units", "first_read_time"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = CmepMeterAndReads.from_json_file(
            extract_outputs, "meters_and_reads.json"
        )
        result = []
        for i in raw_data:
            i.reads = json.dumps(i.reads, cls=DataclassJSONEncoder)
            result.append(tuple(i.__getattribute__(col) for col in self.columns()))
        return result

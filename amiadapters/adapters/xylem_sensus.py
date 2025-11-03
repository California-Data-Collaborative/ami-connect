import csv
from dataclasses import dataclass
from datetime import datetime
import logging
import json
import os
from typing import Generator, List, Tuple

import paramiko
import pytz

from amiadapters.adapters.base import BaseAMIAdapter, GeneralMeterUnitOfMeasure
from amiadapters.configuration.models import SftpConfiguration
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput

logger = logging.getLogger(__name__)


@dataclass
class XylemSensusRead:
    """
    Sensus single reading
    """

    time: str
    code: str
    quantity: str


@dataclass
class XylemSensusMeterAndReads:
    """
    Sensus CMEP-formatted record that includes meter info and hourly readings.

    See MEPMD01 section of https://www.sce.com/sites/default/files/inline-files/14%2B-%2BCalifornia%2BMetering%2BExchange%2BProtocol%2B-%2BV4.1-022013_AA.pdf
    """

    record_type: str
    record_version: str
    sender_id: str
    sender_customer_id: str
    receiver_id: str
    reciever_customer_id: str
    time_stamp: str
    meter_id: str
    purpose: str
    commodity: str
    units: str
    calculation_constant: str
    interval: str
    quantity: str
    reads: list[XylemSensusRead]

    @classmethod
    def from_json_file(cls, extract_output: ExtractOutput, filename: str) -> List:
        """
        Parses instances from JSON file, including nested reads.
        """
        raw_meters_with_reads = extract_output.load_from_file(
            filename, XylemSensusMeterAndReads
        )
        for raw_meter in raw_meters_with_reads:
            raw_meter: XylemSensusMeterAndReads = raw_meter
            reads = []
            for read in raw_meter.reads:
                reads.append(XylemSensusRead(**read))
            raw_meter.reads = reads
        return raw_meters_with_reads


class XylemSensusAdapter(BaseAMIAdapter):
    """
    AMI Adapter that uses SFTP to retrieve Xylem Sensus data.
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        pipeline_configuration,
        configured_sftp: SftpConfiguration,
        sftp_user,
        sftp_password,
        configured_task_output_controller,
        configured_sinks,
    ):
        self.sftp_host = configured_sftp.host
        self.sftp_user = sftp_user
        self.sftp_password = sftp_password
        self.sftp_meter_and_reads_folder = configured_sftp.remote_data_directory
        self.local_download_directory = configured_sftp.local_download_directory
        self.local_known_hosts_file = configured_sftp.local_known_hosts_file
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_sinks,
            None,  # Replace with a XylemSensusRawSnowflakeLoader if needed
        )

    def name(self) -> str:
        return f"xylem-sensus-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:

        with open("/Users/matthewdowell/Desktop/south-tahoe.csv", "r") as f:
            reader = csv.reader(f, delimiter=",")
            result = []
            for row in reader:
                xylem_meter_and_reads = self._parse_cmep_row(row)
                result.append(xylem_meter_and_reads)

        # logger.info(
        #     f"Connecting to Xylem Sensus SFTP for data between {extract_range_start} and {extract_range_end}"
        # )
        # downloaded_files = []
        # try:
        #     with paramiko.SSHClient() as ssh:
        #         ssh.load_host_keys(self.local_known_hosts_file)
        #         ssh.connect(
        #             self.sftp_host,
        #             username=self.sftp_user,
        #             password=self.sftp_password,
        #             look_for_keys=False,
        #             allow_agent=False,
        #         )
        #         with ssh.open_sftp() as sftp:
        #             downloaded_files = (
        #                 self._download_meter_and_read_files_for_date_range(
        #                     sftp, extract_range_start, extract_range_end
        #                 )
        #             )

        #     meters_and_reads = self._parse_downloaded_files(downloaded_files)
        #     output = "\n".join(meters_and_reads)
        # finally:
        #     for f in downloaded_files:
        #         logger.info(f"Cleaning up downloaded file {f}")
        #         os.remove(f)
        output = "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in result)
        return ExtractOutput({"meters_and_reads.json": output})

    def _parse_cmep_row(self, row: list[str]) -> XylemSensusMeterAndReads:
        """
        Parses a single row in a CMEP-formatted file.

        See CMEP docs linked elsewhere in this file for explanation of protocol, which
        puts all of a meter's reads on a single line.
        """
        if row[0] != "MEPMD01":
            raise Exception(f"Unrecognized report format: {row[0]}")

        if len(row) < 15:
            raise Exception(f"Row does not match MEPMD01 format: {row}")

        # The 13th (and last non-reading item) in the row says how many readings will follow in that row
        # Readings come in groups of three values. So if there are 5 readings, then 15 values will follow.
        quantity_index = 13
        number_of_reads = int(row[quantity_index]) if row[quantity_index] else 0
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
                raise Exception(
                    "No date time text for reading, which we do not support"
                )
            reads.append(
                XylemSensusRead(time=date_time_text, code=code, quantity=quantity)
            )

        return XylemSensusMeterAndReads(
            record_type=row[0],
            record_version=row[1],
            sender_id=row[2],
            sender_customer_id=row[3],
            receiver_id=row[4],
            reciever_customer_id=row[5],
            time_stamp=row[6],
            meter_id=row[7],
            purpose=row[8],
            commodity=row[9],
            units=row[10],
            calculation_constant=row[11],
            interval=row[12],
            quantity=row[13],
            reads=reads,
        )

    # def _download_meter_and_read_files_for_date_range(
    #     self,
    #     sftp: paramiko.SFTPClient,
    #     extract_range_start: datetime,
    #     extract_range_end: datetime,
    # ) -> List[str]:
    #     downloaded_files = []
    #     all_files_on_server = sftp.listdir(self.sftp_meter_and_reads_folder)
    #     logger.info(f"Found {len(all_files_on_server)} total files on server")
    #     files_to_download = files_for_date_range(
    #         all_files_on_server, extract_range_start, extract_range_end
    #     )
    #     if not files_to_download:
    #         raise Exception(
    #             f"No files found on server for range {extract_range_start} to {extract_range_end}"
    #         )
    #     os.makedirs(self.local_download_directory, exist_ok=True)
    #     for file in files_to_download:
    #         local_csv = f"{self.local_download_directory}/{file}"
    #         downloaded_files.append(local_csv)
    #         logger.info(
    #             f"Downloading {file} from FTP at {self.sftp_host} to {local_csv}"
    #         )
    #         sftp.get(self.sftp_meter_and_reads_folder + "/" + file, local_csv)
    #     return downloaded_files

    # def _parse_downloaded_files(self, files: List[str]) -> Generator[str, None, None]:
    #     for csv_file in files:
    #         with open(csv_file, newline="", encoding="utf-8") as f:
    #             csv_reader = csv.DictReader(f, delimiter=",")
    #             for data in csv_reader:
    #                 meter_and_read = XylemSensusMeterAndRead(**data)
    #                 yield json.dumps(meter_and_read, cls=DataclassJSONEncoder)

    def _transform(
        self, run_id: str, extract_outputs: ExtractOutput
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        raw_meters_with_reads = XylemSensusMeterAndReads.from_json_file(
            extract_outputs, "meters_and_reads.json"
        )

        transformed_meters_by_device_id = {}
        transformed_reads_by_key = {}

        for raw_meter_with_reads in raw_meters_with_reads:
            raw_meter_with_reads: XylemSensusMeterAndReads = raw_meter_with_reads

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

            # TODO ask CaDC about receiver_id and receiver_customer_id. In sample, both have 3210 unique values out of the 3213 rows
            # TODO so they might be correlated. Confirm it's a good account ID. Is there a location vs account distinction?
            account_id = raw_meter_with_reads.reciever_customer_id
            location_id = None
            meter_id = raw_meter_with_reads.meter_id
            endpoint_id = None

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                meter_id=meter_id,
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
                # We expect duplicate rows for some devices, but they should be identical besides the readings
                raise Exception(
                    f"Found duplicate meters that do not match for device_id {device_id}"
                )

            transformed_meters_by_device_id[device_id] = meter

            # Interval reads
            for raw_read in raw_meter_with_reads.reads:
                flowtime = datetime.strptime(raw_read.time, "%Y%m%d%H%M")
                interval_value, interval_unit = self.map_reading(
                    float(raw_read.quantity),
                    raw_meter_with_reads.units,
                )
                read = GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=device_id,
                    account_id=account_id,
                    location_id=location_id,
                    flowtime=flowtime,
                    register_value=None,
                    register_unit=None,
                    interval_value=interval_value,
                    interval_unit=interval_unit,
                    battery=None,
                    install_date=None,
                    connection=None,
                    estimated=1 if raw_read.code == "E" else 0,
                )
                transformed_reads_by_key[(device_id, flowtime)] = read

        return list(transformed_meters_by_device_id.values()), list(
            transformed_reads_by_key.values()
        )


# TODO DRY this out from Aclara code if we're going to use it
def files_for_date_range(
    files: List[str], extract_range_start: datetime, extract_range_end: datetime
) -> List[str]:
    """
    Given a list of filenames on the Aclara server in the form "CaDC_Readings_05062024.csv", filter
    to the files with data in the given date range.
    """
    result = []
    for filename in files:
        try:
            # e.g. CaDC_Readings_05062024.csv
            date_str = filename[-12:-4]
            date = datetime.strptime(date_str, "%m%d%Y")
            if extract_range_start <= date <= extract_range_end:
                result.append(filename)
        except Exception as e:
            logger.info(
                f"Skipping file {filename} because failed to determine if date is in range: {str(e)}"
            )
    return result

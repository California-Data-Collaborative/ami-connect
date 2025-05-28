import csv
from dataclasses import dataclass
from datetime import datetime
import logging
import json
import os
from typing import Generator, List, Tuple

import paramiko
import pytz

from amiadapters.base import BaseAMIAdapter
from amiadapters.config import ConfiguredSftp, ConfiguredStorageSink, ConfiguredStorageSinkType
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import BaseTaskOutputController, ExtractOutput
from amiadapters.storage.snowflake import SnowflakeStorageSink

logger = logging.getLogger(__name__)


@dataclass
class AclaraMeterAndRead:
    """
    Representation of row in a CSV from an Aclara server,
    which includes meter metadata and meter read data.

    NOTE: We make the attribute names match the column names in the Aclara CSV
    for code convenience.
    """

    AccountNumber: str
    MeterSN: str
    MTUID: str
    Port: str
    AccountType: str
    Address1: str
    City: str
    State: str
    Zip: str
    RawRead: str
    ScaledRead: str
    ReadingTime: str
    LocalTime: str
    Active: str
    Scalar: str
    MeterTypeID: str
    Vendor: str
    Model: str
    Description: str
    ReadInterval: str


class AclaraAdapter(BaseAMIAdapter):
    """
    AMI Adapter that uses SFTP to retrieve Aclara data.
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        configured_sftp: ConfiguredSftp,
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
        task_output_controller = self.create_task_output_controller(
            configured_task_output_controller, org_id
        )
        # Must create storage sinks here because they're Aclara-specific
        storage_sinks = []
        for sink in configured_sinks:
            if sink.type == ConfiguredStorageSinkType.SNOWFLAKE:
                storage_sinks.append(
                    AclaraSnowflakeStorageSink(
                        task_output_controller,
                        org_id,
                        org_timezone,
                        sink,
                    )
                )
        super().__init__(org_id, org_timezone, task_output_controller, storage_sinks)

    def name(self) -> str:
        return f"aclara-{self.org_id}"

    def extract(
        self, run_id: str, extract_range_start: datetime, extract_range_end: datetime
    ):
        logging.info(
            f"Connecting to Aclara SFTP for data between {extract_range_start} and {extract_range_end}"
        )

        with paramiko.SSHClient() as ssh:
            ssh.load_host_keys(self.local_known_hosts_file)
            ssh.connect(
                self.sftp_host,
                username=self.sftp_user,
                password=self.sftp_password,
                look_for_keys=False,
                allow_agent=False,
            )
            with ssh.open_sftp() as sftp:
                downloaded_files = self._download_meter_and_read_files_for_date_range(
                    sftp, extract_range_start, extract_range_end
                )

        meters_and_reads = self._parse_downloaded_files(downloaded_files)

        self.output_controller.write_extract_outputs(
            run_id,
            ExtractOutput({"meters_and_reads.json": "\n".join(meters_and_reads)}),
        )

    def _download_meter_and_read_files_for_date_range(
        self,
        sftp: paramiko.SFTPClient,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> List[str]:
        downloaded_files = []
        # Get the list of remote files
        all_files_on_server = sftp.listdir(self.sftp_meter_and_reads_folder)
        # TODO the files on the server contain data from outside the range, even if their filename suggests otherwise. Is this a problem? Looks like today's file contains data from the last 2.5 days or so.
        files_to_download = files_for_date_range(
            all_files_on_server, extract_range_start, extract_range_end
        )
        os.makedirs(self.local_download_directory, exist_ok=True)
        for file in files_to_download:
            local_csv = f"{self.local_download_directory}/{file}"

            # TODO clean up local files
            downloaded_files.append(local_csv)
            if not os.path.exists(local_csv):
                logging.info(
                    f"Downloading {file} from FTP at {self.sftp_host} to {local_csv}"
                )
                sftp.get(self.sftp_meter_and_reads_folder + "/" + file, local_csv)
            else:
                logging.info(
                    f"File {file} already downloaded at {local_csv}, will be included in output"
                )
        return downloaded_files

    def _parse_downloaded_files(self, files: List[str]) -> Generator[str, None, None]:
        """
        For each downloaded file, parse row into our meter and read object.
        Return Generator with all rows from all files.
        """
        for csv_file in files:
            with open(csv_file, newline="", encoding="utf-8") as f:
                csv_reader = csv.DictReader(f, delimiter=",")
                for data in csv_reader:
                    meter_and_read = AclaraMeterAndRead(**data)
                    yield json.dumps(meter_and_read, cls=DataclassJSONEncoder)

    def transform(self, run_id: str):
        extract_outputs = self.output_controller.read_extract_outputs(run_id)
        text = extract_outputs.from_file("meters_and_reads.json")
        raw_meters_with_reads = [
            AclaraMeterAndRead(**json.loads(d)) for d in text.strip().split("\n")
        ]

        transformed_meters, transformed_reads = self._transform_meters_and_reads(
            raw_meters_with_reads
        )

        self.output_controller.write_transformed_meters(run_id, transformed_meters)
        self.output_controller.write_transformed_meter_reads(run_id, transformed_reads)

    def _transform_meters_and_reads(
        self, raw_meters_with_reads: List[AclaraMeterAndRead]
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        """
        Example:
        AclaraMeterAndRead(AccountNumber='17305709',
                   MeterSN='1',
                   MTUID='2',
                   Port='1',
                   AccountType='RESIDENTIAL',
                   Address1='12 MY LN',
                   City='LOS ANGELES',
                   State='CA',
                   Zip='00000',
                   RawRead='23497071',
                   ScaledRead='023497.071',
                   ReadingTime='2025-05-25 16:00:00.000',
                   LocalTime='2025-05-25 09:00:00.000',
                   Active='1',
                   Scalar='0.001',
                   MeterTypeID='2212',
                   Vendor='BADGER',
                   Model='HR-E LCD',
                   Description='Badger M25/LP HRE LCD 5/8x3/4in 9D 0.001CuFt',
                   ReadInterval='60')
        """
        transformed_meters = set()
        transformed_reads = []

        for meter_and_read in raw_meters_with_reads:
            account_id = meter_and_read.AccountNumber

            if meter_and_read.AccountType == "DETECTOR CHECK":
                continue

            meter_id = meter_and_read.MTUID

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=meter_id,
                account_id=account_id,
                location_id=None,
                meter_id=meter_id,
                endpoint_id=meter_and_read.MeterSN,
                meter_install_date=None,
                meter_size=None,
                meter_manufacturer=meter_and_read.Vendor,
                multiplier=meter_and_read.Scalar,
                location_address=meter_and_read.Address1,
                location_city=meter_and_read.City,
                location_state=meter_and_read.State,
                location_zip=meter_and_read.Zip,
            )
            transformed_meters.add(meter)

            flowtime = self.datetime_from_iso_str(meter_and_read.ReadingTime, pytz.UTC)

            read = GeneralMeterRead(
                org_id=self.org_id,
                device_id=meter_id,
                account_id=account_id,
                location_id=None,
                flowtime=flowtime,
                register_value=float(meter_and_read.RawRead),
                register_unit=None,
                interval_value=None,
                interval_unit=None,
            )
            transformed_reads.append(read)

        return transformed_meters, transformed_reads


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
        except Exception:
            logger.info(f"Skipping file {filename} because failed to parse date")
    return result


class AclaraSnowflakeStorageSink(SnowflakeStorageSink):
    """
    Aclara implementation of Snowflake AMI Storage Sink. In addition to parent class's storage of generalized
    data, this stores raw meters and reads into a Snowflake table.
    """

    def __init__(
        self,
        output_controller: BaseTaskOutputController,
        org_id: str,
        org_timezone: str,
        sink_config: ConfiguredStorageSink,
    ):
        super().__init__(output_controller, sink_config)
        self.org_id = org_id
        self.org_timezone = org_timezone

    def store_raw(self, run_id):
        pass

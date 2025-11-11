import csv
from dataclasses import dataclass
from datetime import datetime
import logging
import json
import os
from typing import Generator, List, Tuple

import paramiko
import pytz
from pytz.tzinfo import DstTzInfo

from amiadapters.adapters.base import BaseAMIAdapter, GeneralMeterUnitOfMeasure
from amiadapters.configuration.models import SftpConfiguration
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader


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
            AclaraRawSnowflakeLoader(),
        )

    def name(self) -> str:
        return f"aclara-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        logger.info(
            f"Connecting to Aclara SFTP for data between {extract_range_start} and {extract_range_end}"
        )
        downloaded_files = []
        try:
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
                    downloaded_files = (
                        self._download_meter_and_read_files_for_date_range(
                            sftp, extract_range_start, extract_range_end
                        )
                    )

            meters_and_reads = self._parse_downloaded_files(downloaded_files)
            output = "\n".join(meters_and_reads)
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
        # Get the list of remote files
        all_files_on_server = sftp.listdir(self.sftp_meter_and_reads_folder)
        logger.info(f"Found {len(all_files_on_server)} total files on server")
        # The files often contain readings from 1-2 days before the date in their filename.
        # We've chosen to include those readings in the extract.
        files_to_download = files_for_date_range(
            all_files_on_server, extract_range_start, extract_range_end
        )
        if not files_to_download:
            raise Exception(
                f"No files found on server for range {extract_range_start} to {extract_range_end}"
            )
        os.makedirs(self.local_download_directory, exist_ok=True)
        for file in files_to_download:
            local_csv = f"{self.local_download_directory}/{file}"
            downloaded_files.append(local_csv)
            logger.info(
                f"Downloading {file} from FTP at {self.sftp_host} to {local_csv}"
            )
            sftp.get(self.sftp_meter_and_reads_folder + "/" + file, local_csv)
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

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        raw_meters_with_reads = extract_outputs.load_from_file(
            "meters_and_reads.json", AclaraMeterAndRead
        )
        return self._transform_meters_and_reads(raw_meters_with_reads)

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
        transformed_meters_by_device_id = {}
        transformed_reads_by_key = {}

        for meter_and_read in raw_meters_with_reads:
            account_id = meter_and_read.AccountNumber

            if meter_and_read.AccountType == "DETECTOR CHECK":
                continue

            device_id = meter_and_read.MeterSN

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=meter_and_read.MeterSN,
                account_id=account_id,
                location_id=None,
                meter_id=meter_and_read.MeterSN,
                endpoint_id=meter_and_read.MTUID,
                meter_install_date=None,
                meter_size=self.parse_meter_size_from_description(
                    meter_and_read.Description
                ),
                meter_manufacturer=meter_and_read.Vendor,
                multiplier=meter_and_read.Scalar,
                location_address=meter_and_read.Address1,
                location_city=meter_and_read.City,
                location_state=meter_and_read.State,
                location_zip=meter_and_read.Zip,
            )
            transformed_meters_by_device_id[device_id] = meter

            flowtime = self.datetime_from_iso_str(meter_and_read.ReadingTime, pytz.UTC)

            register_value = (
                float(meter_and_read.ScaledRead)
                if meter_and_read.ScaledRead != "ERROR"
                else None
            )
            register_value, register_unit = self.map_reading(
                register_value, GeneralMeterUnitOfMeasure.CUBIC_FEET
            )

            read = GeneralMeterRead(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=None,
                flowtime=flowtime,
                register_value=register_value,
                register_unit=register_unit,
                interval_value=None,
                interval_unit=None,
                battery=None,
                install_date=None,
                connection=None,
                estimated=None,
            )
            # Reads are unique by org_id, device_id, and flowtime. This ensures we do not include duplicates in our output.
            key = f"{read.device_id}-{read.flowtime}"
            transformed_reads_by_key[key] = read

        return list(transformed_meters_by_device_id.values()), list(
            transformed_reads_by_key.values()
        )

    def parse_meter_size_from_description(self, description: str) -> str:
        """
        Aclara does not give us meter size, so we try to parse it from the Description field.
        Sample of descriptions and their frequencies (the left most number) across a few days in May 2025:

        36 Sensus W2000 6" 8D 1CuFt
        72 Badger HRE LCD T450 3in 7D 1CuFt
        72 Badger Ultrasonic 4" 9D 0.01CuFt
        72 Elster evoQ4 3in 8D 1CuFt
        72 Elster evoQ4 6in 8D 1CuFt
        72 M35 Badger HR-E LCD 3/4in 9D 0.001Cu.Ft.
        72 SENSUS OMNI C2 3" 7D 1CuFt
        78 Elster evoQ4 4in 8D 1CuFt
        138 Badger HR E-Series 1.5in 9D 0.01Cu.Ft. DD
        144 M170 Badger HR-E LCD 2in 9D 0.01Cu.Ft.
        210 M120 Badger HR-E LCD 1.5in 9D 0.01Cu.Ft.
        216 Badger G2 3" 9D .01 Cu.Ft.
        216 SENSUS OMNI C2 1.5  7D 1CuFt
        282 Badger HR E-Series 3/4in 9D 0.001Cu.Ft. DD
        438 M70 Badger HR-E LCD 1in 9D 0.001Cu.Ft.
        576 SENSUS OMNI C2 2"  7D 1CuFt
        930 SENSUS OMNI T2 2" 7D 1CuFt
        1278 Sensus SRII/aS E-Register 3/4 6D 1CuFt
        1506 Badger HR E-Series 1in 9D 0.001Cu.Ft. DD
        1509 Badger HR E-Series 2in 9D 0.01CuFt
        2238 Badger HR-E LCD LP/M25 5/8x3/4in 9D 0.001Cu.Ft. DD
        2598 Badger M170 HRE LCD 2in 9D 0.01CuFt
        2611 Badger HR E-Series 1.5 Inch 9D 0.01CuFt
        2794 Badger M120 HRE LCD 1.5in 9D 0.01CuFt
        3204 Badger HR E-Series 5/8in 9D 0.001Cu.Ft. DD
        3263 Badger HRE E-Series 3/4in 9D 0.001CuFt
        5640 Sensus SRII/aS E-Register 5/8x3/4 6D 1CuFt
        11610 Badger M35 HRE LCD 3/4in 9D 0.001CuFt
        11832 Badger M40/M55/M70 HRE LCD 1in 9D 0.001CuFt
        15119 Badger HRE E-Series 1in 9D 0.001CuFt
        39258 Sensus SRII/aS E-Register 1 6D 1CuFt
        44435 Badger HRE E-Series 5/8in 9D 0.001CuFt
        403428 Badger M25/LP HRE LCD 5/8x3/4in 9D 0.001CuFt
        """
        if description is None:
            return None
        parts = description.strip().split(" ")
        if len(parts) > 4:
            mapped = self.map_meter_size(parts[4])
            if mapped is not None:
                return mapped
        if len(parts) > 5:
            mapped = self.map_meter_size(parts[5])
            if mapped is not None:
                return mapped
        if len(parts) > 3:
            mapped = self.map_meter_size(parts[3])
            if mapped is not None:
                return mapped
        if len(parts) > 2:
            mapped = self.map_meter_size(parts[2])
            if mapped is not None:
                return mapped
        logger.info(f"Could not find meter size in description: {description}")
        return None


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


class AclaraRawSnowflakeLoader(RawSnowflakeLoader):
    """
    Aclara implementation of raw storage in Snowflake.
    """

    def table_loaders(self):
        fields = set(AclaraMeterAndRead.__dataclass_fields__.keys())
        # The field "localtime" is a reserved word in Snowflake, so we need to escape it
        # import pdb; pdb.set_trace()
        columns = {f if f != "LocalTime" else "`LOCALTIME`" for f in fields}
        return [
            RawSnowflakeTableLoader(
                table_name="aclara_base",
                fields=fields,
                columns=columns,
                unique_by=["device_id", "readingtime"],
                parse_raw_data_fn=lambda extract_outputs: extract_outputs.load_from_file(
                    "meters_and_reads.json", AclaraMeterAndRead
                ),
            ),
        ]

    # def _previous_load(
    #     self,
    #     run_id: str,
    #     org_id: str,
    #     org_timezone: DstTzInfo,
    #     extract_outputs: ExtractOutput,
    #     snowflake_conn,
    # ):
    #     raw_meters_with_reads = extract_outputs.load_from_file(
    #         "meters_and_reads.json", AclaraMeterAndRead
    #     )

    #     created_time = datetime.now(tz=org_timezone)

    #     create_temp_table_sql = (
    #         "CREATE OR REPLACE TEMPORARY TABLE temp_aclara_base LIKE aclara_base;"
    #     )
    #     snowflake_conn.cursor().execute(create_temp_table_sql)

    #     insert_temp_data_sql = f"""
    #         INSERT INTO temp_aclara_base (
    #             org_id,
    #             device_id,
    #             created_time,
    #             ACCOUNTNUMBER,
    #             METERSN,
    #             MTUID,
    #             PORT,
    #             ACCOUNTTYPE,
    #             ADDRESS1,
    #             CITY,
    #             STATE,
    #             ZIP,
    #             RAWREAD,
    #             SCALEDREAD,
    #             READINGTIME,
    #             `LOCALTIME`,
    #             ACTIVE,
    #             SCALAR,
    #             METERTYPEID,
    #             VENDOR,
    #             MODEL,
    #             DESCRIPTION,
    #             READINTERVAL
    #         )
    #             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #     """
    #     rows = [
    #         tuple(
    #             [
    #                 org_id,
    #                 i.MeterSN,
    #                 created_time,
    #                 i.AccountNumber,
    #                 i.MeterSN,
    #                 i.MTUID,
    #                 i.Port,
    #                 i.AccountType,
    #                 i.Address1,
    #                 i.City,
    #                 i.State,
    #                 i.Zip,
    #                 i.RawRead,
    #                 i.ScaledRead,
    #                 i.ReadingTime,
    #                 i.LocalTime,
    #                 i.Active,
    #                 i.Scalar,
    #                 i.MeterTypeID,
    #                 i.Vendor,
    #                 i.Model,
    #                 i.Description,
    #                 i.ReadInterval,
    #             ]
    #         )
    #         for i in raw_meters_with_reads
    #     ]
    #     snowflake_conn.cursor().executemany(insert_temp_data_sql, rows)

    #     merge_sql = f"""
    #         MERGE INTO aclara_base AS target
    #         USING (
    #             -- Use GROUP BY to ensure there are no duplicate rows before merge
    #             SELECT
    #                 org_id,
    #                 device_id,
    #                 READINGTIME,
    #                 max(ACCOUNTNUMBER) as ACCOUNTNUMBER,
    #                 max(METERSN) as METERSN,
    #                 max(MTUID) as MTUID,
    #                 max(PORT) as PORT,
    #                 max(ACCOUNTTYPE) as ACCOUNTTYPE,
    #                 max(ADDRESS1) as ADDRESS1,
    #                 max(CITY) as CITY,
    #                 max(STATE) as STATE,
    #                 max(ZIP) as ZIP,
    #                 max(RAWREAD) as RAWREAD,
    #                 max(SCALEDREAD) as SCALEDREAD,
    #                 max(`LOCALTIME`) as `LOCALTIME`,
    #                 max(ACTIVE) as ACTIVE,
    #                 max(SCALAR) as SCALAR,
    #                 max(METERTYPEID) as METERTYPEID,
    #                 max(VENDOR) as VENDOR,
    #                 max(MODEL) as MODEL,
    #                 max(DESCRIPTION) as DESCRIPTION,
    #                 max(READINTERVAL) as READINTERVAL,
    #                 max(created_time) as created_time
    #             FROM temp_aclara_base
    #             GROUP BY org_id, device_id, READINGTIME
    #         ) AS source
    #         ON source.org_id = target.org_id
    #             AND source.device_id = target.device_id
    #             AND source.READINGTIME = target.READINGTIME
    #         WHEN MATCHED THEN
    #             UPDATE SET
    #                 target.created_time = source.created_time,
    #                 target.ACCOUNTNUMBER = source.ACCOUNTNUMBER,
    #                 target.METERSN = source.METERSN,
    #                 target.MTUID = source.MTUID,
    #                 target.PORT = source.PORT,
    #                 target.ACCOUNTTYPE = source.ACCOUNTTYPE,
    #                 target.ADDRESS1 = source.ADDRESS1,
    #                 target.CITY = source.CITY,
    #                 target.STATE = source.STATE,
    #                 target.ZIP = source.ZIP,
    #                 target.RAWREAD = source.RAWREAD,
    #                 target.SCALEDREAD = source.SCALEDREAD,
    #                 target.`LOCALTIME` = source.`LOCALTIME`,
    #                 target.ACTIVE = source.ACTIVE,
    #                 target.SCALAR = source.SCALAR,
    #                 target.METERTYPEID = source.METERTYPEID,
    #                 target.VENDOR = source.VENDOR,
    #                 target.MODEL = source.MODEL,
    #                 target.DESCRIPTION = source.DESCRIPTION,
    #                 target.READINTERVAL = source.READINTERVAL
    #         WHEN NOT MATCHED THEN
    #             INSERT (
    #                 org_id,
    #                 device_id,
    #                 ACCOUNTNUMBER,
    #                 METERSN,
    #                 MTUID,
    #                 PORT,
    #                 ACCOUNTTYPE,
    #                 ADDRESS1,
    #                 CITY,
    #                 STATE,
    #                 ZIP,
    #                 RAWREAD,
    #                 SCALEDREAD,
    #                 READINGTIME,
    #                 `LOCALTIME`,
    #                 ACTIVE,
    #                 SCALAR,
    #                 METERTYPEID,
    #                 VENDOR,
    #                 MODEL,
    #                 DESCRIPTION,
    #                 READINTERVAL,
    #                 created_time)
    #                     VALUES (
    #                     source.org_id,
    #                     source.device_id,
    #                     source.ACCOUNTNUMBER,
    #                     source.METERSN,
    #                     source.MTUID,
    #                     source.PORT,
    #                     source.ACCOUNTTYPE,
    #                     source.ADDRESS1,
    #                     source.CITY,
    #                     source.STATE,
    #                     source.ZIP,
    #                     source.RAWREAD,
    #                     source.SCALEDREAD,
    #                     source.READINGTIME,
    #                     source.`LOCALTIME`,
    #                     source.ACTIVE,
    #                     source.SCALAR,
    #                     source.METERTYPEID,
    #                     source.VENDOR,
    #                     source.MODEL,
    #                     source.DESCRIPTION,
    #                     source.READINTERVAL,
    #                     source.created_time)
    #     """
    #     snowflake_conn.cursor().execute(merge_sql)

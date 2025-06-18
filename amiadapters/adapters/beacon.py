import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
import json
import logging
import os
import time
from typing import Generator, List, Tuple

from pytz.tzinfo import DstTzInfo
import requests

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import BaseTaskOutputController, ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader

logger = logging.getLogger(__name__)


@dataclass
class Beacon360MeterAndRead:
    """
    Representation of row in the Beacon 360 range report CSV, which includes
    meter metadata and meter read data.

    NOTE: We make the attribute names match the column names in the Beacon 360 API call and resulting CSV
    for code convenience.
    """

    Account_ID: str
    Current_Leak_Rate: str
    Current_Leak_Start_Date: str
    Demand_Zone_ID: str
    Dials: str
    Endpoint_Install_Date: str
    Endpoint_SN: str
    Estimated_Flag: str
    Flow: str
    Flow_Time: str
    Flow_Unit: str
    Location_Address_Line1: str
    Location_Address_Line2: str
    Location_Address_Line3: str
    Location_City: str
    Location_Country: str
    Location_ID: str
    Location_State: str
    Location_ZIP: str
    Location_Continuous_Flow: str
    Location_Latitude: str
    Location_Longitude: str
    Location_Irrigated_Area: str
    Location_Irrigation: str
    Location_Main_Use: str
    Location_Name: str
    Location_Pool: str
    Location_Water_Type: str
    Location_Year_Built: str
    Meter_ID: str
    Meter_Install_Date: str
    Meter_Manufacturer: str
    Meter_Model: str
    Meter_Size: str
    Meter_Size_Desc: str
    Meter_Size_Unit: str
    Meter_SN: str
    Raw_Read: str
    Read: str
    Read_Time: str
    Read_Unit: str
    Register_Number: str
    Register_Resolution: str
    SA_Start_Date: str
    Service_Point_Class_Code: str
    Service_Point_Class_Code_Normalized: str


# Columns we'll request from Beacon 360 API
REQUESTED_COLUMNS = list(Beacon360MeterAndRead.__dataclass_fields__.keys())


class Beacon360Adapter(BaseAMIAdapter):
    """
    AMI Adapter that retrieves data from the Beacon 360 V2 API.

    API Documentation: https://helpbeaconama.net/beacon-web-services/export-data-service-v2-api-preview/#POSTread
    """

    def __init__(
        self,
        api_user: str,
        api_password: str,
        use_cache: bool,
        org_id: str,
        org_timezone: DstTzInfo,
        configured_task_output_controller,
        configured_sinks,
        cache_output_folder: str = "./output",
    ):
        self.user = api_user
        self.password = api_password
        self.use_cache = use_cache
        self.cache_output_folder = cache_output_folder
        super().__init__(
            org_id,
            org_timezone,
            configured_task_output_controller,
            configured_sinks,
            BeaconRawSnowflakeLoader(),
        )

    def name(self) -> str:
        return f"beacon-360-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
        device_ids: List[str] = None,
    ):
        report = self._fetch_range_report(
            extract_range_start, extract_range_end, device_ids=device_ids
        )
        logger.info("Fetched report")
        return ExtractOutput({"meters_and_reads.json": self._report_to_output(report)})

    def _report_to_output(self, report: str):
        return "\n".join(self._report_to_output_stream(report))

    def _report_to_output_stream(self, report: str) -> Generator[str, None, None]:
        csv_reader = csv.DictReader(StringIO(report), delimiter=",")
        for data in csv_reader:
            meter_and_read = Beacon360MeterAndRead(**data)
            yield json.dumps(meter_and_read, cls=DataclassJSONEncoder)

    def _fetch_range_report(
        self,
        extract_range_start: datetime,
        extract_range_end: datetime,
        device_ids: List[str] = None,
    ) -> str:
        """
        Return range report as CSV string, first line with headers.
        Retrieve from cache if configured to do so.
        """
        if self.use_cache:
            logger.info("Attempting to load report from cache")
            cached_report = self._get_cached_report(
                extract_range_start, extract_range_end
            )
            if cached_report is not None:
                logger.info("Loaded report from cache")
                return cached_report
            else:
                logger.info(
                    "Could not load report from cache, continuing with calls to API"
                )

        auth = requests.auth.HTTPBasicAuth(self.user, self.password)

        params = {
            "Start_Date": extract_range_start,
            "End_Date": extract_range_end,
            "Resolution": "hourly",
            "Header_Columns": ",".join(REQUESTED_COLUMNS),
            "Has_Endpoint": True,
        }
        if device_ids:
            params["Meter_ID"] = ",".join(device_ids)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # Request report generation, receive a link for watching its status
        logger.info(
            f"Requesting report for meter reads between {params['Start_Date']} and {params['End_Date']} at {params['Resolution']} resolution"
        )
        if "Meter_ID" in params:
            logger.info(f"Filtering to Meter_IDs: {params["Meter_ID"]}")
        generate_report_response = requests.post(
            url="https://api.beaconama.net/v2/eds/range",
            headers=headers,
            params=params,
            auth=auth,
        )

        if generate_report_response.status_code == 429:
            # Rate limit exceeded
            t = generate_report_response.json()
            secs_to_wait = int(t["args"][2])
            time_to_resume = datetime.now() + timedelta(seconds=secs_to_wait)
            logger.warning(
                f"need to wait {secs_to_wait} seconds until {time_to_resume} ({t})"
            )
            raise Exception("Rate limit exceeded")
        elif generate_report_response.status_code != 202:
            logger.error(
                f"error when requesting report. status code: {generate_report_response.status_code}"
            )
            raise Exception("Failed request to generate report")

        status_url = generate_report_response.json()["statusUrl"]

        # Poll for report status
        i = 0
        max_attempts = 120  # number of minutes
        while True:
            i += 1
            if i >= max_attempts:
                raise Exception(
                    f"Reached max attempts ({max_attempts}) polling for report status"
                )

            logger.info(
                f"Attempt {i}/{max_attempts} while polling for status on report at {status_url}"
            )

            status_response = requests.get(
                url=f"https://api.beaconama.net{status_url}", headers=headers, auth=auth
            )

            if status_response.status_code != 200:
                logger.error(
                    f"error when requesting status. status code: {status_response.status_code} message: {status_response.text}"
                )
                raise Exception("Failed request to get report status")

            status_response_data = status_response.json()
            logger.info(f"Status: {status_response_data}")

            if status_response_data.get("state") == "done":
                break
            elif status_response_data.get("state") == "exception":
                logger.error(
                    f"error found in report status: {status_response_data.get("message")}"
                )
                raise Exception("Exception found in report status")
            else:
                sleep_interval_seconds = 60
                logger.info(f"Sleeping for {sleep_interval_seconds} seconds")
                time.sleep(sleep_interval_seconds)

        # Download report
        report_url = status_response_data["reportUrl"]
        try:
            logger.info(f"Downloading report at {report_url}")
            report_response = self._fetch_report(report_url, headers, auth)
        except Exception as e:
            logger.info(f"Exception downloading report at {report_url}: {e}. Retrying.")
            logger.info(f"Sleeping before retry.")
            time.sleep(60)
            logger.info(f"Retrying download for report at {report_url}")
            report_response = self._fetch_report(report_url, headers, auth)

        if report_response.status_code != 200:
            logger.warning(
                f"Error when downloading report. status code: {report_response.status_code}"
            )
            raise Exception("Failed request to download report")

        report = report_response.text

        self._write_cached_report_and_delete_old_cached_files(
            report, extract_range_start, extract_range_end
        )

        return report

    @staticmethod
    def _fetch_report(report_url, headers, auth):
        return requests.get(
            url="https://api.beaconama.net" + report_url, headers=headers, auth=auth
        )

    def _get_cached_report(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> str:
        cache_file = self._cached_report_file(extract_range_start, extract_range_end)
        if os.path.exists(cache_file):
            with open(cache_file, "r") as f:
                return f.read()
        return None

    def _write_cached_report_and_delete_old_cached_files(
        self, report: str, extract_range_start: datetime, extract_range_end: datetime
    ):
        cache_file = self._cached_report_file(extract_range_start, extract_range_end)
        logger.info(f"Caching report contents at {cache_file}")
        directory = os.path.dirname(cache_file)
        # Create all necessary parent directories
        if directory:
            logger.info(f"Making parent directories for {cache_file}")
            os.makedirs(directory, exist_ok=True)

        # Remove old cache files so we don't fill up the disk
        previous_cache_files = [
            os.path.join(self.cache_output_folder, f)
            for f in os.listdir(directory)
            if "cached-report" in f
        ]
        for f in previous_cache_files:
            os.remove(f)
            logger.info(f"Deleted old cache file {f}")
        print(cache_file)
        with open(cache_file, "w") as f:
            f.write(report)
        logger.info(f"Cached report contents at {cache_file}")

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        text = extract_outputs.from_file("meters_and_reads.json")
        raw_meters_with_reads = [
            Beacon360MeterAndRead(**json.loads(d)) for d in text.strip().split("\n")
        ]
        return self._transform_meters_and_reads(raw_meters_with_reads)

    def _transform_meters_and_reads(
        self, raw_meters_with_reads: List[Beacon360MeterAndRead]
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        transformed_meters_by_device_id = {}
        transformed_reads_by_key = {}
        for meter_and_read in raw_meters_with_reads:
            account_id = meter_and_read.Account_ID
            location_id = meter_and_read.Location_ID
            device_id = meter_and_read.Endpoint_SN

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                meter_id=meter_and_read.Meter_ID,
                endpoint_id=meter_and_read.Endpoint_SN,
                meter_install_date=self.datetime_from_iso_str(
                    meter_and_read.Meter_Install_Date, self.org_timezone
                ),
                meter_size=self.map_meter_size(meter_and_read.Meter_Size_Desc),
                meter_manufacturer=meter_and_read.Meter_Manufacturer,
                multiplier=None,
                location_address=meter_and_read.Location_Address_Line1,
                location_city=meter_and_read.Location_City,
                location_state=meter_and_read.Location_State,
                location_zip=meter_and_read.Location_ZIP,
            )

            transformed_meters_by_device_id[device_id] = meter

            flowtime = self.datetime_from_iso_str(
                meter_and_read.Read_Time, self.org_timezone
            )
            if flowtime is None:
                logger.info(
                    f"Skipping read with no flowtime for account={account_id} location={location_id} device={device_id}"
                )
                continue

            read = GeneralMeterRead(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                flowtime=flowtime,
                register_value=float(meter_and_read.Read),
                register_unit=self.map_unit_of_measure(meter_and_read.Read_Unit),
                interval_value=None,
                interval_unit=None,
            )
            # Reads are unique by org_id, device_id, and flowtime. This ensures we do not include duplicates in our output.
            key = f"{read.device_id}-{read.flowtime}"
            transformed_reads_by_key[key] = read

        return list(transformed_meters_by_device_id.values()), list(
            transformed_reads_by_key.values()
        )

    def _cached_report_file(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> str:
        start, end = extract_range_start.isoformat(), extract_range_end.isoformat()
        return os.path.join(
            self.cache_output_folder, f"{self.name()}-{start}-{end}-cached-report.txt"
        )


class BeaconRawSnowflakeLoader(RawSnowflakeLoader):

    def load(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        output_controller: BaseTaskOutputController,
        snowflake_conn,
    ):
        extract_outputs = output_controller.read_extract_outputs(run_id)
        text = extract_outputs.from_file("meters_and_reads.json")
        raw_meters_with_reads = [
            Beacon360MeterAndRead(**json.loads(d)) for d in text.strip().split("\n")
        ]

        create_temp_table_sql = "CREATE OR REPLACE TEMPORARY TABLE temp_beacon_360_base LIKE beacon_360_base;"
        snowflake_conn.cursor().execute(create_temp_table_sql)

        columns = ", ".join(REQUESTED_COLUMNS)
        qmarks = "?, " * (len(REQUESTED_COLUMNS) - 1) + "?"
        insert_temp_data_sql = f"""
            INSERT INTO temp_beacon_360_base (org_id, device_id, created_time, {columns}) 
                VALUES (?, ?, ?, {qmarks})
        """
        created_time = datetime.now(tz=org_timezone)
        rows = [
            tuple(
                [org_id, i.Endpoint_SN, created_time]
                + [i.__getattribute__(name) for name in REQUESTED_COLUMNS]
            )
            for i in raw_meters_with_reads
        ]
        snowflake_conn.cursor().executemany(insert_temp_data_sql, rows)

        merge_sql = f"""
            MERGE INTO beacon_360_base AS target
            USING (
                -- Use GROUP BY to ensure there are no duplicate rows before merge
                SELECT 
                    org_id,
                    device_id, 
                    Read_Time, 
                    {", ".join([f"max({name}) as {name}" for name in REQUESTED_COLUMNS if name not in {"Read_Time",}])}, 
                    max(created_time) as created_time
                FROM temp_beacon_360_base
                GROUP BY org_id, device_id, Read_Time
            ) AS source
            ON source.org_id = target.org_id 
                AND source.device_id = target.device_id
                AND source.Read_Time = target.Read_Time
            WHEN MATCHED THEN
                UPDATE SET
                    target.created_time = source.created_time,
                    {",".join([f"target.{name} = source.{name}" for name in REQUESTED_COLUMNS])}
            WHEN NOT MATCHED THEN
                INSERT (org_id, device_id, {", ".join(name for name in REQUESTED_COLUMNS)}, created_time) 
                        VALUES (source.org_id, source.device_id, {", ".join(f"source.{name}" for name in REQUESTED_COLUMNS)}, source.created_time)
        """
        snowflake_conn.cursor().execute(merge_sql)

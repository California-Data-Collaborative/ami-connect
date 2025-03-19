import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
import json
import logging
import os
import requests
import time
from typing import List, Tuple

from amiadapters.base import BaseAMIAdapter, DataclassJSONEncoder, GeneralMeter, \
    GeneralMeterRead, GeneralModelJSONEncoder
from amiadapters.config import AMIAdapterConfiguration

logger = logging.getLogger(__name__)

REQUESTED_COLUMNS = [
    'Account_Billing_Cycle',
    'Account_Email',
    'Account_First_Name',
    'Account_Full_Name',
    'Account_ID',
    'Account_Last_Name',
    'Account_Phone',
    'Account_Portal_Status',
    'Account_Status',
    'Alert_Code',
    'Backflow_Gallons',
    'Battery_Level',
    'Billing_Address_Line1',
    'Billing_Address_Line2',
    'Billing_Address_Line3',
    'Billing_City',
    'Billing_Country',
    'Billing_State',
    'Billing_Zip',
    'Connector_Type',
    'Current_Leak_Rate',
    'Current_Leak_Start_Date',
    'Demand_Zone_ID',
    'Dials',
    'Endpoint_Install_Date',
    'Endpoint_SN',
    'Endpoint_Status',
    'Endpoint_Type',
    'Estimated_Flag',
    'Flow',
    'Flow_Time',
    'Flow_Unit',
    'High_Read_Limit',
    'Last_Comm_Time',
    'Location_Address_Line1',
    'Location_Address_Line2',
    'Location_Address_Line3',
    'Location_Address_Parity',
    'Location_Area',
    'Location_Bathrooms',
    'Location_Building_Number',
    'Location_Building_Type',
    'Location_City',
    'Location_Continuous_Flow',
    'Location_Country',
    'Location_County_Name',
    'Location_DHS_Code',
    'Location_District',
    'Location_Funding',
    'Location_ID',
    'Location_Irrigated_Area',
    'Location_Irrigation',
    'Location_Latitude',
    'Location_Longitude',
    'Location_Main_Use',
    'Location_Name',
    'Location_Pool',
    'Location_Population',
    'Location_Site',
    'Location_State',
    'Location_Water_Type',
    'Location_Year_Built',
    'Location_Zip',
    'Low_Read_Limit',
    'Meter_Continuous_Flow',
    'Meter_ID',
    'Meter_Install_Date',
    'Meter_Manufacturer',
    'Meter_Model',
    'Meter_Note',
    'Meter_Size',
    'Meter_Size_Desc',
    'Meter_Size_Unit',
    'Meter_SN',
    'Person_ID',
    'Portal_ID',
    'Raw_Read',
    'Read',
    'Read_Code_1',
    'Read_Code_2',
    'Read_Code_3',
    'Read_Method',
    'Read_Note',
    'Read_Sequence',
    'Read_Time',
    'Read_Unit',
    'Reader_Initials',
    'Register_Note',
    'Register_Number',
    'Register_Resolution',
    'Register_Unit_Of_Measure',
    'SA_Start_Date',
    'Service_Point_Class_Code',
    'Service_Point_Class_Code_Normalized',
    'Service_Point_Cycle',
    'Service_Point_ID',
    'Service_Point_Latitude',
    'Service_Point_Longitude',
    'Service_Point_Route',
    'Service_Point_Timezone',
    'Service_Point_Type',
    'Signal_Strength',
    'Supply_Zone_ID',
    'Trouble_Code',
    'Utility_Use_1',
    'Utility_Use_2'
]


@dataclass
class Beacon360MeterAndRead:
    """
    Representation of row in the Beacon 360 range report CSV, which includes
    meter metadata and meter read data.

    We make the attribute names match the column names in the Beacon 360 CSV for code convenience.
    """
    Account_Billing_Cycle: str
    Account_Email: str
    Account_First_Name: str
    Account_Full_Name: str
    Account_ID: str
    Account_Last_Name: str
    Account_Phone: str
    Account_Portal_Status: str
    Account_Status: str
    Alert_Code: str
    Backflow_Gallons: str
    Battery_Level: str
    Billing_Address_Line1: str
    Billing_Address_Line2: str
    Billing_Address_Line3: str
    Billing_City: str
    Billing_Country: str
    Billing_State: str
    Billing_ZIP: str
    Connector_Type: str
    Current_Leak_Rate: str
    Current_Leak_Start_Date: str
    Demand_Zone_ID: str
    Dials: str
    Endpoint_Install_Date: str
    Endpoint_SN: str
    Endpoint_Status: str
    Endpoint_Type: str
    Estimated_Flag: str
    Flow: str
    Flow_Time: str
    Flow_Unit: str
    High_Read_Limit: str
    Last_Comm_Time: str
    Location_Address_Line1: str
    Location_Address_Line2: str
    Location_Address_Line3: str
    Location_Address_Parity: str
    Location_Area: str
    Location_Bathrooms: str
    Location_Building_Number: str
    Location_Building_Type: str
    Location_City: str
    Location_Continuous_Flow: str
    Location_Country: str
    Location_County_Name: str
    Location_DHS_Code: str
    Location_District: str
    Location_Funding: str
    Location_ID: str
    Location_Irrigated_Area: str
    Location_Irrigation: str
    Location_Latitude: str
    Location_Longitude: str
    Location_Main_Use: str
    Location_Name: str
    Location_Pool: str
    Location_Population: str
    Location_Site: str
    Location_State: str
    Location_Water_Type: str
    Location_Year_Built: str
    Location_ZIP: str
    Low_Read_Limit: str
    Meter_Continuous_Flow: str
    Meter_ID: str
    Meter_Install_Date: str
    Meter_Manufacturer: str
    Meter_Model: str
    Meter_Note: str
    Meter_Size: str
    Meter_Size_Desc: str
    Meter_Size_Unit: str
    Meter_SN: str
    Person_ID: str
    Portal_ID: str
    Raw_Read: str
    Read: str
    Read_Code_1: str
    Read_Code_2: str
    Read_Code_3: str
    Read_Method: str
    Read_Note: str
    Read_Sequence: str
    Read_Time: str
    Read_Unit: str
    Reader_Initials: str
    Register_Note: str
    Register_Number: str
    Register_Resolution: str
    Register_Unit_Of_Measure: str
    SA_Start_Date: str
    Service_Point_Class_Code: str
    Service_Point_Class_Code_Normalized: str
    Service_Point_Cycle: str
    Service_Point_ID: str
    Service_Point_Latitude: str
    Service_Point_Longitude: str
    Service_Point_Route: str
    Service_Point_Timezone: str
    Service_Point_Type: str
    Signal_Strength: str
    Supply_Zone_ID: str
    Trouble_Code: str
    Utility_Use_1: str
    Utility_Use_2: str


class Beacon360Adapter(BaseAMIAdapter):
    """
    AMI Adapter that retrieves data from the Beacon 360 V2 API.

    API Documentation: https://helpbeaconama.net/beacon-web-services/export-data-service-v2-api-preview/#POSTread
    """

    def __init__(self, config: AMIAdapterConfiguration):
        self.output_folder = config.output_folder
        self.user = config.beacon_360_user
        self.password = config.beacon_360_password
        # Use locally cached report instead of fetching from API
        # Probably don't want to use this in production
        # TODO expose this in settings, maybe make it default false?
        self.use_cache = True
    
    def name(self) -> str:
        return f"beacon-360-api"
    
    def extract(self):
        report = self._fetch_range_report()
        meter_with_reads = self._parse_raw_range_report(report)
        with open(self._raw_reads_output_file(), "w") as f:
            content = "\n".join(json.dumps(m, cls=DataclassJSONEncoder) for m in meter_with_reads)
            f.write(content)
    
    def _fetch_range_report(self) -> str:
        """
        Return range report as CSV string, first line with headers.
        Retrieve from cache if configured to do so.
        """
        if self.use_cache:
            cached_report = self._get_cached_report()
            if cached_report is not None:
                logger.info("Loading report from cache")
                return cached_report
        
        auth = requests.auth.HTTPBasicAuth(self.user, self.password)
        
        params = {
            'Start_Date': datetime(2024,8,1),
            'End_Date': datetime(2024,8,2),
            'Resolution': "hourly",
            'Header_Columns': ','.join(REQUESTED_COLUMNS),
            'Has_Endpoint': True
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        # Request report generation, receive a link for watching its status
        logger.info(f"Requesting report for meter reads between {params['Start_Date']} and {params['End_Date']} at {params['Resolution']} resolution")
        generate_report_response = requests.post(
            url='https://api.beaconama.net/v2/eds/range',
            headers=headers,
            params=params,
            auth=auth
        )

        if generate_report_response.status_code == 429:
            # Rate limit exceeded
            t = generate_report_response.json()
            secs_to_wait = int(t['args'][2])
            time_to_resume = datetime.now() + timedelta(seconds=secs_to_wait)
            logger.warning(f"need to wait {secs_to_wait} seconds until {time_to_resume} ({t})")
            raise Exception("Rate limit exceeded")
        elif generate_report_response.status_code != 202:
            logger.error(f"error when requesting report. status code: {generate_report_response.status_code}")
            raise Exception("Failed request to generate report")
        
        status_url = generate_report_response.json()['statusUrl']

        # Poll for report status
        i = 0
        max_attempts = 15  # 15 minutes
        while True:
            i += 1
            if i >= max_attempts:
                raise Exception(f"Reached max attempts ({max_attempts}) polling for report status")
            
            logger.info(f"Attempt {i}/{max_attempts} while polling for status on report at {status_url}")
            
            status_response = requests.get(
                url=f"https://api.beaconama.net{status_url}",
                headers=headers,
                auth=auth)
            
            if status_response.status_code != 200:
                logger.error(f"error when requesting status. status code: {status_response.status_code} message: {status_response.text}")
                raise Exception("Failed request to get report status")

            status_response_data = status_response.json()
            logger.info(f"Status: {status_response_data}")

            if status_response_data.get("state") == "done":
                break
            elif status_response_data.get("state") == "exception":
                logger.error(f"error found in report status: {status_response_data.get("message")}")
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
            logger.warning(f"Error when downloading report. status code: {report_response.status_code}")
            raise Exception("Failed request to download report")

        report = report_response.text

        if self.use_cache:
            logger.info(f"Caching report contents at {self._cached_report_file()}")
            with open(self._cached_report_file(), "w") as f:
                f.write(report)

        return report
    
    @staticmethod
    def _fetch_report(report_url, headers, auth):
        return requests.get(
            url="https://api.beaconama.net" + report_url,
            headers=headers,
            auth=auth
        )
    
    def _get_cached_report(self) -> str:
        if os.path.exists(self._cached_report_file()):
            with open(self._cached_report_file(), "r") as f:
                return f.read()
        return None

    def _parse_raw_range_report(self, report: str) -> List[Beacon360MeterAndRead]:
        """
        Convert the CSV string of a range report into our
        raw model in prep for output.

        Assumes Beacon360MeterAndRead attributes are identical to CSV column names.
        """
        report_csv_rows = report.strip().split("\n")
        if not report_csv_rows:
            return

        csv_reader = csv.DictReader(StringIO(report), delimiter=',')
        meter_with_reads = []
        for data in csv_reader:
            meter_and_read = Beacon360MeterAndRead(**data)
            meter_with_reads.append(meter_and_read)
        
        return meter_with_reads
    
    def transform(self):
        with open(self._raw_reads_output_file(), "r") as f:
            text = f.read()
            raw_meters_with_reads = [Beacon360MeterAndRead(**json.loads(d)) for d in text.strip().split("\n")]
        
        transformed_meters, transformed_reads = self._transform_meters_and_reads(raw_meters_with_reads)

        with open(self._transformed_meter_output_file(), "w") as f:
            f.write("\n".join(json.dumps(v, cls=GeneralModelJSONEncoder) for v in transformed_meters))

        with open(self._transformed_reads_output_file(), "w") as f:
            f.write("\n".join(json.dumps(m, cls=GeneralModelJSONEncoder) for m in transformed_reads))
    
    def _transform_meters_and_reads(self, raw_meters_with_reads: List[Beacon360MeterAndRead]) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        transformed_meters = set()
        transformed_reads = []
        for meter_and_read in raw_meters_with_reads:
            account_id = meter_and_read.Account_ID
            location_id = meter_and_read.Location_ID
            meter_id = meter_and_read.Meter_ID

            meter = GeneralMeter(
                account_id=account_id,
                location_id=location_id,
                meter_id=meter_id,
                size_inches=meter_and_read.Meter_Size_Desc,
            )
            transformed_meters.add(meter)

            flowtime = datetime.fromisoformat(meter_and_read.Read_Time) if meter_and_read.Read_Time else None
            if flowtime is None:
                logger.info(f"Skipping read with no flowtime for account={account_id} location={location_id} meter={meter_id}")
                continue

            read = GeneralMeterRead(
                account_id=account_id,
                location_id=location_id,
                meter_id=meter_id,
                flowtime=flowtime,
                raw_value=float(meter_and_read.Read),
                raw_unit=meter_and_read.Read_Unit,
            )
            transformed_reads.append(read)

        return list(transformed_meters), transformed_reads
    
    def _cached_report_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-cached-report.txt")
    
    def _raw_reads_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-raw-reads.txt")
    
    def _transformed_meter_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-transformed-meters.txt")
    
    def _transformed_reads_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-transformed-reads.txt")

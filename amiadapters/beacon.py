from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import json
import os
import requests
import time

from amiadapters.base import BaseAMIAdapter, DataclassJSONEncoder
from amiadapters.config import AMIAdapterConfiguration

logger = logging.getLogger(__name__)

METER_COLUMNS = [
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
    """
    account_id: str
    location_id: str
    location_address_line_1: str
    location_address_line_2: str
    location_address_line_3: str
    location_city: str
    location_country: str
    location_state: str
    location_zip: str
    meter_id: str
    meter_install_date: str
    meter_size: str
    flow_time: str
    flow_unit: str
    raw_read: float

# 'Flow',
#     'Flow_Time',
#     'Flow_Unit',
# 'Location_Address_Line1',
#     'Location_Address_Line2',
#     'Location_Address_Line3',
#     'Location_City',
#     'Location_Country',
#     'Location_ID',
#     'Location_State',
#     'Location_Zip',
# 'Meter_ID',
#     'Meter_Install_Date',
#     'Meter_Size',
# 'Raw_Read',
#     'Read',


class Beacon360Adapter(BaseAMIAdapter):

    def __init__(self, config: AMIAdapterConfiguration):
        self.output_folder = config.output_folder
        self.user = config.beacon_360_user
        self.password = config.beacon_360_password
        # Use locally cached report instead of fetching from API
        # Probably don't want to use this in production
        self.use_cache = True
    
    def name(self) -> str:
        return f"beacon-360-api"
    
    def extract(self):
        report = self._fetch_range_report()

        report_csv_rows = report.strip().split("\n")
        meter_with_reads = []
        for row in report_csv_rows:
            # TODO should probably just dump out every column we get, which helps w/ debugging and future data needs
            import pdb; pdb.set_trace()
            meter_with_reads.append(
                Beacon360MeterAndRead(

                )
            )

        with open(self._raw_reads_output_file(), "w") as f:
            content = "\n".join(json.dumps(m, cls=DataclassJSONEncoder) for m in meter_with_reads)
            f.write(content)
    
    def _fetch_range_report(self):
        if self.use_cache:
            if os.path.exists(self._cached_report_file()):
                logger.info("Loading report from cache")
                with open(self._cached_report_file(), "r") as f:
                    return f.read()
        
        auth = requests.auth.HTTPBasicAuth(self.user, self.password)
        
        params = {
            'Start_Date': datetime(2024,8,1),
            'End_Date': datetime(2024,8,2),
            'Resolution': "hourly",
            'Header_Columns': ','.join(METER_COLUMNS),
            'Has_Endpoint': True
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        # Request report generation, get back a link for watching its status
        logger.info(f"Requesting report for meter reads between {params['Start_Date']} and {params['End_Date']} at {params['Resolution']} resolution")
        generate_report_response = requests.post(
            url='https://api.beaconama.net/v2/eds/range',
            headers=headers,
            params=params,
            auth=auth
        )

        if generate_report_response.status_code == 429:
            # Rate limit exceeded
            t = json.loads(generate_report_response.text)
            secs_to_wait = int(t['args'][2])
            time_to_resume = datetime.now() + timedelta(seconds=secs_to_wait)
            logger.warning(f"need to wait {secs_to_wait} seconds until {time_to_resume} ({t})")
            raise Exception("Rate limit exceeded")
        elif generate_report_response.status_code != 202:
            logger.error(f"error when requesting report. status code: {generate_report_response.status_code}")
            raise Exception("Failed request to generate report")
        
        status_url = json.loads(generate_report_response.text)['statusUrl']

        # Poll for report status
        i = 0
        max_attempts = 15  # 15 minutes
        while i < max_attempts:
            i += 1
            logger.info(f"Attempt {i}/{max_attempts} while polling for status on report at {status_url}")
            status_response = requests.get(
                url=f"https://api.beaconama.net{status_url}",
                headers=headers,
                auth=auth)
            
            if status_response.status_code != 200:
                logger.error(f"error when requesting status. status code: {status_response.status_code} message: {status_response.text}")
                raise Exception("Failed request to get report status")

            status_response_data = json.loads(status_response.text)
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
    
    def transform(self):
        return super().transform()
    
    def _cached_report_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-cached-report.txt")
    
    def _raw_reads_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-raw-reads.txt")
    
    def _transformed_meter_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-transformed-meters.txt")
    
    def _transformed_reads_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-transformed-reads.txt")

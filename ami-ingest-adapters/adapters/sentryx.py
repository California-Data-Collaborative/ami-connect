from datetime import datetime, timedelta
import json
import requests
import os

from adapters.base import BaseAMIAdapter
from config import AMIAdapterConfiguration


BASE_URL = "https://api.sentryx.io/v1-wm/sites/crookedriverranchor"


class SentryxAdapter(BaseAMIAdapter):

    def __init__(self, config: AMIAdapterConfiguration):
        self.output_folder = config.output_folder
        self.api_key = config.sentryx_api_key
    
    def name(self) -> str:
        # This may need to be more specific, e.g. per-utility
        return "sentryx-api"

    def extract(self):
        url = f"{BASE_URL}/devices/consumption"
    
        headers = {
            "Authorization": self.api_key,
        }

        end_date = datetime.today()
        start_date = end_date - timedelta(days=2)
        params = {
            "Skip": 0,
            "Take": 25,
            "StartDate": start_date.isoformat(),
            "EndDate": end_date.isoformat(),
        }

        # TODO pagination
        # TODO configurable date range
        response = requests.get(url, params=params, headers=headers)
        
        with open(self._raw_output_file(), "w") as f:
            f.write(response.text)

    def transform(self):
        with open(self._raw_output_file(), "r") as f:
            text = f.read()

        data = json.loads(text)

        raw_meters = data.get("meters", [])
        meters_by_id = {}
        meter_reads_by_meter_id = {}
        for raw_meter in raw_meters:
            device_id = raw_meter.get("deviceId")
            if device_id is None:
                continue
            
            if device_id not in meters_by_id:
                meters_by_id[device_id] = {
                    "meter_id": device_id,
                }
            
            for raw_read in raw_meter.get("data", []):
                # TODO normalize datetimes
                flowtime = raw_read.get("timeStamp")
                value = raw_read.get("reading")
                if flowtime is None or value is None:
                    continue
                read = {
                    "meter_id": device_id,
                    "flowtime": flowtime,
                    "raw_value": value
                }
                if device_id not in meter_reads_by_meter_id:
                    meter_reads_by_meter_id[device_id] = []
                meter_reads_by_meter_id[device_id].append(read)
        
        with open(self._transformed_meter_output_file(), "w") as f:
            f.write(json.dumps(list(meters_by_id.values())))

        with open(self._transformed_reads_output_file(), "w") as f:
            f.write(json.dumps(list(meter_reads_by_meter_id.values())))

    def _raw_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-raw.txt")
    
    def _transformed_meter_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-transformed-meters.txt")
    
    def _transformed_reads_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-transformed-reads.txt")

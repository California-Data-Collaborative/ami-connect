from datetime import datetime, timedelta
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

        response = requests.get(url, params=params, headers=headers)
        
        with open(os.path.join(self.output_folder, f"{self.name()}-raw.txt"), "w") as f:
            f.write(response.text)

    def transform(self):
        pass


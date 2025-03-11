from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import json
import requests
import os
from typing import List

from amiadapters.base import BaseAMIAdapter, DataclassJSONEncoder, \
    GeneralMeter, GeneralMeterRead, GeneralModelJSONEncoder
from amiadapters.config import AMIAdapterConfiguration

logger = logging.getLogger(__name__)


BASE_URL = "https://api.sentryx.io/v1-wm/sites"


@dataclass
class SentryxMeter:
    """
    Representation of a Meter in Sentryx API responses, e.g.

    {
            "dmaObjectId": 6,
            "dmaName": null,
            "deviceId": 603451200,
            "isDisconnectableDevice": false,
            "serviceStatus": "NotRDM",
            "deviceStatus": "OK",
            "street": "5800 SW HADDOCK DR, ",
            "city": "CROOKED RIVER RANCH                               ",
            "state": null,
            "zip": "97760",
            "description": "Mueller Systems SSR Ext-3/4(CF)",
            "manufacturer": null,
            "installNotes": "AMR to AMI Conversion",
            "lastReadingDateTime": "2025-03-10T06:00:00",
            "accountId": "1009",
            "lastBilledDate": null,
            "lastBilledRead": null,
            "lastReading": 311315,
            "units": "Unknown",
            "meterSize": "3/4\"",
            "socketId": "131225B002700",
            "billingCycle": 1,
            "firstName": "BROOKS_ MITCH & MIRIAM",
            "lastName": "",
            "email": null,
            "dials": null,
            "billableDials": null,
            "multiplier": 1,
            "isReclaimed": false,
            "dComId": 6034512,
            "port": 1,
            "installDate": "1972-06-10T17:30:00",
            "unbilledConsumption": null,
            "installerName": "",
            "installerEmail": null,
            "route": "1",
            "lateral": null,
            "hasAlerts": false,
            "alertText": null,
            "activeAlerts": [],
            "productType": "MiNodeM",
            "powerLevel": null,
            "groupNames": null,
            "isInput": false,
            "isOutput": false,
            "taskType": "",
            "extSSR": true,
            "isGeneric": false,
            "muellerSerialNumber": "6034512",
            "registerSerialNumber": "1800132638",
            "bodySerialNumber": "1800132638",
            "batteryPlan": null,
            "edrxStartTime": null,
            "edrxEndTime": null,
            "cellularOnDemandReadScheduled": false
        },
    """
    account_id: str
    device_id: str
    device_status: str
    service_status: str
    street: str
    city: str
    state: str
    zip: str
    description: str
    manufacturer: str
    install_notes: str
    install_date: str
    meter_size: str


@dataclass
class SentryxMeterRead:
    time_stamp: str
    reading: float


@dataclass
class SentryxMeterWithReads:
    """
    "deviceId": 601133200,
    "bodySerialNumber": "1800132722",
    "muellerSerialNumber": "6011332",
    "registerSerialNumber": "1800132722",
    "units": "CF",
    "data": [
        {
            "timeStamp": "2024-07-07T01:00:00",
            "reading": 35828,
            "consumption": 0
        },
        ...
    ]
    """
    device_id: str
    units: str
    data: List[SentryxMeterRead]

    @classmethod
    def from_json(cls, d: str):
        """
        Dataclass doesn't handle nested JSON well, so we roll our own.
        """
        meter = SentryxMeterWithReads(**json.loads(d))
        reads = []
        for read in meter.data:
            reads.append(SentryxMeterRead(**read))
        meter.data = reads
        return meter


class SentryxAdapter(BaseAMIAdapter):

    def __init__(self, config: AMIAdapterConfiguration):
        self.output_folder = config.output_folder
        self.api_key = config.sentryx_api_key
        self.utility = "crookedriverranchor"
    
    def name(self) -> str:
        return f"sentryx-api-{self.utility}"

    def extract(self):
        meters = self._extract_all_meters()
        with open(self._raw_meter_output_file(), "w") as f:
            content = "\n".join(json.dumps(m, cls=DataclassJSONEncoder) for m in meters)
            f.write(content)

        meters_with_reads = self._extract_consumption_for_all_meters()
        with open(self._raw_reads_output_file(), "w") as f:
            content = "\n".join(json.dumps(m, cls=DataclassJSONEncoder) for m in meters_with_reads)
            f.write(content)
    
    def _extract_all_meters(self) -> List[SentryxMeter]:
        url = f"{BASE_URL}/{self.utility}/devices"
        
        headers = {
            "Authorization": self.api_key,
        }
        
        params = {"pager.skip": 0, "pager.take": 25}

        meters = []

        last_page = False
        num_meters = 0
        while last_page is False:
            logger.info(f"Extracting meters for {self.utility}, skip={params["pager.skip"]}")
            response = requests.get(url, headers=headers, params=params)
            if not response.ok:
                logger.warning(f"Non-200 response from devices endpoint: {response.status_code}")
                return []
            data = response.json()
            raw_meters = data.get("meters", [])
            num_meters += len(raw_meters)
            for raw_meter in raw_meters:
                meters.append(
                    SentryxMeter(
                        account_id=raw_meter["accountId"],
                        device_id=raw_meter["deviceId"],
                        device_status=raw_meter["deviceStatus"],
                        service_status=raw_meter["serviceStatus"],
                        street=raw_meter["street"],
                        city=raw_meter["city"],
                        state=raw_meter["state"],
                        zip=raw_meter["zip"],
                        description=raw_meter["description"],
                        manufacturer=raw_meter["manufacturer"],
                        install_notes=raw_meter["installNotes"],
                        install_date=raw_meter["installDate"],
                        meter_size=raw_meter["meterSize"]
                    )    
                )
            params["pager.skip"] = num_meters
            last_page = not raw_meters

        logger.info(f"Extracted {len(meters)} meters for {self.utility}")

        return meters

    def _extract_consumption_for_all_meters(self) -> List[SentryxMeterWithReads]:
        url = f"{BASE_URL}/{self.utility}/devices/consumption"
    
        headers = {
            "Authorization": self.api_key,
        }

        end_date = datetime.today()
        start_date = end_date - timedelta(days=2)
        # TODO configurable date range
        params = {
            "skip": 0,
            "take": 25,
            "StartDate": start_date.isoformat(),
            "EndDate": end_date.isoformat(),
        }
        last_page = False
        num_meters = 0
        meters = []
        while last_page is False:
            logger.info(f"Extracting meter reads for {self.utility}, skip={params["skip"]}")
            response = requests.get(url, headers=headers, params=params)
            if not response.ok:
                logger.warning(f"Non-200 response from device consumption endpoint: {response.status_code}")
                return
            data = response.json()
            raw_meters = data.get("meters", [])
            num_meters += len(raw_meters)
            for raw_meter in raw_meters:
                reads = [SentryxMeterRead(time_stamp=i["timeStamp"], reading=i["reading"]) for i in raw_meter["data"]]
                meters.append(
                    SentryxMeterWithReads(
                        device_id=raw_meter["deviceId"],
                        units=raw_meter["units"],
                        data=reads,
                    )
                )
            params["skip"] = num_meters
            last_page = not raw_meters
            
        return meters

    def transform(self):
        with open(self._raw_meter_output_file(), "r") as f:
            text = f.read()
            raw_meters = [SentryxMeter(**json.loads(d)) for d in text.strip().split("\n")]
            
        
        with open(self._raw_reads_output_file(), "r") as f:
            text = f.read()
            raw_meters_with_reads = [SentryxMeterWithReads.from_json(d) for d in text.strip().split("\n")]

        meters_by_id = {}
        for raw_meter in raw_meters:
            # TODO Location? We have components like street, city, state, but no ID
            meter = GeneralMeter(
                meter_id=raw_meter.device_id,
                account_id=raw_meter.account_id,
                size_inches=raw_meter.meter_size,
                location_id=None,
            )
            meters_by_id[meter.meter_id] = meter

        meter_reads = []
        for raw_meter in raw_meters_with_reads:
            device_id = raw_meter.device_id
            meter_metadata = meters_by_id.get(device_id)
            account_id = meter_metadata.account_id if meter_metadata is not None else None
            for raw_read in raw_meter.data:
                # TODO set the timezone if/when we can confirm what it is.
                flowtime = datetime.fromisoformat(raw_read.time_stamp)
                value = raw_read.reading
                read = GeneralMeterRead(
                    meter_id=device_id,
                    account_id=account_id,
                    location_id=None,
                    flowtime=flowtime,
                    raw_value=value,
                    # TODO should this be normalized?
                    raw_unit=raw_meter.units
                )
                meter_reads.append(read)
        
        with open(self._transformed_meter_output_file(), "w") as f:
            f.write("\n".join(json.dumps(v, cls=GeneralModelJSONEncoder) for v in meters_by_id.values()))

        with open(self._transformed_reads_output_file(), "w") as f:
            f.write("\n".join(json.dumps(m, cls=GeneralModelJSONEncoder) for m in meter_reads))

    def _raw_meter_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-raw-meters.txt")
    
    def _raw_reads_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-raw-reads.txt")
    
    def _transformed_meter_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-transformed-meters.txt")
    
    def _transformed_reads_output_file(self) -> str:
        return os.path.join(self.output_folder, f"{self.name()}-transformed-reads.txt")

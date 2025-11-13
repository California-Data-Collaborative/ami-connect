from dataclasses import dataclass
from datetime import datetime
import logging
import json
from typing import List, Tuple

import requests

from amiadapters.models import (
    DataclassJSONEncoder,
    GeneralMeter,
    GeneralMeterRead,
)
from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

logger = logging.getLogger(__name__)


BASE_URL = "https://api.sentryx.io/v1-wm/sites"


@dataclass
class SentryxMeter:
    """
    Representation of a Meter in Sentryx API responses, e.g.

    {
            "dmaObjectId": 6,
            "dmaName": null,
            "deviceId": 12312414,
            "isDisconnectableDevice": false,
            "serviceStatus": "NotRDM",
            "deviceStatus": "OK",
            "street": "58 MY STREET DR, ",
            "city": "MY TOWN                               ",
            "state": null,
            "zip": "10101",
            "description": "Mueller Systems SSR Ext-3/4(CF)",
            "manufacturer": null,
            "installNotes": "AMR to AMI Conversion",
            "lastReadingDateTime": "2025-03-10T06:00:00",
            "accountId": "12",
            "lastBilledDate": null,
            "lastBilledRead": null,
            "lastReading": 311315,
            "units": "Unknown",
            "meterSize": "3/4\"",
            "socketId": "131225B002700",
            "billingCycle": 1,
            "firstName": "JANE",
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
            "muellerSerialNumber": "123",
            "registerSerialNumber": "12412312",
            "bodySerialNumber": "12412312",
            "batteryPlan": null,
            "edrxStartTime": null,
            "edrxEndTime": null,
            "cellularOnDemandReadScheduled": false
        },
    """

    account_id: str
    device_id: int
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
    "deviceId": 123,
    "bodySerialNumber": "2132132",
    "muellerSerialNumber": "124132314",
    "registerSerialNumber": "2132132",
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

    device_id: int
    units: str
    data: List[SentryxMeterRead]

    @classmethod
    def from_json_file(cls, extract_output: ExtractOutput, filename: str):
        """
        Parses SentryxMeterWithReads instances from JSON file, including nested reads.
        """
        raw_meters_with_reads = extract_output.load_from_file(
            filename, SentryxMeterWithReads
        )
        for raw_meter in raw_meters_with_reads:
            reads = []
            for read in raw_meter.data:
                reads.append(SentryxMeterRead(**read))
            raw_meter.data = reads
        return raw_meters_with_reads


class SentryxAdapter(BaseAMIAdapter):
    """
    AMI Adapter for Sentryx API.
    """

    def __init__(
        self,
        api_key: str,
        org_id: str,
        org_timezone: str,
        pipeline_configuration,
        configured_task_output_controller,
        configured_sinks,
        utility_name: str = None,
    ):
        self.api_key = api_key
        self.org_id = org_id
        self.org_timezone = org_timezone
        # This is used to create URLs for the Sentryx API. It must match the name used to generate API credentials.
        # It defaults to the org_id.
        self.utility_name = utility_name if utility_name is not None else org_id
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_sinks,
            RawSnowflakeLoader.with_table_loaders(
                [
                    SentryxRawMetersLoader(),
                    SentryxRawReadsLoader(),
                ]
            ),
        )

    def name(self) -> str:
        return f"sentryx-api-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        logger.info(
            f"Extracting {self.org_id} data from {extract_range_start} to {extract_range_end}"
        )
        meters = self._extract_all_meters()
        meters_with_reads = self._extract_consumption_for_all_meters(
            extract_range_start, extract_range_end
        )
        return ExtractOutput(
            {
                "meters.json": "\n".join(
                    json.dumps(m, cls=DataclassJSONEncoder) for m in meters
                ),
                "reads.json": "\n".join(
                    json.dumps(m, cls=DataclassJSONEncoder) for m in meters_with_reads
                ),
            }
        )

    def _extract_all_meters(self) -> List[SentryxMeter]:
        url = f"{BASE_URL}/{self.utility_name}/devices"

        headers = {
            "Authorization": self.api_key,
        }

        meters = []

        last_page = False
        num_meters = 0
        while last_page is False:
            params = {"pager.skip": num_meters, "pager.take": 25}
            logger.info(
                f"Extracting meters for {self.org_id}, skip={params["pager.skip"]}"
            )
            response = requests.get(url, headers=headers, params=params)
            if not response.status_code == 200:
                raise Exception(
                    f"Non-200 response from devices endpoint: {response.status_code} {response.text}"
                )
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
                        meter_size=raw_meter["meterSize"],
                    )
                )

            last_page = not raw_meters

        logger.info(f"Extracted {len(meters)} meters for {self.org_id}")

        return meters

    def _extract_consumption_for_all_meters(
        self, extract_range_start: datetime, extract_range_end: datetime
    ) -> List[SentryxMeterWithReads]:
        url = f"{BASE_URL}/{self.utility_name}/devices/consumption"

        headers = {
            "Authorization": self.api_key,
        }

        last_page = False
        num_meters = 0
        meters = []
        while last_page is False:
            params = {
                "skip": num_meters,
                "take": 25,
                "StartDate": extract_range_start.isoformat(),
                "EndDate": extract_range_end.isoformat(),
            }
            logger.info(
                f"Extracting meter reads for {self.org_id}, skip={params["skip"]}"
            )
            response = requests.get(url, headers=headers, params=params)
            if not response.status_code == 200:
                raise Exception(
                    f"Non-200 response from device consumption endpoint: {response.status_code} {response.text}"
                )
            data = response.json()
            raw_meters = data.get("meters", [])
            num_meters += len(raw_meters)
            for raw_meter in raw_meters:
                reads = [
                    SentryxMeterRead(time_stamp=i["timeStamp"], reading=i["reading"])
                    for i in raw_meter["data"]
                ]
                meters.append(
                    SentryxMeterWithReads(
                        device_id=raw_meter["deviceId"],
                        units=raw_meter["units"],
                        data=reads,
                    )
                )
            last_page = not raw_meters

        return meters

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        raw_meters = extract_outputs.load_from_file("meters.json", SentryxMeter)
        raw_meters_with_reads = SentryxMeterWithReads.from_json_file(
            extract_outputs, "reads.json"
        )
        return self._transform_meters_and_reads(raw_meters, raw_meters_with_reads)

    def _transform_meters_and_reads(
        self,
        raw_meters: List[SentryxMeter],
        raw_meters_with_reads: List[SentryxMeterWithReads],
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        meters_by_id = {}
        for raw_meter in raw_meters:
            device_id = str(raw_meter.device_id)
            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=raw_meter.account_id,
                location_id=None,
                meter_id=device_id,
                endpoint_id=None,
                meter_install_date=self.datetime_from_iso_str(
                    raw_meter.install_date, self.org_timezone
                ),
                meter_size=self.map_meter_size(raw_meter.meter_size),
                meter_manufacturer=raw_meter.manufacturer,
                multiplier=None,
                location_address=raw_meter.street,
                location_city=raw_meter.city,
                location_state=raw_meter.state,
                location_zip=raw_meter.zip,
            )
            meters_by_id[meter.meter_id] = meter

        meter_reads = []
        for raw_meter in raw_meters_with_reads:
            device_id = str(raw_meter.device_id)
            meter_metadata = meters_by_id.get(device_id)
            account_id = (
                meter_metadata.account_id if meter_metadata is not None else None
            )
            for raw_read in raw_meter.data:
                register_value, register_unit = self.map_reading(
                    raw_read.reading,
                    raw_meter.units,  # Expected to be CF
                )
                read = GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=device_id,
                    account_id=account_id,
                    location_id=None,
                    flowtime=self.datetime_from_iso_str(
                        raw_read.time_stamp, self.org_timezone
                    ),
                    register_value=register_value,
                    register_unit=register_unit,
                    interval_value=None,
                    interval_unit=None,
                    battery=None,
                    install_date=None,
                    connection=None,
                    estimated=None,
                )
                meter_reads.append(read)

        return list(meters_by_id.values()), meter_reads


class SentryxRawMetersLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "sentryx_meter_base"

    def columns(self) -> List[str]:
        return list(SentryxMeter.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["device_id"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("meters.json", SentryxMeter)
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class SentryxRawReadsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "sentryx_read_base"

    def columns(self) -> List[str]:
        return [
            "device_id",
            "TIME_STAMP",
            "READING",
            "UNITS",
        ]

    def unique_by(self) -> List[str]:
        return ["device_id", "TIME_STAMP"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = SentryxMeterWithReads.from_json_file(extract_outputs, "reads.json")
        return [
            tuple(
                [meter_with_reads.device_id]
                + [reading.__getattribute__(name) for name in self.columns()]
            )
            for meter_with_reads in raw_data
            for reading in meter_with_reads
        ]

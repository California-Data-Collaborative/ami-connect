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
from amiadapters.base import BaseAMIAdapter
from amiadapters.config import ConfiguredStorageSink, ConfiguredStorageSinkType
from amiadapters.outputs.base import BaseTaskOutputController, ExtractOutput
from amiadapters.storage.snowflake import SnowflakeStorageSink

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
    def from_json(cls, d: str):
        """
        Parses SentryxMeterWithReads from JSON, including nested reads.
        Dataclass doesn't handle nested JSON well, so we roll our own.
        """
        meter = SentryxMeterWithReads(**json.loads(d))
        reads = []
        for read in meter.data:
            reads.append(SentryxMeterRead(**read))
        meter.data = reads
        return meter


class SentryxAdapter(BaseAMIAdapter):
    """
    AMI Adapter for Sentryx API.
    """

    def __init__(
        self,
        api_key: str,
        org_id: str,
        org_timezone: str,
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

        task_output_controller = self.create_task_output_controller(
            configured_task_output_controller, org_id
        )
        # Must create storage sinks here because it's Sentryx-specific
        storage_sinks = []
        for sink in configured_sinks:
            if sink.type == ConfiguredStorageSinkType.SNOWFLAKE:
                storage_sinks.append(
                    SentryxSnowflakeStorageSink(
                        task_output_controller,
                        org_id,
                        org_timezone,
                        sink,
                    )
                )
        super().__init__(org_id, org_timezone, task_output_controller, storage_sinks)

    def name(self) -> str:
        return f"sentryx-api-{self.org_id}"

    def extract(
        self, run_id: str, extract_range_start: datetime, extract_range_end: datetime
    ):
        logger.info(
            f"Extracting {self.org_id} data from {extract_range_start} to {extract_range_end}"
        )
        meters = self._extract_all_meters()
        meters_with_reads = self._extract_consumption_for_all_meters(
            extract_range_start, extract_range_end
        )
        self.output_controller.write_extract_outputs(
            run_id,
            ExtractOutput(
                {
                    "meters.json": "\n".join(
                        json.dumps(m, cls=DataclassJSONEncoder) for m in meters
                    ),
                    "reads.json": "\n".join(
                        json.dumps(m, cls=DataclassJSONEncoder)
                        for m in meters_with_reads
                    ),
                }
            ),
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
                logger.warning(
                    f"Non-200 response from devices endpoint: {response.status_code}"
                )
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
                logger.warning(
                    f"Non-200 response from device consumption endpoint: {response.status_code}"
                )
                return []
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

    def transform(self, run_id: str):
        extract_outputs = self.output_controller.read_extract_outputs(run_id)

        raw_meter_text = extract_outputs.from_file("meters.json")
        raw_meters = [
            SentryxMeter(**json.loads(d)) for d in raw_meter_text.strip().split("\n")
        ]

        raw_meters_with_reads_text = extract_outputs.from_file("reads.json")
        raw_meters_with_reads = [
            SentryxMeterWithReads.from_json(d)
            for d in raw_meters_with_reads_text.strip().split("\n")
        ]

        transformed_meters, transformed_reads = self._transform_meters_and_reads(
            raw_meters, raw_meters_with_reads
        )

        self.output_controller.write_transformed_meters(run_id, transformed_meters)
        self.output_controller.write_transformed_meter_reads(run_id, transformed_reads)

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
                value = raw_read.reading
                read = GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=device_id,
                    account_id=account_id,
                    location_id=None,
                    flowtime=self.datetime_from_iso_str(
                        raw_read.time_stamp, self.org_timezone
                    ),
                    register_value=value,
                    register_unit=self.map_unit_of_measure(raw_meter.units),
                    interval_value=None,
                    interval_unit=None,
                )
                meter_reads.append(read)

        return list(meters_by_id.values()), meter_reads


class SentryxSnowflakeStorageSink(SnowflakeStorageSink):
    """
    Sentryx implementation of Snowflake AMI Storage Sink. In addition to parent class's storage of generalized
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
        # extract_outputs = self.output_controller.read_extract_outputs(run_id)
        # text = extract_outputs.from_file("meters_and_reads.json")
        # raw_meters_with_reads = [
        #     Beacon360MeterAndRead(**json.loads(d)) for d in text.strip().split("\n")
        # ]

        # conn = self.sink_config.connection()

        # create_temp_table_sql = "CREATE OR REPLACE TEMPORARY TABLE temp_beacon_360_base LIKE beacon_360_base;"
        # conn.cursor().execute(create_temp_table_sql)

        # columns = ", ".join(REQUESTED_COLUMNS)
        # qmarks = "?, " * (len(REQUESTED_COLUMNS) - 1) + "?"
        # insert_temp_data_sql = f"""
        #     INSERT INTO temp_beacon_360_base (org_id, device_id, created_time, {columns})
        #         VALUES (?, ?, ?, {qmarks})
        # """
        # created_time = datetime.now(tz=self.org_timezone)
        # rows = [
        #     tuple(
        #         [self.org_id, i.Meter_ID, created_time]
        #         + [i.__getattribute__(name) for name in REQUESTED_COLUMNS]
        #     )
        #     for i in raw_meters_with_reads
        # ]
        # conn.cursor().executemany(insert_temp_data_sql, rows)

        # merge_sql = f"""
        #     MERGE INTO beacon_360_base AS target
        #     USING (
        #         -- Use GROUP BY to ensure there are no duplicate rows before merge
        #         SELECT
        #             org_id,
        #             device_id,
        #             Read_Time,
        #             {", ".join([f"max({name}) as {name}" for name in REQUESTED_COLUMNS if name not in {"Read_Time",}])},
        #             max(created_time) as created_time
        #         FROM temp_beacon_360_base
        #         GROUP BY org_id, device_id, Read_Time
        #     ) AS source
        #     ON source.org_id = target.org_id
        #         AND source.device_id = target.device_id
        #         AND source.Read_Time = target.Read_Time
        #     WHEN MATCHED THEN
        #         UPDATE SET
        #             target.created_time = source.created_time,
        #             {",".join([f"target.{name} = source.{name}" for name in REQUESTED_COLUMNS])}
        #     WHEN NOT MATCHED THEN
        #         INSERT (org_id, device_id, {", ".join(name for name in REQUESTED_COLUMNS)}, created_time)
        #                 VALUES (source.org_id, source.device_id, {", ".join(f"source.{name}" for name in REQUESTED_COLUMNS)}, source.created_time)
        # """
        # conn.cursor().execute(merge_sql)
        pass

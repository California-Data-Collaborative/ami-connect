from dataclasses import dataclass, replace
from datetime import datetime
import logging
import json
from typing import Dict, Generator, List, Set, Tuple

import requests

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

logger = logging.getLogger(__name__)


@dataclass
class SubecaReading:
    """
    Represents both an interval read (usage) and register read (latest reading) for a device.
    """

    deviceId: str
    usageTime: str
    unit: str
    value: str


@dataclass
class SubecaAccount:
    """
    We combine account and device metadata with the account's latest reading here. It's
    all info we get from the /accounts/{accountId} endpoint.
    """

    accountId: str
    accountStatus: str
    meterSerial: str
    billingRoute: str
    registerSerial: str
    meterSize: str
    createdAt: str
    deviceId: str
    activeProtocol: str
    installationDate: str
    latestCommunicationDate: str
    latestReading: SubecaReading

    @classmethod
    def from_json(cls, d: str):
        """
        Parses SubecaAccount from JSON, including nested reading.
        Dataclass doesn't handle nested JSON well, so we roll our own.
        """
        account = SubecaAccount(**json.loads(d))
        account.latestReading = SubecaReading(**account.latestReading)
        return account


class SubecaAdapter(BaseAMIAdapter):
    """
    AMI Adapter that uses API to retrieve Subeca data.
    """

    READ_UNIT = "cf"

    def __init__(
        self,
        org_id: str,
        org_timezone: str,
        pipeline_configuration,
        api_url: str,
        api_key: str,
        configured_task_output_controller,
        configured_sinks,
    ):
        self.api_url = api_url
        self.api_key = api_key
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_sinks,
            RawSnowflakeLoader.with_table_loaders(
                [
                    RawAccountsLoader(),
                    RawLatestReadingLoader(),
                    RawUsageLoader(),
                ]
            ),
        )

    def name(self) -> str:
        return f"subeca-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:
        logger.info(
            f"Retrieving Subeca data between {extract_range_start} and {extract_range_end}"
        )
        account_ids = self._extract_all_account_ids()

        accounts = []
        usages = []
        for i, account_id in enumerate(account_ids):
            logger.info(
                f"Requesting usage for account {account_id} ({i+1} / {len(account_ids)})"
            )
            usages += self._extract_usages_for_account(
                account_id, extract_range_start, extract_range_end
            )

            logger.info(
                f"Requesting account and device metadata for account {account_id} ({i+1} / {len(account_ids)})"
            )
            accounts.append(self._extract_metadata_for_account(account_id))

        logger.info(f"Extracted {len(accounts)} accounts")
        logger.info(f"Extracted {len(usages)} usage records across all accounts")

        return ExtractOutput(
            {
                "accounts.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in accounts
                ),
                "usages.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in usages
                ),
            }
        )

    def _extract_all_account_ids(self) -> Set[str]:
        """
        Use the /v1/accounts endpoint to get the set of all account IDs.
        """
        headers = {
            "accept": "application/json",
            "x-subeca-api-key": self.api_key,
        }
        params = {
            "pageSize": 100,
        }
        finished = False
        next_token = None
        account_ids = set()
        num_requests = 1
        while not finished and num_requests < 10_000:
            if next_token is not None:
                params["nextToken"] = next_token

            logger.info(f"Requesting Subeca accounts. Request {num_requests}")

            result = requests.get(
                f"{self.api_url}/v1/accounts", params=params, headers=headers
            )
            if not result.ok:
                raise ValueError(
                    f"Invalid response from accounts request: {result} {result.text}"
                )

            response_json = result.json()
            data = response_json["data"]

            for d in data:
                account_ids.add(d["accountId"])

            num_requests += 1

            next_token = response_json.get("nextToken")
            if next_token is None:
                finished = True

        logger.info(f"Retrieved {len(account_ids)} account IDs")
        return account_ids

    def _extract_usages_for_account(
        self,
        account_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> List[SubecaReading]:
        """
        Use the /v1/accounts/{accountId}/usages endpoint to get the usage for this account
        """
        usages = []
        body = {
            "readUnit": self.READ_UNIT,
            "granularity": "hourly",
            "referencePeriod": {
                "start": extract_range_start.strftime("%Y-%m-%d"),
                "end": extract_range_end.strftime("%Y-%m-%d"),
                "utcOffset": "+00:00",
            },
        }
        result = requests.post(
            f"{self.api_url}/v1/accounts/{account_id}/usages",
            json=body,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "x-subeca-api-key": self.api_key,
            },
        )

        if not result.ok:
            raise ValueError(
                f"Invalid response from usages endpoint for account {account_id}: {result.status_code} {result.text}"
            )

        for usage_time, usage in (
            result.json().get("data", {}).get("hourly", {}).items()
        ):
            # Ignore usage with deviceId == ""
            if device_id := usage.get("deviceId"):
                usages.append(
                    SubecaReading(
                        deviceId=device_id,
                        usageTime=usage_time,
                        unit=usage.get("unit"),
                        value=usage.get("value"),
                    )
                )

        return usages

    def _extract_metadata_for_account(self, account_id: str) -> SubecaAccount:
        """
        Use /v1/accounts/{accountId} endpoint to get metadata for this account.

        Example response:
            {
                "accountId": "3.29.4.W",
                "accountStatus": "active",
                "meterSerial": "",
                "billingRoute": "",
                "registerSerial": "5C2D085100009237",
                "meterInfo": {
                    "meterSize": "5/8"
                },
                "device": {
                    "activeProtocol": "LoRaWAN",
                    "installationDate": "2025-06-05T19:33:54+00:00",
                    "latestCommunicationDate": "2025-08-05T20:19:46+00:00",
                    "latestReading": {
                        "value": "16685.9",
                        "unit": "gal",
                        "date": "2025-08-05T20:19:46+00:00"
                        },
                    "deviceId": "5C2D085100009237"
                },
                "createdAt": "2025-05-30T02:37:52+00:00"
            }
        """
        result = requests.get(
            f"{self.api_url}/v1/accounts/{account_id}",
            params={
                "readUnit": self.READ_UNIT,
            },
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "x-subeca-api-key": self.api_key,
            },
        )

        if not result.ok:
            raise ValueError(
                f"Invalid response from account metadata endpoint for account {account_id}: {result.status_code} {result.text}"
            )

        raw_account = result.json()
        raw_device = raw_account.get("device") or {}
        latest_reading = SubecaReading(
            deviceId=raw_device.get("deviceId"),
            usageTime=raw_device.get("latestReading", {}).get("date"),
            value=raw_device.get("latestReading", {}).get("value"),
            unit=raw_device.get("latestReading", {}).get("unit"),
        )
        return SubecaAccount(
            accountId=raw_account["accountId"],
            accountStatus=raw_account.get("accountStatus"),
            meterSerial=raw_account.get("meterSerial"),
            billingRoute=raw_account.get("billingRoute"),
            registerSerial=raw_account.get("registerSerial"),
            meterSize=raw_account.get("meterInfo", {}).get("meterSize"),
            createdAt=raw_account.get("createdAt"),
            deviceId=raw_device.get("deviceId"),
            activeProtocol=raw_device.get("activeProtocol"),
            installationDate=raw_device.get("installationDate"),
            latestCommunicationDate=raw_device.get("latestCommunicationDate"),
            latestReading=latest_reading,
        )

    def _transform(
        self, run_id: str, extract_outputs: ExtractOutput
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        raw_accounts = _read_accounts_file(extract_outputs)
        raw_usages_by_device_id = self._usages_by_device_id(extract_outputs)

        meters_by_id = {}
        reads_by_device_and_time = {}

        for account in raw_accounts:
            device_id = account.deviceId
            if not device_id:
                logger.warning(f"Skipping account {account} with null device ID")
                continue
            account_id = account.accountId
            location_id = account.accountId
            meter_id = account.meterSerial
            endpoint_id = account.registerSerial

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                meter_id=meter_id,
                endpoint_id=endpoint_id,
                meter_install_date=(
                    datetime.fromisoformat(account.installationDate)
                    if account.installationDate
                    else None
                ),
                meter_size=self.map_meter_size(account.meterSize),
                meter_manufacturer=None,
                multiplier=None,
                location_address=None,
                location_city=None,
                location_state=None,
                location_zip=None,
            )
            meters_by_id[device_id] = meter

            # Interval reads
            usages = raw_usages_by_device_id.get(account.deviceId, [])
            for usage in usages:
                if usage.value:
                    flowtime = datetime.fromisoformat(usage.usageTime)
                    interval_value, interval_unit = self.map_reading(
                        float(usage.value),
                        usage.unit,
                    )
                    read = GeneralMeterRead(
                        org_id=self.org_id,
                        device_id=device_id,
                        account_id=account_id,
                        location_id=location_id,
                        flowtime=flowtime,
                        register_value=None,
                        register_unit=None,
                        interval_value=interval_value,
                        interval_unit=interval_unit,
                        battery=None,
                        install_date=None,
                        connection=None,
                        estimated=None,
                    )
                    reads_by_device_and_time[(device_id, flowtime)] = read

            # Tack on register read from latest reading
            if account.latestReading and account.latestReading.value:
                register_value, register_unit = self.map_reading(
                    float(account.latestReading.value),
                    account.latestReading.unit,
                )
                register_read_flowtime = datetime.fromisoformat(
                    account.latestReading.usageTime
                )
                if (device_id, register_read_flowtime) in reads_by_device_and_time:
                    # Join register read onto the interval read object
                    existing_read = reads_by_device_and_time[
                        (device_id, register_read_flowtime)
                    ]
                    read = replace(
                        existing_read,
                        register_value=register_value,
                        register_unit=register_unit,
                    )
                else:
                    # Create a new one
                    read = GeneralMeterRead(
                        org_id=self.org_id,
                        device_id=device_id,
                        account_id=account_id,
                        location_id=location_id,
                        flowtime=register_read_flowtime,
                        register_value=register_value,
                        register_unit=register_unit,
                        interval_value=None,
                        interval_unit=None,
                        battery=None,
                        install_date=None,
                        connection=None,
                        estimated=None,
                    )
                reads_by_device_and_time[(device_id, register_read_flowtime)] = read

        return list(meters_by_id.values()), list(reads_by_device_and_time.values())

    def _usages_by_device_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[SubecaReading]]:
        result = {}
        raw_usages = self._read_file(extract_outputs, "usages.json", SubecaReading)
        for usage in raw_usages:
            if usage.deviceId not in result:
                result[usage.deviceId] = []
            result[usage.deviceId].append(usage)
        return result

    def _read_file(
        self, extract_outputs: ExtractOutput, file: str, raw_dataclass
    ) -> Generator:
        """
        Read a file's contents from extract stage output, create generator
        for each line of text
        """
        lines = extract_outputs.load_from_file(file, raw_dataclass, allow_empty=True)
        yield from lines


class RawAccountsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "SUBECA_ACCOUNT_BASE"

    def columns(self) -> List[str]:
        cols = list(SubecaAccount.__dataclass_fields__.keys())
        cols.remove("latestReading")
        return cols

    def unique_by(self) -> List[str]:
        return ["deviceId"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("accounts.json", SubecaAccount)
        return [
            tuple(i.__getattribute__(col) for col in self.columns()) for i in raw_data
        ]


class RawLatestReadingLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "SUBECA_DEVICE_LATEST_READ_BASE"

    def columns(self) -> List[str]:
        return list(SubecaReading.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["deviceId", "usageTime"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = [i.latestReading for i in _read_accounts_file(extract_outputs)]
        return [
            tuple(i.__getattribute__(col) for col in self.columns()) for i in raw_data
        ]


class RawUsageLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "SUBECA_USAGE_BASE"

    def columns(self) -> List[str]:
        return list(SubecaReading.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["deviceId", "usageTime"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("usages.json", SubecaReading)
        return [
            tuple(i.__getattribute__(col) for col in self.columns()) for i in raw_data
        ]


def _read_accounts_file(extract_outputs: ExtractOutput) -> List[SubecaAccount]:
    """
    Read accounts file from extract stage output, return list of SubecaAccount
    """
    accounts = extract_outputs.load_from_file(
        "accounts.json", SubecaAccount, allow_empty=True
    )
    for account in accounts:
        account.latestReading = SubecaReading(**account.latestReading)
    return accounts

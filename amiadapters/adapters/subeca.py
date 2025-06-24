from dataclasses import dataclass
from datetime import datetime
import logging

import requests

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput

logger = logging.getLogger(__name__)


@dataclass
class SubecaAccount:

    accountId: str
    accountStatus: str
    meterSerial: str
    billingRoute: str
    registerSerial: str
    meterSize: str
    createdAt: str


class SubecaAdapter(BaseAMIAdapter):
    """
    AMI Adapter that uses API to retrieve Subeca data.
    """

    def __init__(
        self,
        org_id: str,
        org_timezone: str,
        api_key: str,
        configured_task_output_controller,
        configured_sinks,
    ):
        self.api_url = "https://sierra-cc.api.subeca.online"
        self.api_key = api_key
        super().__init__(
            org_id,
            org_timezone,
            configured_task_output_controller,
            configured_sinks,
        )

    def name(self) -> str:
        return f"subeca-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        logging.info(
            f"Retrieving Subeca data between {extract_range_start} and {extract_range_end}"
        )
        headers = {
            "accept": "application/json",
            "x-subeca-api-key": self.api_key,
        }

        # Retrieve accounts
        params = {
            "pageSize": 100,
        }
        finished = False
        next_token = None
        accounts = []
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
            next_token = response_json.get("nextToken")
            for d in data:
                accounts.append(
                    SubecaAccount(
                        d["accountId"],
                        d["accountStatus"],
                        d["meterSerial"],
                        d["billingRoute"],
                        d["registerSerial"],
                        d.get("meterInfo", {}).get("meterSize"),
                        d["createdAt"],
                    )
                )
            num_requests += 1
            if next_token is None:
                finished = True
        logger.info(f"Retrieved {len(accounts)} accounts")

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "x-subeca-api-key": self.api_key,
        }
        for account in accounts:
            logger.info(f"Requesting Subeca readings for account {account.accountId}")
            body = {
                "pageSize": 50,
                "readUnit": "ccf",
                "granularity": "hourly",
                # must not be longer than one week
                "referencePeriod": {
                    "start": extract_range_start.strftime("%Y-%m-%d"),
                    "end": extract_range_end.strftime("%Y-%m-%d"),
                    "timezone": self.org_timezone,
                },
            }
            result = requests.post(
                f"{self.api_url}/v1/accounts/{account.accountId}/usages",
                data=body,
                headers=headers,
            )

            if not result.ok:
                raise ValueError(
                    f"Invalid response from usages request for account {account.accountId}: {result} {result.text}"
                )

        raise Exception("hi")
        return ExtractOutput({"meters_and_reads.json": output})

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        text = extract_outputs.from_file("meters_and_reads.json")
        return [], []

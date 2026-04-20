import datetime
import json
import pytz
from pytz import timezone
from unittest.mock import MagicMock, patch

from amiadapters.outputs.base import ExtractOutput
from amiadapters.adapters.subeca import (
    SubecaAccount,
    SubecaAdapter,
    SubecaAlarm,
    SubecaReading,
    SUBECA_RAW_SNOWFLAKE_LOADER,
)
from test.base_test_case import BaseTestCase, MockResponse


def create_mock_account_metadata_response() -> MagicMock:
    mock_account_metadata_response = MagicMock()
    mock_account_metadata_response.ok = True
    mock_account_metadata_response.json.return_value = {
        "accountId": "acct1",
        "accountStatus": "active",
        "meterSerial": "M123",
        "billingRoute": "BR1",
        "registerSerial": "RS123",
        "meterInfo": {"meterSize": "5/8"},
        "createdAt": "2025-08-01T00:00:00+00:00",
        "device": {
            "deviceId": "device1",
            "activeProtocol": "LoRaWAN",
            "installationDate": "2025-06-05T19:33:54+00:00",
            "latestCommunicationDate": "2025-08-05T20:19:46+00:00",
            "latestReading": {
                "value": "16685.9",
                "unit": "gal",
                "date": "2025-08-05T20:19:46+00:00",
            },
        },
    }
    return mock_account_metadata_response


def create_mock_usages_response() -> MagicMock:
    mock_usages_response = MagicMock()
    mock_usages_response.ok = True
    mock_usages_response.json.return_value = {
        "data": {
            "hourly": {
                "2025-08-01T10:00:00+00:00": {
                    "deviceId": "device1",
                    "unit": "cf",
                    "value": 123.4,
                }
            }
        }
    }
    return mock_usages_response


def create_mock_accounts_response() -> MagicMock:
    mock_accounts_response = MagicMock()
    mock_accounts_response.ok = True
    mock_accounts_response.json.return_value = {
        "data": [{"accountId": "acct1"}],
        "nextToken": None,
    }
    return mock_accounts_response


def create_mock_alarms_response() -> MagicMock:
    mock_alarms_response = MagicMock()
    mock_alarms_response.ok = True
    mock_alarms_response.json.return_value = {"data": [], "nextToken": None}
    return mock_alarms_response


ALARM_RESPONSE_ITEM = {
    "accountId": "acc1",
    "accountStatus": "active",
    "meterSerial": "",
    "billingRoute": "",
    "registerSerial": "",
    "alarm": {
        "name": "Radio Low Battery",
        "startAt": "2026-01-02T00:00:00+00:00",
        "endAt": "2026-01-02T01:00:00+00:00",
        "deviceId": "device-001",
    },
}

ALARM_RANGE_START = datetime.datetime(2026, 1, 1)
ALARM_RANGE_END = datetime.datetime(2026, 1, 3)


class TestSubecaAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = SubecaAdapter(
            org_id="test-org",
            org_timezone=pytz.timezone("Africa/Algiers"),
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            api_url="http://localhost/my-url",
            api_key="test-key",
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_meter_alerts=self.TEST_METER_ALERT_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
        )
        self.start_date = datetime.datetime(2024, 1, 2, 0, 0)
        self.end_date = datetime.datetime(2024, 1, 3, 0, 0)

    def test_init(self):
        self.assertEqual("http://localhost/my-url", self.adapter.api_url)
        self.assertEqual("test-key", self.adapter.api_key)
        self.assertEqual("test-org", self.adapter.org_id)
        self.assertEqual(pytz.timezone("Africa/Algiers"), self.adapter.org_timezone)
        self.assertEqual("subeca-test-org", self.adapter.name())

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_success(self, mock_request):
        # --- Mock GET /accounts response ---
        mock_accounts_response = create_mock_accounts_response()
        # --- Mock POST /usages response ---
        mock_usages_response = create_mock_usages_response()
        # --- Mock GET /accounts/{id} metadata response ---
        mock_account_metadata_response = create_mock_account_metadata_response()
        # --- Mock POST /alarms alarms response ---
        mock_alarms_response = create_mock_alarms_response()

        mock_request.side_effect = [
            mock_accounts_response,
            mock_usages_response,
            mock_account_metadata_response,
            mock_alarms_response,
        ]

        result = self.adapter._extract("run1", self.start_date, self.end_date)

        # Verify API calls
        mock_request.assert_any_call(
            "get",
            f"{self.adapter.api_url}/v1/accounts",
            params={"pageSize": 100},
            headers={"accept": "application/json", "x-subeca-api-key": "test-key"},
        )
        self.assertEqual(4, mock_request.call_count)

        # Validate returned ExtractOutput
        accounts = result.load_from_file("accounts.json", SubecaAccount)
        usages = result.load_from_file("usages.json", SubecaReading)

        self.assertEqual(len(accounts), 1)
        self.assertEqual(len(usages), 1)

        account = accounts[0]
        account.latestReading = SubecaReading(**account.latestReading)
        usage = usages[0]

        self.assertEqual(account.accountId, "acct1")
        self.assertEqual(account.latestReading.value, "16685.9")
        self.assertEqual(usage.deviceId, "device1")

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_ignores_usage_with_no_device_id(self, mock_request):
        # --- Mock GET /accounts response ---
        mock_accounts_response = create_mock_accounts_response()

        # --- Mock POST /usages response ---
        mock_usages_response = create_mock_usages_response()
        mock_usages_response.json.return_value["data"]["hourly"][
            "2025-08-01T10:00:00+00:00"
        ] = {
            "deviceId": "",
            "unit": "",
            "value": "",
        }

        # --- Mock GET /accounts/{id} metadata response ---
        mock_account_metadata_response = create_mock_account_metadata_response()

        # --- Mock POST /alarms alarms response ---
        mock_alarms_response = create_mock_alarms_response()

        mock_request.side_effect = [
            mock_accounts_response,
            mock_usages_response,
            mock_account_metadata_response,
            mock_alarms_response,
        ]

        result = self.adapter._extract("run1", self.start_date, self.end_date)

        # Validate returned ExtractOutput
        accounts = result.load_from_file(
            "accounts.json", SubecaAccount, allow_empty=True
        )
        usages = result.load_from_file("usages.json", SubecaReading, allow_empty=True)

        self.assertEqual(len(accounts), 1)
        self.assertEqual(len(usages), 0)

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_can_paginate(self, mock_request):
        # Mock accounts
        mock_accounts_response_1 = create_mock_accounts_response()
        mock_accounts_response_2 = create_mock_accounts_response()
        # Set first response up to continue pagination
        mock_accounts_response_1.json.return_value["nextToken"] = "next"

        # --- Mock POST /usages response ---
        mock_usages_response = create_mock_usages_response()

        # Mock GET /accounts/{accountId}
        mock_account_metadata_response = create_mock_account_metadata_response()

        # --- Mock POST /alarms alarms response ---
        mock_alarms_response = create_mock_alarms_response()

        # The second GET call should return metadata response
        mock_request.side_effect = [
            mock_accounts_response_1,
            mock_accounts_response_2,
            mock_usages_response,
            mock_account_metadata_response,
            mock_alarms_response,
        ]

        result = self.adapter._extract("run1", self.start_date, self.end_date)

        self.assertEqual(5, mock_request.call_count)

        # Validate returned ExtractOutput
        accounts = result.load_from_file(
            "accounts.json", SubecaAccount, allow_empty=True
        )
        usages = result.load_from_file("usages.json", SubecaReading, allow_empty=True)

        self.assertEqual(len(accounts), 1)
        self.assertEqual(len(usages), 1)

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_accounts_api_retries(self, mock_request):
        # Mock accounts, first request fails, second succeeds
        mock_accounts_response_1 = create_mock_accounts_response()
        mock_accounts_response_1 = MagicMock()
        mock_accounts_response_1.ok = False
        mock_accounts_response_1.status_code = 500
        mock_accounts_response_1.text = "Server Error"

        mock_accounts_response_2 = create_mock_accounts_response()

        # --- Mock POST /usages response ---
        mock_usages_response = create_mock_usages_response()

        # Mock GET /accounts/{accountId}
        mock_account_metadata_response = create_mock_account_metadata_response()

        # --- Mock POST /alarms alarms response ---
        mock_alarms_response = create_mock_alarms_response()

        # The second GET call should return metadata response
        mock_request.side_effect = [
            mock_accounts_response_1,
            mock_accounts_response_2,
            mock_usages_response,
            mock_account_metadata_response,
            mock_alarms_response,
        ]

        self.adapter._extract("run1", self.start_date, self.end_date)

        self.assertEqual(5, mock_request.call_count)

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_accounts_api_non_retriable_failure(self, mock_request):
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_request.return_value = mock_response

        with self.assertRaises(ValueError) as ctx:
            self.adapter._extract("run1", self.start_date, self.end_date)

        self.assertIn(
            "Request to http://localhost/my-url/v1/accounts failed: 400 Bad Request",
            str(ctx.exception),
        )

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_usage_api_failure_non_retriable_failure(self, mock_request):
        mock_accounts_response = create_mock_accounts_response()

        # Mock POST /usages to fail in a non-retriable way
        mock_usage_response = MagicMock()
        mock_usage_response.ok = False
        mock_usage_response.status_code = 400
        mock_usage_response.text = "Bad Request"

        mock_request.side_effect = [
            mock_accounts_response,
            mock_usage_response,
        ]

        with self.assertRaises(ValueError) as ctx:
            self.adapter._extract("run1", self.start_date, self.end_date)

        self.assertIn(
            "Request to http://localhost/my-url/v1/accounts/acct1/usages failed: 400 Bad Request",
            str(ctx.exception),
        )

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_usage_api_failure_retries(self, mock_request):
        mock_accounts_response = create_mock_accounts_response()

        # Mock POST /usages to fail
        failed_usage_request = MagicMock()
        failed_usage_request.ok = False
        failed_usage_request.status_code = 500
        failed_usage_request.text = "Bad Request"

        mock_usages_response = create_mock_usages_response()
        mock_metadata_response = create_mock_account_metadata_response()
        mock_request.side_effect = [
            mock_accounts_response,
            failed_usage_request,
            mock_usages_response,
            mock_metadata_response,
            create_mock_alarms_response(),
        ]

        self.adapter._extract("run1", self.start_date, self.end_date)

        # Includes one retry
        self.assertEqual(5, mock_request.call_count)

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_account_metadata_non_retriable_failure(self, mock_request):
        # Mock accounts to return 1 account
        mock_accounts_response = create_mock_accounts_response()
        # --- Mock POST /usages response ---
        mock_usages_response = create_mock_usages_response()

        # Mock GET /accounts/{accountId} to fail in a non-retriable way
        mock_account_metadata_response = MagicMock()
        mock_account_metadata_response.ok = False
        mock_account_metadata_response.status_code = 400
        mock_account_metadata_response.text = "Bad Request"

        mock_request.side_effect = [
            mock_accounts_response,
            mock_usages_response,
            mock_account_metadata_response,
        ]

        with self.assertRaises(ValueError) as ctx:
            self.adapter._extract("run1", self.start_date, self.end_date)

        self.assertIn(
            "Request to http://localhost/my-url/v1/accounts/acct1 failed: 400 Bad Request",
            str(ctx.exception),
        )

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_account_metadata_api_failure_retries(self, mock_request):
        mock_accounts_response = create_mock_accounts_response()

        mock_usages_response = create_mock_usages_response()

        # Mock GET /accounts/{accountId} to fail in retriable way
        mock_metadata_response = MagicMock()
        mock_metadata_response.ok = False
        mock_metadata_response.status_code = 500
        mock_metadata_response.text = "Bad Request"
        mock_metadata_response_2 = create_mock_account_metadata_response()

        # --- Mock POST /alarms alarms response ---
        mock_alarms_response = create_mock_alarms_response()

        mock_request.side_effect = [
            mock_accounts_response,
            mock_usages_response,
            mock_metadata_response,
            mock_metadata_response_2,
            mock_alarms_response,
        ]

        self.adapter._extract("run1", self.start_date, self.end_date)

        # Includes one retry
        self.assertEqual(5, mock_request.call_count)

    def make_extract_output(self, accounts, usages, alarms=None):
        return ExtractOutput(
            {
                "accounts.json": "\n".join(
                    json.dumps(a, default=lambda o: o.__dict__) for a in accounts
                ),
                "usages.json": "\n".join(
                    json.dumps(u, default=lambda o: o.__dict__) for u in usages
                ),
                "alarms.json": "\n".join(
                    json.dumps(a, default=lambda o: o.__dict__) for a in (alarms or [])
                ),
            }
        )

    def test_transform_when_register_read_same_time_as_interval_read(self):
        usage_time = "2025-08-01T10:00:00+00:00"
        account = SubecaAccount(
            accountId="A1",
            accountStatus="active",
            meterSerial="M1",
            billingRoute="",
            registerSerial="R1",
            meterSize="5/8",
            createdAt="2025-08-01T10:00:00+00:00",
            deviceId="D1",
            activeProtocol="LoRaWAN",
            installationDate="2025-08-01T10:00:00+00:00",
            latestCommunicationDate="2025-08-02T10:00:00+00:00",
            latestReading=SubecaReading(
                deviceId="D1", usageTime=usage_time, unit="cf", value="100"
            ),
        )
        usage = SubecaReading(deviceId="D1", usageTime=usage_time, unit="cf", value="1")
        extract_output = self.make_extract_output([account], [usage])

        meters, reads = self.adapter._transform("run-1", extract_output)

        self.assertEqual(len(meters), 1)
        self.assertEqual(meters[0].device_id, "D1")
        self.assertEqual(meters[0].account_id, "A1")
        self.assertEqual(meters[0].location_id, "A1")
        self.assertEqual(len(reads), 1)  # Register read added
        self.assertEqual(reads[0].interval_value, 1)
        self.assertEqual(reads[0].register_value, 100)

    def test_transform_when_usage_is_empty_string(self):
        usage_time = "2025-08-01T10:00:00+00:00"
        account = SubecaAccount(
            accountId="A1",
            accountStatus="active",
            meterSerial="M1",
            billingRoute="",
            registerSerial="R1",
            meterSize="5/8",
            createdAt="2025-08-01T10:00:00+00:00",
            deviceId="D1",
            activeProtocol="LoRaWAN",
            installationDate="2025-08-01T10:00:00+00:00",
            latestCommunicationDate="2025-08-02T10:00:00+00:00",
            latestReading=SubecaReading(
                deviceId="D1", usageTime=usage_time, unit="cf", value="100"
            ),
        )
        usage = SubecaReading(deviceId="D1", usageTime=usage_time, unit="cf", value="")
        extract_output = self.make_extract_output([account], [usage])

        meters, reads = self.adapter._transform("run-1", extract_output)

        self.assertEqual(len(meters), 1)
        self.assertEqual(len(reads), 1)  # Register read added
        self.assertEqual(reads[0].interval_value, None)
        self.assertEqual(reads[0].register_value, 100)

    def test_transform_meter_with_no_reads_is_still_included(self):
        """Meters should be included even if they have no reads."""
        account = SubecaAccount(
            accountId="A1",
            accountStatus="active",
            meterSerial="M1",
            billingRoute="",
            registerSerial="R1",
            meterSize="5/8",
            createdAt="2025-08-01T10:00:00+00:00",
            deviceId="D1",
            activeProtocol="LoRaWAN",
            installationDate="2025-08-01T10:00:00+00:00",
            latestCommunicationDate="2025-08-02T10:00:00+00:00",
            latestReading=SubecaReading(
                deviceId="D1",
                usageTime="2025-08-01T10:00:00+00:00",
                unit="cf",
                value="100",
            ),
        )
        extract_output = self.make_extract_output([account], [])  # no usages

        meters, reads = self.adapter._transform("run-1", extract_output)

        self.assertEqual(len(meters), 1)
        self.assertEqual(meters[0].device_id, "D1")
        self.assertEqual(len(reads), 1)  # Register read added
        self.assertEqual(reads[0].register_value, 100)

    def test_transform_read_with_no_meter_is_excluded(self):
        """Reads with no matching meter (device ID) should be excluded."""
        account = SubecaAccount(
            accountId="A1",
            accountStatus="active",
            meterSerial="M1",
            billingRoute="",
            registerSerial="R1",
            meterSize="5/8",
            createdAt="2025-08-01T10:00:00+00:00",
            deviceId="D1",
            activeProtocol="LoRaWAN",
            installationDate="2025-08-01T10:00:00+00:00",
            latestCommunicationDate="2025-08-02T10:00:00+00:00",
            latestReading=SubecaReading(
                deviceId="D1",
                usageTime="2025-08-01T10:00:00+00:00",
                unit="cf",
                value="100",
            ),
        )
        bad_usage = SubecaReading(
            deviceId="OTHER_DEVICE",
            usageTime="2025-08-01T10:00:00+00:00",
            unit="cf",
            value="10",
        )

        extract_output = self.make_extract_output([account], [bad_usage])

        meters, reads = self.adapter._transform("run-1", extract_output)

        self.assertEqual(len(meters), 1)
        self.assertTrue(all(r.device_id != "OTHER_DEVICE" for r in reads))

    def test_transform_no_device_id(self):
        """If no service point is found, meter still gets created with None for location_id."""
        account = SubecaAccount(
            accountId="A1",
            accountStatus="active",
            meterSerial="M1",
            billingRoute="",
            registerSerial="R1",
            meterSize="5/8",
            createdAt="2025-08-01T10:00:00+00:00",
            deviceId=None,
            activeProtocol="LoRaWAN",
            installationDate="2025-08-01T10:00:00+00:00",
            latestCommunicationDate="2025-08-02T10:00:00+00:00",
            latestReading=SubecaReading(
                deviceId="D1",
                usageTime="2025-08-01T10:00:00+00:00",
                unit="cf",
                value="100",
            ),
        )
        extract_output = self.make_extract_output([account], [])

        meters, _ = self.adapter._transform("run-1", extract_output)

        self.assertEqual(0, len(meters))

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_returns_alarms_from_single_page(self, mock_request):
        mock_request.return_value = MockResponse(
            {"data": [ALARM_RESPONSE_ITEM], "nextToken": None},
            200,
        )

        result = self.adapter._extract_alarms_for_account(
            "acc1", ALARM_RANGE_START, ALARM_RANGE_END
        )

        self.assertEqual(1, len(result))
        alarm = result[0]
        self.assertIsInstance(alarm, SubecaAlarm)
        self.assertEqual("Radio Low Battery", alarm.name)
        self.assertEqual("2026-01-02T00:00:00+00:00", alarm.startAt)
        self.assertEqual("2026-01-02T01:00:00+00:00", alarm.endAt)
        self.assertEqual("device-001", alarm.deviceId)

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_paginates_until_no_next_token_for_alarms(self, mock_request):
        page1 = {
            "data": [ALARM_RESPONSE_ITEM],
            "nextToken": "tok123",
        }
        page2_item = {
            **ALARM_RESPONSE_ITEM,
            "alarm": {
                "name": "Tamper",
                "startAt": "2026-01-03T00:00:00+00:00",
                "endAt": "2026-01-03T01:00:00+00:00",
                "deviceId": "device-002",
            },
        }
        page2 = {"data": [page2_item], "nextToken": None}
        mock_request.side_effect = [MockResponse(page1, 200), MockResponse(page2, 200)]

        result = self.adapter._extract_alarms_for_account(
            "acc1", ALARM_RANGE_START, ALARM_RANGE_END
        )

        self.assertEqual(2, len(result))
        self.assertEqual("Radio Low Battery", result[0].name)
        self.assertEqual("Tamper", result[1].name)
        self.assertEqual(2, mock_request.call_count)

        # Second call should include nextToken in request body
        second_call_body = mock_request.call_args_list[1].kwargs["json"]
        self.assertEqual("tok123", second_call_body["nextToken"])

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_skips_items_without_alarm_field(self, mock_request):
        item_without_alarm = {
            "accountId": "acc1",
            "accountStatus": "active",
        }
        mock_request.return_value = MockResponse(
            {"data": [item_without_alarm, ALARM_RESPONSE_ITEM], "nextToken": None},
            200,
        )

        result = self.adapter._extract_alarms_for_account(
            "acc1", ALARM_RANGE_START, ALARM_RANGE_END
        )

        self.assertEqual(1, len(result))
        self.assertEqual("Radio Low Battery", result[0].name)

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_returns_empty_list_when_no_data_for_alarms(self, mock_request):
        mock_request.return_value = MockResponse(
            {"data": [], "nextToken": None},
            200,
        )

        result = self.adapter._extract_alarms_for_account(
            "acc1", ALARM_RANGE_START, ALARM_RANGE_END
        )

        self.assertEqual([], result)

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_sends_correct_reference_period(self, mock_request):
        mock_request.return_value = MockResponse(
            {"data": [], "nextToken": None},
            200,
        )

        self.adapter._extract_alarms_for_account(
            "acc1", ALARM_RANGE_START, ALARM_RANGE_END
        )

        call_body = mock_request.call_args.kwargs["json"]
        self.assertEqual("2026-01-01", call_body["referencePeriod"]["start"])
        self.assertEqual("2026-01-03", call_body["referencePeriod"]["end"])
        self.assertEqual("+00:00", call_body["referencePeriod"]["utcOffset"])

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_calls_correct_url_for_alarms(self, mock_request):
        mock_request.return_value = MockResponse(
            {"data": [], "nextToken": None},
            200,
        )

        self.adapter._extract_alarms_for_account(
            "my-account", ALARM_RANGE_START, ALARM_RANGE_END
        )

        call_url = mock_request.call_args.args[1]
        self.assertEqual(
            "http://localhost/my-url/v1/accounts/my-account/alarms", call_url
        )

    @patch("amiadapters.adapters.subeca.requests.request")
    def test_extract_output_includes_alarms_json(self, mock_request):
        accounts_response = {
            "data": [{"accountId": "acc1"}],
            "nextToken": None,
        }
        account_detail_response = {
            "accountId": "acc1",
            "accountStatus": "active",
            "meterSerial": "serial1",
            "billingRoute": "route1",
            "registerSerial": "reg1",
            "meterInfo": {"meterSize": "5/8"},
            "device": {
                "activeProtocol": "LoRaWAN",
                "installationDate": None,
                "latestCommunicationDate": None,
                "deviceId": "device-001",
            },
            "createdAt": None,
        }
        usages_response = {"data": {"hourly": {}}}
        alarms_response = {"data": [ALARM_RESPONSE_ITEM], "nextToken": None}

        mock_request.side_effect = [
            MockResponse(accounts_response, 200),
            MockResponse(usages_response, 200),
            MockResponse(account_detail_response, 200),
            MockResponse(alarms_response, 200),
        ]

        output = self.adapter._extract("run-1", ALARM_RANGE_START, ALARM_RANGE_END)

        self.assertIn("alarms.json", output.get_outputs())
        alarms = output.load_from_file("alarms.json", SubecaAlarm, allow_empty=True)
        self.assertEqual(1, len(alarms))
        self.assertEqual("Radio Low Battery", alarms[0].name)
        self.assertEqual("device-001", alarms[0].deviceId)

    def test_maps_alarm_to_general_meter_alert(self):
        alarm = SubecaAlarm(
            name="Radio Low Battery",
            startAt="2026-01-02T00:00:00+00:00",
            endAt="2026-01-02T01:00:00+00:00",
            deviceId="device-001",
        )
        extract_output = self.make_extract_output([], [], alarms=[alarm])

        result = self.adapter._transform_meter_alerts("run-1", extract_output)

        self.assertEqual(1, len(result))
        alert = result[0]
        self.assertEqual("test-org", alert.org_id)
        self.assertEqual("device-001", alert.device_id)
        self.assertEqual("Radio Low Battery", alert.alert_type)
        self.assertEqual(
            datetime.datetime(2026, 1, 2, 0, 0, 0, tzinfo=pytz.UTC), alert.start_time
        )
        self.assertEqual(
            datetime.datetime(2026, 1, 2, 1, 0, 0, tzinfo=pytz.UTC), alert.end_time
        )
        self.assertEqual("subeca", alert.source)

    def test_active_alarm_maps_to_none_end_time(self):
        alarm = SubecaAlarm(
            name="Leak - Now",
            startAt="2026-01-02T00:00:00+00:00",
            endAt=None,
            deviceId="device-002",
        )
        extract_output = self.make_extract_output([], [], alarms=[alarm])

        result = self.adapter._transform_meter_alerts("run-1", extract_output)

        self.assertEqual(1, len(result))
        self.assertIsNone(result[0].end_time)

    def test_returns_empty_list_for_no_alarms(self):
        extract_output = ExtractOutput({"alarms.json": ""})

        result = self.adapter._transform_meter_alerts("run-1", extract_output)

        self.assertEqual([], result)

    def test_maps_multiple_alarms(self):
        alarms = [
            SubecaAlarm(
                name="Tamper",
                startAt="2026-01-01T00:00:00+00:00",
                endAt="2026-01-01T12:00:00+00:00",
                deviceId="device-001",
            ),
            SubecaAlarm(
                name="Dry Meter",
                startAt="2026-01-02T00:00:00+00:00",
                endAt=None,
                deviceId="device-002",
            ),
        ]
        extract_output = self.make_extract_output([], [], alarms=alarms)

        result = self.adapter._transform_meter_alerts("run-1", extract_output)

        self.assertEqual(2, len(result))
        self.assertEqual("Tamper", result[0].alert_type)
        self.assertEqual("device-001", result[0].device_id)
        self.assertEqual("Dry Meter", result[1].alert_type)
        self.assertEqual("device-002", result[1].device_id)
        self.assertIsNone(result[1].end_time)


class TestTransformMeterAlerts(BaseTestCase):

    def setUp(self):
        self.adapter = SubecaAdapter(
            org_id="test-org",
            org_timezone=pytz.timezone("America/Los_Angeles"),
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            api_url="http://localhost/my-url",
            api_key="test-key",
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_meter_alerts=self.TEST_METER_ALERT_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            configured_sinks=[],
        )

    def _make_extract_output(self, alarms):
        return ExtractOutput(
            {
                "alarms.json": "\n".join(
                    json.dumps(a, default=lambda o: o.__dict__) for a in alarms
                ),
            }
        )


class TestSubecaRawSnowflakeLoader(BaseTestCase):
    def setUp(self):
        self.loader = SUBECA_RAW_SNOWFLAKE_LOADER
        self.run_id = "run-123"
        self.org_id = "test-org"
        self.org_timezone = timezone("UTC")

        # Create mock ExtractOutput
        account = SubecaAccount(
            accountId="123",
            accountStatus="active",
            meterSerial="meter-001",
            billingRoute="route-1",
            registerSerial="reg-001",
            meterSize="5/8",
            createdAt="2025-05-30T02:37:52+00:00",
            deviceId="device-001",
            activeProtocol="LoRaWAN",
            installationDate="2025-06-05T19:33:54+00:00",
            latestCommunicationDate="2025-08-05T20:19:46+00:00",
            latestReading=SubecaReading(
                deviceId="device-001",
                usageTime="2025-08-05T20:19:46+00:00",
                unit="gal",
                value="16685.9",
            ),
        )

        usage = SubecaReading(
            deviceId="device-001",
            usageTime="2025-08-01T10:00:00+00:00",
            unit="cf",
            value="12.3",
        )

        self.extract_outputs = ExtractOutput(
            {
                "accounts.json": json.dumps(account, default=lambda o: o.__dict__),
                "usages.json": json.dumps(usage, default=lambda o: o.__dict__),
            }
        )

        self.snowflake_conn = MagicMock()
        self.snowflake_conn.cursor.return_value = MagicMock()

    def test_load_with_mocked_snowflake_conn(self):
        self.loader.load(
            self.run_id,
            self.org_id,
            self.org_timezone,
            self.extract_outputs,
            self.snowflake_conn,
        )

        # Ensure that snowflake cursor executed SQL statements
        self.assertTrue(self.snowflake_conn.cursor.return_value.execute.called)
        self.assertTrue(self.snowflake_conn.cursor.return_value.executemany.called)
        # Each of the 3 load methods calls cursor() three times
        # So we expect at least 3 * 3 = 9 calls to snowflake_conn.cursor()
        self.assertEqual(self.snowflake_conn.cursor.call_count, 9)

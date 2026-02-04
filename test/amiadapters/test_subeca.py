import datetime
import json
import pytz
from pytz import timezone
from unittest.mock import MagicMock, patch

from amiadapters.outputs.base import ExtractOutput
from amiadapters.adapters.subeca import (
    SubecaAccount,
    SubecaAdapter,
    SubecaReading,
    SUBECA_RAW_SNOWFLAKE_LOADER,
)
from test.base_test_case import BaseTestCase


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


class TestSubecaAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = SubecaAdapter(
            org_id="test-org",
            org_timezone=pytz.timezone("Africa/Algiers"),
            pipeline_configuration=self.TEST_PIPELINE_CONFIGURATION,
            api_url="http://localhost/my-url",
            api_key="test-key",
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
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

        mock_request.side_effect = [
            mock_accounts_response,
            mock_usages_response,
            mock_account_metadata_response,
        ]

        result = self.adapter._extract("run1", self.start_date, self.end_date)

        # Verify API calls
        mock_request.assert_any_call(
            "get",
            f"{self.adapter.api_url}/v1/accounts",
            params={"pageSize": 100},
            headers={"accept": "application/json", "x-subeca-api-key": "test-key"},
        )
        self.assertEqual(3, mock_request.call_count)

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

        mock_request.side_effect = [
            mock_accounts_response,
            mock_usages_response,
            mock_account_metadata_response,
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

        # The second GET call should return metadata response
        mock_request.side_effect = [
            mock_accounts_response_1,
            mock_accounts_response_2,
            mock_usages_response,
            mock_account_metadata_response,
        ]

        result = self.adapter._extract("run1", self.start_date, self.end_date)

        self.assertEqual(4, mock_request.call_count)

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

        # The second GET call should return metadata response
        mock_request.side_effect = [
            mock_accounts_response_1,
            mock_accounts_response_2,
            mock_usages_response,
            mock_account_metadata_response,
        ]

        self.adapter._extract("run1", self.start_date, self.end_date)

        self.assertEqual(4, mock_request.call_count)

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
        ]

        self.adapter._extract("run1", self.start_date, self.end_date)

        # Includes one retry
        self.assertEqual(4, mock_request.call_count)

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

        mock_request.side_effect = [
            mock_accounts_response,
            mock_usages_response,
            mock_metadata_response,
            mock_metadata_response_2,
        ]

        self.adapter._extract("run1", self.start_date, self.end_date)

        # Includes one retry
        self.assertEqual(4, mock_request.call_count)

    def make_extract_output(self, accounts, usages):
        return ExtractOutput(
            {
                "accounts.json": "\n".join(
                    json.dumps(a, default=lambda o: o.__dict__) for a in accounts
                ),
                "usages.json": "\n".join(
                    json.dumps(u, default=lambda o: o.__dict__) for u in usages
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

    def test_transform_transform_no_device_id(self):
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

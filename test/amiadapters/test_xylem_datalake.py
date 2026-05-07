import json
from unittest.mock import MagicMock, patch

import requests

from amiadapters.outputs.base import ExtractOutput
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.adapters.xylem_datalake import (
    XylemDatalakeAdapter,
    _create_superset_session_with_retry,
)
from test.base_test_case import BaseTestCase


def _make_account(**overrides):
    defaults = {
        "device_id": "74931234",
        "commodity": "WATER",
        "device_type": "METER",
        "radio_id": "86577068",
        "account_id": "1818",
        "account_name": "TEST CUSTOMER",
        "account_rate_code": "020",
        "account_service_type": "R",
        "account_status": "ACTIVE",
        "asset_address": "851 IRWIN DR",
        "asset_city": "HILLSBOROUGH",
        "asset_state": "CA",
        "asset_zip": "94010",
        "meter_manufacturer": "SE",
        "meter_size": 1.5,
        "display_multiplier": 1,
        "sdp_id": "030-100-010",
        "record_active_flag": "true",
    }
    defaults.update(overrides)
    return defaults


def _make_interval(**overrides):
    defaults = {
        "meter_id": "74931234",
        "radio_id": "86577068",
        "commodity": "WATER",
        "unit_of_measure": "CF",
        "read_time_timestamp": "2026-04-14T08:00:00",
        "rni_multiplier": "1",
        "interval_value": "3",
        "read_time_timestamp_local": "2026-04-14T01:00:00",
    }
    defaults.update(overrides)
    return defaults


def _make_register(**overrides):
    defaults = {
        "meter_id": "74931234",
        "radio_id": "86577068",
        "commodity": "WATER",
        "unit_of_measure": "CF",
        "read_time_timestamp": "2026-04-14T08:00:00",
        "read_quality": "R",
        "flag": "",
        "rni_multiplier": "1",
        "register_value": "59727",
        "read_time_timestamp_local": "2026-04-14T01:00:00",
    }
    defaults.update(overrides)
    return defaults


def _build_extract_output(accounts, intervals, registers):
    return ExtractOutput(
        {
            "account.json": "\n".join(
                json.dumps(a, cls=DataclassJSONEncoder) for a in accounts
            ),
            "water_intervals.json": "\n".join(
                json.dumps(i, cls=DataclassJSONEncoder) for i in intervals
            ),
            "water_registers.json": "\n".join(
                json.dumps(r, cls=DataclassJSONEncoder) for r in registers
            ),
        }
    )


class TestXylemDatalakeAdapter(BaseTestCase):
    def setUp(self):
        self.adapter = XylemDatalakeAdapter(
            org_id="test_org",
            org_timezone="America/Los_Angeles",
            pipeline_configuration=None,
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            agency_code="hlsbo",
            database_id=1,
            client_id="test-client-id",
            username="test",
            password="test",
            configured_sinks=[],
        )

    def test_transform_basic(self):
        extract = _build_extract_output(
            [_make_account()],
            [_make_interval()],
            [_make_register()],
        )
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(meters), 1)
        self.assertIsInstance(meters[0], GeneralMeter)
        self.assertEqual(meters[0].device_id, "74931234")
        self.assertEqual(meters[0].meter_id, "74931234")
        self.assertEqual(meters[0].account_id, "1818")
        self.assertEqual(meters[0].location_id, "030-100-010")
        self.assertEqual(meters[0].endpoint_id, "86577068")
        self.assertEqual(meters[0].location_address, "851 IRWIN DR")
        self.assertEqual(meters[0].location_city, "HILLSBOROUGH")
        self.assertEqual(meters[0].location_state, "CA")

        self.assertEqual(len(reads), 1)
        self.assertIsInstance(reads[0], GeneralMeterRead)
        self.assertEqual(reads[0].device_id, "74931234")
        self.assertEqual(reads[0].account_id, "1818")
        self.assertEqual(reads[0].interval_value, 3.0)
        self.assertEqual(reads[0].register_value, 59727.0)

    def test_transform_interval_only(self):
        extract = _build_extract_output(
            [_make_account()],
            [_make_interval()],
            [],
        )
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(reads), 1)
        self.assertEqual(reads[0].interval_value, 3.0)
        self.assertIsNone(reads[0].register_value)

    def test_transform_register_only(self):
        extract = _build_extract_output(
            [_make_account()],
            [],
            [_make_register()],
        )
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(reads), 1)
        self.assertIsNone(reads[0].interval_value)
        self.assertEqual(reads[0].register_value, 59727.0)

    def test_transform_joins_interval_and_register_by_device_and_time(self):
        extract = _build_extract_output(
            [_make_account()],
            [_make_interval()],
            [_make_register()],
        )
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(reads), 1)
        self.assertEqual(reads[0].interval_value, 3.0)
        self.assertEqual(reads[0].register_value, 59727.0)

    def test_transform_does_not_join_different_timestamps(self):
        extract = _build_extract_output(
            [_make_account()],
            [_make_interval(read_time_timestamp="2026-04-14T08:00:00")],
            [_make_register(read_time_timestamp="2026-04-14T09:00:00")],
        )
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(reads), 2)

    def test_transform_skips_non_water_accounts(self):
        extract = _build_extract_output(
            [_make_account(commodity="ELECTRIC")],
            [_make_interval()],
            [],
        )
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 0)

    def test_transform_skips_reads_without_matching_meter(self):
        extract = _build_extract_output(
            [_make_account(device_id="99999999")],
            [_make_interval(meter_id="74931234")],
            [],
        )
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(meters), 1)
        self.assertEqual(len(reads), 0)

    def test_transform_populates_account_metadata_on_read(self):
        account = _make_account()
        interval = _make_interval()
        extract = _build_extract_output([account], [interval], [])

        meters, reads = self.adapter._transform("run1", extract)
        self.assertEqual(len(reads), 1)
        self.assertEqual(reads[0].account_id, "1818")
        self.assertEqual(reads[0].location_id, "030-100-010")

    def test_transform_multiple_meters(self):
        account1 = _make_account(device_id="111", account_id="A1", radio_id="R1")
        account2 = _make_account(device_id="222", account_id="A2", radio_id="R2")
        interval1 = _make_interval(meter_id="111", radio_id="R1")
        interval2 = _make_interval(meter_id="222", radio_id="R2")

        extract = _build_extract_output(
            [account1, account2],
            [interval1, interval2],
            [],
        )
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(meters), 2)
        self.assertEqual(len(reads), 2)
        device_ids = {m.device_id for m in meters}
        self.assertEqual(device_ids, {"111", "222"})

    def test_transform_empty_extract(self):
        extract = _build_extract_output([], [], [])
        meters, reads = self.adapter._transform("run1", extract)

        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 0)

    def test_parse_flowtime_with_T(self):
        from datetime import datetime

        result = self.adapter._parse_flowtime("2026-04-14T08:00:00")
        self.assertEqual(result, datetime(2026, 4, 14, 8, 0, 0))

    def test_parse_flowtime_with_space(self):
        from datetime import datetime

        result = self.adapter._parse_flowtime("2026-04-14 08:00:00")
        self.assertEqual(result, datetime(2026, 4, 14, 8, 0, 0))


def _query_response(
    status_code, json_data=None, text="", content_type="application/json"
):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = {"Content-Type": content_type}
    resp.text = text
    resp.json.return_value = json_data if json_data is not None else {}
    if 400 <= status_code < 600:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"{status_code} Error", response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestXylemDatalakeQueryRetry(BaseTestCase):
    """Tests for retry behavior in _query and the auth retry wrapper."""

    def setUp(self):
        self.adapter = XylemDatalakeAdapter(
            org_id="test_org",
            org_timezone="America/Los_Angeles",
            pipeline_configuration=None,
            configured_task_output_controller=self.TEST_TASK_OUTPUT_CONTROLLER_CONFIGURATION,
            configured_metrics=self.TEST_METRICS_CONFIGURATION,
            agency_code="hlsbo",
            database_id=1,
            client_id="test-client-id",
            username="test",
            password="test",
            configured_sinks=[],
        )
        self.adapter._session = MagicMock()
        self.adapter._requests_since_csrf = 0

    @patch("amiadapters.adapters.xylem_datalake.time.sleep")
    def test_query_retries_on_5xx_then_succeeds(self, _mock_sleep):
        success = _query_response(
            200, json_data={"status": "success", "data": [{"x": 1}]}
        )
        self.adapter._session.post.side_effect = [
            _query_response(500, text="boom"),
            _query_response(502, text="bad gateway"),
            success,
        ]
        rows = self.adapter._query("SELECT 1")
        self.assertEqual(rows, [{"x": 1}])
        self.assertEqual(self.adapter._session.post.call_count, 3)

    @patch("amiadapters.adapters.xylem_datalake.time.sleep")
    def test_query_raises_after_max_5xx_attempts(self, _mock_sleep):
        self.adapter._session.post.return_value = _query_response(500, text="boom")
        with self.assertRaises(requests.exceptions.HTTPError):
            self.adapter._query("SELECT 1")

    @patch("amiadapters.adapters.xylem_datalake.time.sleep")
    def test_query_retries_on_connection_error_then_succeeds(self, _mock_sleep):
        success = _query_response(
            200, json_data={"status": "success", "data": [{"x": 1}]}
        )
        self.adapter._session.post.side_effect = [
            requests.exceptions.ConnectionError("dns failure"),
            requests.exceptions.Timeout("read timeout"),
            success,
        ]
        rows = self.adapter._query("SELECT 1")
        self.assertEqual(rows, [{"x": 1}])
        self.assertEqual(self.adapter._session.post.call_count, 3)


class TestCreateSupersetSessionWithRetry(BaseTestCase):
    """Tests for the auth retry wrapper."""

    @patch("amiadapters.adapters.xylem_datalake.time.sleep")
    @patch("amiadapters.adapters.xylem_datalake._create_superset_session")
    def test_returns_session_on_first_success(self, mock_create, _mock_sleep):
        sentinel = MagicMock(name="session")
        mock_create.return_value = sentinel
        result = _create_superset_session_with_retry(
            "https://dl", "https://sup", "client", "user", "pw"
        )
        self.assertIs(result, sentinel)
        self.assertEqual(mock_create.call_count, 1)

    @patch("amiadapters.adapters.xylem_datalake.time.sleep")
    @patch("amiadapters.adapters.xylem_datalake._create_superset_session")
    def test_retries_then_succeeds(self, mock_create, _mock_sleep):
        sentinel = MagicMock(name="session")
        mock_create.side_effect = [
            RuntimeError("Superset login returned unexpected shape (status 401)"),
            RuntimeError("Superset login returned unexpected shape (status 401)"),
            sentinel,
        ]
        result = _create_superset_session_with_retry(
            "https://dl", "https://sup", "client", "user", "pw"
        )
        self.assertIs(result, sentinel)
        self.assertEqual(mock_create.call_count, 3)

    @patch("amiadapters.adapters.xylem_datalake.time.sleep")
    @patch("amiadapters.adapters.xylem_datalake._create_superset_session")
    def test_raises_after_max_attempts(self, mock_create, _mock_sleep):
        mock_create.side_effect = RuntimeError("persistent failure")
        with self.assertRaises(RuntimeError):
            _create_superset_session_with_retry(
                "https://dl", "https://sup", "client", "user", "pw"
            )

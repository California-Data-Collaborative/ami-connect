import datetime
import json
import pytz
from unittest import mock

from amiadapters.config import ConfiguredLocalTaskOutputController
from amiadapters.models import GeneralMeterRead
from amiadapters.models import GeneralMeter
from amiadapters.beacon import (
    Beacon360Adapter,
    Beacon360MeterAndRead,
    REQUESTED_COLUMNS,
)

from test.base_test_case import (
    BaseTestCase,
    MockResponse,
    mocked_response_429,
    mocked_response_500,
)


REPORT_CONTENTS_CSV = """\"Account_ID\",\"Endpoint_SN\",\"Estimated_Flag\",\"Flow\",\"Flow_Time\",\"Flow_Unit\",\"Location_Address_Line1\",\"Location_Address_Line2\",\"Location_Address_Line3\",\"Location_City\",\"Location_Country\",\"Location_ID\",\"Location_State\",\"Location_ZIP\",\"Meter_ID\",\"Meter_Install_Date\",\"Meter_Manufacturer\",\"Meter_Model\",\"Meter_Size\",\"Meter_Size_Desc\",\"Meter_Size_Unit\",\"Meter_SN\",\"Raw_Read\",\"Read\",\"Read_Time\",\"Read_Unit\"
\"303022\",\"130615549\",\"0\",\"0.0\",\"2024-08-01 00:59\",\"Gallons\",\"5391 E. MYSTREET\",\"\",\"\",\"Apple\",\"US\",\"303022\",\"CA\",\"93727\",\"1470158170\",\"\",\"BADGER\",\"T-10\",\"0.625\",\"5/8\"\"\",\"INCHES\",\"1470158170\",\"022760\",\"227.6\",\"2024-08-01 00:59\",\"CCF\"
\"303022\",\"130615549\",\"0\",\"0.0\",\"2024-08-01 01:59\",\"Gallons\",\"5391 E. MYSTREET\",\"\",\"\",\"Apple\",\"US\",\"303022\",\"CA\",\"93727\",\"1470158170\",\"\",\"BADGER\",\"T-10\",\"0.625\",\"5/8\"\"\",\"INCHES\",\"1470158170\",\"022760\",\"227.6\",\"2024-08-01 01:59\",\"CCF\"
"""


def mocked_create_range_report(*args, **kwargs):
    data = {
        "edsUUID": "acecd48e8b794f49a61c2b96c9ff9118",
        "statusUrl": "/v2/eds/status/acecd48e8b794f49a61c2b96c9ff9118",
    }
    return MockResponse(data, 202)


def mocked_get_range_report_status_not_finished(*args, **kwargs):
    data = {
        "message": "acecd48e8b794f49a61c2b96c9ff9118 operation running.",
        "progress": {"percentComplete": 5.0},
        "queueTime": "2025-03-18T18:52:41Z",
        "state": "run",
    }
    return MockResponse(data, 200)


def mocked_get_range_report_status_finished(*args, **kwargs):
    data = {
        "endTime": "2025-03-18T18:56:14Z",
        "lastMeterId": "800380.1",
        "message": "acecd48e8b794f49a61c2b96c9ff9118 operation succeeded",
        "queueTime": "2025-03-18T18:52:41Z",
        "reportUrl": "/v1/content/8021910600018033071/users/4928669927440453458/export90868",
        "state": "done",
    }
    return MockResponse(data, 200)


def mocked_get_report_from_link(*args, **kwargs):
    return MockResponse(None, 200, text=REPORT_CONTENTS_CSV)


def mocked_exception_from_status_check(*args, **kwargs):
    data = {"state": "exception"}
    return MockResponse(data, 200)


def mocked_get_consumption_response_last_page(*args, **kwargs):
    data = {"meters": [], "currentPage": 2, "itemsOnPage": 0, "totalCount": 1}
    return MockResponse(data, 200)


class TestBeacon360Adapter(BaseTestCase):

    def setUp(self):
        self.adapter = Beacon360Adapter(
            api_user="user",
            api_password="pass",
            use_cache=False,
            org_id="this-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            configured_task_output_controller=ConfiguredLocalTaskOutputController(
                "/tmp/output"
            ),
            configured_sinks=[],
        )
        self.range_start = datetime.datetime(2024, 1, 2, 0, 0)
        self.range_end = datetime.datetime(2024, 1, 3, 0, 0)

    def test_init(self):
        self.assertEqual("/tmp/output", self.adapter.output_controller.output_folder)
        self.assertEqual("user", self.adapter.user)
        self.assertEqual("pass", self.adapter.password)
        self.assertEqual("beacon-360-this-org", self.adapter.name())

    @mock.patch("requests.get")
    @mock.patch("requests.post")
    def test_fetch_range_report__uses_cache(self, mock_post, mock_get):
        self.adapter.use_cache = True
        self.adapter._get_cached_report = mock.MagicMock(
            return_value=REPORT_CONTENTS_CSV
        )

        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(REPORT_CONTENTS_CSV, result)
        self.assertEqual(0, mock_get.call_count)
        self.assertEqual(0, mock_post.call_count)

    def test_fetch_range_report__throws_exception_when_range_not_valid(
        self,
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(None, self.range_end)

        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, None)

        with self.assertRaises(Exception) as context:
            # End after start
            self.adapter._fetch_range_report(self.range_end, self.range_start)

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_not_finished(),
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__can_fetch_report_from_api(
        self, mock_sleep, mock_post, mock_get
    ):
        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(REPORT_CONTENTS_CSV, result)

        self.assertEqual(1, len(mock_post.call_args_list))
        generater_report_request = mock_post.call_args_list[0]
        self.assertEqual(
            "https://api.beaconama.net/v2/eds/range",
            generater_report_request.kwargs["url"],
        )
        self.assertEqual(
            ",".join(REQUESTED_COLUMNS),
            generater_report_request.kwargs["params"]["Header_Columns"],
        )
        self.assertEqual(
            self.range_start,
            generater_report_request.kwargs["params"]["Start_Date"],
        )
        self.assertEqual(
            self.range_end,
            generater_report_request.kwargs["params"]["End_Date"],
        )
        self.assertTrue(generater_report_request.kwargs["params"]["Has_Endpoint"])
        self.assertEqual(
            "hourly", generater_report_request.kwargs["params"]["Resolution"]
        )
        self.assertEqual(
            {"Content-Type": "application/x-www-form-urlencoded"},
            generater_report_request.kwargs["headers"],
        )

        self.assertEqual(3, len(mock_get.call_args_list))
        self.assertIn(
            "https://api.beaconama.net/v2/eds/status/",
            mock_get.call_args_list[0].kwargs["url"],
        )
        self.assertIn(
            "https://api.beaconama.net/v2/eds/status/",
            mock_get.call_args_list[1].kwargs["url"],
        )
        self.assertIn(
            "https://api.beaconama.net/v1/content/",
            mock_get.call_args_list[2].kwargs["url"],
        )

        self.assertEqual(1, mock_sleep.call_count)

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_not_finished(),
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_response_429()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_rate_limit_exceeded_when_report_generated(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Rate limit exceeded" in str(context.exception))

    @mock.patch("requests.get", side_effect=[])
    @mock.patch("requests.post", side_effect=[mocked_response_500()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_non_202_from_report_generation(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Failed request to generate report" in str(context.exception))

    @mock.patch("requests.get", side_effect=[mocked_response_500()])
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_status_response_non_200(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Failed request to get report status" in str(context.exception))

    @mock.patch("requests.get", side_effect=[mocked_exception_from_status_check()])
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_status_response_indicates_exception(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Exception found in report status" in str(context.exception))

    # Mock the status call response as "not finished" way more times than our max limit
    @mock.patch(
        "requests.get",
        side_effect=[mocked_get_range_report_status_not_finished()] * 500,
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_max_attempts_reached_while_polling_for_status(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Reached max attempts" in str(context.exception))

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_finished(),
            Exception,
            mocked_get_report_from_link(),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__retries_once_when_fetch_report_throws_exception(
        self, mock_sleep, mock_post, mock_get
    ):
        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(REPORT_CONTENTS_CSV, result)
        self.assertEqual(1, mock_sleep.call_count)

    @mock.patch(
        "requests.get",
        side_effect=[mocked_get_range_report_status_finished(), mocked_response_500()],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__throws_exception_when_fetch_report_returns_non_200(
        self, mock_sleep, mock_post, mock_get
    ):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report(self.range_start, self.range_end)

        self.assertTrue("Failed request to download report" in str(context.exception))

    def test_report_to_output(self):
        result = self.adapter._report_to_output_stream(REPORT_CONTENTS_CSV)
        result = [r for r in result]
        result = [Beacon360MeterAndRead(**json.loads(d)) for d in result]

        expected = [
            Beacon360MeterAndRead(
                Account_ID="303022",
                Endpoint_SN="130615549",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 00:59",
                Flow_Unit="Gallons",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_City="Apple",
                Location_Country="US",
                Location_ID="303022",
                Location_State="CA",
                Location_ZIP="93727",
                Meter_ID="1470158170",
                Meter_Install_Date="",
                Meter_Manufacturer="BADGER",
                Meter_Model="T-10",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Raw_Read="022760",
                Read="227.6",
                Read_Time="2024-08-01 00:59",
                Read_Unit="CCF",
            ),
            Beacon360MeterAndRead(
                Account_ID="303022",
                Endpoint_SN="130615549",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 01:59",
                Flow_Unit="Gallons",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_City="Apple",
                Location_Country="US",
                Location_ID="303022",
                Location_State="CA",
                Location_ZIP="93727",
                Meter_ID="1470158170",
                Meter_Install_Date="",
                Meter_Manufacturer="BADGER",
                Meter_Model="T-10",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Raw_Read="022760",
                Read="227.6",
                Read_Time="2024-08-01 01:59",
                Read_Unit="CCF",
            ),
        ]

        self.assertEqual(expected, result)

    def test_transform_meters_and_reads(self):
        raw_meters_with_reads = [
            Beacon360MeterAndRead(
                Account_ID="303022",
                Endpoint_SN="130615549",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 00:59",
                Flow_Unit="Gallons",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_City="Apple",
                Location_Country="US",
                Location_ID="303022",
                Location_State="CA",
                Location_ZIP="93727",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="BADGER",
                Meter_Model="T-10",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Raw_Read="022760",
                Read="227.6",
                Read_Time="2024-08-01 00:59",
                Read_Unit="CCF",
            ),
            Beacon360MeterAndRead(
                Account_ID="303022",
                Endpoint_SN="130615549",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 01:59",
                Flow_Unit="Gallons",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_City="Apple",
                Location_Country="US",
                Location_ID="303022",
                Location_State="CA",
                Location_ZIP="93727",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="BADGER",
                Meter_Model="T-10",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Raw_Read="022760",
                Read="227.6",
                Read_Time="2024-08-01 01:59",
                Read_Unit="CCF",
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )
        transformed_meters = list(sorted(transformed_meters, key=lambda m: m.meter_id))

        expected_meters = [
            GeneralMeter(
                org_id="this-org",
                device_id="1470158170",
                account_id="303022",
                location_id="303022",
                meter_id="1470158170",
                endpoint_id="130615549",
                meter_install_date=datetime.datetime(
                    2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                meter_size="0.625",
                meter_manufacturer="BADGER",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            )
        ]
        self.assertListEqual(expected_meters, transformed_meters)

        expected_reads = [
            GeneralMeterRead(
                org_id="this-org",
                device_id="1470158170",
                account_id="303022",
                location_id="303022",
                flowtime=datetime.datetime(
                    2024, 8, 1, 0, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                register_value=227.6,
                register_unit="CCF",
                interval_value=None,
                interval_unit=None,
            ),
            GeneralMeterRead(
                org_id="this-org",
                device_id="1470158170",
                account_id="303022",
                location_id="303022",
                flowtime=datetime.datetime(
                    2024, 8, 1, 1, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                register_value=227.6,
                register_unit="CCF",
                interval_value=None,
                interval_unit=None,
            ),
        ]
        self.assertListEqual(expected_reads, transformed_reads)

    def test_transform_meters_and_reads__two_different_meters(self):
        raw_meters_with_reads = [
            Beacon360MeterAndRead(
                Account_ID="1",
                Endpoint_SN="130615549",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 00:59",
                Flow_Unit="Gallons",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_City="Apple",
                Location_Country="US",
                Location_ID="303022",
                Location_State="CA",
                Location_ZIP="93727",
                Meter_ID="10101",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="BADGER",
                Meter_Model="T-10",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="10101",
                Raw_Read="022760",
                Read="227.6",
                Read_Time="2024-08-01 01:59",
                Read_Unit="CCF",
            ),
            Beacon360MeterAndRead(
                Account_ID="303022",
                Endpoint_SN="130615549",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 01:59",
                Flow_Unit="Gallons",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_City="Apple",
                Location_Country="US",
                Location_ID="303022",
                Location_State="CA",
                Location_ZIP="93727",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="BADGER",
                Meter_Model="T-10",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Raw_Read="022760",
                Read="227.6",
                Read_Time="2024-08-01 00:59",
                Read_Unit="CCF",
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )
        transformed_meters = list(sorted(transformed_meters, key=lambda m: m.device_id))
        transformed_reads = list(sorted(transformed_reads, key=lambda m: m.device_id))

        self.maxDiff = None
        expected_meters = [
            GeneralMeter(
                org_id="this-org",
                device_id="10101",
                account_id="1",
                location_id="303022",
                meter_id="10101",
                endpoint_id="130615549",
                meter_install_date=datetime.datetime(
                    2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                meter_size="0.625",
                meter_manufacturer="BADGER",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            ),
            GeneralMeter(
                org_id="this-org",
                device_id="1470158170",
                account_id="303022",
                location_id="303022",
                meter_id="1470158170",
                endpoint_id="130615549",
                meter_install_date=datetime.datetime(
                    2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                meter_size="0.625",
                meter_manufacturer="BADGER",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            ),
        ]
        self.assertListEqual(expected_meters, transformed_meters)

        expected_reads = [
            GeneralMeterRead(
                org_id="this-org",
                device_id="10101",
                account_id="1",
                location_id="303022",
                flowtime=datetime.datetime(
                    2024, 8, 1, 1, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                register_value=227.6,
                register_unit="CCF",
                interval_value=None,
                interval_unit=None,
            ),
            GeneralMeterRead(
                org_id="this-org",
                device_id="1470158170",
                account_id="303022",
                location_id="303022",
                flowtime=datetime.datetime(
                    2024, 8, 1, 0, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                register_value=227.6,
                register_unit="CCF",
                interval_value=None,
                interval_unit=None,
            ),
        ]
        self.assertListEqual(expected_reads, transformed_reads)

    def test_transform_meters_and_reads__ignores_reads_when_date_missing(self):
        raw_meters_with_reads = [
            # Read_Time is None
            Beacon360MeterAndRead(
                Account_ID="303022",
                Endpoint_SN="130615549",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 00:59",
                Flow_Unit="Gallons",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_City="Apple",
                Location_Country="US",
                Location_ID="303022",
                Location_State="CA",
                Location_ZIP="93727",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="BADGER",
                Meter_Model="T-10",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Raw_Read="022760",
                Read="227.6",
                Read_Time=None,
                Read_Unit="CCF",
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )
        transformed_meters = list(sorted(transformed_meters, key=lambda m: m.meter_id))

        expected_meters = [
            GeneralMeter(
                org_id="this-org",
                device_id="1470158170",
                account_id="303022",
                location_id="303022",
                meter_id="1470158170",
                endpoint_id="130615549",
                meter_install_date=datetime.datetime(
                    2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
                ),
                meter_size="0.625",
                meter_manufacturer="BADGER",
                multiplier=None,
                location_address="5391 E. MYSTREET",
                location_city="Apple",
                location_state="CA",
                location_zip="93727",
            ),
        ]
        self.assertListEqual(expected_meters, transformed_meters)

        expected_reads = []
        self.assertListEqual(expected_reads, transformed_reads)

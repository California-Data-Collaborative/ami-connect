import datetime
import json
import pytz
from unittest import mock

from amiadapters.beacon import (
    Beacon360Adapter,
    Beacon360MeterAndRead,
    BeaconRawSnowflakeLoader,
    REQUESTED_COLUMNS,
)
from amiadapters.config import ConfiguredLocalTaskOutputController
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput

from test.base_test_case import (
    BaseTestCase,
    MockResponse,
    mocked_response_429,
    mocked_response_500,
)


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


def mocked_get_report_from_link(text, *args, **kwargs):
    return MockResponse(None, 200, text=text)


def mocked_exception_from_status_check(*args, **kwargs):
    data = {"state": "exception"}
    return MockResponse(data, 200)


def mocked_get_consumption_response_last_page(*args, **kwargs):
    data = {"meters": [], "currentPage": 2, "itemsOnPage": 0, "totalCount": 1}
    return MockResponse(data, 200)


def beacon_meter_and_read_factory(
    account_id: str = "303022",
    meter_id: str = "1470158170",
    flow_time: str = "2024-08-01 00:59",
    meter_install_date: str = "",
) -> Beacon360MeterAndRead:
    return Beacon360MeterAndRead(
        Account_ID=account_id,
        Endpoint_SN="130615549",
        Estimated_Flag="0",
        Flow="0.0",
        Flow_Time=flow_time,
        Flow_Unit="Gallons",
        Location_Address_Line1="5391 E. MYSTREET",
        Location_Address_Line2="",
        Location_Address_Line3="",
        Location_City="Apple",
        Location_Country="US",
        Location_ID=account_id,
        Location_State="CA",
        Location_ZIP="93727",
        Meter_ID=meter_id,
        Meter_Install_Date=meter_install_date,
        Meter_Manufacturer="BADGER",
        Meter_Model="T-10",
        Meter_Size="0.625",
        Meter_Size_Desc='5/8"',
        Meter_Size_Unit="INCHES",
        Meter_SN=meter_id,
        Raw_Read="022760",
        Read="227.6",
        Read_Time=flow_time,
        Read_Unit="CCF",
        Current_Leak_Rate="",
        Current_Leak_Start_Date="",
        Demand_Zone_ID="",
        Dials="",
        Endpoint_Install_Date="",
        Location_Continuous_Flow="",
        Location_Latitude="",
        Location_Longitude="",
        Location_Irrigated_Area="",
        Location_Irrigation="",
        Location_Main_Use="",
        Location_Name="",
        Location_Pool="",
        Location_Water_Type="",
        Location_Year_Built="",
        Register_Number="",
        Register_Resolution="",
        SA_Start_Date="",
        Service_Point_Class_Code="",
        Service_Point_Class_Code_Normalized="",
    )


class TestBeacon360Adapter(BaseTestCase):

    report_csv = BaseTestCase.load_fixture("beacon-360-report.csv")

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
            cache_output_folder="/tmp/output",
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
        self.adapter._get_cached_report = mock.MagicMock(return_value=self.report_csv)

        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(self.report_csv, result)
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
            mocked_get_report_from_link(text=report_csv),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__can_fetch_report_from_api(
        self, mock_sleep, mock_post, mock_get
    ):
        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(self.report_csv, result)

        self.assertEqual(1, len(mock_post.call_args_list))
        generate_report_request = mock_post.call_args_list[0]
        self.assertEqual(
            "https://api.beaconama.net/v2/eds/range",
            generate_report_request.kwargs["url"],
        )
        self.assertEqual(
            ",".join(REQUESTED_COLUMNS),
            generate_report_request.kwargs["params"]["Header_Columns"],
        )
        self.assertEqual(
            self.range_start,
            generate_report_request.kwargs["params"]["Start_Date"],
        )
        self.assertEqual(
            self.range_end,
            generate_report_request.kwargs["params"]["End_Date"],
        )
        self.assertTrue(generate_report_request.kwargs["params"]["Has_Endpoint"])
        self.assertEqual(
            "hourly", generate_report_request.kwargs["params"]["Resolution"]
        )
        self.assertEqual(
            {"Content-Type": "application/x-www-form-urlencoded"},
            generate_report_request.kwargs["headers"],
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
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(text=report_csv),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__can_filter_to_meter_ids(
        self, mock_sleep, mock_post, mock_get
    ):
        self.adapter._fetch_range_report(self.range_start, self.range_end, meter_ids=["m1", "m2"])

        self.assertEqual(1, len(mock_post.call_args_list))
        generate_report_request = mock_post.call_args_list[0]
        self.assertEqual(
            "m1,m2",
            generate_report_request.kwargs["params"]["Meter_ID"],
        )

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_range_report_status_not_finished(),
            mocked_get_range_report_status_finished(),
            mocked_get_report_from_link(text=report_csv),
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
            mocked_get_report_from_link(text=report_csv),
        ],
    )
    @mock.patch("requests.post", side_effect=[mocked_create_range_report()])
    @mock.patch("time.sleep")
    def test_fetch_range_report__retries_once_when_fetch_report_throws_exception(
        self, mock_sleep, mock_post, mock_get
    ):
        result = self.adapter._fetch_range_report(self.range_start, self.range_end)
        self.assertEqual(self.report_csv, result)
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
        result = self.adapter._report_to_output_stream(self.report_csv)
        result = [r for r in result]
        result = [Beacon360MeterAndRead(**json.loads(d)) for d in result]

        expected = [
            beacon_meter_and_read_factory(flow_time="2024-08-01 00:59"),
            beacon_meter_and_read_factory(flow_time="2024-08-01 01:59"),
        ]

        self.assertEqual(expected, result)

    def test_transform_meters_and_reads(self):
        raw_meters_with_reads = [
            beacon_meter_and_read_factory(
                flow_time="2024-08-01 00:59", meter_install_date="2016-01-01 23:59"
            ),
            beacon_meter_and_read_factory(
                flow_time="2024-08-01 01:59", meter_install_date="2016-01-01 23:59"
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
            beacon_meter_and_read_factory(
                account_id="1",
                meter_id="10101",
                flow_time="2024-08-01 00:59",
                meter_install_date="2016-01-01 23:59",
            ),
            beacon_meter_and_read_factory(
                account_id="303022",
                meter_id="1470158170",
                flow_time="2024-08-01 01:59",
                meter_install_date="2016-01-01 23:59",
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )
        transformed_meters = list(sorted(transformed_meters, key=lambda m: m.device_id))
        transformed_reads = list(sorted(transformed_reads, key=lambda m: m.device_id))

        expected_meters = [
            GeneralMeter(
                org_id="this-org",
                device_id="10101",
                account_id="1",
                location_id="1",
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
                location_id="1",
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

    def test_transform_meters_and_reads__ignores_reads_when_date_missing(self):
        raw_meters_with_reads = [
            beacon_meter_and_read_factory(flow_time=None),
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
                meter_install_date=None,
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


class TestBeaconRawSnowflakeLoader(BaseTestCase):

    def setUp(self):
        self.conn = mock.Mock()
        self.mock_cursor = mock.Mock()
        self.conn.cursor.return_value = self.mock_cursor
        meter_and_read = beacon_meter_and_read_factory()
        self.output_controller = mock.Mock()
        self.output_controller.read_extract_outputs.return_value = ExtractOutput(
            {
                "meters_and_reads.json": json.dumps(
                    meter_and_read, cls=DataclassJSONEncoder
                )
            }
        )

    def test_load(self):
        loader = BeaconRawSnowflakeLoader()
        loader.load(
            "run-id",
            "org-id",
            pytz.timezone("Europe/Rome"),
            self.output_controller,
            self.conn,
        )
        self.assertEqual(2, self.mock_cursor.execute.call_count)
        self.assertEqual(1, self.mock_cursor.executemany.call_count)

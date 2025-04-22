import datetime
import pytz
from unittest import mock

from amiadapters.base import GeneralMeter, GeneralMeterRead
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


REPORT_CONTENTS_CSV = """\"Account_Billing_Cycle\",\"Account_Email\",\"Account_First_Name\",\"Account_Full_Name\",\"Account_ID\",\"Account_Last_Name\",\"Account_Phone\",\"Account_Portal_Status\",\"Account_Status\",\"Alert_Code\",\"Backflow_Gallons\",\"Battery_Level\",\"Billing_Address_Line1\",\"Billing_Address_Line2\",\"Billing_Address_Line3\",\"Billing_City\",\"Billing_Country\",\"Billing_State\",\"Billing_ZIP\",\"Connector_Type\",\"Current_Leak_Rate\",\"Current_Leak_Start_Date\",\"Demand_Zone_ID\",\"Dials\",\"Endpoint_Install_Date\",\"Endpoint_SN\",\"Endpoint_Status\",\"Endpoint_Type\",\"Estimated_Flag\",\"Flow\",\"Flow_Time\",\"Flow_Unit\",\"High_Read_Limit\",\"Last_Comm_Time\",\"Location_Address_Line1\",\"Location_Address_Line2\",\"Location_Address_Line3\",\"Location_Address_Parity\",\"Location_Area\",\"Location_Bathrooms\",\"Location_Building_Number\",\"Location_Building_Type\",\"Location_City\",\"Location_Continuous_Flow\",\"Location_Country\",\"Location_County_Name\",\"Location_DHS_Code\",\"Location_District\",\"Location_Funding\",\"Location_ID\",\"Location_Irrigated_Area\",\"Location_Irrigation\",\"Location_Latitude\",\"Location_Longitude\",\"Location_Main_Use\",\"Location_Name\",\"Location_Pool\",\"Location_Population\",\"Location_Site\",\"Location_State\",\"Location_Water_Type\",\"Location_Year_Built\",\"Location_ZIP\",\"Low_Read_Limit\",\"Meter_Continuous_Flow\",\"Meter_ID\",\"Meter_Install_Date\",\"Meter_Manufacturer\",\"Meter_Model\",\"Meter_Note\",\"Meter_Size\",\"Meter_Size_Desc\",\"Meter_Size_Unit\",\"Meter_SN\",\"Person_ID\",\"Portal_ID\",\"Raw_Read\",\"Read\",\"Read_Code_1\",\"Read_Code_2\",\"Read_Code_3\",\"Read_Method\",\"Read_Note\",\"Read_Sequence\",\"Read_Time\",\"Read_Unit\",\"Reader_Initials\",\"Register_Note\",\"Register_Number\",\"Register_Resolution\",\"Register_Unit_Of_Measure\",\"SA_Start_Date\",\"Service_Point_Class_Code\",\"Service_Point_Class_Code_Normalized\",\"Service_Point_Cycle\",\"Service_Point_ID\",\"Service_Point_Latitude\",\"Service_Point_Longitude\",\"Service_Point_Route\",\"Service_Point_Timezone\",\"Service_Point_Type\",\"Signal_Strength\",\"Supply_Zone_ID\",\"Trouble_Code\",\"Utility_Use_1\",\"Utility_Use_2\"
    \"\",\"krosso@example.com\",\"\",\"church\",\"303022\",\"\",\"N/A\",\"\",\"\",\"0\",\"\",\"good\",\"5391 E. MYSTREET\",\"\",\"\",\"APPLE\",\"US\",\"FL\",\"93727\",\"None\",\"\",\"\",\"\",\"6\",\"2022-12-15 23:59\",\"130615549\",\"Active\",\"J\",\"0\",\"0.0\",\"2024-08-01 00:59\",\"Gallons\",\"240\",\"2025-03-12 16:32\",\"5391 E. MYSTREET\",\"\",\"\",\"O\",\"\",\"\",\"\",\"\",\"Apple\",\"No\",\"US\",\"\",\"\",\"\",\"\",\"303022\",\"\",\"No\",\"36.743388\",\"-119.709351\",\"\",\"303022\",\"\",\"\",\"\",\"CA\",\"\",\"\",\"93727\",\"236\",\"No\",\"1470158170\",\"2016-01-01 23:59\",\"Sensus\",\"T-10\",\"Provisioned 2014-09-11 00:00:49\",\"0.625\",\"5/8\"\"\",\"INCHES\",\"1470158170\",\"\",\"wildfire80@example.com\",\"022760\",\"227.6\",\"\",\"\",\"\",\"Network\",\"\",\"380\",\"2024-08-01 00:59\",\"CCF\",\"\",\"\",\"\",\"1.0\",\"CUBIC_FEET\",\"2016-09-02 12:00\",\"CM\",\"\",\"\",\"WATER1\",\"36.743388\",\"-119.709351\",\"13\",\"US/Pacific\",\"W\",\"good\",\"\",\"\",\"5/8\"\"\",\"\"
    \"\",\"krosso@example.com\",\"\",\"church\",\"303022\",\"\",\"N/A\",\"\",\"\",\"0\",\"\",\"good\",\"5391 E. MYSTREET\",\"\",\"\",\"APPLE\",\"US\",\"FL\",\"93727\",\"None\",\"\",\"\",\"\",\"6\",\"2022-12-15 23:59\",\"130615549\",\"Active\",\"J\",\"0\",\"0.0\",\"2024-08-01 01:59\",\"Gallons\",\"240\",\"2025-03-12 16:32\",\"5391 E. MYSTREET\",\"\",\"\",\"O\",\"\",\"\",\"\",\"\",\"Apple\",\"No\",\"US\",\"\",\"\",\"\",\"\",\"303022\",\"\",\"No\",\"36.743388\",\"-119.709351\",\"\",\"303022\",\"\",\"\",\"\",\"CA\",\"\",\"\",\"93727\",\"236\",\"No\",\"1470158170\",\"2016-01-01 23:59\",\"Sensus\",\"T-10\",\"Provisioned 2014-09-11 00:00:49\",\"0.625\",\"5/8\"\"\",\"INCHES\",\"1470158170\",\"\",\"wildfire80@example.com\",\"022760\",\"227.6\",\"\",\"\",\"\",\"Network\",\"\",\"380\",\"2024-08-01 01:59\",\"CCF\",\"\",\"\",\"\",\"1.0\",\"CUBIC_FEET\",\"2016-09-02 12:00\",\"CM\",\"\",\"\",\"WATER1\",\"36.743388\",\"-119.709351\",\"13\",\"US/Pacific\",\"W\",\"good\",\"\",\"\",\"5/8\"\"\",\"\""""


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
            intermediate_output="output",
            use_cache=False,
            org_id="this-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            configured_sinks=[],
        )
        self.range_start = datetime.datetime(2024, 1, 2, 0, 0)
        self.range_end = datetime.datetime(2024, 1, 3, 0, 0)

    def test_init(self):
        self.assertEqual("output", self.adapter.output_folder)
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

    def test_parse_raw_range_report(self):
        result = self.adapter._parse_raw_range_report(REPORT_CONTENTS_CSV)
        expected = [
            Beacon360MeterAndRead(
                Account_Billing_Cycle='    ""',
                Account_Email="krosso@example.com",
                Account_First_Name="",
                Account_Full_Name="church",
                Account_ID="303022",
                Account_Last_Name="",
                Account_Phone="N/A",
                Account_Portal_Status="",
                Account_Status="",
                Alert_Code="0",
                Backflow_Gallons="",
                Battery_Level="good",
                Billing_Address_Line1="5391 E. MYSTREET",
                Billing_Address_Line2="",
                Billing_Address_Line3="",
                Billing_City="APPLE",
                Billing_Country="US",
                Billing_State="FL",
                Billing_ZIP="93727",
                Connector_Type="None",
                Current_Leak_Rate="",
                Current_Leak_Start_Date="",
                Demand_Zone_ID="",
                Dials="6",
                Endpoint_Install_Date="2022-12-15 23:59",
                Endpoint_SN="130615549",
                Endpoint_Status="Active",
                Endpoint_Type="J",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 00:59",
                Flow_Unit="Gallons",
                High_Read_Limit="240",
                Last_Comm_Time="2025-03-12 16:32",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_Address_Parity="O",
                Location_Area="",
                Location_Bathrooms="",
                Location_Building_Number="",
                Location_Building_Type="",
                Location_City="Apple",
                Location_Continuous_Flow="No",
                Location_Country="US",
                Location_County_Name="",
                Location_DHS_Code="",
                Location_District="",
                Location_Funding="",
                Location_ID="303022",
                Location_Irrigated_Area="",
                Location_Irrigation="No",
                Location_Latitude="36.743388",
                Location_Longitude="-119.709351",
                Location_Main_Use="",
                Location_Name="303022",
                Location_Pool="",
                Location_Population="",
                Location_Site="",
                Location_State="CA",
                Location_Water_Type="",
                Location_Year_Built="",
                Location_ZIP="93727",
                Low_Read_Limit="236",
                Meter_Continuous_Flow="No",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="Sensus",
                Meter_Model="T-10",
                Meter_Note="Provisioned 2014-09-11 00:00:49",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Person_ID="",
                Portal_ID="wildfire80@example.com",
                Raw_Read="022760",
                Read="227.6",
                Read_Code_1="",
                Read_Code_2="",
                Read_Code_3="",
                Read_Method="Network",
                Read_Note="",
                Read_Sequence="380",
                Read_Time="2024-08-01 00:59",
                Read_Unit="CCF",
                Reader_Initials="",
                Register_Note="",
                Register_Number="",
                Register_Resolution="1.0",
                Register_Unit_Of_Measure="CUBIC_FEET",
                SA_Start_Date="2016-09-02 12:00",
                Service_Point_Class_Code="CM",
                Service_Point_Class_Code_Normalized="",
                Service_Point_Cycle="",
                Service_Point_ID="WATER1",
                Service_Point_Latitude="36.743388",
                Service_Point_Longitude="-119.709351",
                Service_Point_Route="13",
                Service_Point_Timezone="US/Pacific",
                Service_Point_Type="W",
                Signal_Strength="good",
                Supply_Zone_ID="",
                Trouble_Code="",
                Utility_Use_1='5/8"',
                Utility_Use_2="",
            ),
            Beacon360MeterAndRead(
                Account_Billing_Cycle='    ""',
                Account_Email="krosso@example.com",
                Account_First_Name="",
                Account_Full_Name="church",
                Account_ID="303022",
                Account_Last_Name="",
                Account_Phone="N/A",
                Account_Portal_Status="",
                Account_Status="",
                Alert_Code="0",
                Backflow_Gallons="",
                Battery_Level="good",
                Billing_Address_Line1="5391 E. MYSTREET",
                Billing_Address_Line2="",
                Billing_Address_Line3="",
                Billing_City="APPLE",
                Billing_Country="US",
                Billing_State="FL",
                Billing_ZIP="93727",
                Connector_Type="None",
                Current_Leak_Rate="",
                Current_Leak_Start_Date="",
                Demand_Zone_ID="",
                Dials="6",
                Endpoint_Install_Date="2022-12-15 23:59",
                Endpoint_SN="130615549",
                Endpoint_Status="Active",
                Endpoint_Type="J",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 01:59",
                Flow_Unit="Gallons",
                High_Read_Limit="240",
                Last_Comm_Time="2025-03-12 16:32",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_Address_Parity="O",
                Location_Area="",
                Location_Bathrooms="",
                Location_Building_Number="",
                Location_Building_Type="",
                Location_City="Apple",
                Location_Continuous_Flow="No",
                Location_Country="US",
                Location_County_Name="",
                Location_DHS_Code="",
                Location_District="",
                Location_Funding="",
                Location_ID="303022",
                Location_Irrigated_Area="",
                Location_Irrigation="No",
                Location_Latitude="36.743388",
                Location_Longitude="-119.709351",
                Location_Main_Use="",
                Location_Name="303022",
                Location_Pool="",
                Location_Population="",
                Location_Site="",
                Location_State="CA",
                Location_Water_Type="",
                Location_Year_Built="",
                Location_ZIP="93727",
                Low_Read_Limit="236",
                Meter_Continuous_Flow="No",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="Sensus",
                Meter_Model="T-10",
                Meter_Note="Provisioned 2014-09-11 00:00:49",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Person_ID="",
                Portal_ID="wildfire80@example.com",
                Raw_Read="022760",
                Read="227.6",
                Read_Code_1="",
                Read_Code_2="",
                Read_Code_3="",
                Read_Method="Network",
                Read_Note="",
                Read_Sequence="380",
                Read_Time="2024-08-01 01:59",
                Read_Unit="CCF",
                Reader_Initials="",
                Register_Note="",
                Register_Number="",
                Register_Resolution="1.0",
                Register_Unit_Of_Measure="CUBIC_FEET",
                SA_Start_Date="2016-09-02 12:00",
                Service_Point_Class_Code="CM",
                Service_Point_Class_Code_Normalized="",
                Service_Point_Cycle="",
                Service_Point_ID="WATER1",
                Service_Point_Latitude="36.743388",
                Service_Point_Longitude="-119.709351",
                Service_Point_Route="13",
                Service_Point_Timezone="US/Pacific",
                Service_Point_Type="W",
                Signal_Strength="good",
                Supply_Zone_ID="",
                Trouble_Code="",
                Utility_Use_1='5/8"',
                Utility_Use_2="",
            ),
        ]
        self.assertEqual(expected, result)

    def test_transform_meters_and_reads(self):
        raw_meters_with_reads = [
            Beacon360MeterAndRead(
                Account_Billing_Cycle='    ""',
                Account_Email="krosso@example.com",
                Account_First_Name="",
                Account_Full_Name="church",
                Account_ID="303022",
                Account_Last_Name="",
                Account_Phone="N/A",
                Account_Portal_Status="",
                Account_Status="",
                Alert_Code="0",
                Backflow_Gallons="",
                Battery_Level="good",
                Billing_Address_Line1="5391 E. MYSTREET",
                Billing_Address_Line2="",
                Billing_Address_Line3="",
                Billing_City="APPLE",
                Billing_Country="US",
                Billing_State="FL",
                Billing_ZIP="93727",
                Connector_Type="None",
                Current_Leak_Rate="",
                Current_Leak_Start_Date="",
                Demand_Zone_ID="",
                Dials="6",
                Endpoint_Install_Date="2022-12-15 23:59",
                Endpoint_SN="130615549",
                Endpoint_Status="Active",
                Endpoint_Type="J",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 00:59",
                Flow_Unit="Gallons",
                High_Read_Limit="240",
                Last_Comm_Time="2025-03-12 16:32",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_Address_Parity="O",
                Location_Area="",
                Location_Bathrooms="",
                Location_Building_Number="",
                Location_Building_Type="",
                Location_City="Apple",
                Location_Continuous_Flow="No",
                Location_Country="US",
                Location_County_Name="",
                Location_DHS_Code="",
                Location_District="",
                Location_Funding="",
                Location_ID="303022",
                Location_Irrigated_Area="",
                Location_Irrigation="No",
                Location_Latitude="36.743388",
                Location_Longitude="-119.709351",
                Location_Main_Use="",
                Location_Name="303022",
                Location_Pool="",
                Location_Population="",
                Location_Site="",
                Location_State="CA",
                Location_Water_Type="",
                Location_Year_Built="",
                Location_ZIP="93727",
                Low_Read_Limit="236",
                Meter_Continuous_Flow="No",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="Sensus",
                Meter_Model="T-10",
                Meter_Note="Provisioned 2014-09-11 00:00:49",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Person_ID="",
                Portal_ID="wildfire80@example.com",
                Raw_Read="022760",
                Read="227.6",
                Read_Code_1="",
                Read_Code_2="",
                Read_Code_3="",
                Read_Method="Network",
                Read_Note="",
                Read_Sequence="380",
                Read_Time="2024-08-01 00:59",
                Read_Unit="CCF",
                Reader_Initials="",
                Register_Note="",
                Register_Number="",
                Register_Resolution="1.0",
                Register_Unit_Of_Measure="CUBIC_FEET",
                SA_Start_Date="2016-09-02 12:00",
                Service_Point_Class_Code="CM",
                Service_Point_Class_Code_Normalized="",
                Service_Point_Cycle="",
                Service_Point_ID="WATER1",
                Service_Point_Latitude="36.743388",
                Service_Point_Longitude="-119.709351",
                Service_Point_Route="13",
                Service_Point_Timezone="US/Pacific",
                Service_Point_Type="W",
                Signal_Strength="good",
                Supply_Zone_ID="",
                Trouble_Code="",
                Utility_Use_1='5/8"',
                Utility_Use_2="",
            ),
            Beacon360MeterAndRead(
                Account_Billing_Cycle='    ""',
                Account_Email="krosso@example.com",
                Account_First_Name="",
                Account_Full_Name="church",
                Account_ID="303022",
                Account_Last_Name="",
                Account_Phone="N/A",
                Account_Portal_Status="",
                Account_Status="",
                Alert_Code="0",
                Backflow_Gallons="",
                Battery_Level="good",
                Billing_Address_Line1="5391 E. MYSTREET",
                Billing_Address_Line2="",
                Billing_Address_Line3="",
                Billing_City="APPLE",
                Billing_Country="US",
                Billing_State="FL",
                Billing_ZIP="93727",
                Connector_Type="None",
                Current_Leak_Rate="",
                Current_Leak_Start_Date="",
                Demand_Zone_ID="",
                Dials="6",
                Endpoint_Install_Date="2022-12-15 23:59",
                Endpoint_SN="130615549",
                Endpoint_Status="Active",
                Endpoint_Type="J",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 01:59",
                Flow_Unit="Gallons",
                High_Read_Limit="240",
                Last_Comm_Time="2025-03-12 16:32",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_Address_Parity="O",
                Location_Area="",
                Location_Bathrooms="",
                Location_Building_Number="",
                Location_Building_Type="",
                Location_City="Apple",
                Location_Continuous_Flow="No",
                Location_Country="US",
                Location_County_Name="",
                Location_DHS_Code="",
                Location_District="",
                Location_Funding="",
                Location_ID="303022",
                Location_Irrigated_Area="",
                Location_Irrigation="No",
                Location_Latitude="36.743388",
                Location_Longitude="-119.709351",
                Location_Main_Use="",
                Location_Name="303022",
                Location_Pool="",
                Location_Population="",
                Location_Site="",
                Location_State="CA",
                Location_Water_Type="",
                Location_Year_Built="",
                Location_ZIP="93727",
                Low_Read_Limit="236",
                Meter_Continuous_Flow="No",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="Sensus",
                Meter_Model="T-10",
                Meter_Note="Provisioned 2014-09-11 00:00:49",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Person_ID="",
                Portal_ID="wildfire80@example.com",
                Raw_Read="022760",
                Read="227.6",
                Read_Code_1="",
                Read_Code_2="",
                Read_Code_3="",
                Read_Method="Network",
                Read_Note="",
                Read_Sequence="380",
                Read_Time="2024-08-01 01:59",
                Read_Unit="CCF",
                Reader_Initials="",
                Register_Note="",
                Register_Number="",
                Register_Resolution="1.0",
                Register_Unit_Of_Measure="CUBIC_FEET",
                SA_Start_Date="2016-09-02 12:00",
                Service_Point_Class_Code="CM",
                Service_Point_Class_Code_Normalized="",
                Service_Point_Cycle="",
                Service_Point_ID="WATER1",
                Service_Point_Latitude="36.743388",
                Service_Point_Longitude="-119.709351",
                Service_Point_Route="13",
                Service_Point_Timezone="US/Pacific",
                Service_Point_Type="W",
                Signal_Strength="good",
                Supply_Zone_ID="",
                Trouble_Code="",
                Utility_Use_1='5/8"',
                Utility_Use_2="",
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )

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
                meter_manufacturer="Sensus",
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
                Account_Billing_Cycle='    ""',
                Account_Email="krosso@example.com",
                Account_First_Name="",
                Account_Full_Name="church",
                Account_ID="303022",
                Account_Last_Name="",
                Account_Phone="N/A",
                Account_Portal_Status="",
                Account_Status="",
                Alert_Code="0",
                Backflow_Gallons="",
                Battery_Level="good",
                Billing_Address_Line1="5391 E. MYSTREET",
                Billing_Address_Line2="",
                Billing_Address_Line3="",
                Billing_City="APPLE",
                Billing_Country="US",
                Billing_State="FL",
                Billing_ZIP="93727",
                Connector_Type="None",
                Current_Leak_Rate="",
                Current_Leak_Start_Date="",
                Demand_Zone_ID="",
                Dials="6",
                Endpoint_Install_Date="2022-12-15 23:59",
                Endpoint_SN="130615549",
                Endpoint_Status="Active",
                Endpoint_Type="J",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 00:59",
                Flow_Unit="Gallons",
                High_Read_Limit="240",
                Last_Comm_Time="2025-03-12 16:32",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_Address_Parity="O",
                Location_Area="",
                Location_Bathrooms="",
                Location_Building_Number="",
                Location_Building_Type="",
                Location_City="Apple",
                Location_Continuous_Flow="No",
                Location_Country="US",
                Location_County_Name="",
                Location_DHS_Code="",
                Location_District="",
                Location_Funding="",
                Location_ID="303022",
                Location_Irrigated_Area="",
                Location_Irrigation="No",
                Location_Latitude="36.743388",
                Location_Longitude="-119.709351",
                Location_Main_Use="",
                Location_Name="303022",
                Location_Pool="",
                Location_Population="",
                Location_Site="",
                Location_State="CA",
                Location_Water_Type="",
                Location_Year_Built="",
                Location_ZIP="93727",
                Low_Read_Limit="236",
                Meter_Continuous_Flow="No",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="Sensus",
                Meter_Model="T-10",
                Meter_Note="Provisioned 2014-09-11 00:00:49",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Person_ID="",
                Portal_ID="wildfire80@example.com",
                Raw_Read="022760",
                Read="227.6",
                Read_Code_1="",
                Read_Code_2="",
                Read_Code_3="",
                Read_Method="Network",
                Read_Note="",
                Read_Sequence="380",
                Read_Time="2024-08-01 00:59",
                Read_Unit="CCF",
                Reader_Initials="",
                Register_Note="",
                Register_Number="",
                Register_Resolution="1.0",
                Register_Unit_Of_Measure="CUBIC_FEET",
                SA_Start_Date="2016-09-02 12:00",
                Service_Point_Class_Code="CM",
                Service_Point_Class_Code_Normalized="",
                Service_Point_Cycle="",
                Service_Point_ID="WATER1",
                Service_Point_Latitude="36.743388",
                Service_Point_Longitude="-119.709351",
                Service_Point_Route="13",
                Service_Point_Timezone="US/Pacific",
                Service_Point_Type="W",
                Signal_Strength="good",
                Supply_Zone_ID="",
                Trouble_Code="",
                Utility_Use_1='5/8"',
                Utility_Use_2="",
            ),
            Beacon360MeterAndRead(
                Account_Billing_Cycle='    ""',
                Account_Email="krosso@example.com",
                Account_First_Name="",
                Account_Full_Name="church",
                Account_ID="1",
                Account_Last_Name="",
                Account_Phone="N/A",
                Account_Portal_Status="",
                Account_Status="",
                Alert_Code="0",
                Backflow_Gallons="",
                Battery_Level="good",
                Billing_Address_Line1="5391 E. MYSTREET",
                Billing_Address_Line2="",
                Billing_Address_Line3="",
                Billing_City="APPLE",
                Billing_Country="US",
                Billing_State="FL",
                Billing_ZIP="93727",
                Connector_Type="None",
                Current_Leak_Rate="",
                Current_Leak_Start_Date="",
                Demand_Zone_ID="",
                Dials="6",
                Endpoint_Install_Date="2022-12-15 23:59",
                Endpoint_SN="130615549",
                Endpoint_Status="Active",
                Endpoint_Type="J",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 01:59",
                Flow_Unit="Gallons",
                High_Read_Limit="240",
                Last_Comm_Time="2025-03-12 16:32",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_Address_Parity="O",
                Location_Area="",
                Location_Bathrooms="",
                Location_Building_Number="",
                Location_Building_Type="",
                Location_City="Apple",
                Location_Continuous_Flow="No",
                Location_Country="US",
                Location_County_Name="",
                Location_DHS_Code="",
                Location_District="",
                Location_Funding="",
                Location_ID="303022",
                Location_Irrigated_Area="",
                Location_Irrigation="No",
                Location_Latitude="36.743388",
                Location_Longitude="-119.709351",
                Location_Main_Use="",
                Location_Name="303022",
                Location_Pool="",
                Location_Population="",
                Location_Site="",
                Location_State="CA",
                Location_Water_Type="",
                Location_Year_Built="",
                Location_ZIP="93727",
                Low_Read_Limit="236",
                Meter_Continuous_Flow="No",
                Meter_ID="10101",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="Sensus",
                Meter_Model="T-10",
                Meter_Note="Provisioned 2014-09-11 00:00:49",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Person_ID="",
                Portal_ID="wildfire80@example.com",
                Raw_Read="022760",
                Read="227.6",
                Read_Code_1="",
                Read_Code_2="",
                Read_Code_3="",
                Read_Method="Network",
                Read_Note="",
                Read_Sequence="380",
                Read_Time="2024-08-01 01:59",
                Read_Unit="CCF",
                Reader_Initials="",
                Register_Note="",
                Register_Number="",
                Register_Resolution="1.0",
                Register_Unit_Of_Measure="CUBIC_FEET",
                SA_Start_Date="2016-09-02 12:00",
                Service_Point_Class_Code="CM",
                Service_Point_Class_Code_Normalized="",
                Service_Point_Cycle="",
                Service_Point_ID="WATER1",
                Service_Point_Latitude="36.743388",
                Service_Point_Longitude="-119.709351",
                Service_Point_Route="13",
                Service_Point_Timezone="US/Pacific",
                Service_Point_Type="W",
                Signal_Strength="good",
                Supply_Zone_ID="",
                Trouble_Code="",
                Utility_Use_1='5/8"',
                Utility_Use_2="",
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )

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
                meter_manufacturer="Sensus",
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
                meter_manufacturer="Sensus",
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
        ]
        self.assertListEqual(expected_reads, transformed_reads)

    def test_transform_meters_and_reads__ignores_reads_when_date_missing(self):
        raw_meters_with_reads = [
            # Read_Time is None
            Beacon360MeterAndRead(
                Account_Billing_Cycle='    ""',
                Account_Email="krosso@example.com",
                Account_First_Name="",
                Account_Full_Name="church",
                Account_ID="303022",
                Account_Last_Name="",
                Account_Phone="N/A",
                Account_Portal_Status="",
                Account_Status="",
                Alert_Code="0",
                Backflow_Gallons="",
                Battery_Level="good",
                Billing_Address_Line1="5391 E. MYSTREET",
                Billing_Address_Line2="",
                Billing_Address_Line3="",
                Billing_City="APPLE",
                Billing_Country="US",
                Billing_State="FL",
                Billing_ZIP="93727",
                Connector_Type="None",
                Current_Leak_Rate="",
                Current_Leak_Start_Date="",
                Demand_Zone_ID="",
                Dials="6",
                Endpoint_Install_Date="2022-12-15 23:59",
                Endpoint_SN="130615549",
                Endpoint_Status="Active",
                Endpoint_Type="J",
                Estimated_Flag="0",
                Flow="0.0",
                Flow_Time="2024-08-01 00:59",
                Flow_Unit="Gallons",
                High_Read_Limit="240",
                Last_Comm_Time="2025-03-12 16:32",
                Location_Address_Line1="5391 E. MYSTREET",
                Location_Address_Line2="",
                Location_Address_Line3="",
                Location_Address_Parity="O",
                Location_Area="",
                Location_Bathrooms="",
                Location_Building_Number="",
                Location_Building_Type="",
                Location_City="Apple",
                Location_Continuous_Flow="No",
                Location_Country="US",
                Location_County_Name="",
                Location_DHS_Code="",
                Location_District="",
                Location_Funding="",
                Location_ID="303022",
                Location_Irrigated_Area="",
                Location_Irrigation="No",
                Location_Latitude="36.743388",
                Location_Longitude="-119.709351",
                Location_Main_Use="",
                Location_Name="303022",
                Location_Pool="",
                Location_Population="",
                Location_Site="",
                Location_State="CA",
                Location_Water_Type="",
                Location_Year_Built="",
                Location_ZIP="93727",
                Low_Read_Limit="236",
                Meter_Continuous_Flow="No",
                Meter_ID="1470158170",
                Meter_Install_Date="2016-01-01 23:59",
                Meter_Manufacturer="Sensus",
                Meter_Model="T-10",
                Meter_Note="Provisioned 2014-09-11 00:00:49",
                Meter_Size="0.625",
                Meter_Size_Desc='5/8"',
                Meter_Size_Unit="INCHES",
                Meter_SN="1470158170",
                Person_ID="",
                Portal_ID="wildfire80@example.com",
                Raw_Read="022760",
                Read="227.6",
                Read_Code_1="",
                Read_Code_2="",
                Read_Code_3="",
                Read_Method="Network",
                Read_Note="",
                Read_Sequence="380",
                Read_Time=None,
                Read_Unit="CCF",
                Reader_Initials="",
                Register_Note="",
                Register_Number="",
                Register_Resolution="1.0",
                Register_Unit_Of_Measure="CUBIC_FEET",
                SA_Start_Date="2016-09-02 12:00",
                Service_Point_Class_Code="CM",
                Service_Point_Class_Code_Normalized="",
                Service_Point_Cycle="",
                Service_Point_ID="WATER1",
                Service_Point_Latitude="36.743388",
                Service_Point_Longitude="-119.709351",
                Service_Point_Route="13",
                Service_Point_Timezone="US/Pacific",
                Service_Point_Type="W",
                Signal_Strength="good",
                Supply_Zone_ID="",
                Trouble_Code="",
                Utility_Use_1='5/8"',
                Utility_Use_2="",
            ),
        ]
        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(raw_meters_with_reads)
        )

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
                meter_manufacturer="Sensus",
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

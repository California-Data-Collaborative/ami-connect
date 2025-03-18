import datetime
from unittest import mock, TestCase

from amiadapters.base import GeneralMeter, GeneralMeterRead
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.beacon import Beacon360Adapter, Beacon360MeterAndRead, REQUESTED_COLUMNS


class MockResponse:
        def __init__(self, json_data, status_code, text=None):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text

        def json(self):
            return self.json_data


REPORT_CONTENTS_CSV = """
    \"Account_Billing_Cycle\",\"Account_Email\",\"Account_First_Name\",\"Account_Full_Name\",\"Account_ID\",\"Account_Last_Name\",\"Account_Phone\",\"Account_Portal_Status\",\"Account_Status\",\"Alert_Code\",\"Backflow_Gallons\",\"Battery_Level\",\"Billing_Address_Line1\",\"Billing_Address_Line2\",\"Billing_Address_Line3\",\"Billing_City\",\"Billing_Country\",\"Billing_State\",\"Billing_ZIP\",\"Connector_Type\",\"Current_Leak_Rate\",\"Current_Leak_Start_Date\",\"Demand_Zone_ID\",\"Dials\",\"Endpoint_Install_Date\",\"Endpoint_SN\",\"Endpoint_Status\",\"Endpoint_Type\",\"Estimated_Flag\",\"Flow\",\"Flow_Time\",\"Flow_Unit\",\"High_Read_Limit\",\"Last_Comm_Time\",\"Location_Address_Line1\",\"Location_Address_Line2\",\"Location_Address_Line3\",\"Location_Address_Parity\",\"Location_Area\",\"Location_Bathrooms\",\"Location_Building_Number\",\"Location_Building_Type\",\"Location_City\",\"Location_Continuous_Flow\",\"Location_Country\",\"Location_County_Name\",\"Location_DHS_Code\",\"Location_District\",\"Location_Funding\",\"Location_ID\",\"Location_Irrigated_Area\",\"Location_Irrigation\",\"Location_Latitude\",\"Location_Longitude\",\"Location_Main_Use\",\"Location_Name\",\"Location_Pool\",\"Location_Population\",\"Location_Site\",\"Location_State\",\"Location_Water_Type\",\"Location_Year_Built\",\"Location_ZIP\",\"Low_Read_Limit\",\"Meter_Continuous_Flow\",\"Meter_ID\",\"Meter_Install_Date\",\"Meter_Manufacturer\",\"Meter_Model\",\"Meter_Note\",\"Meter_Size\",\"Meter_Size_Desc\",\"Meter_Size_Unit\",\"Meter_SN\",\"Person_ID\",\"Portal_ID\",\"Raw_Read\",\"Read\",\"Read_Code_1\",\"Read_Code_2\",\"Read_Code_3\",\"Read_Method\",\"Read_Note\",\"Read_Sequence\",\"Read_Time\",\"Read_Unit\",\"Reader_Initials\",\"Register_Note\",\"Register_Number\",\"Register_Resolution\",\"Register_Unit_Of_Measure\",\"SA_Start_Date\",\"Service_Point_Class_Code\",\"Service_Point_Class_Code_Normalized\",\"Service_Point_Cycle\",\"Service_Point_ID\",\"Service_Point_Latitude\",\"Service_Point_Longitude\",\"Service_Point_Route\",\"Service_Point_Timezone\",\"Service_Point_Type\",\"Signal_Strength\",\"Supply_Zone_ID\",\"Trouble_Code\",\"Utility_Use_1\",\"Utility_Use_2\"
    \"\",\"krosso@example.com\",\"\",\"church\",\"303022\",\"\",\"N/A\",\"\",\"\",\"0\",\"\",\"good\",\"5391 E. MYSTREET\",\"\",\"\",\"APPLE\",\"US\",\"FL\",\"93727\",\"None\",\"\",\"\",\"\",\"6\",\"2022-12-15 23:59\",\"130615549\",\"Active\",\"J\",\"0\",\"0.0\",\"2024-08-01 00:59\",\"Gallons\",\"240\",\"2025-03-12 16:32\",\"5391 E. MYSTREET\",\"\",\"\",\"O\",\"\",\"\",\"\",\"\",\"Apple\",\"No\",\"US\",\"\",\"\",\"\",\"\",\"303022\",\"\",\"No\",\"36.743388\",\"-119.709351\",\"\",\"303022\",\"\",\"\",\"\",\"CA\",\"\",\"\",\"93727\",\"236\",\"No\",\"1470158170\",\"2016-01-01 23:59\",\"Sensus\",\"T-10\",\"Provisioned 2014-09-11 00:00:49\",\"0.625\",\"5/8\"\"\",\"INCHES\",\"1470158170\",\"\",\"wildfire80@example.com\",\"022760\",\"227.6\",\"\",\"\",\"\",\"Network\",\"\",\"380\",\"2024-08-01 00:59\",\"CCF\",\"\",\"\",\"\",\"1.0\",\"CUBIC_FEET\",\"2016-09-02 12:00\",\"CM\",\"\",\"\",\"WATER1\",\"36.743388\",\"-119.709351\",\"13\",\"US/Pacific\",\"W\",\"good\",\"\",\"\",\"5/8\"\"\",\"\"
    \"\",\"krosso@example.com\",\"\",\"church\",\"303022\",\"\",\"N/A\",\"\",\"\",\"0\",\"\",\"good\",\"5391 E. MYSTREET\",\"\",\"\",\"APPLE\",\"US\",\"FL\",\"93727\",\"None\",\"\",\"\",\"\",\"6\",\"2022-12-15 23:59\",\"130615549\",\"Active\",\"J\",\"0\",\"0.0\",\"2024-08-01 01:59\",\"Gallons\",\"240\",\"2025-03-12 16:32\",\"5391 E. MYSTREET\",\"\",\"\",\"O\",\"\",\"\",\"\",\"\",\"Apple\",\"No\",\"US\",\"\",\"\",\"\",\"\",\"303022\",\"\",\"No\",\"36.743388\",\"-119.709351\",\"\",\"303022\",\"\",\"\",\"\",\"CA\",\"\",\"\",\"93727\",\"236\",\"No\",\"1470158170\",\"2016-01-01 23:59\",\"Sensus\",\"T-10\",\"Provisioned 2014-09-11 00:00:49\",\"0.625\",\"5/8\"\"\",\"INCHES\",\"1470158170\",\"\",\"wildfire80@example.com\",\"022760\",\"227.6\",\"\",\"\",\"\",\"Network\",\"\",\"380\",\"2024-08-01 01:59\",\"CCF\",\"\",\"\",\"\",\"1.0\",\"CUBIC_FEET\",\"2016-09-02 12:00\",\"CM\",\"\",\"\",\"WATER1\",\"36.743388\",\"-119.709351\",\"13\",\"US/Pacific\",\"W\",\"good\",\"\",\"\",\"5/8\"\"\",\"\"
"""


def mocked_create_range_report(*args, **kwargs):
    data = {
        "edsUUID": "acecd48e8b794f49a61c2b96c9ff9118",
        "statusUrl": "/v2/eds/status/acecd48e8b794f49a61c2b96c9ff9118"
    }
    return MockResponse(data, 202)


def mocked_get_range_report_status_not_finished(*args, **kwargs):
    data = {
        "message": "acecd48e8b794f49a61c2b96c9ff9118 operation running.",
        "progress": {
            "percentComplete": 5.0
        },
        "queueTime": "2025-03-18T18:52:41Z",
        "state": "run"
    }
    return MockResponse(data, 200)


def mocked_get_range_report_status_finished(*args, **kwargs):
    data = {
        "endTime": "2025-03-18T18:56:14Z",
        "lastMeterId": "800380.1",
        "message": "acecd48e8b794f49a61c2b96c9ff9118 operation succeeded",
        "queueTime": "2025-03-18T18:52:41Z",
        "reportUrl": "/v1/content/8021910600018033071/users/4928669927440453458/export90868",
        "state": "done"
    }
    return MockResponse(data, 200)


def mocked_get_report_from_link(*args, **kwargs):
    return MockResponse(None, 200, text=REPORT_CONTENTS_CSV)


def mocked_exception_from_status_check(*args, **kwargs):
    data = {
        "state": "exception"
    }
    return MockResponse(data, 200)


def mocked_response_500(*args, **kwargs):
    return MockResponse({}, 500)


def mocked_response_429(*args, **kwargs):
    data = {
        "args": [
            None, None, 3
        ]
    }
    return MockResponse(data, 429)


def mocked_get_consumption_response_last_page(*args, **kwargs):
    data = {
        "meters": [],
        "currentPage": 2,
        "itemsOnPage": 0,
        "totalCount": 1
    }
    return MockResponse(data, 200)


class TestBeacon360Adapter(TestCase):

    def setUp(self):
        config = AMIAdapterConfiguration(
            output_folder="output",
            beacon_360_user="user",
            beacon_360_password="pass",
        )
        self.adapter = Beacon360Adapter(config)
        self.adapter.use_cache = False

    def test_init(self):
        self.assertEqual("output", self.adapter.output_folder)
        self.assertEqual("user", self.adapter.user)
        self.assertEqual("pass", self.adapter.password)
        self.assertEqual("beacon-360-api", self.adapter.name())
    
    @mock.patch('requests.get')
    @mock.patch('requests.post')
    def test_fetch_range_report__uses_cache(self, mock_post, mock_get):
        self.adapter.use_cache = True
        self.adapter._get_cached_report = mock.MagicMock(return_value=REPORT_CONTENTS_CSV)

        result = self.adapter._fetch_range_report()
        self.assertEqual(REPORT_CONTENTS_CSV, result)
        self.assertEqual(0, mock_get.call_count)
        self.assertEqual(0, mock_post.call_count)
    
    @mock.patch('requests.get', side_effect=[mocked_get_range_report_status_not_finished(), mocked_get_range_report_status_finished(), mocked_get_report_from_link()])
    @mock.patch('requests.post', side_effect=[mocked_create_range_report()])
    @mock.patch('time.sleep')
    def test_fetch_range_report__can_fetch_report_from_api(self, mock_sleep, mock_post, mock_get):
        result = self.adapter._fetch_range_report()
        self.assertEqual(REPORT_CONTENTS_CSV, result)
        
        self.assertEqual(1, len(mock_post.call_args_list))
        generater_report_request = mock_post.call_args_list[0]
        self.assertEqual('https://api.beaconama.net/v2/eds/range', generater_report_request.kwargs['url'])
        self.assertEqual(",".join(REQUESTED_COLUMNS), generater_report_request.kwargs['params']['Header_Columns'])
        self.assertEqual(datetime.datetime(2024, 8, 1, 0, 0), generater_report_request.kwargs['params']['Start_Date'])
        self.assertEqual(datetime.datetime(2024, 8, 2, 0, 0), generater_report_request.kwargs['params']['End_Date'])
        self.assertTrue(generater_report_request.kwargs['params']['Has_Endpoint'])
        self.assertEqual('hourly', generater_report_request.kwargs['params']['Resolution'])
        self.assertEqual({'Content-Type': 'application/x-www-form-urlencoded'}, generater_report_request.kwargs['headers'])

        self.assertEqual(3, len(mock_get.call_args_list))
        self.assertIn('https://api.beaconama.net/v2/eds/status/', mock_get.call_args_list[0].kwargs['url'])
        self.assertIn('https://api.beaconama.net/v2/eds/status/', mock_get.call_args_list[1].kwargs['url'])
        self.assertIn('https://api.beaconama.net/v1/content/', mock_get.call_args_list[2].kwargs['url'])

        self.assertEqual(1, mock_sleep.call_count)

    @mock.patch('requests.get', side_effect=[mocked_get_range_report_status_not_finished(), mocked_get_range_report_status_finished(), mocked_get_report_from_link()])
    @mock.patch('requests.post', side_effect=[mocked_response_429()])
    @mock.patch('time.sleep')
    def test_fetch_range_report__throws_exception_when_rate_limit_exceeded_when_report_generated(self, mock_sleep, mock_post, mock_get):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report()

        self.assertTrue("Rate limit exceeded" in str(context.exception))
    
    @mock.patch('requests.get', side_effect=[])
    @mock.patch('requests.post', side_effect=[mocked_response_500()])
    @mock.patch('time.sleep')
    def test_fetch_range_report__throws_exception_when_non_202_from_report_generation(self, mock_sleep, mock_post, mock_get):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report()

        self.assertTrue("Failed request to generate report" in str(context.exception))
    
    @mock.patch('requests.get', side_effect=[mocked_response_500()])
    @mock.patch('requests.post', side_effect=[mocked_create_range_report()])
    @mock.patch('time.sleep')
    def test_fetch_range_report__throws_exception_when_status_response_non_200(self, mock_sleep, mock_post, mock_get):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report()

        self.assertTrue("Failed request to get report status" in str(context.exception))
    
    @mock.patch('requests.get', side_effect=[mocked_exception_from_status_check()])
    @mock.patch('requests.post', side_effect=[mocked_create_range_report()])
    @mock.patch('time.sleep')
    def test_fetch_range_report__throws_exception_when_status_response_indicates_exception(self, mock_sleep, mock_post, mock_get):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report()

        self.assertTrue("Exception found in report status" in str(context.exception))
    
    # Mock the status call response as "not finished" twenty times
    @mock.patch('requests.get', side_effect=[mocked_get_range_report_status_not_finished()]*20)
    @mock.patch('requests.post', side_effect=[mocked_create_range_report()])
    @mock.patch('time.sleep')
    def test_fetch_range_report__throws_exception_when_max_attempts_reached_while_polling_for_status(self, mock_sleep, mock_post, mock_get):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report()

        self.assertTrue("Reached max attempts" in str(context.exception))
    
    @mock.patch('requests.get', side_effect=[mocked_get_range_report_status_finished(), Exception, mocked_get_report_from_link()])
    @mock.patch('requests.post', side_effect=[mocked_create_range_report()])
    @mock.patch('time.sleep')
    def test_fetch_range_report__retries_once_when_fetch_report_throws_exception(self, mock_sleep, mock_post, mock_get):
        result = self.adapter._fetch_range_report()
        self.assertEqual(REPORT_CONTENTS_CSV, result)
        self.assertEqual(1, mock_sleep.call_count)
    
    @mock.patch('requests.get', side_effect=[mocked_get_range_report_status_finished(), mocked_response_500()])
    @mock.patch('requests.post', side_effect=[mocked_create_range_report()])
    @mock.patch('time.sleep')
    def test_fetch_range_report__throws_exception_when_fetch_report_returns_non_200(self, mock_sleep, mock_post, mock_get):
        with self.assertRaises(Exception) as context:
            self.adapter._fetch_range_report()

        self.assertTrue("Failed request to download report" in str(context.exception))

    

import datetime
import pytz
from unittest import mock

from amiadapters.base import GeneralMeter, GeneralMeterRead
from amiadapters.sentryx import (
    SentryxAdapter,
    SentryxMeter,
    SentryxMeterRead,
    SentryxMeterWithReads,
)

from test.base_test_case import BaseTestCase


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


def mocked_get_devices_response_first_page(*args, **kwargs):
    data = {
        "meters": [
            {
                "dmaObjectId": None,
                "dmaName": None,
                "deviceId": 654419700,
                "isDisconnectableDevice": False,
                "serviceStatus": "NotRDM",
                "deviceStatus": "OK",
                "street": "10 SW MYROAD RD",
                "city": "Town",
                "state": None,
                "zip": "10101",
                "description": "Mueller Systems SSR Ext-3/4(CF)-Pit  Plastic   Positive Displacement ",
                "manufacturer": None,
                "installNotes": "",
                "lastReadingDateTime": "2025-03-10T06:00:00",
                "accountId": "1",
                "lastBilledDate": None,
                "lastBilledRead": None,
                "lastReading": 7787.0900000000001,
                "units": "Unknown",
                "meterSize": '3/4"',
                "socketId": "131216BB00200",
                "billingCycle": 1,
                "firstName": "BOB",
                "lastName": "",
                "email": None,
                "dials": None,
                "billableDials": None,
                "multiplier": 0.01,
                "isReclaimed": False,
                "dComId": 6544197,
                "port": 1,
                "installDate": "2022-02-08T20:16:41",
                "unbilledConsumption": None,
                "installerName": "installer",
                "installerEmail": None,
                "route": "1",
                "lateral": "",
                "hasAlerts": False,
                "alertText": None,
                "activeAlerts": [],
                "productType": "MiNodeM",
                "powerLevel": None,
                "groupNames": None,
                "isInput": False,
                "isOutput": False,
                "taskType": "",
                "extSSR": True,
                "isGeneric": False,
                "muellerSerialNumber": "6544197",
                "registerSerialNumber": "70598457",
                "bodySerialNumber": "70598457",
                "batteryPlan": None,
                "edrxStartTime": None,
                "edrxEndTime": None,
                "cellularOnDemandReadScheduled": False,
            },
        ],
        "currentPage": 1,
        "itemsOnPage": 1,
        "totalCount": 1,
    }
    return MockResponse(data, 200)


def mocked_get_devices_response_last_page(*args, **kwargs):
    data = {"meters": [], "currentPage": 2, "itemsOnPage": 0, "totalCount": 1}
    return MockResponse(data, 200)


def mocked_response_500(*args, **kwargs):
    return MockResponse({}, 500)


def mocked_get_consumption_response_first_page(*args, **kwargs):
    data = {
        "meters": [
            {
                "deviceId": 1,
                "bodySerialNumber": "61853840",
                "muellerSerialNumber": "6023318",
                "registerSerialNumber": "61853840",
                "units": "CF",
                "data": [
                    {
                        "timeStamp": "2024-07-07T01:00:00",
                        "reading": 116233.61,
                        "consumption": 0,
                    }
                ],
            },
            {
                "deviceId": 2,
                "bodySerialNumber": "61853840",
                "muellerSerialNumber": "6023318",
                "registerSerialNumber": "61853840",
                "units": "CF",
                "data": [
                    {
                        "timeStamp": "2024-07-08T01:00:00",
                        "reading": 22.61,
                        "consumption": 0,
                    }
                ],
            },
        ],
        "currentPage": 1,
        "itemsOnPage": 2,
        "totalCount": 2,
    }
    return MockResponse(data, 200)


def mocked_get_consumption_response_last_page(*args, **kwargs):
    data = {"meters": [], "currentPage": 2, "itemsOnPage": 0, "totalCount": 1}
    return MockResponse(data, 200)


class TestSentryxAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = SentryxAdapter(
            intermediate_output="output",
            api_key="key",
            org_id="this-utility",
            org_timezone=pytz.timezone("Africa/Algiers"),
            utility_name="my-utility-name",
        )

    def test_init(self):
        self.assertEqual("output", self.adapter.output_folder)
        self.assertEqual("key", self.adapter.api_key)
        self.assertEqual("this-utility", self.adapter.org_id)
        self.assertEqual(pytz.timezone("Africa/Algiers"), self.adapter.org_timezone)
        self.assertEqual("my-utility-name", self.adapter.utility_name)
        self.assertEqual("sentryx-api-this-utility", self.adapter.name())

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_devices_response_first_page(),
            mocked_get_devices_response_last_page(),
        ],
    )
    def test_extract_all_meters(self, mock_get):
        result = self.adapter._extract_all_meters()
        self.assertEqual(1, len(result))
        meter = result[0]
        self.assertEqual("1", meter.account_id)
        self.assertEqual(654419700, meter.device_id)
        self.assertEqual("OK", meter.device_status)
        self.assertEqual("NotRDM", meter.service_status)
        self.assertEqual("10 SW MYROAD RD", meter.street)
        self.assertEqual("Town", meter.city)
        self.assertEqual(None, meter.state)
        self.assertEqual("10101", meter.zip)
        self.assertEqual(
            "Mueller Systems SSR Ext-3/4(CF)-Pit  Plastic   Positive Displacement ",
            meter.description,
        )
        self.assertEqual(None, meter.manufacturer)
        self.assertEqual("", meter.install_notes)
        self.assertEqual("2022-02-08T20:16:41", meter.install_date)
        self.assertEqual('3/4"', meter.meter_size)

        calls = [
            mock.call(
                "https://api.sentryx.io/v1-wm/sites/my-utility-name/devices",
                headers={"Authorization": "key"},
                params={"pager.skip": 0, "pager.take": 25},
            ),
            mock.call(
                "https://api.sentryx.io/v1-wm/sites/my-utility-name/devices",
                headers={"Authorization": "key"},
                params={"pager.skip": 1, "pager.take": 25},
            ),
        ]
        self.assertListEqual(calls, mock_get.call_args_list)

    @mock.patch(
        "requests.get",
        side_effect=[mocked_get_devices_response_first_page(), mocked_response_500()],
    )
    def test_extract_all_meters__non_200_status_code(self, mock_get):
        result = self.adapter._extract_all_meters()
        self.assertEqual(0, len(result))

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_consumption_response_first_page(),
            mocked_get_consumption_response_last_page(),
        ],
    )
    def test_extract_consumption_for_all_meters(self, mock_get):
        result = self.adapter._extract_consumption_for_all_meters()
        expected = [
            SentryxMeterWithReads(
                device_id=1,
                units="CF",
                data=[
                    SentryxMeterRead(
                        time_stamp="2024-07-07T01:00:00", reading=116233.61
                    )
                ],
            ),
            SentryxMeterWithReads(
                device_id=2,
                units="CF",
                data=[
                    SentryxMeterRead(time_stamp="2024-07-08T01:00:00", reading=22.61)
                ],
            ),
        ]
        self.assertListEqual(expected, result)

        calls = [
            mock.call(
                "https://api.sentryx.io/v1-wm/sites/my-utility-name/devices/consumption",
                headers={"Authorization": "key"},
                params={
                    "skip": 0,
                    "take": 25,
                    "StartDate": "2025-03-09T11:02:26.011959",
                    "EndDate": "2025-03-11T11:02:26.011959",
                },
            ),
            mock.call(
                "https://api.sentryx.io/v1-wm/sites/my-utility-name/devices/consumption",
                headers={"Authorization": "key"},
                params={
                    "skip": 2,
                    "take": 25,
                    "StartDate": "2025-03-09T11:02:26.011959",
                    "EndDate": "2025-03-11T11:02:26.011959",
                },
            ),
        ]
        self.assertListEqual(calls, mock_get.call_args_list)

    @mock.patch(
        "requests.get",
        side_effect=[
            mocked_get_consumption_response_first_page(),
            mocked_response_500(),
        ],
    )
    def test_extract_consumption_for_all_meters__non_200_response(self, mock_get):
        result = self.adapter._extract_consumption_for_all_meters()
        self.assertEqual(0, len(result))

    def test_transform_meters_and_reads(self):
        meters = [
            SentryxMeter(
                device_id=1,
                account_id="101",
                meter_size='3/8"',
                device_status=None,
                service_status=None,
                street="my street",
                city="my town",
                state="CA",
                zip="12312",
                description=None,
                manufacturer="manufacturer",
                install_notes=None,
                install_date="2022-02-08T22:10:43",
            )
        ]
        reads = [
            SentryxMeterWithReads(
                device_id=1,
                units="CF",
                data=[
                    SentryxMeterRead(
                        time_stamp="2024-07-07T01:00:00", reading=116233.61
                    )
                ],
            ),
            SentryxMeterWithReads(
                device_id=2,
                units="CF",
                data=[SentryxMeterRead(time_stamp="2024-07-07T01:00:00", reading=11)],
            ),
        ]

        transformed_meters, transformed_reads = (
            self.adapter._transform_meters_and_reads(meters, reads)
        )

        expected_meters = [
            GeneralMeter(
                org_id="this-utility",
                device_id="1",
                account_id="101",
                location_id=None,
                meter_id="1",
                endpoint_id=None,
                meter_install_date=datetime.datetime(2022, 2, 8, 22, 10, 43),
                meter_size="0.375",
                meter_manufacturer="manufacturer",
                multiplier=None,
                location_address="my street",
                location_state="CA",
                location_zip="12312",
            ),
        ]

        expected_reads = [
            GeneralMeterRead(
                org_id="this-utility",
                device_id="1",
                account_id="101",
                location_id=None,
                flowtime=datetime.datetime(2024, 7, 7, 1, 0),
                register_value=116233.61,
                register_unit="CF",
                interval_value=None,
                interval_unit=None,
            ),
            GeneralMeterRead(
                org_id="this-utility",
                device_id="2",
                account_id=None,
                location_id=None,
                flowtime=datetime.datetime(2024, 7, 7, 1, 0),
                register_value=11,
                register_unit="CF",
                interval_value=None,
                interval_unit=None,
            ),
        ]

        self.assertListEqual(expected_meters, transformed_meters)
        self.assertListEqual(expected_reads, transformed_reads)


class TestSentryxMeterWithReads(BaseTestCase):

    def test_from_json(self):
        json_str = """
        {
            "device_id": 601133200,
            "units": "CF",
            "data": [
                {
                    "time_stamp": "2024-07-07T01:00:00",
                    "reading": 35828
                }
            ]
         }
        """
        meter_with_reads = SentryxMeterWithReads.from_json(json_str)
        self.assertEqual(601133200, meter_with_reads.device_id)
        self.assertEqual("CF", meter_with_reads.units)
        self.assertEqual(1, len(meter_with_reads.data))
        self.assertEqual(35828, meter_with_reads.data[0].reading)
        self.assertEqual("2024-07-07T01:00:00", meter_with_reads.data[0].time_stamp)

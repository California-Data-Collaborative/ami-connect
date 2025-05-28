from datetime import datetime
from io import StringIO
import json
import pytz
from unittest.mock import MagicMock, mock_open, patch

from amiadapters.models import DataclassJSONEncoder
from amiadapters.config import ConfiguredLocalTaskOutputController
from amiadapters.aclara import AclaraAdapter, AclaraMeterAndRead, files_for_date_range

from test.base_test_case import BaseTestCase


class TestAclaraAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = AclaraAdapter(
            org_id="this-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            sftp_host="example.com",
            sftp_user="user",
            sftp_password="pw",
            sftp_meter_and_reads_folder="/remote",
            local_download_directory="/tmp/downloads",
            local_known_hosts_file="/tmp/known",
            configured_task_output_controller=ConfiguredLocalTaskOutputController(
                "/tmp/output"
            ),
            configured_sinks=[],
        )
        self.range_start = datetime(2024, 1, 2, 0, 0)
        self.range_end = datetime(2024, 1, 3, 0, 0)

    def meter_and_read_factory(
        self, account_type: str = "Residential"
    ) -> AclaraMeterAndRead:
        return AclaraMeterAndRead(
            AccountNumber="17305709",
            MeterSN="1",
            MTUID="2",
            Port="1",
            AccountType=account_type,
            Address1="12 MY LN",
            City="LOS ANGELES",
            State="CA",
            Zip="00000",
            RawRead="23497071",
            ScaledRead="023497.071",
            ReadingTime="2025-05-25 16:00:00.000",
            LocalTime="2025-05-25 09:00:00.000",
            Active="1",
            Scalar="0.001",
            MeterTypeID="2212",
            Vendor="BADGER",
            Model="HR-E LCD",
            Description="Badger M25/LP HRE LCD 5/8x3/4in 9D 0.001CuFt",
            ReadInterval="60",
        )

    @patch("os.makedirs")
    @patch("os.path.exists")
    @patch("amiadapters.aclara.files_for_date_range")
    def test_downloads_new_files(
        self, mock_files_for_date_range, mock_exists, mock_makedirs
    ):
        sftp_mock = MagicMock()
        sftp_mock.listdir.return_value = ["2024-01-01.csv", "2024-01-02.csv"]
        mock_files_for_date_range.return_value = ["2024-01-01.csv", "2024-01-02.csv"]
        mock_exists.return_value = False  # Pretend files do not exist locally

        # Run
        result = self.adapter._download_meter_and_read_files_for_date_range(
            sftp_mock, datetime(2024, 1, 1), datetime(2024, 1, 2)
        )

        # Assert
        expected_paths = [
            "/tmp/downloads/2024-01-01.csv",
            "/tmp/downloads/2024-01-02.csv",
        ]
        self.assertEqual(result, expected_paths)
        self.assertEqual(sftp_mock.get.call_count, 2)
        sftp_mock.get.assert_any_call(
            "/remote/2024-01-01.csv", "/tmp/downloads/2024-01-01.csv"
        )

    @patch("os.makedirs")
    @patch("os.path.exists")
    @patch("amiadapters.aclara.files_for_date_range")
    def test_skips_existing_files(
        self, mock_files_for_date_range, mock_exists, mock_makedirs
    ):
        sftp_mock = MagicMock()
        sftp_mock.listdir.return_value = ["2024-01-01.csv"]
        mock_files_for_date_range.return_value = ["2024-01-01.csv"]
        mock_exists.return_value = True  # Pretend file already exists

        result = self.adapter._download_meter_and_read_files_for_date_range(
            sftp_mock, datetime(2024, 1, 1), datetime(2024, 1, 1)
        )

        self.assertEqual(result, ["/tmp/downloads/2024-01-01.csv"])
        sftp_mock.get.assert_not_called()

    @patch("builtins.open", new_callable=mock_open)
    def test_parse_downloaded_files(self, mock_file):
        mock_csv_content = (
            "AccountNumber,MeterSN,MTUID,Port,AccountType,Address1,City,State,Zip,"
            "RawRead,ScaledRead,ReadingTime,LocalTime,Active,Scalar,MeterTypeID,Vendor,Model,Description,ReadInterval\n"
            "123,456789,MTU001,1,Residential,123 Main St,Anytown,CA,90210,"
            "1000,120.5,2025-05-25 16:00:00.000,2025-05-25 09:00:00.000,1,1.0,5,VendorA,ModelX,Desc,15\n"
        )
        # Simulate the content of the opened file
        mock_file.return_value = StringIO(mock_csv_content)

        # Run the parser
        files = ["mock_file.csv"]
        result = list(self.adapter._parse_downloaded_files(files))

        # Parse the expected object to JSON for comparison
        expected_obj = AclaraMeterAndRead(
            AccountNumber="123",
            MeterSN="456789",
            MTUID="MTU001",
            Port="1",
            AccountType="Residential",
            Address1="123 Main St",
            City="Anytown",
            State="CA",
            Zip="90210",
            RawRead="1000",
            ScaledRead="120.5",
            ReadingTime="2025-05-25 16:00:00.000",
            LocalTime="2025-05-25 09:00:00.000",
            Active="1",
            Scalar="1.0",
            MeterTypeID="5",
            Vendor="VendorA",
            Model="ModelX",
            Description="Desc",
            ReadInterval="15",
        )
        expected_json = json.dumps(expected_obj, cls=DataclassJSONEncoder)
        self.assertEqual(result, [expected_json])
        mock_file.assert_called_once_with("mock_file.csv", newline="", encoding="utf-8")

    @patch("builtins.open", new_callable=mock_open)
    def test_parse_downloaded_files_opens_multiple_files(self, mock_file):
        # Run on two files
        files = ["mock_file_1.csv", "mock_file_2.csv"]
        list(self.adapter._parse_downloaded_files(files))
        self.assertEqual(2, mock_file.call_count)

    def test_transforms_valid_records(self):
        input_data = [self.meter_and_read_factory()]

        meters, reads = self.adapter._transform_meters_and_reads(input_data)

        self.assertEqual(len(meters), 1)
        self.assertEqual(len(reads), 1)

        meter = list(meters)[0]
        read = reads[0]

        self.assertEqual(meter.org_id, "this-org")
        self.assertEqual(read.device_id, "2")
        self.assertEqual(read.register_value, 23497071)

    def test_skips_detector_check_account_type(self):
        input_data = [self.meter_and_read_factory(account_type="DETECTOR CHECK")]

        meters, reads = self.adapter._transform_meters_and_reads(input_data)
        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 0)


class TestFilesForDateRange(BaseTestCase):
    def setUp(self):
        self.start_date = datetime(2024, 5, 5)
        self.end_date = datetime(2024, 5, 7)

    def test_files_in_range(self):
        files = [
            "CaDC_Readings_05052024.csv",
            "CaDC_Readings_05062024.csv",
            "CaDC_Readings_05072024.csv",
        ]
        result = files_for_date_range(files, self.start_date, self.end_date)
        self.assertEqual(len(result), 3)
        self.assertIn("CaDC_Readings_05062024.csv", result)

    def test_files_out_of_range(self):
        files = [
            "CaDC_Readings_05042024.csv",
            "CaDC_Readings_05082024.csv",
        ]
        result = files_for_date_range(files, self.start_date, self.end_date)
        self.assertEqual(result, [])

    def test_invalid_filenames(self):
        files = [
            "CaDC_Readings_invalid.csv",
            "randomfile.csv",
            "CaDC_Readings_20240506.csv",  # wrong format
        ]
        result = files_for_date_range(files, self.start_date, self.end_date)
        self.assertEqual(result, [])

    def test_partial_valid_and_invalid(self):
        files = [
            "CaDC_Readings_05062024.csv",
            "bad_file.csv",
            "CaDC_Readings_05072024.csv",
        ]
        result = files_for_date_range(files, self.start_date, self.end_date)
        self.assertEqual(
            result, ["CaDC_Readings_05062024.csv", "CaDC_Readings_05072024.csv"]
        )

    def test_boundary_dates_inclusive(self):
        files = [
            "CaDC_Readings_05052024.csv",  # start boundary
            "CaDC_Readings_05072024.csv",  # end boundary
        ]
        result = files_for_date_range(files, self.start_date, self.end_date)
        self.assertEqual(result, files)

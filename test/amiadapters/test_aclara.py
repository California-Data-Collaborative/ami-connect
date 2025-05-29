from datetime import datetime
from io import StringIO
import json
from unittest.mock import MagicMock, mock_open, patch

import pytz

from amiadapters.models import DataclassJSONEncoder
from amiadapters.config import ConfiguredLocalTaskOutputController, ConfiguredSftp
from amiadapters.aclara import AclaraAdapter, AclaraMeterAndRead, files_for_date_range
from test.base_test_case import BaseTestCase


class TestAclaraAdapter(BaseTestCase):

    def setUp(self):
        self.adapter = AclaraAdapter(
            org_id="this-org",
            org_timezone=pytz.timezone("Europe/Rome"),
            configured_sftp=ConfiguredSftp(
                host="example.com",
                remote_data_directory="/remote",
                local_download_directory="/tmp/downloads",
                local_known_hosts_file="/tmp/known",
            ),
            sftp_user="user",
            sftp_password="pw",
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
            "1000,120.5,2025-05-25 16:00:00.000,2025-05-25 09:00:00.000,1,1.0,5,VendorA,ModelX,Badger M25/LP HRE LCD 5/8x3/4in 9D 0.001CuFt,15\n"
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
            Description="Badger M25/LP HRE LCD 5/8x3/4in 9D 0.001CuFt",
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
        self.assertEqual(meter.meter_id, "1")
        self.assertEqual(meter.endpoint_id, "2")
        self.assertEqual(meter.meter_size, "0.625x0.75")
        self.assertEqual(read.device_id, "1")
        self.assertEqual(read.register_value, 23497.071)

    def test_skips_detector_check_account_type(self):
        input_data = [self.meter_and_read_factory(account_type="DETECTOR CHECK")]

        meters, reads = self.adapter._transform_meters_and_reads(input_data)
        self.assertEqual(len(meters), 0)
        self.assertEqual(len(reads), 0)

    def test_parse_meter_size_from_description(self):
        cases = [
            ('Sensus W2000 6" 8D 1CuFt', "6"),
            ("Badger HRE LCD T450 3in 7D 1CuFt", "3"),
            ('Badger Ultrasonic 4" 9D 0.01CuFt', "4"),
            ("Elster evoQ4 3in 8D 1CuFt", "3"),
            ("Elster evoQ4 6in 8D 1CuFt", "6"),
            ("M35 Badger HR-E LCD 3/4in 9D 0.001Cu.Ft.", "0.75"),
            ('SENSUS OMNI C2 3" 7D 1CuFt', "3"),
            ("Elster evoQ4 4in 8D 1CuFt", "4"),
            ("Badger HR E-Series 1.5in 9D 0.01Cu.Ft. DD", "1.5"),
            ("M170 Badger HR-E LCD 2in 9D 0.01Cu.Ft.", "2"),
            ("M120 Badger HR-E LCD 1.5in 9D 0.01Cu.Ft.", "1.5"),
            ('Badger G2 3" 9D .01 Cu.Ft.', "3"),
            ("SENSUS OMNI C2 1.5  7D 1CuFt", "1.5"),
            ("Badger HR E-Series 3/4in 9D 0.001Cu.Ft. DD", "0.75"),
            ("M70 Badger HR-E LCD 1in 9D 0.001Cu.Ft.", "1"),
            ('SENSUS OMNI C2 2"  7D 1CuFt', "2"),
            ('SENSUS OMNI T2 2" 7D 1CuFt', "2"),
            ("Sensus SRII/aS E-Register 3/4 6D 1CuFt", "0.75"),
            ("Badger HR E-Series 1in 9D 0.001Cu.Ft. DD", "1"),
            ("Badger HR E-Series 2in 9D 0.01CuFt", "2"),
            ("Badger HR-E LCD LP/M25 5/8x3/4in 9D 0.001Cu.Ft. DD", "0.625x0.75"),
            ("Badger M170 HRE LCD 2in 9D 0.01CuFt", "2"),
            ("Badger HR E-Series 1.5 Inch 9D 0.01CuFt", "1.5"),
            ("Badger M120 HRE LCD 1.5in 9D 0.01CuFt", "1.5"),
            ("Badger HR E-Series 5/8in 9D 0.001Cu.Ft. DD", "0.625"),
            ("Badger HRE E-Series 3/4in 9D 0.001CuFt", "0.75"),
            ("Sensus SRII/aS E-Register 5/8x3/4 6D 1CuFt", "0.625x0.75"),
            ("Badger M35 HRE LCD 3/4in 9D 0.001CuFt", "0.75"),
            ("Badger M40/M55/M70 HRE LCD 1in 9D 0.001CuFt", "1"),
            ("Badger HRE E-Series 1in 9D 0.001CuFt", "1"),
            ("Sensus SRII/aS E-Register 1 6D 1CuFt", "1"),
            ("Badger HRE E-Series 5/8in 9D 0.001CuFt", "0.625"),
            ("Badger M25/LP HRE LCD 5/8x3/4in 9D 0.001CuFt", "0.625x0.75"),
        ]
        for description, expected in cases:
            result = self.adapter.parse_meter_size_from_description(description)
            self.assertEqual(expected, result, f"Invalid mapping for: {description}")


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

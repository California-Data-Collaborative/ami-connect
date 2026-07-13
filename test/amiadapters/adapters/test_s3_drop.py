import re
from datetime import datetime
from unittest.mock import MagicMock

import pytz

from amiadapters.adapters.s3_drop import (
    date_range_from_filename,
    download_csv_rows,
    list_drop_files,
    select_files_for_range,
)
from test.base_test_case import BaseTestCase

# A generic two-type pattern, structured like a real adapter's (named groups
# "type" and "dates") but deliberately not any specific utility's.
PATTERN = re.compile(
    r"(?P<type>register|interval)_(?P<dates>\d{6}|\d{8}_\d{8})\.csv$",
    re.IGNORECASE,
)

LM = datetime(2026, 7, 1, tzinfo=pytz.UTC)


class TestListDropFiles(BaseTestCase):

    def test_lists_top_level_keys_with_last_modified(self):
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "p/a.csv", "LastModified": LM}]},
            {"Contents": [{"Key": "p/b.csv", "LastModified": LM}]},
        ]
        s3.get_paginator.return_value = paginator

        result = list_drop_files(s3, "bucket", "p/")

        self.assertEqual({"p/a.csv": LM, "p/b.csv": LM}, result)
        paginator.paginate.assert_called_once_with(
            Bucket="bucket", Prefix="p/", Delimiter="/"
        )

    def test_empty_prefix(self):
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{}]
        s3.get_paginator.return_value = paginator
        self.assertEqual({}, list_drop_files(s3, "bucket", "p/"))


class TestSelectFilesForRange(BaseTestCase):

    def test_month_suffix_overlaps(self):
        files = {"p/agency_Register_202607.csv": LM}
        result = select_files_for_range(
            files, PATTERN, datetime(2026, 7, 10), datetime(2026, 7, 12)
        )
        self.assertEqual(list(files), result["register"])

    def test_month_suffix_outside_range(self):
        files = {"p/agency_Register_202605.csv": LM}
        result = select_files_for_range(
            files, PATTERN, datetime(2026, 7, 10), datetime(2026, 7, 12)
        )
        self.assertEqual({}, result)

    def test_date_range_suffix(self):
        cases = [
            # (filename dates, range start, range end, expect match)
            ("20260616_20260619", datetime(2026, 6, 18), datetime(2026, 6, 20), True),
            ("20260616_20260619", datetime(2026, 6, 19), datetime(2026, 6, 21), True),
            # Boundary-inclusive: a file named ..._20260619 can carry reads
            # stamped 06/20 00:00 (hour-ending timestamps), so a range starting
            # exactly at 06/20 matches.
            ("20260616_20260619", datetime(2026, 6, 20), datetime(2026, 6, 22), True),
            ("20260616_20260619", datetime(2026, 6, 21), datetime(2026, 6, 23), False),
            ("20260616_20260619", datetime(2026, 6, 14), datetime(2026, 6, 15), False),
            ("20260616_20260619", datetime(2026, 6, 14), datetime(2026, 6, 16), True),
        ]
        for dates, start, end, expected in cases:
            files = {f"p/agency_Interval_{dates}.csv": LM}
            result = select_files_for_range(files, PATTERN, start, end)
            self.assertEqual(
                expected,
                len(result.get("interval", [])) == 1,
                f"unexpected result for {dates} in [{start}, {end}]",
            )

    def test_routes_types_and_skips_unrecognized(self):
        files = {
            "p/agency_Register_202607.csv": LM,
            "p/agency_Interval_202607.csv": LM,
            "p/testAddress.csv": LM,
            "p/agency_usage_202606.csv": LM,
        }
        result = select_files_for_range(
            files, PATTERN, datetime(2026, 7, 1), datetime(2026, 7, 3)
        )
        self.assertEqual(["p/agency_Register_202607.csv"], result["register"])
        self.assertEqual(["p/agency_Interval_202607.csv"], result["interval"])

    def test_case_insensitive(self):
        files = {"p/agency_REGISTER_202607.csv": LM}
        result = select_files_for_range(
            files, PATTERN, datetime(2026, 7, 1), datetime(2026, 7, 3)
        )
        self.assertEqual(list(files), result["register"])

    def test_december_month_suffix(self):
        files = {"p/agency_Register_202612.csv": LM}
        result = select_files_for_range(
            files, PATTERN, datetime(2026, 12, 30), datetime(2027, 1, 2)
        )
        self.assertEqual(list(files), result["register"])

    def test_timezone_aware_range_bounds_do_not_crash(self):
        # Manual Airflow runs can deliver offset-bearing datetimes
        files = {"p/agency_Register_20260616_20260619.csv": LM}
        result = select_files_for_range(
            files,
            PATTERN,
            datetime(2026, 6, 17, tzinfo=pytz.UTC),
            datetime(2026, 6, 19, tzinfo=pytz.UTC),
        )
        self.assertEqual(list(files), result["register"])

    def test_orders_by_last_modified_not_name(self):
        older = "p/agency_Register_20260617_20260620.csv"
        newer = "p/agency_Register_20260616_20260619.csv"
        files = {
            older: datetime(2026, 6, 20, tzinfo=pytz.UTC),
            newer: datetime(2026, 6, 21, tzinfo=pytz.UTC),
        }
        result = select_files_for_range(
            files, PATTERN, datetime(2026, 6, 16), datetime(2026, 6, 20)
        )
        self.assertEqual([older, newer], result["register"])


class TestDateRangeFromFilename(BaseTestCase):

    def test_month(self):
        start, end = date_range_from_filename("202607")
        self.assertEqual(datetime(2026, 7, 1), start)
        self.assertEqual(datetime(2026, 8, 1), end)

    def test_december_rollover(self):
        start, end = date_range_from_filename("202612")
        self.assertEqual(datetime(2026, 12, 1), start)
        self.assertEqual(datetime(2027, 1, 1), end)

    def test_day_range(self):
        start, end = date_range_from_filename("20260616_20260619")
        self.assertEqual(datetime(2026, 6, 16), start)
        self.assertEqual(datetime(2026, 6, 20), end)


class TestDownloadCsvRows(BaseTestCase):

    def _s3_returning(self, text: str):
        s3 = MagicMock()
        body = MagicMock()
        body.read.return_value = text.encode("utf-8")
        s3.get_object.return_value = {"Body": body}
        return s3

    def test_parses_rows_into_dataclass(self):
        from dataclasses import dataclass

        @dataclass
        class Row:
            A: str
            B: str

        s3 = self._s3_returning("A,B\n1,2\n3,4")
        rows = download_csv_rows(s3, "bucket", "p/f.csv", Row)
        self.assertEqual([Row(A="1", B="2"), Row(A="3", B="4")], rows)
        s3.get_object.assert_called_once_with(Bucket="bucket", Key="p/f.csv")

    def test_strips_utf8_bom(self):
        from dataclasses import dataclass

        @dataclass
        class Row:
            A: str
            B: str

        s3 = self._s3_returning("\ufeffA,B\n1,2")
        rows = download_csv_rows(s3, "bucket", "p/f.csv", Row)
        self.assertEqual([Row(A="1", B="2")], rows)

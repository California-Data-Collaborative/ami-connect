"""
Behavior test for irrigation detection and leak and irrigation event building, on made-up meters.

Loads a few meters' hourly readings into a scratch schema in a separate test database, runs the
detection procedure from sql/irrigation_detection_flowtime.sql and the post-processor's leak and
irrigation steps on them, and checks event times and lengths: each hour keeps its real moment
(UTC-stored and Pacific-stored meters, the daylight-saving fall-back hour), events that cross the
seam between two post-processor windows stay whole, and leaks are not stacked.

Runs in GitHub's checks (.github/workflows/snowflake_behavior_tests.yml) as the test user
AMI_CONNECT_CI_SVC, whose role owns the test database CADC_AMI_CI and can use the production
detection function CADC_AMI.WAVELET.DETECT_IRRIGATION_V4, and nothing else in CADC_AMI.
The default unittest discovery (test*.py) skips this file.

Usage:
    AMI_CONNECT__SNOWFLAKE_CI_ACCOUNT=<account> \\
    AMI_CONNECT__SNOWFLAKE_CI_PRIVATE_KEY="$(cat <key.p8>)" \\
    python -m test.integration.snowflake_postprocessor_behavior_test

Optional: AMI_CONNECT__SNOWFLAKE_CI_USER, _ROLE, _WAREHOUSE, _DATABASE (defaults below) and
AMI_CONNECT__SNOWFLAKE_CI_RUN_ID (names the scratch schema; the workflow sets the GitHub run id) and
AMI_CONNECT__SNOWFLAKE_CI_KEEP=1 (keeps the scratch schema after the run, to inspect a failure).
"""

import datetime
import getpass
import logging
import os
import re
import unittest

import pytz

from amiadapters.configuration.base import create_snowflake_connection
from amiadapters.configuration.models import ConfiguredStorageSink, SnowflakeSecrets
from amiadapters.metrics.base import NOOP_METRICS
from amiadapters.storage.snowflake import SnowflakeStorageSink

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENV = "AMI_CONNECT__SNOWFLAKE_CI_"
DEFAULTS = {
    "USER": "AMI_CONNECT_CI_SVC",
    "ROLE": "AMI_CONNECT_CI_ROLE",
    "WAREHOUSE": "AMI_CONNECT_CI_WH",
    "DATABASE": "CADC_AMI_CI",
}

ORG = "cadc_int_test"
OVERRIDE = 9.0
UTC = pytz.UTC
PACIFIC = pytz.timezone("America/Los_Angeles")

# The fixture: every hour from 2026-10-14 00:00 UTC to 2026-11-23 23:00 UTC (41 days). It contains the
# 2026-11-01 fall-back hour and the seam between the two post-processor windows below.
FIRST_HOUR = datetime.datetime(2026, 10, 14, 0, tzinfo=UTC)
LAST_HOUR = datetime.datetime(2026, 11, 23, 23, tzinfo=UTC)
FIRST_WINDOW = (
    datetime.datetime(2026, 10, 15, tzinfo=UTC),
    datetime.datetime(2026, 11, 13, tzinfo=UTC),
)
SECOND_WINDOW = (
    datetime.datetime(2026, 11, 14, tzinfo=UTC),
    datetime.datetime(2026, 11, 23, tzinfo=UTC),
)

# The detector's rule the values rely on (DETECT_IRRIGATION_V4): the baseline is the 10th percentile of a
# meter's nonzero readings; an hour is a spike when it is at least 3 times the baseline and at least
# baseline + 3 above it; a spike whose jump reaches the district override (9.0) is irrigation in any month.
# So a steady 1.0 CF with 20.0 CF hours classifies the 20.0 hours as irrigation and nothing else.
STEADY = 1.0
WATERING = 20.0


def _in(instant, start, end):
    return start <= instant <= end


def _seam_run(clock_tz, start_hour):
    """A 6-hour watering run that crosses midnight between 11/13 and 11/14 in the meter's own clock."""
    start = clock_tz.localize(datetime.datetime(2026, 11, 13, start_hour))
    return start, start + datetime.timedelta(hours=5)


# 22:00 to 03:00 UTC, and 21:00 to 02:00 Pacific, so neither run touches the meter's regular
# watering at 12:00 and 13:00 UTC (4 and 5 AM Pacific standard time) and each stays its own event
UTC_SEAM = _seam_run(UTC, 22)
PACIFIC_SEAM = _seam_run(PACIFIC, 21)
LEAK_ACROSS_WINDOW = (
    PACIFIC.localize(datetime.datetime(2026, 11, 12, 12)),
    PACIFIC.localize(datetime.datetime(2026, 11, 15, 11)),
)
LEAK_TWO_WINDOWS = (
    datetime.datetime(2026, 10, 16, 0, tzinfo=UTC),
    datetime.datetime(2026, 11, 20, 23, tzinfo=UTC),
)


def _irrigates(seam):
    def value(instant):
        if instant.hour in (12, 13) or _in(instant, *seam):  # instant is in UTC here
            return WATERING
        return STEADY

    return value


# device id -> (stored offset, value for each UTC hour)
METERS = {
    "utc-irrigates": (UTC, _irrigates(UTC_SEAM)),
    "pacific-irrigates": (PACIFIC, _irrigates(PACIFIC_SEAM)),
    "leak-across-window": (
        PACIFIC,
        lambda instant: 1.0 if _in(instant, *LEAK_ACROSS_WINDOW) else 0.0,
    ),
    "leak-two-windows": (
        UTC,
        lambda instant: 0.5 if _in(instant, *LEAK_TWO_WINDOWS) else 0.0,
    ),
    "missing-flowtime": (PACIFIC, _irrigates(PACIFIC_SEAM)),
    "quiet": (UTC, lambda instant: 0.0),
}

# Detection rows of missing-flowtime whose FLOWTIME the test removes: 150 hours inside the first window,
# about 3.5% of that window's detection rows for the org, above the post-processor's 1% guard.
MISSING_FROM = "2026-10-20 00:00"
MISSING_TO = "2026-10-26 06:00"


def _fixture_rows():
    """(device id, the reading's moment as ISO text with its stored offset, interval value) for every hour."""
    rows = []
    hour = FIRST_HOUR
    while hour <= LAST_HOUR:
        for device_id, (stored_tz, value) in METERS.items():
            rows.append(
                (device_id, hour.astimezone(stored_tz).isoformat(), value(hour))
            )
        hour += datetime.timedelta(hours=1)
    return rows


def _insert_readings(cursor, rows):
    """
    Insert the fixture readings with each moment written out as text with its offset, so the stored offset
    is exactly the one an adapter would store (UTC or Pacific). Binding timezone-aware datetimes through the
    Python connector is not used: depending on its version and the batch size, it stores them in UTC or
    reinterprets them in the session time zone.
    """
    for i in range(0, len(rows), 500):
        values = ",\n".join(
            f"('{ORG}', '{d}', 'account-{d}', 'location-{d}', '{t}', {v}, 'CF', 0)"
            for d, t, v in rows[i : i + 500]
        )
        cursor.execute(f"""
            INSERT INTO READINGS (ORG_ID, DEVICE_ID, ACCOUNT_ID, LOCATION_ID, FLOWTIME, INTERVAL_VALUE, INTERVAL_UNIT, ESTIMATED)
            SELECT column1, column2, column3, column4,
                   TO_TIMESTAMP_TZ(column5, 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'), column6, column7, column8
            FROM VALUES {values}
        """)


def _read_sql(name):
    with open(os.path.join(REPO_ROOT, "sql", name)) as f:
        return f.read()


def _procedure_under_test(qualified_schema):
    """
    The detection procedure from sql/irrigation_detection_flowtime.sql, pointed at the scratch tables
    and given one district, the test org.
    """
    sql = _read_sql("irrigation_detection_flowtime.sql")
    start = sql.index("CREATE OR REPLACE PROCEDURE")
    end = sql.index("$$;", start) + len("$$;")
    procedure = sql[start:end]
    procedure = procedure.replace(
        "CADC_AMI.WAVELET.RUN_ALL_IRRIGATION",
        f"{qualified_schema}.RUN_ALL_IRRIGATION",
    )
    procedure = procedure.replace(
        "CADC_AMI.WAVELET.GLOBAL_IRRIGATION_DETECTION",
        f"{qualified_schema}.GLOBAL_IRRIGATION_DETECTION",
    )
    procedure = procedure.replace(
        "CADC_AMI.AMI_CONNECT.READINGS", f"{qualified_schema}.READINGS"
    )
    procedure, replaced = re.subn(
        r"(WITH district_overrides AS \()(.*?)(\),\s*\n\s*watermarks AS \()",
        lambda m: m.group(1)
        + f"\n            SELECT '{ORG}' AS org, {OVERRIDE}::FLOAT AS override\n        "
        + m.group(3),
        procedure,
        count=1,
        flags=re.DOTALL,
    )
    if replaced != 1:
        raise AssertionError(
            "sql/irrigation_detection_flowtime.sql no longer has the lines "
            "'WITH district_overrides AS (' and '),' + 'watermarks AS (' around the district list"
        )
    if "CADC_AMI.WAVELET.DETECT_IRRIGATION_V4" not in procedure:
        raise AssertionError(
            "the procedure no longer calls CADC_AMI.WAVELET.DETECT_IRRIGATION_V4"
        )
    return procedure


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


class SnowflakePostprocessorBehaviorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        settings = {}
        for name in ("ACCOUNT", "PRIVATE_KEY"):
            settings[name] = os.environ.get(ENV + name)
            if not settings[name]:
                raise AssertionError(f"{ENV}{name} is not set")
        for name, default in DEFAULTS.items():
            settings[name] = os.environ.get(ENV + name) or default
        run_id = os.environ.get(ENV + "RUN_ID") or f"LOCAL_{getpass.getuser()}"
        schema = "RUN_" + re.sub(r"[^A-Za-z0-9_]", "_", run_id).upper()
        cls.qualified_schema = f"{settings['DATABASE']}.{schema}"

        def connect(schema_name):
            conn = create_snowflake_connection(
                account=settings["ACCOUNT"],
                user=settings["USER"],
                ssh_key=settings["PRIVATE_KEY"],
                warehouse=settings["WAREHOUSE"],
                database=settings["DATABASE"],
                schema=schema_name,
                role=settings["ROLE"],
            )
            # The production procedure runs under America/Los_Angeles; fix it so the watermark
            # arithmetic is the same on every machine
            conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'America/Los_Angeles'")
            return conn

        admin = connect(None)
        admin.cursor().execute(
            f"CREATE OR REPLACE TRANSIENT SCHEMA {cls.qualified_schema} DATA_RETENTION_TIME_IN_DAYS = 0"
        )
        cls.addClassCleanup(admin.close)
        # AMI_CONNECT__SNOWFLAKE_CI_KEEP=1 leaves the scratch schema in place to inspect a failure
        if os.environ.get(ENV + "KEEP") != "1":
            cls.addClassCleanup(
                lambda: admin.cursor().execute(
                    f"DROP SCHEMA IF EXISTS {cls.qualified_schema} CASCADE"
                )
            )

        cls.conn = connect(schema)
        cls.addClassCleanup(cls.conn.close)
        cursor = cls.conn.cursor()
        cursor.execute(_read_sql("readings.sql"))
        cursor.execute(_read_sql("global_irrigation_detection.sql"))
        cursor.execute(_procedure_under_test(cls.qualified_schema))

        cls.sink = SnowflakeStorageSink(
            ORG,
            UTC,
            ConfiguredStorageSink(
                type="snowflake",
                id="ci",
                secrets=SnowflakeSecrets(
                    account=settings["ACCOUNT"],
                    user=settings["USER"],
                    role=settings["ROLE"],
                    warehouse=settings["WAREHOUSE"],
                    database=settings["DATABASE"],
                    schema=schema,
                    ssh_key=settings["PRIVATE_KEY"],
                ),
            ),
            raw_loader=None,
            metrics=NOOP_METRICS,
            detection_table="global_irrigation_detection",
            readings_table="readings",
        )
        _insert_readings(cursor, _fixture_rows())

        # The procedure reads from its watermark, the newest stored hour of the org. A seed row makes
        # the first call start about 15 days before 2026-10-13 23:00 and reach 21 days after it.
        cursor.execute(f"""
            INSERT INTO GLOBAL_IRRIGATION_DETECTION
                (SOURCE, DEVICE_ID, TIMESTAMP, TOTAL_READING, NON_IRRIGATION_READING, IRRIGATION_READING, FLOWTIME)
            SELECT '{ORG}', 'watermark-seed', '2026-10-13 23:00'::TIMESTAMP_NTZ, 0, 0, 0,
                   '2026-10-13 23:00 +00:00'::TIMESTAMP_TZ
        """)
        cls.call_results = []
        for _ in range(3):
            cursor.execute(f"CALL {cls.qualified_schema}.RUN_ALL_IRRIGATION()")
            cls.call_results.append(cursor.fetchone()[0])
        cursor.execute(
            "DELETE FROM GLOBAL_IRRIGATION_DETECTION WHERE DEVICE_ID = 'watermark-seed'"
        )
        # Facts about the detection rows as the procedure wrote them, before missing-flowtime loses some
        cls.detection_facts = cls._detection_facts()
        cursor.execute(f"""
            UPDATE GLOBAL_IRRIGATION_DETECTION SET FLOWTIME = NULL
            WHERE DEVICE_ID = 'missing-flowtime'
              AND TIMESTAMP >= '{MISSING_FROM}' AND TIMESTAMP < '{MISSING_TO}'
        """)
        cls.flowtime_rows_removed = cursor.rowcount

        # Two consecutive post-processor windows, as backfill_irrigation_detection and
        # backfill_leak_detection run them and as consecutive daily 30-day windows overlap
        handler = _ListHandler()
        snowflake_logger = logging.getLogger("amiadapters.storage.snowflake")
        snowflake_logger.addHandler(handler)
        try:
            cls.sink.exec_irrigation_detection("behavior-test", *FIRST_WINDOW)
        finally:
            snowflake_logger.removeHandler(handler)
        cls.first_window_errors = handler.messages
        cls.sink.exec_leak_detection("behavior-test", *FIRST_WINDOW)
        handler = _ListHandler()
        snowflake_logger.addHandler(handler)
        try:
            cls.sink.exec_irrigation_detection("behavior-test", *SECOND_WINDOW)
        finally:
            snowflake_logger.removeHandler(handler)
        cls.second_window_errors = handler.messages
        cls.sink.exec_leak_detection("behavior-test", *SECOND_WINDOW)

    @classmethod
    def _query(cls, sql):
        cursor = cls.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    @classmethod
    def _detection_facts(cls):
        return {
            "rows": cls._query("SELECT COUNT(*) FROM GLOBAL_IRRIGATION_DETECTION")[0][
                0
            ],
            "readings": cls._query(
                "SELECT COUNT(*) FROM READINGS WHERE INTERVAL_VALUE IS NOT NULL"
            )[0][0],
            "clocks_disagree": cls._query("""
                SELECT COUNT_IF(FLOWTIME IS NULL OR FLOWTIME::TIMESTAMP_NTZ <> TIMESTAMP)
                FROM GLOBAL_IRRIGATION_DETECTION
            """)[0][0],
            "without_reading": cls._query("""
                SELECT COUNT(*) FROM GLOBAL_IRRIGATION_DETECTION d
                LEFT JOIN READINGS r ON r.DEVICE_ID = d.DEVICE_ID AND r.FLOWTIME = d.FLOWTIME
                WHERE r.DEVICE_ID IS NULL
            """)[0][0],
            "irrigation_hours": dict(cls._query(f"""
                    SELECT DEVICE_ID || '|' || IFF(TIMESTAMP < '2026-11-01', 'before', 'after'),
                           LISTAGG(DISTINCT HOUR(TIMESTAMP), ',') WITHIN GROUP (ORDER BY HOUR(TIMESTAMP))
                    FROM GLOBAL_IRRIGATION_DETECTION
                    WHERE DEVICE_ID IN ('utc-irrigates', 'pacific-irrigates')
                      AND IRRIGATION_READING > 0
                      -- leave out the seam runs (21:00 to 02:00 Pacific, 22:00 to 03:00 UTC)
                      AND NOT (TIMESTAMP >= '2026-11-13 21:00' AND TIMESTAMP <= '2026-11-14 03:00')
                    GROUP BY 1
                """)),
            "fall_back_offsets": [row[0] for row in cls._query("""
                    SELECT TO_CHAR(FLOWTIME, 'TZH:TZM') FROM GLOBAL_IRRIGATION_DETECTION
                    WHERE DEVICE_ID = 'pacific-irrigates' AND TIMESTAMP = '2026-11-01 01:00'
                    ORDER BY 1
                """)],
            "utc_clock_of_first_watering": cls._query("""
                SELECT TO_CHAR(MIN(TIMESTAMP), 'YYYY-MM-DD HH24:MI') FROM GLOBAL_IRRIGATION_DETECTION
                WHERE DEVICE_ID = 'utc-irrigates' AND IRRIGATION_READING > 0
            """)[0][0],
        }

    def _events(self, table, device_id, flag):
        """Events of a device as (start in UTC, hours), oldest first."""
        return [(row[0], row[1]) for row in self._query(f"""
                SELECT TO_CHAR(CONVERT_TIMEZONE('UTC', EVENT_START_TS), 'YYYY-MM-DD HH24:MI'), EVENT_HRS
                FROM {table}
                WHERE DEVICE_ID = '{device_id}' AND {flag}
                ORDER BY EVENT_START_TS
            """)]

    # --- detection ---

    def test_procedure_classifies_every_reading_once_and_a_third_call_adds_nothing(
        self,
    ):
        self.assertEqual(self.detection_facts["readings"], self.detection_facts["rows"])
        self.assertTrue(
            self.call_results[2].endswith(": 0 new rows inserted"), self.call_results
        )

    def test_detection_rows_carry_the_readings_instant(self):
        # FLOWTIME is the reading's own instant, and TIMESTAMP is that instant's clock in its own offset
        self.assertEqual(0, self.detection_facts["clocks_disagree"])
        self.assertEqual(0, self.detection_facts["without_reading"])

    def test_utc_meter_keeps_its_utc_clock(self):
        self.assertEqual(
            "12,13", self.detection_facts["irrigation_hours"]["utc-irrigates|before"]
        )
        self.assertEqual(
            "12,13", self.detection_facts["irrigation_hours"]["utc-irrigates|after"]
        )
        self.assertEqual(
            "2026-10-14 12:00", self.detection_facts["utc_clock_of_first_watering"]
        )

    def test_pacific_meter_keeps_its_pacific_clock_across_the_fall_back(self):
        # 12:00 and 13:00 UTC are 5 and 6 AM in daylight time, 4 and 5 AM in standard time. The
        # fall-back day itself is in standard time by 12:00 UTC.
        self.assertEqual(
            "5,6", self.detection_facts["irrigation_hours"]["pacific-irrigates|before"]
        )
        self.assertEqual(
            "4,5", self.detection_facts["irrigation_hours"]["pacific-irrigates|after"]
        )
        # Both readings of the repeated 1 AM hour have a detection row with their own offset
        self.assertEqual(
            ["-07:00", "-08:00"], self.detection_facts["fall_back_offsets"]
        )

    # --- irrigation events ---

    def test_irrigation_rows_keep_the_readings_instant(self):
        unmatched = self._query("""
            SELECT COUNT(*) FROM IRRIGATION_CADC_INT_TEST i
            LEFT JOIN READINGS r ON r.DEVICE_ID = i.DEVICE_ID AND r.FLOWTIME = i.FLOWTIME_TS
            WHERE r.DEVICE_ID IS NULL
        """)[0][0]
        self.assertEqual(0, unmatched)

    def test_watering_across_the_window_seam_is_one_event(self):
        # Seam runs in UTC: 22:00 to 03:00 UTC, and 21:00 to 02:00 Pacific standard time
        for device_id, seam in (
            ("utc-irrigates", UTC_SEAM),
            ("pacific-irrigates", PACIFIC_SEAM),
            ("missing-flowtime", PACIFIC_SEAM),
        ):
            first, last = (t.astimezone(UTC).strftime("%Y-%m-%d %H:%M") for t in seam)
            events = self._events(
                "IRRIGATION_CADC_INT_TEST_AGG", device_id, "IS_IRRIGATION"
            )
            with self.subTest(device_id=device_id):
                # One event of 6 hours from the run's first hour, and no piece of it on its own
                self.assertEqual(
                    [(first, 6)], [e for e in events if first <= e[0] <= last]
                )

    def test_daily_watering_is_two_hour_events_at_the_readings_time(self):
        utc_events = self._events(
            "IRRIGATION_CADC_INT_TEST_AGG", "utc-irrigates", "IS_IRRIGATION"
        )
        pacific_events = self._events(
            "IRRIGATION_CADC_INT_TEST_AGG", "pacific-irrigates", "IS_IRRIGATION"
        )
        for events in (utc_events, pacific_events):
            daily = [e for e in events if e[1] == 2]
            # One per clock day of the two windows, 2026-10-15 to 2026-11-23, all at 12:00 UTC
            self.assertEqual(40, len(daily))
            self.assertEqual({"12:00"}, {e[0][11:] for e in daily})

    def test_no_irrigation_for_a_quiet_meter(self):
        self.assertEqual(
            [], self._events("IRRIGATION_CADC_INT_TEST_AGG", "quiet", "IS_IRRIGATION")
        )

    def test_rows_without_flowtime_are_left_out_and_reported(self):
        self.assertEqual(150, self.flowtime_rows_removed)
        kept = self._query(f"""
            SELECT COUNT(*) FROM IRRIGATION_CADC_INT_TEST
            WHERE DEVICE_ID = 'missing-flowtime'
              AND FLOWTIME_TS::TIMESTAMP_NTZ >= '{MISSING_FROM}' AND FLOWTIME_TS::TIMESTAMP_NTZ < '{MISSING_TO}'
        """)[0][0]
        self.assertEqual(0, kept)
        self.assertTrue(
            any(
                "detection rows" in m and "kept" in m for m in self.first_window_errors
            ),
            self.first_window_errors,
        )
        self.assertEqual([], self.second_window_errors)

    # --- leak events ---

    def test_leak_across_the_window_seam_is_one_event(self):
        self.assertEqual(
            [("2026-11-12 20:00", 72)],  # 12:00 Pacific standard time
            self._events("LEAKS_CADC_INT_TEST_AGG", "leak-across-window", "IS_LEAK"),
        )

    def test_leak_through_two_windows_is_one_event(self):
        self.assertEqual(
            [("2026-10-16 00:00", 864)],
            self._events("LEAKS_CADC_INT_TEST_AGG", "leak-two-windows", "IS_LEAK"),
        )

    def test_no_leak_for_a_quiet_meter(self):
        self.assertEqual(
            0,
            self._query(
                "SELECT COUNT(*) FROM LEAKS_CADC_INT_TEST WHERE DEVICE_ID = 'quiet'"
            )[0][0],
        )

    # --- both tables ---

    def test_no_overlapping_events(self):
        for table, flag in (
            ("IRRIGATION_CADC_INT_TEST_AGG", "IS_IRRIGATION"),
            ("LEAKS_CADC_INT_TEST_AGG", "IS_LEAK"),
        ):
            overlapping = self._query(f"""
                SELECT COUNT(*) FROM {table} a
                JOIN {table} b
                  ON a.DEVICE_ID = b.DEVICE_ID AND a.EVENT_ID <> b.EVENT_ID
                 AND a.EVENT_START_TS < b.EVENT_END_TS AND b.EVENT_START_TS < a.EVENT_END_TS
                WHERE a.{flag} AND b.{flag}
            """)[0][0]
            with self.subTest(table=table):
                self.assertEqual(0, overlapping)


if __name__ == "__main__":
    unittest.main()

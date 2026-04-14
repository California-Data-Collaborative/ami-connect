"""
Xylem Data Lake adapter.

Fetches AMI data from Xylem's Data Lake platform (Apache Superset over Redshift)
via the Superset SQL Lab API. Authentication is via Keycloak PKCE OAuth.

The Data Lake exposes per-agency schemas (e.g., sensus_dm_hlsbo) containing:
  - account: meter/account metadata (current state)
  - water_intervals: per-interval consumption reads
  - water_registers: cumulative register reads
"""

import base64
import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple

import requests
from bs4 import BeautifulSoup

from amiadapters.adapters.base import BaseAMIAdapter, ScheduledExtract
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

logger = logging.getLogger(__name__)


# ── Raw dataclasses ─────────────────────────────────────────────────────────


@dataclass
class DatalakeAccount:
    device_id: str
    commodity: str
    device_type: str
    radio_id: str
    account_id: str
    account_name: str
    account_rate_code: str
    account_service_type: str
    account_status: str
    asset_address: str
    asset_city: str
    asset_state: str
    asset_zip: str
    meter_manufacturer: str
    meter_size: str
    display_multiplier: str
    sdp_id: str
    record_active_flag: str


@dataclass
class DatalakeWaterInterval:
    meter_id: str
    radio_id: str
    commodity: str
    unit_of_measure: str
    read_time_timestamp: str
    rni_multiplier: str
    interval_value: str
    read_time_timestamp_local: str


@dataclass
class DatalakeWaterRegister:
    meter_id: str
    radio_id: str
    commodity: str
    unit_of_measure: str
    read_time_timestamp: str
    read_quality: str
    flag: str
    rni_multiplier: str
    register_value: str
    read_time_timestamp_local: str


# ── Superset Auth ───────────────────────────────────────────────────────────


KEYCLOAK_BASE = "https://cloud.xylem.com/xgs/auth/realms/xgsum"

# Tuning
DELAY_SECONDS = 2
MAX_AUTH_ATTEMPTS = 3
AUTH_BACKOFF_BASE = 10
CSRF_REFRESH_INTERVAL = 10


def _create_superset_session(
    datalake_url: str,
    superset_url: str,
    client_id: str,
    username: str,
    password: str,
) -> requests.Session:
    """Keycloak PKCE OAuth -> Xylem Data Lake -> Superset session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
    })

    code_verifier = secrets.token_urlsafe(32)
    code_challenge = (
        base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        )
        .rstrip(b"=")
        .decode()
    )

    # Step 1: Keycloak authorize
    logger.info("Keycloak authorize")
    resp = session.get(
        f"{KEYCLOAK_BASE}/protocol/openid-connect/auth",
        params={
            "client_id": client_id,
            "redirect_uri": f"{datalake_url}/authorize",
            "response_type": "code",
            "scope": "openid email profile roles offline_access",
            "nonce": secrets.token_hex(24),
            "state": secrets.token_urlsafe(32),
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        },
        allow_redirects=True,
    )
    resp.raise_for_status()
    if "username" not in resp.text.lower():
        raise RuntimeError(f"Expected login page, got: {resp.url}")

    # Step 2+3: Submit credentials
    soup = BeautifulSoup(resp.text, "html.parser")
    form = soup.find("form", {"id": "kc-form-login"})
    if not form:
        raise RuntimeError("No login form found")

    action_url = form.get("action")
    hidden_fields = {
        h["name"]: h.get("value", "")
        for h in form.find_all("input", {"type": "hidden"})
        if h.get("name")
    }
    password_field = form.find("input", {"name": "password"})

    if password_field:
        logger.info("Submitting credentials (single form)")
        form_data = {**hidden_fields, "username": username, "password": password}
        resp = session.post(action_url, data=form_data, allow_redirects=True)
        resp.raise_for_status()
    else:
        logger.info("Submitting email")
        form_data = {**hidden_fields, "username": username}
        resp = session.post(action_url, data=form_data, allow_redirects=True)
        resp.raise_for_status()

        logger.info("Submitting password")
        soup = BeautifulSoup(resp.text, "html.parser")
        form = soup.find("form", {"id": "kc-form-login"})
        if not form:
            raise RuntimeError("No password form found")
        action_url = form.get("action")
        hidden_fields = {
            h["name"]: h.get("value", "")
            for h in form.find_all("input", {"type": "hidden"})
            if h.get("name")
        }
        form_data = {**hidden_fields, "username": username, "password": password}
        resp = session.post(action_url, data=form_data, allow_redirects=True)
        resp.raise_for_status()

    # Step 4: Superset OAuth
    logger.info("Superset OAuth")
    resp = session.get(f"{superset_url}/login/", allow_redirects=True)
    resp.raise_for_status()

    me_resp = session.get(f"{superset_url}/api/v1/me/")
    if "application/json" not in me_resp.headers.get("Content-Type", ""):
        raise RuntimeError(
            f"Superset login failed (status {me_resp.status_code}): {me_resp.text[:300]}"
        )
    user = me_resp.json()["result"]
    logger.info(f"Authenticated as {user['username']}")

    # Step 5: CSRF token
    _refresh_csrf(session, superset_url)

    return session


def _refresh_csrf(session: requests.Session, superset_url: str) -> None:
    """Refresh the CSRF token on an existing session."""
    csrf_resp = session.get(
        f"{superset_url}/api/v1/security/csrf_token/",
        allow_redirects=False,
    )
    if csrf_resp.status_code == 200 and "application/json" in csrf_resp.headers.get("Content-Type", ""):
        session.headers["X-CSRFToken"] = csrf_resp.json()["result"]
        session.headers["Referer"] = f"{superset_url}/sqllab/"
        logger.info("CSRF token refreshed")
    else:
        logger.warning("CSRF token refresh failed — POSTs may fail")


def _session_expired(resp: requests.Response) -> bool:
    """Check if a response indicates an expired session."""
    if resp.status_code in (301, 302, 303, 307, 308, 401, 403):
        return True
    if "application/json" not in resp.headers.get("Content-Type", ""):
        return True
    return False


# ── Adapter ─────────────────────────────────────────────────────────────────


class XylemDatalakeAdapter(BaseAMIAdapter):
    """
    AMI Adapter that retrieves data from a Xylem Data Lake instance via the
    Superset SQL Lab API. Each Data Lake instance is per-agency, identified
    by an agency_code that determines the URL subdomain and Redshift schema.
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        pipeline_configuration,
        configured_task_output_controller,
        configured_metrics,
        agency_code,
        database_id,
        client_id,
        username,
        password,
        chunk_hours=12,
        configured_sinks=None,
    ):
        self.agency_code = agency_code
        self.database_id = database_id
        self.client_id = client_id
        self.username = username
        self.password = password
        self.chunk_hours = chunk_hours
        self.datalake_url = f"https://{agency_code}.datalake.xylem-vue.com"
        self.superset_url = f"https://{agency_code}.superset.datalake.xylem-vue.com"
        self.schema = f"sensus_dm_{agency_code}"
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_metrics,
            configured_sinks,
            XYLEM_DATALAKE_RAW_SNOWFLAKE_LOADER,
        )

    def name(self) -> str:
        return f"xylem-datalake-{self.org_id}"

    def scheduled_extracts(self) -> List[ScheduledExtract]:
        return [
            ScheduledExtract(
                interval=timedelta(days=6),
            )
        ]

    # ── Extract ─────────────────────────────────────────────────────────

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> ExtractOutput:
        self._session = _create_superset_session(
            self.datalake_url,
            self.superset_url,
            self.client_id,
            self.username,
            self.password,
        )
        self._requests_since_csrf = 0

        account_columns = ", ".join(DatalakeAccount.__dataclass_fields__.keys())
        account_rows = self._query(
            f"SELECT {account_columns} FROM {self.schema}.account "
            f"WHERE record_active_flag = 'true'",
        )

        interval_columns = ", ".join(DatalakeWaterInterval.__dataclass_fields__.keys())
        interval_rows = self._query_chunked(
            f"{self.schema}.water_intervals",
            interval_columns,
            "read_time_timestamp",
            extract_range_start,
            extract_range_end,
        )

        register_columns = ", ".join(DatalakeWaterRegister.__dataclass_fields__.keys())
        register_rows = self._query_chunked(
            f"{self.schema}.water_registers",
            register_columns,
            "read_time_timestamp",
            extract_range_start,
            extract_range_end,
        )

        files = {
            "account.json": "\n".join(
                json.dumps(r, cls=DataclassJSONEncoder) for r in account_rows
            ),
            "water_intervals.json": "\n".join(
                json.dumps(r, cls=DataclassJSONEncoder) for r in interval_rows
            ),
            "water_registers.json": "\n".join(
                json.dumps(r, cls=DataclassJSONEncoder) for r in register_rows
            ),
        }
        return ExtractOutput(files)

    def _query_chunked(
        self,
        table: str,
        columns: str,
        date_column: str,
        range_start: datetime,
        range_end: datetime,
    ) -> List[dict]:
        """Query a table in time-based chunks to avoid Superset row limits."""
        all_rows = []
        current_end = range_end
        while current_end > range_start:
            current_start = max(current_end - timedelta(hours=self.chunk_hours), range_start)
            sql = (
                f"SELECT {columns} FROM {table} "
                f"WHERE {date_column} >= '{current_start:%Y-%m-%d %H:%M:%S}' "
                f"AND {date_column} < '{current_end:%Y-%m-%d %H:%M:%S}'"
            )
            rows = self._query(sql)
            all_rows.extend(rows)
            current_end = current_start
        return all_rows

    def _query(self, sql: str) -> List[dict]:
        """Execute a SQL query via the Superset SQL Lab API with session management."""
        auth_failures = 0

        while True:
            # Refresh CSRF periodically
            self._requests_since_csrf += 1
            if self._requests_since_csrf >= CSRF_REFRESH_INTERVAL:
                _refresh_csrf(self._session, self.superset_url)
                self._requests_since_csrf = 0

            try:
                resp = self._session.post(
                    f"{self.superset_url}/api/v1/sqllab/execute/",
                    json={"database_id": self.database_id, "sql": sql, "runAsync": False},
                    allow_redirects=False,
                )
            except requests.exceptions.TooManyRedirects:
                auth_failures += 1
                logger.warning("Redirect loop (session expired)")
                if auth_failures > MAX_AUTH_ATTEMPTS:
                    raise RuntimeError(f"Max re-auth attempts ({MAX_AUTH_ATTEMPTS}) reached")
                self._session = self._reauth(auth_failures - 1)
                self._requests_since_csrf = 0
                continue

            if _session_expired(resp):
                auth_failures += 1
                reason = f"HTTP {resp.status_code}" if resp.status_code != 200 else "non-JSON response"
                logger.warning(f"Session expired ({reason})")
                if auth_failures > MAX_AUTH_ATTEMPTS:
                    raise RuntimeError(f"Max re-auth attempts ({MAX_AUTH_ATTEMPTS}) reached")
                self._session = self._reauth(auth_failures - 1)
                self._requests_since_csrf = 0
                continue

            resp.raise_for_status()
            result = resp.json()
            auth_failures = 0

            if result.get("status") == "error":
                msg = result.get("error") or result.get("message") or "unknown"
                raise RuntimeError(f"Superset query error: {msg}")

            rows = result.get("data", [])
            logger.info(f"Query returned {len(rows)} rows")
            time.sleep(DELAY_SECONDS)
            return rows

    def _reauth(self, consecutive_failures: int) -> requests.Session:
        """Re-authenticate with backoff."""
        if consecutive_failures > 0:
            delay = AUTH_BACKOFF_BASE * (2 ** (consecutive_failures - 1))
            logger.info(f"Backing off {delay}s (failed attempt {consecutive_failures}/{MAX_AUTH_ATTEMPTS})")
            time.sleep(delay)
        else:
            logger.info("Re-authenticating")
        return _create_superset_session(
            self.datalake_url,
            self.superset_url,
            self.client_id,
            self.username,
            self.password,
        )

    # ── Transform ───────────────────────────────────────────────────────

    def _transform(
        self, run_id: str, extract_outputs: ExtractOutput
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        accounts_by_device_id = self._accounts_by_device_id(extract_outputs)

        raw_intervals = self._read_file(
            extract_outputs, "water_intervals.json", DatalakeWaterInterval
        )
        raw_registers = self._read_file(
            extract_outputs, "water_registers.json", DatalakeWaterRegister
        )

        meters_by_device_id = self._transform_meters(accounts_by_device_id)

        reads = self._transform_reads(
            accounts_by_device_id,
            set(meters_by_device_id.keys()),
            list(raw_intervals),
            list(raw_registers),
        )

        return list(meters_by_device_id.values()), reads

    def _transform_meters(
        self,
        accounts_by_device_id: Dict[str, DatalakeAccount],
    ) -> Dict[str, GeneralMeter]:
        meters = {}
        for device_id, account in accounts_by_device_id.items():
            if account.commodity != "WATER":
                logger.info(f"Skipping non-water account {device_id} (commodity={account.commodity})")
                continue

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account.account_id,
                location_id=account.sdp_id,
                meter_id=device_id,
                endpoint_id=account.radio_id,
                meter_install_date=None,
                meter_size=self.map_meter_size(str(account.meter_size)) if account.meter_size is not None else None,
                meter_manufacturer=account.meter_manufacturer,
                multiplier=account.display_multiplier,
                location_address=account.asset_address,
                location_city=account.asset_city,
                location_state=account.asset_state,
                location_zip=account.asset_zip,
            )
            meters[device_id] = meter
        return meters

    def _transform_reads(
        self,
        accounts_by_device_id: Dict[str, DatalakeAccount],
        meter_ids_to_include: Set[str],
        interval_reads: List[DatalakeWaterInterval],
        register_reads: List[DatalakeWaterRegister],
    ) -> List[GeneralMeterRead]:
        reads_by_device_and_time = {}

        for raw_read in interval_reads:
            device_id = raw_read.meter_id
            if device_id not in meter_ids_to_include:
                continue

            flowtime = self._parse_flowtime(raw_read.read_time_timestamp)
            interval_value, interval_unit = self.map_reading(
                float(raw_read.interval_value),
                raw_read.unit_of_measure,
            )

            account = accounts_by_device_id.get(device_id)
            read = GeneralMeterRead(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account.account_id if account else None,
                location_id=account.sdp_id if account else None,
                flowtime=flowtime,
                register_value=None,
                register_unit=None,
                interval_value=interval_value,
                interval_unit=interval_unit,
                battery=None,
                install_date=None,
                connection=None,
                estimated=None,
            )
            reads_by_device_and_time[(device_id, flowtime)] = read

        for raw_read in register_reads:
            device_id = raw_read.meter_id
            if device_id not in meter_ids_to_include:
                continue

            flowtime = self._parse_flowtime(raw_read.read_time_timestamp)
            register_value, register_unit = self.map_reading(
                float(raw_read.register_value),
                raw_read.unit_of_measure,
            )

            if (device_id, flowtime) in reads_by_device_and_time:
                existing_read = reads_by_device_and_time[(device_id, flowtime)]
                read = replace(
                    existing_read,
                    register_value=register_value,
                    register_unit=register_unit,
                )
            else:
                account = accounts_by_device_id.get(device_id)
                read = GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=device_id,
                    account_id=account.account_id if account else None,
                    location_id=account.sdp_id if account else None,
                    flowtime=flowtime,
                    register_value=register_value,
                    register_unit=register_unit,
                    interval_value=None,
                    interval_unit=None,
                    battery=None,
                    install_date=None,
                    connection=None,
                    estimated=None,
                )
            reads_by_device_and_time[(device_id, flowtime)] = read

        return list(reads_by_device_and_time.values())

    def _parse_flowtime(self, raw_flowtime: str) -> datetime:
        """Parse a UTC timestamp from the Data Lake. Timestamps are naive but represent UTC."""
        if "T" in raw_flowtime:
            return datetime.strptime(raw_flowtime, "%Y-%m-%dT%H:%M:%S")
        else:
            return datetime.strptime(raw_flowtime, "%Y-%m-%d %H:%M:%S")

    # ── Helpers ─────────────────────────────────────────────────────────

    def _accounts_by_device_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, DatalakeAccount]:
        raw_accounts = self._read_file(extract_outputs, "account.json", DatalakeAccount)
        result = {}
        for account in raw_accounts:
            if not account.device_id:
                logger.warning(f"Skipping account with null device_id: {account}")
                continue
            result[account.device_id] = account
        return result

    def _read_file(self, extract_outputs: ExtractOutput, file: str, raw_dataclass):
        lines = extract_outputs.load_from_file(file, raw_dataclass, allow_empty=True)
        yield from lines


# ── Raw Snowflake Loaders ───────────────────────────────────────────────────


class XylemDatalakeRawAccountLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "XYLEM_DATALAKE_ACCOUNT_BASE"

    def columns(self) -> List[str]:
        return list(DatalakeAccount.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["device_id"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("account.json", DatalakeAccount, allow_empty=True)
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class XylemDatalakeRawWaterIntervalsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "XYLEM_DATALAKE_WATER_INTERVALS_BASE"

    def columns(self) -> List[str]:
        return list(DatalakeWaterInterval.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id", "read_time_timestamp"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "water_intervals.json", DatalakeWaterInterval, allow_empty=True
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class XylemDatalakeRawWaterRegistersLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "XYLEM_DATALAKE_WATER_REGISTERS_BASE"

    def columns(self) -> List[str]:
        return list(DatalakeWaterRegister.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id", "read_time_timestamp"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "water_registers.json", DatalakeWaterRegister, allow_empty=True
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


XYLEM_DATALAKE_RAW_SNOWFLAKE_LOADER = RawSnowflakeLoader.with_table_loaders(
    [
        XylemDatalakeRawAccountLoader(),
        XylemDatalakeRawWaterIntervalsLoader(),
        XylemDatalakeRawWaterRegistersLoader(),
    ]
)

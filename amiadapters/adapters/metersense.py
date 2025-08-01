from dataclasses import dataclass, replace
from datetime import datetime
import json
import logging
from typing import Dict, Generator, List, Set, Tuple

import oracledb
from pytz.tzinfo import DstTzInfo

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.adapters.connections import open_ssh_tunnel
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader

logger = logging.getLogger(__name__)


@dataclass
class MetersenseAccountService:
    service_id: str
    account_id: str
    location_no: str
    commodity_tp: str
    last_read_dt: str
    active_dt: str
    inactive_dt: str


@dataclass
class MetersenseLocation:
    location_no: str
    alt_location_id: str
    location_class: str
    unit_no: str
    street_no: str
    street_pfx: str
    street_name: str
    street_sfx: str
    street_sfx_dir: str
    city: str
    state: str
    postal_cd: str
    billing_cycle: str
    add_by: str
    add_dt: str
    change_by: str
    change_dt: str
    latitude: str
    longitude: str


@dataclass
class MetersenseMeter:
    meter_id: str
    alt_meter_id: str
    meter_tp: str
    commodity_tp: str
    region_id: str
    interval_length: str
    regread_frequency: str
    channel1_raw_uom: str
    channel2_raw_uom: str
    channel3_raw_uom: str
    channel4_raw_uom: str
    channel5_raw_uom: str
    channel6_raw_uom: str
    channel7_raw_uom: str
    channel8_raw_uom: str
    channel1_multiplier: str
    channel2_multiplier: str
    channel3_multiplier: str
    channel4_multiplier: str
    channel5_multiplier: str
    channel6_multiplier: str
    channel7_multiplier: str
    channel8_multiplier: str
    channel1_final_uom: str
    channel2_final_uom: str
    channel3_final_uom: str
    channel4_final_uom: str
    channel5_final_uom: str
    channel6_final_uom: str
    channel7_final_uom: str
    channel8_final_uom: str
    first_data_ts: str
    last_data_ts: str
    ami_id: str
    power_status: str
    latitude: str
    longitude: str
    exclude_in_reports: str
    add_by: str
    add_dt: str
    change_by: str
    change_dt: str


@dataclass
class MetersenseMetersView:
    meter_id: str
    alt_meter_id: str
    meter_tp: str
    commodity_tp: str
    region_id: str
    interval_length: str
    regread_frequency: str
    channel1_raw_uom: str
    channel2_raw_uom: str
    channel3_raw_uom: str
    channel4_raw_uom: str
    channel5_raw_uom: str
    channel6_raw_uom: str
    channel7_raw_uom: str
    channel8_raw_uom: str
    channel1_multiplier: str
    channel2_multiplier: str
    channel3_multiplier: str
    channel4_multiplier: str
    channel5_multiplier: str
    channel6_multiplier: str
    channel7_multiplier: str
    channel8_multiplier: str
    channel1_final_uom: str
    channel2_final_uom: str
    channel3_final_uom: str
    channel4_final_uom: str
    channel5_final_uom: str
    channel6_final_uom: str
    channel7_final_uom: str
    channel8_final_uom: str
    first_data_ts: str
    last_data_ts: str
    ami_id: str
    power_status: str
    latitude: str
    longitude: str
    exclude_in_reports: str
    nb_dials: str
    backflow: str
    service_point_type: str
    reclaim_inter_prog: str
    power_status_details: str
    comm_module_id: str
    register_constant: str


@dataclass
class MetersenseMeterLocationXref:
    meter_id: str
    active_dt: str
    location_no: str
    inactive_dt: str
    add_by: str
    add_dt: str
    change_by: str
    change_dt: str


@dataclass
class MetersenseIntervalRead:
    """
    Representation of the INTERVALREADS table in Metersense schema.
    """

    meter_id: str
    channel_id: str
    read_dt: str
    read_hr: str
    read_30min_int: str
    read_15min_int: str
    read_5min_int: str
    read_dtm: str
    read_value: str
    uom: str
    status: str
    read_version: str


@dataclass
class MetersenseRegisterRead:
    """
    Representation of the REGISTERREADS table in Metersense schema.
    """

    meter_id: str
    channel_id: str
    read_dtm: str
    read_value: str
    uom: str
    status: str
    read_version: str


class MetersenseAdapter(BaseAMIAdapter):
    """
    AMI Adapter that retrieves Xylem/Sensus data from a Metersense Oracle database.
    The Oracle database is only accessible through an SSH tunnel. This code assumes the tunnel
    infrastructure exists and connects to Oracle through SSH to an intermediate server.

    You may need to:
    - Add your Airflow server's public SSH key to the intermediate server's allowed hosts
    - Add your Airflow server's public IP address to a security group that allows SSH into the intermediate server
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        configured_task_output_controller,
        ssh_tunnel_server_host,
        ssh_tunnel_username,
        ssh_tunnel_key_path,
        database_host,
        database_port,
        database_db_name,
        database_user,
        database_password,
        configured_sinks=None,
    ):
        """
        ssh_tunnel_server_host = hostname or IP of intermediate server
        ssh_tunnel_username = SSH username for intermediate server
        ssh_tunnel_key_path = path to local SSH private key for authentication to intermediate server (the intermediate server must know your public key already!)
        database_host = hostname or IP of the Oracle database
        database_port = port of Oracle database
        database_db_name = database name of Oracle database
        database_user = username for Oracle database
        database_password = password for Oracle database
        """
        self.ssh_tunnel_server_host = ssh_tunnel_server_host
        self.ssh_tunnel_username = ssh_tunnel_username
        self.ssh_tunnel_key_path = ssh_tunnel_key_path
        self.database_host = database_host
        self.database_port = database_port
        self.database_db_name = database_db_name
        self.database_user = database_user
        self.database_password = database_password
        super().__init__(
            org_id,
            org_timezone,
            configured_task_output_controller,
            configured_sinks,
            MetersenseRawSnowflakeLoader(),
        )

    def name(self) -> str:
        return f"metersense-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        with open_ssh_tunnel(
            ssh_tunnel_server_host=self.ssh_tunnel_server_host,
            ssh_tunnel_username=self.ssh_tunnel_username,
            ssh_tunnel_key_path=self.ssh_tunnel_key_path,
            remote_host=self.database_host,
            remote_port=self.database_port,
        ) as ctx:
            logging.info("Created SSH tunnel")
            connection = oracledb.connect(
                user=self.database_user,
                password=self.database_password,
                dsn=f"0.0.0.0:{ctx.local_bind_port}/{self.database_db_name}",
            )

            logger.info("Successfully connected to Oracle Database")

            cursor = connection.cursor()

            files = self._query_tables(cursor, extract_range_start, extract_range_end)

        return ExtractOutput(files)

    def _query_tables(
        self, cursor, extract_range_start: datetime, extract_range_end: datetime
    ) -> Dict[str, str]:
        """
        Run SQL on remote Oracle database to extract all data. We've chosen to do as little
        filtering and joining as possible to preserve the raw data. It comes out in extract
        files per table.
        """
        files = {}
        tables = [
            ("ACCOUNT_SERVICES", MetersenseAccountService, None, None),
            (
                "INTERVALREADS",
                MetersenseIntervalRead,
                extract_range_start,
                extract_range_end,
            ),
            ("LOCATIONS", MetersenseLocation, None, None),
            ("METERS", MetersenseMeter, None, None),
            ("METERS_VIEW", MetersenseMetersView, None, None),
            ("METER_LOCATION_XREF", MetersenseMeterLocationXref, None, None),
            (
                "REGISTERREADS",
                MetersenseRegisterRead,
                extract_range_start,
                extract_range_end,
            ),
        ]
        for table, row_type, start_date, end_date in tables:
            rows = self._extract_table(cursor, table, row_type, start_date, end_date)
            text = "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in rows)
            files[f"{table.lower()}.json"] = text
        return files

    def _extract_table(
        self,
        cursor,
        table_name: str,
        row_type,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ) -> List:
        """
        Query for data from a table in the Oracle database and prep for output.
        """
        query = f"SELECT * FROM {table_name} WHERE 1=1 "
        kwargs = {}

        # Reads should be filtered by date range
        if extract_range_start and extract_range_end:
            query += (
                f" AND READ_DTM BETWEEN :extract_range_start AND :extract_range_end "
            )
            kwargs["extract_range_start"] = extract_range_start
            kwargs["extract_range_end"] = extract_range_end

        logger.info(f"Running query {query} with values {kwargs}")
        cursor.execute(query, kwargs)
        rows = cursor.fetchall()

        # Turn SQL results into our dataclass instances
        # Use the dataclass for SQL column names
        columns = list(row_type.__dataclass_fields__.keys())
        result = []
        for row in rows:
            data = {}
            for name, value in zip(columns, row):
                # Turn datetimes into strings for serialization
                if isinstance(value, datetime):
                    value = value.isoformat()
                data[name] = value
            result.append(row_type(**data))

        logger.info(f"Fetched {len(result)} rows from {table_name}")
        return result

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        accounts_by_location_id = self._accounts_by_location_id(extract_outputs)
        xrefs_by_meter_id = self._xrefs_by_meter_id(extract_outputs)
        meter_views_by_meter_id = self._meter_views_by_meter_id(extract_outputs)
        locations_by_location_id = self._locations_by_location_id(extract_outputs)

        raw_meters = self._read_file(extract_outputs, "meters.json")
        meters_by_device_id = self._transform_meters(
            raw_meters,
            accounts_by_location_id,
            xrefs_by_meter_id,
            meter_views_by_meter_id,
            locations_by_location_id,
        )

        raw_interval_reads = self._read_file(extract_outputs, "intervalreads.json")
        raw_register_reads = self._read_file(extract_outputs, "registerreads.json")
        reads_by_device_and_flowtime = self._transform_reads(
            accounts_by_location_id,
            xrefs_by_meter_id,
            set(meters_by_device_id.keys()),
            raw_interval_reads,
            raw_register_reads,
        )

        return list(meters_by_device_id.values()), list(
            reads_by_device_and_flowtime.values()
        )

    def _accounts_by_location_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[MetersenseAccountService]]:
        """
        Map each location ID to the list of accounts associated with it. The list is sorted with most recently active
        account first.
        """
        raw_account_services = self._read_file(extract_outputs, "account_services.json")
        accounts_by_location_id = {}
        for l in raw_account_services:
            a = MetersenseAccountService(**json.loads(l))
            if not a.location_no or a.commodity_tp != "W":
                continue
            if a.location_no not in accounts_by_location_id:
                accounts_by_location_id[a.location_no] = []
            accounts_by_location_id[a.location_no].append(a)
        for location in accounts_by_location_id.keys():
            accounts_by_location_id[location] = sorted(
                accounts_by_location_id[location],
                key=lambda a: a.inactive_dt,
                reverse=True,
            )
        return accounts_by_location_id

    def _xrefs_by_meter_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[MetersenseMeterLocationXref]]:
        """
        Map each meter ID to the list of locations associated with it. The list is sorted with most recently active
        account first.
        """
        raw_meter_location_xrefs = self._read_file(
            extract_outputs, "meter_location_xref.json"
        )
        xrefs_by_meter_id = {}
        for l in raw_meter_location_xrefs:
            x = MetersenseMeterLocationXref(**json.loads(l))
            if not x.meter_id or not x.location_no:
                continue
            if x.meter_id not in xrefs_by_meter_id:
                xrefs_by_meter_id[x.meter_id] = []
            xrefs_by_meter_id[x.meter_id].append(x)
        for meter_id in xrefs_by_meter_id.keys():
            xrefs_by_meter_id[meter_id] = sorted(
                xrefs_by_meter_id[meter_id], key=lambda x: x.inactive_dt, reverse=True
            )
        return xrefs_by_meter_id

    def _meter_views_by_meter_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, MetersenseMetersView]:
        """
        Map each meter ID to the meter view associated with it.
        """
        raw_meters_views = self._read_file(extract_outputs, "meters_view.json")
        meter_views_by_meter_id = {}
        for l in raw_meters_views:
            mv = MetersenseMetersView(**json.loads(l))
            if not mv.meter_id:
                continue
            meter_views_by_meter_id[mv.meter_id] = mv
        return meter_views_by_meter_id

    def _locations_by_location_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, MetersenseLocation]:
        """
        Map each location ID to the location associated with it.
        """
        raw_locations = self._read_file(extract_outputs, "locations.json")
        locations_by_location_id = {}
        for line in raw_locations:
            l = MetersenseLocation(**json.loads(line))
            if not l.location_no:
                continue
            locations_by_location_id[l.location_no] = l
        return locations_by_location_id

    def _transform_meters(
        self,
        raw_meters: Generator,
        accounts_by_location_id: Dict[str, List[MetersenseAccountService]],
        xrefs_by_meter_id: Dict[str, List[MetersenseMeterLocationXref]],
        meter_views_by_meter_id: Dict[str, MetersenseMetersView],
        locations_by_location_id: Dict[str, MetersenseLocation],
    ) -> Dict[str, GeneralMeter]:
        """
        Join all raw data sources together and transform into general meter format.
        """
        meters_by_device_id = {}
        for raw_meter_str in raw_meters:
            raw_meter = MetersenseMeter(**json.loads(raw_meter_str))
            if raw_meter.commodity_tp != "W":
                continue
            device_id = raw_meter.meter_id

            # TODO remove this after we validate we are handling duplicates
            if device_id in meters_by_device_id:
                raise Exception()

            # Most recent location and account for this meter
            account, location = self._get_account_and_location_for_meter(
                raw_meter.meter_id,
                xrefs_by_meter_id,
                locations_by_location_id,
                accounts_by_location_id,
            )

            meter_view = meter_views_by_meter_id.get(raw_meter.meter_id)

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account.account_id if account else None,
                location_id=location.location_no if location else None,
                meter_id=raw_meter.meter_id,
                endpoint_id=meter_view.comm_module_id if meter_view else None,
                meter_install_date=self.datetime_from_iso_str(
                    raw_meter.add_dt, self.org_timezone
                ),
                meter_size=self.map_meter_size(raw_meter.meter_tp),
                meter_manufacturer=None,
                multiplier=raw_meter.channel1_multiplier,
                location_address=location.street_name if location else None,
                location_city=location.city if location else None,
                location_state=location.state if location else None,
                location_zip=location.postal_cd if location else None,
            )
            meters_by_device_id[device_id] = meter
        return meters_by_device_id

    def _transform_reads(
        self,
        accounts_by_location_id: Dict[str, List[MetersenseAccountService]],
        xrefs_by_meter_id: Dict[str, List[MetersenseMeterLocationXref]],
        device_ids_to_include: Set[str],
        raw_interval_reads: List[MetersenseIntervalRead],
        raw_register_reads: List[MetersenseIntervalRead],
    ) -> Dict[str, GeneralMeter]:
        """
        Join interval and register reads together, join in meter metadata when possible. Only include
        reads for devices that we kept in the meter transform step.
        """
        reads_by_device_and_time = {}

        for raw_interval_read_str in raw_interval_reads:
            raw_interval_read = MetersenseIntervalRead(
                **json.loads(raw_interval_read_str)
            )
            device_id = raw_interval_read.meter_id
            if device_id not in device_ids_to_include:
                continue
            flowtime = self.datetime_from_iso_str(
                raw_interval_read.read_dtm, self.org_timezone
            )
            key = (
                device_id,
                flowtime,
            )

            account_id, location_id = self._get_account_and_location_for_read(
                raw_interval_read.meter_id,
                raw_interval_read.read_dtm,
                xrefs_by_meter_id,
                accounts_by_location_id,
            )

            interval_value, interval_unit = self.map_reading(
                raw_interval_read.read_value,
                raw_interval_read.uom,  # Expected to be CCF
            )

            read = GeneralMeterRead(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                flowtime=flowtime,
                register_value=None,
                register_unit=None,
                interval_value=interval_value,
                interval_unit=interval_unit,
            )
            reads_by_device_and_time[key] = read

        for raw_register_read_str in raw_register_reads:
            raw_register_read = MetersenseRegisterRead(
                **json.loads(raw_register_read_str)
            )
            device_id = raw_register_read.meter_id
            if device_id not in device_ids_to_include:
                continue
            flowtime = self.datetime_from_iso_str(
                raw_register_read.read_dtm, self.org_timezone
            )
            register_value, register_unit = self.map_reading(
                raw_register_read.read_value,
                raw_register_read.uom,  # Expected to be CCF
            )
            key = (
                device_id,
                flowtime,
            )
            if key in reads_by_device_and_time:
                # Join register read onto the interval read object
                old_read = reads_by_device_and_time[key]
                read = replace(
                    old_read,
                    register_value=register_value,
                    register_unit=register_unit,
                )
            else:
                account_id, location_id = self._get_account_and_location_for_read(
                    raw_register_read.meter_id,
                    raw_register_read.read_dtm,
                    xrefs_by_meter_id,
                    accounts_by_location_id,
                )
                read = GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=device_id,
                    account_id=account_id,
                    location_id=location_id,
                    flowtime=flowtime,
                    register_value=register_value,
                    register_unit=register_unit,
                    interval_value=None,
                    interval_unit=None,
                )
            reads_by_device_and_time[key] = read

        return reads_by_device_and_time

    def _get_account_and_location_for_read(
        self,
        meter_id: str,
        read_dtm: str,
        xrefs_by_meter_id: Dict[str, List],
        accounts_by_location_id: Dict[str, List],
    ) -> Tuple[str, str]:
        account_id, location_id = None, None

        flowtime = datetime.fromisoformat(read_dtm)

        xrefs_for_meter = xrefs_by_meter_id.get(meter_id, [])
        for xref in xrefs_for_meter:
            if (
                datetime.fromisoformat(xref.active_dt)
                <= flowtime
                <= datetime.fromisoformat(xref.inactive_dt)
            ):
                location_id = xref.location_no

        accounts_for_location = accounts_by_location_id.get(location_id, [])
        for account in accounts_for_location:
            if (
                datetime.fromisoformat(account.active_dt)
                <= flowtime
                <= datetime.fromisoformat(account.inactive_dt)
            ):
                account_id = account.account_id

        return account_id, location_id

    def _get_account_and_location_for_meter(
        self,
        meter_id: str,
        xrefs_by_meter_id: Dict[str, List[MetersenseMeterLocationXref]],
        locations_by_location_id: Dict[str, MetersenseLocation],
        accounts_by_location_id: Dict[str, List[MetersenseAccountService]],
    ) -> Tuple[MetersenseAccountService, MetersenseLocation]:
        account, location, xref = None, None, None
        if location_xrefs := xrefs_by_meter_id.get(meter_id, []):
            # The most recent record
            xref = location_xrefs[0]
        if xref:
            location = locations_by_location_id.get(xref.location_no)
            if accounts := accounts_by_location_id.get(xref.location_no, []):
                # The most recent record
                account = accounts[0]
        return account, location

    def _read_file(self, extract_outputs: ExtractOutput, file: str) -> Generator:
        """
        Read a file's contents from extract stage output, create generator
        for each line of text
        """
        file_text = extract_outputs.from_file(file)
        if file_text is None:
            raise Exception(f"No output found for file {file}")
        lines = file_text.strip().split("\n")
        if lines == [""]:
            lines = []
        yield from lines


class MetersenseRawSnowflakeLoader(RawSnowflakeLoader):

    def load(self, *args):
        self._load_raw_account_services(*args)
        self._load_raw_locations(*args)
        self._load_raw_meters(*args)
        self._load_raw_meters_views(*args)
        self._load_raw_meter_location_xrefs(*args)
        self._load_raw_interval_reads(*args)
        self._load_raw_register_reads(*args)

    def _load_raw_account_services(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="account_services.json",
            raw_dataclass=MetersenseAccountService,
            table="METERSENSE_ACCOUNT_SERVICES_BASE",
            unique_by=["account_id", "location_no", "inactive_dt"],
        )

    def _load_raw_locations(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="locations.json",
            raw_dataclass=MetersenseLocation,
            table="METERSENSE_LOCATIONS_BASE",
            unique_by=["location_no"],
        )

    def _load_raw_meters(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="meters.json",
            raw_dataclass=MetersenseMeter,
            table="METERSENSE_METERS_BASE",
            unique_by=["meter_id"],
        )

    def _load_raw_meters_views(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="meters_view.json",
            raw_dataclass=MetersenseMetersView,
            table="METERSENSE_METERS_VIEW_BASE",
            unique_by=["meter_id"],
        )

    def _load_raw_meter_location_xrefs(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="meter_location_xref.json",
            raw_dataclass=MetersenseMeterLocationXref,
            table="METERSENSE_METER_LOCATION_XREF_BASE",
            unique_by=["meter_id", "inactive_dt"],
        )

    def _load_raw_interval_reads(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="intervalreads.json",
            raw_dataclass=MetersenseIntervalRead,
            table="METERSENSE_INTERVALREADS_BASE",
            unique_by=["meter_id", "read_dtm"],
        )

    def _load_raw_register_reads(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
    ) -> None:
        self._load_raw_data(
            run_id,
            org_id,
            org_timezone,
            extract_outputs,
            snowflake_conn,
            extract_output_filename="registerreads.json",
            raw_dataclass=MetersenseRegisterRead,
            table="METERSENSE_REGISTERREADS_BASE",
            unique_by=["meter_id", "read_dtm"],
        )

    def _load_raw_data(
        self,
        run_id: str,
        org_id: str,
        org_timezone: DstTzInfo,
        extract_outputs: ExtractOutput,
        snowflake_conn,
        extract_output_filename: str,
        raw_dataclass,
        table: str,
        unique_by: List[str],
    ) -> None:
        """
        Extract raw data from intermediate outputs, then load into raw data table.

        extract_output_filename: name of file in extract_outputs that contains the raw data
        raw_dataclass: e.g. MetersenseRegisterRead, used to deserialize raw data and determine table column names
        table: name of raw data table in Snowflake
        unique_by: list of field names used with org_id to uniquely identify a row in the base table
        """
        text = extract_outputs.from_file(extract_output_filename)
        raw_data = [raw_dataclass(**json.loads(d)) for d in text.strip().split("\n")]
        temp_table = f"temp_{table}"
        fields = list(raw_dataclass.__dataclass_fields__.keys())
        self._create_temp_table(
            snowflake_conn,
            temp_table,
            table,
            fields,
            org_timezone,
            org_id,
            raw_data,
        )
        self._merge_from_temp_table(
            snowflake_conn,
            table,
            temp_table,
            fields,
            unique_by,
        )

    def _create_temp_table(
        self, snowflake_conn, temp_table, table, fields, org_timezone, org_id, raw_data
    ) -> None:
        """
        Insert every object in raw_data into a temp copy of the table.
        """
        logger.info(f"Prepping for raw load to table {table}")

        # Create the temp table
        create_temp_table_sql = (
            f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} LIKE {table};"
        )
        snowflake_conn.cursor().execute(create_temp_table_sql)

        # Insert raw data
        columns_as_comma_str = ", ".join(fields)
        qmarks = "?, " * (len(fields) - 1) + "?"
        insert_temp_data_sql = f"""
            INSERT INTO {temp_table} (org_id, created_time, {columns_as_comma_str}) 
                VALUES (?, ?, {qmarks})
        """
        created_time = datetime.now(tz=org_timezone)
        rows = [
            tuple(
                [org_id, created_time] + [i.__getattribute__(name) for name in fields]
            )
            for i in raw_data
        ]
        snowflake_conn.cursor().executemany(insert_temp_data_sql, rows)

    def _merge_from_temp_table(
        self,
        snowflake_conn,
        table: str,
        temp_table: str,
        fields: List[str],
        unique_by: List[str],
    ) -> None:
        """
        Merge data from temp table into the base table using the unique_by keys
        """
        logger.info(f"Merging {temp_table} into {table}")
        merge_sql = f"""
            MERGE INTO {table} AS target
            USING (
                -- Use GROUP BY to ensure there are no duplicate rows before merge
                SELECT 
                    org_id,
                    {", ".join(unique_by)},
                    {", ".join([f"max({name}) as {name}" for name in fields if name not in unique_by])}, 
                    max(created_time) as created_time
                FROM {temp_table}
                GROUP BY org_id, {", ".join(unique_by)}
            ) AS source
            ON source.org_id = target.org_id 
                {" ".join(f"AND source.{i} = target.{i}" for i in unique_by)}
            WHEN MATCHED THEN
                UPDATE SET
                    target.created_time = source.created_time,
                    {",".join([f"target.{name} = source.{name}" for name in fields])}
            WHEN NOT MATCHED THEN
                INSERT (org_id, {", ".join(name for name in fields)}, created_time) 
                        VALUES (source.org_id, {", ".join(f"source.{name}" for name in fields)}, source.created_time)
        """
        snowflake_conn.cursor().execute(merge_sql)

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, Generator, List, Set, Tuple, Union

import oracledb

from amiadapters.adapters.base import BaseAMIAdapter, ScheduledExtract
from amiadapters.adapters.connections import open_ssh_tunnel
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader, RawSnowflakeTableLoader

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
        pipeline_configuration,
        configured_task_output_controller,
        ssh_tunnel_server_host,
        ssh_tunnel_username,
        ssh_tunnel_key_path,
        ssh_tunnel_private_key,
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
        ssh_tunnel_key_path = path to SSH private key for authentication to intermediate server. Used if no ssh_tunnel_private_key provided.
        ssh_tunnel_private_key = SSH private key for authentication to intermediate server (the intermediate server must know your public key already!)
        database_host = hostname or IP of the Oracle database
        database_port = port of Oracle database
        database_db_name = database name of Oracle database
        database_user = username for Oracle database
        database_password = password for Oracle database
        """
        self.ssh_tunnel_server_host = ssh_tunnel_server_host
        self.ssh_tunnel_username = ssh_tunnel_username
        self.ssh_tunnel_key_path = ssh_tunnel_key_path
        self.ssh_tunnel_private_key = ssh_tunnel_private_key
        self.database_host = database_host
        self.database_port = database_port
        self.database_db_name = database_db_name
        self.database_user = database_user
        self.database_password = database_password
        super().__init__(
            org_id,
            org_timezone,
            pipeline_configuration,
            configured_task_output_controller,
            configured_sinks,
            METERSENSE_RAW_SNOWFLAKE_LOADER,
        )

    def name(self) -> str:
        return f"metersense-{self.org_id}"

    def scheduled_extracts(self) -> List[ScheduledExtract]:
        """
        We've seen in some cases that Metersense meter reads aren't fully represented in the source until two days
        after the flowtime. We set our standard extract range to 3+ days to cover this lag.
        """
        return [
            ScheduledExtract(
                interval=timedelta(days=3),
            )
        ]

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
            ssh_tunnel_private_key=self.ssh_tunnel_private_key,
            remote_host=self.database_host,
            remote_port=self.database_port,
        ) as ctx:
            logger.info("Created SSH tunnel")
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

        raw_meters = self._read_file(extract_outputs, "meters.json", MetersenseMeter)
        meters_by_device_id = self._transform_meters(
            raw_meters,
            accounts_by_location_id,
            xrefs_by_meter_id,
            meter_views_by_meter_id,
            locations_by_location_id,
        )

        raw_interval_reads = self._read_file(
            extract_outputs, "intervalreads.json", MetersenseIntervalRead
        )
        raw_register_reads = self._read_file(
            extract_outputs, "registerreads.json", MetersenseRegisterRead
        )
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
        raw_account_services = self._read_file(
            extract_outputs, "account_services.json", MetersenseAccountService
        )
        accounts_by_location_id = {}
        for a in raw_account_services:
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
            extract_outputs, "meter_location_xref.json", MetersenseMeterLocationXref
        )
        xrefs_by_meter_id = {}
        for x in raw_meter_location_xrefs:
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
        raw_meters_views = self._read_file(
            extract_outputs, "meters_view.json", MetersenseMetersView
        )
        meter_views_by_meter_id = {}
        for mv in raw_meters_views:
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
        raw_locations = self._read_file(
            extract_outputs, "locations.json", MetersenseLocation
        )
        locations_by_location_id = {}
        for l in raw_locations:
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
        for raw_meter in raw_meters:
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
    ) -> Dict[str, GeneralMeterRead]:
        """
        Join interval and register reads together, join in meter metadata when possible. Only include
        reads for devices that we kept in the meter transform step.
        """
        reads_by_device_and_time = {}

        for raw_interval_read in raw_interval_reads:
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
                battery=None,
                install_date=None,
                connection=None,
                estimated=self._map_status_flag(raw_interval_read),
            )
            reads_by_device_and_time[key] = read

        for raw_register_read in raw_register_reads:
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
                    battery=None,
                    install_date=None,
                    connection=None,
                    estimated=self._map_status_flag(raw_register_read),
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

    def _map_status_flag(
        self, raw_read: Union[MetersenseIntervalRead, MetersenseRegisterRead]
    ) -> int:
        """
        Raw reads have a "status" indicator that tell us whether a read was estimated. The expected values are:

        1 = Passed Validation
        2 = Failed Validation
        3 = Estimated
        4 = This value appears not to be in use
        5 = Accepted

        Return 1 if the read was estimated, else 0.
        """
        return 1 if raw_read.status == "3" else 0

    def _read_file(
        self, extract_outputs: ExtractOutput, file: str, raw_dataclass
    ) -> Generator:
        """
        Read a file's contents from extract stage output, create generator
        for each line of text
        """
        lines = extract_outputs.load_from_file(file, raw_dataclass, allow_empty=True)
        yield from lines


class MetersenseRawAccountServicesLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "METERSENSE_ACCOUNT_SERVICES_BASE"

    def columns(self) -> List[str]:
        return list(MetersenseAccountService.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["account_id", "location_no", "inactive_dt"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "account_services.json", MetersenseAccountService
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class MetersenseRawLocationsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "METERSENSE_LOCATIONS_BASE"

    def columns(self) -> List[str]:
        return list(MetersenseLocation.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["location_no"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("locations.json", MetersenseLocation)
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class MetersenseRawMetersLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "METERSENSE_METERS_BASE"

    def columns(self) -> List[str]:
        return list(MetersenseMeter.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file("meters.json", MetersenseMeter)
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class MetersenseRawMetersViewLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "METERSENSE_METERS_VIEW_BASE"

    def columns(self) -> List[str]:
        return list(MetersenseMetersView.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "meters_view.json", MetersenseMetersView
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class MetersenseRawMeterLocationXrefsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "METERSENSE_METER_LOCATION_XREF_BASE"

    def columns(self) -> List[str]:
        return list(MetersenseMeterLocationXref.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id", "inactive_dt"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "meter_location_xref.json", MetersenseMeterLocationXref
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class MetersenseRawIntervalReadsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "METERSENSE_INTERVALREADS_BASE"

    def columns(self) -> List[str]:
        return list(MetersenseIntervalRead.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id", "read_dtm"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "intervalreads.json", MetersenseIntervalRead
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


class MetersenseRawRegisterReadsLoader(RawSnowflakeTableLoader):

    def table_name(self) -> str:
        return "METERSENSE_REGISTERREADS_BASE"

    def columns(self) -> List[str]:
        return list(MetersenseRegisterRead.__dataclass_fields__.keys())

    def unique_by(self) -> List[str]:
        return ["meter_id", "read_dtm"]

    def prepare_raw_data(self, extract_outputs):
        raw_data = extract_outputs.load_from_file(
            "registerreads.json", MetersenseRegisterRead
        )
        return [
            tuple(i.__getattribute__(name) for name in self.columns()) for i in raw_data
        ]


METERSENSE_RAW_SNOWFLAKE_LOADER = RawSnowflakeLoader.with_table_loaders(
    [
        MetersenseRawAccountServicesLoader(),
        MetersenseRawLocationsLoader(),
        MetersenseRawMetersLoader(),
        MetersenseRawMetersViewLoader(),
        MetersenseRawMeterLocationXrefsLoader(),
        MetersenseRawIntervalReadsLoader(),
        MetersenseRawRegisterReadsLoader(),
    ]
)

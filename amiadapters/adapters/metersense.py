from dataclasses import dataclass, replace
from datetime import datetime
import json
import logging
from typing import List, Tuple

import oracledb
import sshtunnel

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput

logger = logging.getLogger(__name__)


@dataclass
class MetersenseMeterLocation:
    """
    Representation of joined rows from the METERS, LOCATIONS, and ACCOUNT_SERVICES tables in Metersense schema.
    """

    meter_id: str
    alt_meter_id: str
    meter_tp: str
    meters_commodity_tp: str
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
    meters_latitude: str
    meters_longitude: str
    exclude_in_reports: str
    meters_add_by: str
    meters_add_dt: str
    meters_change_by: str
    meters_change_dt: str
    locations_location_no: str
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
    locations_add_by: str
    locations_add_dt: str
    locations_change_by: str
    locations_change_dt: str
    locations_latitude: str
    locations_longitude: str
    service_id: str
    account_id: str
    accounts_location_no: str
    accounts_commodity_tp: str
    last_read_dt: str
    active_dt: str
    inactive_dt: str


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
        raw_snowflake_loader=None,
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
            raw_snowflake_loader,
        )

    def name(self) -> str:
        return f"metersense-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
    ):
        with sshtunnel.open_tunnel(
            (self.ssh_tunnel_server_host),
            ssh_username=self.ssh_tunnel_username,
            ssh_pkey=self.ssh_tunnel_key_path,
            remote_bind_address=(self.database_host, self.database_port),
            # Locally, bind to localhost and arbitrary port. Use same host and port later when connecting to Oracle.
            local_bind_address=("0.0.0.0", 10209),
        ) as _:
            logging.info("Created SSH tunnel")
            connection = oracledb.connect(
                user=self.database_user,
                password=self.database_password,
                dsn=f"0.0.0.0:10209/{self.database_db_name}",
            )

            logger.info("Successfully connected to Oracle Database")

            cursor = connection.cursor()

            meters = self._extract_meters(cursor)
            register_reads = self._extract_register_reads(
                cursor, extract_range_start, extract_range_end
            )
            interval_reads = self._extract_interval_reads(
                cursor, extract_range_start, extract_range_end
            )

        # # TODO remove
        # with open("./output/metersense-cache-meter.json", "r") as f:
        #     text = f.read()
        #     meters = [
        #         MetersenseMeterLocation(**json.loads(i)) for i in text.split("\n")
        #     ]
        # with open("./output/metersense-cache-register-reads.json", "r") as f:
        #     text = f.read()
        #     register_reads = [
        #         MetersenseRegisterRead(**json.loads(i)) for i in text.split("\n")
        #     ]
        # with open("./output/metersense-cache-interval-reads.json", "r") as f:
        #     text = f.read()
        #     interval_reads = [
        #         MetersenseIntervalRead(**json.loads(i)) for i in text.split("\n")
        #     ]

        return ExtractOutput(
            {
                "meters.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in meters
                ),
                "interval_reads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in interval_reads
                ),
                "register_reads.json": "\n".join(
                    json.dumps(i, cls=DataclassJSONEncoder) for i in register_reads
                ),
            }
        )

    def _extract_meters(self, cursor) -> List[MetersenseMeterLocation]:
        # TODO remove meter_id filter
        result = cursor.execute(
            """
            SELECT
                m.meter_id,
                m.alt_meter_id,
                m.meter_tp,
                m.commodity_tp,
                m.region_id,
                m.interval_length,
                m.regread_frequency,
                m.channel1_raw_uom,
                m.channel2_raw_uom,
                m.channel3_raw_uom,
                m.channel4_raw_uom,
                m.channel5_raw_uom,
                m.channel6_raw_uom,
                m.channel7_raw_uom,
                m.channel8_raw_uom,
                m.channel1_multiplier,
                m.channel2_multiplier,
                m.channel3_multiplier,
                m.channel4_multiplier,
                m.channel5_multiplier,
                m.channel6_multiplier,
                m.channel7_multiplier,
                m.channel8_multiplier,
                m.channel1_final_uom,
                m.channel2_final_uom,
                m.channel3_final_uom,
                m.channel4_final_uom,
                m.channel5_final_uom,
                m.channel6_final_uom,
                m.channel7_final_uom,
                m.channel8_final_uom,
                m.first_data_ts,
                m.last_data_ts,
                m.ami_id,
                m.power_status,
                m.latitude,
                m.longitude,
                m.exclude_in_reports,
                m.add_by,
                m.add_dt,
                m.change_by,
                m.change_dt,
                l.location_no,
                l.alt_location_id,
                l.location_class,
                l.unit_no,
                l.street_no,
                l.street_pfx,
                l.street_name,
                l.street_sfx,
                l.street_sfx_dir,
                l.city,
                l.state,
                l.postal_cd,
                l.billing_cycle,
                l.add_by,
                l.add_dt,
                l.change_by,
                l.change_dt,
                l.latitude,
                l.longitude,
                a.service_id,
                a.account_id,
                a.location_no,
                a.commodity_tp,
                a.last_read_dt,
                a.active_dt,
                a.inactive_dt
            FROM METERS m
            LEFT JOIN meter_location_xref x ON m.meter_id = x.meter_id
            LEFT JOIN locations l ON l.location_no = x.location_no
            LEFT JOIN account_services a ON a.location_no = x.location_no
            WHERE m.commodity_tp = 'W'
            AND a.commodity_tp = 'W'
            AND x.inactive_dt > SYSTIMESTAMP
            AND a.inactive_dt > SYSTIMESTAMP
            AND m.meter_id = '87195806'
            ORDER BY m.meter_id, l.location_no, a.account_id, a.last_read_dt desc
                                """
        )
        result = cursor.fetchall()
        rows = [i for i in result]
        logger.info(f"Fetched {len(rows)} meter metadata rows")
        columns = list(MetersenseMeterLocation.__dataclass_fields__.keys())
        meters = []
        for row in rows:
            data = {}
            for name, value in zip(columns, row):
                if isinstance(value, datetime):
                    value = value.isoformat()
                data[name] = value
            meters.append(MetersenseMeterLocation(**data))
        return meters

    def _extract_register_reads(
        self, cursor, extract_range_start: datetime, extract_range_end: datetime
    ) -> List[MetersenseRegisterRead]:
        # TODO remove meter_id filter
        register_reads_result = cursor.execute(
            """
            SELECT
                r.meter_id,
                r.channel_id,
                r.read_dtm,
                r.read_value,
                r.uom,
                r.status,
                r.read_version
            FROM registerreads r
            WHERE r.meter_id IN (SELECT DISTINCT meter_id FROM meters m where m.commodity_tp = 'W')
            AND r.read_dtm > :range_start
            AND r.read_dtm < :range_end
            AND r.meter_id = '87195806'
        """,
            range_start=extract_range_start,
            range_end=extract_range_end,
        )
        register_read_rows = [i for i in register_reads_result]
        logger.info(f"Fetched {len(register_read_rows)} register read rows")
        register_read_columns = list(MetersenseRegisterRead.__dataclass_fields__.keys())
        register_reads = []
        for row in register_read_rows:
            data = {}
            for name, value in zip(register_read_columns, row):
                if isinstance(value, datetime):
                    value = value.isoformat()
                data[name] = value
            register_reads.append(MetersenseRegisterRead(**data))
        return register_reads

    def _extract_interval_reads(
        self, cursor, extract_range_start: datetime, extract_range_end: datetime
    ) -> List[MetersenseIntervalRead]:
        # TODO remove meter_id filter
        interval_reads_result = cursor.execute(
            """
            SELECT
                r.meter_id,
                r.channel_id,
                r.read_dt,
                r.read_hr,
                r.read_30min_int,
                r.read_15min_int,
                r.read_5min_int,
                r.read_dtm,
                r.read_value,
                r.uom,
                r.status,
                r.read_version
            FROM intervalreads r
            WHERE r.meter_id IN (SELECT DISTINCT meter_id FROM meters m where m.commodity_tp = 'W')
            AND r.read_dtm > :range_start
            AND r.read_dtm < :range_end
            AND r.meter_id = '87195806'
        """,
            range_start=extract_range_start,
            range_end=extract_range_end,
        )
        interval_reads_rows = [i for i in interval_reads_result]
        logger.info(f"Fetched {len(interval_reads_rows)} interval read rows")
        interval_read_columns = list(MetersenseIntervalRead.__dataclass_fields__.keys())
        interval_reads = []
        for row in interval_reads_rows:
            data = {}
            for name, value in zip(interval_read_columns, row):
                if isinstance(value, datetime):
                    value = value.isoformat()
                data[name] = value
            interval_reads.append(MetersenseIntervalRead(**data))
        return interval_reads

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        raw_meters = [
            MetersenseMeterLocation(**json.loads(d))
            for d in extract_outputs.from_file("meters.json").strip().split("\n")
        ]
        raw_interval_reads = [
            MetersenseIntervalRead(**json.loads(d))
            for d in extract_outputs.from_file("interval_reads.json")
            .strip()
            .split("\n")
        ]
        raw_register_reads = [
            MetersenseRegisterRead(**json.loads(d))
            for d in extract_outputs.from_file("register_reads.json")
            .strip()
            .split("\n")
        ]

        return self._transform_meters_and_reads(
            raw_meters, raw_interval_reads, raw_register_reads
        )

    def _transform_meters_and_reads(
        self,
        raw_meters: List[MetersenseMeterLocation],
        raw_interval_reads: List[MetersenseIntervalRead],
        raw_register_reads: List[MetersenseIntervalRead],
    ) -> Tuple[List[GeneralMeter], List[GeneralMeterRead]]:
        """
        Data questions:
        - for account_id,
        """
        meters_by_device_id = {}
        for raw_meter in raw_meters:
            device_id = raw_meter.meter_id
            if device_id in meters_by_device_id:
                continue
            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                # TODO account_id or service_id?
                account_id=raw_meter.account_id,
                location_id=raw_meter.locations_location_no,
                meter_id=raw_meter.meter_id,
                # TODO anything to populate here?
                endpoint_id=None,
                # TODO should we take into account meters_change_dt? Maybe if change date is less than today, we use it?
                meter_install_date=self.datetime_from_iso_str(
                    raw_meter.meters_add_dt, self.org_timezone
                ),
                # TODO anything to populate here?
                meter_size=None,
                # TODO anything to populate here?
                meter_manufacturer=None,
                multiplier=raw_meter.channel1_multiplier,
                location_address=raw_meter.street_name,
                location_city=raw_meter.city,
                location_state=raw_meter.state,
                location_zip=raw_meter.postal_cd,
            )
            meters_by_device_id[device_id] = meter

        reads_by_device_and_time = {}

        # TODO handle read_version, which may indicate duplicates in meter+time because they're versioned? Could maybe sort by version and make an assumption in this code based off that, e.g. take the first one and ignore the rest
        for raw_interval_read in raw_interval_reads:
            device_id = raw_interval_read.meter_id
            flowtime = self.datetime_from_iso_str(
                raw_interval_read.read_dtm, self.org_timezone
            )
            key = (
                device_id,
                flowtime,
            )

            # TODO this assumes that today's account and location are the same as yesterday's account and location, which may not be true for old reads
            device = meters_by_device_id.get(device_id)
            if device is not None:
                account_id = device.account_id
                location_id = device.location_id
            else:
                account_id = None
                location_id = None

            read = GeneralMeterRead(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                flowtime=flowtime,
                register_value=None,
                register_unit=None,
                interval_value=raw_interval_read.read_value,
                interval_unit=self.map_unit_of_measure(raw_interval_read.uom),
            )
            reads_by_device_and_time[key] = read

        for raw_register_read in raw_register_reads:
            device_id = raw_register_read.meter_id
            flowtime = self.datetime_from_iso_str(
                raw_register_read.read_dtm, self.org_timezone
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
                    register_value=raw_register_read.read_value,
                    register_unit=self.map_unit_of_measure(raw_register_read.uom),
                )
            else:
                # TODO this assumes that today's account and location are the same as yesterday's account and location, which may not be true for old reads
                device = meters_by_device_id.get(device_id)
                if device is not None:
                    account_id = device.account_id
                    location_id = device.location_id
                else:
                    account_id = None
                    location_id = None
                read = GeneralMeterRead(
                    org_id=self.org_id,
                    device_id=device_id,
                    account_id=account_id,
                    location_id=location_id,
                    flowtime=flowtime,
                    register_value=raw_register_read.read_value,
                    register_unit=self.map_unit_of_measure(raw_register_read.uom),
                    interval_value=None,
                    interval_unit=None,
                )
            reads_by_device_and_time[key] = read
        return list(meters_by_device_id.values()), list(
            reads_by_device_and_time.values()
        )

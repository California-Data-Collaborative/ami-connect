from dataclasses import dataclass
from datetime import datetime
import json
import logging
from typing import List

from amiadapters.base import BaseAMIAdapter
from amiadapters.models import DataclassJSONEncoder
from amiadapters.outputs.base import ExtractOutput

logger = logging.getLogger(__name__)


@dataclass
class MetersenseAccountService:
    """
    Representation of the ACCOUNT_SERVICES table in Metersense schema.
    """
    service_id: str
    account_id: str
    location_no: str
    commodity_tp: str
    last_read_dt: str
    active_dt: str
    inactive_dt: str



@dataclass
class MetersenseLocation:
    """
    Representation of the LOCATIONS table in Metersense schema.
    """
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
    """
    Representation of the METERS table in Metersense schema.
    """
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
class MetersenseMeterLocationXref:
    """
    Representation of the METER_LOCATION_XREF table in Metersense schema.
    """
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
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        configured_task_output_controller,
        configured_sinks=None,
        raw_snowflake_loader=None,
    ):
        super().__init__(
            org_id,
            org_timezone,
            configured_task_output_controller,
            configured_sinks,
            raw_snowflake_loader,
        )

    def name(self) -> str:
        return f"metersense-{self.org_id}"

    def extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
        device_ids: List[str] = None,
    ):
        import oracledb
        import sshtunnel

        
        

        with sshtunnel.open_tunnel(
            (ssh_tunnel_server_ip),
            ssh_pkey=ssh_key_path,
            remote_bind_address=(database_host, database_port),
            local_bind_address=('0.0.0.0', database_port)
        ) as tunnel:
            # client = paramiko.SSHClient()
            # client.load_system_host_keys()
            # client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # client.connect('127.0.0.1', 10022)
            # # do some operations with client session
            # client.close()
            logging.info("Created SSH tunnel")
            connection = oracledb.connect(
                user=database_user, password=database_password, dsn=f"0.0.0.0:{database_port}/{database_name}"
            )

        print("Successfully connected to Oracle Database")

        cursor = connection.cursor()

        # Create a table

        cursor.execute(
            """
            select * from test
            """
        )
        result = cursor.fetchall()
        print(result)
        meters = [
            MetersenseMeter(
                meter_id="89671712",
                alt_meter_id="S89671712",
                meter_tp="W-DISC34",
                commodity_tp="W",
                region_id="California",
                interval_length="60",
                regread_frequency="1440",
                channel1_raw_uom="CF",
                channel2_raw_uom=None,
                channel3_raw_uom=None,
                channel4_raw_uom=None,
                channel5_raw_uom=None,
                channel6_raw_uom=None,
                channel7_raw_uom=None,
                channel8_raw_uom=None,
                channel1_multiplier="0.01",
                channel2_multiplier=None,
                channel3_multiplier=None,
                channel4_multiplier=None,
                channel5_multiplier=None,
                channel6_multiplier=None,
                channel7_multiplier=None,
                channel8_multiplier=None,
                channel1_final_uom="CCF",
                channel2_final_uom=None,
                channel3_final_uom=None,
                channel4_final_uom=None,
                channel5_final_uom=None,
                channel6_final_uom=None,
                channel7_final_uom=None,
                channel8_final_uom=None,
                first_data_ts="2023-02-18 00:00:00.000",
                last_data_ts="2025-06-15 00:00:00.000",
                ami_id="default",
                power_status="ON",
                latitude="33.8252339",
                longitude="118.1034094",
                exclude_in_reports="N",
                add_by="ODS",
                add_dt="2023-02-17 23:12:13.000",
                change_by="ODS",
                change_dt="2025-06-15 02:22:07.000"
            ),
        ]

        meter_location_xrefs = [
            MetersenseMeterLocationXref(
                meter_id="89671712",
                active_dt="2023-02-17 00:00:00.000",
                location_no="6523400466",
                inactive_dt="2023-12-25 00:00:00.000",
                add_by="ODS",
                add_dt="2024-01-04 23:12:17.000",
                change_by="ODS",
                change_dt="2024-01-04 23:12:17.000"
            ),
        ]

        account_services = [
            MetersenseAccountService(
                service_id="4453033580",
                account_id="445303358044515438576523400466",
                location_no="6523400466",
                commodity_tp="W",
                last_read_dt="2023-12-25 00:00:00.000",
                active_dt="2021-03-01 00:00:00.000",
                inactive_dt="9999-12-31 00:00:00.000"
            ),
        ]

        locations = [
            MetersenseLocation(
                location_no="6523400466",
                location_id="6523400466",
                location_tp="SFD",
                location_nm=None,
                location_nm2=None,
                address1="3730 LOS COYOTES DIA",
                address2=None,
                city="LONG BEACH",
                state="CA",
                zip="90808-2409",
                county=None,
                add_by="ODS",
                add_dt="2019-10-24 11:47:48.000",
                change_by="ODS",
                change_dt="2025-06-10 23:13:20.000",
                latitude="33.825194555",
                longitude="118.10331495"
            ),
        ]

        register_reads = [
            MetersenseRegisterRead(
                meter_id="89671712",
                channel_id="1",
                read_dtm="2025-06-14 23:00:00.000",
                read_value="171.697000",
                uom="CCF",
                read_version="1",
                read_multiplier="1"
            ),
        ]
        interval_reads = [
            MetersenseIntervalRead(
                meter_id="89671712",
                channel_id="1",
                read_date="2025-06-14",
                read_hour="23",
                daylight_savings_ind="0",
                interval_number="0",
                interval_status="0",
                interval_dtm="2025-06-14 23:00:00.000",
                interval_value="0.003000",
                uom="CCF",
                read_version="1",
                interval_multiplier="1"
            ),
            MetersenseIntervalRead(
                meter_id="89671712",
                channel_id="1",
                read_date="2025-06-14",
                read_hour="24",
                daylight_savings_ind="0",
                interval_number="0",
                interval_status="0",
                interval_dtm="2025-06-14 23:00:00.000",
                interval_value="0.000000",
                uom="CCF",
                read_version="1",
                interval_multiplier="1"
            ),
        ]
        self.output_controller.write_extract_outputs(
            run_id,
            ExtractOutput({
                "meters.json": "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in meters),
                "account_services.json": "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in account_services),
                "locations.json": "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in locations),
                "meter_location_xrefs.json": "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in meter_location_xrefs),
                "interval_reads.json": "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in interval_reads),
                "register_reads.json": "\n".join(json.dumps(i, cls=DataclassJSONEncoder) for i in register_reads),
            }),
        )

    def transform(self, run_id: str):
        extract_outputs = self.output_controller.read_extract_outputs(run_id)
        raw_account_services = {
            MetersenseAccountService(**json.loads(d)) for d in extract_outputs.from_file("account_services.json").strip().split("\n")
        }
        raw_locations = [
            MetersenseLocation(**json.loads(d)) for d in extract_outputs.from_file("locations.json").strip().split("\n")
        ]
        raw_meters = [
            MetersenseMeter(**json.loads(d)) for d in extract_outputs.from_file("meters.json").strip().split("\n")
        ]
        raw_meter_location_xrefs = [
            MetersenseMeterLocationXref(**json.loads(d)) for d in extract_outputs.from_file("meter_location_xrefs.json").strip().split("\n")
        ]

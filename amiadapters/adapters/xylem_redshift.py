from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
import logging
import json
from typing import Dict, Generator, List, Tuple

import psycopg2
import sshtunnel

from amiadapters.adapters.base import BaseAMIAdapter, GeneralMeterUnitOfMeasure
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput

logger = logging.getLogger(__name__)


@dataclass
class XRMeter:
    id: str
    account_rate_code: str
    service_address: str
    meter_status: str
    ert_id: str
    meter_id: str
    meter_id_2: str
    meter_manufacturer: str
    number_of_dials: str
    spd_meter_mult: str
    spd_meter_size: str
    spd_usage_uom: str
    service_point: str
    asset_number: str
    start_date: str
    end_date: str
    is_current: str
    batch_id: str


@dataclass
class XRServicePoint:
    service_address: str
    service_point: str
    account_billing_cycle: str
    read_cycle: str
    asset_address: str
    asset_city: str
    asset_zip: str
    sdp_id: str
    sdp_lat: str
    sdp_lon: str
    service_route: str
    start_date: str
    end_date: str
    is_current: str
    batch_id: str


@dataclass
class XRAmi:
    id: str
    encid: str
    datetime: str
    code: str
    consumption: str
    service_address: str
    service_point: str
    batch_id: str
    meter_serial_id: str
    ert_id: str


class XylemRedshiftAdapter(BaseAMIAdapter):
    """
    AMI Adapter that retrieves Xylem/Sensus data from a Redshift database.
    The Redshift database is only accessible through an SSH tunnel. This code assumes the tunnel
    infrastructure exists and connects to Redshift through SSH to an intermediate server.

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
        database_host = hostname or IP of the Redshift database
        database_port = port of Redshift database
        database_db_name = database name of Redshift database
        database_user = username for Redshift database
        database_password = password for Redshift database
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
            None,
        )

    def name(self) -> str:
        return f"xylem-redshift-{self.org_id}"

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
            # Locally, bind to localhost and arbitrary port. Use same host and port later when connecting to Redshift.
            local_bind_address=("0.0.0.0", 10209),
        ) as _:
            logging.info("Created SSH tunnel")
            connection = psycopg2.connect(
                user=self.database_user,
                password=self.database_password,
                host="0.0.0.0",
                port=10209,
                dbname=self.database_db_name,
            )

            logger.info("Successfully connected to Redshift Database")

            cursor = connection.cursor()

            files = self._query_tables(cursor, extract_range_start, extract_range_end)

        return ExtractOutput(files)

    def _query_tables(
        self, cursor, extract_range_start: datetime, extract_range_end: datetime
    ) -> Dict[str, str]:
        """
        Run SQL on remote Redshift database to extract all data. We've chosen to do as little
        filtering and joining as possible to preserve the raw data. It comes out in extract
        files per table.
        """
        files = {}
        tables = [
            ("meter", XRMeter, None, None),
            ("service_point", XRServicePoint, None, None),
            ("ami", XRAmi, extract_range_start, extract_range_end),
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
        query = f"SELECT * FROM {table_name} t WHERE 1=1 "
        kwargs = {}

        # Reads should be filtered by date range
        if extract_range_start and extract_range_end:
            query += f" AND t.datetime BETWEEN %(extract_range_start)s AND %(extract_range_end)s "
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
                # Cast some values to serializable types
                if isinstance(value, Decimal):
                    value = float(value)
                if isinstance(value, date):
                    value = value.isoformat()
                data[name] = value
            result.append(row_type(**data))

        logger.info(f"Fetched {len(result)} rows from {table_name}")
        return result

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        raw_meters_by_id = self._meters_by_meter_id(extract_outputs)
        service_points_by_ids = self._service_points_by_ids(extract_outputs)
        reads_by_meter_id = self._reads_by_meter_id(extract_outputs)

        meters_by_id = {}
        reads_by_device_and_time = {}

        for meter_id, raw_meters in raw_meters_by_id.items():
            # We take the first meter in the list, which is the most recently active
            raw_meter = raw_meters[0]

            device_id = raw_meter.meter_id
            service_point = service_points_by_ids.get(
                (raw_meter.service_address, raw_meter.service_point)
            )
            raw_reads = reads_by_meter_id.get(raw_meter.meter_id)

            # TODO maybe should be service_address?
            account_id = None
            location_id = service_point.service_address if service_point else None

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                meter_id=raw_meter.meter_id,
                # TODO check
                endpoint_id=raw_meter.ert_id,
                meter_install_date=self.datetime_from_iso_str(
                    raw_meter.start_date, self.org_timezone
                ),
                meter_size=self.map_meter_size(str(raw_meter.spd_meter_size)),
                meter_manufacturer=raw_meter.meter_manufacturer,
                multiplier=raw_meter.spd_meter_mult,
                location_address=service_point.asset_address if service_point else None,
                location_city=service_point.asset_city if service_point else None,
                location_state=None,
                location_zip=service_point.asset_zip if service_point else None,
            )
            meters_by_id[device_id] = meter

            if raw_reads:
                for raw_read in raw_reads:
                    if "T" in raw_read.datetime:
                        flowtime = datetime.strptime(
                            raw_read.datetime, "%Y-%m-%dT%H:%M:%S%z"
                        )
                    else:
                        flowtime = datetime.strptime(
                            raw_read.datetime, "%Y-%m-%d %H:%M:%S.%f %z"
                        )
                    key = (
                        device_id,
                        flowtime,
                    )
                    interval_value, interval_unit = self.map_reading(
                        # TODO confirm original unit
                        float(raw_read.consumption),
                        GeneralMeterUnitOfMeasure.CUBIC_FEET,
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

        return list(meters_by_id.values()), list(reads_by_device_and_time.values())

    def _meters_by_meter_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[XRMeter]]:
        """
        Map each meter ID to the list of meters associated with it. The list is sorted with most recently active
        meter first.
        """
        raw_meters = self._read_file(extract_outputs, "meter.json")

        # Build map
        meters_by_id = {}
        for m in raw_meters:
            meter = XRMeter(**json.loads(m))
            if meter.meter_id not in meters_by_id:
                meters_by_id[meter.meter_id] = []
            meters_by_id[meter.meter_id].append(meter)

        # Sort each meter_id's meters
        for meter_id in meters_by_id.keys():
            meters_by_id[meter_id] = sorted(
                meters_by_id[meter_id],
                # Sort so that most recent end_date is first. Ties are broken by start_date.
                key=lambda m: (m.end_date, m.start_date),
                reverse=True,
            )
        return meters_by_id

    def _service_points_by_ids(
        self, extract_outputs: ExtractOutput
    ) -> Dict[Tuple[str, str], XRServicePoint]:
        """
        Create a map of service points by their unique service_address+service_point.
        """
        raw_service_points = self._read_file(extract_outputs, "service_point.json")
        result = {}
        for sp in raw_service_points:
            service_point = XRServicePoint(**json.loads(sp))
            result[(service_point.service_address, service_point.service_point)] = (
                service_point
            )
        return result

    def _reads_by_meter_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[XRAmi]]:
        """
        Map each meter ID to the list of reads associated with it.
        """
        raw_reads = self._read_file(extract_outputs, "ami.json")

        result = {}
        for r in raw_reads:
            read = XRAmi(**json.loads(r))
            if read.meter_serial_id not in result:
                result[read.meter_serial_id] = []
            result[read.meter_serial_id].append(read)

        return result

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

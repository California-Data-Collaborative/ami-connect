from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
import logging
import json
from typing import Dict, Generator, List, Set, Tuple, Union

import psycopg2
from pytz.tzinfo import DstTzInfo

from amiadapters.adapters.base import BaseAMIAdapter, GeneralMeterUnitOfMeasure
from amiadapters.adapters.connections import open_ssh_tunnel
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from amiadapters.storage.snowflake import RawSnowflakeLoader

logger = logging.getLogger(__name__)


@dataclass
class Meter:
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
class ServicePoint:
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
class Ami:
    """
    Row in "ami" table which contains interval reads.
    """

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


@dataclass
class RegisterRead:
    id: str
    encid: str
    datetime: str
    code: str
    reg_read: str
    service_address: str
    service_point: str
    batch_id: str
    meter_serial_id: str
    ert_id: str


@dataclass
class Customer:
    id: str
    account_id: str
    account_rate_code: str
    service_type: str
    account_status: str
    service_address: str
    customer_number: str
    customer_cell_phone: str
    customer_email: str
    customer_home_phone: str
    customer_name: str
    billing_format_code: str
    start_date: str
    end_date: str
    is_current: str
    batch_id: str


class XylemMoultonNiguelAdapter(BaseAMIAdapter):
    """
    AMI Adapter that retrieves Xylem/Sensus data from a Redshift database for the Moulton Niguel Water District.
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
            XylemMoultonNiguelRawSnowflakeLoader(),
        )

    def name(self) -> str:
        return f"xylem-moulton-niguel-{self.org_id}"

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
            connection = psycopg2.connect(
                user=self.database_user,
                password=self.database_password,
                host="0.0.0.0",
                port=ctx.local_bind_port,
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
            ("meter", Meter, None, None),
            ("service_point", ServicePoint, None, None),
            ("customer", Customer, None, None),
            ("ami", Ami, extract_range_start, extract_range_end),
            ("register_read", RegisterRead, extract_range_start, extract_range_end),
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
        customers_by_service_address = self._customers_by_service_address(
            extract_outputs
        )

        meters_by_id = {}
        reads = []

        for meter_id, raw_meters in raw_meters_by_id.items():
            # We take the first meter in the list, which is the most recently active
            raw_meter = raw_meters[0]

            device_id = raw_meter.meter_id
            service_point = service_points_by_ids.get(
                (raw_meter.service_address, raw_meter.service_point)
            )

            account_id = None
            if customers := customers_by_service_address.get(raw_meter.service_address):
                # Take the first, which is most recently active
                account_id = customers[0].account_id

            location_id = None
            if service_point is not None:
                location_id = self._create_location_id(
                    service_point.service_address, service_point.service_point
                )

            meter = GeneralMeter(
                org_id=self.org_id,
                device_id=device_id,
                account_id=account_id,
                location_id=location_id,
                meter_id=raw_meter.meter_id,
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

        reads = self._transform_reads(
            raw_meters_by_id,
            customers_by_service_address,
            set(meters_by_id.keys()),
            [
                Ami(**json.loads(r))
                for r in self._read_file(extract_outputs, "ami.json")
            ],
            [
                RegisterRead(**json.loads(r))
                for r in self._read_file(extract_outputs, "register_read.json")
            ],
        )

        return list(meters_by_id.values()), reads

    def _transform_reads(
        self,
        raw_meters_by_id: Dict[str, List[Meter]],
        customers_by_service_address: Dict[str, List[Customer]],
        meter_ids_to_include: Set[str],
        interval_reads: List[Ami],
        register_reads: List[RegisterRead],
    ) -> List[GeneralMeterRead]:
        """
        Join reads together and attach the metadata from the time the read was taken.
        """
        reads_by_device_and_time = {}

        # Create a record for every interval read
        for raw_interval_read in interval_reads:
            if raw_interval_read.encid not in meter_ids_to_include:
                continue

            flowtime = self._parse_flowtime(raw_interval_read.datetime)
            interval_value, interval_unit = self.map_reading(
                float(raw_interval_read.consumption),
                GeneralMeterUnitOfMeasure.CUBIC_FEET,
            )
            device_id = raw_interval_read.encid
            location_id, account_id = self._matching_metadata_for_read(
                raw_interval_read, raw_meters_by_id, customers_by_service_address
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
                estimated=None,
            )
            reads_by_device_and_time[(device_id, flowtime)] = read

        # For every register read, join it to the matching record. If no match,
        # create a new record.
        for raw_register_read in register_reads:
            if raw_register_read.encid not in meter_ids_to_include:
                continue
            device_id = raw_register_read.encid
            flowtime = self._parse_flowtime(raw_register_read.datetime)
            register_value, register_unit = self.map_reading(
                float(raw_register_read.reg_read),
                GeneralMeterUnitOfMeasure.CUBIC_FEET,
            )
            if (device_id, flowtime) in reads_by_device_and_time:
                # Join register read onto the interval read object
                existing_read = reads_by_device_and_time[(device_id, flowtime)]
                read = replace(
                    existing_read,
                    register_value=register_value,
                    register_unit=register_unit,
                )
            else:
                location_id, account_id = self._matching_metadata_for_read(
                    raw_register_read, raw_meters_by_id, customers_by_service_address
                )
                # Create a new one
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
                    estimated=None,
                )
            reads_by_device_and_time[(device_id, flowtime)] = read

        return list(reads_by_device_and_time.values())

    def _matching_metadata_for_read(
        self,
        read: Union[Ami, RegisterRead],
        raw_meters_by_id,
        customers_by_service_address,
    ):
        potential_meters = raw_meters_by_id.get(read.encid, [])
        matching_meter = None
        for meter in potential_meters:
            if (
                read.ert_id == meter.ert_id
                and meter.start_date <= read.datetime < meter.end_date
            ):
                matching_meter = meter
                break

        location_id = None
        if matching_meter is not None:
            location_id = self._create_location_id(
                matching_meter.service_address, matching_meter.service_point
            )

        account_id = None
        if matching_meter:
            potential_customers = customers_by_service_address.get(
                matching_meter.service_address, []
            )
            for customer in potential_customers:
                if customer.start_date <= read.datetime < customer.end_date:
                    account_id = customer.account_id
                    break
        return location_id, account_id

    def _create_location_id(self, service_address: str, service_point: str) -> str:
        return f"{service_address}-{service_point}"

    def _parse_flowtime(self, raw_flowtime: str) -> datetime:
        if "T" in raw_flowtime:
            return datetime.strptime(raw_flowtime, "%Y-%m-%dT%H:%M:%S%z")
        else:
            return datetime.strptime(raw_flowtime, "%Y-%m-%d %H:%M:%S.%f %z")

    def _meters_by_meter_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[Meter]]:
        """
        Map each meter ID to the list of meters associated with it. The list is sorted with most recently active
        meter first.
        """
        raw_meters = self._read_file(extract_outputs, "meter.json")

        # Build map
        meters_by_id = {}
        for m in raw_meters:
            meter = Meter(**json.loads(m))
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
    ) -> Dict[Tuple[str, str], ServicePoint]:
        """
        Create a map of service points by their unique service_address+service_point.
        """
        raw_service_points = self._read_file(extract_outputs, "service_point.json")
        result = {}
        for sp in raw_service_points:
            service_point = ServicePoint(**json.loads(sp))
            result[(service_point.service_address, service_point.service_point)] = (
                service_point
            )
        return result

    def _customers_by_service_address(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[Customer]]:
        """
        Create a map of service addresses to the list of customers at that service address. There
        can be many customers per service address - they are sorted by end_date desc.
        """
        raw_customers = self._read_file(extract_outputs, "customer.json")
        result = {}
        for c in raw_customers:
            customer = Customer(**json.loads(c))
            if customer.service_address not in result:
                result[customer.service_address] = []
            result[customer.service_address].append(customer)

        # Sort each service_address's customers
        for service_address in result.keys():
            result[service_address] = sorted(
                result[service_address],
                # Sort so that most recent end_date is first. Ties are broken by start_date.
                key=lambda c: (c.end_date, c.start_date),
                reverse=True,
            )
        return result

    def _interval_reads_by_meter_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[Ami]]:
        """
        Map each meter ID to the list of interval reads associated with it.
        """
        raw_reads = self._read_file(extract_outputs, "ami.json")

        result = {}
        for r in raw_reads:
            read = Ami(**json.loads(r))
            if read.encid not in result:
                result[read.encid] = []
            result[read.encid].append(read)

        return result

    def _register_reads_by_meter_id(
        self, extract_outputs: ExtractOutput
    ) -> Dict[str, List[RegisterRead]]:
        """
        Map each meter ID to the list of register reads associated with it.
        """
        raw_reads = self._read_file(extract_outputs, "register_read.json")

        result = {}
        for r in raw_reads:
            read = RegisterRead(**json.loads(r))
            if read.encid not in result:
                result[read.encid] = []
            result[read.encid].append(read)

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


class XylemMoultonNiguelRawSnowflakeLoader(RawSnowflakeLoader):

    def load(self, *args):
        self._load_raw_meters(*args)
        self._load_raw_service_points(*args)
        self._load_raw_customers(*args)
        self._load_raw_ami(*args)
        self._load_raw_register_reads(*args)

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
            extract_output_filename="meter.json",
            raw_dataclass=Meter,
            table="XYLEM_MOULTON_NIGUEL_METER_BASE",
            unique_by=["ID"],
        )

    def _load_raw_service_points(
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
            extract_output_filename="service_point.json",
            raw_dataclass=ServicePoint,
            table="XYLEM_MOULTON_NIGUEL_SERVICE_POINT_BASE",
            unique_by=["service_address", "service_point"],
        )

    def _load_raw_customers(
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
            extract_output_filename="customer.json",
            raw_dataclass=Customer,
            table="XYLEM_MOULTON_NIGUEL_CUSTOMER_BASE",
            unique_by=["id"],
        )

    def _load_raw_ami(
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
            extract_output_filename="ami.json",
            raw_dataclass=Ami,
            table="XYLEM_MOULTON_NIGUEL_AMI_BASE",
            unique_by=["id"],
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
            extract_output_filename="register_read.json",
            raw_dataclass=RegisterRead,
            table="XYLEM_MOULTON_NIGUEL_REGISTER_READ_BASE",
            unique_by=["id"],
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
        raw_dataclass: e.g. Meter, used to deserialize raw data and determine table column names
        table: name of raw data table in Snowflake
        unique_by: list of field names used with org_id to uniquely identify a row in the base table
        """
        text = extract_outputs.from_file(extract_output_filename)
        if not text:
            logger.info(
                f"No data found for {extract_output_filename}, skipping raw load"
            )
            return
        raw_data = [raw_dataclass(**json.loads(d)) for d in text.strip().split("\n")]
        temp_table = f"temp_{table}"
        fields = [f.lower() for f in raw_dataclass.__dataclass_fields__.keys()]
        unique_by = [u.lower() for u in unique_by]
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
                FROM {temp_table} t
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

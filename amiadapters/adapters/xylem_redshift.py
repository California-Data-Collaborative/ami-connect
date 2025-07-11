from dataclasses import dataclass, replace
from datetime import datetime
import logging

import oracledb
import sshtunnel

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput

logger = logging.getLogger(__name__)


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
        raise Exception("hey")
        # with sshtunnel.open_tunnel(
        #     (self.ssh_tunnel_server_host),
        #     ssh_username=self.ssh_tunnel_username,
        #     ssh_pkey=self.ssh_tunnel_key_path,
        #     remote_bind_address=(self.database_host, self.database_port),
        #     # Locally, bind to localhost and arbitrary port. Use same host and port later when connecting to Oracle.
        #     local_bind_address=("0.0.0.0", 10209),
        # ) as _:
        #     logging.info("Created SSH tunnel")
        #     connection = oracledb.connect(
        #         user=self.database_user,
        #         password=self.database_password,
        #         dsn=f"0.0.0.0:10209/{self.database_db_name}",
        #     )

        #     logger.info("Successfully connected to Oracle Database")

        #     cursor = connection.cursor()

        #     files = self._query_tables(cursor, extract_range_start, extract_range_end)

        # return ExtractOutput(files)

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        return [], []

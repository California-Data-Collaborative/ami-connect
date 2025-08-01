import logging

import sshtunnel

logger = logging.getLogger(__name__)


def open_ssh_tunnel(
    ssh_tunnel_server_host: str,
    ssh_tunnel_username: str,
    ssh_tunnel_key_path: str,
    remote_host: str,
    remote_port: str,
):
    """
    Utility function for opening an SSH tunnel connection.
    """
    return sshtunnel.open_tunnel(
        (ssh_tunnel_server_host),
        ssh_username=ssh_tunnel_username,
        ssh_pkey=ssh_tunnel_key_path,
        remote_bind_address=(remote_host, remote_port),
        # Locally, bind to localhost and arbitrary port.
        # Use same host and port later when connecting to Redshift.
        # Using port "0" will automatically choose a free port. Access
        # it with the local_bind_port property of this returned context.
        local_bind_address=("0.0.0.0", 0),
    )

import logging

import paramiko
import sshtunnel
import tempfile

logger = logging.getLogger(__name__)


def open_ssh_tunnel(
    ssh_tunnel_server_host: str,
    ssh_tunnel_username: str,
    ssh_tunnel_key_path: str,
    remote_host: str,
    remote_port: str,
    ssh_tunnel_private_key: str = None,
):
    """
    Utility function for opening an SSH tunnel connection.

    Tries to use an in-memory SSH key string if provided, otherwise falls back to using a key file path.
    """
    if ssh_tunnel_private_key is not None:
        logger.info("Using SSH key from string for SSH tunnel connection.")
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.write(ssh_tunnel_private_key.encode("utf-8"))
        tmp.flush()
        tmp.close()
        with open(tmp.name) as f:
            pkey = paramiko.RSAKey.from_private_key(f)
    else:
        logger.info(
            f"Using SSH key from path {ssh_tunnel_key_path} for SSH tunnel connection."
        )
        pkey = ssh_tunnel_key_path

    return sshtunnel.open_tunnel(
        (ssh_tunnel_server_host),
        ssh_username=ssh_tunnel_username,
        ssh_pkey=pkey,
        remote_bind_address=(remote_host, remote_port),
        # Locally, bind to localhost and arbitrary port.
        # Use same host and port later when connecting to Redshift.
        # Using port "0" will automatically choose a free port. Access
        # it with the local_bind_port property of this returned context.
        local_bind_address=("0.0.0.0", 0),
        logger=logger,
    )

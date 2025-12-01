from unittest import mock

from amiadapters.adapters import connections
from test.base_test_case import BaseTestCase


class TestOpenSSHTunnel(BaseTestCase):

    @mock.patch("amiadapters.adapters.connections.sshtunnel.open_tunnel")
    def test_open_ssh_tunnel_with_key_path(self, mock_open_tunnel):
        ssh_tunnel_server_host = "example.com"
        ssh_tunnel_username = "user"
        ssh_tunnel_key_path = "/path/to/key"
        remote_host = "remotehost"
        remote_port = 5432

        connections.open_ssh_tunnel(
            ssh_tunnel_server_host,
            ssh_tunnel_username,
            ssh_tunnel_key_path,
            remote_host,
            remote_port,
        )

        mock_open_tunnel.assert_called_once_with(
            (ssh_tunnel_server_host),
            ssh_username=ssh_tunnel_username,
            ssh_pkey=ssh_tunnel_key_path,
            remote_bind_address=(remote_host, remote_port),
            local_bind_address=("0.0.0.0", 0),
        )

    @mock.patch("amiadapters.adapters.connections.sshtunnel.open_tunnel")
    @mock.patch("amiadapters.adapters.connections.paramiko.RSAKey.from_private_key")
    def test_open_ssh_tunnel_with_private_key_string(
        self, mock_from_private_key, mock_open_tunnel
    ):
        ssh_tunnel_server_host = "example.com"
        ssh_tunnel_username = "user"
        ssh_tunnel_key_path = "/path/to/key"
        remote_host = "remotehost"
        remote_port = 5432
        ssh_tunnel_private_key = "PRIVATE KEY DATA"

        mock_key = mock.Mock()
        mock_from_private_key.return_value = mock_key

        connections.open_ssh_tunnel(
            ssh_tunnel_server_host,
            ssh_tunnel_username,
            ssh_tunnel_key_path,
            remote_host,
            remote_port,
            ssh_tunnel_private_key=ssh_tunnel_private_key,
        )

        mock_open_tunnel.assert_called_once_with(
            (ssh_tunnel_server_host),
            ssh_username=ssh_tunnel_username,
            ssh_pkey=mock_key,
            remote_bind_address=(remote_host, remote_port),
            local_bind_address=("0.0.0.0", 0),
        )

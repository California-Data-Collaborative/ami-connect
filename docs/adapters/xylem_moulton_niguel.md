# Xylem Moulton Niguel

The Xylem Moulton Niguel adapter retrieves Xylem/Sensus data from a Redshift database built specifically for the Moulton Niguel Water District. The database is only accessible through an SSH tunnel. The adapter assumes the tunnel infrastructure already exists and that it's possible to connect to the database through the SSH tunnel.

This adapter was built specially for MNWD and is not compatible for other utilities.

## Configuration

- ssh_tunnel_server_host: Hostname or IP of intermediate server used to SSH tunnel
- ssh_tunnel_key_path: Path to local SSH private key for authentication to intermediate server (the intermediate server must know your public key already!)
- database_host: Hostname or IP of the database with AMI data
- database_port: Port of the database with AMI data

Example:
```
python cli.py config add-source my_utility xylem_moulton_niguel America/Los_Angeles --ssh-tunnel-server-host my-tunnel-host --ssh-tunnel-key-path ./key --database-host my-db-host --database-port 1521 --sinks my_snowflake
```

## Secrets

- ssh_tunnel_username: SSH username for SSH to intermediate server
- database_db_name: Database name of Oracle database
- database_user: Username for Oracle database
- database_password: Password for Oracle database

Example:
```
python cli.py config update-secret my_utility --source-type xylem_moulton_niguel --ssh-tunnel-username my_user --database-db-name my_database --database-user my_db_user --database-password my_db_password
```

## Limitations

N/A
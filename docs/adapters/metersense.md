# Metersense

The Metersense adapter retrieves Xylem/Sensus data from a Metersense Oracle database. The Oracle database is only accessible through an SSH tunnel. The adapter assumes the tunnel infrastructure already exists and that it's possible to connect to Oracle through the SSH tunnel.

Some setup outside of AMI Connect may include:
- Adding your Airflow server's public SSH key to the intermediate server's allowed hosts
- Adding your Airflow server's public IP address to a security group that allows SSH into the intermediate server

## Configuration

- ssh_tunnel_server_host: Hostname or IP of intermediate server used to SSH tunnel
- ssh_tunnel_key_path: Path to local SSH private key for authentication to intermediate server (the intermediate server must know your public key already!)
- database_host: Hostname or IP of the Oracle database with AMI data
- database_port: Port of the Oracle database with AMI data

Example:
```
python cli.py config add-source my_utility metersense America/Los_Angeles --ssh-tunnel-server-host my-tunnel-host --ssh-tunnel-key-path ./key --database-host my-db-host --database-port 1521 --sinks my_snowflake
```

Secrets:
```yaml
sources:
 my_utility:
  # SSH username for SSH to intermediate server
  ssh_tunnel_username: ubuntu
  # Database name of Oracle database
  database_db_name: db-name
  # Username for Oracle database
  database_user: dbu
  # Password for Oracle database
  database_password: dbp
```

## Limitations

N/A
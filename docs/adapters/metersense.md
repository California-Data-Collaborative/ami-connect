# Metersense

The Metersense adapter retrieves Xylem/Sensus data from a Metersense Oracle database. The Oracle database is only accessible through an SSH tunnel. The adapter assumes the tunnel infrastructure already exists and that it's possible to connect to Oracle through the SSH tunnel.

Some setup outside of AMI Connect may include:
- Adding your Airflow server's public SSH key to the intermediate server's allowed hosts
- Adding your Airflow server's public IP address to a security group that allows SSH into the intermediate server

## Configuration

Example:
```yaml
sources:
- type: metersense
  org_id: my_utility
  timezone: America/Los_Angeles
  # Hostname or IP of intermediate server used to SSH tunnel
  ssh_tunnel_server_host: tunnel-ip
  # Path to local SSH private key for authentication to intermediate server (the intermediate server must know your public key already!)
  ssh_tunnel_key_path: /key
  # Hostname or IP of the Oracle database with AMI data
  database_host: db-host
  database_port: 1521
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
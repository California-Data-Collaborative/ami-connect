# Xylem Moulton Niguel

The Xylem Moulton Niguel adapter retrieves Xylem/Sensus data from a Redshift database built specifically for the Moulton Niguel Water District. The database is only accessible through an SSH tunnel. The adapter assumes the tunnel infrastructure already exists and that it's possible to connect to the database through the SSH tunnel.

This adapter was built specially for MNWD and is not compatible for other utilities.

## Configuration

Example:
```yaml
sources:
- type: xylem_moulton_niguel
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
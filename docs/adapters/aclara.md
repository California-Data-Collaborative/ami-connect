# Aclara

The Aclara adapter uses SFTP to retrieve meter read data from an Aclara server.

## Configuration

Example:
```yaml
sources:
- type: aclara
  org_id: my_utility
  timezone: America/Los_Angeles
  # Server where data lives
  sftp_host: example.com
  # Directory on remote server where data lives
  sftp_remote_data_directory: ./data
  # Local directory where we'll download the data
  sftp_local_download_directory: ./output
  # Local SSH known hosts file
  sftp_local_known_hosts_file: ./known-hosts
```

Secrets:
```yaml
sources:
 my_utility:
    sftp_user: my_user
    sftp_password: my_password
```

## Limitations

N/A
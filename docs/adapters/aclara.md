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

The `./known-hosts` file is a special SSH known hosts file that should contain info about the Aclara server at `sftp_host`.
If your SFTP connection opens but then shuts immediately, you may have an issue with known hosts. We've worked around it in
the past by running `ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())` in the pipeline code one time, but you don't want to
run with this option in production as it posts security risks.

Secrets:
```yaml
sources:
 my_utility:
    sftp_user: my_user
    sftp_password: my_password
```

## Limitations

N/A
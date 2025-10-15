# Aclara

The Aclara adapter uses SFTP to retrieve meter read data from an Aclara server.

## Configuration

- sftp_host: Server where data lives
- sftp_remote_data_directory: Directory on remote server where data lives, e.g. "./data"
- sftp_local_download_directory: Local directory where we'll download the data, e.g. "./output"
- sftp_local_known_hosts_file: Local SSH known hosts file, e.g. "./known-hosts"

Example:
```
python cli.py config add-source my_utility aclara America/Los_Angeles --sftp-host my-sftp-host --sftp-remote-data-directory ./data --sftp-local-download-directory ./output --sftp-local-known-hosts-file ./known-hosts --sinks my_snowflake
```

The `./known-hosts` file is a special SSH known hosts file that should contain info about the Aclara server at `sftp_host`.

## Secrets

Example:
```
python cli.py config update-secret my_utility --source-type aclara --sftp-user my_user --sftp-password my_password
```

## Limitations

N/A
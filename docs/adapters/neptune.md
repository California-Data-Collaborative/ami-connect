# Neptune 360

The Neptune 360 adapter retrieves data via Neptune 360's HTTP API. This code is not available via open source.
Users must provide their own implementation and make it available at the path specified in the configuration. Our deploy
system takes this into account and allows the deployer to specify that it should pull down Neptune adapter code from
a private GitHub repo.

## Deploy

Our `deploy.sh` script looks for a `AMI_CONNECT_NEPTUNE_REPO_URL` environment variable. If it's nonempty, then the system
will attempt to pull the latest Neptune adapter code from that GitHub repo URL.

The system expects that the server has permission to pull from this GitHub repo. We recommend that you create an SSH key on your
server and add it as a [deploy key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/managing-deploy-keys#set-up-deploy-keys) to your GitHub repo.

## Configuration

- external_adapter_location: Path on remote server to the neptune-ami-connect python package (i.e. the folder with the adapter's python code), which is loaded onto the sys path. Path is relative to directory where Airflow runs python.

Example:
```
python cli.py config add-source my_utility neptune America/Los_Angeles --external-adapter-location ./neptune-ami-connect/neptune --sinks my_snowflake
```

## Secrets

Example:
```
python cli.py config update-secret my_utility --source-type neptune --api-key my_api_key --site-id 1234 --client-id api-client_my_client --client_secret my_secret
```

## Notes

The Neptune 360 API provides a set of Endpoints which we transform into our meters. Endpoints have a meter_number and miu_id and they are
mostly unique by those two IDs. The same is true for consumption and readings: they are mostly unique by meter_number, miu_id, and time.

Our device_id is a concatenation of the two IDs: "{meter_number}-{miu_id}". But even with these IDs concatenated, we see duplicate Endpoints
and readings in the Neptune 360 API response. As of this writing, we assume this is an issue with the API - the duplicate Endpoints in the
data are the same actual Endpoints in the real world, and the duplicate readings are the same actual reading. We log these occurances and drop the duplicate data.

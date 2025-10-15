# Neptune 360

The Neptune 360 adapter retrieves data via Neptune 360's HTTP API. This code is not available via open source.
Users must provide their own implementation and make it available at the path specified in the configuration.

## Configuration

- external_adapter_location: Path on remote server to the neptune-ami-connect package, which is loaded onto the sys path. Path is relative to directory where Airflow runs python.

Example:
```
python cli.py config add-source my_utility neptune America/Los_Angeles --external-adapter-location ./neptune-ami-connect --sinks my_snowflake
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

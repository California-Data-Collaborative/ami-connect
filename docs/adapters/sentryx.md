# Sentryx

The Sentryx adapter retrieves data via Sentryx's HTTP API.

## Configuration

- utility_name: Name of utility as it appears in the Senrtyx API URL

Example:
```
python cli.py config add-source my_utility sentryx America/Los_Angeles --utility-name name-of-my-utility-in-api-url ./neptune-ami-connect --sinks my_snowflake
```

Secrets:
```yaml
sources:
- my_utility:
    sentryx_api_key: key
```

## Limitations

N/A
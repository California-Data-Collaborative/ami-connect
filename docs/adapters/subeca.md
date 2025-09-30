# Subeca

The Subeca adapter retrieves data via Subeca's HTTP API.

## Configuration

- api_url: Name of utility as it appears in the Senrtyx API URL

Example:
```
python cli.py config add-source my_utility subeca America/Los_Angeles --api-url https://my-source-name.api.subeca.online --sinks my_snowflake
```

Secrets:
```yaml
sources:
 my_utility:
    subeca_api_key: key
```

## Limitations

N/A
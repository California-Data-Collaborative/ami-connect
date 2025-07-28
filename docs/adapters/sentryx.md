# Sentryx

The Sentryx adapter retrieves data via Sentryx's HTTP API.

## Configuration

Example:
```yaml
- type: sentryx
  org_id: my_utility
  timezone: America/Los_Angeles
  # Name of utility as it appears in the Senrtyx API URL
  utility_name: u_name
```

Secrets:
```yaml
sources:
- my_utility:
    sentryx_api_key: key
```

## Limitations

N/A
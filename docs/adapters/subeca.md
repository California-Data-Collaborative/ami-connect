# Subeca

The Subeca adapter retrieves data via Subeca's HTTP API.

## Configuration

Example:
```yaml
sources:
- type: subeca
  org_id: my_utility
  timezone: America/Los_Angeles
  api_url: my-source-name.api.subeca.online
```

Secrets:
```yaml
sources:
 my_utility:
    subeca_api_key: key
```

## Limitations

N/A
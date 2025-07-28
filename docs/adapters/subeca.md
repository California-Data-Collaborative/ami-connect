# Subeca

The Subeca adapter retrieves data via Subeca's HTTP API.

## Configuration

Example:
```yaml
sources:
- type: subeca
  org_id: my_utility
  timezone: America/Los_Angeles
```

Secrets:
```yaml
sources:
 my_utility:
    subeca_api_key: key
```

## Limitations

N/A
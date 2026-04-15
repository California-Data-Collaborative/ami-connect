# Xylem Data Lake

The Xylem Data Lake adapter retrieves AMI data from Xylem's Data Lake platform via the Apache Superset SQL Lab API. Authentication uses Keycloak PKCE OAuth. Each Data Lake instance is per-agency, identified by an agency code that determines the URL subdomain and Redshift schema.

## Data Sources

The adapter extracts from three tables in the `sensus_dm_{agency_code}` schema:

- **account** — meter/account metadata (active records only)
- **water_intervals** — per-interval consumption reads
- **water_registers** — cumulative register reads

Reads join to accounts via `meter_id = device_id` (direct join, no intermediate table needed).

## Configuration

- agency_code: Agency identifier that determines the Data Lake subdomain and schema (e.g., "hlsbo" for Hillsborough)
- database_id: Superset database ID (e.g., 1)
- client_id: Keycloak OAuth client ID
- chunk_hours: Hours per query chunk to avoid Superset row limits (default: 12)

Example:
```
python cli.py config add-source my_utility xylem_datalake America/Los_Angeles --config agency-code=hlsbo --config database-id=1 --config client-id=e4b8a1bf-51c5-4ba4-a8e6-15e5186d503e --sinks my_snowflake
```

## Secrets

- username: Xylem Data Lake login email
- password: Xylem Data Lake login password

Example:
```
python cli.py config update-secret my_utility --source-type xylem_datalake --secret username=user@example.com --secret password=mypassword
```

## Limitations

- Authentication is via browser-style Keycloak PKCE flow (username/password). Xylem has announced upcoming MFA requirements which may require changes to the auth mechanism.
- Only active account records are extracted. Deep backfills may need historical account data for correct metadata attribution.
- Meter size values from the Data Lake use Sensus internal codes (e.g., 402) which don't map to the standard size lookup. Meters are created with meter_size=None in these cases.

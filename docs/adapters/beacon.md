# Beacon 360

The Beacon 360 adapter uses Beacon's [HTTP Export API](https://helpbeaconama.net/beacon-web-services/export-data-service-v2-api-preview/#POSTread) to retrieve meter read data. This API requires the adapter to create a report, then poll the API until the report is complete. When the report is complete, the adapter downloads it via a link to a CSV.

A report on the last two days of meter readings for a utility with ~30,000 connections may take 45 minutes to run.

## Configuration

Example:
```
python cli.py config add-source my_utility beacon_360 America/Los_Angeles --sinks my_snowflake
```

Secrets:
```yaml
sources:
 my_utility:
    beacon_360_user: my_user
    beacon_360_password: my_password
```

## Limitations

The Beacon API limits the number of rows in the report to about 1.2 million, where `number of rows = (number of connections) * (number of days) * 24`. This caps the number of days of data we can request for a given utility.

Currently, it's best to limit this adapter's extract date range to a few days. A longer range risks downloading a report that uses too much memory for our Airflow deployment.
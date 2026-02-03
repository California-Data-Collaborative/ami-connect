# AMI Connect Snowflake Data Model Documentation

_Last updated: June 2025_

## Overview

AMI Connect's Snowflake data model is a generalized, vendor-agnostic data model for storing and analyzing Advanced Metering Infrastructure (AMI) data across multiple providers (e.g., Mueller, Badger, MasterMeter). The schema is designed primarily for analytical workflows, but may support user interfaces as well.

The model is denormalized to optimize for time-series analysis and joins across meter metadata. It emphasizes traceability of consumption data to specific meter configurations, locations, and accounts at the time of measurement. It favors preserving details of the upstream data source over aggressive changes in the name of easy querying. That said, effort is made to improve ease of querying when possible.

---

## Key Concepts

- **AMI data source**: A software system that makes AMI data (meter reads and meter metadata) available. The company or organization that manages the software system may also produce the meters themselves. AMI Connect integrates with these data sources and normalizes the data into this Snowflake data model.
- **Org ID**: Identifies the utility for which we're collecting data. A utility's data will come from one AMI data source, depending on who the utility bought their meters from. By convention, the `ORG_ID` is often prefixed with the name of the AMI Connect operator. It's used to ensure there is no ID collision between AMI data sources or utilities. Ex: `cadc_apple_water_co` might be an `ORG_ID` for Apply Water Company in CaDC's AMI Connect pipeline.
- **Meter**: A physical device that measures water usage. Identified by a `DEVICE_ID`. In our system, this is the smallest unit that produces consumption readings. E.g. a compound meter for which multiple readings are available at a given time is considered two meters (with two `DEVICE_ID`s).
- **Device ID**: The main join key between meter metadata and timeseries reads. Some AMI data sources call this a "meter_id", some call it an "endpoint_id", others call it something else. For each data source, we choose which field we call `DEVICE_ID` so that it's always present and always uniquely identies a meter.
- **Endpoint**: The part of a meter that transmits consumption data. Sometimes a meter may have multiple endpoints that each produce consumption data. In those cases, we consider the meter to be two devices - one `DEVICE_ID` per endpoint.
- **Location**: The service address where consumption occurs. May have multiple meters over time. We do our best to include location data for each meter, but are limited by the data source's capabilities.
- **Account**: Represents a billable customer. An account may be associated with multiple locations over time. We do our best to include account for each meter, but are limited by the data source's capabilities.
- **Flowtime**: Timestamp of when a water consumption measurement was taken.


---

## Tables

### `METERS`
A versioned, denormalized table containing metadata about meters, their installation, and associated account/location information. 

The table uses a Type 2 Slowly Changing Dimension pattern to capture changes in meter metadata. The up-to-date row for a given meter is the row with `ROW_ACTIVE_UNTIL = NULL`. Previous rows
will have a populated `ROW_ACTIVE_FROM` and `ROW_ACTIVE_UNTIL` denoting the time period for which that row's metadata was accurate.

For a given `ORG_ID`, rows with `ROW_ACTIVE_UNTIL = NULL` should be unique per `DEVICE_ID`.

`meters.sql` contains the create table statement for the `meters` table.

#### Notes
- **Versioning**: `ROW_ACTIVE_UNTIL IS NULL` identifies the current version of a meter. Combine `ROW_ACTIVE_FROM` and `ROW_ACTIVE_UNTIL` to see what the meter looked like at a time in the past.
- **Nullable account and location data**: Account and location data are not always available from the data source. `ACCOUNT_ID` and `LOCATION_ID` are `NULL` when the data is unavailable.

---

### `READINGS`
Timeseries records of water usage, reported by meter (DEVICE_ID) with optional account and location context. We aim for hourly granularity in this timeseries, though that is dependent on the AMI data provider.

`readings.sql` contains the create table statement for the `readings` table.

#### Notes
- **REGISTER_VALUE**: Total cumulative usage (e.g., since install).
- **INTERVAL_VALUE**: Usage over a discrete period (e.g., between 08:00 and 09:00 today).
- **JOIN to METERS**: Use `ORG_ID`, `DEVICE_ID` to join to `METERS`. If you want the meter metadata *at the time of the reading*, filter by `FLOWTIME BETWEEN ROW_ACTIVE_FROM AND COALESCE(ROW_ACTIVE_UNTIL, CURRENT_TIMESTAMP)`. If you simply want the latest meter metadata, filter by `ROW_ACTIVE_FROM IS NULL`.

---

## Data Dictionary

### `METERS`
| Column             | Type             | Description |
|--------------------|------------------|-------------|
| ORG_ID             | VARCHAR           | Organization identifier picked by AMI Connect user. Sequesters data between utilities. |
| DEVICE_ID          | VARCHAR (NOT NULL)| Chosen by AMI Connect developer per provider to be the unique identifier for a meter device that reports consumption. If a meter has multiple endpoints that report consumption, each endpoint should have its own `DEVICE_ID`. Use this to join to the `READINGS` table. |
| ACCOUNT_ID         | VARCHAR           | Associated account for a billable customer. Optional. |
| LOCATION_ID        | VARCHAR           | Service location ID. Optional. |
| METER_ID           | VARCHAR           | Provider-specific meter ID. May or may not match DEVICE_ID. Optional. |
| ENDPOINT_ID        | VARCHAR           | Endpoint device ID (e.g., radio/telemetry unit). May or may not match `DEVICE_ID`. Optional. |
| METER_INSTALL_DATE | TIMESTAMP_TZ(9)   | When meter was installed. Optional. |
| METER_SIZE         | VARCHAR           | Size of the meter represented in inches (e.g., 0.75). In cases where we can't match to a decimal number of inches, we preserve the raw data source's value. Optional. |
| METER_MANUFACTURER| VARCHAR           | Meter manufacturer name. Optional. |
| MULTIPLIER         | FLOAT             | Unit conversion multiplier (if needed). Optional. |
| LOCATION_ADDRESS   | VARCHAR           | Full street address of the meter's service location. Optional. |
| LOCATION_CITY      | VARCHAR           | City name of the meter's service location. Optional. |
| LOCATION_STATE     | VARCHAR           | State of the meter's service location. Optional. |
| LOCATION_ZIP       | VARCHAR           | ZIP/postal code of the meter's service location. Optional. |
| ROW_ACTIVE_FROM    | TIMESTAMP_TZ(9)   | When this metadata version became active. |
| ROW_ACTIVE_UNTIL   | TIMESTAMP_TZ(9)   | When this version became inactive (NULL = current). |

### `READINGS`
| Column           | Type             | Description |
|------------------|------------------|-------------|
| ORG_ID           | VARCHAR           | Organization identifier. See `METERS` definition. |
| ACCOUNT_ID       | VARCHAR           | Optional account tied to this read. See `METERS` definition. |
| LOCATION_ID      | VARCHAR           | Optional location tied to this read. See `METERS` definition. |
| DEVICE_ID        | VARCHAR (NOT NULL)| The meter that recorded the read. Used for joins to `METERS`. |
| FLOWTIME         | TIMESTAMP_TZ(9)   | Time of measurement. |
| REGISTER_VALUE   | FLOAT             | Cumulative usage recorded at this time. |
| REGISTER_UNIT    | VARCHAR           | Unit of `REGISTER_VALUE` (e.g., CCF, GAL). |
| INTERVAL_VALUE   | FLOAT             | Usage in time interval (e.g., past hour). |
| INTERVAL_UNIT    | VARCHAR           | Unit of `INTERVAL_VALUE`. (e.g., CCF, GAL). |
| BATTERY          | VARCHAR           | Strength of meter's battery at the time of this read. |
| INSTALL_DATE     | TIMESTAMP_TZ(9)   | Time that meter which took this read was installed. |
| CONNECTION       | VARCHAR           | Strength of meter's internet/satellite/etc connection at the time of this read. |
| ESTIMATED        | INT               | Indicates whether this read was estimated by the AMI provider. Values are 1 if estimated else 0. |

---

## Joining Tables

To join a reading with metadata from the time of measurement:

```sql
SELECT r.*, m.METER_SIZE, m.LOCATION_CITY
FROM READINGS r
LEFT JOIN METERS m
  ON r.ORG_ID = m.ORG_ID
 AND r.DEVICE_ID = m.DEVICE_ID
 -- Finds the meter row that was active when the measurement was taken
 AND r.FLOWTIME BETWEEN m.ROW_ACTIVE_FROM AND COALESCE(m.ROW_ACTIVE_UNTIL, CURRENT_TIMESTAMP)
 ;
```

A simpler join will associate a reading with the most recent meter metadata:

```sql
SELECT r.*, m.METER_SIZE, m.LOCATION_CITY
FROM READINGS r
JOIN METERS m
  ON r.ORG_ID = m.ORG_ID
 AND r.DEVICE_ID = m.DEVICE_ID
 -- Finds the most recent meter row
 AND m.ROW_ACTIVE_UNTIL IS NULL
 ;
```

---

## Design Considerations

- **Denormalization**: This model is intentionally denormalized to minimize joins for analytics.
- **Missing Account/Location**: These values may be NULL if unavailable at ingest time. The DEVICE_ID is always populated.
- **No Interpolation**: Interpolation for missing values is left to downstream tools or models.
- **Multi-tenancy**: ORG_ID distinguishes data across utilities.
- **Preserving source data**: When we choose between preserving sometimes quirky patterns in the source data vs. "cleaning it up" in a way that would lose information about the source, we usually prefer to preserve those quirky source data characteristics. We imagine that the tables produced by AMI Connect are a starting point for users and that they'll make other tables or views according to their needs.

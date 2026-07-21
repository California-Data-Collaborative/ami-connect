# Itron Roseville

The Itron Roseville adapter reads City of Roseville's Itron AMI data from CSV
files that Roseville pushes (via Informatica) into a CaDC-owned S3 prefix. It is
the first adapter whose source is a utility-pushed S3 file drop rather than a
vendor API/SFTP/database — the transport-level pieces that aren't specific to
Roseville (cross-account client, subfolder-safe listing, filename date-range
selection, CSV download) are module-level helpers at the bottom of
`amiadapters/adapters/itron_roseville.py`, to be extracted into a shared module
if a second S3-drop source appears.

This adapter was built specially for Roseville and is not compatible with other
utilities. The CSVs are custom database views built by Roseville IT (their AMI
team pipes Itron data onto Roseville-hosted servers), NOT Itron's native
ChoiceConnect export format — an Itron utility onboarding through Itron's
standard hosted-SFTP path would need a different adapter (reusing the S3-drop
helpers only if that utility also delivers via a CaDC S3 drop).

## Data

Two file types with an identical 7-column schema
(`Timestamp,Read_Value,Meter_Serial_Number,EndpointID,Location_ID,Meter_Install_Date,Read_Units`):

- `rosevillecityof_Register_<dates>.csv` — cumulative register reads at ~8-hour cadence
- `rosevillecityof_Interval_<dates>.csv` — hourly consumption; the `Timestamp`
  marks the **end** of the measured hour (verified empirically: hour-ending
  alignment reconciles 99.6% of 8-hour register windows exactly)

The `<dates>` suffix is either `YYYYMM` or `YYYYMMDD_YYYYMMDD`; files are selected
for an extract when their filename date range overlaps the extract range. Keys in
subfolders of the prefix (e.g. `archive/`) are ignored.

Roseville delivers a rolling ~3-day "correction window" of un-finalized reads that
may be re-sent with corrections. Re-delivered rows are absorbed by MERGE upserts
in both the raw base tables and READINGS.

Notable data properties:

- All timestamps are Pacific local time, format `MM/DD/YYYY HH:MM:SS.ffffff`.
- `Read_Units` is `CF_WAT` (cubic feet, water), normalized to `CF`.
- `account_id` is permanently unavailable — Roseville's AMI system does not
  receive account information from their CIS. `Location_ID` equals Cayenta's
  `SERVICE_POINT` (`{LOCATION_NO}_{SERVICE_SEQUENCE}`), which is the join key to
  Roseville billing data on the CaDC side.
- Faulted channels report 32-bit max-value error codes as `Read_Value`
  (`4294967294` = 2^32 - 2 observed; neighboring codes plausible). Values at
  or above 4×10⁹ — orders of magnitude beyond any real reading — are
  preserved in the raw base tables but excluded from transformed reads.

## Configuration

- `s3_bucket`: bucket the utility delivers files into (e.g. `cadc-ami`)
- `s3_prefix`: key prefix of the file drop, with trailing slash (e.g. `rosevillecityof/`)
- `s3_region`: the bucket's region (e.g. `us-east-1`)

Example:

```
python cli.py config add-source cadc_roseville itron_roseville America/Los_Angeles --config s3_bucket=cadc-ami --config s3_prefix=rosevillecityof/ --config s3_region=us-east-1 --sinks my_snowflake
```

## Secrets

The source bucket lives in the CaDC AWS account, not the ami-connect account, so
the pipeline's EC2 instance role cannot read it. The adapter authenticates with a
read-only IAM user (scoped to the prefix) whose keys are stored in this source's
secrets:

```
python cli.py config update-secret cadc_roseville --source-type itron_roseville --secret aws_access_key_id=AKIA... --secret aws_secret_access_key=...
```

## Limitations

- No `account_id` in the feed (by design) — the `meters_score` post-processor
  join to billing data will be empty until account linkage is derived from
  `Location_ID` via billing data.
- A scheduled run whose extract window overlaps no files fails at the load step
  (framework behavior: empty meters/reads raise) — this makes a missed delivery
  visible rather than silent.
- Raw base tables must be created in Snowflake before the first run
  (`sql/itron-roseville-base.sql`).
- DST fall-back: timestamps are wall-clock Pacific with no offset, so the
  repeated 01:00 hour each November yields two indistinguishable rows per
  meter, one of which overwrites the other (surfaced by the transform's
  `overwritten_differing` counter). Shared with every local-time adapter.
- While Roseville uses month-suffix filenames (`_YYYYMM`), the current month's
  file overlaps every daily extract window and is re-downloaded and
  re-processed each run (idempotent but wasteful — ~500MB/day at current
  volumes). This resolves itself once date-range suffixes land. Keeping the
  prefix tidy (archiving superseded files) also bounds per-run download size.

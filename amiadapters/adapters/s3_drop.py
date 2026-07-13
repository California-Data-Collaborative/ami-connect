"""
Shared helpers for "S3 drop-off" AMI sources.

CaDC's standard intake pattern for utilities that can push files: the utility
delivers CSVs into a CaDC-owned S3 prefix (see the CaDC AMI Data Onboarding
Instructions), with a timestamp or date range in each filename to differentiate
files over time. The first source using this pattern is City of Roseville
(itron_roseville.py); these helpers hold the parts that are NOT specific to any
one utility so the next S3-drop adapter can reuse them.

Conventions these helpers encode:
- The drop prefix may contain subfolders (e.g. archive/ for superseded files);
  listing uses Delimiter="/" so only top-level keys are ever considered.
- Filename date tokens are either "YYYYMM" (a whole month) or
  "YYYYMMDD_YYYYMMDD" (an inclusive day range). A file is selected for an
  extract when its date range overlaps the extract range; overlap is
  boundary-inclusive because a file's last reads may be stamped at the
  boundary instant (e.g. hour-ending timestamps at midnight).
- Files are processed oldest-LastModified-first so that when overlapping
  deliveries carry the same logical row (e.g. a rolling correction window),
  the most recently delivered value wins downstream.
- The drop bucket may live in a different AWS account than the pipeline, in
  which case the source's secrets carry a read-only IAM keypair.
"""

import csv
from datetime import datetime, timedelta
import io
import logging
import re
from typing import Dict, List, Tuple

import boto3

logger = logging.getLogger(__name__)


def s3_client_from_keys(
    region: str, aws_access_key_id: str, aws_secret_access_key: str
):
    """
    Build an S3 client from a source's IAM keypair. Used when the drop bucket
    lives outside the pipeline's AWS account, so the EC2 instance role can't
    read it and credentials come from the source's secrets instead.
    """
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


def list_drop_files(s3, bucket: str, prefix: str) -> Dict[str, datetime]:
    """
    List the top-level keys of an S3 drop prefix, returning {key: LastModified}.

    Uses a paginator (no 1000-key cap) and Delimiter="/" so keys inside
    subfolders — e.g. an archive/ folder of superseded files — are never
    returned. The prefix must end with "/" for the delimiter grouping to work;
    normalize it at configuration time.
    """
    result = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for obj in page.get("Contents", []):
            # ListObjectsV2 always returns LastModified
            result[obj["Key"]] = obj["LastModified"]
    return result


def select_files_for_range(
    files: Dict[str, datetime],
    filename_pattern: re.Pattern,
    range_start: datetime,
    range_end: datetime,
) -> Dict[str, List[str]]:
    """
    Select drop files whose filename date range overlaps the extract range,
    grouped by file type and ordered oldest-LastModified-first.

    filename_pattern is a compiled regex, matched against each key's basename,
    with two named groups: "type" (the file-type token, lowercased in the
    result) and "dates" (a date token parsed by date_range_from_filename).
    Non-matching filenames and unparseable date tokens are logged and skipped.

    Scheduled runs deliver naive range bounds (datetime.now() server time), but
    manually triggered runs can carry offsets (Airflow UI params pass through
    datetime.fromisoformat unmodified). Filename dates are naive wall dates, so
    any offset is dropped and comparison is wall-clock-to-wall-clock — a fuzzy
    multi-day extract window absorbs the hour-level slop.
    """
    if range_start.tzinfo is not None:
        range_start = range_start.replace(tzinfo=None)
    if range_end.tzinfo is not None:
        range_end = range_end.replace(tzinfo=None)

    result = {}
    for key in files:
        filename = key.split("/")[-1]
        match = filename_pattern.search(filename)
        if not match:
            logger.info(f"Skipping unrecognized file: {key}")
            continue
        try:
            file_start, file_end = date_range_from_filename(match.group("dates"))
        except Exception as e:
            logger.warning(f"Skipping file {key}, could not parse dates: {str(e)}")
            continue
        if file_start <= range_end and range_start <= file_end:
            result.setdefault(match.group("type").lower(), []).append(key)
        else:
            logger.info(
                f"Skipping file outside extract range ({file_start} to {file_end}): {key}"
            )
    for file_type in result:
        result[file_type].sort(key=lambda k: files[k])
    return result


def date_range_from_filename(dates: str) -> Tuple[datetime, datetime]:
    """
    Parse a filename date token into an inclusive [start, end] datetime range.
    "202607" (YYYYMM) covers the whole month; "20260616_20260619" covers the
    named days through the end of the last day.
    """
    if "_" in dates:
        start_str, end_str = dates.split("_")
        start = datetime.strptime(start_str, "%Y%m%d")
        end = datetime.strptime(end_str, "%Y%m%d") + timedelta(days=1)
    else:
        start = datetime.strptime(dates, "%Y%m")
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
    return start, end


def download_csv_rows(s3, bucket: str, key: str, row_type) -> List:
    """
    Download a CSV drop file and parse each row into row_type, whose dataclass
    attribute names must exactly match the CSV column headers.

    Decodes with utf-8-sig, which transparently strips a UTF-8 BOM if present
    (common in Windows/Informatica exports); a BOM would otherwise corrupt the
    first header name and crash row construction. Schema drift (added, renamed,
    or extra columns) raises immediately — a loud failure is preferred over
    silently mis-parsed data.
    """
    logger.info(f"Downloading s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    text = response["Body"].read().decode("utf-8-sig")
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        rows.append(row_type(**row))
    logger.info(f"Parsed {len(rows)} rows from {key}")
    return rows

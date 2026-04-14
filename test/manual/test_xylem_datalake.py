"""
Manual end-to-end test for the Xylem Data Lake adapter.

Usage (from repo root):
    .venv/bin/python -m test.manual.test_xylem_datalake

Requires:
    - test/fixtures/xylem-datalake-secrets.yaml filled in with real credentials
    - beautifulsoup4 and requests installed

This script:
    1. Loads config from YAML fixtures
    2. Runs extract (authenticates to Data Lake, queries 1 day of data)
    3. Runs transform (converts raw data to GeneralMeter + GeneralMeterRead)
    4. Runs load_raw + load_transformed (writes to Snowflake)
    5. Prints summary stats
"""

import logging
import sys
from datetime import datetime, timedelta

from amiadapters.config import AMIAdapterConfiguration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main():
    config = AMIAdapterConfiguration.from_yaml(
        "test/fixtures/xylem-datalake-config.yaml",
        "test/fixtures/xylem-datalake-secrets.yaml",
    )

    adapters = config.adapters()
    if not adapters:
        logger.error("No adapters created from config. Check your YAML files.")
        return

    adapter = adapters[0]
    logger.info(f"Adapter: {adapter.name()}")

    # Extract a small window: 1 day, recent data
    end = datetime.utcnow()
    start = end - timedelta(days=1)
    logger.info(f"Extract range: {start:%Y-%m-%d %H:%M} -> {end:%Y-%m-%d %H:%M}")

    run_id = f"test_{start:%Y%m%d}_{end:%Y%m%d}"

    # Step 1: Extract
    logger.info("=== EXTRACT ===")
    adapter.extract_and_output(run_id, start, end)

    # Read back extract outputs to print stats
    extract_outputs = adapter.output_controller.read_extract_outputs(run_id)
    for filename, content in extract_outputs.get_outputs().items():
        lines = content.strip().split("\n") if content.strip() else []
        logger.info(f"  {filename}: {len(lines)} rows")

    # Step 2: Transform
    logger.info("=== TRANSFORM ===")
    adapter.transform_and_output(run_id)

    # Read back transform outputs to print stats
    meters = adapter.output_controller.read_transformed_meters(run_id)
    reads = adapter.output_controller.read_transformed_meter_reads(run_id)
    logger.info(f"  Meters: {len(meters)}")
    logger.info(f"  Reads: {len(reads)}")

    if meters:
        m = meters[0]
        logger.info(f"  Sample meter: device_id={m.device_id} account_id={m.account_id} "
                     f"location_id={m.location_id} address={m.location_address}")
    if reads:
        r = reads[0]
        logger.info(f"  Sample read: device_id={r.device_id} flowtime={r.flowtime} "
                     f"interval_value={r.interval_value} register_value={r.register_value}")

    # Step 3: Load to Snowflake
    logger.info("=== LOAD RAW ===")
    adapter.load_raw(run_id)

    logger.info("=== LOAD TRANSFORMED ===")
    adapter.load_transformed(run_id)

    logger.info("=== DONE ===")
    logger.info("Check Snowflake for:")
    logger.info("  - XYLEM_DATALAKE_ACCOUNT_BASE")
    logger.info("  - XYLEM_DATALAKE_WATER_INTERVALS_BASE")
    logger.info("  - XYLEM_DATALAKE_WATER_REGISTERS_BASE")
    logger.info("  - METERS")
    logger.info("  - READINGS")


if __name__ == "__main__":
    main()

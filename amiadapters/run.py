from datetime import datetime
import logging

from amiadapters.config import AMIAdapterConfiguration

logger = logging.getLogger(__name__)


def run_pipeline(
    config_yaml: str,
    secrets_yaml: str,
    extract_range_start=None,
    extract_range_end=None,
):
    """
    Run AMI API adapters to fetch AMI data, then shape it into generalized format, then store it.
    """

    config = AMIAdapterConfiguration.from_yaml(config_yaml, secrets_yaml)
    adapters = config.adapters()
    backfills = {b.org_id: b for b in config.backfills()}
    run_id = f"run-{datetime.now().isoformat()}"

    for adapter in adapters:
        backfill = backfills.get(adapter.org_id)
        start, end = adapter.calculate_extract_range(
            extract_range_start, extract_range_end, backfill_params=backfill
        )
        # TODO REMOVE
        from datetime import timedelta
        start = start - timedelta(days=7)
        logger.info(f"Extracting data for {adapter.name()} from {start} to {end}")
        adapter.extract_and_output(run_id, start, end)
        logger.info(f"Extracted data for {adapter.name()}")

    logger.info(f"Extracted data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(f"Transforming data for {adapter.name()}")
        adapter.transform_and_output(run_id)
        logger.info(f"Transformed data for {adapter.name()}")

    logger.info(f"Transformed data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(f"Loading raw data for {adapter.name()}")
        adapter.load_raw(run_id)
        logger.info(f"Loaded raw data for {adapter.name()}")

    logger.info(f"Loaded raw data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(f"Loading transformed data for {adapter.name()}")
        adapter.load_transformed(run_id)
        logger.info(f"Loaded transformed data for {adapter.name()}")

    logger.info(f"Loaded transformed data for {len(adapters)} adapters")

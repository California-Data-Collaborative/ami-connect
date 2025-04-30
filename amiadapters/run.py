from datetime import datetime
import logging

from amiadapters.base import default_date_range
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
    run_id = f"run-{datetime.now().isoformat()}"

    if extract_range_start is None or extract_range_end is None:
        extract_range_start, extract_range_end = default_date_range(
            extract_range_start, extract_range_end
        )

    for adapter in adapters:
        logger.info(f"Extracting data for {adapter.name()}")
        adapter.extract(run_id, extract_range_start, extract_range_end)
        logger.info(f"Extracted data for {adapter.name()} to {adapter.output_folder}")

    logger.info(f"Extracted data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(
            f"Transforming data for {adapter.name()} from {adapter.output_folder}"
        )
        adapter.transform(run_id)
        logger.info(f"Transformed data for {adapter.name()} to {adapter.output_folder}")

    logger.info(f"Transformed data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(
            f"Loading raw data for {adapter.name()} from {adapter.output_folder}"
        )
        adapter.load_raw(run_id)
        logger.info(
            f"Loaded raw data for {adapter.name()} from {adapter.output_folder}"
        )

    logger.info(f"Loaded raw data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(
            f"Loading transformed data for {adapter.name()} from {adapter.output_folder}"
        )
        adapter.load_transformed(run_id)
        logger.info(
            f"Loaded transformed data for {adapter.name()} from {adapter.output_folder}"
        )

    logger.info(f"Loaded transformed data for {len(adapters)} adapters")

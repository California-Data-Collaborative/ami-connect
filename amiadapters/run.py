import logging

from amiadapters.config import AMIAdapterConfiguration

logger = logging.getLogger(__name__)


def run_pipeline(config_yaml: str, secrets_yaml: str):
    """
    Run AMI API adapters to fetch AMI data, then shape it into generalized format, then store it.
    """
    config = AMIAdapterConfiguration.from_yaml(config_yaml, secrets_yaml)
    adapters = config.adapters()

    for adapter in adapters:
        logger.info(f"Extracting data for {adapter.name()}")
        adapter.extract()
        logger.info(f"Extracted data for {adapter.name()} to {adapter.output_folder}")

    logger.info(f"Extracted data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(
            f"Transforming data for {adapter.name()} from {adapter.output_folder}"
        )
        adapter.transform()
        logger.info(f"Transformed data for {adapter.name()} to {adapter.output_folder}")

    logger.info(f"Transformed data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(
            f"Loading raw data for {adapter.name()} from {adapter.output_folder}"
        )
        adapter.load_raw()
        logger.info(
            f"Loaded raw data for {adapter.name()} from {adapter.output_folder}"
        )

    logger.info(f"Loaded raw data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(
            f"Loading transformed data for {adapter.name()} from {adapter.output_folder}"
        )
        adapter.load_transformed()
        logger.info(
            f"Loaded transformed data for {adapter.name()} from {adapter.output_folder}"
        )

    logger.info(f"Loaded transformed data for {len(adapters)} adapters")

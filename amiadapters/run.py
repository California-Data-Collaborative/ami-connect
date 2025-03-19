import logging

from amiadapters.beacon import Beacon360Adapter
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.sentryx import SentryxAdapter

logger = logging.getLogger(__name__)


def run_extract_transform():
    """
    Run AMI API adapters to fetch AMI data, then shape it into generalized format.
    """
    config = AMIAdapterConfiguration.from_env()
    adapters = [
        # SentryxAdapter(config),
        Beacon360Adapter(config),
    ]
    # for adapter in adapters:
    #     logger.info(f"Extracting data for {adapter.name()}")
    #     adapter.extract()
    #     logger.info(f"Extracted data for {adapter.name()} to {config.output_folder}")
    
    # logger.info(f"Extracted data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(f"Transforming data for {adapter.name()} from {config.output_folder}")
        adapter.transform()
        logger.info(f"Transformed data for {adapter.name()} to {config.output_folder}")
    
    logger.info(f"Transformed data for {len(adapters)} adapters")
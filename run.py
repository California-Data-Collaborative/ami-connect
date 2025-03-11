import logging
import sys

from amiadapters.sentryx import SentryxAdapter
from amiadapters.config import AMIAdapterConfiguration

logger = logging.getLogger(__name__)


def run():
    logging.basicConfig(level=logging.INFO)
    
    config = AMIAdapterConfiguration.from_env()
    adapters = [
        SentryxAdapter(config)
    ]
    for adapter in adapters:
        logger.info(f"Extracting data for {adapter.name()}")
        adapter.extract()
        logger.info(f"Extracted data for {adapter.name()} to {config.output_folder}")
    
    logger.info(f"Extracted data for {len(adapters)} adapters")

    for adapter in adapters:
        logger.info(f"Transforming data for {adapter.name()} from {config.output_folder}")
        adapter.transform()
        logger.info(f"Transformed data for {adapter.name()} to {config.output_folder}")
    
    logger.info(f"Transformed data for {len(adapters)} adapters")
    
    return 0


if __name__ == "__main__":
    """
    Run AMI API adapters to fetch AMI data, then shape it into generalized format.
    """
    sys.exit(run())
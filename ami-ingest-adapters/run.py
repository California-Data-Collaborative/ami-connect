from logging import getLogger
import sys

from adapters.sentryx import SentryxAdapter
from config import AMIAdapterConfiguration

logger = getLogger(__name__)


def run():
    config = AMIAdapterConfiguration.from_env()
    adapters = [
        SentryxAdapter(config)
    ]
    for adapter in adapters:
        logger.info(f"Extracting data for {adapter.name()}")
        adapter.extract()
        logger.info(f"Extracting data for {adapter.name()} to {config.output_folder}")
    
    logger.info(f"Extracted data for {len(adapters)} adapters")
    
    return 0


if __name__ == "__main__":
    """
    Run AMI API adapters to fetch AMI data, then shape it into generalized format.
    """
    sys.exit(run())
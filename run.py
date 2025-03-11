import logging
import sys

from amiadapters.run import run_extract_transform

logger = logging.getLogger(__name__)


def run():
    """
    CLI for AMI Connect system.
    """
    logging.basicConfig(level=logging.INFO)
    run_extract_transform()
    return 0


if __name__ == "__main__":
    sys.exit(run())
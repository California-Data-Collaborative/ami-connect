"""
CLI for AMI Connect that uses Typer (https://typer.tiangolo.com/) under the hood.

Run from root directory with:

    python cli.py --help

"""

from datetime import datetime
import logging
from typing_extensions import Annotated

import typer

from amiadapters.config import (
    AMIAdapterConfiguration,
    ConfiguredTaskOutputControllerType,
)
from amiadapters.outputs.local import LocalTaskOutputController
from amiadapters.outputs.s3 import S3TaskOutputController

DEFAULT_CONFIG_PATH = "./config.yaml"
DEFAULT_SECRETS_PATH = "./secrets.yaml"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


@app.command()
def run(
    config_file: Annotated[
        str, typer.Option(help="Path to local config file.")
    ] = DEFAULT_CONFIG_PATH,
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
    start_date: Annotated[
        datetime, typer.Option(help="Start date in YYYY-MM-DD format.")
    ] = None,
    end_date: Annotated[
        datetime, typer.Option(help="Start date in YYYY-MM-DD format.")
    ] = None,
    org_ids: Annotated[
        list[str],
        typer.Option(
            help="Filter to specified org_ids. Runs all configured orgs by default."
        ),
    ] = None,
):
    """
    Run AMI API adapters to fetch AMI data, then shape it into generalized format, then store it.
    """
    config = AMIAdapterConfiguration.from_yaml(config_file, secrets_file)

    adapters = config.adapters()
    if org_ids:
        adapters = [a for a in adapters if a.org_id in org_ids]

    run_id = f"run-{datetime.now().isoformat()}"

    logger.info(
        f"Running AMI Connect for adapters {", ".join(a.name() for a in adapters)} with parameters start_date={start_date} end_date={end_date}"
    )

    for adapter in adapters:
        start, end = adapter.calculate_extract_range(
            start_date,
            end_date,
        )
        logger.info(f"Extracting data for {adapter.name()} from {start} to {end}")
        adapter.extract_and_output(run_id, start, end)
        logger.info(f"Extracted data for {adapter.name()}")

        logger.info(f"Transforming data for {adapter.name()}")
        adapter.transform_and_output(run_id)
        logger.info(f"Transformed data for {adapter.name()}")

        logger.info(f"Loading raw data for {adapter.name()}")
        adapter.load_raw(run_id)
        logger.info(f"Loaded raw data for {adapter.name()}")

        logger.info(f"Loading transformed data for {adapter.name()}")
        adapter.load_transformed(run_id)
        logger.info(f"Loaded transformed data for {adapter.name()}")

    logger.info(f"Finished for {len(adapters)} adapters")


@app.command()
def download_intermediate_output(
    path: Annotated[
        str,
        typer.Argument(
            help="Path or prefix of path to files for download. If S3, can be anything after the bucket name, e.g. for s3://my-ami-connect-bucket/intermediate_outputs/scheduled__2025-09-15T19:25:00+00:00 you may enter intermediate_outputs/scheduled__2025-09-15T19:25:00+00:00 ."
        ),
    ],
    config_file: Annotated[
        str, typer.Option(help="Path to local config file.")
    ] = DEFAULT_CONFIG_PATH,
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Download intermediate output file(s) of provided name.
    """
    config = AMIAdapterConfiguration.from_yaml(config_file, secrets_file)

    # This code should not live here. It's copied from the base adapter class and it shouldn't live there either.
    configured_task_output_controller = config.task_output_controller()
    if (
        configured_task_output_controller.type
        == ConfiguredTaskOutputControllerType.LOCAL
    ):
        controller = LocalTaskOutputController(
            configured_task_output_controller.output_folder, None
        )
    elif (
        configured_task_output_controller.type == ConfiguredTaskOutputControllerType.S3
    ):
        controller = S3TaskOutputController(
            configured_task_output_controller.s3_bucket_name,
            "placeholder",
            aws_profile_name=configured_task_output_controller.dev_aws_profile_name,
        )
    else:
        raise ValueError(
            f"Task output configuration with invalid type {configured_task_output_controller.type}"
        )
    controller.download_for_path(path, "./output", decompress=True)


if __name__ == "__main__":
    app()

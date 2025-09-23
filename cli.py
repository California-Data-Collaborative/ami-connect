"""
CLI for AMI Connect that uses Typer (https://typer.tiangolo.com/) under the hood.

Run from root directory with:

    python cli.py --help

"""

from datetime import datetime
import logging
from pprint import pprint
from typing import List
from typing_extensions import Annotated

import typer

from amiadapters.config import (
    AMIAdapterConfiguration,
    ConfiguredTaskOutputControllerType,
)
from amiadapters.configuration.base import (
    add_source_configuration,
    get_configuration,
    remove_sink_configuration,
    update_sink_configuration,
    update_source_configuration,
    update_task_output_configuration,
)
from amiadapters.outputs.local import LocalTaskOutputController
from amiadapters.outputs.s3 import S3TaskOutputController

DEFAULT_CONFIG_PATH = "./config.yaml"
DEFAULT_SECRETS_PATH = "./secrets.yaml"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

app = typer.Typer()
# Sub-apps
config_app = typer.Typer(help="Configure the pipeline")
app.add_typer(config_app, name="config")


@app.command()
def run(
    config_file: Annotated[str, typer.Option(help="Path to local config file.")] = None,
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
    if config_file is not None:
        logger.info(
            f"Loading configuration from YAML at {config_file} and {secrets_file}"
        )
        config = AMIAdapterConfiguration.from_yaml(config_file, secrets_file)
    else:
        logger.info(
            f"Loading configuration from database using credentials from {secrets_file}"
        )
        config = AMIAdapterConfiguration.from_database(secrets_file)

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


########################################################################################
# Config commands
########################################################################################
@config_app.command()
def get(
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Get configuration from database.
    """
    sources, sinks, task_output, notifications, backfills = get_configuration(
        None, secrets_file
    )
    pprint(
        {
            "sources": sources,
            "sinks": sinks,
            "task_output": task_output,
            "notifications": notifications,
            "backfills": backfills,
        }
    )


@config_app.command()
def add_source(
    org_id: Annotated[
        str,
        typer.Argument(
            help="Often source's organization name and is used as unique identifier."
        ),
    ],
    type: Annotated[
        str,
        typer.Argument(help="Adapter type."),
    ],
    timezone: Annotated[
        str,
        typer.Argument(
            help="Timezone in which meter read timestamps and other timestamps are represented for this source."
        ),
    ],
    # Type-specific configurations
    use_raw_data_cache: Annotated[
        bool,
        typer.Option(
            help="If pipeline should use raw data cache. Applicable to types: [beacon_360, sentryx]"
        ),
    ] = None,
    sftp_host: Annotated[
        str, typer.Option(help="Applicable to types: [aclara]")
    ] = None,
    sftp_remote_data_directory: Annotated[
        str, typer.Option(help="Applicable to types: [aclara]")
    ] = None,
    sftp_local_download_directory: Annotated[
        str, typer.Option(help="Applicable to types: [aclara]")
    ] = None,
    sftp_local_known_hosts_file: Annotated[
        str, typer.Option(help="Applicable to types: [aclara]")
    ] = None,
    ssh_tunnel_server_host: Annotated[
        str,
        typer.Option(help="Applicable to types: [metersense, xylem_moulton_niguel]"),
    ] = None,
    ssh_tunnel_key_path: Annotated[
        str,
        typer.Option(help="Applicable to types: [metersense, xylem_moulton_niguel]"),
    ] = None,
    database_host: Annotated[
        str,
        typer.Option(help="Applicable to types: [metersense, xylem_moulton_niguel]"),
    ] = None,
    database_port: Annotated[
        str,
        typer.Option(help="Applicable to types: [metersense, xylem_moulton_niguel]"),
    ] = None,
    sinks: Annotated[
        List[str],
        typer.Option(
            help="Collection of sink IDs where data from this source should be stored."
        ),
    ] = None,
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Adds a new source with provided configuration. Different adapter types require specific configuration
    which you can provide as optional arguments to this command.
    """
    new_sink_configuration = {
        "org_id": org_id,
        "type": type,
        "timezone": timezone,
        "use_raw_data_cache": use_raw_data_cache,
        "sftp_host": sftp_host,
        "sftp_remote_data_directory": sftp_remote_data_directory,
        "sftp_local_known_hosts_file": sftp_local_known_hosts_file,
        "sftp_local_download_directory": sftp_local_download_directory,
        "ssh_tunnel_server_host": ssh_tunnel_server_host,
        "ssh_tunnel_key_path": ssh_tunnel_key_path,
        "database_host": database_host,
        "database_port": database_port,
        "sinks": sinks or [],
    }
    add_source_configuration(None, secrets_file, new_sink_configuration)


@config_app.command()
def add_sink(
    id: Annotated[
        str,
        typer.Argument(help="Name of sink used as unique identifier."),
    ],
    type: Annotated[
        str,
        typer.Argument(help="Sink type. Options are: [snowflake]"),
    ],
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Adds or updates a sink with provided configuration.
    """
    new_sink_configuration = {
        "id": id,
        "type": type,
    }
    update_sink_configuration(None, secrets_file, new_sink_configuration)


@config_app.command()
def remove_sink(
    id: Annotated[
        str,
        typer.Argument(help="Name of sink that should be removed from pipeline."),
    ],
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Removes a sink from the pipeline. Sink must not be used by any sources.
    """
    remove_sink_configuration(None, secrets_file, id)


@config_app.command()
def update_task_output(
    bucket_name: Annotated[
        str,
        typer.Argument(help="Name of S3 bucket used for task output."),
    ],
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Updates task output configuration in database. Assumes you're using S3 task output.
    """
    new_task_output_configuration = {
        "type": "s3",
        "s3_bucket": bucket_name,
        "local_output_path": None,
    }
    update_task_output_configuration(None, secrets_file, new_task_output_configuration)


if __name__ == "__main__":
    app()

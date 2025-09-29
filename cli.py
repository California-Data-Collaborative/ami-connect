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
    remove_source_configuration,
    update_backfill_configuration,
    update_notification_configuration,
    update_sink_configuration,
    update_source_configuration,
    update_task_output_configuration,
)
from amiadapters.outputs.local import LocalTaskOutputController
from amiadapters.outputs.s3 import S3TaskOutputController

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
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
    start_date: Annotated[
        datetime, typer.Option(help="Start date in YYYY-MM-DD format.")
    ] = None,
    end_date: Annotated[
        datetime, typer.Option(help="End date in YYYY-MM-DD format.")
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
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Download intermediate output file(s) of provided name.
    """
    config = AMIAdapterConfiguration.from_database(secrets_file)

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
    utility_name: Annotated[
        str,
        typer.Option(
            help="Name of utility as it appears in the Sentryx API URL. Applicable to types: [sentryx]"
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
    api_url: Annotated[
        str,
        typer.Option(
            help='Subeca API URL for this org, e.g. "https://my-utility.api.subeca.online". Applicable to types: [subeca]'
        ),
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
        "utility_name": utility_name,
        "sftp_host": sftp_host,
        "sftp_remote_data_directory": sftp_remote_data_directory,
        "sftp_local_known_hosts_file": sftp_local_known_hosts_file,
        "sftp_local_download_directory": sftp_local_download_directory,
        "ssh_tunnel_server_host": ssh_tunnel_server_host,
        "ssh_tunnel_key_path": ssh_tunnel_key_path,
        "database_host": database_host,
        "database_port": database_port,
        "api_url": api_url,
        "sinks": sinks or [],
    }
    add_source_configuration(None, secrets_file, new_sink_configuration)


@config_app.command()
def update_source(
    org_id: Annotated[
        str,
        typer.Argument(
            help="Often source's organization name and is used as unique identifier."
        ),
    ],
    type: Annotated[str, typer.Option(help="Adapter type.")] = None,
    timezone: Annotated[
        str,
        typer.Option(
            help="Timezone in which meter read timestamps and other timestamps are represented for this source."
        ),
    ] = None,
    # Type-specific configurations
    use_raw_data_cache: Annotated[
        bool,
        typer.Option(
            help="If pipeline should use raw data cache. Applicable to types: [beacon_360, sentryx]"
        ),
    ] = None,
    utility_name: Annotated[
        str,
        typer.Option(
            help="Name of utility as it appears in the Sentryx API URL. Applicable to types: [sentryx]"
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
    api_url: Annotated[
        str,
        typer.Option(
            help='Subeca API URL for this org, e.g. "https://my-utility.api.subeca.online". Applicable to types: [subeca]'
        ),
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
    new_sink_configuration = {"org_id": org_id}
    if type is not None:
        new_sink_configuration["type"] = type
    if timezone is not None:
        new_sink_configuration["timezone"] = timezone
    if use_raw_data_cache is not None:
        new_sink_configuration["use_raw_data_cache"] = use_raw_data_cache
    if utility_name is not None:
        new_sink_configuration["utility_name"] = utility_name
    if sftp_host is not None:
        new_sink_configuration["sftp_host"] = sftp_host
    if sftp_remote_data_directory is not None:
        new_sink_configuration["sftp_remote_data_directory"] = (
            sftp_remote_data_directory
        )
    if sftp_local_known_hosts_file is not None:
        new_sink_configuration["sftp_local_known_hosts_file"] = (
            sftp_local_known_hosts_file
        )
    if sftp_local_download_directory is not None:
        new_sink_configuration["sftp_local_download_directory"] = (
            sftp_local_download_directory
        )
    if ssh_tunnel_server_host is not None:
        new_sink_configuration["ssh_tunnel_server_host"] = ssh_tunnel_server_host
    if ssh_tunnel_key_path is not None:
        new_sink_configuration["ssh_tunnel_key_path"] = ssh_tunnel_key_path
    if database_host is not None:
        new_sink_configuration["database_host"] = database_host
    if database_port is not None:
        new_sink_configuration["database_port"] = database_port
    if api_url is not None:
        new_sink_configuration["api_url"] = api_url
    if sinks is not None:
        new_sink_configuration["sinks"] = sinks
    update_source_configuration(None, secrets_file, new_sink_configuration)


@config_app.command()
def remove_source(
    org_id: Annotated[
        str,
        typer.Argument(
            help="Often source's organization name and is used as unique identifier."
        ),
    ],
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Remove source from configuration, including all associated configuration.
    """
    remove_source_configuration(None, secrets_file, org_id)


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
    # TODO Must add sink health checks
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


@config_app.command()
def update_backfill(
    org_id: Annotated[
        str,
        typer.Argument(
            help="Often source's organization name and is used as unique identifier."
        ),
    ],
    start_date: Annotated[
        datetime,
        typer.Argument(
            help="Earliest date in range to be backfilled in YYYY-MM-DD format."
        ),
    ],
    end_date: Annotated[
        datetime,
        typer.Argument(
            help="Latest date in range to be backfilled in YYYY-MM-DD format."
        ),
    ],
    interval_days: Annotated[
        int,
        typer.Argument(help="Number of days to extract for each run."),
    ],
    schedule: Annotated[
        str,
        typer.Argument(
            help='Crontab-format schedule for backfill, e.g. "15 * * * *" for every hour at the 15th minute. Put it in quotes.'
        ),
    ],
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Updates backfill configuration in database. Matches on org_id+start_date+end_date for update, else adds new backfill.
    """
    new_backfill_configuration = {
        "org_id": org_id,
        "start_date": start_date,
        "end_date": end_date,
        "interval_days": interval_days,
        "schedule": schedule,
    }
    update_backfill_configuration(None, secrets_file, new_backfill_configuration)


@config_app.command()
def update_notification(
    sns_arn: Annotated[
        str,
        typer.Argument(help="Identifies AWS SNS topic used to send notifications."),
    ],
    secrets_file: Annotated[
        str, typer.Option(help="Path to local secrets file.")
    ] = DEFAULT_SECRETS_PATH,
):
    """
    Updates notification configuration in database. As of this writing, assumes you have one row for the failure notification SNS arn.
    """
    new_notification_configuration = {
        "event_type": "dag_failure",
        "sns_arn": sns_arn,
    }
    update_notification_configuration(
        None, secrets_file, new_notification_configuration
    )


if __name__ == "__main__":
    app()

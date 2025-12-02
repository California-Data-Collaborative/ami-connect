"""
CLI for AMI Connect that uses Typer (https://typer.tiangolo.com/) under the hood.

Run from root directory with:

    python cli.py --help

"""

from dataclasses import asdict, fields
from datetime import datetime, timedelta
from functools import wraps
import logging
from pprint import pprint
from typing import List
from typing_extensions import Annotated

import typer

from amiadapters.config import AMIAdapterConfiguration
from amiadapters.configuration.base import (
    add_data_quality_check_configurations,
    add_source_configuration,
    get_configuration,
    remove_backfill_configuration,
    remove_data_quality_check_configurations,
    remove_secret_configuration,
    remove_sink_configuration,
    remove_source_configuration,
    update_backfill_configuration,
    update_notification_configuration,
    update_post_processor_configuration,
    update_secret_configuration,
    update_sink_configuration,
    update_source_configuration,
    update_task_output_configuration,
)
from amiadapters.configuration.env import set_global_aws_profile, set_global_aws_region
from amiadapters.configuration.models import (
    IntermediateOutputType,
    get_secrets_class_type,
)
from amiadapters.configuration.secrets import get_secrets, SecretType
from amiadapters.events.base import EventSubscriber
from amiadapters.outputs.local import LocalTaskOutputController
from amiadapters.outputs.s3 import S3TaskOutputController

HELP_MESSAGE__PROFILE = "Name of pipeline profile to get configuration for. Expected to match the name of the AWS profile in your AWS credentials file."
ANNOTATION__PROFILE = Annotated[
    str,
    typer.Option(
        help=HELP_MESSAGE__PROFILE,
    ),
]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

app = typer.Typer()
# Sub-apps
config_app = typer.Typer(help="Configure the pipeline")
app.add_typer(config_app, name="config")


def sets_environment_from_profile(func):
    """
    Decorator to set environment variables and AWS settings from AWS profile name before running command.

    Any command that accesses secrets or configuration from the database should be decorated with this, and
    it should have the "profile" keyword argument.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        if kwargs.get("local") is None:
            set_global_aws_region("us-west-2")
            set_global_aws_profile(kwargs.get("profile"))
        return func(*args, **kwargs)

    return wrapper


@app.command()
@sets_environment_from_profile
def run(
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
    profile: ANNOTATION__PROFILE = None,
    local: Annotated[
        str,
        typer.Option(
            help="Local configuration file that includes secret and non-secret configuration. Used instead of database configuration if specified. Meant for local development."
        ),
    ] = None,
):
    """
    Run AMI API adapters to fetch AMI data, then shape it into generalized format, then store it.
    """
    if local is not None:
        logger.info(f"Loading configuration from local file {local}")
        config = AMIAdapterConfiguration.from_yaml(local, local)
    else:
        logger.info(f"Loading configuration from database using profile {profile}")
        config = AMIAdapterConfiguration.from_database()

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
            lag=timedelta(days=0),
            # Default two interval of two days if start or end is not specified
            interval=timedelta(days=2),
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

        logger.info(f"Executing postprocess data for {adapter.name()}")
        adapter.post_process(run_id, start, end)
        logger.info(f"Executed postprocess data for {adapter.name()}")

    logger.info(f"Finished for {len(adapters)} adapters")


@app.command()
@sets_environment_from_profile
def download_intermediate_output(
    path: Annotated[
        str,
        typer.Argument(
            help="Path or prefix of path to files for download. If S3, can be anything after the bucket name, e.g. for s3://my-ami-connect-bucket/intermediate_outputs/scheduled__2025-09-15T19:25:00+00:00 you may enter intermediate_outputs/scheduled__2025-09-15T19:25:00+00:00 ."
        ),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Download intermediate output file(s) of provided name.
    """
    config = AMIAdapterConfiguration.from_database()

    # This code should not live here. It's copied from the base adapter class and it shouldn't live there either.
    configured_task_output_controller = config.task_output_controller()
    if configured_task_output_controller.type == IntermediateOutputType.LOCAL.value:
        controller = LocalTaskOutputController(
            configured_task_output_controller.output_folder, None
        )
    elif configured_task_output_controller.type == IntermediateOutputType.S3.value:
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


@app.command()
@sets_environment_from_profile
def read_event_queue(
    name: Annotated[
        str,
        typer.Argument(
            help="Path or prefix of path to files for download. If S3, can be anything after the bucket name, e.g. for s3://my-ami-connect-bucket/intermediate_outputs/scheduled__2025-09-15T19:25:00+00:00 you may enter intermediate_outputs/scheduled__2025-09-15T19:25:00+00:00 ."
        ),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Read a message from the given event queue. Helpful for debugging changes to the event publisher system.
    """
    subscriber = EventSubscriber()
    subscriber.print_message_from_queue(name)


########################################################################################
# Config commands
########################################################################################
@config_app.command()
@sets_environment_from_profile
def get(
    show_secrets: Annotated[
        bool,
        typer.Option(
            help="If true, will show secrets in output. Default is false for security reasons."
        ),
    ] = False,
    profile: ANNOTATION__PROFILE = None,
):
    """
    Get configuration from database.
    """
    secrets = get_secrets()
    sources, sinks, pipeline, notifications, backfills = get_configuration(secrets)

    result = {
        "sources": sources,
        "sinks": sinks,
        "pipeline": asdict(pipeline),
        "notifications": notifications,
        "backfills": backfills,
    }
    if show_secrets:
        result["secrets"] = secrets

    pprint(result)


@config_app.command()
@sets_environment_from_profile
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
    external_adapter_location: Annotated[
        str,
        typer.Option(
            help="Path on Airflow server to Neptune adapter module. Applicable to types: [neptune]"
        ),
    ] = None,
    sinks: Annotated[
        List[str],
        typer.Option(
            help="Collection of sink IDs where data from this source should be stored."
        ),
    ] = None,
    profile: ANNOTATION__PROFILE = None,
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
        "external_adapter_location": external_adapter_location,
        "sinks": sinks or [],
    }
    add_source_configuration(new_sink_configuration)


@config_app.command()
@sets_environment_from_profile
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
    external_adapter_location: Annotated[
        str,
        typer.Option(
            help="Path on Airflow server to Neptune adapter module. Applicable to types: [neptune]"
        ),
    ] = None,
    sinks: Annotated[
        List[str],
        typer.Option(
            help="Collection of sink IDs where data from this source should be stored."
        ),
    ] = None,
    profile: ANNOTATION__PROFILE = None,
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
    if external_adapter_location is not None:
        new_sink_configuration["external_adapter_location"] = external_adapter_location
    if sinks is not None:
        new_sink_configuration["sinks"] = sinks
    update_source_configuration(new_sink_configuration)


@config_app.command()
@sets_environment_from_profile
def remove_source(
    org_id: Annotated[
        str,
        typer.Argument(
            help="Often source's organization name and is used as unique identifier."
        ),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Remove source from configuration, including all associated configuration.
    """
    remove_source_configuration(org_id)


@config_app.command()
@sets_environment_from_profile
def add_sink(
    id: Annotated[
        str,
        typer.Argument(help="Name of sink used as unique identifier."),
    ],
    type: Annotated[
        str,
        typer.Argument(help="Sink type. Options are: [snowflake]"),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Adds or updates a sink with provided configuration.
    """
    new_sink_configuration = {
        "id": id,
        "type": type,
    }
    update_sink_configuration(new_sink_configuration)


@config_app.command()
@sets_environment_from_profile
def remove_sink(
    id: Annotated[
        str,
        typer.Argument(help="Name of sink that should be removed from pipeline."),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Removes a sink from the pipeline. Sink must not be used by any sources.
    """
    remove_sink_configuration(id)


@config_app.command()
@sets_environment_from_profile
def update_task_output(
    bucket_name: Annotated[
        str,
        typer.Argument(help="Name of S3 bucket used for task output."),
    ],
    dev_profile: Annotated[
        str,
        typer.Argument(
            help="Name of AWS profile used to access AWS credentials during local development."
        ),
    ],
    profile: Annotated[str, typer.Option(help=HELP_MESSAGE__PROFILE)] = None,
):
    """
    Updates task output configuration in database. Assumes you're using S3 task output. Requires all arguments
    because we erase and recreate entire task output configuration when we make this update.
    """
    new_task_output_configuration = {
        "type": "s3",
        "s3_bucket": bucket_name,
        "dev_profile": dev_profile,
        "local_output_path": None,
    }
    update_task_output_configuration(new_task_output_configuration)


@config_app.command()
@sets_environment_from_profile
def update_post_processor(
    should_run: Annotated[
        bool,
        typer.Argument(help="True if post processors should run, else False."),
    ],
    profile: Annotated[str, typer.Option(help=HELP_MESSAGE__PROFILE)] = None,
):
    """
    Updates task output configuration in database. Assumes you're using S3 task output. Requires all arguments
    because we erase and recreate entire task output configuration when we make this update.
    """
    update_post_processor_configuration(should_run)


@config_app.command()
@sets_environment_from_profile
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
    profile: ANNOTATION__PROFILE = None,
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
    update_backfill_configuration(new_backfill_configuration)


@config_app.command()
@sets_environment_from_profile
def remove_backfill(
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
    profile: ANNOTATION__PROFILE = None,
):
    """
    Removes backfill configuration in database. Matches on org_id+start_date+end_date.
    """
    remove_backfill_configuration(org_id, start_date, end_date)


@config_app.command()
@sets_environment_from_profile
def update_notification(
    sns_arn: Annotated[
        str,
        typer.Argument(help="Identifies AWS SNS topic used to send notifications."),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Updates notification configuration in database. As of this writing, assumes you have one row for the failure notification SNS arn.
    """
    new_notification_configuration = {
        "event_type": "dag_failure",
        "sns_arn": sns_arn,
    }
    update_notification_configuration(new_notification_configuration)


@config_app.command()
@sets_environment_from_profile
def add_data_quality_checks(
    sink_id: Annotated[
        str,
        typer.Argument(help="Name of sink used as unique identifier."),
    ],
    checks: Annotated[
        List[str],
        typer.Argument(help="Data quality check names that will be added to the sink."),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Adds new data quality checks to the sink.
    """
    new_checks_configuration = {
        "sink_id": sink_id,
        "check_names": checks,
    }
    add_data_quality_check_configurations(new_checks_configuration)


@config_app.command()
@sets_environment_from_profile
def remove_data_quality_checks(
    sink_id: Annotated[
        str,
        typer.Argument(help="Name of sink used as unique identifier."),
    ],
    checks: Annotated[
        List[str],
        typer.Argument(
            help="Data quality check names that will be removed from the sink."
        ),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Removes data quality checks from the sink.
    """
    checks_configuration = {
        "sink_id": sink_id,
        "check_names": checks,
    }
    remove_data_quality_check_configurations(checks_configuration)


@config_app.command()
@sets_environment_from_profile
def update_secret(
    secret_name: Annotated[
        str,
        typer.Argument(
            help="Name of source or sink that uses these secrets as specified in the configuration."
        ),
    ],
    sink_type: Annotated[
        str,
        typer.Option(
            help="Type of sink, e.g. 'snowflake'. Must be specified if secret_type is 'sink'."
        ),
    ] = None,
    source_type: Annotated[
        str,
        typer.Option(
            help="Type of source, e.g. 'subeca'. Must be specified if secret_type is 'source'."
        ),
    ] = None,
    profile: ANNOTATION__PROFILE = None,
    # Type-specific options
    account: Annotated[str, typer.Option(help="Snowflake account name.")] = None,
    user: Annotated[
        str, typer.Option(help="Snowflake user name or other user name.")
    ] = None,
    password: Annotated[
        str, typer.Option(help="Snowflake password or other password.")
    ] = None,
    role: Annotated[str, typer.Option(help="Snowflake role name.")] = None,
    warehouse: Annotated[str, typer.Option(help="Snowflake warehouse name.")] = None,
    database: Annotated[str, typer.Option(help="Snowflake database name.")] = None,
    schema: Annotated[str, typer.Option(help="Snowflake schema name.")] = None,
    api_key: Annotated[str, typer.Option(help="API key, e.g. for Subeca.")] = None,
    sftp_user: Annotated[str, typer.Option(help="SFTP user, e.g. for Aclara.")] = None,
    sftp_password: Annotated[
        str, typer.Option(help="SFTP password, e.g. for Aclara.")
    ] = None,
    ssh_tunnel_username: Annotated[
        str, typer.Option(help="SSH tunnel username, e.g. for Metersense.")
    ] = None,
    ssh_tunnel_private_key: Annotated[
        str,
        typer.Option(
            help="Path to file with SSH tunnel private key, e.g. for Metersense. We read the file's contents and store the contents in the secrets system."
        ),
    ] = None,
    database_db_name: Annotated[
        str, typer.Option(help="Name of database, e.g. for Metersense.")
    ] = None,
    database_user: Annotated[
        str, typer.Option(help="Database username, e.g. for Metersense.")
    ] = None,
    database_password: Annotated[
        str, typer.Option(help="Database password, e.g. for Metersense.")
    ] = None,
    site_id: Annotated[str, typer.Option(help="Site ID, e.g. for Neptune.")] = None,
    client_id: Annotated[str, typer.Option(help="Client ID, e.g. for Neptune.")] = None,
    client_secret: Annotated[
        str, typer.Option(help="Client secret, e.g. for Neptune.")
    ] = None,
):
    """
    Creates or updates a secret. Matches on secret_type+secret_name for update, else adds new secret.
    """
    if sink_type and source_type:
        raise ValueError("Can only specify one of sink_type or source_type, not both.")
    if not sink_type and not source_type:
        raise ValueError("Must specify one of sink_type or source_type.")

    secret_type = SecretType.SOURCES.value if source_type else SecretType.SINKS.value

    if secret_type == SecretType.SOURCES.value and source_type not in [
        "subeca",
        "aclara",
        "beacon_360",
        "sentryx",
        "metersense",
        "xylem_moulton_niguel",
        "neptune",
    ]:
        raise ValueError(
            "source_type must be specified as one of ['subeca', 'aclara', 'beacon_360', 'sentryx', 'metersense', 'xylem_moulton_niguel', 'neptune'] if secret_type is 'source'"
        )
    if secret_type == SecretType.SINKS.value and sink_type not in ["snowflake"]:
        raise ValueError(
            "sink_type must be specified as 'snowflake' if secret_type is 'sink'"
        )

    # dataclass that represents this secret, e.g. SubecaSecrets, SnowflakeSecrets, etc
    secrets_dataclass = get_secrets_class_type(
        source_type if secret_type == SecretType.SOURCES.value else sink_type
    )
    required_fields = fields(secrets_dataclass)

    type_specific_options = {
        "account": account,
        "user": user,
        "password": password,
        "role": role,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "api_key": api_key,
        "sftp_user": sftp_user,
        "sftp_password": sftp_password,
        "ssh_tunnel_username": ssh_tunnel_username,
        "ssh_tunnel_private_key": ssh_tunnel_private_key,
        "database_db_name": database_db_name,
        "database_user": database_user,
        "database_password": database_password,
        "site_id": site_id,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if ssh_tunnel_private_key:
        # Allow user to pass in path to private key file instead of raw key
        with open(ssh_tunnel_private_key, "r") as f:
            type_specific_options["ssh_tunnel_private_key"] = f.read()

    missing_fields = [
        f.name for f in required_fields if type_specific_options.get(f.name) is None
    ]
    if missing_fields:
        raise ValueError(f"Missing values for: {', '.join(missing_fields)}")

    # Instantiate the dataclass with the type-specific fields that are required
    secrets = secrets_dataclass(
        **{f.name: type_specific_options[f.name] for f in required_fields}
    )

    update_secret_configuration(secret_type, secret_name, secrets)


@config_app.command()
@sets_environment_from_profile
def remove_secret(
    secret_type: Annotated[
        str,
        typer.Argument(help="Type of secret. Choices: source, sink"),
    ],
    secret_name: Annotated[
        str,
        typer.Argument(
            help="Name of source or sink that uses these secrets as specified in the configuration."
        ),
    ],
    profile: ANNOTATION__PROFILE = None,
):
    """
    Removes a secret. Matches on secret_type+secret_name.
    """
    if secret_type not in ["source", "sink"]:
        raise ValueError('secret_type must be one of ["source", "sink"]')
    remove_secret_configuration(secret_type, secret_name)


if __name__ == "__main__":
    app()

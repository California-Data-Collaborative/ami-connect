from datetime import datetime
import logging

import snowflake.connector
import yaml

from amiadapters.configuration import database
from amiadapters.configuration import secrets

logger = logging.getLogger(__name__)


def get_configuration(secrets: dict) -> dict:
    logger.info(f"Getting configuration from database.")
    connection = create_snowflake_from_secrets(secrets)
    return database.get_configuration(connection)


def add_source_configuration(
    config_file: str, secrets_file: str, new_source_configuration: dict
):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(f"Adding sources in database with {new_source_configuration}")
        connection = create_snowflake_from_secrets_file(secrets_file)
        return database.add_source_configuration(connection, new_source_configuration)


def update_source_configuration(
    config_file: str, secrets_file: str, new_source_configuration: dict
):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(f"Updating sources in database with {new_source_configuration}")
        connection = create_snowflake_from_secrets_file(secrets_file)
        return database.update_source_configuration(
            connection, new_source_configuration
        )


def remove_source_configuration(config_file: str, secrets_file: str, org_id: str):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(f"Removing source from database with org_id {org_id}")
        connection = create_snowflake_from_secrets_file(secrets_file)
        return database.remove_source_configuration(connection, org_id)


def update_sink_configuration(
    config_file: str, secrets_file: str, new_sink_configuration: dict
):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(f"Updating sinks in database with {new_sink_configuration}")
        connection = create_snowflake_from_secrets_file(secrets_file)
        return database.update_sink_configuration(connection, new_sink_configuration)


def remove_sink_configuration(config_file: str, secrets_file: str, id: str):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(f"Removing sink {id} from database.")
        connection = create_snowflake_from_secrets_file(secrets_file)
        return database.remove_sink_configuration(connection, id)


def update_task_output_configuration(new_task_output_configuration: dict):
    logger.info(
        f"Updating task output configuration in database to {new_task_output_configuration}"
    )
    all_secrets = secrets.get_secrets()
    connection = create_snowflake_from_secrets(all_secrets)
    database.update_task_output_configuration(connection, new_task_output_configuration)


def update_post_processor_configuration(should_run_post_processor: bool):
    logger.info(
        f"Updating post processor configuration in database to {should_run_post_processor}"
    )
    all_secrets = secrets.get_secrets()
    connection = create_snowflake_from_secrets(all_secrets)
    database.update_post_processor_configuration(connection, should_run_post_processor)


def update_backfill_configuration(
    config_file: str, secrets_file: str, new_backfill_configuration: dict
):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(
            f"Updating backfill configuration in database to {new_backfill_configuration}"
        )
        connection = create_snowflake_from_secrets_file(secrets_file)
        database.update_backfill_configuration(connection, new_backfill_configuration)


def remove_backfill_configuration(
    config_file: str,
    secrets_file: str,
    org_id: str,
    start_date: datetime,
    end_date: datetime,
):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(
            f"Removing backfill configuration in database for {org_id} {start_date} {end_date}"
        )
        connection = create_snowflake_from_secrets_file(secrets_file)
        database.remove_backfill_configuration(connection, org_id, start_date, end_date)


def update_notification_configuration(
    config_file: str, secrets_file: str, new_notification_configuration: dict
):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(
            f"Updating notification configuration in database to {new_notification_configuration}"
        )
        connection = create_snowflake_from_secrets_file(secrets_file)
        database.update_notification_configuration(
            connection, new_notification_configuration
        )


def add_data_quality_check_configurations(
    config_file: str, secrets_file: str, new_checks_configuration: dict
):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(
            f"Adding data quality checks to database with {new_checks_configuration}"
        )
        connection = create_snowflake_from_secrets_file(secrets_file)
        database.add_data_quality_check_configurations(
            connection, new_checks_configuration
        )


def remove_data_quality_check_configurations(
    config_file: str, secrets_file: str, checks_configuration: dict
):
    if _use_database_for_config(config_file, secrets_file):
        logger.info(
            f"Removing data quality checks from database with {checks_configuration}"
        )
        connection = create_snowflake_from_secrets_file(secrets_file)
        database.remove_data_quality_check_configurations(
            connection, checks_configuration
        )


def update_secret_configuration(secret_type: str, secret_name: str, new_secrets):
    logger.info(f"Updating secret {secret_name} of type {secret_type}")
    secrets.update_secret_configuration(secret_type, secret_name, new_secrets)


def remove_secret_configuration(secret_type: str, secret_name: str):
    logger.info(f"Removing secret {secret_name} of type {secret_type}")
    secrets.remove_secret_configuration(secret_type, secret_name)


def _use_database_for_config(config_file: str, secrets_file: str) -> bool:
    return config_file is None


def create_snowflake_connection(
    account: str = None,
    user: str = None,
    password: str = None,
    warehouse: str = None,
    database: str = None,
    schema: str = None,
    role: str = None,
):
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        paramstyle="qmark",
    )


def create_snowflake_from_secrets(secrets: dict):
    if "sinks" not in secrets or len(secrets["sinks"]) == 0:
        raise ValueError("No credentials found to connect to Snowflake.")
    snowflake_credentials = list(secrets["sinks"].values())[0]
    return create_snowflake_connection(
        account=snowflake_credentials["account"],
        user=snowflake_credentials["user"],
        password=snowflake_credentials["password"],
        warehouse=snowflake_credentials["warehouse"],
        database=snowflake_credentials["database"],
        schema=snowflake_credentials["schema"],
        role=snowflake_credentials["role"],
    )


def create_snowflake_from_secrets_file(secrets_file: str):
    # For now, load secrets from YAML. In the near future we will load from a more secure source.
    with open(secrets_file, "r") as f:
        secrets = yaml.safe_load(f)
    return create_snowflake_from_secrets(secrets)

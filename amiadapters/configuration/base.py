import logging

import snowflake.connector
import yaml

from amiadapters.configuration import database

logger = logging.getLogger(__name__)


def update_task_output_configuration(
    config_file: str, secrets_file: str, new_task_output_configuration
):
    if config_file is None:
        logger.info(
            f"Updating task output configuration in database to {new_task_output_configuration}"
        )
        connection = create_snowflake_from_secrets_file(secrets_file)
        database.update_task_output_configuration(
            connection, new_task_output_configuration
        )


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


def create_snowflake_from_secrets_file(secrets_file: str):
    # For now, load secrets from YAML. In the near future we will load from a more secure source.
    with open(secrets_file, "r") as f:
        secrets = yaml.safe_load(f)
    # When we have better secrets management, find a better way of accessing this information
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

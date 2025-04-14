"""
CLI for AMI Connect that uses Typer (https://typer.tiangolo.com/) under the hood.

Run from root directory with:

    python cli.py

"""

import logging
from typing_extensions import Annotated

import snowflake.connector
import typer

from amiadapters.config import AMIAdapterConfiguration
from amiadapters.run import run_pipeline

DEFAULT_CONFIG_PATH = "./config.yaml"
DEFAULT_SECRETS_PATH = "./secrets.yaml"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


@app.command()
def init(
    config: Annotated[str, typer.Argument()] = DEFAULT_CONFIG_PATH,
    secrets: Annotated[str, typer.Argument()] = DEFAULT_SECRETS_PATH,
):
    """
    CLI for AMI Connect system.
    """
    logging.info(
        f"Creating Snowflake tables with credentials from {config} and {secrets}"
    )
    config = AMIAdapterConfiguration.from_yaml(config, secrets)
    _init_snowflake(config)
    


def _init_snowflake(config: AMIAdapterConfiguration):
    snowflake_conn = snowflake.connector.connect(
        account=config.secrets.account,
        user=config.secrets.user,
        password=config.secrets.password,
        warehouse=config.secrets.warehouse,
        database=config.secrets.database,
        schema=config.secrets.schema,
        role=config.secrets.role,
        paramstyle="qmark",
    )
    logging.info(
        f"Creating Snowflake tables with credentials from {config} and {secrets}"
    )


@app.command()
def run(
    config: Annotated[str, typer.Argument()] = DEFAULT_CONFIG_PATH,
    secrets: Annotated[str, typer.Argument()] = DEFAULT_SECRETS_PATH,
):
    """
    CLI for AMI Connect system.
    """

    run_pipeline("./config.yaml", "./secrets.yaml")


if __name__ == "__main__":
    app()

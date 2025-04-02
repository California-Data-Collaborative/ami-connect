from datetime import datetime
import os

from airflow.decorators import dag, task
import snowflake.connector

from amiadapters.base import BaseAMIAdapter
from amiadapters.beacon import Beacon360Adapter
from amiadapters.config import AMIAdapterConfiguration
from amiadapters.sentryx import SentryxAdapter


@dag(
    schedule=None,
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=["ami"],
)
def ami_control_dag():

    config = AMIAdapterConfiguration.from_env()
    adapters = [
        # Do not load this data into CaDC Snowflake until we get CaDC-associated credentials for the Sentryx API
        # SentryxAdapter(config),
        Beacon360Adapter(config),
    ]

    snowflake_conn = snowflake.connector.connect(
        account=config.snowflake_account,
        user=config.snowflake_user,
        password=config.snowflake_password,
        warehouse=config.snowflake_warehouse,
        database=config.snowflake_database,
        schema=config.snowflake_schema,
        role=config.snowflake_role,
        paramstyle='qmark',
    )

    @task()
    def extract(adapter: BaseAMIAdapter):
        adapter.extract()

    # @task()
    # def transform(adapter: BaseAMIAdapter):
    #     adapter.transform()

    @task()
    def load_base(adapter: BaseAMIAdapter, snowflake_conn):
        if isinstance(adapter, Beacon360Adapter):
            sql, rows = adapter.load_base_sql()
            snowflake_conn.cursor().executemany(sql, rows)
    
    @task.bash()
    def run_dbt() -> str:
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        return f"cd {dag_folder}/dbt && dbt run"

    # Retrieve data from AMI data sources
    for adapter in adapters:
        extract.override(task_id=f"extract-{adapter.name()}")(adapter)

    # for adapter in adapters:
    #     transform.override(task_id=f"transform-{adapter.name()}")(adapter)

    # Load raw data into database
    for adapter in adapters:
        load_base.override(task_id=f"load-base-{adapter.name()}")(adapter, snowflake_conn)
    
    # Use dbt to transform raw data into generalized format
    run_dbt()


ami_control_dag()

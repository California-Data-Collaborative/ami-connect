from datetime import datetime

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
        # SentryxAdapter(config),
        Beacon360Adapter(config),
    ]

    @task()
    def extract(adapter: BaseAMIAdapter):
        adapter.extract()

    @task()
    def transform(adapter: BaseAMIAdapter):
        adapter.transform()
    
    @task()
    def load_raw(adapter: BaseAMIAdapter, config: AMIAdapterConfiguration):
        adapter.load_raw(config)
    
    @task()
    def load_transformed(adapter: BaseAMIAdapter, config: AMIAdapterConfiguration):
        adapter.load_transformed(config)

    for adapter in adapters:
        extract.override(task_id=f"extract-{adapter.name()}")(adapter)

    for adapter in adapters:
        transform.override(task_id=f"transform-{adapter.name()}")(adapter)
    
    for adapter in adapters:
        load_raw.override(task_id=f"load-raw-{adapter.name()}")(adapter, config)
    
    for adapter in adapters:
        load_transformed.override(task_id=f"load-transformed-{adapter.name()}")(adapter, config)


ami_control_dag()

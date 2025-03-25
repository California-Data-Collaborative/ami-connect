from datetime import datetime

from airflow.decorators import dag, task

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
        SentryxAdapter(config),
        Beacon360Adapter(config),
    ]

    @task()
    def extract(adapter: BaseAMIAdapter):
        adapter.extract()
    
    @task()
    def transform(adapter: BaseAMIAdapter):
        adapter.transform()

    for adapter in adapters:
        extract.override(task_id=f"extract-{adapter.name()}")(adapter)
    
    for adapter in adapters:
        transform.override(task_id=f"transform-{adapter.name()}")(adapter)


ami_control_dag()
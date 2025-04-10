from datetime import datetime
import os
import pathlib

from airflow.decorators import dag, task

from amiadapters.base import BaseAMIAdapter
from amiadapters.config import AMIAdapterConfiguration


@dag(
    schedule=None,
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=["ami"],
)
def ami_control_dag():

    @task()
    def extract(adapter: BaseAMIAdapter):
        adapter.extract()

    @task()
    def transform(adapter: BaseAMIAdapter):
        adapter.transform()

    @task()
    def load_raw(adapter: BaseAMIAdapter):
        adapter.load_raw()

    @task()
    def load_transformed(adapter: BaseAMIAdapter):
        adapter.load_transformed()

    config = AMIAdapterConfiguration.from_yaml(
        os.environ.get(
            "AMI_CONFIG_YAML",
            pathlib.Path(__file__).joinpath("..", "..", "..", "config.yaml").resolve(),
        ),
        os.environ.get(
            "AMI_SECRET_YAML",
            pathlib.Path(__file__).joinpath("..", "..", "..", "secrets.yaml").resolve(),
        ),
    )

    adapters = config.adapters()

    for adapter in adapters:
        extract.override(task_id=f"extract-{adapter.name()}")(adapter)

    for adapter in adapters:
        transform.override(task_id=f"transform-{adapter.name()}")(adapter)

    for adapter in adapters:
        load_raw.override(task_id=f"load-raw-{adapter.name()}")(adapter)

    for adapter in adapters:
        load_transformed.override(task_id=f"load-transformed-{adapter.name()}")(adapter)


ami_control_dag()

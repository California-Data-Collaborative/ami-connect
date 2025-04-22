from datetime import datetime
import os
import pathlib

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from amiadapters.base import BaseAMIAdapter, default_date_range
from amiadapters.config import AMIAdapterConfiguration


@dag(
    schedule=None,
    params={
        "extract_range_start": Param(
            type="string",
            description="Start of date range for which we'll extract meter read data",
            default=None,
        ),
        "extract_range_end": Param(
            type="string",
            description="End of date range for which we'll extract meter read data",
            default=None,
        ),
    },
    catchup=False,
    tags=["ami"],
)
def ami_control_dag():

    @task()
    def extract(adapter: BaseAMIAdapter, **context):
        start, end = (
            context["params"]["extract_range_start"],
            context["params"]["extract_range_end"],
        )

        if isinstance(start, str):
            start = datetime.fromisoformat(start)
        if isinstance(end, str):
            end = datetime.fromisoformat(end)

        if start is None or end is None:
            start, end = default_date_range(start, end)

        adapter.extract(start, end)

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
            pathlib.Path(__file__).joinpath("..", "..", "config.yaml").resolve(),
        ),
        os.environ.get(
            "AMI_SECRET_YAML",
            pathlib.Path(__file__).joinpath("..", "..", "secrets.yaml").resolve(),
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

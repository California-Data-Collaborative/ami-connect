from abc import ABC, abstractmethod
from typing import List

from amiadapters.config import ConfiguredStorageSink
from amiadapters.models import GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput
from datetime import datetime


class BaseAMIStorageSink(ABC):
    """
    A storage sink is any place the AMI Connect pipeline
    outputs data. Examples: Snowflake, local storage.
    """

    def __init__(
        self,
        sink_config: ConfiguredStorageSink,
    ):
        self.sink_config = sink_config

    @abstractmethod
    def store_raw(self, run_id: str, extract_outputs: ExtractOutput):
        pass

    @abstractmethod
    def store_transformed(
        self, run_id: str, meters: List[GeneralMeter], reads: List[GeneralMeterRead]
    ):
        pass

    @abstractmethod
    def exec_postprocessor(self, run_id: str, min_date: datetime, max_date: datetime):
        pass


class BaseAMIDataQualityCheck(ABC):
    """
    A data quality check for the data stored in this sink. A sink may have many
    data quality checks. They're run together to ensure the data in the sink meets
    standard assumptions we've built into our data model.
    """

    # Stores class definition of all concrete data quality check implementations
    all_checks = []

    def __init_subclass__(cls, **kwargs):
        """
        This hook is called when python parses any of this class's child classes.
        We use it to keep track of all child classes so we can automatically instantiate
        them from the configuration.
        """
        super().__init_subclass__(**kwargs)
        BaseAMIDataQualityCheck.all_checks.append(cls)

    def __init__(self, connection):
        self.connection = connection

    @abstractmethod
    def name(self) -> str:
        """
        Name of this check, hyphenated, used to identify the check in our configuration system.
        """
        pass

    @abstractmethod
    def check(self) -> bool:
        """
        Run the check.

        :return: True if check passes, else False.
        """
        pass

    @abstractmethod
    def notify_on_failure(self) -> bool:
        """
        :return: True if we should notify users when this check fails, else False.
        """
        pass

    @classmethod
    def get_all_checks_by_name(cls, connection) -> dict:
        """
        Automatically find all subclasses, instantiate them,
        and return as a map of check name to instance of that check.
        """
        # This is a hack to make python import the data quality check subclass
        # definitions, which adds them to cls.all_checks. We do this so that
        # subclasses are automatically registered, but someday we might decide
        # that's not worth the hackery.
        from amiadapters.storage import snowflake

        result = {}
        for check in cls.all_checks:
            instance = check(connection)
            result[instance.name()] = instance
        return result

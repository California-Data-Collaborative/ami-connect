from dataclasses import dataclass
from datetime import datetime
from typing import List, Union
import pathlib
from pytz import timezone, UTC
from pytz.tzinfo import DstTzInfo
import yaml

import snowflake.connector


class AMIAdapterConfiguration:

    def __init__(self, sources, backfills=None):
        self._sources = sources
        self._backfills = backfills if backfills is not None else []

    @classmethod
    def from_yaml(cls, config_file: str, secrets_file: str):
        """
        Expects paths to a config YAML and a secrets YAML.
        Check config.yaml.example and secrets.yaml.example for examples.
        """
        with open(config_file, "r") as f:
            config_yaml = yaml.safe_load(f)

        with open(secrets_file, "r") as f:
            secrets_yaml = yaml.safe_load(f)

        # Parse all configured storage sinks
        all_sinks = []
        for sink in config_yaml.get("sinks", []):
            sink_id = sink.get("id")
            sink_type = sink.get("type")
            match sink_type:
                case ConfiguredStorageSinkType.SNOWFLAKE:
                    sink_secrets_yaml = secrets_yaml.get("sinks", {}).get(sink_id, {})
                    if not sink_secrets_yaml:
                        raise ValueError(f"Found no secrets for sink {sink_id}")
                    sink_secrets = SnowflakeSecrets(
                        account=sink_secrets_yaml.get("account"),
                        user=sink_secrets_yaml.get("user"),
                        password=sink_secrets_yaml.get("password"),
                        role=sink_secrets_yaml.get("role"),
                        warehouse=sink_secrets_yaml.get("warehouse"),
                        database=sink_secrets_yaml.get("database"),
                        schema=sink_secrets_yaml.get("schema"),
                    )
                    sink = ConfiguredStorageSink(
                        ConfiguredStorageSinkType.SNOWFLAKE, sink_id, sink_secrets
                    )
                case _:
                    raise ValueError(f"Unrecognized sink type {sink_type}")
            all_sinks.append(sink)

        all_sinks_by_id = {s.id: sink for s in all_sinks}

        # Task output controller
        task_output_config = config_yaml.get("task_output")
        if task_output_config is None:
            raise ValueError("Missing task_output in configuration")
        task_output_type = task_output_config.get("type")
        match task_output_type:
            case ConfiguredTaskOutputControllerType.LOCAL:
                task_output_controller = ConfiguredLocalTaskOutputController(
                    task_output_config.get("output_folder"),
                )
            case ConfiguredTaskOutputControllerType.S3:
                task_output_controller = ConfiguredS3TaskOutputController(
                    task_output_config.get("dev_profile"),
                    task_output_config.get("bucket"),
                )
            case _:
                raise ValueError(f"Unrecognized task output type {task_output_type}")

        # Parse all configured sources
        sources = []
        for source in config_yaml.get("sources", []):
            org_id = source.get("org_id")
            type = source.get("type")

            if any(s.org_id == org_id for s in sources):
                raise ValueError(f"Cannot have duplicate org_id: {org_id}")

            # Parse secrets for data source
            this_source_secrets = secrets_yaml.get("sources", {}).get(org_id)
            match type:
                case ConfiguredAMISourceType.BEACON_360:
                    secrets = Beacon360Secrets(
                        this_source_secrets.get("beacon_360_user"),
                        this_source_secrets.get("beacon_360_password"),
                    )
                case ConfiguredAMISourceType.SENTRYX:
                    secrets = SentryxSecrets(
                        this_source_secrets.get("sentryx_api_key"),
                    )
                case _:
                    secrets = None

            # Join any sinks tied to this source
            sink_ids = source.get("sinks", [])
            sinks = []
            for sink_id in sink_ids:
                sink = all_sinks_by_id.get(sink_id)
                if not sink:
                    raise ValueError(f"Unrecognized sink {sink_id} for source {org_id}")
                sinks.append(sink)

            configured_source = ConfiguredAMISource(
                type,
                org_id,
                source.get("timezone"),
                source.get("use_raw_data_cache"),
                task_output_controller,
                source.get("utility_name"),
                secrets,
                sinks,
            )

            sources.append(configured_source)

        # Backfills
        backfills = []
        for backfill_config in config_yaml.get("backfills", []):
            org_id = backfill_config.get("org_id")
            start_date = backfill_config.get("start_date")
            end_date = backfill_config.get("end_date")
            interval_days = backfill_config.get("interval_days")
            schedule = backfill_config.get("schedule")
            if any(
                i is None
                for i in [org_id, start_date, end_date, interval_days, schedule]
            ):
                raise ValueError(f"Invalid backfill config: {backfill_config}")
            if not any(s.org_id == org_id for s in sources):
                continue
            backfills.append(
                Backfill(
                    org_id=org_id,
                    start_date=datetime.combine(
                        start_date, datetime.min.time(), tzinfo=UTC
                    ),
                    end_date=datetime.combine(
                        end_date, datetime.min.time(), tzinfo=UTC
                    ),
                    interval_days=interval_days,
                    schedule=schedule,
                )
            )

        return AMIAdapterConfiguration(sources=sources, backfills=backfills)

    def adapters(self):
        """
        Preferred method for instantiating AMI Adapters off of a user's configuration.
        Reads configuration to see which adapters to run and where to store the data.
        """
        # Circular import, TODO fix
        from amiadapters.beacon import Beacon360Adapter
        from amiadapters.sentryx import SentryxAdapter

        adapters = []
        for source in self._sources:
            match source.type:
                case ConfiguredAMISourceType.BEACON_360:
                    adapters.append(
                        Beacon360Adapter(
                            source.secrets.user,
                            source.secrets.password,
                            source.use_raw_data_cache,
                            source.org_id,
                            source.timezone,
                            source.task_output_controller,
                            source.storage_sinks,
                        )
                    )
                case ConfiguredAMISourceType.SENTRYX:
                    adapters.append(
                        SentryxAdapter(
                            source.secrets.api_key,
                            source.org_id,
                            source.timezone,
                            source.task_output_controller,
                            source.storage_sinks,
                            utility_name=source.utility_name,
                        )
                    )
        return adapters

    def backfills(self) -> List:
        return self._backfills

    def __repr__(self):
        return f"sources=[{", ".join(str(s) for s in self._sources)}]"


@dataclass
class SnowflakeSecrets:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


class ConfiguredStorageSinkType:
    SNOWFLAKE = "snowflake"


class ConfiguredStorageSink:
    """
    Configuration for a storage sink. We include convenience methods for
    creating connections off of the configuration.
    """

    def __init__(self, type: str, id: str, secrets: Union[SnowflakeSecrets]):
        self.type = self._type(type)
        self.id = self._id(id)
        self.secrets = self._secrets(secrets)

    def connection(self):
        match self.type:
            case ConfiguredStorageSinkType.SNOWFLAKE:
                return snowflake.connector.connect(
                    account=self.secrets.account,
                    user=self.secrets.user,
                    password=self.secrets.password,
                    warehouse=self.secrets.warehouse,
                    database=self.secrets.database,
                    schema=self.secrets.schema,
                    role=self.secrets.role,
                    paramstyle="qmark",
                )
            case _:
                ValueError(f"Unrecognized type {self.type}")

    def _type(self, type: str) -> str:
        if type in {
            ConfiguredStorageSinkType.SNOWFLAKE,
        }:
            return type
        raise ValueError(f"Unrecognized storage sink type: {type}")

    def _id(self, id: str) -> str:
        if id is None:
            raise ValueError("Storage sink must have id")
        return id

    def _secrets(self, secrets: Union[SnowflakeSecrets]) -> Union[SnowflakeSecrets]:
        if self.type == ConfiguredStorageSinkType.SNOWFLAKE and isinstance(
            secrets, SnowflakeSecrets
        ):
            return secrets
        raise ValueError(f"Unrecognized secret type for sink type {self.type}")


class ConfiguredTaskOutputControllerType:
    LOCAL = "local"
    S3 = "s3"


class ConfiguredLocalTaskOutputController:

    def __init__(self, output_folder: str):
        self.type = ConfiguredTaskOutputControllerType.LOCAL
        self.output_folder = self._output_folder(output_folder)

    def _output_folder(self, output_folder: str) -> str:
        if output_folder is None:
            raise ValueError(
                "ConfiguredLocalTaskOutputController must have output_folder"
            )
        return output_folder


class ConfiguredS3TaskOutputController:

    def __init__(self, dev_aws_profile_name: str, s3_bucket_name: str):
        self.type = ConfiguredTaskOutputControllerType.S3
        # Only use for specifying local development AWS credentials. Prod should use
        # AMI roles provisioned in terraform.
        self.dev_aws_profile_name = dev_aws_profile_name
        self.s3_bucket_name = self._s3_bucket_name(s3_bucket_name)

    def _s3_bucket_name(self, s3_bucket_name: str) -> str:
        if s3_bucket_name is None:
            raise ValueError(
                "ConfiguredS3TaskOutputController must have s3_bucket_name"
            )
        return s3_bucket_name


@dataclass
class Backfill:
    """
    Configuration for backfilling an organization's data from start_date to end_date.
    """

    org_id: str
    start_date: datetime
    end_date: datetime
    interval_days: str  # Number of days to backfill in one run
    schedule: str  # crontab-formatted string specifying run schedule


@dataclass
class Beacon360Secrets:
    user: str
    password: str


@dataclass
class SentryxSecrets:
    api_key: str


class ConfiguredAMISourceType:
    BEACON_360 = "beacon_360"
    SENTRYX = "sentryx"


class ConfiguredAMISource:
    """
    Configures a single utility's AMI data source and its storage sinks.
    """

    DEFAULT_TIMEZONE = "America/LosAngeles"

    def __init__(
        self,
        type: str,
        org_id: str,
        timezone: str,
        use_raw_data_cache: bool,
        task_output_controller: Union[
            ConfiguredLocalTaskOutputController, ConfiguredS3TaskOutputController
        ],
        utility_name: str,
        secrets: Union[Beacon360Secrets, SentryxSecrets],
        sinks: List[ConfiguredStorageSink],
    ):
        self.type = self._type(type)
        self.org_id = self._org_id(org_id)
        self.timezone = self._timezone(timezone)
        self.use_raw_data_cache = bool(use_raw_data_cache)
        self.task_output_controller = self._task_output_controller(
            task_output_controller
        )
        self.utility_name = utility_name
        self.secrets = self._secrets(secrets)
        self.storage_sinks = self._sinks(sinks)

    def _type(self, type: str) -> str:
        if type in {
            ConfiguredAMISourceType.BEACON_360,
            ConfiguredAMISourceType.SENTRYX,
        }:
            return type
        raise ValueError(f"Unrecognized AMI source type: {type}")

    def _org_id(self, org_id: str) -> str:
        if org_id is None:
            raise ValueError("AMI Source must have org_id")
        return org_id

    def _timezone(self, this_timezone: str) -> DstTzInfo:
        if this_timezone is None:
            return timezone(DEFAULT_TIMEZONE)
        return timezone(this_timezone)

    def _task_output_controller(
        self,
        task_output_controller: Union[
            ConfiguredLocalTaskOutputController, ConfiguredS3TaskOutputController
        ],
    ) -> Union[ConfiguredLocalTaskOutputController, ConfiguredS3TaskOutputController]:
        if task_output_controller is None:
            raise ValueError("AMI Source must have task_output_controller")
        return task_output_controller

    def _secrets(self, secrets: str) -> Union[Beacon360Secrets]:
        if self.type == ConfiguredAMISourceType.BEACON_360 and isinstance(
            secrets, Beacon360Secrets
        ):
            return secrets
        elif self.type == ConfiguredAMISourceType.SENTRYX and isinstance(
            secrets, SentryxSecrets
        ):
            return secrets
        raise ValueError(f"Invalid secrets type for source type {self.type}")

    def _sinks(self, sinks: List[ConfiguredStorageSink]) -> List[ConfiguredStorageSink]:
        """
        Validate that this type of sink is compatible with this data source type.
        """
        if self.type == ConfiguredAMISourceType.BEACON_360:
            if all(s.type == ConfiguredStorageSinkType.SNOWFLAKE for s in sinks):
                return sinks
        elif self.type == ConfiguredAMISourceType.SENTRYX:
            if all(s.type == ConfiguredStorageSinkType.SNOWFLAKE for s in sinks):
                return sinks
        raise ValueError(f"Invalid sink type(s) for source type {self.type}")

    def __repr__(self):
        return f"ConfiguredAMISource[type={self.type}, org_id={self.org_id}, timezone={self.timezone}, use_cache={self.use_raw_data_cache}, task_output_controller={self.task_output_controller} storage_sinks=[{", ".join(s.id for s in self.storage_sinks)}]]"


def find_config_yaml() -> str:
    """
    Find path to config.yaml or throw exception. Use this if you need flexibility
    between test and prod.
    """
    p = pathlib.Path(__file__).joinpath("..", "..", "config.yaml").resolve()
    if not pathlib.Path.exists(p):
        raise Exception(f"Path to config does not exist: {p}")
    return p


def find_secrets_yaml() -> str:
    """
    Find path to secrets.yaml or throw exception. Use this if you need flexibility
    between test and prod.
    """
    p = pathlib.Path(__file__).joinpath("..", "..", "secrets.yaml").resolve()
    if not pathlib.Path.exists(p):
        raise Exception(f"Path to secrets does not exist: {p}")
    return p

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Union
import pathlib

from airflow.providers.amazon.aws.notifications.sns import SnsNotifier
from pytz import timezone, UTC
from pytz.tzinfo import DstTzInfo
import snowflake.connector
import yaml


class AMIAdapterConfiguration:

    def __init__(self, sources, backfills=None, notifications=None):
        self._sources = sources
        self._backfills = backfills if backfills is not None else []
        self._notifications = notifications

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
            if this_source_secrets is None:
                raise ValueError(f"No secrets found for org_id: {org_id}")
            match type:
                case ConfiguredAMISourceType.ACLARA.value.type:
                    secrets = AclaraSecrets(
                        this_source_secrets.get("sftp_user"),
                        this_source_secrets.get("sftp_password"),
                    )
                case ConfiguredAMISourceType.BEACON_360.value.type:
                    secrets = Beacon360Secrets(
                        this_source_secrets.get("beacon_360_user"),
                        this_source_secrets.get("beacon_360_password"),
                    )
                case ConfiguredAMISourceType.METERSENSE.value.type:
                    secrets = MetersenseSecrets(
                        this_source_secrets.get("ssh_tunnel_username"),
                        this_source_secrets.get("database_db_name"),
                        this_source_secrets.get("database_user"),
                        this_source_secrets.get("database_password"),
                    )
                case ConfiguredAMISourceType.SENTRYX.value.type:
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

            # Only certain source types, like ACLARA
            configured_sftp = ConfiguredSftp(
                source.get("sftp_host"),
                source.get("sftp_remote_data_directory"),
                source.get("sftp_local_download_directory"),
                source.get("sftp_local_known_hosts_file"),
            )

            # Only certain source types, like METERSENSE
            configured_ssh_tunnel_to_database = ConfiguredSSHTunnelToDatabase(
                ssh_tunnel_server_host=source.get("ssh_tunnel_server_host"),
                ssh_tunnel_key_path=source.get("ssh_tunnel_key_path"),
                database_host=source.get("database_host"),
                database_port=source.get("database_port"),
            )

            configured_source = ConfiguredAMISource(
                type,
                org_id,
                source.get("timezone"),
                source.get("use_raw_data_cache"),
                task_output_controller,
                source.get("utility_name"),
                configured_sftp,
                configured_ssh_tunnel_to_database,
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

        # Notifications
        notification_config = config_yaml.get("notifications", {})
        on_failure_sns_arn = notification_config.get("dag_failure", {}).get("sns_arn")
        if on_failure_sns_arn:
            notifications = ConfiguredNotifications(
                on_failure_sns_arn=on_failure_sns_arn
            )
        else:
            notifications = None

        return AMIAdapterConfiguration(
            sources=sources, backfills=backfills, notifications=notifications
        )

    def adapters(self):
        """
        Preferred method for instantiating AMI Adapters off of a user's configuration.
        Reads configuration to see which adapters to run and where to store the data.
        """
        # Circular import, TODO fix
        from amiadapters.adapters.aclara import AclaraAdapter
        from amiadapters.adapters.beacon import Beacon360Adapter
        from amiadapters.adapters.metersense import MetersenseAdapter
        from amiadapters.adapters.sentryx import SentryxAdapter

        adapters = []
        for source in self._sources:
            match source.type:
                case ConfiguredAMISourceType.ACLARA.value.type:
                    adapters.append(
                        AclaraAdapter(
                            source.org_id,
                            source.timezone,
                            source.configured_sftp,
                            source.secrets.sftp_user,
                            source.secrets.sftp_password,
                            source.task_output_controller,
                            source.storage_sinks,
                        )
                    )
                case ConfiguredAMISourceType.BEACON_360.value.type:
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
                case ConfiguredAMISourceType.METERSENSE.value.type:
                    adapters.append(
                        MetersenseAdapter(
                            source.org_id,
                            source.timezone,
                            source.task_output_controller,
                            ssh_tunnel_server_host=source.configured_ssh_tunnel_to_database.ssh_tunnel_server_host,
                            ssh_tunnel_username=source.secrets.ssh_tunnel_username,
                            ssh_tunnel_key_path=source.configured_ssh_tunnel_to_database.ssh_tunnel_key_path,
                            database_host=source.configured_ssh_tunnel_to_database.database_host,
                            database_port=source.configured_ssh_tunnel_to_database.database_port,
                            database_db_name=source.secrets.database_db_name,
                            database_user=source.secrets.database_user,
                            database_password=source.secrets.database_password,
                            configured_sinks=source.storage_sinks,
                        )
                    )
                case ConfiguredAMISourceType.SENTRYX.value.type:
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

    def on_failure_sns_notifier(self):
        if (
            self._notifications is not None
            and self._notifications.on_failure_sns_arn is not None
        ):
            return SnsNotifier(
                target_arn=self._notifications.on_failure_sns_arn,
                message="The DAG {{ dag.dag_id }} failed",
                aws_conn_id="aws_default",
                subject="AMI Connect DAG Failure",
                region_name="us-west-2",
            )
        return None

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
        # IAM role provisioned in terraform.
        self.dev_aws_profile_name = dev_aws_profile_name
        self.s3_bucket_name = self._s3_bucket_name(s3_bucket_name)

    def _s3_bucket_name(self, s3_bucket_name: str) -> str:
        if s3_bucket_name is None:
            raise ValueError(
                "ConfiguredS3TaskOutputController must have s3_bucket_name"
            )
        return s3_bucket_name


@dataclass
class ConfiguredNotifications:
    """
    Configuration for sending notifications on DAG/task state change, or other
    """

    on_failure_sns_arn: str  # SNS Topic ARN for notifications when DAGs fail


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
class AclaraSecrets:
    sftp_user: str
    sftp_password: str


@dataclass
class Beacon360Secrets:
    user: str
    password: str


@dataclass
class MetersenseSecrets:
    ssh_tunnel_username: str
    database_db_name: str
    database_user: str
    database_password: str


@dataclass
class SentryxSecrets:
    api_key: str


@dataclass
class ConfiguredSftp:
    host: str
    remote_data_directory: str
    local_download_directory: str
    local_known_hosts_file: str


@dataclass
class ConfiguredSSHTunnelToDatabase:
    ssh_tunnel_server_host: str
    ssh_tunnel_key_path: str
    database_host: str
    database_port: str


class SourceSchema:
    """
    Definition of a source, its secrets configuration and which types of storage
    sink can be used with it.
    """

    def __init__(
        self,
        type: str,
        secret_type: Union[
            AclaraSecrets, Beacon360Secrets, MetersenseSecrets, SentryxSecrets
        ],
        valid_sink_types: List[ConfiguredStorageSinkType],
    ):
        self.type = type
        self.secret_type = secret_type
        self.valid_sink_types = valid_sink_types


class ConfiguredAMISourceType(Enum):
    """
    Define a source type for your adapter here. Tell the pipeline the name of your source type
    so that it can match it to your configuration. Also tell it which secrets type to expect
    and which storage sinks can be used. The pipeline will use this to validate configuration.
    """

    ACLARA = SourceSchema(
        "aclara", AclaraSecrets, [ConfiguredStorageSinkType.SNOWFLAKE]
    )
    BEACON_360 = SourceSchema(
        "beacon_360", Beacon360Secrets, [ConfiguredStorageSinkType.SNOWFLAKE]
    )
    METERSENSE = SourceSchema(
        "metersense", MetersenseSecrets, [ConfiguredStorageSinkType.SNOWFLAKE]
    )
    SENTRYX = SourceSchema(
        "sentryx", SentryxSecrets, [ConfiguredStorageSinkType.SNOWFLAKE]
    )

    @classmethod
    def is_valid_type(cls, the_type: str) -> bool:
        schemas = cls.__members__.values()
        return the_type in set(s.value.type for s in schemas)

    @classmethod
    def is_valid_secret_for_type(
        cls,
        the_type: str,
        secret_type: Union[
            AclaraSecrets, Beacon360Secrets, MetersenseSecrets, SentryxSecrets
        ],
    ) -> bool:
        matching_schema = cls._matching_schema_for_type(the_type)
        return matching_schema.secret_type == secret_type

    @classmethod
    def are_valid_storage_sinks_for_type(
        cls, the_type: str, sinks: List[ConfiguredStorageSink]
    ) -> bool:
        matching_schema = cls._matching_schema_for_type(the_type)
        return all(s.type in matching_schema.valid_sink_types for s in sinks)

    @classmethod
    def _matching_schema_for_type(cls, the_type: str) -> SourceSchema:
        matching_schemas = [
            v.value for v in cls.__members__.values() if v.value.type == the_type
        ]
        if len(matching_schemas) != 1:
            raise ValueError(
                f"Invalid number of matching schemas for type {the_type}: {matching_schemas}"
            )
        return matching_schemas[0]


class ConfiguredAMISource:
    """
    Configures a single utility's AMI data source and its storage sinks.

    As of this writing, instead of doing a polymorphic type for each source,
    we're just dumping all source-specific configuration into this class. E.g. "utility_name"
    is only used by one source type, but we throw it in here with the rest of the kitchen sink.
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
        configured_sftp: ConfiguredSftp,
        configured_ssh_tunnel_to_database: ConfiguredSSHTunnelToDatabase,
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
        self.configured_sftp = self._configured_sftp(configured_sftp)
        self.configured_ssh_tunnel_to_database = (
            self._configured_ssh_tunnel_to_database(configured_ssh_tunnel_to_database)
        )
        self.secrets = self._secrets(secrets)
        self.storage_sinks = self._sinks(sinks)

    def _type(self, type: str) -> str:
        if ConfiguredAMISourceType.is_valid_type(type):
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

    def _configured_sftp(self, configured_sftp: ConfiguredSftp) -> ConfiguredSftp:
        if self.type == ConfiguredAMISourceType.ACLARA.value.type:
            if any(
                i is None
                for i in [
                    configured_sftp.host,
                    configured_sftp.local_download_directory,
                    configured_sftp.local_known_hosts_file,
                    configured_sftp.remote_data_directory,
                ]
            ):
                raise ValueError(
                    f"Invalid SFTP config for source with type {self.type}"
                )
        return configured_sftp

    def _configured_ssh_tunnel_to_database(
        self, configured_ssh_tunnel_to_database: ConfiguredSSHTunnelToDatabase
    ) -> ConfiguredSSHTunnelToDatabase:
        if self.type == ConfiguredAMISourceType.METERSENSE.value.type:
            if any(
                i is None
                for i in [
                    configured_ssh_tunnel_to_database.ssh_tunnel_server_host,
                    configured_ssh_tunnel_to_database.ssh_tunnel_key_path,
                    configured_ssh_tunnel_to_database.database_host,
                    configured_ssh_tunnel_to_database.database_port,
                ]
            ):
                raise ValueError(
                    f"Invalid SSHTunnelToDatabase config for source with type {self.type}"
                )
        return configured_ssh_tunnel_to_database

    def _secrets(self, secrets: str):
        if secrets is None:
            return None

        if ConfiguredAMISourceType.is_valid_secret_for_type(self.type, type(secrets)):
            return secrets

        raise ValueError(f"Invalid secrets type for source type {self.type}")

    def _sinks(self, sinks: List[ConfiguredStorageSink]) -> List[ConfiguredStorageSink]:
        """
        Validate that this type of sink is compatible with this data source type.
        """
        if ConfiguredAMISourceType.are_valid_storage_sinks_for_type(self.type, sinks):
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

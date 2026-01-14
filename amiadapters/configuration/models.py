from abc import ABC
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from enum import Enum
import json
from typing import Any, Dict, List, Optional, Union

import pytz
from pytz.tzinfo import DstTzInfo


class IntermediateOutputType(str, Enum):
    LOCAL = "local"  # Extract and Transform outputs go to local filesystem
    S3 = "s3"  # Extract and Transform outputs go to S3 bucket


@dataclass
class PipelineConfiguration:
    intermediate_output_type: IntermediateOutputType
    intermediate_output_s3_bucket: str
    intermediate_output_dev_profile: str
    intermediate_output_local_output_path: str
    should_run_post_processor: bool
    should_publish_load_finished_events: bool


@dataclass
class BackfillConfiguration:
    """
    Configuration for backfilling an organization's data from start_date to end_date.
    """

    org_id: str
    start_date: datetime
    end_date: datetime
    interval_days: str  # Number of days to backfill in one run
    schedule: str  # crontab-formatted string specifying run schedule


@dataclass
class NotificationsConfiguration:
    """
    Configuration for sending notifications on DAG/task state change, or other
    """

    on_failure_sns_arn: str  # SNS Topic ARN for notifications when DAGs fail


@dataclass
class SftpConfiguration:
    """
    Configuration for connecting to an SFTP server,
    used during extract by some sources like Aclara.
    """

    host: str
    remote_data_directory: str
    local_download_directory: str
    known_hosts_str: str


@dataclass
class SSHTunnelToDatabaseConfiguration:
    """
    Configuration for an SSH tunnel to a database,
    used during extract by some sources like Metersense.
    """

    ssh_tunnel_server_host: str
    ssh_tunnel_key_path: str
    database_host: str
    database_port: str


##############################################################################
# Metrics / Telemetry
##############################################################################
class MetricsBackendType(str, Enum):
    NOOP = "noop"
    CLOUDWATCH = "cloudwatch"


@dataclass
class MetricsConfigurationBase:
    """
    Configuration for a metrics backend that reports stats
    about this system.
    """

    @classmethod
    def from_dict(cls, raw_metrics_config: dict) -> "MetricsConfigurationBase":
        if not raw_metrics_config:
            raw_metrics_config["type"] = MetricsBackendType.NOOP.value

        if not raw_metrics_config.get("type"):
            raise ValueError("Metrics configuration missing type")

        match config_type := raw_metrics_config["type"].lower():
            case MetricsBackendType.NOOP.value:
                config_cls = NoopMetricsConfiguration
            case _:
                raise ValueError(
                    f"Unrecognized metrics configuration type {config_type}"
                )

        metrics_config = config_cls(**raw_metrics_config)
        metrics_config.validate()
        return metrics_config

    def validate(self) -> None:
        pass


@dataclass
class NoopMetricsConfiguration:
    pass


##############################################################################
# Secrets
##############################################################################
class SecretsBase:
    """
    Base class for secrets dataclasses, with convenience method to convert to JSON.
    Secrets dataclasses define the secrets needed for a particular source or sink type.
    They are serialized directly to JSON for storage in AWS Secrets Manager.
    """

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    def validate(self) -> None:
        return

    def _require(self, *fields: str) -> None:
        missing = [
            f for f in fields if not hasattr(self, f) or getattr(self, f) is None
        ]
        if missing:
            raise ValueError(
                f"{self.__class__.__name__} missing required fields: {missing}"
            )


class SinkSecretsBase(SecretsBase):
    """
    Base class for storage sink secrets dataclasses.
    """

    @classmethod
    def from_dict(cls, sink_type: str, raw_secret_config: dict) -> "SinkSecretsBase":
        if not raw_secret_config:
            raise ValueError(f"Found no secrets for sink type {sink_type}")

        match sink_type:
            case ConfiguredStorageSinkType.SNOWFLAKE.value:
                secret_cls = SnowflakeSecrets
            case _:
                raise ValueError(f"Unrecognized sink type {sink_type}")

        # Copy so we don't mutate caller data
        kwargs = dict(raw_secret_config)
        config = secret_cls(**kwargs)
        config.validate()

        return config


@dataclass
class SnowflakeSecrets(SinkSecretsBase):
    account: str
    user: str
    role: str
    warehouse: str
    database: str
    schema: str
    password: str = None  # Deprecated - use ssh_key instead
    ssh_key: str = None

    def validate(self) -> None:
        self._require(
            "account",
            "user",
            "ssh_key",
            "role",
            "warehouse",
            "database",
            "schema",
        )


class SourceSecretsBase(SecretsBase):
    """
    Base class for AMI source secrets dataclasses.
    """

    @classmethod
    def from_dict(
        cls, source_type: str, raw_secret_config: dict
    ) -> "SourceSecretsBase":
        if not raw_secret_config:
            raise ValueError(f"Found no secrets for source type {source_type}")

        secret_cls = ConfiguredAMISourceTypes.get_secret_type_for_source_type(
            source_type
        )

        # Copy so we don't mutate caller data
        kwargs = dict(raw_secret_config)
        config = secret_cls(**kwargs)
        config.validate()

        return config

    def validate(self) -> None:
        return


@dataclass
class AclaraSecrets(SourceSecretsBase):
    sftp_user: str
    sftp_password: str


@dataclass
class Beacon360Secrets(SourceSecretsBase):
    user: str
    password: str


@dataclass
class MetersenseSecrets(SourceSecretsBase):
    ssh_tunnel_private_key: str
    ssh_tunnel_username: str
    database_db_name: str
    database_user: str
    database_password: str


@dataclass
class NeptuneSecrets(SourceSecretsBase):
    site_id: str
    api_key: str
    client_id: str
    client_secret: str


@dataclass
class SentryxSecrets(SourceSecretsBase):
    api_key: str


@dataclass
class SubecaSecrets(SourceSecretsBase):
    api_key: str


@dataclass
class XylemMoultonNiguelSecrets(SourceSecretsBase):
    ssh_tunnel_private_key: str
    ssh_tunnel_username: str
    database_db_name: str
    database_user: str
    database_password: str


@dataclass
class XylemSensusSecrets(SourceSecretsBase):
    sftp_user: str
    sftp_password: str


def get_secrets_class_type(source_type: str):
    return ConfiguredAMISourceTypes.get_secret_type_for_source_type(source_type)


###############################################################################
# Storage sinks
###############################################################################
class ConfiguredStorageSinkType(str, Enum):
    SNOWFLAKE = "snowflake"


class ConfiguredStorageSink:
    """
    Configuration for a storage sink. We include convenience methods for
    creating connections off of the configuration.
    """

    def __init__(
        self,
        type: str,
        id: str,
        secrets: Union[SnowflakeSecrets],
        data_quality_check_names: List = None,
    ):
        self.type = type
        self.id = id
        self.secrets = secrets
        self.data_quality_check_names = data_quality_check_names or []

    @classmethod
    def from_dict(
        cls, raw_sink_config: dict, secrets: SinkSecretsBase
    ) -> "ConfiguredStorageSink":
        data_quality_check_names = raw_sink_config.get("checks", [])
        sink_id = raw_sink_config.get("id")
        sink_type = raw_sink_config.get("type", "").lower()
        config = cls(
            type=sink_type,
            id=sink_id,
            secrets=secrets,
            data_quality_check_names=data_quality_check_names,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if id is None:
            raise ValueError("Storage sink must have id")
        if not self.type in ConfiguredStorageSinkType:
            raise ValueError(
                f"Invalid storage sink type: {self.type} for sink {self.id}"
            )
        if self.type == ConfiguredStorageSinkType.SNOWFLAKE.value and not isinstance(
            self.secrets, SnowflakeSecrets
        ):
            raise ValueError(f"Unrecognized secret type for sink type {self.type}")

    def connection(self):
        from amiadapters.configuration.base import create_snowflake_connection

        match self.type:
            case ConfiguredStorageSinkType.SNOWFLAKE.value:
                return create_snowflake_connection(
                    account=self.secrets.account,
                    user=self.secrets.user,
                    password=self.secrets.password,
                    ssh_key=self.secrets.ssh_key,
                    warehouse=self.secrets.warehouse,
                    database=self.secrets.database,
                    schema=self.secrets.schema,
                    role=self.secrets.role,
                )
            case _:
                ValueError(f"Unrecognized type {self.type}")

    def checks(self) -> List:
        """
        Return names of configured data quality checks for this sink.
        """
        from amiadapters.storage.base import BaseAMIDataQualityCheck

        conn = self.connection()
        checks_by_name = BaseAMIDataQualityCheck.get_all_checks_by_name(conn)
        result = []
        for name in self.data_quality_check_names:
            if name in checks_by_name:
                result.append(checks_by_name[name])
            else:
                raise ValueError(
                    f"Unrecognized data quality check name: {name}. Choices are: {", ".join(checks_by_name.keys())}"
                )
        return result


###############################################################################
# Intermediate task output
###############################################################################
class IntermediateOutputControllerConfiguration(ABC):
    type: IntermediateOutputType


class LocalIntermediateOutputControllerConfiguration(
    IntermediateOutputControllerConfiguration
):

    def __init__(self, output_folder: str):
        self.type = IntermediateOutputType.LOCAL.value
        self.output_folder = self._output_folder(output_folder)

    def _output_folder(self, output_folder: str) -> str:
        if output_folder is None:
            raise ValueError(
                "ConfiguredLocalTaskOutputController must have output_folder"
            )
        return output_folder


class S3IntermediateOutputControllerConfiguration(
    IntermediateOutputControllerConfiguration
):

    def __init__(self, dev_aws_profile_name: str, s3_bucket_name: str):
        self.type = IntermediateOutputType.S3.value
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


##############################################################################
# Sources (Add your adapter's non-secret configuration schema here)
##############################################################################
@dataclass(frozen=True)
class SourceConfigBase:
    """
    Base class for source configuration. This is how we represent the
    non-secret configuration schema for a source. Each source type is different
    and will have its own subclass of this base class, implementing its own validation logic.
    """

    org_id: str
    type: str
    timezone: DstTzInfo
    secrets: SecretsBase
    sinks: List[ConfiguredStorageSink]
    task_output_controller: IntermediateOutputControllerConfiguration
    metrics: MetricsConfigurationBase

    def validate(self) -> None:
        """
        Validate that required fields are present and valid. Subclasses should implement
        this method with their own validation logic and call super().validate() to
        ensure base validation is performed.
        """
        self._require(
            "org_id",
            "type",
            "timezone",
        )

        if not ConfiguredAMISourceTypes.is_valid_type(self.type):
            raise ValueError(f"Unrecognized AMI source type: {self.type}")

        if not ConfiguredAMISourceTypes.are_valid_storage_sinks_for_type(
            self.type, self.sinks
        ):
            raise ValueError(f"Invalid sink type(s) for source type {self.type}")

    def _require(self, *fields: str) -> None:
        missing = [
            f for f in fields if not hasattr(self, f) or getattr(self, f) is None
        ]
        if missing:
            raise ValueError(
                f"{self.org_id} ({self.type}) missing required fields: {missing}"
            )

    @classmethod
    def from_dict(cls, raw_source_config: dict) -> "SourceConfigBase":
        """
        Construct a SourceConfigBase subclass instance from a raw dictionary
        of configuration data.
        """
        if not (source_type := raw_source_config.get("type")):
            raise ValueError("Source config missing required field: type")
        config_cls = ConfiguredAMISourceTypes.get_config_type_for_source_type(
            source_type
        )

        # Copy so we don't mutate caller data
        kwargs = dict(raw_source_config)

        # ---- transforms ----
        kwargs["timezone"] = pytz.timezone(
            kwargs.get("timezone", "America/Los_Angeles")
        )
        if "use_raw_data_cache" in kwargs and kwargs["use_raw_data_cache"] is None:
            kwargs["use_raw_data_cache"] = False
        # Python dataclasses don't coerce types automatically, so we need to do it here for non-str fields
        if "database_port" in kwargs:
            kwargs["database_port"] = int(kwargs["database_port"])
        # ---- end transforms ----

        # TODO derive from configuration
        kwargs["metrics"] = NoopMetricsConfiguration()

        config = config_cls(**kwargs)
        config.validate()

        return config

    def type_specific_config_dict(self) -> Dict[str, Any]:
        base_field_names = {f.name for f in fields(SourceConfigBase)}
        return {
            f.name: getattr(self, f.name)
            for f in fields(type(self))
            if f.name not in base_field_names
        }


@dataclass(frozen=True)
class AclaraSourceConfig(SourceConfigBase):
    sftp_host: str
    sftp_remote_data_directory: str
    sftp_local_download_directory: str
    sftp_known_hosts_str: str
    # Deprecated - use sftp_known_hosts_str instead
    sftp_local_known_hosts_file: Optional[str] = None

    def validate(self):
        super().validate()
        self._require(
            "sftp_host",
            "sftp_known_hosts_str",
            "sftp_local_download_directory",
            "sftp_remote_data_directory",
        )


@dataclass(frozen=True)
class Beacon360SourceConfig(SourceConfigBase):
    use_raw_data_cache: Optional[bool] = False

    def validate(self):
        super().validate()
        if self.use_raw_data_cache is None:
            raise ValueError(
                f"{self.org_id}: use_raw_data_cache must be explicitly set"
            )


@dataclass(frozen=True)
class MetersenseSourceConfig(SourceConfigBase):
    database_host: str
    database_port: int
    ssh_tunnel_server_host: str
    ssh_tunnel_key_path: str

    def validate(self):
        super().validate()
        self._require(
            "database_host",
            "database_port",
            "ssh_tunnel_server_host",
            "ssh_tunnel_key_path",
        )


@dataclass(frozen=True)
class XylemMoultonNiguelSourceConfig(SourceConfigBase):
    database_host: str
    database_port: int
    ssh_tunnel_server_host: str
    ssh_tunnel_key_path: str

    def validate(self):
        super().validate()
        self._require(
            "database_host",
            "database_port",
            "ssh_tunnel_server_host",
            "ssh_tunnel_key_path",
        )


@dataclass(frozen=True)
class NeptuneSourceConfig(SourceConfigBase):
    external_adapter_location: str

    def validate(self):
        super().validate()
        self._require(
            "external_adapter_location",
        )


@dataclass(frozen=True)
class SentryxSourceConfig(SourceConfigBase):
    utility_name: str
    use_raw_data_cache: Optional[bool] = False


@dataclass(frozen=True)
class SubecaSourceConfig(SourceConfigBase):
    api_url: str


@dataclass(frozen=True)
class XylemSensusSourceConfig(SourceConfigBase):
    sftp_host: str
    sftp_remote_data_directory: str
    sftp_local_download_directory: str
    sftp_known_hosts_str: str


class SourceSchema:
    """
    Definition of a source, its secrets configuration and which types of storage
    sink can be used with it.
    """

    def __init__(
        self,
        type: str,
        config_type: SourceConfigBase,
        secret_type: SecretsBase,
        valid_sink_types: List[ConfiguredStorageSinkType],
    ):
        self.type = type
        self.config_type = config_type
        self.secret_type = secret_type
        self.valid_sink_types = valid_sink_types


class ConfiguredAMISourceTypes(Enum):
    """
    Define a source type for your adapter here. Tell the pipeline the name of your source type
    so that it can match it to your configuration. Also tell it which secrets type to expect
    and which storage sinks can be used. The pipeline will use this to validate configuration.
    """

    ACLARA = SourceSchema(
        "aclara",
        AclaraSourceConfig,
        AclaraSecrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )
    BEACON_360 = SourceSchema(
        "beacon_360",
        Beacon360SourceConfig,
        Beacon360Secrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )
    METERSENSE = SourceSchema(
        "metersense",
        MetersenseSourceConfig,
        MetersenseSecrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )
    NEPTUNE = SourceSchema(
        "neptune",
        NeptuneSourceConfig,
        NeptuneSecrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )
    SENTRYX = SourceSchema(
        "sentryx",
        SentryxSourceConfig,
        SentryxSecrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )
    SUBECA = SourceSchema(
        "subeca",
        SubecaSourceConfig,
        SubecaSecrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )
    XYLEM_MOULTON_NIGUEL = SourceSchema(
        "xylem_moulton_niguel",
        XylemMoultonNiguelSourceConfig,
        XylemMoultonNiguelSecrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )
    XYLEM_SENSUS = SourceSchema(
        "xylem_sensus",
        XylemSensusSourceConfig,
        XylemSensusSecrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )

    @classmethod
    def is_valid_type(cls, the_type: str) -> bool:
        schemas = cls.__members__.values()
        return the_type in set(s.value.type for s in schemas)

    @classmethod
    def is_valid_secret_for_type(
        cls,
        the_type: str,
        secret_type: SecretsBase,
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
    def get_config_type_for_source_type(
        cls,
        the_type: str,
    ) -> SourceConfigBase:
        matching_schema = cls._matching_schema_for_type(the_type)
        return matching_schema.config_type

    @classmethod
    def get_secret_type_for_source_type(
        cls,
        the_type: str,
    ) -> SecretsBase:
        matching_schema = cls._matching_schema_for_type(the_type)
        return matching_schema.secret_type

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

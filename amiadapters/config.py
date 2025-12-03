from datetime import datetime
from enum import Enum
from typing import List, Dict, Union
import pathlib

from airflow.providers.amazon.aws.notifications.sns import SnsNotifier
from pytz import timezone, UTC
from pytz.tzinfo import DstTzInfo
import yaml

from amiadapters.adapters.aclara import AclaraAdapter
from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.adapters.beacon import Beacon360Adapter
from amiadapters.adapters.metersense import MetersenseAdapter
from amiadapters.adapters.sentryx import SentryxAdapter
from amiadapters.adapters.subeca import SubecaAdapter
from amiadapters.adapters.xylem_moulton_niguel import XylemMoultonNiguelAdapter
from amiadapters.adapters.xylem_sensus import XylemSensusAdapter
from amiadapters.configuration.base import create_snowflake_from_secrets
from amiadapters.configuration.models import (
    AclaraSecrets,
    BackfillConfiguration,
    Beacon360Secrets,
    ConfiguredStorageSink,
    ConfiguredStorageSinkType,
    IntermediateOutputType,
    IntermediateOutputControllerConfiguration,
    LocalIntermediateOutputControllerConfiguration,
    MetersenseSecrets,
    NeptuneSecrets,
    NotificationsConfiguration,
    PipelineConfiguration,
    S3IntermediateOutputControllerConfiguration,
    SSHTunnelToDatabaseConfiguration,
    SecretsBase,
    SentryxSecrets,
    SftpConfiguration,
    SnowflakeSecrets,
    SubecaSecrets,
    XylemMoultonNiguelSecrets,
    XylemSensusSecrets,
)
from amiadapters.configuration.database import get_configuration
from amiadapters.configuration.secrets import get_secrets


class AMIAdapterConfiguration:

    def __init__(
        self,
        sources,
        task_output_controller,
        pipeline_configuration: PipelineConfiguration,
        backfills=None,
        notifications=None,
        sinks=None,
    ):
        self._sources = sources
        self._task_output_controller = task_output_controller
        self._pipeline_configuration = pipeline_configuration
        self._backfills = backfills if backfills is not None else []
        self._notifications = notifications
        self._sinks = sinks if sinks is not None else []

    @classmethod
    def from_yaml(cls, config_file: str, secrets_file: str):
        """
        Expects paths to a config YAML and a secrets YAML.
        Check config.yaml.example and secrets.yaml.example for examples.
        """
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        with open(secrets_file, "r") as f:
            secrets = yaml.safe_load(f)
            # Allows us to combine config and secrets files into one
            if nested := secrets.get("secrets", {}):
                secrets = nested

        task_output = config.get("task_output", {})
        pipeline = config.get("pipeline", {})
        pipeline_configuration = PipelineConfiguration(
            intermediate_output_type=task_output.get("type"),
            intermediate_output_s3_bucket=task_output.get("bucket"),
            intermediate_output_dev_profile=task_output.get("dev_profile"),
            intermediate_output_local_output_path=task_output.get("output_folder"),
            should_run_post_processor=pipeline.get("run_post_processors", True),
            should_publish_load_finished_events=pipeline.get(
                "should_publish_load_finished_events", True
            ),
        )

        return cls._make_instance(
            config.get("sources", []),
            config.get("sinks", []),
            pipeline_configuration,
            config.get("notifications", {}),
            config.get("backfills", []),
            secrets,
        )

    @classmethod
    def from_database(cls):
        """
        Given a Snowflake connection to an AMI Connect schema, query the configuration tables to get this
        pipeline's config.
        """
        # Get all secrets, including Snowflake creds used to get non-secret configuration
        secrets = get_secrets()

        # When we have better secrets management, find a better way of accessing this information
        connection = create_snowflake_from_secrets(secrets)
        sources, sinks, pipeline_configuration, notifications, backfills = (
            get_configuration(connection)
        )

        return cls._make_instance(
            sources,
            sinks,
            pipeline_configuration,
            notifications,
            backfills,
            secrets,
        )

    @classmethod
    def _make_instance(
        cls,
        configured_sources: List[Dict],
        configured_sinks: List[Dict],
        pipeline_configuration: PipelineConfiguration,
        configured_notifications: Dict,
        configured_backfills: List[Dict],
        configured_secrets: Dict,
    ):
        """
        Other static constructors get their data from YAML, a database, etc. then call this function
        to create our internal config object.
        """
        # Parse all configured storage sinks
        all_sinks = []
        for sink in configured_sinks:
            sink_id = sink.get("id")
            sink_type = sink.get("type")
            checks = sink.get("checks")
            match sink_type:
                case ConfiguredStorageSinkType.SNOWFLAKE:
                    sink_secrets_yaml = configured_secrets.get("sinks", {}).get(
                        sink_id, {}
                    )
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
                        ConfiguredStorageSinkType.SNOWFLAKE,
                        sink_id,
                        sink_secrets,
                        data_quality_check_names=checks,
                    )
                case _:
                    raise ValueError(f"Unrecognized sink type {sink_type}")
            all_sinks.append(sink)

        all_sinks_by_id = {s.id: sink for s in all_sinks}

        # Task output controller
        if pipeline_configuration is None:
            raise ValueError("Missing pipeline configuration")
        task_output_type = pipeline_configuration.intermediate_output_type
        match task_output_type:
            case IntermediateOutputType.LOCAL:
                task_output_controller = LocalIntermediateOutputControllerConfiguration(
                    pipeline_configuration.intermediate_output_local_output_path,
                )
            case IntermediateOutputType.S3:
                task_output_controller = S3IntermediateOutputControllerConfiguration(
                    pipeline_configuration.intermediate_output_dev_profile,
                    pipeline_configuration.intermediate_output_s3_bucket,
                )
            case _:
                raise ValueError(f"Unrecognized task output type {task_output_type}")

        # Parse all configured sources
        sources = []
        for source in configured_sources:
            org_id = source.get("org_id")
            type = source.get("type")

            if any(s.org_id == org_id for s in sources):
                raise ValueError(f"Cannot have duplicate org_id: {org_id}")

            # Parse secrets for data source
            this_source_secrets = configured_secrets.get("sources", {}).get(org_id)
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
                        this_source_secrets.get("user"),
                        this_source_secrets.get("password"),
                    )
                case ConfiguredAMISourceType.METERSENSE.value.type:
                    secrets = MetersenseSecrets(
                        this_source_secrets.get("ssh_tunnel_private_key"),
                        this_source_secrets.get("ssh_tunnel_username"),
                        this_source_secrets.get("database_db_name"),
                        this_source_secrets.get("database_user"),
                        this_source_secrets.get("database_password"),
                    )
                case ConfiguredAMISourceType.NEPTUNE.value.type:
                    secrets = NeptuneSecrets(
                        this_source_secrets.get("site_id"),
                        this_source_secrets.get("api_key"),
                        this_source_secrets.get("client_id"),
                        this_source_secrets.get("client_secret"),
                    )
                case ConfiguredAMISourceType.SENTRYX.value.type:
                    secrets = SentryxSecrets(
                        this_source_secrets.get("api_key"),
                    )
                case ConfiguredAMISourceType.SUBECA.value.type:
                    secrets = SubecaSecrets(
                        this_source_secrets.get("api_key"),
                    )
                case ConfiguredAMISourceType.XYLEM_MOULTON_NIGUEL.value.type:
                    secrets = XylemMoultonNiguelSecrets(
                        this_source_secrets.get("ssh_tunnel_private_key"),
                        this_source_secrets.get("ssh_tunnel_username"),
                        this_source_secrets.get("database_db_name"),
                        this_source_secrets.get("database_user"),
                        this_source_secrets.get("database_password"),
                    )
                case ConfiguredAMISourceType.XYLEM_SENSUS.value.type:
                    secrets = XylemSensusSecrets(
                        this_source_secrets.get("sftp_user"),
                        this_source_secrets.get("sftp_password"),
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
            configured_sftp = SftpConfiguration(
                source.get("sftp_host"),
                source.get("sftp_remote_data_directory"),
                source.get("sftp_local_download_directory"),
                source.get("sftp_known_hosts_str"),
            )

            # Only certain source types, like METERSENSE
            configured_ssh_tunnel_to_database = SSHTunnelToDatabaseConfiguration(
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
                source.get("api_url"),
                configured_sftp,
                configured_ssh_tunnel_to_database,
                secrets,
                sinks,
                source.get("external_adapter_location"),
            )

            sources.append(configured_source)

        # Backfills
        backfills = []
        for backfill_config in configured_backfills:
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
                BackfillConfiguration(
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
        on_failure_sns_arn = configured_notifications.get("dag_failure", {}).get(
            "sns_arn"
        )
        if on_failure_sns_arn:
            notifications = NotificationsConfiguration(
                on_failure_sns_arn=on_failure_sns_arn
            )
        else:
            notifications = None

        return AMIAdapterConfiguration(
            sources=sources,
            task_output_controller=task_output_controller,
            pipeline_configuration=pipeline_configuration,
            backfills=backfills,
            notifications=notifications,
            sinks=all_sinks,
        )

    def adapters(self) -> List[BaseAMIAdapter]:
        """
        Preferred method for instantiating AMI Adapters off of a user's configuration.
        Reads configuration to see which adapters to run and where to store the data.
        """
        adapters = []
        for source in self._sources:
            match source.type:
                case ConfiguredAMISourceType.ACLARA.value.type:
                    adapters.append(
                        AclaraAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
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
                            self._pipeline_configuration,
                            source.task_output_controller,
                            source.storage_sinks,
                        )
                    )
                case ConfiguredAMISourceType.METERSENSE.value.type:
                    adapters.append(
                        MetersenseAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.task_output_controller,
                            ssh_tunnel_server_host=source.configured_ssh_tunnel_to_database.ssh_tunnel_server_host,
                            ssh_tunnel_username=source.secrets.ssh_tunnel_username,
                            ssh_tunnel_key_path=source.configured_ssh_tunnel_to_database.ssh_tunnel_key_path,
                            ssh_tunnel_private_key=source.secrets.ssh_tunnel_private_key,
                            database_host=source.configured_ssh_tunnel_to_database.database_host,
                            database_port=source.configured_ssh_tunnel_to_database.database_port,
                            database_db_name=source.secrets.database_db_name,
                            database_user=source.secrets.database_user,
                            database_password=source.secrets.database_password,
                            configured_sinks=source.storage_sinks,
                        )
                    )
                case ConfiguredAMISourceType.NEPTUNE.value.type:
                    # Neptune code is not in this project because of open source limitations
                    # This code assumes Neptune's code is available at the external_adapter_location
                    # which we append to the python path
                    import sys

                    sys.path.append(source.external_adapter_location)
                    from neptune import NeptuneAdapter

                    adapters.append(
                        NeptuneAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.secrets.site_id,
                            source.secrets.api_key,
                            source.secrets.client_id,
                            source.secrets.client_secret,
                            source.task_output_controller,
                            source.storage_sinks,
                        )
                    )
                case ConfiguredAMISourceType.SENTRYX.value.type:
                    adapters.append(
                        SentryxAdapter(
                            source.secrets.api_key,
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.task_output_controller,
                            source.storage_sinks,
                            utility_name=source.utility_name,
                        )
                    )
                case ConfiguredAMISourceType.SUBECA.value.type:
                    adapters.append(
                        SubecaAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.api_url,
                            source.secrets.api_key,
                            source.task_output_controller,
                            source.storage_sinks,
                        )
                    )
                case ConfiguredAMISourceType.XYLEM_MOULTON_NIGUEL.value.type:
                    adapters.append(
                        XylemMoultonNiguelAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.task_output_controller,
                            ssh_tunnel_server_host=source.configured_ssh_tunnel_to_database.ssh_tunnel_server_host,
                            ssh_tunnel_username=source.secrets.ssh_tunnel_username,
                            ssh_tunnel_key_path=source.configured_ssh_tunnel_to_database.ssh_tunnel_key_path,
                            ssh_tunnel_private_key=source.secrets.ssh_tunnel_private_key,
                            database_host=source.configured_ssh_tunnel_to_database.database_host,
                            database_port=source.configured_ssh_tunnel_to_database.database_port,
                            database_db_name=source.secrets.database_db_name,
                            database_user=source.secrets.database_user,
                            database_password=source.secrets.database_password,
                            configured_sinks=source.storage_sinks,
                        )
                    )
                case ConfiguredAMISourceType.XYLEM_SENSUS.value.type:
                    adapters.append(
                        XylemSensusAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.configured_sftp,
                            source.secrets.sftp_user,
                            source.secrets.sftp_password,
                            source.task_output_controller,
                            source.storage_sinks,
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

    def sinks(self) -> List:
        return self._sinks

    def task_output_controller(self):
        return self._task_output_controller

    def __repr__(self):
        return f"sources=[{", ".join(str(s) for s in self._sources)}]"


class SourceSchema:
    """
    Definition of a source, its secrets configuration and which types of storage
    sink can be used with it.
    """

    def __init__(
        self,
        type: str,
        secret_type: SecretsBase,
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
    NEPTUNE = SourceSchema(
        "neptune", NeptuneSecrets, [ConfiguredStorageSinkType.SNOWFLAKE]
    )
    SENTRYX = SourceSchema(
        "sentryx", SentryxSecrets, [ConfiguredStorageSinkType.SNOWFLAKE]
    )
    SUBECA = SourceSchema(
        "subeca", SubecaSecrets, [ConfiguredStorageSinkType.SNOWFLAKE]
    )
    XYLEM_MOULTON_NIGUEL = SourceSchema(
        "xylem_moulton_niguel",
        XylemMoultonNiguelSecrets,
        [ConfiguredStorageSinkType.SNOWFLAKE],
    )
    XYLEM_SENSUS = SourceSchema(
        "xylem_sensus", XylemSensusSecrets, [ConfiguredStorageSinkType.SNOWFLAKE]
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
        task_output_controller: IntermediateOutputControllerConfiguration,
        utility_name: str,
        api_url: str,
        configured_sftp: SftpConfiguration,
        configured_ssh_tunnel_to_database: SSHTunnelToDatabaseConfiguration,
        secrets: Union[Beacon360Secrets, SentryxSecrets],
        sinks: List[ConfiguredStorageSink],
        external_adapter_location: str,
    ):
        self.type = self._type(type)
        self.org_id = self._org_id(org_id)
        self.timezone = self._timezone(timezone)
        self.use_raw_data_cache = bool(use_raw_data_cache)
        self.task_output_controller = self._task_output_controller(
            task_output_controller
        )
        self.utility_name = utility_name
        self.api_url = api_url
        self.configured_sftp = self._configured_sftp(configured_sftp)
        self.configured_ssh_tunnel_to_database = (
            self._configured_ssh_tunnel_to_database(configured_ssh_tunnel_to_database)
        )
        self.secrets = self._secrets(secrets)
        self.storage_sinks = self._sinks(sinks)
        self.external_adapter_location = external_adapter_location

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
        task_output_controller: IntermediateOutputControllerConfiguration,
    ) -> IntermediateOutputControllerConfiguration:
        if task_output_controller is None:
            raise ValueError("AMI Source must have task_output_controller")
        return task_output_controller

    def _configured_sftp(self, configured_sftp: SftpConfiguration) -> SftpConfiguration:
        if self.type == ConfiguredAMISourceType.ACLARA.value.type:
            if any(
                i is None
                for i in [
                    configured_sftp.host,
                    configured_sftp.local_download_directory,
                    configured_sftp.known_hosts_str,
                    configured_sftp.remote_data_directory,
                ]
            ):
                raise ValueError(
                    f"Invalid SFTP config for source with type {self.type}"
                )
        return configured_sftp

    def _configured_ssh_tunnel_to_database(
        self, configured_ssh_tunnel_to_database: SSHTunnelToDatabaseConfiguration
    ) -> SSHTunnelToDatabaseConfiguration:
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

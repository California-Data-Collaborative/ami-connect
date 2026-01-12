from datetime import datetime
from typing import List, Dict
import pathlib

from pytz import UTC
import yaml

from amiadapters.adapters.aclara import AclaraAdapter
from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.adapters.beacon import Beacon360Adapter
from amiadapters.adapters.metersense import MetersenseAdapter
from amiadapters.adapters.sentryx import SentryxAdapter
from amiadapters.adapters.subeca import SubecaAdapter
from amiadapters.adapters.xylem_moulton_niguel import XylemMoultonNiguelAdapter
from amiadapters.adapters.xylem_sensus import XylemSensusAdapter
from amiadapters.alerts.base import AmiConnectDagFailureNotifier
from amiadapters.configuration.base import create_snowflake_from_secrets
from amiadapters.configuration.models import (
    AclaraSecrets,
    BackfillConfiguration,
    Beacon360Secrets,
    ConfiguredStorageSink,
    IntermediateOutputType,
    LocalIntermediateOutputControllerConfiguration,
    MetersenseSecrets,
    NeptuneSecrets,
    NotificationsConfiguration,
    PipelineConfiguration,
    S3IntermediateOutputControllerConfiguration,
    SentryxSecrets,
    SinkSecretsBase,
    SourceConfigBase,
    SubecaSecrets,
    XylemMoultonNiguelSecrets,
    XylemSensusSecrets,
)
from amiadapters.configuration.database import get_configuration
from amiadapters.configuration.env import (
    get_global_aws_profile,
    get_global_aws_region,
    get_global_airflow_site_url,
)
from amiadapters.configuration.models import ConfiguredAMISourceTypes
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
        self._airflow_site_url = get_global_airflow_site_url()

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
            sink_secrets = SinkSecretsBase.from_dict(
                sink_type, configured_secrets.get("sinks", {}).get(sink_id, {})
            )
            sink = ConfiguredStorageSink.from_dict(sink, sink_secrets)
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

            # Parse secrets for data source
            this_source_secrets = configured_secrets.get("sources", {}).get(org_id)
            if this_source_secrets is None:
                raise ValueError(f"No secrets found for org_id: {org_id}")
            match type:
                case ConfiguredAMISourceTypes.ACLARA.value.type:
                    secrets = AclaraSecrets(
                        this_source_secrets.get("sftp_user"),
                        this_source_secrets.get("sftp_password"),
                    )
                case ConfiguredAMISourceTypes.BEACON_360.value.type:
                    secrets = Beacon360Secrets(
                        this_source_secrets.get("user"),
                        this_source_secrets.get("password"),
                    )
                case ConfiguredAMISourceTypes.METERSENSE.value.type:
                    secrets = MetersenseSecrets(
                        this_source_secrets.get("ssh_tunnel_private_key"),
                        this_source_secrets.get("ssh_tunnel_username"),
                        this_source_secrets.get("database_db_name"),
                        this_source_secrets.get("database_user"),
                        this_source_secrets.get("database_password"),
                    )
                case ConfiguredAMISourceTypes.NEPTUNE.value.type:
                    secrets = NeptuneSecrets(
                        this_source_secrets.get("site_id"),
                        this_source_secrets.get("api_key"),
                        this_source_secrets.get("client_id"),
                        this_source_secrets.get("client_secret"),
                    )
                case ConfiguredAMISourceTypes.SENTRYX.value.type:
                    secrets = SentryxSecrets(
                        this_source_secrets.get("api_key"),
                    )
                case ConfiguredAMISourceTypes.SUBECA.value.type:
                    secrets = SubecaSecrets(
                        this_source_secrets.get("api_key"),
                    )
                case ConfiguredAMISourceTypes.XYLEM_MOULTON_NIGUEL.value.type:
                    secrets = XylemMoultonNiguelSecrets(
                        this_source_secrets.get("ssh_tunnel_private_key"),
                        this_source_secrets.get("ssh_tunnel_username"),
                        this_source_secrets.get("database_db_name"),
                        this_source_secrets.get("database_user"),
                        this_source_secrets.get("database_password"),
                    )
                case ConfiguredAMISourceTypes.XYLEM_SENSUS.value.type:
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

            raw_source_config = source.copy()
            raw_source_config["secrets"] = secrets
            raw_source_config["sinks"] = sinks
            raw_source_config["task_output_controller"] = task_output_controller
            configured_source = SourceConfigBase.from_dict(raw_source_config)

            if any(s.org_id == configured_source.org_id for s in sources):
                raise ValueError(
                    f"Cannot have duplicate org_id: {configured_source.org_id}"
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
                case ConfiguredAMISourceTypes.ACLARA.value.type:
                    adapters.append(
                        AclaraAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.sftp_host,
                            source.sftp_remote_data_directory,
                            source.sftp_local_download_directory,
                            source.sftp_known_hosts_str,
                            source.secrets.sftp_user,
                            source.secrets.sftp_password,
                            source.task_output_controller,
                            source.sinks,
                        )
                    )
                case ConfiguredAMISourceTypes.BEACON_360.value.type:
                    adapters.append(
                        Beacon360Adapter(
                            source.secrets.user,
                            source.secrets.password,
                            source.use_raw_data_cache,
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.task_output_controller,
                            source.sinks,
                        )
                    )
                case ConfiguredAMISourceTypes.METERSENSE.value.type:
                    adapters.append(
                        MetersenseAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.task_output_controller,
                            ssh_tunnel_server_host=source.ssh_tunnel_server_host,
                            ssh_tunnel_username=source.secrets.ssh_tunnel_username,
                            ssh_tunnel_key_path=source.ssh_tunnel_key_path,
                            ssh_tunnel_private_key=source.secrets.ssh_tunnel_private_key,
                            database_host=source.database_host,
                            database_port=source.database_port,
                            database_db_name=source.secrets.database_db_name,
                            database_user=source.secrets.database_user,
                            database_password=source.secrets.database_password,
                            configured_sinks=source.sinks,
                        )
                    )
                case ConfiguredAMISourceTypes.NEPTUNE.value.type:
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
                            source.sinks,
                        )
                    )
                    pass
                case ConfiguredAMISourceTypes.SENTRYX.value.type:
                    adapters.append(
                        SentryxAdapter(
                            source.secrets.api_key,
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.task_output_controller,
                            source.sinks,
                            utility_name=source.utility_name,
                        )
                    )
                case ConfiguredAMISourceTypes.SUBECA.value.type:
                    adapters.append(
                        SubecaAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.api_url,
                            source.secrets.api_key,
                            source.task_output_controller,
                            source.sinks,
                        )
                    )
                case ConfiguredAMISourceTypes.XYLEM_MOULTON_NIGUEL.value.type:
                    adapters.append(
                        XylemMoultonNiguelAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.task_output_controller,
                            ssh_tunnel_server_host=source.ssh_tunnel_server_host,
                            ssh_tunnel_username=source.secrets.ssh_tunnel_username,
                            ssh_tunnel_key_path=source.ssh_tunnel_key_path,
                            ssh_tunnel_private_key=source.secrets.ssh_tunnel_private_key,
                            database_host=source.database_host,
                            database_port=source.database_port,
                            database_db_name=source.secrets.database_db_name,
                            database_user=source.secrets.database_user,
                            database_password=source.secrets.database_password,
                            configured_sinks=source.sinks,
                        )
                    )
                case ConfiguredAMISourceTypes.XYLEM_SENSUS.value.type:
                    adapters.append(
                        XylemSensusAdapter(
                            source.org_id,
                            source.timezone,
                            self._pipeline_configuration,
                            source.sftp_host,
                            source.sftp_remote_data_directory,
                            source.sftp_local_download_directory,
                            source.sftp_known_hosts_str,
                            source.secrets.sftp_user,
                            source.secrets.sftp_password,
                            source.task_output_controller,
                            source.sinks,
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
            return AmiConnectDagFailureNotifier(
                sns_topic_arn=self._notifications.on_failure_sns_arn,
                base_airflow_url=self._airflow_site_url or "localhost:8080",
                aws_profile_name=get_global_aws_profile(),
                aws_region=get_global_aws_region(),
            )
        return None

    def sinks(self) -> List:
        return self._sinks

    def task_output_controller(self):
        return self._task_output_controller

    def __repr__(self):
        return f"sources=[{", ".join(str(s) for s in self._sources)}]"


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

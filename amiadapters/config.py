from dataclasses import dataclass

# import os
from typing import List, Union

# from dotenv import load_dotenv
from pytz import timezone
from pytz.tzinfo import DstTzInfo
import yaml


class AMIAdapterConfiguration:

    # def __init__(self, **kwargs):
    #     self.utility_name = kwargs.get("utility_name")
    #     self.output_folder = kwargs.get("output_folder")
    #     self.sentryx_api_key = kwargs.get("sentryx_api_key")
    #     self.beacon_360_user = kwargs.get("beacon_360_user")
    #     self.beacon_360_password = kwargs.get("beacon_360_password")
    #     self.snowflake_user = kwargs.get("snowflake_user")
    #     self.snowflake_password = kwargs.get("snowflake_password")
    #     self.snowflake_account = kwargs.get("snowflake_account")
    #     self.snowflake_warehouse = kwargs.get("snowflake_warehouse")
    #     self.snowflake_database = kwargs.get("snowflake_database")
    #     self.snowflake_schema = kwargs.get("snowflake_schema")
    #     self.snowflake_role = kwargs.get("snowflake_role")

    # @classmethod
    # def from_env(cls):
    #     # Assumes .env file in working directory
    #     load_dotenv()
    #     return AMIAdapterConfiguration(
    #         utility_name=os.environ.get("UTILITY_NAME"),
    #         output_folder=os.environ.get("AMI_DATA_OUTPUT_FOLDER"),
    #         sentryx_api_key=os.environ.get("SENTRYX_API_KEY"),
    #         beacon_360_user=os.environ.get("BEACON_AUTH_USER"),
    #         beacon_360_password=os.environ.get("BEACON_AUTH_PASSWORD"),
    #         snowflake_user=os.environ.get("SNOWFLAKE_USER"),
    #         snowflake_password=os.environ.get("SNOWFLAKE_PASSWORD"),
    #         snowflake_account=os.environ.get("SNOWFLAKE_ACCOUNT"),
    #         snowflake_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
    #         snowflake_database=os.environ.get("SNOWFLAKE_DATABASE"),
    #         snowflake_schema=os.environ.get("SNOWFLAKE_SCHEMA"),
    #         snowflake_role=os.environ.get("SNOWFLAKE_ROLE"),
    #     )
    def __init__(self, sources):
        self.sources = sources

    def __repr__(self):
        return f"sources=[{", ".join(str(s) for s in self.sources)}]"

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

        # Parse all configured sinks
        all_sinks = []
        for sink in config_yaml.get("sinks", []):
            sink_id = sink.get("id")
            sink_type = sink.get("type")
            if sink_type == ConfiguredStorageSinkType.SNOWFLAKE:
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
            else:
                raise ValueError(f"Unrecognized sink type {sink_type}")
            all_sinks.append(sink)

        all_sinks_by_id = {s.id: sink for s in all_sinks}

        # Parse all configured sources, join their sinks
        sources = []
        for source in config_yaml.get("sources", []):
            org_id = source.get("org_id")
            type = source.get("type")

            # Parse secrets for data source
            this_source_secrets = secrets_yaml.get("sources", {}).get(org_id)
            if type == ConfiguredAMISourceType.BEACON_360:
                secrets = Beacon360Secrets(
                    this_source_secrets.get("beacon_360_user"),
                    this_source_secrets.get("beacon_360_password"),
                )
            else:
                secrets = None

            # Parse sinks
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
                secrets,
                sinks,
            )

            sources.append(configured_source)

        return AMIAdapterConfiguration(sources=sources)


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

    def __init__(self, type: str, id: str, secrets: Union[SnowflakeSecrets]):
        self.type = self._type(type)
        self.id = self._id(id)
        self.secrets = self._secrets(secrets)

    def _type(self, type: str) -> str:
        if type == ConfiguredStorageSinkType.SNOWFLAKE:
            return ConfiguredStorageSinkType.SNOWFLAKE
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


@dataclass
class Beacon360Secrets:
    user: str
    password: str


class ConfiguredAMISourceType:
    BEACON_360 = "beacon_360"


class ConfiguredAMISource:
    """
    A single utility's AMI data source and where we'll store that data
    when we run our pipeline.
    """

    DEFAULT_TIMEZONE = "America/LosAngeles"

    def __init__(
        self,
        type: str,
        org_id: str,
        timezone: str,
        use_raw_data_cache: bool,
        secrets: Union[Beacon360Secrets],
        sinks: List[ConfiguredStorageSink],
    ):
        self.type = self._type(type)
        self.org_id = self._org_id(org_id)
        self.timezone = self._timezone(timezone)
        self.use_raw_data_cache = bool(use_raw_data_cache)
        self.secrets = self._secrets(secrets)
        self.storage_sinks = self._sinks(sinks)

    def _type(self, type: str) -> str:
        if type == ConfiguredAMISourceType.BEACON_360:
            return ConfiguredAMISourceType.BEACON_360
        raise ValueError(f"Unrecognized AMI source type: {type}")

    def _org_id(self, org_id: str) -> str:
        if org_id is None:
            raise ValueError("AMI Source must have org_id")
        return org_id

    def _timezone(self, this_timezone: str) -> DstTzInfo:
        if this_timezone is None:
            return timezone(DEFAULT_TIMEZONE)
        return timezone(this_timezone)

    def _secrets(self, secrets: str) -> Union[Beacon360Secrets]:
        if self.type == ConfiguredAMISourceType.BEACON_360 and isinstance(
            secrets, Beacon360Secrets
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
        raise ValueError(f"Invalid sink type(s) for source type {self.type}")

    def __repr__(self):
        return f"ConfiguredAMISource[type={self.type}, org_id={self.org_id}, timezone={self.timezone}, use_cache={self.use_raw_data_cache}, storage_sinks=[{", ".join(s.id for s in self.storage_sinks)}]]"


if __name__ == "__main__":
    path = "./config.yaml.example"
    secrets_path = "./secrets.yaml.example"
    config = AMIAdapterConfiguration.from_yaml(path, secrets_path)
    print(config)

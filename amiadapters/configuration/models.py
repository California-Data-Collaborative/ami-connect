from abc import ABC
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
import json


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
    local_known_hosts_file: str


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


@dataclass
class SnowflakeSecrets(SecretsBase):
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


@dataclass
class AclaraSecrets(SecretsBase):
    sftp_user: str
    sftp_password: str


@dataclass
class Beacon360Secrets(SecretsBase):
    user: str
    password: str


@dataclass
class MetersenseSecrets(SecretsBase):
    ssh_tunnel_username: str
    database_db_name: str
    database_user: str
    database_password: str


@dataclass
class NeptuneSecrets(SecretsBase):
    site_id: str
    api_key: str
    client_id: str
    client_secret: str


@dataclass
class SentryxSecrets(SecretsBase):
    api_key: str


@dataclass
class SubecaSecrets(SecretsBase):
    api_key: str


@dataclass
class XylemMoultonNiguelSecrets(SecretsBase):
    ssh_tunnel_username: str
    database_db_name: str
    database_user: str
    database_password: str


def get_secrets_class_type(secret_type: str):
    match secret_type:
        case "aclara":
            return AclaraSecrets
        case "beacon_360":
            return Beacon360Secrets
        case "metersense":
            return MetersenseSecrets
        case "neptune":
            return NeptuneSecrets
        case "sentryx":
            return SentryxSecrets
        case "subeca":
            return SubecaSecrets
        case "xylem_moulton_niguel":
            return XylemMoultonNiguelSecrets
        case "snowflake":
            return SnowflakeSecrets
        case _:
            raise ValueError(f"Unrecognized secrets class name: {secret_type}")


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

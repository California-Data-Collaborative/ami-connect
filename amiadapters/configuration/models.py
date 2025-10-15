from dataclasses import asdict, dataclass
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

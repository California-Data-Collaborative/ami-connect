import json
import logging

import boto3
from botocore.exceptions import ClientError


# TODO do not hardcode
AWS_PROFILE_NAME = "cadc-ami-connect"
AWS_REGION = "us-west-2"

SECRET_ID_PREFIX = "ami-connect"

logger = logging.getLogger(__name__)


def get_secrets() -> dict[str, dict]:
    all_secrets = _list_secrets_with_prefix(SECRET_ID_PREFIX)
    return all_secrets


def _list_secrets_with_prefix(prefix: str) -> dict[str, dict]:
    client = _create_aws_secrets_manager_client()

    # Get all secrets from AWS under this prefix
    response = client.batch_get_secret_value(
        Filters=[
            {
                "Key": "name",
                "Values": [
                    prefix,
                ],
            },
        ],
    )

    # Unpack results into nested dictionary
    result = {}
    for secret_value in response.get("SecretValues", []):
        value = json.loads(secret_value["SecretString"])
        # Remove the prefix
        key = secret_value["Name"].replace(prefix, "")
        _unpack_secret_into_dictionary(result, key, value)

    return result


def _unpack_secret_into_dictionary(all_secrets: dict[str, dict], key: str, value: dict):
    """
    Mutates all_secrets dictionary by adding new secret nested where the key specifies.

    Key like /my-prefix/sinks/my-snowflake should result in dict like
        {"sinks": {"my-snowflake": {"user": "x", ...}}}
    """
    key_parts = [i for i in key.split("/") if i]
    current_dict = all_secrets
    while key_parts:
        next_key = key_parts.pop(0)
        if not key_parts:
            # This is the last key in the list, so just set the value
            current_dict[next_key] = value
        else:
            # Move to the next lowest dictionary
            if next_key not in current_dict:
                current_dict[next_key] = {}
            current_dict = current_dict[next_key]


def update_secret(secret_name: str, secret: dict):
    client = _create_aws_secrets_manager_client()
    secret_id = f"{SECRET_ID_PREFIX}/{secret_name}"
    secret_value = json.dumps(secret)

    try:
        # Try to update an existing secret
        response = client.put_secret_value(
            SecretId=secret_id,
            SecretString=secret_value,
        )
        logger.info(f"Updated existing secret: {secret_id}")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            # Secret doesn't exist — create it
            response = client.create_secret(
                Name=secret_id,
                SecretString=secret_value,
            )
            logger.info(f"Created new secret: {secret_id}")
        else:
            raise e

    return response


def _create_aws_secrets_manager_client():
    session = boto3.Session(profile_name=AWS_PROFILE_NAME)
    return session.client("secretsmanager", region_name=AWS_REGION)

import os

AWS_PROFILE_ENV_VAR_NAME = "AMI_CONNECT__AWS_PROFILE"
AWS_REGION_ENV_VAR_NAME = "AMI_CONNECT__AWS_REGION"

DEFAULT_AWS_REGION = "us-west-2"


def set_global_aws_profile(aws_profile: str = None):
    """
    Sets the AWS profile to use for the current process. This is used by boto3 to
    determine which AWS credentials to use during local development.

    In production, AWS access is provided via the EC2 instance role.
    """
    if aws_profile is None and AWS_PROFILE_ENV_VAR_NAME not in os.environ:
        raise ValueError(
            f"No AWS profile specified. If using the CLI, use the --profile option or set the {AWS_PROFILE_ENV_VAR_NAME} environment variable."
        )
    os.environ[AWS_PROFILE_ENV_VAR_NAME] = aws_profile
    os.environ[AWS_REGION_ENV_VAR_NAME] = DEFAULT_AWS_REGION


def set_global_aws_region(aws_region: str):
    """
    Sets the AWS region to use for the current process. This is used by boto3 to
    determine which AWS region to use during local development.

    In production, AWS access is provided via the EC2 instance role.
    """
    os.environ[AWS_REGION_ENV_VAR_NAME] = (
        aws_region if aws_region else DEFAULT_AWS_REGION
    )


def get_global_aws_profile() -> str | None:
    """
    Returns the AWS profile to use for the current process, or None if not set.
    """
    return os.environ.get(AWS_PROFILE_ENV_VAR_NAME)


def get_global_aws_region() -> str | None:
    """
    Returns the AWS region to use for the current process, or None if not set.
    """
    return os.environ.get(AWS_REGION_ENV_VAR_NAME, DEFAULT_AWS_REGION)


def get_global_airflow_site_url() -> str | None:
    """
    Returns the Airflow site URL to use for the current process, or None if not set.
    """
    return os.environ.get("AMI_CONNECT__AIRFLOW_SITE_URL")

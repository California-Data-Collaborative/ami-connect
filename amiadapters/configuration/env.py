import os

AWS_PROFILE_ENV_VAR_NAME = "AMI_CONNECT__AWS_PROFILE"
AWS_REGION_ENV_VAR_NAME = "AMI_CONNECT__AWS_REGION"

DEFAULT_AWS_REGION = "us-west-2"


def set_global_aws_profile(aws_profile: str):
    """
    Sets the AWS profile to use for the current process. This is used by boto3 to
    determine which AWS credentials to use during local development.

    In production, AWS acess is provided via the EC2 instance role.
    """
    os.environ[AWS_PROFILE_ENV_VAR_NAME] = aws_profile
    os.environ[AWS_REGION_ENV_VAR_NAME] = DEFAULT_AWS_REGION


def get_global_aws_profile() -> str | None:
    """
    Returns the AWS profile to use for the current process, or None if not set.
    """
    return os.environ.get(AWS_PROFILE_ENV_VAR_NAME)


def get_global_aws_region() -> str | None:
    """
    Returns the AWS region to use for the current process, or None if not set.
    """
    return os.environ.get(AWS_REGION_ENV_VAR_NAME)

from dataclasses import dataclass
from enum import Enum


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

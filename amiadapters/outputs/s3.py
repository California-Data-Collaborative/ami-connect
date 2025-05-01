import json
import logging
import os
from typing import List

import boto3
from botocore.exceptions import ClientError

from amiadapters.models import GeneralMeter, GeneralMeterRead
from amiadapters.models import GeneralModelJSONEncoder
from amiadapters.outputs.base import BaseTaskOutputController, ExtractOutput

logger = logging.getLogger(__name__)


class S3TaskOutputController(BaseTaskOutputController):
    """
    Uses Amazon S3 for intermediate task outputs.
    """

    EXTRACT = "e"
    TRANSFORM = "t"

    def __init__(
        self, bucket_name: str, org_id: str, s3_prefix: str = "intermediate_outputs"
    ):
        """
        bucket_name: S3 bucket to use
        org_id: identifier for the organization
        s3_prefix: optional S3 prefix (acts like a root folder inside the bucket)
        """
        if not bucket_name or not org_id:
            raise Exception(
                f"Missing required parameter. bucket_name:{bucket_name} org_id:{org_id}"
            )

        self.bucket_name = bucket_name
        self.org_id = org_id
        self.s3_prefix = s3_prefix.strip("/")
        self.s3 = boto3.client("s3")

    def write_extract_outputs(self, run_id: str, outputs: ExtractOutput):
        for name, content in outputs.get_outputs().items():
            key = self._s3_key(run_id, self.EXTRACT, name)
            logger.info(f"Uploading extract output to s3://{self.bucket_name}/{key}")
            self._upload_string_to_s3(key, content)

    def read_extract_outputs(self, run_id: str) -> ExtractOutput:
        prefix = self._s3_key(run_id, self.EXTRACT, "")
        outputs = {}
        logger.info(f"Reading extract outputs from s3://{self.bucket_name}/{prefix}")
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            name = os.path.basename(key)
            content = self._download_string_from_s3(key)
            outputs[name] = content
        return ExtractOutput(outputs)

    def write_transformed_meters(self, run_id: str, meters: List[GeneralMeter]):
        key = self._s3_key(run_id, self.TRANSFORM, "meters.json")
        logger.info(f"Uploading {len(meters)} meters to s3://{self.bucket_name}/{key}")
        data = "\n".join(json.dumps(m, cls=GeneralModelJSONEncoder) for m in meters)
        self._upload_string_to_s3(key, data)

    def read_transformed_meters(self, run_id: str) -> List[GeneralMeter]:
        key = self._s3_key(run_id, self.TRANSFORM, "meters.json")
        logger.info(f"Downloading meters from s3://{self.bucket_name}/{key}")
        text = self._download_string_from_s3(key)
        return [GeneralMeter(**json.loads(line)) for line in text.strip().split("\n")]

    def write_transformed_meter_reads(self, run_id: str, reads: List[GeneralMeterRead]):
        key = self._s3_key(run_id, self.TRANSFORM, "reads.json")
        logger.info(f"Uploading {len(reads)} reads to s3://{self.bucket_name}/{key}")
        data = "\n".join(json.dumps(r, cls=GeneralModelJSONEncoder) for r in reads)
        self._upload_string_to_s3(key, data)

    def read_transformed_meter_reads(self, run_id: str) -> List[GeneralMeterRead]:
        key = self._s3_key(run_id, self.TRANSFORM, "reads.json")
        logger.info(f"Downloading reads from s3://{self.bucket_name}/{key}")
        text = self._download_string_from_s3(key)
        return [
            GeneralMeterRead(**json.loads(line)) for line in text.strip().split("\n")
        ]

    def _s3_key(self, run_id: str, stage: str, name: str) -> str:
        parts = [self.s3_prefix, run_id, self.org_id, stage, name]
        return "/".join(part.strip("/") for part in parts if part)

    def _upload_string_to_s3(self, key: str, content: str):
        try:
            self.s3.put_object(
                Bucket=self.bucket_name, Key=key, Body=content.encode("utf-8")
            )
        except ClientError as e:
            logger.error(f"Failed to upload to S3 at {key}: {e}")
            raise

    def _download_string_from_s3(self, key: str) -> str:
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response["Body"].read().decode("utf-8")
        except ClientError as e:
            logger.error(f"Failed to download from S3 at {key}: {e}")
            raise

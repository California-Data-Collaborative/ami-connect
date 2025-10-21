import json
import logging
import os
import gzip
import io
import shutil
from typing import List

import boto3
from botocore.exceptions import ClientError, ProfileNotFound

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
        self,
        bucket_name: str,
        org_id: str,
        s3_prefix: str = "intermediate_outputs",
        aws_profile_name: str = None,
        s3_client=None,
    ):
        if not bucket_name or not org_id:
            raise Exception(
                f"Missing required parameter. bucket_name:{bucket_name} org_id:{org_id}"
            )

        self.bucket_name = bucket_name
        self.org_id = org_id
        self.s3_prefix = s3_prefix.strip("/")

        if s3_client is None:
            # TODO clean up
            from amiadapters.configuration.env import get_global_aws_profile

            profile = get_global_aws_profile()
            # if profile:
            #     session = boto3.Session(profile_name=profile)
            #     return session.client("secretsmanager", region_name=region)
            # else:
            #     # Use default boto3 client (e.g. IAM role on EC2)
            #     return boto3.client("secretsmanager", region_name=region)
            if profile:
                try:
                    session = boto3.Session(profile_name=profile)
                    self.s3 = session.client("s3")
                except ProfileNotFound as e:
                    logger.info(
                        f"AWS profile '{profile}' not found, falling back to default"
                    )
                    self.s3 = boto3.client("s3")
            else:
                self.s3 = boto3.client("s3")
        else:
            self.s3 = s3_client

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
        key = self._s3_key(run_id, self.TRANSFORM, "meters.json.gz")
        logger.info(f"Uploading {len(meters)} meters to s3://{self.bucket_name}/{key}")
        data = "\n".join(json.dumps(m, cls=GeneralModelJSONEncoder) for m in meters)
        self._upload_string_to_s3(key, data)

    def read_transformed_meters(self, run_id: str) -> List[GeneralMeter]:
        key = self._s3_key(run_id, self.TRANSFORM, "meters.json.gz")
        logger.info(f"Downloading meters from s3://{self.bucket_name}/{key}")
        text = self._download_string_from_s3(key)
        return [GeneralMeter(**json.loads(line)) for line in text.strip().split("\n")]

    def write_transformed_meter_reads(self, run_id: str, reads: List[GeneralMeterRead]):
        key = self._s3_key(run_id, self.TRANSFORM, "reads.json.gz")
        logger.info(f"Uploading {len(reads)} reads to s3://{self.bucket_name}/{key}")
        data = "\n".join(json.dumps(r, cls=GeneralModelJSONEncoder) for r in reads)
        self._upload_string_to_s3(key, data)

    def read_transformed_meter_reads(self, run_id: str) -> List[GeneralMeterRead]:
        key = self._s3_key(run_id, self.TRANSFORM, "reads.json.gz")
        logger.info(f"Downloading reads from s3://{self.bucket_name}/{key}")
        text = self._download_string_from_s3(key)
        return [
            GeneralMeterRead(**json.loads(line)) for line in text.strip().split("\n")
        ]

    def download_for_path(
        self, path: str, output_directory: str, decompress: bool = True
    ):
        """
        Download any S3 objects under the provided path prefix. Works for either
        a prefix or actual object key.

        Intended for local development. Production should access S3 data with
        functions like read_transformed_meters.
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        i = 0
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=path):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Keep relative path
                relative_path = os.path.relpath(key, path)
                local_path = os.path.join(output_directory, relative_path)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                if decompress:
                    # Download into memory and decompress
                    logger.info(
                        f"Downloading and decompressing s3://{self.bucket_name}/{key} → {local_path}"
                    )
                    obj_stream = self.s3.get_object(Bucket=self.bucket_name, Key=key)[
                        "Body"
                    ]
                    with gzip.open(obj_stream, "rb") as gz, open(
                        local_path, "wb"
                    ) as out_f:
                        shutil.copyfileobj(gz, out_f)
                else:
                    logger.info(
                        f"Downloading s3://{self.bucket_name}/{key} → {local_path}"
                    )
                    self.s3.download_file(self.bucket_name, key, local_path)
                i += 1
        logger.info(f"Downloaded {i} files from {path}")

    def _s3_key(self, run_id: str, stage: str, name: str) -> str:
        parts = [self.s3_prefix, run_id, self.org_id, stage, name]
        return "/".join(part.strip("/") for part in parts if part)

    def _upload_string_to_s3(self, key: str, content: str):
        # gzip compress the string
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(content.encode("utf-8"))
        buf.seek(0)

        put_args = {
            "Bucket": self.bucket_name,
            "Key": key,
            "Body": buf.read(),
            "ContentEncoding": "gzip",
            "ContentType": "application/json",
        }

        try:
            self.s3.put_object(**put_args)
        except ClientError as e:
            logger.error(f"Failed to upload to S3 at {key}: {e}")
            raise

    def _download_string_from_s3(self, key: str) -> str:
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            compressed = response["Body"].read()
            with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as gz:
                return gz.read().decode("utf-8")
        except ClientError as e:
            logger.error(f"Failed to download from S3 at {key}: {e}")
            raise

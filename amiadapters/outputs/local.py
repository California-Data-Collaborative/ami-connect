import json
import logging
import os
from typing import List

from amiadapters.models import GeneralMeter, GeneralMeterRead
from amiadapters.models import GeneralModelJSONEncoder
from amiadapters.outputs.base import BaseTaskOutputController, ExtractOutput

logger = logging.getLogger(__name__)


class LocalTaskOutputController(BaseTaskOutputController):
    """
    Uses local file system for intermediate task outputs.
    """

    EXTRACT = "e"
    TRANSFORM = "t"

    def __init__(self, output_folder: str, org_id: str):
        """
        output_folder: directory in filesystem where outputs should be stored
        org_id: org for which this data was extracted
        """
        if not output_folder or not org_id:
            raise Exception(
                f"Missing required parameter. output_folder:{output_folder} org_id:{org_id}"
            )
        self.output_folder = output_folder
        self.org_id = org_id

    def write_extract_outputs(self, run_id: str, outputs: ExtractOutput):
        for name, content in outputs.get_outputs().items():
            path = os.path.join(self._base_dir(run_id), self.EXTRACT, name)
            self._create_parent_directories_if_missing(path)
            logger.info(f"Writing extract output to {path}")
            with open(path, "w") as f:
                f.write(content)
            logger.info(f"Wrote extract output to {path}")

    def read_extract_outputs(self, run_id: str) -> ExtractOutput:
        path = os.path.join(self._base_dir(run_id), self.EXTRACT)
        outputs = {}
        for name in os.listdir(path):
            logger.info(f"Reading extract output at {name}")
            with open(os.path.join(path, name), "r") as f:
                content = f.read()
            outputs[name] = content
            logger.info(f"Finished reading extract output at {name}")
        return ExtractOutput(outputs)

    def write_transformed_meters(self, run_id: str, meters: List[GeneralMeter]):
        path = self._transformed_meters_path(run_id)
        self._create_parent_directories_if_missing(path)
        logger.info(f"Writing {len(meters)} transformed meters to {path}")
        with open(path, "w") as f:
            f.write(
                "\n".join(json.dumps(v, cls=GeneralModelJSONEncoder) for v in meters)
            )
        logger.info(f"Wrote meters to {path}")

    def read_transformed_meters(self, run_id: str) -> List[GeneralMeter]:
        path = self._transformed_meters_path(run_id)
        logger.info(f"Reading meters from {path}")
        with open(path, "r") as f:
            text = f.read()
            meters = [GeneralMeter(**json.loads(d)) for d in text.strip().split("\n")]
        logger.info(f"Read {len(meters)} meters from {path}")
        return meters

    def write_transformed_meter_reads(self, run_id: str, reads: List[GeneralMeterRead]):
        path = self._transformed_reads_path(run_id)
        self._create_parent_directories_if_missing(path)
        logger.info(f"Writing {len(reads)} transformed meters to {path}")
        with open(path, "w") as f:
            f.write(
                "\n".join(json.dumps(v, cls=GeneralModelJSONEncoder) for v in reads)
            )
        logger.info(f"Wrote reads to {path}")

    def read_transformed_meter_reads(self, run_id: str) -> List[GeneralMeterRead]:
        path = self._transformed_reads_path(run_id)
        logger.info(f"Reading meter reads from {path}")
        with open(path, "r") as f:
            text = f.read()
            reads = [
                GeneralMeterRead(**json.loads(d)) for d in text.strip().split("\n")
            ]
        logger.info(f"Read {len(reads)} meter reads from {path}")
        return reads

    def _base_dir(self, run_id: str) -> str:
        return os.path.join(self.output_folder, run_id, self.org_id)

    def _transformed_meters_path(self, run_id: str) -> str:
        return os.path.join(self._base_dir(run_id), self.TRANSFORM, "meters.json")

    def _transformed_reads_path(self, run_id: str) -> str:
        return os.path.join(self._base_dir(run_id), self.TRANSFORM, "reads.json")

    def _create_parent_directories_if_missing(self, path):
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            logger.info(f"Creating parent directories for {path}")
            os.makedirs(directory, exist_ok=True)

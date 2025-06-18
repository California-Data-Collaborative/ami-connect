from dataclasses import dataclass
from datetime import datetime
import logging
from typing import List

from amiadapters.adapters.base import BaseAMIAdapter
from amiadapters.models import DataclassJSONEncoder, GeneralMeter, GeneralMeterRead
from amiadapters.outputs.base import ExtractOutput

logger = logging.getLogger(__name__)


class SubecaAdapter(BaseAMIAdapter):
    """
    AMI Adapter that uses API to retrieve Subeca data.
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        configured_task_output_controller,
        configured_sinks,
    ):
        super().__init__(
            org_id,
            org_timezone,
            configured_task_output_controller,
            configured_sinks,
        )

    def name(self) -> str:
        return f"subeca-{self.org_id}"

    def _extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
        device_ids: List[str] = None,
    ):
        logging.info(
            f"Retrieving Subeca data between {extract_range_start} and {extract_range_end}"
        )
        output = ""
        return ExtractOutput({"meters_and_reads.json": output})

    def _transform(self, run_id: str, extract_outputs: ExtractOutput):
        text = extract_outputs.from_file("meters_and_reads.json")
        return [], []

from datetime import datetime
from typing import List

from amiadapters.base import BaseAMIAdapter


class MetersenseAdapter(BaseAMIAdapter):
    """
    AMI Adapter that retrieves Xylem/Sensus data from a Metersense Oracle database.
    """

    def __init__(
        self,
        org_id,
        org_timezone,
        configured_task_output_controller,
        configured_sinks=None,
        raw_snowflake_loader=None,
    ):
        super().__init__(
            org_id,
            org_timezone,
            configured_task_output_controller,
            configured_sinks,
            raw_snowflake_loader,
        )

    def name(self) -> str:
        return f"metersense-{self.org_id}"

    def extract(
        self,
        run_id: str,
        extract_range_start: datetime,
        extract_range_end: datetime,
        device_ids: List[str] = None,
    ):
        pass

    def transform(self, run_id: str):
        pass

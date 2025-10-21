import boto3
from dataclasses import dataclass
from datetime import datetime
import json
import logging

from amiadapters.configuration.env import get_global_aws_profile, get_global_aws_region

logger = logging.getLogger(__name__)


# Matches value in terraform code that creates the SQS queue
LOAD_FINISHED_QUEUE_NAME = "ami-connect-dag-event-queue"


@dataclass
class LoadFinishedEvent:
    """
    Event indicating that an AMI Connect adapter has finished loading data for
    a given org and date range.
    """

    run_id: str
    org_id: str
    extract_range_start: datetime
    extract_range_end: datetime

    def to_json(self) -> str:
        data = {
            "run_id": self.run_id,
            "org_id": self.org_id,
            "extract_range_start": self.extract_range_start.isoformat(),
            "extract_range_end": self.extract_range_end.isoformat(),
        }
        return json.dumps(data)


class EventPublisher:
    """
    Publishes events based on AMI Connect adapter activity, e.g. when
    an adapter finishes loading data into a sink.

    Uses Amazon SQS to publish events to a queue.
    """

    def __init__(self, sqs=None):
        profile = get_global_aws_profile()
        region = get_global_aws_region()
        if sqs is None:
            if profile:
                session = boto3.Session(profile_name=profile)
                self.sqs = session.client("sqs", region_name=region)
            else:
                self.sqs = boto3.client("sqs", region_name=region)
        else:
            self.sqs = sqs

    def publish_load_finished_event(
        self, run_id: str, org_id: str, start_date: datetime, end_date: datetime
    ):
        event = LoadFinishedEvent(
            run_id=run_id,
            org_id=org_id,
            extract_range_start=start_date,
            extract_range_end=end_date,
        )
        # Get the queue URL
        queue_url = self.sqs.get_queue_url(QueueName=LOAD_FINISHED_QUEUE_NAME)[
            "QueueUrl"
        ]

        # Send a message
        response = self.sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(event.to_json()),
        )
        logger.info(
            f"Load finished event on queue {queue_url}: {response['MessageId']}"
        )


class EventSubscriber:
    """
    Used for debugging, prints events published to the SQS queue.
    """

    def __init__(self):
        profile = get_global_aws_profile()
        region = get_global_aws_region()
        if profile:
            session = boto3.Session(profile_name=profile)
            self.sqs = session.client("sqs", region_name=region)
        else:
            self.sqs = boto3.client("sqs", region_name=region)

    def print_message_from_queue(self, queue_name: str):
        # Get the queue URL
        queue_url = self.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

        # Receive messages
        response = self.sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=3,
        )

        messages = response.get("Messages", [])
        logger.info(f"Received {len(messages)} message(s) from queue {queue_url}")
        for message in messages:
            print(message)

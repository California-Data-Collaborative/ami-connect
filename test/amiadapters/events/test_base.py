from datetime import datetime
from unittest.mock import MagicMock

from amiadapters.events.base import EventPublisher
from test.base_test_case import BaseTestCase


class TestEventPublisher(BaseTestCase):
    def setUp(self):
        self.sqs_mock = MagicMock()
        self.publisher = EventPublisher(sqs=self.sqs_mock)

    def test_publish_load_finished_event_success(self):
        self.sqs_mock.send.return_value = True
        self.sqs_mock.get_queue_url.return_value = {"QueueUrl": "my-queue-url"}
        self.publisher.publish_load_finished_event(
            run_id="test_run",
            org_id="test_org",
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
        )
        self.sqs_mock.send_message.assert_called_once_with(
            QueueUrl="my-queue-url",
            MessageBody='"{\\"run_id\\": \\"test_run\\", \\"org_id\\": \\"test_org\\", \\"extract_range_start\\": \\"2024-01-01T00:00:00\\", \\"extract_range_end\\": \\"2024-01-02T00:00:00\\"}"',
        )

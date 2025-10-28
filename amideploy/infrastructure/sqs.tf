resource "aws_sqs_queue" "ami_connect_dag_event_queue" {
  name                      = "ami-connect-dag-event-queue"
  visibility_timeout_seconds = 3600   # 1 hour for processing
  message_retention_seconds  = 604800 # 7 days
  receive_wait_time_seconds = 20  # Enable long polling

  tags = {
    Name = var.ami_connect_tag
  }
}

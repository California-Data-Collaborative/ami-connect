resource "aws_sns_topic" "ami_connect_airflow_alerts" {
  name = "ami-connect-airflow-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  for_each = toset(var.alert_emails)

  topic_arn = aws_sns_topic.ami_connect_airflow_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}
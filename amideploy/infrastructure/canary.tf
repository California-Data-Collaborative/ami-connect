################################################################################
# CloudWatch Synthetics Canary (periodic health check)
################################################################################
# Make a separate S3 bucket for the canary so that things like retention
# policies aren't muddled
resource "aws_s3_bucket" "ami_connect_canaries_s3_bucket_name" {
  bucket = var.ami_connect_canaries_s3_bucket_name
  force_destroy = true
}

locals {
  # We will use the JS file's md5 to trigger canary/lambda changes when the script changes
  canary_code_hash = filemd5("${path.module}/canary/airflow_canary.js")
}

# Zip the JS file to prep for upload to S3
data "archive_file" "airflow_canary_zip" {
  type        = "zip"
  source_file  = "${path.module}/canary/airflow_canary.js"
  output_path = "${path.module}/build/airflow_canary-${local.canary_code_hash}.zip"
}

# Upload the zipped JS file to S3 so canary/lambda can use it
resource "aws_s3_object" "airflow_canary_code" {
  bucket = var.ami_connect_canaries_s3_bucket_name
  key    = "canaries/airflow_canary-${local.canary_code_hash}.zip"
  source = data.archive_file.airflow_canary_zip.output_path
  etag   = filemd5(data.archive_file.airflow_canary_zip.output_path)
}

# Create the canary, which runs a lambda that calls the JS file
resource "aws_synthetics_canary" "airflow_healthcheck" {
  name                 = "airflow-site-healthcheck"
  artifact_s3_location = "s3://${var.ami_connect_canaries_s3_bucket_name}/canaries/"
  execution_role_arn   = aws_iam_role.ami_connect_pipeline.arn
  handler              = "airflow_canary.handler"
  runtime_version      = "syn-nodejs-puppeteer-12.0"
  schedule {
    expression = "rate(5 minutes)"
  }
  s3_bucket = var.ami_connect_canaries_s3_bucket_name
  s3_key = aws_s3_object.airflow_canary_code.key
  s3_version = aws_s3_object.airflow_canary_code.version_id
  
  run_config {
    timeout_in_seconds = 60
    environment_variables = {
      AIRFLOW_HOSTNAME = var.airflow_hostname
    }
  }
  success_retention_period = 30
  failure_retention_period = 30
  start_canary             = true
}

# Alarm when canary check fails
resource "aws_cloudwatch_metric_alarm" "airflow_down_alarm" {
  alarm_name          = "airflow-site-down"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "SuccessPercent"
  namespace           = "CloudWatchSynthetics"
  period              = 300
  statistic           = "Average"
  threshold           = 90
  alarm_description   = "Alert when Airflow site is down"
  alarm_actions       = [aws_sns_topic.ami_connect_airflow_alerts.arn]
  dimensions = {
    CanaryName = aws_synthetics_canary.airflow_healthcheck.name
  }
}
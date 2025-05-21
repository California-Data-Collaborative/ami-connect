variable "aws_profile" {
  description = "The name of the AWS profile whose credentials you'll use with Terraform."
  type        = string
}

variable "aws_region" {
  description = "The name of the AWS region where your AMI Connect infrastructure will live."
  type        = string
  default     = "us-west-2"
}

variable "airflow_db_password" {
  description = "The password for the Airflow metastore database."
  type        = string
  sensitive   = true
}

variable "airflow_hostname" {
  description = "The host name you'd like for your Airflow website. This should already be a Route 53 instance. Ex: cadc-ami-connect.com"
  type        = string
}

variable "ssh_ip_allowlist" {
  description = "IP CIDR blocks that can SSH into our AWS resources. ex: [192.168.1.1/32]"
  type        = list(string)
}

variable "ami_connect_s3_bucket_name" {
  description = "Name for S3 bucket used for intermediate task outputs. Must be a globally unique name, so include your org name, e.g. my-company-ami-connect-bucket."
  type        = string
}

variable "ami_connect_tag" {
  description = "AWS tag used on all resources for this project."
  type        = string
  default     = "ami-connect"
}

variable "alert_emails" {
  description = "List of emails to subscribe to the Airflow SNS topic for Aiflow alerts"
  type        = list(string)
}

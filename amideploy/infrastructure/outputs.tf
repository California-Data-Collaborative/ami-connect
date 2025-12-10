output "airflow_server_private_key_pem" {
  value     = tls_private_key.airflow_server_private_key.private_key_pem
  sensitive = true
}

output "airflow_site_url" {
  description = "The URL for the Airflow webserver"
  value       = var.airflow_hostname
  sensitive   = true
}

output "airflow_alerts_sns_topic" {
  value     = aws_sns_topic.ami_connect_airflow_alerts.arn
  sensitive = false
}

output "airflow_server_ip" {
  value     = aws_eip.ami_connect_airflow_server_ip.public_ip
  sensitive = false
}

output "airflow_db_host" {
  description = "The hostname for the RDS instance"
  value     = aws_db_instance.ami_connect_airflow_metastore.endpoint
}

output "airflow_db_password" {
  description = "The password for the RDS instance"
  value       = var.airflow_db_password
  sensitive   = true
}
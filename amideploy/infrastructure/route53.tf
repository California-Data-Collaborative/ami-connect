data "aws_route53_zone" "airflow_route53_zone" {
  name = var.airflow_hostname
}

resource "aws_route53_record" "www" {
  zone_id = data.aws_route53_zone.airflow_route53_zone.zone_id
  name    = "www"
  type    = "A"
  ttl     = 300
  records = [aws_eip.ami_connect_airflow_server_ip.public_ip]
}

resource "aws_route53_record" "root" {
  zone_id = data.aws_route53_zone.airflow_route53_zone.zone_id
  name    = ""
  type    = "A"
  ttl     = 300
  records = [aws_eip.ami_connect_airflow_server_ip.public_ip]
}
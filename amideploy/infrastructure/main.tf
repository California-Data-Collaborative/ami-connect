terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  required_version = ">= 1.4.0"
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

provider "tls" {}

resource "tls_private_key" "airflow_server_private_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "generated_airflow_server_key" {
  key_name   = "auto-generated-key"
  public_key = tls_private_key.airflow_server_private_key.public_key_openssh
}

output "airflow_server_private_key_pem" {
  value     = tls_private_key.airflow_server_private_key.private_key_pem
  sensitive = true
}

resource "aws_instance" "ami_connect_airflow_server" {
  ami           = "ami-087f352c165340ea1"
  instance_type = "t3.xlarge"
  vpc_security_group_ids = [aws_security_group.airflow_server_sg.id]
  key_name      = aws_key_pair.generated_airflow_server_key.key_name

  tags = {
    Name = var.ami_connect_tag
  }
}

resource "aws_eip" "ami_connect_airflow_server_ip" {
  instance = aws_instance.ami_connect_airflow_server.id

  tags = {
    Name = var.ami_connect_tag
  }
}

output "airflow_server_ip" {
  value     = aws_eip.ami_connect_airflow_server_ip.public_ip
  sensitive = false
}

resource "aws_security_group" "airflow_server_sg" {
  name        = "airflow_server_sg"
  description = "Allow Airflow app server traffic"

  ingress {
    description = "Allow SSH from allowed IPs only"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.ssh_ip_allowlist
  }

  ingress {
    description = "Allow HTTP from public internet"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = var.ami_connect_tag
  }
}

resource "aws_security_group" "airflow_db_sg" {
  name        = "airflow_db_sg"
  description = "Allow PostgreSQL access from Airflow app servers"

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.airflow_server_sg.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = var.ami_connect_tag
  }
}

resource "aws_db_instance" "ami_connect_airflow_metastore" {
  identifier                 = "prod-postgres-db"
  engine                     = "postgres"
  engine_version             = "16.8"
  instance_class             = "db.t4g.micro"
  allocated_storage          = 100
  storage_type               = "gp3"
  storage_encrypted          = true
  multi_az                   = false
  backup_retention_period    = 7
  deletion_protection        = true
  enabled_cloudwatch_logs_exports = ["postgresql"]
  db_name                    = "airflow_db"
  username                   = "airflow_user"
  password                   = var.airflow_db_password
  vpc_security_group_ids     = [aws_security_group.airflow_db_sg.id]
  skip_final_snapshot        = false
  final_snapshot_identifier  = "final-snapshot-prod-db"

  tags = {
    Name = var.ami_connect_tag
  }
}

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

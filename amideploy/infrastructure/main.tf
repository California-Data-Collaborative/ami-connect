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

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "ami_connect_pipeline" {
  name = "ami-connect-pipeline"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # Production: allow EC2 instances to assume the role
      {
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      },
      # Development: allow any IAM user from the same AWS account to assume the role
      {
        Effect = "Allow",
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name = var.ami_connect_tag
  }
}

resource "aws_iam_policy" "ami_connect_pipeline_s3_policy" {
  name        = "ami-connect-pipeline-s3-access"
  description = "Grants read/write access to the AMI Connect S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ],
        Resource = "${aws_s3_bucket.ami_connect_s3_bucket.arn}/*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket"
        ],
        Resource = aws_s3_bucket.ami_connect_s3_bucket.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_secrets_manager" {
  role       = aws_iam_role.ami_connect_pipeline.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}

# Create an Instance Profile (which EC2 will use)
resource "aws_iam_instance_profile" "ami_instance_profile" {
  name = "ami-connect-pipeline-instance-profile"
  role = aws_iam_role.ami_connect_pipeline.name
}

resource "aws_iam_role_policy_attachment" "ami_connect_attach_policy" {
  role       = aws_iam_role.ami_connect_pipeline.name
  policy_arn = aws_iam_policy.ami_connect_pipeline_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent_attach_policy" {
  role       = aws_iam_role.ami_connect_pipeline.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}


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
  instance_type = var.ami_connect_airflow_server_instance_size
  vpc_security_group_ids = [aws_security_group.airflow_server_sg.id]
  key_name      = aws_key_pair.generated_airflow_server_key.key_name

  iam_instance_profile = aws_iam_instance_profile.ami_instance_profile.name

  user_data = <<-EOF
              #!/bin/bash
              sudo yum install -y amazon-cloudwatch-agent
              cat <<EOC > /opt/aws/amazon-cloudwatch-agent/bin/config.json
              {
                "agent": {
                  "metrics_collection_interval": 60,
                  "run_as_user": "root"
                },
                "metrics": {
                  "append_dimensions": {
                    "InstanceId": "$${aws:InstanceId}"
                  },
                  "metrics_collected": {
                    "mem": {
                      "measurement": [
                        "mem_used_percent"
                      ],
                      "metrics_collection_interval": 60
                    },
                    "disk": {
                      "measurement": [
                        "used_percent"
                      ],
                      "resources": [
                        "/"
                      ],
                      "metrics_collection_interval": 60
                    },
                    "swap": {
                      "measurement": [
                        "swap_used_percent"
                      ],
                      "metrics_collection_interval": 60
                    }
                  }
                }
              }
              EOC

              /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
                -a fetch-config -m ec2 \
                -c file:/opt/aws/amazon-cloudwatch-agent/bin/config.json -s
              EOF

  tags = {
    Name = var.ami_connect_tag
  }
}

resource "aws_sns_topic" "ami_connect_airflow_alerts" {
  name = "ami-connect-airflow-alerts"
}

output "airflow_alerts_sns_topic" {
  value     = aws_sns_topic.ami_connect_airflow_alerts.arn
  sensitive = false
}

resource "aws_sns_topic_subscription" "email" {
  for_each = toset(var.alert_emails)

  topic_arn = aws_sns_topic.ami_connect_airflow_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_iam_policy" "airflow_sns_publish" {
  name        = "AirflowSnsPublishPolicy"
  description = "Allows Airflow to publish messages to the SNS topic"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sns:Publish"
        Resource = aws_sns_topic.ami_connect_airflow_alerts.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_attach_sns" {
  role       = aws_iam_role.ami_connect_pipeline.name
  policy_arn = aws_iam_policy.airflow_sns_publish.arn
}

# SQS queue
resource "aws_sqs_queue" "ami_connect_dag_event_queue" {
  name                      = "ami-connect-dag-event-queue"
  visibility_timeout_seconds = 3600   # 1 hour for processing
  message_retention_seconds  = 604800 # 7 days
  receive_wait_time_seconds = 20  # Enable long polling

  tags = {
    Name = var.ami_connect_tag
  }
}

# Allow EC2 to use SQS
resource "aws_iam_policy" "sqs_access_policy" {
  name        = "ami-connect-sqs-access"
  description = "Allow EC2 to send and receive messages from SQS queue"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueUrl",
          "sqs:GetQueueAttributes"
        ]
        Resource = aws_sqs_queue.ami_connect_dag_event_queue.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_sqs_policy" {
  role       = aws_iam_role.ami_connect_pipeline.name
  policy_arn = aws_iam_policy.sqs_access_policy.arn
}

# Elastic IP
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
    description = "Allow SSH from public internet"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
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
  identifier                 = "ami-connect-airflow-db"
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
  final_snapshot_identifier  = "final-snapshot-ami-connect-airflow-db"

  tags = {
    Name = var.ami_connect_tag
  }
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

resource "aws_s3_bucket" "ami_connect_s3_bucket" {
  bucket = var.ami_connect_s3_bucket_name
  force_destroy = true

  tags = {
    Name = var.ami_connect_tag
  }
}

# Expire objects after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "ami_connect_s3_bucket_lifecycle" {
  bucket = aws_s3_bucket.ami_connect_s3_bucket.id

  rule {
    id     = "expire-objects-after-90-days"
    status = "Enabled"

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

resource "aws_s3_bucket_public_access_block" "ami_connect_s3_bucket" {
  bucket = aws_s3_bucket.ami_connect_s3_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

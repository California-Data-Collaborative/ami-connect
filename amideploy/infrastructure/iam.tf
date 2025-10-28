# Main role for all AMI Connect operations
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
      },
      # Lambdas can assume the role too
      {
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}


# Create an Instance Profile (which EC2 will use)
resource "aws_iam_instance_profile" "ami_instance_profile" {
  name = "ami-connect-pipeline-instance-profile"
  role = aws_iam_role.ami_connect_pipeline.name
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
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetBucketLocation",
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListAllMyBuckets"
        ],
        Resource = [
          aws_s3_bucket.ami_connect_canaries_s3_bucket_name.arn,
          "${aws_s3_bucket.ami_connect_canaries_s3_bucket_name.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_secrets_manager" {
  role       = aws_iam_role.ami_connect_pipeline.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}

resource "aws_iam_role_policy_attachment" "ami_connect_attach_policy" {
  role       = aws_iam_role.ami_connect_pipeline.name
  policy_arn = aws_iam_policy.ami_connect_pipeline_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent_attach_policy" {
  role       = aws_iam_role.ami_connect_pipeline.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
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

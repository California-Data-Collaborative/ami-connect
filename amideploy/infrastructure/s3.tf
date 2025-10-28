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

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.18"
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

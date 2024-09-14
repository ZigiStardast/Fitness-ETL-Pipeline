terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region
  shared_config_files      = [var.aws_config]
  shared_credentials_files = [var.aws_credentials]
  profile                  = var.profile
}


# S3 Bucket
resource "aws_s3_bucket" "fitness-bucket" {
    bucket = var.s3-bucket
    force_destroy = true 
}

/*

# Redshift cluster
resource "aws_redshift_cluster" "fitness-cluster" {
  cluster_identifier = var.redshift_identifier
  database_name      = var.db
  master_username    = var.master_username
  master_password    = var.master_password
  node_type          = "dc2.large"
  cluster_type       = "single-node"
  iam_roles = []
  vpc_security_group_ids = []
  skip_final_snapshot = true
  publicly_accessible = true
}

# IAM Role
resource "aws_iam_role" "redshift_role" {
  name = "RedShiftLoadRole"
  managed_policy_arns = ["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"]  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      },
    ]
  })
}

# Security Group for Redshift
resource "aws_security_group" "sg-redshift" {
  name = "sg-redshift"  
  egress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1" 
    cidr_blocks = ["0.0.0.0/0"] 
  }
}


*/
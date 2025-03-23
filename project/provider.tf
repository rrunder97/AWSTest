terraform {
  required_version = ">= 1.3.0"
  backend "s3" {
    bucket         = "your-terraform-state-bucket"
    key            = "ec2-instance/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "terraform-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
}

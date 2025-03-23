provider "aws" {
  region = "us-east-1"
}

#Specified repo if net new
resource "aws_ecr_repository" "lambda_repo" {
  name = "lambda-docker-repo"
}

resource "aws_lambda_function" "my_lambda" {
  function_name     = "my-lambda-docker"
  role             = aws_iam_role.lambda_role.arn
  image_uri        = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
  package_type     = "Image"
  timeout          = 10
}

resource "aws_iam_role" "lambda_role" {
  name = "lambda-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}
resource "aws_iam_policy" "lambda_s3_policy" {
  name        = "LambdaPolicy"
  description = "Allow Lambda to upload objects to S3 and pull from repo"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.my_bucket.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability"
        ]
        Resource = "${aws_ecr_repository.lambda_repo.arn}"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

output "ecr_url" {
  value = aws_ecr_repository.lambda_repo.repository_url
}

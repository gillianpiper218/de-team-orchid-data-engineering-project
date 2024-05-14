variable "ingestion_lambda" {
    type = string
    default = "ingestion_lambda"
}

resource "aws_lambda_function" "ingestion_lambda" {
    function_name = "${var.lambda_name}"
    role = aws_iam_role.lambda_role.arn
    handler = "ingestion_lambda.lambda_handler"
    runtime = "python3.11"
}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion_lambda.py"
  output_path = "${path.module}/../function.zip"
}

resource "aws_iam_role" "lambda_role" {
    name_prefix = "role-${var.lambda_name}"
    assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role_policy_attachment" "lambda_s3_put_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_ingestion_put_policy.arn
}

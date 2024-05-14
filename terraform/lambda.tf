variable "ingestion_lambda" {
    type = string
    default = "ingestion_lambda"
}

resource "aws_lambda_function" "ingestion_lambda" {
    function_name = "${var.ingestion_lambda}"
    filename = data.archive_file.lambda.output_path
    role = aws_iam_role.ingestion_lambda_role.arn
    handler = "ingestion_lambda.lambda_handler"
    runtime = "python3.11"
}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion_lambda.py"
  output_path = "${path.module}/../function.zip"
}


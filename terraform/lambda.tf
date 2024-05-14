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

    layers        = [aws_lambda_layer_version.lambda_layer.arn]
    depends_on    = [aws_cloudwatch_log_group.ingestion_lambda_log_group]

    logging_config {
    log_format = "Text"
    }

  }

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion_lambda.py"
  output_path = "${path.module}/../function.zip"
}


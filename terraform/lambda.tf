
resource "aws_lambda_function" "ingestion_function" {
    function_name = "${var.ingestion_function_name}"
    filename = data.archive_file.lambda.output_path
    role = aws_iam_role.ingestion_function_role.arn
    handler = "ingestion_function.lambda_handler"
    depends_on    = [aws_cloudwatch_log_group.ingestion_function_log_group]
    runtime = "python3.11"
  }

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion_function.py"
  output_path = "${path.module}/../function.zip"
}


resource "aws_lambda_permission" "allow_eventbridge" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_function.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

# resource "aws_lambda_layer_version" "requests_layer" {
#   layer_name = "requests_layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket = aws_s3_bucket.code_bucket.bucket
#   s3_key = "quotes/layer.zip"
# }



resource "aws_lambda_function" "processing_function" {
    function_name = var.processing_function_name
    filename = data.archive_file.processing_lambda.output_path
    role = aws_iam_role.processing_function_role.arn
    handler = "processing_lambda.lambda_handler" 
    depends_on    = [aws_cloudwatch_log_group.processing_function_log_group]
    source_code_hash = data.archive_file.processing_lambda.output_base64sha256
    layers = [
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-boto3:10",
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-numpy:6",
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-pandas:10",
    aws_lambda_layer_version.modules.arn
]

    runtime = "python3.11"
  }

data "archive_file" "processing_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/processing_lambda.py" 
  output_path = "${path.module}/../function_processing.zip"
}


resource "aws_lambda_permission" "allow_eventbridge_processing" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processing_function.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.processing_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_iam_role_policy_attachment" "secret_manager_policy_attachment_processing" {
  role       = aws_iam_role.processing_function_role.name
  policy_arn = aws_iam_policy.secret_manager_policy.arn
}
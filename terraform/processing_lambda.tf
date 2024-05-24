
resource "aws_lambda_function" "processing_function" {
    function_name = var.processing_function_name
    filename = data.archive_file.processing_lambda.output_path
    role = aws_iam_role.processing_function_role.arn
    handler = "processing_lambda.lambda_handler" #to confirm processing function name
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
  source_file = "${path.module}/../src/processing_lambda.py" #to confirm processing function name
  output_path = "${path.module}/../function_processing.zip"
}

# resource "aws_s3_bucket_notification" "s3_ingestion_notification" {
#   bucket = aws_s3_bucket.ingestion_s3_bucket.bucket

#   lambda_function {
#     lambda_function_arn = aws_lambda_function.processing_function.arn
#     events              = ["s3:ObjectCreated:*"] 
# }
# }

# resource "aws_lambda_permission" "allow_s3_to_invoke_processing_lambda" {
#   statement_id  = "AllowS3InvokeLambda"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.processing_function.function_name
#   principal     = "s3.amazonaws.com"
#   source_arn    = aws_s3_bucket.ingestion_s3_bucket.arn 
# }

resource "aws_lambda_permission" "allow_eventbridge_processing" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processing_function.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.processing_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}
resource "aws_lambda_function" "processing_function" {
    function_name = var.processing_function_name
    filename = data.archive_file.processing_lambda.output_path
    role = aws_iam_role.processing_function_role.arn
    handler = "lambda-ellen.lambda_handler" #to confirm ingestion function name
    depends_on    = [aws_cloudwatch_log_group.processing_function_log_group]

    runtime = "python3.11"
  }

data "archive_file" "processing_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda-ellen.py" #to confirm processing function name
  output_path = "${path.module}/../function.zip"
}

resource "aws_s3_bucket_notification" "s3_ingestion_notification" {
  bucket = aws_s3_bucket.ingestion_s3_bucket.bucket

  lambda_function {
    lambda_function_arn = aws_lambda_function.processing_function.arn
    events              = ["s3:ObjectCreated:*"] 
}
}

resource "aws_lambda_permission" "allow_s3_to_invoke_processing_lambda" {
  statement_id  = "AllowS3InvokeLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processing_function.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.ingestion_s3_bucket.arn 
}
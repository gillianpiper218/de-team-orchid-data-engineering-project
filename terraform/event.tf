resource "aws_cloudwatch_event_rule" "scheduler" {
    name = "scheduler"
    description = "run lambda function every 5 minutes"
    schedule_expression = "rate(5 minutes)"
}


resource "aws_cloudwatch_event_target" "check_every_five_minutes" {
    rule = aws_cloudwatch_event_rule.scheduler.name
    target_id = "ingestion_lambda"
    arn = aws_lambda_function.ingestion_lambda.arn
}

resource "aws_cloudwatch_log_group" "ingestion_lambda_log_group" {
  name              = "/aws/lambda/${var.ingestion_lambda}"
  retention_in_days = 7
  lifecycle {
    prevent_destroy = false
  }
}
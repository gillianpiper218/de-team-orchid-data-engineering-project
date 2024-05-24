resource "aws_cloudwatch_event_rule" "scheduler" {
    name = "scheduler"
    description = "run ingestion lambda function every 5 minutes"
    schedule_expression = "rate(5 minutes)"
}


resource "aws_cloudwatch_event_rule" "processing_scheduler" {
    name = "processing-scheduler"
    description = "run processing lambda function every 7 minutes"
    schedule_expression = "rate(2 minutes)"
}

resource "aws_cloudwatch_event_target" "check_every_five_minutes" {
    rule = aws_cloudwatch_event_rule.scheduler.name
    target_id = "${var.ingestion_function_name}"
    arn = aws_lambda_function.ingestion_function.arn
}

resource "aws_cloudwatch_event_target" "trigger_on_seven" {
    rule = aws_cloudwatch_event_rule.processing_scheduler.name
    target_id = "${var.processing_function_name}"
    arn = aws_lambda_function.processing_function.arn
}

resource "aws_cloudwatch_log_group" "ingestion_function_log_group" {
  name              = "/aws/lambda/${var.ingestion_function_name}"
  retention_in_days = 7
  lifecycle {
    prevent_destroy = false
  }
}

resource "aws_cloudwatch_log_group" "processing_function_log_group" {
  name              = "/aws/lambda/${var.processing_function_name}"
  retention_in_days = 7
  lifecycle {
    prevent_destroy = false
  }
}
# re-run
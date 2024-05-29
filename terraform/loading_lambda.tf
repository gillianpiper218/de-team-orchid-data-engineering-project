resource "aws_lambda_function" "loading_function" {
    function_name = var.loading_function_name
    filename = data.archive_file.loading_lambda.output_path
    role = aws_iam_role.loading_function_role.arn
    handler = "loading_lambda.lambda_handler" 
    depends_on    = [aws_cloudwatch_log_group.loading_function_log_group]
    source_code_hash = data.archive_file.processing_lambda.output_base64sha256
    layers = [
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-boto3:10",
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-numpy:6",
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-pandas:10",
    aws_lambda_layer_version.modules.arn
]

    runtime = "python3.11"
  }

  data "archive_file" "loading_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/loading_lambda.py" 
  output_path = "${path.module}/../function_loading.zip"
}

resource "aws_lambda_permission" "allow_eventbridge_loading" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.loading_function.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.loading_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}


resource "aws_cloudwatch_metric_alarm" "loading_lambda_alarm" {
    alarm_name = "LoadingLambdaCloudwatchAlarm"
    namespace = "LoadingLambdaLogErrors"
    metric_name = "ErrorSampleCount"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = 1
    threshold = 1
    statistic = "SampleCount"
    period = 120
    alarm_description = "This triggers an alarm when the loading lambda function errors"
    alarm_actions = [ "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${var.sns_alert_topic_name}" ]
}

resource "aws_cloudwatch_log_metric_filter" "loading_lambda_error_filter" {
	name = "LoadingLambdaErrorFilter"
	pattern = "Error"
	log_group_name = "/aws/lambda/${var.loading_function_name}"
	
	metric_transformation {
		name = "ErrorSampleCount"
		namespace = "LoadingLambdaLogErrors"
		value = "1"
		}
    depends_on = [ aws_cloudwatch_log_group.loading_function_log_group ]
	}

resource "aws_cloudwatch_event_rule" "loading_scheduler" {
    name = "loading_scheduler"
    description = "run loading lambda function every 9 minutes"
    schedule_expression = "rate(9 minutes)"
}

resource "aws_cloudwatch_event_target" "trigger_on_ten" {
    rule = aws_cloudwatch_event_rule.loading_scheduler.name
    target_id = "${var.loading_function_name}"
    arn = aws_lambda_function.loading_function.arn
}

resource "aws_cloudwatch_log_group" "loading_function_log_group" {
  name              = "/aws/lambda/${var.loading_function_name}"
  retention_in_days = 7
  lifecycle {
    prevent_destroy = false
  }
}

resource "aws_iam_role_policy_attachment" "secret_manager_policy_attachment_loading" {
  role       = aws_iam_role.loading_function_role.name
  policy_arn = aws_iam_policy.secret_manager_policy.arn
}